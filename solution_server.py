#!/usr/bin/env python3
"""
CS3103 Assignment 4 - LiveRecall server.

Required evaluator command:
    python solution_server.py --port 8765

Pipeline
    source -> RevisionState.accept  drop stale, duplicate and post-final updates
           -> FinalHistory.record   finals only; bounded by age and item count
           -> broadcast             enqueue only; never waits for a socket
           -> ClientState           bounded per-client queue + one sender task

Per-client overload policy
    A WebSocket ping written after caption data is answered only once the
    client has read that data (TCP and WebSocket keep order), so a late pong
    measures backlog hidden in socket and client buffers. "Late" is judged
    against the kernel's own TCP RTT/RTO estimate (TCP_INFO), so network delay,
    jitter and a retransmission are not mistaken for a slow reader.

    normal   every revision is forwarded in order (full update coverage).
    blocked  pong late by BLOCK_MARGIN: stop writing. Queued provisionals are
             coalesced (newest per segment, none once the segment is final),
             so the newest state goes out as soon as the pipe drains.
    lagging  a network bottleneck: forward finals only, holding the newest
             provisional per segment, for an adaptive hold that starts short
             and doubles if lag recurs soon after it ends. Triggered when the
             kernel's smoothed RTT exceeds its minimum RTT by QUEUE_DELAY (a
             standing queue our own traffic is building, e.g. a bandwidth
             cap; the minimum is shared by all connections from one address,
             and the hold only counts down once the queue has drained) or
             when a pong is late by LAG_MARGIN with bytes still
             unacknowledged. Pure delay, jitter and loss raise neither signal.
    resync   close with 1013 so the client reconnects with its finalized cursor
             and gets every missed final in one HISTORY message instead of
             draining a stale backlog. Used when a pong is late although the
             client's kernel acknowledged everything (the reader itself is
             slow), or when a final has waited RESYNC_AGE in the queue.
    reader   after a reader resync the client's consumption rate is known
             (messages it drained / time until it reconnected). It then gets
             finals only, or, if it cannot even keep up with the finals,
             periodic HISTORY batches ("batched") so that no final is stranded
             in its receive buffer. Finals-only lasts an adaptive reader hold
             (15 s, doubling to 60 s only while it keeps proving slow), after
             which it returns to full updates; batching lasts 60 s, because a
             failed full-rate re-test would strand finals.

Every structure is bounded: history by age and count, revision state by a
segment window, each client queue by item count, held provisionals per client,
and the remembered lag state by entry count and age. A periodic cyclic garbage
collection keeps process memory flat under heavy reconnect churn.
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import gc
import json
import math
import random
import socket
import struct
import time
from collections import OrderedDict, deque
from typing import Any

import websockets

# --- recovery history -------------------------------------------------------
HISTORY_WINDOW_SECONDS = 60      # most context a newly joining client may request
HISTORY_RETAIN_SECONDS = 90      # >= 60 so a resuming client's whole gap is covered
HISTORY_MAX_ITEMS = 20_000       # hard cap for an extremely bursty source
HISTORY_CHUNK_ITEMS = 2_000      # items per HISTORY message (well below 1 MiB)
REVISION_SEGMENT_WINDOW = 4_096  # open segments tracked behind the newest one
RATE_WINDOW_SECONDS = 20         # window for the source's update/final rates

# --- per-client queue and flow control --------------------------------------
QUEUE_MAX_ITEMS = 256            # pending messages per client
COALESCE_BACKLOG = 8             # a queue this long counts as backpressure
SEND_TIMEOUT_SECONDS = 10.0      # one stalled write closes only that client
MONITOR_TICK_SECONDS = 0.5       # how often each client's state is checked
PROBE_INTERVAL_SECONDS = 1.0     # at most one probe per second (bandwidth cost)
IDLE_PROBE_SECONDS = 2.0         # probe an idle client this often
PROBE_TIMEOUT_SECONDS = 15.0     # an unanswered probe this old closes the client
BLOCK_MARGIN_SECONDS = 0.25      # pong allowance beyond srtt + 4*rttvar
LAG_MARGIN_SECONDS = 1.0         # pong allowance beyond srtt + rto
RESYNC_AGE_SECONDS = 3.0         # a queued final may wait this long
QUEUE_DELAY_SECONDS = 0.25       # srtt - min_rtt beyond this: standing queue
QUEUE_CLEAR_SECONDS = 0.12       # ... and the hold only counts once below this
CONGESTED_TICKS = 2              # consecutive congested checks before acting
PATH_BASELINE_SECONDS = 300.0    # shared min-RTT per remote address (as TCP's)
PATH_BASELINE_MAX = 4_096
NETWORK_HOLD_BASE_SECONDS = 8.0  # first finals-only hold after a network lag
NETWORK_HOLD_MAX_SECONDS = 40.0
HOLD_JITTER = 0.25               # +-25%: clients return to full rate staggered
HOLD_RESET_SECONDS = 30.0        # lag recurring within this doubles the hold
READER_HOLD_BASE_SECONDS = 15.0  # first finals-only hold for a proven slow reader
READER_HOLD_MAX_SECONDS = 60.0   # doubles each time it is still slow afterwards
BATCH_SECONDS = 1.0              # batched client: resync once a final waits this
BATCH_RATE_MARGIN = 1.2          # batch if reader rate < finals rate * margin
FULL_RATE_FRACTION = 0.9         # reader rate >= this * update rate: not slow
RECONNECT_ALLOWANCE_SECONDS = 0.5
HELD_REFRESH_SECONDS = 1.5       # long segments still show progress when lagging
CLIENT_MEMORY_SECONDS = 120.0    # remembered lag state per client_id
CLIENT_MEMORY_MAX = 4_096
GC_INTERVAL_SECONDS = 10.0       # collect cycles left by closed connections

CLOSE_TRY_AGAIN = 1013


# --- validated updates ------------------------------------------------------

class Outgoing:
    """One message shared by every recipient queue (never copied per client)."""

    __slots__ = ("raw", "segment", "revision", "final", "queued_at")

    def __init__(self, raw: str | bytes, segment: int | None = None,
                 revision: int = 0, final: bool = False,
                 queued_at: float = 0.0) -> None:
        self.raw = raw
        self.segment = segment          # None for HISTORY / control messages
        self.revision = revision
        self.final = final
        self.queued_at = queued_at

    @property
    def provisional(self) -> bool:
        return self.segment is not None and not self.final


def valid_update(msg: Any, session: str) -> bool:
    """Minimum CAPTION_UPDATE interface; extra fields are allowed and kept."""
    if not isinstance(msg, dict) or msg.get("type") != "CAPTION_UPDATE":
        return False
    if msg.get("session") != session:
        return False
    segment, revision = msg.get("segment"), msg.get("revision")
    # bool is an int subclass, but it is not a protocol sequence number.
    if type(segment) is not int or segment < 1:
        return False
    if type(revision) is not int or revision < 1:
        return False
    if type(msg.get("final")) is not bool or not isinstance(msg.get("text"), str):
        return False
    source_ts = msg.get("source_ts")
    if type(source_ts) not in (int, float) or not math.isfinite(source_ts):
        return False
    return True


class RevisionState:
    """Authoritative per-segment revision watermark for one lecture.

    A segment accepts only a strictly newer revision, and a final is terminal.
    State is kept per segment, so overlapping segments (segment N+1 starting
    before N is final) are handled. Segments more than REVISION_SEGMENT_WINDOW
    behind the newest one are closed, which keeps memory bounded. The history
    cache may forget captions; this object alone decides what is new.
    """

    def __init__(self, window: int = REVISION_SEGMENT_WINDOW) -> None:
        self.window = window
        self.session: str | None = None
        self.segments: dict[int, tuple[int, bool]] = {}
        self.floor = 0              # every segment <= floor is closed
        self.max_segment = 0

    def accept(self, msg: Any, source_session: str) -> bool:
        """Validate and commit atomically; False changes nothing."""
        if not isinstance(source_session, str) or not source_session:
            return False
        if self.session is not None and self.session != source_session:
            return False
        if not valid_update(msg, source_session):
            return False
        segment, revision = msg["segment"], msg["revision"]
        if segment <= self.floor:
            return False
        previous = self.segments.get(segment)
        if previous is not None and (previous[1] or revision <= previous[0]):
            return False
        self.session = source_session
        self.segments[segment] = (revision, msg["final"])
        if segment > self.max_segment:
            self.max_segment = segment
            new_floor = segment - self.window
            if new_floor > self.floor:
                if new_floor - self.floor > len(self.segments):
                    self.segments = {s: v for s, v in self.segments.items()
                                     if s > new_floor}
                else:
                    for closed in range(self.floor + 1, new_floor + 1):
                        self.segments.pop(closed, None)
                self.floor = new_floor
        return True


class FinalHistory:
    """Bounded recovery cache of finalized captions (never used for dedupe).

    Entries are kept in arrival order with the server's monotonic arrival
    time, so recovery windows do not depend on the source's clock. Each entry
    stores the final's JSON text once, so a HISTORY message is a string join.
    """

    def __init__(self, retain_seconds: float = HISTORY_RETAIN_SECONDS,
                 max_items: int = HISTORY_MAX_ITEMS, *, clock=time.monotonic) -> None:
        if not math.isfinite(retain_seconds) or retain_seconds < HISTORY_WINDOW_SECONDS:
            raise ValueError("History must retain at least 60 seconds")
        if max_items < 1:
            raise ValueError("max_items must be positive")
        self.retain_seconds = retain_seconds
        self.max_items = max_items
        self.clock = clock
        self._items: deque[tuple[float, int, int, str]] = deque()
        self._segments: set[int] = set()

    def __len__(self) -> int:
        return len(self._items)

    def _drop_oldest(self) -> None:
        _, segment, _, _ = self._items.popleft()
        self._segments.discard(segment)

    def _prune(self, now: float) -> None:
        cutoff = now - self.retain_seconds
        while self._items and self._items[0][0] < cutoff:
            self._drop_oldest()

    def record(self, segment: int, revision: int, payload: str) -> None:
        """Cache one accepted final; a segment is stored at most once."""
        now = self.clock()
        self._prune(now)
        if segment in self._segments:
            return
        self._items.append((now, segment, revision, payload))
        self._segments.add(segment)
        while len(self._items) > self.max_items:
            self._drop_oldest()

    def snapshot(self, last_segment: int = 0, last_revision: int = 0,
                 seconds: float = 30) -> list[str]:
        """Finals newer than the cursor that arrived in the last `seconds`,
        ordered oldest to newest."""
        now = self.clock()
        self._prune(now)
        if not math.isfinite(seconds) or seconds <= 0:
            return []
        cutoff = now - min(seconds, self.retain_seconds)
        cursor = (last_segment, last_revision)
        items = [(segment, revision, payload)
                 for arrived, segment, revision, payload in self._items
                 if arrived >= cutoff and (segment, revision) > cursor]
        items.sort(key=lambda item: (item[0], item[1]))
        return [payload for _, _, payload in items]


class Lecture:
    """State of the current lecture. It is replaced only when a finished
    lecture (SOURCE_DONE) is followed by a source starting again at segment 1."""

    def __init__(self, number: int, session: str | None = None) -> None:
        self.number = number
        self.session = session
        self.revisions = RevisionState()
        self.history = FinalHistory()
        self.ended = False
        self._buckets: deque[list[int]] = deque()    # [second, updates, finals]

    def note(self, final: bool, now: float) -> None:
        second = int(now)
        if self._buckets and self._buckets[-1][0] == second:
            bucket = self._buckets[-1]
        else:
            bucket = [second, 0, 0]
            self._buckets.append(bucket)
        bucket[1] += 1
        bucket[2] += int(final)
        while self._buckets and self._buckets[0][0] <= second - RATE_WINDOW_SECONDS:
            self._buckets.popleft()

    def rates(self, now: float) -> tuple[float, float]:
        """(updates/s, finals/s) the source produced recently."""
        while self._buckets and self._buckets[0][0] <= int(now) - RATE_WINDOW_SECONDS:
            self._buckets.popleft()
        if not self._buckets:
            return 0.0, 0.0
        span = min(float(RATE_WINDOW_SECONDS), max(1.0, now - self._buckets[0][0]))
        return (sum(b[1] for b in self._buckets) / span,
                sum(b[2] for b in self._buckets) / span)


LECTURE = Lecture(1)


def history_messages(payloads: list[str], session: str, lecture: int) -> list[str]:
    """Encode finals as HISTORY messages, oldest to newest (normally one)."""
    head = ('{"type":"HISTORY","session":%s,"lecture":%d,"items":['
            % (json.dumps(session), lecture))
    if not payloads:
        return [head + "]}"]
    return [head + ",".join(payloads[i:i + HISTORY_CHUNK_ITEMS]) + "]}"
            for i in range(0, len(payloads), HISTORY_CHUNK_ITEMS)]


# --- remembered lag state ---------------------------------------------------

class LagMemory:
    """What a reconnecting client's previous connections taught us."""

    __slots__ = ("net_hold", "last_exit", "lag_until", "reader_at", "reader_sent",
                 "reader_first", "reader_hold", "reader_until", "batched_until",
                 "updated")

    def __init__(self) -> None:
        self.net_hold = NETWORK_HOLD_BASE_SECONDS
        self.last_exit = -math.inf      # when the last lagging period ended
        self.lag_until = 0.0            # network lag still in force
        self.reader_at = -math.inf      # last resync caused by a slow reader
        self.reader_sent = 0            # captions written on that connection
        self.reader_first = math.inf    # first caption write on that connection
        self.reader_hold = READER_HOLD_BASE_SECONDS
        self.reader_until = 0.0         # slow reader: finals only until then
        self.batched_until = 0.0        # very slow reader: HISTORY batches until then
        self.updated = 0.0


LAG_MEMORY: OrderedDict[Any, LagMemory] = OrderedDict()


def client_key(session: str, client_id: Any) -> Any:
    if isinstance(client_id, bool) or not isinstance(client_id, (int, str)):
        return None
    if isinstance(client_id, str) and len(client_id) > 128:
        return None
    return (session, client_id)


# --- network estimate -------------------------------------------------------

_TCP_INFO = getattr(socket, "TCP_INFO", None)


def tcp_info(sock: Any) -> tuple[float, float, float, int, int, float] | None:
    """(srtt, rttvar, rto, unacked packets, notsent bytes, min_rtt) in seconds
    from Linux TCP_INFO; min_rtt falls back to srtt on old kernels."""
    if sock is None or _TCP_INFO is None:
        return None
    try:
        raw = sock.getsockopt(socket.IPPROTO_TCP, _TCP_INFO, 152)
    except (OSError, AttributeError, TypeError, ValueError):
        return None
    if len(raw) < 76:
        return None
    (rto,) = struct.unpack_from("I", raw, 8)
    (unacked,) = struct.unpack_from("I", raw, 24)
    srtt, rttvar = struct.unpack_from("II", raw, 68)
    notsent = struct.unpack_from("I", raw, 144)[0] if len(raw) >= 148 else 0
    min_rtt = struct.unpack_from("I", raw, 148)[0] if len(raw) >= 152 else 0
    if min_rtt <= 0 or min_rtt == 0xFFFFFFFF:
        min_rtt = srtt
    return srtt / 1e6, rttvar / 1e6, rto / 1e6, unacked, notsent, min_rtt / 1e6


# Connections from one remote address share a path, so they share a baseline:
# a client whose first RTT samples were taken behind an existing queue (e.g.
# 100 handshakes through a capped link) still sees the path's true minimum.
PATH_MIN_RTT: OrderedDict[Any, tuple[float, float]] = OrderedDict()


def path_baseline(address: Any, min_rtt: float, now: float) -> float:
    """Fold this connection's min_rtt into its address's baseline; return it."""
    if address is None:
        return min_rtt
    known = PATH_MIN_RTT.get(address)
    if known is None or now - known[1] > PATH_BASELINE_SECONDS or min_rtt <= known[0]:
        known = (min_rtt, now)
    PATH_MIN_RTT[address] = known
    PATH_MIN_RTT.move_to_end(address)
    while len(PATH_MIN_RTT) > PATH_BASELINE_MAX:
        PATH_MIN_RTT.popitem(last=False)
    return min(known[0], min_rtt)


# --- per-client state -------------------------------------------------------

class ClientState:
    def __init__(self, ws: Any, session: str = "L1", key: Any = None) -> None:
        self.ws = ws
        self.session = session
        self.key = key
        self.loop = asyncio.get_running_loop()
        self.pending: deque[Outgoing] = deque()
        self.held: dict[int, tuple[Outgoing, float]] = {}
        self.wake = asyncio.Event()
        self.sender: asyncio.Task | None = None
        self.monitor: asyncio.Task | None = None
        self.probe_task: asyncio.Task | None = None
        self.closed = False
        self.close_code = 1000
        self.close_reason = ""
        # delivery mode
        self.blocked = False
        self.lagging = False
        self.lag_until = 0.0
        self.batched = False
        self.batch_since: float | None = None
        self.memory = LagMemory()
        # probes
        self.probe_id = 0
        self.probe_sent: float | None = None
        self.probe_lagged = False
        self.probe_timers: list[asyncio.TimerHandle] = []
        self.sent_since_probe = False
        self.last_probe_at = self.loop.time()
        self.rtt_samples: deque[float] = deque(maxlen=20)
        # Resync is allowed only once a pong confirms the client has read its
        # initial HISTORY, so a slow HISTORY transfer cannot cause a loop.
        self.history_written = False
        self.probe_covers_history = False
        self.synced = False
        self.congested_ticks = 0
        self.congestion_lag = False   # lagging because of a standing queue
        # consumption-rate evidence for a reader resync
        self.captions_sent = 0
        self.first_caption_at = math.inf
        transport = getattr(ws, "transport", None)
        get_extra = getattr(transport, "get_extra_info", None)
        self.sock = get_extra("socket") if callable(get_extra) else None
        peer = getattr(ws, "remote_address", None)
        self.address = peer[0] if isinstance(peer, (tuple, list)) and peer else None
        self._restore_memory()

    # -- queue ---------------------------------------------------------------

    def backlogged(self) -> bool:
        return self.blocked or self.lagging or len(self.pending) >= COALESCE_BACKLOG

    def enqueue(self, item: Outgoing) -> bool:
        """Queue one message; False means the queue cannot stay bounded."""
        if self.closed:
            return True
        if item.segment is not None:
            if self.batched:
                # Finals reach a batched client through its next HISTORY.
                if item.final and self.batch_since is None:
                    self.batch_since = self.loop.time()
                return True
            if item.final:
                self.held.pop(item.segment, None)
                if self.backlogged():
                    self._drop_provisionals(item.segment)
            elif self.lagging:
                # Revision state guarantees this is newer than any held one.
                self.held[item.segment] = (item, self.loop.time())
                if len(self.held) > QUEUE_MAX_ITEMS:
                    self.held.pop(min(self.held))
                return True
            elif self.backlogged():
                self._drop_provisionals(item.segment)
        self.pending.append(item)
        if len(self.pending) > QUEUE_MAX_ITEMS:
            self._compact()
            if len(self.pending) > QUEUE_MAX_ITEMS:
                return False
        if not self.blocked:
            self.wake.set()
        return True

    def _drop_provisionals(self, segment: int) -> None:
        if any(x.segment == segment and not x.final for x in self.pending):
            self.pending = deque(x for x in self.pending
                                 if x.segment != segment or x.final)

    def _compact(self) -> None:
        """Keep finals, control messages and the newest provisional per open
        segment; drop provisionals superseded in the queue."""
        finals = {x.segment for x in self.pending if x.segment is not None and x.final}
        newest: dict[int, int] = {}
        for index, x in enumerate(self.pending):
            if x.provisional:
                newest[x.segment] = index
        self.pending = deque(
            x for index, x in enumerate(self.pending)
            if not x.provisional
            or (x.segment not in finals and newest[x.segment] == index))

    def oldest_final_wait(self, now: float) -> float:
        for x in self.pending:
            if x.final:
                return now - x.queued_at
        return 0.0

    async def send_messages(self) -> None:
        """The only task that writes application messages to this socket."""
        while True:
            await self.wake.wait()
            self.wake.clear()
            while self.pending and not self.blocked:
                item = self.pending.popleft()
                timer = self.loop.call_later(
                    SEND_TIMEOUT_SECONDS, self.evict, CLOSE_TRY_AGAIN,
                    "send timed out; reconnect for history")
                try:
                    await self.ws.send(item.raw)
                finally:
                    timer.cancel()
                self.sent_since_probe = True
                if item.segment is None:
                    self.history_written = True
                else:
                    self.captions_sent += 1
                    if self.first_caption_at == math.inf:
                        self.first_caption_at = self.loop.time()

    # -- delivery mode -------------------------------------------------------

    def _restore_memory(self) -> None:
        if self.key is None:
            return
        memory = LAG_MEMORY.get(self.key)
        now = self.loop.time()
        if memory is None or now - memory.updated > CLIENT_MEMORY_SECONDS:
            LAG_MEMORY.pop(self.key, None)
            return
        self.memory = memory
        if memory.reader_sent > 0 and memory.reader_first < now:
            # First reconnect after a slow-reader resync: the client has just
            # drained everything written on that connection, so this is its
            # real consumption rate.
            elapsed = max(1.0, now - memory.reader_first - RECONNECT_ALLOWANCE_SECONDS)
            rate = memory.reader_sent / elapsed
            memory.reader_sent = 0              # use each measurement once
            update_rate, final_rate = LECTURE.rates(now)
            if update_rate > 0 and rate >= FULL_RATE_FRACTION * update_rate:
                pass                            # it kept up: not a slow reader
            elif final_rate > 0 and rate < BATCH_RATE_MARGIN * final_rate:
                # Too slow even for finals: a full-rate re-test would strand
                # finals in its receive buffer, so batching keeps the long hold.
                memory.batched_until = now + READER_HOLD_MAX_SECONDS
            else:
                memory.reader_until = now + memory.reader_hold
        if 0 < memory.batched_until <= now and memory.last_exit < memory.batched_until:
            memory.last_exit = memory.batched_until     # batched period has ended
        if now < memory.batched_until:
            self.batched = True
        elif now < memory.reader_until:
            self.lagging = True
            self.lag_until = memory.reader_until
        elif memory.lag_until > now:
            self.lagging = True
            self.lag_until = now + self.network_hold()

    def _remember(self) -> None:
        if self.key is None:
            return
        now = self.loop.time()
        memory = self.memory
        memory.lag_until = self.lag_until if self.lagging else 0.0
        memory.updated = now
        LAG_MEMORY[self.key] = memory
        LAG_MEMORY.move_to_end(self.key)
        while len(LAG_MEMORY) > CLIENT_MEMORY_MAX:
            LAG_MEMORY.popitem(last=False)

    def enter_lag(self) -> None:
        """Network bottleneck: finals only, for an adaptive hold."""
        now = self.loop.time()
        if not self.lagging:
            memory = self.memory
            if now - memory.last_exit < HOLD_RESET_SECONDS:
                memory.net_hold = min(memory.net_hold * 2, NETWORK_HOLD_MAX_SECONDS)
            else:
                memory.net_hold = NETWORK_HOLD_BASE_SECONDS
            self.lagging = True
            # From now on only finals are written; park queued provisionals.
            for x in self.pending:
                if x.provisional:
                    self.held[x.segment] = (x, now)
            self.pending = deque(x for x in self.pending if not x.provisional)
        self.lag_until = max(self.lag_until, now + self.network_hold())
        self._remember()

    def network_hold(self) -> float:
        """The adaptive hold, randomised so that clients sharing one
        bottleneck do not all return to full rate at the same moment."""
        return self.memory.net_hold * random.uniform(1 - HOLD_JITTER, 1 + HOLD_JITTER)

    def update_lag(self, now: float) -> None:
        """Leave lagging mode after its hold, or release long-held revisions."""
        if not self.lagging or self.blocked:
            return
        if now >= self.lag_until:
            self.lagging = False
            self.congestion_lag = False
            self.memory.last_exit = now
            held = [item for _, (item, _) in sorted(self.held.items())]
            self.held.clear()
            for item in held:
                self.enqueue(item)
            self._remember()
            return
        for segment, (item, since) in list(self.held.items()):
            if now - since >= HELD_REFRESH_SECONDS:
                del self.held[segment]
                self.pending.append(item)
                self.wake.set()

    # -- probes --------------------------------------------------------------

    def network_estimate(self) -> tuple[float, float, float, float]:
        """(srtt, rttvar, rto, min_rtt) seconds from TCP_INFO, else measured
        probe RTTs."""
        info = tcp_info(self.sock)
        if info is not None and info[2] > 0:
            return info[0], info[1], info[2], info[5]
        base = min(self.rtt_samples) if self.rtt_samples else 0.05
        return base, base / 2, max(0.2, 2 * base), base

    def congested(self) -> bool:
        """A standing queue on the path: the smoothed RTT is well above the
        connection's minimum RTT. Delay and jitter move both together, and
        retransmissions are not RTT samples, so neither looks like a queue."""
        info = tcp_info(self.sock)
        if info is None:
            return False
        srtt = info[0]
        base = path_baseline(self.address, info[5], self.loop.time())
        return srtt - base > max(QUEUE_DELAY_SECONDS, 0.5 * srtt)

    def queue_cleared(self) -> bool:
        """Hysteresis for congestion: the queue must drain well below the
        entry level before the finals-only hold may start counting down, so
        clients do not settle at a standing queue just under the threshold."""
        info = tcp_info(self.sock)
        if info is None:
            return True
        base = path_baseline(self.address, info[5], self.loop.time())
        return info[0] - base < QUEUE_CLEAR_SECONDS

    def reader_limited(self) -> bool:
        """True if the client's kernel holds everything we wrote (all bytes
        acknowledged, nothing unsent) although the pong has not come back."""
        info = tcp_info(self.sock)
        if info is None:
            return False
        transport = getattr(self.ws, "transport", None)
        buffered = getattr(transport, "get_write_buffer_size", lambda: 0)()
        return info[3] == 0 and info[4] == 0 and buffered == 0

    def start_probe(self) -> None:
        """Record the probe and arm its deadlines before the ping is written,
        so a ping stuck behind a full transport buffer still counts as late."""
        self.probe_id += 1
        pid = self.probe_id
        now = self.loop.time()
        self.probe_sent = now
        self.last_probe_at = now
        self.probe_lagged = False
        self.sent_since_probe = False
        self.probe_covers_history = self.history_written
        srtt, rttvar, rto, min_rtt = self.network_estimate()
        # Judged against the smoothed RTT: pausing writes during a loss stall
        # would only add the pong's return trip. Queueing delay is handled by
        # congested() instead.
        self.probe_timers = [
            self.loop.call_later(srtt + 4 * rttvar + BLOCK_MARGIN_SECONDS,
                                 self._probe_block, pid),
            self.loop.call_later(srtt + rto + LAG_MARGIN_SECONDS, self._probe_lag, pid),
        ]
        self.probe_task = self.loop.create_task(self._ping(pid))

    async def _ping(self, pid: int) -> None:
        try:
            waiter = await self.ws.ping()
        except Exception:      # connection closing; handle_client cleans up
            return
        if pid != self.probe_id or self.probe_sent is None:
            return
        waiter.add_done_callback(functools.partial(self._pong, pid))

    def _clear_probe(self) -> None:
        for timer in self.probe_timers:
            timer.cancel()
        self.probe_timers = []
        self.probe_sent = None

    def _probe_block(self, pid: int) -> None:
        if pid == self.probe_id and self.probe_sent is not None and not self.blocked:
            self.blocked = True
            self._compact()

    def _probe_lag(self, pid: int) -> None:
        if pid != self.probe_id or self.probe_sent is None or self.closed:
            return
        self.probe_lagged = True
        if self.reader_limited():
            self.reader_resync()
        else:
            self.enter_lag()

    def _pong(self, pid: int, waiter: asyncio.Future) -> None:
        if pid != self.probe_id or self.probe_sent is None:
            return
        now = self.loop.time()
        rtt = now - self.probe_sent
        lagged = self.probe_lagged
        self._clear_probe()
        if waiter.cancelled() or waiter.exception() is not None:
            return
        if self.probe_covers_history:
            self.synced = True
        if not lagged:
            self.rtt_samples.append(rtt)
        if self.lagging and lagged:
            # The hold counts from when the lagging client caught up.
            self.lag_until = max(self.lag_until, now + self.network_hold())
            self._remember()
        if self.blocked:
            self.blocked = False
            self.wake.set()

    async def monitor_flow(self) -> None:
        while not self.closed:
            await asyncio.sleep(MONITOR_TICK_SECONDS)
            now = self.loop.time()
            if not self.batched and self.congested():
                self.congested_ticks += 1
                if self.congested_ticks >= CONGESTED_TICKS:
                    self.enter_lag()      # also extends the hold while congested
                    self.congestion_lag = True
            else:
                self.congested_ticks = 0
            if self.lagging and self.congestion_lag and not self.queue_cleared():
                self.lag_until = max(self.lag_until, now + self.network_hold())
            self.update_lag(now)
            if self.synced:
                if self.batched:
                    if self.batch_since is not None and now - self.batch_since >= BATCH_SECONDS:
                        self.evict(CLOSE_TRY_AGAIN, "batched: reconnect for missed finals")
                        return
                elif self.oldest_final_wait(now) > RESYNC_AGE_SECONDS:
                    self.enter_lag()          # restart in finals-only mode
                    self.evict(CLOSE_TRY_AGAIN, "finals delayed; reconnect for missed finals")
                    return
            if self.probe_sent is not None:
                if now - self.probe_sent > PROBE_TIMEOUT_SECONDS:
                    self.evict(CLOSE_TRY_AGAIN, "client stopped responding")
                    return
            elif ((self.sent_since_probe and now - self.last_probe_at >= PROBE_INTERVAL_SECONDS)
                  or now - self.last_probe_at >= IDLE_PROBE_SECONDS):
                if callable(getattr(self.ws, "ping", None)):
                    self.start_probe()

    # -- closing -------------------------------------------------------------

    def reader_resync(self) -> None:
        """The reader is slower than what we send: resync it now and keep the
        evidence needed to measure how fast it really consumes."""
        if self.closed:
            return
        memory = self.memory
        now = self.loop.time()
        if not self.batched and self.captions_sent > 0:
            # Still slow soon after its last hold ended: hold it longer next
            # time; otherwise this is a fresh episode and starts short.
            if now - memory.last_exit < HOLD_RESET_SECONDS:
                memory.reader_hold = min(memory.reader_hold * 2, READER_HOLD_MAX_SECONDS)
            else:
                memory.reader_hold = READER_HOLD_BASE_SECONDS
            memory.reader_at = now
            memory.reader_sent = self.captions_sent
            memory.reader_first = self.first_caption_at
        self.evict(CLOSE_TRY_AGAIN, "slow reader; reconnect for missed finals")

    def evict(self, code: int, reason: str) -> None:
        if self.closed:
            return
        self.closed = True
        CLIENTS.pop(self.ws, None)
        self.close_code = code
        self.close_reason = reason
        self._remember()
        if self.sender is not None:
            self.sender.cancel()

    def release(self) -> None:
        self._clear_probe()
        self.pending.clear()
        self.held.clear()


CLIENTS: dict[Any, ClientState] = {}


async def broadcast(item: Outgoing, session: str) -> None:
    """Offer one message to every client of the session without awaiting I/O."""
    for client in list(CLIENTS.values()):
        if client.session != session:
            continue
        if not client.enqueue(item):
            client.enter_lag()
            client.evict(CLOSE_TRY_AGAIN, "client queue full; reconnect for missed finals")
    # A buffered source can deliver many messages without yielding. Give the
    # sender tasks (and other connections) a turn even in that case.
    await asyncio.sleep(0)


def start_lecture(session: str) -> Lecture:
    global LECTURE
    LECTURE = Lecture(LECTURE.number + 1, session)
    print(f"new lecture {LECTURE.number}: session={session}", flush=True)
    return LECTURE


async def handle_source(ws, hello: dict[str, Any]) -> None:
    session = hello.get("session")
    if not isinstance(session, str) or not session:
        await ws.close(code=1008, reason="Expected a nonempty source session")
        return
    print(f"source connected: session={session}", flush=True)

    loop = asyncio.get_running_loop()
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if not isinstance(msg, dict):
                continue
            msg_type = msg.get("type")
            if msg_type == "CAPTION_UPDATE":
                lecture = LECTURE
                if not lecture.revisions.accept(msg, session):
                    # A finished lecture followed by a source starting again
                    # at segment 1 is a new lecture, not a replay.
                    if not (lecture.ended and msg.get("segment") == 1
                            and valid_update(msg, session)):
                        continue
                    lecture = start_lecture(session)
                    reset = json.dumps({"type": "SESSION_RESET", "session": session,
                                        "lecture": lecture.number})
                    await broadcast(Outgoing(reset, queued_at=loop.time()), session)
                    if not lecture.revisions.accept(msg, session):
                        continue
                lecture.session = session
                lecture.ended = False
                now = loop.time()
                segment, revision, final = msg["segment"], msg["revision"], msg["final"]
                lecture.note(final, now)
                payload = json.dumps(msg, separators=(",", ":"))
                # Forward the source's text frame unchanged (extra fields kept);
                # a binary frame is re-encoded so every client gets text.
                text = raw if isinstance(raw, str) else payload
                if final:
                    # Record before any client is offered the update, with no
                    # await in between, so HISTORY and live never overlap.
                    lecture.history.record(segment, revision, payload)
                await broadcast(Outgoing(text, segment, revision, final, now), session)
            elif msg_type == "SOURCE_DONE":
                # Test-control message: not forwarded to clients.
                if LECTURE.session in (None, session):
                    LECTURE.ended = True
                # Batched clients collect their last finals now rather than
                # after the next batch interval.
                for client in list(CLIENTS.values()):
                    if (client.session == session and client.batched
                            and client.synced and client.batch_since is not None):
                        client.evict(CLOSE_TRY_AGAIN, "lecture ended; reconnect for last finals")
                print("source finished", flush=True)
            # Forward compatibility: ignore other message types.
    finally:
        print("source disconnected", flush=True)


def parse_cursor(hello: dict[str, Any]) -> tuple[int, int]:
    last_segment = hello.get("last_segment", 0)
    last_revision = hello.get("last_revision", 0)
    if (type(last_segment) is not int or last_segment < 0
            or type(last_revision) is not int or last_revision < 0):
        return 0, 0
    return last_segment, last_revision


async def handle_client(ws, hello: dict[str, Any]) -> None:
    client_id = hello.get("client_id")
    lecture = LECTURE

    # Be liberal in what we accept: a malformed recovery field falls back to
    # its "no state" default (recent context) instead of closing.
    session = hello.get("session")
    if not isinstance(session, str) or not session:
        session = lecture.session or "L1"
    last_segment, last_revision = parse_cursor(hello)
    known_lecture = hello.get("lecture")
    if type(known_lecture) is int and known_lecture != lecture.number:
        last_segment = last_revision = 0      # cursor belongs to an older lecture
    seconds = hello.get("history_seconds", 30)
    if type(seconds) not in (int, float) or not math.isfinite(seconds):
        seconds = 30
    seconds = max(0.0, min(float(seconds), HISTORY_WINDOW_SECONDS))
    if seconds > 0 and (last_segment, last_revision) > (0, 0):
        # Resume: the cursor says exactly what the client already holds, so
        # return every retained final it missed, not only the context window.
        seconds = HISTORY_RETAIN_SECONDS
    items: list[str] = []
    if lecture.session in (None, session):
        items = lecture.history.snapshot(last_segment, last_revision, seconds)

    loop = asyncio.get_running_loop()
    client = ClientState(ws, session, client_key(session, client_id))
    for message in history_messages(items, session, lecture.number):
        client.enqueue(Outgoing(message, queued_at=loop.time()))
    # No await since the snapshot: the client is registered before the next
    # live update can be broadcast, so nothing is missed or duplicated.
    CLIENTS[ws] = client
    client.sender = asyncio.create_task(client.send_messages())
    client.monitor = asyncio.create_task(client.monitor_flow())
    print(f"client connected: id={client_id}; clients={len(CLIENTS)}", flush=True)

    async def receive_messages() -> None:
        # Clients send nothing after hello; keep reading to notice the close.
        async for _ in ws:
            pass

    receiver = asyncio.create_task(receive_messages())
    try:
        await asyncio.wait((client.sender, receiver), return_when=asyncio.FIRST_COMPLETED)
    finally:
        CLIENTS.pop(ws, None)
        client.closed = True
        client._remember()
        tasks = [client.sender, receiver, client.monitor]
        if client.probe_task is not None:
            tasks.append(client.probe_task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        client.release()
        try:
            await ws.close(code=client.close_code, reason=client.close_reason)
        except Exception:
            pass
        print(f"client disconnected: id={client_id}; clients={len(CLIENTS)}", flush=True)


async def handler(ws) -> None:
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        hello = json.loads(raw)
    except (asyncio.TimeoutError, ValueError, TypeError):
        try:
            await ws.close(code=1008, reason="Expected a valid hello message")
        except Exception:
            pass
        return
    except websockets.ConnectionClosed:
        return

    msg_type = hello.get("type") if isinstance(hello, dict) else None
    if msg_type == "SOURCE_HELLO":
        await handle_source(ws, hello)
    elif msg_type == "CLIENT_HELLO":
        await handle_client(ws, hello)
    else:
        await ws.close(code=1008, reason="Expected SOURCE_HELLO or CLIENT_HELLO")


async def collect_garbage() -> None:
    """Closed connections leave reference cycles (connection <-> protocol <->
    tasks). With many reconnects, e.g. resynced slow readers, the automatic
    collector can lag far behind and memory climbs. A full collection every
    few seconds takes ~10 ms and keeps server memory flat."""
    while True:
        await asyncio.sleep(GC_INTERVAL_SECONDS)
        gc.collect()


async def main_async(port: int) -> None:
    collector = asyncio.create_task(collect_garbage())
    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        max_size=2**20,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
    ):
        print(f"LiveRecall listening on ws://0.0.0.0:{port}", flush=True)
        await asyncio.Future()
    collector.cancel()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    try:
        asyncio.run(main_async(args.port))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
