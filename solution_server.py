#!/usr/bin/env python3
"""
CS3103 Assignment 4 - LiveRecall naive working baseline.

This starter is intentionally useful but incomplete. It provides the required
CLI and a basic WebSocket broadcaster so that caption_source.py -> server ->
browser/load clients works on day one.

It DOES NOT implement the graded networking mechanisms:
  - revision-state management or deduplication
  - recent-history recovery or late-join recovery
  - bounded per-client application queues
  - slow-client isolation
  - semantic coalescing / backpressure
  - constrained-bandwidth overload handling

Required evaluator command:
    python solution_server.py --port 8765

Teams should replace or substantially extend the implementation while keeping
this command-line interface and the protocol in the assignment specification.
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import collections
import json
import time
from typing import Any

import websockets

# Currently connected student clients: websocket -> Client (its send queue).
CLIENTS: dict[Any, "Client"] = {}

HISTORY_KEEP_SECONDS = 90      # must be >= 60 (the maximum a client may request)
HISTORY_MAX_ITEMS = 20000       # hard cap, protects against very fast sources
MAX_REQUEST_SECONDS = 60       # clamp for CLIENT_HELLO.history_seconds
HISTORY_CACHE_MAX = 256        # max cached ready-to-send HISTORY payloads

# Three parallel lists, one entry per stored final, oldest first.
_h_times: list[float] = []            # server receive time (monotonic), ascending
_h_keys: list[tuple[int, int]] = []   # (segment, revision) of the final
_h_raw: list[str] = []                # the final's JSON text, exactly as received
# Segments currently held, so a repeated final is not stored twice.
_h_segments: set[int] = set()
# Ready-to-send HISTORY messages, keyed by (window start index, client state).
# Many clients reconnecting together usually ask for the same thing, so the
# payload is built once. Cleared whenever the stored history changes.
_h_cache: dict[tuple[int, tuple[int, int]], str] = {}

def prune_history(now: float) -> None:
    """Drop entries that are too old or exceed the count cap."""
    # bisect_left finds, by binary search, how many entries are older than the
    # age limit (times are ascending) without looking at every entry.
    drop = bisect.bisect_left(_h_times, now - HISTORY_KEEP_SECONDS)
    drop = max(drop, len(_h_times) - HISTORY_MAX_ITEMS)
    if drop <= 0:
        return
    for segment, _ in _h_keys[:drop]:
        _h_segments.discard(segment)
    del _h_times[:drop]
    del _h_keys[:drop]
    del _h_raw[:drop]
    _h_cache.clear()

def store_final(msg: dict[str, Any], raw: str) -> None:
    """Remember a final caption (once per segment) for recovery and late join."""
    try:
        segment = int(msg["segment"])
        revision = int(msg["revision"])
    except (KeyError, TypeError, ValueError):
        return
    now = time.monotonic()
    if segment not in _h_segments:
        _h_times.append(now)
        _h_keys.append((segment, revision))
        _h_raw.append(raw)
        _h_segments.add(segment)
        _h_cache.clear()
    prune_history(now)

def build_history(hello: dict[str, Any]) -> str:
    """Return the HISTORY message (as JSON text) for a CLIENT_HELLO.
 
    Contains finals from the last `history_seconds` that the client does not
    hold yet. The client reports its latest FINAL state as
    (last_segment, last_revision), so only finals strictly newer than that are
    sent, oldest to newest.
    """
    try:
        seconds = int(hello.get("history_seconds", 30))
    except (TypeError, ValueError):
        seconds = 30
        seconds = max(0, min(seconds, MAX_REQUEST_SECONDS))
    try:
        have = (int(hello.get("last_segment", 0)), int(hello.get("last_revision", 0)))
    except (TypeError, ValueError):
        have = (0, 0)
 
    now = time.monotonic()
    prune_history(now)
    # Binary search for the first entry inside the requested time window.~
    # Cost is O(log n) however large the history is; only the entries that are actually returned are looked at afterwards.
    start = bisect.bisect_right(_h_times, now - seconds)
 
    cache_key = (start, have)
    payload = _h_cache.get(cache_key)
    if payload is None:
        picked = [i for i in range(start, len(_h_keys)) if _h_keys[i] > have]
        picked.sort(key=lambda i: _h_keys[i])
        # The finals are already JSON text, so join them instead of re-encoding.
        payload = '{"type":"HISTORY","items":[' + ",".join(_h_raw[i] for i in picked) + "]}"
        if len(_h_cache) < HISTORY_CACHE_MAX:
            _h_cache[cache_key] = payload
    return payload

# ---------------------------------------------------------------------------
# Per-client bounded send queues (requirements 8, 9, 10, 11).
#
# The source loop never waits for a client. For every caption it only puts the
# message into each client's own queue (a quick, non-blocking operation). One
# sender task per client takes messages out of that queue and calls ws.send,
# so a slow client only delays itself.
#
# Each queue is bounded and coalesces when the client lags:
#   - a newer provisional revision of a queued segment REPLACES the queued one
#     (same position), so only the newest revision of a segment is ever queued;
#   - a final REMOVES the queued provisionals of its segment (obsolete);
#   - finals are never dropped. If a client falls so far behind that even the
#     finals exceed MAX_BACKLOG, it is disconnected; it reconnects by itself
#     and recovers the finals it missed from HISTORY.
# While the client keeps up nothing is coalesced, so normal clients get every
# revision. When a lagging client catches up, full updates resume.
#
# How lag is detected: a slow reader's backlog hides in kernel/socket buffers,
# so ws.send() still returns at once and our queue stays short. Instead the
# sender pings the client every LAG_PROBE_INTERVAL; a pong only returns after
# the client has read everything sent before the ping, so a slow pong means the
# client is lagging. A lagging client then gets finals first and at most one
# provisional per PROVISIONAL_GAP, until it has been fine for a while. "Slow"
# is measured against that client's own best round trip, so a slow network
# (high but steady delay) is not mistaken for a slow reader.
# ---------------------------------------------------------------------------

COALESCE_BACKLOG = 4     # also coalesce once this many messages are queued
MAX_BACKLOG = 500        # hard bound per client (~6 min of finals at 1.3/s)
LAG_PROBE_INTERVAL = 0.25 # seconds between lag probes (WebSocket pings)
LAG_ON_SECONDS = 1.0     # a pong this much slower than the client's best round trip = lagging
SLOW_HOLD_SECONDS = 10.0 # stay in lagging mode this long after the last lag sign
MAX_HOLD_SECONDS = 60.0  # a client that keeps lagging is held longer each time, up to this
REFLAG_WINDOW = 30.0     # lagging again within this long after a hold counts as "keeps lagging"
PROVISIONAL_GAP = 30.0   # lagging mode: at most one provisional per 30 s (a final follows within ~1 s)
_closing: set = set()    # keeps references to close tasks until they finish
 
# Queue entry layout: [raw_text, segment, revision, is_final, alive]
_RAW, _SEG, _REV, _FIN, _ALIVE = range(5)

class Client:
    """The send queue of one connected client."""
 
    __slots__ = ("ws", "queue", "prov", "live", "wake",
                "probe_sent", "last_probe", "slow_until", "last_prov", "held",
                "min_rtt", "hold")
 
    def __init__(self, ws, first_message: str) -> None:
        self.ws = ws
        self.queue: collections.deque = collections.deque()
        self.prov: dict[int, list] = {}      # segment -> queued provisional entry
        self.live = 0                        # number of alive entries queued
        self.wake = asyncio.Event()
        self.probe_sent = None               # time of the outstanding ping, if any
        self.last_probe = 0.0
        self.slow_until = 0.0                # lagging mode lasts until this time
        self.hold = SLOW_HOLD_SECONDS        # current hold length (grows if it keeps lagging)
        self.min_rtt = None                  # best ping round trip seen: the network's own delay
        self.last_prov = 0.0                 # when a provisional was last sent
        self.held = False                    # provisionals are waiting for PROVISIONAL_GAP
        # HISTORY is always the first thing this client receives.
        self._append([first_message, None, None, False, True])
 
    def _append(self, entry: list) -> None:
        self.queue.append(entry)
        self.live += 1
        self.wake.set()
 
    def lagging(self, now: float) -> bool:
        return self.live >= COALESCE_BACKLOG or now < self.slow_until
 
    def enqueue(self, raw: str, segment, revision, final: bool, now: float | None = None) -> bool:
        """Queue one caption. Never blocks. Returns False if the client is too far behind."""
        lagging = self.lagging(time.monotonic() if now is None else now)
        if segment is None:                          # unparseable: forward as is
            self._append([raw, None, None, final, True])
        elif final:
            old = self.prov.pop(segment, None)
            if old is not None and lagging:
                old[_ALIVE] = False                  # the final supersedes it
                self.live -= 1
            self._append([raw, segment, revision, True, True])
        else:
            old = self.prov.get(segment)
            if old is not None and revision <= old[_REV]:
                return True                          # stale or repeated revision
            if old is not None and lagging:
                old[_RAW], old[_REV] = raw, revision # replace in place
                return True
            entry = [raw, segment, revision, False, True]
            self.prov[segment] = entry
            self._append(entry)
        return self.live <= MAX_BACKLOG
 
    def pop(self, now: float | None = None):
        """Next entry to send, or None if nothing may be sent right now.
 
        In lagging mode provisionals are held back (moved behind the finals)
        until PROVISIONAL_GAP has passed since the last one was sent.
        """
        now = time.monotonic() if now is None else now
        slow = now < self.slow_until
        self.held = False
        for _ in range(len(self.queue)):
            entry = self.queue.popleft()
            if not entry[_ALIVE]:
                continue
            provisional = not entry[_FIN] and entry[_SEG] is not None
            if provisional and slow and now - self.last_prov < PROVISIONAL_GAP:
                self.queue.append(entry)            # not yet: look at later entries
                self.held = True
                continue
            self.live -= 1
            if provisional:
                self.last_prov = now
                if self.prov.get(entry[_SEG]) is entry:
                    del self.prov[entry[_SEG]]
            return entry
        return None

    def flag_lag(self, now):
        if now >= self.slow_until:
            again = 0 < self.slow_until and now < self.slow_until + REFLAG_WINDOW
            self.hold = min(self.hold * 2, MAX_HOLD_SECONDS) if again else SLOW_HOLD_SECONDS
        self.slow_until = max(self.slow_until, now + self.hold)

    def excess(self, rtt: float) -> float:
        """How much slower this round trip is than the client's best one so far."""
        return rtt - (self.min_rtt if self.min_rtt is not None else rtt)
 
    async def probe(self, now: float) -> None:
        """Ping the client now and then; a pong far slower than usual marks it as lagging."""
        if self.probe_sent is not None:
            if self.excess(now - self.probe_sent) > LAG_ON_SECONDS:   # pong still missing
                self.flag_lag(now)
        elif now - self.last_probe >= LAG_PROBE_INTERVAL:
            self.probe_sent = self.last_probe = now
            waiter = await self.ws.ping()
            waiter.add_done_callback(self._pong)

    def _pong(self, future) -> None:
        sent, self.probe_sent = self.probe_sent, None
        if sent is None or future.cancelled() or future.exception() is not None:
            return
        now = time.monotonic()
        rtt = now - sent
        if self.min_rtt is None or rtt < self.min_rtt:
            self.min_rtt = rtt
        if self.excess(rtt) > LAG_ON_SECONDS:
            self.flag_lag(now)

async def client_sender(client: Client) -> None:
    """Send this client's queued messages, one at a time, as fast as it reads."""
    try:
        while True:
            now = time.monotonic()
            await client.probe(now)
            entry = client.pop(now)
            if entry is None:
                client.wake.clear()
                # Held provisionals need a timed wake-up; otherwise wait for news.
                timeout = PROVISIONAL_GAP if client.held else None
                try:
                    await asyncio.wait_for(client.wake.wait(), timeout)
                except asyncio.TimeoutError:
                    pass
                continue
            await client.ws.send(entry[_RAW])
    except websockets.ConnectionClosed:
        pass
 
 
def evict(client: Client) -> None:
    """Disconnect a client that is too far behind; it will reconnect and resume."""
    CLIENTS.pop(client.ws, None)
    task = asyncio.get_running_loop().create_task(
        client.ws.close(code=1013, reason="too slow; reconnect to resume"))
    _closing.add(task)
    task.add_done_callback(_closing.discard)
 
 
def broadcast(raw: str, msg: dict[str, Any]) -> None:
    """Hand one caption to every client's queue. Never waits, never blocks."""
    final = msg.get("final") is True
    try:
        segment, revision = int(msg["segment"]), int(msg["revision"])
    except (KeyError, TypeError, ValueError):
        segment = revision = None
    now = time.monotonic()
    too_slow = [c for c in CLIENTS.values() if not c.enqueue(raw, segment, revision, final, now)]
    for client in too_slow:
        evict(client)

#
# async def broadcast_naively(raw_message: str) -> None:
#     """Send one message directly to every connected client.

#     This is intentionally a naive baseline. The source handler waits for all
#     sends to complete. Under pressure, this design may accumulate delay and is
#     not the target architecture for the assignment.
#     """
#     if not CLIENTS:
#         return

#     snapshot = list(CLIENTS)
#     results = await asyncio.gather(
#         *(client.send(raw_message) for client in snapshot),
#         return_exceptions=True,
#     )
#     for client, result in zip(snapshot, results):
#         if isinstance(result, Exception):
#             CLIENTS.discard(client)
#

async def handle_source(ws, hello: dict[str, Any]) -> None:
    session = hello.get("session")
    print(f"source connected: session={session}", flush=True)

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type")
            if msg_type == "CAPTION_UPDATE":
                # Store finals BEFORE broadcasting. There is no await between
                # store_final and broadcast, so a joining client either gets
                # this final from HISTORY or from its queue, never both.
                text = raw if isinstance(raw, str) else raw.decode("utf-8")
                if msg.get("final") is True:
                    store_final(msg, text)
                broadcast(text, msg)
            elif msg_type == "SOURCE_DONE":
                # Test-control message. The specification does not require it
                # to be forwarded to clients.
                print("source finished", flush=True)
            else:
                # Forward compatibility: ignore fields/messages this baseline
                # doesn't understand rather than crashing.
                continue
    finally:
        print("source disconnected", flush=True)


async def handle_client(ws, hello: dict[str, Any]) -> None:
    client_id = hello.get("client_id")

    client = Client(ws, build_history(hello))
    CLIENTS[ws] = client
    sender = asyncio.create_task(client_sender(client))
    print(f"client connected: id={client_id}; clients={len(CLIENTS)}", flush=True)

    try:
        # Student clients normally don't send further messages after hello.
        # Keep the WebSocket open until the client disconnects.
        async for _ in ws:
            pass
    finally:
        CLIENTS.pop(ws, None)
        sender.cancel()
        print(f"client disconnected: id={client_id}; clients={len(CLIENTS)}", flush=True)


async def handler(ws) -> None:
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        hello = json.loads(raw)
    except (asyncio.TimeoutError, json.JSONDecodeError):
        await ws.close(code=1008, reason="Expected a valid hello message")
        return

    msg_type = hello.get("type")
    if msg_type == "SOURCE_HELLO":
        await handle_source(ws, hello)
    elif msg_type == "CLIENT_HELLO":
        await handle_client(ws, hello)
    else:
        await ws.close(code=1008, reason="Expected SOURCE_HELLO or CLIENT_HELLO")


async def main_async(port: int) -> None:
    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        max_size=2**20,
        ping_interval=20,
        ping_timeout=20,
    ):
        print(f"LiveRecall baseline listening on ws://0.0.0.0:{port}", flush=True)
        await asyncio.Future()


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
