#!/usr/bin/env python3
"""
CS3103 Assignment 4 - LiveRecall server.

Revision validation precedes finalized-history retention and per-client queues.
Each client has one ordered sender; semantic admission coalesces obsolete
provisionals only under backpressure. Evicted clients recover recent finals.

Required evaluator command:
    python solution_server.py --port 8765

"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, NamedTuple

import websockets


class _Caption(NamedTuple):
    session: str
    segment: int
    revision: int
    final: bool


def caption_metadata(raw: str | bytes) -> _Caption | None:
    """Recognize captions conservatively; treat everything else as protected."""
    try:
        msg = json.loads(raw)
    except (ValueError, TypeError, UnicodeDecodeError):
        return None
    if not isinstance(msg, dict) or msg.get("type") != "CAPTION_UPDATE":
        return None
    session, segment = msg.get("session"), msg.get("segment")
    revision, final = msg.get("revision"), msg.get("final")
    if (not isinstance(session, str) or not session
            or type(segment) is not int or segment < 1
            or type(revision) is not int or revision < 1
            or type(final) is not bool):
        return None
    return _Caption(session, segment, revision, final)


def enqueue_pending(
    pending: deque[str | bytes], message: str | bytes, max_items: int,
) -> bool:
    """Bound pending sends, dropping only obsolete unsent provisionals."""
    if max_items < 1:
        raise ValueError("max_items must be positive")
    if len(pending) > max_items:
        raise ValueError("pending exceeds max_items")
    if len(pending) < max_items:
        pending.append(message)
        return True

    entries = [(raw, caption_metadata(raw)) for raw in (*pending, message)]
    finals = {caption[:2] for _, caption in entries
              if caption is not None and caption.final}
    latest: dict[tuple[str, int], tuple[int, int]] = {}
    for index, (_, caption) in enumerate(entries):
        if caption is not None and not caption.final:
            key = caption[:2]
            latest[key] = max(latest.get(key, (0, -1)),
                              (caption.revision, index))
    entries = [(raw, caption) for index, (raw, caption) in enumerate(entries)
               if caption is None or caption.final
               or (caption[:2] not in finals
                   and latest[caption[:2]][1] == index)]
    if len(entries) > max_items:
        return False

    pending.clear()
    pending.extend(raw for raw, _ in entries)
    return True

CLIENT_QUEUE_MAX_ITEMS = 64  # pending messages, plus at most one in-flight send
CLIENT_SEND_TIMEOUT_SECONDS = 10  # also bounds a stalled HISTORY or final send
LAG_PROBE_INTERVAL_SECONDS = 0.5
# Lag is a pong slower than this client's best round trip by this margin, so a
# steady network delay (e.g. netem) is not mistaken for a slow reader.
LAG_THRESHOLD_SECONDS = 0.25
LAG_INITIAL_RTT_SECONDS = 1.0  # assumed best round trip until one is measured
# Favor final delivery for sustained slow readers. A brief impairment may
# therefore suppress some provisional updates until this hold expires.
# A shorter (10 s, doubling) hold was measured and rejected: each time a slow
# reader leaves the hold it rebuilds a multi-second backlog in its socket
# buffers before the next pong reveals it (T3 slow-client fresh ratio fell
# from ~36% to ~7%).
LAG_HOLD_SECONDS = 60


@dataclass
class ClientState:
    ws: Any
    pending: deque[str | bytes] = field(default_factory=deque)
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    sender: asyncio.Task | None = None
    lag_monitor: asyncio.Task | None = None
    close_code: int = 1000
    close_reason: str = ""
    slow_until: float = 0
    best_rtt: float | None = None
    held_provisionals: dict[tuple[str, int], str | bytes] = field(default_factory=dict)

    def lagging(self) -> bool:
        return asyncio.get_running_loop().time() < self.slow_until

    def lag_timeout(self) -> float:
        """How long a pong may take before this client counts as lagging."""
        best = self.best_rtt if self.best_rtt is not None else LAG_INITIAL_RTT_SECONDS
        return best + LAG_THRESHOLD_SECONDS

    def enqueue(self, message: str | bytes) -> bool:
        caption = caption_metadata(message)
        if caption is not None:
            key = (caption.session, caption.segment)
            if caption.final:
                self.held_provisionals.pop(key, None)
            elif self.lagging():
                # A slow reader's backlog can sit below the application queue
                # in TCP buffers. Hold only its newest unsent active revision.
                self.held_provisionals[key] = message
                return len(self.held_provisionals) <= CLIENT_QUEUE_MAX_ITEMS
            else:
                held = self.held_provisionals.get(key)
                held_caption = caption_metadata(held) if held is not None else None
                if held_caption is not None and held_caption.revision <= caption.revision:
                    self.held_provisionals.pop(key, None)
        if not enqueue_pending(self.pending, message, CLIENT_QUEUE_MAX_ITEMS):
            return False
        self.ready.set()
        return True

    def flush_held(self) -> None:
        if self.lagging() or not self.held_provisionals:
            return
        for message in self.held_provisionals.values():
            if not enqueue_pending(self.pending, message, CLIENT_QUEUE_MAX_ITEMS):
                self.evict("Client send queue full; reconnect for history")
                return
        self.held_provisionals.clear()
        self.ready.set()

    async def monitor_receiver_lag(self) -> None:
        """Use pongs to detect backlog hidden in WebSocket/TCP buffers."""
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(LAG_PROBE_INTERVAL_SECONDS)
            sent = loop.time()
            waiter = await self.ws.ping()
            try:
                await asyncio.wait_for(asyncio.shield(waiter), self.lag_timeout())
            except asyncio.TimeoutError:
                self.slow_until = loop.time() + LAG_HOLD_SECONDS
                # Do not accumulate outstanding probes on a stalled client.
                try:
                    await asyncio.wait_for(waiter, CLIENT_SEND_TIMEOUT_SECONDS)
                except asyncio.TimeoutError:
                    self.evict("Client did not respond to lag probe")
                    return
            rtt = loop.time() - sent
            self.best_rtt = rtt if self.best_rtt is None else min(self.best_rtt, rtt)
            if rtt - self.best_rtt > LAG_THRESHOLD_SECONDS:
                self.slow_until = loop.time() + LAG_HOLD_SECONDS
            self.flush_held()

    def evict(self, reason: str) -> None:
        CLIENTS.pop(self.ws, None)
        self.close_code = 1013
        self.close_reason = reason
        if self.sender is not None:
            self.sender.cancel()

    async def send_messages(self) -> None:
        """The only task that writes application messages to this socket."""
        while True:
            await self.ready.wait()
            while self.pending:
                # A timer avoids creating another task for every send. It
                # cancels this sender on expiry; handle_client owns cleanup.
                timer = asyncio.get_running_loop().call_later(
                    CLIENT_SEND_TIMEOUT_SECONDS, self.evict,
                    "Client send timed out; reconnect for history",
                )
                try:
                    await self.ws.send(self.pending.popleft())
                finally:
                    timer.cancel()
            # No await between checking the deque and clearing the wakeup.
            self.ready.clear()


CLIENTS: dict[Any, ClientState] = {}
CLIENT_SESSIONS: dict[Any, str] = {}


class CaptionRevisionState:
    """Validate and commit updates for one ordered lecture stream.

    Segments increase monotonically; revisions increase within a segment and a
    final is terminal. Only a watermark is needed, so memory stays O(1).
    Keep this object across source reconnects, independently of history expiry.
    """

    def __init__(self) -> None:
        self.session: str | None = None
        self.segment = 0
        self.revision = 0
        self.final = False

    def accept(self, msg: Any, source_session: str) -> bool:
        """Atomically validate/commit a decoded update; False changes nothing.

        Call synchronously once before storing or distributing an update. The
        caller retains the original message/raw payload, including extra fields.
        """
        if not isinstance(msg, dict) or msg.get("type") != "CAPTION_UPDATE":
            return False
        if not isinstance(source_session, str) or not source_session:
            return False
        if msg.get("session") != source_session:
            return False
        if self.session is not None and self.session != source_session:
            return False

        segment, revision = msg.get("segment"), msg.get("revision")
        # bool is an int subclass, but is not a protocol sequence number.
        if type(segment) is not int or segment < 1:
            return False
        if type(revision) is not int or revision < 1:
            return False
        if type(msg.get("final")) is not bool or not isinstance(msg.get("text"), str):
            return False
        source_ts = msg.get("source_ts")
        if type(source_ts) not in (int, float):
            return False
        if isinstance(source_ts, float) and not math.isfinite(source_ts):
            return False
        if segment < self.segment:
            return False
        if segment == self.segment and (self.final or revision <= self.revision):
            return False

        self.session = source_session
        self.segment, self.revision, self.final = segment, revision, msg["final"]
        return True


# The server's history and broadcast path serve one lecture. Never reset this
# on SOURCE_HELLO: a reconnect must not reopen finalized or obsolete segments.
REVISION_STATE = CaptionRevisionState()

HISTORY_MAX_SECONDS = 60


class FinalizedHistory:
    """Bounded recovery cache, separate from authoritative revision state.

    Call record only after revision-state acceptance. Eviction forgets cached
    captions, not whether a revision has been accepted; this cache must never
    be used as the authoritative deduplication store.
    """

    def __init__(self, max_seconds: float = HISTORY_MAX_SECONDS, *, clock=time.time):
        if not math.isfinite(max_seconds) or max_seconds < HISTORY_MAX_SECONDS:
            raise ValueError("History must retain at least 60 seconds")
        self.max_seconds = max_seconds
        self.clock = clock
        self._items: dict[tuple[str, int], dict[str, Any]] = {}

    def _prune(self, now: float) -> None:
        # Source timestamps need not arrive in timestamp order.
        expired = [key for key, item in self._items.items()
                   if item["source_ts"] < now - self.max_seconds]
        for key in expired:
            del self._items[key]

    def record(self, update: dict[str, Any]) -> None:
        """Cache an accepted final, preserving its complete wire payload.

        One entry per (session, segment) prevents duplicate HISTORY items.
        Provisional, duplicate, and older revisions cannot replace a final.
        """
        now = self.clock()
        self._prune(now)
        if update.get("final") is not True:
            return
        if (not isinstance(update.get("session"), str)
                or type(update.get("segment")) is not int
                or update["segment"] < 1
                or type(update.get("revision")) is not int
                or update["revision"] < 1
                or type(update.get("source_ts")) not in (int, float)
                or not math.isfinite(update["source_ts"])):
            return
        if update["source_ts"] < now - self.max_seconds:
            return
        key = (update["session"], update["segment"])
        previous = self._items.get(key)
        if previous is not None and previous["revision"] >= update["revision"]:
            return
        self._items[key] = update.copy()

    def snapshot(self, session: str, last_segment: int = 0,
                 last_revision: int = 0, history_seconds: float = 30
                 ) -> list[dict[str, Any]]:
        """Return only recent finals strictly after the finalized cursor.

        Retain the entire time window even during bursts (assignment section
        3, requirement 8). Reads prune expired data from an idle source too.
        """
        now = self.clock()
        self._prune(now)
        if not math.isfinite(history_seconds):
            raise ValueError("history_seconds must be finite")
        seconds = min(max(history_seconds, 0), HISTORY_MAX_SECONDS)
        if seconds == 0:
            return []
        items = [item.copy() for (item_session, _), item in self._items.items()
                 if item_session == session
                 and item["source_ts"] >= now - seconds
                 and (item["segment"], item["revision"])
                 > (last_segment, last_revision)]
        return sorted(items, key=lambda item: (item["segment"], item["revision"]))


HISTORY = FinalizedHistory()


async def broadcast(raw_message: str | bytes, session: str | None = None) -> None:
    """Enqueue without waiting for socket I/O; evict only overflowing clients."""
    for ws, client in list(CLIENTS.items()):
        if session is not None and CLIENT_SESSIONS.get(ws) != session:
            continue
        if not client.enqueue(raw_message):
            client.evict("Client send queue full; reconnect for history")
    # A buffered source can produce many messages without yielding. Give the
    # sender tasks (and other connections) a turn even in that case.
    await asyncio.sleep(0)


async def handle_source(ws, hello: dict[str, Any]) -> None:
    session = hello.get("session")
    if not isinstance(session, str) or not session:
        await ws.close(code=1008, reason="Expected a nonempty source session")
        return
    print(f"source connected: session={session}", flush=True)

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

            if not isinstance(msg, dict):
                continue
            msg_type = msg.get("type")
            if msg_type == "CAPTION_UPDATE":
                if not REVISION_STATE.accept(msg, session):
                    continue
                # Record before enqueue/eviction; no await until all current
                # recipients have been offered the accepted update.
                if msg["final"]:
                    HISTORY.record(msg)
                await broadcast(raw, session)
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

    # Be liberal in what we accept: a malformed recovery field falls back to
    # its "no state" default (full recent context) instead of closing.
    session = hello.get("session")
    if not isinstance(session, str) or not session:
        session = REVISION_STATE.session or "L1"
    last_segment = hello.get("last_segment", 0)
    last_revision = hello.get("last_revision", 0)
    if (type(last_segment) is not int or last_segment < 0
            or type(last_revision) is not int or last_revision < 0):
        last_segment = last_revision = 0
    seconds = hello.get("history_seconds", 30)
    if type(seconds) not in (int, float) or not math.isfinite(seconds):
        seconds = 30
    items = HISTORY.snapshot(session, last_segment, last_revision, seconds)

    history_message = json.dumps({"type": "HISTORY", "items": items})
    client = ClientState(ws)
    client.enqueue(history_message)
    client.sender = asyncio.create_task(client.send_messages())
    if callable(getattr(ws, "ping", None)):
        client.lag_monitor = asyncio.create_task(client.monitor_receiver_lag())
    CLIENT_SESSIONS[ws] = session
    CLIENTS[ws] = client

    print(f"client connected: id={client_id}; clients={len(CLIENTS)}", flush=True)

    async def receive_messages() -> None:
        # Student clients normally don't send further messages after hello.
        # Keep the WebSocket open until the client disconnects.
        async for _ in ws:
            pass

    receiver = asyncio.create_task(receive_messages())
    try:
        done, _ = await asyncio.wait(
            (client.sender, receiver), return_when=asyncio.FIRST_COMPLETED
        )
        for task in done:
            if not task.cancelled():
                task.result()
    finally:
        CLIENTS.pop(ws, None)
        CLIENT_SESSIONS.pop(ws, None)
        client.sender.cancel()
        receiver.cancel()
        tasks = [client.sender, receiver]
        if client.lag_monitor is not None:
            client.lag_monitor.cancel()
            tasks.append(client.lag_monitor)
        await asyncio.gather(*tasks, return_exceptions=True)
        client.pending.clear()
        client.held_provisionals.clear()
        await ws.close(code=client.close_code, reason=client.close_reason)
        print(f"client disconnected: id={client_id}; clients={len(CLIENTS)}", flush=True)


async def handler(ws) -> None:
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        hello = json.loads(raw)
    except (asyncio.TimeoutError, json.JSONDecodeError, UnicodeDecodeError):
        await ws.close(code=1008, reason="Expected a valid hello message")
        return

    msg_type = hello.get("type") if isinstance(hello, dict) else None
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
        print(f"LiveRecall listening on ws://0.0.0.0:{port}", flush=True)
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
