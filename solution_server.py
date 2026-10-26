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
import json
import time
from typing import Any

import websockets

# Naive global set of currently connected student clients.
# There is deliberately no per-client queue or recovery state here.
CLIENTS: set[Any] = set()

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
    # Binary search for the first entry inside the requested time window.
    # Cost is O(log n) however large the history is; only the entries that are actually returned are looked at afterwards.
    start = bisect.bisect_left(_h_times, now - seconds)
 
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

async def broadcast_naively(raw_message: str) -> None:
    """Send one message directly to every connected client.

    This is intentionally a naive baseline. The source handler waits for all
    sends to complete. Under pressure, this design may accumulate delay and is
    not the target architecture for the assignment.
    """
    if not CLIENTS:
        return

    snapshot = list(CLIENTS)
    results = await asyncio.gather(
        *(client.send(raw_message) for client in snapshot),
        return_exceptions=True,
    )
    for client, result in zip(snapshot, results):
        if isinstance(result, Exception):
            CLIENTS.discard(client)


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
                # Baseline behavior: forward exactly what was received.
                if msg.get("final") is True:
                    store_final(msg, raw if isinstance(raw, str) else raw.decode("utf-8"))
                # No validation, coalescing, or bounded queues.
                await broadcast_naively(raw)
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

    history_message = build_history(hello)
    CLIENTS.add(ws)

    print(f"client connected: id={client_id}; clients={len(CLIENTS)}", flush=True)

    try:
        await ws.send(history_message)
        # Student clients normally don't send further messages after hello.
        # Keep the WebSocket open until the client disconnects.
        async for _ in ws:
            pass
    finally:
        CLIENTS.discard(ws)
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
