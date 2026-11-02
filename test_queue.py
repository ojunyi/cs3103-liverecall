#!/usr/bin/env python3
"""Tests for per-client queues, lag detection and overload policy (req 9-11).

Run from the repository root, with the venv active:
    python test_queue.py

The end-to-end test starts its own server on a free port. Nothing here
modifies the instructor files.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from collections import deque
from pathlib import Path

import websockets

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import solution_server as s                      # noqa: E402
from test_history import Server, hello, source   # noqa: E402


def cap(seg, rev, final=False):
    return json.dumps({"type": "CAPTION_UPDATE", "session": "L1", "segment": seg,
                       "revision": rev, "source_ts": 1.0, "final": final, "text": "t"})


def ids(pending):
    out = []
    for raw in pending:
        m = json.loads(raw)
        out.append((m["segment"], m["revision"], m["final"]) if "segment" in m else m["type"])
    return out


class FakeWS:
    """Stands in for a websocket: ping() returns a future resolved after `rtt`."""

    def __init__(self, rtt):
        self.rtt = rtt
        self.closed = False

    async def ping(self):
        fut = asyncio.get_running_loop().create_future()
        asyncio.get_running_loop().call_later(self.rtt, fut.set_result, None)
        return fut


def test_pending_under_bound_keeps_everything():
    q = deque()
    for rev in (1, 2, 3):
        assert s.enqueue_pending(q, cap(1, rev, rev == 3), 8)
    assert ids(q) == [(1, 1, False), (1, 2, False), (1, 3, True)]
    print("ok: below the bound every revision is queued (full updates)")


def test_pending_full_drops_only_obsolete_provisionals():
    q = deque()
    for raw in (cap(1, 1), cap(1, 2), cap(2, 1), cap(1, 3, True)):
        assert s.enqueue_pending(q, raw, 4)
    assert s.enqueue_pending(q, cap(2, 2), 4)
    # seg 1 provisionals are superseded by its final; seg 2 rev 1 by rev 2
    assert ids(q) == [(1, 3, True), (2, 2, False)], ids(q)
    print("ok: a full queue drops superseded provisionals, keeps finals and the newest revision")


def test_pending_full_of_finals_reports_overflow():
    q = deque(cap(i, 3, True) for i in range(1, 5))
    assert not s.enqueue_pending(q, cap(5, 3, True), 4)
    assert len(q) == 4
    print("ok: a queue that cannot fit a final reports overflow (client is evicted)")


async def test_lag_timeout_is_relative_to_best_rtt():
    c = s.ClientState(FakeWS(0))
    assert c.lag_timeout() == s.LAG_INITIAL_RTT_SECONDS + s.LAG_THRESHOLD_SECONDS
    c.best_rtt = 0.2
    assert abs(c.lag_timeout() - (0.2 + s.LAG_THRESHOLD_SECONDS)) < 1e-9
    print("ok: lag timeout = client's best RTT + threshold")


async def test_lagging_holds_newest_provisional_finals_pass():
    c = s.ClientState(FakeWS(0))
    c.slow_until = asyncio.get_running_loop().time() + 5
    for rev in (1, 2):
        assert c.enqueue(cap(1, rev))
    assert ids(c.pending) == [] and len(c.held_provisionals) == 1
    assert c.enqueue(cap(1, 3, True))
    assert ids(c.pending) == [(1, 3, True)] and not c.held_provisionals
    assert c.enqueue(cap(2, 1))
    c.slow_until = 0                                    # lag ends
    c.flush_held()
    assert ids(c.pending) == [(1, 3, True), (2, 1, False)]
    print("ok: lagging client gets finals at once; only the newest provisional is held, then flushed")


async def probe_once(rtt, best):
    c = s.ClientState(FakeWS(rtt))
    c.best_rtt = best
    task = asyncio.create_task(c.monitor_receiver_lag())
    await asyncio.sleep(s.LAG_PROBE_INTERVAL_SECONDS + rtt + 0.1)
    task.cancel()
    return c


async def test_slow_pong_flags_steady_delay_does_not():
    loop = asyncio.get_running_loop()
    slow = await probe_once(rtt=0.6, best=0.01)
    assert slow.slow_until > loop.time()
    steady = await probe_once(rtt=0.3, best=0.29)       # e.g. netem delay 150 ms each way
    assert steady.slow_until == 0
    print("ok: a pong much slower than usual flags the client; a steady 300 ms RTT does not")


async def test_slow_client_isolated_end_to_end():
    total = 40
    with Server() as srv:
        fast_got, slow_got = [], []

        async def fast():
            async with websockets.connect(srv.uri) as ws:
                await hello(ws, client_id=1)
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 10))
                    if m["type"] == "CAPTION_UPDATE":
                        fast_got.append((m["segment"], m["revision"], time.time() - m["source_ts"]))
                        if m["final"] and m["segment"] == total:
                            return

        async def slow():
            async with websockets.connect(srv.uri, max_queue=4) as ws:
                await hello(ws, client_id=2)
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 30))
                    await asyncio.sleep(0.15)               # reads far slower than the source
                    if m["type"] == "CAPTION_UPDATE":
                        slow_got.append((m["segment"], m["revision"], m["final"]))
                        if m["final"] and m["segment"] == total:
                            return

        tasks = [asyncio.create_task(fast()), asyncio.create_task(slow())]
        await asyncio.sleep(0.3)
        await source(srv.uri, total, gap=0.05)
        await asyncio.wait_for(asyncio.gather(*tasks), 60)

    assert len(fast_got) == 3 * total, len(fast_got)
    assert max(age for *_, age in fast_got) < 0.5
    finals = sorted(seg for seg, _, fin in slow_got if fin)
    assert finals == list(range(1, total + 1)), finals
    assert len(slow_got) < 3 * total
    print(f"ok: fast client got all {3 * total} revisions (max age "
          f"{max(a for *_, a in fast_got) * 1000:.0f} ms); slow client got all {total} finals "
          f"in {len(slow_got)} messages")


async def async_tests():
    await test_lag_timeout_is_relative_to_best_rtt()
    await test_lagging_holds_newest_provisional_finals_pass()
    await test_slow_pong_flags_steady_delay_does_not()
    await test_slow_client_isolated_end_to_end()


if __name__ == "__main__":
    test_pending_under_bound_keeps_everything()
    test_pending_full_drops_only_obsolete_provisionals()
    test_pending_full_of_finals_reports_overflow()
    asyncio.run(async_tests())
    print("\nALL QUEUE TESTS PASSED")
