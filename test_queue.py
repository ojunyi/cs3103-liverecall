#!/usr/bin/env python3
"""Tests for per-client queues, lag detection and the overload policy (req 9-11).

Run from the repository root, with the venv active:
    python test_queue.py

The end-to-end tests start their own server on a free port. Nothing here
modifies the instructor files.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

import websockets

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import solution_server as s                      # noqa: E402
from test_history import Server, hello, source   # noqa: E402


def cap(seg, rev, final=False, queued_at=0.0):
    raw = json.dumps({"type": "CAPTION_UPDATE", "session": "L1", "segment": seg,
                      "revision": rev, "source_ts": 1.0, "final": final, "text": "t"})
    return s.Outgoing(raw, seg, rev, final, queued_at)


def ids(pending):
    return [(x.segment, x.revision, x.final) if x.segment is not None else "CTRL"
            for x in pending]


class FakeWS:
    """Stands in for a websocket: ping() returns a future resolved after `rtt`."""

    def __init__(self, rtt=0.0):
        self.rtt = rtt
        self.sent = []

    async def ping(self):
        fut = asyncio.get_running_loop().create_future()
        asyncio.get_running_loop().call_later(self.rtt, fut.set_result, self.rtt)
        return fut

    async def send(self, raw):
        self.sent.append(raw)


def client(rtt=0.0, key=None):
    c = s.ClientState(FakeWS(rtt), "L1", key)
    c.network_estimate = lambda: (0.01, 0.0, 0.2, 0.01)  # block 0.26 s, lag 1.21 s
    return c


async def test_full_updates_without_backpressure():
    c = client()
    for rev in (1, 2, 3):
        assert c.enqueue(cap(1, rev, rev == 3))
    assert ids(c.pending) == [(1, 1, False), (1, 2, False), (1, 3, True)]
    print("ok: without backpressure every revision is queued (full update coverage)")


async def test_backpressure_coalesces_obsolete_provisionals():
    c = client()
    c.blocked = True
    for x in (cap(1, 1), cap(1, 2), cap(2, 1), cap(1, 3, True), cap(2, 2)):
        assert c.enqueue(x)
    assert ids(c.pending) == [(1, 3, True), (2, 2, False)], ids(c.pending)

    c = client()
    for x in (cap(1, 1), cap(1, 2), cap(2, 1), cap(1, 3, True), cap(2, 2)):
        c.pending.append(x)
    c.probe_sent = 0.0
    c._probe_block(c.probe_id)                     # becoming blocked compacts the queue
    assert c.blocked and ids(c.pending) == [(1, 3, True), (2, 2, False)], ids(c.pending)
    print("ok: under backpressure only finals and the newest provisional per segment stay queued")


async def test_queue_is_bounded():
    c = client()
    c.blocked = True
    for seg in range(1, s.QUEUE_MAX_ITEMS + 1):
        assert c.enqueue(cap(seg, 1, True))
    assert not c.enqueue(cap(s.QUEUE_MAX_ITEMS + 1, 1, True))
    print("ok: a queue that cannot hold another final reports overflow (client resyncs)")


async def test_lagging_sends_finals_and_holds_newest_provisional():
    c = client()
    c.lagging, c.lag_until = True, c.loop.time() + 60
    assert c.enqueue(cap(1, 1)) and c.enqueue(cap(1, 2))
    assert ids(c.pending) == [] and c.held[1][0].revision == 2
    assert c.enqueue(cap(1, 3, True))
    assert ids(c.pending) == [(1, 3, True)] and not c.held
    assert c.enqueue(cap(2, 1))
    c.update_lag(c.loop.time() + s.HELD_REFRESH_SECONDS + 0.1)
    assert ids(c.pending) == [(1, 3, True), (2, 1, False)], ids(c.pending)
    assert c.enqueue(cap(2, 2))
    c.lag_until = 0
    c.update_lag(c.loop.time())
    assert not c.lagging and ids(c.pending)[-1] == (2, 2, False)
    print("ok: lagging forwards finals at once, keeps only the newest provisional, then resumes")


async def test_network_hold_is_adaptive():
    c = client()
    c.enter_lag()
    assert c.lagging and c.memory.net_hold == s.NETWORK_HOLD_BASE_SECONDS
    c.lag_until = 0
    c.update_lag(c.loop.time())
    assert not c.lagging
    c.enter_lag()                                  # recurs soon after: hold doubles
    assert c.memory.net_hold == 2 * s.NETWORK_HOLD_BASE_SECONDS
    c.lag_until = 0
    c.update_lag(c.loop.time())
    c.memory.last_exit -= s.HOLD_RESET_SECONDS + 1
    c.enter_lag()                                  # long healthy period: reset
    assert c.memory.net_hold == s.NETWORK_HOLD_BASE_SECONDS
    print("ok: network lag hold starts short, doubles on recurrence, resets after recovery")


async def probe(rtt, reader=False):
    c = client(rtt)
    c.reader_limited = lambda: reader
    c.captions_sent, c.first_caption_at = 10, c.loop.time() - 5
    c.start_probe()
    seen_blocked = False
    end = c.loop.time() + rtt + 0.2
    while c.loop.time() < end:
        seen_blocked |= c.blocked
        await asyncio.sleep(0.02)
    return c, seen_blocked


async def test_probe_deadlines():
    c, blocked = await probe(0.1)
    assert not blocked and not c.lagging and c.probe_sent is None
    c, blocked = await probe(0.6)            # e.g. one TCP retransmission
    assert blocked and not c.blocked and not c.lagging and not c.closed
    c, _ = await probe(1.6)                  # backlog, bytes not yet acknowledged
    assert c.lagging and not c.closed
    c, _ = await probe(1.6, reader=True)     # backlog, kernel already acked everything
    assert c.closed and c.close_code == s.CLOSE_TRY_AGAIN
    assert c.memory.reader_sent == 10
    print("ok: a short delay only pauses writes; a long one lags (network) or resyncs (slow reader)")


async def test_delay_and_loss_never_look_like_lag():
    """T5-like path (netem 120 ms +- 30 ms each way, 3% loss): the kernel
    reports srtt ~0.24 s, rttvar ~0.04 s, rto ~0.44 s. Pongs delayed by
    jitter, one lost packet (one RTO) or two in a row (RTO + 2*RTO) must not
    switch the client to finals-only."""
    for rtt in (0.18, 0.30, 0.24 + 0.44, 0.24 + 0.44 + 0.88):
        c = client(rtt)
        c.network_estimate = lambda: (0.24, 0.04, 0.44, 0.10)
        c.reader_limited = lambda: False
        c.start_probe()
        await asyncio.sleep(rtt + 0.1)
        assert not c.lagging and not c.blocked and not c.closed, rtt
    print("ok: delay, jitter and up to two consecutive retransmissions never trigger lagging")


async def test_standing_queue_means_congestion():
    """srtt well above min_rtt (a queue our own traffic builds, e.g. a
    bandwidth cap) switches to finals-only; delay and jitter do not."""
    cases = {
        "T5 delay+jitter": ((0.24, 0.04, 0.44, 2, 0, 0.09), False),
        "heavy jitter": ((0.20, 0.06, 0.44, 2, 0, 0.01), False),
        "bandwidth cap": ((0.80, 0.10, 1.20, 9, 0, 0.002), True),
        "queue drained": ((0.03, 0.01, 0.20, 1, 0, 0.002), False),
    }
    real = s.tcp_info
    try:
        for name, (info, expected) in cases.items():
            s.tcp_info = lambda sock, info=info: info
            assert client().congested() is expected, name
        s.tcp_info = lambda sock: cases["bandwidth cap"][0]
        c = client()
        task = asyncio.create_task(c.monitor_flow())
        await asyncio.sleep(s.MONITOR_TICK_SECONDS * (s.CONGESTED_TICKS + 0.5))
        assert c.lagging, "congestion should switch to finals-only"
        # Partly drained (below the entry level, above the clear level): the
        # hold keeps being renewed, so the client does not return to full rate.
        s.tcp_info = lambda sock: (0.35, 0.05, 1.0, 1, 0, 0.15)
        c.lag_until = 0
        await asyncio.sleep(s.MONITOR_TICK_SECONDS * 1.5)
        assert c.lagging, "a half-drained queue must not end the hold"
        # Drained: the hold now runs out and full updates resume.
        s.tcp_info = lambda sock: (0.05, 0.01, 0.2, 1, 0, 0.002)
        c.lag_until = 0
        await asyncio.sleep(s.MONITOR_TICK_SECONDS * 1.5)
        assert not c.lagging, "a drained queue should end the hold"
        task.cancel()
    finally:
        s.tcp_info = real
    print("ok: a standing queue (srtt >> min_rtt) triggers finals-only until it drains; "
          "delay and jitter do not")


async def test_path_baseline_is_shared_per_address():
    """A client whose own min_rtt was measured behind a queue (its handshake
    waited at a capped link) still sees the queue via its address's baseline."""
    real = s.tcp_info
    try:
        s.PATH_MIN_RTT.clear()
        early, late = client(), client()
        early.address = late.address = "10.0.0.7"
        s.tcp_info = lambda sock: (0.05, 0.01, 0.2, 1, 0, 0.002)
        assert not early.congested()                  # learns the path minimum
        s.tcp_info = lambda sock: (0.75, 0.10, 1.2, 4, 0, 0.50)
        assert late.congested(), "late joiner must use the shared baseline"
        other = client()
        other.address = "10.0.0.8"                    # a different path
        assert not other.congested()
    finally:
        s.tcp_info = real
        s.PATH_MIN_RTT.clear()
    print("ok: connections from one address share a path baseline, so late joiners detect queues")


async def test_reader_memory_chooses_mode():
    loop = asyncio.get_running_loop()
    now = loop.time()
    s.LECTURE = s.Lecture(1, "L1")
    for i in range(80):                      # 4 updates/s, one final in three
        s.LECTURE.note(i % 3 == 2, now - 20 + i * 0.25)

    def reconnect(key, rate):
        m = s.LagMemory()
        m.reader_at, m.reader_first = now - 1, now - 10.5
        m.reader_sent = int(rate * 10)
        m.updated = now
        s.LAG_MEMORY[("L1", key)] = m
        return s.ClientState(FakeWS(), "L1", ("L1", key))

    c = reconnect(1, 2.0)                    # between finals rate and update rate
    assert c.lagging and not c.batched
    c = reconnect(2, 1.0)                    # slower than the finals themselves
    assert c.batched and not c.lagging
    c = reconnect(3, 4.0)                    # kept up with everything: not slow
    assert not c.batched and not c.lagging
    assert c.enqueue(cap(1, 1)) and ids(c.pending) == [(1, 1, False)]
    b = reconnect(4, 0.5)
    assert b.enqueue(cap(1, 1)) and b.enqueue(cap(1, 2, True)) and ids(b.pending) == []
    assert b.batch_since is not None
    print("ok: measured reader rate selects finals-only, batched HISTORY, or full updates")


async def reconnecting_reader(uri, total, delay, got, max_queue=16, deadline=90):
    """A slow reader that reconnects with its cursor, like load_clients.py."""
    cursor = (0, 0)
    connections = 0
    end = time.time() + deadline
    while time.time() < end:
        try:
            async with websockets.connect(uri, max_queue=max_queue) as ws:
                connections += 1
                await hello(ws, cursor[0], cursor[1], secs=30, client_id=77)
                while time.time() < end:
                    m = json.loads(await asyncio.wait_for(ws.recv(), 15))
                    items = m["items"] if m["type"] == "HISTORY" else [m]
                    for i in items:
                        if m["type"] == "CAPTION_UPDATE" or i.get("final"):
                            got.append((m["type"], i["segment"], i["revision"], i["final"],
                                        time.time() - i["source_ts"]))
                        if i["final"]:
                            cursor = max(cursor, (i["segment"], i["revision"]))
                    if cursor[0] >= total:
                        return connections
                    await asyncio.sleep(delay)
        except (OSError, asyncio.TimeoutError, websockets.exceptions.WebSocketException):
            await asyncio.sleep(0.4)
    return connections


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

        tasks = [asyncio.create_task(fast()),
                 asyncio.create_task(reconnecting_reader(srv.uri, total, 0.15, slow_got, 4))]
        await asyncio.sleep(0.3)
        await source(srv.uri, total, gap=0.05)
        _, connections = await asyncio.wait_for(asyncio.gather(*tasks), 90)

    assert len(fast_got) == 3 * total, len(fast_got)
    assert max(age for *_, age in fast_got) < 0.5
    finals = [seg for _, seg, _, fin, _ in slow_got if fin]
    assert sorted(finals) == list(range(1, total + 1)), finals
    assert len(finals) == len(set(finals)), "duplicate final"
    print(f"ok: fast client got all {3 * total} revisions (max age "
          f"{max(a for *_, a in fast_got) * 1000:.0f} ms); slow client got all {total} finals "
          f"once over {connections} connection(s)")


async def test_reader_slower_than_finals_gets_every_final():
    total = 60
    with Server() as srv:
        got = []
        reader = asyncio.create_task(reconnecting_reader(srv.uri, total, 0.3, got))
        await asyncio.sleep(0.3)
        await source(srv.uri, total, gap=0.1)            # 10 finals/s, reader ~3 msg/s
        connections = await asyncio.wait_for(reader, 90)
    finals = [seg for _, seg, _, fin, _ in got if fin]
    assert sorted(finals) == list(range(1, total + 1)), finals
    assert len(finals) == len(set(finals)), "duplicate final"
    batched = sum(1 for kind, *_ in got if kind == "HISTORY")
    print(f"ok: a reader slower than the finals rate still got all {total} finals once "
          f"({batched} via HISTORY batches, {connections} connections)")


async def async_tests():
    await test_full_updates_without_backpressure()
    await test_backpressure_coalesces_obsolete_provisionals()
    await test_queue_is_bounded()
    await test_lagging_sends_finals_and_holds_newest_provisional()
    await test_network_hold_is_adaptive()
    await test_probe_deadlines()
    await test_delay_and_loss_never_look_like_lag()
    await test_standing_queue_means_congestion()
    await test_path_baseline_is_shared_per_address()
    await test_reader_memory_chooses_mode()
    await test_slow_client_isolated_end_to_end()
    await test_reader_slower_than_finals_gets_every_final()


if __name__ == "__main__":
    asyncio.run(async_tests())
    print("\nALL QUEUE TESTS PASSED")
