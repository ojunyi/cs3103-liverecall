#!/usr/bin/env python3
"""Unit tests for the per-client send queue (requirements 8-11).

Run from the repository root, with the venv active:
    python test_queue.py
"""

from __future__ import annotations

import asyncio
import json

import solution_server as s


class FakeWS:
    """Stands in for a websocket: records what the sender task delivers."""

    def __init__(self, delay: float = 0.0):
        self.sent: list[str] = []
        self.delay = delay
        self.closed_with = None

    async def send(self, text: str) -> None:
        if self.delay:
            await asyncio.sleep(self.delay)
        self.sent.append(text)

    async def close(self, code=1000, reason=""):
        self.closed_with = code

    async def ping(self):
        """Like websockets: returns a future that completes when the pong arrives."""
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(0.0)                    # an instant pong
        return fut


def cap(seg, rev, final=False):
    return json.dumps({"type": "CAPTION_UPDATE", "segment": seg, "revision": rev,
                       "final": final, "text": f"s{seg}r{rev}"})


def put(client, seg, rev, final=False):
    return client.enqueue(cap(seg, rev, final), seg, rev, final)


def drain(client, now=None):
    """Everything left in the queue, as (segment, revision) pairs."""
    out = []
    while (e := client.pop(now)) is not None:
        out.append((e[1], e[2]))
    return out


def new_client(history="HISTORY"):
    return s.Client(FakeWS(), history)


def test_history_first():
    c = new_client()
    put(c, 1, 1)
    e = c.pop()
    assert e[s._RAW] == "HISTORY"
    print("ok: HISTORY is the first message in a new client's queue")


def test_no_coalescing_while_keeping_up():
    c = new_client(); c.pop()
    got = []
    for seg in range(1, 3):
        for rev, fin in ((1, False), (2, False), (3, True)):
            put(c, seg, rev, fin)
            got += drain(c)                     # the client reads each message as it arrives
    assert got == [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3)]
    print("ok: a client that keeps up receives every revision")


def test_lagging_coalesces():
    c = new_client(); c.pop()
    for seg in range(1, 5):                     # four queued finals: now lagging
        put(c, seg, 3, True)
    put(c, 5, 1); put(c, 5, 2); put(c, 5, 3)    # provisional 1, 2 replaced, then final 3
    assert drain(c) == [(1, 3), (2, 3), (3, 3), (4, 3), (5, 3)]
    c = new_client(); c.pop()
    for seg in range(1, 5):
        put(c, seg, 3, True)
    put(c, 5, 1); put(c, 5, 2)                  # active segment: newest revision kept
    assert drain(c) == [(1, 3), (2, 3), (3, 3), (4, 3), (5, 2)]
    print("ok: lagging client keeps newest revision of the active segment, drops obsolete ones")


def test_replace_keeps_position_and_ignores_stale():
    c = new_client(); c.pop()
    for seg in range(1, 5):
        put(c, seg, 3, True)
    put(c, 5, 2)
    put(c, 5, 1)                                # older revision arrives late: ignored
    put(c, 5, 2)                                # repeat: ignored
    put(c, 6, 1)
    put(c, 5, 3)                                # newer revision of an earlier queued segment
    assert drain(c) == [(1, 3), (2, 3), (3, 3), (4, 3), (5, 3), (6, 1)]
    print("ok: replacement keeps position; stale or repeated revisions are ignored")


def test_every_final_kept_and_bounded():
    c = new_client(); c.pop()
    ok = True
    for seg in range(1, s.MAX_BACKLOG + 1):
        ok = put(c, seg, 3, True) and ok
    assert ok and c.live == s.MAX_BACKLOG
    assert put(c, s.MAX_BACKLOG + 1, 3, True) is False   # one over: too slow
    print(f"ok: finals are never coalesced; backlog bounded at {s.MAX_BACKLOG} (then evicted)")


def test_provisionals_bounded_by_segments():
    c = new_client(); c.pop()
    for seg in range(1, 5):
        put(c, seg, 3, True)
    for rev in range(1, 10000):
        put(c, 5, rev)
    assert c.live == 5, c.live
    print("ok: 10000 revisions of one segment occupy a single queue slot")


def test_malformed_forwarded():
    c = new_client(); c.pop()
    c.enqueue("junk", None, None, False)
    assert c.pop()[s._RAW] == "junk"
    print("ok: messages without a usable segment/revision are forwarded unchanged")


def test_lagging_mode_holds_provisionals_sends_finals_first():
    c = new_client(); c.pop()
    now = 1000.0
    c.slow_until = now + 100                    # the lag probe flagged this client
    c.last_prov = now - 0.1                     # a provisional was sent just now
    for seg, rev, fin in ((1, 1, False), (1, 2, False), (2, 3, True), (3, 1, False)):
        c.enqueue(cap(seg, rev, fin), seg, rev, fin, now)
    assert c.live == 3                          # provisional 1 replaced in place
    e = c.pop(now)                              # held provisionals are skipped: the final goes first
    assert (e[s._SEG], e[s._REV]) == (2, 3)
    assert c.pop(now) is None and c.held        # provisionals must wait PROVISIONAL_GAP
    later = now + s.PROVISIONAL_GAP + 0.1
    e = c.pop(later)                            # then exactly one provisional goes
    assert e[s._FIN] is False and c.pop(later) is None and c.held
    print("ok: lagging mode sends finals first and at most one provisional per PROVISIONAL_GAP")


def test_lag_mode_ends_and_full_updates_resume():
    c = new_client(); c.pop()
    c.slow_until = 1005.0
    c.enqueue(cap(1, 1), 1, 1, False, 1006.0)   # after the hold: no coalescing any more
    c.enqueue(cap(1, 2), 1, 2, False, 1006.0)
    assert drain(c, 1006.0) == [(1, 1), (1, 2)]
    print("ok: after SLOW_HOLD_SECONDS without a lag sign the client gets every revision again")


class PingWS(FakeWS):
    """A websocket whose pongs take the given times (seconds), in order."""

    def __init__(self, latencies):
        super().__init__()
        self.latencies = list(latencies)

    async def ping(self):
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        loop.call_later(self.latencies.pop(0), fut.set_result, 0.0)
        return fut


async def probe_times(client, n, step=0.05):
    for _ in range(n):
        await client.probe(s.time.monotonic())
        await asyncio.sleep(step)
        client.last_probe = 0.0          # allow the next probe straight away


async def test_slow_pong_flags_client():
    old = s.LAG_ON_SECONDS
    s.LAG_ON_SECONDS = 0.1
    try:
        c = s.Client(PingWS([0.01, 0.4]), "H")       # normal round trip, then a late pong
        await probe_times(c, 2, step=0.5)
        assert c.slow_until > 0, "a pong far slower than usual did not flag the client"
    finally:
        s.LAG_ON_SECONDS = old
    print("ok: a pong much slower than the client's own best round trip flags it as lagging")


async def test_slow_network_is_not_a_slow_client():
    old = s.LAG_ON_SECONDS
    s.LAG_ON_SECONDS = 0.1
    try:
        c = s.Client(PingWS([0.25] * 6), "H")        # every round trip is slow, but steady
        await probe_times(c, 6, step=0.3)
        assert c.slow_until == 0.0, "a steady high round trip was mistaken for a slow reader"
    finally:
        s.LAG_ON_SECONDS = old
    print("ok: a steady high network delay does not flag the client")


def test_repeat_offender_held_longer():
    c = new_client()
    c.flag_lag(100.0)
    assert c.hold == s.SLOW_HOLD_SECONDS and c.slow_until == 100.0 + s.SLOW_HOLD_SECONDS
    c.flag_lag(105.0)                                 # still inside the hold: no escalation
    assert c.hold == s.SLOW_HOLD_SECONDS
    c.flag_lag(c.slow_until + 1)                      # lags again soon after the hold ended
    assert c.hold == 2 * s.SLOW_HOLD_SECONDS
    c.flag_lag(c.slow_until + 1)
    assert c.hold == min(4 * s.SLOW_HOLD_SECONDS, s.MAX_HOLD_SECONDS)
    c.flag_lag(c.slow_until + s.REFLAG_WINDOW + 1)    # long calm in between: back to the base hold
    assert c.hold == s.SLOW_HOLD_SECONDS
    print("ok: a client that keeps lagging is held longer; a calm one starts over")


async def test_slow_client_isolated_and_catches_up():
    """A slow reader falls behind but still receives every final, in order,
    and a fast client is unaffected."""
    fast, slow = FakeWS(), FakeWS(delay=0.02)
    cf, cs = s.Client(fast, "H"), s.Client(slow, "H")
    s.CLIENTS.clear()
    s.CLIENTS[fast], s.CLIENTS[slow] = cf, cs
    tasks = [asyncio.create_task(s.client_sender(c)) for c in (cf, cs)]
    for seg in range(1, 41):
        for rev, fin in ((1, False), (2, False), (3, True)):
            msg = json.loads(cap(seg, rev, fin))
            s.broadcast(cap(seg, rev, fin), msg)
        await asyncio.sleep(0.01)
    await asyncio.sleep(1.5)
    for t in tasks:
        t.cancel()
    def finals(w):
        return [json.loads(x)["segment"] for x in w.sent if x != "H" and json.loads(x)["final"]]
    assert finals(fast) == list(range(1, 41)), "fast client missed finals"
    assert finals(slow) == list(range(1, 41)), "slow client missed finals"
    assert len(fast.sent) == 1 + 40 * 3, len(fast.sent)       # no coalescing for the fast one
    assert len(slow.sent) < len(fast.sent)                    # the slow one got fewer provisionals
    print(f"ok: slow client got all 40 finals ({len(slow.sent)} messages vs {len(fast.sent)} for the fast client)")
    s.CLIENTS.clear()


async def test_overflow_evicts_and_source_never_waits():
    stuck = FakeWS(delay=3600)                  # never finishes sending
    c = s.Client(stuck, "H")
    s.CLIENTS.clear(); s.CLIENTS[stuck] = c
    task = asyncio.create_task(s.client_sender(c))
    for seg in range(1, s.MAX_BACKLOG + 50):
        s.broadcast(cap(seg, 3, True), json.loads(cap(seg, 3, True)))   # no await: cannot block
    await asyncio.sleep(0.05)
    assert stuck not in s.CLIENTS and stuck.closed_with == 1013
    task.cancel()
    print("ok: broadcast never waits; a stuck client is evicted with code 1013 at the bound")


if __name__ == "__main__":
    test_history_first()
    test_no_coalescing_while_keeping_up()
    test_lagging_coalesces()
    test_replace_keeps_position_and_ignores_stale()
    test_every_final_kept_and_bounded()
    test_provisionals_bounded_by_segments()
    test_malformed_forwarded()
    test_lagging_mode_holds_provisionals_sends_finals_first()
    test_lag_mode_ends_and_full_updates_resume()
    asyncio.run(test_slow_pong_flags_client())
    asyncio.run(test_slow_network_is_not_a_slow_client())
    test_repeat_offender_held_longer()
    asyncio.run(test_slow_client_isolated_and_catches_up())
    asyncio.run(test_overflow_evicts_and_source_never_waits())
    print("\nALL QUEUE TESTS PASSED")