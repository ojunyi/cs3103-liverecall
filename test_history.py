#!/usr/bin/env python3
"""Tests for the LiveRecall server: revision state, history, late join, resume.

Run from the repository root, with the venv active:
    python test_history.py

It starts its own server on a free port, so stop any server you left running
(it does not touch port 8765). Nothing here modifies the instructor files.
"""

from __future__ import annotations

import asyncio
import json
import socket
import subprocess
import sys
import time
from pathlib import Path

import websockets
from websockets.sync.client import connect as sync_connect

HERE = Path(__file__).resolve().parent      # folder containing this script
sys.path.insert(0, str(HERE))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    """Runs solution_server.py as a child process on a free port."""

    def __enter__(self):
        self.port = free_port()
        self.uri = f"ws://127.0.0.1:{self.port}"
        self.proc = subprocess.Popen(
            [sys.executable, "solution_server.py", "--port", str(self.port)],
            cwd=HERE, stdout=subprocess.DEVNULL,
        )
        # Wait until the server answers a real CLIENT_HELLO. (Probing with a bare
        # TCP connect makes the server log a harmless but ugly "opening
        # handshake failed" traceback.)
        deadline = time.time() + 10
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError("solution_server.py exited during start-up")
            try:
                with sync_connect(self.uri, open_timeout=1) as ws:
                    ws.send(json.dumps({"type": "CLIENT_HELLO", "client_id": 0,
                                        "session": "L1", "last_segment": 0,
                                        "last_revision": 0, "history_seconds": 0}))
                    ws.recv(timeout=2)
                return self
            except (OSError, TimeoutError, websockets.exceptions.WebSocketException):
                time.sleep(0.1)
        raise RuntimeError("server did not start")

    def __exit__(self, *exc):
        self.proc.terminate()
        self.proc.wait(timeout=5)


def update(seg, rev, final, text=None, **extra):
    msg = {"type": "CAPTION_UPDATE", "session": "L1", "segment": seg, "revision": rev,
           "source_ts": time.time(), "final": final, "text": text or f"seg{seg} rev{rev}"}
    msg.update(extra)
    return json.dumps(msg)


async def send_events(uri, events, gap=0.01, done=False):
    """events: iterable of (segment, revision, final)."""
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"type": "SOURCE_HELLO", "session": "L1"}))
        for seg, rev, final in events:
            await ws.send(update(seg, rev, final, extra_field="ignored"))
            await asyncio.sleep(gap)
        if done:
            await ws.send(json.dumps({"type": "SOURCE_DONE", "session": "L1"}))
            await asyncio.sleep(0.05)


async def source(uri, n_segments, gap=0.01, start=1, on_segment=None, done=False):
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"type": "SOURCE_HELLO", "session": "L1"}))
        for seg in range(start, start + n_segments):
            if on_segment:
                on_segment(seg)
            for rev in (1, 2, 3):
                await ws.send(update(seg, rev, rev == 3, extra_field="ignored"))
            await asyncio.sleep(gap)
        if done:
            await ws.send(json.dumps({"type": "SOURCE_DONE", "session": "L1"}))
            await asyncio.sleep(0.05)


async def hello(ws, last_seg=0, last_rev=0, secs=30, client_id=1, **extra):
    msg = {"type": "CLIENT_HELLO", "client_id": client_id, "session": "L1",
           "last_segment": last_seg, "last_revision": last_rev, "history_seconds": secs}
    msg.update(extra)
    await ws.send(json.dumps(msg))


async def get_history_msg(uri, last_seg=0, last_rev=0, secs=30, **extra):
    async with websockets.connect(uri) as ws:
        await hello(ws, last_seg, last_rev, secs, **extra)
        msg = json.loads(await asyncio.wait_for(ws.recv(), 3))
        assert msg["type"] == "HISTORY", msg
        return msg


async def get_history(uri, last_seg=0, last_rev=0, secs=30, **extra):
    return (await get_history_msg(uri, last_seg, last_rev, secs, **extra))["items"]


async def collect(uri, out, idle=1.5, **hello_args):
    """Record (kind, type, segment, revision, final) until idle for `idle` s."""
    async with websockets.connect(uri) as ws:
        await hello(ws, **hello_args)
        try:
            while True:
                m = json.loads(await asyncio.wait_for(ws.recv(), idle))
                if m["type"] == "HISTORY":
                    out.extend(("H", i["segment"], i["revision"], i["final"]) for i in m["items"])
                elif m["type"] == "CAPTION_UPDATE":
                    out.append(("L", m["segment"], m["revision"], m["final"]))
                else:
                    out.append((m["type"], m.get("lecture")))
        except asyncio.TimeoutError:
            return


async def join_midstream(uri, out, last_segment):
    """Join, then collect HISTORY + live finals until the last segment arrives."""
    async with websockets.connect(uri) as ws:
        await hello(ws, secs=60, client_id=3)
        end = time.time() + 20
        while time.time() < end:
            try:
                m = json.loads(await asyncio.wait_for(ws.recv(), 0.4))
            except asyncio.TimeoutError:
                continue
            if m["type"] == "HISTORY":
                out.extend(("H", i["segment"]) for i in m["items"])
            elif m.get("final"):
                out.append(("L", m["segment"]))
                if m["segment"] >= last_segment:
                    return


async def protocol_tests():
    with Server() as srv:
        assert await get_history(srv.uri) == []
        print("ok: empty history before any caption")

        await source(srv.uri, 10)
        items = await get_history(srv.uri)
        assert [i["segment"] for i in items] == list(range(1, 11))
        assert all(i["final"] is True and i["revision"] == 3 for i in items)
        assert all(i.get("extra_field") == "ignored" for i in items)
        print("ok: late join gets 10 finals, oldest to newest, finals only, extra fields kept")

        segs = [i["segment"] for i in await get_history(srv.uri, 6, 3)]
        assert segs == [7, 8, 9, 10], segs
        print("ok: resume sends only finals after last_segment/last_revision")

        assert await get_history(srv.uri, 10, 3) == []
        print("ok: up-to-date client gets an empty HISTORY")

        assert await get_history(srv.uri, secs=0) == []
        assert len(await get_history(srv.uri, secs=9999)) == 10
        print("ok: history_seconds is clamped (0 gives none, 9999 is clamped to 60)")

        # junk values must not crash the server
        async with websockets.connect(srv.uri) as ws:
            await ws.send(json.dumps({"type": "CLIENT_HELLO", "client_id": 9,
                                      "last_segment": "abc", "history_seconds": None}))
            msg = json.loads(await asyncio.wait_for(ws.recv(), 3))
            assert msg["type"] == "HISTORY"
        async with websockets.connect(srv.uri) as ws:
            await ws.send("not json")
            await asyncio.wait_for(ws.wait_closed(), 3)
        print("ok: junk CLIENT_HELLO values and a non-JSON hello are tolerated")

        # Duplicates, stale revisions and post-final revisions are never forwarded.
        out: list = []
        task = asyncio.create_task(collect(srv.uri, out, last_seg=10, last_rev=3))
        await asyncio.sleep(0.3)
        await send_events(srv.uri, [(11, 1, False), (11, 1, False), (11, 3, False),
                                    (11, 2, False), (11, 4, True), (11, 5, False),
                                    (10, 4, True), (12, 1, False)])
        await task
        live = [x[1:] for x in out if x[0] == "L"]
        assert live == [(11, 1, False), (11, 3, False), (11, 4, True), (12, 1, False)], live
        print("ok: duplicate, stale, post-final and closed-segment updates are dropped")

    with Server() as srv:
        # Segment N+1 may start before segment N is final.
        out = []
        task = asyncio.create_task(collect(srv.uri, out))
        await asyncio.sleep(0.3)
        await send_events(srv.uri, [(1, 1, False), (2, 1, False), (1, 2, True),
                                    (2, 2, False), (3, 1, False), (2, 3, True), (3, 2, True)])
        await task
        finals = [x[1] for x in out if x[0] == "L" and x[3]]
        assert finals == [1, 2, 3], out
        print("ok: overlapping segments keep every final")

    with Server() as srv:
        # A restarted source without SOURCE_DONE must not replay old segments.
        await source(srv.uri, 3)
        out = []
        task = asyncio.create_task(collect(srv.uri, out, last_seg=3, last_rev=3))
        await asyncio.sleep(0.3)
        await source(srv.uri, 3)
        await task
        assert out == [], out
        print("ok: a replay from a reconnected source is ignored")

    with Server() as srv:
        # SOURCE_DONE then a source starting at segment 1 is a new lecture.
        await source(srv.uri, 3, done=True)
        first = await get_history_msg(srv.uri)
        out = []
        task = asyncio.create_task(collect(srv.uri, out, last_seg=3, last_rev=3))
        await asyncio.sleep(0.3)
        await source(srv.uri, 2, done=True)
        await task
        assert out[0] == ("SESSION_RESET", first["lecture"] + 1), out
        assert [x[1:] for x in out[1:] if x[0] == "L" and x[3]] == [(1, 3, True), (2, 3, True)]
        stale = await get_history_msg(srv.uri, 3, 3, lecture=first["lecture"])
        assert stale["lecture"] == first["lecture"] + 1
        assert [i["segment"] for i in stale["items"]] == [1, 2]
        print("ok: after SOURCE_DONE a new lecture resets state and old cursors")

    with Server() as srv:
        # A resuming client gets its whole gap, not only history_seconds.
        await source(srv.uri, 10)
        await asyncio.sleep(1.5)
        assert await get_history(srv.uri, secs=1) == []
        segs = [i["segment"] for i in await get_history(srv.uri, 3, 3, secs=1)]
        assert segs == list(range(4, 11)), segs
        print("ok: resume returns every retained missed final; late join honours history_seconds")

    # Mid-stream join. The client joins when the source reaches segment 40 of
    # 120 (not after a fixed sleep: Windows timers are coarse, ~15 ms, so short
    # sleeps can run far faster than intended and the source may already be done).
    total, join_at = 120, 40
    with Server() as srv:
        reached = asyncio.Event()
        task = asyncio.create_task(source(
            srv.uri, total, gap=0.05,
            on_segment=lambda seg: reached.set() if seg == join_at else None))
        await reached.wait()
        out = []
        await join_midstream(srv.uri, out, total)
        await task
        segs = [s for _, s in out]
        n_hist = sum(1 for k, _ in out if k == "H")
        n_live = len(segs) - n_hist
        assert n_hist > 0 and n_live > 0, (
            f"join was not mid-stream ({n_hist} from HISTORY, {n_live} live)")
        assert len(segs) == len(set(segs)), "duplicate final delivered"
        assert segs == sorted(segs), "finals out of order"
        assert set(range(min(segs), total + 1)) == set(segs), "gap in finals"
        print(f"ok: mid-stream join ({n_hist} from HISTORY, {n_live} live): "
              f"segments {min(segs)}..{total} contiguous, no duplicates")


def unit_tests():
    import solution_server as s

    class Clock:
        now = 1_000_000.0

        def __call__(self):
            return self.now

    def segments(payloads):
        return [json.loads(p)["segment"] for p in payloads]

    # bounded by age: nothing older than the retention window survives
    clock = Clock()
    h = s.FinalHistory(clock=clock)
    for i in range(1, 51):
        h.record(i, 3, update(i, 3, True))
    clock.now += h.retain_seconds + 1
    h.record(51, 3, update(51, 3, True))
    assert segments(h.snapshot(0, 0, 60)) == [51] and len(h) == 1
    print("ok: finals older than the retention window are dropped")

    # bounded by count as well
    h = s.FinalHistory(max_items=5, clock=Clock())
    for i in range(1, 21):
        h.record(i, 3, update(i, 3, True))
    assert len(h) == 5 and segments(h.snapshot(0, 0, 60)) == [16, 17, 18, 19, 20]
    print("ok: history is also bounded by item count")

    # a segment is stored once
    h = s.FinalHistory(clock=Clock())
    h.record(1, 3, update(1, 3, True))
    h.record(1, 3, update(1, 3, True))
    h.record(2, 3, update(2, 3, True))
    assert segments(h.snapshot(0, 0, 60)) == [1, 2]
    print("ok: a repeated final is stored once")

    # cursor, ordering, window, clamp to retention
    clock = Clock()
    h = s.FinalHistory(clock=clock)
    for i in range(1, 11):
        clock.now = 1_000_000.0 - 70 + 7 * i               # ages 63 s .. 0 s
        h.record(i, 3, update(i, 3, True))
    clock.now = 1_000_000.0
    assert segments(h.snapshot(6, 3, 60)) == [7, 8, 9, 10]
    assert segments(h.snapshot(0, 0, 15)) == [8, 9, 10]
    assert segments(h.snapshot(0, 0, 9999)) == list(range(1, 11))
    assert h.snapshot(0, 0, 0) == []
    print("ok: cursor, 15/60 s windows and retention clamp")

    # out-of-order finals are returned sorted
    h = s.FinalHistory(clock=Clock())
    for seg in (2, 1, 3):
        h.record(seg, 2, update(seg, 2, True))
    assert segments(h.snapshot(0, 0, 60)) == [1, 2, 3]
    print("ok: HISTORY items are ordered oldest to newest by segment")

    # revision watermark: newer only, finals are terminal, per segment
    r = s.RevisionState()

    def upd(seg, rev, fin=False, **kw):
        m = {"type": "CAPTION_UPDATE", "session": "L1", "segment": seg,
             "revision": rev, "source_ts": 1.0, "final": fin, "text": "t"}
        m.update(kw)
        return r.accept(m, "L1")
    assert upd(1, 1) and upd(1, 2) and not upd(1, 2) and not upd(1, 1)
    assert upd(1, 3, True) and not upd(1, 4)               # nothing after a final
    assert upd(2, 1) and not upd(1, 5, True)               # a final segment stays closed
    assert upd(3, 1) and upd(2, 2, True)                   # overlapping segments
    assert not upd(4, True) and not upd(4, 1, text=None) and not upd(4, 1, session="L2")
    assert not upd(4, 1, source_ts=float("nan")) and not upd(0, 1) and not upd(4, 0)
    assert upd(4, 1, extra="ignored")
    print("ok: revision state rejects stale, duplicate, post-final and malformed updates")

    r = s.RevisionState(window=10)
    for seg in range(1, 101):
        assert r.accept({"type": "CAPTION_UPDATE", "session": "L1", "segment": seg,
                         "revision": 1, "source_ts": 1.0, "final": True, "text": "t"}, "L1")
    assert len(r.segments) <= 10 and r.floor == 90
    print("ok: revision state is bounded by a segment window")

    # HISTORY messages are valid JSON, chunked only when very large
    msgs = s.history_messages([update(1, 3, True), update(2, 3, True)], "L1", 4)
    body = json.loads(msgs[0])
    assert len(msgs) == 1 and body["lecture"] == 4 and len(body["items"]) == 2
    many = [update(i, 1, True) for i in range(1, s.HISTORY_CHUNK_ITEMS + 2)]
    chunks = [json.loads(m)["items"] for m in s.history_messages(many, "L1", 1)]
    assert [len(c) for c in chunks] == [s.HISTORY_CHUNK_ITEMS, 1]
    print("ok: HISTORY encoding and chunking")

    # timing: a reconnect storm against about 60 s of finals
    clock = Clock()
    h = s.FinalHistory(clock=clock)
    for i in range(1, 81):
        clock.now = 1_000_000.0 - 60 + i * 0.75
        h.record(i, 3, update(i, 3, True))
    n = 5000
    t0 = time.perf_counter()
    for k in range(n):
        s.history_messages(h.snapshot(40 + (k % 10), 3, 60), "L1", 1)
    ms = (time.perf_counter() - t0) * 1000
    print(f"info: {n} HISTORY snapshots took {ms:.0f} ms ({ms / n * 1000:.1f} us each)")


if __name__ == "__main__":
    asyncio.run(protocol_tests())
    unit_tests()
    print("\nALL TESTS PASSED")
