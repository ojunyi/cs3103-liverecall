#!/usr/bin/env python3
"""Tests for the LiveRecall server history (late join, resume, bounds).

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


async def source(uri, n_segments, gap=0.01, start=1, on_segment=None):
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"type": "SOURCE_HELLO", "session": "L1"}))
        for seg in range(start, start + n_segments):
            if on_segment:
                on_segment(seg)
            for rev in (1, 2, 3):
                await ws.send(json.dumps({
                    "type": "CAPTION_UPDATE", "session": "L1", "segment": seg,
                    "revision": rev, "source_ts": time.time(), "final": rev == 3,
                    "text": f"seg{seg} rev{rev}", "extra_field": "ignored"}))
            await asyncio.sleep(gap)


async def hello(ws, last_seg=0, last_rev=0, secs=30, client_id=1):
    await ws.send(json.dumps({
        "type": "CLIENT_HELLO", "client_id": client_id, "session": "L1",
        "last_segment": last_seg, "last_revision": last_rev, "history_seconds": secs}))


async def get_history(uri, last_seg=0, last_rev=0, secs=30):
    async with websockets.connect(uri) as ws:
        await hello(ws, last_seg, last_rev, secs)
        msg = json.loads(await asyncio.wait_for(ws.recv(), 3))
        assert msg["type"] == "HISTORY", msg
        return msg["items"]


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
        print("ok: late join gets 10 finals, oldest to newest, finals only")

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
        print("ok: junk CLIENT_HELLO values are tolerated")

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
        out: list = []
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

    def final(seg, ts, rev=3):
        return {"type": "CAPTION_UPDATE", "session": "L1", "segment": seg,
                "revision": rev, "source_ts": ts, "final": True, "text": "t"}

    def keys(items):
        return [(i["segment"], i["revision"]) for i in items]

    # bounded by age: nothing older than max_seconds survives
    clock = Clock()
    h = s.FinalizedHistory(clock=clock)
    for i in range(1, 51):
        h.record(final(i, clock.now))
    clock.now += h.max_seconds + 1
    h.record(final(51, clock.now))
    assert keys(h.snapshot("L1", history_seconds=60)) == [(51, 3)]
    assert len(h._items) == 1
    print("ok: finals older than the history window are dropped")

    # a repeated or older final is stored once; a stale one never replaces it
    clock = Clock()
    h = s.FinalizedHistory(clock=clock)
    h.record(final(1, clock.now)); h.record(final(1, clock.now)); h.record(final(1, clock.now, rev=2))
    h.record(final(2, clock.now))
    assert keys(h.snapshot("L1")) == [(1, 3), (2, 3)]
    print("ok: repeated final stored once; an older revision cannot replace it")

    # cursor, ordering, window and clamp
    clock = Clock()
    h = s.FinalizedHistory(clock=clock)
    for i in range(1, 11):
        h.record(final(i, clock.now - 70 + 7 * i))      # ages 63 s .. 0 s
    assert [k for k, _ in keys(h.snapshot("L1", 6, 3, 60))] == [7, 8, 9, 10]
    assert [k for k, _ in keys(h.snapshot("L1", 0, 0, 15))] == [8, 9, 10]
    assert keys(h.snapshot("L1", 0, 0, 9999)) == keys(h.snapshot("L1", 0, 0, 60))
    assert h.snapshot("L1", 0, 0, 0) == [] and h.snapshot("L2") == []
    print("ok: cursor, 15/60 s windows, clamp to 60 s and session filter")

    # revision watermark: newer only, finals are terminal
    r = s.CaptionRevisionState()
    def upd(seg, rev, fin=False, **kw):
        m = {"type": "CAPTION_UPDATE", "session": "L1", "segment": seg,
             "revision": rev, "source_ts": 1.0, "final": fin, "text": "t"}
        m.update(kw)
        return r.accept(m, "L1")
    assert upd(1, 1) and upd(1, 2) and not upd(1, 2) and not upd(1, 1)
    assert upd(1, 3, True) and not upd(1, 4)              # nothing after a final
    assert upd(2, 1) and not upd(1, 5, True)              # no going back
    assert not upd(3, True) and not upd(3, 1, text=None) and not upd(3, 1, session="L2")
    assert upd(3, 1, extra="ignored")
    print("ok: revision watermark rejects stale, duplicate, post-final and malformed updates")

    # timing: a reconnect storm against about 60 s of finals
    clock = Clock()
    h = s.FinalizedHistory(clock=clock)
    for i in range(1, 81):
        h.record(final(i, clock.now - 60 + i * 0.75))
    n = 5000
    t0 = time.perf_counter()
    for k in range(n):
        h.snapshot("L1", 40 + (k % 10), 3, 60)
    ms = (time.perf_counter() - t0) * 1000
    print(f"info: {n} HISTORY snapshots took {ms:.0f} ms ({ms / n * 1000:.1f} us each)")


if __name__ == "__main__":
    asyncio.run(protocol_tests())
    unit_tests()
    print("\nALL TESTS PASSED")