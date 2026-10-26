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
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return self
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("server did not start")

    def __exit__(self, *exc):
        self.proc.terminate()
        self.proc.wait(timeout=5)


async def source(uri, n_segments, gap=0.01, start=1):
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"type": "SOURCE_HELLO", "session": "L1"}))
        for seg in range(start, start + n_segments):
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


async def join_midstream(uri, out):
    async with websockets.connect(uri) as ws:
        await hello(ws, secs=60, client_id=3)
        end = time.time() + 4
        while time.time() < end:
            try:
                m = json.loads(await asyncio.wait_for(ws.recv(), 0.4))
            except asyncio.TimeoutError:
                continue
            if m["type"] == "HISTORY":
                out.extend(("H", i["segment"]) for i in m["items"])
            elif m.get("final"):
                out.append(("L", m["segment"]))


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

    with Server() as srv:
        task = asyncio.create_task(source(srv.uri, 300))
        await asyncio.sleep(1.0)                       # join while the source is running
        out: list = []
        await join_midstream(srv.uri, out)
        await task
        segs = [s for _, s in out]
        n_hist = sum(1 for k, _ in out if k == "H")
        assert len(segs) == len(set(segs)), "duplicate final delivered"
        assert segs == sorted(segs), "finals out of order"
        assert set(range(min(segs), 301)) == set(segs), "gap in finals"
        print(f"ok: mid-stream join ({n_hist} from HISTORY, {len(segs) - n_hist} live): "
              f"segments {min(segs)}..300 contiguous, no duplicates")


def unit_tests():
    import solution_server as s

    def reset():
        s._h_times.clear(); s._h_keys.clear(); s._h_raw.clear()
        s._h_segments.clear(); s._h_cache.clear()

    def final(seg, rev=3):
        msg = {"type": "CAPTION_UPDATE", "segment": seg, "revision": rev,
               "source_ts": 0.0, "final": True, "text": "t"}
        s.store_final(msg, json.dumps(msg))

    def items_of(payload):
        m = json.loads(payload)
        assert m["type"] == "HISTORY"
        return [(i["segment"], i["revision"]) for i in m["items"]]

    # bounded by count
    reset()
    for i in range(1, s.HISTORY_MAX_ITEMS + 5001):
        final(i)
    assert len(s._h_times) == s.HISTORY_MAX_ITEMS == len(s._h_segments)
    assert s._h_keys[-1][0] == s.HISTORY_MAX_ITEMS + 5000
    print(f"ok: history capped at {s.HISTORY_MAX_ITEMS} entries")

    # bounded by age
    reset()
    for i in range(1, 51):
        final(i)
    s._h_times[:] = [t - 1000 for t in s._h_times]       # pretend they are 1000 s old
    s.prune_history(time.monotonic())
    assert not s._h_times and not s._h_segments
    print("ok: entries older than HISTORY_KEEP_SECONDS are dropped")

    # a repeated final is stored once; cache follows changes
    reset()
    final(1); final(1); final(2)
    assert s._h_keys == [(1, 3), (2, 3)]
    first = s.build_history({"last_segment": 0, "last_revision": 0, "history_seconds": 30})
    assert items_of(first) == [(1, 3), (2, 3)]
    assert s.build_history({"last_segment": 0, "last_revision": 0, "history_seconds": 30}) is first
    final(3)                                              # cache must not serve stale data
    again = s.build_history({"last_segment": 0, "last_revision": 0, "history_seconds": 30})
    assert items_of(again) == [(1, 3), (2, 3), (3, 3)]
    other = s.build_history({"last_segment": 2, "last_revision": 3, "history_seconds": 30})
    assert items_of(other) == [(3, 3)]
    print("ok: repeated final stored once; cache is invalidated and keyed per client state")

    # cache cannot grow without bound
    reset()
    for i in range(1, 11):
        final(i)
    for k in range(s.HISTORY_CACHE_MAX * 3):
        s.build_history({"last_segment": k % 7, "last_revision": k, "history_seconds": 30})
    assert len(s._h_cache) <= s.HISTORY_CACHE_MAX
    print("ok: payload cache is bounded")

    # timing: a reconnect storm
    reset()
    for i in range(1, 81):                                # about 60 s of finals
        final(i)
    n = 5000
    t0 = time.perf_counter()
    for k in range(n):
        s.build_history({"last_segment": 40 + (k % 10), "last_revision": 3, "history_seconds": 60})
    ms = (time.perf_counter() - t0) * 1000
    print(f"info: {n} HISTORY builds took {ms:.0f} ms ({ms / n * 1000:.1f} us each)")


if __name__ == "__main__":
    asyncio.run(protocol_tests())
    unit_tests()
    print("\nALL TESTS PASSED")
