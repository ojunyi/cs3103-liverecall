# LiveRecall (CS3103 Assignment 4)

Live captions pushed to many browser clients over WebSockets, with revision replacement, "What did I miss?", reconnect/resume, and overload handling.

```
caption_source.py --WebSocket--> solution_server.py --WebSocket--> browser clients (client/)
```

## 1. Setup

Requires Python 3. Run all commands from the repository root.

### Windows (PowerShell)

```powershell
python -m venv venv                 # create (once)
venv\Scripts\activate               # START the venv (every new terminal)
pip install -r requirements.txt     # once, and again if requirements.txt changes
```

If activation is blocked, run `Set-ExecutionPolicy -Scope Process Bypass` first.

### Ubuntu

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip iproute2
python3 -m venv venv                # create (once)
source venv/bin/activate            # START the venv (every new terminal)
pip install -r requirements.txt
```

### Ending the venv

```
deactivate                          # END the venv (any OS). Closing the terminal also ends it.
```

To remove it completely, delete the `venv/` folder. It is in `.gitignore`, so it is never committed.

Notes:
- Each terminal needs its own activation (server, source, load clients).
- `git add`, `commit`, `push` work with or without the venv active.
- If you add a dependency, add it to `requirements.txt`, not just your local install.

## 2. Run it

Terminal 1, server:
```
python solution_server.py --port 8765
```

Browser: open `client/index.html` (the page connects to `ws://127.0.0.1:8765`).

Terminal 2, caption source:
```
python caption_source.py --duration 120
```

Check the setup: `python submission_check.py`

Public tests (details in `PUBLIC_TESTS.md`):
```
python run_demo.py --clients 20 --duration 20 --runs 3
python run_demo.py --clients 100 --duration 40 --runs 3
python run_demo.py --clients 100 --duration 60 --runs 3 --slow-percent 10 --slow-ms 500
python run_demo.py --clients 100 --duration 60 --runs 3 --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5
```

Do not modify `caption_source.py`, `load_clients.py`, `run_demo.py` or `analyse_results.py`.

## 3. TODO: mandatory requirements (PDF Section 3)

"Where" shows which file the work lives in: **S** = `solution_server.py`, **C** = `client/`.
Assign one owner per row (initials) so two people do not edit the same function at once.

| # | Requirement | What must be true | Where | Owner |
|---|---|---|---|---|
| 1 | Live captions | Show incoming captions continuously; provisional (`final=false`) and final (`final=true`) look different | S, C | |
| 2 | Revision replacement | A newer revision of a segment replaces the older one, never a duplicate line; an older revision never overwrites a newer one | S, C | |
| 3 | What did I miss? | Client keeps at least 60 s of finals locally; button shows 30 s by default, with 15/30/60 s options; uses local data, no server request in normal operation | C | JY |
| 4 | Automatic reconnect | Client reconnects by itself and shows its connection state | C | JY |
| 5 | Incremental recovery | After a short disconnect, only the missed finals come back (using `last_segment`/`last_revision`), not the whole lecture | S, C | JY |
| 6 | Late join | New client (`last_segment=0`) gets recent context via `HISTORY`, then live updates | S, C | JY |
| 7 | No duplicate finals | A final is never shown twice after reconnect or recovery (watch the gap between sending `HISTORY` and registering for live updates) | S, C | JY |
| 8 | Bounded history | Server keeps at least 60 s of finals but never an unbounded transcript or unbounded per-client backlog | S | |
| 9 | Concurrent clients | Many simultaneous clients (public tests use 100; the hidden test is larger); the source handler must not wait on client sends | S | |
| 10 | Slow-client isolation | A slow reader causes no unbounded delay or memory growth for normal clients (per-client bounded queue) | S | |
| 11 | Graceful overload | When a client lags, obsolete provisional revisions may be coalesced or dropped; every final is preserved and the newest revision of the active segment is kept; return to full updates when caught up | S | |


# Team XX - LiveRecall

## Team members

- Name / student ID / responsibility

## Ubuntu setup

Exact commands from a fresh Ubuntu environment.

## Install

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Required server

```bash
python solution_server.py --port 8765
```

## Browser client

Exact commands/steps.

## Public deterministic tests

Exact commands and configuration used for T1-T5, including independent runs.

## Mandatory deterministic application demo

Exact commands/steps for `caption_source.py`, browser clients, revision replacement, recall, late join, and reconnect/recovery.

## Design notes

Briefly describe:

- revision/deduplication state;
- bounded server history;
- reconnect/late-join recovery;
- per-client buffering/backpressure policy;
- overload behavior and trade-offs.

## Optional ASR bonus

Only if attempted. Keep these dependencies separate from the mandatory server path.
