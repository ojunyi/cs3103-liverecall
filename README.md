# Team 16 - LiveRecall

## Team Members

- Ong Jun Yi / A0301890J
- Julian Lim / A0292058Y
- Xia Wenhao / A0252901M

LiveRecall sends revision-based lecture captions from the supplied deterministic
source through a WebSocket server to browser clients. The browser replaces older
revisions, keeps recent finalized captions for local recall, and reconnects with
its latest finalized cursor.

## Ubuntu setup

Tested on Ubuntu 25.04.

1. Clone the repository and enter it:

```bash
git clone https://github.com/ojunyi/cs3103-liverecall.git
cd cs3103-liverecall
```

2. Install the necessary dependencies from the repository root:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip iproute2
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python submission_check.py
```

The mandatory server has no cloud, database, or ASR dependency. Python 3.10+
and the WebSocket package in `requirements.txt` are sufficient. Activate the
virtual environment in each new terminal.

## Demo video

[Watch the demo video](./Demo.mp4) (about 3 min). It shows: provisional-to-final
replacement, the 15/30/60 s recall options, a client reconnecting after a simulated
network drop, a late-joining browser, and a slow client falling behind while the
others stay fresh.

If the video doesn't load on GitHub, view it on Google Drive instead:
[Video](https://drive.google.com/file/d/1N4YQciID9JAd9pTKbo161CQx-QpiSXRH/view?usp=sharing)

## Deterministic browser demo

In three terminals, run:

```bash
python solution_server.py --port 8765
python -m http.server 8000 --directory client
python caption_source.py --duration 120
```

Open `http://127.0.0.1:8000` in two browser windows before starting the
source. Observe provisional text being replaced by a final, use the default
30-second recall and 15/60-second options, disconnect one client briefly and
watch it reconnect, then open a late-joining browser. Recall uses local final
captions; reconnect/late join uses the server's recent `HISTORY` response.

Restart `solution_server.py` before running `caption_source.py` again. The server
keeps its revision state for the whole lecture and the source restarts at segment
1, so a second source run against the same server is rejected and clients receive
no captions.

## Public deterministic tests

You may run the following tests. Do not edit the supplied
source, load client, runner, or analyser when reporting results.

```bash
## T1 - Basic Correctness
python run_demo.py --clients 20 --duration 60 --runs 3

## T2 - Client Scaling
python run_demo.py --clients 100 --duration 60 --runs 3

## T3 - Slow Clients
python run_demo.py --clients 100 --duration 60 --runs 3 --slow-percent 10 --slow-ms 500

## T4 - Disconnect Reconnect
python run_demo.py --clients 100 --duration 60 --runs 3 --disconnect-percent 20 --disconnect-at 15

```

For T5 and T6, use a full Ubuntu VM with working `tc/netem` and run:

```bash
## T5 - Network Impairment
sudo bash network_profiles/delay_loss.sh lo
python run_demo.py --clients 100 --duration 60 --runs 3
sudo bash network_profiles/reset.sh lo

## T6 - T3 (Slow Clients) + T5 (Network Impairment)
sudo bash network_profiles/delay_loss.sh lo
python run_demo.py --clients 100 --duration 60 --runs 3 --slow-percent 20 --slow-ms 500
sudo bash network_profiles/reset.sh lo
```

## Design notes

**Revision and deduplication state.** The server checks every update against a
single O(1) watermark (session, segment, revision, final) before storing or
forwarding it. The watermark is separate from history and survives source
reconnects, so a finalized segment can never reopen. The browser applies the same
ordering per segment, so duplicate or stale revisions are ignored.

**Bounded server history.** The server keeps finals only, one per segment, and
prunes anything older than 60 seconds by `source_ts` on every read and write.
Memory grows with the caption rate inside the window, not with lecture length.

**Reconnect and late-join recovery.** Clients send their finalized cursor in
`CLIENT_HELLO` and receive finals strictly newer than it as `HISTORY`, within the
requested window (at most 60 seconds). A late joiner has a zero cursor, so it gets
the whole window. Snapshot and client registration happen with no `await` between
them, so no final falls between `HISTORY` and the live stream.

**Per-client buffering and backpressure.** Broadcast only enqueues, so a slow client
cannot delay the others. Each client has one sender task and a 64-message queue.
When the queue is full, obsolete provisionals are coalesced; finals and `HISTORY`
are kept. Pings every 0.5 seconds detect backlog hidden in TCP buffers, measured
against each client's best round trip so steady network delay is not mistaken for
a slow reader.

**Overload behavior and trade-offs.** A lagging client gets finals only for 60
seconds after its latest lag signal, then the newest held provisionals and full
updates resume. A client is evicted (close code 1013) if a send or lag probe stalls
for 10 seconds, or its queue stays full after coalescing; it reconnects and recovers
its missed finals from `HISTORY`. This favors final completeness over provisional
freshness: a briefly slow client stays on finals only for up to 60 seconds, and
finals from an outage longer than 60 seconds cannot be recovered.
