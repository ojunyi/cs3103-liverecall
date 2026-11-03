# LiveRecall

LiveRecall sends revision-based lecture captions from the supplied deterministic
source through a WebSocket server to browser clients. The browser replaces older
revisions, keeps recent finalized captions for local recall, and reconnects with
its latest finalized cursor.

## Ubuntu setup

Run these commands from the repository root on Ubuntu. Use the same Ubuntu VM
for the server, source, and public load clients.

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

## Public deterministic tests

Run at least three independent runs of each prescribed workload. The runner
restarts the server for each run and prints medians. Do not edit the supplied
source, load client, runner, or analyser when reporting results.

```bash
python run_demo.py --clients 20 --duration 20 --runs 3 --output-stem t1
python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t2
python run_demo.py --clients 100 --duration 60 --runs 3 --slow-percent 10 --slow-ms 500 --output-stem t3
python run_demo.py --clients 100 --duration 60 --runs 3 --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5 --output-stem t4
```

For T5, use a full Ubuntu VM with working `tc/netem` and run:

```bash
(
  set -e
  trap 'sudo bash network_profiles/reset.sh lo' EXIT
  sudo bash network_profiles/delay_loss.sh lo
  python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t5
)
```

The trap resets the profile even if the run fails. See `PUBLIC_TESTS.md` for
test interpretation. Keep the raw CSV/JSON results and compare one stress test
against the same earlier/basic configuration.

## Design and limits

The server validates increasing revisions before retaining finals or sending
live updates. It keeps a rolling 60-second final history and sends only finals
newer than the client's finalized cursor. Each client has one sender and a
64-item pending queue; obsolete unsent provisional revisions can be coalesced
under pressure. A lag probe detects a slow receiver even when backlog is hidden
in transport buffers. It holds provisionals for 60 seconds after the latest lag
signal, favoring final delivery but delaying return to full-revision coverage
after a short impairment. A stalled send or irreducibly full queue closes only
that client, which may reconnect for recent finals. Long outages beyond the
history window cannot be recovered by this bounded design.

All server logic is in `solution_server.py`. The required entry point is
`python solution_server.py --port <integer>`.