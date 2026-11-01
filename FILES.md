# Package Contents

- `solution_server.py` - revision-aware WebSocket server with bounded history and queues.
- `caption_source.py` - deterministic revision source; exact-segment and long/demo modes.
- `load_clients.py` - scalable clients with role labels, finalized resume state, and history duplicate/stale checks.
- `run_demo.py` - independent public runs + median summary.
- `analyse_results.py` - correctness/freshness/coverage, slow-reader split, and recovery timing.
- `monitor_server.py` - optional Ubuntu RSS memory observation helper.
- `client/` - revision-aware browser with recall and reconnect.
- `network_profiles/` - public `tc/netem` examples.
- `START_HERE.md`, `DEMO_GUIDE.md`, `PUBLIC_TESTS.md`, `PROTOCOL_EXAMPLES.md`, `NETWORKING_PRIMER.md`, `UBUNTU_SETUP.md` - student guides.
- `submission_check.py` - startup/WebSocket compatibility checker; `--final` checks final deliverables.
- `AI_USAGE.md`, `CONTRIBUTIONS.md` - required team-completed declarations; templates are also provided.
- `bonus_asr/` - optional-only bonus guidance.
