# AI / Coding-Assistant Usage

## Tools used

List the generative-AI/coding tools used by the team and what each was used for.
- **Claude (Anthropic; Claude Code / claude.ai sessions, Claude Sonnet 5.5)**, used for:
  - Designing and drafting the server-side history for requirements 5, 6 and 8 (history half) in `solution_server.py`.
  - Drafting `test_history.py` (protocol tests plus unit tests for the history bounds and cache - basically a testcase).
  - Drafting the client changes in `client/app.js` for requirements 3 and 5.

## One useful suggestion

Describe one suggestion that materially helped the project and how you verified it.

Claude suggested keeping the finalized-caption history in parallel sorted lists and using `bisect` to find the start of the requested window, instead of scanning a list or re-encoding JSON for every client. The history is bounded by age (90 s) and by count (20 000), and ready-to-send HISTORY payloads are cached and keyed by the client's state, so many clients reconnecting together cost one build each.

How we verified it: `python test_history.py` checks finals only, oldest to newest, resume after `(last_segment, last_revision)`, clamping of `history_seconds`, junk-tolerant hello messages, the count and age bounds, the cache bound, and a mid-stream join with no gaps or duplicates. We also ran `run_demo.py` (late join: 100% completeness, no duplicates or stale finals) and checked in the browser that `ws.close()` reconnects and the missed finals appear once each, with no gaps.

## One suggestion that was wrong, inefficient, or required modification

Include a short generated suggestion/code fragment or precise description, explain the problem, and describe your correction.

## Responsibility statement

We understand that the team remains responsible for the submitted code, experimental results, and claims, and that every member may be asked to explain or modify relevant code during the viva.
