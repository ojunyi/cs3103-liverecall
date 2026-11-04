# AI / Coding-Assistant Usage

## Tools used

- **Claude (Anthropic; Claude Code / claude.ai sessions, Claude Sonnet 5.5 and Claude Opus 5.5)**, used for:
  - Designing and drafting an early server-side history for requirements 5 and 6 (history half) and client changes for requirements 3 and 5. A later rewrite replaced this code, so it is not in the submitted `solution_server.py` or `client/app.js`.
  - Comparing the team's branches, diagnosing the lag probe under network delay and drafting its fix, and running experiments on slow-mode hold policies.

- **OpenAI Codex**, used for:
  - Drafting revision validation, bounded finalized history, per-client queues, semantic backpressure, and browser revision, recall, and reconnect behavior.
  - Reviewing separate agent worktrees, integrating their changes, adapting test interfaces, and running protocol tests, browser checks, and measurements during development.
  - Preparing black-box protocol tests, a public-run provenance wrapper, and setup and experiment guidance. The tests and wrapper were for development only and are not in the submission. The supplied measurement programs were preserved.

## One useful suggestion

Codex suggested separating revision validation from final-caption history. `CaptionRevisionState` keeps a small, lasting watermark to reject stale updates, while `FinalizedHistory` keeps recent finals for replay. Expiring a caption from recovery history therefore does not make an old revision acceptable again. We verified the separation by testing it in the Ubuntu environment.

## One suggestion that was wrong, inefficient, or required modification

The original Codex lag probe used the same 0.25-second pong timeout for every client. This could classify a client with steady network delay as slow even when it was keeping up. We thus modified the timeout to be a client's best measured RTT plus a margin (`LAG_THRESHOLD_SECONDS`, later raised to 1 second from 0.25), using an initial estimate until an RTT is available. We also tried a shorter 10-second slow-mode hold that doubled after repeated lag, but the recorded T3 slow-client fresh ratio fell from about 90% to about 40% as socket backlog repeatedly rebuilt. This was measured before the threshold was raised to 1 second. We kept `LAG_HOLD_SECONDS` at 60 seconds.

## Responsibility statement

We understand that the team remains responsible for the submitted code, experimental results, and claims, and that every member may be asked to explain or modify relevant code during the viva.
