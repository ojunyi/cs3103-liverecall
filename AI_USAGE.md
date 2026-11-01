# AI / coding-assistant usage (integration draft)

This declaration covers this integration/testing work only. The team must add the other members' tools and work before submission.

## Tools and tasks

OpenAI Codex agents were used for revision validation, bounded finalized history, per-client queues, semantic backpressure, browser revision/recall/reconnect behavior, and verification/documentation. The integration coordinator reviewed their separate worktrees and tests, reconciled changes in stages, adapted test interfaces, exercised real browsers with the supplied deterministic source, and measured the combined code. Codex also prepared the black-box protocol tests, public-run provenance wrapper and setup/experiment guidance. The supplied measurement programs were preserved. pypdf extracted the assignment PDF for inspection; it is a development-only tool.

## Useful suggestion and verification

Test recovery contents as well as response speed: a fast empty HISTORY does not establish correctness. The baseline tests directly compared recovered finalized segment/revision IDs against known source events. The starter returned empty HISTORY and failed these assertions. The combined implementation passed revision/history/queue/policy and black-box checks during development; the browser recovered twelve missed finals using its real finalized cursor. The team must run and document final measurements for the submitted code.

## Suggestion requiring modification

The first provenance-wrapper version used `git rev-parse HEAD` unconditionally. Linux Git in WSL could not read this Windows-created worktree's absolute `C:/...` Git pointer, so that version stopped before running any experiment. It was changed to record the Git metadata error explicitly and retain content hashes rather than fabricate a commit. Host Git identifies the baseline as `5ccaac8`; final experiments should retain native Ubuntu checkout metadata. The corrected wrapper launched the baseline T4 experiment. This is an actual observed limitation and correction, not a hypothetical rejected suggestion.

A second integration correction replaced generated tests using Python 3.11-only
`asyncio.timeout` with bounded Python 3.10-compatible waits. Revision tests also
needed adapting from historical `store_final`/`broadcast_naively` helpers to the
combined `HISTORY.record`/`broadcast` interface. Copying an entire agent server
would have overwritten another component; the coordinator integrated the relevant
class and handler edits instead.

## Team responsibility

Before submission, all team members must review this declaration and add missing tool use and critical evaluations from their own work. The team remains responsible for submitted code, experimental results and claims, and every member may be asked to explain or modify relevant code during the viva.
