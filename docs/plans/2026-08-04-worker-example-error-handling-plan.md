# Worker Example Error Handling Plan

Date: 2026-08-04
Status: completed
Class: 4 — the fix changes public executable guidance for queued/background
work and crosses the shell worker, CLI exit-code, test, and documentation
boundaries. No CLI shape, product spec, storage format, or library API changes.
Plan type: implementation against existing specs

## Goal

Make the published single-consumer worker examples truthful under processing,
acknowledgement, and broker failures. A failed business handler must stop before
the worker advances to newer messages; a failed delete must be fatal and report
the duplicate-side-effect risk; and broker exit code `2` must remain distinct
from operational failures.

## Source Documents

- `docs/program-theory.md` [THEORY-2], [THEORY-3], [REV-THEORY-003]
- `docs/specs/10-cli.md` [SB-CLI-1], [SB-CLI-2], [SB-CLI-4]
- `docs/specs/11-delivery.md` [SB-DELIVERY-1], [SB-DELIVERY-2],
  [SB-DELIVERY-4], [SB-DELIVERY-8]
- User bug report in the 2026-08-04 session: four enumerated defects in
  `examples/safe_worker.sh` and `examples/resilient_worker.sh`
- Prior investigation learning `watch_stdout_checkpoint_precedes_shell_handler`

## Spec Baseline

- `e31f7f68795004818700341619adc0e2298b1cc5` —
  `docs/specs/10-cli.md` and `docs/specs/11-delivery.md` at plan authoring.
- No spec delta is proposed. The scripts currently violate or obscure the
  existing three-exit-code and delivery/application-completion boundaries.

## Context and Key Files

Files to modify:

- `examples/safe_worker.sh`: currently pipes streaming peek-watch output into a
  shell loop. The watch process considers a JSON line delivered after stdout
  flush, so later shell processing cannot reject that delivery. The script also
  has no self-contained handler contract and ignores delete failure.
- `examples/resilient_worker.sh`: already peeks one message at a time and stops
  on processing or delete failure, but `|| true` and stderr suppression collapse
  every peek error into empty-queue polling.
- `tests/test_worker_examples.py`: new black-box subprocess coverage using fake
  external commands at the process boundary. Tests execute the shipped scripts.
- `examples/README.md` and root `README.md`: align the public descriptions with
  one-message polling, stop-on-failure, and explicit handler configuration.
- `docs/specs/10-cli.md` and `docs/specs/11-delivery.md`: add non-normative
  Related Plans backlinks only; the existing normative text does not change.
- `CHANGELOG.md`: record the user-visible correction under Unreleased.
- This plan and `docs/plans/README.md`: traceability, review, and execution
  evidence.

Files to read first:

- `simplebroker/commands.py`: `cmd_peek` returns `2` for no match and `1` for an
  operational/input error.
- `simplebroker/watcher.py`: peek-watch progress advances after its in-process
  handler returns; for CLI watch that handler is the stdout write/flush, not the
  downstream shell business handler.
- `tests/test_resilient_worker.py`: covers the abstract checkpoint pattern but
  does not execute either shell example.
- `docs/agent-context/runbooks/hardening-plans.md` and
  `docs/agent-context/runbooks/adversarial-acceptance-probes.md`.

Comprehension gates before editing:

1. Can a downstream shell process make the already-flushed CLI watch handler
   return failure? No. The processes have no acknowledgement channel.
2. Which peek result is ordinary idle state? Exit `2` only. Exit `0` requires a
   JSON message; exit `1` is operational/input failure and must remain visible.

## Invariants and Constraints

- Queue delivery state remains distinct from successful application work.
- `safe_worker.sh` processes at most one observed message before deciding
  whether to delete, stop, or poll again. It must not use streaming watch for a
  business acknowledgement loop.
- Both scripts treat peek `0` as data, `2` as empty, and every other result as
  fatal. Broker stderr remains visible.
- Both scripts stop after processing failure. The pending message is retried by
  a later run, not by the current process.
- Both scripts stop after delete failure and explain that completed external
  side effects may repeat on a later run.
- Delete is by exact message ID and occurs only after handler success.
- Peek is not a reservation. The examples remain single-consumer patterns; the
  move-to-inflight guidance remains the concurrent-worker path.
- Message payloads are passed as one quoted command argument. No `eval`, shell
  word splitting, or interpolation of message contents is introduced. Trailing
  newlines are preserved. NUL is rejected before handler invocation or delete
  because Bash variables cannot represent it.
- `safe_worker.sh` requires `PROCESS_TASK` to contain exactly one executable
  path or command name, with no embedded arguments. It validates that command
  before touching the queue and invokes it only as
  `"$PROCESS_TASK" "$message"`. It must not silently delete messages through a
  success-returning placeholder or use `eval`/word splitting.
- Existing CLI/library behavior, watch internals, storage, and checkpoint file
  format do not change.
- Checkpoints are exactly the initial sentinel `0` or one 19-digit broker
  message ID. All broker, parse, handler, acknowledgement, checkpoint-read,
  checkpoint-validation, and checkpoint-write failures are fatal. Empty queue
  is the only ordinary nonzero broker result. Status output is diagnostic;
  there are no best-effort failures in the acknowledgement path.

Hidden coupling: the shell scripts consume the CLI's documented process exit
codes and JSON object shape. A zero-status command with empty or invalid JSON is
a protocol failure, not an empty queue.

Stop and re-plan if the fix requires changing CLI exit codes, watcher state,
the checkpoint format, adding a dependency, or creating a second worker
execution path.

## Rollback, Rollout, and One-Way Doors

Rollback is a file-level revert of the two examples, their black-box tests, and
the aligned docs/changelog. There is no data migration or mixed-version runtime
coupling. The corrected examples ship together with their descriptions.

There are no one-way doors. Users who copied an older script retain its defect,
so the changelog and README must make the stop-on-failure behavior observable.
Post-release success is a worker exiting nonzero with broker diagnostics on
operational failure, and no report of operational failure as "No new messages."

## Dependency-Ordered Tasks

### 1. Lock public failures with black-box tests

- Execute the actual shell scripts with temporary fake `broker`, handler, and
  sleep commands on `PATH`.
- First prove the tests fail against the current scripts.
- Cover every enumerated branch: missing handler, processing failure, delete
  failure, upstream peek failure, exit-2 empty polling, zero-status empty output,
  resilient-worker operational peek failure, and corrupt or unreadable
  checkpoint input, including numeric-but-invalid `123`.
- Fire the payload boundary with a multi-trailing-newline message and a NUL
  message on both scripts. Record complete broker argv so exact-ID delete is an
  assertion rather than an inference.
- Make exit-2 polling deterministic: a stateful fake broker returns `2` once,
  then `1`; a recording no-op `sleep` prevents delay. Assert one idle report
  followed by visible operational failure and nonzero exit.
- Stop if tests need to source or rewrite production scripts; the process
  boundary is the contract.

### 2. Repair `safe_worker.sh`

- Enable strict shell mode including `pipefail`.
- Require `PROCESS_TASK` as one executable path/name with no embedded
  arguments, validate it, and poll one JSON message per `broker peek` call.
- Branch on the captured peek status without `|| true`; validate JSON fields;
  quote payload and ID; stop on handler or delete failure.
- Stop if any branch can proceed to a newer message after a handler or delete
  failure.

### 3. Repair `resilient_worker.sh`

- Capture peek output and status separately while preserving stderr.
- Treat only status `2` as idle, and reject successful empty/invalid output.
- Reject unreadable checkpoint input and anything other than `0` or exactly 19
  decimal digits before calling the broker. This prevents a truncated numeric
  checkpoint from being reinterpreted as another timestamp unit by `--after`.
- Preserve its existing checkpoint ordering: update only after delete success.
- Stop if error handling changes the checkpoint format or makes delete failure
  recover in-process.

### 4. Align public guidance

- Update both README surfaces and Unreleased changelog text.
- Add non-normative Related Plans backlinks to both governing specs without
  changing their requirement text.
- Keep the single-consumer limitation and move-to-inflight alternative clear.
- Do not revise normative specs unless implementation reveals a real contract
  gap; that would escalate to a separate class-5 spec slice.

### 5. Verify and review

- Run targeted tests after each red-green slice, then shell syntax, full pytest,
  Ruff, mypy, DOM fixtures, and traceability checks from current state.
- Obtain independent plan review before implementation and independent
  completed-work review after all gates.

## Testing and Verification

The black-box tests must not mock Bash, JSON parsing, process exit propagation,
or the shipped script. Temporary executable collaborators may simulate the
external `broker`, business handler, and `sleep`; their calls and statuses are
recorded for assertions.

Commands:

```bash
bash -n examples/safe_worker.sh examples/resilient_worker.sh
uv run pytest tests/test_worker_examples.py -q
uv run pytest
uv run ruff check .
uv run mypy .
python3 bin/check-dom15-fixtures
python3 bin/check-doc-links
```

If a named repository command is absent, record that fact and use the closest
owning gate listed by `bin/` or project configuration. Do not silently omit it.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| [SB-DELIVERY-8] | Pass message payload as one quoted handler argument. | Preserve all representable bytes including trailing newlines; reject NUL before handler/delete. | Bash variables cannot store NUL, so silent truncation would acknowledge the wrong payload. | N/A — documented executable-example limitation; broker contract unchanged. |

## Independent Review Loop

| Date | Stage | Reviewer result | Disposition |
|------|-------|-----------------|-------------|
| 2026-08-04 | Independent plan review | F1 high: checkpoint-read failures were declared fatal but absent from tasks/tests. F2 medium: handler interface was underspecified. F3 medium: governing-spec backlinks were omitted. F4 medium: exit-2 polling test had no deterministic termination. | Accepted all four: added checkpoint read/validation probes, exact `PROCESS_TASK` contract, non-normative backlinks, and a sequenced `2` then `1` fake-broker test. Re-review required before implementation. |
| 2026-08-04 | Independent plan re-review | F1 remained partly open: generic integer validation would accept a truncated numeric checkpoint and let `--after` reinterpret its unit. | Accepted: checkpoint format is now exactly `0` or 19 decimal digits, with numeric-invalid `123` as a firing probe. Final re-review required. |
| 2026-08-04 | Independent final plan re-review | PASS. | Plan approved for implementation. |
| 2026-08-04 | Independent completion review | F1 P1: resilient delete hid broker stderr. F2 P1: command substitution altered trailing-newline/NUL payloads. F3 P2: tests did not fire resilient ack/checkpoint-write or exact argv. | Accepted all: preserve delete stderr; sentinel-preserve trailing newlines and reject NUL pre-handler/delete; add deterministic handler override, payload probes, full broker argv, resilient delete failure, and checkpoint-write failure tests. Re-review required. |
| 2026-08-04 | Independent completion re-review | PASS; reviewer independently reran all 22 script-level tests. | F1–F3 closed. |

The plan reviewer receives the governing specs, this plan, and both current
scripts. The completion reviewer receives the final diff plus exact gate
output. Every finding is fixed or answered in this table before closure.

## Out of Scope

- Changing `broker watch`, `QueueWatcher`, CLI exit codes, or JSON shapes.
- Providing concurrent-worker reservation beyond the existing move-to-inflight
  pointer.
- Designing automatic backoff, dead-letter policy, or exactly-once external
  side effects.
- Refactoring unrelated example scripts or the existing abstract checkpoint
  tests.

## Fresh-Eyes Review

- [x] Every bug-report branch has a firing black-box test.
- [x] No current worker can advance after handler or acknowledgement failure.
- [x] Only exit `2` is described or treated as empty queue.
- [x] `safe_worker.sh` has an explicit, safe handler interface.
- [x] Public descriptions match the executable examples.
- [x] No existing user worktree change was overwritten.

## Execution Evidence

- Red baseline: `uv run pytest tests/test_worker_examples.py -q -n 0`
  failed all initial 13 cases against the original scripts, reproducing
  successful pipeline exits and error-to-empty conversion.
- Targeted final: `uv run pytest tests/test_worker_examples.py
  tests/test_resilient_worker.py -q -n 0` passed 27 tests.
- Full suite: `uv run pytest` passed 2,429 tests with 17 platform/opt-in
  skips in 49.72 seconds.
- Static and shell gates: `uv run ruff check .`, `uv run mypy .`, `bash -n
  examples/safe_worker.sh examples/resilient_worker.sh`, and `shellcheck
  examples/safe_worker.sh examples/resilient_worker.sh` all passed. Mypy
  checked 85 source files.
- Repository gates: `python3 bin/check-dom15-fixtures`,
  `bin/check-doc-paths`, and `git diff --check` all passed.
- Independent plan review passed after checkpoint-format and deterministic-test
  amendments. Independent completion review found three issues; all were fixed,
  and completion re-review passed with an independent 22-test rerun.
- Landing scope was restricted to the worker-plan file/index row, two scripts,
  worker tests, two README surfaces, two spec backlinks, and changelog entry.
