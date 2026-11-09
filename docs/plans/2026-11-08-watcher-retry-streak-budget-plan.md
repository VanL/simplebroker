# Watcher retry limits per failure streak

Class: 4 — risky: the [DOM-5] "compatibility surface" trigger fires (review
F1). Public `run_forever()` and `run()` callers, and `threading.excepthook` for
`run_in_thread()`, no longer receive the exceptions a healthy long-lived
watcher used to raise (`TimeoutError` after 300 s of uptime; the third failure
over the run's life), and every retry now emits a WARNING log record, which
Python's last-resort handler prints to stderr when the application has not
configured logging (for example `broker watch`). Private implementation names
and silent spec text do not exclude a compatibility change. It also changes
intended (observable) watcher behavior with no governing spec text, contract
test or explicit user requirement as intent evidence. No other risky trigger
fires: no async or deferred work; one execution context; no CLI shape or
storage change; rollback is a plain revert with no rollout order. No normative
spec text changes, so not class 5 (see Q2 for the class-5 follow-up).
Hardening content: Hidden couplings; Rollback, Rollout and Success Signals;
Testing Plan (what not to mock); Context and Key Files; Fresh-Eyes
comprehension questions.
Status: completed — revision 2 implemented and verified on branch
`perf/sb-watcher-retry-budget` (base `perf/sb-base` = `5d29fbf`). Revision 1
(`2a732d8`) passed cross-family review SB-7 with no blocker (F1, F2 fixed in
`9473af3`). The owner decided Q1 on 2026-11-09; revision 2 implements that
decision and passed Tier-1 Codex review of `9473af3..79fa179` on
2026-11-09 (see Revision 2 verification). Q2 is deferred; integration gates passed.
Owner: SimpleBroker product owner; implementation agent for this track.
Boundary: `simplebroker/watcher.py` `BaseWatcher._run_with_retries`,
`_record_healthy_wait`, `_check_retry_timeout`, and the two
`_process_messages` implementations (`BaseWatcher`, `QueueMoveWatcher`), plus
the `MAX_TOTAL_RETRY_TIME` docstring in `simplebroker/_constants.py`.
`PollingStrategy`, activity waiters, backends, `_handle_retry` backoff and
`_process_with_retry` are out of scope.
Verification: shared watcher tests on SQLite and PostgreSQL that fail on
`5d29fbf` (revision 1) and on `9473af3` (revision 2) for the intended reasons;
two `SM-WATCHER-LIFECYCLE` transition rows; owning watcher test files; shared
watcher suite on PostgreSQL; ruff, format, mypy; fix-reverted sensitivity and
mutation controls.
Required action: complete integration verification and landing under
`2026-11-09-performance-branches-integration-plan.md`; review is complete
and Q2 is deferred outside this landing.

## Goal

- [x] A watcher whose run is older than `MAX_TOTAL_RETRY_TIME` (300 s)
      survives an ordinary failure that its retry recovers from, and handles a
      later write.
- [x] Failures separated by successful recoveries do not accumulate toward
      `max_retries`: three such failures do not end the run.
- [x] Within one failure streak the limits are unchanged: `max_retries` (3)
      consecutive failures, or 300 s, end the run; the first streak is still
      measured from run start.
- [x] Revision 2 (owner decision 2026-11-09): only a processing wait that
      returns normally after a restart ends a streak. A failure that recurs at
      the first wait after every restart ends the run after three failures,
      with the exception surfaced.
- [x] Revision 2: every retry logs a WARNING naming the error, the retry
      number and limit, and the streak age.
- [x] No new knob, path or public signature.

## Source Documents

Read in the `docs/agent-context/context.index.yaml` order:
`docs/program-theory.md`, `docs/agent-context/README.md`,
`docs/agent-context/decision-hierarchy.md`, `docs/agent-context/principles.md`,
`docs/agent-context/engineering-principles.md`,
`docs/agent-context/runbooks/writing-plans.md` (required sections),
`docs/agent-context/lessons.md`, `docs/lessons.md` (Golden Rules and the dated
entries after the watermark), and `skills/test-audit/SKILL.md` (authoring
gate).

- Theory: `docs/program-theory.md` [THEORY-3] (watcher/waiter: "adapter from
  queue activity to bounded waiting or consumption"), [THEORY-4] (explicit
  safety over magical recovery), [THEORY-5] and [REV-THEORY-002] (the backend
  owns its service topology and recovery), [THEORY-6] (failures must stay
  diagnosable).
- Contracts: `docs/specs/16-python-library-api.md` [SB-API-6] (watcher
  surface and lifecycle; says nothing about retry limits);
  `docs/specs/11-delivery.md` [SB-DELIVERY-2] (watch modes; no retry text).
  Searched `README.md`, `docs/guides/python.md`, `docs/agent-kernel.md`: no
  watcher retry contract.
- Code-level intent (the only statement of intent found):
  `simplebroker/_constants.py` `MAX_TOTAL_RETRY_TIME` docstring "Maximum time
  in seconds to retry watcher initialization before giving up" (since
  `4be9046`, 2025-08-05; the inline constant dates from `671f410`, 2025-07-29,
  a bulk refactor with no stated rationale), and the
  `_check_retry_timeout` argument doc "start_time: Time when retries started".
  Both describe a budget for retrying, not for the run.
- Implementation rationale:
  `docs/implementation/07-complexity-and-state-machine-map.md`
  (`SM-WATCHER-LIFECYCLE`, "retry state").
- Finding: Q1 of the SB-6 listener-recovery plan
  (`2026-11-08-pg-listener-failure-recovery-plan.md`, branch
  `perf/sb-listener-recovery`, not on this branch), with its probe in the
  coordinator scratchpad. Not reproduced here with that probe because it needs
  the SB-6 listener fix; the tests below inject the failure through seams that
  exist on `5d29fbf`.

Source spec: None — `[SB-API-6]` does not specify watcher retry limits; this
fix aligns the code with its own documented intent.

## Spec Baseline

- `5d29fbf0786bc59116d80fa6fc2319ba97ae9dce` for every file above. Plan type:
  implementation; no spec revision. `docs/specs/16-python-library-api.md`
  gains only a non-normative Related Plans backlink.

## Context and Key Files (as of `5d29fbf`)

- `simplebroker/watcher.py`
  - `BaseWatcher._run_with_retries` (lines 941–990): `retry_count = 0` and
    `start_time = _monotonic()` once per run. Each loop pass calls
    `_check_retry_timeout(start_time, retry_count)`, then `_start_strategy()`,
    the initial `_drain_queue()` and `_process_messages()`. An ordinary
    exception increments `retry_count`, calls `_handle_retry` (sleep
    `2**retry_count` s, thread-local cleanup) and loops; at `max_retries` (3)
    it re-raises.
  - `BaseWatcher._check_retry_timeout` (lines 847–863): raises `TimeoutError`
    when `_monotonic() - start_time > MAX_TOTAL_RETRY_TIME`.
  - `_monotonic` (line 94): the module-owned clock seam tests patch.
  - `interruptible_sleep` (imported at line 86): the backoff sleep tests patch.
- `simplebroker/_constants.py` line 211: `MAX_TOTAL_RETRY_TIME = 300`.
- Existing proofs: `tests/test_watcher_edge_cases.py::test_absolute_timeout_exceeded`
  (time passes inside the first failing attempt),
  `::test_watcher_max_retries_exceeded` (persistent initial-drain failure);
  `tests/test_watcher_transition_tables.py` rows `RETRY_THEN_RUN` and
  `TERMINAL_ERROR`. All exercise only the first streak, so none can tell a
  whole-run budget from a per-streak one.

## Root cause

The retry state (`retry_count`, `start_time`) is scoped to the whole run, not
to the failures it is meant to bound. Nothing resets it when the watcher
recovers, so:

- a watcher whose run is older than 300 s raises `TimeoutError` at the top of
  its first retry, before it tries to reconnect;
- any watcher dies on its third failure over its life, even when it recovered
  from the first two.

Base evidence (new tests, fix absent) is in Verification below.

## Design

Revision 2 (owner decision 2026-11-09) replaces revision 1's health witness.

A failure streak is the sequence of consecutive failed attempts since the
watcher was last healthy. The watcher is healthy once, after its latest
(re)start, a processing wait (`wait_for_activity()` in `_process_messages`)
returns normally, through activity or timeout. A successful strategy start and
initial drain are not enough: a restart whose first wait fails stays in the
open streak.

- `_run_with_retries` clears `self._healthy_wait_since_start` before each
  attempt.
- Both `_process_messages` implementations (`BaseWatcher`, `QueueMoveWatcher`)
  call `_record_healthy_wait()` right after every wait that returns, which sets
  the flag. It is a single setter, so the coupling has one name.
- On an ordinary failure, `_run_with_retries` checks the flag. If it is set,
  the streak is over: `retry_count = 0` and `streak_start = _monotonic()`, so
  the new streak is timed from this failure. Then the count increments as
  before.
- The first streak opens at run start, as before, so time spent in a failing
  first attempt still counts (`test_absolute_timeout_exceeded` unchanged).
  Uniform reading: the budget measures time since the watcher was last
  healthy, or since it began trying to become healthy.
- Each failure that will be retried (`retry_count < max_retries`) logs one
  WARNING from `_run_with_retries`, the streak owner:
  `Watcher error (retry n/3, failure streak age X.Xs): <Type>: <message>`.
  The terminal failure keeps `_handle_retry`'s existing ERROR (`Watcher failed
  after 3 retries`); `_handle_retry` keeps its DEBUG backoff line and its
  signature. Like that ERROR, the WARNING is not gated by
  `BROKER_LOGGING_ENABLED`.
- `_check_retry_timeout` is unchanged apart from its docstring; it receives the
  streak start.

Why the clock restarts at the failure, not at the healthy wait: a healthy
processing loop runs for hours, so a clock restarted at the witness would find
`elapsed > 300` at the next failure and die, reproducing the bug.

Revision 1 used a successful start plus initial drain as the witness. That left
a failure that recurs at the first wait after every restart in an indefinite
2 s restart loop (register row 5, class D in revision 1). The owner chose the
processing-wait witness (the reviewer's alternative), which bounds that loop
with the existing limits.

## Invariants and Constraints

1. Per streak: at most `max_retries` failures and `MAX_TOTAL_RETRY_TIME`
   seconds; the backoff schedule `2**retry_count` and its stop-interruptible
   sleep are unchanged.
2. The first streak is measured from run start.
3. Terminal paths are unchanged: `_ErrorHandlerFailure`,
   `_IteratorCleanupFailure` and `KeyboardInterrupt` bypass retry; stop exits
   cleanly; exhaustion re-raises the original exception.
4. `_handle_retry`, `_check_retry_timeout` and `_run_with_retries` keep their
   signatures (subclasses and tests override them).
5. No new configuration, constant or public name.
6. A streak ends only at `_record_healthy_wait()`; a subclass that overrides
   `_process_messages` without calling it keeps every failure of the run in
   one streak (the `5d29fbf` behavior), never a looser one.

## Hidden couplings

- The witness crosses methods: `_run_with_retries` clears
  `_healthy_wait_since_start`, the two `_process_messages` implementations set
  it through `_record_healthy_wait()`, and the failure branch reads it. Comments
  at each site name the other. No in-repository or Weft subclass overrides
  `_process_messages` (checked by grep; Weft's `MultiQueueWatcher` inherits
  `BaseWatcher._process_messages`).
- `_handle_retry(e, retry_count, max_retries)` derives the backoff from the
  count, so a new streak also restarts the backoff at 2 s.
- SB-6 (`perf/sb-listener-recovery`) depends on this loop to recover a
  PostgreSQL watcher after the server ends the LISTEN session; its CHANGELOG
  entry states the old limits ("at most two recoveries per run, and none once
  the run is more than five minutes old"), which this change makes false.
  Merging both requires rewording that sentence. The Redis track (SB-8) has the
  same dependency.
- Without SB-6, a PostgreSQL watcher whose shared listener failed re-acquires
  the failed listener on every restart: start and drain succeed and the first
  wait raises. With revision 2 that run ends after three failures with the
  error surfaced, as on `5d29fbf`; with SB-6 the new listener's wait returns
  and closes the streak.

## Observable-difference register

Against `5d29fbf` (revision 2):

| Change | Class | Theory basis |
|--------|-------|--------------|
| A healthy watcher whose run is older than 300 s survives an ordinary failure that its retry recovers from and keeps handling writes (was: `TimeoutError` at the first retry, before any reconnect) | C — no loss; restores recovery | [THEORY-3] bounded waiting or consumption; [THEORY-5]/[REV-THEORY-002] the backend owns its restart, so the watcher must not turn a recoverable substrate event terminal because of its own uptime |
| Failures separated by genuine recoveries (a restart whose processing wait then returns) do not accumulate; a new streak starts with retry count 0 and the 2 s backoff (was: the third failure over the run's life was terminal, and backoff kept growing) | C — no loss | same |
| A failure that recurs at the first processing wait after every restart, although each start and initial drain succeed, stays one streak: the third consecutive failure ends the run and the exception reaches the `run_forever()`/`run()` caller or `threading.excepthook` | N — same terminal behavior as `5d29fbf`. Revision 1's D row (an indefinite 2 s restart loop) is withdrawn by the owner decision of 2026-11-09 | [THEORY-4] explicit safety over magical recovery; [THEORY-6] the persistent failure stays diagnosable from the owning operation |
| Within one streak, 3 consecutive failures or 300 s still end the run; the first streak is still timed from run start | N — unchanged | [THEORY-4] guarantees named narrowly |
| `TimeoutError` text unchanged; its "Retries" and "Time elapsed" now report the current streak, which is the budget actually enforced | N — same text, consistent meaning | [THEORY-6] diagnosable from the owning operation |
| Each retried failure logs WARNING `Watcher error (retry n/3, failure streak age X.Xs): <Type>: <message>` (was DEBUG). Not gated by `BROKER_LOGGING_ENABLED`, like the existing exhaustion ERROR; an application without logging configuration (for example `broker watch`) sees it on stderr through Python's last-resort handler; stdout is unchanged | A — owner-decided 2026-11-09 | [THEORY-4] failures are reported rather than absorbed; [THEORY-6] diagnosable; [THEORY-4] Unix composability: diagnostics on stderr, data on stdout |
| A subclass that overrides `_process_messages` must call `_record_healthy_wait()` after each normal wait, or all its failures stay in one streak | N — such a subclass keeps the `5d29fbf` behavior; private hook, no known override | [THEORY-4] small concept count: one named setter |

## Alternatives (plan-local unless the owner promotes them)

- Restart the clock at establishment: rejected; reproduces the bug after 300 s
  of healthy running (see Design).
- Time every streak, including the first, from its first failure: rejected;
  changes the first-attempt budget that `test_absolute_timeout_exceeded` pins
  with no evidence that it is wrong.
- Successful start plus initial drain as the witness (revision 1): replaced
  by owner decision 2026-11-09; it left the recurring first-wait failure in an
  unbounded restart loop.
- A cap on recovered streaks or a new knob: not chosen by the owner (Q1).
- Logging alone (WARNING without a changed witness): not a bound (review F2);
  the owner chose it together with the processing-wait witness.
- Emit the WARNING from `_handle_retry`: it lacks the streak age, and adding a
  parameter would break existing `_handle_retry` overrides.

## Tasks

1. RED: two shared tests in `tests/test_watcher_edge_cases.py` (budget age;
   accumulated count) and one `SM-WATCHER-LIFECYCLE` row; confirm they fail on
   `5d29fbf` for the intended reasons. Gate: stop if a failure has any other
   cause. Done.
2. Fix `_run_with_retries`; update the `_check_retry_timeout` and
   `MAX_TOTAL_RETRY_TIME` docstrings; add the later-streak limit pin. Gate:
   owning test files pass on SQLite and PostgreSQL. Done.
3. Docs: implementation doc 07 `SM-WATCHER-LIFECYCLE` row, spec 16 Related
   Plans backlink, CHANGELOG Unreleased/Fixed, this plan and its index row.
   Done.
4. Sensitivity control, full gates, commit. Done (`2a732d8`; review
   dispositions `9473af3`).
5. Revision 2 (owner decision 2026-11-09): RED tests for the recurring
   first-wait failure, the WARNING log, the move-watcher witness and the
   `RESTART_FAILURE_BEFORE_HEALTHY_WAIT` row; rework the recovery scenarios so
   each recovery includes a normal processing wait; move the witness into both
   `_process_messages` implementations; WARNING per retry; docs; sensitivity
   and mutation controls; gates; commit. Done.

## Testing Plan

- `tests/test_watcher_edge_cases.py` (marked `shared`; runs on SQLite and, via
  `bin/pytest-pg`, PostgreSQL). Failures are injected through the public
  `polling_strategy=` constructor seam: a `PollingStrategy` subclass raises a
  scripted `OperationalError` from `wait_for_activity` (where a native waiter
  reports a lost server connection) or from `start` (a restart that cannot
  reconnect), then delegates to the real method. The clock seam is
  `watcher._monotonic` patched to real time plus an offset; retry backoff
  (`interruptible_sleep` calls of at least 1 s) returns at once, and the
  strategy's sub-second burst sleeps stay real. No real sleep decides an
  outcome; the watcher runs synchronously in the test thread.
  - `test_failure_after_retry_budget_age_recovers_and_handles_later_write`:
    a processing wait returns, then the next wait moves the clock 301 s
    forward and fails; the recovered watcher must handle a write made after
    recovery. `5d29fbf`: `TimeoutError`.
  - `test_move_watcher_failure_after_retry_budget_age_recovers`: the same
    scenario through `QueueMoveWatcher`'s own `_process_messages`.
  - `test_recovered_failures_do_not_accumulate_retry_count`: three failures,
    the second and third each after a restart whose processing wait returned
    (each failure writes one message, which the next restart's initial drain
    delivers, so the next wait returns on that activity); the watcher must
    then handle a later write. `5d29fbf`: the third failure propagates.
  - `test_wait_failure_after_every_restart_ends_run_after_retry_limit`: the
    first processing wait after every restart fails although start and drain
    succeed; the third failure must reach the `run_forever()` caller.
    `9473af3`: does not raise (keeps restarting).
  - `test_each_retry_logs_warning_with_streak_context` (caplog): two retries in
    one streak log WARNING with the retry number, the error and the streak age
    (the second at least 5 s, after a 5 s clock jump). `9473af3`: no WARNING.
  - `test_failure_streak_after_recovery_keeps_retry_limits[retries|time]`:
    after one recovery, restarts fail; the run must still end after three
    consecutive failures, or with `TimeoutError` once that streak passes 300 s.
    Pins invariant 1 against an over-broad reset.
- `tests/test_watcher_transition_tables.py` (SQLite only, state-machine
  inventory): `RECOVERED_FAILURE_NEW_STREAK` (a `_process_messages` override
  records a healthy wait, outlasts the budget and fails, three times; the run
  reaches a fourth entry with backoffs `[2, 2, 2]`) and
  `RESTART_FAILURE_BEFORE_HEALTHY_WAIT` (the override fails three times without
  a healthy wait; the third failure propagates with backoffs `[2, 4]`).
- Do not mock the queue, backend, drain or handler dispatch. Only the clock,
  the backoff sleep and the scripted failures are controlled.

## Verification and Gates

### Revision 2 (run 2026-11-09, foreground, worktree state = the revision 2 commit)

Code: `simplebroker/watcher.py` `BaseWatcher._process_messages` (865; witness
at 882), `_record_healthy_wait` (904–913), `_run_with_retries` (957–1031:
witness cleared at 982, new streak at 1014–1018, WARNING at 1020–1025),
`QueueMoveWatcher._process_messages` (2192; witness at 2200);
`MAX_TOTAL_RETRY_TIME` docstring at `simplebroker/_constants.py` 211.

| Command | Result |
|---------|--------|
| New tests on `9473af3` code (SQLite): `python -m pytest -n 0 tests/test_watcher_edge_cases.py -k "every_restart or logs_warning or retry_budget_age or accumulate_retry_count or keeps_retry_limits"` and the two transition rows | `test_wait_failure_after_every_restart_ends_run_after_retry_limit`: `Failed: DID NOT RAISE OperationalError` (keeps restarting); `test_each_retry_logs_warning_with_streak_context`: `assert 0 == 2` (no WARNING records); `RESTART_FAILURE_BEFORE_HEALTHY_WAIT`: `DID NOT RAISE RuntimeError`; the reworked recovery scenarios, both limit pins and `RECOVERED_FAILURE_NEW_STREAK` pass |
| Same selection with revision 2 (SQLite) | all pass |
| `python -m pytest -n 4 tests/test_watcher_edge_cases.py tests/test_watcher_transition_tables.py tests/test_state_machine_policy.py tests/test_plan_context_gate.py` | 87 passed |
| `python -m pytest -n 4 tests/test_watcher*.py tests/test_state_machine_policy.py tests/test_cli_watch.py tests/test_queue_move_watcher.py tests/test_multi_queue_watcher_example.py tests/test_commands_error_ownership.py tests/test_commands_stdout_delivery.py tests/test_python_library_api_contract_sb_api.py tests/test_connection_config.py tests/test_move_checkpoint_semantics.py tests/test_cli_broken_pipe.py tests/test_ruff_policy.py tests/test_plan_context_gate.py examples/tests` (SQLite) | 557 passed |
| `uv run ./bin/pytest-pg tests/test_watcher_edge_cases.py -n 4` (PostgreSQL 18) | 35 passed |
| `uv run ./bin/pytest-pg tests/test_watcher*.py -n 4` (shared watcher suite on PostgreSQL) | 186 passed |
| `uv run ./bin/pytest-pg tests/test_queue_move_watcher.py tests/test_cli_watch.py` plus PostgreSQL `test_pg_notify.py`, `test_pg_activity_waiter_lifecycle.py`, `test_pg_example_multi_queue_watcher.py`, `-n 4` | 19 passed (shared), 21 passed (extension) |
| Sensitivity: revision 2 code reverted (`git apply -R` of the saved `simplebroker/` patch), tests kept; SQLite and PostgreSQL | the recurring-failure test fails `DID NOT RAISE OperationalError`, the WARNING test fails `assert 0 == 2`, `RESTART_FAILURE_BEFORE_HEALTHY_WAIT` fails `DID NOT RAISE RuntimeError` (SQLite); 2 failed, 4 passed on PostgreSQL with the same messages |
| Control W1: witness moved, no WARNING | only the WARNING test fails |
| Control W2: `QueueMoveWatcher._process_messages` does not record healthy waits | only `test_move_watcher_failure_after_retry_budget_age_recovers` fails |
| Control W3: `BaseWatcher._process_messages` does not record healthy waits | the budget-age, accumulation and both limit tests fail |
| Restore | `simplebroker/watcher.py` sha256 `35d2a9cd…85cd9` before and after; `git diff -- simplebroker/` equals the saved patch (sha256 `7e1656a0…3304a`) |
| `ruff check .`; `bin/ruff_suppression_index.py --check`; `ruff format --check` on the CI paths | all clean (351 files already formatted) |
| mypy on `simplebroker`, `bin/release.py`, `bin/ruff_suppression_index.py`, both extension packages; mypy on every core test file (CI flags) | no issues (68 and 217 source files) |
| `python3 bin/check-plan-context`; `python3 bin/check-doc-paths`; `python3 bin/check-dom15-fixtures` | all OK |

### Revision 1 (run 2026-11-08 at `2a732d8`; superseded witness, kept as history)

Fix at `2a732d8`: `simplebroker/watcher.py` `_run_with_retries` lines
941–1006 (`streak_start`/`established` at 956–957, reset at 979–980, new
streak at 999–1001).

| Command | Result |
|---------|--------|
| `python -m pytest -n 0 tests/test_watcher_edge_cases.py -k "retry_budget_age or accumulate_retry_count or keeps_retry_limits"` plus the transition row, on `5d29fbf` code (SQLite) | budget-age test: `TimeoutError: Watcher retry timeout exceeded (300s). Retries: 1, Time elapsed: 301.0s`; accumulation test: `OperationalError: server closed the connection (3)` with log `Watcher failed after 3 retries`; `RECOVERED_FAILURE_NEW_STREAK`: same `TimeoutError` (301.0 s); limit pins: `TimeoutError ... Retries: 1, Time elapsed: 1000.0s` |
| same selection with the fix (SQLite) | all pass; `RETRY_THEN_RUN`, `TERMINAL_ERROR`, `test_absolute_timeout_exceeded`, `test_watcher_max_retries_exceeded` unchanged and passing |
| `python -m pytest -n 4 tests/test_watcher_edge_cases.py tests/test_watcher_transition_tables.py tests/test_state_machine_policy.py tests/test_plan_context_gate.py` | 83 passed |
| `python -m pytest -n 4 tests/test_watcher*.py tests/test_state_machine_policy.py` (SQLite) | 256 passed |
| `python -m pytest -n 4 examples/tests` (SQLite) | 88 passed |
| `uv run ./bin/pytest-pg tests/test_watcher_edge_cases.py -n 4` (PostgreSQL 18) | 32 passed |
| `uv run ./bin/pytest-pg tests/test_watcher*.py -n 4` (shared watcher suite on PostgreSQL) | 183 passed |
| Sensitivity: fix reverted (`git apply -R` of the saved `watcher.py` patch), tests kept; SQLite | both RED tests and the transition row fail with the base messages above; the two limit pins fail as on base |
| Sensitivity, PostgreSQL: `uv run ./bin/pytest-pg tests/test_watcher_edge_cases.py -n 0 -k "..."` | 4 failed (same messages as SQLite), 2 passed (existing first-streak tests) |
| Control M1: clock restarted at establishment (the literal "reset both" reading) | budget-age test, both limit pins and the transition row fail; accumulation test passes |
| Control M2: count reset only, clock still from run start | budget-age test, both limit pins and the transition row fail |
| Control M3: clock restarted at the next failure, count never reset | accumulation test and the transition row fail (`Watcher failed after 3 retries`) |
| Restore | `simplebroker/watcher.py` sha256 `700c88c2…eb61f` before and after; `git diff` equals the saved patch (sha256 `7b5e19fb…5dd73`) |
| `ruff check .`; `bin/ruff_suppression_index.py --check`; `ruff format --check` on the CI paths | all clean (351 files already formatted) |
| mypy on `simplebroker`, `bin/release.py`, `bin/ruff_suppression_index.py`, both extension packages; mypy on every core test file (CI flags) | no issues (68 and 217 source files) |
| `python3 bin/check-plan-context`; `python3 bin/check-dom15-fixtures`; `python3 bin/check-doc-paths`; `bin/coalesce-check` | all OK |

## Rollback, Rollout and Success Signals

- Rollback: revert the revision 2 commit to return to revision 1's witness,
  or all three branch commits to return to `5d29fbf`. No persistence, schema,
  wire or public API change.
- Rollout: none. One-way doors: none.
- Post-deploy signal: after a backend restart a long-running watcher logs one
  WARNING `Watcher error (retry 1/3, failure streak age ...)` and continues; no
  `Watcher retry timeout exceeded` for watchers whose backend recovered within
  the streak limits. A watcher that cannot get one working wait after restarts
  logs `Watcher failed after 3 retries` and its caller sees the error.

## Deviation Log

- 2026-11-08: The brief's "reset both after a successful start and drain"
  cannot be applied literally to the time budget (see Design); the count
  resets at establishment and the clock restarts at the next failure.
- 2026-11-09: Reclassified from class 3 to class 4 after review F1. Class 4
  requires review before implementation; the track brief assigns the
  cross-family review to the coordinator after implementation and forbids
  calling reviewers here, so implementation preceded review. The review found
  no blocker and asked for no implementation change.
- 2026-11-09: Owner decision on Q1 (via the coordinator) changes the health
  witness from start plus initial drain to the first processing wait that
  returns normally, and raises each retry log to WARNING. Implemented as
  revision 2 after review SB-7; not re-reviewed yet.

## Out of Scope

- Changing the per-streak limits or the backoff, and gating the retry WARNING
  on `BROKER_LOGGING_ENABLED`.
- Listener recovery (SB-6) and idle connections (SB-4).

## Owner questions

- Q1 — decided 2026-11-09 by the owner, on the coordinator's
  recommendation: a streak ends only after a processing wait that returns
  normally (the reviewer's alternative), reusing the existing limits with no
  new knob; and every retry logs at WARNING. Implemented in revision 2; the
  options considered were ratifying revision 1's restart loop, a cap on short
  streaks, this witness, and WARNING logging alone (which would not bound the
  loop).
- Q2 — deferred follow-up, not a landing blocker: stating the per-streak
  retry contract in `[SB-API-6]` (three consecutive failures or 300 s per
  streak; the first streak timed from run start) would be a separate class-5
  spec addition. On 2026-11-09 the owner accepted the review recommendation
  to land without further contract expansion.

## Independent Review Loop

- Author family: Claude (implementation agent)
- Reviewer family: Codex (OpenAI)
- Review invocation: `skills/call-agent/SKILL.md`, run by the coordinator (`timeout 3600 codex exec -s read-only -c model_reasoning_effort="high"` in this worktree; combined plan and implementation review)

## Review and execution record

Review SB-7, 2026-11-09, of `2a732d8` against `5d29fbf`. Verdict: no blocker.
The reviewer verified both SHAs, the nine-file diff, the baseline causal path,
the cited history and test falsifiability; paired in-memory probes with the
unchanged retry-owner methods reproduced the baseline failures and confirmed
recovery and exhaustion. Documentation gates and `git diff --check` passed.
The SQLite and PostgreSQL suite counts were not rerun by the reviewer (the
review was read-only). Both findings ask only for plan changes; no code
changed.

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| F1 | P3 | The loss of the exception that public `run_forever()` callers used to see is a [DOM-5] compatibility surface; private names and silent spec text do not exclude it | Accepted, fixed: reclassified as class 4 in the plan header and the `docs/plans/README.md` index row; existing hardening content kept and listed in the header; Deviation Log records that review followed implementation |
| F2 | P3 | Q1 presented a higher retry log level as a bound, but it only improves visibility and leaves the restart loop unbounded | Accepted, fixed: Q1 now separates bounded-policy options from the observability-only option, and records the reviewer's alternative (a successful processing wait qualifies recovery, reusing the existing limits with no new knob, at the cost of changing the health witness and both processing-loop implementations) as an owner option; the Alternatives entry is updated to match; the owner decision stays open |

Revision 2 (owner decision on Q1, `9473af3..79fa179`) passed the
coordinator review recorded below. The earlier pending-review status is closed.

## Fresh-Eyes Review

- Can a later streak escape the time budget? No: the first failure after a
  healthy wait sets `streak_start`, and every loop pass checks it.
- Can a later streak escape the count? No: the count resets only when a
  failure follows a healthy wait, which a restart that fails before or at its
  first wait never reaches.
- Can a stale healthy flag leak into the next attempt? No: `_run_with_retries`
  clears it before every attempt.
- Does the first streak change? No: it opens at run start and stays open until
  a failure follows a healthy wait.
- Comprehension check for reviewers: (a) why does the clock restart at the
  failure rather than at the healthy wait? (b) why is a successful start and
  initial drain not a recovery? (c) what does a `_process_messages` override
  need to do, and what happens if it does not?

### Revision 2 verification (coordinator)

Tier 1, Codex (`codex exec -s read-only`, reasoning effort high), 2026-11-09,
scoped to `9473af3..79fa179` (owner Q1 decision: reset only after a processing
wait returns normally; WARNING per retry). Verdict: PASS, no new blocking
defect. Write-free probes of both revisions' method bodies (both processing
loops, witness-omission controls) confirmed: a recurring first-wait failure
surfaces the original exception on failure three with backoffs `[2, 4]`; real
recovery resets; the WARNING format matches. The database-backed suites were
inspected, not rerun, by the reviewer. Review loop closed; Q2 (an [SB-API-6]
per-streak contract) is deferred outside this landing by the 2026-11-09
owner disposition above.

## Integration closure, 2026-11-09

Completed under [the integration plan](2026-11-09-performance-branches-integration-plan.md).
The owner accepted the recommendations; combined notification/lifecycle
regressions, core and extension tests, PostgreSQL/Redis shared API tests,
static/doc gates and independent agy reviews are recorded there. Semantic
merge repairs preserve this plan's intended behavior. Post-merge benchmark
reporting remains owned by that integration plan.
