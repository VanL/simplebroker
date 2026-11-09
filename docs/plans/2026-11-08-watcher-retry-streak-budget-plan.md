# Watcher retry limits per failure streak

Class: 3 — changes intended (observable) watcher behavior with no governing
spec text, contract test or explicit user requirement as intent evidence, so
not class 2. No [DOM-5] risky trigger fires: no async or deferred work is
added; one execution context; the retry policy is not a documented public
contract (`[SB-API-6]`, README and guides are silent; `_run_with_retries`,
`_check_retry_timeout` and `MAX_TOTAL_RETRY_TIME` are private); no CLI shape or
storage change; rollback is a plain revert with no rollout order. The closest
candidate, "compatibility surface", was considered and rejected for that
reason; a reviewer may escalate. No normative spec text changes, so not class
5 (see Q2 for the class-5 follow-up).
Status: active — implemented and verified on branch
`perf/sb-watcher-retry-budget` (base `perf/sb-base` = `5d29fbf`); cross-family
review pending (coordinator).
Owner: SimpleBroker product owner; implementation agent for this track.
Boundary: `simplebroker/watcher.py` `BaseWatcher._run_with_retries` and
`_check_retry_timeout`, plus the `MAX_TOTAL_RETRY_TIME` docstring in
`simplebroker/_constants.py`. `PollingStrategy`, activity waiters, backends,
`_handle_retry` backoff and `_process_with_retry` are out of scope.
Verification: shared watcher tests on SQLite and PostgreSQL that fail on
`5d29fbf` for the intended reasons; one `SM-WATCHER-LIFECYCLE` transition row;
owning watcher test files; shared watcher suite on PostgreSQL; ruff, format,
mypy; fix-reverted sensitivity control.
Required action: coordinator runs the cross-family review and records it
below; owner answers Q1 (the one diminution row) and Q2.

## Goal

- [x] A watcher whose run is older than `MAX_TOTAL_RETRY_TIME` (300 s)
      survives an ordinary failure that its retry recovers from, and handles a
      later write.
- [x] Failures separated by successful recoveries do not accumulate toward
      `max_retries`: three such failures do not end the run.
- [x] Within one failure streak the limits are unchanged: `max_retries` (3)
      consecutive failures, or 300 s, end the run; the first streak is still
      measured from run start.
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

A failure streak is the sequence of consecutive failed attempts since the
watcher was last established. The watcher is established once
`_start_strategy()` and the initial drain have both succeeded (the brief's
"successful start and drain"; it is the only health witness the loop owns,
because `_process_messages` and `_drain_queue` are subclass hooks).

- At establishment, `_run_with_retries` sets `retry_count = 0` and marks the
  streak closed.
- The first failure after establishment opens a new streak: its time budget
  starts at that failure (`streak_start = _monotonic()`).
- The first streak opens at run start, as before, so time spent in a failing
  first attempt still counts (`test_absolute_timeout_exceeded` unchanged).
  Uniform reading: the budget measures time since the watcher was last
  healthy, or since it began trying to become healthy.
- `_check_retry_timeout` is unchanged apart from its docstring; it receives the
  streak start.

Why not restart the clock at establishment (the literal reading of "reset both
after a successful start and drain"): the processing loop then runs for hours,
and the next failure finds `elapsed > 300` and dies, reproducing the bug.

Why no other health witness (for example "the processing loop completed one
wait"): it would require a hook inside the overridable `_process_messages`
(and `QueueMoveWatcher` overrides it) or new strategy state; the loop already
owns the start and initial-drain boundary.

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

## Hidden couplings

- `_handle_retry(e, retry_count, max_retries)` derives the backoff from the
  count, so the reset also resets the backoff to 2 s for a new streak.
- SB-6 (`perf/sb-listener-recovery`) depends on this loop to recover a
  PostgreSQL watcher after the server ends the LISTEN session; its CHANGELOG
  entry states the old limits ("at most two recoveries per run, and none once
  the run is more than five minutes old"), which this change makes false.
  Merging both requires rewording that sentence.
- Without SB-6, a PostgreSQL watcher whose shared listener failed re-acquires
  the failed listener on every restart: start and drain succeed, the next wait
  raises. With this change that is the row-4 shape below (indefinite 2 s
  restart loop) instead of death after three failures. The Redis registry has
  the same shape (SB-6 Q2).

## Observable-difference register

| Change | Class | Theory basis |
|--------|-------|--------------|
| A watcher whose run is older than 300 s survives an ordinary failure that its retry recovers from and keeps handling writes (was: `TimeoutError` at the first retry, before any reconnect) | C — no loss; restores recovery | [THEORY-3] bounded waiting or consumption; [THEORY-5]/[REV-THEORY-002] the backend owns its restart, so the watcher must not turn a recoverable substrate event terminal because of its own uptime |
| Failures separated by successful recoveries do not accumulate; a new streak starts with retry count 0 and the 2 s backoff (was: the third failure over the run's life was terminal, and backoff kept growing) | C — no loss | same |
| Within one streak, 3 consecutive failures or 300 s still end the run; the first streak is still timed from run start | N — unchanged | [THEORY-4] guarantees named narrowly |
| `TimeoutError` text unchanged; its "Retries" and "Time elapsed" now report the current streak, which is the budget actually enforced | N — same text, consistent meaning | [THEORY-6] diagnosable from the owning operation |
| A failure that recurs after every successful start and initial drain (for example a processing-loop or native-wait failure that reconnecting does not fix) now restarts indefinitely every ~2 s, delivering through each restart's initial drain, with each failure logged at DEBUG; was: the run ended on the third failure (about 6 s) and the exception reached the `run_forever()` caller or `threading.excepthook` | D — loss of the terminal signal for this one failure shape; owner ratification requested (Q1) | Argued under [THEORY-3] (the watcher still performs its owned function: each restart re-establishes and drains) and [THEORY-4] (stated narrowly in CHANGELOG rather than implied as general recovery). Counter-pressure: [THEORY-4] "explicit safety over magical recovery" and [THEORY-6] diagnosability, since the failure is visible only at DEBUG |

## Alternatives (plan-local unless the owner promotes them)

- Restart the clock at establishment: rejected; reproduces the bug after 300 s
  of healthy running (see Design).
- Time every streak, including the first, from its first failure: rejected;
  changes the first-attempt budget that `test_absolute_timeout_exceeded` pins
  with no evidence that it is wrong.
- Health witness inside `_process_messages`: rejected; second path into a
  subclass hook.
- A cap on recovered streaks or a new knob: rejected by the brief (no new
  knobs); offered to the owner as an option under Q1.

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
4. Sensitivity control, full gates, commit. Done.

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
    the first wait after a successful start and drain moves the clock 301 s
    forward and fails; the recovered watcher must handle a write made after
    recovery. Base: `TimeoutError`.
  - `test_recovered_failures_do_not_accumulate_retry_count`: the first wait
    after each of three successful starts fails (each failure writes one
    message, which the next restart's initial drain delivers); the watcher must
    then handle a later write. Base: the third failure propagates.
  - `test_failure_streak_after_recovery_keeps_retry_limits[retries|time]`:
    after one recovery, restarts fail; the run must still end after three
    consecutive failures, or with `TimeoutError` once that streak passes 300 s.
    Pins invariant 1 against an over-broad reset.
- `tests/test_watcher_transition_tables.py` (SQLite only, state-machine
  inventory): new row `RECOVERED_FAILURE_NEW_STREAK`; `_process_messages`
  fails on its first three entries and each established period outlasts the
  budget; the run must reach a fourth entry with backoffs `[2, 2, 2]`.
- Do not mock the queue, backend, drain or handler dispatch. Only the clock,
  the backoff sleep and the scripted failures are controlled.

## Verification and Gates (run 2026-11-08, foreground, worktree state = this commit)

Fix: `simplebroker/watcher.py` `_run_with_retries` lines 941–1006
(`streak_start`/`established` at 956–957, reset at 979–980, new streak at
999–1001); `_check_retry_timeout` docstring at 847; `MAX_TOTAL_RETRY_TIME`
docstring at `simplebroker/_constants.py` 211.

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

- Rollback: revert the commit. No persistence, schema, wire or public API
  change.
- Rollout: none. One-way doors: none.
- Post-deploy signal: a long-running watcher logs one `Watcher error (retry
  1/3)` at DEBUG after a backend restart and continues; no `Watcher retry
  timeout exceeded` for watchers whose backend recovered within the streak
  limits.

## Deviation Log

- 2026-11-08: The brief's "reset both after a successful start and drain"
  cannot be applied literally to the time budget (see Design); the count
  resets at establishment and the clock restarts at the next failure.

## Out of Scope

- Changing the per-streak limits, the backoff, or the retry log levels.
- Listener recovery (SB-6) and idle connections (SB-4).

## Owner questions

- Q1: ratify register row 5 (indefinite 2 s restart loop for a failure that
  recurs after every successful start and initial drain), or choose a bound
  for it, for example a cap on consecutive streaks that end within one retry
  cycle, or raising the retry log level. Either is a new rule, not part of
  this fix.
- Q2: should `[SB-API-6]` state the per-streak retry contract (three
  consecutive failures or 300 s per streak; the first streak timed from run
  start)? That is a class-5 spec addition.

## Independent Review Loop

- Author family: Claude (implementation agent)
- Reviewer family: pending — coordinator runs the cross-family review
- Review invocation: coordinator-owned per the track brief (`skills/call-agent/SKILL.md`)

## Review and execution record

(Coordinator: record the cross-family review, findings and dispositions here.)

## Fresh-Eyes Review

- Can a later streak escape the time budget? No: the first failure after
  establishment sets `streak_start`, and every loop pass checks it.
- Can a later streak escape the count? No: the count resets only at
  establishment, which a failing restart never reaches.
- Does the first streak change? No: it opens at run start and stays open until
  establishment.
- Comprehension check for reviewers: (a) why does restarting the clock at
  establishment not fix the bug? (b) which failure shape loses its terminal
  signal? (c) why must the first streak stay timed from run start?
