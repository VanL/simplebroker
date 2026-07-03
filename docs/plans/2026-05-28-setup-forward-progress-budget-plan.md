# Setup Forward Progress Budget Plan

Date: 2026-05-28

Status: planned

Owner: SimpleBroker

## Purpose

Fix setup timeout failures where SimpleBroker is still making forward progress
inside setup, but the current fixed schema deadline expires before the phase can
finish.

The target failure mode is a Windows CI setup bootstrap like this:

```text
Setup phase 'schema' for '...\weft-tests.db' could not make progress within
10.0s: setup deadline expired
```

The important detail is that the error can occur after earlier schema work has
succeeded. The current code gives the whole schema phase one fixed 10 second
budget. That means slow but progressing setup work can consume the budget before
the final SQLite `commit` gets a useful retry window.

The new behavior should be:

```text
Each coarse setup phase accounts for timeout independently.
Inside the schema phase, successful setup operations reset the idle budget.
Failure means no setup operation completed for the idle timeout.
```

This plan is written for an engineer who is skilled, but new to this codebase
and likely to over-mock tests or over-design APIs. Follow the tasks in order.
Use red-green TDD. Keep the implementation boring. Do not solve adjacent
problems unless a red test proves this plan cannot work.

## Problem Summary

Current setup sequencing lives in three coarse phases:

```text
connection -> schema-vN -> optimization
```

The phases are coordinated with `PhaseLockService`, but the schema phase has its
own internal deadline:

- `simplebroker/db.py`
  - `BrokerCore.__init__()` runs connection setup, schema setup, then
    optimization setup.
  - `_setup_schema()` currently creates one absolute deadline with
    `time.monotonic() + SETUP_RETRY_MAX_ELAPSED`.
  - `_setup_database()`, `_verify_database_magic()`, and `_migrate_schema()` all
    share that same deadline.
- `simplebroker/helpers.py`
  - `execute_setup_with_retry()` accepts that deadline.
  - If the deadline has already expired, it raises `OperationalError` before it
    even tries the next setup operation.
- `simplebroker/_backends/sqlite/schema.py`
  - SQLite schema initialization performs several setup operations and then
    calls `run_with_retry(runner.commit)`.

In the Weft failure, the stack shows the deadline expiring while trying to enter
the final SQLite schema `commit`. The final commit likely did not get a real
retry window because earlier schema work consumed the shared phase budget.

This is not primarily a prune command issue. It happens in the harness fixture
before the test body runs. It is broker bootstrap under slow Windows CI I/O.

## Locked Decisions

Do not reopen these decisions during implementation unless a red test proves
the plan cannot work.

### Keep the Fix Internal

Do not add a public environment variable, public config key, CLI flag, or user
documentation for setup timeouts in this first PR.

Reason: the current bug is deadline accounting, not missing user tuning. A
public knob would make Weft CI pass only by asking downstream users to know an
internal SimpleBroker timeout.

### Do Not Add Persistent Schema Subphase Markers

Do not create PhaseLock markers for individual schema statements such as
`schema-create-tables`, `schema-create-indexes`, or `schema-meta`.

Reason: PhaseLock markers live outside the database. A marker written before
the relevant database transaction is durably committed can lie after a crash or
failed commit. Splitting schema into durable subphases is possible, but it is a
larger migration design than this bug requires.

### Do Not Rewrite PhaseLock

`simplebroker/_phaselock.py` is a standalone primitive. Keep it that way.

Do not make PhaseLock import SimpleBroker modules, know about SQLite, know about
`SetupPhase`, or enforce SimpleBroker-specific retry policy.

It is fine to rely on PhaseLock phase entry as a boundary. It is not fine to
teach PhaseLock how SQLite schema bootstrap should measure progress.

### Do Not Add a Hard Total Schema Timeout Yet

This plan is about forward progress. The first implementation should fail when
setup has been idle too long, not when the total wall-clock phase duration gets
large.

Reason: SimpleBroker's built-in schema work is finite. A total cap would be a
separate policy choice and risks reintroducing the same failure shape under a
larger number.

If a future custom backend loops forever while reporting successful operations,
that backend has a plugin bug. Do not solve that hypothetical in this PR.

### Do Not Change Durability Defaults

Do not set `BROKER_SYNC_MODE=NORMAL` or `OFF` in SimpleBroker tests as the fix.

Reason: changing SQLite sync mode changes durability semantics. It can be a
downstream test harness workaround, but it should not be the primary
SimpleBroker fix for deadline accounting.

### Preserve the Short Setup Busy Timeout

Keep the existing short setup busy timeout behavior:

- normal SQLite busy timeout is controlled by `BROKER_BUSY_TIMEOUT`
- setup-specific SQLite busy timeout is capped by `SETUP_BUSY_TIMEOUT_CAP_MS`
- `SQLiteRunner._setup_operation_context()` applies and restores that cap

The progress-budget fix should sit above this behavior, not replace it.

## Repository Primer

Runtime package:

- `simplebroker/`

Core files for this plan:

- `simplebroker/helpers.py`
  - Owns `_execute_with_retry()` and `execute_setup_with_retry()`.
  - Defines `SETUP_RETRY_MAX_ELAPSED`, `SETUP_RETRY_DELAY`,
    `SETUP_RETRY_MAX_DELAY`, `SETUP_BUSY_TIMEOUT_CAP_MS`, and
    `SETUP_PHASE_LOCK_TIMEOUT`.
  - This is the right place for a tiny setup progress budget helper.
- `simplebroker/db.py`
  - Owns `BrokerCore`.
  - Runs setup in this order: `CONNECTION`, `SCHEMA`, `OPTIMIZATION`.
  - Owns `_setup_schema()`, `_setup_database()`, and `_run_setup_with_retry()`.
  - This is where schema setup should share one progress budget that refreshes
    after successful schema operations.
- `simplebroker/_runner.py`
  - Owns `SQLiteRunner`.
  - Maps `SetupPhase` values to PhaseLock names.
  - Calls `PhaseLockService.run_phases()`.
  - Runs built-in connection and optimization setup.
  - This plan should not require code changes here unless tests reveal that a
    setup budget starts before phase entry.
- `simplebroker/_phaselock.py`
  - Standalone phase coordinator.
  - Owns advisory lock acquisition and phase completion markers.
  - Should not gain SimpleBroker-specific setup retry logic.
- `simplebroker/_backends/sqlite/schema.py`
  - Owns SQLite schema bootstrap and migrations.
  - It should keep receiving a `run_with_retry` callback. Do not hard-code
    retry budgets into this module.
- `simplebroker/_backends/sqlite/runtime.py`
  - Owns SQLite connection setup, including WAL and setup busy timeout.
  - Touch only if a red test proves connection setup needs progress refreshes
    inside its own implementation.

Primary tests:

- `tests/test_helpers_coverage.py`
  - Add focused tests for setup progress budget semantics.
  - Use fake time and fake sleep here. That is appropriate because this file
    tests retry-budget accounting, not SQLite.
- `tests/test_runner_error_handling.py`
  - Update the current schema deadline test.
  - Add tests proving `BrokerCore._setup_schema()` refreshes the shared schema
    budget after successful setup operations.
- `tests/test_sqlite_setup_contention.py`
  - Existing subprocess tests for real SQLite setup contention.
  - Do not add timing-sensitive Windows-only tests here unless the deterministic
    unit tests are insufficient.
- `tests/test_phaselock.py`
  - Existing tests for PhaseLock primitives.
  - Touch only if you change generic PhaseLock behavior. This plan should not
    require that.

Helpful docs:

- `docs/plans/2026-05-14-phaselock-windows-setup-contention-hardening-plan.md`
  - Prior setup hardening plan. Read the locked decisions before touching
    PhaseLock or setup busy timeout behavior.
- `README.md` development section near the bottom
  - Lists local test, lint, format, and type-check commands.
- `.github/workflows/test.yml`
  - Shows the Windows CI shape: xdist, `--dist loadgroup`, and a fallback
    xattr-disabled run.

## Desired Design

### Vocabulary

Use these words consistently in code comments, tests, and error messages:

- **PhaseLock wait timeout**
  - Time spent trying to acquire the setup phase advisory lock.
  - Owned by `PhaseLockService`.
  - Existing diagnostic messages mention `timeout=...`, `elapsed=...`, and
    lock paths.
- **Setup idle timeout**
  - Time since the last successful setup operation inside the current setup
    phase.
  - Owned by SimpleBroker setup retry code.
  - This is the timeout this plan changes.
- **Forward progress**
  - A setup operation returned successfully through `execute_setup_with_retry()`.
  - For SQLite schema bootstrap, that includes successful `runner.run(...)`
    calls, successful metadata checks, and successful `runner.commit`.

Do not use "progress" to mean "PhaseLock marker was written" inside schema
bootstrap. The schema marker is written only after the whole schema phase action
succeeds.

### Phase Boundaries

The current coarse phase boundaries are good:

```text
CONNECTION
SCHEMA
OPTIMIZATION
```

Keep them. The key invariant is that a setup idle budget starts after the
process has entered the phase action, not before waiting for the PhaseLock.

For schema, this means the budget must be created inside the callable passed to
`run_exclusive_setup(SetupPhase.SCHEMA, operation)`.

Do not create the schema budget before `run_exclusive_setup()` is called. That
would count time spent waiting for another setup owner against this process's
schema work.

### Progress Budget Shape

Add a small helper in `simplebroker/helpers.py`. Suggested name:
`SetupProgressBudget`.

Keep it tiny. It should do only three things:

1. remember the last successful setup operation time
2. report the remaining idle budget
3. refresh the last successful setup operation time after success

Suggested behavior:

```python
budget = SetupProgressBudget()

remaining = budget.remaining()
if remaining <= 0:
    raise OperationalError(...)

result = _execute_with_retry(..., max_elapsed=remaining, ...)
budget.record_progress()
return result
```

Do not add callbacks, observers, context managers, metrics emitters, global
registries, or public configuration. This helper is not a framework.

This is a cooperative retry budget, not a preemptive timer. It bounds retry
loops and checks before the next setup operation. It cannot interrupt a single
blocking SQLite or OS call in the middle. That is acceptable because setup
SQLite busy timeout is already capped by `SETUP_BUSY_TIMEOUT_CAP_MS`.

Important implementation detail: do not use `SETUP_RETRY_MAX_ELAPSED` as a
default argument value. Tests monkeypatch that constant. Use `None` as the
default and read `SETUP_RETRY_MAX_ELAPSED` inside `__init__()`.

Good:

```python
def __init__(self, idle_timeout: float | None = None) -> None:
    self.idle_timeout = (
        SETUP_RETRY_MAX_ELAPSED if idle_timeout is None else idle_timeout
    )
```

Bad:

```python
def __init__(self, idle_timeout: float = SETUP_RETRY_MAX_ELAPSED) -> None:
    ...
```

### Retry Helper API

Change `execute_setup_with_retry()` to accept the progress budget:

```python
def execute_setup_with_retry(
    operation: Callable[[], T],
    *,
    phase: str,
    target: str,
    stop_event: threading.Event | None = None,
    progress_budget: SetupProgressBudget | None = None,
) -> T:
    ...
```

Replace the old `deadline` parameter rather than supporting both paths.

Reason: keeping both `deadline` and `progress_budget` creates two meanings for
setup timeout and invites future bugs. `execute_setup_with_retry()` is an
internal helper, not documented public API.

When `progress_budget` is `None`, keep current per-operation behavior:

```text
one call to execute_setup_with_retry gets up to SETUP_RETRY_MAX_ELAPSED
```

When `progress_budget` is present:

```text
one call gets the remaining idle budget
successful return refreshes the budget
failed retries do not refresh the budget
```

This preserves existing connection setup behavior while allowing schema setup
to share a progress-aware budget across many internal operations.

### Error Messages

Replace the misleading "setup deadline expired" wording for the new budget
path.

Preferred message shape:

```text
Setup phase 'schema' for 'target.db' made no progress for 10.0s:
setup idle timeout expired
```

When `_execute_with_retry()` raises a lock/busy `OperationalError`, preserve the
current contextual wrapper, but make the timeout meaning clear:

```text
Setup phase 'schema' for 'target.db' made no progress for 10.0s:
database is locked
```

Do not claim "made no progress for 10.0s" for an immediate non-lock setup
failure such as a readonly database or malformed SQL. For those, keep setup
context without inventing a timeout:

```text
Setup phase 'schema' for 'target.db' failed: attempt to write a readonly database
```

One simple way to keep this honest:

- if the budget is already expired before an operation starts, raise the idle
  timeout message
- if `_execute_with_retry()` raises and the budget is now exhausted, raise the
  no-progress message with the underlying lock/busy error
- if `_execute_with_retry()` raises while budget remains, raise a setup failed
  message with the underlying error

Do not expose stack traces, retry counters, or internal class names in the
message.

### Why Not Persistent Schema Subphases

It is tempting to create PhaseLock phases for each schema operation. Do not do
that here.

The current schema bootstrap runs several operations and commits at the end:

```text
CREATE TABLE
DROP old indexes
CREATE indexes
INSERT metadata
commit
```

A persistent marker such as `schema-create-indexes` would be wrong unless the
database state it represents has been durably committed and can be verified on
resume. Otherwise a process can crash after the marker but before the commit,
and a later process can skip work that did not actually persist.

The progress budget gives us the useful part of subphase awareness without
inventing partial schema completion semantics.

## Bite-Sized Tasks

### Task 1: Add Red Tests For Setup Progress Budget Semantics

Files to touch:

- `tests/test_helpers_coverage.py`

Add tests before implementation.

Use fake monotonic time and fake `interruptible_sleep()`. This is a good use of
controlled fakes because the behavior under test is pure retry-budget
accounting.

If fake sleep advances by the requested wait amount, also monkeypatch
`helpers.time.time` to a stable value such as `lambda: 0.0`. `_execute_with_retry()`
uses `time.time()` only to add retry jitter, and the tests should not depend on
wall-clock jitter.

Add these tests:

1. `test_execute_setup_with_retry_refreshes_progress_budget_after_success`

   Shape:

   - monkeypatch `helpers.time.monotonic`
   - monkeypatch `helpers.time.time` if fake sleep uses the requested wait
   - monkeypatch `helpers.interruptible_sleep`
   - monkeypatch `helpers.SETUP_RETRY_MAX_ELAPSED` to a small value, such as
     `0.15`
   - create one `SetupProgressBudget`
   - run an operation that raises `OperationalError("database is locked")` until
     fake time is just below the idle timeout, then succeeds
   - immediately run a second operation with the same budget
   - assert the second operation runs

   This test should fail under the old fixed-deadline design.

2. `test_execute_setup_with_retry_fails_when_no_operation_makes_progress`

   Shape:

   - use one `SetupProgressBudget`
   - run an operation that always raises `OperationalError("database is locked")`
   - fake sleep advances time
   - assert `OperationalError`
   - assert the message says "made no progress" or "setup idle timeout expired"

   Do not assert the exact full string. Assert stable substrings.

3. `test_execute_setup_with_retry_does_not_refresh_budget_on_failed_attempts`

   Shape:

   - run a failing operation with a shared budget until timeout
   - then call a second operation that would succeed if executed
   - assert the second operation is not executed and timeout is raised first

   This guards against the bad implementation where every retry attempt resets
   progress even though nothing completed.

4. Update or replace
   `test_execute_setup_with_retry_honors_shared_deadline`

   The old invariant is intentionally changing. The new invariant is:

   ```text
   shared schema setup budget is idle/progress based, not total wall-clock based
   ```

Test design warning:

- Do not mock SQLite here.
- Do not spawn subprocesses here.
- Do not sleep for real.
- Do not make the tests depend on Windows.

### Task 2: Implement `SetupProgressBudget`

Files to touch:

- `simplebroker/helpers.py`

Implementation notes:

- Keep the class close to `execute_setup_with_retry()`.
- Use `time.monotonic()`.
- Store the last progress timestamp at construction time.
- Read `SETUP_RETRY_MAX_ELAPSED` at construction time when no explicit timeout
  is supplied.
- Return floats in seconds.
- Keep type hints simple.

Suggested public surface inside the module:

```python
class SetupProgressBudget:
    """Track idle time between successful setup operations."""

    def __init__(self, idle_timeout: float | None = None) -> None:
        ...

    def remaining(self) -> float:
        ...

    def record_progress(self) -> None:
        ...
```

If you need a helper to format the timeout message, keep it private and local to
`helpers.py`.

Do not make this a dataclass unless it actually improves clarity. A regular
class is fine because it owns mutable time state.

### Task 3: Update `execute_setup_with_retry()`

Files to touch:

- `simplebroker/helpers.py`

Implementation notes:

- Remove the `deadline` parameter.
- Add `progress_budget`.
- If `progress_budget` is `None`, keep the existing behavior:

  ```python
  max_elapsed = SETUP_RETRY_MAX_ELAPSED
  ```

- If `progress_budget` is present:

  ```python
  max_elapsed = progress_budget.remaining()
  if max_elapsed <= 0:
      raise OperationalError(...)
  ```

- Call `_execute_with_retry()` with `max_elapsed=max_elapsed`.
- Only after `_execute_with_retry()` returns successfully, call
  `progress_budget.record_progress()`.
- Return the operation result unchanged.
- If `_execute_with_retry()` raises, do not record progress.

Watch this edge case:

```python
result = _execute_with_retry(...)
if progress_budget is not None:
    progress_budget.record_progress()
return result
```

Do not write:

```python
try:
    return _execute_with_retry(...)
finally:
    progress_budget.record_progress()
```

That would refresh progress after failures.

### Task 4: Wire Schema Setup To A Shared Progress Budget

Files to touch:

- `simplebroker/db.py`

Implementation notes:

- Update `_run_setup_with_retry()` to accept `progress_budget` instead of
  `deadline`.
- Pass `progress_budget` to `execute_setup_with_retry()`.
- Update `_setup_database()` to accept and forward `progress_budget`.
- In `_setup_schema()`, create the budget inside the phase action:

  ```python
  def operation() -> None:
      progress_budget = SetupProgressBudget()
      self._setup_database(progress_budget=progress_budget)
      self._run_setup_with_retry(
          self._verify_database_magic,
          phase=SetupPhase.SCHEMA,
          progress_budget=progress_budget,
      )
      self._run_setup_with_retry(
          self._migrate_schema,
          phase=SetupPhase.SCHEMA,
          progress_budget=progress_budget,
      )
  ```

- Import `SetupProgressBudget` from `simplebroker.helpers`.
- Remove the now-unused `time` import from `db.py` only if it becomes unused.

Important invariant:

The budget must be created inside `operation()`, not before
`run_exclusive_setup()`. Time spent waiting for another process's PhaseLock
must not count against this process's schema idle budget.

### Task 5: Audit Phase Budget Boundaries

Files to touch:

- usually none
- `simplebroker/_runner.py` only if a red test proves a budget starts before
  phase entry

Current connection setup already gets its own per-call retry budget because
`BrokerCore.__init__()` enters `SetupPhase.CONNECTION` before `_setup_schema()`,
and `_setup_connection_phase()` calls `execute_setup_with_retry()` without a
shared schema deadline.

Do not change `_setup_connection_phase()` just for symmetry. That would be
untested churn. The implementation work for this plan belongs in schema setup
unless a red test shows otherwise.

What to verify while reviewing the diff:

- `BrokerCore.__init__()` still calls connection setup before schema setup.
- `_setup_schema()` still creates the progress budget inside the schema phase
  action.
- no setup budget is created before a PhaseLock wait.

Do not change `_setup_optimization_phase()` unless a red test requires it. It
currently applies in-memory PRAGMA settings to the current connection and does
not use retry loops.

### Task 6: Replace The Schema Deadline Wiring Test

Files to touch:

- `tests/test_runner_error_handling.py`

Replace `test_schema_setup_uses_one_shared_retry_deadline`.

New tests:

1. `test_schema_setup_refreshes_idle_budget_after_forward_progress`

   Shape:

   - use the existing `MinimalBrokerCore` style from the old test
   - fake runner's `run_exclusive_setup()` should assert
     `phase == SetupPhase.SCHEMA`, call the operation, and return `True`
   - fake backend's `initialize_database()` should call `run_with_retry()` for:
     - a first operation that initially raises lock errors and then succeeds
     - a second operation that records it ran
   - fake monotonic time and fake sleep should make the first operation consume
     nearly all of the idle budget before it succeeds
   - assert the second operation runs

   This proves the bug fix at the `BrokerCore._setup_schema()` wiring layer.

2. `test_schema_setup_still_fails_when_no_schema_operation_progresses`

   Shape:

   - fake backend calls `run_with_retry()` for one operation that always raises
     `OperationalError("database is locked")`
   - fake time advances through sleep
   - assert `OperationalError`
   - assert message mentions no progress or setup idle timeout

3. `test_schema_setup_budget_starts_inside_phase_action`

   Shape:

   - fake runner's `run_exclusive_setup()` advances fake monotonic time by a
     large amount before calling the operation
   - fake backend has one immediate successful setup operation
   - assert `_setup_schema()` succeeds

   This guards the phase-boundary invariant:

   ```text
   waiting to enter the schema phase does not consume schema work budget
   ```

Test design warning:

- These tests may use a fake runner and fake backend because they test
  `BrokerCore` wiring, not SQLite correctness.
- Keep the fakes tiny and behavior-specific.
- Do not use `MagicMock` for this. Small local classes are easier to read and
  harder to accidentally over-specify.
- Do not assert exact fake sleep counts unless the count is the behavior under
  test. Prefer asserting operation execution and stable error substrings.

### Task 7: Keep Real SQLite Coverage As A Gate, Not A New Flake Source

Files to touch:

- Usually none.
- Maybe `tests/test_sqlite_setup_contention.py` only if a deterministic gap
  remains.

Run the existing real SQLite setup contention tests after the deterministic
unit tests pass:

```bash
uv run pytest tests/test_sqlite_setup_contention.py
```

Do not add a real-time Windows latency simulation. That will be flaky and will
not prove the budget semantics as well as fake monotonic tests.

If you need one real SQLite regression, make it deterministic:

- hold a SQLite exclusive lock for a short, controlled duration
- release it before the idle timeout
- assert a `Queue` can initialize and write

But prefer the existing tests unless there is a red coverage gap.

### Task 8: Update Error Text Expectations

Files to touch:

- `tests/test_helpers_coverage.py`
- `tests/test_runner_error_handling.py`
- any other test found by:

```bash
rg -n "setup deadline expired|could not make progress|SETUP_RETRY_MAX_ELAPSED|deadline=" tests simplebroker
```

Expected changes:

- tests should stop expecting "setup deadline expired"
- new tests should expect "made no progress" or "setup idle timeout expired"
- context should still include phase and target

Do not loosen tests to `match="OperationalError"` or generic exception text.
The diagnostics are part of the behavior.

### Task 9: Run Focused Tests

Run these first:

```bash
uv run pytest tests/test_helpers_coverage.py -k "setup"
uv run pytest tests/test_runner_error_handling.py -k "schema_setup"
uv run pytest tests/test_sqlite_setup_contention.py
```

Then run the PhaseLock fallback gate used in CI:

```bash
PHASELOCK_ENABLE_XATTRS=0 uv run pytest \
  tests/test_phaselock.py \
  tests/test_runner_validation.py \
  tests/test_runner_error_handling.py \
  tests/test_queue_config_defaults.py \
  tests/test_sqlite_setup_contention.py
```

Why this matters:

- the first two commands prove the new budget semantics
- the SQLite contention command proves real setup still works
- the xattr-disabled command catches Windows-style fallback marker behavior

### Task 10: Run Full Quality Gates

Before opening the PR, run:

```bash
uv run pytest -m "not slow"
uv run ruff check simplebroker tests bin
uv run ruff format --check simplebroker tests bin
uv run mypy simplebroker bin/release.py
```

If local time is short, at minimum run the focused tests plus ruff and mypy.
Do not claim the full suite passed unless it actually ran.

### Task 11: Downstream Weft Verification

This plan fixes SimpleBroker. The original symptom was in Weft CI, so verify
against Weft before calling the incident closed.

Recommended downstream validation:

1. Build or install the SimpleBroker branch into the Weft test environment.
2. Re-run the failing Weft Windows job with xdist enabled.
3. Confirm `test_system_prune_rejects_invalid_options` no longer fails during
   `weft_harness` setup.
4. Confirm no other harness-backed test fails with the new setup idle timeout
   message.

If Weft still flakes:

- add temporary per-operation setup timing logs in SimpleBroker or the Weft
  harness
- identify whether the stall is:
  - before entering schema phase
  - inside one schema operation with no success for the idle timeout
  - after schema completes
- do not immediately raise the timeout or add harness retries without that
  evidence

Acceptable fallback if SimpleBroker cannot be released quickly:

- Weft may retry `build_context()` only for the specific SimpleBroker setup
  idle-timeout failure.

That is a downstream mitigation, not the primary fix.

## Invariants To Protect

The implementation is correct only if all of these remain true:

- Waiting for the PhaseLock does not consume schema idle budget.
- Entering the schema phase creates a fresh setup progress budget.
- A successful setup operation refreshes the idle budget.
- A failed retry attempt does not refresh the idle budget.
- If no setup operation succeeds for `SETUP_RETRY_MAX_ELAPSED`, setup fails.
- The schema completion marker is written only after the whole schema phase
  action succeeds.
- No persistent marker is written for a partial uncommitted schema substep.
- Normal operation busy timeout behavior remains unchanged.
- Setup busy timeout remains capped by `SETUP_BUSY_TIMEOUT_CAP_MS`.
- No new public config, CLI flag, dependency, or runtime service is added.

## Code Style Rules

Use the existing project style:

- Python 3.10 compatible type hints.
- `ruff` line length is 88.
- Keep imports sorted by ruff/isort.
- Prefer small local classes in tests over `MagicMock`.
- Use `pytest.MonkeyPatch` for fake time and sleep.
- Prefer real SQLite temp files for SQLite behavior.
- Use fakes only for retry accounting and `BrokerCore` wiring.
- Keep comments short and tied to non-obvious invariants.

Do not add broad abstractions:

- no setup manager object
- no retry policy registry
- no callback framework
- no per-backend budget plugin API
- no metrics layer
- no new package

## Test Design Guidance

The biggest testing risk is over-mocking.

Use this rule:

```text
Fake clocks for timing math.
Use real SQLite for SQLite behavior.
Use tiny local fakes for BrokerCore wiring.
Do not mock PhaseLock unless you are explicitly testing caller sequencing.
```

Examples:

- Good helper test:
  - fake `time.monotonic`
  - fake `interruptible_sleep`
  - operation raises `OperationalError("database is locked")`
  - assert budget refresh behavior
- Bad helper test:
  - mock `sqlite3.Connection`
  - mock cursor methods
  - assert exact calls unrelated to retry budget
- Good schema wiring test:
  - fake backend with two `run_with_retry()` calls
  - fake runner that invokes the schema phase action
  - assert second operation runs after first succeeds late
- Bad schema wiring test:
  - assert every internal helper call order with `MagicMock`
  - duplicate the implementation instead of testing observable behavior

## Review Checklist

Before submitting the implementation PR, review the diff with these questions:

1. Did we replace fixed schema deadline semantics with idle/progress semantics?
2. Does any timeout still start before entering the PhaseLock phase action?
3. Does any failed retry accidentally record progress?
4. Did we leave any `deadline=` call sites behind?
5. Did we change public config or docs unnecessarily?
6. Did we create any PhaseLock markers for partial schema work?
7. Did the tests fail before implementation and pass after?
8. Do tests assert behavior rather than internal call trivia?
9. Are error messages clearer than before?
10. Did we run the focused gates and record any gates not run?

## Non-Goals

This plan does not:

- make Windows CI fast
- change SQLite durability defaults
- add setup telemetry
- add a public setup timeout setting
- retry Weft harness setup by default
- redesign schema migrations
- split schema bootstrap into committed subtransactions
- rewrite PhaseLock
- change lock acquisition timeout behavior

## Expected Outcome

After implementation, a slow Windows CI run can spend more than 10 seconds total
inside schema setup if individual setup operations continue succeeding. It will
still fail if setup stalls for the idle timeout without any successful operation.

The final SQLite schema `commit` should receive a fresh retry window when prior
schema work has completed successfully. That directly addresses the observed
Weft failure without masking real setup stalls.
