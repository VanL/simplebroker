# Phaselock Windows Setup Contention Hardening Plan

Date: 2026-05-14

Status: implemented

Owner: SimpleBroker

## Purpose

Make SimpleBroker's SQLite setup coordination robust under contention,
especially on Windows.

The golden rule for this work:

```text
We can tolerate latency. We cannot tolerate incorrectness or lack of progress.
```

This plan is written for an engineer who is skilled, but new to this codebase
and likely to over-mock tests or over-design APIs. Follow the tasks in order.
Use red-green TDD. Keep the implementation boring. Do not solve adjacent
problems unless a red test proves this plan cannot work.

## Problem Summary

Recent Windows CI failures showed normal CLI operations timing out in first
write, broadcast, or move paths. The important stack frames were not in test
logic. They were inside SQLite setup and transaction calls:

- `simplebroker/_backends/sqlite/runtime.py`: `PRAGMA journal_mode=WAL`
- `simplebroker/_backends/sqlite/schema.py`: `CREATE TABLE` and meta setup
- `simplebroker/_runner.py`: `BEGIN IMMEDIATE`
- `simplebroker/_runner.py`: `commit`

The phaselock stack frames show the process had already entered
`PhaseLockService._run_status_phases()` and was running the phase action. That
means the phaselock wait itself was not the only issue. The current design can
acquire the setup phase lock, then spend too long inside SQLite's own busy
waits and SimpleBroker's Python retry loop. The test harness eventually kills
the subprocess, which is a lack-of-progress failure.

The fix is not "increase test timeouts." The fix is to make setup work under
the phaselock bounded, retryable, and diagnostically clear.

## Locked Decisions

Do not reopen these decisions unless a red test proves the plan cannot work.

### Completion Markers Stay Boolean

When xattrs are unavailable, fallback phase completion sidecars remain empty
files. Their only meaning is existence.

Do not write metadata, pid, timestamps, phase names, JSON, checksums, or schema
versions into completion sidecars.

Reason: same-directory temp file creation plus `os.replace()` gives us an
atomic transition from "phase incomplete" to "phase complete." Adding marker
content creates partial-write, flush, parse, and corruption questions that are
not needed for correctness.

### Lock Files Are Not Completion Markers

It is acceptable to write a byte to the stable lock file if needed for Windows
byte-range locking. That file is not a completion marker. Its existence still
does not mean lock ownership.

The correctness rule remains:

```text
Only an acquired advisory lock means lock ownership.
Only a phase marker created after a successful action means completion.
```

### No New Public API

Do not add public configuration, public lock APIs, background daemons,
callbacks, or user-visible setup modes.

Internal helper parameters are allowed when they make setup retry behavior
explicit and testable.

### No Runtime Dependencies

SimpleBroker's core package has no runtime dependencies. Do not add one.

### Phaselock Stays Standalone

`simplebroker/_phaselock.py` is intentionally portable. It must remain usable
outside SimpleBroker.

That means:

- no imports from `simplebroker.*`
- no SQLite imports
- no SimpleBroker exception classes such as `OperationalError`
- no knowledge of `SetupPhase`, `SCHEMA_VERSION`, queues, brokers, or database
  validation
- no SimpleBroker-specific filenames except defaults already provided as
  generic constructor arguments

`_phaselock.py` may know generic concepts:

- target path
- phase names supplied by the caller
- advisory lock path
- xattr marker state
- fallback status marker state
- timeout and elapsed time

SimpleBroker-specific setup policy belongs in `simplebroker/_runner.py`,
`simplebroker/_backends/sqlite/runtime.py`, `simplebroker/_backends/sqlite/schema.py`,
`simplebroker/helpers.py`, and `simplebroker/db.py`.

Do not move SQLite retry budgets, database validation, stale database
classification, or SimpleBroker error wrapping into `_phaselock.py`.

### No Silent Data Destruction

Never delete or overwrite a non-empty user database just because setup markers
look stale.

It is safe to discard fallback status markers when the target DB is missing or
zero bytes. It is not safe to "repair" a non-empty invalid database by clearing
markers and treating it as new. Non-empty invalid targets should fail clearly.

### Scope Lock

Primary scope:

- phaselock primitive hardening
- SQLite setup phase marker invalidation
- setup-phase retry/busy-timeout progress
- tests proving no incorrect setup skip and no unbounded setup wait

Do not broaden into general write throughput or watcher behavior. If `BEGIN
IMMEDIATE` contention remains after setup hardening, capture it as a separate
transaction-progress plan.

## Repository Primer

Runtime package:

- `simplebroker/`

Core files for this plan:

- `simplebroker/_phaselock.py`
  - Standalone phase coordinator.
  - Must remain stdlib-only and usable outside SimpleBroker.
  - Must not import from `simplebroker.*`.
  - Must not know about SQLite, `SetupPhase`, schema versions, or broker
    validation.
  - Owns advisory file lock acquisition.
  - Uses xattrs when available.
  - Uses empty status sidecar files when xattrs are unavailable.
- `simplebroker/_runner.py`
  - Owns `SQLiteRunner`.
  - Maps `SetupPhase` values to phase names and phase actions.
  - Calls `PhaseLockService.run_phases()`.
  - Tracks in-process completed setup phases.
- `simplebroker/_backends/sqlite/runtime.py`
  - Owns SQLite connection-wide setup: SQLite version, path validation,
    auto-vacuum setup, WAL mode.
- `simplebroker/_backends/sqlite/schema.py`
  - Owns SQLite schema creation and migration setup.
- `simplebroker/helpers.py`
  - Owns `_execute_with_retry()`, the generic retry helper for database lock
    errors.
- `simplebroker/db.py`
  - Owns `BrokerCore`, setup sequencing, and transactional operations.
  - Touch only if needed to pass explicit setup deadlines into existing setup
    helpers.

Primary tests:

- `tests/test_phaselock.py`
  - Existing unit and subprocess tests for xattr/fallback phase locking.
  - Add phaselock primitive and marker behavior tests here.
- `tests/test_runner_validation.py`
  - Existing setup marker and validation tests.
  - Add stale marker invalidation tests here if style fits.
- `tests/test_runner_error_handling.py`
  - Existing setup and runner error-path tests.
  - Add bounded setup failure tests here if they fit.
- `tests/test_commands_init.py` or a new focused test file
  - Use only if you need CLI-level proof that first-write setup makes progress
    under contention.

Useful docs to check while implementing:

- Python `sqlite3` docs:
  - `sqlite3.connect(timeout=...)`
  - `PRAGMA busy_timeout`
  - `isolation_level=None`
- SQLite docs:
  - WAL mode
  - busy handlers and busy timeout
  - `BEGIN IMMEDIATE`
- Python `msvcrt.locking` docs for Windows byte-range locking.
- Python `os.replace` docs for atomic same-filesystem rename.
- Pytest docs for `tmp_path`, subprocess tests, and `pytest.mark.skipif`.

Use these docs to clarify standard-library behavior. Do not add new
dependencies.

## Engineering Rules

### Red-Green TDD

For each task:

1. Add a focused failing test.
2. Run only that test and confirm it fails for the expected reason.
3. Implement the smallest code change.
4. Run the focused test again.
5. Run the local gate for the touched area.
6. Refactor only after tests are green.

If a proposed red test passes on current code, do not force a code change. Step
back and either improve the test or mark that task unnecessary.

### Prefer Real Integration Over Mocks

Use real temp files, real SQLite databases, real `Queue` or CLI calls, and real
subprocesses for contention behavior.

Mocks are acceptable only for tiny deterministic units that cannot reasonably
be exercised otherwise, such as simulating `msvcrt` on non-Windows. Even then,
also keep or add a real Windows-only test when possible.

Bad test shape:

```text
Mock PhaseLockService and assert method X was called.
```

Good test shape:

```text
Create real fallback status markers plus a zero-byte DB, run a real first
write, and assert schema setup actually runs instead of trusting stale markers.
```

### DRY

Do not create a separate retry implementation in every setup function.

Prefer one shared elapsed-time retry helper in `simplebroker/helpers.py` and
one shared internal setup retry policy used by both:

- `SQLiteRunner` connection setup in `simplebroker/_runner.py`
- `BrokerCore` schema setup in `simplebroker/db.py`

Do not duplicate retry constants separately in runner and core code.

### YAGNI

Do not add:

- owner files required for correctness
- contentful completion markers
- public timeout config
- a setup daemon
- a cross-process heartbeat
- a background cleanup thread
- a new lock backend

Add only what the tests require for correctness and progress.

### Failure Quality

When setup cannot make progress, raise a normal SimpleBroker `OperationalError`
before the external test harness kills the process.

The error should include enough text to diagnose:

- target path
- phase name
- elapsed time or deadline
- last SQLite lock/busy error when available
- whether xattrs or fallback status files were being used

Do not put this diagnostic data in completion sidecars.

## Invariants

These invariants must hold after the work:

1. A phase is marked complete only after its action returns successfully.
2. A fallback completion sidecar is empty and correctness depends only on its
   atomic existence.
3. Lock file existence is never treated as ownership.
4. If the DB target is missing or zero bytes, stale fallback status markers do
   not cause setup to skip required work.
5. If the DB target is non-empty and invalid, setup fails clearly. It must not
   silently overwrite or reinitialize user data.
6. `_phaselock.py` remains standalone and contains no SimpleBroker-specific or
   SQLite-specific setup policy.
7. Setup SQLite busy waits are short enough that Python retry logic can observe
   failure and keep making progress.
8. Setup retries are bounded by elapsed time, not by an exponential retry count
   that can outlive the CLI subprocess timeout.
9. Contenders either observe completion or continue retrying until a bounded
   SimpleBroker error is raised. They do not hang until killed externally.
10. Existing xattr behavior still works where xattrs are available.
11. Existing fallback status behavior still works where xattrs are unavailable.

## Bite-Sized Tasks

### Task 1: Add a Regression Test for Zero-Byte DB With Stale Status Markers

Purpose:

Prove fallback completion markers cannot make setup skip work when the target
database is empty or partially created.

Files to touch:

- Prefer `tests/test_runner_validation.py`.
- If the existing style does not fit, add a small focused test to a new
  `tests/test_sqlite_setup.py`.
- Do not put this DB-specific test in `tests/test_phaselock.py`; phaselock
  tests should stay about the standalone phase coordinator.

Test shape:

1. Create `tmp_path / "broker.db"` as a zero-byte file.
2. Create fallback status marker files next to it:
   - `broker.setup.status.connection`
   - `broker.setup.status.schema-v{SCHEMA_VERSION}`
   - `broker.setup.status.optimization`
3. Use a real SimpleBroker operation against that path:
   - `Queue("q", persistent=True, db_path=str(db_path)).write("msg")`, or
   - `run_cli("write", "--file", str(db_path), "q", "msg", cwd=tmp_path)`
4. Assert the write succeeds.
5. Assert the DB now contains valid SimpleBroker schema/magic using existing
   validation helpers or a normal read.
6. Assert old stale markers were not trusted to skip setup.

Expected red behavior before implementation:

- The operation should fail because setup sees stale markers and does not
  initialize the zero-byte target correctly.

If this test unexpectedly passes:

- Do not skip the task. Add a more direct test at the runner level:
  instantiate `SQLiteRunner`, pre-create markers through `PhaseLockService`,
  then call setup through the real runner and assert the marker cleanup path
  ran.
- Keep that direct test outside `tests/test_phaselock.py`, because the behavior
  being tested is SimpleBroker's runner policy, not phaselock itself.

Implementation files:

- `simplebroker/_runner.py`

Implementation guidance:

- Replace the narrow `_database_file_exists()` setup cleanup trigger with a
  helper that detects whether setup markers are unsafe for the current target.
- Suggested name: `_target_needs_fresh_setup_markers()`.
- It should return true when:
  - the target path does not exist
  - the target path exists and `stat().st_size == 0`
- It should return false for non-empty files.
- In `run_exclusive_setup()`, before checking `service.has_phase(...)`, discard
  fallback status markers when the phase is `SetupPhase.CONNECTION` and the new
  helper returns true.

Do not:

- Delete the database file.
- Clear markers for non-empty invalid databases.
- Add marker content.
- Special-case only Windows.

Focused test command:

```bash
uv run pytest tests/test_runner_validation.py -q
```

If you added a new file, run that file too.

### Task 2: Preserve Clear Failure for Non-Empty Invalid DBs

Purpose:

Protect correctness. Stale marker cleanup must not turn a non-empty invalid
file into a fresh SimpleBroker database.

Files to touch:

- `tests/test_runner_validation.py`
- Possibly `simplebroker/_runner.py` only if Task 1's implementation is too
  broad.

Test shape:

1. Write non-SQLite bytes to `broker.db`, for example `b"not sqlite"`.
2. Create fallback status marker files next to it.
3. Run a real setup-triggering operation.
4. Assert it fails with a clear validation/setup error.
5. Assert the file still contains the original bytes.

Expected behavior:

- This may already pass. Keep the test if it protects the Task 1 boundary.

Implementation guidance:

- If the test fails by reinitializing the file, fix the stale-marker helper so
  only missing or zero-byte targets clear markers.
- If the test fails with a confusing downstream "no such table" error, prefer
  moving validation earlier for non-empty targets rather than reinitializing.

Focused test command:

```bash
uv run pytest tests/test_runner_validation.py -q
```

### Task 3: Harden the Windows Lock File Primitive Without Changing Marker Semantics

Purpose:

Remove ambiguity from `msvcrt.locking(..., 1)` on an empty lock file. The lock
file is stable and shared. It can contain one byte because it is not a
completion marker.

Files to touch:

- `simplebroker/_phaselock.py`
- `tests/test_phaselock.py`

Standalone boundary:

- This task is allowed in `_phaselock.py` because preparing the advisory lock
  file is generic lock behavior.
- Do not import anything from SimpleBroker.
- Do not reference SQLite or setup phases.

Test shape:

Add a cross-platform test:

```text
with PhaseLockService(target).locked():
    assert service.lock_path.exists()
    assert service.lock_path.stat().st_size >= 1
```

Why cross-platform:

- It avoids depending on fake `msvcrt`.
- It documents that lock files are allowed to be non-empty.
- It should fail on current code because the lock file is commonly zero bytes.

Keep the existing Windows-only `test_msvcrt_lock_blocks_second_process_on_windows`.
Do not replace it with a mock.

Implementation guidance:

- Add a small private helper in `_AdvisoryLock`, for example
  `_prepare_lock_file(lock_file: BinaryIO)`.
- Before calling `_try_lock(lock_file)`:
  - seek to end
  - if size is zero, write one byte such as `b"\0"`
  - flush the Python file object
  - with `contextlib.suppress(OSError)`, call `os.fsync(lock_file.fileno())`
  - seek back to byte 0
- Then lock byte 0 on Windows.

Keep this code small. Do not add a lock-file format.

Focused test command:

```bash
uv run pytest tests/test_phaselock.py::test_lock_file_is_prepared_for_byte_range_locking -q
uv run pytest tests/test_phaselock.py -q
```

Use the actual test name you add.

### Task 4: Add Elapsed-Time Retry Support for Setup Work

Purpose:

Replace setup retry behavior based on large retry counts plus exponential
backoff with a progress budget based on elapsed time.

Files to touch:

- `simplebroker/helpers.py`
- `tests/test_runner_error_handling.py` or a focused helper test file

Standalone boundary:

- Do not put this retry helper in `_phaselock.py`.
- `_phaselock.py` should continue to report lock-acquisition timeouts with its
  own generic `PhaseLockTimeout`.
- SimpleBroker setup retries and `OperationalError` handling belong outside
  phaselock.

Design:

Extend `_execute_with_retry()` with optional elapsed-time controls while
preserving existing call sites:

```python
def _execute_with_retry(
    operation,
    *,
    max_retries=10,
    retry_delay=0.05,
    stop_event=None,
    max_elapsed: float | None = None,
    max_retry_delay: float | None = None,
) -> T:
    ...
```

Behavior:

- Existing callers without `max_elapsed` behave the same except for harmless
  refactoring.
- When `max_elapsed` is set:
  - use `time.monotonic()`
  - before each retry, check elapsed time
  - cap sleep so it does not overshoot the remaining budget by more than a
    tiny scheduling margin
  - raise the last `OperationalError` when the budget expires
  - keep `stop_event` behavior intact
- `max_retry_delay` caps exponential backoff for setup. A reasonable setup cap
  is 100-250ms.

Tests:

Use focused unit tests here. This is retry math, not database behavior.

Add tests for:

1. A lock error is retried until success before `max_elapsed`.
2. A repeated lock error raises before an unbounded exponential schedule.
3. `stop_event` still interrupts sleep.
4. Non-lock `OperationalError` is not retried.

Use a fake operation that raises SimpleBroker `OperationalError("database is
locked")`. It is acceptable to monkeypatch time or use tiny budgets here,
because this tests a pure helper.

Do not:

- Change all database call sites to use elapsed budgets.
- Add public config.
- Swallow or wrap non-lock errors.

Focused test command:

```bash
uv run pytest tests/test_runner_error_handling.py -q
```

If you create a new helper test file, run that file.

### Task 5: Use Short SQLite Busy Timeouts During Setup

Purpose:

Prevent SQLite's C-level busy handler from blocking longer than the Python setup
retry budget can observe.

Files to touch:

- `simplebroker/_backends/sqlite/runtime.py`
- `simplebroker/_runner.py`
- possibly `simplebroker/helpers.py` if Task 4 did not already add the needed
  support
- tests in `tests/test_runner_error_handling.py` or a new focused setup test

Implementation guidance:

- Add a narrow internal helper to compute setup busy timeout.
- Do not add public config.
- Suggested behavior:
  - use the configured `BROKER_BUSY_TIMEOUT`
  - cap setup-specific busy timeout at 250ms
  - if the configured timeout is lower than 250ms, respect the lower value
- In `setup_connection_phase()`, open the setup connection with matching short
  timeout:

```python
sqlite3.connect(db_path, isolation_level=None, timeout=setup_timeout_seconds)
setup_conn.execute(f"PRAGMA busy_timeout={setup_timeout_ms}")
```

Reason:

- `sqlite3.connect(timeout=...)` and `PRAGMA busy_timeout` are related but not
  identical surfaces. Keep them aligned for setup.

Tests:

Add a small focused test proving setup uses the capped timeout. A narrow fake
connection is acceptable for this exact assertion if the public behavior is
also covered by integration tests in later tasks.

Also add or keep a real integration test in Task 7.

Do not:

- Reduce normal operation busy timeout globally.
- Change `BROKER_BUSY_TIMEOUT` semantics for non-setup reads/writes.
- Sleep in tests to "make contention likely."

Focused test command:

```bash
uv run pytest tests/test_runner_error_handling.py -q
```

### Task 6: Route Setup Phases Through the New Bounded Retry Policy

Purpose:

Make all setup phase SQLite work use the same bounded progress policy.

Files to touch:

- `simplebroker/_runner.py`
- maybe `simplebroker/db.py` if schema setup currently routes through
  `BrokerCore._run_with_retry()` and needs a setup-specific budget
- tests from Tasks 1, 4, and 5

Standalone boundary:

- Do not modify `PhaseLockService` to own action deadlines.
- `PhaseLockService.timeout` remains a lock-acquisition timeout.
- The caller-owned phase action is responsible for its own bounded retry
  behavior.

Implementation guidance:

- Add one shared internal setup retry policy. Keep it outside `_phaselock.py`.
- Acceptable shapes:
  - private constants in `simplebroker/_runner.py` plus a small
    `_setup_retry_kwargs()` helper that `db.py` can import, or
  - a private helper in `simplebroker/helpers.py` that wraps
    `_execute_with_retry()` for setup work.
- Choose the shape that avoids circular imports and duplicated constants.
- The shared policy should call `_execute_with_retry()` with:
  - a total elapsed budget no greater than the phase lock timeout
  - capped retry delay
  - the existing stop event when the caller has one
- Use the policy from:
  - `SQLiteRunner._setup_connection_phase()` for connection-wide setup
  - `BrokerCore._setup_database()` / schema setup, by passing a setup-specific
    `run_with_retry` callable into `backend_plugin.initialize_database(...)`
  - optimization setup only if it can encounter lock/busy errors

Current code detail:

- `SQLiteRunner._setup_connection_phase()` currently calls
  `_execute_with_retry()` directly.
- SQLite schema creation currently flows through `BrokerCore._setup_database()`
  and `BrokerCore._run_with_retry()`, not through a `SQLiteRunner` helper.
- Do not pretend `SQLiteRunner` alone owns all setup retries. That would leave
  schema setup on the old unbounded retry behavior.

Important design point:

`PhaseLockService.timeout` currently bounds lock acquisition. It does not bound
the phase action after the lock is acquired. This task adds a bound to the
phase action's internal retry behavior. It does not attempt to interrupt a
currently executing SQLite C call. The short setup busy timeout from Task 5 is
what makes the action observable by Python.

Error behavior:

- On deadline, raise `OperationalError`.
- Include phase name and target path.
- Include the last lock/busy error if available.

Do not:

- Add a thread to run setup so it can be killed.
- Use signals or async.
- Catch broad `Exception` and keep retrying.
- Mark the phase complete after deadline failure.

Focused test command:

```bash
uv run pytest tests/test_runner_error_handling.py tests/test_runner_validation.py -q
```

### Task 7: Add a Real Multi-Process Setup Contention Test

Purpose:

Prove first-time setup makes progress when multiple processes hit the same
fresh SQLite DB at once.

Files to touch:

- Prefer a new focused test file:
  `tests/test_sqlite_setup_contention.py`
- Reuse existing helpers from `tests/conftest.py` only if they make the test
  clearer.

Test shape:

1. Use `tmp_path` and one shared DB path.
2. Spawn several Python subprocesses, for example 8 on normal platforms and a
   smaller number if needed on slow Windows.
3. Each subprocess should run real SimpleBroker code, for example:

```python
from simplebroker import Queue
q = Queue("setup_contention", persistent=True, db_path=str(db_path))
q.write("msg")
q.close()
```

4. Start them close together.
5. Assert every subprocess exits 0.
6. Read the queue and assert all messages are present exactly once.
7. Assert the setup lock file exists.
8. Assert fallback status markers exist on no-xattr platforms.

Avoid:

- Mocking `PhaseLockService`.
- Counting internal method calls.
- Sleeping for arbitrary fixed durations as the main assertion.

Use subprocess timeouts only as a safety valve, not as proof of correctness.
The primary assertion is all processes complete and data is correct.

Focused test command:

```bash
uv run pytest tests/test_sqlite_setup_contention.py -q
```

Windows gate:

Run this on Windows 3.13 and 3.14 in CI before trusting the fix.

### Task 8: Add a Real Locked-Database Setup Retry Test

Purpose:

Prove setup retries through temporary SQLite lock contention instead of hanging
inside one long busy wait.

Files to touch:

- `tests/test_sqlite_setup_contention.py`

Test shape:

1. Create a DB path.
2. Start a helper subprocess that opens the SQLite DB and holds a write lock.
   Use a real SQLite connection, not a mock.
3. While the lock is held, start a SimpleBroker subprocess that performs first
   write setup against the same DB.
4. Release the lock.
5. Assert the SimpleBroker subprocess completes successfully.
6. Assert the message exists.

Keep the lock hold time short, for example 0.5s to 1.0s. The purpose is to
prove retry/progress, not to make CI slow.

If this test is flaky:

- Replace timing assumptions with ready/release sentinel files.
- The lock-holder subprocess should create `ready` after it has the DB lock.
- The test should create `release` when it wants the lock holder to commit and
  exit.

Do not:

- Use a fixed `time.sleep(2)` as the synchronization mechanism.
- Assert exact retry counts.
- Make this Windows-only unless it cannot be made reliable elsewhere.

Focused test command:

```bash
uv run pytest tests/test_sqlite_setup_contention.py -q
```

### Task 9: Improve Phaselock Timeout Diagnostics Without New Correctness Files

Purpose:

Make future failures actionable while preserving the sidecar design.

Files to touch:

- `simplebroker/_phaselock.py`
- `tests/test_phaselock.py`

Standalone boundary:

- Diagnostics in `_phaselock.py` must stay generic.
- Good: target path, lock path, elapsed time, phase names, marker state.
- Bad: SQLite error text, `OperationalError`, schema version constants, broker
  magic, queue names.

Current diagnostics already include target, target existence, xattr state,
marked/missing phases, and status files. Keep that.

Add only low-risk diagnostics that come from existing state:

- lock path
- elapsed time
- timeout
- lock file stat when available
- phase names

Do not:

- Create owner/progress files as part of correctness.
- Add marker content.
- Add pid tracking that tests depend on.

Tests:

Extend existing timeout tests to assert the diagnostic string includes:

- `target=`
- `missing=[...]`
- `timeout=`
- `elapsed=`
- lock path or lock stat detail

Focused test command:

```bash
uv run pytest tests/test_phaselock.py -q
```

### Task 10: Re-Run the Original Failure Class

Purpose:

Prove the fix applies to the observed class of failures, not just new tests.

Run focused Windows-sensitive CLI tests locally where possible:

```bash
uv run pytest tests/test_smoke.py -q
uv run pytest tests/test_security_fixes.py::test_safe_path_within_directory -q
uv run pytest tests/test_safety_fixes.py::test_cleanup_toctou_fix -q
uv run pytest tests/test_cli_move.py::TestConcurrentOperations::test_concurrent_moves_no_duplication -q
```

Run the broader local gate:

```bash
uv run ruff check simplebroker tests
uv run mypy simplebroker extensions examples
uv run pytest tests/test_phaselock.py tests/test_runner_validation.py tests/test_runner_error_handling.py -q
uv run pytest tests/test_sqlite_setup_contention.py -q
```

Run full shared SQLite tests before merging:

```bash
uv run pytest
```

CI gates that matter most:

- Windows 3.10, 3.11, 3.12, 3.13, 3.14
- Linux current supported versions
- macOS current supported versions
- Postgres suite if touched files affect shared runner contracts:

```bash
PYTEST_ADDOPTS='-x --maxfail=1' uv run --extra dev ./bin/pytest-pg --fast
```

## Review Checklist

Before opening a PR, check every item:

- Completion sidecars are still empty existence markers.
- No public API was added.
- No runtime dependency was added.
- `_phaselock.py` still imports only stdlib modules and has no SimpleBroker or
  SQLite knowledge.
- No test depends on exact sleep timing when sentinel synchronization would do.
- No broad mocks replace real SQLite setup behavior.
- Missing or zero-byte DBs discard stale fallback status markers.
- Non-empty invalid DBs are not overwritten or silently reinitialized.
- Setup uses a short SQLite busy timeout.
- Setup retry has an elapsed budget.
- Existing retry callers keep their previous behavior unless explicitly opted
  into the setup budget.
- Phases are marked only after successful actions.
- Phase failure leaves only earlier completed phase markers.
- Windows byte-range locking is tested or at least covered by a cross-platform
  lock-file preparation invariant.
- Timeout errors are normal exceptions with useful diagnostics, not harness
  kills.

## Expected Implementation Order

Use this order to keep risk low:

1. Task 1: zero-byte target stale marker red test.
2. Task 2: non-empty invalid target guard test.
3. Implement stale-marker invalidation in `_runner.py`.
4. Task 3: lock file preparation test and implementation.
5. Task 4: elapsed-time retry helper tests and implementation.
6. Task 5: setup busy-timeout test and implementation.
7. Task 6: route setup through bounded retry.
8. Task 7 and Task 8: real multi-process contention tests.
9. Task 9: diagnostics.
10. Task 10: full gates.

Do not implement Tasks 4-6 before Tasks 1-2. Correctness comes before progress.

## What Not To Do

Do not "fix" this by:

- increasing `run_cli()` timeout
- increasing `PhaseLockService.timeout`
- sleeping longer in tests
- marking phases before actions finish
- using file existence of `.setup.lock` as ownership
- putting metadata in completion sidecars
- deleting non-empty DB files
- adding a second setup code path for Windows
- adding backward-compatible legacy `.connection.done` marker support
- mocking away SQLite in the contention tests

## If the Plan Starts Failing

If implementation evidence pushes away from this plan, stop and reassess.

Acceptable reasons to revise:

- a red test proves setup is already bounded and the hang is entirely in normal
  post-setup transactions
- a real Windows run proves `msvcrt.locking` is not involved and lock-file
  preparation adds no value
- a real integration test proves stale markers cannot skip setup even for
  missing or zero-byte targets

Not acceptable reasons:

- "The test passes if we make the timeout bigger."
- "Mocking PhaseLockService is easier."
- "A contentful marker would make debugging nicer."
- "Only Windows 3.13/3.14 failed, so special-case those versions."

If the primary evidence shifts to normal `BEGIN IMMEDIATE` contention after
setup completes, create a separate transaction-progress plan. Do not silently
expand this phaselock plan into a full write-contention redesign.
