# Phaselock Single Fallback Status Cursor Plan

Date: 2026-05-20

Status: draft

Owner: SimpleBroker

## Purpose

Clean up SimpleBroker's fallback setup-marker files while preserving the
external setup lock and the no-content sentinel model.

The examples in this plan use `broker.db`. Current code derives sidecar names
with `Path.with_suffix(".setup.lock")` and
`Path.with_suffix(".setup.status")`, so `broker.db` maps to `broker.setup.*`
sidecars. If the target path is `.broker.db`, the current sidecars are
`.broker.setup.*`.

The target steady-state file set for a fully initialized SQLite database should
look like this when xattrs are unavailable:

```text
broker.db
broker.setup.lock
broker.setup.status.optimization
```

The stable lock file remains the cross-process rendezvous point. The fallback
status file becomes a cursor. Its filename means "this phase and every earlier
phase in the configured phase order completed successfully."

This plan is written for an engineer who is skilled, but new to this codebase
and likely to over-mock tests or invent unnecessary abstractions. Follow the
tasks in order. Use red-green TDD. Keep the change boring.

## Problem Summary

Today, when xattrs are unavailable, `PhaseLockService` writes one empty sidecar
file per completed phase:

```text
broker.setup.lock
broker.setup.status.connection
broker.setup.status.schema-v4
broker.setup.status.optimization
```

This works for correctness, but it leaves visible detritus next to the user
database. The detritus is surprising because each file is only a boolean setup
cache entry. The user still needs the external lock, but does not need one
fallback status file per phase.

We can keep the important properties:

- lock ownership still comes only from an acquired advisory lock
- completion markers remain empty files
- status changes are made with same-directory atomic rename
- no JSON, metadata, timestamps, pid files, or marker contents
- phase actions are marked only after they return successfully

The change is to collapse fallback sidecars into one monotonic status cursor.

## Current Code Map

Primary implementation:

- `simplebroker/_phaselock.py`
  - Standalone phase coordinator.
  - Must remain Python standard library only.
  - Must not import from `simplebroker.*`.
  - Owns advisory file lock acquisition.
  - Owns xattr marker behavior.
  - Owns fallback status sidecar behavior.

SimpleBroker integration:

- `simplebroker/_runner.py`
  - Owns `SetupPhase`.
  - Creates `PhaseLockService` in `_phase_lock_service()`.
  - Converts `SetupPhase.SCHEMA` into `schema-v{SCHEMA_VERSION}`.
  - Calls `PhaseLockService.run_phases()` from `run_exclusive_setup()`.
  - Currently calls phaselock one phase at a time.

Schema orchestration:

- `simplebroker/db.py`
  - Calls runner setup in this order:
    - `SetupPhase.CONNECTION`
    - schema setup through `run_exclusive_setup(SetupPhase.SCHEMA, operation)`
    - `SetupPhase.OPTIMIZATION`
  - Should not need code changes for this plan unless a red integration test
    proves the runner API cannot express the needed phase order.

Tests that currently mention fallback status files:

- `tests/test_phaselock.py`
  - Main unit and subprocess tests for phase lock behavior.
- `tests/test_runner_validation.py`
  - Stale marker invalidation and database validation behavior.
- `tests/test_runner_error_handling.py`
  - Runner setup coordination and cleanup behavior.
- `tests/test_queue_config_defaults.py`
  - User-facing default DB location behavior and marker checks.
- `tests/test_sqlite_setup_contention.py`
  - Multiprocess setup contention behavior.

## Design Decisions

### Keep the Stable Lock File

Do not rename `broker.setup.lock` into a status file.

The lock file path must be stable. Every process must open and lock the same
path. If the lock holder renames the lock file on POSIX, another process can
open a new file at the original lock path and acquire a different lock. On
Windows, renaming a locked file may fail. Both outcomes are wrong.

Correct model:

```text
broker.setup.lock
```

exists as the stable advisory lock rendezvous file. Its existence does not mean
ownership.

### Use One Fallback Status Cursor

In fallback sidecar mode, the status file is the highest completed phase.

Given this ordered phase list:

```text
connection
schema-v4
optimization
```

these files mean:

```text
broker.setup.status.connection
```

`connection` is complete.

```text
broker.setup.status.schema-v4
```

`connection` and `schema-v4` are complete.

```text
broker.setup.status.optimization
```

`connection`, `schema-v4`, and `optimization` are complete.

The file contents remain empty. The phase name lives in the filename.

### Add an Explicit Generic Phase Order

This is the most important implementation detail.

SimpleBroker currently calls `PhaseLockService.run_phases()` one phase at a
time. That means `_phaselock.py` cannot infer that `schema-v4` comes after
`connection` from the `run_phases()` argument alone.

Therefore `PhaseLockService` needs an optional generic phase order. Suggested
constructor argument:

```python
phase_order: Iterable[str] | None = None
```

`_phaselock.py` must treat this as a generic ordered set of phase names. It
must not know about `SetupPhase`, SQLite, schema versions, or SimpleBroker.

`SQLiteRunner._phase_lock_service()` should pass the SimpleBroker setup order:

```python
("connection", f"schema-v{SCHEMA_VERSION}", "optimization")
```

Do not duplicate those strings at every call site. Add one private helper in
`simplebroker/_runner.py` and reuse it.

### Keep Xattrs Independent

Do not change xattr marker semantics unless a test proves it is required.

When xattrs work, `PhaseLockService` may continue to store one xattr per phase.
The cleanup problem is about visible fallback files. Xattrs do not create
visible detritus next to the DB file.

### Backward Compatibility With Existing Sidecars

Existing users may already have multiple fallback files from older versions:

```text
broker.setup.status.connection
broker.setup.status.schema-v4
broker.setup.status.optimization
```

The new implementation must read those files correctly, choose the highest
valid known phase in the configured phase order, and compact the set on the
next locked fallback-status pass. This matters even when no phase action runs:
if all legacy markers already exist, the run will skip every action, but it
should still converge to one cursor after acquiring the setup lock.

Generic non-strict POSIX callers may return on the unlocked fast path when all
phases are already marked. That is acceptable for generic use. SimpleBroker
passes `strict_marker_locking=True`, so its setup path acquires the lock before
trusting fallback status markers and can compact old detritus.

Do not add a migration script. The migration should happen naturally inside
the existing phaselock path.

### Crash-Tolerant Cleanup

Move the cursor forward before deleting older markers.

Correct sequence after a phase action succeeds:

1. Create a temporary empty file in the same directory.
2. `os.replace(tmp, destination_status_path)` for the new cursor.
3. Best-effort unlink every other fallback status file for the same target.

This can briefly leave two status files. A crash between step 2 and step 3 can
also leave two status files. That is acceptable. Read logic must choose the
highest valid known phase in explicit phase order, not assume there is exactly
one file.

Do not delete the old cursor before creating the new cursor. That creates a
crash window where completed setup progress is lost.

## Non-Goals

Do not:

- remove the external lock file
- use lock file existence as ownership
- rename the lock file as a status marker
- write content into status files
- add JSON, metadata, pid, timestamp, hostname, checksums, or schema summaries
- add a background cleanup daemon
- add a public cleanup command
- add a new runtime dependency
- change SQLite setup retry behavior unless a red test proves this plan needs it
- broaden this into general write-throughput or transaction-contention work
- delete or recreate a non-empty user DB because markers look stale

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
back and improve the test or mark that task unnecessary.

### Prefer Real Behavior Over Mocks

Use real temporary files, real `PhaseLockService`, real SQLite databases, real
`Queue` or `SQLiteRunner`, and real subprocesses for contention behavior.

Mocks are acceptable only for tiny deterministic seams that are otherwise hard
to force, such as disabling xattrs. Prefer `PhaseLockService(...,
use_xattrs=False)` in phaselock tests and the existing `_force_status_sidecars`
helper in runner validation tests.

Bad test shape:

```text
Mock PhaseLockService and assert _mark_status_phase was called.
```

Good test shape:

```text
Run real phases with use_xattrs=False, then assert only
broker.setup.status.optimization remains and a second real run skips all
actions.
```

### DRY

Do not scatter this tuple throughout the codebase:

```python
("connection", f"schema-v{SCHEMA_VERSION}", "optimization")
```

Define one private helper in `SQLiteRunner`, for example:

```python
def _setup_phase_marker_names(self) -> tuple[str, ...]:
    ...
```

Use that helper in:

- `_phase_lock_service()`
- any runner-side tests that need the expected status cursor
- any runner helper that reasons about marker names

Inside `_phaselock.py`, centralize fallback cursor logic in small helpers. Do
not reimplement "find highest status cursor" in `has_phase()`,
`_all_marked()`, `_status_paths_for()`, and diagnostics separately.

### YAGNI

Do not add:

- a marker file format
- a marker parser
- a status database
- a lock owner protocol
- cleanup scheduling
- a public phase registry
- a second lock path
- special cases for only macOS, only Linux, or only Windows

The explicit phase order and one cursor marker are enough.

## Invariants

These invariants must hold after the implementation:

1. The advisory lock path is stable for the lifetime of a target DB path.
2. Lock file existence is never treated as lock ownership.
3. A phase is marked complete only after its action returns successfully.
4. Fallback status files remain empty sentinels.
5. In normal steady state, fallback mode leaves at most one status sentinel for
   the target.
6. Crash leftovers with more than one status sentinel are tolerated and compacted
   on the next successful status update.
7. Read logic never uses filename sort order to infer phase progress. It uses
   the explicit phase order.
8. Unknown fallback status marker names are not trusted as completion evidence.
9. Existing old-style per-phase fallback markers are read as the highest valid
   known cursor and compacted after the next locked fallback-status pass.
10. Missing or zero-byte DB targets still discard stale fallback status markers.
11. Non-empty invalid DB targets are not overwritten or silently reinitialized.
12. `_phaselock.py` remains standalone and stdlib-only.
13. Xattr behavior remains green where xattrs are available.
14. SimpleBroker one-phase-at-a-time setup calls still work because the runner
    passes the full setup phase order into `PhaseLockService`.

## Implementation Tasks

### Task 1: Add Focused Cursor Tests in `tests/test_phaselock.py`

Purpose:

Lock down the generic fallback cursor behavior before touching implementation.

Files to touch:

- `tests/test_phaselock.py`

Add tests that use `PhaseLockService(target, use_xattrs=False, ...)` with real
temp files. Do not mock internals.

Test 1A: full run leaves one highest cursor

1. Create `target = tmp_path / "broker.db"` and `target.touch()`.
2. Create a service with fallback sidecars:

   ```python
   service = PhaseLockService(target, use_xattrs=False)
   ```

3. Run three phases in order:

   ```python
   phases = (
       Phase("connection", lambda: calls.append("connection")),
       Phase("schema-v4", lambda: calls.append("schema")),
       Phase("optimization", lambda: calls.append("optimization")),
   )
   ```

4. Assert:
   - all three actions ran
   - `result.completed == ("connection", "schema-v4", "optimization")`
   - `result.skipped == ()`
   - `result.status_paths == (tmp_path / "broker.setup.status.optimization",)`
   - only `broker.setup.status.optimization` exists
   - `broker.setup.status.connection` does not exist
   - `broker.setup.status.schema-v4` does not exist
   - `service.lock_path.exists()`

Expected red behavior today:

- all three per-phase status files exist.

Test 1B: highest cursor skips earlier phases on second run

Use the same service and phases after Test 1A's first run, then call
`run_phases(phases)` again.

Assert:

- no actions run on the second call
- `completed == ()`
- `skipped == ("connection", "schema-v4", "optimization")`
- only the optimization cursor exists

Test 1C: failure leaves only last successful cursor and resumes

1. Run phases where schema raises after connection succeeds.
2. Assert only `broker.setup.status.connection` exists.
3. Re-run with schema and optimization succeeding.
4. Assert connection is skipped, schema and optimization complete, and final
   state is only `broker.setup.status.optimization`.

Test 1D: multiple old-style markers are read and compacted

1. Pre-create old sidecars:
   - `broker.setup.status.connection`
   - `broker.setup.status.schema-v4`
2. Run all three phases with fallback sidecars.
3. Assert connection and schema are skipped.
4. Assert optimization runs.
5. Assert final state is only `broker.setup.status.optimization`.

Test 1E: fully complete old-style markers compact even when no action runs

1. Pre-create old sidecars for all phases:
   - `broker.setup.status.connection`
   - `broker.setup.status.schema-v4`
   - `broker.setup.status.optimization`
2. Use a service with fallback sidecars and strict marker locking:

   ```python
   service = PhaseLockService(
       target,
       use_xattrs=False,
       strict_marker_locking=True,
   )
   ```

3. Run all three phases.
4. Assert all phases are skipped.
5. Assert no action ran.
6. Assert final state is only `broker.setup.status.optimization`.

Test 1F: unknown status marker is not trusted

1. Pre-create `broker.setup.status.future-phase`.
2. Run a real `connection` phase.
3. Assert connection action runs.
4. Assert final state is only `broker.setup.status.connection`.

Focused command:

```bash
uv run pytest tests/test_phaselock.py -q
```

If the whole file is slow while developing, run individual tests with `-k`:

```bash
uv run pytest tests/test_phaselock.py -q -k "cursor"
```

### Task 2: Implement Cursor Helpers in `simplebroker/_phaselock.py`

Purpose:

Make fallback sidecars represent one monotonic cursor while keeping
`_phaselock.py` generic and standalone.

Files to touch:

- `simplebroker/_phaselock.py`

Implementation guidance:

1. Add optional phase order support.

   Suggested constructor argument:

   ```python
   phase_order: Iterable[str] | None = None
   ```

   Store it as a tuple of phase names or `None`.

2. Validate phase order defensively.

   Requirements:

   - phase names must be non-empty
   - phase names must not contain NUL
   - duplicate phase names should raise `ValueError`

   Reuse or extract validation from `attr_key()` so validation rules do not
   drift.

3. Add one helper that determines the effective order.

   Suggested shape:

   ```python
   def _status_phase_order_for(self, phases: tuple[Phase, ...] = ()) -> tuple[str, ...]:
       ...
   ```

   Rules:

   - if `self._phase_order` is set, use it
   - otherwise use the names from the current `run_phases()` tuple
   - if neither exists, fall back to exact-marker behavior for `has_phase()`

4. Add one helper that finds the current fallback cursor.

   Suggested shape:

   ```python
   def _status_cursor_phase(self, order: tuple[str, ...]) -> str | None:
       ...
   ```

   Rules:

   - inspect existing files using `_status_path_for_phase(phase_name)`
   - do not sort filenames
   - choose the highest phase by index in `order`
   - ignore unknown status files for completion

5. Update `_has_status_phase()`.

   It should return true when:

   - exact fallback marker exists and no phase order is available, or
   - a known cursor exists at the same or later index than the queried phase

   Suggested private signature:

   ```python
   def _has_status_phase(
       self,
       phase_name: str,
       *,
       order: tuple[str, ...] | None = None,
   ) -> bool:
       ...
   ```

6. Update `_all_marked()`, `_run_status_phases()`, `_status_paths_for()`, and
   `_phase_diagnostics()` to use the shared cursor helpers.

7. Update `_mark_status_phase()`.

   Suggested signature:

   ```python
   def _mark_status_phase(
       self,
       phase_name: str,
       *,
       order: tuple[str, ...] | None = None,
   ) -> None:
       ...
   ```

   Required sequence:

   - create temp file next to the status files
   - `os.replace(tmp_path, destination)`
   - best-effort unlink every file from `_status_paths()` except destination

   Do not delete old markers before the new cursor exists.

8. Compact fallback status markers after a locked status run, even if no phase
   action ran.

   Suggested behavior:

   - after `_run_status_phases()` finishes under the acquired lock, find the
     highest valid known cursor for the effective order
   - if one exists, unlink every other status file for the target
   - do not create a new marker if there is no known cursor
   - keep cleanup best-effort

   This is what migrates the fully complete old-style state where all phases
   are skipped and no `_mark_status_phase()` call happens.

9. Keep `status_path_for_phase(phase_name)` as a candidate cursor path helper.

   Existing tests and runner code use this helper. Do not remove it in this
   plan. Update docstring if needed:

   ```text
   Return the fallback status cursor path for a phase.
   ```

10. Keep `discard_status_markers()` deleting all fallback status files.

   It should continue to remove:

   - old per-phase markers
   - new cursor marker
   - leftover temp marker files matching the same prefix

What not to do:

- Do not import `SCHEMA_VERSION`.
- Do not import `SetupPhase`.
- Do not use filename lexical order.
- Do not write marker contents.
- Do not require the caller to pass a new public cleanup command.

Focused command:

```bash
uv run pytest tests/test_phaselock.py -q -k "cursor or status_marker or status"
```

Then run:

```bash
uv run pytest tests/test_phaselock.py -q
```

### Task 3: Preserve Existing Xattr Behavior

Purpose:

Make sure the fallback cleanup work does not break xattr users.

Files to touch:

- `tests/test_phaselock.py`
- `simplebroker/_phaselock.py` only if the tests fail

Test updates:

Review existing real-xattr tests:

- `test_real_xattr_runtime_marks_and_skips_completed_phases`
- `test_real_xattr_runtime_resumes_from_last_marked_phase`
- `test_failure_while_lock_held_leaves_partial_xattrs_and_blocks_contenders`

These tests may not need structural changes. Keep their meaning:

- xattr phase markers remain per-phase
- phase resume works
- fallback status files are not required when xattrs work

If you add assertions, keep them conditional on xattrs actually being supported
by the runtime/filesystem. Do not fake xattrs for the main coverage.

Focused command:

```bash
uv run pytest tests/test_phaselock.py -q -k "xattr"
```

### Task 4: Wire SimpleBroker's Phase Order Through `SQLiteRunner`

Purpose:

Make one-phase-at-a-time runner calls compatible with cursor semantics.

Files to touch:

- `simplebroker/_runner.py`

Implementation guidance:

1. Add a private phase-order helper.

   Suggested shape:

   ```python
   def _setup_phase_marker_names(self) -> tuple[str, ...]:
       return tuple(self._phase_marker_name(phase) for phase in self._setup_phase_order())

   def _setup_phase_order(self) -> tuple[SetupPhase, ...]:
       return (
           SetupPhase.CONNECTION,
           SetupPhase.SCHEMA,
           SetupPhase.OPTIMIZATION,
       )
   ```

   If you do not need both helpers, keep only the marker-name helper. Do not
   over-abstract.

2. Update `_phase_lock_service()`.

   Pass the full marker-name order:

   ```python
   phase_order=self._setup_phase_marker_names()
   ```

3. Do not change `db.py` unless a red test proves the runner helper is not
   enough.

Why this matters:

`run_exclusive_setup(SetupPhase.SCHEMA, operation)` currently calls
`run_phases()` with only `schema-v4`. Without the full phase order,
`PhaseLockService.has_phase("connection")` cannot know that a
`schema-v4` or `optimization` cursor implies connection completed.

Focused command:

```bash
uv run pytest tests/test_runner_validation.py tests/test_runner_error_handling.py -q
```

### Task 5: Update Runner and Queue Tests for Cursor Semantics

Purpose:

Make existing tests assert the new file shape instead of the old detritus.

Files to touch:

- `tests/test_runner_validation.py`
- `tests/test_runner_error_handling.py`
- `tests/test_queue_config_defaults.py`
- maybe `tests/test_sqlite_setup_contention.py`

Guidance:

1. Update helper names if needed.

   Existing helper:

   ```python
   _write_status_markers(db_path)
   ```

   It currently writes all old per-phase files. Keep it for migration/stale
   marker tests, but rename only if the tests become unclear. Good names:

   - `_write_legacy_status_markers`
   - `_write_status_cursor`

   Do not make one helper do both.

2. Add or update a runner-level test for final file shape.

   Suggested test location:

   - `tests/test_runner_validation.py` if using `Queue`
   - `tests/test_runner_error_handling.py` if using `SQLiteRunner`

   Test shape:

   - force fallback sidecars with `_force_status_sidecars(monkeypatch)`
   - create a real `Queue` using `tmp_path / "broker.db"`
   - write and read one message
   - assert exactly one fallback status file remains
   - assert it is `broker.setup.status.optimization`
   - assert the lock file exists

3. Update cleanup tests.

   `test_cleanup_marker_files_preserves_shared_setup_sidecars` should still
   prove cleanup does not delete shared setup files for real DB paths. It can
   include legacy sidecars because cleanup must not unlink shared status files
   for real DBs merely because a handle closed.

   `test_cleanup_marker_files_still_cleans_mock_sidecars` should keep proving
   mock-path cleanup behavior. Use the new cursor path for the main case.

4. Update queue config tests.

   Tests that call `PhaseLockService(...).has_phase(f"schema-v{SCHEMA_VERSION}")`
   may need the same phase order as the runner. Prefer asserting through the
   filesystem only where the user-visible behavior is file shape. If you keep
   `has_phase()`, construct the service with the same phase order used by
   `SQLiteRunner`.

   Do not duplicate the order tuple in many tests. A tiny test helper is fine.

Focused commands:

```bash
uv run pytest tests/test_runner_validation.py -q
uv run pytest tests/test_runner_error_handling.py -q
uv run pytest tests/test_queue_config_defaults.py -q
uv run pytest tests/test_sqlite_setup_contention.py -q
```

### Task 6: Add Migration Coverage for Real SimpleBroker Setup

Purpose:

Prove that existing users with old per-phase fallback files naturally converge
to one cursor file without losing setup correctness.

Files to touch:

- `tests/test_runner_validation.py`

Test shape:

1. Force fallback sidecars.
2. Create a valid SimpleBroker DB through a real `Queue` write.
3. Close the queue.
4. Remove any new cursor if needed and pre-create old legacy status files:
   - `broker.setup.status.connection`
   - `broker.setup.status.schema-v{SCHEMA_VERSION}`
   - `broker.setup.status.optimization`
5. Open the same DB with `Queue` and perform another write.
6. Assert the operation succeeds.
7. Assert final fallback state is exactly:
   - `broker.setup.status.optimization`

Important:

This is not a phaselock unit test. It verifies runner integration, one-phase
setup calls, and real database behavior.

Do not mock `SQLiteRunner.run_exclusive_setup`.

Focused command:

```bash
uv run pytest tests/test_runner_validation.py -q -k "legacy or cursor"
```

### Task 7: Preserve Stale Marker Safety Tests

Purpose:

Make sure cleanup does not reopen old correctness holes.

Files to touch:

- `tests/test_runner_validation.py`
- `simplebroker/_runner.py` only if tests fail

Existing tests to preserve:

- `test_zero_byte_database_discards_stale_status_markers`
- `test_nonempty_invalid_database_with_stale_markers_fails_without_reinit`
- `test_connection_marker_check_does_not_open_sqlite_connection`

Required behavior:

- Missing or zero-byte DB target: stale fallback status files are discarded and
  setup runs.
- Non-empty invalid DB target: setup fails clearly and does not overwrite user
  bytes.
- Existing connection marker check remains passive for valid existing DBs.

Adjust helper setup for cursor mode, but keep the assertions about safety.

Focused command:

```bash
uv run pytest tests/test_runner_validation.py -q
```

### Task 8: Preserve Lock Contention Semantics

Purpose:

Make sure the cleaner status files do not weaken setup serialization.

Files to touch:

- `tests/test_phaselock.py`
- `tests/test_runner_error_handling.py`
- `tests/test_sqlite_setup_contention.py`
- `simplebroker/_phaselock.py` only if tests fail

Existing tests to preserve:

- `test_no_xattr_existing_status_marker_does_not_bypass_held_lock`
- `test_no_xattr_waiter_does_not_skip_when_phase_marked_while_lock_is_held`
- `test_no_xattr_non_strict_waiter_skips_when_phase_marked_while_lock_is_held`
- `test_process_local_lock_serializes_threads`
- `test_lock_timeout_when_another_process_holds_lock`
- `test_run_exclusive_setup_marker_does_not_bypass_held_lock`
- `test_concurrent_first_writes_serialize_setup`

Update marker creation to use the cursor semantics. For example, if a test
means "schema is complete", create the `schema-v{SCHEMA_VERSION}` cursor, not
every old per-phase marker unless the test is explicitly about migration.

Focused commands:

```bash
uv run pytest tests/test_phaselock.py -q -k "lock or waiter or process_local"
uv run pytest tests/test_runner_error_handling.py -q -k "marker_does_not_bypass_held_lock"
uv run pytest tests/test_sqlite_setup_contention.py -q
```

### Task 9: Improve Diagnostics Without Adding Marker Content

Purpose:

Keep timeout errors useful after the marker model changes.

Files to touch:

- `simplebroker/_phaselock.py`
- `tests/test_phaselock.py`

Current `_phase_diagnostics()` reports:

- target
- target existence
- xattrs availability
- marked phases
- missing phases
- status files

Update it, if needed, to also make cursor state clear:

- current fallback cursor phase, if known
- current fallback cursor file, if known
- all fallback status files found

Do not write diagnostics into sidecar files. Diagnostics belong in exception
messages and test failure output.

Focused command:

```bash
uv run pytest tests/test_phaselock.py -q -k "timeout"
```

### Task 10: Run the Full Local Gate for the Touched Area

Purpose:

Catch behavior that unit tests miss.

Run these commands before calling the implementation done:

```bash
uv run pytest tests/test_phaselock.py -q
uv run pytest tests/test_runner_validation.py -q
uv run pytest tests/test_runner_error_handling.py -q
uv run pytest tests/test_queue_config_defaults.py -q
uv run pytest tests/test_sqlite_setup_contention.py -q
```

Then run the broader suite if local time allows:

```bash
uv run pytest -q
```

If `uv` is not available in the environment, use:

```bash
python -m pytest ...
```

Do not hide timing failures by increasing test sleeps or lock timeouts. If a
test becomes flaky, investigate the synchronization primitive in the test.

## Implementation Notes for `_phaselock.py`

The following helper structure is one reasonable design. It is guidance, not a
mandatory exact patch.

Suggested private helpers:

```python
def _validate_phase_name(self, phase_name: str) -> None:
    ...

def _validate_phase_order(self, phase_order: Iterable[str]) -> tuple[str, ...]:
    ...

def _status_phase_order_for(
    self,
    phases: tuple[Phase, ...] = (),
) -> tuple[str, ...]:
    ...

def _status_cursor_phase(
    self,
    order: tuple[str, ...],
) -> str | None:
    ...

def _status_cursor_path(
    self,
    order: tuple[str, ...],
) -> Path | None:
    ...

def _discard_obsolete_status_markers(self, keep: Path) -> None:
    ...
```

Keep names short and local to the concepts. Do not build a `StatusCursor`
class unless the code becomes clearer with it. It probably will not.

Important details:

- Use `_status_path_for_phase(phase_name)` when checking known phases.
- Use `_status_paths()` only for cleanup and diagnostics.
- Use explicit order indexes, not filename sort.
- Make cleanup best-effort with `contextlib.suppress(OSError)`.
- Keep `os.replace()` as the atomic publish step.
- Keep temp files in the same directory as the status destination.
- Ensure temp file names still match the status prefix so
  `discard_status_markers()` can remove abandoned temp files.

## Implementation Notes for `_runner.py`

`SQLiteRunner` should be the only SimpleBroker-specific place that knows this
ordered marker list:

```text
connection
schema-v{SCHEMA_VERSION}
optimization
```

Why not put this in `_phaselock.py`:

`_phaselock.py` is a reusable generic primitive. It should not know about
SQLite setup, schema versioning, or SimpleBroker's phase enum.

Why not put this in `db.py`:

The runner owns `SetupPhase` marker names and the `PhaseLockService`
construction. Passing the order there keeps the lock/marker integration in one
place.

## Test Design Warnings

Avoid these mistakes:

- Do not mock `os.replace()` and call that a passing atomicity test. Use real
  files and assert observable file state.
- Do not assert private helper calls. Assert phase actions, skipped phases, and
  filesystem state.
- Do not sort status filenames and compare the last one. That bakes in the bug
  the implementation must avoid.
- Do not write tests that depend on xattrs existing on the developer's machine.
  Real xattr tests should skip when unsupported; fallback tests should force
  fallback.
- Do not make tests pass by deleting all status files after setup. The final
  cursor is still needed as the fallback skip hint.
- Do not add sleep-based synchronization when an event, ready file, or process
  handshake is possible.

## Expected Final File States

Fresh fallback run after connection only:

```text
broker.db
broker.setup.lock
broker.setup.status.connection
```

Fresh fallback run after schema:

```text
broker.db
broker.setup.lock
broker.setup.status.schema-v4
```

Fresh fallback run after optimization:

```text
broker.db
broker.setup.lock
broker.setup.status.optimization
```

Crash-tolerated temporary state:

```text
broker.db
broker.setup.lock
broker.setup.status.connection
broker.setup.status.schema-v4
```

This temporary state is acceptable only as a leftover from a crash or between
publishing the new cursor and deleting the old cursor. The next successful
status update should compact it back to one cursor.

Unknown status file:

```text
broker.setup.status.future-phase
```

This file is not trusted as completion evidence. It may be removed during the
next successful status update for that target.

## Acceptance Checklist

The implementation is done only when all of these are true:

- Fallback mode creates one steady-state status cursor, not one marker per
  completed phase.
- Existing legacy per-phase markers are interpreted and compacted, including
  when every phase is skipped and no action runs.
- The lock file remains stable and is never renamed into status.
- In fallback mode, `PhaseLockService.has_phase("connection")` returns true
  when the configured phase order has an `optimization` cursor.
- In fallback mode, `PhaseLockService.has_phase("optimization")` returns false
  when the cursor is only `schema-v4`.
- Unknown status files are not trusted.
- Phase failure leaves the cursor at the last successful phase.
- Re-running after failure resumes from the cursor.
- Zero-byte targets discard stale fallback markers and run setup.
- Non-empty invalid targets fail without being overwritten.
- Xattr tests still pass or skip for real xattr support.
- Multiprocess setup contention tests still pass.
- `_phaselock.py` remains stdlib-only and has no SimpleBroker imports.
- No new runtime dependency is added.

## What Not To Do

Do not "fix" this by:

- deleting all marker files after setup
- storing phase names inside one `.setup.status` file
- renaming `.setup.lock` into `.setup.status.<phase>`
- using mtime to decide the latest status marker
- using lexical sort to decide the latest status marker
- adding `.done` files again
- making cleanup run on every `Queue.close()`
- trusting unknown phase names
- special-casing only the user's current phase names in `_phaselock.py`
- changing setup order to make the cursor easier
- increasing timeouts to make contention tests pass

## Useful References

Check these docs only as needed:

- Python `os.replace`: atomic same-filesystem replacement.
- Python `pathlib.Path.glob`: cleanup and diagnostics for status sidecars.
- Python `urllib.parse.quote`: existing filename escaping for phase names.
- Python `fcntl.flock`: POSIX advisory lock behavior.
- Python `msvcrt.locking`: Windows byte-range lock behavior.
- Pytest `tmp_path`: real filesystem tests.
- Pytest subprocess patterns: use ready files or events instead of sleeps.

Do not add new dependencies after reading those docs.

## Self-Review Findings Applied

I reviewed the plan for hidden ambiguity and corrected these points before
marking it ready to implement:

1. Cursor semantics cannot work with the current runner's one-phase-at-a-time
   calls unless `PhaseLockService` receives a full phase order. The plan now
   requires a generic `phase_order` argument and runner wiring.
2. Renaming the lock file into a status file would break advisory-lock
   rendezvous semantics. The plan keeps the lock path stable.
3. Deleting the old cursor before publishing the new cursor would create a
   crash window that loses progress. The plan requires publish first, cleanup
   second.
4. Assuming exactly one status file exists is not crash-tolerant and does not
   migrate old per-phase files. The plan requires read logic to handle multiple
   files and choose the highest valid known phase by explicit order.
5. Filename sorting would be wrong because phase order is semantic, not
   lexical. The plan explicitly bans sort-based phase inference.
6. Updating only `_phaselock.py` would leave runner and queue tests with stale
   assumptions. The plan names all known test files that inspect status files.
7. Migrating only after `_mark_status_phase()` would miss fully complete legacy
   marker sets where every action is skipped. The plan now requires compaction
   after a locked fallback-status pass, even when no phase action ran.

If implementation evidence pushes the work away from "stable lock plus one
fallback status cursor," stop and reassess before expanding scope.
