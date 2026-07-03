# Phaselock Atomic Status File Plan

Date: 2026-05-20

Status: draft

Owner: SimpleBroker

Supersedes:

- `docs/plans/2026-05-20-phaselock-single-fallback-status-cursor-plan.md`

## Purpose

Move SimpleBroker's setup coordination files to this model:

```text
broker.db
broker.lock
broker.status
```

`broker.lock` is the stable advisory lock rendezvous file. Its existence means
nothing. Only an acquired advisory lock means ownership.

`broker.status` is the fallback completion cache when xattrs are unavailable.
It is a single atomically replaced file containing the independent set of
completed phase names.

The goal is cleaner filesystem state without weakening the current correctness
model. The current one-file-per-phase fallback model has strong semantics:
`schema-v4` being marked complete is independent from `optimization` being
marked complete. This plan preserves that by storing an explicit completed
phase set in one file.

## Naming Rules

Current code derives sidecars with `Path.with_suffix(...)`.

For `broker.db`:

```text
current lock:   broker.setup.lock
current status: broker.setup.status.<phase>
new lock:       broker.lock
new status:     broker.status
```

For `.broker.db`:

```text
current lock:   .broker.setup.lock
current status: .broker.setup.status.<phase>
new lock:       .broker.lock
new status:     .broker.status
```

Implementation target:

- `PhaseLockService(...).lock_path == target.with_suffix(".lock")`
- `PhaseLockService(...).status_base_path == target.with_suffix(".status")`
- SimpleBroker's `SQLiteRunner._phase_lock_service()` uses those names.

Keep the name `status_base_path` in the first implementation if that avoids
churn. It will now point at the single status file, not a prefix for per-phase
sidecars. Add a small doc comment if that name becomes misleading.

## Problem Summary

Today, fallback status mode writes one empty sidecar per completed setup phase:

```text
broker.setup.lock
broker.setup.status.connection
broker.setup.status.schema-v4
broker.setup.status.optimization
```

This is correct but noisy. A previous cursor-file proposal reduced the noise,
but made marker meaning depend on phase order: `optimization` implied all prior
phases. That is a weaker correctness model.

This plan uses one contentful status file instead:

```text
broker.lock
broker.status
```

Example `broker.status` content:

```text
connection
schema-v4
optimization
```

Each line is an independent completion fact. `optimization` does not imply
`schema-v4`; both must be listed if both are complete.

## Core Invariants

These are non-negotiable:

1. The lock path is stable. Do not rename `broker.lock` into any status file.
2. Lock file existence is never treated as ownership.
3. A phase is marked complete only after its action returns successfully.
4. Status file updates are never in-place writes.
5. Status publication is temp-file write, flush, `os.replace()`.
6. Readers see either the old complete status file or the new complete status
   file.
7. The status file is a cache of completed setup work, not proof that the DB is
   valid.
8. Missing or zero-byte DB targets discard stale fallback status.
9. Non-empty invalid DB targets fail clearly. They are not overwritten or
   silently reinitialized because status looks stale.
10. `_phaselock.py` remains standalone and stdlib-only.
11. Xattr behavior remains independent and still uses one xattr per phase.
12. Old per-phase fallback markers are not trusted. They are cleaned up after
    the new setup/validation path has run.

## Lock Rename Compatibility Boundary

Changing from `broker.setup.lock` to `broker.lock` is a lock-path migration.

That means old SimpleBroker code and new SimpleBroker code will not coordinate
with each other unless extra dual-lock compatibility is added. This plan does
not add dual-lock compatibility because doing so keeps the old lock file alive
as a coordination object and moves away from the requested file model.

Assumption:

- Mixed old-version and new-version processes using the same DB at the same
  time are unsupported for this migration.

If mixed-version concurrency must be supported, stop and replace this plan with
a dual-lock transition plan. Do not silently implement half of a dual-lock
scheme. A correct dual-lock transition would need a fixed acquisition order,
tests with old and new lock paths, and a later cleanup release. That is a
materially different direction.

New code treats `broker.setup.lock` as legacy detritus, not as a coordination
primitive. It does not acquire it, does not trust it, and may remove it after
the new `broker.lock` path has successfully coordinated setup/status work.
That cleanup is only sound under the assumption above: mixed old-version and
new-version processes are unsupported.

## Status File Format

Use the simplest useful format:

```text
connection
schema-v4
optimization
```

Rules:

- UTF-8 text.
- One phase name per line.
- Final newline is optional but writers should include one.
- Blank lines are ignored.
- Duplicate phase names are collapsed.
- Phase names must pass the same validation as xattr phase names:
  - non-empty after stripping the trailing newline
  - no NUL
  - no path separators are interpreted because names are content, not paths
- The status file has no JSON, version object, pid, timestamp, checksum,
  hostname, schema summary, lock owner, or comment syntax.

Why no JSON:

- It adds a parser contract and migration surface without adding correctness.
- The data is only a set of strings.

Why content is safe:

- The file is never modified in place.
- Writers publish by replacing the entire file atomically.
- A crash can leave an abandoned temp file, but should not leave a partially
  written `broker.status`.

## Atomic Write Protocol

When a phase action succeeds and fallback status mode is active:

1. Read the currently trusted completed phase set.
2. Add the successful phase name.
3. Write the complete set to a temp file in the same directory.
4. Flush the temp file.
5. Best-effort `os.fsync(temp_fd)`.
6. `os.replace(temp_path, broker.status)`.
7. Best-effort fsync the parent directory on platforms where that is possible.
8. Best-effort remove abandoned temp files and old legacy sidecars, including
   `broker.setup.lock` and `broker.setup.status.<phase>`.

Never do this:

```python
with open(status_path, "w") as f:
    f.write(...)
```

That is an in-place truncate/write update and is not acceptable for this plan.

## Malformed Status Policy

`broker.status` is a cache. It is not the database.

If the status file is missing, empty, unreadable, invalid UTF-8, too large, or
contains invalid phase names, do not trust it as completion evidence.

Fallback behavior:

- Treat malformed or unreadable status as an empty completed set.
- Run setup phases under the advisory lock.
- Replace the malformed status file only after a phase action succeeds.
- Include status parse/read errors in timeout diagnostics where practical.

Why not fail immediately:

- A cache file should not permanently brick a valid database.
- Setup phases are required to be idempotent.
- Runner validation still protects non-empty invalid DB files.

Do not silently treat malformed content as completed phases. The only safe
interpretation is "no trusted fallback completion facts."

Put a small max-size guard on the status file, for example 64 KiB. A normal
status file has only a few lines. If the file is larger than the guard, treat it
as malformed and do not trust it.

## Legacy Cleanup

Existing users may have old files:

```text
broker.setup.lock
broker.setup.status.connection
broker.setup.status.schema-v4
broker.setup.status.optimization
```

Cleanup rules:

- New code uses `broker.lock` for locking.
- New code does not acquire or trust `broker.setup.lock`.
- New code does not use old per-phase status sidecars as completion evidence.
- If old per-phase status sidecars exist and `broker.status` is missing or does
  not mark a requested phase, new code reruns the normal setup/validation path.
- After a successful locked setup pass, new code best-effort deletes
  `broker.setup.lock`, old `broker.setup.status.<phase>` files, and old status
  temp files. This applies to both xattr-backed and fallback-backed runs. In
  fallback mode, it applies whether phases ran and published `broker.status`, or
  all requested phases were already present in a valid `broker.status`.
- New code never treats old per-phase sidecars as lock ownership.
- New code never uses old sidecars or the old lock to skip setup for missing or
  zero-byte DBs.

This intentionally sacrifices old fallback skip hints during the migration. The
cost is bounded: setup phases are idempotent, and redoing validation/setup once
is safer than trusting stale external cache files.

`broker.setup.lock` is not a completion marker and not a lock for new code. It
is included in best-effort legacy cleanup after the new locked path succeeds.

## Current Code Map

Primary implementation:

- `simplebroker/_phaselock.py`
  - Standalone phase coordinator.
  - Must remain stdlib-only.
  - Must not import `simplebroker.*`.
  - Owns advisory lock acquisition.
  - Owns xattr phase markers.
  - Owns fallback status file read/write and legacy status cleanup.

SimpleBroker integration:

- `simplebroker/_runner.py`
  - Owns `SetupPhase`.
  - Creates `PhaseLockService` in `_phase_lock_service()`.
  - Converts schema phase to `schema-v{SCHEMA_VERSION}`.
  - Currently passes `lock_suffix=".setup.lock"` and
    `status_suffix=".setup.status"`.
  - Must move to `lock_suffix=".lock"` and `status_suffix=".status"` or rely on
    new `PhaseLockService` defaults.

Schema orchestration:

- `simplebroker/db.py`
  - Calls setup in order:
    - connection
    - schema
    - optimization
  - Should not need code changes for this plan unless a red test proves it.

Tests that inspect status/lock files:

- `tests/test_phaselock.py`
- `tests/test_runner_validation.py`
- `tests/test_runner_error_handling.py`
- `tests/test_queue_config_defaults.py`
- `tests/test_sqlite_setup_contention.py`

## Engineering Rules

### Red-Green TDD

For each task:

1. Add a focused failing test.
2. Run only that test and confirm it fails for the expected reason.
3. Implement the smallest code change.
4. Run the focused test again.
5. Run the local gate for the touched area.
6. Refactor only after tests are green.

If a proposed red test passes on current code, improve the test or mark that
specific task unnecessary. Do not invent unrelated work.

### Prefer Real Behavior Over Mocks

Use real temp files, real `PhaseLockService`, real SQLite databases, real
`Queue` or `SQLiteRunner`, and real subprocesses for contention behavior.

Mocks are acceptable only for deterministic environment seams:

- forcing xattrs unavailable
- simulating an unreadable status file if the platform makes permissions hard

Bad test:

```text
Mock _write_status_file and assert it was called.
```

Good test:

```text
Run real phases with use_xattrs=False, read broker.status from disk, and assert
the completed phase names match the actions that actually succeeded.
```

### DRY

Do not write separate status parsers in tests, runner, and phaselock.

Keep status parsing/writing in `_phaselock.py`. Tests may read text from
`broker.status` to assert final state, but production code outside
`_phaselock.py` should ask `PhaseLockService`.

Do not scatter legacy path construction. Add private helpers in
`_phaselock.py`, for example:

```python
def _legacy_status_base_path(self) -> Path: ...
def _legacy_status_path_for_phase(self, phase_name: str) -> Path: ...
```

### YAGNI

Do not add:

- JSON
- a status schema version
- checksums
- pid files
- lock owner files
- a cleanup daemon
- a public migration command
- a background repair thread
- a new dependency
- a public phase registry
- dual-lock compatibility unless the migration assumption changes

The system needs one stable lock and one atomically replaced fallback cache.

## Implementation Tasks

### Task 1: Add Status File Unit Tests

Purpose:

Pin down the new fallback status semantics before changing implementation.

Files to touch:

- `tests/test_phaselock.py`

Use real temp files. Use `PhaseLockService(target, use_xattrs=False)`.

Test 1A: full fallback run writes one status file

1. Create `target = tmp_path / "broker.db"` and `target.touch()`.
2. Run phases:
   - `connection`
   - `schema-v4`
   - `optimization`
3. Assert all actions ran.
4. Assert `service.lock_path == tmp_path / "broker.lock"`.
5. Assert `service.lock_path.exists()`.
6. Assert `service.status_base_path == tmp_path / "broker.status"`.
7. Assert `broker.status` exists.
8. Assert `broker.status` content has exactly:

   ```text
   connection
   schema-v4
   optimization
   ```

9. Assert no `broker.status.<phase>` files exist.
10. Assert no `broker.setup.status.<phase>` files exist.

Expected red behavior today:

- lock path is `broker.setup.lock`
- status paths are per-phase files
- no `broker.status` content file exists

Test 1B: second run skips from status content

1. Run Test 1A's first setup.
2. Run the same phases again with actions that append to a fresh list.
3. Assert no second-run actions ran.
4. Assert `completed == ()`.
5. Assert all phases are in `skipped`.
6. Assert status content is unchanged.

Test 1C: failure writes only successful phases

1. Run `connection`, then make `schema-v4` raise `RuntimeError("boom")`.
2. Assert `broker.status` contains only `connection`.
3. Re-run with schema and optimization succeeding.
4. Assert connection is skipped.
5. Assert schema and optimization complete.
6. Assert final status content contains all three phases.

Test 1D: status facts are independent, not cursor-based

1. Pre-create `broker.status` with:

   ```text
   connection
   optimization
   ```

2. Run phases `connection`, `schema-v4`, `optimization`.
3. Assert only schema action runs.
4. Assert final status contains all three phases.

This test protects the main correctness advantage over the cursor plan.

Test 1E: malformed status is not trusted

1. Pre-create `broker.status` with invalid content, such as invalid UTF-8 bytes
   or a NUL-containing phase name.
2. Run a real `connection` phase.
3. Assert connection action runs.
4. Assert final status contains only `connection`.

Do not assert a hard failure for malformed cache content.

Focused command:

```bash
uv run pytest tests/test_phaselock.py -q -k "status"
```

### Task 2: Implement Status Parser and Atomic Writer

Purpose:

Create the low-level fallback status file machinery in `_phaselock.py`.

Files to touch:

- `simplebroker/_phaselock.py`

Implementation guidance:

1. Change defaults:

   ```python
   lock_suffix: str = ".lock"
   status_suffix: str = ".status"
   ```

2. Keep `self.lock_path = self.target.with_suffix(lock_suffix)`.
3. Keep or rename `self.status_base_path = self.target.with_suffix(status_suffix)`.
4. Add a small status size limit constant near the top of the module:

   ```python
   _MAX_STATUS_BYTES = 64 * 1024
   ```

5. Add private parser helpers.

   Suggested shape:

   ```python
   def _read_status_phases(self) -> tuple[set[str], str | None]:
       ...
   ```

   Return the completed phase set plus an optional diagnostic string. If the
   file is missing or malformed, return an empty set and a diagnostic.

6. Add private writer helper.

   Suggested shape:

   ```python
   def _write_status_phases(self, phases: Iterable[str]) -> None:
       ...
   ```

   Required behavior:

   - validate phase names
   - write all phase names to a same-directory temp file
   - include a trailing newline when there is at least one phase
   - flush
   - best-effort fsync temp file
   - `os.replace(tmp_path, self.status_base_path)`
   - best-effort fsync parent directory
   - best-effort unlink temp on failure

7. Extract phase-name validation so xattrs and status content share rules.

   Suggested helper:

   ```python
   def _validate_phase_name(phase_name: str) -> None:
       ...
   ```

8. Do not write directly to `broker.status`.
9. Do not parse status content outside `_phaselock.py`.

Focused command:

```bash
uv run pytest tests/test_phaselock.py -q -k "status"
```

### Task 3: Replace Per-Phase Fallback Markers With Status Content

Purpose:

Make `run_phases()` use the new contentful status file when xattrs are
unavailable.

Files to touch:

- `simplebroker/_phaselock.py`
- `tests/test_phaselock.py`

Implementation guidance:

1. Update `has_phase()`.

   Fallback mode should return true only when the phase name appears in the
   parsed `broker.status` set. It must not return true because an old
   `broker.setup.status.<phase>` file exists.

2. Update `_all_marked()`.

   In fallback mode, all requested phases must be present in the completed set.

3. Update `_run_status_phases()`.

   Suggested algorithm under the acquired lock:

   - read status phase set
   - for each phase in order:
     - if present in completed set: append to `skipped`
     - otherwise run action
     - after action returns, add phase name to completed set
     - atomically write full completed set to `broker.status`
     - append to `completed`
   - after the loop, best-effort cleanup old status sidecars and temp files

4. Update `PhaseRunResult.status_paths`.

   In fallback mode, return `(self.status_base_path,)` when the status file
   exists. Do not return one path per phase.

5. Keep xattr mode behavior as-is.

6. Preserve strict marker locking behavior.

   SimpleBroker currently passes `strict_marker_locking=True`; marker checks in
   setup must still happen after acquiring the advisory lock.

Focused command:

```bash
uv run pytest tests/test_phaselock.py -q
```

### Task 4: Add Legacy Sidecar Cleanup Tests

Purpose:

Ensure old per-phase fallback files are ignored as completion facts and cleaned
after the new status path succeeds.

Files to touch:

- `tests/test_phaselock.py`

Test 4A: legacy sidecars are ignored, then cleaned

1. Pre-create:

   ```text
   broker.setup.status.connection
   broker.setup.status.schema-v4
   ```

2. Run phases `connection`, `schema-v4`, `optimization`.
3. Assert connection, schema, and optimization actions all run.
4. Assert `broker.status` contains all three phases.
5. Assert old `broker.setup.status.connection` and
   `broker.setup.status.schema-v4` are gone.

Test 4B: sparse legacy sidecars are also ignored

1. Pre-create:

   ```text
   broker.setup.status.optimization
   ```

2. Run phases `connection`, `schema-v4`, `optimization`.
3. Assert all three actions run.
4. Assert final `broker.status` contains all three phases.
5. Assert old `broker.setup.status.optimization` is gone.

Test 4C: valid new status cleans legacy sidecars even when actions skip

1. Pre-create `broker.status` with all requested phases.
2. Pre-create old sidecars:
   - `broker.setup.status.connection`
   - `broker.setup.status.schema-v4`
3. Use `PhaseLockService(..., use_xattrs=False, strict_marker_locking=True)`.
4. Run phases `connection`, `schema-v4`, `optimization`.
5. Assert all phases are skipped.
6. Assert no actions run.
7. Assert old sidecars are gone.

Test 4D: old lock file is ignored, then cleaned

1. Pre-create `broker.setup.lock`.
2. Run a fallback phase.
3. Assert `broker.setup.lock` is gone.
4. Assert `broker.lock` exists.

This proves new code does not keep the old lock path as a coordination file.
Mixed old/new versions are already unsupported by this plan.

Focused command:

```bash
uv run pytest tests/test_phaselock.py -q -k "legacy"
```

### Task 5: Wire New Names Through `SQLiteRunner`

Purpose:

Make SimpleBroker use `broker.lock` and `broker.status`.

Files to touch:

- `simplebroker/_runner.py`

Implementation guidance:

1. Update `_phase_lock_service()`.

   Either remove explicit suffix arguments and rely on new defaults, or pass:

   ```python
   lock_suffix=".lock",
   status_suffix=".status",
   ```

   Prefer passing them explicitly in `_runner.py` so SimpleBroker's file naming
   is obvious at the integration point.

2. Update `_should_cleanup_tracked_file()`.

   It currently preserves files whose names start with
   `service.status_base_path.name + "."`. That was for per-phase sidecars.

   New behavior:

   - preserve `service.lock_path`
   - preserve `service.status_base_path`
   - do not preserve legacy `broker.setup.lock` merely because it is tracked
   - do not preserve legacy `broker.setup.status.<phase>` files merely because
     they are tracked; they are old completion caches, not active lock files
   - keep mock-path cleanup behavior for paths containing `"Mock"`

3. Do not change `db.py` unless a test proves the runner change is insufficient.

Focused command:

```bash
uv run pytest tests/test_runner_error_handling.py -q -k "cleanup_marker_files"
```

### Task 6: Update Runner Validation Tests

Purpose:

Preserve stale marker safety with the new status file.

Files to touch:

- `tests/test_runner_validation.py`

Update helpers:

- Replace `_write_status_markers(db_path)` with two explicit helpers:

  ```python
  def _write_status_file(db_path: Path, phases: Iterable[str]) -> None: ...
  def _write_legacy_status_sidecars(db_path: Path, phases: Iterable[str]) -> None: ...
  ```

  Keep helpers local to the test file.

Tests to preserve and update:

- `test_zero_byte_database_discards_stale_status_markers`
- `test_nonempty_invalid_database_with_stale_markers_fails_without_reinit`
- `test_connection_marker_check_does_not_open_sqlite_connection`

Required assertions:

- For missing or zero-byte target, stale `broker.status` and legacy sidecars do
  not make setup skip required work.
- For non-empty invalid DB, setup fails clearly and original bytes remain.
- For valid existing DB, connection marker checks stay passive and do not open
  SQLite validation merely to inspect status.

Add a final-state test:

1. Force fallback sidecars by monkeypatching xattrs unavailable.
2. Create a real `Queue` against `tmp_path / "broker.db"`.
3. Write and read one message.
4. Assert `broker.lock` exists.
5. Assert `broker.status` exists.
6. Assert `broker.status` lists `connection`, `schema-v{SCHEMA_VERSION}`, and
   `optimization`.
7. Assert no `broker.setup.status.*` files exist for a fresh DB.

Focused command:

```bash
uv run pytest tests/test_runner_validation.py -q
```

### Task 7: Update Runner Error Handling and Cleanup Tests

Purpose:

Make cleanup tests match the new file model while preserving the shared-file
lifetime rule.

Files to touch:

- `tests/test_runner_error_handling.py`

Update these tests:

- `test_cleanup_marker_files_preserves_shared_setup_sidecars`
- `test_run_exclusive_setup_marker_does_not_bypass_held_lock`
- `test_cleanup_marker_files_still_cleans_mock_sidecars`

Required behavior:

- For real DB paths, `cleanup_marker_files()` must not delete `broker.lock` or
  `broker.status`.
- For real DB paths, cleanup may remove legacy `broker.setup.lock` and legacy
  `broker.setup.status.<phase>` files if they are tracked. New code treats both
  as detritus.
- For mock DB paths, cleanup should still remove tracked sidecars.
- A marker published while the setup lock is held must not let a contender skip
  before the lock is released.

When a test needs to publish completion manually, prefer using the public-ish
phaselock API:

```python
service.mark_phase(phase_name)
```

If xattrs are unavailable and you need fallback status, write `broker.status`
through a small test helper that uses the same newline format. Do not call
private phaselock helpers from tests unless there is no stable alternative.

Focused command:

```bash
uv run pytest tests/test_runner_error_handling.py -q
```

### Task 8: Update Queue Config and Setup Contention Tests

Purpose:

Keep user-facing behavior and multiprocess setup serialization green.

Files to touch:

- `tests/test_queue_config_defaults.py`
- `tests/test_sqlite_setup_contention.py`

Queue config tests:

- Tests that instantiate `PhaseLockService(path).has_phase(...)` should still
  work after default suffix changes.
- If fallback mode is forced, assert `broker.status` content, not
  `broker.setup.status.<phase>` files.
- Assert no `.done` files as before.

Setup contention tests:

- Keep real subprocess tests.
- Assert `service.lock_path.exists()` now means `broker.lock` exists.
- Do not add sleeps to make contention pass.

Focused commands:

```bash
uv run pytest tests/test_queue_config_defaults.py -q
uv run pytest tests/test_sqlite_setup_contention.py -q
```

### Task 9: Preserve Xattr Behavior

Purpose:

Ensure the fallback status-file work does not break xattr-backed completion.

Files to touch:

- `tests/test_phaselock.py`
- `simplebroker/_phaselock.py` only if tests fail

Existing tests to preserve:

- `test_real_xattr_runtime_marks_and_skips_completed_phases`
- `test_real_xattr_runtime_resumes_from_last_marked_phase`
- `test_failure_while_lock_held_leaves_partial_xattrs_and_blocks_contenders`

Assertions:

- xattrs still mark individual phases
- status file is not required when xattrs work
- lock path is `broker.lock`
- resume behavior is unchanged

Focused command:

```bash
uv run pytest tests/test_phaselock.py -q -k "xattr"
```

### Task 10: Improve Diagnostics

Purpose:

Make lock timeouts and status problems diagnosable after the format change.

Files to touch:

- `simplebroker/_phaselock.py`
- `tests/test_phaselock.py`

Update `_phase_diagnostics()` to include, where practical:

- target path
- target exists
- xattrs available
- status path
- whether status exists
- parsed completed phases
- status parse/read error, if any
- legacy status sidecars found
- missing requested phases

Do not write diagnostics into `broker.status`.

Focused command:

```bash
uv run pytest tests/test_phaselock.py -q -k "timeout"
```

### Task 11: Full Local Gate

Run these before calling the implementation done:

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

If `uv` is unavailable:

```bash
python -m pytest ...
```

Do not increase sleeps, process timeouts, or lock timeouts to hide failures.

## Implementation Notes for `_phaselock.py`

Suggested private helper structure:

```python
_MAX_STATUS_BYTES = 64 * 1024

def _validate_phase_name(phase_name: str) -> None: ...

def _read_status_phases(self) -> tuple[set[str], str | None]: ...

def _write_status_phases(self, phases: Iterable[str]) -> None: ...

def _legacy_status_base_path(self) -> Path: ...

def _legacy_status_path_for_phase(self, phase_name: str) -> Path: ...

def _legacy_status_paths(self) -> list[Path]: ...

def _cleanup_legacy_status_files(self) -> None: ...
```

Keep this small. Do not introduce a `StatusStore` class unless the functions
become hard to read. They probably will not.

Ordering status file output:

- Preserve existing file order for existing valid phases where convenient.
- Append newly completed phases in the order actions complete.
- Deterministic output is useful for tests, but correctness must depend on set
  membership, not line order.

Temp file names:

- Put temp files next to `broker.status`.
- Include pid and `time.time_ns()` as the current code does.
- Use a suffix/prefix that cleanup can recognize, for example
  `broker.status.tmp.<pid>.<time_ns>`.

Parent directory fsync:

- Best effort only.
- It is okay to suppress `OSError` and platform-specific unsupported errors.

## Test Design Warnings

Avoid these mistakes:

- Do not mock `os.replace()`.
- Do not assert private helper calls.
- Do not parse status in production code outside `_phaselock.py`.
- Do not test only the happy path.
- Do not use file existence of `broker.lock` as ownership.
- Do not create `broker.status` by writing in place in production code.
- Do not make malformed status content count as completion.
- Do not add a phase-order cursor interpretation.
- Do not treat `broker.setup.lock` as active coordination state for new code.
- Do not rely on old `broker.setup.status.<phase>` files as completion facts.
- Do not implement dual-lock compatibility casually.

## Acceptance Checklist

The implementation is complete only when all are true:

- Fresh fallback setup creates `broker.lock` and `broker.status`.
- Fresh fallback setup does not create `broker.setup.lock` or
  `broker.setup.status.<phase>`.
- `broker.status` is written by temp file plus `os.replace()`, never in place.
- `broker.status` lists independent completed phase names.
- Sparse status content does not imply missing phases are complete.
- Phase failure records only phases whose actions succeeded.
- Re-running after failure resumes from `broker.status`.
- Legacy per-phase status sidecars are ignored as completion facts.
- Legacy per-phase status sidecars are cleaned after a successful locked setup
  pass.
- Legacy `broker.setup.lock` is ignored as coordination state and cleaned after
  a successful locked setup pass.
- Missing or zero-byte DB targets discard stale fallback status.
- Non-empty invalid DB targets fail without being overwritten.
- Xattr mode still works.
- Multiprocess setup contention still serializes setup.
- `_phaselock.py` remains stdlib-only and has no SimpleBroker imports.
- No new runtime dependency is added.

## What Not To Do

Do not "fix" this by:

- putting status content into `broker.lock`
- replacing `broker.lock`
- renaming `broker.lock`
- writing status content in place
- storing JSON
- storing timestamps or pids
- deleting all status after setup
- treating `optimization` as proof that schema completed
- relying on old per-phase status files
- relying on old `broker.setup.lock`
- deleting non-empty user DBs
- increasing timeouts to pass contention tests

## Useful References

Check only as needed:

- Python `os.replace`: atomic replacement on the same filesystem.
- Python file object `flush`.
- Python `os.fsync`.
- Python `pathlib.Path.with_suffix`.
- Python `fcntl.flock`.
- Python `msvcrt.locking`.
- Pytest `tmp_path`.
- Pytest subprocess patterns.

Do not add dependencies after reading those docs.

## Self-Review Findings Applied

I reviewed this plan for hidden ambiguity and corrected these points:

1. The first tempting plan, a cursor sentinel, is cleaner but weaker. This plan
   keeps independent phase facts in content.
2. Content is only safe if the status file is atomically replaced. The plan now
   bans in-place writes explicitly.
3. Putting content in the lock file would couple status persistence to lock
   rendezvous and would make replacement unsafe. The plan keeps two files with
   one job each.
4. Renaming the lock path creates an old/new mixed-version compatibility
   boundary. The plan states that assumption directly and avoids a quiet
   half-dual-lock design.
5. Old per-phase markers are not reliable migration evidence. The plan now
   requires redoing validation/setup and cleaning old status markers rather than
   relying on them.
6. Malformed status should not count as completion, but should not permanently
   brick a valid DB. The plan treats it as no trusted status and lets idempotent
   setup/DB validation decide the outcome.
7. Legacy `broker.setup.lock` follows the same policy as old status sidecars:
   new code does not rely on it and cleans it after the new locked path
   succeeds. This depends on the explicit no-mixed-old/new-process assumption.

If any of these decisions becomes unacceptable during implementation, stop and
revise the plan instead of drifting into a different design.
