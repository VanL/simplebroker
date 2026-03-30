# SQLite Isolation Foundation Plan

## Purpose

This document is the implementation plan for isolating SQLite-specific code so the internal backend abstraction becomes cleaner without changing the public API or current default behavior.

This plan is intentionally conservative.

- The goal is **not** to add Postgres support in this change set.
- The goal is **not** to redesign the queue model.
- The goal is **not** to reinterpret `db_path`, add a backend registry, or add `.env` loading.

The goal is to make the current SQLite implementation easier to reason about and easier to replace later.

Assume the implementing engineer is technically strong, but:

- has zero context for this codebase
- is likely to over-abstract if not constrained
- is likely to over-mock tests unless told not to

Follow this plan in order. Keep each step small. Keep the suite green after every task.

## Scope Locks

These decisions are locked for this plan. Do not revisit them while implementing.

### Public API lock

The public API must remain exactly the same in this plan.

- `Queue`, `QueueWatcher`, `QueueMoveWatcher`, CLI commands, and current option names stay unchanged.
- `db_path` continues to mean a SQLite database file path in this plan.
- Do **not** reinterpret `db_path` to mean DSN, database name, or backend identifier.
- Do **not** add user-facing backend selection.

### Behavior lock

The default built-in backend remains SQLite.

- SimpleBroker proper remains a SQLite-backed project.
- Current SQLite CLI behavior must remain unchanged.
- Current queue ordering remains based on the internal ordering column already used by the code.
- Existing watcher, vacuum, and status behavior should remain unchanged unless a change is required to preserve behavior during refactor.
- Existing `runner=` injection behavior must remain unchanged for current callers.

### Project-boundary lock

This plan is about enabling future external backends, not shipping them inside SimpleBroker.

- Do **not** create a built-in Postgres backend as part of SimpleBroker proper.
- Do **not** add `PostgresRunner`, Postgres-specific SQL, Postgres docs, or Postgres CLI/config support in this plan.
- The only acceptable future outcome enabled by this plan is that someone could later build a Postgres example, extension, or out-of-tree backend more cleanly.
- If a future Postgres backend is created, it should be an example or extension first, not part of the default built-in SimpleBroker implementation.

### Architecture lock

This plan is about **physical isolation first**, not full pluggability.

- Do **not** add a backend registry.
- Do **not** add dependency-injection containers.
- Do **not** split `_constants.py` in this plan.
- Do **not** add `.env` support in this plan.
- Do **not** redesign `SQLRunner` unless a step below explicitly calls for it. The default answer is “leave it alone.”

## Why This Is The Right First Slice

The repository already has one useful extension seam: [`SQLRunner`](/Users/van/Developer/simplebroker/simplebroker/_runner.py) and the injected-runner path used by [`Queue(..., runner=...)`](/Users/van/Developer/simplebroker/simplebroker/sbqueue.py).

The code is still heavily SQLite-coupled in four places:

1. SQL text and schema/catalog queries
2. file validation and SQLite-only path assumptions
3. connection setup and PRAGMA/WAL tuning
4. database maintenance helpers in `db.py`

### Terminology: "vacuum" vs `VACUUM`

In this codebase, "vacuum" is a broker-level maintenance concept first.

- `BrokerCore.vacuum()` means "clean up claimed messages"
- SQLite `VACUUM` is only one optional implementation detail used for file compaction
- `compact=True` is the path that asks SQLite to reclaim disk space more aggressively

Do not conflate the broker cleanup behavior with SQLite's `VACUUM` command.

If you try to solve “generic backend support” before physically isolating those concerns, you will create one of two bad outcomes:

- a fake abstraction that still leaks SQLite everywhere
- a large rewrite that changes behavior and destabilizes the suite

This plan avoids both.

## Current State Map

Current coupling is roughly this:

```text
CLI / Queue / Watcher / Commands
            |
            v
        DBConnection
            |
            v
        BrokerCore / BrokerDB
            |
            +--> simplebroker._sql            (SQLite SQL text)
            +--> sqlite_master / PRAGMA       (SQLite schema + metadata)
            +--> SELECT changes()             (SQLite rowcount)
            +--> claimed-message cleanup + optional SQLite compaction
            +--> filesystem db_path semantics (SQLite file model)
            |
            v
        SQLiteRunner
            |
            +--> sqlite3
            +--> PRAGMA busy_timeout
            +--> PRAGMA journal_mode=WAL
            +--> PRAGMA synchronous/cache_size
```

Target state after this plan:

```text
CLI / Queue / Watcher / Commands
            |
            v
        DBConnection
            |
            v
        BrokerCore / BrokerDB
            |
            +--> simplebroker._sql            (compat shim)
            |       |
            |       +--> simplebroker._sql.sqlite
            |
            +--> simplebroker._sqlite_backend
                    |
                    +--> file validation
                    +--> runner setup/tuning
                    +--> SQLite-only maintenance helpers
                    +--> SQLite-only schema/bootstrap helpers
```

Read this diagram as **directional**, not literal.

It shows where SQLite-specific code should trend, not a promise that `_sqlite_backend.py` becomes the sole owner of all SQLite runtime behavior.

In particular, [`SQLiteRunner`](/Users/van/Developer/simplebroker/simplebroker/_runner.py) will still own:

- connection lifecycle
- thread-local state
- fork safety
- setup orchestration
- lock/marker file handling
- exception translation

This is **not** the final backend abstraction. It is the minimum structure that makes the next backend step tractable.

## Repository Primer

### Project shape

- Runtime code lives in `simplebroker/`.
- Tests live in `tests/`.
- Examples live in `examples/`.
- Long-form planning docs live in `docs/plans/`.
- Tooling configuration lives in `pyproject.toml`.

### Files you must read before changing anything

- `pyproject.toml`
- `README.md`
- `simplebroker/_sql.py`
- `simplebroker/_runner.py`
- `simplebroker/db.py`
- `simplebroker/helpers.py`
- `simplebroker/cli.py`
- `simplebroker/commands.py`
- `simplebroker/sbqueue.py`
- `simplebroker/watcher.py`
- `simplebroker/ext.py`
- `tests/test_sql_internals.py`
- `tests/test_commands_init.py`
- `tests/test_pragma_settings.py`
- `tests/test_status_command.py`
- `tests/test_vacuum_compact.py`
- `tests/test_watcher.py`
- `tests/test_constants.py`
- `tests/test_project_scoping.py`
- `tests/test_custom_runner_integration.py`
- `tests/test_ext_imports.py`
- `examples/async_pooled_broker.py`

### Tooling and commands

Use these commands from the repo root.

- Install dev tools:
  - `uv sync --extra dev`
- Targeted test runs:
  - `uv run pytest -q -n 0 tests/test_sql_internals.py`
  - `uv run pytest -q -n 0 tests/test_commands_init.py tests/test_pragma_settings.py`
  - `uv run pytest -q -n 0 tests/test_status_command.py tests/test_vacuum_compact.py`
  - `uv run pytest -q -n 0 tests/test_watcher.py -k "data_version or polling"`
  - `uv run pytest -q -n 0 tests/test_constants.py tests/test_project_scoping.py`
- Full validation:
  - `uv run pytest -q -n 0`
  - `uv run ruff check .`
  - `uv run mypy simplebroker`

Notes:

- `pytest-xdist` is enabled by default; use `-n 0` while iterating on DB behavior.
- Do not add dependencies.
- Prefer `uv run ...` consistently so the environment is predictable.

## Engineering Rules

### Red-Green TDD

For behavior changes:

1. write or strengthen a real test
2. run it and watch it fail
3. make the smallest change that turns it green
4. refactor only after behavior is pinned

For pure code-movement refactors with no intended behavior change:

1. identify the smallest existing targeted test set that covers the code you are moving
2. run it before editing
3. move code
4. rerun it immediately
5. do not combine unrelated moves in one step

### Prefer real integration tests over mocks

Use real temporary SQLite databases and real runtime objects.

Good:

- `Queue`
- `BrokerDB`
- `DBConnection`
- temporary directories / temporary database files
- existing integration tests

Bad:

- mocking `sqlite3.connect`
- mocking `BrokerDB`
- asserting call counts on low-level helpers instead of queue/database state
- tests that verify file names or helper function names instead of behavior

This repository already has enough implementation-detail testing. Do not add more unless there is no reasonable behavioral assertion.

### DRY

- If two call sites need the same SQLite helper, move it once.
- If a new helper would have only one caller, do not extract it yet.
- Prefer a few well-named module-level helper functions over micro-classes.

### YAGNI

Do not build abstractions for imagined backends.

This plan is allowed to create:

- one `_sql` package
- one SQLite-only support module
- small shims/wrappers needed for compatibility

This plan is **not** allowed to create:

- backend registries
- backend factories
- backend plugin discovery
- multiple new class hierarchies
- constants packages
- environment-loading frameworks

### Style guidance

- Match existing naming and typing style.
- Keep docstrings factual and short.
- Prefer explicit names like `validate_sqlite_database()` over clever names.
- Avoid “shared” helpers that are SQLite-shaped but named generically.

## Explicit Non-Goals

These came up in discussion and are intentionally deferred.

### Not in scope: `.env` support

Do not add current-directory or project-directory `.env` lookup in this plan.

Reason:

- `load_config()` is imported and cached at module import time in multiple modules.
- Adding directory-sensitive `.env` semantics is a separate config-lifecycle change.
- That would materially expand test surface and could easily destabilize CLI/project scoping.

If `.env` support is desired later, it should be its own plan.

### Not in scope: `_constants/` split

Do not split [`simplebroker/_constants.py`](/Users/van/Developer/simplebroker/simplebroker/_constants.py) in this plan.

Reason:

- most constants are application-level, not backend-specific
- splitting constants now would create churn without paying down the actual coupling
- backend-specific validation belongs with backend support code, not in a global constants registry

### Not in scope: first-class Postgres support

Do not add a Postgres runner, DSN parsing, or CLI backend selection in this plan.

This plan is only laying the foundation.

Be explicit about the project boundary:

- this work is meant to **enable** a future Postgres example or extension
- this work is **not** meant to make Postgres a built-in default backend of SimpleBroker
- adding a built-in Postgres backend in `simplebroker/` would be contrary to the scope of this plan and contrary to the intended project direction

## Success Criteria

At the end of this work:

- `from simplebroker._sql import ...` still works
- the default SQLite path behaves exactly the same to users
- SimpleBroker proper is still clearly SQLite-only after this refactor
- SQLite-only code is physically easier to find
- the code clearly distinguishes “generic broker logic” from “SQLite implementation details”
- existing injected-runner tests still pass unchanged
- the full suite, lint, and type-check all pass

## Implementation Strategy

Implement this in four incremental PRs.

Each PR should be independently reviewable and shippable.

Do **not** start PR2 before PR1 is green and merged locally.

Sizing guidance:

- PR1 and PR2 should be small.
- PR3 and PR4 are medium refactors even if they are behavior-preserving.
- Do not pretend PR3/PR4 are “trivial cleanup”; budget enough time for careful review and full-suite verification.

## PR1: Turn `_sql.py` Into `_sql/sqlite.py` With A Compatibility Shim

### Goal

Move SQL text into a package layout without breaking any existing imports or behavior.

### Primary files to touch

- `simplebroker/_sql/__init__.py` (new)
- `simplebroker/_sql/sqlite.py` (new)
- `simplebroker/_sql.py` (delete in the same commit that creates the package)

### Files to read, but avoid touching if possible

- `simplebroker/db.py`
- `simplebroker/_runner.py`
- `tests/test_sql_internals.py`
- `examples/async_pooled_broker.py`

### Required implementation decisions

1. Create `simplebroker/_sql/` as a package.
2. Move the full contents of current `_sql.py` into `simplebroker/_sql/sqlite.py`.
3. Make `simplebroker/_sql/__init__.py` re-export the current SQLite symbols so all existing imports continue to work unchanged.

### Atomicity requirement

The `_sql.py` to `_sql/` package conversion is not a two-step migration.

You cannot leave both a module file and a package with the same import name in place.

This must be a single commit that:

1. deletes `simplebroker/_sql.py`
2. creates `simplebroker/_sql/__init__.py`
3. creates `simplebroker/_sql/sqlite.py`

Do not stage this as “create package first, delete file later.”

Recommended pattern:

```python
from .sqlite import ...
```

Be explicit. Do not use `import *` inside the package shim.

### What not to do

- Do not introduce `_sql/shared.py` yet.
- Do not rename SQL constants in this PR.
- Do not change placeholder style (`?`) in this PR.
- Do not update all imports to `from ._sql.sqlite import ...`; keep existing callers using `from ._sql import ...` for now.

### Why this order matters

This keeps the diff mechanical and low-risk:

- physical move first
- semantic cleanup later

If you change import style at the same time, review becomes much harder and failures become harder to localize.

### Tests and gates

Run:

- `uv run pytest -q -n 0 tests/test_sql_internals.py`
- `uv run pytest -q -n 0 tests/test_custom_runner_integration.py`

Then run:

- `uv run pytest -q -n 0 tests/test_queue_api_comprehensive.py -k "read or write or move or peek"`
- `uv run pytest -q -n 0 tests/test_ext_imports.py`

Gate:

- `tests/test_sql_internals.py` passes unchanged
- examples importing `simplebroker._sql` still do not require edits
- no user-facing behavior changed

## PR2: Create A SQLite Support Module And Move File Validation + Runner Setup There

### Goal

Move obvious SQLite-only runtime behavior out of generic-looking modules.

### Primary files to touch

- `simplebroker/_sqlite_backend.py` (new)
- `simplebroker/helpers.py`
- `simplebroker/_runner.py`

### Files to read, but avoid touching if possible

- `simplebroker/commands.py`
- `simplebroker/cli.py`
- `tests/test_commands_init.py`
- `tests/test_pragma_settings.py`
- `tests/test_runner_error_handling.py`
- `tests/test_safety_fixes.py`

### What belongs in `simplebroker/_sqlite_backend.py`

Move these functions or their logic into the new module:

- SQLite file validation:
  - `_validate_sqlite_database()` logic
  - `_is_valid_sqlite_db()` logic
- SQLite connection setup helpers:
  - SQLite version check
  - WAL setup
  - PRAGMA busy timeout
  - PRAGMA cache/synchronous tuning
  - connection-phase and optimization-phase helper functions

Recommended public shape for this internal module:

- `validate_sqlite_database(...)`
- `is_valid_sqlite_db(...)`
- `check_sqlite_version(...)`
- `setup_sqlite_connection_phase(...)`
- `apply_sqlite_connection_settings(...)`
- `apply_sqlite_optimization_settings(...)`

Use explicit function names. Do not create a `SQLiteBackendManager` class unless you have at least three pieces of state that genuinely need to live together. You probably do not.

### Recommended signature style

Prefer helpers that take explicit primitives over helpers that take the entire runner object.

Recommended examples:

- `apply_sqlite_connection_settings(conn: sqlite3.Connection, config: dict[str, Any])`
- `apply_sqlite_optimization_settings(conn: sqlite3.Connection, config: dict[str, Any])`
- `check_sqlite_version()`
- `setup_sqlite_wal(db_path: str, *, is_new_database: bool, config: dict[str, Any])`

Avoid helpers shaped like:

- `setup_sqlite_connection_phase(runner)`
- `apply_sqlite_settings(runner)`

Reason:

- explicit primitive arguments are easier to test
- they make the dependency on SQLite concrete without coupling `_sqlite_backend.py` to `SQLiteRunner` internals
- they reduce the chance that `_sqlite_backend.py` becomes a second home for runner state management

For PR2 specifically, expect `SQLiteRunner` to unpack its own state and pass primitives into the helper functions.

### Internal contract rule

Helpers used by the generic broker path must accept generic inputs whenever possible.

That means:

- if a helper is called from `BrokerCore`, it should prefer `SQLRunner` inputs over raw `sqlite3.Connection` objects unless it is clearly on the built-in `SQLiteRunner` path
- if a helper needs a filesystem path or runner-private field like `_db_path`, that dependency must be explicit and guarded
- do not silently assume every `SQLRunner` is a `SQLiteRunner`

This matters because the repository already tests injected runners via [`tests/test_custom_runner_integration.py`](/Users/van/Developer/simplebroker/tests/test_custom_runner_integration.py).

### Compatibility requirement

Keep these old helper import paths working for now:

- `simplebroker.helpers._validate_sqlite_database`
- `simplebroker.helpers._is_valid_sqlite_db`

Do this by turning the old helpers into thin wrappers or re-exports.

Reason:

- existing tests import them directly
- this keeps the external/internal contract stable during the refactor

### `SQLiteRunner` refactor rule

After this PR, [`SQLiteRunner`](/Users/van/Developer/simplebroker/simplebroker/_runner.py) should be an orchestrator, not a dumping ground for SQLite tuning logic.

It should still:

- own thread-local connections
- own fork-safety handling
- own setup phase orchestration
- translate `sqlite3` exceptions into broker exceptions

It should no longer contain large blocks of inline SQLite tuning/setup logic if those blocks can live in `_sqlite_backend.py`.

### What not to do

- Do not change `SQLRunner`.
- Do not change the behavior of WAL setup.
- Do not change lock file semantics.
- Do not move generic retry helpers out of `helpers.py`.
- Do not touch CLI behavior in this PR.

### Tests and gates

Run:

- `uv run pytest -q -n 0 tests/test_commands_init.py`
- `uv run pytest -q -n 0 tests/test_pragma_settings.py`
- `uv run pytest -q -n 0 tests/test_runner_error_handling.py -k "WAL or sqlite_version"`
- `uv run pytest -q -n 0 tests/test_safety_fixes.py -k "sqlite_version_check"`

Then run:

- `uv run pytest -q -n 0 tests/test_constants.py tests/test_project_scoping.py`
- `uv run pytest -q -n 0 tests/test_custom_runner_integration.py tests/test_ext_imports.py`

Invariants:

- invalid non-SQLite files are still rejected the same way
- valid SQLite files still validate the same way
- WAL is still enabled on init
- pragma/tuning behavior is unchanged

Gate:

- no change to CLI output or exceptions for current SQLite paths

## PR3: Move SQLite-Only Maintenance And Status Helpers Out Of `db.py`

### Goal

Reduce SQLite leakage inside [`simplebroker/db.py`](/Users/van/Developer/simplebroker/simplebroker/db.py) without changing queue semantics.

### Primary files to touch

- `simplebroker/_sqlite_backend.py`
- `simplebroker/db.py`

### Files to read, but avoid touching if possible

- `simplebroker/watcher.py`
- `simplebroker/commands.py`
- `tests/test_status_command.py`
- `tests/test_vacuum_compact.py`
- `tests/test_watcher.py`
- `tests/test_watcher_edge_cases.py`
- `tests/test_edge_cases.py`

### Move these SQLite-only concerns

Move the implementation logic for these SQLite-only operations into `_sqlite_backend.py`:

- row count retrieval after delete (`SELECT changes()`)
- SQLite data version lookup (`PRAGMA data_version`)
- filesystem DB size calculation for SQLite-backed runners
- SQLite pieces of broker cleanup/compaction, including compact/incremental behavior

The generic broker methods in `db.py` should become smaller and clearly delegate to SQLite helpers.

Recommended outcome:

- `BrokerCore.delete()` still owns the high-level delete contract
- `BrokerCore.get_data_version()` remains a thin SQLite-aware wrapper; delegation is acceptable, but do not force a cosmetic move if it makes the code less clear
- `BrokerCore.status()` still returns the same shape, but SQLite-specific size lookup is delegated
- broker cleanup keeps its high-level orchestration in `db.py`
- SQLite compaction leaf operations move out of `db.py` where practical

### Critical constraint: `SELECT changes()` must stay on the same runner connection

[`BrokerCore.delete()`](/Users/van/Developer/simplebroker/simplebroker/db.py#L1945) currently relies on:

- executing `DELETE ...`
- then immediately executing `SELECT changes()`
- on the same runner-backed connection
- before any unrelated statement intervenes

That SQLite behavior is connection-local.

Therefore:

- it is fine to extract this into a helper
- it is **not** fine to route it through a separate connection
- it is **not** fine to compute row count later
- it is **not** fine to hide it behind an abstraction that loses the same-connection guarantee

The helper, if extracted, must execute the delete and rowcount read as one same-runner unit.

### Vacuum extraction reality check

[`BrokerCore._do_vacuum_without_lock()`](/Users/van/Developer/simplebroker/simplebroker/db.py#L2215) does not decompose cleanly because it interleaves:

- `BrokerCore` lock ownership
- transaction boundaries
- retry-sensitive control flow
- broker cleanup semantics
- SQLite-specific commands and PRAGMAs

Do **not** assume this entire method should move to `_sqlite_backend.py`.

The more realistic target is:

- keep the broker cleanup loop, lock orchestration, and transaction orchestration in `db.py`
- move only the leaf SQLite operations to `_sqlite_backend.py`

Examples of acceptable leaf helpers:

- check current `auto_vacuum` mode
- run `SET_AUTO_VACUUM_INCREMENTAL`
- run full `VACUUM`
- run `INCREMENTAL_VACUUM`

If extracting the whole method would require passing `(runner, lock, config)` around, that is a sign the extraction is wrong. Keep the orchestration in `db.py`.

More explicit guidance:

- the deletion of claimed messages is the broker-level "vacuum" behavior
- the SQLite `VACUUM` command is only the optional compaction sub-step
- if you move code out of `db.py`, prioritize moving the SQLite compaction details, not the broker cleanup policy

### Important constraint

Do **not** rewrite message retrieval, write paths, claim/move semantics, or generator semantics in this PR.

This PR is about maintenance/status isolation only.

### Contract rule for this PR

Do not make `BrokerCore` more SQLite-specific than it is today.

If a moved helper would force `BrokerCore` to depend on:

- `sqlite3.Connection`
- `SQLiteRunner` concrete type
- direct filesystem path access without a guard

then stop and simplify the move.

The point of this PR is to relocate SQLite code, not to hide it behind a more brittle dependency.

### Acceptable temporary compromise

If a small amount of SQLite-specific branching remains in `db.py` after this PR, that is acceptable **only** if:

- the branching is obvious
- the helper it calls is clearly SQLite-labeled
- the code is materially simpler than before

This PR does not need a final backend abstraction. It needs a cleaner boundary.

### What not to do

- Do not add a new backend capability protocol yet.
- Do not redesign status payloads.
- Do not make `vacuum()` generic for backends that do not exist yet.
- Do not change watcher polling behavior.
- Do not move `SELECT changes()` into a helper that cannot prove same-runner/same-connection execution.

### Inline SQL guidance for this PR

There are still SQLite-specific raw SQL strings in `db.py`, for example:

- `SELECT changes()`
- `SELECT EXISTS(SELECT 1 FROM messages WHERE claimed = 1 LIMIT 1)`

When you touch one of these in PR3:

- move it to `simplebroker._sql.sqlite` if doing so clarifies ownership and keeps the diff small
- otherwise leave it in place and document the reason in the PR description

Do not create a side quest to eliminate every inline SQL string in one pass.

### Tests and gates

Run:

- `uv run pytest -q -n 0 tests/test_status_command.py`
- `uv run pytest -q -n 0 tests/test_vacuum_compact.py`
- `uv run pytest -q -n 0 tests/test_edge_cases.py -k "vacuum or status"`
- `uv run pytest -q -n 0 tests/test_watcher.py -k "data_version"`
- `uv run pytest -q -n 0 tests/test_watcher_edge_cases.py -k "data_version or PRAGMA"`
- `uv run pytest -q -n 0 tests/test_custom_runner_integration.py`

Invariants:

- `status()` output is unchanged for SQLite
- `db_size` remains populated for SQLite-backed runs
- broker cleanup behavior remains exactly the same
- `vacuum(compact=True)` still triggers the same optional SQLite compaction behavior as before
- watcher fallback behavior remains unchanged when data-version probing fails

Gate:

- all targeted tests above pass before touching schema/bootstrap logic

## PR4: Move SQLite Schema / Bootstrap / Migration Logic Out Of `db.py`

### Goal

Physically separate SQLite schema/bootstrap logic from generic queue operations.

### Primary files to touch

- `simplebroker/_sqlite_backend.py`
- `simplebroker/db.py`

### Files to read, but avoid touching if possible

- `simplebroker/_timestamp.py`
- `tests/test_commands_init.py`
- `tests/test_message_claim.py`
- `tests/test_pragma_settings.py`
- `tests/test_sql_internals.py`

### Move these concerns

Move the SQLite-specific implementation logic for:

- initial schema/bootstrap orchestration
- the SQLite catalog checks inside metadata verification
- schema version checks
- schema migrations (`claimed`, timestamp unique index, aliases)
- direct `sqlite_master` / `pragma_table_info` usage

Clarification:

- the SQL statement text itself already lives in `_sql/sqlite.py` after PR1
- PR4 moves the SQLite-specific orchestration and catalog/probing logic out of `db.py`
- PR4 does **not** require moving every meta-table read or write out of `db.py`

After this PR, `db.py` should primarily contain:

- queue/business behavior
- transaction orchestration
- retry orchestration
- queue validation
- timestamp orchestration

SQLite catalog and migration details should no longer dominate the file.

### Recommended shape

Keep this simple.

Good:

- module-level SQLite helper functions
- a small SQLite schema helper object only if the alternative is passing a long, unstable parameter list through many functions

Bad:

- a generic migration framework
- backend registries
- multiple layers of abstract base classes
- dynamic backend loading
- a schema helper that owns business logic unrelated to SQLite bootstrap/migration

### Important design rule

Do not move logic merely to reduce line count.

Move it only if the new boundary is clearer:

- broker logic stays in `db.py`
- SQLite-specific schema/bootstrap logic lives in `_sqlite_backend.py`

Mixed-concern method guidance:

[`BrokerCore._verify_database_magic()`](/Users/van/Developer/simplebroker/simplebroker/db.py#L574) is not purely SQLite-specific.

It currently does two different things:

1. SQLite catalog probing via `sqlite_master` to check whether the `meta` table exists
2. broker-level verification of the `magic` and `schema_version` rows inside that table

In PR4:

- move the SQLite catalog/probing part out of `db.py`
- keep the broker business rule checks in `db.py` unless moving them clearly improves readability without hiding the actual rule

Do not move business rules out of `db.py` just because the surrounding method also contains SQLite code.

If you need more than:

- one new SQLite support module
- one tiny SQLite schema helper object

then the design is probably drifting. Stop and simplify.

### Tests and gates

Run:

- `uv run pytest -q -n 0 tests/test_commands_init.py`
- `uv run pytest -q -n 0 tests/test_message_claim.py`
- `uv run pytest -q -n 0 tests/test_sql_internals.py`
- `uv run pytest -q -n 0 tests/test_pragma_settings.py`

Then run the full validation set:

- `uv run pytest -q -n 0`
- `uv run ruff check .`
- `uv run mypy simplebroker`

Invariants:

- init behavior is unchanged
- migrations still work on existing databases
- metadata validation behavior is unchanged
- timestamp uniqueness guarantees are unchanged

Gate:

- full suite, lint, and type-check pass

## Stop / Escalate Conditions

Stop and ask for direction instead of improvising if any of these become true:

1. isolating SQLite code appears to require a public API change
2. preserving behavior appears to require changing `SQLRunner`
3. the refactor wants more than one new SQLite support module plus one tiny helper object
4. tests only pass when replaced with heavy mocking
5. the work starts drifting toward DSNs, backend registries, `.env` loading, constants-package design, or a built-in Postgres backend
6. preserving behavior appears to require reworking SQLite connection-local semantics such as `SELECT changes()`

If one of these happens, do not push through by inventing a larger framework.
That means the task has changed and needs a new decision, not a clever workaround.

## What To Do About `_constants.py`

Do not split it now.

Instead:

- leave application-wide constants where they are
- if a helper in `_sqlite_backend.py` needs SQLite-specific config validation, pass the already-loaded config dictionary in
- if SQLite-only defaults need clearer documentation, update comments/docstrings in place

Only consider a `_constants/` package later if there is repeated, demonstrated backend-specific config logic across multiple real backends.

That is not true today.

## What To Do About `.env` Loading

Defer it completely.

Document this in the PR description so nobody tries to “sneak it in.”

If `.env` is revisited later, the plan must answer:

- when `.env` is loaded relative to module import
- whether project-scoped lookup and cwd lookup can disagree
- how tests isolate environment across modules that cache `_config`

Do not mix that problem with SQLite isolation.

## Testing Philosophy For This Plan

### Use the existing suite as the main safety net

Most tasks in this plan are behavior-preserving refactors. Existing tests are already broad.

Your job is not to invent a mock-heavy unit-test matrix for moved code.
Your job is to preserve behavior while moving code into clearer locations.

### Add tests only when the move creates a real new contract risk

Examples of acceptable new tests:

- a regression test proving `simplebroker._sql` import compatibility still works after package conversion
- a regression test proving helper re-exports still exist if external/internal callers rely on them

Examples of bad new tests:

- “this private helper calls that private helper”
- “this exact function name exists in this exact module”
- “the new module has N lines”

### Mandatory final gate

Before considering the work done, run:

- `uv run pytest -q -n 0`
- `uv run ruff check .`
- `uv run mypy simplebroker`

Do not skip the full suite just because targeted tests passed.

## Reviewer Checklist

The reviewer should explicitly verify all of the following:

- `_sql` is now a package, but old imports still work
- SQLite-only validation no longer lives primarily in `helpers.py`
- SQLite-only runner setup no longer lives primarily inline in `_runner.py`
- SQLite-only maintenance/bootstrap logic has been reduced in `db.py`
- injected-runner behavior still passes the existing contract tests
- no public API changed
- no CLI semantics changed
- no new dependencies were added
- no backend registry or speculative abstraction was introduced
- no built-in Postgres backend was added

## Common Failure Modes To Avoid

### Failure mode 1: Over-abstracting too early

Symptom:

- new backend base classes
- registries
- enum matrices
- dynamic import tricks

Fix:

- delete them
- keep only SQLite-specific helper modules plus compatibility shims

### Failure mode 2: Breaking internal-but-relied-on imports

Symptom:

- tests or examples fail because `from simplebroker._sql import ...` no longer works
- `tests/test_commands_init.py` fails because helper import paths changed

Fix:

- keep shims and re-exports until a later cleanup plan explicitly removes them

### Failure mode 3: Mixing config work into backend isolation

Symptom:

- `.env` loader appears
- `_constants/` split appears
- `load_config()` lifecycle changes

Fix:

- revert those changes
- keep this plan focused

### Failure mode 4: Mock-heavy tests that do not prove behavior

Symptom:

- lots of monkeypatching
- few or no real SQLite file tests

Fix:

- replace with real temporary-database integration tests or rely on existing suite

## Final Deliverable Summary

When this plan is complete, the codebase should have:

- `simplebroker/_sql/__init__.py`
- `simplebroker/_sql/sqlite.py`
- `simplebroker/_sqlite_backend.py`

And these modules should be smaller and more focused:

- `simplebroker/_runner.py`
- `simplebroker/db.py`
- `simplebroker/helpers.py`

Without changing:

- user-visible API
- SQLite default behavior
- CLI semantics
- test outcomes

That is the correct end state for this phase.
