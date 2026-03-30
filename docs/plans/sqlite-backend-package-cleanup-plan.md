# SQLite Backend Package Cleanup Plan

## Purpose

This document is the implementation plan for the next cleanup step after the SQLite isolation foundation refactor.

The current code is better than it was, but [`simplebroker/_sqlite_backend.py`](/Users/van/Developer/simplebroker/simplebroker/_sqlite_backend.py) is now a broad grab-bag of SQLite-specific helpers:

- file validation
- runner setup and tuning
- schema/bootstrap and migrations
- maintenance helpers

That is acceptable as an intermediate step, but not a good long-term shape.

This plan does **not** change the public API.

This plan does **not** add a built-in Postgres backend.

This plan does **not** add user-facing backend selection.

This plan does two things only:

1. move the SQLite helper blob into an internal package with focused modules
2. establish one standard **internal** backend access path for built-in code

Assume the implementing engineer is technically strong, but:

- has zero context for this codebase
- is likely to over-abstract if not constrained
- is likely to over-mock tests unless told not to

Follow this plan in order. Keep the scope tight. Keep the suite green after every task.

## Scope Locks

These decisions are locked for this plan. Do not revisit them while implementing.

### Public API lock

The public API must remain exactly the same in this plan.

- `Queue`, `QueueWatcher`, `QueueMoveWatcher`, CLI commands, and current option names stay unchanged.
- `db_path` continues to mean a SQLite database file path in this plan.
- Do **not** add user-facing backend selection.
- Do **not** add public config, CLI flags, environment variables, or docs for backend choice.

### Behavior lock

The built-in runtime behavior remains SQLite.

- SimpleBroker proper remains a SQLite-backed project.
- Existing SQLite CLI behavior must remain unchanged.
- Existing watcher, vacuum, status, and schema behavior must remain unchanged.
- Existing `runner=` injection behavior must remain unchanged for current callers.

### Project-boundary lock

This plan is still about enabling future external backends, not shipping them inside SimpleBroker.

- Do **not** add `PostgresRunner`.
- Do **not** add Postgres SQL.
- Do **not** add Postgres docs or tests.
- Do **not** create a public backend plugin system.

The only acceptable future outcome enabled by this plan is that built-in SQLite code has a cleaner internal shape and there is a clearer internal path a future extension could follow.

### Architecture lock

This plan introduces a tiny internal resolver and nothing more.

- Do **not** add abstract base classes for backends.
- Do **not** add dynamic backend loading via `importlib`.
- Do **not** add a public registry.
- Do **not** split `_constants.py`.
- Do **not** add `.env` support.
- Do **not** redesign `SQLRunner`.
- Do **not** move [`simplebroker/_sql/sqlite.py`](/Users/van/Developer/simplebroker/simplebroker/_sql/sqlite.py) into `_backends/` in this plan.

### Shim lock

[`simplebroker/_sqlite_backend.py`](/Users/van/Developer/simplebroker/simplebroker/_sqlite_backend.py) must be deleted.

- Do **not** keep a compatibility shim.
- Do **not** preserve imports from `simplebroker._sqlite_backend`.
- This was never a declared public API and should not be treated as one now.

This deletion must happen in the same PR that creates the new package and updates all internal imports.

## Why This Is The Right Next Slice

The foundation refactor succeeded in moving SQLite-only logic into one place, but it stopped at a “bag of helpers” module.

That was the right first move.

The right next move is **not** “make the backend system generic.”

The right next move is:

- package the SQLite implementation cleanly
- give built-in code one standard internal access path
- stop there

If you go further than that now, you will create one of two bad outcomes:

- a fake general backend abstraction that still only means SQLite
- an internal API that is more complex than the real problem

This plan avoids both.

## Current State

After the foundation refactor, the code looks roughly like this:

```text
helpers.py / _runner.py / db.py
            |
            +--> simplebroker._sqlite_backend
                    |
                    +--> validation helpers
                    +--> runtime/WAL helpers
                    +--> schema/bootstrap helpers
                    +--> maintenance helpers
```

That is already better than SQLite logic being spread everywhere, but `_sqlite_backend.py` is now too broad.

## Target State

After this plan, the shape should look roughly like this:

```text
helpers.py / _runner.py / db.py
            |
            +--> simplebroker._backends
                    |
                    +--> get_backend(...)
                    +--> get_configured_backend(...)
                    |
                    +--> sqlite
                           |
                           +--> validation.py
                           +--> runtime.py
                           +--> schema.py
                           +--> maintenance.py
```

Built-in modules should use one standard internal path:

```python
from ._backends import get_configured_backend

db_backend = get_configured_backend(_config)
```

Then call neutral backend interface names such as:

- `db_backend.validate_database(...)`
- `db_backend.setup_connection_phase(...)`
- `db_backend.initialize_database(...)`
- `db_backend.get_data_version(...)`

## Important Design Decisions Already Made

These are decisions, not open questions.

### 1. Use `_backends/`, not `backends/`

The package must be internal:

- [`simplebroker/_backends/`](/Users/van/Developer/simplebroker/simplebroker/_backends)

Do **not** create `simplebroker/backends/`.

`backends/` would imply a supported public surface. That is not the project direction.

### 2. Keep SQLite as the only built-in backend

The resolver exists only to give built-in code a standard internal access path.

At the end of this plan:

- the only built-in backend is still `"sqlite"`
- the default behavior is still SQLite
- external backends still come later, if ever, and should start as extensions or examples

### 3. The resolver is internal-only

Use a tiny internal resolver in [`simplebroker/_backends/__init__.py`](/Users/van/Developer/simplebroker/simplebroker/_backends/__init__.py).

Good:

- `get_backend(name: str)`
- `get_configured_backend(config: Mapping[str, object])`

Bad:

- backend plugin managers
- abstract registries
- dynamic import strings
- public backend discovery APIs

### 4. Do not add a real config feature

The resolver may read an internal config key such as `"_BROKER_INTERNAL_BACKEND"`, but:

- it must default to `"sqlite"` when absent
- it must **not** require changing `load_config()`
- it must **not** read a new environment variable
- it must **not** be documented as user-facing configuration

This matters because [`load_config()`](/Users/van/Developer/simplebroker/simplebroker/_constants.py) currently returns only public/declared configuration values. Do not expand that public surface just to support this cleanup.

### 5. Keep `schema.py` broad for now

Do **not** split into:

- `bootstrap.py`
- `catalog.py`
- `migrations.py`

Not yet.

For this codebase, bootstrap + catalog probing + migrations are one coherent concern right now. Keep them together in:

- [`simplebroker/_backends/sqlite/schema.py`](/Users/van/Developer/simplebroker/simplebroker/_backends/sqlite/schema.py)

Split later only if that file becomes clearly awkward.

### 6. Delete `_sqlite_backend.py` atomically

This is important.

You cannot:

- create `_backends/sqlite/`
- leave `_sqlite_backend.py` around
- “come back later” to delete it

Do this in one atomic PR:

1. create the new package
2. update every internal import
3. delete `_sqlite_backend.py`

There is no compatibility requirement for that private module.

## Repository Primer

### Project shape

- Runtime code lives in `simplebroker/`
- Tests live in `tests/`
- Examples live in `examples/`
- Planning docs live in `docs/plans/`
- Tooling configuration lives in `pyproject.toml`

### Files you must read before changing anything

Read these files first. Do not skip them.

- `pyproject.toml`
- `README.md`
- `simplebroker/_backends` if it already exists from partial work
- `simplebroker/_sqlite_backend.py`
- `simplebroker/_sql/__init__.py`
- `simplebroker/_sql/sqlite.py`
- `simplebroker/_runner.py`
- `simplebroker/db.py`
- `simplebroker/helpers.py`
- `simplebroker/_constants.py`
- `simplebroker/cli.py`
- `simplebroker/commands.py`
- `simplebroker/sbqueue.py`
- `simplebroker/watcher.py`
- `tests/test_runner_validation.py`
- `tests/test_commands_init.py`
- `tests/test_project_scoping.py`
- `tests/test_pragma_settings.py`
- `tests/test_status_command.py`
- `tests/test_vacuum_compact.py`
- `tests/test_watcher.py`
- `tests/test_watcher_edge_cases.py`
- `tests/test_custom_runner_integration.py`
- `tests/test_ext_imports.py`
- [`docs/plans/sqlite-isolation-foundation-plan.md`](/Users/van/Developer/simplebroker/docs/plans/sqlite-isolation-foundation-plan.md)

### Tooling and commands

Use these commands from the repo root.

- Install dev tools:
  - `uv sync --extra dev`

- Serial targeted runs while iterating:
  - `uv run pytest -q -n 0 tests/test_runner_validation.py`
  - `uv run pytest -q -n 0 tests/test_commands_init.py tests/test_project_scoping.py`
  - `uv run pytest -q -n 0 tests/test_pragma_settings.py tests/test_status_command.py`
  - `uv run pytest -q -n 0 tests/test_vacuum_compact.py`
  - `uv run pytest -q -n 0 tests/test_watcher.py -k "data_version"`
  - `uv run pytest -q -n 0 tests/test_watcher_edge_cases.py -k "data_version or PRAGMA"`
  - `uv run pytest -q -n 0 tests/test_custom_runner_integration.py tests/test_ext_imports.py`

- Full validation:
  - `uv run pytest -q`
  - `uv run ruff check .`
  - `uv run mypy simplebroker`

Notes:

- Use `-n 0` while iterating on database behavior and import changes.
- Use the project-default `uv run pytest -q` for the final full-suite gate unless you are debugging a specific serial-only failure.
- Do not add dependencies.

## Engineering Rules

### Red-Green TDD

For behavior changes:

1. write or strengthen a real test
2. run it and watch it fail
3. make the smallest change that turns it green
4. refactor only after behavior is pinned

For pure code-movement refactors with no intended behavior change:

1. identify the smallest targeted suite covering the code you are moving
2. run it before editing
3. move the code
4. rerun immediately
5. do not combine unrelated cleanup in the same step

### Prefer real integration tests over mocks

Use real temporary SQLite databases and real runtime objects.

Good:

- `Queue`
- `BrokerDB`
- `DBConnection`
- temporary directories
- real database files
- existing integration tests

Bad:

- mocking `sqlite3.connect`
- mocking `BrokerDB`
- asserting helper call counts instead of real database behavior
- “tests” that only prove a function was renamed or moved

This repository already has plenty of implementation-detail testing. Do not add more unless there is no reasonable behavioral assertion.

Preferred outcome:

- zero test changes

Acceptable outcome:

- one or two tiny direct tests only if you need to pin genuinely new internal resolver behavior that is otherwise uncovered

### DRY

- If two call sites need the same SQLite helper, move it once.
- If a new helper would have only one caller, do not extract it yet.
- Do not create thin wrappers that only rename a function without clarifying ownership.

### YAGNI

- Do not create interfaces for non-existent backends.
- Do not build a registry system beyond the tiny resolver in `_backends/__init__.py`.
- Do not create future-facing abstractions just because the word “backend” appears in the code.

### Import hygiene

Avoid circular imports.

Backend modules under `_backends/sqlite/` must **not** import:

- `BrokerCore`
- `BrokerDB`
- `SQLiteRunner`

at runtime.

If type hints are needed, use `TYPE_CHECKING` guards or `Protocol`-compatible signatures. The current code already relies on this pattern. Preserve it.

### Internal API rule

Once this plan is complete, built-in modules should not reach into deep SQLite submodules directly.

Good:

```python
from ._backends import get_configured_backend

db_backend = get_configured_backend(_config)
db_backend.get_data_version(...)
```

Bad:

```python
from ._backends.sqlite.maintenance import get_data_version
from ._backends.sqlite.runtime import setup_connection_phase
```

The package root should be the internal interface.

## PR1: Package The SQLite Backend And Delete `_sqlite_backend.py`

### Goal

Replace the one large helper module with an internal package that has focused modules.

### Primary files to add

- `simplebroker/_backends/__init__.py`
- `simplebroker/_backends/sqlite/__init__.py`
- `simplebroker/_backends/sqlite/validation.py`
- `simplebroker/_backends/sqlite/runtime.py`
- `simplebroker/_backends/sqlite/schema.py`
- `simplebroker/_backends/sqlite/maintenance.py`

### Primary files to touch

- `simplebroker/helpers.py`
- `simplebroker/_runner.py`
- `simplebroker/db.py`

### Primary file to delete

- `simplebroker/_sqlite_backend.py`

### Files to read, but avoid touching if possible

- `simplebroker/_sql/__init__.py`
- `simplebroker/_sql/sqlite.py`
- `simplebroker/commands.py`
- `simplebroker/cli.py`
- `tests/test_sql_internals.py`

### Package shape for PR1

Use this structure:

- `validation.py`
  - `validate_database`
  - `is_valid_database`

- `runtime.py`
  - `check_version`
  - `apply_connection_settings`
  - `apply_optimization_settings`
  - `setup_connection_phase`

- `schema.py`
  - `initialize_database`
  - `meta_table_exists`
  - `messages_has_claimed_column`
  - `ts_unique_index_exists`
  - `ensure_schema_v2`
  - `ensure_schema_v3`
  - `ensure_schema_v4`

- `maintenance.py`
  - `delete_and_count_changes`
  - `database_size_bytes`
  - `get_data_version`
  - `has_claimed_messages`
  - `delete_claimed_batch`
  - `compact_database`
  - `maybe_run_incremental_vacuum`

The interface names above are intentionally neutral because callers will eventually use them through `db_backend`.

Do **not** keep `sqlite_` prefixes on the exported interface names in `sqlite/__init__.py`. The package path already supplies the namespace.

### Caller pattern for PR1

For this PR only, callers may use:

```python
from ._backends import sqlite as sqlite_backend
```

Then call:

- `sqlite_backend.validate_database(...)`
- `sqlite_backend.setup_connection_phase(...)`
- `sqlite_backend.initialize_database(...)`

This keeps the first move mostly physical and low-risk. The resolver comes in PR2.

In PR1, [`simplebroker/_backends/__init__.py`](/Users/van/Developer/simplebroker/simplebroker/_backends/__init__.py) should stay minimal:

- expose the `sqlite` package
- do not add resolver logic yet
- do not add backend maps yet

### Critical constraints for PR1

#### 1. Delete `_sqlite_backend.py` in the same PR

No shim. No alias module. No re-export file.

#### 2. Preserve same-connection semantics

The helper that replaces SQLite `SELECT changes()` must still:

- execute the delete
- then execute `SELECT changes()`
- on the same runner-backed connection
- with no unrelated statement in between

Do not hide this behind a helper that breaks that guarantee.

#### 3. Preserve runtime warning behavior

Warnings for:

- invalid `BROKER_WAL_AUTOCHECKPOINT`
- invalid `BROKER_SYNC_MODE`
- permission issues

must remain behaviorally unchanged.

#### 4. Avoid cycle-prone imports

Do not make `_backends/sqlite/` depend on `_runner.py` at runtime.

### What not to do in PR1

- Do not add the resolver yet.
- Do not change `load_config()`.
- Do not add an internal config key yet.
- Do not split `schema.py` further.
- Do not move `_sql/sqlite.py` into `_backends/sqlite/`.
- Do not change tests unless needed to pin a real regression.

### Tests and gates for PR1

Run:

- `uv run pytest -q -n 0 tests/test_runner_validation.py`
- `uv run pytest -q -n 0 tests/test_commands_init.py tests/test_project_scoping.py`
- `uv run pytest -q -n 0 tests/test_pragma_settings.py`
- `uv run pytest -q -n 0 tests/test_status_command.py tests/test_vacuum_compact.py`
- `uv run pytest -q -n 0 tests/test_watcher.py -k "data_version"`
- `uv run pytest -q -n 0 tests/test_watcher_edge_cases.py -k "data_version or PRAGMA"`
- `uv run pytest -q -n 0 tests/test_custom_runner_integration.py tests/test_ext_imports.py`
- `uv run ruff check .`
- `uv run mypy simplebroker`

Invariants:

- SQLite file validation behavior is unchanged
- runner setup/WAL behavior is unchanged
- schema/bootstrap behavior is unchanged
- watcher-relevant data-version behavior is unchanged
- injected-runner behavior is unchanged
- no import path in runtime code still references `_sqlite_backend.py`

Gate:

- all targeted tests above pass
- `_sqlite_backend.py` is deleted
- no new test mocks were introduced

## PR2: Add The Tiny Internal Resolver And Standardize Caller Access

### Goal

Introduce one standard internal access path for built-in backend code:

```python
db_backend = get_configured_backend(_config)
```

### Primary files to touch

- `simplebroker/_backends/__init__.py`
- `simplebroker/helpers.py`
- `simplebroker/_runner.py`
- `simplebroker/db.py`

### Files to read, but avoid touching if possible

- `simplebroker/_constants.py`
- `simplebroker/cli.py`
- `simplebroker/commands.py`

### Resolver requirements

In [`simplebroker/_backends/__init__.py`](/Users/van/Developer/simplebroker/simplebroker/_backends/__init__.py), add:

- `DEFAULT_BACKEND = "sqlite"`
- a tiny built-in backend map
- `get_backend(name: str)`
- `get_configured_backend(config: Mapping[str, object])`

Rules:

- only `"sqlite"` is supported
- unsupported names raise a clear `RuntimeError`
- `get_configured_backend(config)` must default to `"sqlite"` when the internal key is absent

Recommended internal key:

- `"_BROKER_INTERNAL_BACKEND"`

This key is internal only.

### Important constraint: do not touch `load_config()`

Do **not** add `"_BROKER_INTERNAL_BACKEND"` to [`load_config()`](/Users/van/Developer/simplebroker/simplebroker/_constants.py).

That would expand the effective configuration surface and invite scope drift into env/config work.

Instead, `get_configured_backend(config)` must do:

- `config.get("_BROKER_INTERNAL_BACKEND", "sqlite")`

or equivalent.

### Caller pattern for PR2

Replace:

```python
from ._backends import sqlite as sqlite_backend
```

with:

```python
from ._backends import get_configured_backend

db_backend = get_configured_backend(_config)
```

Then update all uses to call `db_backend.*`.

This applies to [`simplebroker/helpers.py`](/Users/van/Developer/simplebroker/simplebroker/helpers.py) too.

[`helpers.py`](/Users/van/Developer/simplebroker/simplebroker/helpers.py) currently does not define `_config`. In PR2, add:

```python
from ._constants import load_config

_config = load_config()
db_backend = get_configured_backend(_config)
```

That is acceptable here because:

- `load_config()` is already used this way in multiple runtime modules
- this does **not** create a new public config feature
- it keeps the internal access path consistent across built-in modules

### Important design note

Module-level binding is acceptable here.

That means backend selection is effectively import-time for built-in code.

That is fine in this plan.

Do **not** turn this into per-instance backend selection. That is a different problem and out of scope.

### What not to do in PR2

- Do not add public environment variables.
- Do not document backend choice in README or CLI help.
- Do not add tests that only assert internal alias names.
- Do not over-engineer the resolver into a plugin system.
- Do not mix `get_backend()` in one module and `get_configured_backend(_config)` in another unless there is a very strong reason. Prefer one consistent built-in pattern.

### Tests and gates for PR2

Run:

- `uv run pytest -q -n 0 tests/test_runner_validation.py`
- `uv run pytest -q -n 0 tests/test_commands_init.py tests/test_project_scoping.py`
- `uv run pytest -q -n 0 tests/test_pragma_settings.py tests/test_status_command.py`
- `uv run pytest -q -n 0 tests/test_vacuum_compact.py`
- `uv run pytest -q -n 0 tests/test_watcher.py -k "data_version"`
- `uv run pytest -q -n 0 tests/test_watcher_edge_cases.py -k "data_version or PRAGMA"`
- `uv run pytest -q -n 0 tests/test_custom_runner_integration.py tests/test_ext_imports.py`
- `uv run ruff check .`
- `uv run mypy simplebroker`

Optional test:

- Add one tiny direct test for `get_backend("sqlite")` and unsupported-name failure **only if** you believe the error path needs pinning and the existing suite does not already cover it.

If you add that test:

- keep it direct
- do not mock anything
- put it in a small focused file such as `tests/test_ext_imports.py` or a new backend-resolver test

Gate:

- built-in modules access the backend only through `db_backend`
- no public/backend-facing docs or config were added
- all targeted tests above pass

## PR3: Clean Up The Package Surface And Remove Remaining SQLite Stragglers

### Goal

Polish the new internal package so it is coherent and boring.

This is the cleanup PR after the structural move and resolver change.

### Primary files to touch

- `simplebroker/_backends/sqlite/__init__.py`
- `simplebroker/_backends/sqlite/validation.py`
- `simplebroker/_backends/sqlite/runtime.py`
- `simplebroker/_backends/sqlite/schema.py`
- `simplebroker/_backends/sqlite/maintenance.py`
- `simplebroker/_sql/sqlite.py`

### Cleanup tasks

#### 1. Move remaining inline SQLite SQL strings into `_sql/sqlite.py`

Examples to look for:

- `SELECT COUNT(*) FROM sqlite_master ...`
- `SELECT changes()`
- `SELECT EXISTS(SELECT 1 FROM messages WHERE claimed = 1 LIMIT 1)`

Do this only where it improves ownership and keeps diffs small.

Do **not** create a second SQL abstraction.

#### 2. Tighten sloppy helper signatures

Example:

- prefer `db_path: str | Path | None` over `runner: object` when only a path is needed

But:

- keep runner-based helpers where same-connection semantics actually matter

Examples that should remain runner-based:

- `delete_and_count_changes`
- migration helpers
- compact/incremental-vacuum helpers

#### 3. Keep the package root curated

`sqlite/__init__.py` should export the intentional interface only.

Do not use:

- `from .validation import *`
- `from .runtime import *`

Keep exports explicit.

### What not to do in PR3

- Do not create more submodules just to reduce line count.
- Do not split `schema.py` unless a coherent split becomes obvious and necessary.
- Do not move `_sql` under `_backends`.
- Do not change behavior just because code looks cleaner.

### Tests and gates for PR3

Run:

- `uv run pytest -q -n 0 tests/test_sql_internals.py`
- `uv run pytest -q -n 0 tests/test_runner_validation.py`
- `uv run pytest -q -n 0 tests/test_commands_init.py tests/test_project_scoping.py`
- `uv run pytest -q -n 0 tests/test_pragma_settings.py tests/test_status_command.py`
- `uv run pytest -q -n 0 tests/test_vacuum_compact.py`
- `uv run pytest -q -n 0 tests/test_watcher.py -k "data_version"`
- `uv run pytest -q -n 0 tests/test_watcher_edge_cases.py -k "data_version or PRAGMA"`
- `uv run pytest -q -n 0 tests/test_custom_runner_integration.py tests/test_ext_imports.py`

Then run full validation:

- `uv run pytest -q`
- `uv run ruff check .`
- `uv run mypy simplebroker`

Invariants:

- package structure is clearer than before
- built-in code still behaves exactly the same
- same-connection SQLite semantics are preserved
- no public API changed

Gate:

- full suite green
- ruff green
- mypy green

## Stop / Escalate Conditions

Stop and escalate instead of improvising if any of these happen:

1. deleting `_sqlite_backend.py` requires keeping a shim to avoid internal breakage
2. implementing the resolver seems to require changes to `load_config()` or environment variables
3. the backend package starts demanding abstract base classes, registries, or plugin loading
4. `schema.py` becomes so large or tangled that you feel forced to invent a migration framework
5. import cycles appear unless backend modules start importing `BrokerCore`, `BrokerDB`, or `SQLiteRunner` at runtime
6. preserving behavior appears to require public backend configuration or CLI/backend flags

If one of those occurs, do not quietly change direction. Stop and discuss.

## Success Criteria

This plan is complete when all of the following are true:

- [`simplebroker/_sqlite_backend.py`](/Users/van/Developer/simplebroker/simplebroker/_sqlite_backend.py) is gone
- SQLite helper code lives under [`simplebroker/_backends/sqlite/`](/Users/van/Developer/simplebroker/simplebroker/_backends/sqlite)
- built-in runtime code uses `db_backend = get_configured_backend(_config)`
- the resolver is internal-only and defaults to SQLite
- no public backend selection exists
- no backend shim exists
- the final full validation set is green

## Reviewer Checklist

Use this checklist when reviewing the implementation.

- Does the code still present SQLite as the only built-in backend?
- Was `_sqlite_backend.py` actually deleted, not turned into a shim?
- Did the implementer avoid changing `load_config()` or adding public config?
- Do `helpers.py`, `_runner.py`, and `db.py` use the backend package root rather than deep SQLite submodule imports?
- Are exported interface names neutral and appropriate for `db_backend.*` calls?
- Are same-connection SQLite operations still guaranteed where required?
- Is `schema.py` still a coherent single concern rather than the start of a framework?
- Were tests kept behavior-focused and low-mock?
- Is the full suite green?
