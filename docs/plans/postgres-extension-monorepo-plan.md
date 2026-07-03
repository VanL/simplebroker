# Postgres Extension Monorepo Plan

## Purpose

This document is the implementation plan for adding a real Postgres-backed
SimpleBroker extension without turning SimpleBroker core into a built-in
multi-backend product.

The outcome of this plan should be:

- `simplebroker` remains the SQLite-first core package
- a Postgres backend exists as a **separate package** in this repo
- core exposes a **small public backend plugin hook**
- the same core CLI can drive either SQLite or Postgres when a project config
  selects the backend
- the same core Python API can use Postgres through an explicit `runner=...`
  path

This plan is intentionally practical.

- It does **not** try to design a grand backend framework.
- It does **not** add a built-in Postgres backend under `simplebroker/`.
- It does **not** require `src/` layout.
- It does **not** assume the existing SQLite-heavy internals are already backend
  neutral. They are not.

Assume the implementing engineer is technically strong, but:

- has zero context for this repository
- is likely to over-abstract if not constrained
- is likely to over-mock tests unless told not to

Follow this plan in order. Keep each step small. Keep the suite green after each
task.

## Scope Locks

These decisions are locked for this plan. Do not revisit them while
implementing.

### Product boundary lock

SimpleBroker core remains SQLite-first.

- Do **not** add `simplebroker/postgres/`, `simplebroker/backends/postgres/`, or
  any other built-in Postgres implementation under `simplebroker/`.
- The Postgres code lives in a **separate subproject**:
  - `extensions/simplebroker_pg/`
- The Postgres package is published separately.
- Core may expose a public plugin hook, but it must not ship Postgres itself.

### Packaging lock

Use a monorepo sibling subproject with a separate wheel.

- Put the extension in:
  - `extensions/simplebroker_pg/`
- Give it its own `pyproject.toml`.
- Put runtime code in a top-level package directory inside that subproject:
  - `extensions/simplebroker_pg/simplebroker_pg/`
- Do **not** use `src/`.
- Do **not** try to make one `pyproject.toml` build two unrelated wheels.

### Public API lock

Do not break the existing SQLite user experience.

- Existing SQLite `Queue`, `QueueWatcher`, `QueueMoveWatcher`, and CLI command
  behavior must continue to work unchanged when no project config selects a
  different backend.
- Existing CLI flags remain in place.
- `runner=` remains the advanced Python API extension seam.
- Do **not** add a public `backend=` argument to `Queue` in this plan.
- Do **not** reinterpret `-f/--file` as a DSN flag in this plan.

Additive behavior is allowed:

- a public backend plugin hook in `simplebroker.ext`
- a project config file that can select a backend for CLI/project usage
- additive internal types or helpers needed to carry backend/target information

### Dependency lock

Core keeps its current dependency promise.

- `simplebroker` itself must still have zero runtime dependencies in
  [`pyproject.toml`](/Users/van/Developer/simplebroker/pyproject.toml).
- Existing no-dependency tests for `simplebroker/` must remain true.
- The Postgres extension subproject may add its own dependency on `psycopg`
  3.x.
- Do **not** add `sqlalchemy`, `alembic`, `asyncpg`, `psycopg2`, or any ORM.

### Plugin lock

Use a small public plugin hook. Do not build a framework.

- The public hook belongs in [`simplebroker/ext.py`](/Users/van/Developer/simplebroker/simplebroker/ext.py).
- External backends are discovered by entry point name, not arbitrary import
  strings.
- The entry point group must be:
  - `simplebroker.backends`
- Core keeps its internal `_backends/` package for built-in SQLite internals.
- Do **not** expose `simplebroker._backends` as the public extension API.
- Do **not** add plugin managers, dependency-injection containers, or
  configuration registries.
- Do **not** extend or publicize the current internal
  `"_BROKER_INTERNAL_BACKEND"` config key. It is the wrong public seam.

### Project-config lock

Backend-selected CLI usage is project-config-driven in v1.

- Add a project config file:
  - `.broker.toml`
- The CLI may use this file to select a backend and target.
- Do **not** add a full public environment-variable backend-selection matrix in
  this first slice.
- `broker init` for a non-SQLite backend uses the project config if present.
- Do **not** make `broker init` author `.broker.toml` in v1. The file is
  user-authored.

### Postgres ownership-boundary lock

The Postgres extension must own a schema, not an entire database.

- The Postgres project config must include an explicit schema name in backend
  options.
- The schema must not be `public` in v1.
- `--cleanup` for Postgres drops only the managed schema contents or schema
  itself, not the entire database.
- This schema boundary is what makes `init`, `cleanup`, and validation safe.

If you try to avoid this ownership boundary, stop and escalate. The cleanup and
validation semantics become too ambiguous.

## Why This Is The Right Shape

There are three important facts about the current repository:

1. The public extension seam today is `runner=` via
   [`SQLRunner`](/Users/van/Developer/simplebroker/simplebroker/_runner.py) and
   [`simplebroker.ext`](/Users/van/Developer/simplebroker/simplebroker/ext.py).
2. Core database behavior is **not** backend-neutral yet because
   [`simplebroker/db.py`](/Users/van/Developer/simplebroker/simplebroker/db.py)
   still imports SQL from [`simplebroker._sql`](/Users/van/Developer/simplebroker/simplebroker/_sql/__init__.py)
   and lifecycle helpers from internal SQLite-oriented backend modules.
3. A large part of the suite is already black-box CLI testing via
   [`tests/conftest.py`](/Users/van/Developer/simplebroker/tests/conftest.py).

That means:

- a separate package is the right product boundary
- a small public plugin hook is the right runtime boundary
- the same CLI can exercise multiple backends if project config selects them
- but core must first stop binding SQL/dialect globally at import time

## Current State Map

Current state, simplified:

```text
python -m simplebroker.cli
        |
        v
    cli.py / commands.py
        |
        v
    Queue / DBConnection / BrokerDB / BrokerCore
        |
        +--> module-global SQLite SQL from simplebroker._sql
        +--> module-global backend helpers from simplebroker._backends (SQLite only)
        +--> SQLite path validation in helpers.py / cli.py
        |
        v
    SQLRunner
        |
        +--> default SQLiteRunner
        +--> injected custom runners (today: effectively SQLite-shaped)
```

The key blocker is not packaging. The key blocker is that `BrokerCore` still
chooses SQL and backend helpers globally instead of per backend instance.

## Target State

Target state after this plan:

```text
python -m simplebroker.cli
        |
        +--> legacy SQLite path mode
        |       |
        |       +--> current SQLite defaults and path semantics
        |
        +--> project config mode (.broker.toml)
                |
                +--> backend plugin name
                +--> backend target
                +--> backend options (e.g. schema)
                |
                v
         public plugin resolver (simplebroker.ext)
                |
                +--> built-in sqlite plugin adapter
                +--> external entry-point plugin(s)
                        |
                        +--> simplebroker_pg

Queue / DBConnection / BrokerCore
        |
        +--> backend plugin resolved per instance
        +--> SQL namespace resolved per instance
        +--> runner comes from:
                - explicit caller-supplied runner
                - or built-in SQLite default
                - or backend plugin create_runner(...)
```

### Important consequence

For CLI and project-config-driven usage, backend selection can be transparent.

For direct Python API usage, the advanced path remains:

```python
from simplebroker import Queue
from simplebroker_pg import PostgresRunner

runner = PostgresRunner(dsn, schema="simplebroker_app")
queue = Queue("jobs", runner=runner, persistent=True)
```

Do **not** try to make `Queue(..., db_path=..., backend="postgres")` part of
this first slice.

## Concrete Design Decisions

These are decisions, not open questions.

### 1. Public backend hook lives in `simplebroker.ext`

Add the plugin-facing contract to
[`simplebroker/ext.py`](/Users/van/Developer/simplebroker/simplebroker/ext.py).

Do not make external packages import internal `_backends`.

The public hook should include:

- `BackendPlugin` protocol
- `get_backend_plugin(name: str) -> BackendPlugin`
- any small support types needed by that protocol

### 2. Entry-point discovery, not arbitrary import strings

Use stdlib `importlib.metadata` entry points.

Good:

- built-in `"sqlite"` resolved internally
- external `"postgres"` resolved via entry point

Bad:

- `"package.module:thing"` strings in project config
- hand-rolled importlib object loading
- `eval` or module globals

### 3. Promote the backend contract just enough

Do not create separate “tiny public” and “real internal” backend contracts. That
would duplicate the same responsibilities in two places.

Instead, promote the existing backend responsibilities into one public extension
contract and add only the missing pieces needed for external backends:

- `name`
- `sql` namespace object
- `create_runner(target, *, backend_options, config)`
- `initialize_target(...)`
- `validate_target(...)`
- `cleanup_target(...)`
- existing lifecycle/schema/maintenance helpers already represented in the
  current built-in backend interface

Public names must be **target-oriented**, not SQLite-file-oriented.

Good:

- `validate_target`
- `initialize_target`
- `cleanup_target`

Bad:

- `validate_database(file_path=...)`
- `is_valid_sqlite_db(...)`

The built-in SQLite adapter may wrap existing file-based helpers internally, but
the public extension contract must not bake “file path” or “SQLite” into its
names.

### 4. SQL is provided as a namespace object, not as 40 forwarding methods

Do **not** put every SQL constant directly on the plugin object.

Each backend plugin should expose a `sql` namespace/module object with the same
shape the core needs:

- constants like `INSERT_MESSAGE`
- builder functions like `build_retrieve_query(...)`

That keeps the contract DRY and mirrors the existing
[`simplebroker._sql`](/Users/van/Developer/simplebroker/simplebroker/_sql/__init__.py)
shape.

### 5. Backend binding must be per instance, not module-global

This is non-negotiable.

The current module-global pattern:

- `db_backend = get_configured_backend(_config)`
- direct `from ._sql import ...`

must be removed from the core path that executes queue/database operations.

`BrokerCore` must know which backend plugin and SQL namespace it is using for
that specific runner instance.

### 6. Legacy injected runners must keep working

Existing callers that supply a custom SQLite-like runner must not break.

Compatibility rule:

- if a supplied runner advertises a backend plugin, use it
- otherwise treat the runner as SQLite-compatible

Do not break
[`tests/test_custom_runner_integration.py`](/Users/van/Developer/simplebroker/tests/test_custom_runner_integration.py).

### 6a. Name the runner-to-backend contract once

Do not make `BrokerCore` guess between multiple ad hoc runner attributes.

Add one additive protocol in
[`simplebroker/ext.py`](/Users/van/Developer/simplebroker/simplebroker/ext.py),
for example:

- `BackendAwareRunner(SQLRunner, Protocol)`

with one required attribute or property:

- `backend_plugin: BackendPlugin`

Implementation rule:

- when core is given an explicit runner, first check for `runner.backend_plugin`
- if present, use that plugin and its SQL namespace
- if absent, fall back to the built-in SQLite plugin for backward compatibility

Do **not** invent multiple optional attribute names like `backend`,
`backend_name`, `plugin`, `_backend`, or `_sql_namespace`.

One runner metadata seam is enough.

### 7. Project config file is the CLI backend selector

Add `.broker.toml` with this initial format:

```toml
version = 1
backend = "sqlite"
target = ".broker.db"

[backend_options]
# backend-specific settings
```

For Postgres, require:

```toml
version = 1
backend = "postgres"
target = "postgresql://postgres:postgres@localhost:5432/appdb"

[backend_options]
schema = "simplebroker_app"
```

Rules:

- `version` is required
- `backend` is required
- `target` is required
- `backend_options` is optional in general, but required for Postgres schema in
  v1

### 8. Project scope search must support config files first

When project scope is enabled:

1. search upward for `.broker.toml`
2. if found, use it and stop
3. if not found, preserve the current legacy SQLite database-file search

This preserves old SQLite behavior while enabling project-scoped external
backends.

### 9. `-f/--file` remains a SQLite-oriented path override in v1

Do not overload `-f` into a backend-agnostic DSN flag in this first slice.

If a non-SQLite project config is active:

- use the target from project config
- do not accept path-only overrides that imply legacy SQLite behavior

This avoids confusing precedence rules and keeps the first extension slice
under control.

### 10. Postgres watchers use polling fallback in v1

Do not try to implement LISTEN/NOTIFY in the first Postgres slice.

`get_data_version()` for Postgres may return `None`, and watchers should fall
back to the existing polling behavior. That is acceptable for v1.

## Repository Primer

### Project shape

- Core runtime code: `simplebroker/`
- Core tests: `tests/`
- Planning docs: `docs/plans/`
- Examples: `examples/`
- CI workflow: `.github/workflows/test.yml`
- New extension subproject target:
  - `extensions/simplebroker_pg/`

### Files you must read before changing anything

Read these first. Do not skip them.

- `pyproject.toml`
- `.github/workflows/test.yml`
- `README.md`
- `simplebroker/ext.py`
- `simplebroker/_runner.py`
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `simplebroker/watcher.py`
- `simplebroker/helpers.py`
- `simplebroker/cli.py`
- `simplebroker/commands.py`
- `simplebroker/_backends/__init__.py`
- `simplebroker/_backends/sqlite/__init__.py`
- `simplebroker/_sql/__init__.py`
- `simplebroker/_sql/sqlite.py`
- `tests/conftest.py`
- `tests/test_custom_runner_integration.py`
- `tests/test_ext_imports.py`
- `tests/test_no_dependencies.py`
- `tests/test_commands_init.py`
- `tests/test_project_scoping.py`
- `tests/test_cli_watch.py`
- `tests/test_status_command.py`
- `tests/test_vacuum_compact.py`
- `examples/example_extension_implementation.md`

### Tooling and commands

Use these commands from the repo root unless otherwise noted.

Core setup:

- `uv sync --extra dev`

Install the extension subproject into the same environment once it exists:

- `uv pip install -e "./extensions/simplebroker_pg[dev]"`

Local Postgres for development, if you do not already have one:

- `docker run --name simplebroker-pg-test -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=simplebroker_test -p 54329:5432 -d postgres:17`
- Example DSN:
  - `postgresql://postgres:postgres@127.0.0.1:54329/simplebroker_test`

Core targeted tests while iterating:

- `uv run pytest -q -n 0 tests/test_ext_imports.py tests/test_custom_runner_integration.py`
- `uv run pytest -q -n 0 tests/test_commands_init.py tests/test_project_scoping.py`
- `uv run pytest -q -n 0 tests/test_status_command.py tests/test_vacuum_compact.py`
- `uv run pytest -q -n 0 tests/test_cli_watch.py`

Core full validation:

- `uv run pytest -q -n 0`
- `uv run ruff check .`
- `uv run mypy simplebroker`

Extension targeted tests after the subproject exists:

- `uv run pytest -q -n 0 extensions/simplebroker_pg/tests`

Packaging smoke tests:

- `uv build`
- `cd extensions/simplebroker_pg && uv build`

Notes:

- `pytest-xdist` is enabled by default in the repo. Use `-n 0` while iterating
  on backend behavior.
- Do not add dependencies to core.
- Do not use mocks for database behavior when a real temp SQLite DB or real
  Postgres schema can be used.

## Engineering Rules

### Red-Green TDD

For additive behavior:

1. write a real failing test
2. run it and confirm it is red for the correct reason
3. make the smallest change that turns it green
4. refactor only after the behavior is pinned

For pure mechanical refactors:

1. identify the smallest covering existing test slice
2. run it before editing
3. make the move
4. rerun immediately

### Prefer real CLI and real database tests over mocks

Good:

- `run_cli(...)` from black-box subprocess tests
- temp workdirs
- real `.broker.toml` files
- real SQLite database files
- real Postgres schemas in a real Postgres instance
- thin recording wrappers over a real runner when testing runner boundaries

Bad:

- mocking `BrokerCore`
- mocking `DBConnection`
- mocking `psycopg` cursors
- asserting helper function call counts instead of observable broker behavior

### DRY

- If the same CLI setup is needed in both core and extension tests, move it once
  into a reusable test helper.
- If the same SQL contract names are needed across backends, put them on the SQL
  namespace object once. Do not duplicate forwarding wrappers.
- If a helper will only have one caller, do not extract it yet.

### YAGNI

Do not add:

- an ORM
- a query AST
- SQLAlchemy models
- Alembic migrations
- async support
- LISTEN/NOTIFY
- multiple competing plugin discovery mechanisms
- backend-specific CLI subcommands
- automatic project-config authoring

## Non-Goals

- No built-in Postgres backend inside `simplebroker/`
- No `src/` layout
- No branch-based product split
- No transparent Python `Queue(..., backend="postgres")` path in v1
- No attempt to make every SQLite-specific test pass unchanged against Postgres
- No `public` schema support for Postgres cleanup/init in v1
- No `-f` DSN reinterpretation

## Task 0: Baseline And Design Freeze

### Goal

Understand the current seam and establish a no-surprises baseline.

### Files to touch

- None

### Steps

1. Read every file listed in the primer.
2. Run:
   - `uv sync --extra dev`
   - `uv run pytest -q -n 0`
   - `uv run ruff check .`
   - `uv run mypy simplebroker`
3. Write down, in your own words:
   - how `Queue(..., runner=...)` reaches `BrokerCore`
   - where SQL is currently chosen
   - how CLI project scope resolves `.broker.db` today
   - which tests are truly backend-neutral and which are SQLite-specific

### Gate

You can explain why a new `PostgresRunner` alone is insufficient without a
backend/dialect binding change in `BrokerCore`.

## Task 1: Freeze The Extension Contract With Red Tests

### Goal

Pin the desired behavior before refactoring the backend boundary.

### Primary files to touch

- `tests/test_ext_imports.py`
- `tests/test_custom_runner_integration.py`
- `tests/conftest.py`
- optional new test files:
  - `tests/test_backend_plugin_resolution.py`
  - `tests/test_project_config.py`

### Required tests

1. **Public extension imports**
   - Extend `tests/test_ext_imports.py` to assert new plugin-facing exports from
     `simplebroker.ext`.
   - This should be the first red test for the new public hook.

2. **Legacy runner compatibility**
   - Keep the current `RecordingRunner(SQLiteRunner)` tests passing unchanged.
   - Add one explicit test that a runner without `backend_plugin` still uses
     the SQLite dialect path.

3. **Project config parsing and precedence**
   - Add failing tests for:
     - loading `.broker.toml`
     - preferring project config over legacy project-scope DB discovery
     - falling back to legacy SQLite behavior when no config file exists

4. **CLI helper flexibility**
   - Extend `run_cli(...)` in `tests/conftest.py` so it can accept an optional
     `env` mapping without changing existing callers.
   - This is not for mocks. It is to drive real subprocess behavior.

### Notes

- Do not start with Postgres-specific tests yet.
- First freeze the new public/core behavior and keep SQLite green.

### Gate

- existing injected-runner tests still pass
- new ext import tests fail before the code is added
- project-config tests fail for the right reason before implementation

## Task 2: Add The Public Backend Plugin Hook In Core

### Goal

Create the public plugin surface without changing runtime behavior yet.

### Primary files to touch

- `simplebroker/ext.py`
- optional new internal resolver module, recommended:
  - `simplebroker/_backend_plugins.py`
- `simplebroker/_backends/sqlite/__init__.py`
- `tests/test_ext_imports.py`
- optional:
  - `tests/test_backend_plugin_resolution.py`

### Design requirements

1. Add a public `BackendPlugin` protocol to `simplebroker.ext`.
2. Add a public `get_backend_plugin(name: str)` function.
3. Add a public `BackendAwareRunner` protocol so external runners can expose
   their backend plugin without private conventions.
4. Resolve `"sqlite"` internally without entry-point lookup.
5. Resolve external backends via entry point group `simplebroker.backends`.
6. Keep `_backends/` internal. Do not document it as the extension API.

### Implementation notes

- The resolver should live in an internal module, then be re-exported from
  `simplebroker.ext`.
- Do not make `simplebroker.ext` itself full of importlib logic.
- SQLite’s current internal backend package should be adapted so it can satisfy
  the public `BackendPlugin` protocol.

### Tests

- `uv run pytest -q -n 0 tests/test_ext_imports.py tests/test_backend_plugin_resolution.py`
- `uv run ruff check simplebroker/ext.py simplebroker/_backend_plugins.py tests/test_ext_imports.py`
- `uv run mypy simplebroker`

### Gate

- `simplebroker.ext` exports the new hook cleanly
- built-in `"sqlite"` resolves without entry points
- asking for an unknown backend fails with a clear exception

## Task 3: Make Backend And SQL Selection Per Instance

### Goal

Stop `BrokerCore` and related code from binding SQL/backend helpers globally.

This is the most important core refactor in the whole plan.

### Primary files to touch

- `simplebroker/db.py`
- `simplebroker/_runner.py`
- `simplebroker/sbqueue.py`
- `simplebroker/_sql/__init__.py` if needed for compatibility only
- optional new internal support module:
  - `simplebroker/_backend_binding.py`

### Required design

1. `BrokerCore` must hold:
   - `self._backend_plugin`
   - `self._sql`
2. `BrokerCore` must get those from either:
   - explicit constructor input
   - `runner.backend_plugin` when the runner satisfies
     `BackendAwareRunner`
   - or SQLite fallback for legacy runners
3. `DBConnection` must preserve current caller-owned runner behavior.
4. Legacy injected SQLite-like runners must continue to work.

### Strong recommendation

Do not keep dozens of module-level aliases in `db.py`.

Prefer:

```python
self._sql.INSERT_MESSAGE
self._sql.CHECK_PENDING_MESSAGES
self._sql.build_retrieve_query(...)
```

This is a large mechanical change. Keep it separate from CLI or Postgres work.

### Do not do this

- Do not try to leave `db.py` importing SQLite SQL globally “for now”.
- Do not create one abstraction for reads and another for writes.
- Do not hide backend choice in a mutable global singleton.

### Tests

Run at least:

- `uv run pytest -q -n 0 tests/test_custom_runner_integration.py`
- `uv run pytest -q -n 0 tests/test_status_command.py tests/test_vacuum_compact.py`
- `uv run pytest -q -n 0 tests/test_cli_watch.py`
- `uv run pytest -q -n 0 tests/test_queue_api_comprehensive.py`
- `uv run ruff check simplebroker/db.py simplebroker/_runner.py simplebroker/sbqueue.py`
- `uv run mypy simplebroker`

### Gate

- no module-global SQL selection in the active core path
- legacy injected-runner tests still pass
- all SQLite behavior remains green

## Task 4: Add Project Config And Backend-Aware CLI Resolution

### Goal

Allow the CLI to select a backend via project config while preserving legacy
SQLite path behavior.

### Primary files to touch

- `simplebroker/cli.py`
- `simplebroker/helpers.py`
- `simplebroker/commands.py`
- `simplebroker/_constants.py`
- recommended new internal module:
  - `simplebroker/_project_config.py`
- recommended new internal type:
  - `simplebroker/_targets.py`

### Required design

Create an internal resolved-target object. Use a dataclass. Example fields:

- `backend_name`
- `plugin`
- `target`
- `backend_options`
- `project_root`
- `used_project_scope`
- `legacy_sqlite_path_mode`

This object is internal. It is for CLI/command plumbing only.

Strong recommendation:

- make `DBConnection` and `Queue` accept this internal resolved-target object in
  addition to the existing string path input
- keep this additive and undocumented in the public API
- let those classes own plugin-created runners when the caller did **not**
  supply `runner=...`

Do **not** make each command hand-roll its own runner lifecycle in `try/finally`
blocks. Centralize ownership once.

### Required behavior

1. Add `.broker.toml` parsing.
2. Project scope search order becomes:
   - project config file
   - then legacy SQLite DB search
3. Legacy SQLite path mode still works exactly as before when no project config
   exists.
4. Non-SQLite project-config mode must skip SQLite-specific file validation and
   path checks that do not make sense.
5. `broker init`:
   - legacy SQLite path mode: preserve current behavior
   - project-config mode: call plugin `initialize_target(...)`
6. `broker --cleanup`:
   - legacy SQLite path mode: preserve current behavior
   - project-config mode: call plugin `cleanup_target(...)`
7. `broker status`, `write`, `read`, `peek`, `move`, `watch`, `vacuum`,
   `compact`, aliases:
   - route through the resolved backend target

### Important constraint

Do not overload `-f` into a DSN flag in this PR.

For v1:

- legacy path mode uses `-f` as today
- project-config mode uses the config file target

If you find yourself inventing complex precedence around DSNs and `-f`, stop.

Also keep `BrokerDB(db_path)` SQLite-first in this slice. Do not try to make
the direct `BrokerDB` constructor transparently backend-generic while you are
also changing the CLI resolution path.

### Tests

Core SQLite regression tests:

- `uv run pytest -q -n 0 tests/test_commands_init.py tests/test_project_scoping.py`
- `uv run pytest -q -n 0 tests/test_cli_watch.py`
- `uv run pytest -q -n 0 tests/test_status_command.py tests/test_vacuum_compact.py`

New project-config tests:

- config file discovery
- config-over-legacy precedence
- SQLite config file mode with relative target resolved relative to config file
- init/status/write/read roundtrip in SQLite config-file mode

### Gate

- CLI still works unchanged in legacy SQLite mode
- project config selects SQLite correctly
- non-SQLite backends are now plumbed end-to-end through the CLI resolution path

## Task 5: Create The `simplebroker_pg` Subproject Skeleton

### Goal

Create the separate package and wire packaging/discovery before implementing
full Postgres behavior.

### Files to add

- `extensions/simplebroker_pg/pyproject.toml`
- `extensions/simplebroker_pg/README.md`
- `extensions/simplebroker_pg/simplebroker_pg/__init__.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `extensions/simplebroker_pg/simplebroker_pg/sql.py`
- `extensions/simplebroker_pg/simplebroker_pg/schema.py`
- `extensions/simplebroker_pg/simplebroker_pg/validation.py`
- `extensions/simplebroker_pg/tests/`

### Packaging requirements

- No `src/`
- Add the entry point:
  - `simplebroker.backends`
  - `postgres = "simplebroker_pg.plugin:get_backend_plugin"`
- Depend on `simplebroker`
- Add Postgres dependency in this subproject only

### Dependency decision

Use `psycopg` 3.x for this extension.

Recommended for the first implementation:

- `psycopg[binary]>=3,<4`

Do not use `psycopg2`. Do not use SQLAlchemy.

### Tests

- packaging smoke test:
  - `uv pip install -e "./extensions/simplebroker_pg[dev]"`
- import smoke test in extension package

### Gate

- the extension package installs cleanly
- entry-point discovery finds `"postgres"`
- core package still has zero runtime dependencies

## Task 6: Implement The Postgres Backend Plugin And Runner

### Goal

Make the extension actually usable.

### Primary files to touch

- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `extensions/simplebroker_pg/simplebroker_pg/sql.py`
- `extensions/simplebroker_pg/simplebroker_pg/schema.py`
- `extensions/simplebroker_pg/simplebroker_pg/validation.py`

### Required Postgres design

#### Runner

Implement `PostgresRunner` with:

- thread-local psycopg connections
- transaction control matching the `SQLRunner` contract
- error translation to SimpleBroker exceptions
- setup-phase tracking compatible with `SetupPhase`

Do not add pooling in v1 unless a concrete test proves it is required.

#### Backend plugin

Implement plugin methods for:

- `create_runner(...)`
- `initialize_target(...)`
- `validate_target(...)`
- `cleanup_target(...)`
- schema/bootstrap hooks
- maintenance hooks
- SQL namespace exposure

#### Schema

Use a dedicated schema and create broker-owned tables there.

Minimum tables:

- `messages`
- `meta`
- `aliases`

Use a hidden internal ordering column, for example:

- `order_id BIGSERIAL PRIMARY KEY`

Message IDs remain the existing `ts BIGINT` values.

Ordering for claim/move/peek is by internal ordering column, not by `ts`.

#### Query strategy

Use real Postgres locking semantics:

- `FOR UPDATE SKIP LOCKED`
- CTE-based claim/move/delete patterns

Do not try to mimic SQLite query text.

#### Validation

Validation must check:

- target is reachable
- configured schema is valid
- schema is initialized when required
- magic/schema-version values are correct

#### `get_data_version`

Return `None` in v1 unless a simple reliable analogue exists. Polling fallback
is acceptable.

#### Vacuum / compact

Broker vacuum still means “delete claimed messages”.

For Postgres:

- normal vacuum: delete claimed rows in batches
- `compact=True`: run the backend’s compact hook after cleanup

For v1, a safe choice is:

- `VACUUM (ANALYZE)` on managed tables

Do **not** use `VACUUM FULL` in v1.

### Critical safety rule

Refuse to run the Postgres extension against schema `public`.

The extension must require an explicit non-public schema so `init`, `cleanup`,
and validation have a safe ownership boundary.

### Tests

Add real extension tests for:

- plugin initialization and validation
- queue write/read/peek
- move and claim semantics
- ordering by internal order column
- alias behavior
- watcher basic consume/peek behavior with polling fallback
- cleanup/init against a dedicated schema

Use a real Postgres instance. No mocks.

### Gate

- explicit Python `runner=PostgresRunner(...)` works
- core CLI can operate a Postgres-backed project when project config selects it
- SQLite suite remains green

## Task 7: Reuse Black-Box Tests Deliberately

### Goal

Exploit the existing black-box suite without pretending every test is
backend-neutral.

### Primary files to touch

- `tests/conftest.py`
- one or more new shared test helpers, recommended:
  - `tests/helper_scripts/cli_runner.py`
  - `tests/helper_scripts/project_config.py`
- selected new contract test files, recommended:
  - `tests/contracts/test_cli_contract.py`
  - `extensions/simplebroker_pg/tests/test_cli_contract.py`

### Test strategy

Split tests into three buckets.

#### Bucket A: reusable black-box contract tests

These should run against SQLite and Postgres with the same CLI:

- write/read/peek
- `--all`
- move
- delete
- list
- aliases
- status
- watcher basic consume/peek
- since filters
- broker vacuum semantics

#### Bucket B: SQLite-only tests

Keep these SQLite-only:

- path security
- symlink/file validation
- WAL/PRAGMA behavior
- SQLite-specific watcher/data-version behavior
- filesystem cleanup semantics

#### Bucket C: Postgres-only tests

Add these for the extension:

- schema validation and refusal of `public`
- Postgres locking behavior
- `FOR UPDATE SKIP LOCKED` concurrency
- schema cleanup semantics
- Postgres-specific status/size reporting

### DRY rule for tests

Do not duplicate the same CLI contract test body twice.

Extract small shared helpers for:

- writing project configs
- invoking CLI with project-config mode
- provisioning a dedicated Postgres schema for a test

### Gate

- shared black-box contract tests pass against SQLite and Postgres
- SQLite-only tests remain under core
- Postgres-only tests live with the extension package

## Task 8: CI, Packaging, And Documentation

### Goal

Ship the extension cleanly without destabilizing the core matrix.

### Primary files to touch

- `.github/workflows/test.yml`
- `README.md`
- `examples/example_extension_implementation.md`
- `extensions/simplebroker_pg/README.md`
- optional example:
  - `extensions/simplebroker_pg/examples/basic_usage.py`

### CI plan

Keep the existing core matrix intact.

Add a separate Linux-only Postgres job:

- one Python version is enough initially (`3.12` recommended)
- use a GitHub Actions Postgres service container
- install both core and extension packages
- run extension tests
- run the shared backend-neutral contract slice against Postgres

Do **not** make the full core OS/Python matrix depend on Postgres.

### Documentation plan

Root README:

- brief advanced section on external backend plugins
- state clearly that SQLite remains the default built-in backend
- point to the separate Postgres package

Extension README:

- installation
- project config format
- schema requirement
- explicit Python `PostgresRunner` usage
- CLI usage through `.broker.toml`

### Gate

- core CI remains readable and reasonably fast
- extension CI proves the package works
- docs are explicit about the boundary between core and extension

## Reviewer Checklist

Before merging, verify all of these are true.

- There is no built-in Postgres implementation under `simplebroker/`.
- The Postgres package lives under `extensions/simplebroker_pg/` with no
  `src/` directory.
- Core still has zero runtime dependencies.
- The public plugin hook is in `simplebroker.ext`, not `_backends`.
- Backend selection in `BrokerCore` is per instance, not module-global.
- Legacy SQLite runner injection still works.
- `.broker.toml` selects backend/target for CLI project mode.
- Legacy SQLite project-scope behavior still works when no config file is
  present.
- Postgres requires an explicit non-public schema.
- Postgres cleanup does not drop an entire database.
- Shared black-box tests are reused where they are genuinely backend-neutral.
- SQLite-only tests remain SQLite-only.
- No heavy mocks were added for database behavior.

## Stop / Escalate Conditions

Stop and escalate if any of these happen.

1. You find yourself adding a built-in `postgres` backend under
   `simplebroker/`.
2. You find yourself trying to make `-f` a generic DSN flag in the first slice.
3. You need SQLAlchemy, Alembic, or another ORM to make progress.
4. You cannot define a safe ownership boundary for Postgres cleanup/init.
5. You are about to duplicate the full internal backend contract into a second
   “public” contract instead of promoting the minimum real one.
6. You are trying to make every existing SQLite-specific test pass unchanged
   against Postgres.
7. The plan starts drifting toward a core product that ships and documents
   multiple built-in backends by default.

If any of those occur, stop, document the blocker, and ask for a design
decision before continuing.
