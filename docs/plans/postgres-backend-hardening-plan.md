# Postgres Backend Hardening Plan

## Purpose

This document is the implementation plan for turning the Postgres backend into a
first-class SimpleBroker backend without lowering the project’s quality bar.

This plan covers four things:

1. fixing the concrete pg backend correctness issues already found
2. tightening the backend plugin contract so it stops leaking SQLite history
3. borrowing the good ideas from `pgmq` without adopting `pgmq` itself
4. documenting a deferred, correctness-gated path for an optional local pg daemon

Assume the implementer is technically strong, but:

- has zero context for this repository
- is likely to over-abstract if not constrained
- is likely to over-mock tests unless told not to
- may chase “clever” SQL reuse even when explicit backend SQL would be clearer

Follow this plan in order. Keep each step small. Keep the suite green after each
task.

## Locked Decisions

These are decisions, not open questions. Do not revisit them while implementing.

### Product boundary lock

- `simplebroker` remains SQLite-first core software.
- Postgres stays a separate extension package in:
  - `extensions/simplebroker_pg/`
- Do **not** move Postgres runtime code under `simplebroker/_backends/`.
- Do **not** add a runtime dependency on `pgmq`.

### `pgmq` lock

We are **not** adopting `pgmq` as the hidden implementation of the current
`postgres` backend.

We **are** allowed to borrow the useful ideas:

- stable queue ordering with a monotonic physical key
- queue-local advisory locking for FIFO behavior
- efficient single-statement claim/update patterns
- wake-up hints inspired by PG-native notifications

We are **not** adopting:

- visibility-timeout semantics
- JSONB as the primary message model
- `pgmq`’s API shape
- per-queue table explosion
- `pgmq`’s schema or lifecycle as-is

### FIFO lock

Same-queue serialization is required for the pg backend.

- Consumers on the same queue must wait rather than overtake.
- Different queues may still proceed concurrently.
- `FOR UPDATE SKIP LOCKED` is not acceptable for same-queue claim/move paths.

This is an intentional design choice. It matches SQLite’s practical semantics
more closely and is the correct tradeoff for SimpleBroker.

### Backend SQL lock

Each backend owns its SQL.

- Add `extensions/simplebroker_pg/simplebroker_pg/_sql.py`.
- If a SQL statement is truly identical in semantics and shape, importing it is
  fine.
- If the backend needs different SQL, write different SQL. Prefer explicit
  duplication over dialect-normalization hacks.

Do **not** keep the current pattern where core builds SQLite-flavored WHERE
fragments and the pg backend string-rewrites them.

### Backend-appropriate operations lock

Operations like `init`, `cleanup`, `vacuum`, `compact`, and watcher wake-ups
must have real pg semantics.

- Backend-specific behavior is acceptable.
- Silent no-ops are not acceptable.
- “Equivalent” does not mean “identical implementation”; it means “real, safe,
  and documented behavior.”

### Schema ownership lock

The pg backend owns a schema, not an entire database.

- The configured schema must be explicit.
- `public` remains forbidden.
- `init` may create that schema and the broker-owned objects inside it.
- `cleanup` may destroy only a schema that SimpleBroker can prove it owns.
- If the schema exists but is foreign, partially broker-shaped, or otherwise not
  clearly owned, pg must refuse to touch it destructively.

### Quality lock

This work is correctness-first.

- Do not start the daemon work until the correctness and parity tasks are done.
- Do not ship LISTEN/NOTIFY without real integration coverage.
- Do not widen the plugin API more than necessary.
- Do not try to make the whole core fully backend-generic in one pass.

## Repository Primer

### Project shape

- Core runtime code lives in `simplebroker/`.
- Built-in SQLite backend code lives in `simplebroker/_backends/sqlite/`.
- Built-in SQLite SQL lives in `simplebroker/_sql/sqlite.py`.
- Postgres extension code lives in `extensions/simplebroker_pg/simplebroker_pg/`.
- Core tests live in `tests/`.
- Extension tests live in `extensions/simplebroker_pg/tests/`.
- Plan docs live in `docs/plans/`.

### The current runtime path

At a high level, the important path is:

```text
CLI / Queue
  -> target resolution / project config
  -> backend plugin
  -> runner
  -> BrokerCore / BrokerDB in simplebroker/db.py
  -> backend SQL namespace + backend lifecycle hooks
```

That means most pg fixes touch both:

- the extension package
- the backend-neutral orchestration in `simplebroker/db.py`

### Files you must read before changing anything

Read these first, in this order:

- `pyproject.toml`
- `extensions/simplebroker_pg/pyproject.toml`
- `simplebroker/_constants.py`
- `simplebroker/_backend_plugins.py`
- `simplebroker/ext.py`
- `simplebroker/_targets.py`
- `simplebroker/_project_config.py`
- `simplebroker/commands.py`
- `simplebroker/cli.py`
- `simplebroker/_runner.py`
- `simplebroker/db.py`
- `simplebroker/watcher.py`
- `simplebroker/_sql/sqlite.py`
- `simplebroker/_backends/sqlite/plugin.py`
- `simplebroker/_backends/sqlite/schema.py`
- `simplebroker/_backends/sqlite/maintenance.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `extensions/simplebroker_pg/simplebroker_pg/schema.py`
- `extensions/simplebroker_pg/simplebroker_pg/validation.py`
- `extensions/simplebroker_pg/simplebroker_pg/sql.py`
- `extensions/simplebroker_pg/tests/test_pg_integration.py`
- `tests/conftest.py`
- `tests/test_cleanup.py`
- `tests/test_broadcast.py`
- `tests/test_broadcast_integration.py`
- `tests/test_message_by_timestamp.py`
- `tests/test_status_command.py`
- `tests/test_sql_internals.py`

### External references to consult only when you reach the relevant task

- PostgreSQL advisory locks:
  - [docs](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)
- PostgreSQL `LISTEN` / `NOTIFY`:
  - [docs](https://www.postgresql.org/docs/current/sql-listen.html)
  - [docs](https://www.postgresql.org/docs/current/sql-notify.html)
- Psycopg transaction behavior:
  - [docs](https://www.psycopg.org/psycopg3/docs/basic/transactions.html)
- `pgmq` reference material:
  - [README](https://github.com/pgmq/pgmq)
  - [SQL schema/functions](https://raw.githubusercontent.com/pgmq/pgmq/main/pgmq-extension/sql/pgmq.sql)

Do not cargo-cult these references. Read them to understand the technique, then
apply only what matches SimpleBroker semantics.

## Local Environment And Tooling

### Use `uv`

The local workflow for this repository should use `uv`.

From the repo root:

- `uv sync --extra dev`
- `uv pip install -e "./extensions/simplebroker_pg[dev]"`

Use the local `.venv` if `uv` creates one. Do not add global tooling
dependencies.

### Local Postgres target

There is a local Postgres available via `../mm/.env`.

Setup for a shell session:

```bash
set -a
source ../mm/.env
set +a
export SIMPLEBROKER_PG_TEST_DSN="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:${POSTGRES_PORT:-5432}/simplebroker_pg_plan"
export BROKER_TEST_BACKEND=postgres
```

Rules:

- Use host `127.0.0.1`, not the Docker hostname, in local test DSNs.
- Do not hardcode credentials in committed files.
- Use the dedicated database `simplebroker_pg_plan`.
- Tests should create and clean up distinct schemas inside that database.

### Commands to use during development

Targeted pg extension tests:

- `uv run pytest -q -n 0 extensions/simplebroker_pg/tests`

Shared CLI tests under the pg backend:

- `BROKER_TEST_BACKEND=postgres uv run pytest -q -n 0 tests/test_cleanup.py tests/test_broadcast.py tests/test_broadcast_integration.py tests/test_message_by_timestamp.py tests/test_status_command.py`

Lint:

- `uv run ruff check .`

Type-check:

- `uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg`

Important:

- Use `-n 0` for pg work. Concurrency and timing tests are much easier to reason
  about in one test process.
- Do **not** try to run the entire core suite with `BROKER_TEST_BACKEND=postgres`.
  Too many tests still directly exercise SQLite internals and are not marked.

## Engineering Rules

### TDD: red, green, refactor

For each task:

1. add or strengthen a real test that fails on current `main`
2. run that targeted test and confirm it is red for the right reason
3. make the smallest code change that turns it green
4. refactor only after the contract is pinned by tests

Do not “fix code and then add tests.”

### Prefer real integration tests over mocks

Use real Postgres schemas and real `Queue` / `BrokerDB` / `PostgresRunner`
behavior.

Good:

- real psycopg connections
- real schema creation and cleanup
- real threads for lock-order tests
- real CLI subprocesses through `run_cli`

Bad:

- mocking psycopg cursors
- monkeypatching `runner.run()` to return fake rows
- asserting internal call counts instead of queue/database state
- fake transaction behavior

The only acceptable fake in this work is a tiny recording helper that delegates
to a real runner when you must verify emitted SQL shape. Use that sparingly.

### DRY

- Share helpers only when they are used at least three times.
- Prefer one small `extensions/simplebroker_pg/tests/_helpers.py` over
  copy-pasted DSN/schema setup in every test.
- Prefer one small internal query-spec object over passing backend-specific
  string fragments around.

### YAGNI

Do not build a grand backend framework.

Specifically:

- no ORM
- no migration framework
- no generic SQL AST
- no async rewrite
- no daemon in the same change set as correctness fixes
- no attempt to make every single core path perfectly backend-neutral

### Documentation discipline

Any backend-specific semantic difference that remains after this work must be:

- intentional
- tested
- documented in `README.md` and `extensions/simplebroker_pg/README.md`

If it is not documented, treat it as a bug.

## Current Failure Map

This is the problem set this plan fixes.

1. pg vacuum is effectively unreachable because core short-circuits on missing
   `db_path`.
2. pg cleanup can drop any syntactically valid schema whether SimpleBroker owns
   it or not.
3. pg claim/move paths use `SKIP LOCKED`, which allows overtaking and can make
   exact-id operations look temporarily “not found”.
4. pg broadcast relies on SQLite’s `BEGIN IMMEDIATE` semantics even though the
   pg runner only issues plain `BEGIN`.
5. pg delete counting materializes one row per deleted message into Python.
6. the backend plugin contract hardcodes SQLite history:
   - version-specific `ensure_schema_v2/v3/v4`
   - raw SQL strings passed into plugin delete helpers
   - maintenance split into SQLite-shaped micro-hooks
7. the pg SQL layer is not actually backend-owned because it normalizes
   SQLite-flavored WHERE fragments from core.
8. watcher change detection is SQLite-specific (`PRAGMA data_version`) and the
   pg backend has no equivalent fast path.
9. the pg test surface is far too thin for a backend with different transaction
   semantics.

## Target State

After this plan:

- the pg extension has its own `_sql.py` and owns its SQL cleanly
- plugin schema migration is generic and backend-owned
- backend schema versioning is plugin-owned rather than hardcoded in the public
  plugin protocol
- init/validate/cleanup operate on an explicit schema ownership state machine
- same-queue claim/move behavior is FIFO and serialized
- exact-id claim/move waits for locked rows instead of skipping them
- broadcast has an explicit pg write barrier
- vacuum and compact are real pg operations, not no-ops
- delete counts are O(1) in Python memory
- watchers can use pg-native wake-up hints while still treating the database as
  the source of truth
- docs clearly describe any intentional backend differences

## Task 0: Establish A Safe Baseline

### Goal

Start from a known-good environment and confirm you understand the current core
and extension boundaries.

### Files to touch

- None

### Steps

1. Read all files listed in the primer.
2. Set up the environment exactly as described above.
3. Run:
   - `uv sync --extra dev`
   - `uv pip install -e "./extensions/simplebroker_pg[dev]"`
   - `uv run pytest -q -n 0 tests/test_backend_plugin_resolution.py tests/test_project_config.py tests/test_ext_imports.py`
   - `uv run pytest -q -n 0 extensions/simplebroker_pg/tests/test_pg_integration.py`
   - `uv run ruff check .`
   - `uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg`
4. Confirm you can explain:
   - how a `Queue` operation reaches `BrokerCore`
   - how the backend plugin is selected
   - where the pg extension currently leaks SQLite assumptions

### Gate

Do not start implementation until you can explain, in one paragraph each:

- why the current `SKIP LOCKED` SQL changes SimpleBroker semantics
- why pg cleanup is unsafe today
- why the current plugin protocol does not scale past schema version 4

## Task 1: Build A Real pg Test Harness Before Fixing Code

### Goal

Expand pg coverage first so every later code change is pinned by real behavior.

### Primary files to touch

- `extensions/simplebroker_pg/tests/test_pg_integration.py`
- `extensions/simplebroker_pg/tests/_helpers.py` (new)

### Additional test files to add

- `extensions/simplebroker_pg/tests/test_pg_ownership.py`
- `extensions/simplebroker_pg/tests/test_pg_fifo_semantics.py`
- `extensions/simplebroker_pg/tests/test_pg_broadcast.py`
- `extensions/simplebroker_pg/tests/test_pg_vacuum.py`
- `extensions/simplebroker_pg/tests/test_pg_watch_notify.py`

### Test helper requirements

Create one shared pg test helper module that provides:

- DSN lookup
- unique schema name generation
- best-effort schema cleanup
- direct psycopg connection helper
- queue/runner construction helpers

Keep helpers thin. They should remove setup noise, not hide behavior.

### Red tests to add first

Add these tests before changing runtime code:

1. `init` is idempotent for an already-owned schema.
2. `init` refuses a foreign schema that already contains unrelated objects.
3. `cleanup` returns `False` for a missing schema.
4. `cleanup` refuses a foreign or partially broker-shaped schema.
5. vacuum deletes claimed rows while leaving unclaimed rows intact.
6. same-queue `read_one()` does not overtake a locked head row.
7. `read_one(exact_timestamp=...)` waits for the target row instead of acting
   like it does not exist.
8. `move_one(exact_timestamp=...)` waits for the target row instead of acting
   like it does not exist.
9. broadcast waits behind a concurrent writer and then includes the newly
   committed queue.
10. watcher wake-up via pg notifications works end-to-end.

### Test design guidance

For concurrency tests, use real locking.

Recommended pattern for overtaking tests:

- write two messages to the same queue
- open a raw psycopg transaction
- lock the head row with `FOR UPDATE`
- start a thread calling `read_one()` or `move_one()`
- assert the thread does **not** complete while the row is locked
- release the lock
- assert the thread returns the original head row, not the second row

Why this test matters:

- broken `SKIP LOCKED` returns the second row immediately
- correct FIFO behavior waits and then returns the first row

For broadcast serialization tests:

- open a raw psycopg transaction
- insert a message into a new queue but do not commit
- start a thread calling `broadcast`
- assert broadcast blocks
- commit the writer transaction
- assert broadcast completes
- assert the newly committed queue received the broadcast

For vacuum tests:

- create a mix of claimed and unclaimed rows
- call `vacuum()` and `vacuum(compact=True)`
- assert claimed rows are gone
- assert remaining rows are still readable in order

### Existing shared tests to reuse under `BROKER_TEST_BACKEND=postgres`

Use these as black-box cross-backend gates, not as replacements for the new pg
tests:

- `tests/test_cleanup.py`
- `tests/test_broadcast.py`
- `tests/test_broadcast_integration.py`
- `tests/test_message_by_timestamp.py`
- `tests/test_status_command.py`

### What not to do

- Do not fake row locking with mocks.
- Do not rely on sleep-only timing tests when a real lock can prove the point.
- Do not create a separate database per test. Use one dedicated database and
  distinct schemas.

### Gate

Run:

- `uv run pytest -q -n 0 extensions/simplebroker_pg/tests/test_pg_ownership.py extensions/simplebroker_pg/tests/test_pg_fifo_semantics.py extensions/simplebroker_pg/tests/test_pg_broadcast.py extensions/simplebroker_pg/tests/test_pg_vacuum.py extensions/simplebroker_pg/tests/test_pg_watch_notify.py`

The new tests must be red for the expected current bugs before you fix code.

## Task 2: Tighten The Backend Plugin Contract And SQL Ownership

### Goal

Remove the SQLite-history leakage from the plugin API and make backend-owned SQL
explicit.

### Primary files to touch

- `simplebroker/_backend_plugins.py`
- `simplebroker/_constants.py`
- `simplebroker/ext.py`
- `simplebroker/db.py`
- `simplebroker/_backends/sqlite/plugin.py`
- `simplebroker/_backends/sqlite/schema.py`
- `simplebroker/_sql/sqlite.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py` (new)
- `extensions/simplebroker_pg/simplebroker_pg/schema.py`
- `tests/test_sql_internals.py`

### Required contract changes

Make these API changes to the backend plugin protocol:

1. Replace version-specific migration hooks:
   - remove `ensure_schema_v2`
   - remove `ensure_schema_v3`
   - remove `ensure_schema_v4`
   - add one generic `migrate_schema(...)`
2. Move schema version ownership to the backend plugin:
   - add `schema_version: int`
   - core must validate against `plugin.schema_version`, not a hardcoded
     version ladder in the plugin protocol
   - stop using the global `SCHEMA_VERSION` constant as a cross-backend version
     source; either retire it or make it SQLite-local only
3. Replace raw SQL delete counting:
   - remove `delete_and_count_changes(runner, sql, params)`
   - add `delete_messages(runner, *, queue: str | None) -> int`
4. Replace SQLite-shaped maintenance micro-hooks:
   - remove `has_claimed_messages`
   - remove `delete_claimed_batch`
   - remove `compact_database`
   - remove `maybe_run_incremental_vacuum`
   - add one semantic method such as
     `vacuum(runner, *, compact: bool, config: Mapping[str, Any]) -> None`

These changes are the point of this task. Do not preserve the old contract and
add more hooks on top of it.

### SQL ownership changes

Create `extensions/simplebroker_pg/simplebroker_pg/_sql.py` and move all pg SQL
there.

Delete `extensions/simplebroker_pg/simplebroker_pg/sql.py` in the same change
set once imports are updated. Do not keep both files around.

Then remove the current backend leak:

- core must stop building SQLite-flavored WHERE strings
- pg must stop normalizing `"claimed = 0"` into `"claimed = FALSE"`

Recommended approach:

- introduce one small internal query-spec object for retrieve operations
- make both SQLite and pg SQL builders accept that spec
- keep the spec dumb and explicit

A good spec contains fields like:

- `queue`
- `exact_timestamp`
- `since_timestamp`
- `require_unclaimed`
- `limit`
- `offset`
- `target_queue`

Do **not** build a generic SQL expression tree. A tiny dataclass is enough.

### Schema migration shape

Use one backend-owned migration function with an ordered list of migrations.

In `simplebroker/db.py`, replace:

- `_ensure_schema_v2()`
- `_ensure_schema_v3()`
- `_ensure_schema_v4()`

with one `_migrate_schema()` call that:

- reads the current stored schema version
- compares it to `self._backend_plugin.schema_version`
- asks the backend plugin to apply missing migrations in order
- fails cleanly if the stored version is newer than the backend supports

Good:

- `plugin.schema_version = <backend target version>`
- `plugin.migrate_schema(...)` applies missing steps up to that version

Bad:

- `ensure_schema_v2`, `ensure_schema_v3`, `ensure_schema_v4`, `ensure_schema_v5`
- a new public plugin method for every schema increment forever

### SQLite expectations

SQLite should also be moved to the new contract in the same change set.

- Keep SQLite behavior unchanged.
- Keep SQLite schema version at its current value unless a real SQLite migration
  is needed.
- Do not break the built-in backend while cleaning up the pg path.

### Tests to add or update

- update `tests/test_sql_internals.py` for the new query-spec builder contract
- add pg SQL builder coverage in `extensions/simplebroker_pg/tests/test_pg_sql.py`
- add one test confirming `delete_messages(..., queue=None)` and
  `delete_messages(..., queue="x")` return correct counts without materializing
  rows in Python

### What not to do

- Do not keep the old contract “for compatibility” inside this repo.
- Do not create a backend-agnostic SQL DSL.
- Do not hide SQL generation in utility functions that obscure query shape.
- Do not rename `begin_immediate()` in this change set. Remove the backend-neutral
  code’s dependency on SQLite-only locking semantics instead.

### Gate

Run:

- `uv run pytest -q -n 0 tests/test_sql_internals.py extensions/simplebroker_pg/tests/test_pg_sql.py`
- `uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg`

## Task 3: Implement A Safe Schema Ownership State Machine

### Goal

Make pg `init`, `validate`, and `cleanup` safe, explicit, and predictable.

### Primary files to touch

- `extensions/simplebroker_pg/simplebroker_pg/validation.py`
- `extensions/simplebroker_pg/simplebroker_pg/schema.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `simplebroker/commands.py`
- `simplebroker/cli.py`
- `extensions/simplebroker_pg/README.md`

### Required ownership states

Model the configured schema as one of these states:

- `ABSENT`
- `EMPTY`
- `OWNED`
- `FOREIGN`
- `PARTIAL_SIMPLEBROKER`

Definitions:

- `ABSENT`: schema does not exist
- `EMPTY`: schema exists but has no broker objects and no foreign objects
- `OWNED`: schema contains the broker tables and the correct magic marker
- `FOREIGN`: schema contains objects that are not broker-owned
- `PARTIAL_SIMPLEBROKER`: schema looks like an incomplete or damaged broker
  schema and must be repaired manually, not silently adopted

### Required behavior

`initialize_target(...)`:

- `ABSENT` -> create schema and initialize broker objects
- `EMPTY` -> initialize broker objects
- `OWNED` -> idempotent success and run migrations if needed
- `FOREIGN` -> fail
- `PARTIAL_SIMPLEBROKER` -> fail

`validate_target(..., verify_initialized=True)`:

- `OWNED` -> success if version is supported
- any other state -> fail with a precise error

`validate_target(..., verify_initialized=False)`:

- verify DSN connectivity
- verify schema name rules
- permit `ABSENT`, `EMPTY`, or `OWNED`
- reject `FOREIGN` and `PARTIAL_SIMPLEBROKER`

`cleanup_target(...)`:

- `ABSENT` -> return `False`
- `OWNED` -> drop the schema and return `True`
- `EMPTY` -> fail; we do not own it
- `FOREIGN` -> fail
- `PARTIAL_SIMPLEBROKER` -> fail

### Implementation guidance

Add one inspection helper in `validation.py` that returns structured state and
details. Keep the policy decisions in `plugin.py`, not buried inside ad-hoc SQL
checks.

Use the broker magic row as the primary ownership signal.

Do not treat “schema name matches regex” as ownership.

### Required tests

- `test_pg_init_creates_absent_schema`
- `test_pg_init_adopts_empty_schema_only`
- `test_pg_init_refuses_foreign_schema`
- `test_pg_cleanup_refuses_foreign_schema`
- `test_pg_cleanup_refuses_partial_broker_schema`
- `test_pg_cleanup_owned_schema_only`
- `test_pg_validate_verify_initialized_false_rejects_foreign_schema`

### Documentation updates

Document the exact pg ownership rules in:

- `extensions/simplebroker_pg/README.md`
- the main `README.md` backend/project-config section if it mentions cleanup or
  init behavior

### Gate

Run:

- `BROKER_TEST_BACKEND=postgres uv run pytest -q -n 0 extensions/simplebroker_pg/tests/test_pg_ownership.py tests/test_cleanup.py`

## Task 4: Restore FIFO Claim/Move Semantics With Queue-Local Advisory Locks

### Goal

Make same-queue claim and move behavior match SQLite’s practical FIFO semantics.

### Primary files to touch

- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `extensions/simplebroker_pg/simplebroker_pg/schema.py`
- `simplebroker/db.py`
- `extensions/simplebroker_pg/tests/test_pg_fifo_semantics.py`

### Required behavior

- oldest visible row in a queue wins
- a concurrent reader on the same queue waits instead of overtaking
- exact-timestamp claim/move waits for the target row instead of behaving as
  though it is missing
- different queues can still make progress concurrently

### Recommended implementation

Use a queue-local advisory **transaction** lock for claim/move.

Suggested shape:

1. derive a stable integer lock key from queue name
2. take `pg_advisory_xact_lock(...)` for that queue
3. select/update the head row in deterministic `order_id` order
4. return results in selection order

Important:

- do **not** use Python’s built-in `hash()`
- do use a stable, deterministic hash helper
- keep the lock namespace separate from any maintenance lock namespace

Recommended hash helper:

- a small helper using `hashlib.blake2b(..., digest_size=8)` and signed 64-bit
  conversion

That helper is deterministic across processes and test runs. Add a unit test for
it.

Generator note:

- `at_least_once` generator batches must hold the same queue lock for the full
  open transaction
- otherwise one consumer can interleave into another consumer’s uncommitted
  batch and violate queue-local serialization

### SQL guidance

Remove `SKIP LOCKED` from same-queue claim/move SQL.

For exact-once single-statement operations, it is acceptable to:

- keep explicit transactions if they make the code simpler, or
- rely on one statement per autocommit transaction

Do **not** spend time micro-optimizing away `BEGIN` calls unless the test or
profile data proves it matters. The required fix is semantic correctness, not a
transaction-style rewrite.

### Index improvements to include here

Add a partial index that helps `since_timestamp` scans:

- `(queue, ts, order_id) WHERE claimed = FALSE`

Keep the existing queue/order index for straight FIFO head selection.

This is a backend-specific physical optimization and is appropriate for pg.

Because this adds backend-specific schema objects, bump the pg backend schema
version and migrate it through the new generic migration hook.

### Required tests

- `test_same_queue_read_does_not_overtake_locked_head`
- `test_same_queue_move_does_not_overtake_locked_head`
- `test_exact_timestamp_read_waits_for_locked_row`
- `test_exact_timestamp_move_waits_for_locked_row`
- `test_different_queues_can_progress_concurrently`

### What not to do

- Do not reintroduce `SKIP LOCKED` for “throughput”.
- Do not serialize all queues globally just because one queue must be FIFO.
- Do not add per-queue tables.

### Gate

Run:

- `uv run pytest -q -n 0 extensions/simplebroker_pg/tests/test_pg_fifo_semantics.py`
- `BROKER_TEST_BACKEND=postgres uv run pytest -q -n 0 tests/test_message_by_timestamp.py`

## Task 5: Add An Explicit pg Write Barrier For Broadcast

### Goal

Make pg broadcast semantics deliberate rather than accidentally weaker than
SQLite.

### Primary files to touch

- `simplebroker/db.py`
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
- `extensions/simplebroker_pg/tests/test_pg_broadcast.py`

### Required behavior

Broadcast must:

- see a stable set of queues for the transaction
- insert to each target queue atomically
- not silently race concurrent writers creating new queues

### Recommended implementation

Use an explicit pg table-level write barrier inside the broadcast transaction.

Recommended shape:

1. begin transaction
2. `LOCK TABLE messages IN SHARE ROW EXCLUSIVE MODE`
3. select distinct queues
4. filter by pattern if needed
5. generate timestamps
6. insert broadcast rows
7. commit

Why this approach:

- it matches SQLite’s “writer barrier” intent more closely
- it does not require every mutating path in the system to cooperate with a new
  advisory lock protocol
- it keeps the rule local to the one operation that needs global queue-set
  stability

### Tests to add

- `test_broadcast_blocks_behind_concurrent_writer`
- `test_broadcast_includes_new_queue_committed_before_barrier_release`
- `test_broadcast_pattern_still_works_under_barrier`

Also run the existing shared CLI broadcast tests under pg.

### What not to do

- Do not rely on `begin_immediate()` for pg semantics here.
- Do not use a global advisory lock unless you also prove every conflicting
  mutator participates. That is more complex than needed.

### Gate

Run:

- `uv run pytest -q -n 0 extensions/simplebroker_pg/tests/test_pg_broadcast.py`
- `BROKER_TEST_BACKEND=postgres uv run pytest -q -n 0 tests/test_broadcast.py tests/test_broadcast_integration.py`

## Task 6: Make Vacuum, Compact, And Delete Counting Real pg Operations

### Goal

Give the pg backend real maintenance semantics and remove the current no-op /
materialization bugs.

### Primary files to touch

- `simplebroker/_backend_plugins.py`
- `simplebroker/db.py`
- `simplebroker/_backends/sqlite/plugin.py`
- `simplebroker/_backends/sqlite/maintenance.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
- `extensions/simplebroker_pg/tests/test_pg_vacuum.py`

### Required design changes

Move vacuum orchestration behind the backend plugin.

Core should no longer:

- assume a filesystem lock path exists
- call backend-specific delete batches directly
- decide how compaction works for every backend

Recommended contract:

- `plugin.vacuum(runner, *, compact: bool, config: Mapping[str, Any]) -> None`

Backend responsibilities:

- SQLite keeps file-lock-based vacuum semantics
- pg uses a schema-scoped advisory **session** lock and backend-appropriate
  compaction

Use a session-level advisory lock for pg vacuum because the operation spans
multiple transactions and separate `VACUUM` statements.

### pg vacuum semantics

Implement pg vacuum in two phases:

1. logical cleanup
   - delete claimed rows in batches
   - commit after each batch
2. physical cleanup
   - `compact=False`: rely on autovacuum; optionally run `ANALYZE`
   - `compact=True`: run `VACUUM (ANALYZE)` on broker tables

This is backend-appropriate and honest.

### Delete counting fix

Do not materialize deleted rows into Python.

Use one of these:

- a server-side counting CTE:
  - `WITH deleted AS (DELETE ... RETURNING 1) SELECT COUNT(*) FROM deleted`
- or a runner API that exposes rowcount cleanly

Prefer the counting CTE in this change set unless a rowcount result object is
already needed for another reason. It works with the current runner shape and
keeps scope down.

### Required tests

- `test_pg_vacuum_deletes_claimed_rows`
- `test_pg_compact_runs_without_error_after_cleanup`
- `test_pg_concurrent_vacuums_serialize`
- `test_pg_delete_messages_returns_correct_count_for_queue`
- `test_pg_delete_messages_returns_correct_count_for_all`

For the delete-count tests, use enough rows to prove you are not relying on a
row-per-delete Python result list. You do not need to build a memory benchmark;
you do need to remove the old SQL shape and assert the new count is correct.

### What not to do

- Do not keep the `hasattr(self, "db_path")` early return.
- Do not make pg vacuum a no-op “because Postgres has autovacuum”.
- Do not add a maintenance daemon in order to fix vacuum.

### Gate

Run:

- `uv run pytest -q -n 0 extensions/simplebroker_pg/tests/test_pg_vacuum.py`
- `BROKER_TEST_BACKEND=postgres uv run pytest -q -n 0 tests/test_status_command.py`

## Task 7: Add LISTEN/NOTIFY Wake-Up Hints For Watchers

### Goal

Reduce idle watcher latency on pg without changing watcher correctness.

### Primary files to touch

- `simplebroker/_backend_plugins.py`
- `simplebroker/watcher.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
- `extensions/simplebroker_pg/tests/test_pg_watch_notify.py`

### Correctness rule

Notifications are wake-up hints only.

The database remains the source of truth.

That means:

- a dropped notification must not lose a message
- a duplicated notification must not duplicate message delivery
- watchers must still re-check the database after waking

### Recommended abstraction

Add one small optional backend hook for watcher wake-up support.

Good shape:

- `create_activity_waiter(target, *, backend_options, queue_name, stop_event) -> ActivityWaiter | None`

Where `ActivityWaiter` is a tiny protocol with methods like:

- `wait(timeout: float) -> bool`
- `close() -> None`

Use this inside `PollingStrategy` as an additional wake-up source. If it returns
`None`, existing polling behavior remains unchanged.

### pg implementation guidance

Use a schema-scoped notification channel and queue-name payload filtering.

Recommended design:

- one channel per schema namespace
- queue name or `"*"` in the payload
- watchers listen for their queue name or `"*"`
- writes, moves-to-target, and broadcasts send notifications

Channel naming rule:

- do not derive the channel name directly from the raw schema string
- channel identifiers are length-limited
- use a stable hashed channel name derived from schema identity

Listener connection rule:

- use one dedicated autocommit psycopg connection for `LISTEN`
- do not reuse the main runner connection for blocking notification waits

Do **not** overfit this:

- do not send message bodies in notifications
- do not treat notifications as acknowledgements
- do not build a separate durable notification queue

Application-issued `pg_notify(...)` is sufficient for this change set. Do not
add triggers unless profiling later proves the application path is the problem.

### Tests to add

- `test_pg_watcher_is_woken_by_notify`
- `test_pg_watcher_filters_other_queue_notifications`
- `test_pg_watcher_falls_back_if_notification_connection_dies`
- `test_pg_broadcast_notification_wakes_all_relevant_watchers`

The first test should set a relatively slow polling interval so a fast pass
proves the notify wake-up path is doing real work.

### What not to do

- Do not replace polling entirely.
- Do not remove `data_version` support for SQLite.
- Do not add backend-specific watcher code paths directly in CLI commands.

### Gate

Run:

- `uv run pytest -q -n 0 extensions/simplebroker_pg/tests/test_pg_watch_notify.py`

## Task 8: Clean Up Backend-Specific Documentation And Examples

### Goal

Make the documented contract match the actual backend behavior.

### Primary files to touch

- `README.md`
- `extensions/simplebroker_pg/README.md`
- `examples/example_extension_implementation.md`

### Required doc updates

Document these explicitly:

- pg schema ownership rules
- pg FIFO behavior and same-queue serialization
- pg vacuum / compact semantics
- pg watcher notifications as hints only
- the fact that pg borrows ideas from `pgmq` but does not implement `pgmq`
  semantics

If the public backend plugin contract changed, update the extension example doc
to match the new hook names and migration shape.

### Gate

Run:

- `uv run pytest -q -n 0 tests/test_ext_imports.py tests/test_project_config.py`

Then manually read both READMEs and confirm they do not claim behavior that the
tests contradict.

## Task 9: Final Parity And Regression Sweep

### Goal

Prove the new pg backend is real, safe, and integrated without breaking SQLite.

### Required validation

Run all of the following before you consider this work done:

- `uv run pytest -q -n 0 extensions/simplebroker_pg/tests`
- `BROKER_TEST_BACKEND=postgres uv run pytest -q -n 0 tests/test_cleanup.py tests/test_broadcast.py tests/test_broadcast_integration.py tests/test_message_by_timestamp.py tests/test_status_command.py`
- `uv run pytest -q -n 0 tests/test_backend_plugin_resolution.py tests/test_project_config.py tests/test_ext_imports.py`
- `BROKER_TEST_BACKEND=sqlite uv run pytest -q -n 0 tests/test_cleanup.py tests/test_broadcast.py tests/test_broadcast_integration.py tests/test_message_by_timestamp.py tests/test_status_command.py`
- `uv run ruff check .`
- `uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg`

Notes:

- The shared test subset is intentionally run twice:
  - once with `BROKER_TEST_BACKEND=postgres`
  - once with `BROKER_TEST_BACKEND=sqlite`
- Do not skip the SQLite pass. Contract cleanup in the plugin layer can break
  SQLite quietly if you are not careful.

### Final acceptance checklist

All of these must be true:

- pg cleanup never drops a foreign schema
- pg vacuum is reachable and removes claimed rows
- pg exact-id operations do not transiently lie when rows are locked
- pg same-queue reads and moves do not overtake
- pg broadcast has a real write barrier
- pg delete counting does not materialize one row per delete into Python
- pg watchers can wake on notifications and still behave correctly when
  notifications are lost
- SQLite behavior remains unchanged
- docs match reality

## Deferred Follow-On: Self-Managed pg Daemon

This is reasonable, but it is **not** part of the correctness plan above.

Do not start this until every previous gate is green.

### Why a daemon is reasonable for pg

Because Postgres already stores the durable state, a local daemon can stay thin.
Its job would be:

- keep imports warm
- keep a connection pool warm
- keep a LISTEN connection warm
- expose a small local RPC surface for the CLI

That can reduce per-command startup cost without changing external semantics.

### Why it must be deferred

The daemon amplifies any correctness bug that already exists.

Do not build a daemon on top of:

- weak cleanup semantics
- weak watcher semantics
- weak FIFO guarantees

### If and only if you start this follow-on

Keep it extension-local:

- `extensions/simplebroker_pg/simplebroker_pg/daemon.py`
- `extensions/simplebroker_pg/simplebroker_pg/daemon_client.py`
- `extensions/simplebroker_pg/simplebroker_pg/daemon_protocol.py`

### Locked daemon constraints

- opt-in or backend-scoped auto mode only; do not change SQLite behavior
- local IPC only
- auto-start on demand
- idle shutdown after timeout
- config hash / schema / DSN validation on connect
- stale socket cleanup
- watchers and long-running streams pin the daemon so it cannot exit mid-work

### Daemon tests to require before shipping

- first CLI call auto-starts the daemon
- a second CLI call reuses the daemon
- daemon shuts down after idle timeout
- stale socket/pid cleanup works
- config mismatch forces reconnect or restart
- active watcher prevents idle shutdown
- killing the daemon does not corrupt the queue; the next CLI call recovers

### What not to do

- Do not turn the daemon into the source of truth.
- Do not make the daemon mandatory for pg usage.
- Do not combine daemon work with the correctness PR.

## Final Reminder

This plan is intentionally opinionated.

If you find yourself drifting toward any of the following, stop and back up:

- “I can keep `SKIP LOCKED`; it’s probably fine”
- “I’ll just make pg vacuum a no-op”
- “I’ll keep the old plugin contract and add one more hook”
- “I can reuse SQLite SQL and patch it with string replacements”
- “I’ll test this with mocks because it’s faster”
- “I’ll build the daemon first and fix correctness later”

Those are all wrong turns for this repository.
