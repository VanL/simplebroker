# Physical Delete and Batch Delete Plan

Date: 2026-05-17

Status: proposed

Owner: SimpleBroker

## Purpose

Change exact message deletion so `delete` physically removes rows or backend
entries instead of marking them claimed. Add a batch exact-delete API so cleanup
callers can physically remove many known message IDs without doing one
operation per row.

This is a SimpleBroker fix. Downstream users such as Weft exposed the bug, but
the API contract is wrong in SimpleBroker: `Queue.delete(message_id=...)` says
"delete" and behaves like "claim".

Read this whole plan before coding. The intended engineer is skilled but has no
context for this repository. Follow the tasks in order. Use red-green TDD. Keep
the implementation boring and direct.

## Locked Decisions

Do not reopen these decisions unless a red test proves the plan cannot work.

### Semantics

`read` and `claim` remain logical consume operations.

- They mark messages claimed.
- They keep the performance win that motivated the claimed-row abstraction.
- Claimed rows stay hidden from normal reads and peeks.
- `vacuum` and auto-vacuum remain responsible for physical cleanup of rows
  claimed by reads.

`delete` becomes physical delete.

- `Queue.delete(message_id=...)` must remove the matching backend entry.
- `broker delete QUEUE -m ID` must remove the matching backend entry.
- Deleting by ID must work for both unclaimed and claimed messages.
- Deleting by ID must not remove a message from a different queue, even though
  timestamps are globally unique in SQL backends.
- Whole-queue delete and all-queue delete already physically remove rows. Keep
  that behavior.

There is no backwards compatibility mode.

- Do not add a feature flag.
- Do not keep a soft-delete option under `delete`.
- If a caller wants logical consume by ID, they should use `read_one()`,
  `read(message_id=...)`, or the internal claim API.

### Batch API

Add a real batch exact-delete API.

Use these names unless implementation uncovers a concrete conflict:

```python
BrokerCore.delete_message_ids(queue: str, message_ids: Sequence[int]) -> int
Queue.delete_many(message_ids: Sequence[int]) -> int
```

Return the number of physically deleted messages.

- Empty input returns `0`.
- Duplicate IDs count once.
- Missing IDs do not count.
- IDs from another queue do not count and must not be removed.
- Claimed and unclaimed rows both count if they match the queue and ID.

Do not add a CLI batch delete in this change. The immediate need is a Python API
for cleanup callers. A CLI shape for batch input needs separate UX decisions.

### Batch Implementation

Do not implement batch delete as:

- `ts = ? OR ts = ? OR ...`
- one `DELETE` statement per ID
- interpolated SQL strings containing IDs

The `OR` form can be prohibitively slow because it creates a huge expression
tree, increases SQL parse/plan cost, and can produce bad planner choices. A
one-row loop recreates the exact cleanup hot path that caused this incident.

Use set-based delete per backend:

- SQLite: stage IDs in a connection-local temporary table, then delete by
  joining that table to `messages`.
- Postgres: pass IDs as an array and delete through an `unnest`/CTE join.
- Redis: use one Lua script to delete all matching IDs atomically from pending
  and claimed structures.

Rejected alternatives:

- Large `OR` predicate: this is the known bad path. It creates a large SQL
  expression tree and can spend too much time parsing or choosing a plan before
  doing useful delete work.
- One statement per ID: simple but wrong for this workload. It increases
  Python overhead, backend round trips, lock churn, and transaction overhead.
- SQLite giant `IN (?, ?, ...)` or `VALUES` CTE as the only strategy: better
  than `OR`, but it still creates large SQL strings and hits SQLite parameter
  limits. A temp table keeps the delete query stable and lets inserts be
  chunked.
- Postgres temp table plus `COPY`: likely faster for extremely large batches,
  but more code and more connection-state complexity. Start with array
  `unnest`, then require the slow benchmark to prove whether that is enough.
- Public `SKIP LOCKED`: useful for a nonblocking maintenance-only API, but bad
  for `delete_many()` because an existing locked row would look like a missing
  row. Keep public delete deterministic.

### In-Flight Batches

Do not silently delete messages reserved by an active at-least-once batch.

SQL backends protect active generator batches through transactions and row
locks. Use ordinary transactional delete semantics. Do not add `SKIP LOCKED` to
the public delete path in this change because it would make `delete_many()`
return "not deleted" for rows that exist but are temporarily locked.

Redis has explicit `reserved` sets. The Redis batch delete script must recover
stale batches first. If any requested ID is still reserved after recovery, it
should fail the operation with `OperationalError` and delete nothing. This
matches the existing queue-wide Redis delete safety rule.

### Scope Boundaries

Do not tune auto-vacuum in this change.

Auto-vacuum is still useful, but it is not the fix for exact cleanup. This plan
only changes explicit delete semantics and adds batch delete. A later change may
make claim-heavy workloads trigger auto-vacuum checks more directly.

Do not change timestamp generation, queue ordering, watcher behavior, or move
semantics.

## Repository Primer

Runtime package:

- `simplebroker/`

Core API files:

- `simplebroker/sbqueue.py`
  - Public `Queue` API.
  - `Queue.delete(message_id=...)` currently calls `connection.claim_one(...)`.
  - Add `Queue.delete_many(...)` here.
- `simplebroker/db.py`
  - SQL-backed `BrokerCore` and `BrokerDB`.
  - Add `BrokerCore.delete_message_ids(...)` here.
  - Use `_run_with_retry`, `_lock`, `_check_fork_safety`,
    `_validate_queue_name`, and `_assert_no_reentrant_mutation_during_batch`.
- `simplebroker/_backend_plugins.py`
  - Protocol definitions.
  - Add `delete_message_ids(...)` to `BackendPlugin` for SQL-backed plugins.
  - Add `delete_message_ids(...)` to `BrokerConnection`.
- `simplebroker/commands.py`
  - CLI command implementations.
  - `cmd_delete()` routes exact CLI delete through `Queue.delete(...)`; it may
    not need code changes after `Queue.delete` is fixed.

SQLite files:

- `simplebroker/_backends/sqlite/maintenance.py`
  - Existing physical full-queue delete and vacuum helpers.
  - Add the SQLite batch delete helper here.
- `simplebroker/_backends/sqlite/plugin.py`
  - Backend plugin adapter.
  - Add `delete_message_ids(...)` and delegate to maintenance.
- `simplebroker/_sql/sqlite.py`
  - SQLite SQL constants.
  - Add temp-table and batch-delete SQL constants or small SQL builder helpers.
- `simplebroker/_sql/__init__.py`
  - Re-export new SQLite SQL constants only if maintenance imports them through
    `simplebroker._sql`.

Postgres files:

- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
  - Add plugin `delete_message_ids(...)`.
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
  - Add the Postgres set-based delete SQL.
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
  - Usually no change. Only update `_should_prepare()` if the new delete query
    is hot enough to force statement preparation.

Redis files:

- `extensions/simplebroker_redis/simplebroker_redis/core.py`
  - Direct backend core. Add `delete_message_ids(...)`.
  - `delete(queue)` already physically deletes pending and claimed entries.
- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
  - Add one Lua script for atomic batch delete by IDs.
- `extensions/simplebroker_redis/simplebroker_redis/keys.py`
  - Usually no change. Reuse `encode_id()` and existing key helpers.

Docs:

- `README.md`
  - Update every claim that `delete <queue> -m <id>` is claim-based.
  - Add `Queue.delete_many(...)` to the Python API examples or API list if the
    README has a nearby section.

Tests:

- `tests/test_message_by_timestamp.py`
  - CLI exact-ID behavior. Some current assertions encode the old soft-delete
    behavior and must change.
- `tests/test_queue_api_comprehensive.py`
  - Existing `Queue.delete` tests.
- `tests/test_queue_api_additions.py`
  - Public queue API additions.
- `tests/test_batch_operations.py` or a new `tests/test_batch_delete.py`
  - Shared backend-agnostic batch behavior through the `broker` fixture.
- `tests/test_batch_delete_sqlite.py`
  - SQLite-only raw table row-count invariants, if a new shared
    `tests/test_batch_delete.py` is created.
- `tests/test_message_claim.py`
  - Keep read/claim tests. Do not weaken the invariant that reads mark claimed.
- `tests/test_performance.py`
  - Slow performance tests live here. Add only marked slow benchmark coverage.
- `extensions/simplebroker_pg/tests/test_pg_maintenance.py`
  - Postgres physical row-count regression tests.
- `extensions/simplebroker_redis/tests/test_redis_integration.py`
  - Redis storage/key cleanup regression tests.

Tooling:

- Use `rg` for search.
- Prefer `uv run pytest ...`.
- Default pytest excludes `slow` tests through `pyproject.toml`.
- Backend service tests need environment variables:
  - Postgres shared tests: `BROKER_TEST_BACKEND=postgres` and
    `SIMPLEBROKER_PG_TEST_DSN`.
  - Redis shared tests: `BROKER_TEST_BACKEND=redis` and
    `SIMPLEBROKER_REDIS_TEST_URL` or `SIMPLEBROKER_VALKEY_TEST_URL`.

## Engineering Rules

- Red-green TDD: write a failing behavioral test before changing each layer.
- DRY: one public batch API should drive exact single-message delete and
  downstream cleanup. Do not leave one path using claim and another using
  physical delete.
- YAGNI: no new config, no new CLI batch syntax, no soft-delete compatibility
  branch, no generalized query builder framework.
- Real storage tests over mocks: prove row/key counts change in real SQLite,
  Postgres, and Redis storage. Mocks are acceptable only for narrow error-path
  unit tests where a real backend cannot trigger the condition.
- Keep backend behavior aligned: SQLite, Postgres, and Redis should share the
  same public semantics even though their implementations differ.
- Keep code style local: small helpers, explicit names, existing retry and
  transaction patterns, Python 3.10-compatible type syntax.
- Do not touch unrelated dirty files. At the time this plan was written,
  `bin/release.py` and `tests/test_release_script.py` were already modified.

## Bite-Sized Tasks

### Task 1: Add Red Tests for Physical Exact Delete

Files to touch:

- `tests/test_queue_api_comprehensive.py`
- `tests/test_message_by_timestamp.py`
- `tests/test_message_claim.py` only if a claim test needs a clarifying
  assertion

Add or update tests before implementation.

Required tests:

1. `Queue.delete(message_id=...)` physically removes an unclaimed message.
   - Write three messages.
   - Capture timestamps through `peek_generator(with_timestamps=True)`.
   - Call `q.delete(message_id=middle_ts)`.
   - Assert return value is `True`.
   - Assert `peek_generator(with_timestamps=False)` returns the other two
     messages in original order.
   - For SQLite-specific coverage, assert `SELECT COUNT(*) FROM messages`
     dropped from `3` to `2` and `claimed` did not increase.

2. `Queue.delete(message_id=...)` physically removes a claimed message.
   - Write two messages.
   - Capture timestamps.
   - Claim one message with `q.read_one(exact_timestamp=target_ts)` or
     `broker.claim_one(..., exact_timestamp=target_ts)`.
   - Assert queue stats show one claimed row before delete.
   - Call `q.delete(message_id=target_ts)`.
   - Assert return value is `True`.
   - Assert queue stats show `claimed == 0` for that row and total count
     decreased.
   - For SQLite-specific coverage, assert total row count decreased. Do not
     assert file size.

3. CLI `broker delete QUEUE -m ID` deletes an already claimed row.
   - Update the current old-behavior assertion in
     `tests/test_message_by_timestamp.py` that says delete should fail on a
     claimed message.
   - New expectation: delete exits `0`, produces no stdout, and physically
     removes the row.

4. CLI exact delete no longer creates a claimed row.
   - Update the old exact-delete vacuum test. It may be renamed to
     `test_delete_by_timestamp_physically_removes_row`.
   - New expectation before manual vacuum: total rows are already lower and
     `claimed` did not increase because exact delete is physical now.
   - Keep the separate read-by-timestamp vacuum test unchanged. Reads still
     claim and need vacuum.

5. Queue mismatch remains safe.
   - Keep or strengthen the existing test where an ID from `queue1` is deleted
     through `queue2`.
   - Assert the delete returns not found and `queue1` still has its message.

Do not over-mock these. Use the real `Queue`, `BrokerDB`, and CLI helpers. For
SQLite row-count assertions, use `sqlite3.connect(db_path)` directly as existing
tests already do.

Expected red failure:

- The claimed-row delete test should fail because current
  `Queue.delete(message_id=...)` calls `claim_one(...)`, which ignores already
  claimed rows.
- The physical row-count assertions should fail because current exact delete
  only marks rows claimed.

### Task 2: Add Red Tests for Batch Delete Semantics

Files to touch:

- Prefer a new `tests/test_batch_delete.py`.
- Optionally add public `Queue.delete_many` coverage to
  `tests/test_queue_api_additions.py`.

Mark backend-agnostic tests with:

```python
pytestmark = [pytest.mark.shared]
```

Use the existing `broker` and `queue_factory` fixtures from `tests/conftest.py`.
These fixtures run against SQLite by default and can run against Postgres or
Redis when backend environment variables are set.

Required shared tests:

1. `BrokerCore.delete_message_ids` deletes a mixed set.
   - Write five messages to `jobs`.
   - Capture timestamps with `broker.peek_many("jobs", limit=10)`.
   - Claim one timestamp so it is hidden but still counted in total.
   - Call `broker.delete_message_ids("jobs", [claimed_ts, pending_ts, missing_ts])`.
   - Assert return value is `2`.
   - Assert the claimed row is gone, the pending row is gone, the missing ID did
     not count, and remaining messages are still readable in FIFO order.
   - Assert `stats.pending + stats.claimed == stats.total`.

2. Duplicate IDs count once.
   - Write two messages.
   - Call `delete_message_ids("jobs", [ts1, ts1, ts1])`.
   - Assert return value is `1`.
   - Assert total count decreased by one.

3. Empty batch is a no-op.
   - Call `delete_message_ids("jobs", [])`.
   - Assert return value is `0`.
   - Assert no queue stats changed.

4. Queue mismatch does not delete globally unique IDs from another queue.
   - Write one message to `a` and one to `b`.
   - Delete the `a` timestamp through queue `b`.
   - Assert return value is `0`.
   - Assert both queues still have their original messages.

5. Public `Queue.delete_many` delegates to the same behavior.
   - Use `queue_factory("jobs")`.
   - Write messages and capture timestamps.
   - Call `q.delete_many([ts1, ts3])`.
   - Assert return value is `2`.
   - Assert only the non-deleted message remains.

SQLite-specific invariant test:

- Put this in a separate `tests/test_batch_delete_sqlite.py` with
  `pytestmark = [pytest.mark.sqlite_only]`, or use an explicit backend skip.
  Do not put a raw SQLite storage test in the same module as shared tests
  unless it is guaranteed not to run during `BROKER_TEST_BACKEND=postgres` or
  `BROKER_TEST_BACKEND=redis` runs.
- Add one test that directly checks the `messages` table row count before and
  after `delete_message_ids`.
- Include a claimed row in the batch.
- Assert row count drops immediately.
- Assert `SELECT COUNT(*) FROM messages WHERE claimed = 1` drops for the
  claimed row that was physically deleted.

Avoid fragile timing thresholds in normal tests. Correctness tests should be
deterministic.

### Task 3: Add Backend-Specific Physical Storage Tests

Files to touch:

- `extensions/simplebroker_pg/tests/test_pg_maintenance.py`
- `extensions/simplebroker_redis/tests/test_redis_integration.py`

Postgres test:

- Use `pg_core` and request `pg_runner` in the same test.
- Write three messages.
- Capture timestamps.
- Claim one message.
- Query `SELECT COUNT(*) FROM messages` through `pg_runner` before delete.
- Call `pg_core.delete_message_ids("jobs", [claimed_ts, pending_ts])`.
- Query `SELECT COUNT(*) FROM messages` again.
- Assert count dropped by `2`.
- Assert claimed count dropped for the claimed row.

Redis test:

- Use `RedisRunner`, `RedisKeys`, and `Queue` or `plugin.create_core(...)`.
- Write three messages.
- Capture timestamps.
- Claim one message.
- Before delete, assert the namespace body hash has three entries:
  `runner.client.hlen(RedisKeys(redis_namespace).bodies) == 3`.
- Call `core.delete_message_ids("jobs", [claimed_ts, pending_ts])`.
- Assert return value is `2`.
- Assert the body hash has one entry left.
- Assert the deleted IDs are absent from `pending`, `claimed`, and `all_ids`.

Do not mock Redis. If Redis is unavailable, these extension tests already skip
through fixtures.

### Task 4: Extend the Backend and Connection Contracts

Files to touch:

- `simplebroker/_backend_plugins.py`

Add to `BackendPlugin`:

```python
def delete_message_ids(
    self,
    runner: SQLRunner,
    *,
    queue: str,
    message_ids: Sequence[int],
) -> int: ...
```

`Sequence` is already imported in this file.

Add to `BrokerConnection`:

```python
def delete_message_ids(self, queue: str, message_ids: Sequence[int]) -> int: ...
```

Use `message_ids`, not `timestamps`, at the public/protocol boundary. The CLI
and Python API call these "message IDs" even though SQL stores them in `ts`.

Do not add capability flags. All supported backends must implement this.
For SQL-backed backends, that means the backend plugin method. For direct
backends such as Redis, that means the concrete core method that satisfies
`BrokerConnection`; the Redis plugin object itself does not need a dead
SQL-style method if `BrokerCore` never calls it.

Expected red failure:

- Type checking and runtime tests should fail until SQLite and Postgres
  implement the plugin method and Redis implements the direct core method.

### Task 5: Implement `BrokerCore.delete_message_ids`

Files to touch:

- `simplebroker/db.py`

Add a method near `delete()`:

```python
def delete_message_ids(self, queue: str, message_ids: Sequence[int]) -> int:
    ...
```

Import `Sequence` from `collections.abc` if `db.py` does not already import it.

Required behavior:

- Call `_check_fork_safety()`.
- Validate `queue` with `_validate_queue_name(queue)`.
- Guard reentrant mutation with `_assert_no_reentrant_mutation_during_batch(...)`.
- Return `0` immediately for empty input.
- Deduplicate IDs once at the core layer. A simple `tuple(dict.fromkeys(message_ids))`
  is enough. Deletion order does not matter, but this preserves caller order for
  diagnostics if needed later.
- Do not filter out claimed rows.
- Use `_run_with_retry`.
- Use one explicit transaction around the backend plugin call:
  - acquire `self._lock`
  - `self._runner.begin_immediate()`
  - call `self._backend_plugin.delete_message_ids(...)`
  - `self._runner.commit()`
  - rollback on any exception
- Return the backend count.

Do not call `_should_vacuum()` from this method. It physically deletes rows, so
it does not create claimed-row debt.

Do not generate timestamps or touch metadata.

### Task 6: Implement SQLite Batch Delete Without `OR`

Files to touch:

- `simplebroker/_backends/sqlite/maintenance.py`
- `simplebroker/_backends/sqlite/plugin.py`
- `simplebroker/_sql/sqlite.py`
- `simplebroker/_sql/__init__.py` if constants are imported through
  `simplebroker._sql`

Use a connection-local temp table:

```sql
CREATE TEMP TABLE IF NOT EXISTS simplebroker_delete_message_ids (
    ts INTEGER PRIMARY KEY
) WITHOUT ROWID
```

Then:

```sql
DELETE FROM simplebroker_delete_message_ids
```

Insert IDs in chunks with bound parameters:

```sql
INSERT OR IGNORE INTO simplebroker_delete_message_ids(ts)
VALUES (?), (?), ...
```

Use a conservative chunk size such as `500` IDs per insert. Do not add a config
setting for this. The goal is to avoid SQLite parameter limits and huge SQL
strings.

Delete by joining staged IDs to `messages`:

```sql
DELETE FROM messages
WHERE id IN (
    SELECT m.id
    FROM messages AS m
    JOIN simplebroker_delete_message_ids AS d
      ON d.ts = m.ts
    WHERE m.queue = ?
)
```

Then get the count with the existing `SELECT_CHANGES` helper on the same
connection.

Finally, clear the temp table again:

```sql
DELETE FROM simplebroker_delete_message_ids
```

Implementation notes:

- The temp table is per SQLite connection, so it will not conflict across
  processes.
- The `ts` primary key deduplicates IDs cheaply.
- The join avoids a huge `OR` predicate.
- The delete predicate includes queue filtering so an ID from another queue is
  not removed.
- Use the existing `id INTEGER PRIMARY KEY` for the final delete.
- Keep SQL bound. Do not format IDs into SQL.
- Keep the helper in `maintenance.py`; do not spread SQLite temp-table logic
  into `db.py`.

Add `SQLiteBackendPlugin.delete_message_ids(...)` in
`simplebroker/_backends/sqlite/plugin.py` and delegate to the maintenance helper.

### Task 7: Implement Postgres Batch Delete Without `OR`

Files to touch:

- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- Possibly `extensions/simplebroker_pg/simplebroker_pg/runner.py`

Add SQL shaped like this:

```sql
WITH ids(ts) AS MATERIALIZED (
    SELECT DISTINCT unnest(?::bigint[])
),
deleted AS (
    DELETE FROM messages AS m
    USING ids
    WHERE m.queue = ?
      AND m.ts = ids.ts
    RETURNING 1
)
SELECT COUNT(*) FROM deleted
```

Parameter order should be:

```python
(list(message_ids), queue)
```

`PostgresRunner` adapts qmark placeholders to psycopg `%s` placeholders, so
`?::bigint[]` becomes `%s::bigint[]`.

Implementation notes:

- Use a Python `list` for the array parameter. Psycopg adapts it to a Postgres
  array.
- `SELECT DISTINCT` keeps duplicate IDs from inflating join work.
- The queue predicate protects against accidental cross-queue deletion.
- Do not use `SKIP LOCKED` in this public delete method. A locked existing row
  should not look missing.
- Do not create a temp table or COPY path for Postgres in this change. That may
  be faster for extremely large batches, but it is more code and not needed
  until measurement proves the array CTE path is insufficient.

Add `PostgresBackendPlugin.delete_message_ids(...)` and return the integer count
from the query.

Consider `_should_prepare()` only after tests pass. If the query becomes a hot
path and the normalized SQL is stable, add a targeted prepare condition. Do not
change prepared-statement behavior preemptively.

### Task 8: Implement Redis Batch Delete Atomically

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`

Add a Lua script, for example `DELETE_MESSAGE_IDS`.

Inputs:

- Keys:
  - queue pending zset
  - queue claimed zset
  - queue reserved zset
  - bodies hash
  - all IDs zset
  - queues set
- Args:
  - queue name
  - encoded message IDs

Script behavior:

1. Check every requested ID against `reserved`.
2. If any requested ID is reserved, return `-1` and delete nothing.
3. For each requested ID:
   - remove it from `pending`
   - remove it from `claimed`
   - if it was removed from either, delete its body from `bodies`
   - remove it from `all_ids`
   - increment the deleted count once
4. If `pending`, `claimed`, and `reserved` are all empty after deletion, remove
   the queue from `queues`.
5. Return the deleted count.

In `RedisBrokerCore.delete_message_ids(...)`:

- Validate the queue.
- Call `_assert_no_reentrant_mutation_during_batch(...)`.
- Return `0` for empty input.
- Deduplicate IDs.
- Recover stale batches before the Lua script. Use direct
  `recover_stale_batches(...)` here rather than the throttled
  `_maybe_recover_stale_batches()` helper, because stale reserved IDs should
  not block explicit delete.
- Encode IDs with `encode_id()`.
- If the Lua script returns `-1`, raise `OperationalError`.
- Return the script count.

Do not publish queue activity for delete. Existing queue-wide delete does not
publish and this change should not invent new watcher semantics.

### Task 9: Wire the Public `Queue` API

Files to touch:

- `simplebroker/sbqueue.py`

Change `Queue.delete(message_id=...)`:

```python
with self.get_connection() as connection:
    if message_id is not None:
        return connection.delete_message_ids(self.name, [message_id]) > 0
    return connection.delete(self.name) > 0
```

Remove the comment saying exact delete uses `claim_one`.

Add:

```python
def delete_many(self, message_ids: Sequence[int]) -> int:
    ...
```

Docstring requirements:

- State that this physically deletes exact message IDs from this queue.
- State that it deletes claimed and unclaimed messages.
- State that it returns the number actually deleted.
- State that missing IDs and IDs from other queues are ignored.

Add `Sequence` import if needed.

Do not change `read`, `read_one`, `read_many`, or generator behavior.

### Task 10: Confirm CLI Wiring

Files to touch:

- `simplebroker/commands.py`

`cmd_delete()` already calls `Queue.delete(message_id=exact_timestamp)` for
exact message delete. After Task 9, that path should become physical without
additional CLI code.

Still inspect and update comments:

- Replace any "delete by timestamp" comment that implies claim semantics.
- Keep exit codes:
  - success: `0`
  - not found: `2`
  - invalid ID: `1`

Do not add batch CLI flags here.

### Task 11: Update README and API Docs

Files to touch:

- `README.md`

At minimum update these current claims:

- Command table currently says `delete <queue> [-m <id>]` claims a specific
  message by ID for later vacuum.
- Note near the command table currently says exact delete uses claim semantics.
- Any safe-processing examples that mention delete by ID should remain valid,
  but the text should say delete removes the row immediately.
- The `QueueStats.claimed` explanation should no longer say "read or deleted
  but not yet vacuumed." It should say claimed rows are messages read/claimed
  but not yet vacuumed.

Add a concise `Queue.delete_many([...])` mention near the `Queue.delete(...)`
example if that section exists.

Do not rewrite broad README sections unrelated to delete semantics.

### Task 12: Add Performance Coverage Without Making CI Flaky

Files to touch:

- `tests/test_performance.py`

Add a slow SQLite benchmark for batch delete.

Suggested shape:

- Mark with `@pytest.mark.slow`.
- Write 10,000 messages with a persistent `Queue`.
- Capture 5,000 timestamps.
- Delete those 5,000 timestamps with `Queue.delete_many(...)` or
  `BrokerDB.delete_message_ids(...)`.
- Assert the row count dropped by 5,000.
- Assert runtime is comfortably below a conservative threshold calibrated like
  existing performance tests.

This test should not run in the default commit path because `pyproject.toml`
excludes `slow`. It exists to catch accidental one-row loops or OR-chain
implementations during manual performance runs.

Do not put a hard large-batch wall-clock test in the default suite.

### Task 13: Run Focused Tests

SQLite default tests:

```bash
uv run pytest -q -n 0 \
  tests/test_queue_api_comprehensive.py::TestQueueDelete \
  tests/test_queue_api_additions.py \
  tests/test_message_by_timestamp.py \
  tests/test_batch_delete.py \
  tests/test_batch_delete_sqlite.py \
  tests/test_message_claim.py
```

If `tests/test_batch_delete.py` or `tests/test_batch_delete_sqlite.py` was not
created and tests were added elsewhere, replace those paths with the actual
files.

Broader SQLite regression:

```bash
uv run pytest -q -n 0 \
  tests/test_batch_operations.py \
  tests/test_vacuum_compact.py \
  tests/test_queue_metadata.py \
  tests/test_cli_queue_metadata.py
```

Default suite:

```bash
uv run pytest
```

Slow SQLite performance gate:

```bash
uv run pytest -q -n 0 -m slow tests/test_performance.py
```

Postgres extension tests, if a DSN is available:

```bash
SIMPLEBROKER_PG_TEST_DSN="$SIMPLEBROKER_PG_TEST_DSN" \
uv run pytest -q -n 0 extensions/simplebroker_pg/tests/test_pg_maintenance.py
```

Shared tests against Postgres, if a DSN is available:

```bash
BROKER_TEST_BACKEND=postgres \
SIMPLEBROKER_PG_TEST_DSN="$SIMPLEBROKER_PG_TEST_DSN" \
uv run pytest -q -n 0 tests/test_batch_delete.py
```

Redis extension tests, if Redis or Valkey is available:

```bash
SIMPLEBROKER_REDIS_TEST_URL="$SIMPLEBROKER_REDIS_TEST_URL" \
uv run pytest -q -n 0 extensions/simplebroker_redis/tests/test_redis_integration.py
```

Shared tests against Redis, if Redis or Valkey is available:

```bash
BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_REDIS_TEST_URL="$SIMPLEBROKER_REDIS_TEST_URL" \
uv run pytest -q -n 0 tests/test_batch_delete.py
```

Static checks:

```bash
uv run ruff check simplebroker extensions tests
uv run mypy simplebroker extensions/simplebroker_pg extensions/simplebroker_redis
```

If `mypy` over extensions is not currently clean, do not hide that. Record the
existing failure and still run `uv run mypy simplebroker`.

## Required Invariants

These are the gates for the implementation. If any fail, the change is not
done.

- `read` and `claim` mark rows claimed; they do not physically delete rows.
- `delete(message_id=...)` physically removes an unclaimed row.
- `delete(message_id=...)` physically removes an already claimed row.
- `delete_many([...])` physically removes all matching claimed and unclaimed
  rows in one backend-level batch operation.
- Duplicate IDs do not inflate the returned delete count.
- Missing IDs do not inflate the returned delete count.
- IDs from another queue are not deleted.
- Queue stats preserve `pending + claimed == total`.
- SQLite tests assert table row counts, not database file size.
- Postgres tests assert table row counts, not `pg_total_relation_size`.
- Redis tests assert key/body membership changes, not only visible queue output.
- Exact delete does not increase claimed count.
- Vacuum still removes rows that were claimed by reads.
- No new public API path uses `claim_one` to implement delete.
- Batch delete code contains no generated `OR` chain and no one-row delete loop.

## Common Pitfalls

- Do not "fix" Weft instead of SimpleBroker. Weft should eventually call the
  new batch API, but the SimpleBroker API must be truthful first.
- Do not use `Queue.delete(message_id=...)` as a wrapper around
  `read_one(exact_timestamp=...)`. That is the bug.
- Do not assert SQLite file size after physical delete. SQLite may keep pages
  allocated until vacuum. Assert row counts.
- Do not write broad mocks around `BrokerCore`. They will miss the storage
  invariant that caused this incident.
- Do not add a batch delete method that loops over `Queue.delete(...)`. The
  whole point is one backend-level set operation.
- Do not change `move(..., require_unclaimed=False)`. Moving claimed messages is
  a separate supported behavior.
- Do not silently skip Redis reserved messages. Raise and delete nothing.
- Do not introduce a new config knob for batch size unless measurement proves
  the fixed chunk size is a problem.

## Fresh-Eyes Review Checklist

Before opening the PR, step away and re-read the diff against this checklist:

1. Does the public API read naturally?
   - `read` claims.
   - `delete` deletes.
   - `delete_many` deletes many exact IDs.

2. Is there one source of truth?
   - Single exact delete should call the same lower-level physical delete path
     as batch delete with a one-item list.
   - No parallel exact-delete implementation should remain.

3. Is the batch delete really set-based?
   - SQLite stages IDs then deletes by join.
   - Postgres deletes through an array/CTE join.
   - Redis deletes through one Lua script.
   - No `OR` chain.
   - No per-ID backend round trip.

4. Are claimed rows handled intentionally?
   - Exact delete of a claimed row succeeds.
   - Read of a claimed row still does not return it.
   - Vacuum behavior for read-claimed rows is unchanged.

5. Are tests proving physical storage?
   - SQLite checks `messages` row count.
   - Postgres checks `messages` row count.
   - Redis checks body/key membership.

6. Did any old README text still say exact delete means claim?
   - Search with `rg -n "delete.*claim|claimed.*deleted|later vacuum" README.md simplebroker docs`.

7. Did the implementation drift into auto-vacuum tuning?
   - If yes, revert that part unless there is a separate explicit plan and
     tests. This change is delete semantics plus batch delete only.

## Rollout Notes

This is a behavioral correction, not a compatibility-preserving change.

Suggested release note:

```text
Exact message delete now physically removes matching messages immediately,
including already claimed messages. Reads still use claimed-row semantics for
fast consumption and are cleaned up by vacuum. A new batch exact-delete API
allows cleanup callers to remove many known message IDs with one backend-level
operation.
```

Downstream follow-up:

- Update Weft pruning to call `Queue.delete_many(...)` or
  `BrokerCore.delete_message_ids(...)` for exact cleanup candidates.
- Add a Weft regression test that asserts total backend rows drop after cleanup,
  not only visible queue rows.
