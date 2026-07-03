# `delete_from_queues` API Plan

Date: 2026-05-20

Status: proposed

Owner: SimpleBroker

## Purpose

Add a Python API for physically deleting all messages from a selected set of
queues, optionally bounded by an exclusive upper timestamp.

The target public method is:

```python
broker.delete_from_queues(
    queue_names: Sequence[str],
    *,
    before_timestamp: int | None = None,
) -> int
```

This is a client/API-only addition. Do not add a CLI command or CLI flag in this
change.

This plan assumes the implementer is a skilled engineer with no SimpleBroker
context. Read the whole plan before coding. Follow the tasks in order. Use
red-green TDD. Keep the implementation boring and direct.

## Locked Decisions

Do not reopen these decisions unless a red test proves the plan cannot work.

### Semantics

`delete_from_queues` means physical delete.

- Delete removes matching backend entries.
- Delete includes claimed and unclaimed messages.
- Delete does not mean "claim", "hide", "ack", or "vacuum later".
- Missing queues and empty queues are not errors.
- The return value is the number of messages physically deleted.

`before_timestamp` is optional.

- `before_timestamp=None` deletes every message in the selected queues.
- `before_timestamp=X` deletes messages with `ts < X`.
- The boundary is strict. A message with `ts == X` must remain.
- Do not add `after_timestamp` in this change.
- Do not add a `claimed_only`, `pending_only`, `dry_run`, or batch-size option.

Input handling:

- `queue_names` is a sequence of queue names.
- A bare string or bytes object is not a valid `queue_names` value. Reject it
  before mutation so `delete_from_queues("alpha")` cannot be interpreted as
  five one-character queue names.
- Empty input returns `0`.
- Duplicate queue names are accepted and count once.
- Invalid queue names raise `ValueError`.
- Validate every queue name before mutating storage.
- Preserve normal queue-name validation. Do not invent a looser validation path.
- `before_timestamp` is either `None` or a non-negative integer less than
  `SQLITE_MAX_INT64`. Validate this once in shared code so SQLite, Postgres,
  and Redis do not diverge on negative or oversized bounds.

### Public API Surface

Expose this as a broker/core method, not as a single-queue `Queue` method.

The public usage should be:

```python
from simplebroker import open_broker, target_for_directory

target = target_for_directory(".")
with open_broker(target) as broker:
    deleted = broker.delete_from_queues(
        ["jobs.high", "jobs.low"],
        before_timestamp=cutoff_ts,
    )
```

Reason: `Queue` represents one queue. A method that takes many queues belongs on
the cross-queue broker connection returned by `open_broker()`.

Implementation files still use `BrokerCore` / `BrokerConnection` names because
that is how this repository models the runtime surface.

Do not add a top-level convenience function in this change. `open_broker()` is
already the public cross-queue API.

Do not add a CLI command. The user request is API/client-only.

### Backend Requirements

SQLite and Postgres must use set-based backend operations.

Do not implement SQL backends as:

- one `delete(queue)` call per queue
- one SQL statement per queue
- a giant `queue = ? OR queue = ? OR ...` predicate
- interpolated SQL containing queue names

Use backend-native set membership:

- SQLite: stage queue names in a connection-local temporary table, then delete
  by joining or subquerying that temp table.
- Postgres: use `DELETE FROM messages WHERE queue = ANY(?::text[])`, wrapped in
  a count-returning CTE.
- Redis: use Redis-native key operations and Lua for atomic state transitions.

### Redis Reserved Batches

Redis has explicit `reserved` sets for active `at_least_once` generator batches.

Do not silently delete messages that are reserved by an active batch.

For `before_timestamp=None`:

- If any selected queue has any reserved message after stale-batch recovery,
  fail with `OperationalError`.
- Delete nothing if the operation fails.

For `before_timestamp=X`:

- If any selected queue has a reserved message with `ts < X` after stale-batch
  recovery, fail with `OperationalError`.
- Reserved messages with `ts >= X` do not match the delete predicate and should
  not block the operation.

This is a safety rule, not a soft-delete exception. It prevents commit/rollback
semantics for an active Redis generator batch from becoming incoherent.

### Scope Boundaries

Do not change:

- `Queue.delete(...)`
- `Queue.delete_many(...)`
- CLI `broker delete`
- read, claim, peek, move, or watcher semantics
- timestamp generation
- queue aliases
- auto-vacuum policy
- package dependency boundaries

Do not add a migration. Existing storage can support this operation.

If implementing this plan starts requiring a materially different API shape or
a backend migration, stop and report the problem instead of drifting.

## Repository Primer

Runtime package:

- `simplebroker/`

Public API files:

- `simplebroker/__init__.py`
  - Exports `open_broker`, `Queue`, watcher types, and helper APIs.
  - Usually no code change is needed unless documentation or `__all__` changes.
- `simplebroker/_timestamp.py`
  - Owns shared timestamp generation and validation helpers.
  - Add one small private/public-internal helper for validating integer
    timestamp bounds if no equivalent helper exists when implementation starts.
- `simplebroker/db.py`
  - Owns `DBConnection`, `open_broker`, `BrokerCore`, and `BrokerDB`.
  - `open_broker(...)` yields a `BrokerConnection`.
  - Add `BrokerCore.delete_from_queues(...)` here.
- `simplebroker/_backend_plugins.py`
  - Owns `BackendPlugin` and `BrokerConnection` protocols.
  - Add `delete_from_queues(...)` to the `BrokerConnection` protocol.
  - Add a SQL-backend plugin hook for set-based delete.
- `simplebroker/sbqueue.py`
  - Owns single-queue `Queue`.
  - Do not add the multi-queue API here.

SQLite files:

- `simplebroker/_sql/sqlite.py`
  - SQLite SQL constants and small SQL builders.
  - Add temp-table and delete SQL for staged queue-name deletes.
- `simplebroker/_sql/__init__.py`
  - Re-exports SQLite SQL constants used by backend helper modules.
  - Update only if the SQLite maintenance helper imports the new SQL through
    `simplebroker._sql`.
- `simplebroker/_backends/sqlite/maintenance.py`
  - Physical delete and vacuum helpers.
  - Add the SQLite `delete_from_queues(...)` helper here.
- `simplebroker/_backends/sqlite/plugin.py`
  - Backend adapter.
  - Add a method delegating to the maintenance helper.

Postgres files:

- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
  - Postgres SQL namespace.
  - Add count-returning delete SQL using `queue = ANY(?::text[])`.
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
  - Backend adapter.
  - Add a method delegating to the new SQL.
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
  - Usually no change.
  - Only update `_should_prepare()` if benchmarking later proves the new delete
    query is hot enough to force preparation.

Redis files:

- `extensions/simplebroker_redis/simplebroker_redis/core.py`
  - Direct Redis implementation of the broker protocol.
  - Add `RedisBrokerCore.delete_from_queues(...)`.
- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
  - Lua scripts for atomic Redis transitions.
  - Add one script for multi-queue delete.
- `extensions/simplebroker_redis/simplebroker_redis/keys.py`
  - Key and ID helpers.
  - Usually no change. Reuse `encode_id`, `min_bound`, and `max_bound`.

Docs:

- `README.md`
  - Add a short `open_broker(...).delete_from_queues(...)` example near the
    cross-queue / advanced API section.
  - Do not document a CLI command.

Tests:

- `tests/test_delete_from_queues.py`
  - New shared behavior tests against the real broker fixture.
- `tests/test_batch_delete_sqlite.py`
  - Add SQLite-only raw storage assertions only if shared tests do not cover an
    SQLite-specific invariant.
- `extensions/simplebroker_pg/tests/test_pg_maintenance.py`
  - Add real Postgres tests for storage and row-count behavior.
- `extensions/simplebroker_redis/tests/test_redis_integration.py`
  - Add Redis key cleanup tests.
- `extensions/simplebroker_redis/tests/test_redis_batches.py`
  - Add active reserved-batch safety tests.

Useful existing plans:

- `docs/plans/2026-05-17-physical-delete-and-batch-delete-plan.md`
  - This established that delete means physical delete.
  - Reuse the batch-delete style and safety model.
- `docs/plans/2026-05-13-targeted-queue-metadata-api-plan.md`
  - Good model for adding broker/core methods and shared backend tests.
- `docs/plans/2026-05-14-simplebroker-redis-second-backend-plan.md`
  - Good model for Redis direct-core behavior.

Useful external docs:

- Python `collections.abc.Sequence`
- SQLite temporary tables
- SQLite `DELETE` and `changes()`
- PostgreSQL `DELETE`, `RETURNING`, and `ANY`
- Redis `EVAL`, `ZRANGEBYLEX`, `ZREM`, `HDEL`, `SREM`

## Code Style

Follow current project style.

- Use `from __future__ import annotations` in new Python files.
- Prefer `collections.abc.Sequence`.
- Use `X | None`, `list[str]`, and `dict[str, Any]`.
- Keep imports ordered: stdlib, third-party, local.
- Keep names boring and exact:
  - public/core method: `delete_from_queues`
  - argument: `queue_names`
  - timestamp filter: `before_timestamp`
- Do not add a new abstraction unless it removes real duplication.
- Keep comments rare and useful. Explain transaction or reserved-batch edges,
  not obvious syntax.
- Use existing retry, validation, and transaction patterns. Do not bypass them.
- Do not over-mock. Behavior tests must use real temp SQLite databases and real
  Redis/Postgres services when those backend tests run.

## API Contract

Add this method to the internal/public broker connection protocol:

```python
def delete_from_queues(
    self,
    queue_names: Sequence[str],
    *,
    before_timestamp: int | None = None,
) -> int: ...
```

Behavior:

- Return an `int` count of physically deleted messages.
- `queue_names` must be a sequence of queue names, not a bare string or bytes.
- Empty `queue_names` returns `0`.
- Duplicate queue names count once.
- Claimed rows are eligible.
- Unclaimed rows are eligible.
- Messages in non-selected queues are not eligible.
- If `before_timestamp` is set, only rows with `ts < before_timestamp` are
  eligible.
- A row with `ts == before_timestamp` is not eligible.
- `before_timestamp` must be `None` or an integer in the same valid storage
  range as native SimpleBroker timestamps: `0 <= before_timestamp < SQLITE_MAX_INT64`.
- Invalid queue names raise `ValueError` before any backend mutation.
- Reentrant mutation during an active `at_least_once` generator batch raises the
  same clear runtime error shape as other mutating APIs.

Add or reuse a shared helper for timestamp-bound validation. A concrete shape:

```python
def validate_timestamp_bound(name: str, value: int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int or None")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    if value >= SQLITE_MAX_INT64:
        raise ValueError(f"{name} exceeds maximum timestamp value")
    return value
```

`simplebroker/_timestamp.py` is the best home if no equivalent helper already
exists. Do not add parsing of strings, dates, or Unix timestamps to this API.
That belongs to CLI timestamp parsing, not this Python method.

Recommended implementation shape in `BrokerCore`:

```python
def delete_from_queues(
    self,
    queue_names: Sequence[str],
    *,
    before_timestamp: int | None = None,
) -> int:
    self._check_fork_safety()
    self._assert_no_reentrant_mutation_during_batch("delete_from_queues")
    if isinstance(queue_names, (str, bytes)):
        raise TypeError("queue_names must be a sequence of queue names, not a string")
    before_timestamp = validate_timestamp_bound(
        "before_timestamp", before_timestamp
    )

    deduped = tuple(dict.fromkeys(queue_names))
    for queue in deduped:
        self._validate_queue_name(queue)
    if not deduped:
        return 0

    def _do_delete_from_queues() -> int:
        transaction_open = False
        with self._lock:
            self._runner.begin_immediate()
            transaction_open = True
            try:
                deleted_count = self._backend_plugin.delete_from_queues(
                    self._runner,
                    queue_names=deduped,
                    before_timestamp=before_timestamp,
                )
                self._runner.commit()
                transaction_open = False
                return deleted_count
            except Exception:
                if transaction_open:
                    self._runner.rollback()
                raise

    return self._run_with_retry(_do_delete_from_queues)
```

Do not copy this blindly if surrounding code has changed. Match the current
`delete_message_ids(...)` transaction style in `simplebroker/db.py`.

## Test Design Rules

Red-green TDD is mandatory for this change.

Write tests before implementation for each behavior slice. A good sequence is:

1. Shared API tests that fail because the method does not exist.
2. Protocol/core method implementation.
3. SQLite backend implementation until shared tests pass.
4. Postgres tests and implementation.
5. Redis tests and implementation.
6. README update after behavior is stable.

Do not over-mock:

- Do not mock `BrokerCore`.
- Do not mock `DBConnection`.
- Do not mock SQL strings as proof of behavior.
- Do not mock Redis pipelines as proof of deletion.

Use real storage:

- SQLite shared tests use the existing `broker` fixture.
- SQLite raw invariants may use `sqlite3` only in SQLite-only tests.
- Postgres extension tests use real `PostgresRunner` fixtures and raw SQL only
  to assert storage state after API calls.
- Redis extension tests use real Redis/Valkey fixtures and raw key reads only
  to assert storage state after API calls.

## Bite-Sized Tasks

### Task 1: Add Shared Red Tests

Create `tests/test_delete_from_queues.py`.

Mark the module shared:

```python
import pytest

pytestmark = [pytest.mark.shared]
```

Use the existing `broker` fixture. It yields a real broker core for the active
backend.

Test 1: method deletes selected queues only.

Setup:

- Write two messages to `alpha`.
- Write one message to `beta`.
- Write one message to `gamma`.
- Claim one message from `alpha` using `broker.claim_one("alpha")`.

Exercise:

```python
deleted = broker.delete_from_queues(["alpha", "beta"])
```

Assert:

- `deleted == 3`.
- `broker.peek_many("alpha", limit=10, with_timestamps=False) == []`.
- `broker.peek_many("beta", limit=10, with_timestamps=False) == []`.
- `broker.peek_many("gamma", limit=10, with_timestamps=False) == ["..."]`.
- `broker.get_queue_stat("alpha").total == 0`.
- `broker.get_queue_stat("beta").total == 0`.
- `broker.get_queue_stat("gamma").total == 1`.

Invariant: claimed rows count and are physically removed.

Test 2: `before_timestamp` uses a strict upper bound.

Setup:

- Write `old-alpha` to `alpha`.
- Write `old-beta` to `beta`.
- Capture timestamps through `peek_generator(..., with_timestamps=True)`.
- Write `boundary-alpha` to `alpha`.
- Capture `boundary_ts` for `boundary-alpha`.
- Write `new-alpha` to `alpha`.
- Write `new-beta` to `beta`.
- Write `old-gamma` to `gamma` before or after as needed to prove non-selected
  queues are untouched.

Exercise:

```python
deleted = broker.delete_from_queues(
    ["alpha", "beta"],
    before_timestamp=boundary_ts,
)
```

Assert:

- `deleted == 2`.
- `old-alpha` and `old-beta` are gone.
- `boundary-alpha` remains because the predicate is strict.
- `new-alpha` and `new-beta` remain.
- `gamma` remains untouched.

Invariant: `ts < before_timestamp`, never `ts <= before_timestamp`.

Test 3: duplicate queue names count once.

Setup:

- Write one message to `alpha`.
- Write one message to `beta`.

Exercise:

```python
deleted = broker.delete_from_queues(["alpha", "alpha", "beta", "alpha"])
```

Assert:

- `deleted == 2`.
- both selected queues are empty.

Invariant: dedupe prevents duplicate input from affecting counts.

Test 4: empty input is a no-op.

Setup:

- Write one message to `alpha`.

Exercise:

```python
deleted = broker.delete_from_queues([])
```

Assert:

- `deleted == 0`.
- `alpha` still contains the message.

Test 5: bare string input is rejected before mutation.

Setup:

- Write one message to `alpha`.

Exercise:

```python
with pytest.raises(TypeError):
    broker.delete_from_queues("alpha")
```

Assert after the exception:

- `alpha` still contains its message.

Invariant: a string is one queue name in human terms but a `Sequence[str]` in
Python's type system. The API must not treat it as an iterable of character
queue names.

Test 6: invalid queue names fail before mutation.

Setup:

- Write one message to `alpha`.
- Write one message to `beta`.

Exercise:

```python
with pytest.raises(ValueError):
    broker.delete_from_queues(["alpha", "bad queue name", "beta"])
```

Assert after the exception:

- `alpha` still contains its message.
- `beta` still contains its message.

Invariant: validation happens before mutation.

Test 7: invalid timestamp bounds fail before mutation.

Setup:

- Write one message to `alpha`.

Exercise:

```python
with pytest.raises(ValueError):
    broker.delete_from_queues(["alpha"], before_timestamp=-1)
```

Assert after the exception:

- `alpha` still contains its message.

Add one type test only if the project already tests runtime type errors for
programmatic timestamp arguments. Do not overbuild a timestamp parser here.

Do not assert private SQL or private keys in this shared test file.

Run the red tests:

```bash
python -m pytest tests/test_delete_from_queues.py
```

Expected result before implementation: failures because `delete_from_queues`
does not exist.

### Task 2: Extend the Broker Protocols

File: `simplebroker/_backend_plugins.py`

Add `delete_from_queues(...)` to `BrokerConnection`.

Add a SQL backend plugin hook to `BackendPlugin`:

```python
def delete_from_queues(
    self,
    runner: SQLRunner,
    *,
    queue_names: Sequence[str],
    before_timestamp: int | None = None,
) -> int: ...
```

Reason: `BrokerCore` owns validation and transactions, but backend plugins own
the set-based storage-specific delete.

Do not add an optional `getattr` fallback loop. That fallback would reintroduce
one-delete-per-queue behavior and hide missing backend implementations.

Note about Redis: Redis is a direct backend and will implement
`delete_from_queues(...)` on `RedisBrokerCore`, not through the SQL plugin hook.

Run a focused import/type smoke test:

```bash
python -m pytest tests/test_ext_imports.py tests/test_backend_plugin_resolution.py
```

### Task 3: Implement `BrokerCore.delete_from_queues`

File: `simplebroker/db.py`

Add the method near existing delete methods:

- `delete(...)`
- `delete_message_ids(...)`
- `broadcast(...)`

Use the same structure as `delete_message_ids(...)`:

- `_check_fork_safety()`
- `_assert_no_reentrant_mutation_during_batch("delete_from_queues")`
- reject bare `str` / `bytes` `queue_names`
- validate `before_timestamp` with the shared timestamp-bound helper
- dedupe queue names with `tuple(dict.fromkeys(queue_names))`
- validate all deduped queue names before mutation
- return `0` for empty deduped input
- wrap backend work in `begin_immediate()` / `commit()` / `rollback()`
- call through `_run_with_retry(...)`

Do not validate queue names after opening the transaction. That creates an
unnecessary transaction for invalid input and weakens the "fail before mutation"
test.

Do not silently cast non-string queue names to strings. Existing queue
validation expects queue names as strings. If current validation would raise for
a bad type, keep that behavior.

Run the shared red tests again. They should now fail at backend hook
implementation, not at missing method.

### Task 4: Implement SQLite SQL Helpers

Files:

- `simplebroker/_sql/sqlite.py`
- `simplebroker/_sql/__init__.py` if needed

Add a temp table for staged queue names:

```sql
CREATE TEMP TABLE IF NOT EXISTS simplebroker_delete_queue_names (
    queue TEXT PRIMARY KEY
) WITHOUT ROWID
```

Add clear SQL:

```sql
DELETE FROM simplebroker_delete_queue_names
```

Add an insert builder similar to `build_insert_delete_message_ids_query(...)`:

```python
def build_insert_delete_queue_names_query(count: int) -> str:
    ...
```

It should build:

```sql
INSERT OR IGNORE INTO simplebroker_delete_queue_names(queue)
VALUES (?), (?), ...
```

Validate `count >= 1`.

Add delete SQL:

```sql
DELETE FROM messages
WHERE queue IN (
    SELECT queue
    FROM simplebroker_delete_queue_names
)
```

Add before-filter delete SQL:

```sql
DELETE FROM messages
WHERE queue IN (
    SELECT queue
    FROM simplebroker_delete_queue_names
)
AND ts < ?
```

Use existing `SELECT_CHANGES` to get the row count on the same connection.

Do not use `IN (?, ?, ...)` directly against caller input. The temp table keeps
the delete statement stable and avoids SQLite parameter-limit surprises.

### Task 5: Implement SQLite Backend Helper

Files:

- `simplebroker/_backends/sqlite/maintenance.py`
- `simplebroker/_backends/sqlite/plugin.py`

In `maintenance.py`, add:

```python
def delete_from_queues(
    runner: SQLRunner,
    *,
    queue_names: Sequence[str],
    before_timestamp: int | None = None,
) -> int:
    ...
```

Implementation notes:

- If `queue_names` is empty, return `0`.
- Create the temp table.
- Clear it before inserting.
- Insert queue names in chunks. Reuse the same chunk size as
  `_DELETE_MESSAGE_IDS_INSERT_CHUNK_SIZE` or introduce
  `_DELETE_QUEUE_NAMES_INSERT_CHUNK_SIZE = 500`.
- Run the delete statement with or without `before_timestamp`.
- Read `changes()` on the same connection.
- Clear the temp table after successful delete.

If an exception happens after staging queue names, the caller rolls back and the
next call clears before inserting. Do not add elaborate recovery code unless a
test proves stale temp-table content can leak into a later operation.

In `plugin.py`:

- import the helper
- add `SQLiteBackendPlugin.delete_from_queues(...)`
- delegate to the helper

Run:

```bash
python -m pytest tests/test_delete_from_queues.py
```

Expected result after Task 5: shared tests pass for default SQLite.

### Task 6: Add SQLite-Specific Storage Regression Tests

File: `tests/test_batch_delete_sqlite.py` or a new
`tests/test_delete_from_queues_sqlite.py`.

Mark SQLite-only if the module is new:

```python
pytestmark = [pytest.mark.sqlite_only]
```

Add one raw-storage test:

- Create a temp SQLite-backed broker.
- Write messages to more than two queues.
- Claim some messages.
- Call `delete_from_queues(...)`.
- Open the SQLite database with `sqlite3`.
- Assert no `messages` rows remain for selected queues.
- Assert rows remain for non-selected queues.
- Assert `COUNT(*)` matches the public return count.

Add one chunking test if the implementation chunks inserts:

- Create at least 501 queue names.
- Write one message to each selected queue.
- Call `delete_from_queues(queue_names)`.
- Assert return count equals the number of unique queue names.

Keep this test modest. It should prove the temp-table insert chunking path, not
become a performance benchmark.

Run:

```bash
python -m pytest tests/test_delete_from_queues.py tests/test_delete_from_queues_sqlite.py
```

Adjust the second filename if you add the tests to an existing module.

### Task 7: Implement Postgres SQL and Plugin Hook

Files:

- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`

Add SQL constants:

```sql
WITH deleted AS (
    DELETE FROM messages
    WHERE queue = ANY(?::text[])
    RETURNING 1
)
SELECT COUNT(*) FROM deleted
```

and:

```sql
WITH deleted AS (
    DELETE FROM messages
    WHERE queue = ANY(?::text[])
      AND ts < ?
    RETURNING 1
)
SELECT COUNT(*) FROM deleted
```

Name them clearly, for example:

- `DELETE_FROM_QUEUES_COUNT`
- `DELETE_FROM_QUEUES_BEFORE_COUNT`

In `PostgresBackendPlugin.delete_from_queues(...)`:

- choose the correct SQL based on `before_timestamp`
- pass `list(queue_names)` for the array parameter
- pass `before_timestamp` only for the before-filter query
- return the integer count

Do not introduce a temp table in Postgres. `ANY(?::text[])` is the chosen shape
for this API.

Do not update `_should_prepare()` unless later benchmarks show this query is a
hot path. Keep this implementation small.

### Task 8: Add Postgres Tests

File: `extensions/simplebroker_pg/tests/test_pg_maintenance.py`

Add real Postgres tests. Use existing `pg_core`, `pg_runner`, and `pg_plugin`
fixtures.

Test 1: full selected-queue delete.

- Write messages to `alpha`, `beta`, and `gamma`.
- Claim one message from `alpha`.
- Call `pg_core.delete_from_queues(["alpha", "beta"])`.
- Assert returned count includes the claimed row.
- Query `SELECT queue, COUNT(*) FROM messages GROUP BY queue`.
- Assert only `gamma` remains.

Test 2: before-filter delete.

- Write old messages.
- Capture a boundary timestamp.
- Write newer messages.
- Call `pg_core.delete_from_queues(["alpha", "beta"], before_timestamp=boundary_ts)`.
- Assert only rows with `ts < boundary_ts` from selected queues were removed.
- Assert a row with `ts == boundary_ts` remains.

Run without a service first:

```bash
PYTHONPATH=extensions/simplebroker_pg:. python -m pytest extensions/simplebroker_pg/tests/test_pg_maintenance.py
```

Expected without `SIMPLEBROKER_PG_TEST_DSN`: tests skip cleanly.

Run with a service when available:

```bash
SIMPLEBROKER_PG_TEST_DSN='postgresql://...' \
PYTHONPATH=extensions/simplebroker_pg:. \
python -m pytest extensions/simplebroker_pg/tests/test_pg_maintenance.py
```

Also run shared tests against Postgres when a service is available:

```bash
BROKER_TEST_BACKEND=postgres \
SIMPLEBROKER_PG_TEST_DSN='postgresql://...' \
PYTHONPATH=extensions/simplebroker_pg:. \
python -m pytest tests/test_delete_from_queues.py
```

### Task 9: Implement Redis Lua Script

File: `extensions/simplebroker_redis/simplebroker_redis/scripts.py`

Add one script for multi-queue delete. Suggested name:

```python
DELETE_FROM_QUEUES = """
...
"""
```

Goal: one script invocation should either delete all matching messages across
the selected queues or delete nothing if a matching reserved message exists.

Suggested shape:

- `KEYS[1]`: bodies hash
- `KEYS[2]`: all_ids sorted set
- `KEYS[3]`: queues set
- for each queue:
  - pending key
  - claimed key
  - reserved key
- `ARGV[1]`: queue count
- `ARGV[2]`: max lex bound, `"+"` for no before filter or
  `"(0000000000000000000"` style from `max_bound(before_timestamp)`
- `ARGV[3...]`: queue names in the same order as their key triplets

Script phases:

1. Validate no matching reserved messages exist in any selected queue.
   - Use `ZRANGEBYLEX reserved - maxb LIMIT 0 1`.
   - If any match, return `-1`.
2. For each queue, collect pending IDs in range.
3. For each queue, collect claimed IDs in range.
4. Remove collected IDs from that queue's pending and claimed sets.
5. Remove collected IDs from global `bodies`.
6. Remove collected IDs from global `all_ids`.
7. If a queue's pending, claimed, and reserved sets are all empty, remove the
   queue name from the queues set.
8. Return the number of deleted IDs.

Do not delete reserved IDs.

Do not publish activity. Delete does not create pending messages.

Potential Redis script concern: a very large delete can collect many IDs in Lua.
That is already true of current Redis queue delete behavior, which materializes
IDs before deleting bodies. Do not introduce chunking in this first change
unless tests or service limits prove it is necessary. If chunking becomes
necessary, stop and report because it weakens the all-or-nothing reserved-batch
safety model.

### Task 10: Implement `RedisBrokerCore.delete_from_queues`

File: `extensions/simplebroker_redis/simplebroker_redis/core.py`

Add a method matching the broker protocol:

```python
def delete_from_queues(
    self,
    queue_names: Sequence[str],
    *,
    before_timestamp: int | None = None,
) -> int:
    ...
```

Implementation requirements:

- `_assert_no_reentrant_mutation_during_batch("delete_from_queues")`
- reject bare `str` / `bytes` `queue_names`
- validate `before_timestamp` with the same timestamp-bound helper used by
  `BrokerCore`
- dedupe queue names with `tuple(dict.fromkeys(queue_names))`
- validate every deduped queue name before mutation
- return `0` for empty input
- call `recover_stale_batches(...)` before checking reserved sets
- build Redis keys for every queue
- use `max_bound(before_timestamp)` for the upper lex bound
- call the new Lua script once
- if script returns `-1`, raise `OperationalError`
- return the deleted count

Do not implement this by calling `delete(queue)` in a loop. That would make
partial deletion possible if a later queue has an active reserved batch.

Do not call `_refuse_reserved(...)` directly for the before-filter case because
it refuses any reserved message in the queue. For `before_timestamp`, only
matching reserved messages should block.

### Task 11: Add Redis Tests

Files:

- `extensions/simplebroker_redis/tests/test_redis_integration.py`
- `extensions/simplebroker_redis/tests/test_redis_batches.py`

In `test_redis_integration.py`, add storage cleanup coverage:

1. Full selected-queue delete:
   - Write messages to `alpha`, `beta`, `gamma`.
   - Claim one message from `alpha`.
   - Call `core.delete_from_queues(["alpha", "beta"])`.
   - Assert return count includes claimed and pending messages.
   - Assert bodies for deleted IDs are gone.
   - Assert deleted IDs are gone from `all_ids`.
   - Assert `gamma` remains.

2. Before-filter delete:
   - Write old messages.
   - Capture a boundary timestamp.
   - Write boundary/new messages.
   - Call `core.delete_from_queues(["alpha", "beta"], before_timestamp=boundary_ts)`.
   - Assert strict boundary behavior.
   - Assert deleted IDs are removed from `bodies`, per-queue sets, and `all_ids`.

In `test_redis_batches.py`, add reserved-batch safety coverage:

1. Full delete refuses active reserved batch:
   - Write messages to `alpha` and `beta`.
   - Start an `at_least_once` claim generator on `beta`.
   - Advance it once so a message is reserved.
   - Call `core.delete_from_queues(["alpha", "beta"])`.
   - Assert `OperationalError`.
   - Assert `alpha` and `beta` messages remain. This proves all-or-nothing.
   - Close the generator.

2. Before-filter only blocks matching reserved IDs:
   - Matching-reserved branch: write one message, record `old_ts`, start an
     `at_least_once` claim generator, advance it once, then call
     `delete_from_queues(["alpha"], before_timestamp=old_ts + 1)`. Assert
     `OperationalError` and no deletion.
   - Non-matching-reserved branch: write `old-delete` and `new-reserved`,
     record both timestamps, start an `at_least_once` claim generator with
     `after_timestamp=old_ts` and `batch_size=1`, advance it once so only
     `new-reserved` is reserved, then call
     `delete_from_queues(["alpha"], before_timestamp=new_ts)`. Assert
     `old-delete` is deleted and `new-reserved` remains reserved.
   - Close generators in `finally` blocks.
   - Keep the setup minimal. This test should prove predicate-sensitive
     reserved behavior, not every generator path.

Run without a Redis client dependency first:

```bash
PYTHONPATH=extensions/simplebroker_redis:. python -m pytest extensions/simplebroker_redis/tests
```

If `redis` is not installed, this will fail at import. That is an environment
gap, not a code result. In a configured dev environment, run:

```bash
SIMPLEBROKER_REDIS_TEST_URL='redis://127.0.0.1:6379/0' \
PYTHONPATH=extensions/simplebroker_redis:. \
python -m pytest extensions/simplebroker_redis/tests
```

Also run shared tests against Redis when a service is available:

```bash
BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_REDIS_TEST_URL='redis://127.0.0.1:6379/0' \
PYTHONPATH=extensions/simplebroker_redis:. \
python -m pytest tests/test_delete_from_queues.py
```

### Task 12: Add README Documentation

File: `README.md`

Find the `open_broker()` or cross-queue operations section. Add a concise
example:

```python
from simplebroker import open_broker, target_for_directory

target = target_for_directory(".")

with open_broker(target) as broker:
    deleted = broker.delete_from_queues(
        ["jobs.high", "jobs.low"],
        before_timestamp=cutoff_ts,
    )
```

Document:

- This physically deletes matching messages.
- Claimed and unclaimed messages are both eligible.
- `before_timestamp` is strict (`ts < before_timestamp`).
- This is Python API only.

Do not add CLI docs.

### Task 13: Run Focused Verification

Run SQLite-focused tests:

```bash
python -m pytest \
  tests/test_delete_from_queues.py \
  tests/test_batch_delete_sqlite.py \
  tests/test_batch_delete.py \
  tests/test_message_by_timestamp.py \
  tests/test_queue_api_additions.py \
  tests/test_generator_methods.py
```

Adjust file names if the repo has changed. The goal is:

- new multi-queue delete tests
- existing exact delete tests
- generator/reentrant mutation tests
- timestamp-specific delete behavior

Run import and plugin smoke tests:

```bash
python -m pytest \
  tests/test_backend_plugin_resolution.py \
  tests/test_ext_imports.py
```

Run Postgres extension tests in a local import path:

```bash
PYTHONPATH=extensions/simplebroker_pg:. \
python -m pytest extensions/simplebroker_pg/tests
```

Without `SIMPLEBROKER_PG_TEST_DSN`, service-dependent tests should skip.

Run Redis extension tests in a local import path:

```bash
PYTHONPATH=extensions/simplebroker_redis:. \
python -m pytest extensions/simplebroker_redis/tests
```

If the `redis` Python package is missing, install the dev dependencies or run in
the project dev environment before treating failures as code failures.

### Task 14: Run Backend Service Gates When Available

Postgres gate:

```bash
BROKER_TEST_BACKEND=postgres \
SIMPLEBROKER_PG_TEST_DSN='postgresql://...' \
PYTHONPATH=extensions/simplebroker_pg:. \
python -m pytest tests/test_delete_from_queues.py extensions/simplebroker_pg/tests
```

Redis gate:

```bash
BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_REDIS_TEST_URL='redis://127.0.0.1:6379/0' \
PYTHONPATH=extensions/simplebroker_redis:. \
python -m pytest tests/test_delete_from_queues.py extensions/simplebroker_redis/tests
```

If those services are unavailable, say that plainly in the final implementation
notes. Do not fake these with mocks.

### Task 15: Run Quality Gates

Run the normal test suite:

```bash
python -m pytest
```

Run lint/type gates if this repo's current workflow expects them:

```bash
python -m ruff check .
python -m mypy simplebroker extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis
```

If `ruff` or `mypy` is not installed in the active environment, report the
environment gap instead of silently skipping.

## Invariants Checklist

The final implementation is not complete unless these are true:

- `open_broker(...).delete_from_queues([...])` exists.
- The method returns a deleted-message count.
- Empty queue list returns `0`.
- Bare string or bytes `queue_names` is rejected before mutation.
- Duplicate queue names count once.
- Invalid `before_timestamp` bounds fail before mutation.
- Invalid queue name fails before deleting any selected queue.
- Claimed messages are physically deleted.
- Unclaimed messages are physically deleted.
- Non-selected queues are untouched.
- `before_timestamp` is strict: `ts < before_timestamp`.
- A row with `ts == before_timestamp` remains.
- SQLite uses a temp table, not per-queue deletes.
- Postgres uses `queue = ANY(?::text[])`, not per-queue deletes.
- Redis does not partially delete selected queues when a matching active
  reserved batch exists.
- Redis removes deleted IDs from per-queue sets, `bodies`, and `all_ids`.
- No CLI surface was added.
- Existing `delete`, `delete_many`, read, peek, move, and watcher tests still
  pass.

## Common Failure Modes

Avoid these mistakes:

- Adding `Queue.delete_from_queues(...)`. A `Queue` is single-queue scoped.
- Naming the method `delete_queues`. That sounds like deleting queue metadata,
  but queues are implicit in message rows.
- Implementing SQL as a Python loop over queue names.
- Forgetting claimed rows. Delete means delete.
- Using `ts <= before_timestamp`.
- Validating queue names after the first delete has already run.
- Updating sqlite but not pg or Redis.
- Adding a CLI command because it is easy. It is out of scope.
- Mocking `BrokerCore` and thinking behavior is tested.
- Letting an installed `simplebroker_pg` package shadow the local extension
  during tests. Use `PYTHONPATH=extensions/simplebroker_pg:.`.
- Letting an installed `simplebroker_redis` package shadow the local extension
  during tests. Use `PYTHONPATH=extensions/simplebroker_redis:.`.

## Review Checklist Before Landing

After implementation, review the diff with these questions:

1. Is the public API exactly `delete_from_queues(queue_names, *,
   before_timestamp=None)`?
2. Did any CLI files change? If yes, is it truly necessary? It probably is not.
3. Does all validation happen before mutation?
4. Are SQL backends set-based?
5. Does Redis have an all-or-nothing reserved-batch guard?
6. Are tests primarily real storage tests, not mocks?
7. Did README document Python API semantics without implying a CLI feature?
8. Did service-gated tests either pass or skip for explicit environment reasons?

If the answer to any of these is no, fix that before landing.

## Stop Conditions

Stop and report instead of forcing the plan if any of these happen:

- A backend cannot implement the method without changing delete semantics.
- Redis cannot provide all-or-nothing behavior for matching reserved batches
  without a much larger redesign.
- The implementation needs a storage migration.
- The API starts drifting toward a CLI feature or a `Queue` method.
- The plan starts requiring changes to read/claim/move/watch behavior.

Those would mean the implementation is moving away from the agreed direction.
