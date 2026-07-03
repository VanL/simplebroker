# Latest Pending Timestamp API Plan

Date: 2026-06-18

Status: proposed

Owner: SimpleBroker

Primary downstream: Taut

## Purpose

Taut needs `taut list` to show the newest pending message timestamp for each
thread queue. Today it can only use public SimpleBroker APIs, so it scans each
queue from oldest to newest with `peek_generator(with_timestamps=True)`. That
is correct, but it is O(queue history). Taut must not read SimpleBroker private
tables directly.

Add a small public API that answers this one indexed question:

```python
Queue("thread").latest_pending_timestamp() -> int | None
```

The method returns the largest timestamp among pending, unclaimed messages in
that queue, or `None` when there are no pending messages. It does not claim,
consume, move, reserve, delete, or otherwise mutate a message.

## Locked Decisions

Do not reopen these decisions unless a red test proves the plan cannot work.

### API Shape

Use this public queue method:

```python
class Queue:
    def latest_pending_timestamp(self) -> int | None: ...
```

Also add the backend/core method that `Queue` delegates to:

```python
class BrokerConnection(Protocol):
    def latest_pending_timestamp(self, queue: str) -> int | None: ...
```

```python
class BrokerCore:
    def latest_pending_timestamp(self, queue: str) -> int | None: ...
```

```python
class RedisBrokerCore:
    def latest_pending_timestamp(self, queue: str) -> int | None: ...
```

Do not name the public method `latest_timestamp()`. This repository already has
`Queue.last_ts` and `refresh_last_ts()`, which mean the broker-global generated
timestamp high-water mark, not a row in one queue. A vague method name invites a
wrong implementation that reads `meta.last_ts`.

Do not add `peek_latest(...)`. Taut needs only the timestamp. Returning the body
would widen the API, add a heavier query path, and create new ordering semantics
that are not needed for the first version.

### Semantics

`latest_pending_timestamp()` means:

```text
max(message.ts)
where message.queue == this queue
and message is pending/unclaimed/claimable
```

It must return:

- `None` for an empty queue.
- `None` for a queue with only claimed rows.
- The largest timestamp for pending rows when pending and claimed rows coexist.
- The largest pending timestamp in this queue only. Other queues and
  broker-global `last_ts` must not affect the result.

It must not:

- Claim, consume, reserve, move, delete, vacuum, publish activity, or update
  queue metadata.
- Return generated timestamps that do not correspond to a pending row.
- Return claimed-but-not-vacuumed rows.
- Expose SQL, table names, or backend-private storage details to callers.

### Backend Coverage

Implement the API for all currently supported backends:

- SQLite via `BrokerCore` and the built-in SQLite SQL namespace.
- Postgres via the shared `BrokerCore` path and the Postgres SQL namespace.
- Redis via `RedisBrokerCore`, using the pending ZSET and skipping reserved IDs.

The shared public API tests must run across SQLite, Postgres, and Redis. This is
not a SQLite-only feature.

### SQLite Schema

Add a SQLite partial index for the query:

```sql
CREATE INDEX IF NOT EXISTS idx_messages_pending_queue_ts
ON messages(queue, ts)
WHERE claimed = 0
```

SQLite can scan an ascending index backward for `ORDER BY ts DESC LIMIT 1`.
Use `DESC` only if `EXPLAIN QUERY PLAN` shows a clear advantage. Do not add
both ascending and descending variants.

Bump built-in SQLite `SCHEMA_VERSION` from `4` to `5` and add a migration that
creates this index for existing databases. New databases must create it during
bootstrap.

Postgres already has a partial pending timestamp index:

```sql
CREATE INDEX IF NOT EXISTS idx_messages_queue_ts_order_unclaimed
ON messages (queue, ts, order_id)
WHERE claimed = FALSE
```

Do not bump the Postgres schema version unless an implementation test proves
the existing index is insufficient. It should support a backward scan for
`ORDER BY ts DESC LIMIT 1`.

### Redis Pending Means Not Reserved

Redis has `pending`, `claimed`, and `reserved` per-queue structures. Reserved
IDs are pending IDs temporarily held by an active at-least-once batch. Existing
`RedisBrokerCore.has_pending_messages()` treats pending minus reserved as
claimable pending work. The new method must follow that rule.

Redis implementation shape:

```text
ZREVRANGEBYLEX queue:pending + - LIMIT offset batch_size
for each encoded ID from newest to oldest:
  if not present in queue:reserved:
      return decode_id(encoded)
return None when no unreserved pending ID remains
```

Use small batches, for example 64 IDs, to avoid scanning the entire pending set
when only a few newest rows are reserved. Do not call `recover_stale_batches()`
from this read API unless you also intentionally change `has_pending_messages()`
in the same PR. Keep the new method aligned with existing read behavior.

### Non-Goals

- No bulk API in this change. `taut list` may eventually need
  `latest_pending_timestamps(queue_names)` or a richer queue metadata result,
  but the requested API is one queue. Add the narrow method first.
- No CLI command or flag. This is a Python API for Taut. Add CLI only if a real
  public use case appears.
- No `QueueStats` field. It is a frozen public dataclass and many tests compare
  equality directly.
- No `latest_timestamp()` alias. Avoid cementing an ambiguous name.
- No body-returning `peek_latest()`.
- No change to `peek_many`, `has_pending`, watcher behavior, claim semantics,
  vacuum semantics, or timestamp generation.
- No direct SQL access from Taut or embedding applications.

## What Already Exists

Use existing code. Do not rebuild it.

| Existing piece | File | Reuse |
|---|---|---|
| Public `Queue` delegation through `get_connection()` | `simplebroker/sbqueue.py` | Add a thin method matching `has_pending()` and `stats()`. |
| Backend/core protocol | `simplebroker/_backend_plugins.py` | Add one method to `BrokerConnection`. |
| SQL-backed core with validation and retry discipline | `simplebroker/db.py` | Add `BrokerCore.latest_pending_timestamp(queue)`. |
| SQL namespace contract | `simplebroker/_sql/_contract.py` | Add one required SQL constant. |
| SQLite SQL constants and schema migrations | `simplebroker/_sql/sqlite.py`, `simplebroker/_backends/sqlite/schema.py` | Add query constant, index constant, and schema v5 migration. |
| Postgres SQL namespace and pending timestamp index | `extensions/simplebroker_pg/simplebroker_pg/_sql.py`, `extensions/simplebroker_pg/simplebroker_pg/schema.py` | Add only the query constant unless tests prove schema work is needed. |
| Redis lexicographic timestamp IDs | `extensions/simplebroker_redis/simplebroker_redis/keys.py` | Use `encode_id`, `decode_id`, and lexicographic bounds. |
| Redis pending/reserved semantics | `extensions/simplebroker_redis/simplebroker_redis/core.py` | Mirror `has_pending_messages()` behavior. |
| Backend-agnostic `queue_factory` fixture | `tests/conftest.py` | Mark the new Python API tests as `shared` so they run on all backends. |

## Required Reading Before Coding

Read these files before making changes:

1. `simplebroker/sbqueue.py`
   - Read `last_ts`, `refresh_last_ts`, `has_pending`, and `stats`.
   - The new method should look like `has_pending`: validate through the core,
     not by touching storage.
2. `simplebroker/db.py`
   - Read `BrokerCore.has_pending_messages`, `get_queue_stat`, `_run_with_retry`,
     and `_validate_queue_name`.
   - Follow lock and retry patterns. Do not invent a separate runner path.
3. `simplebroker/_backend_plugins.py`
   - Read `BrokerConnection`. Protocol changes must land with implementers or
     mypy will fail.
4. `simplebroker/_sql/_contract.py`
   - Add the SQL constant to the required backend SQL namespace.
5. `simplebroker/_sql/sqlite.py`
   - Read existing indexes, `CHECK_PENDING_MESSAGES`, and queue stats SQL.
6. `simplebroker/_backends/sqlite/schema.py`
   - Read `initialize_database`, `migrate_schema`, and `ensure_schema_v4`.
   - Add schema v5 in the same style.
7. `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
   - Add the Postgres query constant with `claimed = FALSE`.
8. `extensions/simplebroker_pg/simplebroker_pg/schema.py`
   - Confirm the existing partial `(queue, ts, order_id) WHERE claimed = FALSE`
     index is still present.
9. `extensions/simplebroker_redis/simplebroker_redis/core.py`
   - Read `_zrange_pending`, `_peek_rows`, `has_pending_messages`,
     `_claim_batch_generator`, and reserved batch handling.
10. `tests/conftest.py`
    - Understand `broker_target`, `broker`, `queue_factory`, and marker rules.
11. `tests/test_sqlite_schema.py`
    - Extend schema migration/index tests.
12. `README.md` and `CHANGELOG.md`
    - Document the public API and the schema/index change.

Tooling reference:

- Use `uv run ...` for Python commands.
- Use `rg` for search.
- Use `apply_patch` or a normal editor for code changes. Do not generate code
  with one-off scripts unless the change is purely mechanical.
- Run `uv run ruff format <changed files>` before final gates.

## Architecture

The call path stays narrow:

```text
Taut
  |
  v
Queue.latest_pending_timestamp()
  |
  v
BrokerConnection.latest_pending_timestamp(queue)
  |
  +--> BrokerCore.latest_pending_timestamp(queue)
  |       |
  |       +--> SQL namespace GET_LATEST_PENDING_TIMESTAMP
  |              SQLite: messages(queue, ts) partial pending index
  |              Postgres: existing queue/ts pending index
  |
  +--> RedisBrokerCore.latest_pending_timestamp(queue)
          |
          +--> pending ZSET newest-to-oldest, skipping reserved IDs
```

State filter:

```text
SQLite/Postgres rows:

  pending row          claimed row
  claimed = 0/false    claimed = 1/true
       |                    |
       v                    v
  eligible             not eligible

Redis IDs:

  in pending ZSET and not in reserved ZSET   -> eligible
  in pending ZSET and in reserved ZSET       -> not eligible during active batch
  in claimed ZSET                            -> not eligible
```

## Implementation Tasks

Follow these tasks in order. Each task is intentionally small. Use red-green
TDD: write the test, watch it fail for the expected reason, implement the
smallest correct code, then rerun the focused test.

### Task 1: Baseline and Branch Hygiene

- [ ] Run `git status --short`.
- [ ] Note unrelated dirty files. Do not revert them.
- [ ] Run a focused baseline if the tree is already suspicious:
  - `uv run pytest tests/test_queue_metadata.py tests/test_sqlite_schema.py -q`
- [ ] Confirm `pyproject.toml` and `simplebroker/_constants.py` both currently
  report version `4.7.1`. Do not copy version bump instructions from older
  plans.

Expected result: no code changes yet, just confidence in the starting point.

### Task 2: Add Shared Public API Tests First

Create `tests/test_latest_pending_timestamp.py`.

Add at top:

```python
from __future__ import annotations

import pytest

pytestmark = [pytest.mark.shared]
```

Use `queue_factory` for `Queue` tests and `broker` for direct core tests. Do
not mock `BrokerCore`, SQL runners, Redis clients, or timestamp generation.
These tests should hit real backends.

Required tests:

1. Empty queue:
   - `queue_factory("jobs").latest_pending_timestamp() is None`
2. Generated timestamp with no row:
   - Call `q.generate_timestamp()`.
   - Assert `q.last_ts` or `q.refresh_last_ts()` sees a timestamp.
   - Assert `q.latest_pending_timestamp() is None`.
   - This prevents accidental implementation via `meta.last_ts`.
3. Several pending messages:
   - Write three messages.
   - Collect timestamps with `q.peek_many(10, with_timestamps=True)`.
   - Assert latest equals `max(ts for _, ts in rows)`.
4. Claimed latest is ignored:
   - Write three messages.
   - Claim the newest by exact timestamp:
     `q.read_one(exact_timestamp=newest_ts, with_timestamps=True)`.
   - Assert latest now equals the next-largest pending timestamp.
   - Claim remaining rows.
   - Assert latest is `None`.
5. Claimed-only queue:
   - Write one message, read it, assert `q.exists()` is still true if the row
     has not been vacuumed, and assert latest is `None`.
6. Queue isolation:
   - Write an older message to `jobs`.
   - Write a newer message to `other`.
   - Assert `jobs.latest_pending_timestamp()` returns the `jobs` timestamp.
7. Exact-ID insert out of write order:
   - Generate three timestamps with `generate_timestamp()`.
   - Insert them into a fresh queue using `insert_messages()` in non-sorted
     order.
   - Assert latest is the largest inserted ID, not insertion order.
8. Non-mutating behavior:
   - Capture `q.peek_many(10, with_timestamps=True)` and `q.stats()`.
   - Call `q.latest_pending_timestamp()` twice.
   - Assert the peek rows and stats are unchanged.
9. Core path:
   - Use the `broker` fixture.
   - Write to `broker.write("jobs", "m")`.
   - Assert `broker.latest_pending_timestamp("jobs")` returns the row timestamp
     observed through `broker.peek_many("jobs", with_timestamps=True)`.

Run the red test:

```bash
uv run pytest tests/test_latest_pending_timestamp.py -q
```

Expected red failure: `AttributeError` on `Queue.latest_pending_timestamp` or
`BrokerConnection.latest_pending_timestamp`. If it fails because of fixtures or
test logic, fix the test before writing production code.

### Task 3: Add the Public Queue/Core/Protocol Method

Touch these files together:

- `simplebroker/_backend_plugins.py`
- `simplebroker/sbqueue.py`
- `simplebroker/db.py`

Protocol:

```python
def latest_pending_timestamp(self, queue: str) -> int | None: ...
```

Queue method:

```python
def latest_pending_timestamp(self) -> int | None:
    """Return the newest pending message timestamp for this queue, if any."""
    with self.get_connection() as connection:
        return connection.latest_pending_timestamp(self.name)
```

Core method behavior:

- Call `self._check_fork_safety()`.
- Validate the queue name with `self._validate_queue_name(queue)`.
- Use `self._run_with_retry(...)` like `has_pending_messages()`.
- Use `self._lock`.
- Execute one backend SQL constant, fetch rows, return `int(rows[0][0])` or
  `None`.
- Do not start a write transaction.
- Do not call `prepare_queue_operation()`; peeks and `has_pending_messages()`
  do not require it.

Implementation sketch:

```python
def latest_pending_timestamp(self, queue: str) -> int | None:
    self._check_fork_safety()
    self._validate_queue_name(queue)

    def _do_latest() -> int | None:
        with self._lock:
            rows = list(
                self._runner.run(
                    self._sql.GET_LATEST_PENDING_TIMESTAMP,
                    (queue,),
                    fetch=True,
                )
            )
            if not rows or rows[0][0] is None:
                return None
            return int(rows[0][0])

    return self._run_with_retry(_do_latest)
```

Keep it boring and similar to existing code. Do not add a helper abstraction
unless two real call sites need it.

Run:

```bash
uv run pytest tests/test_latest_pending_timestamp.py -q
```

Expected failure after this task: missing SQL constant or missing Redis method.

### Task 4: Add SQL Namespace Constants

Touch:

- `simplebroker/_sql/_contract.py`
- `simplebroker/_sql/sqlite.py`
- `simplebroker/_sql/__init__.py` if constants are re-exported there
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`

Add `GET_LATEST_PENDING_TIMESTAMP` to `BackendSQLNamespace` and
`_REQUIRED_SQL_ATTRIBUTES`.

SQLite SQL:

```sql
SELECT ts
FROM messages
WHERE queue = ? AND claimed = 0
ORDER BY ts DESC
LIMIT 1
```

Postgres SQL:

```sql
SELECT ts
FROM messages
WHERE queue = ? AND claimed = FALSE
ORDER BY ts DESC
LIMIT 1
```

Return no row when no pending message exists. Do not use `MAX(ts)` here unless
tests show a material reason. `ORDER BY ts DESC LIMIT 1` aligns with the new
index and avoids a full aggregate path on weaker planners.

Run:

```bash
uv run pytest tests/test_latest_pending_timestamp.py -q
```

Expected remaining failure: Redis method missing.

### Task 5: Add the SQLite Pending Timestamp Index and Migration

Touch:

- `simplebroker/_constants.py`
- `simplebroker/_sql/sqlite.py`
- `simplebroker/_sql/__init__.py`
- `simplebroker/_backends/sqlite/schema.py`
- `tests/test_sqlite_schema.py`

Change:

```python
SCHEMA_VERSION: Final[int] = 5
```

Add SQL constant:

```python
CREATE_PENDING_QUEUE_TS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_messages_pending_queue_ts
ON messages(queue, ts)
WHERE claimed = 0
"""
```

Schema work:

- Import the new index constant.
- In `initialize_database()`, run the index creation for new databases after
  `claimed` exists.
- Add `pending_queue_ts_index_exists(runner: SQLRunner) -> bool` test helper,
  or local test query, matching existing `ts_unique_index_exists()`.
- Add `ensure_schema_v5(...)`.
- Update `migrate_schema()` to call v2, v3, v4, then v5.
- When migrating a v4 database, create the index and write schema version `5`.
- When current version is already `5`, ensure the index exists idempotently.

Tests to update or add in `tests/test_sqlite_schema.py`:

1. Bootstrap creates the new index and writes `SCHEMA_VERSION`.
2. Migration from v1 now records versions `[2, 3, 4, 5]`.
3. Migration from v4 creates `idx_messages_pending_queue_ts` and records `[5]`.
4. Calling `ensure_schema_v5(... current_version=5 ...)` is idempotent and does
   not append another version.

After the schema bump, run `rg "schema-v4" tests simplebroker`. Do not
mechanically rewrite generic `PhaseLockService` unit tests in
`tests/test_phaselock.py`; those use arbitrary sample phase names. Only update
tests that are explicitly asserting the current SimpleBroker schema phase and
are not already deriving it from `SCHEMA_VERSION`.

Add a query plan test in `tests/test_sqlite_schema.py`. Do not put this in
`tests/test_latest_pending_timestamp.py`; that module should stay purely
`shared` so `bin/pytest-pg` and `bin/pytest-redis` select it cleanly.

```sql
EXPLAIN QUERY PLAN
SELECT ts
FROM messages
WHERE queue = ? AND claimed = 0
ORDER BY ts DESC
LIMIT 1
```

Assert `idx_messages_pending_queue_ts` appears in the plan text. Do not assert
the exact planner wording beyond the index name; SQLite wording varies.

Run:

```bash
uv run pytest tests/test_sqlite_schema.py tests/test_latest_pending_timestamp.py -q
```

### Task 6: Add Redis Implementation

Touch:

- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/tests/test_redis_latest_pending_timestamp.py`

Set the Redis extension test module marker:

```python
pytestmark = [pytest.mark.redis_only]
```

Add `RedisBrokerCore.latest_pending_timestamp(queue: str) -> int | None`.

Rules:

- Validate the queue name.
- Check pending IDs newest to oldest.
- Skip IDs present in the queue's `reserved` ZSET.
- Return decoded int for the first unreserved pending ID.
- Remember that Redis stores encoded timestamps as ZSET members; decode the
  encoded member before returning the public timestamp.
- Return `None` if no such ID exists.
- Do not include claimed IDs.
- Do not mutate Redis state.

Implementation sketch:

```python
def latest_pending_timestamp(self, queue: str) -> int | None:
    self._validate_queue_name(queue)
    pending = self._qkey(queue, "pending")
    reserved = self._qkey(queue, "reserved")
    offset = 0
    batch_size = 64

    while True:
        ids = [
            str(encoded)
            for encoded in response_list(
                self._client.execute_command(
                    "ZREVRANGEBYLEX",
                    pending,
                    "+",
                    "-",
                    "LIMIT",
                    offset,
                    batch_size,
                )
            )
        ]
        if not ids:
            return None
        for encoded in ids:
            if self._client.zscore(reserved, encoded) is None:
                return decode_id(encoded)
        offset += len(ids)
```

This mirrors nearby Redis code that converts Redis response members with
`str(...)` before passing them to `decode_id(...)`. The Redis runner is already
configured with `decode_responses=True`; do not add a separate byte-decoding
path unless tests prove it is needed.

Prefer a small helper only if it also simplifies existing Redis code without
making the diff larger. YAGNI applies: one readable method is fine.

Redis-specific tests:

1. Empty, several pending, claimed-only, and claimed-latest cases, using real
   Redis fixtures. Prefer the existing `redis_runner`, `redis_url`, and
   `redis_namespace` fixtures from `extensions/simplebroker_redis/tests/conftest.py`;
   they already skip cleanly when no Redis/Valkey URL is configured.
2. Reserved latest is skipped:
   - Write an old message and a newer message.
   - Capture `old_ts`.
   - Start an at-least-once claim generator with
     `after_timestamp=old_ts`, `batch_size=1`.
   - Advance it once so the newer message is reserved but not committed.
   - Assert `latest_pending_timestamp("jobs") == old_ts`.
   - Finish or close the generator so cleanup can run.
3. All pending IDs reserved returns `None`.

Do not mock Redis. If a test needs internal state, drive it through public
claim generator behavior or a real Redis runner fixture.

Run:

```bash
uv run pytest extensions/simplebroker_redis/tests/test_redis_latest_pending_timestamp.py -q
uv run pytest tests/test_latest_pending_timestamp.py -q
```

When Redis is unavailable locally, use:

```bash
uv run bin/pytest-redis
```

The bin script is the preferred full Redis gate because it manages the test
service environment used by this repo.

### Task 7: Add a Focused Postgres Test

Shared tests should already cover Postgres through `bin/pytest-pg`. Add one
extension test only for Postgres-specific confidence:

- `extensions/simplebroker_pg/tests/test_pg_latest_pending_timestamp.py`

Set the Postgres extension test module marker:

```python
pytestmark = [pytest.mark.pg_only]
```

Test:

- Use the existing `pg_core` fixture from
  `extensions/simplebroker_pg/tests/conftest.py`; it already gives each test a
  fresh schema and skips cleanly when no Postgres DSN is configured.
- Write three rows through `pg_core`.
- Claim the newest by exact timestamp.
- Assert `pg_core.latest_pending_timestamp("jobs")` returns the next newest.
- Claim all pending rows.
- Assert `None`.

Optional but useful: run `EXPLAIN` for the Postgres query and assert the plan can
use `idx_messages_queue_ts_order_unclaimed`. Do not make this a hard CI gate if
Postgres planner output is unstable under tiny test tables. Prefer behavioral
coverage over brittle plan assertions.

Run:

```bash
uv run pytest extensions/simplebroker_pg/tests/test_pg_latest_pending_timestamp.py -q
uv run bin/pytest-pg
```

### Task 8: Documentation

Touch:

- `README.md`
- `CHANGELOG.md`
- `extensions/simplebroker_redis/README.md` if Redis backend docs list API
  coverage
- `extensions/simplebroker_pg/README.md` only if it has a public API coverage
  section that would now be stale

README placement:

- Add a short subsection near "Queue metadata" or "Tracking the last generated
  timestamp".
- Explicitly distinguish `latest_pending_timestamp()` from `last_ts`.

Suggested wording:

````markdown
### Latest pending timestamp for one queue

Use `Queue.latest_pending_timestamp()` when you need the newest pending message
timestamp in one queue without scanning the queue:

```python
queue = Queue("tasks")
latest = queue.latest_pending_timestamp()
if latest is None:
    print("no pending messages")
```

This returns the largest timestamp for pending, unclaimed messages in that queue.
It does not consume or mutate messages. It is different from `queue.last_ts`,
which is the broker-global generated timestamp high-water mark and may refer to
another queue or to a generated timestamp with no message row.
````

CHANGELOG:

- Add an "Unreleased" entry if the file has one.
- Otherwise add an entry under the next release heading used by the project.
- Mention the SQLite schema v5 index and backend coverage.

Do not update package versions unless the release workflow for this repo
requires it. The implementation should work on the current 4.7.1 source tree.

### Task 9: Full Verification Gates

Run focused gates first:

```bash
uv run pytest tests/test_latest_pending_timestamp.py tests/test_sqlite_schema.py -q
uv run pytest extensions/simplebroker_redis/tests/test_redis_latest_pending_timestamp.py -q
uv run pytest extensions/simplebroker_pg/tests/test_pg_latest_pending_timestamp.py -q
```

Then run quality gates:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy simplebroker \
  extensions/simplebroker_redis/simplebroker_redis \
  extensions/simplebroker_pg/simplebroker_pg
```

Then backend gates:

```bash
uv run pytest
uv run bin/pytest-pg
uv run bin/pytest-redis
```

If Postgres or Redis service gates cannot run in the current environment, state
that clearly in the handoff. Do not pretend shared backend coverage ran.

### Task 10: Handoff Checklist

Before handing off or opening a PR, verify these invariants explicitly:

- [ ] `Queue.latest_pending_timestamp()` exists and is documented.
- [ ] `BrokerCore.latest_pending_timestamp(queue)` exists for users of
  `open_broker()`.
- [ ] `RedisBrokerCore.latest_pending_timestamp(queue)` exists.
- [ ] SQLite and Postgres use backend SQL constants, not inline SQL from
  `Queue`.
- [ ] SQLite schema version is 5 and the new partial index is created for both
  new and migrated databases.
- [ ] A stale `schema-v4` setup marker does not skip the new `schema-v5` setup
  phase.
- [ ] Redis skips reserved IDs.
- [ ] Empty queue returns `None`.
- [ ] Claimed-only queue returns `None`.
- [ ] Newest claimed row is ignored and the next newest pending row is returned.
- [ ] A generated timestamp with no row does not leak through as latest pending.
- [ ] Calling the method does not alter `peek_many`, `stats`, `has_pending`, or
  watcher behavior.
- [ ] Shared API tests pass across SQLite, Postgres, and Redis.

## Test Design Guidance

The engineer implementing this should resist over-mocking. This feature is about
backend semantics, so mocks are the wrong test tool.

Good tests:

- Use `queue_factory`, `broker`, `pg_core`, or real Redis fixtures.
- Assert observable public behavior.
- Create claimed rows by actually reading messages.
- Create reserved Redis IDs by actually starting an at-least-once generator.
- Use direct SQLite only for schema/index plan assertions.

Bad tests:

- Mocking `SQLRunner.run` and asserting a string was passed.
- Mocking Redis ZSET calls.
- Testing private `_retrieve` behavior.
- Reading `messages` from Taut or any embedding project.
- Asserting exact SQLite or Postgres planner prose beyond a stable index name.

Behavior matrix:

```text
Case                              Expected latest
--------------------------------  ----------------
missing queue                     None
empty queue                       None
only generated last_ts            None
pending [10, 20, 30]              30
pending [10, 20], claimed [30]    20
claimed [10, 20, 30]              None
jobs [10], other [40]             10 for jobs
Redis pending [10, 20R, 30R]      10, where R means reserved
```

Coverage map:

```text
CODE PATHS                                      TESTS

Queue.latest_pending_timestamp()
  |
  +-- delegates through get_connection()
      +-- [shared] empty, pending, claimed-only, non-mutating
      +-- [shared] generated last_ts is ignored

BrokerCore.latest_pending_timestamp(queue)
  |
  +-- validates queue name
  +-- executes GET_LATEST_PENDING_TIMESTAMP
  +-- [shared] core fixture direct call
  +-- [sqlite] query plan uses idx_messages_pending_queue_ts
  +-- [pg] behavior through pg_core

SQLite schema v5
  |
  +-- bootstrap creates index
  +-- migrate v1 -> v5 records [2, 3, 4, 5]
  +-- migrate v4 -> v5 creates index idempotently

RedisBrokerCore.latest_pending_timestamp(queue)
  |
  +-- newest-to-oldest pending ZSET read
  +-- skips reserved IDs
  +-- ignores claimed ZSET
  +-- [redis] reserved newest and all-reserved cases
```

## Failure Modes

| Failure mode | How the plan covers it |
|---|---|
| Engineer reads `Queue.last_ts` and ships broker-global behavior | Locked method name, generated-timestamp-with-no-row test, docs distinction. |
| SQLite query scans large claimed history | New partial `(queue, ts) WHERE claimed = 0` index plus query plan test. |
| Postgres implementation bypasses backend SQL namespace | SQL constant added to Postgres namespace and protocol/contract validation catches omissions. |
| Redis returns a reserved ID from an active at-least-once batch | Redis-specific reserved tests and implementation loop skip `reserved` ZSET members. |
| Feature works on SQLite only | New core API tests are explicitly marked `shared`; PG and Redis bin gates required. |
| Method mutates message state by accidentally using claim/read path | Non-mutating test compares `peek_many` and `stats` before and after. |
| The API name is confused with `last_ts` | No `latest_timestamp` alias and README calls out the distinction. |
| Schema migration leaves old DBs without the index | `ensure_schema_v5` migration tests for v1 and v4 databases. |

## Parallelization

Sequential implementation is recommended. The protocol, shared API tests, SQL
constants, SQLite migration, and Redis method all touch shared contracts. Parallel
worktrees would create avoidable merge conflicts for a small feature.

The only safe split is documentation after the API name and semantics are
settled, but documentation is small enough to keep in the same lane.

## Engineering Principles

- DRY: one public method name threaded through `Queue`, `BrokerConnection`, and
  each backend core. Do not create separate SQLite/Postgres code paths in
  `Queue`.
- YAGNI: no bulk API, no CLI, no body-returning peek API, no QueueStats change.
- Red-green TDD: write shared behavior tests before code, then backend-specific
  tests for schema and Redis reserved behavior.
- Explicit over clever: use a clear SQL constant and a clear Redis loop.
- Boring by default: reuse existing timestamp encoding, SQL runner, schema
  migration, and backend fixture patterns.

## Review Pass

I re-read this plan for latent ambiguity and corrected these issues before
marking it ready:

1. Ambiguous API name:
   - Initial idea: `latest_timestamp()`.
   - Correction: lock `latest_pending_timestamp()` to avoid conflict with
     `Queue.last_ts`.
2. SQLite performance:
   - Initial risk: the query could use a less selective index and scan claimed
     history.
   - Correction: require schema v5 with a partial pending queue/timestamp index.
3. Backend coverage:
   - Initial risk: Python API tests in `tests/` default to SQLite-only.
   - Correction: require `pytestmark = [pytest.mark.shared]` for the new API
     test module.
4. Redis semantics:
   - Initial risk: reading the newest pending ZSET member could return a
     reserved active-batch ID.
   - Correction: require pending minus reserved semantics, matching
     `has_pending_messages()`, plus Redis reserved tests.
5. Scope creep:
   - Initial temptation: add a bulk API for Taut list.
   - Correction: defer it. The one-queue API is enough to remove O(history)
     scans and does not block a later bulk API.
6. Mutation risk:
   - Initial risk: implementing via read/claim helpers.
   - Correction: require non-mutating tests and a read-only query path.
7. Documentation wording:
   - Initial risk: explaining the API as a message ID API because Redis stores
     encoded timestamp IDs internally.
   - Correction: public docs must say timestamp; backend internals can still
     decode encoded IDs.
8. Mixed backend markers:
   - Initial risk: putting SQLite query-plan assertions in the shared behavior
     test module.
   - Correction: keep `tests/test_latest_pending_timestamp.py` entirely
     `shared` and put SQLite plan assertions in `tests/test_sqlite_schema.py`.
9. Extension test selection:
   - Initial risk: adding Redis or Postgres extension tests without the marker
     that the repo's backend scripts select.
   - Correction: require `redis_only` for Redis extension tests and `pg_only`
     for Postgres extension tests.
10. Schema phase marker churn:
    - Initial risk: a schema bump prompts a broad, mechanical rewrite of
      generic `schema-v4` phaselock tests.
    - Correction: call out that most `schema-v4` literals are arbitrary sample
      phase names; only current-schema assertions should change.

No unresolved decisions remain for the implementing engineer. If the plan starts
drifting toward a bulk metadata API, CLI surface, or body-returning latest peek,
stop and file a separate plan. That would be a materially different feature.
