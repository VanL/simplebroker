# Message Body Search API Plan

Date: 2026-05-20

Status: proposed

Owner: SimpleBroker

## Purpose

Add a bounded Python API for finding message IDs in one queue by literal
substring match against message bodies.

This is an API-only feature. Do not add CLI support in this change.

The target broker method is:

```python
broker.find_message_ids(
    queue: str,
    *,
    body_contains: str,
    limit: int = 100,
    after_timestamp: int | None = None,
    before_timestamp: int | None = None,
    include_claimed: bool = False,
) -> list[int]
```

The target `Queue` convenience method is:

```python
queue.find_message_ids(
    *,
    body_contains: str,
    limit: int = 100,
    after_timestamp: int | None = None,
    before_timestamp: int | None = None,
    include_claimed: bool = False,
) -> list[int]
```

This plan assumes the implementer is a skilled engineer with no SimpleBroker
context and weak instincts around test design. Follow the tasks in order. Use
red-green TDD. Keep the implementation boring and bounded.

## Locked Decisions

Do not reopen these decisions unless a red test or backend limitation proves the
plan cannot work.

### API Semantics

`find_message_ids` is a bounded search/inspection API.

- It returns message IDs, which are SimpleBroker timestamps.
- It does not return bodies.
- It does not claim, move, delete, hide, or reorder messages.
- It does not create queues.
- It does not publish activity.
- It is queue-scoped. It searches exactly one queue per call.
- Missing queues and empty queues return `[]`.
- The return order follows the backend's normal queue order:
  - SQLite: `messages.id`
  - Postgres: `messages.order_id`
  - Redis: encoded timestamp order, matching Redis queue ordering today

The method belongs on both:

- `BrokerConnection` / `BrokerCore`, because backend work happens there.
- `Queue`, as a thin single-queue convenience wrapper, because this is a
  single-queue client operation and composes naturally with `Queue.delete_many`.

Do not add:

- CLI commands or flags.
- Cross-queue body search.
- Regex search.
- SQL `LIKE` wildcard search.
- Glob search.
- Case-insensitive search.
- JSON-path search.
- Body-returning search.
- Search-and-delete, search-and-claim, or search-and-move methods.
- Storage migrations or new indexes.

If callers want to delete matches, they can compose explicitly:

```python
ids = queue.find_message_ids(body_contains="expired:", limit=100)
deleted = queue.delete_many(ids)
```

### Match Semantics

Search is literal substring search.

- `body_contains="100%"` matches bodies containing the literal characters
  `100%`.
- `body_contains="a_c"` matches bodies containing the literal characters `a_c`.
- `%`, `_`, `.`, `*`, `[`, `]`, `?`, and other pattern characters have no
  special meaning.
- Matching is case-sensitive.
- Matching is byte/text literal as stored by the backend. Do not add collation
  or Unicode normalization in this change.
- Do not strip the needle before searching. Spaces can be meaningful.

Validation:

- `body_contains` must be `str`.
- `body_contains` must contain at least 3 non-whitespace characters.
- `body_contains` must be at most 1024 characters.
- Reject empty and whitespace-only strings.

The non-whitespace minimum means count characters where `not ch.isspace()`;
do not approximate this with `len(body_contains.strip())`.

Examples:

```python
"abc"      # valid
" a b "    # invalid: only two non-whitespace characters
"a b c"    # valid: three non-whitespace characters
"   "      # invalid
"OK"       # invalid in v1, even though some callers may want it
```

The minimum length is a guardrail, not an index. A search can still scan the
queue.

### Bounds And Limits

Use these constants in shared code:

```python
BODY_SEARCH_DEFAULT_LIMIT = 100
BODY_SEARCH_MAX_LIMIT = 1000
BODY_SEARCH_MIN_NON_WHITESPACE_CHARS = 3
BODY_SEARCH_MAX_NEEDLE_LENGTH = 1024
BODY_SEARCH_REDIS_SCAN_CHUNK_SIZE = 500
```

`limit` behavior:

- Default is `100`.
- Must be an `int`, not `bool`.
- Must be `1 <= limit <= 1000`.
- The method returns at most `limit` IDs.

Timestamp filters:

- `after_timestamp=None` means no lower timestamp bound.
- `after_timestamp=X` means `ts > X`.
- `before_timestamp=None` means no upper timestamp bound.
- `before_timestamp=X` means `ts < X`.
- Both bounds are strict.
- Use the existing shared timestamp-bound validation if present:
  `simplebroker._timestamp.validate_timestamp_bound`.
- If that helper is not present when implementation starts, add it rather than
  duplicating timestamp validation across SQL and Redis.
- Do not parse strings, dates, or Unix timestamps in this Python API.

`after_timestamp >= before_timestamp` should return `[]`, not raise. That
matches existing timestamp-filter behavior in read/peek/move paths.

### Claimed And Reserved Messages

Default behavior:

- `include_claimed=False`
- Search only visible, unclaimed messages.

When `include_claimed=True`:

- SQLite/Postgres include both `claimed = 0/FALSE` and claimed rows.
- Redis includes pending and claimed sets.
- Redis still excludes active reserved IDs. Reserved IDs are an internal
  at-least-once batch state, not stable committed claimed rows.
- Redis should run stale-batch recovery first, using the runner's configured
  stale-batch policy.

This differs from delete semantics on purpose. This is a non-mutating search
API, not an admin data-erasure API.

### Performance Model

This is not an indexed search feature in v1.

- Callers must assume O(messages in queue) scan cost.
- Existing queue indexes help narrow to one queue for SQL backends.
- The body substring predicate is not indexed.
- `limit` limits returned matches. It does not guarantee the backend avoids
  scanning if matches are rare.
- Timestamp bounds can reduce scan cost if callers provide them.

Do not add SQLite FTS, Postgres trigram indexes, Redis secondary indexes, or
schema migrations in this change. Those are future features if real demand
appears.

### API Surface

Public usage through `open_broker`:

```python
from simplebroker import open_broker, target_for_directory

target = target_for_directory(".")

with open_broker(target) as broker:
    ids = broker.find_message_ids(
        "jobs",
        body_contains="tenant:acme",
        limit=100,
    )
```

Public usage through `Queue`:

```python
from simplebroker import Queue

jobs = Queue("jobs")
ids = jobs.find_message_ids(body_contains="tenant:acme", limit=100)
```

Do not export a top-level `simplebroker.find_message_ids`.

Do not add anything to `simplebroker.__all__` unless a new public symbol is
introduced. Adding methods to existing exported classes does not require an
`__all__` change.

## Repository Primer

Runtime package:

- `simplebroker/`

Core public API files:

- `simplebroker/db.py`
  - Owns `DBConnection`, `open_broker`, `BrokerCore`, and `BrokerDB`.
  - Add `BrokerCore.find_message_ids(...)`.
  - This is where shared SQL-backed validation and transaction/retry wrapping
    belongs.
- `simplebroker/sbqueue.py`
  - Owns public single-queue `Queue`.
  - Add `Queue.find_message_ids(...)` as a thin wrapper around the broker
    connection method.
- `simplebroker/_backend_plugins.py`
  - Owns `BackendPlugin` and `BrokerConnection` protocols.
  - Add the broker method and SQL-backend plugin hook.
- `simplebroker/_timestamp.py`
  - Reuse `validate_timestamp_bound` for timestamp filters if present.
- `simplebroker/_message_search.py`
  - New small helper module for shared search validation constants/functions.
  - This avoids duplicating validation in `BrokerCore` and Redis direct core.

SQLite files:

- `simplebroker/_sql/sqlite.py`
  - Add a small SQL builder for message-body search.
- `simplebroker/_backends/sqlite/plugin.py`
  - Add `SQLiteBackendPlugin.find_message_ids(...)`.
- `simplebroker/_backends/sqlite/__init__.py`
  - Do not change if the SQLite implementation lives entirely in `plugin.py`
    and `sqlite.py`.

Postgres files:

- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
  - Add a SQL builder for body search.
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
  - Add `PostgresBackendPlugin.find_message_ids(...)`.
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
  - Do not change in v1.
  - Do not update `_should_prepare()` unless benchmarking later proves this is
    hot enough to force preparation.

Redis files:

- `extensions/simplebroker_redis/simplebroker_redis/core.py`
  - Add `RedisBrokerCore.find_message_ids(...)`.
  - Use bounded client-side chunking. Do not use a huge Lua script for search.
- `extensions/simplebroker_redis/simplebroker_redis/keys.py`
  - Do not change in v1. Reuse `encode_id`, `decode_id`, `min_bound`, and
    `max_bound`.
- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
  - Do not change in v1. Search must not use a whole-queue Lua scan.

Docs:

- `README.md`
  - Add a short API example near the `open_broker()` / advanced API section and
    a short `Queue` example near the existing `Queue.delete_many(message_ids)`
    documentation.
  - State that this is literal substring search and may scan the queue.
  - Do not add CLI docs.

Tests:

- `tests/test_find_message_ids.py`
  - New shared tests for broker-level behavior.
- `tests/test_queue_api_additions.py`
  - Add `Queue.find_message_ids(...)` wrapper coverage and a required
    composition test showing `find_message_ids` results can feed
    `Queue.delete_many`.
- `extensions/simplebroker_pg/tests/test_pg_search.py`
  - Add service-gated real Postgres tests.
- `extensions/simplebroker_redis/tests/test_redis_integration.py`
  - Add service-gated real Redis tests.

Useful existing tests to mimic:

- `tests/test_delete_from_queues.py`
  - Shared broker-level tests, real backends, no mocks.
- `tests/test_batch_delete.py`
  - Exact ID delete behavior and queue wrapper style.
- `tests/test_queue_api_additions.py`
  - High-level `Queue` method conventions.
- `extensions/simplebroker_pg/tests/test_pg_maintenance.py`
  - Real Postgres extension test pattern.
- `extensions/simplebroker_redis/tests/test_redis_integration.py`
  - Real Redis extension test pattern.

## Code Style

Follow current project style.

- Use `from __future__ import annotations` in new Python files.
- Prefer `collections.abc.Sequence` if a sequence type is needed.
- Use `X | None`, `list[int]`, and precise builtin collection types.
- Keep names boring:
  - public method: `find_message_ids`
  - body needle argument: `body_contains`
  - claimed flag: `include_claimed`
- Do not introduce a search abstraction hierarchy.
- Do not add a generic predicate API.
- Do not add SQL string interpolation with caller data.
- Do not over-comment. Add comments only around Redis chunking if needed.
- Do not over-mock. Behavior tests must use real storage.

## Bite-Sized Tasks

### Task 1: Add Shared Red Tests

Create `tests/test_find_message_ids.py`.

Mark the module shared:

```python
import pytest

pytestmark = [pytest.mark.shared]
```

Use the existing `broker` fixture. It yields a real broker core for the active
backend.

Test 1: finds literal substring in one queue only.

Setup:

- Write `tenant:acme invoice 1` to `jobs`.
- Write `tenant:acme invoice 2` to `jobs`.
- Write `tenant:globex invoice 1` to `jobs`.
- Write `tenant:acme invoice 3` to `other`.

Exercise:

```python
ids = broker.find_message_ids(
    "jobs",
    body_contains="tenant:acme",
    limit=10,
)
```

Assert:

- `len(ids) == 2`.
- `ids == [timestamp_for_jobs_acme_1, timestamp_for_jobs_acme_2]`.
- `broker.peek_many("jobs", limit=10, with_timestamps=False)` still returns all
  three `jobs` messages.

Invariant: search is non-mutating and queue-scoped.

Test 2: matching is literal, not SQL pattern matching.

Setup:

- Write `progress 100% ready` to `jobs`.
- Write `progress 100x ready` to `jobs`.
- Write `code a_c ready` to `jobs`.
- Write `code abc ready` to `jobs`.

Exercise and assert:

```python
assert broker.find_message_ids("jobs", body_contains="100%", limit=10) == [
    ts_for_progress_100_percent
]
assert broker.find_message_ids("jobs", body_contains="a_c", limit=10) == [
    ts_for_code_a_underscore_c
]
```

Invariant: `%` and `_` do not behave like SQL `LIKE` wildcards.

Test 3: matching is case-sensitive.

Setup:

- Write `Needle` to `jobs`.
- Write `needle` to `jobs`.

Exercise:

```python
ids = broker.find_message_ids("jobs", body_contains="Needle", limit=10)
```

Assert only the capitalized body matches.

Test 4: default excludes claimed rows, `include_claimed=True` includes them.

Setup:

- Write `target pending` to `jobs`.
- Write `target claimed` to `jobs`.
- Claim `target claimed` by exact timestamp.

Exercise:

```python
visible_ids = broker.find_message_ids("jobs", body_contains="target", limit=10)
all_ids = broker.find_message_ids(
    "jobs",
    body_contains="target",
    limit=10,
    include_claimed=True,
)
```

Assert:

- `visible_ids == [pending_ts]`.
- `all_ids == [pending_ts, claimed_ts]` or the backend's normal queue order if
  claimed row order differs.

Prefer writing the expected order from actual timestamps/order captured during
setup. Do not sort the returned IDs in the assertion unless the contract says
sorting is caller-owned. The contract says backend queue order.

Test 5: timestamp bounds are strict.

Setup:

- Write `target old`.
- Write `target boundary`.
- Capture `boundary_ts`.
- Write `target new`.

Exercise:

```python
before_ids = broker.find_message_ids(
    "jobs",
    body_contains="target",
    limit=10,
    before_timestamp=boundary_ts,
)
after_ids = broker.find_message_ids(
    "jobs",
    body_contains="target",
    limit=10,
    after_timestamp=boundary_ts,
)
between_ids = broker.find_message_ids(
    "jobs",
    body_contains="target",
    limit=10,
    after_timestamp=boundary_ts,
    before_timestamp=boundary_ts,
)
```

Assert:

- `before_ids == [old_ts]`.
- `after_ids == [new_ts]`.
- `between_ids == []`.

Invariant: `after_timestamp` is `ts > X`; `before_timestamp` is `ts < X`.

Test 6: `limit` caps returned matches.

Setup:

- Write five matching messages.

Exercise:

```python
ids = broker.find_message_ids("jobs", body_contains="target", limit=2)
```

Assert:

- `len(ids) == 2`.
- IDs are the first two matches in queue order.

Test 7: missing queue and existing queue with no matches return empty list.

Setup:

- Write `ordinary message` to `jobs`.

Exercise:

```python
assert broker.find_message_ids("missing", body_contains="target", limit=10) == []
assert broker.find_message_ids("jobs", body_contains="absent", limit=10) == []
```

Test 8: validation rejects unsafe inputs before scanning.

Use real storage and assert existing messages remain after each error.

Cases:

- invalid queue name: `broker.find_message_ids("bad queue", body_contains="target")`
- non-string needle: `body_contains=123`
- empty needle: `body_contains=""`
- whitespace-only needle: `body_contains="   "`
- fewer than 3 non-whitespace characters: `"ab"` and `"a b"`
- too-long needle: `"x" * 1025`
- `limit=0`
- `limit=1001`
- `limit=True`
- invalid timestamp bound: `before_timestamp=-1`

Do not assert private SQL or backend internals in this shared test file.

Run the red tests:

```bash
python -m pytest tests/test_find_message_ids.py
```

Expected result before implementation: failures because `find_message_ids` does
not exist.

### Task 2: Add Shared Validation Helpers

File: `simplebroker/_message_search.py`

Create this new internal helper module:

```python
from __future__ import annotations

BODY_SEARCH_DEFAULT_LIMIT = 100
BODY_SEARCH_MAX_LIMIT = 1000
BODY_SEARCH_MIN_NON_WHITESPACE_CHARS = 3
BODY_SEARCH_MAX_NEEDLE_LENGTH = 1024
BODY_SEARCH_REDIS_SCAN_CHUNK_SIZE = 500


def validate_body_contains(value: str) -> str:
    ...


def validate_body_search_limit(value: int) -> int:
    ...
```

Validation details:

- Reject `bool` for `limit`, even though `bool` is an `int` subclass.
- Reject `bool` and non-`str` for `body_contains`.
- Count non-whitespace characters with:

```python
sum(1 for char in value if not char.isspace())
```

- Return the original `value`, not a stripped value.

Reason: SQL-backed `BrokerCore` and direct `RedisBrokerCore` both need identical
validation. A tiny shared helper is justified; duplicating this logic would
invite backend divergence.

Add focused unit tests only if the shared behavior tests do not pin the
validation well enough. Prefer behavior tests first.

### Task 3: Extend Protocols

File: `simplebroker/_backend_plugins.py`

Add a SQL backend plugin hook to `BackendPlugin`:

```python
def find_message_ids(
    self,
    runner: SQLRunner,
    *,
    queue: str,
    body_contains: str,
    limit: int,
    after_timestamp: int | None = None,
    before_timestamp: int | None = None,
    include_claimed: bool = False,
) -> list[int]: ...
```

Add the broker method to `BrokerConnection`:

```python
def find_message_ids(
    self,
    queue: str,
    *,
    body_contains: str,
    limit: int = ...,
    after_timestamp: int | None = None,
    before_timestamp: int | None = None,
    include_claimed: bool = False,
) -> list[int]: ...
```

Do not add an optional `getattr` fallback. Missing backend implementations
should fail loudly during development.

If any test-local fake backend plugin fails because the protocol grew, update
that fake to delegate to the SQLite plugin or implement a minimal real-storage
path. Do not loosen the protocol to satisfy a fake.

Run import smoke tests after this task:

```bash
python -m pytest tests/test_backend_plugin_resolution.py tests/test_ext_imports.py
```

### Task 4: Implement `BrokerCore.find_message_ids`

File: `simplebroker/db.py`

Add the method near other broker-level read/maintenance APIs. A reasonable
location is after `peek_generator(...)` or near `delete_message_ids(...)`.

Implementation requirements:

- `_check_fork_safety()`
- `_validate_queue_name(queue)`
- validate `body_contains` through `validate_body_contains(...)`
- validate `limit` through `validate_body_search_limit(...)`
- validate timestamp bounds through `validate_timestamp_bound(...)`
- do not open a write transaction
- do not call `begin_immediate()`
- use `_run_with_retry(...)`
- hold `self._lock` while calling the backend plugin
- call `self._backend_plugin.find_message_ids(...)`
- return a `list[int]`

Recommended shape:

```python
def find_message_ids(
    self,
    queue: str,
    *,
    body_contains: str,
    limit: int = BODY_SEARCH_DEFAULT_LIMIT,
    after_timestamp: int | None = None,
    before_timestamp: int | None = None,
    include_claimed: bool = False,
) -> list[int]:
    self._check_fork_safety()
    self._validate_queue_name(queue)
    body_contains = validate_body_contains(body_contains)
    limit = validate_body_search_limit(limit)
    after_timestamp = validate_timestamp_bound("after_timestamp", after_timestamp)
    before_timestamp = validate_timestamp_bound("before_timestamp", before_timestamp)

    def _do_find_message_ids() -> list[int]:
        with self._lock:
            return self._backend_plugin.find_message_ids(
                self._runner,
                queue=queue,
                body_contains=body_contains,
                limit=limit,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                include_claimed=include_claimed,
            )

    return self._run_with_retry(_do_find_message_ids)
```

Do not add transaction rollback logic. This is a read-only operation.

Do not call `_assert_no_reentrant_mutation_during_batch(...)`. This is not a
mutation. It should be allowed from read-only reentrant contexts unless existing
code already blocks all broker access there.

Run the shared tests again. They should now fail at the SQLite backend hook, not
at missing broker method.

### Task 5: Add SQLite SQL Builder And Plugin Hook

Files:

- `simplebroker/_sql/sqlite.py`
- `simplebroker/_backends/sqlite/plugin.py`

In `simplebroker/_sql/sqlite.py`, add a builder:

```python
def build_find_message_ids_query(
    *,
    after_timestamp: int | None,
    before_timestamp: int | None,
    include_claimed: bool,
) -> str:
    ...
```

The generated query should be equivalent to:

```sql
SELECT ts
FROM messages
WHERE queue = ?
  AND instr(body, ?) > 0
  -- include only when include_claimed is False:
  AND claimed = 0
  -- include only when after_timestamp is not None:
  AND ts > ?
  -- include only when before_timestamp is not None:
  AND ts < ?
ORDER BY id
LIMIT ?
```

Parameter order must be:

1. `queue`
2. `body_contains`
3. `after_timestamp`, if present
4. `before_timestamp`, if present
5. `limit`

Use SQLite `instr(body, ?) > 0`, not `LIKE`.

In `SQLiteBackendPlugin.find_message_ids(...)`:

- Build the SQL through the builder.
- Build params in the exact order documented above.
- Run with `fetch=True`.
- Return `[int(row[0]) for row in rows]`.

Do not add a new SQLite index or migration.

Run:

```bash
python -m pytest tests/test_find_message_ids.py
```

Expected after Task 5: shared tests pass on default SQLite except any Queue
wrapper tests that have not been added yet.

### Task 6: Add `Queue.find_message_ids`

File: `simplebroker/sbqueue.py`

Add a thin wrapper near other read/peek helpers:

```python
from ._message_search import BODY_SEARCH_DEFAULT_LIMIT


def find_message_ids(
    self,
    *,
    body_contains: str,
    limit: int = BODY_SEARCH_DEFAULT_LIMIT,
    after_timestamp: int | None = None,
    before_timestamp: int | None = None,
    include_claimed: bool = False,
) -> list[int]:
    with self.get_connection() as connection:
        return connection.find_message_ids(
            self.name,
            body_contains=body_contains,
            limit=limit,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
            include_claimed=include_claimed,
        )
```

Follow the surrounding `Queue` style:

- Use its existing connection context manager.
- Do not duplicate validation; let `BrokerCore` / direct backend validate.
- Do not make this a generator.
- Do not return bodies.

Add tests to `tests/test_queue_api_additions.py`:

1. `Queue.find_message_ids(...)` returns IDs for matching messages.
2. It composes with `Queue.delete_many(...)`:

```python
ids = q.find_message_ids(body_contains="target", limit=10)
assert q.delete_many(ids) == len(ids)
```

Run:

```bash
python -m pytest tests/test_find_message_ids.py tests/test_queue_api_additions.py
```

### Task 7: Add Postgres SQL And Plugin Hook

Files:

- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`

Prefer a builder over many constants because the optional predicates are small
and this avoids combinatorial SQL constants:

```python
def build_find_message_ids_query(
    *,
    after_timestamp: int | None,
    before_timestamp: int | None,
    include_claimed: bool,
) -> str:
    ...
```

The generated SQL should be equivalent to:

```sql
SELECT ts
FROM messages
WHERE queue = ?
  AND strpos(body, ?) > 0
  -- include only when include_claimed is False:
  AND claimed = FALSE
  -- include only when after_timestamp is not None:
  AND ts > ?
  -- include only when before_timestamp is not None:
  AND ts < ?
ORDER BY order_id
LIMIT ?
```

Use `strpos(body, ?) > 0`, not `LIKE` or regex operators.

In `PostgresBackendPlugin.find_message_ids(...)`:

- Build params in the same order as SQLite.
- Run with `fetch=True`.
- Return `[int(row[0]) for row in rows]`.

Do not add `pg_trgm`, GIN/GiST indexes, generated columns, or migrations.

Do not update `_should_prepare()` in `runner.py` unless later benchmarking shows
this query is hot. This API is explicitly not a hot queue primitive.

### Task 8: Add Postgres Tests

File: `extensions/simplebroker_pg/tests/test_pg_search.py`.

Mark new file:

```python
pytestmark = [pytest.mark.pg_only]
```

Use existing real Postgres fixtures:

- `pg_core`
- `pg_runner`

Add tests:

1. Finds literal substring and preserves rows.
2. `include_claimed=False` excludes claimed rows.
3. `include_claimed=True` includes claimed rows.
4. `%` and `_` are literal.

Keep these extension tests shorter than shared tests. They should prove the
Postgres backend hook works against real Postgres, not duplicate every shared
behavior.

Run without a service first:

```bash
PYTHONPATH=extensions/simplebroker_pg:. \
python -m pytest extensions/simplebroker_pg/tests/test_pg_search.py
```

Expected without `SIMPLEBROKER_PG_TEST_DSN`: clean skips.

Run with a service when available:

```bash
SIMPLEBROKER_PG_TEST_DSN='postgresql://...' \
PYTHONPATH=extensions/simplebroker_pg:. \
python -m pytest extensions/simplebroker_pg/tests/test_pg_search.py
```

Also run shared tests against Postgres when a service is available:

```bash
BROKER_TEST_BACKEND=postgres \
SIMPLEBROKER_PG_TEST_DSN='postgresql://...' \
PYTHONPATH=extensions/simplebroker_pg:. \
python -m pytest tests/test_find_message_ids.py
```

### Task 9: Implement Redis Search

File: `extensions/simplebroker_redis/simplebroker_redis/core.py`

Add `RedisBrokerCore.find_message_ids(...)` with the same signature as
`BrokerCore`.

Validation requirements:

- `_check_fork_safety()`
- `_validate_queue_name(queue)`
- shared `validate_body_contains(...)`
- shared `validate_body_search_limit(...)`
- shared `validate_timestamp_bound(...)`
- stale-batch recovery before scanning if the runner's stale-batch policy allows
  it

Implementation requirements:

- Do not use a Lua script that scans a whole queue.
- Do not call `ZRANGE ... 0 -1` for large sets.
- Scan candidate IDs in chunks of `BODY_SEARCH_REDIS_SCAN_CHUNK_SIZE`.
- Use Redis lexicographic timestamp bounds:
  - `min_bound(after_timestamp)`
  - `max_bound(before_timestamp)`
- Use pending set only when `include_claimed=False`.
- Use pending and claimed sets when `include_claimed=True`.
- Always exclude reserved IDs, including when `include_claimed=True`.
- Return decoded integer timestamps.
- Stop as soon as `limit` matches have been collected.

Recommended simple shape:

1. Build a list of source keys:
   - pending only, or pending plus claimed.
2. Maintain a per-source lower lex bound.
3. Fetch up to `BODY_SEARCH_REDIS_SCAN_CHUNK_SIZE` IDs from each source with
   `ZRANGEBYLEX`.
4. Merge candidates by encoded ID so returned IDs follow Redis queue order.
5. For a candidate:
   - skip it if it is reserved
   - fetch body from `bodies`
   - append decoded ID if `body_contains in body`
6. Advance the source lower bound past every consumed ID with `({encoded_id}`,
   even if the ID is reserved, missing a body, or does not match.
7. Continue until:
   - `len(matches) == limit`, or
   - every source is exhausted.

Use pipelines where they keep the code clear, but do not contort the
implementation. Correct bounded behavior matters more than micro-optimizing v1.

Do not publish activity.

Do not block active reserved batches. This is a read-only search. It should skip
reserved IDs rather than error.

### Task 10: Add Redis Tests

File: `extensions/simplebroker_redis/tests/test_redis_integration.py`

Add service-gated real Redis tests:

1. Finds literal substring in pending messages.
2. Does not mutate Redis storage.
3. `include_claimed=False` excludes claimed set.
4. `include_claimed=True` includes claimed set.
5. `%` and `_` are literal.
6. Timestamp bounds are strict.

Add one reserved-batch test in
`extensions/simplebroker_redis/tests/test_redis_batches.py`:

- Write `target reserved`.
- Start an `at_least_once` claim generator and advance it once.
- Call `find_message_ids(... include_claimed=True)`.
- Assert the reserved ID is not returned.
- Close the generator in `finally`.

Run without Redis installed:

```bash
PYTHONPATH=extensions/simplebroker_redis:. \
python -m pytest extensions/simplebroker_redis/tests/test_redis_integration.py
```

If it fails with `ModuleNotFoundError: No module named 'redis'`, report the
environment gap. Do not replace this with mocks.

Run with a configured Redis/Valkey service:

```bash
SIMPLEBROKER_REDIS_TEST_URL='redis://127.0.0.1:6379/0' \
PYTHONPATH=extensions/simplebroker_redis:. \
python -m pytest \
  extensions/simplebroker_redis/tests/test_redis_integration.py \
  extensions/simplebroker_redis/tests/test_redis_batches.py
```

Run shared tests against Redis when a service is available:

```bash
BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_REDIS_TEST_URL='redis://127.0.0.1:6379/0' \
PYTHONPATH=extensions/simplebroker_redis:. \
python -m pytest tests/test_find_message_ids.py
```

### Task 11: Add README Documentation

File: `README.md`

Document near the advanced `open_broker()` section:

```python
with open_broker(target) as broker:
    ids = broker.find_message_ids(
        "jobs",
        body_contains="tenant:acme",
        limit=100,
    )
```

Document near the existing `Queue.delete_many(message_ids)` docs:

```python
ids = queue.find_message_ids(body_contains="tenant:acme", limit=100)
deleted = queue.delete_many(ids)
```

State explicitly:

- This is literal substring search.
- It returns message IDs/timestamps.
- It may scan the selected queue.
- `body_contains` must contain at least 3 non-whitespace characters.
- Default search excludes claimed messages.
- `include_claimed=True` includes claimed messages where the backend has stable
  claimed rows.
- No CLI command exists.

Do not add benchmark claims. Do not imply indexed search.

### Task 12: Focused Verification

Run the new shared and queue tests:

```bash
python -m pytest \
  tests/test_find_message_ids.py \
  tests/test_queue_api_additions.py
```

Run nearby behavior tests:

```bash
python -m pytest \
  tests/test_batch_delete.py \
  tests/test_message_by_timestamp.py \
  tests/test_generator_methods.py \
  tests/test_queue_api_comprehensive.py
```

Run plugin/import smoke tests:

```bash
python -m pytest \
  tests/test_backend_plugin_resolution.py \
  tests/test_ext_imports.py
```

Run full local tests:

```bash
python -m pytest
```

Run lint:

```bash
python -m ruff check .
```

Run mypy if the environment has all optional extension dependencies installed:

```bash
python -m mypy \
  simplebroker \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis
```

If mypy fails only because optional `redis` stubs/package are unavailable, report
that environment gap. Do not hide it.

### Task 13: Service Backend Gates

Postgres:

```bash
BROKER_TEST_BACKEND=postgres \
SIMPLEBROKER_PG_TEST_DSN='postgresql://...' \
PYTHONPATH=extensions/simplebroker_pg:. \
python -m pytest \
  tests/test_find_message_ids.py \
  extensions/simplebroker_pg/tests/test_pg_search.py
```

Redis:

```bash
BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_REDIS_TEST_URL='redis://127.0.0.1:6379/0' \
PYTHONPATH=extensions/simplebroker_redis:. \
python -m pytest \
  tests/test_find_message_ids.py \
  extensions/simplebroker_redis/tests/test_redis_integration.py \
  extensions/simplebroker_redis/tests/test_redis_batches.py
```

If service backends are unavailable, say so plainly in implementation notes.
Do not fake service gates with mocks.

## Invariants Checklist

The implementation is not complete unless these are true:

- `open_broker(...).find_message_ids(...)` exists.
- `Queue.find_message_ids(...)` exists.
- The method returns `list[int]`.
- It searches one queue only.
- It does not mutate storage.
- It returns at most `limit` IDs.
- `limit` must be `1..1000` and not `bool`.
- `body_contains` must be a string.
- `body_contains` must contain at least 3 non-whitespace characters.
- `body_contains` must be at most 1024 characters.
- Matching is literal substring search.
- `%` and `_` are not wildcards.
- Matching is case-sensitive.
- Missing queues return `[]`.
- Default excludes claimed rows.
- `include_claimed=True` includes stable claimed rows.
- Redis excludes active reserved IDs.
- Timestamp bounds are strict.
- SQLite uses `instr(body, ?) > 0`, not `LIKE`.
- Postgres uses `strpos(body, ?) > 0`, not `LIKE`, regex, or trigram.
- Redis scans in bounded chunks, not one whole-queue Lua script.
- No CLI surface was added.
- No storage migration or search index was added.

## Common Failure Modes

Avoid these mistakes:

- Returning bodies instead of IDs. That increases data exposure and payload size.
- Adding `read_matching`, `claim_matching`, or `delete_matching`.
- Using SQL `LIKE` and accidentally treating `%` / `_` as wildcards.
- Accepting one- or two-character needles.
- Checking `len(body_contains.strip()) >= 3` instead of counting
  non-whitespace characters.
- Letting `limit=True` pass as `1`.
- Forgetting `include_claimed=False` by default.
- Implementing Redis search with a full-set `ZRANGE 0 -1`.
- Implementing Redis search with a long-running Lua script.
- Adding a migration or index in v1.
- Adding CLI support because it is easy.
- Mocking `BrokerCore`, SQL runners, or Redis clients and calling that coverage.

## Review Checklist Before Landing

Before landing implementation, review the diff with these questions:

1. Is the public API exactly `find_message_ids(...)` with the locked signature?
2. Is the feature exposed through both `open_broker()` and `Queue`?
3. Does every backend use the same validation rules?
4. Are substring matches literal and case-sensitive?
5. Is the Redis implementation bounded and client-side chunked?
6. Did any code add CLI behavior or docs?
7. Did any code add an index, migration, FTS table, trigram extension, or
   secondary Redis index?
8. Are tests mostly real-storage behavior tests?
9. Do docs state that this may scan the selected queue?

If any answer is no, fix that before landing.

## Stop Conditions

Stop and report instead of forcing the plan if any of these happen:

- A backend cannot implement literal substring matching without changing stored
  message format.
- Redis search cannot be made bounded without a much larger cursor API.
- Product requirements shift from bounded ID search to full search/indexing.
- The API starts drifting toward regex, SQL pattern matching, or JSON querying.
- The implementation requires a storage migration.
- The implementation starts changing read, claim, move, delete, or watcher
  semantics.

Those would mean the work is moving away from the agreed API-only investigation.
