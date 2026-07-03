# Exact Message Import API Plan

Date: 2026-06-01

Status: superseded

Superseded on 2026-06-01 by the 4.3.0 corrective exact-ID insert API. This
document is historical context only. Its locked decisions are not the current
implementation contract.

Current contract:

```python
broker.insert_messages([
    (queue, message, message_id),
])

queue.insert_messages([
    (message, message_id),
])
```

`insert_messages(...)` is the only public exact-ID insertion API. It accepts one
or more records, validates the whole batch, rejects duplicate IDs, advances
`last_ts` above the largest supplied ID inside the same transaction, and inserts
pending messages with their exact IDs. The 4.1/4.2 APIs `import_message(...)`,
`import_messages(...)`, and `write_reserved_message(...)` were removed before
they had external compatibility requirements.

Owner: SimpleBroker

## Purpose

Add a narrow Python API for writing a SimpleBroker message with an exact
message ID during restore/import workflows.

This is API-only. Do not add CLI flags, CLI commands, or JSONL import/export
commands in SimpleBroker.

The target broker method is:

```python
broker.import_message(
    queue: str,
    message: str,
    *,
    message_id: int,
) -> None
```

The target `Queue` convenience method is:

```python
queue.import_message(
    message: str,
    *,
    message_id: int,
) -> None
```

This plan assumes the implementer is a skilled developer with no SimpleBroker
context and uneven instincts around API and test design. Follow the tasks in
order. Use red-green TDD. Keep the implementation boring and direct.

## Problem Context

SimpleBroker uses one integer as both timestamp and durable message ID. Normal
writes intentionally allocate this ID through the broker timestamp generator.

The normal write path lives in `simplebroker/db.py`:

- `BrokerCore.write(...)` validates fork safety, queue name, reentrant mutation
  state, and message size.
- `_do_write_with_ts_retry(...)` calls `generate_timestamp()`.
- `_do_write_transaction(...)` inserts `(queue, body, ts)` through the backend
  SQL namespace.

That is the right default behavior. Most callers must not choose their own
message IDs.

Restore/import is different. Downstream systems such as Weft have durable
contracts where an already allocated broker message ID is embedded in a payload.
If restore rewrites that ID, the restored broker state is internally
inconsistent.

The current downstream workaround reaches into private SimpleBroker internals.
The purpose of this change is to make the needed capability public, small, and
hard to misuse.

## Locked Decisions

Do not reopen these decisions unless a red test or real backend limitation
proves the plan cannot work.

### API Surface

Add exactly these public APIs:

```python
BrokerCore.import_message(queue: str, message: str, *, message_id: int) -> None
Queue.import_message(message: str, *, message_id: int) -> None
```

Update the `BrokerConnection` protocol so `open_broker(...)` users and backend
implementations have the same contract.

Do not add:

- CLI support.
- Batch import.
- Upsert/ignore-on-conflict behavior.
- Claimed-message import.
- Cross-queue import.
- A public API for forcing `last_ts` to an arbitrary value.
- A public API that writes an exact message ID equal to the current high-water
  mark.
- A public API named `write(..., timestamp=...)` or `write(..., message_id=...)`.

The name must be `import_message`, not `write_message_with_timestamp`. The word
`import` communicates that this is an identity-preserving restore primitive,
not ordinary message production.

### Critical Invariant

`import_message` must only accept a `message_id` that is strictly lower than the
broker's current `last_ts`.

In code terms:

```python
if message_id >= current_last_ts:
    raise ValueError(...)
```

This invariant is intentional.

- It keeps `import_message` from becoming a second timestamp allocator.
- It keeps the timestamp generator's high-water mark ahead of imported rows.
- It avoids advancing `last_ts` based on caller-controlled historical data.
- It means future normal writes remain strictly above imported IDs.

`import_message` must not call `generate_timestamp()` internally. It must not
advance `last_ts`. Callers that want a preflight check should read the broker's
current high-water mark through the existing public API and fail before writes
if their import IDs are not historical for that broker:

```python
current_last_ts = broker.refresh_last_timestamp()
assert max(imported_message_ids) < current_last_ts
for record in records:
    broker.import_message(record.queue, record.body, message_id=record.message_id)
```

This is a feature, not a bug. It makes import a historical restore operation,
not a way to allocate or reserve IDs. A fresh broker whose `last_ts` is still
`0` cannot import ordinary nonzero IDs through this API. That caller should
fail before writes or use a different restore strategy.

### Downstream Weft Implication

This API is enough for Weft dump/load when the destination broker's current
`last_ts` is already above the maximum imported message ID. Weft can read that
value with the public broker timestamp-cache API and fail before writes if the
condition is not true.

This API is not a direct drop-in replacement for Weft's current preallocated
spawn-request write path if that path does:

```python
tid = queue.generate_timestamp()
broker.import_message("weft.spawn.requests", body, message_id=tid)
```

That must fail because `tid == current_last_ts`, and the invariant requires
`tid < current_last_ts`.

Weft has two valid options after this SimpleBroker API exists:

- For dump/load: read destination `last_ts` and require every imported message
  ID to be lower before importing.
- For live preallocated spawn requests: keep its private workaround until a
  separate reserved-ID API is designed, unless Weft can prove a later broker
  allocation has already made `tid < current last_ts`.

Do not weaken the SimpleBroker invariant to make the live spawn path easier.
That would turn import into a general exact-ID write API.

### Message Semantics

An imported message is a normal pending message after insertion.

- It is visible to `peek`, `read`, `move`, `delete`, `find_message_ids`, queue
  stats, and watchers exactly like a normal write.
- It is inserted with the supplied `message_id`.
- It is pending/unclaimed. Do not add a `claimed` parameter.
- It preserves normal backend queue ordering for newly visible messages.
- It must trigger the same backend activity notification behavior as normal
  writes. For Postgres this should happen by reusing the existing backend
  `INSERT_MESSAGE` SQL. For Redis this means publishing after the import Lua
  script succeeds.

Duplicate message IDs are errors.

- Do not make import idempotent.
- Do not silently ignore duplicates.
- Do not overwrite an existing message body.
- Let the backend uniqueness error surface as `IntegrityError` where the normal
  write path would surface it, or translate direct-backend duplicate codes to
  the existing SimpleBroker `IntegrityError`.

### Validation Semantics

Validate the same things normal writes validate:

- fork safety
- queue name
- active same-thread at-least-once generator mutation state
- message size

Also validate `message_id`:

- It must be an `int`.
- `bool` is not accepted.
- It must be non-negative.
- It must be lower than `SQLITE_MAX_INT64`.
- It must be strictly lower than the current `last_ts` observed by the broker
  during the import operation.

Do not accept strings in this API. Callers that load IDs from JSON must parse
them before calling `import_message`.

Use the existing `validate_timestamp_bound` behavior for type/range checks:
type violations such as `bool` or `str` raise `TypeError`; range violations
raise `ValueError`. Use `ValueError` for the `message_id >= current_last_ts`
invariant violation.

### Transaction Semantics

For SQL-backed cores, keep the path as two thin steps:

1. Refresh/read current `last_ts` through the existing broker timestamp API.
2. Reject if `message_id >= current_last_ts`.
3. Insert the message by reusing the existing retry wrapper and exact insert
   helper.

Do not create a second import-specific SQL transaction helper unless tests prove
the existing helper cannot be reused.

Do not advance `last_ts` after insert.

For direct backends such as Redis, use existing backend primitives as much as
possible. Since `last_ts` only moves upward, it is safe to read it before the
write and reject conservatively. If `message_id < observed last_ts`, the
relation cannot become false before the insert. Then reuse the existing Redis
write script for uniqueness, storage, queue indexing, and queue registration.

### DRY/YAGNI Constraints

Reuse existing validation and write plumbing where it fits:

- `_check_fork_safety`
- `_validate_queue_name`
- `_assert_no_reentrant_mutation_during_batch`
- `_validate_message_size`
- `_run_with_retry`
- `refresh_last_timestamp`
- `_do_write_transaction`
- `_sql.INSERT_MESSAGE`
- backend plugin `read_last_ts`

Do not duplicate SQL string literals already present in the backend SQL
namespace.

Do not add abstraction layers for hypothetical future batch import, arbitrary
high-water mark mutation, alternate conflict policies, claimed-state import, or
backend-specific import hooks.

Thinness is a requirement, not a preference. If the implementation can be a
small method that validates, reads current `last_ts`, and delegates to an
existing exact insert/write primitive, do that. Do not introduce a new helper,
backend hook, SQL string, Lua script, or transaction wrapper merely because the
operation has a new public name.

## Files To Read First

Read these files before editing:

- `simplebroker/db.py`
  - `open_broker`
  - `BrokerCore.write`
  - `_do_write_with_ts_retry`
  - `_do_write_transaction`
  - `delete_message_ids`
  - `find_message_ids`
- `simplebroker/sbqueue.py`
  - `Queue.write`
  - `Queue.generate_timestamp`
  - `Queue.delete_many`
  - `Queue.find_message_ids`
- `simplebroker/_backend_plugins.py`
  - `BrokerConnection` protocol
  - `BackendPlugin` protocol
- `simplebroker/_timestamp.py`
  - `validate_timestamp_bound`
  - `TimestampGenerator.generate`
- `simplebroker/_sql/sqlite.py`
  - `INSERT_MESSAGE`
  - `CREATE_MESSAGES_TABLE`
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
  - `INSERT_MESSAGE`
  - notification behavior
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
  - `write`
  - `_write_message`
  - `_publish`
- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
  - `WRITE_MESSAGE`, to confirm it can be reused unchanged
- `tests/conftest.py`
  - `broker`
  - `queue_factory`
  - backend markers and fixtures
- `pyproject.toml`
  - pytest markers
  - ruff and mypy config

Read these docs for public contract style:

- `README.md`, section `Generating timestamps without writing`
- `README.md`, section `Advanced: Custom Extensions`
- `docs/plans/2026-05-17-physical-delete-and-batch-delete-plan.md`
- `docs/plans/2026-05-20-message-body-search-api-plan.md`

## Implementation Tasks

### Task 1: Add Failing Shared Broker Tests

Create `tests/test_import_message.py`.

Mark the module shared:

```python
import pytest

pytestmark = [pytest.mark.shared]
```

Use the real `broker` fixture from `tests/conftest.py`. Do not mock
`BrokerCore`, `TimestampGenerator`, `SQLRunner`, or backend plugins.

Add these tests first and watch them fail.

#### Test: broker import preserves exact ID and body

Arrange:

```python
message_id = broker.generate_timestamp()
current_last_ts = broker.generate_timestamp()
assert message_id < current_last_ts
```

Act:

```python
broker.import_message("jobs", "restored body", message_id=message_id)
```

Assert:

```python
assert broker.peek_one(
    "jobs",
    exact_timestamp=message_id,
    with_timestamps=True,
) == ("restored body", message_id)
```

Then write a normal message and assert the new normal message ID is higher than
the pre-import `last_ts`:

```python
broker.write("jobs", "normal")
rows = broker.peek_many("jobs", limit=10, with_timestamps=True)
assert rows[-1][0] == "normal"
assert rows[-1][1] > current_last_ts
```

This proves import did not move the high-water mark backward and did not poison
normal writes.

#### Test: broker import rejects message_id equal to last_ts

Arrange:

```python
message_id = broker.generate_timestamp()
```

Act/assert:

```python
with pytest.raises(ValueError, match="lower than current last_ts"):
    broker.import_message("jobs", "body", message_id=message_id)
```

This is the exact invariant that prevents preallocated IDs from being imported
without a separate high-water mark.

#### Test: broker import rejects message_id greater than last_ts

Arrange:

```python
message_id = broker.generate_timestamp() + 1
```

Act/assert:

```python
with pytest.raises(ValueError, match="lower than current last_ts"):
    broker.import_message("jobs", "body", message_id=message_id)
```

#### Test: broker import rejects invalid message IDs

Parametrize invalid values:

```python
@pytest.mark.parametrize("message_id", [True, -1, 9223372036854775807])
def test_import_message_rejects_invalid_message_id(broker, message_id):
    ...
```

Use `ValueError` or `TypeError` according to the existing
`validate_timestamp_bound` behavior. Do not contort the implementation to make
every invalid value raise the same exception if the existing helper already
sets a clear precedent.

Important: the test should assert the API rejects these values before inserting
anything. After the exception, assert `broker.peek_one("jobs") is None`.

#### Test: broker import rejects duplicate exact IDs

Arrange:

```python
from simplebroker.ext import IntegrityError

message_id = broker.generate_timestamp()
broker.generate_timestamp()
broker.import_message("jobs", "first", message_id=message_id)
```

Act/assert:

```python
with pytest.raises(IntegrityError):
    broker.import_message("other", "duplicate", message_id=message_id)
```

Assert only the first message exists:

```python
assert broker.peek_one("jobs", exact_timestamp=message_id) == "first"
assert broker.peek_one("other", exact_timestamp=message_id) is None
```

This tests the global uniqueness invariant. Do not mock the uniqueness check.

#### Test: broker import validates queue name and message size

Use one focused test for queue name and one for message size. Do not create a
large matrix.

For queue name, use an existing invalid-name pattern from
`tests/test_queue_validation.py`.

For message size, copy the tiny-config pattern from
`tests/test_queue_config_defaults.py::test_queue_write_uses_configured_message_size_limit`.
Use a real Queue or broker configured with `{"BROKER_MAX_MESSAGE_SIZE": 3}` and
assert importing `"toolong"` raises the same maximum-size error family. Do not
mock `_validate_message_size`.

### Task 2: Add Failing Queue Wrapper Tests

Add Queue-level tests to `tests/test_import_message.py` using `queue_factory`.

#### Test: queue import delegates to broker and preserves exact ID

Arrange:

```python
q = queue_factory("jobs")
message_id = q.generate_timestamp()
q.generate_timestamp()
```

Act:

```python
q.import_message("restored body", message_id=message_id)
```

Assert:

```python
assert q.peek(message_id=message_id, with_timestamps=True) == (
    "restored body",
    message_id,
)
```

Also assert `q.last_ts` remains at or above the second generated timestamp after
import. Do not assert an exact value unless the existing `Queue.last_ts`
behavior makes that stable across backends.

### Task 3: Add Optional Activity-Waiter Tests Only If Existing Helpers Make It Cheap

Do not start here. Implement core behavior first.

After import works, decide whether watcher wake-up behavior needs explicit
coverage. The rule is:

- Add the test if an existing activity-waiter test can be copied in less than
  about 20 lines and run against the active backend.
- Skip it if it requires new polling infrastructure, sleeps, subprocesses, or
  backend-specific setup.

The key engineering point is that implementation should reuse the same insert
path normal writes use. If the same backend insert path already has watcher
coverage, import-specific watcher tests are not mandatory for this change.

### Task 4: Implement `BrokerCore.import_message`

Edit `simplebroker/db.py`.

Add `BrokerCore.import_message(...)` near `write(...)`, not at the bottom of the
file. The method belongs beside ordinary message production.

Use the existing timestamp read and exact insert helper. Do not add an
import-specific transaction helper unless a red test proves the existing helper
cannot be reused.

Use `validate_timestamp_bound("message_id", message_id)` inline. Do not add a
new `_validate_import_message_id` helper unless a second local caller appears.
Be careful: `validate_timestamp_bound` returns `int | None` because it also
serves optional timestamp filters. For this API, `None` is invalid.

Suggested structure:

```python
def import_message(self, queue: str, message: str, *, message_id: int) -> None:
    self._check_fork_safety()
    self._validate_queue_name(queue)
    self._assert_no_reentrant_mutation_during_batch("import_message")
    self._validate_message_size(message)
    normalized_id = validate_timestamp_bound("message_id", message_id)
    if normalized_id is None:
        raise TypeError("message_id must be an int")

    current_last_ts = self.refresh_last_timestamp()
    if normalized_id >= current_last_ts:
        raise ValueError(
            "imported message_id must be lower than current last_ts"
        )

    self._run_with_retry(
        lambda: self._do_write_transaction(queue, message, normalized_id)
    )
```

This is intentionally thin:

- existing timestamp API for `last_ts`
- existing validation helpers
- existing retry wrapper
- existing exact insert helper
- existing backend `INSERT_MESSAGE`

Implementation notes:

- Do not call `generate_timestamp()`.
- Do not call `advance_last_ts`.
- Do not call `write_last_ts`.
- Do not increment `_write_count`.
- Do not trigger auto-vacuum from import.
- Do not duplicate `INSERT INTO messages ...` SQL in `db.py`.
- Reuse `_sql.INSERT_MESSAGE` so Postgres keeps its existing `pg_notify`
  behavior.
- Do not add a new helper solely to wrap `_do_write_transaction`.

### Task 5: Update the Public Broker Protocol

Edit `simplebroker/_backend_plugins.py`.

Add this method to `BrokerConnection` near `write`:

```python
def import_message(self, queue: str, message: str, *, message_id: int) -> None: ...
```

Use `None`. This API has no meaningful return value.

Do not add a `BackendPlugin` method for SQL backends. `BrokerCore` can implement
this through the existing SQL namespace and existing plugin metadata hooks.

### Task 6: Add `Queue.import_message`

Edit `simplebroker/sbqueue.py`.

Add the method near `Queue.write`:

```python
def import_message(self, message: str, *, message_id: int) -> None:
    """Import a pending message with an exact existing message ID."""
    with self.get_connection() as connection:
        connection.import_message(self.name, message, message_id=message_id)
        self._update_last_ts_hint(connection)
```

Keep this wrapper thin. It should not duplicate queue validation, message-size
validation, or `last_ts` checks.

Do not add a high-level CLI-mirroring method. This is already the public Queue
method.

### Task 7: Implement Thin Redis Direct Backend Support

The Redis backend does not use `BrokerCore`, so it needs its own implementation.

Do not add a new Redis Lua script. Reuse the existing `WRITE_MESSAGE` script.
That script already owns the atomic duplicate check, body write, all-ID index
write, pending-queue index write, and queue-set registration.

Edit `extensions/simplebroker_redis/simplebroker_redis/core.py`.

Add `import_message(...)` near `write(...)`.

Reuse:

- `_check_fork_safety`
- `_validate_queue_name`
- `_validate_message_size`
- `_assert_no_reentrant_mutation_during_batch`
- `refresh_last_timestamp()`
- existing ID encoding helpers
- `scripts.WRITE_MESSAGE`
- `_publish(queue)` after success
- existing Redis error translation
- `IntegrityError` from `simplebroker._exceptions` for duplicate IDs

Implementation shape:

```python
current_last_ts = self.refresh_last_timestamp()
if normalized_id >= current_last_ts:
    raise ValueError("imported message_id must be lower than current last_ts")
encoded = encode_id(normalized_id)
result = self._client.eval(
    scripts.WRITE_MESSAGE,
    5,
    self._keys.meta,
    self._keys.bodies,
    self._keys.all_ids,
    self._keys.pending(queue),
    self._keys.queues,
    queue,
    encoded,
    message,
)
```

Translate `WRITE_MESSAGE` results as follows:

- `1`: publish and return `None`.
- `-1`: raise `IntegrityError`.
- `-2`: raise `OperationalError`.

Do not implement Redis import as a new multi-command write. One preflight read
of `last_ts` followed by the existing atomic `WRITE_MESSAGE` script is thin and
correct because `last_ts` only increases.

### Task 8: Check Postgres Support Without Adding a Postgres-Specific Method

Postgres should work through `BrokerCore.import_message` because the Postgres
plugin provides a backend SQL namespace with `INSERT_MESSAGE`.

Verify that `extensions/simplebroker_pg/simplebroker_pg/_sql.py` `INSERT_MESSAGE`
still sends the existing `pg_notify`. Do not add a second Postgres insert
string unless a red test proves the existing one cannot work.

If Postgres needs additional locking, prefer using existing plugin hooks only if
there is a clear reason. Do not add advisory locks by default. The uniqueness
constraint and transaction are the important correctness boundaries for this
API.

### Task 9: Update Public Docs

Edit `README.md`.

Add a short section near `Generating timestamps without writing`, probably
after that section and before `Tracking the last generated timestamp`.

Title suggestion:

```markdown
### Importing messages with existing IDs
```

Document:

- This is a restore/import API.
- Normal producers should use `write`.
- `message_id` must be lower than current `last_ts`.
- Use `refresh_last_timestamp()` on a broker handle, or `refresh_last_ts()` on a
  Queue, to inspect the current high-water mark before importing.
- No CLI surface exists for this.

Include a concise example:

```python
from simplebroker import open_broker

records = [
    ("tasks", "restore one", 1837025672140161024),
    ("tasks", "restore two", 1837025672140162048),
]

with open_broker("/path/to/.broker.db") as broker:
    current_last_ts = broker.refresh_last_timestamp()
    if max(message_id for _, _, message_id in records) >= current_last_ts:
        raise RuntimeError("import IDs are not historical for this broker")
    for queue, body, message_id in records:
        broker.import_message(queue, body, message_id=message_id)
```

Keep the docs small. Do not document Weft in the SimpleBroker README.

### Task 10: Update Changelog

Edit `CHANGELOG.md`.

Add one bullet under the unreleased/current top section:

```markdown
- Added API-only `import_message(...)` on broker handles and `Queue` for
  restoring pending messages with exact historical message IDs. The supplied ID
  must be lower than the broker's current `last_ts`.
```

Do not mention CLI.

### Task 11: Export Nothing New From `simplebroker/__init__.py`

Do not edit `simplebroker/__init__.py` unless tests prove it is necessary.

The public API is a method on existing exported objects (`Queue` and broker
handles returned by `open_broker`). There is no new top-level function to export.

### Task 12: Type and Style Cleanup

Run ruff on touched Python files:

```bash
uv run ruff check simplebroker tests extensions/simplebroker_redis
```

Run mypy if the branch is otherwise clean enough:

```bash
uv run mypy simplebroker
```

Do not fix unrelated style or typing problems outside touched files.

## Test Plan

Use red-green TDD. Do not implement all code and then write tests.

### Local SQLite Gate

Run the new tests without xdist while iterating:

```bash
uv run pytest -n 0 tests/test_import_message.py
```

Then run the related API tests:

```bash
uv run pytest -n 0 tests/test_import_message.py tests/test_queue_api_additions.py
```

Then run the default suite if time allows:

```bash
uv run pytest
```

### Postgres Gate

If Postgres test dependencies and `SIMPLEBROKER_PG_TEST_DSN` are available:

```bash
BROKER_TEST_BACKEND=postgres uv run pytest -n 0 tests/test_import_message.py
```

Also run the Postgres extension tests that cover write/notify behavior:

```bash
BROKER_TEST_BACKEND=postgres uv run pytest -n 0 extensions/simplebroker_pg/tests/test_pg_notify.py
```

If no Postgres test DSN is available, say that explicitly in the final handoff.

### Redis Gate

If Redis/Valkey test dependencies and `SIMPLEBROKER_VALKEY_TEST_URL` or
`SIMPLEBROKER_REDIS_TEST_URL` are available:

```bash
BROKER_TEST_BACKEND=redis uv run pytest -n 0 tests/test_import_message.py
```

Also run the Redis integration tests that cover queue storage and activity:

```bash
BROKER_TEST_BACKEND=redis uv run pytest -n 0 extensions/simplebroker_redis/tests/test_redis_integration.py
```

If no Redis test URL is available, say that explicitly in the final handoff.

### Invariant Checklist

Before considering the implementation done, verify these invariants with real
tests:

- `import_message` writes a pending message with exactly the supplied ID.
- `import_message` does not call `generate_timestamp`.
- `import_message` does not advance `last_ts`.
- `import_message` rejects `message_id == current last_ts`.
- `import_message` rejects `message_id > current last_ts`.
- Normal `write` after import still generates an ID above the observed
  pre-import `last_ts`.
- Duplicate message IDs fail and do not overwrite existing messages.
- Queue validation still runs.
- Message-size validation still runs or is visibly reused.
- Same-thread mutation during an active at-least-once generator batch is still
  blocked.
- Redis direct backend enforces the `last_ts` relation with a conservative
  preflight read, then reuses the existing atomic `WRITE_MESSAGE` script for
  storage.
- Postgres import sends the same wake-up notification as normal write by
  reusing `INSERT_MESSAGE`.
- No SimpleBroker CLI files changed for this API.

### "Do Not Over-Mock" Test Guidance

Do not write tests that patch `_do_write_transaction`,
`TimestampGenerator.generate`, `_backend_plugin.read_last_ts`, or Redis client
methods just to prove a branch was called.

Prefer black-box checks:

- Insert through `import_message`.
- Read through `peek_one`, `peek_many`, or `Queue.peek`.
- Inspect `last_ts` through `get_cached_last_timestamp`,
  `refresh_last_timestamp`, or `Queue.last_ts` only when needed.
- Trigger duplicate behavior with a real duplicate ID.
- Trigger validation with real invalid input.

A small unit test with a fake may be acceptable only if it protects a protocol
or type-shape regression that cannot be hit through the public API. Start with
real broker tests.

## Error Messages

Keep messages boring and searchable.

For the high-water invariant, use one message family everywhere:

```text
imported message_id must be lower than current last_ts
```

It is fine to include values if useful:

```text
imported message_id must be lower than current last_ts: message_id=..., last_ts=...
```

Do not mention Weft or task IDs in SimpleBroker exceptions.

## Review Checklist For The Implementer

Before asking for review, run this manual checklist:

- The public API name is `import_message` everywhere.
- `write(...)` behavior is unchanged.
- `generate_timestamp()` behavior is unchanged.
- `Queue.write(...)` behavior is unchanged.
- No CLI parser or command file was modified.
- SQL import reuses `_sql.INSERT_MESSAGE`.
- Redis import reuses `scripts.WRITE_MESSAGE`; no new Lua script was added.
- There is no batch import API.
- There is no conflict policy parameter.
- There is no arbitrary `last_ts` setter.
- Tests use `broker` and `queue_factory` fixtures instead of mocks.
- The README tells callers to read the current high-water mark before import.
- The changelog says API-only.

## Downstream Migration Notes

This section is not part of the SimpleBroker implementation, but it explains
how downstream users should consume the API.

For a dump/load tool:

1. Parse all message records.
2. Compute `max_message_id`.
3. Open the destination broker.
4. Read the destination broker's current `last_ts` with
   `refresh_last_timestamp()` or the equivalent Queue API.
5. Fail before writes if `max_message_id >= current_last_ts`.
6. Import each record with `import_message`.
7. If any import fails, roll back using the caller's existing restore strategy.

Do not put batch rollback into SimpleBroker for this change. Weft already owns
snapshot/rollback behavior for file-backed load. Other callers can make their
own transaction or snapshot decisions.

For Weft live spawn requests:

1. If Weft wants to use `import_message`, it must ensure `tid < current last_ts`
   before calling it.
2. A just-generated `tid` is equal to current `last_ts`, so it is not importable
   through this API.
3. Do not generate an extra timestamp just to make this API fit live spawn
   writes. If live preallocated writes matter, keep the private workaround for
   now or design a separate reserved-ID API later.

## Self-Review

### Review Pass 1: Does This Drift From The Requested API?

No. The plan stays API-only and does not add CLI, batch import, upsert, or
high-water mark mutation. It adds a single broker method and a single Queue
wrapper.

The only extra work is Redis support because Redis is a direct backend and
would otherwise violate the public `BrokerConnection` contract.

### Review Pass 2: Is The Strict `< last_ts` Invariant Implementable?

Yes, but it has a real consequence: a fresh broker with `last_ts == 0` cannot
import ordinary nonzero historical IDs through this API.

That consequence is documented. The plan does not hide it by making
`import_message` call `generate_timestamp()` internally, and it does not tell
callers to prime `last_ts` just for import. Callers should read `last_ts`, then
import only if the IDs are already below it.

### Review Pass 3: Is There A Race Around `last_ts`?

For SQL backends, checking `last_ts` before delegating to the existing insert
helper is sufficient for the invariant. If `message_id < observed last_ts`, any
concurrent timestamp generation can only move `last_ts` higher, so the relation
remains true.

If `message_id >= observed last_ts`, the implementation rejects immediately. A
concurrent future writer might have made the import valid later, but rejecting
is conservative and keeps the API simple. Callers can retry after observing a
later `last_ts`.

For Redis, the same monotonic argument applies. The plan reads `last_ts`
first, then reuses the existing atomic `WRITE_MESSAGE` Lua script for storage
and duplicate protection. No new Lua script is needed.

### Review Pass 4: Is Reusing `INSERT_MESSAGE` Correct?

Yes. SQLite `INSERT_MESSAGE` is the canonical insert. Postgres `INSERT_MESSAGE`
also performs the existing `pg_notify`, so reusing it preserves watcher behavior.

The plan explicitly avoids duplicating insert SQL in `db.py`.

### Review Pass 5: Are The Tests Too Mocked?

No. The planned tests use real broker and queue fixtures. They assert behavior
through public read/peek APIs and real duplicate constraints.

The plan warns against patching internals.

### Review Pass 6: Is The Plan Too Large?

The implementation is small. The plan is detailed because the API is sharp and
the implementer is assumed to lack context. The scope remains bounded:

- one broker method
- one Queue method
- one protocol addition
- one thin Redis direct-backend method that reuses the existing write script
- docs and changelog
- shared behavior tests

No material direction change is needed.
