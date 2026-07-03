# simplebroker-redis Implementation Plan

## Purpose

Build `simplebroker-redis` as a separate backend extension package that preserves
SimpleBroker's public Python API and CLI behavior while using the Valkey/Redis
7.x command surface honestly.

The implementation should follow the same product and packaging pattern as
`simplebroker-pg`:

- core `simplebroker` remains SQLite-first
- Redis support lives in `extensions/simplebroker_redis/`
- the extension is discovered through the `simplebroker.backends` entry point
- project config and env-selected backend flows work through the same core CLI
- explicit Python usage works with `Queue(..., runner=RedisRunner(...))`
- the compatibility target is Valkey 7.x and Redis 7.x
- the required integration test target is Valkey, not Redis

The important difference from Postgres: Redis is not SQL. Do not build a fake
`SQLRunner` that pattern-matches SQL strings. Add the smallest direct-backend
core seam needed, then keep behavior native to the Valkey/Redis data model.

Assume the implementer is a skilled developer with no context for this repo and
weak test instincts. Follow this plan in order. Keep each task bite-sized. Use
red-green TDD. Prefer real Valkey integration tests over mocks.

## Locked Decisions

These are decisions, not open questions. Do not revisit them while implementing.

### Product Boundary

`simplebroker` stays SQLite-first.

- Do not add Redis runtime code under `simplebroker/_backends/`.
- Do not add a runtime Redis dependency to the core `simplebroker` package.
- Put Redis code in `extensions/simplebroker_redis/`.
- Publish it as a separate package named `simplebroker-redis`.
- Add a convenience extra in core only after the extension package exists and
  tests pass.
- Keep the public backend name `redis`. Valkey speaks the same wire protocol for
  the command subset used here, so the user-facing backend stays
  `backend = "redis"`.

### API Boundary

Preserve the external API.

- `Queue`, `QueueWatcher`, `QueueMoveWatcher`, and CLI commands keep their
  current call shapes.
- Do not add a public `backend=` argument to `Queue`.
- Do not reinterpret `--file` or `--dir` as Redis target flags.
- Do support `.broker.toml` with `backend = "redis"`.
- Do support env-selected backend usage through `BROKER_BACKEND=redis`.
- Do support explicit Python usage with `RedisRunner` the same way PG supports
  `PostgresRunner`.

### Redis Design

Use Valkey/Redis-native structures. Do not use SQL emulation.

- Do not implement `RedisRunner.run(sql, ...)` as a SQL dispatcher.
- Do not import or reuse SQLite SQL constants for Redis behavior.
- Do not use Redis Streams as the primary storage model in v1.
- Do not use Redis 8.x-only commands.
- Use Lua scripts for multi-key atomic operations.
- Use sorted sets as lexicographic indexes, not numeric timestamp scores.
- Use Pub/Sub only as watcher wake-up hints. Durable state lives in normal keys.

### Semantics

The Valkey/Redis backend must match SimpleBroker's externally visible behavior
unless this plan explicitly calls out a documented backend tradeoff.

Required:

- FIFO within a queue.
- Message IDs are the existing 64-bit hybrid timestamp integers.
- `read` / `claim` marks messages claimed before returning.
- `peek` never mutates message state.
- `move` is atomic from source queue to destination queue.
- `move(..., message_id=...)` can move claimed messages when
  `require_unclaimed=False`.
- `delete`, `status`, queue stats, aliases, broadcast, vacuum, and cleanup have
  real Valkey/Redis behavior.
- Watchers can use Redis Pub/Sub wakeups but must still verify pending messages
  after wakeup.

Valkey/Redis-specific tradeoff:

- SQLite and PG can keep an at-least-once generator batch inside an open
  database transaction. Valkey/Redis cannot keep a client-side iterator inside
  a server-side transaction. The backend must emulate this with a temporary
  batch reservation key plus explicit commit/rollback scripts. If the process
  dies mid-batch, recovery is delayed until stale batch cleanup runs. This must
  be documented and tested.

### Dependency Lock

Core keeps zero runtime dependencies.

- `pyproject.toml` at repo root must still have `dependencies = []`.
- Redis client dependencies belong only to
  `extensions/simplebroker_redis/pyproject.toml`.
- Use the official Python client package, `redis`.
- Do not add `rq`, `dramatiq`, `celery`, SQLAlchemy, or any queue framework.

Initial dependency recommendation:

```toml
dependencies = [
    "simplebroker>=3.5.0,<4",
    "redis>=5,<7",
]
```

This is a Python client version bound, not a server version bound. Server
compatibility is Valkey 7.x and Redis 7.x. Do not rely on newer server commands
without revising this plan.

## Repository Primer

Read these files before changing code:

- `pyproject.toml`
- `simplebroker/_backend_plugins.py`
- `simplebroker/ext.py`
- `simplebroker/_targets.py`
- `simplebroker/_project_config.py`
- `simplebroker/_broker_session.py`
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `simplebroker/watcher.py`
- `simplebroker/_timestamp.py`
- `simplebroker/_runner.py`
- `simplebroker/_sql/_contract.py`
- `simplebroker/_backends/sqlite/plugin.py`
- `extensions/simplebroker_pg/pyproject.toml`
- `extensions/simplebroker_pg/README.md`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `extensions/simplebroker_pg/simplebroker_pg/schema.py`
- `extensions/simplebroker_pg/simplebroker_pg/validation.py`
- `extensions/simplebroker_pg/tests/conftest.py`
- `extensions/simplebroker_pg/tests/test_pg_integration.py`
- `extensions/simplebroker_pg/tests/test_pg_notify.py`
- `tests/conftest.py`
- `tests/helper_scripts/broker_factory.py`
- `tests/test_backend_plugin_resolution.py`
- `tests/test_generator_methods.py`
- `tests/test_exactly_once_delivery.py`
- `tests/test_queue_api_comprehensive.py`
- `tests/test_aliases_db.py`
- `tests/test_activity_waiter_api.py`

Useful external references:

- Valkey/Redis `ZRANGE BYLEX`: https://redis.io/docs/latest/commands/zrange/
- Valkey/Redis sorted-set lexicographic indexing: https://redis.io/docs/latest/develop/data-types/sorted-sets/
- Valkey/Redis Lua `EVAL`: https://redis.io/docs/latest/commands/eval/
- redis-py pipelines and transactions: https://redis.io/docs/latest/develop/clients/redis-py/transpipe/
- redis-py client guide: https://redis.io/docs/latest/integrate/redis-py/
- Valkey/Redis `PUBLISH`: https://redis.io/docs/latest/commands/publish/
- Redis Streams, for why v1 is not stream-backed: https://redis.io/docs/latest/develop/data-types/streams/

## Current Architecture Facts

The current core path is:

```text
Queue / CLI / Watcher
  -> DBConnection
  -> BrokerCore or BrokerDB
  -> backend plugin
  -> SQLRunner
  -> backend SQL namespace
```

That works for SQLite and Postgres because both can be represented by SQL plus a
few backend hooks.

Valkey/Redis does not fit this shape. The bad implementation is a `RedisRunner`
that accepts SQLite/PG SQL text and maps a few known strings to Redis commands.
That would be brittle, hard to test, and wrong at the abstraction boundary.

The right shape is:

```text
Queue / CLI / Watcher
  -> DBConnection
  -> BrokerLike protocol
       -> BrokerCore / BrokerDB for SQL backends
       -> RedisBrokerCore for Redis
```

The public API does not change. Only the internal core seam gets wider.

## Target Valkey/Redis Data Model

Use a namespace prefix for every key. The namespace is the Valkey/Redis
equivalent of the PG schema ownership boundary.

Example namespace:

```text
simplebroker:app_v1
```

Validate namespace names strictly:

- non-empty string
- max 128 characters
- allowed characters: `A-Z`, `a-z`, `0-9`, `_`, `-`, `.`, `:`
- no whitespace
- no glob characters: `*`, `?`, `[`, `]`
- no Redis Cluster hash-tag braces: `{`, `}`

Key layout:

```text
{prefix}:meta
{prefix}:queues
{prefix}:aliases
{prefix}:bodies
{prefix}:q:{queue}:pending
{prefix}:q:{queue}:claimed
{prefix}:q:{queue}:reserved
{prefix}:batches:{token}:ids
{prefix}:batches:{token}:meta
{prefix}:activity
```

Where:

- `meta` is a hash with fields:
  - `magic`
  - `schema_version`
  - `last_ts`
  - `alias_version`
- `queues` is a set of queue names with at least one pending or claimed message.
- `aliases` is a hash mapping alias name to target queue.
- `bodies` is a hash mapping timestamp string to message body.
- each queue has three sorted sets:
  - `pending`: unclaimed message IDs
  - `claimed`: claimed message IDs awaiting vacuum or direct move/delete
  - `reserved`: IDs temporarily held by open at-least-once generator batches
- batch keys hold temporary at-least-once generator reservations.
- `activity` is a Pub/Sub channel, not durable state.

### Why Not Sorted-Set Numeric Scores?

Valkey/Redis sorted-set scores are floating-point values. SimpleBroker message
IDs are 64-bit integers around `1.8e18`, which are not safe to store as exact
sorted-set scores.

Use lexicographic ordering instead:

- Store every message ID as a fixed-width 19-digit decimal string.
- Add all members to sorted sets with score `0`.
- Query with lex ranges.
- Numeric order matches lexicographic order for fixed-width decimal IDs.
- Encode with `f"{timestamp:019d}"`.
- Decode with `int(encoded)`.
- Reject encoded IDs longer than 19 digits. That would mean the core timestamp
  contract changed and Redis ordering needs a new representation.

Lex bounds:

- no lower bound: `-`
- no upper bound: `+`
- after timestamp: `({encoded_after}`
- before timestamp: `({encoded_before}`
- exact timestamp: direct member lookup, not a range scan

Do not use `ZPOPMIN` for timestamp ordering. It is tempting, but it leans on
score ordering and hides the precision trap.

## Target Valkey/Redis Operation Semantics

Implement multi-key mutations as Lua scripts. The scripts are the Valkey/Redis
equivalent of the SQL transactions in `BrokerCore`.

### Timestamp Storage

Reuse `simplebroker._timestamp.TimestampGenerator`; do not copy the hybrid clock
algorithm.

Implementation pattern:

- `RedisRunner` is a resource handle with:
  - Redis connection pool/client
  - namespace
  - backend plugin
  - lifecycle methods
- `RedisBackendPlugin.read_last_ts(runner)` reads `HGET meta last_ts`.
- `RedisBackendPlugin.advance_last_ts(runner, new_ts=...)` runs a Lua compare
  and set:
  - read current `last_ts`
  - if current `< new_ts`, write `new_ts` and return true
  - otherwise return false
- `RedisBrokerCore` creates `TimestampGenerator(redis_runner, backend_plugin=...)`.

This keeps timestamp behavior shared and avoids a forked timestamp algorithm.

### Write

Atomic Lua script:

- validate meta exists and magic matches
- `HSET bodies id body`
- `ZADD q:{queue}:pending 0 id`
- `SADD queues queue`
- `PUBLISH activity queue`

Python side still validates queue name and message size using the same helpers
or equivalent code path as `BrokerCore`.

### Peek

Lua is optional for read-only peek. Use direct Redis calls unless Lua makes the
code simpler.

Rules:

- exact timestamp:
  - check `ZSCORE pending id`
  - if present, return body and timestamp
  - do not return claimed rows unless the existing API explicitly asks for
    claimed rows; it currently does not for peek
- range:
  - use lex bounds over `pending`
  - apply `after_timestamp` and `before_timestamp`
  - return bodies in FIFO order
- pagination:
  - `peek_generator` uses offset-based lex ranges like SQL does today
  - this is acceptable because peek is non-mutating

### Claim / Read

Atomic Lua script:

- select IDs from `pending` by lex range
- skip IDs present in the same queue's `reserved` zset
- for each ID:
  - remove from `pending`
  - add to `claimed`
  - read body
- leave queue in `queues` if `claimed` or `pending` still has entries
- return `(body, timestamp)` pairs

This gives the existing exactly-once behavior: once the call returns, the
message is no longer pending.

### At-Least-Once Generator Batches

Implement this. Do not punt it to exactly-once one-by-one behavior, because the
shared suite tests rollback of unfinished batches.

Claim generator flow:

1. `begin_claim_batch` Lua script:
   - create a unique token with `uuid.uuid4().hex`
   - select up to `batch_size` IDs from source `pending`, skipping IDs already
     present in source `reserved`
   - add selected IDs to source `reserved`
   - add selected IDs to `batches:{token}:ids`
   - write `batches:{token}:meta` with:
     - `op = "claim"`
     - `source = queue`
     - `created_ns = time.time_ns()`
   - return body/id pairs
2. Python yields each item from that in-memory batch.
3. If the whole batch is yielded, `commit_claim_batch` moves IDs from batch to
   source `claimed`, removes them from source `pending` and source `reserved`,
   deletes batch keys, and publishes no queue activity.
4. If the generator is closed or raises before the batch completes,
   `rollback_claim_batch` removes IDs from source `reserved`, deletes batch
   keys, and preserves FIFO order because the IDs never left `pending`.

Move generator flow is analogous:

- `begin_move_batch` reserves IDs from source `pending` by adding them to source
  `reserved`
- commit removes IDs from source `pending` and source `reserved`, adds them to
  target `pending`, updates queue sets, and publishes target
- rollback removes IDs from source `reserved`

Why this shape matters:

- Pending messages remain visible to `peek` and stats while a batch is open,
  which is closer to SQL transaction isolation.
- Other claim/move consumers still skip reserved IDs, so they do not double
  deliver the active batch.
- `has_pending_messages` should look for at least one unreserved pending ID,
  not just `ZCARD pending > 0`, otherwise watchers can spin while a batch is
  open.
- Destructive queue-wide mutations must not trample active reservations. `delete`
  for a queue, all-queue delete, and cleanup should recover stale batches first;
  if non-stale reserved IDs remain, fail with `OperationalError` rather than
  corrupting an active generator.

Re-entrant mutation behavior:

- Match `BrokerCore` by tracking active same-thread at-least-once batch state.
- Same-thread mutating calls during an active batch must raise a `RuntimeError`
  with the same message shape used by current tests:
  `separate BrokerDB/Queue instance`
- Read-only re-entry such as `peek_one` must not deadlock.

Stale recovery:

- Add `recover_stale_batches(max_age_seconds)` in `RedisBrokerCore`.
- Run it during core initialization and before starting a new at-least-once
  batch.
- It scans batch meta keys for this namespace only.
- For stale claim/move batches, it removes IDs from the source `reserved` zset
  and deletes the batch keys.
- Default stale age: 300 seconds.
- Add `BROKER_REDIS_STALE_BATCH_SECONDS` as a Redis-extension option, not a core
  config key.

This is the main Valkey/Redis tradeoff. Document it in the extension README.

### Move

Atomic Lua script:

- exact timestamp:
  - if `require_unclaimed=True`, source must contain ID in `pending`
    and not in `reserved`
  - if `require_unclaimed=False`, source may contain ID in `pending` or `claimed`,
    but not in `reserved`
- non-exact:
  - select IDs only from source `pending`, skipping `reserved`
- remove selected IDs from source `pending`, `claimed`, and `reserved`
- add IDs to target `pending`
- add target to `queues`
- remove source from `queues` only if source pending and claimed zsets are both empty
- publish target queue if at least one message moved
- return body/id pairs

### Delete

Queue delete:

- recover stale batches first
- if the queue still has non-stale reserved IDs, raise `OperationalError`
- remove all IDs in source `pending` and `claimed` from `bodies`
- delete pending, claimed, and reserved zsets
- remove queue from `queues`
- return count

Message delete by ID is already implemented by `Queue.delete(message_id=...)`
as `claim_one(exact_timestamp=...)`. That means it only deletes pending
messages, matching current behavior. Do not invent a new public delete-by-ID
semantics in this plan.

All-queue delete:

- recover stale batches first
- if any queue still has non-stale reserved IDs, raise `OperationalError`
- iterate every queue in `queues`
- remove all pending and claimed IDs from `bodies`
- delete queue pending, claimed, and reserved zsets
- clear `queues`
- preserve `meta` and `aliases`

### Vacuum

Valkey/Redis vacuum removes claimed messages.

- For each queue, remove up to `BROKER_VACUUM_BATCH_SIZE` IDs from `claimed`.
- Delete corresponding bodies.
- Remove empty queues from `queues`.
- Repeat until no claimed IDs remain or until the configured batch loop completes.
- `compact=True` is a no-op for Valkey/Redis, but should still run the claimed
  cleanup.
- Do not call `FLUSHDB`.

### Status and Metadata

`status()` returns:

- `total_messages`: pending plus claimed across all queues
- `last_timestamp`: `meta.last_ts`
- `db_size`: `0` in v1

Do not implement namespace memory scanning in v1. A future version can add a
bounded `MEMORY USAGE` scan behind a helper, but that is outside this plan.

`get_meta()` returns the same logical fields as SQLite/PG:

- `magic`
- `schema_version`
- `last_ts`
- `alias_version`

### Aliases

Use the `aliases` hash.

- `resolve_alias`: `HGET`
- `list_aliases`: `HGETALL`, sorted in Python
- `aliases_for_target`: scan hash values in Python and sort aliases
- `add_alias` and `remove_alias`: Lua or Redis transaction

Do not over-optimize reverse alias lookups in v1. Alias counts should be small.

### Broadcast

Required behavior: broadcast sends one new message to each queue that exists at
the broadcast snapshot.

Implementation:

- get queue snapshot from `SMEMBERS queues`
- filter `pattern` in Python with `fnmatchcase`, same as core
- generate one timestamp per selected queue
- run one Lua script to insert all queue/message/timestamp triples atomically
- publish `*` or publish each queue

Prefer publishing `*` for broadcast. Activity waiters must treat `*` as a
wildcard wakeup, just like the PG waiter.

Potential race:

- A queue can be deleted between Python's snapshot and the broadcast Lua script.
- The script must check `SISMEMBER queues queue` before inserting.
- This may skip a queue deleted after the snapshot. That is acceptable for v1
  and should be documented as the closest practical equivalent to the SQL
  transaction lock.

Do not add a global Valkey/Redis lock for broadcast in v1. It would reduce
throughput and is not needed for the existing API.

### Activity Waiters

Use Valkey/Redis Pub/Sub as wake-up hints.

Single queue waiter:

- subscribe to `{prefix}:activity`
- maintain a process-local listener thread per Valkey/Redis URL plus namespace
- wake only when payload is the queue name or `*`
- ignore unrelated queues

Multi-queue waiter:

- same listener
- wake when payload is any watched queue name or `*`

Rules:

- Pub/Sub notifications are hints only.
- Watchers must still call `has_pending_messages`.
- Missed Pub/Sub notifications must not lose data. Polling still catches work.
- `close()` must unregister queue state and release listener references.

## Task 1: Add A Direct Backend Core Protocol

Files to touch:

- `simplebroker/_backend_plugins.py`
- `simplebroker/ext.py`
- `simplebroker/db.py`
- `simplebroker/_broker_session.py`
- `simplebroker/sbqueue.py` only if type hints need a shared alias
- `tests/test_backend_plugin_resolution.py`
- `tests/test_process_broker_session.py`

Goal:

Add an internal protocol that lets a backend return an object implementing the
same methods `Queue`, commands, and watchers already call on `BrokerCore`.

Implementation sketch:

1. Add a `BrokerConnection` protocol in `simplebroker/_backend_plugins.py` or a
   new `simplebroker/_broker_protocol.py`.
2. Include the methods actually used by `Queue`, `commands`, and `watcher`:
   - `set_stop_event`
   - `generate_timestamp`
   - `get_cached_last_timestamp`
   - `refresh_last_timestamp`
   - `write`
   - `claim_one`
   - `claim_many`
   - `claim_generator`
   - `peek_one`
   - `peek_many`
   - `peek_generator`
   - `move_one`
   - `move_many`
   - `move_generator`
   - `delete`
   - `broadcast`
   - `list_queues`
   - `get_queue_stats`
   - `queue_exists`
   - `queue_exists_and_has_messages`
   - `get_queue_stat`
   - `list_queue_stats`
   - `get_overall_stats`
   - `count_claimed_messages`
   - `status`
   - `has_pending_messages`
   - `get_data_version`
   - `vacuum`
   - `get_alias_version`
   - `resolve_alias`
   - `canonicalize_queue`
   - `has_alias`
   - `list_aliases`
   - `aliases_for_target`
   - `add_alias`
   - `remove_alias`
   - `get_meta`
   - `close`
   - `shutdown`
3. Add optional plugin methods:
   - `create_core(target, backend_options=None, config=None, stop_event=None)`
   - `create_core_from_runner(runner, config=None, stop_event=None)`
4. In `DBConnection._create_managed_connection`, if a non-SQL plugin exposes
   `create_core`, call it instead of `create_runner` plus `BrokerCore`.
5. In `DBConnection.get_core`, support the same direct-core path.
6. In external runner mode, if the runner's plugin exposes
   `create_core_from_runner`, use it instead of wrapping the runner in
   `BrokerCore`.
7. In `_ProcessBrokerSession._create_core`, support direct-core plugins.
8. Keep SQLite and PG paths unchanged.

Testing:

- Add a dummy direct backend plugin in `tests/test_backend_plugin_resolution.py`.
- Assert `Queue("jobs", db_path=ResolvedTarget("dummy_direct", ...))` uses the
  direct core and can write/read with no SQL runner calls.
- Assert explicit runner mode can use `Queue(..., runner=DummyDirectRunner(...))`.
- Assert persistent queues for the same direct target can be closed without
  leaking or double-closing.

Red-green gate:

```bash
uv run pytest tests/test_backend_plugin_resolution.py tests/test_process_broker_session.py -q
uv run pytest tests/test_queue_connection_manager.py -q
```

Do not start the Valkey/Redis implementation until this seam is green.

## Task 2: Scaffold `simplebroker-redis`

Files to add:

- `extensions/simplebroker_redis/pyproject.toml`
- `extensions/simplebroker_redis/LICENSE`
- `extensions/simplebroker_redis/README.md`
- `extensions/simplebroker_redis/simplebroker_redis/__init__.py`
- `extensions/simplebroker_redis/simplebroker_redis/_constants.py`
- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_redis/simplebroker_redis/runner.py`
- `extensions/simplebroker_redis/simplebroker_redis/validation.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
- `extensions/simplebroker_redis/tests/conftest.py`

Files to modify:

- root `pyproject.toml`

Package shape should mirror `extensions/simplebroker_pg`.

`pyproject.toml` entry point:

```toml
[project.entry-points."simplebroker.backends"]
redis = "simplebroker_redis.plugin:get_backend_plugin"
```

Root convenience extra:

```toml
[project.optional-dependencies]
redis = [
    "simplebroker-redis>=0.1.0,<1",
]

[tool.uv.sources]
simplebroker-redis = { path = "./extensions/simplebroker_redis", editable = true }
```

Do not add the Redis client to the core `dev` dependency list. Install the
extension editable in local Valkey test setup.

Testing:

```bash
uv pip install -e "./extensions/simplebroker_redis[dev]"
uv run python -c "from simplebroker_redis import RedisRunner, get_backend_plugin; print(get_backend_plugin().name)"
```

## Task 3: Implement Valkey/Redis Config Parsing And Ownership Validation

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_redis/simplebroker_redis/validation.py`
- `extensions/simplebroker_redis/simplebroker_redis/_constants.py`
- `extensions/simplebroker_redis/tests/test_redis_init_backend.py`
- `extensions/simplebroker_redis/tests/test_redis_ownership.py`

Config inputs:

- `BROKER_BACKEND_TARGET`: full Valkey/Redis URL, e.g. `redis://127.0.0.1:6379/0`
- `BROKER_BACKEND_HOST`
- `BROKER_BACKEND_PORT`
- `BROKER_BACKEND_PASSWORD`
- `BROKER_BACKEND_DATABASE`: Valkey/Redis DB number, not a SQL database name
- `BROKER_BACKEND_SCHEMA`: use as default namespace for env-selected backend
- TOML `target`: Valkey/Redis URL
- TOML `[backend_options].namespace`: namespace, preferred over schema wording

Do not add Valkey/Redis-specific keys to core `_constants.py` unless absolutely
needed. Parse backend-specific options in the extension from `backend_options`.
The core default `BROKER_BACKEND_DATABASE` value is `"simplebroker"` because it
was introduced for Postgres. This backend must not use that as a DB number. If
the backend is selected and no target URL or database number is supplied, default
to DB `0`.

`plugin.init_backend(...)` should return:

```python
{
    "target": "redis://...",
    "backend_options": {"namespace": "..."},
}
```

Validation states:

- `ABSENT`: no namespace keys
- `OWNED`: meta exists and `magic == SIMPLEBROKER_MAGIC`
- `FOREIGN`: keys exist under namespace but no valid SimpleBroker meta
- `PARTIAL_SIMPLEBROKER`: some SimpleBroker-looking keys exist but meta is bad

Rules:

- `initialize_target` may initialize `ABSENT` or `OWNED`.
- `validate_target(..., verify_initialized=False)` accepts `ABSENT` and `OWNED`,
  refuses `FOREIGN` and `PARTIAL_SIMPLEBROKER`.
- `validate_target(..., verify_initialized=True)` requires `OWNED`.
- `cleanup_target` deletes only keys under the configured namespace and only when
  state is `OWNED`.
- `cleanup_target` should recover stale batches before deleting. If non-stale
  active batch reservations remain, it should refuse cleanup with `DatabaseError`
  unless the caller explicitly added a future force option. Do not add that force
  option in v1.
- Never call `FLUSHDB`.
- Use `SCAN` plus batched `UNLINK` or `DEL`. Prefer `UNLINK` if available, fall
  back to `DEL`.

Tests must use a real Valkey 7.x server. They can skip if
`SIMPLEBROKER_VALKEY_TEST_URL` is unset. `SIMPLEBROKER_REDIS_TEST_URL` may be
accepted as a backwards-compatible alias, but the documented path is Valkey.

Test cases:

- `init_backend` builds URL from host/port/password/db.
- `target` overrides individual host/port/db parts.
- TOML namespace overrides env schema.
- invalid namespace is rejected.
- cleanup refuses foreign namespace.
- cleanup deletes only namespaced keys and leaves unrelated keys untouched.

## Task 4: Implement `RedisRunner`

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/runner.py`
- `extensions/simplebroker_redis/tests/test_redis_runner_lifecycle.py`

`RedisRunner` is a resource handle, not a SQL runner.

It should expose:

- `target`
- `namespace`
- `client`
- `backend_plugin`
- `stale_batch_seconds`
- `close()`
- `shutdown()`
- `release_thread_connection()` as a no-op for compatibility with
  `_ProcessBrokerSession`
- fork detection: recreate pool/client after `os.fork()`

Connection strategy:

- Use `redis.ConnectionPool.from_url(target, decode_responses=True)`.
- Create `redis.Redis(connection_pool=pool)`.
- `ping()` during validation or setup, not on every operation.
- Keep runner thread-safe by relying on redis-py's connection pool and
  Valkey/Redis atomic scripts.

If `run`, `begin_immediate`, `commit`, or `rollback` are present for structural
compatibility, they must raise a clear `NotImplementedError`:

```text
RedisRunner is not SQL-capable; use RedisBackendPlugin.create_core(...)
```

Tests:

- runner can `ping`
- runner carries namespace and stale batch recovery settings
- `close()` and `shutdown()` are idempotent
- fork detection does not reuse parent pool
- accidental `run("...")` fails clearly

## Task 5: Implement Valkey/Redis Lua Scripts

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
- `extensions/simplebroker_redis/tests/test_redis_scripts.py`

Keep scripts in Python string constants. Do not build Lua by string
concatenating untrusted queue names.

Required scripts:

- `INIT_NAMESPACE`
- `ADVANCE_LAST_TS`
- `WRITE_MESSAGE`
- `CLAIM_MESSAGES`
- `MOVE_MESSAGES`
- `DELETE_QUEUE`
- `DELETE_ALL_MESSAGES`
- `VACUUM_CLAIMED`
- `ADD_ALIAS`
- `REMOVE_ALIAS`
- `BROADCAST`
- `BEGIN_CLAIM_BATCH`
- `COMMIT_CLAIM_BATCH`
- `ROLLBACK_CLAIM_BATCH`
- `BEGIN_MOVE_BATCH`
- `COMMIT_MOVE_BATCH`
- `ROLLBACK_MOVE_BATCH`
- `RECOVER_STALE_BATCH`

Script conventions:

- Every script receives explicit keys through `KEYS`.
- Every script receives queue names and IDs through `ARGV`.
- Every script checks `meta.magic`.
- Return simple arrays that Python converts into typed results.
- Keep result shapes documented beside the script constant.

Unit tests should call scripts through a redis-py client connected to a real
Valkey server with a temporary namespace. Do not mock the server. A fake server
will not catch Lua, sorted-set, or Pub/Sub errors.

Specific invariants to test:

- fixed-width ID lex order matches numeric write order
- claimed messages are absent from pending reads
- reserved messages are skipped by claim/move but remain visible to peek until
  batch commit
- queue delete refuses to run over a non-stale active batch
- move preserves original message ID and body
- delete removes body hash entries
- vacuum removes only claimed messages
- namespace isolation holds with two namespaces in the same Valkey DB
- batch rollback replays unfinished messages at the correct position

## Task 6: Implement `RedisBrokerCore`

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_redis/tests/test_redis_integration.py`
- `extensions/simplebroker_redis/tests/test_redis_generator_semantics.py`
- `extensions/simplebroker_redis/tests/test_redis_queue_metadata.py`
- `extensions/simplebroker_redis/tests/test_redis_maintenance.py`

`RedisBrokerCore` should implement the `BrokerConnection` protocol from Task 1.

Code style:

- Keep public method names and signatures aligned with `BrokerCore`.
- Keep validation behavior aligned with `BrokerCore`.
- Use small private helpers for:
  - key construction
  - ID encoding/decoding
  - result row conversion
  - queue validation
  - message size validation
  - active batch state
- Do not subclass `BrokerCore`.
- Do not copy large chunks of SQL-oriented code.
- Duplicate small validation helpers if needed, but prefer importing stable
  helpers/constants from core.

Important methods:

- `generate_timestamp`
- `get_cached_last_timestamp`
- `refresh_last_timestamp`
- `write`
- `claim_one`
- `claim_many`
- `claim_generator`
- `peek_one`
- `peek_many`
- `peek_generator`
- `move_one`
- `move_many`
- `move_generator`
- `delete`
- `broadcast`
- `list_queues`
- `get_queue_stats`
- `list_queue_stats`
- `queue_exists`
- `get_queue_stat`
- `get_overall_stats`
- `count_claimed_messages`
- `status`
- `has_pending_messages`
- `vacuum`
- alias methods:
  - `get_alias_version`
  - `resolve_alias`
  - `canonicalize_queue`
  - `has_alias`
  - `list_aliases`
  - `aliases_for_target`
  - `add_alias`
  - `remove_alias`
- `get_meta`
- `close`
- `shutdown`

Testing strategy:

- Start with `write`, `peek_one`, `claim_one`.
- Then `peek_many`, `claim_many`, generators.
- Then `move`.
- Then metadata and aliases.
- Then vacuum and status.
- Then broadcast.

Run focused shared tests against Valkey after each slice once the test harness is
ready:

```bash
BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest tests/test_queue_api_comprehensive.py tests/test_generator_methods.py -m shared -q
```

## Task 7: Wire The Redis Plugin

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_redis/simplebroker_redis/__init__.py`
- `extensions/simplebroker_redis/tests/test_redis_integration.py`

Plugin requirements:

- `name = "redis"`
- `schema_version = REDIS_SCHEMA_VERSION`
- entry point returns singleton from `get_backend_plugin()`
- `init_backend` mirrors PG's config pattern
- `create_runner` returns `RedisRunner`
- `create_core` returns `RedisBrokerCore`
- `create_core_from_runner` returns `RedisBrokerCore`
- `initialize_target` initializes namespace
- `validate_target` validates namespace ownership
- `cleanup_target` removes only owned namespace keys
- `create_activity_waiter` returns Valkey/Redis waiter
- `create_activity_waiter_for_queues` returns Valkey/Redis multi-queue waiter

Because the existing `BackendPlugin` protocol requires SQL members, update the
core protocol so non-SQL direct plugins are accepted without a fake SQL
namespace. Do this carefully:

- `ensure_backend_sql_namespace(...)` should still run for SQL-backed plugins.
- Redis plugin should identify itself as direct/non-SQL.
- Existing tests that reject malformed SQL plugins must still pass.
- SQLite and PG should not need behavior changes.

Use this shape:

```python
class BackendPlugin(Protocol):
    name: str
    schema_version: int
    sql: BackendSQLNamespace | None
    is_direct_backend: bool = False
```

Then resolver logic:

- if `plugin.sql is not None`, validate SQL namespace
- if `plugin.sql is None`, require `create_core`

Do not silently accept a plugin with neither SQL nor direct core support.

## Task 8: Extend Backend-Agnostic Test Harness For Valkey

Files to touch:

- `tests/conftest.py`
- `tests/helper_scripts/broker_factory.py`
- `pyproject.toml` markers
- `bin/pytest-redis`

Add `BROKER_TEST_BACKEND=redis` support analogous to Postgres, but back it with
Valkey in the documented test workflow.
Add a `redis_only` pytest marker alongside the existing `pg_only` and
`sqlite_only` markers.

Test env:

```bash
export SIMPLEBROKER_VALKEY_TEST_URL="redis://127.0.0.1:6379/15"
export BROKER_TEST_BACKEND=redis
```

Harness behavior:

- create one namespace per xdist worker, e.g. `pytest_worker_gw0`
- write a `.broker.toml` with:

```toml
version = 1
backend = "redis"
target = "redis://127.0.0.1:6379/15"

[backend_options]
namespace = "pytest_worker_gw0"
```

- initialize namespace through plugin
- reset between tests by calling `cleanup_target` then `initialize_target`
- never call `FLUSHDB`
- make `make_target(..., backend="redis")` return `ResolvedTarget("redis", url,
  {"namespace": namespace})`

Update collection rules:

- Keep SQLite-only tests SQLite-only.
- Shared tests should run under Valkey through `BROKER_TEST_BACKEND=redis`
  unless they assert SQLite/PG internals.
- Add Redis-only extension tests under `extensions/simplebroker_redis/tests/`.

Initial shared test gate:

```bash
BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest -m shared -q
```

Expect failures at first. Fix behavior, not tests, unless the test asserts
SQLite-specific internals.

## Task 9: Implement Valkey/Redis Activity Waiters

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/runner.py`
- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_redis/tests/test_redis_notify.py`
- `tests/test_activity_waiter_api.py` if shared assumptions need broadening

Mirror the PG listener structure, but adapt to Valkey/Redis Pub/Sub.

Components:

- `_SharedRedisActivityListener`
- `_SharedRedisActivityRegistry`
- `RedisActivityWaiter`
- `RedisMultiQueueActivityWaiter`

Listener registry key:

```python
(os.getpid(), target_url_without_password_or_with_normalized_target, namespace)
```

Behavior:

- one daemon listener thread per target + namespace
- subscribe to `{prefix}:activity`
- payload is queue name or `*`
- maintain per-queue versions and fan-in versions like PG
- close is idempotent
- listener errors wake waiters and re-raise as `OperationalError`

Tests:

- single waiter ignores unrelated queue notifications
- single waiter wakes on matching write
- move wakes destination queue waiter
- broadcast wakes all via `*`
- multi-queue waiter wakes for any watched queue and ignores others
- closing waiters unregisters listener state
- listener close wakes blocked waiters
- listener error is surfaced

Use real Valkey. Do not mock Pub/Sub.

## Task 10: CLI And Project Config Integration

Files to touch:

- `extensions/simplebroker_redis/tests/test_redis_integration.py`
- `README.md`
- `extensions/simplebroker_redis/README.md`

Core project config should already work after plugin wiring because
`resolve_project_target` delegates non-SQLite target shaping to
`plugin.init_backend`.

Test these exact flows:

1. Explicit runner Python:

```python
from simplebroker import Queue
from simplebroker_redis import RedisRunner

runner = RedisRunner("redis://127.0.0.1:6379/15", namespace="simplebroker_app")
queue = Queue("jobs", runner=runner, persistent=True)
try:
    queue.write("hello")
    assert queue.read() == "hello"
finally:
    queue.close()
    runner.close()
```

2. Project config CLI:

```bash
broker init
broker write jobs hello
broker read jobs
broker --cleanup
```

3. Env-selected CLI:

```bash
BROKER_BACKEND=redis \
BROKER_BACKEND_TARGET=redis://127.0.0.1:6379/15 \
BROKER_BACKEND_SCHEMA=simplebroker_app \
broker init
```

4. Password URL handling:

- password in target URL
- password from `BROKER_BACKEND_PASSWORD`
- redacted display target in CLI status/error output

5. Missing Valkey/Redis server:

- fails as a connection/target error
- does not report "backend not available"

## Task 11: Documentation

Files to touch:

- `extensions/simplebroker_redis/README.md`
- root `README.md`
- `CHANGELOG.md` only when the implementation is landing

Extension README must include:

- installation:
  - `pipx inject simplebroker simplebroker-redis`
  - `uv add simplebroker-redis`
  - `uv add "simplebroker[redis]"`
- Python usage with `RedisRunner`
- `.broker.toml` usage
- env-selected usage
- namespace ownership and cleanup rules
- Pub/Sub wakeup caveat: hints only
- at-least-once generator caveat: stale batch recovery is delayed after process
  death
- why v1 does not use Redis Streams
- the compatibility target: Valkey 7.x and Redis 7.x
- the test target: Valkey 7.x

Do not over-promise durability:

- Valkey/Redis persistence depends on the server's RDB/AOF config.
- SimpleBroker cannot make an in-memory Valkey/Redis deployment durable.
- Say this plainly.

## Task 12: Full Verification Gates

Local Valkey setup:

```bash
docker run --rm -p 6379:6379 valkey/valkey:7.2
```

or use an existing Valkey 7.x and point:

```bash
export SIMPLEBROKER_VALKEY_TEST_URL="redis://127.0.0.1:6379/15"
```

Install:

```bash
uv sync --extra dev
uv pip install -e "./extensions/simplebroker_redis[dev]"
```

Core fast gates:

```bash
uv run pytest tests/test_backend_plugin_resolution.py -q
uv run pytest tests/test_process_broker_session.py -q
uv run pytest tests/test_activity_waiter_api.py -q
```

Valkey-backed extension gates:

```bash
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest extensions/simplebroker_redis/tests -q
```

Shared behavior gate:

```bash
BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest -m shared -q
```

SQLite regression gate:

```bash
uv run pytest -q
```

Postgres regression gate, if a PG DSN is available:

```bash
BROKER_TEST_BACKEND=postgres \
SIMPLEBROKER_PG_TEST_DSN="$SIMPLEBROKER_PG_TEST_DSN" \
uv run pytest -m shared -q

SIMPLEBROKER_PG_TEST_DSN="$SIMPLEBROKER_PG_TEST_DSN" \
uv run pytest extensions/simplebroker_pg/tests -q
```

Packaging gate:

```bash
uv run python -m build ./extensions/simplebroker_redis
uv run python -m build
```

No-dependency gate:

```bash
uv run pytest tests/test_no_dependencies.py -q
```

## Test Design Rules For This Work

Do not over-mock.

Good tests:

- call `Queue`, `BrokerCore`-like APIs, or the CLI
- use a real Valkey 7.x server
- assert visible behavior and key invariants
- isolate with namespaces
- clean up only the test namespace

Bad tests:

- mocking redis-py command methods for behavior coverage
- asserting exact private helper call counts
- making a fake Redis/Valkey server that cannot execute Lua
- testing implementation strings instead of behavior

Where unit tests are acceptable:

- config parsing
- namespace validation
- target URL construction
- ID encoding and lex-bound helpers
- result-shape conversion

Where integration tests are required:

- Lua scripts
- Pub/Sub waiters
- generator rollback/commit
- concurrent reads/moves
- cleanup ownership
- CLI round trips

## Engineering Guardrails

DRY:

- Reuse core timestamp generation.
- Reuse core queue-name and message-size rules where practical.
- Reuse PG test harness patterns.
- Reuse waiter registry ideas from PG.

YAGNI:

- No Redis Cluster or Valkey cluster support in v1. The planned Lua scripts use
  multiple keys and assume a single logical database namespace.
- No Streams backend in v1.
- No visibility timeout feature.
- No delayed messages.
- No retry/dead-letter system.
- No reverse alias index.
- No memory usage scan unless cheap and bounded.

Correctness over cleverness:

- Prefer one clear Lua script per atomic operation.
- Keep scripts small enough to reason about.
- Prefer explicit Valkey/Redis key names over generic script builders.
- Do not chase a shared abstraction that makes SQLite, PG, and Redis all worse.

## Known Risks And Counterarguments

### "Why not Redis Streams?"

Steelman: Streams look like queues, include IDs, consumer groups, pending
entries, and blocking reads.

Reason not to use them in v1:

- SimpleBroker needs `peek`, exact message ID operations, `move` between queues,
  claimed-message stats, vacuum, and broadcast to existing queues.
- Streams model append-only logs plus consumer-group acknowledgement, not the
  current pending/claimed row model.
- Newer stream commands such as `XACKDEL` improve this, but they raise minimum
  server version expectations and still do not map cleanly to move/broadcast.

Sorted sets plus hashes are less trendy but match the current API more closely.

### "Why add a direct-core seam instead of fitting Redis into SQLRunner?"

Because the SQLRunner seam means "execute SQL with transaction control." Redis
does not do that. Faking it creates a hidden, fragile DSL based on SQL string
identity. That is the wrong abstraction.

The direct-core seam is the smaller honest change. It keeps the public API
stable and isolates Valkey/Redis differences inside the Redis extension.

### "Why not make all backends implement the direct protocol immediately?"

Because that is a bigger rewrite with no immediate payoff. SQLite and PG already
work through SQL. Add the direct path only for backends that need it.

### "Is Valkey/Redis durability equivalent to SQLite or Postgres?"

No. Durability depends on server configuration. A Valkey or Redis deployment
without AOF/RDB persistence can lose data on restart. The extension must not
hide this.

## Bite-Sized Implementation Order

Use this exact order:

1. Add the direct-backend protocol and dummy tests.
2. Scaffold `extensions/simplebroker_redis`.
3. Implement config parsing and namespace validation.
4. Implement `RedisRunner`.
5. Implement key helpers and ID encoding.
6. Implement namespace init, meta reads, timestamp advance.
7. Implement write/peek/claim-one.
8. Implement claim-many and exactly-once generators.
9. Implement at-least-once batch claim rollback/commit.
10. Implement move-one/many and move generator rollback/commit.
11. Implement delete, vacuum, status, queue stats.
12. Implement aliases.
13. Implement broadcast.
14. Wire plugin direct-core methods.
15. Extend shared test harness for Redis.
16. Run shared tests and fix behavior gaps.
17. Implement Redis activity waiters.
18. Add CLI integration tests.
19. Add docs.
20. Run full SQLite, Valkey-backed Redis, and available PG gates.

Do not skip to waiters or docs before the core behavior is green.

## Final Acceptance Criteria

The work is done when:

- `simplebroker` still has zero runtime dependencies.
- SQLite tests still pass.
- Existing PG tests still pass when PG is available.
- Redis extension tests pass against a real Valkey 7.x server.
- Shared backend tests pass with `BROKER_TEST_BACKEND=redis`.
- `broker init/write/read/peek/move/delete/list/status/watch` work with
  `backend = "redis"` project config against Valkey.
- `Queue(..., runner=RedisRunner(...))` works.
- `--cleanup` deletes only the owned Valkey/Redis namespace.
- Pub/Sub waiters wake promptly but polling still handles missed notifications.
- at-least-once generator rollback tests pass.
- README clearly documents Valkey/Redis durability and stale batch recovery
  tradeoffs.

## Plan Review Notes

This plan deliberately chooses sorted sets plus hashes over Streams. That is the
right direction for preserving the current API.

The highest-risk part is the at-least-once generator emulation. If implementing
that starts forcing a much larger transaction framework or global Valkey/Redis locks,
stop and reassess. The acceptable fallback is not to silently weaken semantics;
it is to return with a focused explanation of which SimpleBroker guarantee the
backend cannot preserve cleanly and what product tradeoff should change.

The second highest-risk part is widening the backend plugin contract. Keep it
small. If SQLite or PG need broad rewrites, the seam is too invasive.
