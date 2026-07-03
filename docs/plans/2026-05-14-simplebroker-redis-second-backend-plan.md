# simplebroker-redis Second Backend Hardening Plan

## Purpose

Turn the current ignored `simplebroker-redis` prototype into a real second
extension backend for SimpleBroker, published separately and kept in sync with
core and `simplebroker-pg`.

This is not a new product direction. Keep the same decisions already made:

- package name: `simplebroker-redis`
- public backend name: `redis`
- implementation package: `extensions/simplebroker_redis/`
- compatibility target: Valkey 7.x and Redis 7.x
- required integration test target: Valkey 7.x
- core package remains SQLite-first with zero runtime dependencies
- Redis support stays optional and extension-owned

The second-backend goal is stronger than a prototype goal. The backend must be
good enough to prove that SimpleBroker has a real backend contract beyond SQL,
without bending the public API or turning SimpleBroker into a distributed task
queue framework.

## Non-Negotiables

These are locked. Do not revisit them while implementing.

- Do not add `redis` or redis-py to root `dependencies`.
- Do not put Redis runtime code under `simplebroker/_backends/`.
- Do not implement a fake SQL runner that pattern-matches SQL strings.
- Do not use Redis Streams in v1.
- Do not add retries, delayed jobs, dead-letter queues, consumer groups, or
  visibility timeouts in v1.
- Do not support Redis Cluster or Valkey cluster in v1.
- Do not use Redis 8.x-only commands.
- Do not weaken the public SimpleBroker API to fit Redis.
- Do not call `FLUSHDB`, ever.
- Do not mock redis-py command methods for behavior coverage.

## Current Prototype State

There is an ignored prototype at:

- `extensions/simplebroker_redis/`
- `bin/pytest-redis`

It has the right broad shape:

- separate package
- `redis` backend entry point
- direct core path instead of SQL emulation
- Valkey-backed tests
- shared tests can run with `BROKER_TEST_BACKEND=redis`
- scoped Pub/Sub is started
- sorted sets use fixed-width lexicographic IDs

It is not ready to publish. Known hardening gaps:

- `BEGIN` batch reservation is not atomic enough. It selects IDs, checks
  reservation state, then pipelines reservation. Two consumers can race.
- at-least-once `move_generator` commit is suspect because `_move_rows` skips
  reserved IDs.
- stale batch recovery is not implemented.
- duplicate timestamp protection is incomplete. `HSET bodies id body` can
  overwrite if a duplicate ID is generated.
- cleanup does not yet refuse active non-stale reservations.
- Pub/Sub listener lifecycle needs stronger close/refcount cleanup.
- tests that inspect private SQL internals are still mixed into `shared`.
- Redis-specific timestamp resilience should be implemented with Redis-native
  Lua and tested directly.

This plan keeps the direction, but requires replacing those weak spots.

## Repository Primer

Before editing, read these files:

- `pyproject.toml`
- `.gitignore`
- `simplebroker/_backend_plugins.py`
- `simplebroker/ext.py`
- `simplebroker/db.py`
- `simplebroker/_broker_session.py`
- `simplebroker/sbqueue.py`
- `simplebroker/watcher.py`
- `simplebroker/_timestamp.py`
- `simplebroker/_runner.py`
- `simplebroker/_targets.py`
- `simplebroker/_project_config.py`
- `simplebroker/commands.py`
- `simplebroker/cli.py`
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
- `tests/test_queue_api_comprehensive.py`
- `tests/test_generator_methods.py`
- `tests/test_exactly_once_delivery.py`
- `tests/test_move.py`
- `tests/test_move_by_id.py`
- `tests/test_batch_operations.py`
- `tests/test_aliases_db.py`
- `tests/test_activity_waiter_api.py`
- `tests/test_timestamp_resilience.py`
- `tests/test_transaction_error_propagation.py`
- `tests/test_watcher_thundering_herd.py`

Useful external docs:

- Valkey/Redis `EVAL`: https://redis.io/docs/latest/commands/eval/
- Valkey/Redis `ZRANGE BYLEX`: https://redis.io/docs/latest/commands/zrange/
- Valkey/Redis sorted sets: https://redis.io/docs/latest/develop/data-types/sorted-sets/
- Valkey/Redis Pub/Sub: https://redis.io/docs/latest/develop/pubsub/
- redis-py pipelines: https://redis.io/docs/latest/develop/clients/redis-py/transpipe/
- redis-py connection pools: https://redis.io/docs/latest/integrate/redis-py/

## Target Data Model

Every key belongs to one namespace prefix:

```text
simplebroker:{namespace}
```

Key layout:

```text
{prefix}:meta
{prefix}:queues
{prefix}:aliases
{prefix}:bodies
{prefix}:all_ids
{prefix}:q:{queue}:pending
{prefix}:q:{queue}:claimed
{prefix}:q:{queue}:reserved
{prefix}:batches:{token}:ids
{prefix}:batches:{token}:meta
{prefix}:activity:all
{prefix}:activity:q:{queue}
```

Meaning:

- `meta`: hash with `magic`, `schema_version`, `last_ts`, `alias_version`.
- `queues`: set of queues with pending, claimed, or reserved broker state.
- `aliases`: hash of alias name to target queue.
- `bodies`: hash of encoded message ID to message body.
- `all_ids`: sorted set of every live message ID, score `0`, used for cheap
  max timestamp resync and duplicate-ID checks.
- `pending`: sorted set of unclaimed IDs for one queue.
- `claimed`: sorted set of claimed IDs for one queue.
- `reserved`: sorted set of IDs reserved by open at-least-once batches.
- `batches:*`: temporary reservation metadata.
- `activity:*`: Pub/Sub channels only. They are not durable state.

Ordering:

- Store message IDs as fixed-width 19-digit decimal strings.
- Store every sorted-set score as `0`.
- Use lexicographic ranges, not numeric scores.
- Never store SimpleBroker message IDs as Redis sorted-set scores.
- Never use `ZPOPMIN` for message ordering.

Why `all_ids` exists:

- It makes duplicate-ID detection and timestamp resync backend-native.
- It avoids scanning every queue to compute the max message ID.
- It is still simple. It is not a second queue index; it is global ID metadata.
- It must be queried lexicographically. To get the max ID, use the
  lexicographic equivalent of "last member", not numeric score ordering. Scores
  are always `0`.

## Target Semantics

The Redis backend must satisfy all public shared behavior unless a backend
tradeoff is explicitly documented.

Required:

- FIFO within a queue.
- Message IDs remain existing SimpleBroker hybrid timestamps.
- `read` / `claim` marks messages claimed before returning in exactly-once mode.
- `peek` never mutates state.
- `move` is atomic from source to destination.
- `move(..., message_id=...)` can move claimed messages when
  `require_unclaimed=False`.
- at-least-once generators reserve, then commit or rollback.
- aborted at-least-once generators replay unfinished messages in FIFO position.
- process death during a batch leaves reservations until stale recovery.
- cleanup refuses active non-stale reservations.
- namespace cleanup only deletes owned namespace keys.
- Pub/Sub wakeups are precise but still hints.

Documented Redis-specific tradeoffs:

- Durability depends on Valkey/Redis persistence and replication settings.
- `db_size` is `0` in v1 unless a bounded namespace memory estimator is added.
- Pub/Sub is not durable. Polling remains part of watcher correctness.
- at-least-once generator rollback is emulated with reservation keys, not a
  database transaction.
- Redis Cluster is unsupported in v1.

## Code Style

Use the existing repo style.

- Python only. No extra framework.
- Keep root `dependencies = []`.
- Put redis-py only in `extensions/simplebroker_redis/pyproject.toml`.
- Keep methods named like `BrokerCore` where they implement the same behavior.
- Prefer small helper functions over broad abstraction layers.
- Put Lua scripts in `extensions/simplebroker_redis/simplebroker_redis/scripts.py`.
- Keep script inputs explicit: `KEYS` for keys, `ARGV` for data.
- Never build Lua by concatenating queue names or message values into the script.
- Keep Redis key construction in one helper module or one small helper class.
- Avoid importing private core helpers unless there is no stable alternative.
  If importing private helpers is necessary, isolate it and add a comment.
- Comments should explain non-obvious invariants, not narrate simple code.

## Test Design Rules

Do not over-mock.

Good tests:

- call `Queue`, `RedisBrokerCore`, CLI, or Lua scripts through a real redis-py
  client connected to Valkey
- assert behavior and invariants
- isolate using unique namespaces
- clean up only the test namespace
- include concurrency tests for reservation races

Bad tests:

- mocking redis-py command methods for behavior
- asserting private helper call counts
- fake Redis servers
- tests that pass without Lua executing
- tests that only check strings exist

Acceptable unit tests:

- config parsing
- namespace validation
- key helper construction
- ID encoding and lex bounds
- result-shape conversion

Required integration tests:

- every Lua script
- timestamp conflict and resync behavior
- duplicate-ID rejection
- reservation begin/commit/rollback
- stale batch recovery
- cleanup ownership and active reservation refusal
- Pub/Sub waiters
- CLI project config flows
- shared test suite against Valkey

## Task 1: Cleanly Define The Direct Backend Contract

Files to touch:

- `simplebroker/_backend_plugins.py`
- `simplebroker/ext.py`
- `simplebroker/db.py`
- `simplebroker/_broker_session.py`
- `tests/test_backend_plugin_resolution.py`
- `tests/test_process_broker_session.py`

Goal:

Make direct backends a first-class internal extension path without turning every
backend into a direct backend.

Implementation:

1. Keep `BrokerConnection` as the internal protocol for objects used by
   `Queue`, CLI, and watchers.
2. Keep SQL backends on the existing `BrokerCore(SQLRunner)` path.
3. Allow `BackendPlugin.sql` to be `None`.
4. If `plugin.sql is None`, require:
   - `is_direct_backend = True`
   - callable `create_core(...)`
   - callable `create_core_from_runner(...)`
5. If `plugin.sql is not None`, keep `ensure_backend_sql_namespace(...)`.
6. Reject malformed plugins clearly:
   - SQL namespace missing methods
   - direct plugin without `create_core`
   - plugin with `sql is None` but `is_direct_backend` false
7. Keep `Queue(..., runner=RedisRunner(...))` working through
   `create_core_from_runner`.
8. Keep process-shared persistent queues working for both SQL and direct
   backends.

Tests:

- Dummy SQL plugin still validates SQL namespace.
- Dummy malformed SQL plugin fails fast.
- Dummy direct plugin with no SQL and a fake direct core resolves.
- Dummy direct plugin without `create_core` fails fast.
- `DBConnection(ResolvedTarget("dummy_direct", ...))` uses direct core.
- `Queue(..., runner=DummyDirectRunner(...))` uses `create_core_from_runner`.
- SQLite and PG tests still pass.

Gate:

```bash
uv run pytest tests/test_backend_plugin_resolution.py tests/test_process_broker_session.py -q
uv run pytest tests/test_queue_connection_manager.py -q
```

Do not touch Redis behavior until this seam is green.

## Task 2: Normalize The Redis Package Scaffold

Files to touch:

- `.gitignore`
- `pyproject.toml`
- `extensions/simplebroker_redis/pyproject.toml`
- `extensions/simplebroker_redis/LICENSE`
- `extensions/simplebroker_redis/README.md`
- `extensions/simplebroker_redis/simplebroker_redis/__init__.py`
- `extensions/simplebroker_redis/simplebroker_redis/py.typed`
- `extensions/simplebroker_redis/simplebroker_redis/_constants.py`
- `extensions/simplebroker_redis/simplebroker_redis/keys.py`
- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
- `extensions/simplebroker_redis/simplebroker_redis/runner.py`
- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_redis/simplebroker_redis/validation.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/tests/conftest.py`

Goal:

Make the package mirror `simplebroker-pg` closely enough that release and
maintenance are predictable.

Implementation:

1. Keep the directory ignored while developing if requested by the maintainer.
2. Before landing, remove the ignore for `extensions/simplebroker_redis/` and
   `bin/pytest-redis`.
3. Add root extra:

```toml
[project.optional-dependencies]
redis = [
    "simplebroker-redis>=0.1.0,<1",
]

[tool.uv.sources]
simplebroker-redis = { path = "./extensions/simplebroker_redis", editable = true }
```

4. Add extension dependency:

```toml
dependencies = [
    "simplebroker>=3.5.0,<4",
    "redis>=5,<7",
]
```

5. Entry point:

```toml
[project.entry-points."simplebroker.backends"]
redis = "simplebroker_redis.plugin:get_backend_plugin"
```

6. Export only:
   - `RedisRunner`
   - `RedisBackendPlugin`
   - `get_backend_plugin`

Tests:

```bash
uv run --with redis --with-editable ./extensions/simplebroker_redis \
  python -c "from simplebroker_redis import RedisRunner, get_backend_plugin; assert get_backend_plugin().name == 'redis'"
```

Gate:

```bash
uv run python -m build ./extensions/simplebroker_redis
```

## Task 3: Centralize Redis Key And ID Helpers

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/keys.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_redis/tests/test_redis_keys.py`

Goal:

Make key names and ID encoding boring, explicit, and hard to get subtly wrong.

Implementation:

Create helpers for:

- namespace validation wrapper
- prefix construction
- `meta`
- `queues`
- `aliases`
- `bodies`
- `all_ids`
- queue `pending`
- queue `claimed`
- queue `reserved`
- batch `ids`
- batch `meta`
- activity `all`
- activity queue channel
- ID encode/decode
- lex lower/upper bounds

Rules:

- ID encode: `f"{timestamp:019d}"`
- reject negative timestamps
- reject encoded IDs longer than 19 digits
- no glob or whitespace in namespace
- no `{` or `}` in namespace in v1
- queue names still use core queue-name validation

Tests:

- numeric order equals lex order for representative timestamps
- `after_timestamp` uses exclusive lower bound
- `before_timestamp` uses exclusive upper bound
- exact timestamp uses inclusive exact bound
- invalid namespace is rejected
- key helpers do not produce unscoped keys

Gate:

```bash
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest extensions/simplebroker_redis/tests/test_redis_keys.py -q
```

## Task 4: Move Every Atomic Mutation Into Lua Scripts

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/tests/test_redis_scripts.py`

Goal:

Use Lua as the transaction boundary. Do not mix read-check-pipeline sequences
for operations that must be atomic.

Required scripts:

- `INIT_NAMESPACE`
- `ADVANCE_LAST_TS`
- `WRITE_MESSAGE`
- `CLAIM_MESSAGES`
- `MOVE_MESSAGES`
- `BEGIN_CLAIM_BATCH`
- `COMMIT_CLAIM_BATCH`
- `ROLLBACK_CLAIM_BATCH`
- `BEGIN_MOVE_BATCH`
- `COMMIT_MOVE_BATCH`
- `ROLLBACK_MOVE_BATCH`
- `RECOVER_STALE_BATCHES`
- `DELETE_QUEUE`
- `DELETE_ALL_MESSAGES`
- `VACUUM_CLAIMED`
- `ADD_ALIAS`
- `REMOVE_ALIAS`
- `BROADCAST`

Script rules:

- Every script checks `meta.magic`.
- Every script receives all keys through `KEYS`.
- Every script receives queue names, IDs, bodies, and options through `ARGV`.
- Every script returns simple arrays or integers.
- Python converts raw script output into typed results.
- Script constants include a short comment documenting result shape.
- Do not concatenate user data into script text.

Tests:

- Call scripts through redis-py against real Valkey.
- Test wrong namespace/meta fails.
- Test each script's result shape.
- Test queue state after each script.
- Test namespace isolation by running same script in two namespaces.

Gate:

```bash
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest extensions/simplebroker_redis/tests/test_redis_scripts.py -q
```

## Task 5: Implement Redis-Native Timestamp Resilience

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_redis/tests/test_redis_timestamp_resilience.py`
- `tests/test_timestamp_resilience.py`

Goal:

Give Redis the same logical timestamp safety as SQL backends, using Redis
facilities instead of SQL private internals.

Required behavior:

- `TimestampGenerator` remains the canonical timestamp generator.
- `RedisBackendPlugin.advance_last_ts(...)` uses Lua compare-and-set.
- `WRITE_MESSAGE` rejects duplicate message ID atomically.
- Duplicate ID maps to `IntegrityError`.
- `RedisBrokerCore.write(...)` retries timestamp conflicts like `BrokerCore`.
- Retry behavior should match the SQL core shape:
  - first duplicate: increment conflict metric and retry after a short backoff
  - second duplicate: resync timestamp state, increment resync metric, retry
  - third duplicate: raise a clear `RuntimeError`
- Redis conflict metrics match names:
  - `ts_conflict_count`
  - `ts_resync_count`
- Redis `_resync_timestamp_generator()` sets `meta.last_ts` to max live ID.
- Max live ID comes from `{prefix}:all_ids`, not a full DB scan.
- Because `all_ids` uses score `0`, max lookup must use lexicographic ordering,
  specifically `ZREVRANGEBYLEX key + - LIMIT 0 1`, not score ordering.

Write script invariant:

```text
If id already exists in bodies or all_ids:
  return duplicate-id error
Else:
  HSET bodies id body
  ZADD all_ids 0 id
  ZADD q:{queue}:pending 0 id
  SADD queues queue
```

Tests:

- normal writes leave conflict metrics at zero
- forced duplicate ID raises/retries and eventually writes a new ID
- forced permanent duplicate fails with clear `RuntimeError`
- corrupt `meta.last_ts` to zero, run Redis resync, next write succeeds
- `all_ids` max equals highest live pending or claimed message ID
- vacuum/delete/move keep `all_ids` correct
- no test uses `broker._runner.run(...)` for Redis

Shared test taxonomy:

- Keep public timestamp behavior in `shared`.
- Move SQL fault injection tests to `sql_backend_only` or guard them with a skip
  for direct backends.
- Add Redis-native fault injection under `redis_only`.

Gate:

```bash
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest extensions/simplebroker_redis/tests/test_redis_timestamp_resilience.py -q

BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest tests/test_timestamp_resilience.py -m shared -q
```

## Task 6: Fix Claim And Move Semantics

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/tests/test_redis_claim_move.py`
- `tests/test_move.py`
- `tests/test_move_by_id.py`
- `tests/test_exactly_once_delivery.py`

Goal:

Make exactly-once claim/move behavior match shared tests under concurrency.

Claim rules:

- Select from `pending` by lex range.
- Skip IDs in `reserved`.
- Move selected IDs from `pending` to `claimed` in one Lua script.
- Return `(body, id)` pairs in FIFO order.
- Remove queue from `queues` only when `pending`, `claimed`, and `reserved` are
  empty.

Move rules:

- If `require_unclaimed=True`, source must contain ID in `pending` and not
  `reserved`.
- If `require_unclaimed=False`, exact ID may come from `pending` or `claimed`,
  but never `reserved`.
- Non-exact moves select from `pending` only and skip `reserved`.
- Move removes from source state and adds to target `pending`.
- Move preserves message ID and body.
- Move publishes target activity.

Tests:

- FIFO claim
- claim exact timestamp
- claim after/before timestamp leaves skipped messages pending
- move one/many
- move by ID from pending
- move by ID from claimed with `require_unclaimed=False`
- move refuses same source and target
- concurrent claim consumers never receive duplicate IDs
- concurrent move consumers never move duplicate IDs
- reserved IDs are skipped

Gate:

```bash
BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest \
  tests/test_exactly_once_delivery.py \
  tests/test_move.py \
  tests/test_move_by_id.py \
  -m shared -q
```

## Task 7: Implement At-Least-Once Batch Reservations Correctly

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/tests/test_redis_generator_semantics.py`
- `tests/test_generator_methods.py`
- `tests/test_batch_operations.py`

Goal:

Match SQL generator semantics without pretending Redis has open transactions.

Required flow:

Claim batch:

1. `BEGIN_CLAIM_BATCH` Lua:
   - choose unreserved pending IDs by lex range
   - atomically add selected IDs to `reserved`
   - atomically create `batches:{token}:ids`
   - atomically create `batches:{token}:meta`
   - return body/id pairs
2. Python yields from returned in-memory rows.
3. If fully yielded, `COMMIT_CLAIM_BATCH` Lua:
   - verify batch belongs to claim/source
   - remove IDs from `pending`
   - remove IDs from `reserved`
   - add IDs to `claimed`
   - delete batch keys
4. If not fully yielded, `ROLLBACK_CLAIM_BATCH` Lua:
   - remove IDs from `reserved`
   - delete batch keys
   - leave `pending` unchanged

Move batch:

1. `BEGIN_MOVE_BATCH` reserves IDs from source `pending`.
2. Commit must not call normal `_move_rows` if normal move skips reserved IDs.
3. `COMMIT_MOVE_BATCH` Lua must:
   - verify batch belongs to move/source/target
   - remove IDs from source `pending`
   - remove IDs from source `reserved`
   - add IDs to target `pending`
   - update `queues`
   - publish target activity from Python after successful commit
4. Rollback removes source reservations only.

Important invariants:

- Pending IDs remain visible to `peek` while reserved.
- Other claim/move consumers skip reserved IDs.
- `has_pending_messages` returns true only if at least one unreserved pending ID
  matches the watcher filter.
- Same-thread mutating re-entry during an active batch raises the same
  `separate BrokerDB/Queue instance` message shape as core.
- Read-only re-entry such as `peek_one` does not deadlock.

Tests:

- closing claim generator before batch end replays unfinished messages
- closing move generator before batch end keeps messages in source
- completed claim generator moves messages to claimed
- completed move generator moves messages to target
- concurrent batch begin cannot reserve same ID twice
- batch exact timestamp works
- batch after/before timestamp works
- same-thread mutation during open batch raises
- read-only peek during open batch works

Gate:

```bash
BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest tests/test_generator_methods.py tests/test_batch_operations.py -m shared -q

SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest extensions/simplebroker_redis/tests/test_redis_generator_semantics.py -q
```

## Task 8: Implement Stale Batch Recovery

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/simplebroker_redis/runner.py`
- `extensions/simplebroker_redis/tests/test_redis_stale_batches.py`
- `extensions/simplebroker_redis/README.md`

Goal:

Make process-death recovery explicit, bounded to the namespace, and testable.

Implementation:

- Add runner option `stale_batch_seconds`, default `300`.
- Accept env/backend option `BROKER_REDIS_STALE_BATCH_SECONDS` only inside the
  Redis extension path. Do not add a root config constant unless needed by core.
- `recover_stale_batches(max_age_seconds)` scans only
  `{prefix}:batches:*:meta`.
- For stale claim/move batches:
  - read source queue
  - read batch IDs
  - remove those IDs from source `reserved`
  - delete batch keys
- Do not recover non-stale batches.
- Run recovery:
  - during `RedisBrokerCore` initialization
  - before starting any at-least-once batch
  - before destructive cleanup/delete

Tests:

- non-stale reservation is preserved
- stale claim reservation is removed
- stale move reservation is removed
- stale recovery only touches its namespace
- after recovery, messages can be claimed/moved again
- cleanup calls recovery before refusing active reservations

Gate:

```bash
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest extensions/simplebroker_redis/tests/test_redis_stale_batches.py -q
```

## Task 9: Harden Delete, Vacuum, Cleanup, And Ownership

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_redis/simplebroker_redis/validation.py`
- `extensions/simplebroker_redis/tests/test_redis_ownership.py`
- `extensions/simplebroker_redis/tests/test_redis_maintenance.py`
- `tests/test_cleanup.py`

Goal:

Protect user data and active batches. This is a backend safety surface.

Ownership states:

- `ABSENT`: no namespace keys
- `OWNED`: valid SimpleBroker meta
- `FOREIGN`: keys under namespace but no valid SimpleBroker meta
- `PARTIAL_SIMPLEBROKER`: some SimpleBroker-looking keys but invalid meta

Rules:

- initialize accepts `ABSENT` and `OWNED`.
- validate initialized requires `OWNED`.
- validate for init accepts `ABSENT` and `OWNED`, rejects `FOREIGN` and
  `PARTIAL_SIMPLEBROKER`.
- cleanup accepts only `OWNED` or `ABSENT`.
- cleanup never deletes foreign namespace keys.
- cleanup never calls `FLUSHDB`.
- cleanup recovers stale batches first.
- cleanup refuses non-stale reservations with `DatabaseError`.
- queue delete refuses non-stale reservations with `OperationalError`.
- all-queue delete refuses non-stale reservations with `OperationalError`.
- vacuum removes claimed bodies and `all_ids` entries only.
- vacuum does not touch pending or reserved IDs.

Tests:

- cleanup absent namespace returns false/no-op
- cleanup owned namespace deletes only matching keys
- cleanup foreign namespace refuses
- cleanup partial namespace refuses
- queue delete active reservation refuses
- queue delete stale reservation recovers then succeeds
- vacuum removes claimed and keeps pending
- vacuum updates `all_ids`
- delete updates `all_ids`
- no test DB is flushed

Gate:

```bash
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest \
  extensions/simplebroker_redis/tests/test_redis_ownership.py \
  extensions/simplebroker_redis/tests/test_redis_maintenance.py \
  -q

BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest tests/test_cleanup.py -m shared -q
```

## Task 10: Finish RedisRunner Lifecycle

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/runner.py`
- `extensions/simplebroker_redis/tests/test_redis_runner_lifecycle.py`
- `simplebroker/_broker_session.py`

Goal:

Make `RedisRunner` a resource handle, not a SQL runner.

Required:

- `target`
- `namespace`
- `client`
- `backend_plugin`
- `stale_batch_seconds`
- `close()`
- `shutdown()`
- `release_thread_connection()`
- fork detection that recreates pool/client in the child
- clear errors for accidental SQL methods

SQL method behavior:

If `run`, `begin_immediate`, `commit`, or `rollback` exist, they must raise:

```text
RedisRunner is not SQL-capable; use RedisBackendPlugin.create_core(...)
```

Do not make them perform partial compatibility tricks.

Tests:

- runner can `ping`
- runner has namespace
- runner carries stale batch setting
- close and shutdown are idempotent
- fork detection does not reuse parent pool
- accidental `run("SELECT 1")` fails clearly
- process-shared persistent queues reuse appropriate runner/core state

Gate:

```bash
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest extensions/simplebroker_redis/tests/test_redis_runner_lifecycle.py -q
```

## Task 11: Harden Pub/Sub Waiters

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_redis/simplebroker_redis/runner.py`
- `extensions/simplebroker_redis/tests/test_redis_notify.py`
- `tests/test_activity_waiter_api.py`
- `tests/test_watcher_thundering_herd.py`
- `tests/test_watcher_race_conditions.py`

Goal:

Make scoped Pub/Sub useful without making it the correctness source.

Channel shape:

```text
{prefix}:activity:q:{queue}
{prefix}:activity:all
```

Rules:

- Queue names currently validate to alphanumeric, `_`, `.`, and `-`, so raw
  queue names are safe in channel suffixes. If queue-name rules ever broaden,
  add an explicit channel encoder before changing the validator.
- Single-queue waiter subscribes through one shared listener.
- Multi-queue waiter reuses the same listener and tracks multiple queues.
- Broadcast wakes all through `activity:all`.
- Move wakes destination only.
- Write wakes target queue.
- Notifications are hints. Watchers must still check durable state.
- Missed notifications must not lose messages.
- Close unregisters queue state.
- Listener registry removes listeners when no waiters remain.
- Listener errors wake waiters and surface as `OperationalError`.

Tests:

- single waiter ignores unrelated queue
- write wakes matching queue
- move wakes destination
- broadcast wakes all
- multi-queue waiter wakes for any watched queue
- closing waiters unregisters
- registry drops listener when last waiter closes
- listener close wakes blocked waiters
- missed notification is caught by polling
- thundering-herd test passes under full Valkey xdist gate

Gate:

```bash
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest extensions/simplebroker_redis/tests/test_redis_notify.py -q

BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest tests/test_activity_waiter_api.py tests/test_watcher_thundering_herd.py -m shared -q
```

## Task 12: Clean Up Shared Test Taxonomy

Files to touch:

- `pyproject.toml`
- `tests/conftest.py`
- `tests/test_timestamp_resilience.py`
- `tests/test_transaction_error_propagation.py`
- any shared test that reaches private SQL internals

Goal:

Make `shared` mean public backend behavior. Do not make Redis pass SQL-private
tests, and do not hide real Redis behavior gaps by over-skipping.

Markers:

- `shared`: public API behavior all supported backends must pass
- `sqlite_only`: SQLite storage/file behavior
- `pg_only`: Postgres extension behavior
- `redis_only`: Redis extension behavior
- `sql_backend_only`: SQL runner internals and SQL fault injection

Rules:

- Tests that call `broker._runner.run(...)` are not shared unless they first
  prove they are on a SQL backend.
- Tests that patch `begin_immediate` are SQL-backend tests.
- Tests that assert file size or SQLite path behavior are SQLite-only.
- Tests that assert `db_size > 0` must account for Redis `db_size == 0`.
- Redis-only tests must cover equivalent Redis-native fault injection.

Tests:

- Run collection under SQLite, PG, and Redis and inspect counts.
- No accidental unmarked SQL-private tests should run under Redis.

Gate:

```bash
uv run pytest tests/test_dev_scripts.py -q

BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest -m shared -q
```

## Task 13: Extend The Backend-Agnostic Test Harness For Redis

Files to touch:

- `tests/conftest.py`
- `tests/helper_scripts/broker_factory.py`
- `bin/pytest-redis`
- `tests/test_dev_scripts.py`

Goal:

Make Redis as easy to test as PG, with a Valkey container by default.

Harness behavior:

- `BROKER_TEST_BACKEND=redis`
- test URL from `SIMPLEBROKER_VALKEY_TEST_URL`
- optionally accept `SIMPLEBROKER_REDIS_TEST_URL` as alias
- one namespace per xdist worker
- per-test reset via `cleanup_target` then `initialize_target`
- `.broker.toml` written with:

```toml
version = 1
backend = "redis"
target = "redis://127.0.0.1:6379/15"

[backend_options]
namespace = "pytest_worker_gw0"
```

`bin/pytest-redis` behavior:

- starts `valkey/valkey:7.2`
- publishes random host port
- sets `SIMPLEBROKER_VALKEY_TEST_URL`
- runs shared tests with `BROKER_TEST_BACKEND=redis`
- runs extension tests with `redis_only`
- supports `--fast`
- supports `--keep-container`
- routes `tests/...` to shared suite
- routes `extensions/simplebroker_redis/...` to extension suite
- mirrors `bin/pytest-pg` style closely enough to maintain together

Tests:

- dev-script tests for argument routing
- env building
- no Redis tests require a preexisting local Valkey when using script

Gate:

```bash
bin/pytest-redis --fast
bin/pytest-redis extensions/simplebroker_redis/tests -q
bin/pytest-redis tests/test_queue_api_comprehensive.py -q
```

## Task 14: Complete CLI And Project Config Integration

Files to touch:

- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `simplebroker/_project_config.py` only if existing plugin path cannot handle it
- `simplebroker/_targets.py` only if redaction needs adjustment
- `tests/test_project_config.py`
- `tests/test_commands_init.py`
- `extensions/simplebroker_redis/tests/test_redis_cli.py`

Goal:

All normal CLI flows work with `.broker.toml` and env-selected Redis.

Config inputs:

- `BROKER_BACKEND=redis`
- `BROKER_BACKEND_TARGET=redis://...`
- `BROKER_BACKEND_HOST`
- `BROKER_BACKEND_PORT`
- `BROKER_BACKEND_PASSWORD`
- `BROKER_BACKEND_DATABASE`
- `BROKER_BACKEND_SCHEMA` as namespace fallback
- TOML `target`
- TOML `[backend_options].namespace`

Rules:

- TOML target overrides env host/port/db.
- TOML namespace overrides env schema.
- Password in URL and password env both work.
- CLI display redacts password.
- Missing Valkey server reports connection/target error, not "backend missing".
- `--cleanup` cleans only the namespace.
- After cleanup, `broker init` re-creates the namespace.

Tests:

- `broker init`
- `broker write jobs hello`
- `broker read jobs`
- `broker peek jobs`
- `broker move jobs done`
- `broker list`
- `broker status`
- `broker watch` smoke test
- `broker --cleanup`
- env-selected init
- password redaction
- missing server error

Gate:

```bash
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest extensions/simplebroker_redis/tests/test_redis_cli.py -q
```

## Task 15: Document Backend Tradeoffs

Files to touch:

- `extensions/simplebroker_redis/README.md`
- root `README.md`
- `CHANGELOG.md` only when landing
- docs/plans file if design changes during implementation

Goal:

Users should know exactly what this backend is good for and what it does not
promise.

Extension README must include:

- install:
  - `pipx inject simplebroker simplebroker-redis`
  - `uv add simplebroker-redis`
  - `uv add "simplebroker[redis]"`
- Python usage with `RedisRunner`
- `.broker.toml` usage
- env-selected usage
- Valkey 7.x test target
- Redis 7.x compatibility target
- namespace ownership model
- cleanup safety rules
- durability caveat
- Pub/Sub caveat
- at-least-once stale batch recovery caveat
- no Redis Cluster in v1
- why not Streams in v1
- recommended Valkey local test command:

```bash
docker run --rm -p 6379:6379 valkey/valkey:7.2
```

Required wording:

```text
The Redis backend optimizes for low-latency shared queue access. Durability
depends on Valkey/Redis persistence and replication settings. For strongest
storage guarantees, use SQLite or Postgres.
```

Do not imply Redis is non-durable by definition. Do not imply it has the same
durability profile as SQLite or Postgres by default.

Tests:

- README examples import correctly
- CLI snippets are smoke-tested where practical

Gate:

```bash
uv run pytest tests/test_no_dependencies.py -q
uv run python -m build ./extensions/simplebroker_redis
```

## Task 16: Packaging And Release Integration

Files to touch:

- `pyproject.toml`
- `extensions/simplebroker_redis/pyproject.toml`
- `bin/pytest-redis`
- `bin/packaging-smoke` or `simplebroker/_scripts.py` if packaging smoke should
  include Redis
- `tests/test_release_script.py`
- `tests/test_dev_scripts.py`

Goal:

The Redis extension can be built, smoke-installed, and versioned in sync with
core and PG.

Implementation:

- Make root extra `redis` point at `simplebroker-redis`.
- Ensure root package still has `dependencies = []`.
- Ensure extension package includes `py.typed`, license, README, and package
  modules.
- Add Redis extension build to packaging smoke once it is no longer ignored.
- Decide version policy:
  - core and PG remain independent package versions
  - Redis can start at `0.1.0`
  - dependency range should follow core compatibility, e.g. `simplebroker>=3.5.0,<4`
- Add release checklist item: run SQLite, PG, Redis gates.

Gates:

```bash
uv run python -m build
uv run python -m build ./extensions/simplebroker_pg
uv run python -m build ./extensions/simplebroker_redis
uv run pytest tests/test_no_dependencies.py -q
```

## Task 17: Full Verification Matrix

Run these before asking for review.

Core SQLite:

```bash
uv run pytest -q
```

Postgres, if DSN available:

```bash
BROKER_TEST_BACKEND=postgres \
SIMPLEBROKER_PG_TEST_DSN="$SIMPLEBROKER_PG_TEST_DSN" \
uv run pytest -m shared -q

SIMPLEBROKER_PG_TEST_DSN="$SIMPLEBROKER_PG_TEST_DSN" \
uv run pytest extensions/simplebroker_pg/tests -q
```

Redis/Valkey:

```bash
bin/pytest-redis --fast
bin/pytest-redis extensions/simplebroker_redis/tests -q
```

Focused Redis shared behavior:

```bash
BROKER_TEST_BACKEND=redis \
SIMPLEBROKER_VALKEY_TEST_URL=redis://127.0.0.1:6379/15 \
uv run pytest \
  tests/test_queue_api_comprehensive.py \
  tests/test_generator_methods.py \
  tests/test_exactly_once_delivery.py \
  tests/test_move.py \
  tests/test_move_by_id.py \
  tests/test_aliases_db.py \
  tests/test_watcher_thundering_herd.py \
  -m shared -q
```

Static sanity:

```bash
python -m compileall simplebroker tests extensions/simplebroker_redis
uv run pytest tests/test_no_dependencies.py -q
```

Acceptance criteria:

- root runtime dependencies remain empty
- SQLite full test suite passes
- PG shared and extension suites pass when available
- Redis fast shared gate passes against Valkey
- Redis extension tests pass against Valkey
- no Redis behavior tests use fake Redis
- no fake SQL runner exists
- Redis README documents durability and transaction tradeoffs
- cleanup cannot delete foreign or active reserved state
- stale recovery is tested
- duplicate timestamp protection is tested
- Pub/Sub remains a hint, not the source of truth

## Bite-Sized Implementation Order

Follow this exact order:

1. Clean direct backend contract and tests.
2. Normalize Redis package scaffold.
3. Centralize key and ID helpers.
4. Move Lua constants into `scripts.py`.
5. Implement and test `INIT_NAMESPACE` and ownership validation.
6. Implement and test timestamp compare-and-set.
7. Implement and test atomic `WRITE_MESSAGE` with duplicate-ID protection.
8. Implement Redis timestamp resilience and conflict metrics.
9. Implement exactly-once claim.
10. Implement exactly-once move.
11. Implement at-least-once claim batches.
12. Implement at-least-once move batches.
13. Implement stale batch recovery.
14. Harden delete, cleanup, and vacuum.
15. Implement aliases.
16. Implement broadcast.
17. Harden Pub/Sub waiters and lifecycle.
18. Clean shared test taxonomy.
19. Finish Redis test harness and `bin/pytest-redis`.
20. Complete CLI integration tests.
21. Complete docs.
22. Run full verification matrix.
23. Remove gitignore entries when ready to land the extension.

Do not jump to docs or packaging before the behavior and safety gates are green.

## Fresh-Eyes Review Notes

I re-reviewed this plan for drift and failure modes.

Potential bad direction: adding `all_ids` might look like extra indexing. It is
acceptable because it directly solves duplicate-ID protection and timestamp
resync. It does not add a user feature.

Potential bad direction: adding Redis-specific timestamp resilience could fork
the timestamp algorithm. The plan avoids that. `TimestampGenerator` remains
canonical; Redis only implements the storage and repair hooks.

Potential bad direction: Pub/Sub could become a delivery mechanism. The plan
keeps Pub/Sub scoped and useful, but still treats it as a hint.

Potential bad direction: shared tests could be weakened. The plan says the
opposite: keep public behavior shared, move only private SQL fault-injection
tests out of `shared`, and add Redis-native equivalent tests.

Potential bad direction: direct backend API could become a broad rewrite. The
plan keeps SQLite and PG on their existing SQL path and adds only the direct path
needed by non-SQL backends.

Potential bad direction: Redis could become a task queue framework. The plan
explicitly excludes retries, delayed jobs, dead-letter queues, Streams, and
consumer groups in v1.

Final judgment: this remains aligned with the discussed direction. Redis/Valkey
is reasonable as a second extension backend if it is framed as a fast shared
backend with different durability and transaction tradeoffs, and if the hardening
tasks above are completed before publication.
