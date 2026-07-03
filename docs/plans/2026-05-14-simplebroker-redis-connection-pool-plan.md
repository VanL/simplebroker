# simplebroker-redis Command ConnectionPool Integration Plan

## Purpose

Make the Redis/Valkey backend's command-traffic connection model explicit,
bounded, observable, and tested.

The desired model is:

- one process-local `RedisRunner` per `(target, namespace, config)` session
- one command `ConnectionPool` owned by that runner
- one `redis.Redis` command client backed by that pool
- lightweight `RedisBrokerCore` objects that share the runner and client
- one separate Pub/Sub listener connection per `(target, namespace)`

Do not try to make Redis look like SQLite or Postgres by multiplexing all
traffic over one physical command connection. That would require a global lock
around every Redis command because RESP command/response ordering is connection
scoped. It would serialize watcher pre-checks and writes, recreate the exact
contention seen in the Redis watcher failures, and waste Redis's normal client
model.

This plan is about regular broker commands only. Pub/Sub remains separate.

## Current State

The ignored Redis extension currently has this broad shape:

- `extensions/simplebroker_redis/simplebroker_redis/runner.py`
  creates a redis-py `ConnectionPool` lazily inside `RedisRunner.client`.
- `RedisRunner.client` returns one redis-py `Redis` object that is backed by
  the pool.
- `_ProcessBrokerSession` shares one `RedisRunner` across process-local
  persistent queues for the same target.
- Redis Pub/Sub uses `_SharedRedisActivityListener` in `plugin.py`, with a
  separate `redis.Redis.from_url(...)` client.
- Recent watcher work made per-thread core creation cheaper and shortened the
  native activity fallback poll window.

The current pool use is implicit and underspecified:

- `ConnectionPool.from_url(...)` is used directly.
- pool size is redis-py's default, not a SimpleBroker decision.
- pool exhaustion behavior is not tested.
- pool timeout is not mapped to SimpleBroker's timeout semantics.
- fork handling closes the runner, but pool recreation is not tested.
- docs do not explain the command pool versus Pub/Sub connection split.

That is not enough for a real backend. It works, but it is too accidental.

## Non-Negotiables

- Do not add Redis dependencies to the root package.
- Do not add Redis-specific configuration to the root SimpleBroker public API
  unless there is no reasonable extension-owned alternative.
- Do not share one physical Redis command connection across threads.
- Do not put Pub/Sub traffic on the command pool.
- Do not allow command-pool exhaustion to hang indefinitely.
- Do not use mocks for Redis behavior coverage.
- Do not broaden this into Redis Cluster support.
- Do not introduce a new async implementation.
- Do not change the external broker API.
- Do not use Lua for connection-pool behavior. This is client-side resource
  management, not Redis data mutation.

## Repository Primer

Read these files before editing:

- `simplebroker/_broker_session.py`
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `simplebroker/watcher.py`
- `simplebroker/_backend_plugins.py`
- `extensions/simplebroker_redis/simplebroker_redis/runner.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_redis/simplebroker_redis/validation.py`
- `extensions/simplebroker_redis/tests/conftest.py`
- `extensions/simplebroker_redis/tests/test_redis_integration.py`
- `extensions/simplebroker_redis/tests/test_redis_batches.py`
- `bin/pytest-redis`
- `tests/conftest.py`
- `tests/test_process_broker_session.py`
- `tests/test_watcher_race_conditions.py`
- `tests/test_watcher_thundering_herd.py`

Useful docs:

- redis-py connection pools:
  `https://redis.readthedocs.io/en/stable/connections.html`
- redis-py `BlockingConnectionPool`:
  `https://redis.readthedocs.io/en/stable/connections.html#redis.BlockingConnectionPool`
- Redis Pub/Sub:
  `https://redis.io/docs/latest/develop/pubsub/`
- Valkey Pub/Sub:
  `https://valkey.io/commands/psubscribe/`

Local fact checked during planning:

```text
redis-py version observed in the local dev environment: 7.4.0
redis.BlockingConnectionPool(max_connections=50, timeout=20, ...)
ConnectionPool.from_url(url, **kwargs)
BlockingConnectionPool.from_url(url, **kwargs)
```

Use local introspection again before implementing if the dependency changes.

## Design Decision

Use `redis.BlockingConnectionPool` for regular command traffic.

Why not plain `ConnectionPool`?

- Plain pools fail immediately once exhausted.
- Under watcher contention, a bounded wait is more useful than immediate
  failures when the server and app are healthy.
- `BlockingConnectionPool` gives explicit `max_connections` and `timeout`.
- Exhaustion still remains bounded, so failures become debuggable
  `OperationalError`s instead of indefinite liveness bugs.

Why not one physical connection?

- redis-py `Redis` instances are safe to use across threads because each
  command borrows a connection from the pool.
- A single shared physical connection would need a process-wide command lock.
- That would turn concurrent watcher pre-checks into serialized network
  round trips.
- It would make Redis worse than SQLite for the exact workload Redis should
  handle well.

Why keep Pub/Sub separate?

- Redis subscribed connections are special. Once subscribed, they are not
  normal command connections.
- A listener needs long-lived blocking reads.
- Borrowing from the command pool would pin a pool slot forever.
- A dedicated listener connection makes the resource model clear:
  command pool for broker state, listener connection for wake hints.

## Pool Defaults

Initial defaults:

```text
max_connections = 50
pool_timeout = max(0.001, BROKER_BUSY_TIMEOUT / 1000)
```

Rationale:

- redis-py's `BlockingConnectionPool` default max is 50. Keep it unless tests
  prove SimpleBroker needs something else.
- `BROKER_BUSY_TIMEOUT` is already the user-facing "do not wait forever on
  backend contention" knob.
- Mapping milliseconds to seconds gives Redis a coherent default without adding
  root config keys.
- A minimum timeout avoids accidental zero-timeout behavior if a user sets an
  unusually small busy timeout.

Do not add new environment variables in the first implementation. Support
backend options first:

```toml
[backend_options]
namespace = "simplebroker_redis_v1"
max_connections = 50
pool_timeout = 5.0
```

These are extension-owned options. They must not be required.

## Configuration Contract

Supported backend options:

- `namespace`: existing Redis namespace.
- `schema`: existing alias for namespace compatibility.
- `max_connections`: optional integer, must be `>= 1`.
- `pool_timeout`: optional number, seconds, must be `> 0`.

Rejected options:

- unknown backend options must raise `DatabaseError`. A typo in pool settings
  should not silently fall back to defaults.
- invalid `max_connections` or `pool_timeout` must raise `DatabaseError`.

Important: do not put pool options into the namespace validator. Namespace
validation belongs in `validation.py`, but pool option parsing belongs near the
runner/plugin boundary.

## Target Implementation Shape

Add a small internal options object in `runner.py`:

```python
@dataclass(frozen=True, slots=True)
class RedisPoolOptions:
    max_connections: int = 50
    timeout: float = 5.0
```

Use a helper:

```python
def pool_options_from_config(
    config: Mapping[str, Any] | None,
    backend_options: Mapping[str, Any] | None,
) -> RedisPoolOptions:
    ...
```

The helper may live in `runner.py` if only the runner uses it. If `plugin.py`
needs the same validation for tests or docs, use a small `pool.py` module:

```text
extensions/simplebroker_redis/simplebroker_redis/pool.py
```

Prefer `pool.py` if the parsing gets longer than about 40 lines. Do not bury
validation inside `RedisRunner.__init__`.

`RedisRunner` should look conceptually like this:

```python
class RedisRunner:
    def __init__(..., pool_options: RedisPoolOptions | None = None):
        self.pool_options = pool_options or default_pool_options(...)
        self._pool = None
        self._client = None
        self._client_lock = threading.Lock()

    @property
    def client(self) -> redis.Redis:
        self._check_fork()
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    self._pool = self._create_pool()
                    self._client = redis.Redis(connection_pool=self._pool)
        return self._client

    def _create_pool(self) -> redis.BlockingConnectionPool:
        return redis.BlockingConnectionPool.from_url(
            self.target,
            decode_responses=True,
            max_connections=self.pool_options.max_connections,
            timeout=self.pool_options.timeout,
        )
```

The double-check lock matters. Without it, concurrent first use can create
multiple pools and leak the losers. That risk is real now that
`_ProcessBrokerSession` permits parallel per-thread core creation.

## Bite-Sized Tasks

### Task 1: Add failing tests for explicit pool behavior

Files:

- `extensions/simplebroker_redis/tests/test_redis_pool.py`

Write tests first.

Tests to add:

1. `test_runner_uses_blocking_connection_pool`
   - Create a `RedisRunner`.
   - Access `runner.client`.
   - Assert `runner._pool` is a `redis.BlockingConnectionPool`.
   - This is a narrow implementation test. It is acceptable because the
     connection pool is the subject of the feature.

2. `test_runner_reuses_one_pool_for_multiple_clients`
   - Access `runner.client` several times.
   - Assert the same client object and same pool object are reused.
   - Close the runner.

3. `test_pool_options_from_backend_options`
   - Construct runner through plugin with:
     `backend_options={"namespace": ns, "max_connections": 3, "pool_timeout": 0.25}`.
   - Access `runner.client`.
   - Assert runner stores parsed pool options.

4. `test_invalid_pool_options_raise_database_error`
   - `max_connections=0`
   - `max_connections="nope"`
   - `pool_timeout=-1`
   - `pool_timeout="nope"`
   - These should raise `DatabaseError`.

5. `test_pool_exhaustion_is_bounded`
   - Use `max_connections=1`, `pool_timeout=0.1`.
   - Borrow and hold the only low-level connection from `runner._pool`.
   - Attempt a normal command through `runner.client.ping()`.
   - Assert it raises an exception mapped to SimpleBroker `OperationalError`
     at the broker/core boundary, or a redis-py timeout at the runner-only
     level if the test stays below the core.
   - Prefer testing through `RedisBrokerCore.write(...)` so the public backend
     behavior is covered.
   - Always release the borrowed connection in `finally`.

6. `test_shutdown_disconnects_pool`
   - Create runner, access client, capture pool.
   - Call `runner.shutdown()`.
   - Assert `runner._pool is None` and `runner._client is None`.
   - Then access `runner.client` again and assert a new pool is created.

Do not mock redis-py. Use Valkey via `bin/pytest-redis`.

Expected red state:

- `test_runner_uses_blocking_connection_pool` fails because current code uses
  `ConnectionPool`.
- option validation tests fail because the options do not exist.

### Task 2: Add pool option parsing

Files:

- `extensions/simplebroker_redis/simplebroker_redis/runner.py`
- optionally `extensions/simplebroker_redis/simplebroker_redis/pool.py`

Implement:

- `RedisPoolOptions`
- parser for backend options
- default timeout derived from config

Rules:

- `max_connections` must be an `int` or a string that parses cleanly as an
  integer.
- reject booleans. `True` is an `int` subclass in Python, but it is not a pool
  size.
- `pool_timeout` must be an `int`, `float`, or numeric string.
- reject NaN and infinity.
- reject negative timeout.
- reject timeout `0`. A zero timeout is a surprising operational default and
  can turn normal short contention into immediate failures.

Recommended behavior:

```text
max_connections >= 1
pool_timeout > 0
```

Use `DatabaseError` for invalid backend options.

Keep comments minimal. The code should be clear from names.

### Task 3: Wire pool options through the plugin

Files:

- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_redis/simplebroker_redis/runner.py`

Update:

- `RedisBackendPlugin.create_runner(...)`
- `RedisBackendPlugin.create_core(...)`
- `RedisBackendPlugin.create_core_from_runner(...)` only if needed

`create_runner(...)` should pass `config` and `backend_options` into the pool
option parser.

Important:

- Do not parse pool options in `init_backend(...)` unless the values need to be
  preserved in returned backend options.
- If `init_backend(...)` receives TOML backend options with pool keys, they
  should be carried through in `backend_options`, not dropped.

Concrete issue to check:

Current `_namespace_from_options(...)` extracts namespace but `init_backend`
returns only `{"namespace": namespace}`. If TOML has pool options, this would
drop them. Fix that.

Target shape:

```python
def init_backend(...):
    options = dict(toml_options or {})
    namespace = _namespace_from_options(config, options)
    backend_options = dict(options)
    backend_options["namespace"] = namespace
    backend_options.pop("schema", None)
    return {"target": target, "backend_options": backend_options}
```

Do not let `schema` and `namespace` disagree silently. If both are present and
they differ, raise `DatabaseError`. If only `schema` is present, normalize it to
`namespace` in the returned backend options.

### Task 4: Replace raw pool creation with `BlockingConnectionPool`

Files:

- `extensions/simplebroker_redis/simplebroker_redis/runner.py`

Implement:

- `self._client_lock = threading.Lock()`
- `_create_pool()`
- lazy pool creation with double-check locking
- `redis.BlockingConnectionPool.from_url(...)`

Keep `decode_responses=True`.

Do not pass unsupported kwargs. Verify with local introspection:

```bash
uv run --with redis python - <<'PY'
import inspect, redis
print(redis.__version__)
print(inspect.signature(redis.BlockingConnectionPool))
print(inspect.signature(redis.BlockingConnectionPool.from_url))
PY
```

Error mapping:

- Core methods already translate many `redis.RedisError` exceptions via
  `_translate_redis_error`.
- Confirm pool exhaustion exceptions are subclasses of `redis.RedisError`.
- If they are not, catch the concrete redis-py exception and map it to
  `OperationalError`.

### Task 5: Preserve fork safety

Files:

- `extensions/simplebroker_redis/simplebroker_redis/runner.py`

The current `_check_fork()` calls `self.close()` when PID changes. Keep that.

Add test:

- `extensions/simplebroker_redis/tests/test_redis_pool.py`

Test without actually forking:

- create runner and pool
- mutate `runner._pid` to an impossible old value
- access `runner.client`
- assert old pool is disconnected/replaced and `_pid` is current PID

This is one of the rare cases where touching private state is acceptable. The
fork-safety mechanism is private and hard to exercise deterministically in a
unit test.

### Task 6: Make Pub/Sub separation explicit

Files:

- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_redis/tests/test_redis_pool.py`
- `extensions/simplebroker_redis/tests/test_redis_integration.py`

Do not change the listener to use the command pool.

Add test:

- configure command pool with `max_connections=1`
- create activity waiter
- hold the only command-pool connection
- publish activity from a separate raw redis client or release the held
  command connection before writing if needed
- assert the waiter object itself can still be created and closed

Better version:

- `max_connections=1`
- create waiter first
- perform normal write/read traffic
- assert listener did not consume the only command pool slot

Avoid brittle assertions against private listener internals unless necessary.

### Task 7: Update docs

Files:

- `extensions/simplebroker_redis/README.md`
- possibly `docs/plans/2026-05-14-simplebroker-redis-second-backend-plan.md`
  if it still says the command connection model is unresolved

Document:

- regular commands use a redis-py `BlockingConnectionPool`
- Pub/Sub uses a separate dedicated connection
- defaults:
  - `max_connections=50`
  - `pool_timeout` derived from `BROKER_BUSY_TIMEOUT`
- backend options:
  - `max_connections`
  - `pool_timeout`
- pool exhaustion raises an operational error rather than hanging forever

Keep README prose short. This is an extension README, not an essay.

### Task 8: Run focused gates

Run:

```bash
bin/pytest-redis extensions/simplebroker_redis/tests/test_redis_pool.py -q
bin/pytest-redis extensions/simplebroker_redis/tests -q
uv run pytest tests/test_process_broker_session.py -q
```

Then run the watcher cluster that motivated the change:

```bash
bin/pytest-redis \
  tests/test_watcher.py::TestQueueWatcher::test_after_timestamp_database_filtering \
  tests/test_watcher_race_conditions.py::test_concurrent_pre_check_timing \
  tests/test_watcher_race_conditions.py::test_concurrent_writers_readers \
  tests/test_watcher_race_conditions.py::test_pre_check_database_contention \
  tests/test_watcher_thundering_herd.py::test_thundering_herd_with_multiple_active_queues \
  tests/test_watcher_thundering_herd.py::test_thundering_herd_mitigation \
  -q
```

Then run:

```bash
bin/pytest-redis --fast -q
uv run ruff check simplebroker/_broker_session.py simplebroker/watcher.py \
  extensions/simplebroker_redis/simplebroker_redis \
  extensions/simplebroker_redis/tests \
  bin/pytest-redis
python -m compileall simplebroker/_broker_session.py simplebroker/watcher.py \
  extensions/simplebroker_redis/simplebroker_redis \
  bin/pytest-redis
```

If any watcher failure appears after the pool change, do not relax the tests.
Investigate whether:

- pool exhaustion is occurring
- first-use core construction is serialized again
- Pub/Sub is waking all queues instead of the target queue
- fallback polling is too slow for missed Redis Pub/Sub messages
- command traffic accidentally uses the Pub/Sub connection

## Invariants

Command-pool invariants:

- one `RedisRunner` owns at most one live command pool per process PID
- `RedisRunner.client` is lazy
- concurrent first access cannot leak multiple pools
- `RedisRunner.close()` disconnects and clears the pool
- `RedisRunner.shutdown()` disconnects and clears the pool
- PID change closes the old pool before creating a new one
- `RedisBrokerCore.close()` must not shut down the shared pool
- `RedisBrokerCore.shutdown()` may shut down the runner only when the core owns
  it through the existing ownership path

Watcher invariants:

- adding a native Redis activity waiter must not disable fallback polling
- queue-scoped waiter wakes only for its queue, not for every namespace write
- missed Pub/Sub messages are recovered by fallback polling
- many watchers can call `_has_pending_messages()` concurrently without
  serializing on Python locks

Error invariants:

- invalid pool config raises `DatabaseError`
- command-pool exhaustion raises `OperationalError` through broker operations
- pool exhaustion never hangs indefinitely
- listener failures still surface through waiter `wait(...)` as
  `OperationalError`

API invariants:

- no change to `Queue`, `BrokerDB`, `QueueWatcher`, CLI, or public backend name
- no root dependency change
- no new required config
- no Redis-specific public methods

## Anti-Patterns To Avoid

- Do not add `threading.Lock` around every Redis command.
- Do not make every `RedisBrokerCore` create its own pool.
- Do not close the pool in `RedisBrokerCore.close()`.
- Do not call `gc.collect()` in Redis hot close/release paths.
- Do not put a Pub/Sub listener on a command-pool connection.
- Do not make pool tests pass with monkeypatched redis-py behavior.
- Do not treat Redis Pub/Sub as durable or replayable.
- Do not paper over liveness failures by increasing watcher test timeouts.

## Red-Green TDD Sequence

1. Add `test_redis_pool.py` with pool type, option parsing, bounded exhaustion,
   shutdown, and fork-safety tests. Confirm red.
2. Implement `RedisPoolOptions` and parsing. Confirm option tests green.
3. Switch to `BlockingConnectionPool`. Confirm pool type green.
4. Add lazy creation locking. Confirm concurrent/first-use tests green.
5. Add exhaustion mapping. Confirm bounded exhaustion green.
6. Run extension tests. Fix only Redis extension issues.
7. Run process-session tests. Fix shared-session regressions if any.
8. Run watcher cluster. Fix liveness, not test thresholds.
9. Run `bin/pytest-redis --fast -q`.
10. Run lint and compile gates.

## Fresh-Eyes Review

Potential ambiguity: should pool settings be root config or backend options?

Decision: backend options first. The root package should not grow Redis-specific
environment variables while the Redis extension is still separate and ignored.
`BROKER_BUSY_TIMEOUT` can seed the default because it already expresses a
backend wait budget.

Potential bad direction: one physical connection for consistency with SQLite
and Postgres.

Reject it. SQLite and Postgres can expose one process-local session without
serializing every operation in the same way. Redis command connections are
protocol streams. Sharing one physical Redis connection across threads safely
requires a command lock. That is exactly the wrong shape for watcher
concurrency.

Potential hidden bug: making `RedisBrokerCore.close()` disconnect the pool.

Reject it. In the process-local session path, queue operations release core
state frequently. Closing the pool there would turn every queue operation into
connection churn and would reintroduce liveness failures.

Potential hidden bug: using pool timeout as retry timeout.

Do not do that. Pool timeout only bounds waiting for a client connection. It is
not a retry policy for failed Redis commands. Keep command retries limited to
existing SimpleBroker retry behavior.

Potential hidden bug: testing pool exhaustion by sleeping while holding a high
level `redis.Redis` object.

Avoid it. Borrow a low-level connection from the pool deliberately, or use a
pipeline/pubsub state that demonstrably pins a connection. Make the test
release resources in `finally`.

Potential hidden bug: assuming Pub/Sub is covered by command-pool tests.

Reject it. Pub/Sub is intentionally outside the command pool. Keep direct
activity-waiter tests separate.

This plan still matches the previous Redis direction. It does not move toward a
materially different backend design. It makes the existing Redis-native,
process-local runner model explicit and testable.
