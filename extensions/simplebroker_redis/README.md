# simplebroker-redis

Valkey/Redis backend extension for SimpleBroker.

This package exposes the public SimpleBroker backend name `redis`. It targets
Valkey 7.x and Redis 7.x and the test suite runs against Valkey.

## Requirements

- Python 3.11+
- Valkey 7.x or Redis 7.x

Durability depends on the server configuration. A Valkey or Redis deployment
without AOF/RDB persistence can lose messages on restart. Use SQLite or
Postgres when you need storage durability from the broker stack by default.

Regular broker commands use a redis-py `BlockingConnectionPool` owned by the
process-local Redis runner. Pub/Sub wake hints use a separate dedicated
connection because subscribed Redis connections cannot serve normal commands.

Pool defaults:

- `max_connections = 50`
- `pool_timeout = BROKER_BUSY_TIMEOUT / 1000`

The defaults can be overridden in project backend options:

```toml
[backend_options]
namespace = "simplebroker_redis_v1"
max_connections = 50
pool_timeout = 5.0
```

Pool exhaustion is bounded by `pool_timeout` and surfaces as an operational
error from broker operations.

## Concurrency semantics

Queue deletion is atomic per queue. `delete()` first snapshots the queue
registry, then one Lua invocation per selected queue rechecks active
at-least-once reservations and removes that queue's pending, claimed, body,
and global-ID state together. A queue created after the registry snapshot is
outside the operation and is not deleted. If a reservation starts between
per-queue invocations, deletion stops with an error; queues already processed
remain deleted, while that reserved queue and later queues remain intact.

Patternless `broadcast()` selects the current queue registry and inserts every
copy in one Lua invocation. A queue cannot be missed or resurrected by a
concurrent write or deletion that commits before that invocation. Activity
notifications and maintenance accounting run after the atomic insert commits.

Patterned broadcasts deliberately keep a client-side queue snapshot so their
matching stays exactly Python `fnmatchcase` syntax. A queue created after the
snapshot can miss that broadcast; a queue deleted after the snapshot can be
recreated by it. Use a patternless broadcast when atomic registry selection is
required.
