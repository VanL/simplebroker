# simplebroker-redis

Valkey/Redis backend extension for SimpleBroker.

This package exposes the public SimpleBroker backend name `redis`. It targets
Valkey 7.x and Redis 7.x and the test suite runs against Valkey.

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
