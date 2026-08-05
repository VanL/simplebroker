# simplebroker-pg

Postgres backend plugin for SimpleBroker.

This package is intentionally separate from `simplebroker` itself. SimpleBroker
remains SQLite-first. This package adds a Postgres backend through the public
backend plugin hook.

## Requirements

- Python 3.11+
- PostgreSQL
- A dedicated schema for SimpleBroker tables

`public` is intentionally rejected.

Exact-target broadcast requires backend API v5: SimpleBroker 5.6.1 or newer
and `simplebroker-pg` 3.3.1 or newer. Default selection intersects requested
names with existing queues. Python `create_missing=True` instead inserts into
the complete requested set, intentionally recreating a queue deleted before
the broadcast lock is acquired. Selection and insertion occur in one
PostgreSQL transaction.

## Core Compatibility

This first-party extension moves in lockstep with the SimpleBroker backend
seam, although the core and extension package version numbers do not match.
The extension declares its backend API version independently, and SimpleBroker
checks that handshake when it resolves the plugin. An incompatible pair fails
at backend resolution with upgrade-or-pin guidance instead of running against
an unknown interface. The backend API version is separate from the PostgreSQL
storage schema version and is not stored in the database.

Use core and extension releases published together. The package dependency is
an install-time minimum; the runtime handshake is the authoritative interface
check. Install the extension through the core release's `pg` extra. See the
[backend authoring guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/backends.md#backend-authoring)
for the handshake boundary.

## Installation

```bash
# Fresh install through SimpleBroker's convenience extra
pipx install "simplebroker[pg]"

# Add to an existing pipx-installed simplebroker (recommended)
pipx inject simplebroker simplebroker-pg

# Or install through the convenience extra in a project
uv add "simplebroker[pg]"

# Or install the extension directly with uv
uv add simplebroker-pg

# Or install the extension directly with pip
pip install simplebroker-pg
```

`simplebroker[pg]` still installs this package as a separate distribution.
Postgres support is not built into the default `simplebroker` install.

## Python Usage

```python
from simplebroker import Queue
from simplebroker_pg import PostgresRunner

runner = PostgresRunner(
    "postgresql://postgres@127.0.0.1:54329/simplebroker_test",
    schema="simplebroker_app",
)

queue = Queue("jobs", runner=runner, persistent=True)
try:
    queue.write("hello")
    print(queue.read())
finally:
    queue.close()
    runner.close()
```

## Multi-Queue Activity Waiters

Postgres supports `simplebroker.create_activity_waiter_for_queues(...)` with
one process-local shared LISTEN/NOTIFY listener per DSN and schema. The waiter
wakes when any watched queue receives activity, ignores unrelated queue
notifications, and returns the same `ActivityWaiter | None` shape as the core
API.

Wakeups are hints. After `wait(timeout)` returns `True`, callers should still
drain queues through normal SimpleBroker reads or moves. Close the multi-queue
waiter explicitly when the watcher lifecycle ends.

## CLI Usage

Create `.broker.toml` in the project root, or use the configured
`BROKER_PROJECT_CONFIG_PATH` / `BROKER_PROJECT_CONFIG_NAME` location:

```toml
version = 1
backend = "postgres"
target = "postgresql://postgres@127.0.0.1:54329/simplebroker_test"

[backend_options]
schema = "simplebroker_app"
```

Then use the normal CLI from any child directory with project scope enabled:

```bash
broker init
broker write jobs hello
broker read jobs
```

You can also run entirely from environment variables without a project config:

```bash
BROKER_BACKEND=postgres \
BROKER_BACKEND_TARGET='postgresql://postgres@127.0.0.1:54329/simplebroker_test' \
BROKER_BACKEND_SCHEMA='simplebroker_app' \
BROKER_BACKEND_PASSWORD='postgres' \
broker init
```

Notes:

- In env-only backend configuration, `BROKER_BACKEND_TARGET` overrides the
  host/port/user/database parts.
- `BROKER_BACKEND_HOST`, `BROKER_BACKEND_PORT`, `BROKER_BACKEND_USER`,
  `BROKER_BACKEND_PASSWORD`, and `BROKER_BACKEND_DATABASE` are only used when
  there is no target from project config or env.
- When project TOML provides the target or schema, the project file wins.
  `BROKER_BACKEND_PASSWORD` can still be supplied from env and is never
  written to project TOML.
- The Postgres database must already exist. `broker init` creates the managed schema/tables
  inside that database; it does not create the database itself.
- Missing backend/plugin errors are distinct from target/auth errors. Invalid schema names,
  bad passwords, malformed targets, and missing databases are reported as validation or
  connection failures, not as "backend not available" errors.
