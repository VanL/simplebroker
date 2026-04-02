# simplebroker-pg

Postgres backend plugin for SimpleBroker.

This package is intentionally separate from `simplebroker` itself. SimpleBroker
remains SQLite-first. This package adds a Postgres backend through the public
backend plugin hook.

## Requirements

- PostgreSQL
- A dedicated schema for SimpleBroker tables

`public` is intentionally rejected.

## Installation

```bash
uv pip install -e "./extensions/simplebroker_pg[dev]"
```

## Python Usage

```python
from simplebroker import Queue
from simplebroker_pg import PostgresRunner

runner = PostgresRunner(
    "postgresql://postgres@127.0.0.1:54329/simplebroker_test",
    schema="simplebroker_app",
)

queue = Queue("jobs", runner=runner, persistent=True)
queue.write("hello")
print(queue.read())
```

## CLI Usage

Create `.simplebroker.toml` in the project root:

```toml
version = 1
backend = "postgres"
target = "postgresql://postgres@127.0.0.1:54329/simplebroker_test"

[backend_options]
schema = "simplebroker_app"
```

Then use the normal CLI from any child directory with project scope enabled:

```bash
BROKER_PROJECT_SCOPE=1 python -m simplebroker.cli init
BROKER_PROJECT_SCOPE=1 python -m simplebroker.cli write jobs hello
BROKER_PROJECT_SCOPE=1 python -m simplebroker.cli read jobs
```

You can also run entirely from environment variables without a project config:

```bash
BROKER_BACKEND=postgres \
BROKER_BACKEND_TARGET='postgresql://postgres@127.0.0.1:54329/simplebroker_test' \
BROKER_BACKEND_SCHEMA='simplebroker_app' \
BROKER_BACKEND_PASSWORD='postgres' \
python -m simplebroker.cli init
```

Notes:

- `BROKER_BACKEND_TARGET` overrides the whole target string.
- `BROKER_BACKEND_HOST`, `BROKER_BACKEND_PORT`, `BROKER_BACKEND_USER`,
  `BROKER_BACKEND_PASSWORD`, and `BROKER_BACKEND_DATABASE` are only used when there is no
  target from env or toml.
- `BROKER_BACKEND_PASSWORD` is never written to `.simplebroker.toml`.
- The Postgres database must already exist. `broker init` creates the managed schema/tables
  inside that database; it does not create the database itself.
- Missing backend/plugin errors are distinct from target/auth errors. Invalid schema names,
  bad passwords, malformed targets, and missing databases are reported as validation or
  connection failures, not as "backend not available" errors.
