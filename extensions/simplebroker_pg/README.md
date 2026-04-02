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
    "postgresql://postgres:postgres@127.0.0.1:54329/simplebroker_test",
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
target = "postgresql://postgres:postgres@127.0.0.1:54329/simplebroker_test"

[backend_options]
schema = "simplebroker_app"
```

Then use the normal CLI from any child directory with project scope enabled:

```bash
BROKER_PROJECT_SCOPE=1 python -m simplebroker.cli init
BROKER_PROJECT_SCOPE=1 python -m simplebroker.cli write jobs hello
BROKER_PROJECT_SCOPE=1 python -m simplebroker.cli read jobs
```
