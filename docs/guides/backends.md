# Backends: Postgres, Redis/Valkey, and Backend Authoring

SimpleBroker core remains SQLite-first so that basic usage has no dependencies
outside the Python standard library. This guide covers the optional Postgres
and Redis/Valkey backend extensions, backend selection through
`.broker.toml`, the cross-backend benchmark harness, and notes for backend
authors.

Normative public surfaces:
[`docs/specs/16-python-library-api.md`](../specs/16-python-library-api.md)
`[SB-API-11]` (backend seam) and related clauses.

## First-party backend extensions

If you need a different backend, use one of the first-party extension packages.
The SimpleBroker repository includes sibling Postgres and Valkey/Redis packages.
`simplebroker[pg]` and `simplebroker[redis]` are convenience extras that
install the `simplebroker-pg` / `simplebroker-redis` plugin packages for you;
both are developed in the same repository under `extensions/`.

For end users:

```bash
uv add "simplebroker[pg]"     # Postgres backend
uv add "simplebroker[redis]"  # Valkey/Redis backend
```

For local development against the sibling extension in the repository:

```bash
uv pip install -e "./extensions/simplebroker_pg[dev]"
uv pip install -e "./extensions/simplebroker_redis[dev]"
```

Explicit Python usage:

```python
from simplebroker import Queue
from simplebroker_pg import PostgresRunner

runner = PostgresRunner(
    "postgresql://postgres:postgres@127.0.0.1:54329/simplebroker_test",
    schema="simplebroker_app",
)

queue = Queue("jobs", runner=runner, persistent=True)
try:
    queue.write("hello")
finally:
    queue.close()
    runner.close()
```

When persistent queues resolve their backend from a path or project config, handles
for the same resolved backend target share process-local backend session state.
For Postgres this prevents the number of queue handles in one process from
allocating one runner or pool each. Backends that support retained thread
checkouts, including Postgres, keep one checked-out backend connection per
persistent reactor thread until the queue/session is cleaned up. Transient queue
handles still return their checkouts after each operation.

An explicitly injected `runner=` remains caller-owned. Reuse the same runner
object yourself when you want several queues to share an injected backend.
For `PostgresRunner`, call `runner.close()` or `runner.shutdown()` when you are
done with the explicitly created runner so its connection pool is closed.

## Selecting a backend with `.broker.toml`

CLI/project usage is selected through a `.broker.toml` file in the project
root:

```toml
version = 1
backend = "postgres"
target = "postgresql://postgres@127.0.0.1:54329/simplebroker_test"

[backend_options]
schema = "simplebroker_app"
```

When `.broker.toml` is present, it owns the backend target and target-shaping
options for that project. Env is still the right place for supplemental secret
material such as `BROKER_BACKEND_PASSWORD`. See the
[configuration guide](configuration.md) for discovery, precedence, and the
trust model.

## Backend authoring

Third-party backend extensions are welcome as proposed PRs or maintained
packages, but there is not yet a stable standalone backend SDK.
`simplebroker.ext` is the public surface for embedders and custom runners, while
full backend packages also use private core modules. Backend plugins declare a
code-level `backend_api_version`; core checks it during backend resolution and
release tooling verifies the first-party packages move with the core backend
seam. That handshake is not stored in broker databases or backend metadata.

Backend API v2 publicly exports `DeliveryGuarantee`,
`validate_delivery_guarantee()`, `MaintenanceSchedule`, and
`vacuum_is_eligible()` from `simplebroker.ext`. Backend packages must use these
exports instead of importing their underscore-prefixed implementation modules.

There are two backend shapes:

1. **SQL-runner-shaped backends** reuse SimpleBroker's shared `BrokerCore`.
   They provide a runner plus a SQL namespace matching the core query contract.
   Postgres is the reference implementation.
2. **Direct-core backends** implement the broker core protocol directly because
   the storage system is not SQL-shaped. Redis/Valkey is the reference
   implementation: it uses Redis data structures and Lua scripts, so forcing it
   through the SQL runner abstraction would make both correctness and
   operations worse.

**Why does Redis/Valkey use a parallel core instead of the Postgres runner
model?** Postgres is relational, so the SQL-runner contract fits. Redis is a
key/value data-structure server; a direct core can express reserved batches,
Lua-backed transitions, Pub/Sub wake hints, and namespace cleanup honestly.

The shared backend tests in the SimpleBroker repository document expected
behavior for SimpleBroker backends. They are a useful reference for extension
PRs, not a turnkey certification kit for arbitrary external packages.

### Runner sharing and transaction ownership

A runner shared across threads must preserve transaction-owner progress. After
`begin_immediate()` succeeds, another thread must not hold a runner resource
needed by the owner to reach `commit()` or `rollback()` while waiting on
storage state owned by that transaction. Implementations may satisfy this with
a transaction-scoped lock, a retained connection checkout, or an equivalent
backend mechanism. The required `SQLRunner` method set does not change.
A deliberately shared `SQLiteRunner` serializes reads and writes behind its
active transaction owner; foreign waits are bounded by the configured SQLite
busy timeout.

SQLite fork recovery assumes the child is single-threaded when a runner is
first touched. If multiple child threads race that first touch, recovery can
interleave. The bounded failure mode is an operation error or one extra
abandoned inherited connection, not reuse of a parent SQLite connection.

## Cross-backend benchmarking

Use `bin/benchmark.py` to record a best-of-three matrix across storage backend,
access type, and workload. PostgreSQL and Redis use the same automatic Docker
startup, readiness, and cleanup paths as `bin/pytest-pg` and
`bin/pytest-redis`; every sample gets a fresh SQLite file, PostgreSQL schema,
or Redis namespace.

```bash
# Fast local smoke, including every access type and workload
uv run python bin/benchmark.py --backends sqlite --operations 3

# Full 3 x 3 x 4 matrix with the optional backend packages
uv run --locked --extra pg --extra redis python bin/benchmark.py

# Retain all three raw samples and the selected best sample as JSON
uv run --locked --extra pg --extra redis \
  python bin/benchmark.py --format json > benchmark.json

# Append the SQLite-only configuration sensitivity table
uv run python bin/benchmark.py --backends sqlite \
  --access-types optimized-api --sqlite-tuning
```

The access types are `cli` (a fresh CLI process per operation), `api` (the
public `Queue` API with its default ephemeral connection behavior), and
`optimized-api` (the same API calls and payloads with `persistent=True`). The
workloads are repeated single-message `writes`, pre-seeded `reads`, repeated
non-consuming `peeks`, and `mixed`, which repeats `write`, `peek`, `read` and
counts each primitive action as one operation. Setup, seeding, queue
construction, and correctness checks are outside the timed interval. The
default is 100 operations per sample; `--operations` accepts any value of at
least three.

`--sqlite-tuning` appends a separate table that keeps the operation surface at
`optimized-api` while changing one SQLite setting at a time. It covers sync
mode, cache size, WAL auto-checkpointing, and automatic vacuum, plus one
explicit combined profile. Every row prints its configuration delta. Unsafe or
cost-shifting experiments are labeled; they are measurements, not recommended
production settings. Read commit intervals and generator batch sizes are not
included because these workloads issue one operation per API call.

PostgreSQL and Redis have no honest matching table. PostgreSQL commit policy
and Redis AOF/RDB persistence are server configuration, not equivalent
SimpleBroker settings. Redis pool size affects concurrent saturation rather
than this sequential benchmark. Presenting those as comparable “tuned” rows
would mix durability and workload contracts.

The current M4 MacBook Pro best-of-three result catalog is in the root
[`README.md`](../../README.md#performance--tuning). Treat it as a reproducible
snapshot, not a performance guarantee; rerun this script on the deployment
hardware and topology that matter to you.

Automatic vacuum is disabled uniformly so its process-local scheduling does
not charge only the persistent API path. Other SimpleBroker settings stay at
their defaults. Server durability remains backend-managed; in particular, the
temporary Valkey container does not make an AOF/RDB durability claim. CLI
results include Python startup and argument parsing. SQLite is a local file
while PostgreSQL and Redis run in Docker, so the output is useful for
access/backend comparison but is not a pure storage-engine ranking. The tool
creates only disposable benchmark targets; it does not use the application's
configured backend.

The older `tests.backend_benchmark` harness remains useful for an expanded,
CLI-only workload set. It reuses the test suite's `run_cli()` hook:

```bash
# Quick SQLite-only smoke run
uv run python -m tests.backend_benchmark --backends sqlite --iterations 1 --warmups 0

# SQLite vs Postgres vs Redis comparison with automatic Docker setup/teardown
uv run --with-editable './extensions/simplebroker_pg[dev]' \
  --with-editable './extensions/simplebroker_redis[dev]' \
  python -m tests.backend_benchmark --backends sqlite postgres redis \
  --pg-docker --redis-docker

# Machine-readable output
uv run --with-editable './extensions/simplebroker_pg[dev]' \
  --with-editable './extensions/simplebroker_redis[dev]' \
  python -m tests.backend_benchmark --backends sqlite postgres redis \
  --pg-docker --redis-docker --format json
```

That harness measures end-to-end CLI behavior for repeated single-message
`write` and `read`, bulk `read --all`, bulk `move --all`, and repeated
`--status --json` calls.

If you already have backend services running, pass `--pg-dsn` /
`--redis-url` or set `SIMPLEBROKER_PG_TEST_DSN` /
`SIMPLEBROKER_VALKEY_TEST_URL` instead of using the Docker flags.
