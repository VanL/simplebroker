# Async Examples

SimpleBroker's supported API is synchronous. This directory shows two distinct
ways to use it from asyncio code. They do not have the same stability boundary.

## Recommended: wrap the public API

[`async_wrapper.py`](async_wrapper.py) runs ordinary public `Queue`,
`QueueWatcher`, and `open_broker` calls in a thread pool. It has no dependency
beyond SimpleBroker and the Python standard library. It can use any backend
supported by the public target resolver.

```python
from pathlib import Path

from examples.async_wrapper import AsyncBroker


async def consume_one(root: Path) -> str | None:
    async with AsyncBroker.from_root(root) as broker:
        await broker.push("tasks", "build")
        return await broker.pop("tasks", order="newest")
```

`pop()` and `peek()` accept the public bounded `order="oldest"` default and
`order="newest"` alternative. `stream_messages()` is deliberately
oldest-only. It starts one destructive synchronous read per active iterator
step; it does not prefetch a hidden batch.

A synchronous read commits its claim before the async generator yields. If the
awaiting coroutine is cancelled while that one executor call is running, that
current row may still become claimed before cancellation is reported. Later
rows are not consumed by that iteration. Likewise, breaking after one yielded
message leaves later rows pending.

Run the recommended demo from the repository root:

```bash
uv run python examples/async_wrapper.py
```

It uses a temporary SQLite target and closes its watchers and executor.

## Advanced: pooled SQLite implementation

[`async_pooled_broker.py`](async_pooled_broker.py) is an advanced,
SQLite-specific example. It imports private SimpleBroker SQL and constants to
build a separate async queue core with `aiosqlite` and `aiosqlitepool`. It is
not a backend plugin, does not implement the synchronous `SQLRunner` extension
contract, and may need changes when SimpleBroker internals change.

In this repository, select the development extra when running it:

```bash
uv run --extra dev python examples/async_simple_example.py simple
```

For an application consuming the source outside this repository, install
`aiosqlite>=0.22.1` and `aiosqlitepool>=1.0.0` in the application environment.

### Setup ownership

The async implementation owns no schema DDL. `async_broker(...)` snapshots
configuration once, invokes the public synchronous `open_broker(...)` path in a
worker thread, waits for canonical admission and migration to finish, and only
then constructs the async pool. The production SQLite setup path owns the
cross-process phase lock. Runtime async connections perform a read-only schema
compatibility check and reject an incompatible target.

Cancelling async context entry does not stop the setup worker thread. The
context waits for that worker to finish and close before propagating
cancellation, and it does not construct the pool afterward.

Do not share a live pool across a process fork. Each process should enter its
own `async_broker(...)` context after process creation. Concurrent processes
still rely on SQLite and SimpleBroker's canonical setup/locking rules, not an
in-process `asyncio.Lock`.

### Selection and delivery

This advanced subset traverses live queues in ascending public message-ID order
only. It does not expose the bounded `order="newest"` option. Use the public
wrapper when that supported surface is required.

Ordinary `read()` and `stream(commit_interval=1)` operations commit a claim
before yielding a message. Application failure or cancellation after that
point does not restore the original pending row. A retry or dead-letter write
therefore creates a new message with a new public ID; it is not a move or
restoration of the consumed row.

With `commit_interval > 1`, the example marks a bounded batch inside one open
write transaction and yields the batch before committing it. Closing or
failing the iterator before commit rolls that batch back, which can expose
already-processed bodies again. It also holds the write transaction across
application processing. While that transaction is open, the broker instance
rejects other operations; exhaust or close the batch iterator first.
This is an advanced throughput tradeoff, not a blanket delivery guarantee.
Keep the interval at `1` unless the application has measured the benefit and
is prepared for replay and lock duration.

### Surface shown by the advanced core

- `AsyncQueue.write()`, `read()`, `read_all()`, `stream()`, `size()`, and
  `move_to()`
- queue listing and pending/total statistics
- move, broadcast, delete, and vacuum operations
- one fixed-size async SQLite connection pool

It does not reproduce every public `Queue`, watcher, command, backend, or
configuration behavior. In particular, the async core's API should not be used
as a substitute contract for SimpleBroker itself.

### Configuration

The example samples the normal `BROKER_*` configuration once at context entry.
The most relevant SQLite controls are:

- `BROKER_BUSY_TIMEOUT`
- `BROKER_CACHE_MB`
- `BROKER_SYNC_MODE`
- `BROKER_WAL_AUTOCHECKPOINT`
- `BROKER_AUTO_VACUUM`
- `BROKER_AUTO_VACUUM_INTERVAL`
- `BROKER_VACUUM_THRESHOLD`
- `BROKER_VACUUM_BATCH_SIZE`
- `BROKER_MAX_MESSAGE_SIZE`

The canonical forms and limits belong to the
[`configuration guide`](../docs/guides/configuration.md), not this example.

Run the smaller advanced demonstration from the repository root:

```bash
uv run --extra dev python examples/async_simple_example.py simple
uv run --extra dev python examples/async_simple_example.py batch
```

[`async_simple_example.py`](async_simple_example.py) calls out
claim-before-processing and oldest-only traversal in its source. Its retry and
dead-letter branches write replacement messages because the source rows were
already consumed.

## Performance

Pooling avoids opening a connection for every operation, but throughput and
latency depend on the workload, pool size, transaction duration, SQLite
settings, and storage. The repository's [`bin/benchmark.py`](../bin/benchmark.py)
measures the supported synchronous CLI and `Queue` surfaces. It does not
validate this advanced async implementation. Profile the exact application
workload before choosing the advanced path or a pool size.

## Executable evidence

From the repository root:

```bash
uv run --extra dev pytest -n0 \
  examples/test_async_pooled_broker.py \
  tests/test_example_async_stream_transitions.py \
  tests/test_sql_builder_validity.py
```

These tests exercise canonical startup and migration, caller-sidecar
preservation, commit-order ID allocation, returned-row order normalization,
recommended-wrapper bounded order, and early stream exit. They test observed
behavior without treating raw database tuple layout or engine return order as
a contract.
