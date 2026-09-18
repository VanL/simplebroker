# Advanced Python API and Embedding

This guide covers the Python surface beyond the README's API tour: delivery
guarantees in practice, generators and their thread rules, watchers, async
integration, cross-queue operations, exact-ID workflows, sidecar tables, the
reactor pattern, embedding, the command layer, and extension hooks.

Normative public surfaces (package root, `simplebroker.ext`, command layer):
[`docs/specs/16-python-library-api.md`](../specs/16-python-library-api.md)
(`[SB-API-1]`–`[SB-API-13]`). Delivery rules:
[`docs/specs/11-delivery.md`](../specs/11-delivery.md)
(`[SB-DELIVERY-1]`–`[SB-DELIVERY-8]`).

## Delivery guarantees in practice

Materialized batch APIs such as `Queue.read_many()` and `Queue.move_many()`
commit before returning their result lists. They accept
`delivery_guarantee="at_least_once"` for compatibility, but it does not defer
commit or roll back when caller processing fails. A consume claim can commit
before a failed handoff; a committed move retains the message at its destination
(`[SB-DELIVERY-1]`, `[SB-DELIVERY-3]`, `[SB-DELIVERY-5]`).

Use generator APIs such as `Queue.read_generator()`, `Queue.move_generator()`,
and `Queue.stream_messages(batch_processing=True, commit_interval=N)` with
`N > 1` when you need retry-on-stop batch processing. In
`delivery_guarantee="at_least_once"` generator mode, SimpleBroker commits a
batch only after the full batch has been yielded; stopping mid-batch rolls that
batch back for retry.

Queue-owned read, move, and stream iterators are thread-affine: create,
iterate, exhaust, and close them on the same thread — and never abandon one.
Peek iterators establish their owner thread on first advancement; later
advancement, exhaustion, and close stay on that thread. Exactly-once modes
still retain the suspended outer Queue operation after each committed yield.
An abandoned generator may be finalized by the garbage collector on an
arbitrary thread, which counts as foreign-thread finalization even though you
never wrote any cross-thread code. The same applies to `sidecar()` sessions.
When a loop may exit early, close the generator explicitly:

```python
from contextlib import closing

with closing(q.read_generator(delivery_guarantee="at_least_once")) as messages:
    for message in messages:
        process(message)
        if should_stop():
            break
```

If a SQL-backed `at_least_once` generator or SQL `sidecar()` session is
nevertheless finalized from another thread, SimpleBroker records the
violation and emits a `RuntimeWarning` instead of corrupting cleanup state.
That broker instance is then permanently poisoned: core operations on it that
reach a poison check promptly raise `OperationalError` (message prefix
"cross-thread finalization", `retryable=False`) rather than blocking
indefinitely. Redis and Valkey do not use this poison mechanism; foreign-thread
use remains unsupported there. Poisoning never adds a hang to `Queue.close()`:
depending on how the handle shares its session, close returns normally
(possibly suppressing the internal error) or raises the same diagnostic. When
foreign finalization happens through a persistent shared `Queue` wrapper,
final close may first wait the existing five-second session-drain bound because
the operation lease belongs to the original thread. Recovery is restarting the
process: the interrupted batch's transaction is discarded when the process
exits, and its messages remain available for delivery afterward — they are not
lost and not silently committed. The poison state is per broker instance;
other processes or instances sharing the same SQLite database do not see it,
but their writes are already bounded by the database busy timeout and retry
budgets in the default configuration. This is a safety net, not a supported
pattern — the contract remains same-thread use. See also
[`docs/implementation/04-cross-thread-finalization-poisoning.md`](../implementation/04-cross-thread-finalization-poisoning.md).

Only `"exactly_once"` and `"at_least_once"` are valid selector values. Unknown
values raise `ValueError` before a connection or message-state mutation; lazy
generators raise on first iteration.

### Closeable Queue iterators

`Queue.read_generator()`, `Queue.peek_generator()`,
`Queue.move_generator()`, and `Queue.stream_messages()` return the
package-root `CloseableIterator`. So do the high-level `all_messages=True`
read, peek, and move views. Creating one is lazy and starts no Queue operation.
Read, move, and stream iterators must be created, advanced, exhausted, and
closed on one thread. A peek iterator establishes its owner thread on first
advancement; later advancement, exhaustion, and explicit close stay there.

Use `contextlib.closing()` when the loop may stop early:

```python
from contextlib import closing

with closing(q.peek_generator(with_timestamps=True)) as messages:
    for body, message_id in messages:
        inspect(body, message_id)
        if found_enough():
            break
```

Exhaustion means advancing until `StopIteration`; merely receiving the last
row leaves the iterator suspended. Close it before closing its Queue or
higher-level client. Cleanup ends the iterator-owned Queue operation. It does
not by itself destroy a persistent Queue's cached resources or take ownership
of a caller-injected runner. If `cleanup_connections()` was requested while the
iterator was suspended, this outermost operation exit also completes the
deferred caller-thread release. Closing the Queue releases its lease; it does
not request thread-cache cleanup. Message settlement still follows
the selected delivery mode. For peek, cleanup does not acknowledge messages or
turn live offset paging into a snapshot.

An explicit `cleanup_connections()` request remains pending until that
outermost operation exits. A failed nested Queue acquisition never closes or
releases the still-running outer operation.

Peeks can also inspect claimed (consumed but not yet vacuumed) messages:

```python
q.peek_many(10, include_claimed=True)  # pending + claimed, in message-ID order
```

Claimed rows are deletion-pending — vacuum may remove them at any time — so
`include_claimed` is an inspection tool, not delivery state.

### Write-time pending windows

For a dedicated single-producer snapshot or state feed, one write can claim
the older pending values atomically:

```python
from simplebroker import Queue

with Queue("current-state") as state:
    message_id = state.write("version 42", keep_newest=5)
```

`Queue.write(message, *, keep_newest=None)` still returns only the new message
ID. `None` is an ordinary write. Otherwise the value must be an exact `int`
from 1 through 9999; booleans and digit strings raise `TypeError`, while an
out-of-range integer raises `ValueError`, before target acquisition.

The successful operation leaves pending the highest `N` public message IDs,
including the new row. Existing claimed rows do not count. Displaced rows
become claimed and remain visible to claimed inspection until vacuum removes
them. This is a per-call transition, not a stored queue size or physical cap;
later ordinary writes, broadcasts, exact inserts, or moves may grow the
pending set again. On a shared queue it can claim another producer's work.
Concurrent operations observe a serial order around the atomic keep-write;
the work is linear in the number of displaced rows and has no bounded-time
guarantee. Normative behavior is [SB-DELIVERY-9](../specs/11-delivery.md#write-time-pending-window-sb-delivery-9).

Bounded operations can instead select the highest eligible public message ID
first:

```python
from simplebroker import Queue

with Queue("jobs") as jobs:
    jobs.write("first")
    jobs.write("second")
    newest = jobs.read_one(order="newest")
    recent = jobs.peek_many(limit=10, order="newest")
    moved = jobs.move_one("archive", order="newest")
```

`order` is available on one-message and bounded-many read, peek, and move
operations. It is not accepted by generator, all-messages, stream, or watch
forms. The default and the only alternative are `"oldest"` and `"newest"`.
Both names refer to integer public message-ID order, not physical insertion
order or wall-clock chronology. See `[SB-SELECT-5]` in
`docs/specs/14-timestamp-selection.md`.

Whole-broker backup and migration mirror the CLI:

```python
from simplebroker import dump_lines, load_lines, open_broker

with open_broker("src.db") as src, open_broker("dst.db") as dst:
    load_lines(dst, dump_lines(src, include=["tasks*"]))
```

## Finding and deleting messages by ID or content

`Queue.delete()` with no arguments deliberately removes every row in that
Queue and returns whether it removed anything. For one row, pass a concrete
integer or exact 19-digit string as `message_id`. Explicit
`Queue.delete(message_id=None)` is rejected as ambiguous before a backend
mutation; narrow optional IDs before calling.

For cleanup paths that already know many exact message IDs, use
`Queue.delete_many(message_ids)` to physically delete them in one backend-level
batch. IDs may be integers or exact 19-digit strings; duplicate IDs are counted
once after normalization.

High-level `Queue.move()` returns an ordinary mutable dictionary with
`message: str` and `timestamp: int`, described by the package-root
`MovedMessage` `TypedDict`. With `all_messages=True`, it returns an iterator of
those dictionaries. Granular `move_one`, `move_many`, and `move_generator`
retain their string and `(message, timestamp)` tuple shapes. Literal flag
values on read, peek, and move narrow through `@overload`; a runtime `bool`
keeps the safe union and does not change runtime dispatch.

For diagnostic or administrative paths that need to locate messages by body
content, use `Queue.find_message_ids(...)` and then pass the returned IDs to
`delete_many()` or another ID-based API:

```python
with Queue("tasks") as q:
    ids = q.find_message_ids(
        body_contains="order-123",
        limit=50,
    )
    q.delete_many(ids)
```

Body search is literal and case-sensitive; `%`, `_`, `*`, and other pattern
characters have no special meaning. The search string must contain at least
three non-whitespace characters and be at most 1024 characters long. The limit
must be between 1 and 1000. By default only pending messages are searched; pass
`include_claimed=True` to include claimed-but-not-vacuumed messages. This API
can scan every message in the selected queue, so keep it out of hot request
paths and prefer exact message IDs when possible.
Results are always limited in ascending public message-ID order;
`find_message_ids()` has no newest-order control.

## Serializing message IDs in application JSON

Queue, connection, and watcher callback APIs return broker message IDs and
high-water values as Python integers. SimpleBroker's built-in JSON output
already converts those values to the canonical string form. No caller action
is needed for CLI JSON, dump output, or `json_print_handler`.

When an application-owned JSON object includes one of those Python values,
format the known identity field before passing the object to an ordinary JSON
encoder:

```python
import json

from simplebroker import format_message_id

message_id = 1234567890123456789  # e.g. returned by Queue.write()
document = json.dumps(
    {
        "source_message_id": format_message_id(message_id),
    }
)
```

`format_message_id` returns a scalar string, not quoted JSON text; the JSON
encoder supplies the quotes. It accepts the exact-ID forms owned by
[`[SB-ID-4]`](../specs/13-message-identity.md#exact-id-normalization-and-insertion-sb-id-4),
while the returned JSON representation is owned by
[`[SB-ID-1]`](../specs/13-message-identity.md#representation-and-identity-sb-id-1).
Python and backend values remain integers.

Convert explicitly where your code constructs a field known to carry a broker
message ID or high-water value. Do not install a generic encoder or walk a
mapping by names such as `timestamp` or `id`: that would also change opaque
message bodies and unrelated application timestamps. The stable public import
is `simplebroker.format_message_id`; the private implementation module and
`simplebroker.ext` are not alternate import surfaces.

## Latest pending timestamp

Use `Queue.latest_pending_timestamp()` when you need the newest pending
timestamp in one queue without scanning the queue:

```python
queue = Queue("tasks")
latest = queue.latest_pending_timestamp()
if latest is None:
    print("no pending messages")
```

This returns the largest timestamp for pending, unclaimed messages in that
queue. It does not consume, claim, reserve, move, or mutate messages.

This is different from `queue.last_ts`, which is the broker-global generated
timestamp high-water mark and may refer to another queue or to a generated
timestamp with no message row.

## Generating timestamps without writing

Sometimes you need a broker-compatible timestamp/ID before enqueueing a message (for logging, correlation IDs, or backpressure planning). You can ask SimpleBroker to generate one without writing a row:

```python
queue = Queue("tasks", db_path="/path/to/.broker.db")
ts = queue.generate_timestamp()  # alias: queue.get_ts()

print(ts)  # Monotonic within a database
```

`generate_timestamp()` and `get_ts()` allocate a broker-compatible ID and
advance broker-global high-water state without writing a message row. Exact
allocation behavior is normative in `[SB-ID-2]` and `[SB-ID-3]`.

## Inserting messages with exact IDs

Use `insert_messages(...)` when your application already has the SimpleBroker
message ID and needs the row to be written with that exact ID. This covers both
dump/load restore and live protocols that preallocate an ID before the message
body is ready.

```python
from simplebroker import open_broker

with open_broker("/path/to/.broker.db") as broker:
    message_id = broker.generate_timestamp()
    broker.insert_messages([("tasks", "spawn request payload", message_id)])
```

Pass one record for a single insert, or several records for a batch:

```python
from simplebroker import open_broker

records = [
    ("tasks", "restore one", 1837025672140161024),
    ("tasks", "restore two", 1837025672140162048),
]

with open_broker("/path/to/.broker.db") as broker:
    broker.insert_messages(records)
```

`insert_messages(...)` stores caller-supplied IDs unchanged. Exact-ID
normalization, batch preflight, duplicate handling, and high-water
consequences are normative in `[SB-ID-4]`. Dump/load line format,
fresh-target policy, and cross-backend restore behavior remain with the
persistence-I/O concern (`[SB-IO-1]`–`[SB-IO-5]`).

New exact insertion requires a positive ID. A legacy dump containing ID `0`
cannot be restored unchanged; inspect the source row and intentionally assign
a compatible positive SimpleBroker ID before loading it into a new target.

> **Supply IDs allocated by a compatible SimpleBroker timestamp generator.**
> An arbitrarily far-future ID advances broker high-water into that interval
> and can make later `write()` calls fail after the logical counter is
> exhausted, until the wall clock catches up. See `[SB-ID-4]`.

For a single queue handle, pass `(message, message_id)` pairs:

```python
queue.insert_messages([("restore one", 1837025672140161024)])
```

There is no CLI surface for exact-ID inserts.

## Tracking broker-global timestamp high-water

`Queue.last_ts` is a per-handle cache of broker-global allocation high-water
state. It is not queue-local and need not identify a current message row.
`Queue.refresh_last_ts()` explicitly refreshes it. Exact cache and high-water
semantics are normative in `[SB-ID-3]`.

For long-lived watchers or background processes, force a refresh without creating a new message by calling `queue.refresh_last_ts()`, which performs a lightweight, non-blocking read of the meta table:

```python
queue = Queue("tasks")
print(queue.last_ts)  # 0 on a fresh broker target

queue.write("build artifacts ready")
print(queue.last_ts)  # Updated immediately after the write

# Later, detect external writers without adding a message
queue.refresh_last_ts()
print(queue.last_ts)
```

`Queue.last_ts` may be stale relative to other writers, including while a
watcher is running. Call `queue.refresh_last_ts()` when freshness matters.
SQLite watchers also refresh the cache when `PRAGMA data_version` reports a
change, but that does not strengthen the portable cache contract.

## Watchers in depth

The watcher uses an efficient polling strategy:

- **Burst mode**: First 100 checks with zero delay for immediate message pickup
- **Smart backoff**: Gradually increases polling interval to 0.1s maximum
- **Low overhead**: Uses SQLite's data_version to detect changes without querying
- **Graceful shutdown**: Handles SIGINT and SIGTERM cleanly

```python
import logging

from simplebroker import Queue, QueueWatcher


# Peek-and-acknowledge pattern (message stays until delete by id)
def process_message(message: str, timestamp: int):
    """Process message and acknowledge only on success."""
    logging.info(f"Processing: {message}")

    # Simulate processing that might fail
    if "error" in message:
        raise ValueError("Simulated processing failure")

    # If we get here, processing succeeded
    # Now explicitly acknowledge by deleting the message
    with Queue("tasks") as q:
        q.delete(message_id=timestamp)
    logging.info(f"Message {timestamp} acknowledged")


def handle_error(exception: Exception, message: str, timestamp: int) -> bool:
    """Log error and optionally move to dead-letter queue."""
    logging.error(f"Failed to process message {timestamp}: {exception}")
    # Message remains in queue for retry since we're using peek=True

    # Optional: After N retries, move to a dead-letter queue
    # Queue("errors").write(f"{timestamp}:{message}:{exception}")

    return True  # Continue watching


watcher = QueueWatcher(
    queue="tasks",
    handler=process_message,
    error_handler=handle_error,
    peek=True,  # observe without claiming; delete by id to acknowledge
)

# Start watching (blocks until stopped)
try:
    watcher.run_forever()
except KeyboardInterrupt:
    print("Watcher stopped by user")
```

The error callback contract is exactly
`(exception, message, timestamp)` and one handler failure invokes it at most
once. Returning `True` or `None` means “keep watching,” not “acknowledge” or
“skip.” Returning `False`, raising `StopWatching`, or raising the internal
`StopException` stops cleanly. To skip a poison message, the error callback
must explicitly delete it or move it to another queue.

If the error callback raises another ordinary exception, that exception is
terminal for the watcher run. Synchronous `run()` and `run_forever()` raise it
after cleanup, with the message-handler exception as its cause. A
`run_in_thread()` failure is left uncaught for Python's standard
`threading.excepthook`; logging is optional diagnostics, not the failure
signal. No later message is dispatched in that run. Consume claims and moves
that committed before the callback are not undone. In peek mode, progress does
not advance past the failed message, so it and later messages remain pending.

Raise `simplebroker.watcher.StopWatching` from a message handler or error
handler to stop the watcher cleanly. Handlers that catch broad `Exception`
must re-raise `StopWatching` and the internal `StopException`, which the
watcher converts, so shutdown is not swallowed.

### Move mode

`QueueMoveWatcher` (CLI: `broker watch --move DEST`) provides continuous
queue-to-queue message migration:

- **Drains entire queue**: Moves ALL messages from source to destination
- **Atomic operation**: Each message is atomically moved before being displayed
- **No filtering**: Incompatible with timestamp filters such as `--after` and `--before` (would leave messages stranded)
- **Concurrent safe**: Multiple move watchers can run safely without data loss

### Thread-based background processing

Use `run_in_thread()` to run watchers in background threads:

```python
from pathlib import Path
from simplebroker import QueueWatcher


def handle_message(msg: str, ts: int):
    print(f"Processing: {msg}")


# Create watcher with database path (recommended for thread safety)
watcher = QueueWatcher(
    "orders",
    handle_message,
    db=Path("my.db"),
)

# Start in background thread
thread = watcher.run_in_thread()

# Do other work...

# Stop when done
watcher.stop()
thread.join()
```

`watcher.is_running()` reports active execution. It is false before start,
remains true while stop cleanup is still running, and becomes false after a
normal stop or fatal exit.

The watcher run thread releases its own cached broker core when `run()` or
`run_forever()` exits. It also closes the lease of a Queue it constructed from
a name. For a Queue supplied by the caller, the watcher leaves that Queue's
lease open, but the Queue reacquires its run-thread cache on its next use there.
If `stop()` runs while the watcher is idle, it closes the polling strategy and
any Queue the watcher constructed; it does not release the thread cache of the
caller that invoked `stop()`. Close a caller-supplied Queue through its owner or
its `BrokerSession`.

That idle-stop rule also applies when the thread calling `stop()` is the
watcher's own worker. A subclass that replaces `run()` with its own loop, then
calls `stop()` from that loop's `finally`, is stopping an idle watcher as far
as `BaseWatcher` can tell, so its worker's cached core stays retained while
any other Queue or session keeps the process session alive. Release it on the
run thread before that thread exits, after the last operation has finished:

```python
class ReactorWatcher(BaseWatcher):
    def __init__(self, queue: Queue, handler) -> None:
        self.jobs = queue  # constructed by this subclass, so it owns the lease
        super().__init__(queue, handler)

    def run_forever(self) -> None:
        try:
            self.run_until_stopped()  # the subclass's own loop
        finally:
            try:
                self.jobs.cleanup_connections()  # or close a BrokerSession here
            finally:
                self.stop(join=False)
```

`cleanup_connections()` defers until an open operation on this thread unwinds
and never refuses; closing a `BrokerSession` also drops its lease but refuses
while this thread still has an open queue or connection operation on the same
target.

### Context manager support

For cleaner resource management, watchers can be used as context managers which automatically start the thread and ensure proper cleanup:

```python
import time
from simplebroker import QueueWatcher


def handle_message(msg: str, ts: int):
    print(f"Received: {msg}")


# Automatic thread management with context manager
with QueueWatcher("notifications", handle_message, db="my.db") as watcher:
    # Thread is started automatically
    # Do other work while watcher processes messages
    time.sleep(10)

# Thread is automatically stopped and joined when exiting the context
# Ensures proper cleanup even if an exception occurs
```

Context exit requests stop and join on a best-effort basis. An ordinary stop
or cleanup exception is suppressed and never replaces an exception raised by
the `with` body; failed cleanup remains retryable with a later `stop()`.
Background watcher failures still report through `threading.excepthook` and
are not replayed into the thread exiting the context.

Garbage collection is not a watcher stop mechanism. Keep explicit ownership of
the watcher and call `stop()` or use the context manager. Collecting an idle
watcher does not signal a caller-supplied Queue or move thread-local database
cleanup onto the collector thread.

## Async integration

SimpleBroker is synchronous by design for simplicity, but can be easily integrated with async applications:

```python
import asyncio
import concurrent.futures
from simplebroker import Queue


class AsyncQueue:
    """Async wrapper for SimpleBroker Queue using thread pool executor."""

    def __init__(self, queue_name: str, db_path: str = ".broker.db"):
        self.queue_name = queue_name
        self.db_path = db_path
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    async def write(self, message: str) -> int:
        """Write message asynchronously."""
        loop = asyncio.get_event_loop()

        def _write():
            with Queue(self.queue_name, db_path=self.db_path) as q:
                return q.write(message)

        return await loop.run_in_executor(self._executor, _write)

    async def read(self) -> str | None:
        """Read message asynchronously."""
        loop = asyncio.get_event_loop()

        def _read():
            with Queue(self.queue_name, db_path=self.db_path) as q:
                return q.read()

        return await loop.run_in_executor(self._executor, _read)


# Usage
async def main():
    tasks_queue = AsyncQueue("tasks")

    # Write messages concurrently
    await asyncio.gather(
        tasks_queue.write("Task 1"),
        tasks_queue.write("Task 2"),
        tasks_queue.write("Task 3"),
    )

    # Read messages
    while msg := await tasks_queue.read():
        print(f"Got: {msg}")
```

**Key async integration strategies:**

1. **Use Queue API**: Prefer the high-level Queue class for single-queue operations
2. **Thread Pool Executor**: Run SimpleBroker's sync methods in threads
3. **One Queue Per Operation**: Create fresh Queue instances for thread safety
4. **open_broker for Advanced Use**: Use `open_broker()` for cross-queue operations

See [`examples/async_wrapper.py`](../../examples/async_wrapper.py) for a complete async wrapper implementation including:
- Async context manager for proper cleanup
- Background watcher with asyncio coordination
- Bounded `pop()` and `peek()` with public `order="oldest"` / `"newest"`
- Oldest-only streaming with one destructive read per iterator step
- Concurrent queue operations

The wrapper's stream commits the current claim before yielding. Cancellation
while that one executor read is running may therefore leave that one row
claimed, but the wrapper does not prefetch or claim later rows. The separate
[`examples/async_pooled_broker.py`](../../examples/async_pooled_broker.py) is a
SQLite-only internal example, not the supported async or backend-extension
surface; see [`examples/ASYNC_README.md`](../../examples/ASYNC_README.md) before
using it.

## Cross-queue operations with open_broker

For advanced use cases requiring cross-queue access, resolve a target once and
open a backend-agnostic broker handle:

```python
from simplebroker import open_broker, target_for_directory

target = target_for_directory("/srv/myapp")

with open_broker(target) as broker:
    stats = broker.get_queue_stat("tasks")
    print(f"{stats.queue}: {stats.pending} pending")

    for queue_name in broker.list_queues(prefix="jobs."):
        print(queue_name)

    for stats in broker.list_queue_stats(prefix="jobs."):
        print(f"{stats.queue}: {stats.pending} pending")

    broker.broadcast("System maintenance at 5pm")

    recipients = [
        f"notify.{member_id}"
        for member_id in ("alice", "bob", "carol")
        if member_id != "bob"
    ]
    delivered = broker.broadcast(
        "Thread updated",
        queue_names=recipients,
        create_missing=True,
    )
    print(f"delivered to all {delivered} requested inboxes")

    cutoff_ts = broker.get_cached_last_timestamp()
    deleted = broker.delete_from_queues(
        ["jobs.high", "jobs.low"],
        before_timestamp=cutoff_ts,
    )

    renamed = broker.rename_queue("jobs.pending", "jobs.ready")
    print(f"renamed {renamed.messages_renamed} messages")

    matching_ids = broker.find_message_ids(
        "jobs.high",
        body_contains="customer-123",
        limit=50,
        before_timestamp=cutoff_ts,
    )
```

`delete_from_queues()` physically deletes matching messages from the selected
queues. Claimed and unclaimed messages are both eligible. When
`before_timestamp` is provided, the bound is strict: only messages with
`ts < before_timestamp` are deleted.

`rename_queue()` retags all existing messages, including claimed rows, from the
old queue name to the new queue name. The target queue must not already have
messages. Aliases targeting the old queue are retargeted by default; pass
`retarget_aliases=False` to leave them pointing at the old name.

`find_message_ids()` returns message IDs from one queue whose body contains a
literal, case-sensitive substring. It does not delete or mutate messages.
Timestamp bounds are strict: `after_timestamp` means `ts > after_timestamp` and
`before_timestamp` means `ts < before_timestamp`. This is intentionally an
API-only surface because it may require a full queue scan.

## Advanced: Custom Extensions

**Note:** `BrokerCore` is the shared SQL behavior layer around a supplied
runner. `BrokerDB` is its distinct SQLite-owning specialization: it resolves a
database path, creates and owns `SQLiteRunner`, leaves file permissions to the
filesystem and operator policy, and manages SQLite lifecycle. Most application
code should still compose `Queue` or
use `open_broker()`; use `BrokerDB` directly only when that low-level SQLite
ownership boundary is specifically required. Application code should not import
underscore-prefixed modules.

```python
from simplebroker import Queue


class PriorityQueueSystem:
    """Example: Priority queue system using multiple standard queues."""

    def __init__(self, db_path: str = ".broker.db"):
        self.db_path = db_path

    def write_with_priority(self, base_queue: str, message: str, priority: int = 0):
        """Write message with priority (higher = more important)."""
        queue_name = f"{base_queue}_p{priority}"
        with Queue(queue_name, db_path=self.db_path) as q:
            q.write(message)

    def read_highest_priority(self, base_queue: str) -> str | None:
        """Read from highest priority queue first."""
        # Check queues in priority order
        for priority in range(9, -1, -1):
            queue_name = f"{base_queue}_p{priority}"
            with Queue(queue_name, db_path=self.db_path) as q:
                msg = q.read()
                if msg:
                    return msg
        return None
```

The first-party backend packages use the explicit extension contracts in
`simplebroker.ext`; see the [backends guide](backends.md).
See [`examples/`](../../examples/) for application-level patterns.

## Activity waiters

Advanced watcher integrations can ask SimpleBroker for one native wake waiter
across several queues:

```python
from threading import Event

from simplebroker import Queue, create_activity_waiter_for_queues

stop_event = Event()
queues = [
    Queue("jobs.high", persistent=True),
    Queue("jobs.low", persistent=True),
]
waiter = create_activity_waiter_for_queues(queues, stop_event=stop_event)
```

The return value is `ActivityWaiter | None`. `None` means the backend has no
efficient multi-queue wake path and the caller should keep polling. A returned
waiter is only a wake hint: `wait(timeout)` means some watched queue may have
changed, not that a message is guaranteed to be available. Close the returned
waiter from the caller's watcher lifecycle; it is not owned by any one `Queue`.

An embedding that owns a `PollingStrategy` can pass a finite, non-negative
timeout to `wait_for_activity(timeout)`. The timeout bounds only a native
activity waiter that would otherwise retain control across repeated passes.
The polling fallback still performs one ordinary configured pass, even for
`timeout=0`; it does not create a second, faster SQLite polling cadence. A
native zero timeout performs one nonblocking observation.

Publish local result, stop, or signal state before calling
`strategy.notify_activity()`. The call may come from a foreign thread or
Python signal handler because it only arms one coalescing latch. The serialized
wait owner applies the existing activity bookkeeping on its next pass. With
the default strategy, first observation is bounded by one quiet pass: nominally
100 ms, plus the configured ±15% jitter and normal scheduler delay. Several
notifications may coalesce, so the caller-owned state remains the authority.

An activity waiter is a close-only leaf resource. Keep the live waiter
reference and call `close()` directly; do not keep a set of Python `id()`
values or another closed-object ledger. The first close is terminal before
backend cleanup starts. It attempts every independently safe ordinary cleanup
action, raises the first failure with later failures retained as exception
notes, and leaves every later close as a no-op. This remains true when the
first close raises, so calling `close()` again is not a cleanup retry. The
strategy or caller must serialize `wait()`, replacement, transfer, and close.
See `[SB-API-6]` in the
[Python library contract](../specs/16-python-library-api.md#watchers-and-activity-waiters-sb-api-6).

For watcher subclasses, `BaseWatcher` and `PollingStrategy` are exported from
`simplebroker.ext`. A subclass that runs its own loop instead of `run()` owns
its run thread's cache release; see the idle-stop note under
"Thread-based background processing" above.
If a subclass needs a custom native waiter, override
`BaseWatcher._create_activity_waiter(queue)` instead of copying the watcher retry
loop. If a caller-owned waiter is attached to a strategy and later closed by the
caller, use `PollingStrategy.detach_activity_waiter(expected=waiter)` first so
the strategy releases it without closing it.

After selecting a new authoritative queue set, the serialized strategy owner
can build a new fixed-set waiter and install it before the next wait without
replacing the strategy object or discarding its data-version and local-activity
state. The new candidate remains caller-owned until replacement returns
successfully:

```python
candidate = create_activity_waiter_for_queues(new_queues, stop_event=stop_event)
try:
    displaced = strategy.replace_activity_waiter(candidate)
except Exception:
    # Installation did not complete; the candidate is still caller-owned.
    if candidate is not None:
        candidate.close()  # Terminal even if this cleanup call raises.
    raise

# Installation succeeded. The strategy owns candidate, and the caller owns
# the displaced waiter returned from the previous generation.
if displaced is not None:
    displaced.close()
```

No identity deduplication is required during handoff. If two owner paths
defensively call `close()` on the same live waiter, the second call has no
effect. Keep the live object reference for the handoff itself; Python `id()`
is unique only during that object's lifetime and can be recycled later.

`None` is a valid candidate and selects polling fallback, as it does for
SQLite. PostgreSQL and Redis/Valkey return fixed-set waiters, so rebuild them
when the authoritative queue set changes. Replacement never closes the
displaced waiter. During a handoff, the installed waiter may briefly coexist
with an uninstalled candidate or a displaced caller-owned waiter. The strategy
owner must serialize replacement with `wait_for_activity()`, `start()`,
`close()`, and other replacements; the method is not a cross-thread handoff
primitive.

Passing the object that is already installed is an exact no-op. This includes
passing `None` while polling fallback is already active: the method returns
`None`, makes no state change, and does not reset the wait cadence. A distinct
replacement preserves the data-version cache and local-activity hints, but
resets polling backoff and the backend-native generation for responsiveness.
Coalesce superseded topology generations before building and installing new
waiters so rapid changes do not repeatedly restart that cadence.

## PostgreSQL connection pressure

Normative behavior: `[SB-API-13]` in the
[Python library contract](../specs/16-python-library-api.md#first-party-postgresql-inspection-sb-api-13).

`Queue.backend_name` lets an embedder narrow to a backend-specific capability
without adding that capability to every backend. The PostgreSQL extension
exposes the connection-pressure helper at its package root:

```python
from simplebroker_pg import get_connection_stats

if queue.backend_name == "postgres":
    pressure = get_connection_stats(queue)
    observed = pressure["numbackends"]
```

The result contains exactly `numbackends`, `max_connections`,
`superuser_reserved_connections`, and `reserved_connections`. `numbackends`
is the unfiltered server-wide sum from `pg_stat_database`, so an ordinary role
can observe established connections owned by other roles and databases without
a monitoring grant or installed function. The tradeoff is deliberate: it can
also include autovacuum and other database-attached workers that do not consume
a `max_connections` client slot. Treat it as a conservative pressure signal,
not an exact client count.

The helper uses the Queue's normal connection lease, lock, and retry path. A
target-resolved persistent Queue reuses its thread's process-session core and,
on PostgreSQL, borrows a checkout for the operation. An ephemeral Queue
releases its operation-owned connection afterward. The helper does not use
sidecar and creates no table, function, role, grant, or schema object.

This is an observation, not a reservation. Other clients can connect after the
statement. Admission control must retain a safety margin and tolerate both
early rejection from worker overcount and concurrent overshoot.

## Sidecar tables

Normative packaging: `[SB-API-7]` in
[`docs/specs/16-python-library-api.md`](../specs/16-python-library-api.md).

Embedding applications sometimes need a few of their own tables living in the
broker's database — operational state that should share the broker's durability
and backups without a second storage system. The sidecar API supports exactly
that, through the broker's own locking and retry discipline:

```python
from simplebroker import Queue

q = Queue("jobs", db_path="app.db")

# Transactional writes: BEGIN IMMEDIATE through the broker's retry loop,
# commit on clean exit, rollback if the block raises.
with q.sidecar(transaction=True) as s:
    s.run("CREATE TABLE IF NOT EXISTS myapp_state (k TEXT PRIMARY KEY, v TEXT)")
    s.run("INSERT INTO myapp_state (k, v) VALUES (?, ?)", ("cursor", "42"))

# Autocommit reads/writes: each statement retried on lock contention.
with q.sidecar() as s:
    rows = list(s.run("SELECT v FROM myapp_state WHERE k = ?", ("cursor",), fetch=True))
```

Rules of the road:

- **Prefix your tables** (`myapp_...`) and never touch the broker's own tables —
  see `simplebroker.ext.RESERVED_TABLE_NAMES`.
- SQL migration may rebuild only those reserved broker objects. Successful or
  failed migration leaves caller-owned sidecar definitions, rows, indexes,
  constraints, and sequence state unchanged. Dependencies on a retired private
  broker column are unsupported. **SQLite:** migration does not block on them;
  attached indexes and triggers are dropped with the old broker table, while a
  detached view or foreign-key definition may survive but be broken. Take a
  whole-file backup before migrating a target that may contain them.
  **PostgreSQL:** `RESTRICT` makes the dependency fail without mutation, and
  migration never uses `CASCADE`. Columns or constraints added inside
  `messages`, `meta`, the aliases table, or broker-owned indexes are
  unsupported reserved-object changes, not sidecars.
- Connection lifetime is backend-specific. SQLite persistent queues retain a
  caller-thread connection in the cached core until explicit cleanup or final
  shared-session shutdown. PostgreSQL persistent queues share a bounded
  process-session pool and automatically return each completed operation's
  checkout; callers do not check connections in or out. Closing a persistent
  Queue releases only its lease. `cleanup_connections()` and
  `BrokerSession.recycle_thread()` remain core-cache controls, but completed
  PostgreSQL operations do not need them to free pool capacity.
- Use `?` (qmark) placeholders. They work natively on SQLite and are translated
  by the Postgres backend (where sidecar tables live in the broker's configured
  schema). Other SQL dialect differences are yours to manage.
- The Redis backend has no SQL storage: `sidecar()` raises
  `SidecarUnavailableError` there. Catch it to probe the capability.
- Don't nest sidecar transactions and don't call queue operations inside a
  `sidecar(transaction=True)` block on the same persistent handle — SQLite
  cannot nest write transactions.
- On PostgreSQL, use this sidecar API rather than a separate raw connection and
  keep `transaction=True` blocks short. Sidecars opened from a persistent Queue
  or `BrokerSession` share that process session's three default command
  checkouts. An open transaction retains one until commit or rollback; enough
  retained transactions make later operations on that session wait and
  eventually fail at the checkout timeout. Non-transactional sidecar statements
  return their checkouts automatically. An ephemeral Queue owns a separate
  runner for its sidecar session and is outside the shared process-session
  ceiling.
- Schema setup: idempotent `CREATE TABLE IF NOT EXISTS` (plus additive
  `ALTER TABLE`) inside a `transaction=True` session is race-safe across
  processes.

## Reactor pattern

Applications that combine worker threads with sidecar tables should keep broker
handles and durable writes on one owning thread, then pass broker-free work
between threads with Python `queue.Queue`.
[`examples/reference_reactor.py`](../../examples/reference_reactor.py) is the
copyable reference for that shape.

The example layers a reusable `BaseReactor` on
[`examples/multi_queue_watcher.py`](../../examples/multi_queue_watcher.py), then
shows one concrete `Reactor` policy with:

- fixed, pairwise-distinct input, output, and control lanes;
- retained peek discovery plus a durable terminal-state ledger for
  input/control queues;
- informational maximum checkpoints that do not filter discovery;
- at-least-once exact-ID output replay through a durable sidecar outbox;
- broker-free worker payloads and result envelopes; and
- short sidecar transactions on the reactor thread.

This is not a database lease. SimpleBroker already supports many processes
using the same SQLite broker database through WAL, short write transactions, and
retry on contention. Each discovery pass starts at the lowest public message ID
and uses only pass-local paging. Terminal `reactor_seen` state suppresses
completed work. This is required because exact insertion, load, or an
ID-preserving move can add a lower pending ID after a larger checkpoint was
recorded. A stale `inflight` row is redispatchable after restart;
`result_recorded`, `output_written`, and `control_processed` are terminal for
discovery. Two live reactors watching the same lane add another duplicate
execution path. Output replay is also at-least-once: exact-ID insert handles a
matching replay collision, while an occupant with a different body raises and
leaves the pending outbox row unchanged. A crash after the outbox write and
before the sidecar `output_written` mark can replay the output if a downstream
consumer already vacuumed the claimed output row.
Make processors and control commands idempotent, deduplicate downstream by
output message ID rather than payload, and prefer one logical reactor per
workstream when duplicate execution or non-idempotent side effects matter.
Each pending row retains its recorded output queue. If a restart configures a
different route, replay raises and leaves the row pending instead of silently
rerouting it. In background mode this ends the drive thread and its finalizer
closes reactor resources; restore the recorded topology or migrate the sidecar
row explicitly before restarting. Control messages may be plain-text commands
or JSON objects. Other valid JSON shapes receive an error reply and are
checkpointed so they cannot poison the lane.
If output replay is stuck, the example backpressures new input dispatch but
keeps the control lane responsive; `STATUS` exposes `pending_output_backlog` and
`output_backlog_blocked`, and `STOP` still works. A pending control message caps
output replay to a small budget for that turn rather than starving it entirely.
The budget bounds rows returned and materialized, not the underlying SQLite scan
without a supporting index. Complete discovery also revisits retained source
history, so a long-running application needs a retention or compaction policy.
Constructing `Reactor` is not side-effect-free: it creates its `reactor_*`
sidecar schema, loads informational checkpoints, and starts idle workers.
Pending outputs replay on the first driven turn.

Run the focused tests with:

```bash
uv run pytest -n0 examples/tests
```

## Embedding SimpleBroker in Your Project

For embedded use, the current best practice is to put a small project-level
client or context object in front of SimpleBroker. Let that object resolve the
broker target once, translate your application's settings into `BROKER_*`
config keys, and hand out queues bound to that target. Application code should
call the client instead of open-coding `Queue(...)` across the codebase.

For a worker or task scope that uses several queues, `BrokerSession` names the
lifetime of their shared process resources:

```python
from simplebroker import BrokerSession

with BrokerSession.connect(target, config=config) as session:
    jobs = session.queue("jobs")
    failures = session.queue("failures")
    jobs.write("render invoice")
```

The `with` block must run and exit on the thread whose cache it owns. Exit
recycles that thread's cached core, closes every Queue minted by the session,
and drops the session's own lease. Closing the session from a different thread
recycles only that other thread's cache. If another live handle or Queue shares
the same resolved target and configuration, `session.recycle_thread()` affects
its cache on the calling thread too: there is one cache per process-session key
and thread, not one per handle. Use `session.connection()` for broadcast,
statistics, and alias operations that should share the same runner or pool.
Create a fresh session in a forked child; inherited session handles reject use.
Close all Queue iterators and exit all Queue or session connection contexts on
the same process-session key before the session exits. This includes operations
opened through a sibling handle or a directly constructed persistent Queue;
closing the session while one is active on the calling thread is rejected.

A retained minted Queue can be used after the session closes, like any closed
persistent Queue that is reused. If no other lease remains, that operation
starts a new process session; `queue.session` identifies the closed handle only
while the handle is still alive.

`Queue.conn` is deprecated compatibility access. Use `queue.session` when you
need the `BrokerSession` that minted a Queue, `session.connection()` for
connection-level work sharing that session, or `open_broker()` when the work
should own an independent connection. No runtime warning is emitted in 8.3.x.

Weft is the reference implementation of this pattern. Its public
`WeftClient` owns a resolved `WeftContext`; `WeftContext.queue(name)` constructs
`Queue(name, db_path=context.broker_target, config=context.broker_config)`, and
`WeftContext.broker()` uses `open_broker()` for backend-agnostic cross-queue
operations. That keeps SQLite, Postgres, and Redis/Valkey selection behind one
client contract.

The same shape works for smaller projects:

```python
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from simplebroker import (
    BrokerTarget,
    Queue,
    Config,
    open_broker,
    resolve_config,
    target_for_directory,
)


@dataclass(frozen=True)
class AppBrokerClient:
    target: BrokerTarget
    config: Config

    @classmethod
    def from_root(cls, root: str | Path, **overrides: Any) -> "AppBrokerClient":
        root_path = Path(root)
        (root_path / ".myapp").mkdir(parents=True, exist_ok=True)
        config = resolve_config(
            override={
                "BROKER_PROJECT_CONFIG_PATH": ".myapp",
                "BROKER_PROJECT_CONFIG_NAME": "broker.toml",
                "BROKER_DEFAULT_DB_NAME": ".myapp/broker.db",
                **overrides,
            }
        )
        return cls(target_for_directory(root_path, config=config), config)

    def queue(self, name: str, *, persistent: bool = False) -> Queue:
        return Queue(
            name,
            db_path=self.target,
            persistent=persistent,
            config=self.config,
        )

    def broker(self):
        return open_broker(self.target, config=self.config)


client = AppBrokerClient.from_root("/srv/myapp")
client.queue("jobs").write("render invoice")

with client.broker() as broker:
    print(broker.get_queue_stat("jobs"))
```

The stable embedding surface is the public package API exported from
`simplebroker` plus the extension contracts in `simplebroker.ext`. Treat
underscore-prefixed modules and raw storage details as implementation. If your
application has its own environment namespace, use the
[shared configuration resolver](configuration.md#shared-configuration-for-embedders)
with that prefix and explicit `env`. Copy `DEFAULT_CONFIG` to change defaults
or add user validators. Pass the returned Config directly through all handles;
custom fields remain available and no downstream boundary rereads environment.
Omit env for isolation. Avoid importing `simplebroker._constants` or guessing
database paths.

Configuration passed to a Queue, watcher, or broker must be a `Config`, or
`TypeError` is raised; the instance retains that object. Convert mappings with `resolve_config(override=namespaced_values)`
before passing them. Operational methods use the retained snapshot unless an
explicit per-call Config is supplied. Such a Config replaces the snapshot for
that operation. To derive a partial override, resolve it explicitly with
`resolve_config(config=base_config, override=namespaced_changes)`. A watcher given an existing
Queue inherits that Queue's snapshot unless an explicit Config replaces it.
Explicit watcher config changes watcher-local policy, not the supplied Queue's
retained operation config. Handles without a Config use defaults and do not
read `BROKER_*` environment variables; to honor them, pass
`resolve_config(env=os.environ)` once at startup. Target and Queue
representations, plus cross-target errors, redact connection passwords and all
backend-option values. `serialize_broker_target()` is different: it is a
lossless process-transport payload, may contain credentials, and must not be
logged or exposed.

For a child process, use `serialize_config(config)` to send JSON and
`deserialize_config(payload, defaults=fields)` to rebuild configuration with the
child's local declarations. Both helpers are exported from `simplebroker`.
The receiver preserves the namespace and validates values without reading env
or TOML. Pass declarations explicitly for application-specific validation;
JSON transport excludes validators and live Queue/session resources. Recreate handles in
the child using the restored Config and separately transported BrokerTarget.
See [configuration transport](configuration.md#shared-configuration-for-embedders)
for the sender/receiver example and supported JSON values. Config transport,
like target transport, may carry credentials and must not be logged.

For trusted Python spawn arguments, Config also supports ordinary pickle,
preserving declarations and importable validator references without revalidation.
Lambdas and local functions remain unpicklable under normal Python rules.

### Command layer

Normative: `[SB-API-10]` in
[`docs/specs/16-python-library-api.md`](../specs/16-python-library-api.md)
(and CLI presentation in [`docs/specs/10-cli.md`](../specs/10-cli.md)).

`simplebroker.commands` is supported public embedding surface: the programmatic
equivalent of the CLI. Each `cmd_*` function mirrors one CLI subcommand — it
prints the same payload and returns an integer code for ordinary outcomes (`0`
success or `2` not found / queue empty). Invalid input and operational failures
raise their Python exceptions. The CLI alone turns those failures into a
diagnostic and process exit `1`. Import the functions directly and drive the
broker without shelling out:

```python
from simplebroker.commands import cmd_write, cmd_read, cmd_list

db = "/srv/myapp/.myapp/broker.db"

cmd_write(db, "jobs", "render invoice")  # -> 0
rc = cmd_read(db, "jobs")  # prints the message, returns 0 (or 2 if empty)
cmd_list(db)  # prints queue names, returns 0
```

Exact-ID selector conflicts raise before opening the target, and an exact-ID
delete requires a queue. `cmd_load` raises its original `ValueError`,
`IntegrityError`, or `TimestampError`; the shell CLI adds the `broker load:`
context and recovery hint.

The five streaming command functions (`cmd_read`, `cmd_peek`, `cmd_move`,
`cmd_dump`, and `cmd_watch`) treat a closed stdout consumer as a clean stop and
return `0`. Every other output-producing command function returns `1` with a
plain or JSON error diagnostic when result delivery fails. `cmd_write` and
`cmd_rename` may have committed before that failure; inspect state before a
retry. The internal closed-pipe signal never escapes this public layer.

Where a command accepts `quiet`, it suppresses only its owned human
commentary. It does not install a blanket `RuntimeWarning` filter. Plain
message output warns once per invocation for embedded newlines; JSON message
records do not warn.

Error and process-signal translation belongs to the CLI wrapper, not ordinary
direct `cmd_*` calls. The wrapper returns `130` when an unhandled
`KeyboardInterrupt` reaches it; `cmd_watch` retains its own normal-stop
handling and returns `0`.

The names in `simplebroker.commands.__all__` are stable under the same
compatibility policy as the package's other public exports.

`DatabaseError` is importable from `simplebroker.ext`. It is the common base
for SimpleBroker's package-defined `OperationalError`, `IntegrityError`, and
`DataError` storage exceptions:

```python
from simplebroker.ext import DatabaseError
```

`BrokerError` remains the root of every package-defined SimpleBroker exception.
It is not an exhaustive catch for every runtime failure: connection-retry
exhaustion, fork-safety violations, and repeated timestamp-conflict exhaustion
can raise plain `RuntimeError`. `DatabaseError` no longer subclasses
`OSError`; catch `OSError` separately for filesystem and process failures.
