# MultiQueueWatcher Examples

This directory contains a demonstration-only `MultiQueueWatcher`: an extension
that monitors multiple queues on one broker target and dispatches them from one
thread with round-robin scheduling. Its consume path claims a row before the
application handler runs.

## 📁 Files

- **`multi_queue_watcher.py`** - Complete MultiQueueWatcher implementation with example
- **`reference_reactor.py`** - Sidecar-aware single-writer reactor layered on MultiQueueWatcher
- **`multi_queue_patterns.py`** - Usage patterns and techniques
- **`MULTI_QUEUE_README.md`** - This documentation file

## 🏗️ Architecture Overview

MultiQueueWatcher extends `BaseWatcher` to provide:

```
┌─────────────────────────────────────┐
│         MultiQueueWatcher           │
├─────────────────────────────────────┤
│ • Single dispatch thread/target     │
│ • Round-robin fairness              │
│ • Per-queue handlers                │
│ • Inherited BaseWatcher features    │
│   - Polling strategy                │
│   - Error handling                  │
│   - Lifecycle management            │
│   - Signal handling (SIGINT)        │
└─────────────────────────────────────┘
```

### Design Principles

1. **Shared target**: All managed queues preserve the same public path or
   `BrokerTarget`. Persistent handles may share process-local backend resources
   through SimpleBroker; the example does not promise one transaction spanning
   queue operations.

2. **Round-Robin Processing**: Active queues are processed in round-robin order, preventing queue starvation.

3. **Activity detection**: Uses the selected backend's activity/version
   mechanism. SQLite uses `PRAGMA data_version`; other backends own their
   corresponding waiter or polling behavior.

4. **Single dispatch thread**: Application handlers run serially in this
   watcher. Other processes and handles can still race for broker rows, and the
   backend still uses its normal locking and retry rules.

### Reactor Reference

`reference_reactor.py` shows the stricter pattern for applications that need worker
threads plus sidecar tables. `BaseReactor` is the reusable seam: it owns the
single-thread process/wait/stop loop, local activity wakeups, and resource-close
ordering. `Reactor` is the concrete demo policy layered on top: it owns input
completion state, informational checkpoints, output replay, control replies,
and the worker pool.

The reactor thread owns the reactor's persistent `Queue` handles and is the only
normal writer to the reactor sidecar tables. Other threads may use their own
short-lived `Queue` handles to submit input, send control messages, or inspect
results, but they must not share the reactor's owned handles. Workers receive
`WorkItem` dataclasses through Python `queue.Queue` and return `WorkerResult`
dataclasses the same way. This is a single-writer reactor with broker-free
workers. It is not a database lease: SimpleBroker owns storage-level
multi-process contention with short SQLite write transactions and retry. The
reactor contract is about logical workstream ownership.

The operational contract is:

1. `process_once()`, `run_until_stopped()`, and `start()` are single-owner drive
   paths. Do not drive the same reactor from two threads.
2. Shutdown is two phase. `_reactor_stop_event` stops the reactor loop without
   interrupting durable writes; the inherited `_stop_event` is set only during
   final resource close.
3. A joining `stop()` waits for an active drive thread before closing reactor
   queue handles. If an external caller uses `stop(join=False)`, the drive
   thread's `run_until_stopped()` finalizer performs the close when it exits.
4. Source and control queues are retained with peeks. Each discovery pass starts
   at the lowest public message ID and uses pass-local bounds only for paging.
   Terminal `reactor_seen` rows, not a numeric checkpoint, suppress completed
   work. Complete scans revisit retained history, so production code needs a
   retention or compaction policy; scan cost grows with uncompacted rows.
5. Output publication is at-least-once with exact-ID idempotent replay. The
   reactor records a pending result row, publishes the exact output message ID,
   then marks it written. On an exact-ID collision, replay accepts the occupant
   only when its body matches the pending payload, including when the occupant
   is already claimed. The write and sidecar mark are separate commits. If a
   crash lands between
   them and a downstream consumer has already vacuumed the claimed output row,
   replay can deliver the same logical output again. Downstream consumers should
   deduplicate by output message ID rather than payload; message bodies are
   payload only and may legally duplicate byte-for-byte. Each pending row keeps
   its recorded output queue. A configured-route mismatch raises and leaves the
   row pending. In background mode the error ends the drive thread and the
   reactor finalizer closes its owned resources. Restore the recorded topology
   or migrate the row explicitly before restarting.
6. Many processes may use the same broker database. A stale persisted
   `inflight` row is redispatched after restart; `result_recorded`,
   `output_written`, and `control_processed` are terminal for discovery. More
   than one reactor watching the same input or control lane adds another
   duplicate execution path. Prefer one logical reactor per workstream when
   duplicate pure work or non-idempotent side effects matter.
7. Control replies are at-least-once. A crash after writing the reply and before
   recording `control_processed` can produce a duplicate reply after restart.
   Plain-text commands and JSON objects are accepted. Other valid JSON shapes,
   including quoted command strings, receive an error reply and are checkpointed.
8. Worker count gives cross-queue parallelism, not unlimited per-queue
   parallelism. Each input queue has one in-flight message to preserve order.
9. The instance is one-shot. Construct, run, stop, then dispose it. `BaseReactor`
   seals inherited dynamic queue mutators by default because role-aware dynamic
   lanes must also update checkpoint and sidecar state. All input, output,
   control-input, and control-output role names must be pairwise distinct;
   construction rejects overlaps before opening resources.
10. A stuck processor can stall its source queue. Production code should add
    worker deadlines or an in-flight reaper if processors are not tightly
    bounded.
11. Do not hold long SQLite write transactions across worker CPU or IO time.
    This design keeps workers off broker handles and lets the reactor thread do
    short durable turns so SimpleBroker's contention retry model can work.
    Single-thread reactor ownership avoids shared-runner lock inversion only
    while every broker effect stays on that thread. Do not call a queue
    operation through another same-target persistent handle from inside
    `sidecar(transaction=True)`; allocate queue IDs before entering that
    sidecar transaction and re-check durable state after entry.
12. A stuck output replay backpressures new input dispatch, but it must not make
    the control lane unresponsive. `STATUS` reports `pending_output_backlog` and
    `output_backlog_blocked`; `STOP` still works while the output sink is stuck.
    Pending control traffic caps output replay to a small budget for that turn
    rather than starving it entirely. The budget bounds rows returned and
    materialized, not the underlying SQLite scan without a supporting index.
13. Constructing `Reactor` creates durable schema, loads checkpoints, and starts
    idle worker threads. The first `process_once()` or background drive turn
    replays pending output before dispatching new input. Do not treat construction
    as a side-effect-free configuration step.

To build a new reactor, subclass `BaseReactor` and keep broker effects on the
reactor thread. Override `_drain_reactor_results()` for broker-free worker
results, `_drain_reactor_backlog()` for durable retry rows, `_drain_queue()` for
the queue policy, and `_close_reactor_resources()` for extra owned handles.

## 🚀 Quick Start

```python
from multi_queue_watcher import MultiQueueWatcher


# Define handlers for different queue types
def orders_handler(message: str, timestamp: int) -> None:
    print(f"🛒 Processing order: {message}")


def notifications_handler(message: str, timestamp: int) -> None:
    print(f"📧 Sending notification: {message}")


# Create watcher with queue-specific handlers
watcher = MultiQueueWatcher(
    queues=["orders", "notifications", "analytics"],
    queue_handlers={
        "orders": orders_handler,
        "notifications": notifications_handler,
        # 'analytics' will use default_handler
    },
    db="my_app.db",
)

# Start processing
watcher.start()

# Stop when done (or use context manager)
watcher.stop()
```

## 📋 Usage Patterns

### Pattern 1: Basic Multi-Queue Setup

```python
# Different handlers for different message types
watcher = MultiQueueWatcher(
    queues=["urgent", "normal", "audit"],
    queue_handlers={
        "urgent": urgent_handler,
        "normal": normal_handler,
        "audit": audit_handler,
    },
)
```

### Pattern 2: Priority Queue Simulation

```python
class PriorityMultiQueueWatcher(MultiQueueWatcher):
    def _drain_queue(self) -> None:
        # Process high-priority queue 3x more often
        if "high_priority" in self._active_queues:
            for _ in range(3):
                # Process high priority message
                pass
        # Then process other queues normally
```

### Pattern 3: Load Balancing

```python
# Distribute similar work across multiple worker queues
watcher = MultiQueueWatcher(
    queues=["worker_a", "worker_b", "worker_c"], default_handler=process_work_item
)

# Round-robin distributes work fairly across workers
```

### Pattern 4: Queue-Specific Error Handling

```python
def custom_error_handler(exc: Exception, message: str, timestamp: int) -> bool:
    queue_name = get_current_queue()  # Custom logic
    if "critical" in queue_name:
        return False  # Stop on critical errors
    else:
        return True  # Continue on non-critical errors
```

### Pattern 5: Monitoring and Metrics

```python
class MonitoredMultiQueueWatcher(MultiQueueWatcher):
    def _dispatch(self, message: str, timestamp: int, *, config=None) -> None:
        start_time = time.time()
        if config is not None:
            super()._dispatch(message, timestamp, config=config)
        else:
            super()._dispatch(message, timestamp)
        self.record_metrics(time.time() - start_time)
```

## 🔧 Configuration Options

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `queues` | `list[str]` | Required | list of queue names to monitor |
| `default_handler` | `Callable` | local `print_message` | Default message handler |
| `queue_handlers` | `dict[str, Callable]` | `None` | Queue-specific handlers |
| `db` | `str/Path/BrokerTarget` | `None` | Public broker target or path |
| `error_handler` | `Callable` | `default_error_handler` | Error handling function |
| `polling_strategy` | `PollingStrategy` | `None` | Custom polling strategy |
| `check_interval` | `int` | `10` | How often to check inactive queues |

### Handler Function Signature

```python
def handler(message: str, timestamp: int) -> None:
    """
    Args:
        message: The message body as a string
        timestamp: SimpleBroker's unique 64-bit timestamp ID
    """
    pass
```

### Error Handler Signature

```python
def error_handler(exc: Exception, message: str, timestamp: int) -> bool | None:
    """
    Args:
        exc: The exception that occurred
        message: The message being processed when error occurred
        timestamp: The message timestamp
        
    Returns:
        True: Continue processing other messages
        False: Stop the watcher
        None: Continue processing (same as True)
    """
    pass
```

## ⚡ Performance Characteristics

### Single Dispatch Thread

- Handlers do not run in parallel inside one watcher.
- Slow handlers delay every queue handled by that watcher.
- Backend locking, contention, and cross-process races still apply.

### Shared Database Benefits

- **Target identity**: Every managed Queue uses the same normalized public target
- **Connection reuse**: Persistent handles may share process-local resources
- **Activity detection**: Uses the selected backend's waiter/version mechanism
- **Operation boundary**: Each Queue call keeps its own transaction semantics

### Scalability Profile

The watcher stores one Queue entry per configured name and checks active queues
every turn plus inactive queues at `check_interval`. Memory and scan work grow
with queue count. Measure with the intended backend and workload; at higher
counts, consider separate watcher groups.

## 🎯 When to Use MultiQueueWatcher

### Use For:
- **Multiple queue types** with different processing logic
- **Round-robin processing** requirements (no queue starvation)
- **Resource-constrained** environments
- **Moderate message volume** per queue
- **Shared processing infrastructure**

### Consider Alternatives For:
- **Single queue** - use `QueueWatcher`
- **High-throughput single queue** - use `QueueWatcher` 
- **CPU-intensive processing** - consider multiple processes
- **Independent queue processing** - use multiple `QueueWatcher` instances

## 🧪 Running the Examples

### Complete Example
```bash
cd examples/
python multi_queue_watcher.py
```

This runs a demonstration showing:
- Queue setup with different handlers
- Round-robin message processing
- Processing demonstration
- Statistics and monitoring

### Pattern Examples
```bash
cd examples/
python multi_queue_patterns.py
```

This demonstrates five usage patterns:
1. Basic multi-queue setup
2. Priority queue simulation  
3. Queue-specific error handling
4. Load balancing patterns
5. Monitoring and metrics

### Expected Output

```
🚀 MultiQueueWatcher Example
==================================================
📁 Using temporary database: /tmp/.../multi_queue_example.db

📦 Populating queues with sample messages...
   Added 3 messages to 'orders' queue
   Added 3 messages to 'notifications' queue
   Added 3 messages to 'analytics' queue

🔧 Creating MultiQueueWatcher for queues: ['orders', 'notifications', 'analytics', 'logs', 'default']

▶️  Starting MultiQueueWatcher...

🛒 [ORDER] Processing order #1001 for $29.99 at 1837025672140161024
📧 [NOTIFICATION] EMAIL: Welcome to our service! at 1837025672140161025
📊 [ANALYTICS] Event 'page_view' from user user123 at 1837025672140161026
🛒 [ORDER] Processing order #1002 for $149.99 at 1837025672140161027
📧 [NOTIFICATION] SMS: Your order has shipped at 1837025672140161028
...
```

## 🔍 Implementation Details

### Queue Discovery Algorithm

```python
def _update_active_queues(self) -> None:
    # 1. Check currently active queues
    still_active = [q for q in self._active_queues if has_messages(q)]
    
    # 2. Periodically check inactive queues
    if self._check_counter % self._check_interval == 0:
        for inactive_queue in inactive_queues:
            if has_messages(inactive_queue):
                still_active.append(inactive_queue)
    
    # 3. Update round-robin iterator if queues changed
    if still_active != self._active_queues:
        self._active_queues = still_active
        self._queue_iterator = itertools.cycle(still_active)
```

### Message Processing Flow

```
1. PollingStrategy detects activity (any queue has changes)
2. _update_active_queues() refreshes active queue list
3. Round-robin through active queues:
   a. Read one message from queue
   b. Switch to queue-specific handler
   c. Call inherited _dispatch() for size validation & error handling
   d. Restore original handler
4. Remove empty queues from active list
5. Notify PollingStrategy of activity
```

### Error Isolation

Consume commits the claim before dispatch. If one queue's handler fails:

1. The failed message remains claimed. It is not restored or retried.
2. The queue's error handler decides whether the watcher stops or continues.
3. If it continues, the round-robin scheduler may dispatch later queues.
4. Applications that need retry should first move work to an inflight queue and
   settle it explicitly, or own another durable retry design.

## 🤝 Contributing

To extend or improve MultiQueueWatcher:

1. **Extend the base class** for custom behavior
2. **Override `_drain_queue()`** for custom scheduling
3. **Add metrics collection** in `_dispatch()`
4. **Implement custom error recovery** in error handlers
5. **Add queue lifecycle management** for dynamic queues

### Example Extension

```python
class CustomMultiQueueWatcher(MultiQueueWatcher):
    def _drain_queue(self) -> None:
        # Custom processing logic
        pass

    def add_queue(self, queue_name: str, handler: Callable) -> None:
        # Dynamic queue addition
        pass

    def remove_queue(self, queue_name: str) -> None:
        # Dynamic queue removal
        pass
```

## 📚 Related Documentation

- **[SimpleBroker README](../README.md)** - Core SimpleBroker documentation
- **[QueueWatcher Examples](simple_watcher_example.py)** - Single-queue watcher examples
- **[Watcher API contract](../docs/specs/16-python-library-api.md#watchers-and-activity-waiters-sb-api-6)** - Public root and extension imports

---

*MultiQueueWatcher provides multi-queue message processing in SimpleBroker applications.*
