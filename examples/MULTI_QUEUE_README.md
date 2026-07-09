# MultiQueueWatcher Examples

This directory contains examples demonstrating the **MultiQueueWatcher** class - an extension of SimpleBroker that monitors and processes multiple queues in a single thread with round-robin scheduling.

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
│ • Single thread, single DB          │
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

1. **Single Database Connection**: All queues share one database connection, avoiding the N-connection overhead of multiple `QueueWatcher` instances.

2. **Round-Robin Processing**: Active queues are processed in round-robin order, preventing queue starvation.

3. **Activity Detection**: Uses `PRAGMA data_version` to detect changes across all queues in the shared database.

4. **Single-Threaded**: Eliminates thread synchronization, context switching, and race conditions.

### Reactor Reference

`reference_reactor.py` shows the stricter pattern for applications that need worker
threads plus sidecar tables. `BaseReactor` is the reusable seam: it owns the
single-thread process/wait/stop loop, local activity wakeups, and resource-close
ordering. `Reactor` is the concrete demo policy layered on top: it owns input
checkpoints, sidecar rows, output replay, control replies, and the worker pool.

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
4. Source and control queues are read with peek-plus-sidecar checkpoints. This
   preserves queue rows; production code needs a retention or compaction policy.
5. Output publication is at-least-once with exact-ID idempotent replay. The
   reactor records a pending result row, publishes the exact output message ID,
   then marks it written. Exact-ID insert handles the normal replay collision,
   but the write and sidecar mark are separate commits. If a crash lands between
   them and a downstream consumer has already vacuumed the claimed output row,
   replay can deliver the same logical output again. Downstream consumers should
   deduplicate by output message ID.
6. Many processes may use the same broker database. Source and control lanes are
   already at-least-once because they use peek-plus-checkpoint semantics: a
   restart can re-run uncheckpointed work even with one reactor. More than one
   reactor watching the same input or control lane adds another duplicate
   execution path. Prefer one logical reactor per workstream when duplicate pure
   work or non-idempotent side effects matter.
7. Control replies are at-least-once. A crash after writing the reply and before
   checkpointing the control input can produce a duplicate reply after restart.
8. Worker count gives cross-queue parallelism, not unlimited per-queue
   parallelism. Each input queue has one in-flight message to preserve order.
9. The instance is one-shot. Construct, run, stop, then dispose it. `BaseReactor`
   seals inherited dynamic queue mutators by default because role-aware dynamic
   lanes must also update checkpoint and sidecar state.
10. A stuck processor can stall its source queue. Production code should add
    worker deadlines or an in-flight reaper if processors are not tightly
    bounded.
11. Do not hold long SQLite write transactions across worker CPU or IO time.
    This design keeps workers off broker handles and lets the reactor thread do
    short durable turns so SimpleBroker's contention retry model can work.
12. A stuck output replay backpressures new input dispatch, but it must not make
    the control lane unresponsive. `STATUS` reports `pending_output_backlog` and
    `output_backlog_blocked`; `STOP` still works while the output sink is stuck.
    Pending control traffic caps output replay to a small budget for that turn
    rather than starving it entirely.
13. Constructing `Reactor` performs durable setup and pending-output replay, then
    starts worker threads. Do not treat construction as a side-effect-free
    configuration step.

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
    queues=['orders', 'notifications', 'analytics'],
    queue_handlers={
        'orders': orders_handler,
        'notifications': notifications_handler,
        # 'analytics' will use default_handler
    },
    db='my_app.db'
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
    queues=['urgent', 'normal', 'audit'],
    queue_handlers={
        'urgent': urgent_handler,
        'normal': normal_handler,
        'audit': audit_handler,
    }
)
```

### Pattern 2: Priority Queue Simulation

```python
class PriorityMultiQueueWatcher(MultiQueueWatcher):
    def _drain_queue(self) -> None:
        # Process high-priority queue 3x more often
        if 'high_priority' in self._active_queues:
            for _ in range(3):
                # Process high priority message
                pass
        # Then process other queues normally
```

### Pattern 3: Load Balancing

```python
# Distribute similar work across multiple worker queues
watcher = MultiQueueWatcher(
    queues=['worker_a', 'worker_b', 'worker_c'],
    default_handler=process_work_item
)

# Round-robin distributes work fairly across workers
```

### Pattern 4: Queue-Specific Error Handling

```python
def custom_error_handler(exc: Exception, message: str, timestamp: int) -> bool:
    queue_name = get_current_queue()  # Custom logic
    if 'critical' in queue_name:
        return False  # Stop on critical errors
    else:
        return True   # Continue on non-critical errors
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
| `default_handler` | `Callable` | `simple_print_handler` | Default message handler |
| `queue_handlers` | `dict[str, Callable]` | `None` | Queue-specific handlers |
| `db` | `str/Path/BrokerDB` | `None` | Database path or instance |
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

### Single Thread Benefits

- **Memory Efficiency**: No per-thread stack allocation (saves 1-8MB per thread)
- **CPU Cache**: No context switching between threads
- **Latency**: No thread scheduling jitter
- **No Synchronization**: No locks, race conditions, or deadlocks

### Shared Database Benefits

- **Connection Efficiency**: One SQLite connection vs. N connections
- **Transaction Consistency**: All queues in same transaction scope
- **Activity Detection**: `PRAGMA data_version` detects changes across all queues
- **Resource Management**: Single connection to manage

### Scalability Profile

| Queue Count | Memory Usage | Performance | Complexity |
|-------------|--------------|-------------|------------|
| 1-10 | O(1) | High | Low |
| 10-50 | O(1) | Good | Low |
| 50+ | O(1) | Moderate* | Medium |

*At higher queue counts, consider batching or hierarchical organization

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

Each queue's processing is isolated - if one queue's handler fails:
1. Error is logged and handled per error_handler
2. Processing continues with next queue in round-robin
3. Failed queue remains in active list for retry
4. Other queues are unaffected

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
- **[BaseWatcher API](../simplebroker/watcher.py)** - Base class documentation

---

*MultiQueueWatcher provides multi-queue message processing in SimpleBroker applications.*
