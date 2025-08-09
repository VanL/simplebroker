# ADVANCED: Async SimpleBroker with Connection Pooling

**NOTE: This documentation covers the ADVANCED async_pooled_broker.py example which uses
internal SimpleBroker APIs to build a custom async implementation. Most users should use:**
- **async_wrapper.py** - Simpler async wrapper around the standard Queue API (RECOMMENDED)
- **python_api.py** - Standard synchronous API examples

This directory contains a high-performance custom async implementation of SimpleBroker using `aiosqlite` and `aiosqlitepool`.
This is an advanced example for users who need custom database-level extensions beyond what the standard API provides.

## Features

- **Full async/await support** - Native asyncio implementation
- **Connection pooling** - High-performance connection pool for concurrent operations
- **Feature parity** - All SimpleBroker features including timestamps, transactions, vacuum
- **Production ready** - Comprehensive error handling and resilience
- **High throughput** - Optimized for concurrent producers and consumers

## Installation

```bash
uv add aiosqlite aiosqlitepool
```

## Quick Start

```python
import asyncio
from async_pooled_broker import AsyncQueue, async_broker

async def main():
    # Create broker with connection pool
    async with async_broker("myqueue.db", pool_size=10) as broker:
        queue = AsyncQueue("tasks", broker)
        
        # Write messages
        await queue.write("Task 1")
        await queue.write("Task 2")
        
        # Read messages
        while msg := await queue.read():
            print(f"Processing: {msg}")

asyncio.run(main())
```

## Architecture

### AsyncBrokerCore

The async version of BrokerCore that provides all queue operations:

- `write()` - Add messages to queues
- `read()` - Read and remove messages  
- `stream_read()` - Stream messages efficiently
- `list_queues()` - List all queues with counts
- `move()` - Move messages between queues
- `broadcast()` - Send to all queues
- `vacuum()` - Clean up claimed messages

### PooledAsyncSQLiteRunner

High-performance SQLite runner with connection pooling:

- Configurable pool size (default: 10)
- Thread-safe connection management
- Automatic connection recycling
- Transaction support with dedicated connections

### AsyncQueue

High-level queue interface for common operations:

```python
queue = AsyncQueue("my_queue", broker)
await queue.write("message")
msg = await queue.read()
async for msg in queue.stream():
    process(msg)
```

## Performance

The async implementation with pooling provides significant performance improvements:

- **Write throughput**: 5,000-10,000 msgs/sec
- **Read throughput**: 10,000-20,000 msgs/sec  
- **Concurrent operations**: Scales linearly with pool size
- **Low latency**: Sub-millisecond operations

## Examples

### Basic Worker Pattern

```python
async def worker(queue: AsyncQueue, worker_id: int):
    while True:
        msg = await queue.read()
        if msg is None:
            await asyncio.sleep(0.1)
            continue
        
        # Process message
        print(f"Worker {worker_id}: {msg}")
        await process_message(msg)

# Run multiple workers
workers = [worker(queue, i) for i in range(5)]
await asyncio.gather(*workers)
```

### Batch Processing

For maximum throughput, use batch commits:

```python
# Process with at-least-once delivery
async for msg in queue.stream(commit_interval=50):
    await process(msg)
    # Messages committed in batches of 50
```

### Priority Queues

```python
high = AsyncQueue("high_priority", broker)
medium = AsyncQueue("medium_priority", broker) 
low = AsyncQueue("low_priority", broker)

# Process by priority
while True:
    if msg := await high.read():
        await process_high(msg)
    elif msg := await medium.read():
        await process_medium(msg)
    elif msg := await low.read():
        await process_low(msg)
    else:
        await asyncio.sleep(0.1)
```

### Error Handling

```python
async def resilient_worker(queue: AsyncQueue, dlq: AsyncQueue):
    while True:
        msg = await queue.read()
        if msg is None:
            continue
            
        try:
            await process(msg)
        except ProcessingError:
            # Move to dead letter queue
            await dlq.write(f"FAILED:{msg}")
        except Exception as e:
            # Retry later
            await queue.write(f"RETRY:{msg}")
```

## Configuration

Environment variables:

- `BROKER_BUSY_TIMEOUT` - SQLite busy timeout (default: 5000ms)
- `BROKER_CACHE_MB` - Cache size in MB (default: 10)
- `BROKER_SYNC_MODE` - Sync mode: OFF, NORMAL, FULL, EXTRA (default: FULL)
- `BROKER_AUTO_VACUUM` - Enable auto vacuum (default: 1)
- `BROKER_AUTO_VACUUM_INTERVAL` - Vacuum check interval (default: 100)
- `BROKER_VACUUM_THRESHOLD` - Vacuum threshold percentage (default: 10)
- `BROKER_MAX_MESSAGE_SIZE` - Max message size in bytes (default: 10MB)

## Running Examples

```bash
# Run main worker example
python async_simple_example.py

# Run simple example
python async_simple_example.py simple

# Run batch processing example  
python async_simple_example.py batch

# Run comprehensive examples with benchmarks
python async_pooled_broker.py
```

## Differences from Sync Version

1. **All methods are async** - Use `await` for all operations
2. **Connection pooling** - Better concurrency than thread-local connections
3. **AsyncIterator** - Use `async for` with streaming
4. **No fork safety needed** - Asyncio is single-process
5. **Explicit initialization** - Database setup happens on first use

## Best Practices

1. **Use connection pooling** - Set pool size based on concurrent operations
2. **Batch when possible** - Use `commit_interval > 1` for bulk operations
3. **Handle timeouts** - Use `asyncio.wait_for()` to avoid blocking
4. **Graceful shutdown** - Use events to coordinate shutdown
5. **Monitor performance** - Track queue sizes and processing rates

## Integration with Existing Code

### Recommended Approach: Use async_wrapper.py

For most users, the simpler async_wrapper.py provides async functionality while using the standard API:

```python
# Standard sync code
from simplebroker import Queue
with Queue("tasks") as q:
    q.write("sync message")

# Async wrapper (RECOMMENDED)
from async_wrapper import AsyncBroker
async with AsyncBroker("broker.db") as broker:
    msg = await broker.pop("tasks")  # Gets "sync message"
```

### Advanced Approach: Custom async implementation

The custom async_pooled_broker implementation can also coexist with the sync version:

```python
# Custom async code (ADVANCED - uses internal APIs)
from async_pooled_broker import AsyncQueue, async_broker
async with async_broker("broker.db") as broker:
    queue = AsyncQueue("tasks", broker)
    msg = await queue.read()  # Gets "sync message"
```

Both implementations use the same database schema and are fully compatible.

**WARNING**: The async_pooled_broker uses internal APIs that may change between versions.
Use async_wrapper.py for production code unless you need custom extensions.