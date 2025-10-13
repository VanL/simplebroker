# SimpleBroker Examples

This directory contains examples for using SimpleBroker, from basic usage to advanced extensions.

## Quick Start - Recommended Examples

**For most users, start with these examples:**

1. **[python_api.py](python_api.py)** - Standard Python API usage (RECOMMENDED STARTING POINT)
   - Uses the public `Queue` and `QueueWatcher` classes
   - Shows all common operations and patterns
   - Production-ready code examples

2. **[async_wrapper.py](async_wrapper.py)** - Simple async wrapper (RECOMMENDED FOR ASYNC)
   - Wraps the standard Queue API for async/await usage
   - Uses thread pool executor for compatibility
   - No external dependencies beyond asyncio

## Advanced Examples

These examples show how to extend SimpleBroker using internal APIs or the extensibility API:

## ⚠️ Important Disclaimer

**These examples are for demonstration purposes only.** They intentionally omit some robustness checks, error handling, and production-level features to maintain clarity and educational focus. 

**Before using any code from these examples in production:**
- Add comprehensive error handling and recovery mechanisms
- Implement proper input validation and sanitization
- Add monitoring, logging, and alerting capabilities
- Test thoroughly under your specific workload conditions
- Consider security implications for your environment

## Standalone Utilities

### [sqlite_connect.py](sqlite_connect.py) - SQLite Connection Utilities

**[sqlite_connect.py](sqlite_connect.py)** extracts SQLite connection management patterns from SimpleBroker into a standalone module. It provides thread-safe connection handling, path validation, and database setup utilities that can be used in other projects.

Features include thread-local connections, fork safety detection, cross-platform file locking, path security validation, and WAL mode setup. The module includes comprehensive error handling and retry logic for database contention.

**[test_sqlite_connect.py](test_sqlite_connect.py)** contains the test suite for the utility.

## Security Considerations

When working with message queues:
- Messages can contain untrusted data including newlines and shell metacharacters
- Always use `--json` output with proper JSON parsing (e.g., `jq`) in shell scripts
- Validate and sanitize all message content before processing
- Never use `eval` or similar dynamic execution on message content
- Implement proper access controls and authentication where needed

## Examples

### Bash Scripts

- **[resilient_worker.sh](resilient_worker.sh)** - Production-ready message processor with checkpoint recovery
  - Implements peek-and-acknowledge pattern to prevent data loss
  - Atomic checkpoint updates for crash safety
  - Per-message checkpointing with graceful shutdown
  - Automatic retry on failure

- **[dead_letter_queue.sh](dead_letter_queue.sh)** - Dead letter queue patterns for handling failures
  - Simple DLQ with retry mechanisms
  - Retry tracking with configurable limits
  - Time-based retry delays with exponential backoff
  - Queue monitoring and alerting patterns

- **[queue_migration.sh](queue_migration.sh)** - Message migration between queues
  - Simple queue renaming
  - Filtered migrations based on content
  - Time-based migrations
  - Safe transformation during migration (no eval)
  - Queue splitting and merging patterns

- **[work_stealing.sh](work_stealing.sh)** - Load balancing and work distribution
  - Round-robin distribution
  - Load-based task assignment
  - Work stealing between workers
  - Priority-based distribution
  - Multi-worker simulation with monitoring

### Python Examples

#### Standard API (Recommended)

- **[python_api.py](python_api.py)** - Comprehensive examples using the standard Python API
  - Basic queue operations with `Queue` class (write, read, peek, move, delete)
  - Error handling patterns with retry logic
  - Custom watcher implementation with `QueueWatcher`
  - Checkpoint-based processing
  - Thread-safe cleanup examples
  - **START HERE for Python usage**

- **[simple_watcher_example.py](simple_watcher_example.py)** - Default handlers demonstration
  - Shows `simple_print_handler`, `json_print_handler`, and `logger_handler`
  - Examples of building custom handlers using defaults as building blocks
  - Good introduction to watcher patterns before diving into python_api.py

- **[multi_queue_watcher.py](multi_queue_watcher.py)** - Multi-queue processing with fairness
  - Complete `MultiQueueWatcher` implementation for monitoring multiple queues
  - Single-thread, shared-database design for efficiency
  - Round-robin fairness prevents queue starvation
  - Per-queue handlers with fallback to default
  - See **[MULTI_QUEUE_README.md](MULTI_QUEUE_README.md)** for detailed documentation

- **[multi_queue_patterns.py](multi_queue_patterns.py)** - Advanced multi-queue usage patterns
  - Priority queue simulation
  - Load balancing across worker queues
  - Queue-specific error handling strategies
  - Monitoring and metrics collection
  - Dynamic queue management patterns

- **[async_wrapper.py](async_wrapper.py)** - Async wrapper around standard API
  - Simple async/await interface using thread pool
  - Works with standard `Queue` and `QueueWatcher` classes
  - No external dependencies
  - **USE THIS for async applications**

#### Advanced Extensions

- **[logging_runner.py](logging_runner.py)** - Custom SQLRunner extension (ADVANCED)
  - Shows how to wrap the default SQLiteRunner
  - Demonstrates the SQLRunner protocol implementation
  - For users who need custom database middleware

### Advanced Extensions

See **[example_extension_implementation.md](example_extension_implementation.md)** for comprehensive examples including:

- **Daemon Mode Runner** - Background thread processing with auto-stop
- **Async SQLite Runner** - Full async implementation with aiosqlite
- **Connection Pool Runner** - High-concurrency optimization
- **Testing with Mock Runner** - Comprehensive mock runner for unit tests
- **Complete Async Queue** - Production-ready async queue with all features

### Custom Async Implementation (Advanced)

**WARNING: These examples use internal APIs and are for advanced users only.**
**For standard async usage, use [async_wrapper.py](async_wrapper.py) instead.**

- **[async_pooled_broker.py](async_pooled_broker.py)** - Custom async implementation (ADVANCED)
  - Uses internal SimpleBroker APIs to build custom async broker
  - Requires aiosqlite and aiosqlitepool
  - Full AsyncBrokerCore implementation with feature parity
  - Connection pooling for optimal concurrency
  - **Only use if async_wrapper.py doesn't meet your needs**
  
- **[async_simple_example.py](async_simple_example.py)** - Examples using the custom async implementation
  - Uses the advanced async_pooled_broker
  - Worker pattern with async/await
  - Batch processing examples
  - **Consider async_wrapper.py first**

- **[ASYNC_README.md](ASYNC_README.md)** - Documentation for custom async implementation
  - Covers the advanced async_pooled_broker approach
  - Installation and setup for custom implementation
  - Performance benchmarks
  - **Most users should use async_wrapper.py instead**

## Running the Examples

1. Basic logging example:
   ```bash
   python examples/logging_runner.py
   ```

2. High-performance async examples:
   ```bash
   # Install dependencies
   uv add aiosqlite aiosqlitepool
   
   # Run comprehensive async examples with benchmarks
   python examples/async_pooled_broker.py
   
   # Run simple async worker example
   python examples/async_simple_example.py
   
   # Run batch processing example
   python examples/async_simple_example.py batch
   ```

3. For advanced examples, copy the code from `example_extension_implementation.md` and adapt as needed.

## Using SimpleBroker - Standard Approach

For most users, use the standard API:

```python
# Standard synchronous usage
from simplebroker import Queue, QueueWatcher

with Queue("myqueue") as q:
    q.write("Hello, World!")
    msg = q.read()
    print(msg)

# For watching queues
watcher = QueueWatcher(
    db=".broker.db",
    queue="myqueue",
    handler=lambda msg, ts: print(f"Got: {msg}")
)
watcher.run_in_thread()
```

For async applications, use the async wrapper:

```python
from async_wrapper import AsyncBroker

async with AsyncBroker("broker.db") as broker:
    await broker.push("myqueue", "Hello async!")
    msg = await broker.pop("myqueue")
```

## Creating Your Own Extension (Advanced)

Only create custom extensions if the standard API doesn't meet your needs.

To create a custom runner:

1. Import the necessary components:
   ```python
   from simplebroker.ext import SQLRunner, OperationalError, IntegrityError
   ```

2. Implement the SQLRunner protocol:
   ```python
   class MyRunner(SQLRunner):
       def run(self, sql, params=(), *, fetch=False):
           # Your implementation
           pass
       
       def begin_immediate(self):
           # Start transaction
           pass
       
       def commit(self):
           # Commit transaction
           pass
       
       def rollback(self):
           # Rollback transaction
           pass
       
       def close(self):
           # Cleanup
           pass
   ```

3. Use with the Queue API:
   ```python
   from simplebroker import Queue
   
   runner = MyRunner(config)
   with Queue("myqueue", runner=runner) as q:
       q.write("Hello from custom runner!")
   ```

## Important Notes

- All extensions MUST use `TimestampGenerator` for timestamp consistency
- Raise `OperationalError` for retryable conditions (locks, busy database)
- Raise `IntegrityError` for constraint violations
- Be thread-safe if used in multi-threaded contexts
- Be fork-safe (detect and recreate connections after fork)

See the extensibility specification for complete details.