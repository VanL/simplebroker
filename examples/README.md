# SimpleBroker Extension Examples

This directory contains examples showing how to extend SimpleBroker using the extensibility API introduced in v2.0.

## ⚠️ Important Disclaimer

**These examples are for demonstration purposes only.** They intentionally omit some robustness checks, error handling, and production-level features to maintain clarity and educational focus. 

**Before using any code from these examples in production:**
- Add comprehensive error handling and recovery mechanisms
- Implement proper input validation and sanitization
- Add monitoring, logging, and alerting capabilities
- Test thoroughly under your specific workload conditions
- Consider security implications for your environment

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

### Python Extensions

- **[python_api.py](python_api.py)** - Comprehensive examples using the Python API
  - Basic queue operations (write, read, peek, move, delete)
  - Error handling patterns with retry logic
  - Custom watcher implementation with statistics
  - Checkpoint-based processing
  - Thread-safe cleanup examples

- **[logging_runner.py](logging_runner.py)** - Simple example that logs all SQL operations
  - Shows how to wrap the default SQLiteRunner
  - Demonstrates the SQLRunner protocol implementation
  - Uses the new Queue API

### Advanced Extensions

See **[example_extension_implementation.md](example_extension_implementation.md)** for comprehensive examples including:

- **Daemon Mode Runner** - Background thread processing with auto-stop
- **Async SQLite Runner** - Full async implementation with aiosqlite
- **Connection Pool Runner** - High-concurrency optimization
- **Testing with Mock Runner** - Comprehensive mock runner for unit tests
- **Complete Async Queue** - Production-ready async queue with all features

## Running the Examples

1. Basic logging example:
   ```bash
   python examples/logging_runner.py
   ```

2. For advanced examples, copy the code from `example_extension_implementation.md` and adapt as needed.

## Creating Your Own Extension

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