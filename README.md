# SimpleBroker

*A lightweight message queue backed by SQLite. No setup required, just works.*

```bash
$ pipx install simplebroker
$ broker write tasks "ship it 🚀"
$ broker read tasks
ship it 🚀
```

SimpleBroker is a zero-configuration message queue that runs anywhere Python runs. It's designed to be simple enough to understand in an afternoon, yet powerful enough for real work.

## Table of Contents

- [Features](#features)
- [Ideal Use Cases](#ideal-use-cases)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Command Reference](#command-reference)
  - [Global Options](#global-options)
  - [Commands](#commands)
  - [Command Options](#command-options)
  - [Exit Codes](#exit-codes)
- [⚠️ Critical Safety Notes](#️-critical-safety-notes)
  - [Potential Data Loss with `watch`](#potential-data-loss-with-watch)
  - [Safe Message Handling](#safe-message-handling)
- [Core Concepts](#core-concepts)
  - [Timestamps as Message IDs](#timestamps-as-message-ids)
  - [JSON for Safe Processing](#json-for-safe-processing)
  - [Checkpoint-based Processing](#checkpoint-based-processing)
- [Common Patterns](#common-patterns)
- [Real-time Queue Watching](#real-time-queue-watching)
- [Python API](#python-api)
- [Performance & Tuning](#performance--tuning)
- [Architecture & Technical Details](#architecture--technical-details)
- [Development & Contributing](#development--contributing)
- [License](#license)

## Features

- **Zero configuration** - No servers, daemons, or complex setup
- **SQLite-backed** - Rock-solid reliability with true ACID guarantees
- **Concurrent safe** - Multiple processes can read/write simultaneously
- **Simple CLI** - Intuitive commands that work with pipes and scripts
- **Portable** - Each directory gets its own isolated `.broker.db`
- **Fast** - 1000+ messages/second throughput
- **Lightweight** - ~1500 lines of code, no external dependencies
- **Real-time** - Built-in watcher for event-driven workflows

## Ideal Use Cases

- **Shell Scripting:** Decouple stages of a complex script
- **Background Jobs:** Manage tasks for cron jobs or systemd services
- **Development:** Simple message queue for local development without Docker
- **Data Pipelines:** Pass file paths or data chunks between processing steps
- **CI/CD Pipelines:** Coordinate build stages without external dependencies
- **Log Processing:** Buffer logs before aggregation or analysis
- **Simple IPC:** Communication between processes on the same machine

**Good for:** Scripts, cron jobs, small services, development  
**Not for:** Distributed systems, pub/sub, high-frequency trading

## Installation

```bash
# Use pipx for global installation (recommended)
pipx install simplebroker

# Or install with uv to use as a library
uv add simplebroker

# Or with pip
pip install simplebroker
```

The CLI is available as both `broker` and `simplebroker`.

**Requirements:**
- Python 3.8+
- SQLite 3.35+ (released March 2021) - required for `DELETE...RETURNING` support

## Quick Start

```bash
# Write a message
$ broker write myqueue "Hello, World!"

# Read the message (removes it)
$ broker read myqueue
Hello, World!

# Write from stdin
$ echo "another message" | broker write myqueue -

# Read all messages at once
$ broker read myqueue --all

# Peek without removing
$ broker peek myqueue

# Move messages between queues
$ broker move myqueue processed
$ broker move errors retry --all

# List all queues
$ broker list
myqueue: 3
processed: 1

# Broadcast to all queues
$ broker broadcast "System maintenance at 5pm"

# Clean up when done
$ broker --cleanup
```

## Command Reference

### Global Options

- `-d, --dir PATH` - Use PATH instead of current directory
- `-f, --file NAME` - Database filename or absolute path (default: `.broker.db`)
  - If an absolute path is provided, the directory is extracted automatically
  - Cannot be used with `-d` if the directories don't match
- `-q, --quiet` - Suppress non-error output
- `--cleanup` - Delete the database file and exit
- `--vacuum` - Remove claimed messages and exit
- `--version` - Show version information
- `--help` - Show help message

### Commands

| Command | Description |
|---------|-------------|
| `write <queue> <message\|->` | Add message to queue (use `-` for stdin) |
| `read <queue> [options]` | Remove and return message(s) |
| `peek <queue> [options]` | Return message(s) without removing |
| `move <source> <dest> [options]` | Atomically transfer messages between queues |
| `list [--stats]` | Show queues and message counts |
| `delete <queue> [-m <id>]` | Delete queue or specific message |
| `delete --all` | Delete all queues |
| `broadcast <message\|->` | Send message to all existing queues |
| `watch <queue> [options]` | Watch queue for new messages |

### Command Options

**Common options for read/peek/move:**
- `--all` - Process all messages
- `--json` - Output as line-delimited JSON (includes timestamps)
- `-t, --timestamps` - Include timestamps in output
- `-m <id>` - Target specific message by its 19-digit timestamp ID
- `--since <timestamp>` - Process messages newer than timestamp

**Watch options:**
- `--peek` - Monitor without consuming
- `--move <dest>` - Continuously drain to destination queue
- `--quiet` - Suppress startup message

**Timestamp formats for `--since`:**
- ISO 8601: `2024-01-15T14:30:00Z` or `2024-01-15` (midnight UTC)
- Unix seconds: `1705329000` or `1705329000s`
- Unix milliseconds: `1705329000000ms`
- Unix nanoseconds/Native hybrid: `1837025672140161024` or `1837025672140161024ns`

**Best practice:** Heuristics are used to distinguish between different values for interactive use, but explicit suffixes (s/ms/ns) are recommended for clarity if referring to particular times. 

### Exit Codes
- `0` - Success (returns 0 even when no messages match filters like `--since`)
- `1` - General error (e.g., database access error, invalid arguments)
- `2` - Queue empty or no matching messages (only when queue is actually empty or no messages match the criteria)

**Note:** The `delete` command marks messages as "claimed" for performance. Use `--vacuum` to permanently remove them.

## ⚠️ Critical Safety Notes

### Potential Data Loss with `watch`

When using `watch` in its default consuming mode, messages are **permanently removed** from the queue *before* your script or handler processes them. If your script fails or crashes, **the message is lost forever**.

**For critical data, you MUST use a safe processing pattern.**

**Recommended Safe Watch Pattern (Peek and Acknowledge):**

```bash
#!/bin/bash
# safe-worker.sh - A robust worker using the peek-and-acknowledge pattern

# Watch in peek mode, which does not remove messages
broker watch tasks --peek --json | while IFS= read -r line; do
    message=$(echo "$line" | jq -r '.message')
    timestamp=$(echo "$line" | jq -r '.timestamp')
    
    echo "Processing message ID: $timestamp"
    if process_task "$message"; then
        # Success: remove the specific message by its unique ID
        broker delete tasks -m "$timestamp"
    else
        echo "Failed to process, message remains in queue for retry." >&2
        # Optional: move to a dead-letter queue
        # echo "$message" | broker write failed_tasks -
    fi
done
```

### Safe Message Handling

Messages can contain any characters including newlines, control characters, and shell metacharacters:
- **Shell injection risks** - When piping output to shell commands, malicious message content could execute unintended commands
- **Special characters** - Messages containing newlines or other special characters can break shell pipelines that expect single-line output
- **Queue names** - Limited to alphanumeric + underscore/hyphen/period (cannot start with hyphen or period)
- **Message size** - Limited to 10MB

**Always use `--json` for safe handling** - see examples below.

## Core Concepts

### Timestamps as Message IDs
Every message receives a unique 64-bit number that serves dual purposes as a timestamp and unique message ID. Timestamps are always included in JSON output. 
Timestamps can be included in regular output by passing the -t/--timestamps flag. 

Timestamps are:
- **Unique** - No collisions even with concurrent writers (enforced by database constraint)
- **Time-ordered** - Natural chronological sorting
- **Efficient** - 64-bit integers, not UUIDs
- **Meaningful** - Can extract creation time from the ID

The format:
- High 44 bits: milliseconds since Unix epoch
- Low 20 bits: logical counter for sub-millisecond ordering
- Similar to Twitter's Snowflake IDs or UUID7


### JSON for Safe Processing

Messages with newlines or special characters can break shell pipelines. Use `--json` to avoid shell issues:

```bash
# Problem: newlines break line counting
$ broker write alerts "ERROR: Database connection failed\nRetrying in 5 seconds..."
$ broker read alerts | wc -l
2  # Wrong! One message counted as two

# Solution: JSON output (line-delimited)
$ broker write alerts "ERROR: Database connection failed\nRetrying in 5 seconds..."
$ broker read alerts --json
{"message": "ERROR: Database connection failed\nRetrying in 5 seconds...", "timestamp": 1837025672140161024}

# Parse safely with jq
$ broker read alerts --json | jq -r '.message'
ERROR: Database connection failed
Retrying in 5 seconds...
```

### Checkpoint-based Processing

Use `--since` for resumable processing:

```bash
# Save checkpoint after processing
$ result=$(broker read tasks --json)
$ checkpoint=$(echo "$result" | jq '.timestamp')

# Resume from checkpoint
$ broker read tasks --all --since "$checkpoint"

# Or use human-readable timestamps
$ broker read tasks --all --since "2024-01-15T14:30:00Z"
```

## Common Patterns

<details>
<summary>Basic Worker Loop</summary>

```bash
while msg=$(broker read work 2>/dev/null); do
    echo "Processing: $msg"
    # do work...
done
```
</details>

<details>
<summary>Multiple Queues</summary>

```bash
# Different queues for different purposes
$ broker write emails "send welcome to user@example.com"
$ broker write logs "2023-12-01 system started"
$ broker write metrics "cpu_usage:0.75"

$ broker list
emails: 1
logs: 1
metrics: 1
```
</details>

<details>
<summary>Fan-out with Broadcast</summary>

```bash
# Send to all queues at once
$ broker broadcast "shutdown signal"

# Each worker reads from its own queue
$ broker read worker1  # -> "shutdown signal"
$ broker read worker2  # -> "shutdown signal"
```

**Note:** Broadcast sends to all *existing* queues at execution time. There's a small race window for queues created during broadcast.
</details>

<details>
<summary>Unix Tool Integration</summary>

```bash
# Store command output
$ df -h | broker write monitoring -

# Process files through a queue
$ find . -name "*.log" | while read f; do
    broker write logfiles "$f"
done

# Parallel processing with xargs
$ broker read logfiles --all | xargs -P 4 -I {} process_log {}

# Remote queue via SSH
$ echo "remote task" | ssh server "cd /app && broker write tasks -"
$ ssh server "cd /app && broker read tasks"


### Integration with Unix Tools

```bash
# Pipe to queue
$ df -h | broker write monitoring -

# Store command output
$ df -h | broker write monitoring -
$ broker peek monitoring

# Process files through a queue
$ find . -name "*.log" | while read f; do
    broker write logfiles "$f"
done

# Parallel processing with xargs
$ broker read logfiles --all | xargs -P 4 -I {} process_log {}

# Use absolute paths for databases in specific locations
$ broker -f /var/lib/myapp/queue.db write tasks "backup database"
$ broker -f /var/lib/myapp/queue.db read tasks

# Reserving work using move
$ msg_json=$(broker move todo in-process --json --limit 1 2>/dev/null)
  if [ -n "$msg_json" ]; then
      msg_id=$(echo "$msg_json" | jq -r '.[0].id')
      msg_data=$(echo "$msg_json" | jq -r '.[0].data')

      echo "Processing message $msg_id: $msg_data"

      # Process the message here
      # ...

      # Delete after successful processing
      broker delete in-process -m "$msg_id"
  else
      echo "No messages to process"
  fi
```
</details>

<details>
<summary>Dead Letter Queue Pattern</summary>

```bash
# Process messages, moving failures to DLQ
while msg=$(broker read tasks); do
    if ! process_task "$msg"; then
        echo "$msg" | broker write dlq -
    fi
done

# Retry failed messages
broker move dlq tasks --all
```
</details>

<details>
<summary>Resilient Worker with Checkpointing</summary>

```bash
#!/bin/bash
# resilient-worker.sh - Process messages with checkpoint recovery

QUEUE="events"
CHECKPOINT_FILE="/var/lib/myapp/checkpoint"
BATCH_SIZE=100

# Load last checkpoint (default to 0 if first run)
last_checkpoint=$(cat "$CHECKPOINT_FILE" 2>/dev/null || echo 0)
echo "Starting from checkpoint: $last_checkpoint"

while true; do
    # Check if there are messages newer than our checkpoint
    if ! broker peek "$QUEUE" --json --since "$last_checkpoint" >/dev/null 2>&1; then
        echo "No new messages, sleeping..."
        sleep 5
        continue
    fi
    
    echo "Processing new messages..."
    
    # Process messages one at a time to avoid data loss
    processed=0
    while [ $processed -lt $BATCH_SIZE ]; do
        # Read exactly one message newer than checkpoint
        message_data=$(broker read "$QUEUE" --json --since "$last_checkpoint" 2>/dev/null)
        
        # Check if we got a message
        if [ -z "$message_data" ]; then
            echo "No more messages to process"
            break
        fi
        
        # Extract message and timestamp
        message=$(echo "$message_data" | jq -r '.message')
        timestamp=$(echo "$message_data" | jq -r '.timestamp')
        
        # Process the message
        echo "Processing: $message"
        if ! process_event "$message"; then
            echo "Error processing message, will retry on next run"
            # Exit without updating checkpoint - failed message will be reprocessed
            exit 1
        fi
        
        # Atomically update checkpoint ONLY after successful processing
        echo "$timestamp" > "$CHECKPOINT_FILE.tmp"
        mv "$CHECKPOINT_FILE.tmp" "$CHECKPOINT_FILE"
        
        # Update our local variable for next iteration
        last_checkpoint="$timestamp"
        processed=$((processed + 1))
    done
    
    if [ $processed -eq 0 ]; then
        echo "No messages processed, sleeping..."
        sleep 5
    else
        echo "Batch complete, processed $processed messages"
    fi
done
```

Key features:
- **No data loss from pipe buffering** - Reads messages one at a time
- **Atomic checkpoint updates** - Uses temp file + rename for crash safety
- **Per-message checkpointing** - Updates checkpoint after each successful message
- **Batch processing** - Processes up to BATCH_SIZE messages at a time for efficiency
- **Failure recovery** - On error, exits without updating checkpoint so failed message is retried
</details>

## Real-time Queue Watching

The `watch` command provides three modes for monitoring queues:

1. **Consume** (default): Process and remove messages from the queue
2. **Peek** (`--peek`): Monitor messages without removing them
3. **Move** (`--move DEST`): Drain ALL messages to another queue

```bash
# Start watching a queue (consumes messages)
$ broker watch tasks

# Watch without consuming (peek mode)
$ broker watch tasks --peek

# Watch with JSON output (timestamps always included)
$ broker watch tasks --json
{"message": "task 1", "timestamp": 1837025672140161024}

# Continuously drain one queue to another
$ broker watch source_queue --move destination_queue
```

The watcher uses an efficient polling strategy:
- **Burst mode**: First 100 checks with zero delay for immediate message pickup
- **Smart backoff**: Gradually increases polling interval to 0.1s maximum
- **Low overhead**: Uses SQLite's data_version to detect changes without querying
- **Graceful shutdown**: Handles Ctrl-C (SIGINT) cleanly

### Move Mode (`--move`)

The `--move` option provides continuous queue-to-queue message migration:

```bash
# Like: tail -f /var/log/app.log | tee -a /var/log/processed.log
$ broker watch source_queue --move dest_queue
```

Key characteristics:
- **Drains entire queue**: Moves ALL messages from source to destination
- **Atomic operation**: Each message is atomically moved before being displayed
- **No filtering**: Incompatible with `--since` (would leave messages stranded)
- **Concurrent safe**: Multiple move watchers can run safely without data loss

## Python API

SimpleBroker also provides a Python API for more advanced use cases:

```python
from simplebroker import Queue, QueueWatcher
import logging

# Basic usage
with Queue("tasks") as q:
    q.write("process order 123")
    message = q.read()  # Returns: "process order 123"

# Safe peek-and-acknowledge pattern (recommended for critical data)
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
    
    # Optional: After N retries, move to dead-letter queue
    # Queue("errors").write(f"{timestamp}:{message}:{exception}")
    
    return True  # Continue watching

# Use peek=True for safe mode - messages aren't removed until explicitly acknowledged
watcher = QueueWatcher(
    queue=Queue("tasks"),
    handler=process_message,
    error_handler=handle_error,
    peek=True  # True = safe mode - just observe, don't consume
)

# Start watching (blocks until stopped)
try:
    watcher.watch()
except KeyboardInterrupt:
    print("Watcher stopped by user")
```

### Thread-Based Background Processing

Use `run_in_thread()` to run watchers in background threads:

```python
from pathlib import Path
from simplebroker import QueueWatcher

def handle_message(msg: str, ts: int):
    print(f"Processing: {msg}")

# Create watcher with database path (recommended for thread safety)
watcher = QueueWatcher(
    Path("my.db"),
    "orders",
    handle_message
)

# Start in background thread
thread = watcher.run_in_thread()

# Do other work...

# Stop when done
watcher.stop()
thread.join()
```

### Async Integration Patterns

SimpleBroker is synchronous by design for simplicity, but can be easily integrated with async applications. Here's how to build an async wrapper using only stdlib:

```python
import asyncio
import concurrent.futures
from simplebroker import BrokerDB

class AsyncBroker:
    """Minimal async wrapper using thread pool executor."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._executor = concurrent.futures.ThreadPoolExecutor()
        
    async def push(self, queue: str, message: str) -> int:
        """Push message asynchronously."""
        loop = asyncio.get_event_loop()
        with BrokerDB(self.db_path) as db:
            return await loop.run_in_executor(
                self._executor, 
                db.push, 
                queue, 
                message
            )
    
    async def pop(self, queue: str) -> str | None:
        """Pop message asynchronously."""
        loop = asyncio.get_event_loop()
        with BrokerDB(self.db_path) as db:
            return await loop.run_in_executor(
                self._executor,
                db.pop,
                queue
            )

# Usage
async def main():
    broker = AsyncBroker("async.db")
    
    # Push messages concurrently
    await asyncio.gather(
        broker.push("tasks", "Task 1"),
        broker.push("tasks", "Task 2"),
        broker.push("tasks", "Task 3")
    )
    
    # Pop messages
    while msg := await broker.pop("tasks"):
        print(f"Got: {msg}")
```

**Key async integration strategies:**

1. **Thread Pool Executor**: Run SimpleBroker's sync methods in threads
2. **One DB Connection Per Operation**: Create fresh connections for thread safety
3. **Async Context Managers**: Manage lifecycle and cleanup
4. **Streaming Generators**: For continuous message consumption

See [`examples/async_wrapper.py`](examples/async_wrapper.py) for a complete async wrapper implementation including:
- Async context manager for proper cleanup
- Background watcher with asyncio coordination
- Streaming message consumption
- Concurrent queue operations

### Custom Extensions

SimpleBroker's simple design makes it easy to extend:

```python
from simplebroker import BrokerDB

class PriorityQueue(BrokerDB):
    """Example: Add priority support to queues."""
    
    def push_with_priority(self, queue: str, message: str, priority: int = 0):
        """Push message with priority (higher = more important)."""
        # Encode priority in message or use custom table
        return self.push(f"{queue}:p{priority}", message)
    
    def pop_highest_priority(self, queue_prefix: str):
        """Pop from highest priority queue first."""
        # Check queues in priority order
        for priority in range(9, -1, -1):
            msg = self.pop(f"{queue_prefix}:p{priority}")
            if msg:
                return msg
        return None
```

See [`examples/`](examples/) directory for more patterns including async processing and custom runners.

## Performance & Tuning

- **Throughput**: 1000+ messages/second on typical hardware
- **Latency**: <10ms for write, <10ms for read
- **Scalability**: Tested with 100k+ messages per queue
- **Optimization**: Use `--all` for bulk operations

### Environment Variables

<details>
<summary>Click to see all configuration options</summary>

**Core Settings:**
- `BROKER_BUSY_TIMEOUT` - SQLite busy timeout in milliseconds (default: 5000)
- `BROKER_CACHE_MB` - SQLite page cache size in megabytes (default: 10)
  - Larger cache improves performance for repeated queries and large scans
  - Recommended: 10-50 MB for typical workloads, 100+ MB for heavy use
- `BROKER_SYNC_MODE` - SQLite synchronous mode: FULL, NORMAL, or OFF (default: FULL)
  - `FULL`: Maximum durability, safe against power loss (default)
  - `NORMAL`: ~25% faster writes, safe against app crashes, small risk on power loss
  - `OFF`: Fastest but unsafe - only for testing or non-critical data

**Read Performance:**
- `BROKER_READ_COMMIT_INTERVAL` - Number of messages to read before committing in `--all` mode (default: 1)
  - Default of 1 provides exactly-once delivery guarantee
  - Increase for better performance with at-least-once delivery guarantee

**Vacuum Settings:**
- `BROKER_AUTO_VACUUM` - Enable automatic vacuum of claimed messages (default: true)
- `BROKER_VACUUM_THRESHOLD` - Number of claimed messages before auto-vacuum triggers (default: 10000)
- `BROKER_VACUUM_BATCH_SIZE` - Number of messages to delete per vacuum batch (default: 1000)
- `BROKER_VACUUM_LOCK_TIMEOUT` - Seconds before a vacuum lock is considered stale (default: 300)

**Watcher Tuning:**
- `SIMPLEBROKER_INITIAL_CHECKS` - Number of checks with zero delay (default: 100)
- `SIMPLEBROKER_MAX_INTERVAL` - Maximum polling interval in seconds (default: 0.1)

Example configurations:
```bash
# High-throughput configuration
export BROKER_SYNC_MODE=NORMAL
export BROKER_READ_COMMIT_INTERVAL=100
export SIMPLEBROKER_INITIAL_CHECKS=1000

# Low-latency configuration  
export SIMPLEBROKER_MAX_INTERVAL=0.01
export BROKER_CACHE_MB=50

# Power-saving configuration
export SIMPLEBROKER_INITIAL_CHECKS=50
export SIMPLEBROKER_MAX_INTERVAL=0.5
```
</details>

## Architecture & Technical Details

<details>
<summary>Database Schema and Internals</summary>

SimpleBroker uses a single SQLite database with Write-Ahead Logging (WAL) enabled:

```sql
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Ensures strict FIFO ordering
    queue TEXT NOT NULL,
    body TEXT NOT NULL,
    ts INTEGER NOT NULL UNIQUE,            -- Unique hybrid timestamp serves as message ID
    claimed INTEGER DEFAULT 0              -- For read optimization
);
```

**Key design decisions:**
- The `id` column guarantees global FIFO ordering across all processes
- The `ts` column serves as the public message identifier with uniqueness enforced
- WAL mode enables concurrent readers and writers
- Claim-based deletion enables ~3x faster reads
</details>

<details>
<summary>Concurrency and Delivery Guarantees</summary>

**Exactly-Once Delivery:** Read and move operations use atomic `DELETE...RETURNING` operations. A message is delivered exactly once to a consumer by default.

**FIFO Ordering:** Messages are always read in the exact order they were written to the database, regardless of which process wrote them. This is guaranteed by SQLite's autoincrement and row-level locking.

**Message Lifecycle:**
1. **Write Phase**: Message inserted with unique timestamp
2. **Claim Phase**: Read marks message as "claimed" (fast, logical delete)
3. **Vacuum Phase**: Background process permanently removes claimed messages

This optimization is transparent - messages are still delivered exactly once.
</details>

<details>
<summary>Security Considerations</summary>

- **Queue names**: Validated (alphanumeric + underscore + hyphen + period only)
- **Message size**: Limited to 10MB
- **Database files**: Created with 0600 permissions (user-only)
- **SQL injection**: Prevented via parameterized queries
- **Message content**: Not validated - can contain any text including shell metacharacters
</details>

## Development & Contributing

SimpleBroker uses [`uv`](https://github.com/astral-sh/uv) for package management and [`ruff`](https://github.com/astral-sh/ruff) for linting.

```bash
# Clone the repository
git clone git@github.com:VanL/simplebroker.git
cd simplebroker

# Install development environment
uv sync --all-extras

# Run tests
uv run pytest              # Fast tests only
uv run pytest -m ""        # All tests including slow ones

# Lint and format
uv run ruff check --fix simplebroker tests
uv run ruff format simplebroker tests
uv run mypy simplebroker
```

**Contributing guidelines:**
1. Keep it simple - the entire codebase should stay understandable
2. Maintain backward compatibility
3. Add tests for new features
4. Update documentation
5. Run linting and tests before submitting PRs

## License

MIT © 2025 Van Lindberg

## Acknowledgments

Built with Python, SQLite, and the Unix philosophy.