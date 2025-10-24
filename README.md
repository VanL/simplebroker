# SimpleBroker

  [![CI](https://github.com/VanL/simplebroker/actions/workflows/test.yml/badge.svg)](https://github.com/VanL/simplebroker/actions/workflows/test.yml)
  [![codecov](https://codecov.io/gh/VanL/simplebroker/branch/main/graph/badge.svg)](https://codecov.io/gh/VanL/simplebroker)
  [![PyPI version](https://badge.fury.io/py/simplebroker.svg)](https://badge.fury.io/py/simplebroker)
  [![Python versions](https://img.shields.io/pypi/pyversions/simplebroker.svg)](https://pypi.org/project/simplebroker/)

*A lightweight message queue backed by SQLite. No setup required, just works.*

```bash
$ pipx install simplebroker
$ broker write tasks "ship it 🚀"
$ broker read tasks
ship it 🚀
```

SimpleBroker is a zero-configuration, no-dependency message queue that runs anywhere Python runs. It's designed to be simple enough to understand in an afternoon, yet powerful enough for real work.

## Table of Contents

- [Features](#features)
- [Use Cases](#use-cases)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Command Reference](#command-reference)
  - [Global Options](#global-options)
  - [Commands](#commands)
  - [Command Options](#command-options)
  - [Exit Codes](#exit-codes)
- [Critical Safety Notes](#️-critical-safety-notes)
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
- [Project Scoping](#project-scoping)
  - [Basic Project Scoping](#basic-project-scoping)
  - [Project Database Names](#project-database-names)
  - [Error Behavior](#error-behavior-when-no-project-database-found)
  - [Project Initialization](#project-initialization)
  - [Precedence Rules](#precedence-rules)
  - [Security Notes](#security-notes)
  - [Common Use Cases](#common-use-cases)
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
- **Lightweight** - ~2500 lines of code, no external dependencies
- **Real-time** - Built-in watcher for event-driven workflows

## Use Cases

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
- Python 3.10+
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

# list all queues
$ broker list
myqueue: 3
processed: 1

# Broadcast to all queues
$ broker broadcast "System maintenance at 5pm"
# Target only matching queues using fnmatch-style globs
$ broker broadcast --pattern 'jobs-*' "Pipeline paused"

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
- `--status` - Show global message count, last timestamp, and DB size (`--status --json` for JSON output)
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
| `delete <queue> [-m <id>]` | Delete queue or specific message (marks for removal; use `--vacuum` to reclaim space) |
| `delete --all` | Delete all queues (marks for removal; use `--vacuum` to reclaim space) |
| `broadcast <message\|->` | Send message to all existing queues |
| `watch <queue> [options]` | Watch queue for new messages |
| `alias <add|remove|list|->` | Manage queue aliases |
| `init [--force]` | Initialize SimpleBroker database in current directory (does not accept `-d` or `-f` flags) |

#### Queue Aliases

Use aliases when two agents refer to the same underlying queue with different names. Aliases are stored in the database, persist across processes, and update atomically.

```bash
$ broker alias add task1.outbox agent1-to-agent2
$ broker alias add task2.inbox agent1-to-agent2
$ broker write @task1.outbox "Job ready"
$ broker read @task2.inbox
Job ready
$ broker alias list
task1.outbox -> agent1-to-agent2
task2.inbox -> agent1-to-agent2
$ broker alias list --target agent1-to-agent2
task1.outbox -> agent1-to-agent2
task2.inbox -> agent1-to-agent2
$ broker write task1.outbox "goes to literal queue"
$ broker read task1.outbox
goes to literal queue
$ broker alias remove task1.outbox
```

- Plain queue names (`task1.outbox`) always refer to the literal queue. Use the
  `@` prefix (`@task1.outbox`) to opt into alias resolution—if the alias is not
  defined the command fails.
- Alias names are plain queue names (no `@` prefix); when *using* an alias on the CLI, prefix it with `@`.
- Use `alias list --target <queue>` to see which aliases point to a specific queue (reverse lookup).
- A target must be a real queue name (not another alias). Attempts to alias an alias or create cycles raise `ValueError`.
- Removing an alias does not affect stored messages; they remain under the canonical queue name.

### Command Options

**Common options for read/peek/move:**
- `--all` - Process all messages (CLI moves up to 1,000,000 per invocation; rerun for larger queues or use the Python API generators)
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
- `2` - Queue empty, no matching messages, or invalid message ID format (only when queue is actually empty, no messages match the criteria, or the provided message ID has an invalid format)

**Note:** The `delete` command marks messages as "claimed" for performance. Use `--vacuum` to permanently remove them.

## Critical Safety Notes

### Safe Message Handling

Messages can contain any characters including newlines, control characters, and shell metacharacters:
- **Shell injection risks** - When piping output to shell commands, malicious message content could execute unintended commands
- **Special characters** - Messages containing newlines or other special characters can break shell pipelines that expect single-line output
- **Queue names** - Limited to alphanumeric + underscore/hyphen/period (cannot start with hyphen or period)
- **Message size** - Limited to 10MB

**Always use `--json` for safe handling** - see examples below.

### Robust message handling with `watch`

When using `watch` in its default consuming mode, messages are **permanently removed** from the queue *before* your script or handler processes them. If your script fails or crashes, **the message is lost**. For critical data, you must use a safe processing pattern (move or peek-then-delete) that ensures that your data is not removed until you can acknowledge receipt. Example:

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
- High 52 bits: microseconds since Unix epoch
- Low 12 bits: logical counter for sub-microsecond ordering
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

**Alias interaction:** Broadcast operations ignore aliases and work only on literal queue names. Pattern matching with `--pattern` matches queue names, not alias names.
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
$ msg_json=$(broker move todo in-process --json 2>/dev/null)
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
from simplebroker.db import DBConnection
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
```

### Generating timestamps without writing

Sometimes you need a broker-compatible timestamp/ID before enqueueing a message (for logging, correlation IDs, or backpressure planning). You can ask SimpleBroker to generate one without writing a row:

```python
with DBConnection("/path/to/.broker.db") as conn:
    db = conn.get_connection()
    ts = db.generate_timestamp()  # alias: db.get_ts()

queue = Queue("tasks", db_path="/path/to/.broker.db")
ts2 = queue.generate_timestamp()  # alias: queue.get_ts()

print(ts2 > ts)  # Monotonic within a database
```

Notes:
- Timestamps are monotonic per database and match what `Queue.write()` uses internally.
- Generating a timestamp does not reserve a slot; it simply gives you the next ID.
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

### Context Manager Support

For cleaner resource management, watchers can be used as context managers which automatically start the thread and ensure proper cleanup:

```python
import time
from simplebroker import QueueWatcher

def handle_message(msg: str, ts: int):
    print(f"Received: {msg}")

# Automatic thread management with context manager
with QueueWatcher("my.db", "notifications", handle_message) as watcher:
    # Thread is started automatically
    # Do other work while watcher processes messages
    time.sleep(10)
    
# Thread is automatically stopped and joined when exiting the context
# Ensures proper cleanup even if an exception occurs
```

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
        
    async def write(self, message: str) -> None:
        """Write message asynchronously."""
        loop = asyncio.get_event_loop()
        def _write():
            with Queue(self.queue_name, self.db_path) as q:
                q.write(message)
        await loop.run_in_executor(self._executor, _write)
    
    async def read(self) -> str | None:
        """Read message asynchronously."""
        loop = asyncio.get_event_loop()
        def _read():
            with Queue(self.queue_name, self.db_path) as q:
                return q.read()
        return await loop.run_in_executor(self._executor, _read)

# Usage
async def main():
    tasks_queue = AsyncQueue("tasks")
    
    # Write messages concurrently
    await asyncio.gather(
        tasks_queue.write("Task 1"),
        tasks_queue.write("Task 2"),
        tasks_queue.write("Task 3")
    )
    
    # Read messages
    while msg := await tasks_queue.read():
        print(f"Got: {msg}")
```

For advanced use cases requiring direct database access, you can use the `DBConnection` context manager:

```python
from simplebroker.db import DBConnection

# Safe database connection management for cross-queue operations
with DBConnection("myapp.db") as conn:
    db = conn.get_connection()
    
    # Perform cross-queue operations
    queues = db.get_queue_stats()
    for queue_name, stats in queues.items():
        print(f"{queue_name}: {stats['pending']} pending")
    
    # Broadcast to all queues
    db.broadcast("System maintenance at 5pm")
```

**Key async integration strategies:**

1. **Use Queue API**: Prefer the high-level Queue class for single-queue operations
2. **Thread Pool Executor**: Run SimpleBroker's sync methods in threads
3. **One Queue Per Operation**: Create fresh Queue instances for thread safety
4. **DBConnection for Advanced Use**: Use DBConnection context manager for cross-queue operations

See [`examples/async_wrapper.py`](examples/async_wrapper.py) for a complete async wrapper implementation including:
- Async context manager for proper cleanup
- Background watcher with asyncio coordination
- Streaming message consumption
- Concurrent queue operations

### Advanced: Custom Extensions

**Note:** This is an advanced example showing how to extend SimpleBroker's internals. Most users should use the standard Queue API.

```python
from simplebroker.db import BrokerDB, DBConnection
from simplebroker import Queue

class PriorityQueueSystem:
    """Example: Priority queue system using multiple standard queues."""
    
    def __init__(self, db_path: str = ".broker.db"):
        self.db_path = db_path
    
    def write_with_priority(self, base_queue: str, message: str, priority: int = 0):
        """Write message with priority (higher = more important)."""
        queue_name = f"{base_queue}_p{priority}"
        with Queue(queue_name, self.db_path) as q:
            q.write(message)
    
    def read_highest_priority(self, base_queue: str) -> str | None:
        """Read from highest priority queue first."""
        # Check queues in priority order
        for priority in range(9, -1, -1):
            queue_name = f"{base_queue}_p{priority}"
            with Queue(queue_name, self.db_path) as q:
                msg = q.read()
                if msg:
                    return msg
        return None

# For even more advanced use requiring database subclassing:
class CustomBroker(BrokerDB):
    """Example of extending BrokerDB directly (advanced users only)."""
    
    def custom_operation(self):
        # Access internal database methods
        with self._lock:
            # Your custom SQL operations here
            pass
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
- `BROKER_WAL_AUTOCHECKPOINT` - WAL auto-checkpoint threshold in pages (default: 1000)
  - Controls when SQLite automatically moves WAL data to the main database
  - Default of 1000 pages ≈ 1MB (with 1KB page size)
  - Increase for high-traffic scenarios to reduce checkpoint frequency
  - Set to 0 to disable automatic checkpoints (manual control only)
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
- `BROKER_INITIAL_CHECKS` - Number of checks with zero delay (default: 100)
- `BROKER_MAX_INTERVAL` - Maximum polling interval in seconds (default: 0.1)

**Database Naming:**
- `BROKER_DEFAULT_DB_NAME` - name of the broker database file (default: .broker.db)
- Corresponds to the -f/--file command line argument
- Can be a compound path including a single directory (e.g., ".subdirectory/broker.db")
- Applies to all scopes 

Example configurations:
```bash
# High-throughput configuration
export BROKER_SYNC_MODE=NORMAL
export BROKER_READ_COMMIT_INTERVAL=100
export BROKER_INITIAL_CHECKS=1000

# Low-latency configuration  
export BROKER_MAX_INTERVAL=0.01
export BROKER_CACHE_MB=50

# Power-saving configuration
export BROKER_INITIAL_CHECKS=50
export BROKER_MAX_INTERVAL=0.5

# Project scoping configuration
export BROKER_PROJECT_SCOPE=true
export BROKER_DEFAULT_DB_NAME=project-queue.db
```
</details>

## Project Scoping

SimpleBroker provides flexible database scoping modes to handle different use cases:

**Directory Scope (Default):** Each directory gets its own independent `.broker.db`  
**Project Scope:** Git-like upward search for shared project database  
**Global Scope:** Use a specific location for all broker operations

This allows multiple scripts and processes to share broker databases according to your needs.

### Basic Project Scoping

Enable project scoping by setting the environment variable:

```bash
export BROKER_PROJECT_SCOPE=true
```

With project scoping enabled, SimpleBroker searches upward from the current directory to find an existing `.broker.db` file. If found, it uses that database instead of creating a new one in the current directory.

```bash
# Project structure:
# /home/user/myproject/.broker.db  ← Project database
# /home/user/myproject/scripts/
# /home/user/myproject/scripts/worker.py

cd /home/user/myproject/scripts
export BROKER_PROJECT_SCOPE=true
broker write tasks "process data"  # Uses /home/user/myproject/.broker.db
```

**Benefits:**
- **Shared state**: All project scripts use the same message queue
- **Location independence**: Works from any subdirectory
- **Zero configuration**: Just set the environment variable
- **Git-like behavior**: Intuitive for developers familiar with version control

### Global Scope

Use a specific directory for all broker operations. Must be an absolute path.

```bash
export BROKER_DEFAULT_DB_LOCATION=/var/lib/myapp
# Uses: /var/lib/myapp/.broker.db for all operations
```

**Use cases:**
- **System-wide queues**: Central message broker for multiple applications
- **Shared storage**: Use network-mounted directories for distributed access
- **Privilege separation**: Store databases in controlled system directories

**Note:** `BROKER_DEFAULT_DB_LOCATION` corresponds to the `-d/--dir` command line argument and is ignored when `BROKER_PROJECT_SCOPE=true`.

### Project Database Names

Control the database filename used in any scoping mode:

```bash
export BROKER_DEFAULT_DB_NAME=project-queue.db
export BROKER_PROJECT_SCOPE=true
```
Now project scoping searches for `project-queue.db` instead of `.broker.db`.

To better support git-like operation, the BROKER_DEFAULT_DB_NAME can be a compound name including a single subdirectory:

```bash
export BROKER_DEFAULT_DB_NAME=.project/queue.db
export BROKER_PROJECT_SCOPE=true
```
Now project scoping searches for `.project/queue.db` instead of `.broker.db`.

**Use cases:**
- **Multiple projects**: Use different names to avoid conflicts
- **Descriptive names**: `analytics.db`, `build-queue.db`, etc.
- **Environment separation**: `dev-queue.db` vs `prod-queue.db`
- **Using config directories**: `.config/broker.db` vs `.broker.db`

**Note:** If no project database is found during the upward search, SimpleBroker will error out and ask you to run `broker init` to create one.

### Error Behavior When No Project Database Found

When project scoping is enabled but no project database is found, SimpleBroker will error out with a clear message:

```bash
export BROKER_PROJECT_SCOPE=true
cd /tmp/isolated_directory
broker write tasks "test message"
# Error: No SimpleBroker database found in project scope.
# Run 'broker init' to create a project database.
```

**This is intentional behavior** - SimpleBroker requires explicit initialization to avoid accidentally creating databases in unexpected locations.

### Project Initialization

Use `broker init` to create a project database in the current directory:

```bash
cd /home/user/myproject
broker init
# Creates /home/user/myproject/.broker.db
```

**With custom database name:**
```bash
export BROKER_DEFAULT_DB_NAME=project-queue.db
cd /home/user/myproject
broker init
# Creates /home/user/myproject/project-queue.db

# Force reinitialize existing database
broker init --force
```

**Important:** `broker init` always creates the database in the current working directory and does not accept `-d` or `-f` flags. It only respects `BROKER_DEFAULT_DB_NAME` for custom filenames.

**Directory structure examples:**
```bash
# Web application
webapp/
├── .broker.db          ← Project queue (created by: broker init)
├── frontend/
│   └── build.py        ← Uses ../broker.db 
├── backend/
│   └── worker.py       ← Uses ../broker.db
└── scripts/
    └── deploy.sh       ← Uses ../broker.db

# Data pipeline
pipeline/
├── queues.db           ← Custom name (BROKER_DEFAULT_DB_NAME=queues.db)
├── extract/
│   └── scraper.py      ← Uses ../queues.db
├── transform/
│   └── processor.py    ← Uses ../queues.db
└── load/
    └── uploader.py     ← Uses ../queues.db
```

### Precedence Rules

Database path resolution follows strict precedence rules:

1. **Explicit CLI flags** (`-f`, `-d`) - Always override all other settings
2. **Project scoping** (`BROKER_PROJECT_SCOPE=true`) - Git-like upward search, errors if not found
3. **Global scope** (`BROKER_DEFAULT_DB_LOCATION`) - Used when project scoping disabled
4. **Built-in defaults** - Current directory + `.broker.db`

**Environment variable interactions:**
- `BROKER_DEFAULT_DB_NAME` applies to all scoping modes
- `BROKER_DEFAULT_DB_LOCATION` ignored when `BROKER_PROJECT_SCOPE=true`
- `BROKER_PROJECT_SCOPE=true` takes precedence over global scope settings

**Examples:**

```bash
export BROKER_PROJECT_SCOPE=true
export BROKER_DEFAULT_DB_NAME=project.db

# 1. Explicit absolute path (highest precedence)
broker -f /explicit/path/queue.db write test "msg"
# Uses: /explicit/path/queue.db

# 2. Explicit directory + filename
broker -d /explicit/dir -f custom.db write test "msg"  
# Uses: /explicit/dir/custom.db

# 3. Project scoping finds existing database
# (assuming /home/user/myproject/.config/project.db exists)
cd /home/user/myproject/subdir
broker write test "msg"
# Uses: /home/user/myproject/.config/project.db

# 4. Project scoping enabled but no database found (errors out)
cd /tmp/isolated
broker write test "msg"
# Error: No SimpleBroker database found. Run 'broker init' to create one.

# 5. Built-in defaults (no project scoping)
unset BROKER_PROJECT_SCOPE BROKER_DEFAULT_DB_NAME
broker write test "msg"
# Uses: /tmp/isolated/.broker.db
```

**Decision flowchart:**
```
CLI flags (-f absolute path)?
├─ YES → Use absolute path
└─ NO → CLI flags (-d + -f)?
   ├─ YES → Use directory + filename
   └─ NO → BROKER_PROJECT_SCOPE=true?
      ├─ NO → Use env defaults or built-in defaults
      └─ YES → Search upward for database
         ├─ FOUND → Use project database
         └─ NOT FOUND → Error with message to run 'broker init'
```

### Security Notes

Project scoping includes several security measures to prevent unauthorized access:

**Boundary detection:**
- Stops at filesystem root (`/` on Unix, `C:\` on Windows)
- Respects filesystem mount boundaries
- Maximum 100 directory levels to prevent infinite loops

**Database validation:**
- Only uses files with SimpleBroker magic string
- Validates database schema and structure
- Rejects corrupted or foreign databases

**Permission checking:**
- Respects file system access controls
- Skips directories with permission issues
- Validates read/write access before using database

**Traversal limits:**
- Maximum 100 directory levels to prevent infinite loops
- Prevents symlink loop exploitation
- Uses existing path resolution security

**Warnings:**

⚠️ **Project scoping allows accessing databases in parent directories.** Only enable in trusted environments where this behavior is desired.

⚠️ **Database sharing:** Multiple processes will share the same database when project scoping is enabled. Ensure your application handles concurrent access appropriately.

⚠️ **No automatic fallback:** When project scoping is enabled but no database is found, SimpleBroker will error out rather than creating a database automatically. You must run `broker init` to create a project database.

**Best practices:**
```bash
# Safe: Enable only in controlled environments
if [[ "$PWD" == /home/user/myproject/* ]]; then
    export BROKER_PROJECT_SCOPE=true
fi

# Safe: Use explicit paths for sensitive operations
broker -f /secure/path/queue.db write secrets "sensitive data"

# Safe: Validate environment before enabling
if [[ -r "/home/user/myproject/.broker.db" ]]; then
    export BROKER_PROJECT_SCOPE=true
fi
```

### Common Use Cases

**Build systems:**
```bash
# Root project queue for build coordination
cd /project && broker init
export BROKER_PROJECT_SCOPE=true

# Frontend build (any subdirectory)
cd /project/frontend
broker write build-tasks "compile assets"

# Backend build (different subdirectory)  
cd /project/backend
broker read build-tasks  # Gets "compile assets"
```

**Data pipelines:**
```bash
# Pipeline coordination
export BROKER_PROJECT_SCOPE=true
export BROKER_DEFAULT_DB_NAME=pipeline.db
cd /data-pipeline && broker init

# Extract phase
cd /data-pipeline/extractors
broker write raw-data "/path/to/file1.csv"

# Transform phase  
cd /data-pipeline/transformers
broker read raw-data  # Gets "/path/to/file1.csv"
broker write clean-data "/path/to/processed1.json"

# Load phase
cd /data-pipeline/loaders  
broker read clean-data  # Gets "/path/to/processed1.json"
```

**Development workflows:**
```bash
# Development environment setup
cd ~/myproject
export BROKER_PROJECT_SCOPE=true
export BROKER_DEFAULT_DB_NAME=dev-queue.db
broker init

# Testing from any location
cd ~/myproject/tests
broker write test-data "integration-test-1"

# Application reads from any location
cd ~/myproject/src
broker read test-data  # Gets "integration-test-1"
```

**CI/CD integration:**
```bash
# Build script (in any project subdirectory)
#!/bin/bash
export BROKER_PROJECT_SCOPE=true
export BROKER_DEFAULT_DB_NAME=ci-queue.db

# Ensure project queue exists
if ! broker list >/dev/null 2>&1; then
    broker init
fi

# Add build tasks
broker write builds "compile-frontend"
broker write builds "run-tests" 
broker write builds "build-docker"
broker write builds "deploy-staging"
```

**Multi-service coordination:**
```bash
# Service discovery queue
export BROKER_PROJECT_SCOPE=true
export BROKER_DEFAULT_DB_NAME=services.db

# Service A registers itself
cd /app/service-a
broker write registry "service-a:healthy:port:8080"

# Service B discovers Service A
cd /app/service-b  
broker peek registry  # Sees "service-a:healthy:port:8080"
```

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
