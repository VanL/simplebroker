# SimpleBroker

  [![CI](https://github.com/VanL/simplebroker/actions/workflows/test.yml/badge.svg)](https://github.com/VanL/simplebroker/actions/workflows/test.yml)
  [![codecov](https://codecov.io/gh/VanL/simplebroker/branch/main/graph/badge.svg)](https://codecov.io/gh/VanL/simplebroker)
  [![PyPI version](https://badge.fury.io/py/simplebroker.svg)](https://badge.fury.io/py/simplebroker)
  [![Python versions](https://img.shields.io/pypi/pyversions/simplebroker.svg)](https://pypi.org/project/simplebroker/)

*A lightweight message queue backed by SQLite. No server, no daemon, no
dependency resolver surprises.*

```bash
$ pipx install simplebroker
$ broker write tasks "ship it"
$ broker read tasks
ship it
```

SimpleBroker exists for the space between shell pipes and a real broker fleet:
local automation, agents, cron jobs, test harnesses, small services, and
project-local coordination that need durable queue semantics without operating
Redis, RabbitMQ, or a cloud service. The default install has no runtime
dependencies and stores its state in one SQLite database.

## Recommended For

- **Python projects that need a queue without infrastructure.** Most queue
  stacks assume Redis, RabbitMQ, Celery, or a managed service. SimpleBroker's
  default install does not. That matters for tools shipped to users who should
  not have to set up a queue server.
- **Shell scripts, cron jobs, and CI/CD pipelines.** `broker write tasks
  "build #123"` composes with pipes, exit codes, and `--json` like a Unix tool.
- **Coding agents that need a queue primitive.** The CLI gives agents a durable
  coordination point without an MCP server, daemon, or project-specific setup.
- **Library and tool authors embedding queue semantics.** Use a small client or
  context object over SimpleBroker, translate your app settings into `BROKER_*`
  config, and hand out queues bound to one resolved broker target. Weft is the
  reference implementation of this pattern.

## Table of Contents

- [SimpleBroker](#simplebroker)
  - [Recommended For](#recommended-for)
  - [Table of Contents](#table-of-contents)
  - [Features](#features)
  - [Use Cases](#use-cases)
  - [Installation](#installation)
  - [Quick Start](#quick-start)
  - [Command Reference](#command-reference)
    - [Global Options](#global-options)
    - [Commands](#commands)
      - [Queue Aliases](#queue-aliases)
    - [Command Options](#command-options)
    - [Exit Codes](#exit-codes)
  - [Critical Safety Notes](#critical-safety-notes)
    - [Safe Message Handling](#safe-message-handling)
    - [Robust message handling with `watch`](#robust-message-handling-with-watch)
  - [Core Concepts](#core-concepts)
    - [Timestamps as Message IDs](#timestamps-as-message-ids)
    - [JSON for Safe Processing](#json-for-safe-processing)
    - [Checkpoint-based Processing](#checkpoint-based-processing)
  - [Common Patterns](#common-patterns)
  - [Real-time Queue Watching](#real-time-queue-watching)
    - [Move Mode (`--move`)](#move-mode---move)
  - [Python API](#python-api)
    - [Delivery guarantees](#delivery-guarantees)
    - [Queue metadata](#queue-metadata)
    - [Latest pending timestamp](#latest-pending-timestamp)
    - [Generating timestamps without writing](#generating-timestamps-without-writing)
    - [Inserting messages with exact IDs](#inserting-messages-with-exact-ids)
    - [Tracking the last generated timestamp](#tracking-the-last-generated-timestamp)
    - [Thread-Based Background Processing](#thread-based-background-processing)
    - [Context Manager Support](#context-manager-support)
    - [Advanced: Custom Extensions](#advanced-custom-extensions)
    - [Sidecar tables (advanced)](#sidecar-tables-advanced)
  - [Embedding SimpleBroker in Your Project](#embedding-simplebroker-in-your-project)
  - [Performance \& Tuning](#performance--tuning)
    - [Cross-Backend Benchmarking](#cross-backend-benchmarking)
    - [Environment Variables](#environment-variables)
  - [Project Scoping](#project-scoping)
    - [Basic Project Scoping](#basic-project-scoping)
    - [Global Scope](#global-scope)
    - [Project Database Names](#project-database-names)
    - [Project Config Names](#project-config-names)
    - [Error Behavior When No Project Database Found](#error-behavior-when-no-project-database-found)
    - [Project Initialization](#project-initialization)
    - [Precedence Rules](#precedence-rules)
    - [Security Notes](#security-notes)
    - [Common Use Cases](#common-use-cases)
  - [Architecture \& Technical Details](#architecture--technical-details)
  - [Development \& Contributing](#development--contributing)
    - [Releases](#releases)
  - [License](#license)
  - [Acknowledgments](#acknowledgments)

## Features

- **Zero configuration** - No servers, daemons, or complex setup
- **SQLite-backed** - Rock-solid reliability with true ACID guarantees
- **Concurrent safe** - Multiple processes can read/write simultaneously
- **Simple CLI** - Intuitive commands that work with pipes and scripts
- **Portable** - Each directory gets its own isolated `.broker.db`
- **Fast** - 1000+ messages/second throughput
- **Lightweight** - No external dependencies and a compact operational model
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

# Or install with optional Postgres support
pipx install "simplebroker[pg]"

# Or install with uv to use as a library
uv add simplebroker
uv add "simplebroker[pg]"

# Or with pip
pip install simplebroker
pip install "simplebroker[pg]"
```

The CLI is available as both `broker` and `simplebroker`.

**Requirements:**
- Python 3.11+
- SQLite 3.35+ (released March 2021) - required for `RETURNING` support


## Quick Start

```bash
# Write a message
$ broker write myqueue "Hello, World!"

# Read the message (removes it)
$ broker read myqueue
Hello, World!

# Write from stdin
$ echo "another message" | broker write myqueue
$ echo "another message" | broker write myqueue -

# Read all messages at once
$ broker read myqueue --all

# Peek without removing
$ broker peek myqueue

# Move messages between queues
$ broker move myqueue processed
$ broker move errors retry --all

# Rename existing queue state
$ broker rename processed archive.ready
$ broker rename @legacy archive.ready --json

# list all queue names
$ broker list
myqueue
processed
$ broker list --stats
myqueue: 3
processed: 1
$ broker stats myqueue
myqueue: 3
$ broker exists myqueue
$ broker list --prefix jobs. --stats

# Broadcast to all queues
$ broker broadcast "System maintenance at 5pm"
# Target only matching queues using fnmatch-style globs
$ broker broadcast --pattern 'jobs-*' "Pipeline paused"

# Clean up when done
$ broker --cleanup
```

## Command Reference

### Global Options

Global options must appear before the command, for example `broker -f queue.db read jobs`.

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
| `write <queue> [message\|-]` | Add message to queue (omit or use `-` for stdin) |
| `read <queue> [options]` | Remove and return message(s) |
| `peek <queue> [options]` | Return message(s) without removing |
| `move <source> <dest> [options]` | Atomically transfer messages between queues |
| `rename <old> <new> [--json]` | Retag all existing messages from one queue name to another |
| `exists <queue> [--json]` | Check whether a queue has any messages, including claimed rows |
| `stats <queue> [--json]` | Show pending, claimed, and total counts for one queue |
| `list [--stats] [--prefix PREFIX \| --pattern GLOB] [--json]` | Show queue names; `--stats` adds counts |
| `delete <queue> [-m <id>]` | Delete a queue immediately, or physically delete a specific message by ID |
| `delete --all` | Delete all queues immediately |
| `broadcast <message\|->` | Send message to all existing queues |
| `watch <queue> [options]` | Watch queue for new messages |
| `alias <add\|remove\|list\|->` | Manage queue aliases |
| `dump [--include <glob>] [--exclude <glob>]` | Write all queues to stdout as ndjson (pending messages only, deterministic; globs match queue names, aliases match on their own name or their target, exclude wins, the flags compose) |
| `load` | Restore a dump from stdin into a fresh broker (duplicate message IDs fail loudly); exit codes 0/1 |
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
- `rename` accepts `@alias` operands on the CLI and records canonical queue
  names in JSON output. The Python API uses literal queue names only.

### Command Options

**Common options for read/peek/move:**
- `--all` - Process all messages (CLI moves up to 1,000,000 per invocation; rerun for larger queues or use the Python API generators)
- `--json` - Output as line-delimited JSON (includes timestamps)
- `-t, --timestamps` - Include timestamps in output
- `-m <id>` - Target specific message by its 19-digit timestamp ID
- `--after <timestamp>` - Process messages newer than timestamp
- `--before <timestamp>` - Process messages older than timestamp (`read`, `peek`, and `move`; not `watch`)

**Watch options:**
- `--peek` - Monitor without consuming
- `--move <dest>` - Continuously drain to destination queue
- `--quiet` - Suppress startup message

**Queue metadata options:**
- `stats <queue>` reports counts for exactly one queue without scanning all queues.
- `exists <queue>` exits `0` when the queue has any row and `2` when it has none.
- `list` reports queue names. Use `list --stats` when you need counts.
- `list --prefix <prefix>` uses a literal queue-name prefix.
- `list --pattern <glob>` uses fnmatch-style matching.
- `--json` on `exists`, `stats`, or `list` emits JSON suitable for scripts.

Queues are implicit: a queue exists when at least one message row exists for
that name, including claimed rows. After vacuum removes claimed rows, a
claimed-only queue no longer exists.

**Timestamp formats for `--after` and `--before`:**
- ISO 8601: `2024-01-15T14:30:00Z` or `2024-01-15` (midnight UTC)
- Unix seconds: `1705329000` or `1705329000s`
- Unix milliseconds: `1705329000000ms`
- Unix nanoseconds/Native hybrid: `1837025672140161024` or `1837025672140161024ns`

**Best practice:** Heuristics are used to distinguish between different values for interactive use, but explicit suffixes (s/ms/ns) are recommended for clarity if referring to particular times. 

`--after` and `--before` use strict open bounds. Combined together, they select
messages where `after_timestamp < message_timestamp < before_timestamp`.

### Exit Codes
- `0` - Success
- `1` - General error (e.g., database access error, invalid arguments)
- `2` - Queue empty or no matching messages

**Note:** `delete <queue>`, `delete --all`, and `delete <queue> -m <id>` remove matching rows immediately. Reads still use claimed-row semantics and are reclaimed by `--vacuum`.

## Critical Safety Notes

### Safe Message Handling

Messages can contain any characters including newlines, control characters, and shell metacharacters:
- **Shell injection risks** - When piping output to shell commands, malicious message content could execute unintended commands
- **Special characters** - Messages containing newlines or other special characters can break shell pipelines that expect single-line output
- **Queue names** - Limited to alphanumeric + underscore/hyphen/period (cannot start with hyphen or period)
- **Message size** - Limited to 10MB by default; override with `BROKER_MAX_MESSAGE_SIZE`
- **NUL characters** - Message bodies are UTF-8 text. The Postgres backend cannot store a raw NUL (`\x00`) character: `write()` raises `OperationalError` there, while SQLite and Redis round-trip it. JSON-encoded payloads are unaffected (compliant serializers escape NUL as `\u0000`), but avoid casting bodies to `jsonb` in sidecar queries: jsonb rejects `\u0000` even escaped.

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
- High 52 bits: microseconds after Unix epoch
- Low 12 bits: logical counter for sub-microsecond ordering
- Similar to Twitter's Snowflake IDs or UUID7
- The format is compatible with time.time_ns(), but the precision is closer to microseconds (~4 μs) due to limits on the precision of the host clock


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

Use `--after` for resumable processing:

```bash
# Save checkpoint after processing
$ result=$(broker read tasks --json)
$ checkpoint=$(echo "$result" | jq '.timestamp')

# Resume from checkpoint
$ broker read tasks --all --after "$checkpoint"

# Or use human-readable timestamps
$ broker read tasks --all --after "2024-01-15T14:30:00Z"

# Process a bounded open interval
$ broker peek tasks --all --after "$start" --before "$end"

# Also show consumed rows not yet vacuumed (inspection only)
$ broker peek tasks --all --include-claimed
```

Claimed rows are deletion-pending — vacuum may remove them at any time;
`--include-claimed` is an inspection tool, not delivery state.

```bash
# Back up, restore, or migrate between backends — dumps are plain ndjson.
# Load targets a FRESH broker (duplicate message IDs fail loudly).
$ broker dump > backup.ndjson
$ broker dump --include 'tasks*' --exclude 'tasks_tmp' | (cd /fresh/dir && broker load)
$ broker dump | BROKER_BACKEND=postgres BROKER_BACKEND_TARGET="$DSN" broker load
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
emails
logs
metrics
$ broker list --stats
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
# Pipe command output into a queue
$ df -h | broker write monitoring -
$ broker peek monitoring

# Process files through a queue
$ find . -name "*.log" | while read f; do
    broker write logfiles "$f"
done

# Parallel processing with xargs
$ broker read logfiles --all | xargs -P 4 -I {} process_log {}

# Remote queue via SSH
$ echo "remote task" | ssh server "cd /app && broker write tasks -"
$ ssh server "cd /app && broker read tasks"

# Use absolute paths for databases in specific locations
$ broker -f /var/lib/myapp/queue.db write tasks "backup database"
$ broker -f /var/lib/myapp/queue.db read tasks

# Reserving work using move
$ msg_json=$(broker move todo in-process --json 2>/dev/null)
  if [ -n "$msg_json" ]; then
      msg_id=$(echo "$msg_json" | jq -r '.timestamp')
      msg_data=$(echo "$msg_json" | jq -r '.message')

      echo "Processing message $msg_id: $msg_data"

      # Process the message here
      # ...

      # Delete after successful processing
      broker delete in-process -m "$msg_id"
  else
      echo "No messages to process"
  fi

# broker move --all --json emits ndjson: one JSON object per line
$ broker move todo in-process --all --json | while IFS= read -r msg_json; do
    msg_id=$(echo "$msg_json" | jq -r '.timestamp')
    msg_data=$(echo "$msg_json" | jq -r '.message')
    process_message "$msg_data" && broker delete in-process -m "$msg_id"
done
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
    echo "Processing new messages..."
    
    # Process messages one at a time with peek-then-delete acknowledgement
    processed=0
    while [ $processed -lt $BATCH_SIZE ]; do
        # Peek exactly one message newer than checkpoint without removing it
        message_data=$(broker peek "$QUEUE" --json --after "$last_checkpoint" 2>/dev/null)
        
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
            # Exit without deleting or checkpointing - failed message will be reprocessed
            exit 1
        fi

        # Acknowledge successful processing by deleting the exact message
        if ! broker delete "$QUEUE" -m "$timestamp" >/dev/null 2>&1; then
            echo "Warning: processed message $timestamp but failed to delete it" >&2
            echo "It may be reprocessed on the next run" >&2
            exit 1
        fi
        
        # Atomically update checkpoint ONLY after successful processing and delete
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
- **No data loss from pipe buffering** - Peeks and acknowledges messages one at a time
- **Atomic checkpoint updates** - Uses temp file + rename for crash safety
- **Per-message checkpointing** - Updates checkpoint after each successful message
- **Batch processing** - Processes up to BATCH_SIZE messages at a time for efficiency
- **Failure recovery** - On error, exits without deleting or checkpointing so failed message is retried
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
- **No filtering**: Incompatible with timestamp filters such as `--after` and `--before` (would leave messages stranded)
- **Concurrent safe**: Multiple move watchers can run safely without data loss

## Python API

SimpleBroker also provides a Python API for more advanced use cases:

```python
from simplebroker import Queue, QueueWatcher
import logging

# Basic usage
with Queue("tasks") as q:
    q.write("process order 123")
    print(q.exists())
    print(q.stats())
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
    # Message remains in queue for retry after we're using peek=True
    
    # Optional: After N retries, move to dead-letter queue
    # Queue("errors").write(f"{timestamp}:{message}:{exception}")
    
    return True  # Continue watching

# Use peek=True for safe mode - messages aren't removed until explicitly acknowledged
```

For cleanup paths that already know many exact message IDs, use
`Queue.delete_many(message_ids)` to physically delete them in one backend-level
batch.

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

### Delivery guarantees

Materialized batch APIs such as `Queue.read_many()` and `Queue.move_many()`
commit before returning their result lists. Passing
`delivery_guarantee="at_least_once"` is supported on those APIs and is
satisfied by the stricter exactly-once materialization behavior.

Use generator APIs such as `Queue.read_generator()` and `Queue.move_generator()`
when you need retry-on-stop batch processing. In `delivery_guarantee="at_least_once"`
generator mode, SimpleBroker commits a batch only after the full batch has been
yielded; stopping mid-batch rolls that batch back for retry.

Peeks can also inspect claimed (consumed but not yet vacuumed) messages:

```python
q.peek_many(10, include_claimed=True)   # pending + claimed, in message-ID order
```

Claimed rows are deletion-pending — vacuum may remove them at any time — so
`include_claimed` is an inspection tool, not delivery state.

Whole-broker backup and migration mirror the CLI:

```python
from simplebroker import dump_lines, load_lines, open_broker

with open_broker("src.db") as src, open_broker("dst.db") as dst:
    load_lines(dst, dump_lines(src, include=["tasks*"]))
```

### Queue metadata

Use targeted metadata APIs when you need queue existence or counts:

```python
from simplebroker import Queue, QueueStats

queue = Queue("tasks")

if queue.exists():
    stats: QueueStats = queue.stats()
    print(stats.pending, stats.claimed, stats.total)
```

`QueueStats.pending` is the unclaimed count. `QueueStats.claimed` is the count
of messages already read or claimed but not yet vacuumed. `QueueStats.exists`
is true when `total > 0`.

For cross-queue metadata, `open_broker(...).list_queues()` returns queue names
only, including claimed-only queues. Use `list_queue_stats()` when you need
counts.

### Latest pending timestamp

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

### Generating timestamps without writing

Sometimes you need a broker-compatible timestamp/ID before enqueueing a message (for logging, correlation IDs, or backpressure planning). You can ask SimpleBroker to generate one without writing a row:

```python
queue = Queue("tasks", db_path="/path/to/.broker.db")
ts = queue.generate_timestamp()  # alias: queue.get_ts()

print(ts)  # Monotonic within a database
```

Notes:
- Timestamps are monotonic per database and match what `Queue.write()` uses internally.
- Generating a timestamp advances the broker high-water mark. Normal `write()`
  calls will not reuse that ID, but no message row exists until you write one.

### Inserting messages with exact IDs

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

`insert_messages(...)` validates the full batch, rejects duplicate IDs, advances
`last_ts` above the largest supplied ID inside the same transaction, and inserts
pending messages with their exact IDs. Normal producers should still use
`write(...)`, which allocates IDs through the broker timestamp generator.

> **Supply IDs that came from a SimpleBroker timestamp generator** (your own
> reserved ID, or the source broker's dump). Because `insert_messages(...)` moves
> `last_ts` to just above the largest ID in the batch, an arbitrarily large ID
> pushes the high-water mark far into the future and stalls later `write()` calls
> until the wall clock catches up. IDs must be non-negative and below `2**63 - 1`.

For a single queue handle, pass `(message, message_id)` pairs:

```python
queue.insert_messages([("restore one", 1837025672140161024)])
```

There is no CLI surface for exact-ID inserts.

### Tracking the last generated timestamp

Each `Queue` instance caches the most recent `meta.last_ts` value it has seen via the `queue.last_ts` attribute. The cache updates automatically after calls to `queue.write()` and `queue.generate_timestamp()`.

For long-lived watchers or background processes, force a refresh without creating a new message by calling `queue.refresh_last_ts()`, which performs a lightweight, non-blocking read of the meta table:

```python
queue = Queue("tasks")
print(queue.last_ts)  # None until we generate or refresh

queue.write("build artifacts ready")
print(queue.last_ts)  # Updated immediately after the write

# Later, detect external writers without adding a message
queue.refresh_last_ts()
print(queue.last_ts)
```

Watchers automatically refresh their queue's `last_ts` whenever `PRAGMA data_version` reports changes, so you always have a current view of the most recent timestamp while the watcher is running.
```python
watcher = QueueWatcher(
    queue=Queue("tasks"),
    handler=process_message,
    error_handler=handle_error,
    peek=True  # True = safe mode - just observe, don't consume
)

# Start watching (blocks until stopped)
try:
    watcher.run_forever()
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

### Context Manager Support

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
            with Queue(self.queue_name, db_path=self.db_path) as q:
                q.write(message)
        await loop.run_in_executor(self._executor, _write)
    
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
        tasks_queue.write("Task 3")
    )
    
    # Read messages
    while msg := await tasks_queue.read():
        print(f"Got: {msg}")
```

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

**Key async integration strategies:**

1. **Use Queue API**: Prefer the high-level Queue class for single-queue operations
2. **Thread Pool Executor**: Run SimpleBroker's sync methods in threads
3. **One Queue Per Operation**: Create fresh Queue instances for thread safety
4. **open_broker for Advanced Use**: Use `open_broker()` for cross-queue operations

See [`examples/async_wrapper.py`](examples/async_wrapper.py) for a complete async wrapper implementation including:
- Async context manager for proper cleanup
- Background watcher with asyncio coordination
- Streaming message consumption
- Concurrent queue operations

### Advanced: Custom Extensions

**Note:** Most application extensions should compose the public `Queue` API. Do
not subclass `BrokerDB` or import underscore-prefixed modules for application
logic.

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

Backend authors should use the explicit extension contracts in
`simplebroker.ext`; see [Advanced: External Backend Plugins](#advanced-external-backend-plugins).
See [`examples/`](examples/) for application-level patterns.

### Sidecar tables (advanced)

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
- Connection lifetime follows the `Queue`: ephemeral queues get in and get out
  per session; `persistent=True` queues reuse their connection.
- Use `?` (qmark) placeholders. They work natively on SQLite and are translated
  by the Postgres backend (where sidecar tables live in the broker's configured
  schema). Other SQL dialect differences are yours to manage.
- The Redis backend has no SQL storage: `sidecar()` raises
  `SidecarUnavailableError` there. Catch it to probe the capability.
- Don't nest sidecar transactions and don't call queue operations inside a
  `sidecar(transaction=True)` block on the same persistent handle — SQLite
  cannot nest write transactions.
- Schema setup: idempotent `CREATE TABLE IF NOT EXISTS` (plus additive
  `ALTER TABLE`) inside a `transaction=True` session is race-safe across
  processes.

## Embedding SimpleBroker in Your Project

For embedded use, the current best practice is to put a small project-level
client or context object in front of SimpleBroker. Let that object resolve the
broker target once, translate your application's settings into `BROKER_*`
config keys, and hand out queues bound to that target. Application code should
call the client instead of open-coding `Queue(...)` across the codebase.

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
    open_broker,
    resolve_config,
    target_for_directory,
)


@dataclass(frozen=True)
class AppBrokerClient:
    target: BrokerTarget
    config: dict[str, Any]

    @classmethod
    def from_root(cls, root: str | Path, **overrides: Any) -> "AppBrokerClient":
        root_path = Path(root)
        (root_path / ".myapp").mkdir(parents=True, exist_ok=True)
        config = resolve_config(
            {
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
application needs its own environment namespace, translate those values into a
config dict and pass it through `resolve_config()`; avoid importing
`simplebroker._constants` or guessing database paths.

## Performance & Tuning

- **Throughput**: 1000+ messages/second on typical hardware
- **Latency**: <10ms for write, <10ms for read
- **Scalability**: Tested with 100k+ messages per queue
- **Optimization**: Use `--all` for bulk operations

### Cross-Backend Benchmarking

If you want an apples-to-apples CLI benchmark for SQLite vs Postgres, the repo
includes a black-box harness that reuses the same `run_cli()` hook as the test
suite:

```bash
# Quick SQLite-only smoke run
uv run python -m tests.backend_benchmark --backends sqlite --iterations 1 --warmups 0

# SQLite vs Postgres comparison
export SIMPLEBROKER_PG_TEST_DSN="postgresql://postgres:postgres@127.0.0.1:54329/simplebroker_test"
uv run python -m tests.backend_benchmark --backends sqlite postgres

# Machine-readable output
uv run python -m tests.backend_benchmark --backends sqlite postgres --format json
```

The harness measures end-to-end CLI behavior for repeated single-message
`write` and `read`, bulk `read --all`, bulk `move --all`, and repeated
`--status --json` calls.

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
  - Increase for better throughput with at-least-once delivery semantics
  - For values > 1, each batch is committed only after the full batch has been yielded to the consumer
  - If processing stops mid-batch (crash/interrupt), unread messages in that batch are rolled back and retried
  - Larger values keep transactions open longer and can increase write lock contention; tune batch size to workload

**Vacuum Settings:**
- `BROKER_AUTO_VACUUM` - Enable automatic vacuum of claimed messages (default: true)
- `BROKER_VACUUM_THRESHOLD` - Claimed-message ratio that triggers auto-vacuum (default: 10%)
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

**Project Config Naming:**
- `BROKER_PROJECT_CONFIG_NAME` - project config filename (default: .broker.toml)
- `BROKER_PROJECT_CONFIG_PATH` - optional directory prefix for project config discovery
- Relative prefixes are searched under each candidate project directory
- Use these to namespace embedded consumers away from standalone SimpleBroker config

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

### Project Config Names

Project config discovery can be namespaced independently from standalone
SimpleBroker by setting a config name, a config path prefix, or both:

```bash
export BROKER_PROJECT_SCOPE=true
export BROKER_PROJECT_CONFIG_PATH=.weft
export BROKER_PROJECT_CONFIG_NAME=broker.toml
```

Now project scoping searches upward for `.weft/broker.toml` instead of
`.broker.toml`. An equivalent compact form is:

```bash
export BROKER_PROJECT_CONFIG_NAME=.weft/broker.toml
```

This follows the same single-directory rule as `BROKER_DEFAULT_DB_NAME`.
`BROKER_PROJECT_CONFIG_PATH` may also be an absolute directory when one fixed
config location should be used.

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

**Important:** `broker init` does not accept `-d` or `-f` flags. In legacy
SQLite mode it initializes the current directory and respects
`BROKER_DEFAULT_DB_NAME` for custom filenames. When project scope finds a
configured project TOML file, `broker init` initializes that project target
instead.

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

SimpleBroker resolves the active broker target in this order:

1. **Explicit CLI SQLite file selection** (`-f`, or `-d/-f`) for non-`init`
   commands
2. **Project config** discovered upward from the working directory when project
   scope is enabled, using `BROKER_PROJECT_CONFIG_PATH` and
   `BROKER_PROJECT_CONFIG_NAME`
3. **Legacy project SQLite discovery** using `BROKER_DEFAULT_DB_NAME` when
   project scope is enabled
4. **Env-selected non-SQLite backend** using `BROKER_BACKEND=...`
5. **SQLite defaults** from `BROKER_DEFAULT_DB_LOCATION`, the current
   directory, and `BROKER_DEFAULT_DB_NAME`

**Notes:**
- `BROKER_DEFAULT_DB_NAME` affects legacy SQLite discovery and default SQLite
  targets. It does not override project config.
- `BROKER_PROJECT_CONFIG_NAME` and `BROKER_PROJECT_CONFIG_PATH` affect project
  config discovery and explicit-root project config resolution.
- `BROKER_DEFAULT_DB_LOCATION` is only part of the SQLite default path.
- When project TOML provides backend target fields, the project file is
  authoritative. Env remains appropriate for secrets such as
  `BROKER_BACKEND_PASSWORD`.

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

# 4. Project scope can also discover a .broker.toml before env backends
# (assuming /home/user/myproject/.broker.toml exists)
cd /home/user/myproject/subdir
broker write test "msg"
# Uses the project target from /home/user/myproject/.broker.toml

# 5. Project scoping enabled but no project config or database found (errors out)
cd /tmp/isolated
broker write test "msg"
# Error: No SimpleBroker database found. Run 'broker init' to create one.

# 6. Built-in defaults (no project scoping)
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
      └─ YES → Search upward for project config, then legacy SQLite database
         ├─ PROJECT CONFIG FOUND → Use configured project target
         ├─ SQLITE DATABASE FOUND → Use project database
         └─ NOTHING FOUND → Error with message to run 'broker init'
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

**Warning:** Project scoping allows accessing databases in parent directories. Only enable in trusted environments where this behavior is desired.

**Warning:** Multiple processes will share the same database when project scoping is enabled. Ensure your application handles concurrent access appropriately.

**Warning:** When project scoping is enabled but no database is found, SimpleBroker will error out rather than creating a database automatically. You must run `broker init` to create a project database.

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
<summary>Design Philosophy</summary>

SimpleBroker is optimized for boring deployment and predictable embedding.
Four rules shape the code and API:

1. **One config path.** Supported runtime knobs are represented as `BROKER_*`
   keys, loaded from environment variables by `load_config()`, and normalized
   through `resolve_config()`. Not every internal constant is user-configurable;
   the contract is that runtime configuration goes through one typed path.
2. **No base runtime dependencies.** The root `pyproject.toml` keeps
   `dependencies = []`. Optional backends live in separate packages such as
   `simplebroker-pg` and `simplebroker-redis`. Small portability modules are
   kept in-tree when they protect the zero-dependency install path.
3. **Public API first.** Application embedders should use the names exported
   from `simplebroker`: `Queue`, watcher classes, broker target helpers,
   `open_broker()`, and `resolve_config()`. Backend authors should use
   `simplebroker.ext`. Underscore-prefixed modules are implementation details.
4. **CLI and library share the same operational model.** `broker write tasks
   "hi"` and `Queue("tasks").write("hi")` should mean the same queue operation
   over the same resolved target. The CLI has shell-specific affordances and
   the library has Python-specific helpers, but the queue semantics stay shared.

</details>

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

**Exactly-Once Delivery:** Read and move operations use atomic backend
transitions. A message is delivered exactly once to a consumer by default.

**FIFO Ordering:** Messages are read in write order for a queue, regardless of
which process wrote them. SQLite uses the autoincrement `id` plus serialized
write transactions; other backends must preserve the same public ordering
contract.

**Message Lifecycle:**
1. **Write Phase**: Message inserted with unique timestamp
2. **Claim Phase**: Read marks message as "claimed" (fast, logical delete)
3. **Vacuum Phase**: Background process permanently removes claimed messages

This optimization is transparent - messages are still delivered exactly once.
</details>

<details>
<summary>Security Considerations</summary>

- **Queue names**: Validated (alphanumeric + underscore + hyphen + period only)
- **Message size**: Limited to 10MB by default; override with `BROKER_MAX_MESSAGE_SIZE`
- **Database files**: Created with 0600 permissions (user-only)
- **SQL injection**: Prevented via parameterized queries
- **Message content**: Not validated - can contain any text including shell metacharacters
</details>

<details>
<summary>Advanced: External Backend Plugins</summary>

SimpleBroker core remains SQLite-first so that basic usage has no dependencies outside 
the Python standard library.

If you need a different backend, use an external plugin package through
`simplebroker.ext`. This repository includes sibling Postgres and Valkey/Redis
packages. `simplebroker[pg]` is a convenience extra that installs the external
`simplebroker-pg` plugin package for you; the Redis package is developed under
`extensions/simplebroker_redis`.

For end users:

```bash
uv add "simplebroker[pg]"
```

For local development against the sibling extension in this repository:

```bash
uv pip install -e "./extensions/simplebroker_pg[dev]"
uv pip install -e "./extensions/simplebroker_redis[dev]"
```

There are two backend shapes:

1. **SQL-runner-shaped backends** reuse SimpleBroker's shared `BrokerCore`.
   They provide a runner plus a SQL namespace matching the core query contract.
   Postgres is the reference implementation.
2. **Direct-core backends** implement the broker core protocol directly because
   the storage system is not SQL-shaped. Redis/Valkey is the reference
   implementation: it uses Redis data structures and Lua scripts, so forcing it
   through the SQL runner abstraction would make both correctness and
   operations worse.

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

An explicitly injected `runner=` remains caller-owned. Reuse the same runner
object yourself when you want several queues to share an injected backend.
For `PostgresRunner`, call `runner.close()` or `runner.shutdown()` when you are
done with the explicitly created runner so its connection pool is closed.

CLI/project usage is selected through a `.broker.toml` file in the project
root:

```toml
version = 1
backend = "postgres"
target = "postgresql://postgres:postgres@127.0.0.1:54329/simplebroker_test"

[backend_options]
schema = "simplebroker_app"
```

When `.broker.toml` is present, it owns the backend target and target-shaping
options for that project. Env is still the right place for supplemental secret
material such as `BROKER_BACKEND_PASSWORD`.
</details>

<details>
<summary>Things That Look Weird but Aren't</summary>

**Why so many `BROKER_*` settings?** `load_config()` documents 32 config keys
because SimpleBroker is also embedded by larger tools. Most users should never
touch most of them. Embedders such as Weft translate their own namespace into
those keys and pass the result through `resolve_config()`, which keeps
configuration mechanical instead of one-off.

**Why is `BROKER_SYNC_MODE=FULL` the default?** The default favors durability
over benchmark numbers. `NORMAL` is faster and often reasonable, but it changes
the power-loss risk profile. SimpleBroker starts from the safer default and
lets callers opt into the tradeoff.

**Why does `_phaselock.py` exist?** SQLite setup has to be safe across
processes and platforms. The phase-lock module coordinates setup work with
file locks and extended-attribute fallback so multiple processes do not race
schema or optimization phases. It is internal, but deliberately self-contained.

**Why are read messages marked claimed before vacuum removes them?** Claiming
keeps reads fast and atomic while deferring physical cleanup. Vacuum removes
claimed rows later. This is why queue stats distinguish pending, claimed, and
total rows.

**Why does Redis/Valkey use a parallel core instead of the Postgres runner
model?** Postgres is relational, so the SQL-runner contract fits. Redis is a
key/value data-structure server; a direct core can express reserved batches,
Lua-backed transitions, Pub/Sub wake hints, and namespace cleanup honestly.

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
PHASELOCK_ENABLE_XATTRS=0 uv run pytest tests/test_phaselock.py tests/test_runner_validation.py tests/test_runner_error_handling.py tests/test_queue_config_defaults.py tests/test_sqlite_setup_contention.py
uv run ./bin/pytest-pg     # All PG-backed tests with automatic Docker setup/teardown
uv run ./bin/pytest-redis  # All Redis-backed tests with automatic Docker setup/teardown (Valkey)
HYPOTHESIS_PROFILE=ci uv run pytest tests/test_property_*.py  # deeper property-test run (50 -> 200 examples per property)
python fuzz/fuzz_timestamp_validate.py  # coverage-guided fuzzing via Atheris (Linux only; see fuzz/*.py)
uv run ./bin/pytest-pg -q tests/test_watcher_metrics.py -k basic
uv run ./bin/packaging-smoke --python 3.11

# Lint and format
uv run ruff check --fix simplebroker tests bin
uv run ruff format simplebroker tests bin
uv run mypy simplebroker bin/release.py
```

Property-based tests (`tests/test_property_*.py`, powered by Hypothesis)
check parser totality/round-trips and run a stateful model of queue
semantics against every backend; failures print a `@reproduce_failure`
blob that replays the exact case. The `fuzz/` harnesses drive the same
properties coverage-guided under Atheris (weekly via the Fuzz workflow);
a fuzz crash is a real property violation, replayable with plain pytest.

**Contributing guidelines:**
1. Keep it simple - the entire codebase should stay understandable
2. Maintain backward compatibility
3. Add tests for new features
4. Update documentation
5. Run linting and tests before submitting PRs

### Releases

Use the repo-local release helper instead of pushing release tags by hand:

```bash
# Release simplebroker
python bin/release.py --version 3.1.10

# Release simplebroker-pg
python bin/release.py pg --version 1.0.6

# Release every current unpublished package version with one local check run
python bin/release.py all

# Preview the checks, version files, commit, and tag action
python bin/release.py --dry-run
```

The helper checks the target version against GitHub Releases and PyPI, runs the
release checks, updates version files when needed, commits release-file changes,
pushes the branch, and then pushes the release tag. The tag workflow waits for
the relevant normal test workflows on the same commit to finish green, then
builds the distributions, publishes them to PyPI with trusted publishing, and
creates the GitHub Release from the same top-level gate workflow. Keeping the
build, attestation, and publish steps in the gate workflow makes PyPI's trusted
publisher identity match the artifact attestation build-config URI.
Core releases wait for `Test`, `Test Postgres Extension`, and
`Test Redis Extension`; extension releases wait for `Test` plus their matching
backend workflow.

PyPI trusted publisher entries should use repository `VanL/simplebroker`, the
`pypi` environment, and these GitHub Actions workflows:

- `release-gate.yml` for `simplebroker`
- `release-gate-pg.yml` for `simplebroker-pg`
- `release-gate-redis.yml` for `simplebroker-redis`

Use `python bin/release.py all` after version files have already been bumped
across packages. It scans `simplebroker`, `simplebroker-pg`, and
`simplebroker-redis`, skips versions already published on GitHub Releases or
PyPI, runs the combined release checks once, syncs root extension extras when
the core package is part of the batch, creates one release commit if needed, and
pushes all selected tags. Extension tags are prepared before the core tag so a
batch release can carry new extension baselines and the matching core package
together.

When releasing only `simplebroker` with updated extension extras, the extension
versions must already be available on PyPI first. The `all` target is the path
for releasing unpublished extension baselines and the core package together.

## License

MIT © Van Lindberg

## Acknowledgments

Built with Python, SQLite, and the Unix philosophy.
