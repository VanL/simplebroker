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

**Exact product behavior** is owned by the canonical specs in
`docs/specs/` (`10-cli.md` … `17-ops.md`, codes `[SB-CLI-*]` …
`[SB-OPS-*]`), registered in
`docs/specs/product-section-registry.md`. This README is the human entry:
catalogs, examples, and short restatements with links. Agents should prefer
`docs/agent-kernel.md` for use orientation.

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
    - [Filtering by message id (`--after` / `--before`)](#filtering-by-message-id---after----before)
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
    - [Reactor example (advanced)](#reactor-example-advanced)
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
**Not for:** Broker fleets, pub/sub, distributed task frameworks, application
orchestration, or high-frequency trading

This is an ownership boundary, not a host-count claim. Cooperating processes
raise distributed-systems issues, and optional Postgres or Redis backends can
serve clients on multiple hosts. SimpleBroker owns queue-operation semantics;
the backend owns service topology, replication, availability, and recovery;
the application owns work execution and business retries. SQLite files remain
local-only: do not put them on NFS or another shared network filesystem.

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
# Target an exact set of existing literal queues; --queue is repeatable
$ broker broadcast \
    --queue notify.alice \
    --queue notify.carol \
    "Thread updated"

# Clean up when done
$ broker --cleanup
```

## Command Reference

Residual queue/broker operations (existence, metadata, delete, rename, aliases,
vacuum): `docs/specs/17-ops.md` (`[SB-OPS-1]`–`[SB-OPS-6]`). Delivery, identity,
selection, broadcast, dump/load, CLI packaging, and library surfaces have their
own specs (see `docs/specs/product-section-registry.md`).

### Global Options

Global options must appear before the command, for example `broker -f queue.db read jobs`.

- `-d, --dir PATH` - Use PATH instead of current directory
- `-f, --file NAME` - Database filename or absolute path (default: `.broker.db`)
  - If an absolute path is provided, the directory is extracted automatically
  - Cannot be used with `-d` if the directories don't match
- `-q, --quiet` - Suppress non-error output
- `--cleanup` - Delete the database file and exit
- `--vacuum` - Remove claimed messages and exit
- `--compact` - With `--vacuum`, also run SQLite VACUUM to reclaim disk space
- `--status` - Show global message count, last timestamp, and DB size (`--status --json` for JSON output)
- `--version` - Show version information
- `--help` - Show help message

### Commands

| Command | Description |
|---------|-------------|
| `write <queue> [message\|-]` | Add message to queue (omit or use `-` for stdin); `-t`/`--json` print the new message's ID |
| `read <queue> [options]` | Remove and return message(s) |
| `peek <queue> [options]` | Return message(s) without removing |
| `move <source> <dest> [options]` | Atomically transfer messages between queues |
| `rename <old> <new> [--json]` | Retag all existing messages from one queue name to another |
| `exists <queue> [--json]` | Check whether a queue has any messages, including claimed rows |
| `stats <queue> [--json]` | Show pending, claimed, and total counts for one queue |
| `list [--stats] [--prefix PREFIX \| --pattern GLOB] [--json]` | Show queue names; `--stats` adds counts |
| `delete <queue> [-m <id>]` | Delete a queue immediately, or physically delete a specific message by ID |
| `delete --all` | Delete all queues immediately |
| `broadcast [--pattern GLOB \| --queue QUEUE ...] <message\|->` | Send one message atomically to all existing queues, matching existing queues, or a repeatable exact set of existing literal queue names |
| `watch <queue> [options]` | Watch queue for new messages |
| `alias <add\|remove\|list>` | Manage queue aliases |
| `dump [--include <glob>] [--exclude <glob>]` | Write queues to stdout as `simplebroker-dump` v1 ndjson (pending only, deterministic; globs on queue names; aliases match name or target; exclude wins) — `[SB-IO-*]` |
| `load` | Restore a dump from stdin into a **fresh** broker (duplicate ids fail loudly); exit codes 0/1 — `[SB-IO-4]` |
| `init` | Initialize SimpleBroker database in current directory (does not accept `-d` or `-f` flags) |

`read --all`, `peek --all`, `dump`, and `watch` treat a downstream stdout
consumer closing its pipe as a clean shutdown. See [Pipe behavior](#pipe-behavior).

#### Queue Aliases

Normative: `docs/specs/17-ops.md` `[SB-OPS-5]`.

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
- `-m <id>` - Target a specific message by its exact 19-digit message ID
- `--after <timestamp>` - Process messages with id **strictly greater** than
  the bound (a filter, not a complete stream offset)
- `--before <timestamp>` - Process messages with id **strictly less** than the
  bound (`read`, `peek`, and `move`; not `watch`)

Normative filter predicates, late older ids (including after `move` or exact
insert), and watch progress: `docs/specs/14-timestamp-selection.md`
`[SB-SELECT-1]`–`[SB-SELECT-4]`. CLI string forms for non-exact bounds:
`[SB-CLI-5]` in `docs/specs/10-cli.md`.

**Write options:**
- `-t, --timestamps` - Print the new message's 19-digit timestamp ID on stdout
- `--json` - Print `{"timestamp": <id>}` for the new message (the message body
  is not echoed back, unlike read/peek JSON)

Place write output flags before the queue name (`broker write -t tasks "job"`).
They are also recognized after a literal message or after the stdin marker
`-`. A dash-leading operand after the queue name is still treated as literal
message content, so use `broker write -t tasks -- "-literal"` to combine a
flag with a message that starts with `-`.

**Watch options:**
- `--peek` - Monitor without consuming
- `--move <dest>` - Continuously drain to destination queue
- Use the global `-q` flag (`broker -q watch ...`) to suppress the startup message

**Queue metadata options:**
- `stats <queue>` reports counts for exactly one queue without scanning all queues.
- `exists <queue>` exits `0` when the queue has any row and `2` when it has none.
- `list` reports queue names. Use `list --stats` when you need counts.
- `list --prefix <prefix>` uses a literal queue-name prefix.
- `list --pattern <glob>` uses fnmatch-style matching.
- `--json` on `exists`, `stats`, or `list` emits JSON suitable for scripts.

Normative metadata and existence: `docs/specs/17-ops.md` `[SB-OPS-1]`–
`[SB-OPS-2]`.

Queues are implicit: a queue exists when at least one message row exists for
that name, including claimed rows. After vacuum removes claimed rows, a
claimed-only queue no longer exists.

**Timestamp formats for `--after` and `--before`:** see `[SB-CLI-5]`
(ISO, date-only UTC midnight, Unix s/ms/ns, native hybrid; suffixes
recommended). Bounds are strict open intervals after parse
(`[SB-SELECT-1]`).

`-m` / `--message` targets one exact 19-digit message id (`[SB-ID-4]`). A
malformed value errors and exits `1`; a well-formed id with no match is silent
and exits `2`.

### Exit Codes
- `0` - Success
- `1` - General error (e.g., database access error, invalid arguments)
- `2` - Queue empty or no matching messages

Normative detail: `docs/specs/10-cli.md` ([SB-CLI-1]–[SB-CLI-5]).

`watch` exits `0` when stopped by SIGINT/SIGTERM or when its stdout consumer
closes the pipe (see [Pipe behavior](#pipe-behavior)).

**Note:** `delete <queue>`, `delete --all`, and `delete <queue> -m <id>` remove
matching rows immediately (`[SB-OPS-3]`). Reads still use claimed-row semantics
and are reclaimed by `--vacuum` (`[SB-OPS-6]`).

## Critical Safety Notes

Delivery claim, peek, watch, and move rules:
`docs/specs/11-delivery.md` (`[SB-DELIVERY-1]`–`[SB-DELIVERY-7]`).

### Safe Message Handling

Messages can contain any characters including newlines, control characters, and shell metacharacters:
- **Shell injection risks** - When piping output to shell commands, malicious message content could execute unintended commands
- **Special characters** - Messages containing newlines or other special characters can break shell pipelines that expect single-line output
- **Queue names** - Limited to alphanumeric + underscore/hyphen/period (cannot start with hyphen or period)
- **Message size** - Limited to 10MB by default; override with `BROKER_MAX_MESSAGE_SIZE`
- **NUL characters** - Message bodies are UTF-8 text. The Postgres backend cannot store a raw NUL (`\x00`) character: `write()` raises `OperationalError` there, while SQLite and Redis round-trip it. JSON-encoded payloads are unaffected (compliant serializers escape NUL as `\u0000`), but avoid casting bodies to `jsonb` in sidecar queries: jsonb rejects `\u0000` even escaped.

**Always use `--json` for safe handling** - see examples below.

### Robust message handling with `watch`

When using `watch` in its default consuming mode, messages are
**permanently removed** from the queue *before* your script or handler
processes them. If your script fails or crashes, **the message is lost**.
For critical work, prefer atomically moving each message to an inflight queue,
then deleting it there after successful processing. Peek-then-delete is not a
reservation: it is safe only for a single consumer or when duplicate handling
is idempotent. Do not delete or move source rows while iterating `peek --all`
or `Queue.peek_generator()`, because their live offset pagination can skip
messages.

Normative delivery contract:
`docs/specs/11-delivery.md` ([SB-DELIVERY-1]–[SB-DELIVERY-7]).

Single-consumer example:

```bash
#!/bin/bash
# safe-worker.sh - single-consumer peek-and-acknowledge example
# For concurrent workers, use move-to-inflight instead.

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

Every stored message has a public integer message ID, exposed as `timestamp`
in JSON. Message bodies are payload and may duplicate. Producers should retain
the ID returned by `Queue.write()` or printed by `broker write -t` / `--json`;
`queue.last_ts` is a broker-global high-water cache, not the identity of that
write.

Broker-generated message IDs are positive and equal generation time within
the encoding grain (~4 µs). ID `0` is reserved as the lower-bound origin.
Exact selectors still accept zero so legacy rows can be inspected and cleaned
up. `move` preserves IDs. Exact insertion may supply caller-chosen IDs.
`--after` / `--before` are selection filters, not a guarantee that nothing
appears “behind” a bound.

ID representation, allocation, write returns, high-water/cache meaning,
exact-ID forms, and ID-preserving move are normative in the
[message identity contract](docs/specs/13-message-identity.md)
`[SB-ID-1]` through `[SB-ID-5]`.

Uniqueness is the ordinary coexistence rule for stored rows. Applications
needing durable idempotency persist the message ID themselves.

Exact-ID Python operations accept an integer ID or a string of exactly
19 decimal digits (`[SB-ID-4]`). Surrounding whitespace is stripped, and
"decimal digit" is Python's `str.isdecimal()`, so non-ASCII decimal digits
are accepted. Python `after_timestamp` / `before_timestamp` remain
integer bounds; CLI date and unit-suffix parsing apply only to CLI range
flags.


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

### Filtering by message id (`--after` / `--before`)

`--after` / `--before` are **filters** on message id after parse
(`[SB-SELECT-2]`). You may resume from a last-seen id with `--after`, but
that does not guarantee a complete history under moves or exact-id inserts
(`[SB-SELECT-3]`). Full rules:
`docs/specs/14-timestamp-selection.md`.

```bash
# Continue after a previously seen id
$ result=$(broker read tasks --json)
$ last=$(echo "$result" | jq '.timestamp')
$ broker read tasks --all --after "$last"

# Human-readable bound (CLI string forms: [SB-CLI-5])
$ broker read tasks --all --after "2024-01-15T14:30:00Z"

# Open interval
$ broker peek tasks --all --after "$start" --before "$end"

# Inspection only: include claimed rows not yet vacuumed
$ broker peek tasks --all --include-claimed
```

Claimed rows are deletion-pending — vacuum may remove them at any time;
`--include-claimed` is an inspection tool, not delivery state.

```bash
# Back up, restore, or migrate between backends (normative: [SB-IO-*]).
# Pending-only dump; load into a FRESH broker (duplicate ids fail loudly).
$ broker dump > backup.ndjson
$ broker dump --include 'tasks*' --exclude 'tasks_tmp' | (cd /fresh/dir && broker load)
$ broker dump | BROKER_BACKEND=postgres BROKER_BACKEND_TARGET="$DSN" broker load
```

Full dump/load and claimed-row inspection rules:
`docs/specs/15-persistence-io.md`.

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

# Send to an exact existing set
$ broker broadcast \
    --queue worker1 \
    --queue worker2 \
    "targeted shutdown"

# Each worker reads from its own queue
$ broker read worker1  # -> "shutdown signal"
$ broker read worker2  # -> "shutdown signal"
```

Broadcast can target all existing queues, names matching a pattern, or an
exact set of literal queue names. Python callers may explicitly create missing
exact targets. Selection, validation, result counts, queue-creation policy,
atomicity, CLI behavior, and backend compatibility are normative in the
[broadcast contract](docs/specs/12-broadcast.md) `[SB-BCAST-1]`
through `[SB-BCAST-6]`.

Broadcast is queue fan-out, not pub/sub: it inserts ordinary pending messages
into the selected queues. Aliases are not targets, and CLI broadcast never
creates queues.
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
3. **Move** (`--move DEST`): Continuously drain ALL messages to another queue

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

Move mode drains the entire source queue with one atomic move per message
and is incompatible with `--after`/`--before` filters (they would leave
messages stranded). Watch behavior is normative in
`docs/specs/11-delivery.md` (`[SB-DELIVERY-2]`, `[SB-DELIVERY-3]`); the
polling strategy and move-mode details are in the
[Python guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/python.md#watchers-in-depth).

### Pipe behavior

When the process consuming SimpleBroker's stdout exits (for example,
`broker watch q | head -1`), SimpleBroker stops at its next delivery attempt
and exits `0`. An idle watcher does not learn the pipe closed until it next
tries to write to it. With the default consume semantics, the message whose
delivery detected the closed pipe was already claimed and is not returned to
the queue; no further messages are claimed. A configured at-least-once
`read --all` batch instead rolls back its still-uncommitted batch when the
stream closes.

Exit `0` means SimpleBroker shut down cleanly. It does not validate that the
consumer processed any particular message; check the consumer's own exit
status.

## Python API

Normative public surfaces (package root, `simplebroker.ext`, command layer):
`docs/specs/16-python-library-api.md` (`[SB-API-1]`–`[SB-API-12]`).

The Python API mirrors the CLI over the same queue semantics:

```python
from simplebroker import Queue

with Queue("tasks") as q:
    message_id = q.write("process order 123")  # returns the committed message ID
    print(q.exists())
    print(q.stats())
    message = q.read()  # Returns: "process order 123"
```

### Delivery guarantees

Default operations are exactly-once: reads and moves commit the claim
atomically before your code sees the message. Generator APIs accept
`delivery_guarantee="at_least_once"` for retry-on-stop batch processing;
generators are thread-affine and must be closed on their own thread.
Normative rules: `docs/specs/11-delivery.md`
(`[SB-DELIVERY-1]`–`[SB-DELIVERY-7]`); worked patterns, generator rules,
and the cross-thread safety net are in the
[Python guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/python.md#delivery-guarantees-in-practice).

### Queue metadata

Normative: `docs/specs/17-ops.md` `[SB-OPS-1]`–`[SB-OPS-2]`.

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

### Watching from Python

```python
from simplebroker import QueueWatcher


def handle_message(msg: str, ts: int):
    print(f"Processing: {msg}")


watcher = QueueWatcher("orders", handle_message, db="my.db")
thread = watcher.run_in_thread()   # background thread; watcher.stop() to end
```

Watchers also work as context managers and support peek mode, error
handlers, and clean shutdown via `StopWatching`. Details:
[Python guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/python.md#watchers-in-depth).

### Going deeper

The [Python guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/python.md)
covers the rest of the API surface:

- delivery guarantees in practice, generators, and thread rules
- async integration and thread-based background processing
- cross-queue operations with `open_broker()` (broadcast, rename,
  `delete_from_queues`, `find_message_ids`)
- exact-ID workflows: `generate_timestamp()`, `insert_messages()`,
  `latest_pending_timestamp()`, high-water tracking (`[SB-ID-2]`–`[SB-ID-4]`)
- sidecar tables (`[SB-API-7]`), the reactor pattern, and activity waiters
- custom extensions (`BrokerCore` / `BrokerDB` boundaries)

## Embedding SimpleBroker in Your Project

For embedded use, put a small project-level client or context object in
front of SimpleBroker: resolve the broker target once, translate your
application's settings into `BROKER_*` keys, and hand out queues bound to
that target. Weft is the reference implementation of this pattern.

The full pattern — client shape, configuration snapshots, redaction rules,
and the `simplebroker.commands` command layer (`[SB-API-10]`, the
programmatic CLI equivalent) — is in the
[Python guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/python.md#embedding-simplebroker-in-your-project).

## Performance & Tuning

- **Throughput**: 1000+ messages/second on typical hardware
- **Latency**: <10ms for write, <10ms for read
- **Scalability**: Tested with 100k+ messages per queue
- **Optimization**: Use `--all` for bulk operations

### Cross-Backend Benchmarking

The repository includes a black-box CLI benchmark harness for SQLite,
Postgres, and Redis. Commands and options:
[backends guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/backends.md#cross-backend-benchmarking).

### Environment Variables

The most-used settings:

| Variable | Default | Purpose |
|----------|---------|---------|
| `BROKER_BUSY_TIMEOUT` | `5000` | SQLite busy timeout (ms) |
| `BROKER_SYNC_MODE` | `FULL` | Durability mode; `NORMAL` is ~25% faster with a small power-loss risk |
| `BROKER_READ_COMMIT_INTERVAL` | `1` | Messages per commit in `--all` mode; `1` keeps exactly-once delivery |
| `BROKER_DEFAULT_DB_NAME` | `.broker.db` | Database filename (all scopes) |
| `BROKER_PROJECT_SCOPE` | unset | Enable git-like upward project discovery |
| `BROKER_MAX_MESSAGE_SIZE` | 10MB | Maximum message body size |

The full catalog — all 32 keys, vacuum and watcher tuning, database and
config naming — is in the
[configuration guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/configuration.md).

## Project Scoping

By default, each directory gets its own isolated `.broker.db`, and a
`.broker.toml` in the current directory is honored. Two more modes cover
shared use:

- **Project scope** (`BROKER_PROJECT_SCOPE=true`): git-like upward search
  for a shared project config or database. Requires explicit
  `broker init`; SimpleBroker never creates databases in unexpected
  locations.
- **Global scope** (`BROKER_DEFAULT_DB_LOCATION=/abs/path`): one fixed
  location for all broker operations.

```bash
cd /project && broker init
export BROKER_PROJECT_SCOPE=true
cd /project/anywhere/below
broker write build-tasks "compile assets"   # shares /project/.broker.db
```

Discovery precedence, database and config naming, boundary and trust
rules, and security notes:
[configuration guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/configuration.md).
Do not put SQLite databases on network filesystems; use the Postgres or
Redis backends for multi-host access.


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

The mechanical internals — storage schema, claim lifecycle, and
cross-process setup coordination — are explained in
[docs/implementation/09-storage-schema-and-claim-lifecycle.md](docs/implementation/09-storage-schema-and-claim-lifecycle.md).
Security posture (queue-name validation, size limits, file permissions,
config-secret handling) is in the
[configuration guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/configuration.md).
Optional Postgres and Redis/Valkey backends, backend selection, and
backend authoring are in the
[backends guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/backends.md).

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
uv run pytest -m ""        # All tests including benchmarks
PHASELOCK_ENABLE_XATTRS=0 uv run pytest tests/test_phaselock.py tests/test_runner_validation.py tests/test_runner_error_handling.py tests/test_queue_config_defaults.py tests/test_sqlite_setup_contention.py
uv run ./bin/pytest-pg     # All PG-backed tests with automatic Docker setup/teardown
uv run ./bin/pytest-redis  # All Redis-backed tests with automatic Docker setup/teardown (Valkey)
HYPOTHESIS_PROFILE=ci uv run pytest tests/test_property_*.py  # deeper property-test run (50 -> 200 examples per property)
python fuzz/fuzz_timestamp_validate.py  # coverage-guided fuzzing via Atheris (Linux only; see fuzz/*.py)
uv run ./bin/pytest-pg -q tests/test_watcher_metrics.py -k basic
uv run ./bin/packaging-smoke --python 3.11

# Lint and format
uv run ruff check .
uv run ruff format simplebroker tests bin .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run mypy simplebroker bin/release.py
MYPYPATH=. uv run mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases \
  --allow-untyped-defs --allow-incomplete-defs \
  $(find tests -type f -name '*.py' -not -path '*/__pycache__/*' | sort)
```

The Ruff lint gate extends the locked release's stable defaults with the
repository's existing `E`, `W`, `F`, `I`, `B`, `C4`, and `UP` families. Lint
discovery covers all tracked Python files and Python-shebang tools. Formatting
keeps the explicit path boundary shown above and does not format Markdown.

CI uses one pinned uv release while local development accepts the compatible
uv 0.12 line. Update both policies and all three lockfiles with one command:

```bash
python bin/bump_uv.py \
  --ci-version 0.12.0 \
  --required-version '>=0.12.0,<0.13'
python bin/bump_uv.py --check
```

Run the update with system Python. This still works when a newly installed uv
falls outside the old repository range. Review the workflow and lockfile diffs
before running the normal tests.

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
python bin/release.py --version X.Y.Z

# Release simplebroker-pg
python bin/release.py pg --version X.Y.Z

# Release every current unpublished package version with one local check run
python bin/release.py all

# Preview the checks, version files, commit, and tag action
python bin/release.py --dry-run

# Read back the release-related GitHub settings without changing anything
uv run python bin/release.py --check-repository-settings
```

Replace `X.Y.Z` with the next unpublished version for the package being
released.

Real releases must run from `main`. The helper checks the target version against
GitHub Releases and PyPI, verifies the repository's immutable-release, tag,
environment, and Actions SHA-pinning settings, runs the local release checks,
updates and commits release files, and pushes the release commit to `main`. It
then waits for the target's normal workflows to pass on that exact commit and
checks that the commit is still reachable from a freshly fetched `origin/main`.
Only then does it create the final tag at the tested SHA and push it.

Remote release tags are permanent. They are never moved or deleted. A wrong
remote tag requires a new version. A local-only tag may be replaced before it
is pushed.

The tag workflow rechecks the normal workflows and tag SHA, builds and attests
the distributions, and stages every distribution and Sigstore bundle on a
draft GitHub Release. It publishes to PyPI with trusted publishing only after
that complete draft exists, then verifies the exact draft asset set and
publishes the immutable GitHub Release. Keeping the build, attestation, and
publish steps in the top-level gate workflow makes PyPI's trusted-publisher
identity match the artifact attestation build-config URI.
The local release helper also ruff-checks `examples/`, runs all
pytest-discovered example tests under `examples/`, mypy-checks every Python
example file, and mypy-checks the selected extension test tree. Core and batch
releases also mypy-check every root-test Python file; extension releases retain
their selected extension-test scope. Those extra local checks are not part of
the CI release workflows.
Core releases wait for `Test`, `Test Postgres Extension`, and
`Test Redis Extension`; extension releases wait for `Test` plus their matching
backend workflow.

If pre-tag CI fails, is cancelled, is missing, or times out, no final tag has
been created. Fix `main` and rerun the helper with the same unpublished
version. An interrupted helper can also be rerun at the same release commit;
it resumes the exact-SHA check without creating another release commit. If a
transient publication step fails after the tag exists, retry the workflow only
when the tag still points at the same SHA. If recovery needs a code change, use
the next patch version. Never delete, move, or reuse a published tag or version.

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
