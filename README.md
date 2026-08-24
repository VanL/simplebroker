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
  not have to set up a queue server. Zero configuration: no servers, no
  daemons; each directory gets its own isolated `.broker.db` with ACID
  durability and safe concurrent access. On an M4 MacBook Pro,
  [`bin/benchmark.py`](bin/benchmark.py) measured 962.8 mixed ops/second
  through the default API and 7,879.6 through the persistent optimized API.
- **Shell scripts, cron jobs, and CI/CD pipelines.** `broker write tasks
  "build #123"` composes with pipes, exit codes, and `--json` like a Unix
  tool — decouple script stages, coordinate build steps, buffer logs, or
  pass work between processes on one machine.
- **Coding agents that need a queue primitive.** The CLI gives agents a durable
  coordination point without an MCP server, daemon, or project-specific setup.
- **Library and tool authors embedding queue semantics.** Use a small client or
  context object over SimpleBroker, translate your app settings into `BROKER_*`
  config, and hand out queues bound to one resolved broker target. Weft is the
  reference implementation of this pattern.
- **Event-driven workflows** via the built-in real-time watcher.

**Not for:** Broker fleets, pub/sub, distributed task frameworks, or applications
needing very high scale or throughput (like high-frequency trading).

## Table of Contents

- [SimpleBroker](#simplebroker)
  - [Recommended For](#recommended-for)
  - [The SimpleBroker Model](#the-simplebroker-model)
  - [The SimpleBroker API](#the-simplebroker-api)
  - [Project Specifications and Agent Instructions](#project-specifications-and-agent-instructions)
  - [Installation](#installation)
  - [Quick Start](#quick-start)
  - [Command Reference](#command-reference)
  - [Critical Safety Notes](#critical-safety-notes)
  - [Core Concepts](#core-concepts)
  - [Common Patterns](#common-patterns)
  - [Real-time Queue Watching](#real-time-queue-watching)
  - [Embedding SimpleBroker in Your Project](#embedding-simplebroker-in-your-project)
  - [Performance & Tuning](#performance--tuning)
  - [Project Scoping](#project-scoping)
  - [Going Further](#going-further)
  - [License](#license)

## The SimpleBroker Model

SimpleBroker's core concept is the durable named queue - basically Unix pipes
made durable, inspectable, and resumable. Cooperating processes exchange 
messages through named queues on one resolved broker target (the "backend"). 

- SimpleBroker owns the semantics of the queue operations: write, claim,
peek, move, delete, watch. 
- The backend owns storage and topology. SQLite is the default. Optional Postgres
or Redis/Valkey services can also be used and provide their own replication,
availability, and recovery features, separate from SimpleBroker.
- The application or script using SimpleBroker owns what messages mean: task
execution, business retries, worker topology, and completion.

SimpleBroker's one concession to application state is the ability to route 
`sidecar` tables to the same backend (on the SQLite and pg backends). This
allows embedders to use the same configuration information and connection - but
SimpleBroker does not constrain what is in sidecar tables.

## The SimpleBroker API

The SimpleBroker API is designed to mirror the CLI: `broker write tasks "hi"` 
and `Queue("tasks").write("hi")` mean the same queue operation over the same 
resolved target.

The same configuration and tuning capabilities are resolved from the environment
or configuration files (`BROKER_*` keys) and passed into functions and classes.
Embedders translate their own settings into those keys to avoid name clashes.

The public API consists of the names exported from `simplebroker` and 
`simplebroker.ext`. Underscore-prefixed modules are implementation details and
may change. Specifications: `docs/specs/16-python-library-api.md` (`[SB-API-1]`–`[SB-API-12]`).

## Project Specifications and Agent Instructions

Exact product behavior is owned by the canonical specs in `docs/specs/` 
(`10-cli.md` … `17-ops.md`, codes `[SB-CLI-*]` … `[SB-OPS-*]`), registered in
`docs/specs/product-section-registry.md`. 

Agents should use `docs/agent-kernel.md` for use orientation. The `AGENTS.md`
file is the entry point for agents doing work on this codebase, including the
[program-theory.md](https://github.com/VanL/simplebroker/blob/main/docs/program-theory.md)
and the broader agent context and runbooks.

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

# After stopping all users, destructively remove the target state
$ broker --cleanup
```

## Command Reference

Residual queue/broker operations (existence, metadata, delete, rename, aliases,
vacuum, destructive target cleanup): `docs/specs/17-ops.md`
(`[SB-OPS-1]`–`[SB-OPS-7]`). Delivery, identity, selection, broadcast,
dump/load, CLI packaging, and library surfaces have their own specs (see
`docs/specs/product-section-registry.md`).

### Global Options

Global options must appear before the command, for example `broker -f queue.db read jobs`.

- `-d, --dir PATH` - Use PATH instead of current directory
- `-f, --file NAME` - Database filename or absolute path (default: `.broker.db`)
  - If an absolute path is provided, the directory is extracted automatically
  - Cannot be used with `-d` if the directories don't match
- `-q, --quiet` - Suppress non-error human commentary, including the plain
  message-newline and alias-shadow warnings. Payload, errors, and unrelated
  Python warnings remain visible.
- `--cleanup` - Destructively delete the configured backend target state and
  exit. SQLite cleanup attempts the database and its known SQLite and
  SimpleBroker companion files. It is non-atomic; stop all activity and make
  any required backup first (`[SB-OPS-7]`).
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

`read`, `peek`, `move`, `dump`, and `watch` treat a downstream stdout consumer
closing its pipe as a clean shutdown, including exact-message and `--all`
forms. Effects completed before the failed output remain completed. See
[Pipe behavior](#pipe-behavior).

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
- Alias names and targets use the normal queue-name syntax. The target is a
  canonical queue name, but it need not contain messages yet. New aliases must
  remain flat: neither creation order may make an alias point to another
  alias. Conflicting concurrent additions yield one winner.
- Legacy invalid alias rows remain listable, one-hop resolvable, and removable;
  SimpleBroker does not rewrite or recursively resolve them.
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
- `--json` - Print `{"timestamp": "<19-digit-id>"}` for the new message (the message body
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

Queues are implicit: a queue exists when at least one message row exists for
that name, including claimed rows. After vacuum removes claimed rows, a
claimed-only queue no longer exists. Normative specs: `docs/specs/17-ops.md` 
`[SB-OPS-1]`–`[SB-OPS-2]`.

**Timestamp formats for `--after` and `--before`:** 
- ISO 8601: `2024-01-15T14:30:00Z` or `2024-01-15` (midnight UTC)
- Unix seconds: `1705329000` or `1705329000s`
- Unix milliseconds: `1705329000000ms`
- Unix nanoseconds/Native hybrid: `1837025672140161024` or `1837025672140161024ns`

Fractional seconds are not supported in ISO, bare numeric, or suffixed numeric
bounds. Use integer `ms`, integer `ns`, or a native hybrid message ID when you
need finer granularity than seconds. Numeric spellings contain decimal digits
only; signs and separators such as `_` are rejected. Unicode decimal digits are
accepted and normalized before parsing.

**Best practice:** Heuristics distinguish bare numeric values for interactive
use, but explicit suffixes (`s`/`ms`/`ns`) are recommended when a particular
unit is intended.

`--after` and `--before` use strict open bounds. Combined together, they select
messages where `after_timestamp < message_timestamp < before_timestamp`.

`-m` / `--message` is stricter than `--after` and `--before`: it accepts only
the exact 19-digit broker message ID. A malformed `-m` value prints
`invalid message ID: expected exactly 19 digits within range` on stderr and
exits `1`. A well-formed ID that does not match a message is silent and exits
`2`.

Normative specifications: see `[SB-ID-4]`, `[SB-CLI-5]`
(ISO, date-only UTC midnight, Unix s/ms/ns, native hybrid; suffixes
recommended). Bounds are strict open intervals after parse
(`[SB-SELECT-1]`).

### Exit Codes
- `0` - Success
- `1` - General error (e.g., database access error, invalid arguments)
- `2` - Queue empty or no matching messages
- `130` - An unhandled keyboard interrupt reached the outer CLI wrapper

Normative specifications: `docs/specs/10-cli.md` ([SB-CLI-1]–[SB-CLI-5]).

`watch` exits `0` when stopped by SIGINT/SIGTERM or when its stdout consumer
closes the pipe (see [Pipe behavior](#pipe-behavior)). Other commands return
`130` when a `KeyboardInterrupt` escapes to the process wrapper; effects
completed before the interrupt are not rolled back.

**Note:** `delete <queue>`, `delete --all`, and `delete <queue> -m <id>` remove
matching rows immediately (`[SB-OPS-3]`). Reads still use claimed-row semantics
and are reclaimed by `--vacuum` (`[SB-OPS-6]`).

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
`docs/specs/11-delivery.md` ([SB-DELIVERY-1]–[SB-DELIVERY-8]).

Single-consumer example: [`examples/safe_worker.sh`](https://github.com/VanL/simplebroker/blob/main/examples/safe_worker.sh)
polls one message at a time and acknowledges it by deleting its exact ID only
after the configured `PROCESS_TASK` command succeeds. It exits on processing,
acknowledgement, parse, or broker failure, so the current process cannot advance
past a failed message. The Bash example preserves trailing newlines but rejects
NUL payloads, which shell variables cannot represent. JSON message IDs are
exact 19-digit strings, and the script validates that shape before streaming
bodies to the handler's standard input. For concurrent workers, use the
move-to-inflight recipe in
[`docs/agent-kernel.md`](https://github.com/VanL/simplebroker/blob/main/docs/agent-kernel.md).

## Core Concepts

### Timestamps as Message IDs
Every stored message has a public integer message ID. Python and storage keep
that integer domain; SimpleBroker-owned JSON exposes it as an exact 19-digit
ASCII decimal string (usually in `timestamp`). Broker-generated IDs include a
physical time component, but caller-supplied exact IDs need not represent
creation time. Timestamps can be included in regular output by passing the
`-t` / `--timestamps` flag.

Built-in SimpleBroker JSON already uses the canonical string. When JSON owned
by your application includes a broker message ID or high-water value returned
by the Python API, format that field through the public package-root helper:

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

The application-owned `source_message_id` name is illustrative. Convert only
known broker identity fields in JSON you construct; do not rewrite message
bodies or unrelated application timestamps. See the
[Python embedding guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/python.md#serializing-message-ids-in-application-json)
for the full boundary rules.

Stored message IDs are:
- **Unique within a backend** - No two stored rows share an ID.
- **Monotonic when broker-generated** - Exact insertion may add a smaller ID
  later, and `move` preserves an existing ID.
- **Efficient** - They are 64-bit integers, not UUIDs.
- **Time-related when broker-generated** - The physical component reflects
  generation time within the encoding grain.

Message bodies are payload only and may duplicate byte-for-byte. Message IDs are
the sole durable identity for targeted broker operations and application-level
deduplication. Because vacuum physically removes claimed rows, SimpleBroker does
not retain a permanent tombstone for every historical ID; consumers that require
idempotency should persist and deduplicate by message ID.

Broker-generated message IDs are positive and equal generation time within
the encoding grain (~4 µs). Exact insertion via the API may supply caller-chosen
IDs except `0`, which is reserved as a lower-bound and empty-high-water origin.
Exact selectors accept zero so legacy rows can be inspected and cleaned up.
The `move` operation preserves IDs.

The value `queue.last_ts` is a broker-global high-water cache, not the identity
of the last write or a record of the broker's last activity.

The format:
- High 52 bits: physical component from `time.time_ns()`, aligned to 4096 ns
  steps rather than counted in microseconds.
- Low 12 bits: logical counter.
- The format is compatible with nanosecond Unix time, but the effective time
  grain is ~4 µs (4096 ns). The physical component is not a microsecond counter.

Python APIs that target one exact message ID, such as
`Queue.read(message_id=...)`, `Queue.peek(message_id=...)`,
`Queue.move(message_id=...)`, `Queue.delete(message_id=...)`,
`Queue.delete_many(...)`, and exact-ID granular methods, accept either an
integer ID or an exact 19-digit string ID. Malformed string IDs raise
`ValueError`; unsupported types, including `bool`, raise `TypeError`.
`Queue.delete()` with no argument is the intentional queue-wide delete and
returns whether it removed anything. `Queue.delete(message_id=None)` is
ambiguous and raises `TypeError` before storage mutation; pass a concrete ID
for targeted deletion.

High-level `Queue.move()` returns an ordinary dictionary described by the
package-root `MovedMessage` `TypedDict`, with `message: str` and
`timestamp: int`. `all_messages=True` returns an iterator of the same
dictionaries. The granular move methods retain their string/tuple shapes.

ID representation, allocation, write returns, high-water/cache meaning,
exact-ID forms, and ID-preserving move are normative in the
[message identity contract](https://github.com/VanL/simplebroker/blob/main/docs/specs/13-message-identity.md)
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

Messages with newlines or special characters can break shell pipelines. Use
`--json` to avoid shell issues. JSON message IDs are strings, so `jq -r`
extracts their exact digit text without numeric conversion.
Loud plain `read`, `peek`, `move`, and `watch` output warns once when any
emitted body contains an embedded newline, including exact, range, all, and
timestamped selections. `--quiet` suppresses that commentary; JSON output
never emits it.

```bash
# Problem: newlines break line counting
$ printf 'ERROR: Database connection failed\nRetrying in 5 seconds...' | broker write alerts -
$ broker read alerts | wc -l
2  # Wrong! One message counted as two

# Solution: JSON output (line-delimited)
$ printf 'ERROR: Database connection failed\nRetrying in 5 seconds...' | broker write alerts -
$ broker read alerts --json
{"message": "ERROR: Database connection failed\nRetrying in 5 seconds...", "timestamp": "1837025672140161024"}

# Parse safely with jq
$ broker read alerts --json | jq -r '.message'
ERROR: Database connection failed
Retrying in 5 seconds...
```

### Filtering by message id (`--after` / `--before`)

`--after` / `--before` and the corresponding API-level `after_timestamp` and 
`before_timestamp` arguments are integer bounds used for selection only. ID `0` is 
used as the lower-bound origin so rows moved into a queue or inserted via the API can
be inspected and cleaned up. 

Normative specifications: `[SB-SELECT-2]` and (`[SB-SELECT-3]`. Full rules:
`docs/specs/14-timestamp-selection.md`.

```bash
# Continue after a previously seen id
$ result=$(broker read tasks --json)
$ last=$(echo "$result" | jq -r '.timestamp')
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
# Pending-only bounded dump; load is mutating and intended for a FRESH broker.
# The header bounds message IDs and restores broker-global allocation high-water.
# Future clock skew warns; excessive skew refuses before mutation unless forced.
$ broker dump > backup.ndjson
$ broker dump --include 'tasks*' --exclude 'tasks_tmp' | (cd /fresh/dir && broker load)
$ broker dump | BROKER_BACKEND=postgres BROKER_BACKEND_TARGET="$DSN" broker load
# Explicit recovery escape hatch; still warns and may impair writes until catch-up.
$ broker load --force < future-watermark-backup.ndjson
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
[broadcast contract](https://github.com/VanL/simplebroker/blob/main/docs/specs/12-broadcast.md) `[SB-BCAST-1]`
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

# Reserving work with an atomic move: see the move-to-inflight recipe
# in docs/agent-kernel.md (safe under concurrent workers).

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
# Single-consumer peek-and-ack loop. process_task_json reads one JSON object
# from stdin; successful processing is acknowledged by exact-id deletion.
while true; do
    if msg_json=$(broker peek tasks --json); then
        :
    else
        rc=$?
        [ "$rc" -eq 2 ] && break  # queue is empty
        exit "$rc"                # broker failure is fatal
    fi

    # Validate the exact string token before passing it back to the CLI.
    msg_id=$(printf '%s\n' "$msg_json" | python3 -c \
        'import json, sys; v=json.load(sys.stdin)["timestamp"]; (type(v) is str and len(v) == 19 and v.isascii() and v.isdecimal() and int(v) < 2**63) or sys.exit("invalid message ID"); print(v)') || exit 1

    if printf '%s\n' "$msg_json" | process_task_json; then
        broker delete tasks -m "$msg_id" >/dev/null || exit 1
    else
        # Exact-id move is atomic: a failed move leaves the source pending.
        broker move tasks dlq -m "$msg_id" >/dev/null || exit 1
    fi
done

# Retry failed messages
broker move dlq tasks --all
```
</details>

For a checkpointing worker with atomic checkpoint updates and
per-message acknowledge-by-delete, explicit empty-queue exit handling, and
fatal operational-error handling, see
[`examples/resilient_worker.sh`](https://github.com/VanL/simplebroker/blob/main/examples/resilient_worker.sh).
Set `PROCESS_EVENT` to one executable command/path to replace its demonstration
handler; the message is streamed to the command's standard input. Its checkpoint
is an informational record of the last acknowledgement, not a `--after` filter,
because older pending IDs can arrive later through exact insertion or move.

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
{"message": "task 1", "timestamp": "1837025672140161024"}

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
stream closes. Exact reads and moves may have completed their atomic claim or
move before output detects closure; `move --all` completes its selected atomic
moves before printing them. Those completed effects are not reversed.

Exit `0` means SimpleBroker shut down cleanly. It does not validate that the
consumer processed any particular message; check the consumer's own exit
status.

That clean-stop rule is limited to `read`, `peek`, `move`, `dump`, and
`watch`. If a downstream consumer closes while a finite result such as help,
version, `list`, `stats`, status, or an opted-in write/rename result is being
delivered, SimpleBroker returns `1` with a controlled stderr diagnostic. A
write or rename may already be durable; inspect broker state before retrying.

### Delivery guarantees

Default operations claim atomically: the claim commits before your code
sees the message — the consume claim boundary, `[SB-DELIVERY-1]`. That is
broker delivery state, not proof of application processing
(see [Critical Safety Notes](#critical-safety-notes)). Generator APIs
accept `delivery_guarantee="at_least_once"` (`[SB-DELIVERY-5]`) for
retry-on-stop batch processing; generators are thread-affine and must be
closed on their own thread.

Specifications: `docs/specs/11-delivery.md`
(`[SB-DELIVERY-1]`–`[SB-DELIVERY-8]`); worked patterns, generator rules,
and the cross-thread safety net are in the
[Python guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/python.md#delivery-guarantees-in-practice).

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

Specifications: `docs/specs/17-ops.md` `[SB-OPS-1]`–`[SB-OPS-2]`.

### Watching from Python

```python
from simplebroker import QueueWatcher


def handle_message(msg: str, ts: int):
    print(f"Processing: {msg}")


watcher = QueueWatcher("orders", handle_message, db="my.db")
thread = watcher.run_in_thread()  # background thread; watcher.stop() to end
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
application’s settings into `BROKER_*` keys, and hand out queues bound to
that target. Use `resolve_isolated_config()` and preserve its immutable
`ResolvedConfig` result when ambient `BROKER_*` must not affect the embedding
application. Use `snapshot_config()` when several handles should deliberately
share one ambient-derived configuration receipt. New handles otherwise sample
current ambient configuration at their documented construction or invocation
boundary; existing handles remain fixed. Weft is the reference implementation
of the client shape.

The full pattern — client shape, configuration snapshots, redaction rules,
and the `simplebroker.commands` command layer (`[SB-API-10]`, the
programmatic CLI equivalent) — is in the
[Python guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/python.md#embedding-simplebroker-in-your-project).

## Performance & Tuning

[`bin/benchmark.py`](bin/benchmark.py) records a best-of-three
operations/second matrix across workload, access type, and backend. This M4
MacBook Pro run used 100 operations per sample, 100-byte messages, automatic
vacuum disabled, and all other SimpleBroker settings at their defaults:

| Backend | Access | Writes | Reads | Peeks | Mixed |
|---------|--------|-------:|------:|------:|------:|
| `sqlite` | `cli` | 14.7 | 12.4 | 12.5 | 13.1 |
| `sqlite` | `api` | 909.1 | 898.2 | 1,139.8 | 962.8 |
| `sqlite` | `optimized-api` | 7,125.2 | 5,945.4 | 22,314.6 | 7,879.6 |
| `pg` | `cli` | 6.3 | 5.8 | 5.9 | 6.1 |
| `pg` | `api` | 105.7 | 113.1 | 130.9 | 102.5 |
| `pg` | `optimized-api` | 767.5 | 1,039.8 | 4,423.3 | 1,402.0 |
| `redis` | `cli` | 8.0 | 7.9 | 8.0 | 8.0 |
| `redis` | `api` | 250.2 | 263.0 | 205.7 | 145.8 |
| `redis` | `optimized-api` | 2,145.0 | 5,252.6 | 3,934.5 | 3,700.5 |

`cli` includes a fresh Python process for every operation. `api` uses the
default ephemeral connection behavior. `optimized-api` runs the same calls
with `persistent=True`. These are point-in-time measurements from a short
local run, not performance guarantees; topology, hardware, load, and software
versions will change the result. The attributable raw measurements are in the
[result artifact](benchmarks/results/2026-08-10-m4-matrix.json). Use `--all`
for bulk operations where its
delivery semantics fit the workload.

For normal use in the embedding or shell-tool context, SimpleBroker is
unlikely to be the bottleneck: the processes it coordinates typically take
milliseconds to minutes per work item. Full numbers and tuning guidance:
[configuration guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/configuration.md#performance-and-tuning).

### Cross-Backend Benchmarking

The repository benchmark measures CLI, API, and optimized API access across
SQLite, Postgres, and Redis. Reproduction commands and workload definitions:
[backends guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/backends.md#cross-backend-benchmarking).
An opt-in `--sqlite-tuning` argument appends a separate SQLite setting
sensitivity table without expanding the default matrix.

### Environment Variables

Most users will not need to adjust any settings. If tuning is desired, the most likely settings will be: 

| Variable | Default | Purpose |
|----------|---------|---------|
| `BROKER_BUSY_TIMEOUT` | `5000` | SQLite busy timeout (ms) |
| `BROKER_SYNC_MODE` | `FULL` | Durability mode; `NORMAL` can improve write throughput with a small power-loss risk, so benchmark the tradeoff on your workload |
| `BROKER_READ_COMMIT_INTERVAL` | `1` | Messages per commit in `--all` mode; `1` keeps the per-message claim boundary (`[SB-DELIVERY-1]`), higher values batch with at-least-once semantics (`[SB-DELIVERY-5]`) |
| `BROKER_DEFAULT_DB_NAME` | `.broker.db` | Database filename (all scopes) |
| `BROKER_PROJECT_SCOPE` | unset | Enable git-like upward project discovery |
| `BROKER_MAX_MESSAGE_SIZE` | 10MB | Maximum message body size |

The full settings catalog — vacuum and watcher tuning, database and
config naming, and the rest of the documented keys — is in the
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
For use by more than one OS user, that guide defines the required effective
permissions on the SQLite database, all companion files, and their containing
directory. SimpleBroker does not provision or preserve a cross-user sharing
policy.
Do not put SQLite databases on network filesystems; use the Postgres or
Redis backends for multi-host access.


## Development & Contributing

Development setup, the test harness, lint and type checks, and the
release procedure are in
[CONTRIBUTING.md](https://github.com/VanL/simplebroker/blob/main/CONTRIBUTING.md).
Keep it simple, maintain backward compatibility, add tests, and update
documentation.


## Going Further

| Need | Where |
|------|-------|
| Agent-oriented use and embedding kernel | [docs/agent-kernel.md](https://github.com/VanL/simplebroker/blob/main/docs/agent-kernel.md) |
| Advanced Python API, embedding, sidecar, reactor | [Python guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/python.md) |
| Full configuration, scoping, tuning, security | [Configuration guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/configuration.md) |
| Postgres / Redis backends and backend authoring | [Backends guide](https://github.com/VanL/simplebroker/blob/main/docs/guides/backends.md) |
| Exact behavior contracts (`[SB-*]` codes) | [docs/specs/](https://github.com/VanL/simplebroker/blob/main/docs/specs/00-specs-index.md) — CLI `docs/specs/10-cli.md`, delivery `docs/specs/11-delivery.md`, broadcast `docs/specs/12-broadcast.md`, identity `docs/specs/13-message-identity.md`, selection `docs/specs/14-timestamp-selection.md`, dump/load `docs/specs/15-persistence-io.md`, library `docs/specs/16-python-library-api.md`, operations `docs/specs/17-ops.md` |
| Runnable examples (workers, DLQ, migration, reactor) | [examples/](https://github.com/VanL/simplebroker/blob/main/examples/README.md) |
| Storage internals and design rationale | [docs/implementation/](https://github.com/VanL/simplebroker/blob/main/docs/implementation/00-implementation-index.md) |
| Behavior changes by release | [CHANGELOG.md](https://github.com/VanL/simplebroker/blob/main/CHANGELOG.md) |
| Contributing and releases | [CONTRIBUTING.md](https://github.com/VanL/simplebroker/blob/main/CONTRIBUTING.md) |


## License

MIT © Van Lindberg

## Acknowledgments

Built with Python, SQLite, and the Unix philosophy.
