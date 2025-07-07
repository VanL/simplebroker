# SimpleBroker

*A lightweight message queue backed by SQLite. No setup required, just works.*

```bash
$ pipx install simplebroker
$ broker write tasks "ship it 🚀"
$ broker read tasks
ship it 🚀
```

SimpleBroker gives you a zero-configuration message queue that runs anywhere Python runs. It's designed to be simple enough to understand in an afternoon, yet powerful enough for real work.

## Features

- **Zero configuration** - No servers, daemons, or complex setup
- **SQLite-backed** - Rock-solid reliability with true ACID guarantees  
- **Concurrent safe** - Multiple processes can read/write simultaneously
- **Simple CLI** - Intuitive commands that work with pipes and scripts
- **Portable** - Each directory gets its own isolated `.broker.db`
- **Fast** - 1000+ messages/second throughput
- **Lightweight** - ~700 lines of code, no external dependencies

## Installation

```bash
# Install with uv 
uv add simplebroker

# Or with pip
pip install simplebroker

# Or with pipx for global installation (recommended)
pipx install simplebroker
```

The CLI is available as both `broker` and `simplebroker`.

**Requirements:**
- Python 3.8+
- SQLite 3.35+ (released March 2021) - required for `DELETE...RETURNING` support

## Quick Start

```bash
# Create a queue and write a message
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

# List all queues
$ broker list
myqueue: 3

# Broadcast to all queues
$ broker broadcast "System maintenance at 5pm"

# Clean up when done
$ broker --cleanup
```

## Command Reference

### Global Options

- `-d, --dir PATH` - Use PATH instead of current directory
- `-f, --file NAME` - Database filename (default: `.broker.db`)
- `-q, --quiet` - Suppress non-error output (intended reads excepted)
- `--cleanup` - Delete the database file and exit
- `--version` - Show version information
- `--help` - Show help message

### Commands

| Command | Description |
|---------|-------------|
| `write <queue> <message>` | Add a message to the queue |
| `write <queue> -` | Add message from stdin |
| `read <queue> [--all] [--json]` | Remove and return message(s) |
| `peek <queue> [--all] [--json]` | Return message(s) without removing |
| `list` | Show all queues and message counts (note: counts are a snapshot and may change during concurrent operations) |
| `purge <queue>` | Delete all messages in queue |
| `purge --all` | Delete all queues |
| `broadcast <message>` | Send message to all existing queues |

### Exit Codes

- `0` - Success
- `1` - General error
- `2` - Queue is empty

## Examples

### Basic Queue Operations

```bash
# Create a work queue
$ broker write work "process customer 123"
$ broker write work "process customer 456"

# Worker processes tasks
$ while msg=$(broker read work 2>/dev/null); do
    echo "Processing: $msg"
    # do work...
done
```

### Using Multiple Queues

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

### Fan-out Pattern

```bash
# Send to all queues at once
$ broker broadcast "shutdown signal"

# Each worker reads from its own queue
$ broker read worker1  # -> "shutdown signal"
$ broker read worker2  # -> "shutdown signal"
```

**Note on broadcast behavior**: The `broadcast` command sends a message to all *existing* queues at the moment of execution. There's a small race window - if a new queue is created after the broadcast starts but before it completes, that queue won't receive the message. This is by design to keep the operation simple and atomic.

### Integration with Unix Tools

```bash
# Store command output
$ df -h | broker write monitoring -
$ broker peek monitoring

# Process files through a queue
$ find . -name "*.log" | while read f; do
    broker write logfiles "$f"
done

# Parallel processing with xargs
$ broker read logfiles --all | xargs -P 4 -I {} process_log {}
```

### Safe Handling with JSON Output

Messages containing newlines, quotes, or other special characters can break shell pipelines. The `--json` flag provides a safe way to handle any message content:

```bash
# Problem: Messages with newlines break shell processing
$ broker write alerts "ERROR: Database connection failed\nRetrying in 5 seconds..."
$ broker read alerts | wc -l
2  # Wrong! This is one message, not two

# Solution: Use --json for safe handling
$ broker write alerts "ERROR: Database connection failed\nRetrying in 5 seconds..."
$ broker read alerts --json
{"message": "ERROR: Database connection failed\nRetrying in 5 seconds..."}

# Parse JSON safely in scripts
$ broker read alerts --json | jq -r '.message'
ERROR: Database connection failed
Retrying in 5 seconds...

# Multiple messages with --all --json (outputs ndjson)
$ broker write safe "Line 1\nLine 2"
$ broker write safe 'Message with "quotes"'
$ broker write safe "Tab\there"
$ broker read safe --all --json
{"message": "Line 1\nLine 2"}
{"message": "Message with \"quotes\""}
{"message": "Tab\there"}

# Parse each line with jq
$ broker read safe --all --json | jq -r '.message'
Line 1
Line 2
Message with "quotes"
Tab	here
```

The JSON output uses line-delimited JSON (ndjson) format:
- Each message is output on its own line as: `{"message": "content"}`
- This format is streaming-friendly and works well with tools like `jq`

This is the recommended approach for handling messages that may contain special characters, as mentioned in the Security Considerations section.

### Remote Queue via SSH

```bash
# Write to remote queue
$ echo "remote task" | ssh server "cd /app && broker write tasks -"

# Read from remote queue  
$ ssh server "cd /app && broker read tasks"
```

## Design Philosophy

SimpleBroker follows the Unix philosophy: do one thing well. It's not trying to replace RabbitMQ or Redis - it's for when you need a queue without the complexity.

**What SimpleBroker is:**
- A simple, reliable message queue
- Perfect for scripts, cron jobs, and small services
- Easy to understand and debug
- Portable between environments

**What SimpleBroker is not:**
- A distributed message broker
- A pub/sub system
- A replacement for production message queues
- Suitable for high-frequency trading

## Technical Details

### Storage

Messages are stored in a SQLite database with Write-Ahead Logging (WAL) enabled for better concurrency. Each message is stored with:

```sql
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Ensures strict FIFO ordering
    queue TEXT NOT NULL,
    body TEXT NOT NULL,
    ts INTEGER NOT NULL                       -- Millisecond timestamp + hybrid logical clock 
)
```

The `id` column guarantees global FIFO ordering across all processes.
Note: FIFO ordering is strictly guaranteed by the `id` column, not the timestamp.

### Concurrency

SQLite's built-in locking handles concurrent access. Multiple processes can safely read and write simultaneously. Messages are delivered **exactly once** by default using atomic `DELETE...RETURNING` operations.

**Delivery Guarantees**
- **Default behavior**: All reads (single and bulk) provide exactly-once delivery with immediate commits
- **Performance optimization**: For bulk reads (`--all`), you can trade safety for speed by setting `BROKER_READ_COMMIT_INTERVAL` to a number greater than 1 to batch messages. If a consumer crashes mid-batch, uncommitted messages remain in the queue and will be redelivered to the next consumer (at-least-once delivery).

**FIFO Ordering Guarantee:**
- **True FIFO ordering across all processes**: Messages are always read in the exact order they were written to the database, regardless of which process wrote them
- **Guaranteed by SQLite's autoincrement**: Each message receives a globally unique, monotonically increasing ID
- **No ordering ambiguity**: Even when multiple processes write simultaneously, SQLite ensures strict serialization

### Performance

- **Throughput**: 1000+ messages/second on typical hardware
- **Latency**: <10ms for write, <10ms for read
- **Scalability**: Tested with 100k+ messages per queue

### Security

- Queue names are validated (alphanumeric + underscore + hyphen + period only, can't start with hyphen or period)
- Message size limited to 10MB
- Database files created with 0600 permissions
- SQL injection prevented via parameterized queries

**Security Considerations:**
- **Message bodies are not validated** - they can contain any text including newlines, control characters, and shell metacharacters
- **Shell injection risks** - When piping output to shell commands, malicious message content could execute unintended commands
- **Special characters** - Messages containing newlines or other special characters can break shell pipelines that expect single-line output
- **Recommended practice** - Always sanitize or validate message content before using it in shell commands or other security-sensitive contexts

### Environment Variables

SimpleBroker can be configured via environment variables:

- `BROKER_BUSY_TIMEOUT` - SQLite busy timeout in milliseconds (default: 5000)
- `BROKER_READ_COMMIT_INTERVAL` - Number of messages to read before committing in `--all` mode (default: 1)
  - Default of 1 provides exactly-once delivery guarantee (~10,000 messages/second)
  - Increase for better performance at the cost of at-least-once delivery:
    - 10: ~96,000 messages/second (9.4x faster)
    - 50: ~286,000 messages/second (28x faster)
    - 100: ~335,000 messages/second (33x faster)
  - Higher values mean more messages may be redelivered if consumer crashes

## Development

SimpleBroker uses [`uv`](https://github.com/astral-sh/uv) for package management and [`ruff`](https://github.com/astral-sh/ruff) for linting and formatting.

```bash
# Clone the repository
git clone git@github.com:VanL/simplebroker.git
cd simplebroker

# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install all dependencies including dev extras
uv sync --all-extras

# Run tests (fast tests only, in parallel)
uv run pytest

# Run all tests including slow ones (with 1000+ subprocess spawns)
uv run pytest -m ""

# Run tests with coverage
uv run pytest --cov=simplebroker --cov-report=term-missing

# Run specific test files
uv run pytest tests/test_smoke.py

# Run tests in a single process (useful for debugging)
uv run pytest -n 0

# Lint and format code
uv run ruff check simplebroker tests  # Check for issues
uv run ruff check --fix simplebroker tests  # Fix auto-fixable issues
uv run ruff format simplebroker tests  # Format code

# Type check
uv run mypy simplebroker
```

### Development Workflow

1. **Before committing**:
   ```bash
   uv run ruff check --fix simplebroker tests
   uv run ruff format simplebroker tests
   uv run mypy simplebroker
   uv run pytest
   ```

2. **Building packages**:
   ```bash
   uv build  # Creates wheel and sdist in dist/
   ```

3. **Installing locally for testing**:
   ```bash
   uv pip install dist/simplebroker-*.whl
   ```

## Contributing

Contributions are welcome! Please:

1. Keep it simple - the entire codebase should stay under 1000 lines
2. Maintain backward compatibility
3. Add tests for new features
4. Update documentation
5. Run `uv run ruff` and `uv run pytest` before submitting PRs

### Setting up for development

```bash
# Fork and clone the repo
git clone git@github.com:VanL/simplebroker.git
cd simplebroker

# Install development environment
uv sync --all-extras

# Create a branch for your changes
git checkout -b my-feature

# Make your changes, then validate
uv run ruff check --fix simplebroker tests
uv run ruff format simplebroker tests
uv run pytest

# Push and create a pull request
git push origin my-feature
```

## License

MIT © 2025 Van Lindberg

## Acknowledgments

Built with Python, SQLite, and the Unix philosophy. 