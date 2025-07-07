# SimpleBroker

*A lightweight message queue backed by SQLite. No setup required, just works.*

```bash
$ pip install simplebroker
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
- **Lightweight** - ~550 lines of code, no external dependencies

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
- `-q, --quiet` - Suppress non-error output
- `--cleanup` - Delete the database file and exit
- `--version` - Show version information
- `--help` - Show help message

### Commands

| Command | Description |
|---------|-------------|
| `write <queue> <message>` | Add a message to the queue |
| `write <queue> -` | Add message from stdin |
| `read <queue> [--all]` | Remove and return message(s) |
| `peek <queue> [--all]` | Return message(s) without removing |
| `list` | Show all queues and message counts |
| `purge <queue>` | Delete all messages in queue |
| `purge --all` | Delete all queues |
| `broadcast <message>` | Send message to all existing queues |

### Exit Codes

- `0` - Success
- `3` - Queue is empty
- `1` - General error

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

Messages are stored in a SQLite database with Write-Ahead Logging (WAL) enabled for better concurrency. Each queue is implemented as a table:

```sql
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue TEXT NOT NULL,
    body TEXT NOT NULL,
    ts REAL DEFAULT ((julianday('now') - 2440587.5) * 86400.0)
)
```

### Concurrency

SQLite's built-in locking handles concurrent access. Multiple processes can safely read and write simultaneously. Messages are delivered exactly once using atomic DELETE operations.

**FIFO Ordering Guarantees:**
- **Within a process**: Strict FIFO ordering is guaranteed - messages are always read in the exact order they were written
- **Across processes**: Messages are ordered by actual write time to the database, not submission time. Due to process scheduling, messages may interleave when multiple processes write concurrently
- **Unique timestamps**: Each message receives a unique timestamp using microsecond precision plus a counter, preventing ambiguous ordering

### Performance

- **Throughput**: 1000+ messages/second on typical hardware
- **Latency**: <10ms for write, <10ms for read
- **Scalability**: Tested with 100k+ messages per queue

### Security

- Queue names are validated (alphanumeric + underscore + hyphen only)
- Message size limited to 10MB
- Database files created with 0600 permissions
- SQL injection prevented via parameterized queries

## Development

SimpleBroker uses [`uv`](https://github.com/astral-sh/uv) for package management and [`ruff`](https://github.com/astral-sh/ruff) for linting and formatting.

```bash
# Clone the repository
git clone git@github.com:VanL/simplebroker.git
cd simplebroker

# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=simplebroker --cov-report=term-missing

# Lint and format code
ruff check simplebroker tests  # Check for issues
ruff check --fix simplebroker tests  # Fix auto-fixable issues
ruff format simplebroker tests  # Format code

# Type check
mypy simplebroker
```

### Development Workflow

1. **Before committing**:
   ```bash
   ruff check --fix simplebroker tests
   ruff format simplebroker tests
   mypy simplebroker
   pytest
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
5. Run `ruff` and `pytest` before submitting PRs

### Setting up for development

```bash
# Fork and clone the repo
git clone git@github.com:VanL/simplebroker.git
cd simplebroker

# Install development environment
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"

# Create a branch for your changes
git checkout -b my-feature

# Make your changes, then validate
ruff check --fix simplebroker tests
ruff format simplebroker tests
pytest

# Push and create a pull request
git push origin my-feature
```

## License

MIT © 2025 Van Lindberg

## Acknowledgments

Built with Python, SQLite, and the Unix philosophy. 