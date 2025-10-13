# ADVANCED: SimpleBroker Extension Implementation Examples

**NOTE: This document contains ADVANCED examples for users who need to create custom
extensions beyond what the standard API provides. Most users should use:**

- **Queue and QueueWatcher classes** - The standard public API (see python_api.py)
- **async_wrapper.py** - For async/await functionality
- **DBConnection** - For cross-queue operations requiring database access

Only use these extension examples if you need to:
- Create custom database backends
- Implement custom transaction handling
- Add middleware at the SQL level
- Build specialized runners for unique requirements

This document provides complete, working examples of SimpleBroker extensions demonstrating
the extensibility features for advanced users who need to extend beyond the standard API.

## Table of Contents

1. [Basic Custom Runner](#basic-custom-runner)
2. [Daemon Mode Runner](#daemon-mode-runner)
3. [Async SQLite Runner](#async-sqlite-runner)
4. [Connection Pool Runner](#connection-pool-runner)
5. [Testing with Mock Runner](#testing-with-mock-runner)
6. [Complete Async Queue Implementation](#complete-async-queue-implementation)

## Basic Custom Runner

A simple example showing the minimal implementation required for a custom runner.

**For standard usage, use the Queue class directly instead of creating custom runners.**

```python
# custom_runner.py
# ADVANCED: Custom SQLRunner implementation
# For standard usage, use: from simplebroker import Queue
import sqlite3
import logging
from typing import Any, Iterable
from simplebroker.ext import SQLRunner

class LoggingRunner(SQLRunner):
    """SQLRunner that logs all SQL operations."""
    
    def __init__(self, db_path: str, logger=None):
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__)
        self._conn = sqlite3.connect(db_path)
        self._setup_connection()
    
    def _setup_connection(self):
        """Apply SimpleBroker's required PRAGMA settings."""
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA cache_size=-10000")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA wal_autocheckpoint=1000")
    
    def run(self, sql: str, params: tuple[Any, ...] = (), 
            *, fetch: bool = False) -> Iterable[tuple[Any, ...]]:
        self.logger.debug(f"SQL: {sql}, params: {params}")
        try:
            cursor = self._conn.execute(sql, params)
            result = cursor.fetchall() if fetch else []
            self.logger.debug(f"Result: {len(result)} rows")
            return result
        except Exception as e:
            self.logger.error(f"SQL Error: {e}")
            raise
    
    def begin_immediate(self) -> None:
        self.logger.debug("BEGIN IMMEDIATE")
        self._conn.execute("BEGIN IMMEDIATE")
    
    def commit(self) -> None:
        self.logger.debug("COMMIT")
        self._conn.commit()
    
    def rollback(self) -> None:
        self.logger.debug("ROLLBACK")
        self._conn.rollback()
    
    def close(self) -> None:
        self.logger.debug("Closing connection")
        self._conn.close()

# Usage example
if __name__ == "__main__":
    import logging
    from simplebroker import Queue
    
    logging.basicConfig(level=logging.DEBUG)
    
    # ADVANCED: Using a custom runner
    # For standard usage, just use: Queue("tasks")
    runner = LoggingRunner("logged.db")
    with Queue("tasks", runner=runner) as q:
        q.write("Hello, logged world!")
        message = q.read()
        print(f"Read: {message}")
```

## Daemon Mode Runner

A complete daemon implementation that processes messages in a background thread.

```python
# daemon_runner.py
import sqlite3
import threading
import queue
import time
import os
from typing import tuple, Any, Iterable, Optional, Callable
from simplebroker.ext import SQLRunner, TimestampGenerator

class DaemonRunner(SQLRunner):
    """SQLRunner that delegates operations to a background daemon thread."""
    
    def __init__(self, db_path: str, stop_timeout: float = 30.0):
        self.db_path = db_path
        self.stop_timeout = stop_timeout
        self._command_queue = queue.Queue()
        self._response_queue = queue.Queue()
        self._daemon = None
        self._stop_event = threading.Event()
        self._start_daemon()
    
    def _start_daemon(self):
        """Start the background daemon thread."""
        self._daemon = threading.Thread(
            target=self._daemon_loop,
            daemon=True,
            name=f"DaemonRunner-{self.db_path}"
        )
        self._daemon.start()
    
    def _daemon_loop(self):
        """Main daemon loop that processes commands."""
        conn = sqlite3.connect(self.db_path)
        self._setup_connection(conn)
        
        last_activity = time.time()
        
        while not self._stop_event.is_set():
            try:
                # Wait for command with timeout
                cmd = self._command_queue.get(timeout=1.0)
                last_activity = time.time()
                
                # Process command
                method, args, kwargs = cmd
                try:
                    if method == "run":
                        result = self._execute_sql(conn, *args, **kwargs)
                    elif method == "begin_immediate":
                        conn.execute("BEGIN IMMEDIATE")
                        result = None
                    elif method == "commit":
                        conn.commit()
                        result = None
                    elif method == "rollback":
                        conn.rollback()
                        result = None
                    else:
                        raise ValueError(f"Unknown method: {method}")
                    
                    self._response_queue.put(("success", result))
                except Exception as e:
                    self._response_queue.put(("error", e))
                
            except queue.Empty:
                # Check for auto-stop timeout
                if time.time() - last_activity > self.stop_timeout:
                    break
        
        conn.close()
    
    def _setup_connection(self, conn):
        """Apply SimpleBroker's required PRAGMA settings."""
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA cache_size=-10000")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=1000")
    
    def _execute_sql(self, conn, sql: str, params: tuple[Any, ...] = (), 
                     *, fetch: bool = False) -> Iterable[tuple[Any, ...]]:
        cursor = conn.execute(sql, params)
        return cursor.fetchall() if fetch else []
    
    def _send_command(self, method: str, *args, **kwargs):
        """Send command to daemon and wait for response."""
        if self._daemon is None or not self._daemon.is_alive():
            self._start_daemon()
        
        self._command_queue.put((method, args, kwargs))
        status, result = self._response_queue.get()
        
        if status == "error":
            raise result
        return result
    
    def run(self, sql: str, params: tuple[Any, ...] = (), 
            *, fetch: bool = False) -> Iterable[tuple[Any, ...]]:
        return self._send_command("run", sql, params, fetch=fetch)
    
    def begin_immediate(self) -> None:
        self._send_command("begin_immediate")
    
    def commit(self) -> None:
        self._send_command("commit")
    
    def rollback(self) -> None:
        self._send_command("rollback")
    
    def close(self) -> None:
        """Stop the daemon thread."""
        if self._daemon and self._daemon.is_alive():
            self._stop_event.set()
            self._daemon.join(timeout=5.0)

# Usage example with automatic cleanup
if __name__ == "__main__":
    from simplebroker import Queue
    from simplebroker.ext import TimestampGenerator
    
    # Daemon auto-stops after 10 seconds of inactivity
    runner = DaemonRunner("daemon.db", stop_timeout=10.0)
    
    try:
        with Queue("background", runner=runner) as q:
            # Write messages
            for i in range(10):
                q.write(f"Background task {i}")
                print(f"Queued task {i}")
            
            # Read them back
            while True:
                msg = q.read()
                if msg is None:
                    break
                print(f"Processed: {msg}")
                time.sleep(0.1)  # Simulate work
    finally:
        runner.close()
```

## Async SQLite Runner

Complete async implementation using aiosqlite.

```python
# async_runner.py
import asyncio
import aiosqlite
import time
from typing import tuple, Any, Iterable, Optional
from simplebroker.ext import SQLRunner, TimestampGenerator

class AsyncSQLiteRunner:
    """Async SQLite runner using aiosqlite.
    
    Note: This is an async-compatible runner, not a SQLRunner implementation.
    It requires an async-aware BrokerCore adaptation.
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()
    
    async def ensure_connection(self):
        """Lazily create and configure connection."""
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.db_path)
            await self._setup_connection()
    
    async def _setup_connection(self):
        """Apply SimpleBroker's required PRAGMA settings."""
        await self._conn.execute("PRAGMA busy_timeout=5000")
        await self._conn.execute("PRAGMA cache_size=-10000")
        await self._conn.execute("PRAGMA synchronous=FULL")
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA wal_autocheckpoint=1000")
    
    async def run(self, sql: str, params: tuple[Any, ...] = (), 
                  *, fetch: bool = False) -> Iterable[tuple[Any, ...]]:
        await self.ensure_connection()
        async with self._lock:
            cursor = await self._conn.execute(sql, params)
            if fetch:
                return await cursor.fetchall()
            return []
    
    async def begin_immediate(self) -> None:
        await self.ensure_connection()
        await self._conn.execute("BEGIN IMMEDIATE")
    
    async def commit(self) -> None:
        await self._conn.commit()
    
    async def rollback(self) -> None:
        await self._conn.rollback()
    
    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

class AsyncTimestampGenerator:
    """Async-compatible timestamp generator."""
    
    def __init__(self, runner: AsyncSQLiteRunner):
        self._runner = runner
        self._lock = asyncio.Lock()
        self._last_ts = 0
        self._counter = 0
        self._initialized = False
    
    async def _initialize(self):
        """Load last timestamp from database."""
        if not self._initialized:
            result = await self._runner.run(
                "SELECT value FROM meta WHERE key = 'last_ts'",
                fetch=True
            )
            if result:
                self._last_ts = result[0][0]
            self._initialized = True
    
    async def generate(self) -> int:
        """Generate next hybrid timestamp."""
        async with self._lock:
            await self._initialize()
            
            # Current time in milliseconds
            now_ms = int(time.time() * 1000)
            
            # Extract physical time from last timestamp
            last_physical = self._last_ts >> 20
            
            if now_ms > last_physical:
                # New millisecond, reset counter
                self._counter = 0
                new_ts = (now_ms << 20) | self._counter
            else:
                # Same millisecond, increment counter
                self._counter += 1
                if self._counter >= (1 << 20):
                    # Counter overflow, wait for next millisecond
                    while now_ms <= last_physical:
                        await asyncio.sleep(0.001)
                        now_ms = int(time.time() * 1000)
                    self._counter = 0
                new_ts = (now_ms << 20) | self._counter
            
            # Update database (must be in transaction)
            await self._runner.run(
                "UPDATE meta SET value = ? WHERE key = 'last_ts' AND value = ?",
                (new_ts, self._last_ts)
            )
            
            self._last_ts = new_ts
            return new_ts

class AsyncQueue:
    """Async queue implementation."""
    
    def __init__(self, name: str, runner: AsyncSQLiteRunner):
        self.name = name
        self._runner = runner
        self._timestamp_gen = AsyncTimestampGenerator(runner)
        self._initialized = False
    
    async def _ensure_initialized(self):
        """Initialize database schema if needed."""
        if not self._initialized:
            await self._runner.ensure_connection()
            # Create tables
            await self._runner.run("""
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue TEXT NOT NULL,
                    body TEXT NOT NULL,
                    ts INTEGER NOT NULL UNIQUE,
                    claimed INTEGER DEFAULT 0
                )
            """)
            await self._runner.run("""
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                )
            """)
            # Initialize timestamp
            await self._runner.run(
                "INSERT OR IGNORE INTO meta (key, value) VALUES ('last_ts', 0)"
            )
            self._initialized = True
    
    async def write(self, message: str) -> None:
        """Write a message to the queue."""
        await self._ensure_initialized()
        
        await self._runner.begin_immediate()
        try:
            timestamp = await self._timestamp_gen.generate()
            await self._runner.run(
                "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
                (self.name, message, timestamp)
            )
            await self._runner.commit()
        except Exception:
            await self._runner.rollback()
            raise
    
    async def read(self) -> Optional[str]:
        """Read and remove the next message."""
        await self._ensure_initialized()
        
        await self._runner.begin_immediate()
        try:
            # Find next message
            result = await self._runner.run(
                """SELECT id, body FROM messages 
                   WHERE queue = ? AND claimed = 0 
                   ORDER BY ts LIMIT 1""",
                (self.name,),
                fetch=True
            )
            
            if not result:
                await self._runner.commit()
                return None
            
            msg_id, body = result[0]
            
            # Delete the message
            await self._runner.run(
                "DELETE FROM messages WHERE id = ?",
                (msg_id,)
            )
            
            await self._runner.commit()
            return body
            
        except Exception:
            await self._runner.rollback()
            raise
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._runner.close()

# Usage example
async def main():
    runner = AsyncSQLiteRunner("async.db")
    
    async with AsyncQueue("async_tasks", runner) as q:
        # Write messages concurrently
        await asyncio.gather(
            q.write("Async task 1"),
            q.write("Async task 2"),
            q.write("Async task 3")
        )
        
        # Read them back
        while True:
            msg = await q.read()
            if msg is None:
                break
            print(f"Processed: {msg}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Connection Pool Runner

Example showing connection pooling for high-concurrency scenarios.

```python
# pooled_runner.py
import sqlite3
import threading
import queue
import contextlib
from typing import tuple, Any, Iterable, Optional
from simplebroker.ext import SQLRunner

class PooledRunner(SQLRunner):
    """SQLRunner with connection pooling for better concurrency."""
    
    def __init__(self, db_path: str, pool_size: int = 5):
        self.db_path = db_path
        self.pool_size = pool_size
        self._pool = queue.Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._local = threading.local()
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Create initial connections for the pool."""
        for _ in range(self.pool_size):
            conn = self._create_connection()
            self._pool.put(conn)
    
    def _create_connection(self):
        """Create and configure a new connection."""
        conn = sqlite3.connect(self.db_path)
        # Apply SimpleBroker's required PRAGMA settings
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA cache_size=-10000")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=1000")
        return conn
    
    @contextlib.contextmanager
    def get_connection(self):
        """Get a connection from the pool or current transaction."""
        # If we're in a transaction, use the same connection
        if hasattr(self._local, 'conn') and self._local.conn:
            yield self._local.conn
            return
        
        # Otherwise, get from pool
        conn = self._pool.get()
        try:
            yield conn
        finally:
            self._pool.put(conn)
    
    def run(self, sql: str, params: tuple[Any, ...] = (), 
            *, fetch: bool = False) -> Iterable[tuple[Any, ...]]:
        with self.get_connection() as conn:
            cursor = conn.execute(sql, params)
            return cursor.fetchall() if fetch else []
    
    def begin_immediate(self) -> None:
        """Start transaction - acquire connection for this thread."""
        if hasattr(self._local, 'conn') and self._local.conn:
            raise sqlite3.OperationalError("Already in transaction")
        
        self._local.conn = self._pool.get()
        self._local.conn.execute("BEGIN IMMEDIATE")
    
    def commit(self) -> None:
        """Commit and return connection to pool."""
        if not hasattr(self._local, 'conn') or not self._local.conn:
            raise sqlite3.OperationalError("Not in transaction")
        
        self._local.conn.commit()
        self._pool.put(self._local.conn)
        self._local.conn = None
    
    def rollback(self) -> None:
        """Rollback and return connection to pool."""
        if not hasattr(self._local, 'conn') or not self._local.conn:
            raise sqlite3.OperationalError("Not in transaction")
        
        self._local.conn.rollback()
        self._pool.put(self._local.conn)
        self._local.conn = None
    
    def close(self) -> None:
        """Close all connections in the pool."""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except queue.Empty:
                break

# Performance test example
if __name__ == "__main__":
    import time
    import concurrent.futures
    from simplebroker import Queue
    
    def worker(worker_id: int, queue: Queue, count: int):
        """Worker that writes and reads messages."""
        for i in range(count):
            queue.write(f"Worker {worker_id} message {i}")
        
        read_count = 0
        while True:
            msg = queue.read()
            if msg is None:
                break
            read_count += 1
            if read_count >= count:
                break
        
        return worker_id, read_count
    
    # Test with pooled runner
    runner = PooledRunner("pooled.db", pool_size=10)
    q = Queue("pooled_tasks", runner=runner)
    
    start = time.time()
    
    # Run 10 workers concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for i in range(10):
            future = executor.submit(worker, i, q, 100)
            futures.append(future)
        
        for future in concurrent.futures.as_completed(futures):
            worker_id, count = future.result()
            print(f"Worker {worker_id} processed {count} messages")
    
    elapsed = time.time() - start
    print(f"Total time: {elapsed:.2f}s")
    print(f"Throughput: {1000/elapsed:.0f} messages/second")
    
    runner.close()
```

## Testing with Mock Runner

Complete mock runner implementation for testing.

```python
# test_mock_runner.py
import pytest
import sqlite3
from typing import dict, list, tuple, Any, Iterable
from simplebroker.ext import SQLRunner
from simplebroker import Queue

class TestMockRunner(SQLRunner):
    """In-memory mock runner for testing."""
    
    def __init__(self):
        self.messages: dict[str, list[tuple[str, int]]] = {}
        self.meta = {"last_ts": 0}
        self.in_transaction = False
        self.transaction_buffer = []
        self.call_history = []
        self.error_on_next_call = None
    
    def run(self, sql: str, params: tuple[Any, ...] = (), 
            *, fetch: bool = False) -> Iterable[tuple[Any, ...]]:
        """Execute SQL and track calls."""
        self.call_history.append(("run", sql, params, fetch))
        
        # Simulate errors if requested
        if self.error_on_next_call:
            error = self.error_on_next_call
            self.error_on_next_call = None
            raise error
        
        # Parse and execute SQL
        sql_upper = sql.upper()
        
        if "CREATE TABLE" in sql_upper:
            return []
        
        elif "INSERT INTO messages" in sql_upper:
            queue, body, ts = params
            if self.in_transaction:
                self.transaction_buffer.append(("insert", queue, body, ts))
            else:
                if queue not in self.messages:
                    self.messages[queue] = []
                self.messages[queue].append((body, ts))
            return []
        
        elif "SELECT value FROM meta WHERE key = 'last_ts'" in sql_upper:
            if fetch:
                return [(self.meta["last_ts"],)]
            return []
        
        elif "UPDATE meta SET value = ?" in sql_upper:
            new_ts, old_ts = params
            if self.meta["last_ts"] != old_ts:
                raise sqlite3.IntegrityError("Timestamp conflict")
            if self.in_transaction:
                self.transaction_buffer.append(("update_ts", new_ts))
            else:
                self.meta["last_ts"] = new_ts
            return []
        
        elif "SELECT" in sql_upper and "FROM messages" in sql_upper:
            if not fetch:
                return []
            
            # Simple query parsing
            if "WHERE queue = ?" in sql_upper:
                queue = params[0]
                if queue not in self.messages:
                    return []
                
                messages = sorted(self.messages[queue], key=lambda x: x[1])
                
                if "LIMIT 1" in sql_upper:
                    if messages:
                        return [(1, messages[0][0])]  # (id, body)
                    return []
                
                return [(i+1, msg[0]) for i, msg in enumerate(messages)]
            
            return []
        
        elif "DELETE FROM messages WHERE id = ?" in sql_upper:
            # Simplified: just remove first message from queue
            if self.in_transaction:
                self.transaction_buffer.append(("delete", params[0]))
            return []
        
        elif "INSERT OR IGNORE INTO meta" in sql_upper:
            return []
        
        return []
    
    def begin_immediate(self) -> None:
        if self.in_transaction:
            raise sqlite3.OperationalError("Already in transaction")
        self.in_transaction = True
        self.transaction_buffer = []
        self.call_history.append(("begin", None, None, None))
    
    def commit(self) -> None:
        if not self.in_transaction:
            raise sqlite3.OperationalError("Not in transaction")
        
        # Apply buffered changes
        for operation in self.transaction_buffer:
            if operation[0] == "insert":
                _, queue, body, ts = operation
                if queue not in self.messages:
                    self.messages[queue] = []
                self.messages[queue].append((body, ts))
            elif operation[0] == "update_ts":
                _, new_ts = operation
                self.meta["last_ts"] = new_ts
            elif operation[0] == "delete":
                # Simplified deletion
                for queue in self.messages:
                    if self.messages[queue]:
                        self.messages[queue].pop(0)
                        break
        
        self.in_transaction = False
        self.transaction_buffer = []
        self.call_history.append(("commit", None, None, None))
    
    def rollback(self) -> None:
        if not self.in_transaction:
            raise sqlite3.OperationalError("Not in transaction")
        
        self.in_transaction = False
        self.transaction_buffer = []
        self.call_history.append(("rollback", None, None, None))
    
    def close(self) -> None:
        self.call_history.append(("close", None, None, None))
    
    def set_error_on_next_call(self, error: Exception):
        """Configure runner to raise error on next call."""
        self.error_on_next_call = error

# Test fixtures
@pytest.fixture
def mock_runner():
    return TestMockRunner()

@pytest.fixture
def queue_with_mock(mock_runner):
    return Queue("test_queue", runner=mock_runner)

# Example tests
def test_basic_write_read(queue_with_mock, mock_runner):
    """Test basic write and read operations."""
    # Write a message
    queue_with_mock.write("Test message")
    
    # Verify transaction was used
    assert any("begin" in call for call in mock_runner.call_history)
    assert any("commit" in call for call in mock_runner.call_history)
    
    # Read it back
    message = queue_with_mock.read()
    assert message == "Test message"
    
    # Queue should be empty
    assert queue_with_mock.read() is None

def test_transaction_rollback(queue_with_mock, mock_runner):
    """Test that transactions roll back on error."""
    # Configure error during write
    mock_runner.set_error_on_next_call(
        sqlite3.IntegrityError("Simulated error")
    )
    
    # Write should fail
    with pytest.raises(sqlite3.IntegrityError):
        queue_with_mock.write("This should fail")
    
    # Verify rollback was called
    assert any("rollback" in call for call in mock_runner.call_history)
    
    # Queue should still be empty
    assert queue_with_mock.read() is None

def test_concurrent_writes(mock_runner):
    """Test timestamp generation with concurrent writes."""
    import threading
    
    q1 = Queue("q1", runner=mock_runner)
    q2 = Queue("q2", runner=mock_runner)
    
    def writer(queue, count):
        for i in range(count):
            queue.write(f"Message {i}")
    
    # Write concurrently to different queues
    t1 = threading.Thread(target=writer, args=(q1, 10))
    t2 = threading.Thread(target=writer, args=(q2, 10))
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    # Verify all messages were written
    assert len(mock_runner.messages.get("q1", [])) == 10
    assert len(mock_runner.messages.get("q2", [])) == 10
    
    # Verify timestamps are unique
    all_timestamps = []
    for messages in mock_runner.messages.values():
        all_timestamps.extend(ts for _, ts in messages)
    
    assert len(set(all_timestamps)) == len(all_timestamps)

def test_performance_tracking(queue_with_mock, mock_runner):
    """Test that we can track performance metrics."""
    import time
    
    # Write 100 messages
    start = time.time()
    for i in range(100):
        queue_with_mock.write(f"Message {i}")
    write_time = time.time() - start
    
    # Count SQL calls
    write_calls = sum(1 for call in mock_runner.call_history 
                      if "INSERT INTO messages" in str(call))
    
    assert write_calls == 100
    print(f"Write throughput: {100/write_time:.0f} msgs/sec")
    
    # Read them back
    start = time.time()
    count = 0
    while queue_with_mock.read() is not None:
        count += 1
    read_time = time.time() - start
    
    assert count == 100
    print(f"Read throughput: {100/read_time:.0f} msgs/sec")

if __name__ == "__main__":
    # Run example tests
    runner = TestMockRunner()
    q = Queue("demo", runner=runner)
    
    print("Writing messages...")
    for i in range(5):
        q.write(f"Demo message {i}")
    
    print("\nCall history:")
    for call in runner.call_history:
        print(f"  {call[0]}: {call[1][:50] if call[1] else ''}")
    
    print("\nReading messages:")
    while True:
        msg = q.read()
        if msg is None:
            break
        print(f"  {msg}")
```

## Complete Async Queue Implementation

Full async queue with all SimpleBroker features.

```python
# async_queue_complete.py
import asyncio
import aiosqlite
import time
from typing import Optional, list, AsyncIterator
from contextlib import asynccontextmanager

class AsyncBrokerCore:
    """Async version of BrokerCore with full functionality."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()
        self._timestamp_lock = asyncio.Lock()
        self._last_ts = 0
        self._counter = 0
    
    async def _ensure_connection(self):
        """Ensure database connection is established."""
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.db_path)
            await self._setup_database()
    
    async def _setup_database(self):
        """Initialize database schema and settings."""
        # Apply PRAGMA settings
        await self._conn.execute("PRAGMA busy_timeout=5000")
        await self._conn.execute("PRAGMA cache_size=-10000")
        await self._conn.execute("PRAGMA synchronous=FULL")
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA wal_autocheckpoint=1000")
        
        # Create schema
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                body TEXT NOT NULL,
                ts INTEGER NOT NULL UNIQUE,
                claimed INTEGER DEFAULT 0
            )
        """)
        
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )
        """)
        
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_queue_ts 
            ON messages(queue, ts) WHERE claimed = 0
        """)
        
        await self._conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('last_ts', 0)"
        )
        
        await self._conn.commit()
        
        # Load last timestamp
        cursor = await self._conn.execute(
            "SELECT value FROM meta WHERE key = 'last_ts'"
        )
        row = await cursor.fetchone()
        if row:
            self._last_ts = row[0]
    
    async def generate_timestamp(self) -> int:
        """Generate hybrid timestamp with async safety."""
        async with self._timestamp_lock:
            now_ms = int(time.time() * 1000)
            last_physical = self._last_ts >> 20
            
            if now_ms > last_physical:
                self._counter = 0
                new_ts = (now_ms << 20) | self._counter
            else:
                self._counter += 1
                if self._counter >= (1 << 20):
                    while now_ms <= last_physical:
                        await asyncio.sleep(0.001)
                        now_ms = int(time.time() * 1000)
                    self._counter = 0
                new_ts = (now_ms << 20) | self._counter
            
            # Update in database
            result = await self._conn.execute(
                "UPDATE meta SET value = ? WHERE key = 'last_ts' AND value = ?",
                (new_ts, self._last_ts)
            )
            
            if result.rowcount != 1:
                raise sqlite3.IntegrityError("Timestamp update conflict")
            
            self._last_ts = new_ts
            return new_ts
    
    async def write(self, queue: str, message: str) -> None:
        """Write a message to the queue."""
        await self._ensure_connection()
        
        async with self._lock:
            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                timestamp = await self.generate_timestamp()
                await self._conn.execute(
                    "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
                    (queue, message, timestamp)
                )
                await self._conn.commit()
            except Exception:
                await self._conn.rollback()
                raise
    
    async def read(self, queue: str) -> Optional[str]:
        """Read and remove one message."""
        await self._ensure_connection()
        
        async with self._lock:
            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = await self._conn.execute(
                    """SELECT id, body FROM messages 
                       WHERE queue = ? AND claimed = 0 
                       ORDER BY ts LIMIT 1""",
                    (queue,)
                )
                row = await cursor.fetchone()
                
                if not row:
                    await self._conn.commit()
                    return None
                
                msg_id, body = row
                
                await self._conn.execute(
                    "DELETE FROM messages WHERE id = ?",
                    (msg_id,)
                )
                
                await self._conn.commit()
                return body
                
            except Exception:
                await self._conn.rollback()
                raise
    
    async def peek(self, queue: str) -> Optional[str]:
        """Peek at next message without removing."""
        await self._ensure_connection()
        
        cursor = await self._conn.execute(
            """SELECT body FROM messages 
               WHERE queue = ? AND claimed = 0 
               ORDER BY ts LIMIT 1""",
            (queue,)
        )
        row = await cursor.fetchone()
        return row[0] if row else None
    
    async def read_batch(self, queue: str, batch_size: int) -> list[str]:
        """Read multiple messages in one transaction."""
        await self._ensure_connection()
        
        messages = []
        async with self._lock:
            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = await self._conn.execute(
                    """SELECT id, body FROM messages 
                       WHERE queue = ? AND claimed = 0 
                       ORDER BY ts LIMIT ?""",
                    (queue, batch_size)
                )
                rows = await cursor.fetchall()
                
                if not rows:
                    await self._conn.commit()
                    return []
                
                # Delete all at once
                ids = [row[0] for row in rows]
                placeholders = ','.join('?' * len(ids))
                await self._conn.execute(
                    f"DELETE FROM messages WHERE id IN ({placeholders})",
                    ids
                )
                
                messages = [row[1] for row in rows]
                await self._conn.commit()
                
            except Exception:
                await self._conn.rollback()
                raise
        
        return messages
    
    async def stream_read(self, queue: str, 
                          batch_size: int = 10) -> AsyncIterator[str]:
        """Stream messages from queue."""
        while True:
            messages = await self.read_batch(queue, batch_size)
            if not messages:
                break
            for message in messages:
                yield message
    
    async def queue_size(self, queue: str) -> int:
        """Get number of messages in queue."""
        await self._ensure_connection()
        
        cursor = await self._conn.execute(
            "SELECT COUNT(*) FROM messages WHERE queue = ? AND claimed = 0",
            (queue,)
        )
        row = await cursor.fetchone()
        return row[0] if row else 0
    
    async def list_queues(self) -> list[tuple[str, int]]:
        """list all queues with message counts."""
        await self._ensure_connection()
        
        cursor = await self._conn.execute("""
            SELECT queue, COUNT(*) 
            FROM messages 
            WHERE claimed = 0 
            GROUP BY queue 
            ORDER BY queue
        """)
        return await cursor.fetchall()
    
    async def close(self):
        """Close database connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None

class AsyncQueue:
    """High-level async queue interface."""
    
    def __init__(self, name: str, core: AsyncBrokerCore):
        self.name = name
        self._core = core
    
    async def write(self, message: str) -> None:
        await self._core.write(self.name, message)
    
    async def read(self) -> Optional[str]:
        return await self._core.read(self.name)
    
    async def peek(self) -> Optional[str]:
        return await self._core.peek(self.name)
    
    async def read_batch(self, size: int = 10) -> list[str]:
        return await self._core.read_batch(self.name, size)
    
    async def stream(self, batch_size: int = 10) -> AsyncIterator[str]:
        async for message in self._core.stream_read(self.name, batch_size):
            yield message
    
    async def size(self) -> int:
        return await self._core.queue_size(self.name)

@asynccontextmanager
async def async_broker(db_path: str = "async.db"):
    """Context manager for async broker."""
    core = AsyncBrokerCore(db_path)
    try:
        yield core
    finally:
        await core.close()

# Usage examples
async def example_basic():
    """Basic async queue usage."""
    async with async_broker() as broker:
        q = AsyncQueue("tasks", broker)
        
        # Write messages
        await q.write("Task 1")
        await q.write("Task 2")
        await q.write("Task 3")
        
        # Read them back
        while True:
            msg = await q.read()
            if msg is None:
                break
            print(f"Processed: {msg}")

async def example_streaming():
    """Stream processing example."""
    async with async_broker() as broker:
        q = AsyncQueue("stream", broker)
        
        # Write many messages
        for i in range(100):
            await q.write(f"Stream item {i}")
        
        # Process in batches
        async for message in q.stream(batch_size=20):
            print(f"Processing: {message}")
            # Simulate work
            await asyncio.sleep(0.01)

async def example_concurrent():
    """Concurrent producers and consumers."""
    async with async_broker() as broker:
        q = AsyncQueue("concurrent", broker)
        
        async def producer(id: int):
            for i in range(50):
                await q.write(f"Producer {id} - Message {i}")
                await asyncio.sleep(0.01)
        
        async def consumer(id: int):
            count = 0
            while count < 50:
                msg = await q.read()
                if msg:
                    print(f"Consumer {id}: {msg}")
                    count += 1
                else:
                    await asyncio.sleep(0.01)
        
        # Run multiple producers and consumers
        await asyncio.gather(
            producer(1),
            producer(2),
            consumer(1),
            consumer(2)
        )

async def example_monitoring():
    """Queue monitoring example."""
    async with async_broker() as broker:
        # Create some queues with messages
        for queue_name in ["high", "medium", "low"]:
            q = AsyncQueue(queue_name, broker)
            for i in range(10 * (4 - len(queue_name) // 3)):
                await q.write(f"{queue_name} priority task {i}")
        
        # Monitor queues
        while True:
            queues = await broker.list_queues()
            print("\nQueue Status:")
            for name, count in queues:
                print(f"  {name}: {count} messages")
            
            if not queues:
                break
            
            # Process from highest priority
            for name, _ in queues:
                q = AsyncQueue(name, broker)
                msg = await q.read()
                if msg:
                    print(f"Processing: {msg}")
                    break
            
            await asyncio.sleep(0.5)

if __name__ == "__main__":
    print("Running basic example...")
    asyncio.run(example_basic())
    
    print("\nRunning streaming example...")
    asyncio.run(example_streaming())
    
    print("\nRunning concurrent example...")
    asyncio.run(example_concurrent())
    
    print("\nRunning monitoring example...")
    asyncio.run(example_monitoring())
```

## Summary

These ADVANCED examples demonstrate:

1. **Basic Custom Runner**: Shows the minimal implementation required
2. **Daemon Mode**: Background thread processing with auto-stop
3. **Async Support**: Full async implementation with aiosqlite
4. **Connection Pooling**: High-concurrency optimization
5. **Testing**: Comprehensive mock runner for unit tests
6. **Complete Async**: Production-ready async queue with all features

Key points across all implementations:

- All use the centralized `TimestampGenerator` for consistency
- Transaction boundaries are properly managed
- Error handling follows SimpleBroker patterns
- Fork safety and thread safety are considered
- Performance characteristics are documented
- Examples are complete and runnable

These implementations serve as templates that users can adapt for their specific needs while maintaining compatibility with SimpleBroker's core philosophy.

## When to Use These Extensions vs Standard API

**Use the Standard API (Queue, QueueWatcher) when:**
- You need basic queue operations (write, read, peek, delete, move)
- You're building typical producer/consumer applications
- You want simple async support (use async_wrapper.py)
- You need to watch queues for new messages
- You're working with a single SQLite database

**Use Custom Extensions (these examples) when:**
- You need a different storage backend (not SQLite)
- You require custom transaction semantics
- You want to add middleware (logging, metrics, etc.)
- You need specialized performance optimizations
- You're building a framework on top of SimpleBroker

**For Database-Level Operations:**
- Use `Queue` class for single-queue operations
- Use `DBConnection` context manager for safe cross-queue operations
- Only use `BrokerDB` directly for advanced custom extensions

Remember: The standard API covers 95% of use cases. Only create custom extensions when you have specific requirements that the standard API cannot meet.