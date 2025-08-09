#!/usr/bin/env python3
"""ADVANCED EXAMPLE: High-performance async SimpleBroker implementation using aiosqlite and aiosqlitepool.

NOTE: This is an ADVANCED example showing how to build a custom async extension by accessing
SimpleBroker's internal APIs. Most users should use the standard Queue API from the main package.

For standard usage, see python_api.py or async_wrapper.py which demonstrate the public API.

This example demonstrates how to build a fully async version of SimpleBroker with:
- Connection pooling for high concurrency
- Async-compatible BrokerCore implementation
- Full feature parity with the synchronous version
- Production-ready error handling and resilience
- Direct access to internal SQL and database operations

Requirements:
    pip install aiosqlite aiosqlitepool

Usage:
    See the main() function for examples of how to use the async API.

WARNING: This example uses internal APIs that may change between versions.
Only use this approach if you need custom extensions beyond what the public API provides.
"""

import asyncio
import os
import threading
import time
import warnings
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional, Protocol, Tuple

try:
    import aiosqlite
    from aiosqlitepool import ConnectionPool
except ImportError:
    raise ImportError(
        "This example requires aiosqlite and aiosqlitepool. "
        "Install with: pip install aiosqlite aiosqlitepool"
    ) from None

# ADVANCED: Import SimpleBroker internals for custom extension
# Note: These are internal APIs - most users should use the public API instead:
#   from simplebroker import Queue, QueueWatcher
from simplebroker._exceptions import DataError, IntegrityError, OperationalError
from simplebroker._sql import (
    CHECK_CLAIMED_COLUMN as SQL_PRAGMA_TABLE_INFO_MESSAGES_CLAIMED,
)
from simplebroker._sql import (
    CREATE_MESSAGES_TABLE as SQL_CREATE_TABLE_MESSAGES,
)
from simplebroker._sql import (
    CREATE_META_TABLE as SQL_CREATE_TABLE_META,
)
from simplebroker._sql import (
    CREATE_QUEUE_TS_ID_INDEX as SQL_CREATE_IDX_MESSAGES_QUEUE_TS_ID,
)
from simplebroker._sql import (
    CREATE_TS_UNIQUE_INDEX as SQL_CREATE_IDX_MESSAGES_TS_UNIQUE,
)
from simplebroker._sql import (
    CREATE_UNCLAIMED_INDEX as SQL_CREATE_IDX_MESSAGES_UNCLAIMED,
)
from simplebroker._sql import (
    DELETE_ALL_MESSAGES as SQL_DELETE_ALL_MESSAGES,
)
from simplebroker._sql import (
    DELETE_QUEUE_MESSAGES as SQL_DELETE_MESSAGES_BY_QUEUE,
)
from simplebroker._sql import (
    DROP_OLD_INDEXES,
    build_claim_batch_query,
    build_claim_single_query,
    build_move_by_id_query,
    build_peek_query,
)
from simplebroker._sql import (
    GET_DISTINCT_QUEUES as SQL_SELECT_DISTINCT_QUEUES,
)
from simplebroker._sql import (
    GET_QUEUE_STATS as SQL_SELECT_QUEUES_STATS,
)
from simplebroker._sql import (
    INSERT_MESSAGE as SQL_INSERT_MESSAGE,
)
from simplebroker._sql import (
    LIST_QUEUES_UNCLAIMED as SQL_SELECT_QUEUES_UNCLAIMED,
)

# ADVANCED: Import database constants for low-level operations
# For standard usage, these constants are handled internally by the Queue class
from simplebroker.db import (
    LOGICAL_COUNTER_BITS,
    MAX_LOGICAL_COUNTER,
    MAX_MESSAGE_SIZE,
    SCHEMA_VERSION,
    SIMPLEBROKER_MAGIC,
)


def _validate_queue_name(queue: str) -> Optional[str]:
    """Validate queue name and return error message or None if valid."""
    if not queue:
        return "Queue name cannot be empty"
    if len(queue) > 256:
        return f"Queue name too long ({len(queue)} > 256 characters)"
    if not queue.replace("-", "").replace("_", "").replace(".", "").isalnum():
        return "Queue name can only contain letters, numbers, hyphens, underscores, and dots"
    return None


class AsyncSQLRunner(Protocol):
    """Async version of the SQLRunner protocol."""

    async def run(
        self,
        sql: str,
        params: Tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> List[Tuple[Any, ...]]:
        """Execute SQL and optionally return rows."""
        ...

    async def begin_immediate(self) -> None:
        """Start an immediate transaction."""
        ...

    async def commit(self) -> None:
        """Commit the current transaction."""
        ...

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        ...

    async def close(self) -> None:
        """Close the connection and release resources."""
        ...


class PooledAsyncSQLiteRunner:
    """High-performance async SQLite runner with connection pooling."""

    def __init__(self, db_path: str, pool_size: int = 10, max_connections: int = 20):
        self.db_path = db_path
        self.pool_size = pool_size
        self.max_connections = max_connections
        self._pool: Optional[ConnectionPool] = None
        self._local = threading.local()  # Thread local storage

    async def _ensure_pool(self) -> ConnectionPool:
        """Lazily create the connection pool."""
        if self._pool is None:
            self._pool = ConnectionPool(
                self.db_path,
                minsize=self.pool_size,
                maxsize=self.max_connections,
            )
            # Apply pragmas to the pool's connection factory
            original_factory = self._pool._connection_factory

            async def configured_factory() -> aiosqlite.Connection:
                conn = await original_factory()
                await self._setup_connection(conn)
                return conn

            self._pool._connection_factory = configured_factory
        return self._pool

    async def _setup_connection(self, conn: aiosqlite.Connection) -> None:
        """Apply SimpleBroker's required PRAGMA settings."""
        # Check SQLite version
        cursor = await conn.execute("SELECT sqlite_version()")
        version_row = await cursor.fetchone()
        if version_row:
            version_parts = [int(x) for x in version_row[0].split(".")]
            if version_parts < [3, 35, 0]:
                raise RuntimeError(
                    f"SQLite version {version_row[0]} is too old. "
                    f"SimpleBroker requires SQLite 3.35.0 or later."
                )

        # Apply all pragmas
        busy_timeout = int(os.environ.get("BROKER_BUSY_TIMEOUT", "5000"))
        await conn.execute(f"PRAGMA busy_timeout={busy_timeout}")

        cache_mb = int(os.environ.get("BROKER_CACHE_MB", "10"))
        await conn.execute(f"PRAGMA cache_size=-{cache_mb * 1024}")

        sync_mode = os.environ.get("BROKER_SYNC_MODE", "FULL").upper()
        if sync_mode not in ("OFF", "NORMAL", "FULL", "EXTRA"):
            warnings.warn(
                f"Invalid BROKER_SYNC_MODE '{sync_mode}', defaulting to FULL",
                RuntimeWarning,
                stacklevel=4,
            )
            sync_mode = "FULL"
        await conn.execute(f"PRAGMA synchronous={sync_mode}")

        # Enable WAL mode
        cursor = await conn.execute("PRAGMA journal_mode=WAL")
        result = await cursor.fetchone()
        if result and result[0].lower() != "wal":
            raise RuntimeError(f"Failed to enable WAL mode, got: {result}")

        await conn.execute("PRAGMA wal_autocheckpoint=1000")

    async def run(
        self, sql: str, params: Tuple[Any, ...] = (), *, fetch: bool = False
    ) -> List[Tuple[Any, ...]]:
        """Execute SQL and optionally return rows."""
        pool = await self._ensure_pool()

        # Check if we're in a transaction
        if hasattr(self._local, "conn") and self._local.conn:
            # Use the transaction connection
            try:
                cursor = await self._local.conn.execute(sql, params)
                if fetch:
                    result = await cursor.fetchall()
                    return list(result)
                return []
            except aiosqlite.OperationalError as e:
                raise OperationalError(str(e)) from e
            except aiosqlite.IntegrityError as e:
                raise IntegrityError(str(e)) from e
            except aiosqlite.DatabaseError as e:
                # aiosqlite doesn't expose DataError directly
                if "DATATYPE MISMATCH" in str(e).upper():
                    raise DataError(str(e)) from e
                raise
        else:
            # Get connection from pool
            async with pool.acquire() as conn:
                try:
                    cursor = await conn.execute(sql, params)
                    if fetch:
                        result = await cursor.fetchall()
                        return list(result)
                    return []
                except aiosqlite.OperationalError as e:
                    raise OperationalError(str(e)) from e
                except aiosqlite.IntegrityError as e:
                    raise IntegrityError(str(e)) from e
                except aiosqlite.DatabaseError as e:
                    # aiosqlite doesn't expose DataError directly
                    if "DATATYPE MISMATCH" in str(e).upper():
                        raise DataError(str(e)) from e
                    raise

    async def begin_immediate(self) -> None:
        """Start an immediate transaction."""
        if hasattr(self._local, "conn") and self._local.conn:
            raise OperationalError("Already in transaction")

        pool = await self._ensure_pool()
        # Acquire a connection for this transaction
        self._local.conn = await pool.acquire()
        try:
            await self._local.conn.execute("BEGIN IMMEDIATE")
        except aiosqlite.OperationalError as e:
            # Release connection on error
            await pool.release(self._local.conn)
            self._local.conn = None
            raise OperationalError(str(e)) from e

    async def commit(self) -> None:
        """Commit the current transaction."""
        if not hasattr(self._local, "conn") or not self._local.conn:
            raise OperationalError("Not in transaction")

        pool = await self._ensure_pool()
        try:
            await self._local.conn.commit()
        except aiosqlite.OperationalError as e:
            raise OperationalError(str(e)) from e
        finally:
            # Always release connection back to pool
            await pool.release(self._local.conn)
            self._local.conn = None

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        if not hasattr(self._local, "conn") or not self._local.conn:
            raise OperationalError("Not in transaction")

        pool = await self._ensure_pool()
        try:
            await self._local.conn.rollback()
        except aiosqlite.OperationalError as e:
            raise OperationalError(str(e)) from e
        finally:
            # Always release connection back to pool
            await pool.release(self._local.conn)
            self._local.conn = None

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None


class AsyncTimestampGenerator:
    """Async version of the timestamp generator."""

    def __init__(self, runner: AsyncSQLRunner):
        self._runner = runner
        self._lock = asyncio.Lock()
        self._last_physical_ms = 0
        self._counter = 0

    async def generate(self) -> int:
        """Generate a unique hybrid timestamp."""
        async with self._lock:
            # Generate timestamp atomically with database update
            max_retries = 100

            for attempt in range(max_retries):
                # Current time in milliseconds
                now_ms = int(time.time() * 1000)

                # Check if we're in a new millisecond
                if now_ms > self._last_physical_ms:
                    # New millisecond, reset counter
                    self._last_physical_ms = now_ms
                    self._counter = 0
                else:
                    # Same millisecond, increment counter
                    self._counter += 1
                    if self._counter > MAX_LOGICAL_COUNTER:
                        # Counter overflow, wait for next millisecond
                        while now_ms <= self._last_physical_ms:
                            await asyncio.sleep(0.001)
                            now_ms = int(time.time() * 1000)
                        self._last_physical_ms = now_ms
                        self._counter = 0

                # Build the hybrid timestamp
                timestamp = (now_ms << LOGICAL_COUNTER_BITS) | self._counter

                # Try to update in database
                try:
                    # Get current value
                    rows = await self._runner.run(
                        "SELECT value FROM meta WHERE key = 'last_ts'", fetch=True
                    )
                    current_ts = rows[0][0] if rows else 0

                    # Only update if our timestamp is greater
                    if timestamp > current_ts:
                        await self._runner.run(
                            "UPDATE meta SET value = ? WHERE key = 'last_ts' AND value = ?",
                            (timestamp, current_ts),
                        )
                        # No IntegrityError means success
                        return timestamp
                    else:
                        # Database has a newer timestamp, sync with it
                        db_physical = current_ts >> LOGICAL_COUNTER_BITS
                        db_counter = current_ts & MAX_LOGICAL_COUNTER

                        if db_physical > self._last_physical_ms:
                            self._last_physical_ms = db_physical
                            self._counter = db_counter + 1
                        elif db_physical == self._last_physical_ms:
                            self._counter = max(self._counter, db_counter + 1)

                        # Retry with updated state
                        continue

                except IntegrityError:
                    # Concurrent update, retry
                    if attempt < max_retries - 1:
                        await asyncio.sleep(0.001 * (attempt + 1))
                        continue
                    raise

            raise RuntimeError(
                f"Failed to generate timestamp after {max_retries} attempts"
            )


class AsyncBrokerCore:
    """Async version of BrokerCore with full feature parity."""

    def __init__(self, runner: AsyncSQLRunner):
        self._runner = runner
        self._lock = asyncio.Lock()
        self._timestamp_gen: Optional[AsyncTimestampGenerator] = None
        self._write_count = 0
        self._vacuum_interval = int(
            os.environ.get("BROKER_AUTO_VACUUM_INTERVAL", "100")
        )
        self._initialized = False

    async def _ensure_initialized(self) -> None:
        """Ensure database is initialized."""
        if not self._initialized:
            await self._setup_database()
            await self._verify_database_magic()
            await self._ensure_schema_v2()
            await self._ensure_schema_v3()
            self._timestamp_gen = AsyncTimestampGenerator(self._runner)
            self._initialized = True

    async def _setup_database(self) -> None:
        """Set up database with optimized settings and schema."""
        async with self._lock:
            # Create tables
            await self._execute_with_retry(
                lambda: self._runner.run(SQL_CREATE_TABLE_MESSAGES)
            )

            # Drop old indexes
            for drop_sql in DROP_OLD_INDEXES:
                await self._execute_with_retry(
                    lambda sql=drop_sql: self._runner.run(sql)
                )

            # Create optimized index
            await self._execute_with_retry(
                lambda: self._runner.run(SQL_CREATE_IDX_MESSAGES_QUEUE_TS_ID)
            )

            # Create partial index for unclaimed messages
            rows = await self._execute_with_retry(
                lambda: self._runner.run(
                    SQL_PRAGMA_TABLE_INFO_MESSAGES_CLAIMED, fetch=True
                )
            )
            if rows and rows[0][0] > 0:
                await self._execute_with_retry(
                    lambda: self._runner.run(SQL_CREATE_IDX_MESSAGES_UNCLAIMED)
                )

            # Create meta table
            await self._execute_with_retry(
                lambda: self._runner.run(SQL_CREATE_TABLE_META)
            )
            await self._execute_with_retry(
                lambda: self._runner.run(
                    "INSERT OR IGNORE INTO meta (key, value) VALUES ('last_ts', 0)"
                )
            )

            # Insert magic string and schema version
            await self._execute_with_retry(
                lambda: self._runner.run(
                    "INSERT OR IGNORE INTO meta (key, value) VALUES ('magic', ?)",
                    (SIMPLEBROKER_MAGIC,),
                )
            )
            await self._execute_with_retry(
                lambda: self._runner.run(
                    "INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', ?)",
                    (SCHEMA_VERSION,),
                )
            )

            await self._execute_with_retry(self._runner.commit)

    async def _verify_database_magic(self) -> None:
        """Verify database magic string and schema version."""
        async with self._lock:
            try:
                # Check if meta table exists
                rows = await self._runner.run(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='meta'",
                    fetch=True,
                )
                if not rows or rows[0][0] == 0:
                    return

                # Check magic string
                rows = await self._runner.run(
                    "SELECT value FROM meta WHERE key = 'magic'", fetch=True
                )
                if rows and rows[0][0] != SIMPLEBROKER_MAGIC:
                    raise RuntimeError(
                        f"Database magic string mismatch. Expected '{SIMPLEBROKER_MAGIC}', "
                        f"found '{rows[0][0]}'"
                    )

                # Check schema version
                rows = await self._runner.run(
                    "SELECT value FROM meta WHERE key = 'schema_version'", fetch=True
                )
                if rows and rows[0][0] > SCHEMA_VERSION:
                    raise RuntimeError(
                        f"Database schema version {rows[0][0]} is newer than supported version "
                        f"{SCHEMA_VERSION}"
                    )
            except OperationalError:
                pass

    async def _ensure_schema_v2(self) -> None:
        """Migrate to schema with claimed column."""
        async with self._lock:
            rows = await self._runner.run(
                SQL_PRAGMA_TABLE_INFO_MESSAGES_CLAIMED, fetch=True
            )
            if rows and rows[0][0] > 0:
                return

            try:
                await self._runner.begin_immediate()
                await self._runner.run(
                    "ALTER TABLE messages ADD COLUMN claimed INTEGER DEFAULT 0"
                )
                await self._runner.run(SQL_CREATE_IDX_MESSAGES_UNCLAIMED)
                await self._runner.commit()
            except Exception as e:
                await self._runner.rollback()
                if "duplicate column name" not in str(e):
                    raise

    async def _ensure_schema_v3(self) -> None:
        """Add unique constraint to timestamp column."""
        async with self._lock:
            rows = await self._runner.run(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_messages_ts_unique'",
                fetch=True,
            )
            if rows and rows[0][0] > 0:
                return

            try:
                await self._runner.begin_immediate()
                await self._runner.run(SQL_CREATE_IDX_MESSAGES_TS_UNIQUE)
                await self._runner.commit()
            except IntegrityError as e:
                await self._runner.rollback()
                if "UNIQUE constraint failed" in str(e):
                    raise RuntimeError(
                        "Cannot add unique constraint on timestamp column: "
                        "duplicate timestamps exist"
                    ) from e
                raise
            except Exception as e:
                await self._runner.rollback()
                if "already exists" not in str(e):
                    raise

    async def _execute_with_retry(
        self,
        operation: Any,
        *,
        max_retries: int = 10,
        retry_delay: float = 0.05,
    ) -> Any:
        """Execute an async operation with retry logic."""
        for attempt in range(max_retries):
            try:
                return await operation()
            except OperationalError as e:
                msg = str(e).lower()
                if "locked" in msg or "busy" in msg:
                    if attempt < max_retries - 1:
                        jitter = (time.time() * 1000) % 25 / 1000
                        wait = retry_delay * (2**attempt) + jitter
                        await asyncio.sleep(wait)
                        continue
                raise

    def _validate_queue_name(self, queue: str) -> None:
        """Validate queue name."""
        error = _validate_queue_name(queue)
        if error:
            raise ValueError(error)

    async def write(self, queue: str, message: str) -> None:
        """Write a message to a queue."""
        await self._ensure_initialized()
        self._validate_queue_name(queue)

        # Check message size
        message_size = len(message.encode("utf-8"))
        if message_size > MAX_MESSAGE_SIZE:
            raise ValueError(
                f"Message size ({message_size} bytes) exceeds maximum allowed size "
                f"({MAX_MESSAGE_SIZE} bytes)"
            )

        # Generate timestamp outside transaction
        if self._timestamp_gen is None:
            raise RuntimeError("Timestamp generator not initialized")
        timestamp = await self._timestamp_gen.generate()

        # Write with retry
        await self._execute_with_retry(
            lambda: self._do_write_transaction(queue, message, timestamp)
        )

        # Check if vacuum needed
        if int(os.environ.get("BROKER_AUTO_VACUUM", "1")) == 1:
            self._write_count += 1
            if self._write_count >= self._vacuum_interval:
                self._write_count = 0
                if await self._should_vacuum():
                    await self._vacuum_claimed_messages()

    async def _do_write_transaction(
        self, queue: str, message: str, timestamp: int
    ) -> None:
        """Core write transaction logic."""
        async with self._lock:
            await self._runner.begin_immediate()
            try:
                await self._runner.run(
                    SQL_INSERT_MESSAGE,
                    (queue, message, timestamp),
                )
                await self._runner.commit()
            except Exception:
                await self._runner.rollback()
                raise

    async def read(
        self,
        queue: str,
        peek: bool = False,
        all_messages: bool = False,
        exact_timestamp: Optional[int] = None,
    ) -> List[str]:
        """Read message(s) from a queue."""
        messages = []
        async for message in self.stream_read(
            queue, peek=peek, all_messages=all_messages, exact_timestamp=exact_timestamp
        ):
            messages.append(message)
        return messages

    async def stream_read(
        self,
        queue: str,
        peek: bool = False,
        all_messages: bool = False,
        exact_timestamp: Optional[int] = None,
        commit_interval: int = 1,
        since_timestamp: Optional[int] = None,
    ) -> AsyncIterator[str]:
        """Stream messages from a queue."""
        await self._ensure_initialized()
        self._validate_queue_name(queue)

        # Build WHERE clause
        params: List[Any]
        if exact_timestamp is not None:
            where_conditions = ["ts = ?", "queue = ?", "claimed = 0"]
            params = [exact_timestamp, queue]
        else:
            where_conditions = ["queue = ?", "claimed = 0"]
            params = [queue]
            if since_timestamp is not None:
                where_conditions.append("ts > ?")
                params.append(since_timestamp)

        if peek:
            # Peek mode - no locking needed
            offset = 0
            batch_size = 100 if all_messages else 1

            while True:
                query = build_peek_query(where_conditions)
                rows = await self._runner.run(
                    query, tuple(params + [batch_size, offset]), fetch=True
                )

                if not rows:
                    break

                for row in rows:
                    yield row[0]  # body

                if not all_messages:
                    break

                offset += batch_size
        else:
            # Delete mode
            if all_messages:
                # Process in batches
                while True:
                    if commit_interval == 1:
                        # Exactly-once delivery
                        async with self._lock:
                            await self._execute_with_retry(self._runner.begin_immediate)

                            try:
                                query = build_claim_single_query(where_conditions)
                                rows = await self._runner.run(
                                    query, tuple(params), fetch=True
                                )

                                if not rows:
                                    await self._runner.rollback()
                                    break

                                await self._runner.commit()
                                message = rows[0]
                            except Exception:
                                await self._runner.rollback()
                                raise

                        yield message[0]  # body
                    else:
                        # At-least-once delivery
                        batch_messages = []

                        async with self._lock:
                            await self._execute_with_retry(self._runner.begin_immediate)

                            try:
                                query = build_claim_batch_query(where_conditions)
                                batch_messages = await self._runner.run(
                                    query, tuple(params + [commit_interval]), fetch=True
                                )

                                if not batch_messages:
                                    await self._runner.rollback()
                                    break
                            except Exception:
                                await self._runner.rollback()
                                raise

                        # Yield messages
                        for row in batch_messages:
                            yield row[0]  # body

                        # Commit after yielding
                        async with self._lock:
                            try:
                                await self._runner.commit()
                            except Exception:
                                await self._runner.rollback()
                                raise
            else:
                # Single message
                async with self._lock:
                    await self._execute_with_retry(self._runner.begin_immediate)

                    try:
                        query = build_claim_single_query(where_conditions)
                        rows = await self._runner.run(query, tuple(params), fetch=True)

                        if rows:
                            await self._runner.commit()
                            yield rows[0][0]  # body
                        else:
                            await self._runner.rollback()
                    except Exception:
                        await self._runner.rollback()
                        raise

    async def list_queues(self) -> List[Tuple[str, int]]:
        """List all queues with their unclaimed message counts."""
        await self._ensure_initialized()

        async def _do_list() -> List[Tuple[Any, ...]]:
            async with self._lock:
                result = await self._runner.run(SQL_SELECT_QUEUES_UNCLAIMED, fetch=True)
                return list(result)

        rows = await self._execute_with_retry(_do_list)
        return [(str(row[0]), int(row[1])) for row in rows]

    async def get_queue_stats(self) -> List[Tuple[str, int, int]]:
        """Get all queues with both unclaimed and total message counts."""
        await self._ensure_initialized()

        async def _do_stats() -> List[Tuple[Any, ...]]:
            async with self._lock:
                result = await self._runner.run(SQL_SELECT_QUEUES_STATS, fetch=True)
                return list(result)

        rows = await self._execute_with_retry(_do_stats)
        return [(str(row[0]), int(row[1]), int(row[2])) for row in rows]

    async def delete(self, queue: Optional[str] = None) -> None:
        """Delete messages from queue(s)."""
        await self._ensure_initialized()
        if queue is not None:
            self._validate_queue_name(queue)

        async def _do_delete() -> None:
            async with self._lock:
                if queue is None:
                    await self._runner.run(SQL_DELETE_ALL_MESSAGES)
                else:
                    await self._runner.run(SQL_DELETE_MESSAGES_BY_QUEUE, (queue,))
                await self._runner.commit()

        await self._execute_with_retry(_do_delete)

    async def broadcast(self, message: str) -> None:
        """Broadcast a message to all existing queues."""
        await self._ensure_initialized()

        async def _do_broadcast() -> None:
            async with self._lock:
                await self._runner.begin_immediate()
                try:
                    # Get all queues
                    rows = await self._runner.run(
                        SQL_SELECT_DISTINCT_QUEUES, fetch=True
                    )
                    queues = [row[0] for row in rows]

                    # Generate timestamps for all
                    for queue in queues:
                        if self._timestamp_gen is None:
                            raise RuntimeError("Timestamp generator not initialized")
                        timestamp = await self._timestamp_gen.generate()
                        await self._runner.run(
                            SQL_INSERT_MESSAGE,
                            (queue, message, timestamp),
                        )

                    await self._runner.commit()
                except Exception:
                    await self._runner.rollback()
                    raise

        await self._execute_with_retry(_do_broadcast)

    async def move(
        self,
        source_queue: str,
        dest_queue: str,
        *,
        message_id: Optional[int] = None,
        require_unclaimed: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Move message(s) from one queue to another."""
        await self._ensure_initialized()
        self._validate_queue_name(source_queue)
        self._validate_queue_name(dest_queue)

        async def _do_move() -> Optional[Dict[str, Any]]:
            async with self._lock:
                await self._execute_with_retry(self._runner.begin_immediate)

                try:
                    if message_id is not None:
                        # Move by ID
                        where_conditions = ["id = ?", "queue = ?"]
                        params = [message_id, source_queue]

                        if require_unclaimed:
                            where_conditions.append("claimed = 0")

                        rows = await self._runner.run(
                            build_move_by_id_query(where_conditions),
                            (dest_queue, *params),
                            fetch=True,
                        )
                    else:
                        # Move oldest
                        rows = await self._runner.run(
                            """
                            UPDATE messages
                            SET queue = ?, claimed = 0
                            WHERE id IN (
                                SELECT id FROM messages
                                WHERE queue = ? AND claimed = 0
                                ORDER BY id
                                LIMIT 1
                            )
                            RETURNING id, body, ts
                            """,
                            (dest_queue, source_queue),
                            fetch=True,
                        )

                    if rows:
                        await self._runner.commit()
                        message = rows[0]
                        return {
                            "id": int(message[0]),
                            "body": str(message[1]),
                            "ts": int(message[2]),
                        }
                    else:
                        await self._runner.rollback()
                        return None
                except Exception:
                    await self._runner.rollback()
                    raise

        result = await self._execute_with_retry(_do_move)
        return result  # type: ignore[no-any-return]

    async def _should_vacuum(self) -> bool:
        """Check if vacuum is needed."""
        async with self._lock:
            rows = await self._runner.run(
                """SELECT
                    SUM(CASE WHEN claimed = 1 THEN 1 ELSE 0 END) as claimed,
                    COUNT(*) as total
                   FROM messages""",
                fetch=True,
            )
            if not rows:
                return False

            claimed_count = rows[0][0] or 0
            total_count = rows[0][1] or 0

            if total_count == 0:
                return False

            threshold_pct = float(os.environ.get("BROKER_VACUUM_THRESHOLD", "10")) / 100
            return bool(
                (claimed_count >= total_count * threshold_pct)
                or (claimed_count > 10000)
            )

    async def _vacuum_claimed_messages(self) -> None:
        """Delete claimed messages in batches."""
        batch_size = int(os.environ.get("BROKER_VACUUM_BATCH_SIZE", "1000"))

        while True:
            async with self._lock:
                await self._runner.begin_immediate()
                try:
                    # Check if any claimed messages exist
                    check_result = await self._runner.run(
                        "SELECT EXISTS(SELECT 1 FROM messages WHERE claimed = 1 LIMIT 1)",
                        fetch=True,
                    )
                    if not check_result or not check_result[0][0]:
                        await self._runner.rollback()
                        break

                    # Delete batch
                    await self._runner.run(
                        """DELETE FROM messages
                           WHERE id IN (
                               SELECT id FROM messages
                               WHERE claimed = 1
                               LIMIT ?
                           )""",
                        (batch_size,),
                    )
                    await self._runner.commit()
                except Exception:
                    await self._runner.rollback()
                    raise

            # Brief pause between batches
            await asyncio.sleep(0.001)

    async def vacuum(self) -> None:
        """Manually trigger vacuum of claimed messages."""
        await self._ensure_initialized()
        await self._vacuum_claimed_messages()

    async def close(self) -> None:
        """Close the runner."""
        await self._runner.close()


class AsyncQueue:
    """High-level async queue interface."""

    def __init__(self, name: str, broker: AsyncBrokerCore):
        self.name = name
        self._broker = broker

    async def write(self, message: str) -> None:
        """Write a message to this queue."""
        await self._broker.write(self.name, message)

    async def read(
        self,
        *,
        peek: bool = False,
        all_messages: bool = False,
    ) -> Optional[str]:
        """Read a single message from this queue."""
        messages = await self._broker.read(
            self.name, peek=peek, all_messages=all_messages
        )
        return messages[0] if messages else None

    async def read_all(self, *, peek: bool = False) -> List[str]:
        """Read all messages from this queue."""
        return await self._broker.read(self.name, peek=peek, all_messages=True)

    async def stream(
        self,
        *,
        peek: bool = False,
        all_messages: bool = True,
        commit_interval: int = 1,
    ) -> AsyncIterator[str]:
        """Stream messages from this queue."""
        async for message in self._broker.stream_read(
            self.name,
            peek=peek,
            all_messages=all_messages,
            commit_interval=commit_interval,
        ):
            yield message

    async def size(self) -> int:
        """Get the number of unclaimed messages in this queue."""
        queues = await self._broker.list_queues()
        for queue_name, count in queues:
            if queue_name == self.name:
                return count
        return 0

    async def move_to(
        self,
        dest_queue: str,
        *,
        message_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Move a message to another queue."""
        return await self._broker.move(
            self.name,
            dest_queue,
            message_id=message_id,
        )


@asynccontextmanager
async def async_broker(
    db_path: str,
    *,
    pool_size: int = 10,
    max_connections: int = 20,
) -> AsyncIterator[AsyncBrokerCore]:
    """Context manager for async broker with connection pooling."""
    runner = PooledAsyncSQLiteRunner(db_path, pool_size, max_connections)
    broker = AsyncBrokerCore(runner)
    try:
        yield broker
    finally:
        await broker.close()


async def example_basic() -> None:
    """Basic usage example."""
    print("=== Basic Async Queue Example ===")

    async with async_broker("async_example.db") as broker:
        queue = AsyncQueue("tasks", broker)

        # Write some messages
        print("Writing messages...")
        await queue.write("Task 1: Process order")
        await queue.write("Task 2: Send email")
        await queue.write("Task 3: Update inventory")

        # Read them back
        print("\nReading messages:")
        while True:
            msg = await queue.read()
            if msg is None:
                break
            print(f"  - {msg}")

        print(f"\nQueue size: {await queue.size()}")


async def example_concurrent() -> None:
    """High-concurrency example with multiple producers and consumers."""
    print("\n=== Concurrent Producers/Consumers Example ===")

    async with async_broker("async_example.db", pool_size=20) as broker:
        queue = AsyncQueue("concurrent", broker)

        # Producer coroutine
        async def producer(producer_id: int, count: int) -> None:
            for i in range(count):
                await queue.write(f"Producer {producer_id} - Message {i}")
                if i % 10 == 0:
                    await asyncio.sleep(0.001)  # Simulate varying production rates
            print(f"Producer {producer_id} finished")

        # Consumer coroutine
        async def consumer(consumer_id: int, target_count: int) -> None:
            consumed = 0
            while consumed < target_count:
                msg = await queue.read()
                if msg:
                    consumed += 1
                    if consumed % 50 == 0:
                        print(f"Consumer {consumer_id} processed {consumed} messages")
                else:
                    await asyncio.sleep(0.01)
            print(f"Consumer {consumer_id} finished ({consumed} messages)")

        # Start multiple producers and consumers
        start_time = time.time()

        # 5 producers, 100 messages each = 500 total messages
        producers = [producer(i, 100) for i in range(5)]
        # 3 consumers, targeting different amounts
        consumers = [
            consumer(1, 200),
            consumer(2, 150),
            consumer(3, 150),
        ]

        await asyncio.gather(*producers, *consumers)

        elapsed = time.time() - start_time
        print(f"\nProcessed 500 messages in {elapsed:.2f} seconds")
        print(f"Throughput: {500 / elapsed:.0f} messages/second")


async def example_streaming() -> None:
    """Streaming example with batch processing."""
    print("\n=== Streaming Example ===")

    async with async_broker("async_example.db") as broker:
        queue = AsyncQueue("stream", broker)

        # Write many messages
        print("Writing 1000 messages...")
        for i in range(1000):
            await queue.write(f"Stream item {i:04d}")

        # Process in batches using streaming
        print("\nStreaming with batch commit (commit_interval=50):")
        count = 0
        start_time = time.time()

        async for _message in queue.stream(commit_interval=50):
            count += 1
            if count % 100 == 0:
                print(f"  Processed {count} messages...")

        elapsed = time.time() - start_time
        print(f"\nStreamed {count} messages in {elapsed:.2f} seconds")
        print(f"Throughput: {count / elapsed:.0f} messages/second")


async def example_multiple_queues() -> None:
    """Example with multiple queues and priorities."""
    print("\n=== Multiple Queues Example ===")

    async with async_broker("async_example.db") as broker:
        # Create queues with different priorities
        high = AsyncQueue("high_priority", broker)
        medium = AsyncQueue("medium_priority", broker)
        low = AsyncQueue("low_priority", broker)

        # Write messages to different queues
        print("Writing messages to priority queues...")
        for i in range(10):
            await high.write(f"HIGH: Critical task {i}")
        for i in range(20):
            await medium.write(f"MEDIUM: Normal task {i}")
        for i in range(30):
            await low.write(f"LOW: Background task {i}")

        # Process by priority
        print("\nProcessing by priority:")
        total_processed = 0

        while total_processed < 60:
            # Try high priority first
            msg = await high.read()
            if msg:
                print(f"  {msg}")
                total_processed += 1
                continue

            # Then medium
            msg = await medium.read()
            if msg:
                print(f"  {msg}")
                total_processed += 1
                continue

            # Finally low
            msg = await low.read()
            if msg:
                print(f"  {msg}")
                total_processed += 1
                continue

            # No messages in any queue
            break

        print(f"\nTotal processed: {total_processed}")


async def example_resilience() -> None:
    """Example showing error handling and resilience."""
    print("\n=== Resilience Example ===")

    async with async_broker("async_example.db") as broker:
        queue = AsyncQueue("resilient", broker)

        # Simulate message processing with potential failures
        async def process_with_retry(message: str, max_retries: int = 3) -> bool:
            """Process a message with retry logic."""
            for attempt in range(max_retries):
                try:
                    # Simulate processing that might fail
                    if "fail" in message and attempt < 2:
                        raise Exception("Simulated processing error")

                    print(f"  Successfully processed: {message}")
                    return True
                except Exception as e:
                    print(f"  Attempt {attempt + 1} failed for: {message} ({e})")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(0.1 * (attempt + 1))
                    else:
                        # Move to dead letter queue
                        await queue._broker.write("dead_letter", f"FAILED: {message}")
                        print(f"  Moved to dead letter queue: {message}")
                        return False
            return False  # Should never reach here

        # Write test messages
        print("Writing test messages...")
        await queue.write("Normal message 1")
        await queue.write("This will fail twice")
        await queue.write("Normal message 2")
        await queue.write("This will fail permanently")
        await queue.write("Normal message 3")

        # Process with resilience
        print("\nProcessing with retry logic:")
        while True:
            msg = await queue.read()
            if msg is None:
                break
            await process_with_retry(msg)

        # Check dead letter queue
        dlq = AsyncQueue("dead_letter", broker)
        print(f"\nDead letter queue size: {await dlq.size()}")
        print("Dead letter messages:")
        async for msg in dlq.stream():
            print(f"  - {msg}")


async def benchmark() -> None:
    """Performance benchmark."""
    print("\n=== Performance Benchmark ===")

    # Test different pool sizes
    for pool_size in [1, 5, 10, 20]:
        print(f"\nTesting with pool_size={pool_size}")

        async with async_broker(
            "benchmark.db",
            pool_size=pool_size,
            max_connections=pool_size * 2,
        ) as broker:
            queue = AsyncQueue("benchmark", broker)

            # Clear queue
            await broker.delete("benchmark")

            # Write benchmark
            message_count = 1000
            start_time = time.time()

            # Concurrent writes
            async def write_batch(
                batch_id: int, count: int, q: AsyncQueue = queue
            ) -> None:
                for i in range(count):
                    await q.write(f"Batch {batch_id} - Message {i}")

            # Split into batches for concurrent writing
            batch_size = message_count // pool_size
            write_tasks = [write_batch(i, batch_size) for i in range(pool_size)]

            await asyncio.gather(*write_tasks)
            write_time = time.time() - start_time
            write_throughput = message_count / write_time

            print(f"  Write: {write_throughput:.0f} msgs/sec")

            # Read benchmark
            start_time = time.time()
            read_count = 0

            # Concurrent reads
            async def read_batch(reader_id: int, q: AsyncQueue = queue) -> int:
                count = 0
                while True:
                    msg = await q.read()
                    if msg is None:
                        break
                    count += 1
                return count

            # Multiple concurrent readers
            read_tasks = [read_batch(i) for i in range(min(pool_size, 4))]
            counts = await asyncio.gather(*read_tasks)
            read_count = sum(counts)

            read_time = time.time() - start_time
            read_throughput = read_count / read_time

            print(f"  Read: {read_throughput:.0f} msgs/sec")
            print(
                f"  Total: {(write_throughput + read_throughput) / 2:.0f} msgs/sec avg"
            )


async def main() -> None:
    """Run all examples."""
    examples = [
        example_basic,
        example_concurrent,
        example_streaming,
        example_multiple_queues,
        example_resilience,
        benchmark,
    ]

    for example in examples:
        await example()
        print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    print("SimpleBroker Async Pooled Implementation")
    print("========================================")
    print("This example requires: pip install aiosqlite aiosqlitepool")
    print()

    # Run all examples
    asyncio.run(main())
