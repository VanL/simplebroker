"""ADVANCED EXAMPLE: Async SQLite queue implementation using aiosqlitepool.

NOTE: This is an ADVANCED SQLite-specific example. It shows how to build an
async extension that shares SimpleBroker's SQLite schema and core semantics, but
it is not a backend plugin and it does not implement the synchronous SQLRunner
extension protocol.

For standard usage, see python_api.py or async_wrapper.py which demonstrate the public API.

This example demonstrates:
- Connection pooling for high concurrency
- An async-compatible queue core
- Schema compatibility with the built-in SQLite backend
- Direct access to internal SQLite SQL and database operations

Requirements:
    pip install aiosqlite aiosqlitepool

Usage:
    See the main() function for examples of how to use the async API.

WARNING: This example uses internal SQLite APIs that may change between versions.
Use async_wrapper.py when you need the supported multi-backend async pattern.
"""

import asyncio
import contextvars
import re
import time
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, Protocol, cast

try:
    import aiosqlite
    from aiosqlitepool import SQLiteConnectionPool  # type: ignore[import-untyped]
except ImportError:
    raise ImportError(
        "This example requires aiosqlite and aiosqlitepool. "
        "Install with: pip install aiosqlite aiosqlitepool"
    ) from None

from simplebroker import resolve_config

# ADVANCED: Import SimpleBroker internals for low-level SQLite compatibility.
# Standard applications should use Queue, QueueWatcher, or async_wrapper.py.
from simplebroker._constants import (
    LOGICAL_COUNTER_MASK,
    MAX_LOGICAL_COUNTER,
    MAX_QUEUE_NAME_LENGTH,
    SCHEMA_VERSION,
    SIMPLEBROKER_MAGIC,
)
from simplebroker._sql import (
    ALTER_MESSAGES_ADD_CLAIMED as SQL_ALTER_MESSAGES_ADD_CLAIMED,
)
from simplebroker._sql import (
    ALTER_MESSAGES_RENAME_V5 as SQL_ALTER_MESSAGES_RENAME_V5,
)
from simplebroker._sql import (
    ALTER_MESSAGES_V6_RENAME_CURRENT as SQL_ALTER_MESSAGES_V6_RENAME_CURRENT,
)
from simplebroker._sql import (
    CHECK_CLAIMED_COLUMN as SQL_PRAGMA_TABLE_INFO_MESSAGES_CLAIMED,
)
from simplebroker._sql import (
    CREATE_ALIAS_TARGET_INDEX as SQL_CREATE_IDX_QUEUE_ALIASES_TARGET,
)
from simplebroker._sql import (
    CREATE_ALIASES_TABLE as SQL_CREATE_TABLE_QUEUE_ALIASES,
)
from simplebroker._sql import (
    CREATE_MESSAGES_TABLE as SQL_CREATE_TABLE_MESSAGES,
)
from simplebroker._sql import (
    CREATE_META_TABLE as SQL_CREATE_TABLE_META,
)
from simplebroker._sql import (
    CREATE_PENDING_QUEUE_TS_INDEX as SQL_CREATE_IDX_MESSAGES_PENDING_QUEUE_TS,
)
from simplebroker._sql import (
    CREATE_QUEUE_TS_INDEX as SQL_CREATE_IDX_MESSAGES_QUEUE_TS,
)
from simplebroker._sql import (
    CREATE_TS_UNIQUE_INDEX as SQL_CREATE_IDX_MESSAGES_TS_UNIQUE,
)
from simplebroker._sql import (
    DELETE_ALL_MESSAGES as SQL_DELETE_ALL_MESSAGES,
)
from simplebroker._sql import (
    DELETE_QUEUE_MESSAGES as SQL_DELETE_MESSAGES_BY_QUEUE,
)
from simplebroker._sql import DROP_MESSAGES_V5 as SQL_DROP_MESSAGES_V5
from simplebroker._sql import (
    GET_DISTINCT_QUEUES as SQL_SELECT_DISTINCT_QUEUES,
)
from simplebroker._sql import (
    GET_QUEUE_STATS as SQL_SELECT_QUEUES_STATS,
)
from simplebroker._sql import (
    INSERT_ALIAS_VERSION_META as SQL_INSERT_ALIAS_VERSION_META,
)
from simplebroker._sql import (
    INSERT_MESSAGE as SQL_INSERT_MESSAGE,
)
from simplebroker._sql import (
    build_claim_batch_query,
    build_claim_single_query,
    build_move_by_timestamp_query,
    build_peek_query,
    create_messages_table_sql,
)
from simplebroker.ext import (
    DataError,
    IntegrityError,
    OperationalError,
    vacuum_is_eligible,
)

QUEUE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$")


def _validate_queue_name(queue: str) -> str | None:
    """Validate queue name and return error message or None if valid."""
    if not queue:
        return "Invalid queue name: cannot be empty"
    if len(queue) > MAX_QUEUE_NAME_LENGTH:
        return f"Invalid queue name: exceeds {MAX_QUEUE_NAME_LENGTH} characters"
    if not QUEUE_NAME_PATTERN.match(queue):
        return (
            "Invalid queue name: must contain only letters, numbers, periods, "
            "underscores, and hyphens. Cannot begin with a hyphen or a period"
        )
    return None


class AsyncSQLRunner(Protocol):
    """Async version of the SQLRunner protocol."""

    async def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> list[tuple[Any, ...]]:
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

    def __init__(
        self,
        db_path: str,
        pool_size: int = 10,
        max_connections: int = 20,
        *,
        config: Mapping[str, Any] | None = None,
    ):
        self.db_path = db_path
        self.pool_size = pool_size
        self.max_connections = max_connections
        self._config = resolve_config(config)
        self._pool: SQLiteConnectionPool | None = None
        self._transaction_conn: contextvars.ContextVar[aiosqlite.Connection | None] = (
            contextvars.ContextVar("transaction_conn", default=None)
        )
        self._transaction_context: contextvars.ContextVar[
            AbstractAsyncContextManager[aiosqlite.Connection] | None
        ] = contextvars.ContextVar("transaction_context", default=None)

    async def _ensure_pool(self) -> SQLiteConnectionPool:
        """Lazily create the connection pool."""
        if self._pool is None:

            async def configured_factory() -> aiosqlite.Connection:
                conn = await aiosqlite.connect(self.db_path)
                await self._setup_connection(conn)
                return conn

            # aiosqlitepool exposes a fixed-size pool. Use max_connections as
            # the pool cap to preserve this example's public constructor shape.
            self._pool = SQLiteConnectionPool(
                configured_factory,
                pool_size=self.max_connections,
            )
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
        busy_timeout = int(self._config["BROKER_BUSY_TIMEOUT"])
        await conn.execute(f"PRAGMA busy_timeout={busy_timeout}")

        cache_mb = int(self._config["BROKER_CACHE_MB"])
        await conn.execute(f"PRAGMA cache_size=-{cache_mb * 1024}")

        sync_mode = str(self._config["BROKER_SYNC_MODE"])
        await conn.execute(f"PRAGMA synchronous={sync_mode}")

        # Enable WAL mode
        cursor = await conn.execute("PRAGMA journal_mode=WAL")
        result = await cursor.fetchone()
        if result and result[0].lower() != "wal":
            raise RuntimeError(f"Failed to enable WAL mode, got: {result}")

        wal_autocheckpoint = int(self._config["BROKER_WAL_AUTOCHECKPOINT"])
        await conn.execute(f"PRAGMA wal_autocheckpoint={wal_autocheckpoint}")

    async def _run_on_connection(
        self,
        conn: aiosqlite.Connection,
        sql: str,
        params: tuple[Any, ...],
        *,
        fetch: bool,
        commit: bool,
    ) -> list[tuple[Any, ...]]:
        try:
            cursor = await conn.execute(sql, params)
            if fetch:
                result = await cursor.fetchall()
                return cast(list[tuple[Any, ...]], list(result))
            if commit:
                await conn.commit()
            return []
        except aiosqlite.OperationalError as exc:
            raise OperationalError(str(exc)) from exc
        except aiosqlite.IntegrityError as exc:
            raise IntegrityError(str(exc)) from exc
        except aiosqlite.DatabaseError as exc:
            if "DATATYPE MISMATCH" in str(exc).upper():
                raise DataError(str(exc)) from exc
            raise

    async def run(
        self, sql: str, params: tuple[Any, ...] = (), *, fetch: bool = False
    ) -> list[tuple[Any, ...]]:
        """Execute SQL and optionally return rows."""
        pool = await self._ensure_pool()

        # Check if we're in a transaction
        transaction_conn = self._transaction_conn.get()
        if transaction_conn is not None:
            return await self._run_on_connection(
                transaction_conn,
                sql,
                params,
                fetch=fetch,
                commit=False,
            )
        async with pool.connection() as conn:
            return await self._run_on_connection(
                conn,
                sql,
                params,
                fetch=fetch,
                commit=True,
            )

    async def begin_immediate(self) -> None:
        """Start an immediate transaction."""
        if self._transaction_conn.get() is not None:
            raise OperationalError("Already in transaction")

        pool = await self._ensure_pool()
        # Keep one pooled connection checked out for the transaction lifetime.
        conn_context = pool.connection()
        conn = await conn_context.__aenter__()
        self._transaction_context.set(conn_context)
        self._transaction_conn.set(conn)
        try:
            await conn.execute("BEGIN IMMEDIATE")
        except aiosqlite.OperationalError as e:
            # Release connection on error
            await conn_context.__aexit__(None, None, None)
            self._transaction_conn.set(None)
            self._transaction_context.set(None)
            raise OperationalError(str(e)) from e

    async def commit(self) -> None:
        """Commit the current transaction."""
        conn = self._transaction_conn.get()
        conn_context = self._transaction_context.get()
        if conn is None or conn_context is None:
            return

        try:
            await conn.commit()
        except aiosqlite.OperationalError as e:
            raise OperationalError(str(e)) from e
        finally:
            # Always release connection back to pool
            await conn_context.__aexit__(None, None, None)
            self._transaction_conn.set(None)
            self._transaction_context.set(None)

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        conn = self._transaction_conn.get()
        conn_context = self._transaction_context.get()
        if conn is None or conn_context is None:
            return

        try:
            await conn.rollback()
        except aiosqlite.OperationalError as e:
            raise OperationalError(str(e)) from e
        finally:
            # Always release connection back to pool
            await conn_context.__aexit__(None, None, None)
            self._transaction_conn.set(None)
            self._transaction_context.set(None)

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
        self._last_ts = 0

    async def generate(self) -> int:
        """Generate a unique hybrid timestamp."""
        async with self._lock:
            max_retries = 100

            for attempt in range(max_retries):
                rows = await self._runner.run(
                    "SELECT value FROM meta WHERE key = 'last_ts'", fetch=True
                )
                if not rows:
                    raise RuntimeError("meta.last_ts missing")

                current_ts = int(rows[0][0])
                latest_ts = max(self._last_ts, current_ts)
                time_mask = ~LOGICAL_COUNTER_MASK
                now_base = time.time_ns() & time_mask
                last_base = latest_ts & time_mask
                last_counter = latest_ts & LOGICAL_COUNTER_MASK

                if now_base > last_base:
                    candidate = now_base
                else:
                    counter = last_counter + 1
                    if counter >= MAX_LOGICAL_COUNTER:
                        while now_base <= last_base:
                            await asyncio.sleep(0.001)
                            now_base = time.time_ns() & time_mask
                        counter = 0
                    candidate = now_base | counter

                try:
                    await self._runner.run(
                        "UPDATE meta SET value = ? WHERE key = 'last_ts' AND value < ?",
                        (candidate, candidate),
                    )
                    rows = await self._runner.run(
                        "SELECT value FROM meta WHERE key = 'last_ts'", fetch=True
                    )
                    stored_ts = int(rows[0][0]) if rows else 0
                    self._last_ts = stored_ts
                    if stored_ts == candidate:
                        return candidate

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
    """Async SQLite queue core for this extension example."""

    def __init__(
        self,
        runner: AsyncSQLRunner,
        *,
        config: Mapping[str, Any] | None = None,
    ):
        self._runner = runner
        self._config = resolve_config(config)
        self._lock = asyncio.Lock()
        self._timestamp_gen: AsyncTimestampGenerator | None = None
        self._write_count = 0
        self._vacuum_interval = int(self._config["BROKER_AUTO_VACUUM_INTERVAL"])
        self._max_message_size = int(self._config["BROKER_MAX_MESSAGE_SIZE"])
        self._initialized = False

    async def _ensure_initialized(self) -> None:
        """Ensure database is initialized."""
        if not self._initialized:
            await self._setup_database()
            await self._verify_database_magic()
            await self._ensure_schema_v2()
            await self._ensure_schema_v3()
            await self._ensure_schema_v4()
            await self._ensure_schema_v5()
            await self._ensure_schema_v6()
            self._timestamp_gen = AsyncTimestampGenerator(self._runner)
            self._initialized = True

    async def _setup_database(self) -> None:
        """Set up database with optimized settings and schema."""
        async with self._lock:
            # Create tables
            await self._execute_with_retry(
                lambda: self._runner.run(SQL_CREATE_TABLE_MESSAGES)
            )

            # Create optimized index
            await self._execute_with_retry(
                lambda: self._runner.run(SQL_CREATE_IDX_MESSAGES_QUEUE_TS)
            )

            # Create partial index for unclaimed messages
            rows = await self._execute_with_retry(
                lambda: self._runner.run(
                    SQL_PRAGMA_TABLE_INFO_MESSAGES_CLAIMED, fetch=True
                )
            )
            if rows and rows[0][0] > 0:
                await self._execute_with_retry(
                    lambda: self._runner.run(SQL_CREATE_IDX_MESSAGES_PENDING_QUEUE_TS)
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
            await self._execute_with_retry(
                lambda: self._runner.run(SQL_CREATE_TABLE_QUEUE_ALIASES)
            )
            await self._execute_with_retry(
                lambda: self._runner.run(SQL_CREATE_IDX_QUEUE_ALIASES_TARGET)
            )
            await self._execute_with_retry(
                lambda: self._runner.run(SQL_INSERT_ALIAS_VERSION_META)
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

    async def _read_schema_version_locked(self) -> int:
        rows = await self._runner.run(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            fetch=True,
        )
        return int(rows[0][0]) if rows and rows[0][0] is not None else 1

    async def _write_schema_version_locked(self, version: int) -> None:
        await self._runner.run(
            "INSERT INTO meta (key, value) VALUES ('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (version,),
        )

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
            current_version = await self._read_schema_version_locked()
            rows = await self._runner.run(
                SQL_PRAGMA_TABLE_INFO_MESSAGES_CLAIMED, fetch=True
            )
            if rows and rows[0][0] > 0:
                if current_version < 2:
                    await self._runner.begin_immediate()
                    try:
                        await self._write_schema_version_locked(2)
                        await self._runner.commit()
                    except Exception:
                        await self._runner.rollback()
                        raise
                return

            try:
                await self._runner.begin_immediate()
                await self._runner.run(SQL_ALTER_MESSAGES_ADD_CLAIMED)
                await self._runner.run(SQL_CREATE_IDX_MESSAGES_PENDING_QUEUE_TS)
                if current_version < 2:
                    await self._write_schema_version_locked(2)
                await self._runner.commit()
            except Exception as e:
                await self._runner.rollback()
                if "duplicate column name" not in str(e):
                    raise

    async def _ensure_schema_v3(self) -> None:
        """Add unique constraint to timestamp column."""
        async with self._lock:
            current_version = await self._read_schema_version_locked()
            rows = await self._runner.run(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_messages_ts_unique'",
                fetch=True,
            )
            if rows and rows[0][0] > 0:
                if current_version < 3:
                    await self._runner.begin_immediate()
                    try:
                        await self._write_schema_version_locked(3)
                        await self._runner.commit()
                    except Exception:
                        await self._runner.rollback()
                        raise
                return

            try:
                await self._runner.begin_immediate()
                await self._runner.run(SQL_CREATE_IDX_MESSAGES_TS_UNIQUE)
                if current_version < 3:
                    await self._write_schema_version_locked(3)
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

    async def _ensure_schema_v4(self) -> None:
        """Ensure schema v4 objects used by queue aliases exist."""
        async with self._lock:
            current_version = await self._read_schema_version_locked()
            try:
                await self._runner.begin_immediate()
                await self._runner.run(SQL_CREATE_TABLE_QUEUE_ALIASES)
                await self._runner.run(SQL_CREATE_IDX_QUEUE_ALIASES_TARGET)
                await self._runner.run(SQL_INSERT_ALIAS_VERSION_META)
                if current_version < 4:
                    await self._write_schema_version_locked(4)
                await self._runner.commit()
            except Exception:
                await self._runner.rollback()
                raise

    async def _ensure_schema_v5(self) -> None:
        """Ensure schema v5's pending queue/timestamp index exists."""
        async with self._lock:
            current_version = await self._read_schema_version_locked()
            try:
                await self._runner.begin_immediate()
                await self._runner.run(SQL_CREATE_IDX_MESSAGES_PENDING_QUEUE_TS)
                if current_version < 5:
                    await self._write_schema_version_locked(5)
                await self._runner.commit()
            except Exception:
                await self._runner.rollback()
                raise

    async def _ensure_schema_v6(self) -> None:
        """Retire the private row key and make public message ID the row key."""
        async with self._lock:
            current_version = await self._read_schema_version_locked()
            foreign_keys_enabled = False
            legacy_alter_table_enabled = False
            if current_version < 6:
                rows = await self._runner.run("PRAGMA foreign_keys", fetch=True)
                foreign_keys_enabled = bool(rows[0][0])
                rows = await self._runner.run("PRAGMA legacy_alter_table", fetch=True)
                legacy_alter_table_enabled = bool(rows[0][0])
                if foreign_keys_enabled:
                    await self._runner.run("PRAGMA foreign_keys = OFF")
                if not legacy_alter_table_enabled:
                    await self._runner.run("PRAGMA legacy_alter_table = ON")
            try:
                await self._runner.begin_immediate()
                if current_version < 6:
                    await self._runner.run(
                        create_messages_table_sql(
                            "simplebroker_messages_v6",
                            if_not_exists=False,
                        )
                    )
                    await self._runner.run(
                        "INSERT INTO simplebroker_messages_v6 "
                        "(queue, body, ts, claimed) "
                        "SELECT queue, body, ts, claimed FROM messages"
                    )
                    await self._runner.run(SQL_ALTER_MESSAGES_RENAME_V5)
                    await self._runner.run(SQL_ALTER_MESSAGES_V6_RENAME_CURRENT)
                    await self._runner.run(SQL_DROP_MESSAGES_V5)
                    await self._write_schema_version_locked(6)
                else:
                    rows = await self._runner.run(
                        "PRAGMA table_info(messages)", fetch=True
                    )
                    shape = tuple(
                        (
                            str(row[1]),
                            str(row[2]).upper(),
                            int(row[3]),
                            row[4],
                            int(row[5]),
                        )
                        for row in rows
                    )
                    expected_shape = (
                        ("queue", "TEXT", 1, None, 0),
                        ("body", "TEXT", 1, None, 0),
                        ("ts", "INTEGER", 1, None, 1),
                        ("claimed", "INTEGER", 0, "0", 0),
                    )
                    if shape != expected_shape:
                        raise RuntimeError(
                            "SQLite schema version 6 requires public message ID "
                            "as the sole row key in the canonical message shape"
                        )
                await self._runner.run(SQL_CREATE_IDX_MESSAGES_QUEUE_TS)
                await self._runner.run(SQL_CREATE_IDX_MESSAGES_PENDING_QUEUE_TS)
                await self._runner.commit()
            except Exception:
                await self._runner.rollback()
                raise
            finally:
                if current_version < 6:
                    try:
                        if not legacy_alter_table_enabled:
                            await self._runner.run("PRAGMA legacy_alter_table = OFF")
                    finally:
                        if foreign_keys_enabled:
                            await self._runner.run("PRAGMA foreign_keys = ON")

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
                if ("locked" in msg or "busy" in msg) and attempt < max_retries - 1:
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
        if message_size > self._max_message_size:
            raise ValueError(
                f"Message size ({message_size} bytes) exceeds maximum allowed size "
                f"({self._max_message_size} bytes)"
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
        if int(self._config["BROKER_AUTO_VACUUM"]) == 1:
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
        exact_timestamp: int | None = None,
    ) -> list[str]:
        """Read message(s) from a queue."""
        messages = []
        async for message in self.stream_read(
            queue, peek=peek, all_messages=all_messages, exact_timestamp=exact_timestamp
        ):
            messages.append(message)
        return messages

    async def _stream_peek(
        self,
        where_conditions: list[str],
        params: list[Any],
        *,
        all_messages: bool,
    ) -> AsyncGenerator[str]:
        offset = 0
        batch_size = 100 if all_messages else 1
        while True:
            rows = await self._runner.run(
                build_peek_query(where_conditions),
                tuple(params + [batch_size, offset]),
                fetch=True,
            )
            if not rows:
                return
            for row in rows:
                yield row[0]
            if not all_messages:
                return
            offset += batch_size

    async def _stream_all_exactly_once(
        self,
        where_conditions: list[str],
        params: list[Any],
    ) -> AsyncGenerator[str]:
        while True:
            async with self._lock:
                await self._execute_with_retry(self._runner.begin_immediate)
                try:
                    rows = await self._runner.run(
                        build_claim_single_query(where_conditions),
                        tuple(params),
                        fetch=True,
                    )
                    if not rows:
                        await self._runner.rollback()
                        return
                    await self._runner.commit()
                    message = rows[0]
                except Exception:
                    await self._runner.rollback()
                    raise
            yield message[0]

    async def _stream_all_at_least_once(
        self,
        where_conditions: list[str],
        params: list[Any],
        *,
        commit_interval: int,
    ) -> AsyncGenerator[str]:
        while True:
            async with self._lock:
                await self._execute_with_retry(self._runner.begin_immediate)
                try:
                    batch_messages = await self._runner.run(
                        build_claim_batch_query(where_conditions),
                        tuple(params + [commit_interval]),
                        fetch=True,
                    )
                    if not batch_messages:
                        await self._runner.rollback()
                        return
                except Exception:
                    await self._runner.rollback()
                    raise

            batch_committed = False
            try:
                for row in batch_messages:
                    yield row[0]
                async with self._lock:
                    await self._runner.commit()
                    batch_committed = True
            except BaseException:
                if not batch_committed:
                    await self._runner.rollback()
                raise

    async def _stream_one(
        self,
        where_conditions: list[str],
        params: list[Any],
    ) -> AsyncGenerator[str]:
        async with self._lock:
            await self._execute_with_retry(self._runner.begin_immediate)
            try:
                rows = await self._runner.run(
                    build_claim_single_query(where_conditions),
                    tuple(params),
                    fetch=True,
                )
                if rows:
                    await self._runner.commit()
                    yield rows[0][0]
                else:
                    await self._runner.rollback()
            except Exception:
                await self._runner.rollback()
                raise

    async def stream_read(
        self,
        queue: str,
        peek: bool = False,
        all_messages: bool = False,
        exact_timestamp: int | None = None,
        commit_interval: int = 1,
        after_timestamp: int | None = None,
    ) -> AsyncIterator[str]:
        """Stream messages from a queue."""
        await self._ensure_initialized()
        self._validate_queue_name(queue)

        # Build WHERE clause
        params: list[Any]
        if exact_timestamp is not None:
            where_conditions = ["ts = ?", "queue = ?", "claimed = 0"]
            params = [exact_timestamp, queue]
        else:
            where_conditions = ["queue = ?", "claimed = 0"]
            params = [queue]
            if after_timestamp is not None:
                where_conditions.append("ts > ?")
                params.append(after_timestamp)

        stream: AsyncGenerator[str]
        if peek:
            stream = self._stream_peek(
                where_conditions,
                params,
                all_messages=all_messages,
            )
        elif all_messages and commit_interval == 1:
            stream = self._stream_all_exactly_once(where_conditions, params)
        elif all_messages:
            stream = self._stream_all_at_least_once(
                where_conditions,
                params,
                commit_interval=commit_interval,
            )
        else:
            stream = self._stream_one(where_conditions, params)

        try:
            async for message in stream:
                yield message
        finally:
            await stream.aclose()

    async def list_queues(self) -> list[str]:
        """List queue names."""
        await self._ensure_initialized()

        async def _do_list() -> list[tuple[Any, ...]]:
            async with self._lock:
                result = await self._runner.run(SQL_SELECT_DISTINCT_QUEUES, fetch=True)
                return list(result)

        rows = await self._execute_with_retry(_do_list)
        return [str(row[0]) for row in rows]

    async def get_queue_stats(self) -> list[tuple[str, int, int]]:
        """Get all queues with both unclaimed and total message counts."""
        await self._ensure_initialized()

        async def _do_stats() -> list[tuple[Any, ...]]:
            async with self._lock:
                result = await self._runner.run(SQL_SELECT_QUEUES_STATS, fetch=True)
                return list(result)

        rows = await self._execute_with_retry(_do_stats)
        return [(str(row[0]), int(row[1]), int(row[2])) for row in rows]

    async def delete(self, queue: str | None = None) -> None:
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
        message_id: int | None = None,
        require_unclaimed: bool = True,
    ) -> dict[str, Any] | None:
        """Move message(s) from one queue to another."""
        await self._ensure_initialized()
        self._validate_queue_name(source_queue)
        self._validate_queue_name(dest_queue)

        async def _do_move() -> dict[str, Any] | None:
            async with self._lock:
                await self._execute_with_retry(self._runner.begin_immediate)

                try:
                    if message_id is not None:
                        # Public message ID is the timestamp.
                        where_conditions = ["ts = ?", "queue = ?"]
                        params = [message_id, source_queue]

                        if require_unclaimed:
                            where_conditions.append("claimed = 0")

                        rows = await self._runner.run(
                            build_move_by_timestamp_query(where_conditions),
                            (dest_queue, *params),
                            fetch=True,
                        )
                    else:
                        # Move oldest
                        rows = await self._runner.run(
                            """
                            UPDATE messages
                            SET queue = ?, claimed = 0
                            WHERE ts IN (
                                SELECT ts FROM messages
                                WHERE queue = ? AND claimed = 0
                                ORDER BY ts
                                LIMIT 1
                            )
                            RETURNING body, ts
                            """,
                            (dest_queue, source_queue),
                            fetch=True,
                        )

                    if rows:
                        await self._runner.commit()
                        message = rows[0]
                        return {
                            "id": int(message[1]),
                            "body": str(message[0]),
                            "ts": int(message[1]),
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

            return vacuum_is_eligible(
                claimed_count=int(claimed_count),
                total_count=int(total_count),
                threshold=float(self._config["BROKER_VACUUM_THRESHOLD"]),
            )

    async def _vacuum_claimed_messages(self) -> None:
        """Delete claimed messages in batches."""
        batch_size = int(self._config["BROKER_VACUUM_BATCH_SIZE"])

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
                           WHERE ts IN (
                               SELECT ts FROM messages
                               WHERE claimed = 1
                               ORDER BY ts
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
    ) -> str | None:
        """Read a single message from this queue."""
        messages = await self._broker.read(
            self.name, peek=peek, all_messages=all_messages
        )
        return messages[0] if messages else None

    async def read_all(self, *, peek: bool = False) -> list[str]:
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
        stream = cast(
            AsyncGenerator[str, None],
            self._broker.stream_read(
                self.name,
                peek=peek,
                all_messages=all_messages,
                commit_interval=commit_interval,
            ),
        )
        try:
            async for message in stream:
                yield message
        finally:
            await stream.aclose()

    async def size(self) -> int:
        """Get the number of unclaimed messages in this queue."""
        queue_stats = await self._broker.get_queue_stats()
        for queue_name, count, _total in queue_stats:
            if queue_name == self.name:
                return count
        return 0

    async def move_to(
        self,
        dest_queue: str,
        *,
        message_id: int | None = None,
    ) -> dict[str, Any] | None:
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
    config: Mapping[str, Any] | None = None,
) -> AsyncIterator[AsyncBrokerCore]:
    """Context manager for async broker with connection pooling."""
    resolved_config = resolve_config(config)
    runner = PooledAsyncSQLiteRunner(
        db_path,
        pool_size,
        max_connections,
        config=resolved_config,
    )
    broker = AsyncBrokerCore(runner, config=resolved_config)
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
                        raise RuntimeError("Simulated processing error")

                    print(f"  Successfully processed: {message}")
                    return True
                except RuntimeError as e:
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
