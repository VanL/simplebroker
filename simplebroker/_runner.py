"""SQL execution abstraction for SimpleBroker extensions.

This module provides the SQLRunner protocol and default SQLiteRunner implementation
that enables SimpleBroker to be extended with custom backends while maintaining
its core philosophy and performance characteristics.
"""

import os
import sqlite3
import threading
import warnings
from typing import Any, Iterable, Protocol, Tuple, cast

from ._exceptions import DataError, IntegrityError, OperationalError
from .helpers import _execute_with_retry


class SQLRunner(Protocol):
    """Executes SQL with transaction control.

    Contract requirements:
    - Must handle thread-local or concurrency-safe connections
    - Must guarantee transactional boundaries as BrokerCore expects
    - Must raise OperationalError on locking for retry logic
    - Must be fork-safe (recreate connections after os.fork())
    - Must handle connection lifecycle (open/close)
    """

    def run(
        self,
        sql: str,
        params: Tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Iterable[Tuple[Any, ...]]:
        """Execute SQL and optionally return rows.

        Args:
            sql: SQL statement to execute
            params: Parameters for the SQL statement
            fetch: If True, return results; if False, return empty iterable

        Returns:
            Iterable of result rows if fetch=True, empty iterable otherwise

        Raises:
            OperationalError: For database locks/busy (enables retry)
            IntegrityError: For constraint violations
            DataError: For data format/type errors
            Other BrokerError subclasses as appropriate
        """
        ...

    def begin_immediate(self) -> None:
        """Start an immediate transaction."""
        ...

    def commit(self) -> None:
        """Commit the current transaction."""
        ...

    def rollback(self) -> None:
        """Rollback the current transaction."""
        ...

    def close(self) -> None:
        """Close the connection and release resources."""
        ...


class SQLiteRunner:
    """Default synchronous SQLite implementation with thread-local connections."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._thread_local = threading.local()
        # Store PID to detect fork
        self._pid = os.getpid()
        # For backward compatibility, expose _conn as a property
        # that returns the current thread's connection

    @property
    def _conn(self) -> sqlite3.Connection:
        """Backward compatibility property for accessing connection."""
        return self._get_connection()

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create a thread-local connection.

        This ensures each thread has its own SQLite connection, avoiding
        potential deadlocks and following SQLite best practices for
        multi-threaded applications.
        """
        # Check if we've been forked
        current_pid = os.getpid()
        if current_pid != self._pid:
            # Process was forked, clear thread-local storage
            self._thread_local = threading.local()
            self._pid = current_pid

        # Check if this thread has a connection
        if not hasattr(self._thread_local, "conn"):
            # Create new connection for this thread with autocommit mode
            # This is crucial for proper transaction handling
            self._thread_local.conn = sqlite3.connect(
                self._db_path, isolation_level=None
            )
            _execute_with_retry(lambda: self._setup_connection(self._thread_local.conn))

        return cast(sqlite3.Connection, self._thread_local.conn)

    def _setup_connection(self, conn: sqlite3.Connection) -> None:
        """Apply all PRAGMA settings from existing BrokerDB."""
        # Check SQLite version (requires 3.35.0+ for RETURNING clause)
        cursor = conn.execute("SELECT sqlite_version()")
        if cursor:
            version = cursor.fetchone()
            if version:
                version_parts = [int(x) for x in version[0].split(".")]
                if version_parts < [3, 35, 0]:
                    raise RuntimeError(
                        f"SQLite version {version[0]} is too old. "
                        f"SimpleBroker requires SQLite 3.35.0 or later for RETURNING clause support."
                    )

        # Busy timeout (default 5000ms)
        busy_timeout = int(os.environ.get("BROKER_BUSY_TIMEOUT", "5000"))
        conn.execute(f"PRAGMA busy_timeout={busy_timeout}")

        # Cache size (default 10MB)
        cache_mb = int(os.environ.get("BROKER_CACHE_MB", "10"))
        conn.execute(f"PRAGMA cache_size=-{cache_mb * 1000}")

        # Synchronous mode (default FULL)
        sync_mode = os.environ.get("BROKER_SYNC_MODE", "FULL").upper()
        if sync_mode not in ("OFF", "NORMAL", "FULL", "EXTRA"):
            warnings.warn(
                f"Invalid BROKER_SYNC_MODE '{sync_mode}', defaulting to FULL",
                RuntimeWarning,
                stacklevel=4,
            )
            sync_mode = "FULL"
        conn.execute(f"PRAGMA synchronous={sync_mode}")

        # Enable WAL mode
        cursor = conn.execute("PRAGMA journal_mode=WAL")
        if cursor:
            result = cursor.fetchone()
            if result and result[0].lower() != "wal":
                raise RuntimeError(f"Failed to enable WAL mode, got: {result}")

        # WAL autocheckpoint
        conn.execute("PRAGMA wal_autocheckpoint=1000")

    def run(
        self, sql: str, params: Tuple[Any, ...] = (), *, fetch: bool = False
    ) -> Iterable[Tuple[Any, ...]]:
        """Execute SQL and optionally return rows."""
        try:
            conn = self._get_connection()
            cursor = conn.execute(sql, params)
            # Only fetch if explicitly requested
            if fetch:
                return cursor.fetchall()
            return []
        except sqlite3.OperationalError as e:
            raise OperationalError(str(e)) from e
        except sqlite3.IntegrityError as e:
            raise IntegrityError(str(e)) from e
        except sqlite3.DataError as e:
            raise DataError(str(e)) from e

    def begin_immediate(self) -> None:
        """Start an immediate transaction."""
        try:
            conn = self._get_connection()
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as e:
            raise OperationalError(str(e)) from e
        except sqlite3.IntegrityError as e:
            raise IntegrityError(str(e)) from e
        except sqlite3.DataError as e:
            raise DataError(str(e)) from e

    def commit(self) -> None:
        """Commit the current transaction."""
        try:
            conn = self._get_connection()
            conn.commit()
        except sqlite3.OperationalError as e:
            raise OperationalError(str(e)) from e
        except sqlite3.IntegrityError as e:
            raise IntegrityError(str(e)) from e
        except sqlite3.DataError as e:
            raise DataError(str(e)) from e

    def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            conn = self._get_connection()
            conn.rollback()
        except sqlite3.OperationalError as e:
            raise OperationalError(str(e)) from e
        except sqlite3.IntegrityError as e:
            raise IntegrityError(str(e)) from e
        except sqlite3.DataError as e:
            raise DataError(str(e)) from e

    def close(self) -> None:
        """Close the connection and release resources."""
        # Close the current thread's connection if it exists
        if hasattr(self._thread_local, "conn"):
            try:
                self._thread_local.conn.close()
            except Exception:
                pass  # Ignore errors during cleanup
            delattr(self._thread_local, "conn")
