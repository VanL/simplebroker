"""SQL execution abstraction for SimpleBroker extensions.

This module provides the SQLRunner protocol and default SQLiteRunner implementation
that enables SimpleBroker to be extended with custom backends while maintaining
its core philosophy and performance characteristics.
"""

import os
import sqlite3
import warnings
from typing import Any, Iterable, Protocol, Tuple

from ._exceptions import DataError, IntegrityError, OperationalError


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
    """Default synchronous SQLite implementation."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._setup_connection()

    def _setup_connection(self) -> None:
        """Apply all PRAGMA settings from existing BrokerDB."""
        # Check SQLite version (requires 3.35.0+ for RETURNING clause)
        cursor = self._conn.execute("SELECT sqlite_version()")
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
        self._conn.execute(f"PRAGMA busy_timeout={busy_timeout}")

        # Cache size (default 10MB)
        cache_mb = int(os.environ.get("BROKER_CACHE_MB", "10"))
        self._conn.execute(f"PRAGMA cache_size=-{cache_mb * 1000}")

        # Synchronous mode (default FULL)
        sync_mode = os.environ.get("BROKER_SYNC_MODE", "FULL").upper()
        if sync_mode not in ("OFF", "NORMAL", "FULL", "EXTRA"):
            warnings.warn(
                f"Invalid BROKER_SYNC_MODE '{sync_mode}', defaulting to FULL",
                RuntimeWarning,
                stacklevel=4,
            )
            sync_mode = "FULL"
        self._conn.execute(f"PRAGMA synchronous={sync_mode}")

        # Enable WAL mode
        cursor = self._conn.execute("PRAGMA journal_mode=WAL")
        if cursor:
            result = cursor.fetchone()
            if result and result[0].lower() != "wal":
                raise RuntimeError(f"Failed to enable WAL mode, got: {result}")

        # WAL autocheckpoint
        self._conn.execute("PRAGMA wal_autocheckpoint=1000")

    def run(
        self, sql: str, params: Tuple[Any, ...] = (), *, fetch: bool = False
    ) -> Iterable[Tuple[Any, ...]]:
        """Execute SQL and optionally return rows."""
        try:
            cursor = self._conn.execute(sql, params)
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
            self._conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as e:
            raise OperationalError(str(e)) from e
        except sqlite3.IntegrityError as e:
            raise IntegrityError(str(e)) from e
        except sqlite3.DataError as e:
            raise DataError(str(e)) from e

    def commit(self) -> None:
        """Commit the current transaction."""
        try:
            self._conn.commit()
        except sqlite3.OperationalError as e:
            raise OperationalError(str(e)) from e
        except sqlite3.IntegrityError as e:
            raise IntegrityError(str(e)) from e
        except sqlite3.DataError as e:
            raise DataError(str(e)) from e

    def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            self._conn.rollback()
        except sqlite3.OperationalError as e:
            raise OperationalError(str(e)) from e
        except sqlite3.IntegrityError as e:
            raise IntegrityError(str(e)) from e
        except sqlite3.DataError as e:
            raise DataError(str(e)) from e

    def close(self) -> None:
        """Close the connection and release resources."""
        self._conn.close()
