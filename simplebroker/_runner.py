"""SQL execution abstraction for SimpleBroker extensions.

This module provides the SQLRunner protocol and default SQLiteRunner implementation
that enables SimpleBroker to be extended with custom backends while maintaining
its core philosophy and performance characteristics.
"""

import os
import sqlite3
import threading
import time
import warnings
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Literal, Protocol, Set, Tuple, Union, cast

from ._exceptions import DataError, IntegrityError, OperationalError
from .helpers import _execute_with_retry


class SetupPhase(Enum):
    """Generic setup phases that any SQL implementation might have."""

    CONNECTION = "connection"  # Basic connectivity and critical settings (WAL mode)
    OPTIMIZATION = "optimization"  # Performance settings (cache, synchronous mode)


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

    def setup(self, phase: SetupPhase) -> None:
        """Run specific setup phase.

        Args:
            phase: The setup phase to execute

        Note: Implementations should make this idempotent.
        """
        ...

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        """Check if a setup phase has been completed.

        Args:
            phase: The setup phase to check

        Returns:
            True if the phase has been completed
        """
        ...


class SQLiteRunner:
    """Default synchronous SQLite implementation with thread-local connections."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._thread_local = threading.local()
        # Store PID to detect fork
        self._pid = os.getpid()
        # Track completed setup phases
        self._completed_phases: Set[SetupPhase] = set()
        self._setup_lock = threading.Lock()
        # Track created marker files for cleanup
        self._created_files: Set[Path] = set()
        # Track if we created the database file (for cleanup of test mocks)
        self._created_db = False
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
            # Process was forked, need to clean up inherited connection
            if hasattr(self._thread_local, "conn"):
                try:
                    # Close the stale connection from parent process
                    self._thread_local.conn.close()
                except Exception:
                    # Ignore errors - connection might already be closed
                    pass
            # Clear thread-local storage for the new process
            self._thread_local = threading.local()
            # Also reset setup phases for the new process
            with self._setup_lock:
                self._completed_phases.clear()
            self._pid = current_pid

        # Check if this thread has a connection
        if not hasattr(self._thread_local, "conn"):
            # Check if database exists before creating connection
            db_existed = os.path.exists(self._db_path)

            # Create new connection for this thread with autocommit mode
            # This is crucial for proper transaction handling
            self._thread_local.conn = sqlite3.connect(
                self._db_path, isolation_level=None
            )

            # Track if we created the database (for test cleanup)
            if not db_existed and os.path.exists(self._db_path):
                self._created_db = True
                # Track the database file for cleanup if it looks like a mock path
                if "Mock" in self._db_path:
                    self._created_files.add(Path(self._db_path))

            # Apply per-connection settings
            self._apply_connection_settings(self._thread_local.conn)
        else:
            # Check if optimization phase was completed after connection was created
            if SetupPhase.OPTIMIZATION in self._completed_phases and not hasattr(
                self._thread_local, "optimization_applied"
            ):
                # Apply optimization settings to existing connection
                self._apply_optimization_settings(self._thread_local.conn)
                self._thread_local.optimization_applied = True

        return cast(sqlite3.Connection, self._thread_local.conn)

    def _apply_connection_settings(self, conn: sqlite3.Connection) -> None:
        """Apply per-connection settings that don't require exclusive locks."""
        # Always set busy timeout for each connection
        busy_timeout = int(os.environ.get("BROKER_BUSY_TIMEOUT", "5000"))
        conn.execute(f"PRAGMA busy_timeout={busy_timeout}")

        # Set WAL autocheckpoint for each connection
        # Default to 1000 pages (≈1MB) if not specified
        wal_autocheckpoint = int(os.environ.get("BROKER_WAL_AUTOCHECKPOINT", "1000"))
        if wal_autocheckpoint < 0:
            warnings.warn(
                f"Invalid BROKER_WAL_AUTOCHECKPOINT '{wal_autocheckpoint}', "
                "must be >= 0. Using default of 1000.",
                stacklevel=2,
            )
            wal_autocheckpoint = 1000
        conn.execute(f"PRAGMA wal_autocheckpoint={wal_autocheckpoint}")

        # Apply optimization settings if that phase is complete
        if SetupPhase.OPTIMIZATION in self._completed_phases:
            self._apply_optimization_settings(conn)
            self._thread_local.optimization_applied = True

    def _check_sqlite_version(self) -> None:
        """Check SQLite version requirement."""
        conn = sqlite3.connect(":memory:")
        try:
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
        finally:
            conn.close()

    def _setup_connection_phase(self) -> None:
        """Setup critical connection settings including WAL mode."""
        # First check SQLite version
        self._check_sqlite_version()

        def do_setup() -> None:
            # Use a separate connection for WAL setup to avoid holding locks
            setup_conn = sqlite3.connect(self._db_path, isolation_level=None)
            try:
                # Set timeout for setup operations
                setup_conn.execute("PRAGMA busy_timeout=10000")

                # Check current journal mode
                cursor = setup_conn.execute("PRAGMA journal_mode")
                current_mode = cursor.fetchone()[0] if cursor else "delete"

                if current_mode.lower() != "wal":
                    # Enable WAL mode - this requires an exclusive lock
                    cursor = setup_conn.execute("PRAGMA journal_mode=WAL")
                    if cursor:
                        result = cursor.fetchone()
                        if result and result[0].lower() != "wal":
                            raise RuntimeError(
                                f"Failed to enable WAL mode, got: {result}"
                            )

                # WAL autocheckpoint is now set per-connection in _apply_connection_settings

            finally:
                setup_conn.close()

        # Use retry logic for setup operations
        _execute_with_retry(do_setup, max_retries=30, retry_delay=0.1)

    def _setup_optimization_phase(self) -> None:
        """Setup performance optimizations."""
        # These settings are stored and applied to each connection
        # They don't require exclusive database locks
        pass  # Settings are applied in _apply_optimization_settings

    def _apply_optimization_settings(self, conn: sqlite3.Connection) -> None:
        """Apply optimization settings to a connection."""
        # Cache size (default 10MB)
        # Negative values mean KiB (kibibytes), so we multiply by 1024
        cache_mb = int(os.environ.get("BROKER_CACHE_MB", "10"))
        conn.execute(f"PRAGMA cache_size=-{cache_mb * 1024}")

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

    def setup(self, phase: SetupPhase) -> None:
        """Run specific setup phase in an idempotent manner.

        Args:
            phase: The setup phase to execute

        File Locking Strategy:
            - Unix/Linux/macOS: Uses fcntl for truly atomic file locking
            - Windows with msvcrt: Uses Windows locking API for proper exclusive locks
            - Windows without msvcrt: Falls back to open(path, 'x') which has a race
              condition between checking file existence and creating it. This is a
              check-then-act operation that could allow multiple processes to think
              they have the lock if they check at the same time.

        The Windows fallback is less robust but better than no locking. Production
        Windows deployments should ensure msvcrt is available (it's part of the
        Python standard library on Windows).
        """
        # Quick check without lock
        if phase in self._completed_phases:
            return

        # Use file-based lock for cross-process coordination
        try:
            lock_path = Path(self._db_path).with_suffix(f".{phase.value}.lock")

            # Ensure parent directory exists
            lock_path.parent.mkdir(parents=True, exist_ok=True)

            # Track for cleanup
            self._created_files.add(lock_path)
        except (ValueError, OSError, TypeError):
            # Handle invalid paths (e.g., from mocked tests)
            # Just skip setup for invalid paths
            return

        # Import here to avoid circular dependency

        # Platform-specific file locking
        try:
            import fcntl

            has_fcntl = True
        except ImportError:
            has_fcntl = False
            # Try Windows-specific locking
            try:
                import msvcrt

                has_msvcrt = True
            except ImportError:
                has_msvcrt = False

        # Try to acquire file lock with timeout
        max_wait = 10.0  # seconds
        start_time = time.time()
        lock_file: Union[Any, None] = None
        lock_acquired = False

        while not lock_acquired:
            try:
                if has_fcntl:
                    # Unix-like systems
                    try:
                        # Create or open lock file
                        lock_file = open(lock_path, "w")
                        # Set restrictive permissions for security
                        try:
                            os.chmod(lock_path, 0o600)
                        except OSError:
                            # Don't fail on permission issues (e.g., Windows)
                            pass
                        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        lock_acquired = True
                    except OSError:
                        if lock_file:
                            lock_file.close()
                            lock_file = None
                        # Continue trying until timeout
                        if time.time() - start_time > max_wait:
                            raise OperationalError(
                                f"Timeout waiting for setup lock: {phase.value}"
                            ) from None
                        # Note: Using time.sleep here instead of interruptible_sleep because:
                        # 1. This is low-level database setup code without a stop event
                        # 2. The wait is very short (50ms) for file lock acquisition
                        # 3. This runs during initialization, not in long-running threads
                        time.sleep(0.05)
                        continue
                else:
                    # Windows file locking fallback
                    if has_msvcrt:
                        # Use msvcrt for proper Windows file locking
                        try:
                            # Open or create the lock file
                            lock_file = open(lock_path, "a+b")
                            lock_file.seek(0)
                            # Set restrictive permissions for security
                            try:
                                os.chmod(lock_path, 0o600)
                            except OSError:
                                # Windows may not support chmod
                                pass

                            # Try to acquire exclusive lock with msvcrt
                            # msvcrt.locking raises OSError if lock cannot be acquired
                            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]
                            lock_acquired = True
                        except OSError:
                            # Lock is held by another process
                            if lock_file:
                                lock_file.close()
                                lock_file = None

                            if time.time() - start_time > max_wait:
                                raise OperationalError(
                                    f"Timeout waiting for setup lock: {phase.value}"
                                ) from None
                            # Note: Using time.sleep for file lock acquisition (see comment above)
                            time.sleep(0.05)
                            continue
                    else:
                        # Last resort: Windows without msvcrt
                        # WARNING: This approach uses 'x' mode which is NOT truly atomic
                        # It's a check-then-act operation susceptible to race conditions
                        # between checking if file exists and creating it exclusively.
                        # This should only be used when msvcrt is not available.
                        if lock_path.exists():
                            # Check if lock is stale (older than max_wait)
                            try:
                                if time.time() - lock_path.stat().st_mtime > max_wait:
                                    # Stale lock, remove it
                                    lock_path.unlink()
                            except OSError:
                                pass  # Another process might have removed it

                            # Wait before retry
                            if time.time() - start_time > max_wait:
                                raise OperationalError(
                                    f"Timeout waiting for setup lock: {phase.value}"
                                ) from None
                            # Note: Using time.sleep for file lock acquisition (see comment above)
                            time.sleep(0.05)  # Shorter wait on Windows
                            continue

                        # Try to create lock file exclusively
                        # NOTE: Race condition exists here between check and creation
                        try:
                            lock_file = open(lock_path, "x")
                            # Set restrictive permissions for security
                            try:
                                os.chmod(lock_path, 0o600)
                            except OSError:
                                # Some systems may not support chmod
                                pass
                            lock_acquired = True
                        except FileExistsError:
                            # File was created by another process, retry
                            pass
            except OSError:
                if lock_file:
                    lock_file.close()
                    lock_file = None

                if time.time() - start_time > max_wait:
                    raise OperationalError(
                        f"Timeout waiting for setup lock: {phase.value}"
                    ) from None
                # Note: Using time.sleep for file lock acquisition (see comment above)
                time.sleep(0.05)  # Shorter wait for faster retries

        try:
            # Double-check with thread lock
            with self._setup_lock:
                if phase in self._completed_phases:
                    return

                # Check if another process already completed this phase
                try:
                    marker_path = Path(self._db_path).with_suffix(
                        f".{phase.value}.done"
                    )
                    if marker_path.exists():
                        self._completed_phases.add(phase)
                        return
                except (ValueError, OSError, TypeError):
                    # Invalid path, skip
                    return

                # Execute the phase
                if phase == SetupPhase.CONNECTION:
                    self._setup_connection_phase()
                elif phase == SetupPhase.OPTIMIZATION:
                    self._setup_optimization_phase()

                # Mark as complete
                self._completed_phases.add(phase)
                try:
                    # Create marker file - only set permissions if it doesn't exist
                    if not marker_path.exists():
                        marker_path.touch(mode=0o600)
                    else:
                        # File exists, just touch it to update timestamp
                        marker_path.touch()
                    # Track for cleanup
                    self._created_files.add(marker_path)
                except (ValueError, OSError, TypeError):
                    # Invalid path, but phase is complete in memory
                    pass

        finally:
            # Release file lock
            if lock_file:
                if has_fcntl:
                    try:
                        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    except OSError:
                        pass
                elif "has_msvcrt" in locals() and has_msvcrt:
                    try:
                        # Unlock before closing on Windows
                        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
                    except OSError:
                        pass
                lock_file.close()

            # Only unlink lock file if we're not using msvcrt
            # (msvcrt needs the file to exist for other processes to lock)
            if not ("has_msvcrt" in locals() and has_msvcrt):
                try:
                    lock_path.unlink()
                except OSError:
                    pass

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        """Check if a setup phase has been completed.

        Args:
            phase: The setup phase to check

        Returns:
            True if the phase has been completed
        """
        if phase in self._completed_phases:
            return True

        # Check for marker file from another process
        try:
            marker_path = Path(self._db_path).with_suffix(f".{phase.value}.done")
            if marker_path.exists():
                with self._setup_lock:
                    self._completed_phases.add(phase)
                return True
        except (ValueError, OSError, TypeError):
            # Invalid path
            pass

        return False

    def cleanup_marker_files(self) -> None:
        """Clean up any marker files created during setup.

        This is particularly useful for tests that use mocked paths.
        """
        for file_path in self._created_files:
            try:
                if file_path.exists():
                    file_path.unlink()
            except (OSError, ValueError, TypeError):
                # Ignore errors during cleanup
                pass
        self._created_files.clear()

    def __enter__(self) -> "SQLiteRunner":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        """Exit context manager - cleanup marker files."""
        self.cleanup_marker_files()
        self.close()
        return False
