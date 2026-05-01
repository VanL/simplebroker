"""SQL execution abstraction for SimpleBroker extensions.

This module provides the SQLRunner protocol and default SQLiteRunner implementation
that enables SimpleBroker to be extended with custom backends while maintaining
its core philosophy and performance characteristics.
"""

from __future__ import annotations

import itertools
import os
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable
from enum import Enum
from pathlib import Path
from typing import Any, Literal, Protocol, cast

# Platform-specific imports for file locking
try:
    import fcntl

    HAS_FCNTL = True
except ImportError:
    HAS_FCNTL = False
    fcntl = None  # type: ignore[assignment]

try:
    import msvcrt

    HAS_MSVCRT = True
except ImportError:
    HAS_MSVCRT = False
    msvcrt = None  # type: ignore[assignment]

import contextlib
import sys

# Self was added to typing in Python 3.11
if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing import TypeVar

    Self = TypeVar("Self", bound="SQLiteRunner")  # type: ignore[misc]

from ._backends import get_configured_backend
from ._constants import SCHEMA_VERSION, ConnectionPhase, load_config
from ._exceptions import DataError, IntegrityError, OperationalError
from .helpers import _execute_with_retry

# Load config once at module level
_config = load_config()
db_backend = get_configured_backend(_config)


class SetupPhase(Enum):
    """Generic setup phases that any SQL implementation might have."""

    CONNECTION = ConnectionPhase.CONNECTION
    SCHEMA = ConnectionPhase.SCHEMA
    OPTIMIZATION = ConnectionPhase.OPTIMIZATION


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
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Iterable[tuple[Any, ...]]:
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

    _instance_counter = itertools.count()  # Unique instance ID for debugging

    def __init__(self, db_path: str) -> None:
        self.instance_id = next(self._instance_counter)
        self._db_path = db_path
        self._thread_local = threading.local()
        # Store PID to detect fork
        self._pid = os.getpid()
        # Track completed setup phases
        self._completed_phases: set[SetupPhase] = set()
        self._setup_lock = threading.Lock()
        # Track created marker files for cleanup
        self._created_files: set[Path] = set()
        # Track if we created the database file (for cleanup of test mocks)
        self._created_db = False
        # Track all connections across all threads for robust cleanup
        # Note: sqlite3.Connection doesn't support weak references, so we use a regular set
        self._all_connections: set[sqlite3.Connection] = set()
        self._connections_lock = threading.Lock()
        # For backward compatibility, expose _conn as a property
        # that returns the current thread's connection

    @property
    def _conn(self) -> sqlite3.Connection:
        """Backward compatibility property for accessing connection."""
        return self.get_connection()

    def get_connection(self) -> sqlite3.Connection:
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
            # Clear tracked connections from parent process
            with self._connections_lock:
                self._all_connections.clear()
            self._pid = current_pid

        # Check if this thread has a connection
        if not hasattr(self._thread_local, "conn"):
            # Check if database exists before creating connection
            db_existed = os.path.exists(self._db_path)

            # Create new connection for this thread with autocommit mode
            # This is crucial for proper transaction handling
            self._thread_local.conn = sqlite3.connect(
                self._db_path,
                isolation_level=None,
            )

            # Track the new connection for centralized cleanup
            with self._connections_lock:
                self._all_connections.add(self._thread_local.conn)

            # Track if we created the database (for test cleanup)
            if not db_existed and os.path.exists(self._db_path):
                self._created_db = True
                # Track the database file for cleanup if it looks like a mock path
                if "Mock" in self._db_path:
                    self._created_files.add(Path(self._db_path))

            # Apply per-connection settings
            self._apply_connection_settings(self._thread_local.conn)
        # Check if optimization phase was completed after connection was created
        elif SetupPhase.OPTIMIZATION in self._completed_phases and not hasattr(
            self._thread_local,
            "optimization_applied",
        ):
            # Apply optimization settings to existing connection
            self._apply_optimization_settings(self._thread_local.conn)
            self._thread_local.optimization_applied = True

        return cast("sqlite3.Connection", self._thread_local.conn)

    def _apply_connection_settings(
        self, conn: sqlite3.Connection, *, config: dict[str, Any] = _config
    ) -> None:
        """Apply per-connection settings that don't require exclusive locks."""
        db_backend.apply_connection_settings(
            conn,
            config=config,
            optimization_complete=SetupPhase.OPTIMIZATION in self._completed_phases,
        )
        if SetupPhase.OPTIMIZATION in self._completed_phases:
            self._thread_local.optimization_applied = True

    def _setup_connection_phase(self) -> None:
        """Setup critical connection settings including WAL mode."""
        _execute_with_retry(
            lambda: db_backend.setup_connection_phase(self._db_path, config=_config),
            max_retries=30,
            retry_delay=0.1,
        )

    def _setup_optimization_phase(self) -> None:
        """Setup performance optimizations."""
        # Apply optimization settings to the current thread's connection
        # This ensures they take effect immediately after setup
        if hasattr(self._thread_local, "conn"):
            self._apply_optimization_settings(self._thread_local.conn)
            self._thread_local.optimization_applied = True

    def _apply_optimization_settings(
        self, conn: sqlite3.Connection, *, config: dict[str, Any] = _config
    ) -> None:
        """Apply optimization settings to a connection."""
        db_backend.apply_optimization_settings(conn, config=config)

    def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Iterable[tuple[Any, ...]]:
        """Execute SQL and optionally return rows."""
        try:
            conn = self.get_connection()
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
            conn = self.get_connection()
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
            conn = self.get_connection()
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
            conn = self.get_connection()
            conn.rollback()
        except sqlite3.OperationalError as e:
            raise OperationalError(str(e)) from e
        except sqlite3.IntegrityError as e:
            raise IntegrityError(str(e)) from e
        except sqlite3.DataError as e:
            raise DataError(str(e)) from e

    def close(self) -> None:
        """Close all connections created by this runner and release resources."""
        # Close ALL connections created by this runner instance across all threads
        # This is critical for preventing resource leaks and file locking issues on Windows
        with self._connections_lock:
            for conn in self._all_connections:
                try:
                    conn.close()
                except Exception:
                    pass  # Ignore errors during cleanup
            self._all_connections.clear()

        # Also clean up the current thread's local storage for good hygiene
        if hasattr(self._thread_local, "conn"):
            with contextlib.suppress(Exception):
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
        self.run_exclusive_setup(
            phase, lambda: self._execute_builtin_setup_phase(phase)
        )

    def run_exclusive_setup(
        self,
        phase: SetupPhase,
        operation: Callable[[], None],
    ) -> bool:
        """Run a setup operation once under the phase's cross-process lock.

        Returns True when this runner executed *operation*, or False when the
        phase had already been completed by this or another process.
        """
        if phase == SetupPhase.CONNECTION and not self._database_file_exists():
            self._discard_stale_completion_markers()

        # Quick check without lock
        if phase in self._completed_phases:
            return False

        # Fast-path: check if another process already completed this phase
        if self._is_phase_already_completed(phase):
            with self._setup_lock:
                self._completed_phases.add(phase)
            return False

        # Get lock path for this phase
        lock_path = self._get_lock_path(phase)
        if lock_path is None:
            return False  # Invalid path, skip setup

        # Acquire lock with timeout
        lock_file = self._acquire_lock_with_timeout(lock_path, timeout=10.0)

        try:
            return self._execute_setup_under_lock(phase, operation)
        finally:
            # Release lock
            self._release_lock(lock_file, lock_path)

    def _get_lock_path(self, phase: SetupPhase) -> Path | None:
        """Get lock file path for the given phase."""
        try:
            lock_path = Path(self._db_path).with_suffix(f".{phase.value}.lock")
            # Ensure parent directory exists
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            # Track for cleanup
            self._created_files.add(lock_path)
            return lock_path
        except (ValueError, OSError, TypeError):
            # Handle invalid paths (e.g., from mocked tests)
            return None

    def _acquire_lock_with_timeout(
        self,
        lock_path: Path,
        timeout: float,
    ) -> Any | None:
        """Acquire file lock with platform-specific method and timeout."""
        start_time = time.monotonic()
        lock_file = None

        while time.monotonic() - start_time < timeout:
            lock_file = self._try_acquire_lock(lock_path)
            if lock_file is not None:
                return lock_file

            # Note: Using time.sleep here instead of interruptible_sleep because:
            # 1. This is low-level database setup code without a stop event
            # 2. The wait is very short (50ms) for file lock acquisition
            # 3. This runs during initialization, not in long-running threads
            time.sleep(0.05)

        msg = f"Timeout waiting for setup lock: {lock_path.name}"
        raise OperationalError(msg)

    def _try_acquire_lock(self, lock_path: Path) -> Any | None:
        """Try to acquire lock once using appropriate platform method."""
        # Try Unix fcntl first
        if HAS_FCNTL:
            lock_file = self._try_fcntl_lock(lock_path)
            if lock_file is not None:
                return lock_file

        # Try Windows msvcrt
        if HAS_MSVCRT:
            lock_file = self._try_msvcrt_lock(lock_path)
            if lock_file is not None:
                return lock_file

        # Fallback to exclusive file creation
        return self._try_exclusive_create_lock(lock_path)

    def _try_fcntl_lock(self, lock_path: Path) -> Any | None:
        """Try to acquire lock using fcntl (Unix/Linux/macOS)."""
        if not HAS_FCNTL:
            return None

        try:
            lock_file = open(lock_path, "w")
            try:
                os.chmod(lock_path, 0o600)
            except OSError:
                pass  # Don't fail on permission issues
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return lock_file
        except OSError:
            if "lock_file" in locals():
                lock_file.close()
            return None

    def _try_msvcrt_lock(self, lock_path: Path) -> Any | None:
        """Try to acquire lock using msvcrt (Windows)."""
        if not HAS_MSVCRT:
            return None

        try:
            lock_file = open(lock_path, "a+b")
            lock_file.seek(0)
            try:
                os.chmod(lock_path, 0o600)
            except OSError:
                pass  # Windows may not support chmod
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]
            return lock_file
        except OSError:
            if "lock_file" in locals():
                lock_file.close()
            return None

    def _try_exclusive_create_lock(self, lock_path: Path) -> Any | None:
        """Fallback lock using exclusive file creation (less robust)."""
        if lock_path.exists():
            # Check if lock is stale (older than 10 seconds)
            try:
                if time.time() - lock_path.stat().st_mtime > 10.0:
                    lock_path.unlink()  # Remove stale lock
            except OSError:
                pass  # Another process might have removed it
            return None

        try:
            # WARNING: Race condition exists here between check and creation
            lock_file = open(lock_path, "x")
            try:
                os.chmod(lock_path, 0o600)
            except OSError:
                pass  # Some systems may not support chmod
            return lock_file
        except FileExistsError:
            return None

    def _execute_setup_under_lock(
        self,
        phase: SetupPhase,
        operation: Callable[[], None],
    ) -> bool:
        """Execute the setup phase with thread synchronization."""
        with self._setup_lock:
            if phase in self._completed_phases:
                return False

            # Check if another process already completed this phase
            if self._is_phase_already_completed(phase):
                self._completed_phases.add(phase)
                return False

            operation()

            # Mark as complete
            self._mark_phase_complete(phase)
            return True

    def _execute_builtin_setup_phase(self, phase: SetupPhase) -> None:
        """Execute a built-in SQLite setup phase."""
        if phase == SetupPhase.CONNECTION:
            self._setup_connection_phase()
        elif phase == SetupPhase.OPTIMIZATION:
            self._setup_optimization_phase()

    def _is_phase_already_completed(self, phase: SetupPhase) -> bool:
        """Check if another process already completed this phase."""
        try:
            if not self._database_file_exists():
                return False
            marker_path = self._get_marker_path(phase)
            return marker_path.exists()
        except (ValueError, OSError, TypeError):
            return False

    def _mark_phase_complete(self, phase: SetupPhase) -> None:
        """Mark a phase as complete by creating a marker file."""
        self._completed_phases.add(phase)
        try:
            marker_path = self._get_marker_path(phase)
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

    def _get_marker_path(self, phase: SetupPhase) -> Path:
        """Get marker file path for the given setup phase."""
        suffix = (
            f".schema-v{SCHEMA_VERSION}.done"
            if phase == SetupPhase.SCHEMA
            else f".{phase.value}.done"
        )
        return Path(self._db_path).with_suffix(suffix)

    def _database_file_exists(self) -> bool:
        """Return whether the database file currently exists."""
        try:
            return Path(self._db_path).exists()
        except (ValueError, OSError, TypeError):
            return False

    def _discard_stale_completion_markers(self) -> None:
        """Remove completion markers left behind after database deletion."""
        try:
            db_path = Path(self._db_path)
            marker_paths = [
                db_path.with_suffix(".connection.done"),
                db_path.with_suffix(".optimization.done"),
            ]
            marker_paths.extend(db_path.parent.glob(f"{db_path.stem}.schema-v*.done"))
            for marker_path in marker_paths:
                with contextlib.suppress(OSError):
                    marker_path.unlink()
        except (ValueError, OSError, TypeError):
            pass

    def _release_lock(self, lock_file: Any | None, lock_path: Path) -> None:
        """Release the lock file using appropriate method."""
        if lock_file is None:
            return

        # Try fcntl unlock
        if HAS_FCNTL:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            except (OSError, AttributeError):
                pass

        # Try msvcrt unlock
        if HAS_MSVCRT:
            try:
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
            except (OSError, AttributeError):
                pass

        # Close the file
        with contextlib.suppress(Exception):
            lock_file.close()

        # Only unlink lock file if we're not using msvcrt
        # (msvcrt needs the file to exist for other processes to lock)
        if not HAS_MSVCRT:
            with contextlib.suppress(OSError):
                lock_path.unlink()

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
            marker_path = self._get_marker_path(phase)
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

        Shared setup coordination files must outlive any one BrokerDB/Queue
        handle. Weft and other callers create many short-lived handles against
        the same database; if one handle unlinks a shared ``.lock`` or
        ``.done`` sidecar while another process is still using it, the next
        opener can bypass the intended cross-process serialization. Keep those
        files for real databases, but preserve the mock-path cleanup behavior
        that older tests rely on.
        """
        for file_path in self._created_files:
            try:
                if not self._should_cleanup_tracked_file(file_path):
                    continue
                if file_path.exists():
                    file_path.unlink()
            except (OSError, ValueError, TypeError):
                # Ignore errors during cleanup
                pass
        self._created_files.clear()

    def _should_cleanup_tracked_file(self, file_path: Path) -> bool:
        """Return whether this runner should unlink a tracked file on cleanup."""
        # Mock-path tests intentionally exercise path cleanup using synthetic
        # database names. Preserve that behavior for those scenarios.
        if "Mock" in self._db_path:
            return True

        shared_sidecar_suffixes = {
            ".connection.lock",
            ".optimization.lock",
            ".schema.lock",
            ".connection.done",
            ".optimization.done",
        }
        suffix_key = "".join(file_path.suffixes[-2:])
        if suffix_key in shared_sidecar_suffixes:
            return False
        if suffix_key.startswith(".schema-v") and suffix_key.endswith(".done"):
            return False
        return True

    def __enter__(self) -> Self:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        """Exit context manager - cleanup marker files."""
        self.cleanup_marker_files()
        self.close()
        return False


# ~
