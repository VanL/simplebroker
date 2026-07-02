"""SQL execution abstraction for SimpleBroker extensions.

This module provides the SQLRunner protocol and default SQLiteRunner implementation
that enables SimpleBroker to be extended with custom backends while maintaining
its core philosophy and performance characteristics.
"""

from __future__ import annotations

import contextlib
import itertools
import logging
import os
import sqlite3
import threading
from collections.abc import Callable, Iterable, Iterator, Mapping
from enum import Enum
from pathlib import Path
from typing import Any, Literal, Protocol, Self, cast

from ._backends import get_configured_backend
from ._constants import SCHEMA_VERSION, ConnectionPhase, load_config, resolve_config
from ._exceptions import DatabaseError, DataError, IntegrityError, OperationalError
from ._phaselock import Phase, PhaseLockService, PhaseLockTimeout, PhaseLockUnavailable
from .helpers import (
    SETUP_PHASE_LOCK_TIMEOUT,
    execute_setup_with_retry,
    setup_busy_timeout_ms,
)

# Load config once at module level
_config = load_config()
db_backend = get_configured_backend(_config)
logger = logging.getLogger(__name__)


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
    - Must raise OperationalError on locking for retry logic; backends
      whose messages do not contain SQLite's lock/busy phrases must set
      OperationalError.retryable = True on contention errors (see
      simplebroker._exceptions.OperationalError)
    - Must be fork-safe (recreate connections after os.fork())
    - Must handle connection lifecycle (open/close)

    Optional hooks (probed via getattr, no-op if absent):
    - run_exclusive_setup(phase, operation) -- serialize cross-process
      schema setup (probed in db.py BrokerCore setup, called as
      run_exclusive_setup(SetupPhase.SCHEMA, operation))
    - shutdown() -- release process-wide resources beyond close()
      (probed in _runner connection-lifecycle helpers and db.py
      DBConnection teardown)
    - lease_thread_connection() / release_thread_connection() -- explicit
      thread-connection leasing for process-shared runners (probed in
      _runner lifecycle helpers and db.py BrokerCore.close)
    - cleanup_marker_files() -- remove on-disk setup markers
      (probed in db.py BrokerCore.close paths)
    - _setup_operation_context / _db_path -- setup-phase context manager
      and diagnostic path attribute (probed in db.py setup coordination)
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


def close_owned_runner(runner: SQLRunner) -> None:
    """Close an owned runner, preferring full shutdown when supported."""

    shutdown = getattr(runner, "shutdown", None)
    if callable(shutdown):
        shutdown()
        return
    runner.close()


def lease_runner_thread_connection(runner: SQLRunner) -> bool:
    """Ask a runner to retain this thread's backend checkout when supported."""

    lease_thread_connection = getattr(runner, "lease_thread_connection", None)
    if callable(lease_thread_connection):
        lease_thread_connection()
        return True
    return False


def release_runner_thread_connection(runner: SQLRunner) -> None:
    """Release this thread's retained backend checkout when supported."""

    release_thread_connection = getattr(runner, "release_thread_connection", None)
    if callable(release_thread_connection):
        release_thread_connection()


class SQLiteRunner:
    """Default synchronous SQLite implementation with thread-local connections."""

    _instance_counter = itertools.count()  # Unique instance ID for debugging

    def __init__(
        self, db_path: str, *, config: Mapping[str, Any] | None = None
    ) -> None:
        self.instance_id = next(self._instance_counter)
        self._db_path = db_path
        self._config = resolve_config(config)
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
        self._operation_lock = threading.RLock()
        self._connection_generation = 0
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
                self._connection_generation += 1
            self._pid = current_pid
            self._recover_completed_phases_from_markers()

        if hasattr(self._thread_local, "conn"):
            conn_generation = getattr(self._thread_local, "conn_generation", None)
            if conn_generation is None:
                self._thread_local.conn_generation = self._connection_generation
            elif conn_generation != self._connection_generation:
                with contextlib.suppress(AttributeError):
                    delattr(self._thread_local, "conn")
                with contextlib.suppress(AttributeError):
                    delattr(self._thread_local, "conn_generation")

        # Check if this thread has a connection
        if not hasattr(self._thread_local, "conn"):
            # Check if database exists before creating connection
            db_existed = os.path.exists(self._db_path)
            if getattr(self._thread_local, "setup_busy_timeout", False):
                connect_timeout_ms = setup_busy_timeout_ms(self._config)
            else:
                connect_timeout_ms = int(self._config["BROKER_BUSY_TIMEOUT"])

            # Create new connection for this thread with autocommit mode
            # This is crucial for proper transaction handling
            self._thread_local.conn = sqlite3.connect(
                self._db_path,
                isolation_level=None,
                timeout=max(0, connect_timeout_ms) / 1000,
                # SQLiteRunner owns these internal connections. Disable
                # sqlite's same-thread restriction so session cleanup can close
                # worker-thread connections after queue operations return.
                check_same_thread=False,
            )

            # Track the new connection for centralized cleanup
            with self._connections_lock:
                self._all_connections.add(self._thread_local.conn)
                self._thread_local.conn_generation = self._connection_generation

            # Track if we created the database (for test cleanup)
            if not db_existed and os.path.exists(self._db_path):
                self._created_db = True
                # Track the database file for cleanup if it looks like a mock path
                if "Mock" in self._db_path:
                    self._created_files.add(Path(self._db_path))

            # Apply per-connection settings
            self._apply_connection_settings(self._thread_local.conn)
            if getattr(self._thread_local, "setup_busy_timeout", False):
                self._apply_busy_timeout(
                    self._thread_local.conn,
                    setup_busy_timeout_ms(self._config),
                )
        # Check if optimization phase was completed after connection was created
        elif SetupPhase.OPTIMIZATION in self._completed_phases and not hasattr(
            self._thread_local,
            "optimization_applied",
        ):
            # Apply optimization settings to existing connection
            self._apply_optimization_settings(self._thread_local.conn)
            self._thread_local.optimization_applied = True

        return cast("sqlite3.Connection", self._thread_local.conn)

    def _apply_connection_settings(self, conn: sqlite3.Connection) -> None:
        """Apply per-connection settings that don't require exclusive locks."""
        db_backend.apply_connection_settings(
            conn,
            config=self._config,
            optimization_complete=SetupPhase.OPTIMIZATION in self._completed_phases,
        )
        if SetupPhase.OPTIMIZATION in self._completed_phases:
            self._thread_local.optimization_applied = True

    def _recover_completed_phases_from_markers(self) -> None:
        """Refresh process-local setup phase state from durable markers."""
        for phase in SetupPhase:
            self.is_setup_complete(phase)

    def _setup_connection_phase(self) -> None:
        """Setup critical connection settings including WAL mode."""
        execute_setup_with_retry(
            lambda: db_backend.setup_connection_phase(
                self._db_path,
                config=self._config,
                busy_timeout_ms=setup_busy_timeout_ms(self._config),
            ),
            phase=str(SetupPhase.CONNECTION.value),
            target=self._db_path,
        )

    def _setup_optimization_phase(self) -> None:
        """Setup performance optimizations."""
        # Apply optimization settings to the current thread's connection
        # This ensures they take effect immediately after setup
        if hasattr(self._thread_local, "conn"):
            self._apply_optimization_settings(self._thread_local.conn)
            self._thread_local.optimization_applied = True

    def _apply_optimization_settings(self, conn: sqlite3.Connection) -> None:
        """Apply optimization settings to a connection."""
        db_backend.apply_optimization_settings(conn, config=self._config)

    @contextlib.contextmanager
    def _setup_operation_context(self) -> Iterator[None]:
        """Temporarily use the shorter setup busy timeout on this thread."""

        previous = getattr(self._thread_local, "setup_busy_timeout", False)
        self._thread_local.setup_busy_timeout = True
        if hasattr(self._thread_local, "conn"):
            self._apply_busy_timeout(
                self._thread_local.conn,
                setup_busy_timeout_ms(self._config),
            )
        try:
            yield
        finally:
            if previous:
                self._thread_local.setup_busy_timeout = previous
            else:
                with contextlib.suppress(AttributeError):
                    del self._thread_local.setup_busy_timeout
            if hasattr(self._thread_local, "conn"):
                self._apply_busy_timeout(
                    self._thread_local.conn,
                    int(self._config["BROKER_BUSY_TIMEOUT"]),
                )

    def _apply_busy_timeout(self, conn: sqlite3.Connection, timeout_ms: int) -> None:
        """Apply only the SQLite busy timeout to an existing connection."""

        cursor = conn.execute(f"PRAGMA busy_timeout={timeout_ms}")
        cursor.close()

    def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Iterable[tuple[Any, ...]]:
        """Execute SQL and optionally return rows."""
        with self._operation_lock:
            try:
                conn = self.get_connection()
                cursor = conn.execute(sql, params)
                try:
                    # Only fetch if explicitly requested
                    if fetch:
                        return cursor.fetchall()
                    return []
                finally:
                    cursor.close()
            except sqlite3.OperationalError as e:
                raise OperationalError(str(e)) from e
            except sqlite3.IntegrityError as e:
                raise IntegrityError(str(e)) from e
            except sqlite3.DataError as e:
                raise DataError(str(e)) from e
            except sqlite3.DatabaseError as e:
                raise DatabaseError(str(e)) from e

    def begin_immediate(self) -> None:
        """Start an immediate transaction."""
        with self._operation_lock:
            try:
                conn = self.get_connection()
                cursor = conn.execute("BEGIN IMMEDIATE")
                cursor.close()
            except sqlite3.OperationalError as e:
                raise OperationalError(str(e)) from e
            except sqlite3.IntegrityError as e:
                raise IntegrityError(str(e)) from e
            except sqlite3.DataError as e:
                raise DataError(str(e)) from e
            except sqlite3.DatabaseError as e:
                raise DatabaseError(str(e)) from e

    def commit(self) -> None:
        """Commit the current transaction."""
        with self._operation_lock:
            try:
                conn = self.get_connection()
                conn.commit()
            except sqlite3.OperationalError as e:
                raise OperationalError(str(e)) from e
            except sqlite3.IntegrityError as e:
                raise IntegrityError(str(e)) from e
            except sqlite3.DataError as e:
                raise DataError(str(e)) from e
            except sqlite3.DatabaseError as e:
                raise DatabaseError(str(e)) from e

    def rollback(self) -> None:
        """Rollback the current transaction."""
        with self._operation_lock:
            try:
                conn = self.get_connection()
                conn.rollback()
            except sqlite3.OperationalError as e:
                raise OperationalError(str(e)) from e
            except sqlite3.IntegrityError as e:
                raise IntegrityError(str(e)) from e
            except sqlite3.DataError as e:
                raise DataError(str(e)) from e
            except sqlite3.DatabaseError as e:
                raise DatabaseError(str(e)) from e

    def close(self) -> None:
        """Close all connections created by this runner and release resources."""
        with self._operation_lock:
            # Close ALL connections created by this runner instance across all threads.
            # Keep failed closes tracked so cleanup does not drop the last reference.
            with self._connections_lock:
                self._connection_generation += 1
                connections = list(self._all_connections)

            closed_connections = []
            for conn in connections:
                if self._close_tracked_connection(conn):
                    closed_connections.append(conn)

            with self._connections_lock:
                for conn in closed_connections:
                    self._all_connections.discard(conn)

            # Also clean up the current thread's local storage for good hygiene
            if hasattr(self._thread_local, "conn"):
                with contextlib.suppress(Exception):
                    delattr(self._thread_local, "conn")
            if hasattr(self._thread_local, "conn_generation"):
                with contextlib.suppress(Exception):
                    delattr(self._thread_local, "conn_generation")

    def _close_tracked_connection(self, conn: sqlite3.Connection) -> bool:
        """Close one tracked connection, returning whether close succeeded."""
        self._prepare_connection_for_close(conn)
        try:
            conn.close()
        except Exception as exc:
            if self._config["BROKER_LOGGING_ENABLED"]:
                logger.warning("Error closing SQLite connection: %s", exc)
            return False
        return True

    def _prepare_connection_for_close(self, conn: sqlite3.Connection) -> None:
        """Best-effort cleanup so connection close cannot inherit long busy waits."""

        with contextlib.suppress(Exception):
            conn.interrupt()
        with contextlib.suppress(Exception):
            cursor = conn.execute("PRAGMA busy_timeout=0")
            cursor.close()
        with contextlib.suppress(Exception):
            conn.rollback()

    def setup(self, phase: SetupPhase) -> None:
        """Run specific setup phase in an idempotent manner.

        Args:
            phase: The setup phase to execute

        Setup coordination is delegated to PhaseLockService. That keeps all
        completion markers and advisory lock behavior on one code path.

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
        service = self._phase_lock_service()
        phase_name = self._phase_marker_name(phase)
        if phase in self._completed_phases and not service.strict_marker_locking:
            return False

        if phase == SetupPhase.CONNECTION and self._target_needs_fresh_setup_markers():
            self._discard_stale_completion_markers()
        elif not service.strict_marker_locking and self._has_valid_completion_marker(
            service, phase, phase_name
        ):
            with self._setup_lock:
                self._completed_phases.add(phase)
            return False

        ran = False

        def guarded_operation() -> None:
            nonlocal ran
            with self._setup_lock:
                operation()
                ran = True

        try:
            result = service.run_phases((Phase(phase_name, guarded_operation),))
        except (PhaseLockTimeout, PhaseLockUnavailable) as exc:
            raise OperationalError(str(exc)) from exc

        self._created_files.add(result.lock_path)
        self._created_files.update(result.status_paths)

        if phase_name in result.completed or phase_name in result.skipped:
            with self._setup_lock:
                self._completed_phases.add(phase)

        return ran

    def _phase_lock_service(self) -> PhaseLockService:
        return PhaseLockService(
            self._db_path,
            namespace="user.simplebroker",
            lock_suffix=".lock",
            status_suffix=".status",
            timeout=SETUP_PHASE_LOCK_TIMEOUT,
            retry_delay=0.05,
            # SQLite setup markers must be a happens-after barrier, not just
            # completion hints. Waiters should observe a completed phase only
            # after the prior setup owner has released the advisory lock.
            strict_marker_locking=True,
        )

    def _phase_marker_name(self, phase: SetupPhase) -> str:
        if phase == SetupPhase.SCHEMA:
            return f"schema-v{SCHEMA_VERSION}"
        return str(phase.value)

    def _execute_builtin_setup_phase(self, phase: SetupPhase) -> None:
        """Execute a built-in SQLite setup phase."""
        if phase == SetupPhase.CONNECTION:
            self._setup_connection_phase()
        elif phase == SetupPhase.OPTIMIZATION:
            self._setup_optimization_phase()

    def _target_needs_fresh_setup_markers(self) -> bool:
        """Return whether fallback completion markers cannot describe target state."""

        try:
            db_path = Path(self._db_path)
            if not db_path.exists():
                return True
            # SQLite database headers are 16 bytes. Smaller non-empty files
            # cannot be initialized databases, so stale markers cannot be trusted.
            return db_path.stat().st_size < 16
        except (ValueError, OSError, TypeError):
            return False

    def _has_valid_completion_marker(
        self,
        service: PhaseLockService,
        phase: SetupPhase,
        phase_name: str,
    ) -> bool:
        """Return whether an existing marker can be trusted for this phase."""

        if phase != SetupPhase.CONNECTION:
            return service.has_phase(phase_name)

        if self._target_needs_fresh_setup_markers():
            self._discard_stale_completion_markers()
            return False

        if not service.has_phase(phase_name):
            return False

        return True

    def _discard_stale_completion_markers(self) -> None:
        """Remove fallback completion markers left behind after database deletion."""
        with contextlib.suppress(ValueError, OSError, TypeError):
            self._phase_lock_service().discard_status_markers()

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        """Check if a setup phase has been completed.

        Args:
            phase: The setup phase to check

        Returns:
            True if the phase has been completed

        """
        if phase in self._completed_phases:
            return True

        with contextlib.suppress(ValueError, OSError, TypeError):
            service = self._phase_lock_service()
            phase_name = self._phase_marker_name(phase)
            if self._has_valid_completion_marker(service, phase, phase_name):
                with self._setup_lock:
                    self._completed_phases.add(phase)
                return True

        return False

    def cleanup_marker_files(self) -> None:
        """Clean up any marker files created during setup.

        Shared setup coordination files must outlive any one BrokerDB/Queue
        handle. Weft and other callers create many short-lived handles against
        the same database; if one handle unlinks a shared setup lock or status
        file while another process is still using it, the next opener can
        bypass the intended cross-process serialization. Keep those files for
        real databases, but preserve the mock-path cleanup behavior that older
        tests rely on.
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

        with contextlib.suppress(ValueError, OSError, TypeError):
            service = self._phase_lock_service()
            if file_path == service.lock_path:
                return False
            if file_path == service.status_base_path:
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
