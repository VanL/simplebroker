"""Generic SQLite connection and management utilities.

This module provides robust SQLite connection management, path validation, and database
utilities extracted from SimpleBroker. It can be used as a standalone library in any
project requiring secure, high-performance SQLite operations.

Key Features:
- Thread-local connection management with fork safety
- Cross-platform file locking (Unix fcntl, Windows msvcrt)
- Comprehensive path security validation
- Database retry logic for handling contention
- WAL mode setup and SQLite optimizations
- Environment-based configuration

Usage:
    from sqlite_connect import SQLiteConnectionManager, validate_database_path

    # Create connection manager
    manager = SQLiteConnectionManager("database.db")

    # Get optimized connection
    conn = manager.get_connection()

    # Use with context manager
    with SQLiteConnectionManager("database.db") as manager:
        conn = manager.get_connection()
        cursor = conn.execute("SELECT 1")

Security:
    All paths are validated for safety against traversal attacks, shell injection,
    and platform-specific reserved names. Connections use secure defaults.
"""

from __future__ import annotations

import contextlib
import itertools
import os
import platform
import re
import sqlite3
import threading
import time
import warnings
from enum import Enum
from pathlib import Path, PurePath
from typing import Any, Callable, Dict, Optional, TypeVar, cast

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

# Type variables
T = TypeVar("T")

# ==============================================================================
# EXCEPTIONS
# ==============================================================================


class SQLiteConnectError(Exception):
    """Base exception for sqlite_connect errors."""

    pass


class DatabaseError(SQLiteConnectError):
    """Database validation or access errors."""

    pass


class OperationalError(SQLiteConnectError):
    """Database operational errors."""

    pass


class StopException(SQLiteConnectError):
    """Exception raised when operations are interrupted."""

    pass


# ==============================================================================
# CONSTANTS AND CONFIGURATION
# ==============================================================================

# Path security validation constants
_COMMON_DANGEROUS_CHARS = [
    "\0",  # Null byte - can truncate paths
    "\r",
    "\n",  # Line endings - can cause injection
    "\t",  # Tab - can cause parsing issues
    "\x7f",  # DEL character
]

_UNIX_SHELL_CHARS = [
    "|",
    "&",
    ";",
    "$",
    "`",
    '"',
    "'",
    "<",
    ">",
    "(",
    ")",
    "{",
    "}",
    "[",
    "]",
    "*",
    "?",
    "~",
    "^",
    "!",
    "#",
]

_WINDOWS_DANGEROUS_CHARS = [
    ":",
    "*",
    "?",
    '"',
    "<",
    ">",
    "|",
]

# Platform-specific character lists
_unix_chars = _COMMON_DANGEROUS_CHARS + _UNIX_SHELL_CHARS
_windows_chars = _COMMON_DANGEROUS_CHARS + _WINDOWS_DANGEROUS_CHARS

# Pre-compile regex patterns for performance
_UNIX_DANGEROUS_REGEX = re.compile(f"[{re.escape(''.join(_unix_chars))}]")
_WINDOWS_DANGEROUS_REGEX = re.compile(f"[{re.escape(''.join(_windows_chars))}]")

# Windows reserved names
_WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}


class SetupPhase(Enum):
    """Database setup phases."""

    CONNECTION = "connection"
    OPTIMIZATION = "optimization"


# Default configuration
DEFAULT_CONFIG = {
    "BUSY_TIMEOUT": 5000,  # SQLite busy timeout in milliseconds
    "CACHE_MB": 10,  # SQLite cache size in MB
    "SYNC_MODE": "FULL",  # Synchronous mode: FULL/NORMAL/OFF
    "WAL_AUTOCHECKPOINT": 1000,  # WAL checkpoint threshold in pages
    "MAX_RETRIES": 10,  # Max retries for locked database
    "RETRY_DELAY": 0.05,  # Initial retry delay in seconds
}

# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================


def _parse_bool(value: str) -> bool:
    """Parse string to boolean."""
    if not value:
        return False
    return value.lower().strip() in ("1", "true", "yes", "on")


def load_sqlite_config() -> Dict[str, Any]:
    """Load SQLite configuration from environment variables with defaults."""
    return {
        "BUSY_TIMEOUT": int(
            os.environ.get("SQLITE_BUSY_TIMEOUT", str(DEFAULT_CONFIG["BUSY_TIMEOUT"]))
        ),
        "CACHE_MB": int(
            os.environ.get("SQLITE_CACHE_MB", str(DEFAULT_CONFIG["CACHE_MB"]))
        ),
        "SYNC_MODE": str(os.environ.get(
            "SQLITE_SYNC_MODE", DEFAULT_CONFIG["SYNC_MODE"]
        )).upper(),
        "WAL_AUTOCHECKPOINT": int(
            os.environ.get(
                "SQLITE_WAL_AUTOCHECKPOINT", str(DEFAULT_CONFIG["WAL_AUTOCHECKPOINT"])
            )
        ),
        "MAX_RETRIES": int(
            os.environ.get("SQLITE_MAX_RETRIES", str(DEFAULT_CONFIG["MAX_RETRIES"]))
        ),
        "RETRY_DELAY": float(
            os.environ.get("SQLITE_RETRY_DELAY", str(DEFAULT_CONFIG["RETRY_DELAY"]))
        ),
    }


def interruptible_sleep(
    seconds: float,
    stop_event: Optional[threading.Event] = None,
    chunk_size: float = 0.1,
) -> bool:
    """Sleep for specified duration, can be interrupted by stop event.

    Args:
        seconds: Number of seconds to sleep
        stop_event: Optional threading.Event that can interrupt the sleep
        chunk_size: Maximum duration of each sleep chunk

    Returns:
        True if sleep completed, False if interrupted
    """
    if seconds <= 0:
        return True

    event = stop_event or threading.Event()

    if seconds <= chunk_size:
        return not event.wait(timeout=seconds)

    start_time = time.perf_counter()
    target_end_time = start_time + seconds

    while time.perf_counter() < target_end_time:
        remaining = target_end_time - time.perf_counter()
        if remaining <= 0:
            break

        if event.wait(timeout=min(chunk_size, remaining)):
            return stop_event is None or not stop_event.is_set()

    return True


def execute_with_retry(
    operation: Callable[[], T],
    *,
    max_retries: int = 10,
    retry_delay: float = 0.05,
    stop_event: Optional[threading.Event] = None,
) -> T:
    """Execute database operation with retry logic for locked database errors.

    Args:
        operation: Callable that performs the database operation
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries (exponential backoff applied)
        stop_event: Optional threading.Event to interrupt retry loop

    Returns:
        Result of the operation

    Raises:
        The last exception if all retries fail
    """
    locked_markers = (
        "database is locked",
        "database table is locked",
        "database schema is locked",
        "database is busy",
        "database busy",
    )

    for attempt in range(max_retries):
        try:
            return operation()
        except (sqlite3.OperationalError, OperationalError) as e:
            msg = str(e).lower()
            if any(marker in msg for marker in locked_markers):
                if attempt < max_retries - 1:
                    # Exponential backoff + jitter
                    jitter = (time.time() * 1000) % 25 / 1000  # 0-25ms jitter
                    wait = retry_delay * (2**attempt) + jitter
                    if not interruptible_sleep(wait, stop_event):
                        raise StopException("Retry interrupted by stop event") from None
                    continue
            raise

    raise AssertionError("Unreachable code")


def validate_safe_path_components(path: str, context: str = "path") -> None:
    """Validate path components don't contain dangerous characters or reserved names.

    Args:
        path: Path string to validate
        context: Description for error messages

    Raises:
        ValueError: If path contains dangerous characters or reserved names

    Security Features:
        - Prevents path traversal attacks (..)
        - Blocks null bytes and control characters
        - Prevents shell injection on Unix/Mac
        - Blocks Windows reserved names
        - Validates each path component separately
    """
    if not isinstance(path, str) or not path:
        raise ValueError(f"{context} must be a non-empty string")

    normalized_path = path.replace("\\", "/")
    pure_path = PurePath(normalized_path)

    # Platform-specific dangerous character check
    is_windows = platform.system() == "Windows"
    dangerous_regex = _WINDOWS_DANGEROUS_REGEX if is_windows else _UNIX_DANGEROUS_REGEX

    # Check for dangerous characters
    if dangerous_regex.search(path):
        if is_windows and ":" in path:
            # Check for legitimate Windows drive letter
            drive_pattern = re.compile(r"^[A-Za-z]:")
            if drive_pattern.match(path):
                path_without_drive = path[2:]
                if dangerous_regex.search(path_without_drive):
                    match = dangerous_regex.search(path_without_drive)
                    char = match.group() if match else "unknown"
                    raise ValueError(
                        f"{context} contains dangerous character '{char}': {path}"
                    )
            else:
                match = dangerous_regex.search(path)
                char = match.group() if match else "unknown"
                raise ValueError(
                    f"{context} contains dangerous character '{char}': {path}"
                )
        else:
            match = dangerous_regex.search(path)
            char = match.group() if match else "unknown"
            raise ValueError(f"{context} contains dangerous character '{char}': {path}")

    # Validate each path component
    for part in pure_path.parts:
        if not part:
            continue

        if part == "..":
            raise ValueError(
                f"{context} must not contain parent directory references: {path}"
            )

        if part == ".":
            raise ValueError(
                f"{context} must not contain current directory references: {path}"
            )

        # Windows reserved names
        if is_windows:
            name_without_ext = part.split(".")[0].upper()
            if name_without_ext in _WINDOWS_RESERVED_NAMES:
                raise ValueError(
                    f"{context} contains Windows reserved name '{part}': {path}"
                )

        # Check for leading/trailing spaces
        if part.startswith(" ") or part.endswith(" "):
            raise ValueError(
                f"{context} component cannot start or end with spaces: '{part}' in {path}"
            )

        # Check component length
        if len(part) > 255:
            raise ValueError(
                f"{context} component too long (max 255 chars): '{part[:50]}...' in {path}"
            )

    # Check for current directory patterns
    if (
        "/./" in normalized_path
        or normalized_path.startswith("./")
        or normalized_path == "."
    ):
        raise ValueError(
            f"{context} must not contain current directory references: {path}"
        )

    # Check total path length
    max_path_length = 260 if is_windows else 1024
    if len(path) > max_path_length:
        raise ValueError(
            f"{context} too long (max {max_path_length} chars): {len(path)} chars in {path[:50]}..."
        )


def validate_database_path(
    file_path: Path, check_magic: bool = False, magic_string: Optional[str] = None
) -> None:
    """Validate that a file is a valid SQLite database.

    Args:
        file_path: Path to validate
        check_magic: Whether to verify a magic string in the database
        magic_string: Magic string to check for (if check_magic is True)

    Raises:
        DatabaseError: If validation fails
    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    if not file_path.exists():
        raise DatabaseError(f"Database file does not exist: {file_path}")

    if not file_path.is_file():
        raise DatabaseError(f"Path exists but is not a regular file: {file_path}")

    # Check permissions
    if not os.access(file_path.parent, os.R_OK | os.W_OK):
        raise DatabaseError(f"Parent directory is not accessible: {file_path.parent}")

    if not os.access(file_path, os.R_OK | os.W_OK):
        raise DatabaseError(f"Database file is not readable/writable: {file_path}")

    # Check SQLite header
    try:
        with open(file_path, "rb") as f:
            header = f.read(16)
            if header != b"SQLite format 3\x00":
                raise DatabaseError(
                    f"File is not a valid SQLite database (invalid header): {file_path}"
                )
    except OSError as e:
        raise DatabaseError(f"Cannot read database file: {file_path} ({e})") from e

    # Check database integrity
    conn = None
    try:
        conn = sqlite3.connect(f"file:{file_path}?mode=ro", uri=True)
        cursor = conn.cursor()
        cursor.execute("PRAGMA schema_version")
        cursor.fetchone()

        if check_magic and magic_string:
            cursor.execute("SELECT value FROM meta WHERE key = 'magic'")
            magic_row = cursor.fetchone()
            if magic_row is None:
                raise DatabaseError(f"Database is missing metadata: {file_path}")
            if magic_row[0] != magic_string:
                raise DatabaseError(f"Database has incorrect magic string: {file_path}")

    except sqlite3.DatabaseError as e:
        raise DatabaseError(
            f"Database corruption or invalid format: {file_path} ({e})"
        ) from e
    except sqlite3.Error as e:
        raise DatabaseError(
            f"SQLite error while validating database: {file_path} ({e})"
        ) from e
    except OSError as e:
        raise DatabaseError(
            f"OS error while accessing database: {file_path} ({e})"
        ) from e
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def is_valid_sqlite_database(
    file_path: Path, check_magic: bool = False, magic_string: Optional[str] = None
) -> bool:
    """Check if file is a valid SQLite database.

    Args:
        file_path: Path to check
        check_magic: Whether to verify magic string
        magic_string: Magic string to verify

    Returns:
        True if valid SQLite database, False otherwise
    """
    try:
        validate_database_path(file_path, check_magic, magic_string)
        return True
    except DatabaseError:
        return False


# ==============================================================================
# CONNECTION MANAGER
# ==============================================================================


class SQLiteConnectionManager:
    """Thread-safe SQLite connection manager with optimization and security features.

    Features:
        - Thread-local connections for safety
        - WAL mode setup for better concurrency
        - Automatic performance optimizations
        - Fork safety detection
        - Cross-platform file locking for setup operations
        - Comprehensive error handling

    Usage:
        manager = SQLiteConnectionManager("database.db")
        conn = manager.get_connection()

        # Or with context manager
        with SQLiteConnectionManager("database.db") as manager:
            conn = manager.get_connection()
    """

    _instance_counter = itertools.count()

    def __init__(self, db_path: str, config: Optional[Dict[str, Any]] = None):
        """Initialize connection manager.

        Args:
            db_path: Path to SQLite database file
            config: Optional configuration dict (uses environment defaults if None)
        """
        self.instance_id = next(self._instance_counter)
        self._db_path = str(db_path)
        self._config = config or load_sqlite_config()
        self._thread_local = threading.local()
        self._pid = os.getpid()
        self._completed_phases: set[SetupPhase] = set()
        self._setup_lock = threading.Lock()
        self._created_files: set[Path] = set()
        self._all_connections: set[sqlite3.Connection] = set()
        self._connections_lock = threading.Lock()

        # Validate database path for security
        validate_safe_path_components(self._db_path, "Database path")

        # Ensure parent directory exists
        db_file = Path(self._db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)

    def get_connection(self) -> sqlite3.Connection:
        """Get or create thread-local SQLite connection.

        Returns:
            sqlite3.Connection: Thread-local connection with optimizations applied
        """
        # Check for fork
        current_pid = os.getpid()
        if current_pid != self._pid:
            self._handle_fork(current_pid)

        # Get or create thread-local connection
        if not hasattr(self._thread_local, "conn"):
            self._create_thread_connection()
        elif SetupPhase.OPTIMIZATION in self._completed_phases and not hasattr(
            self._thread_local, "optimization_applied"
        ):
            self._apply_optimization_settings(self._thread_local.conn)
            self._thread_local.optimization_applied = True

        return cast("sqlite3.Connection", self._thread_local.conn)

    def _handle_fork(self, current_pid: int) -> None:
        """Handle process fork by cleaning up inherited connections."""
        if hasattr(self._thread_local, "conn"):
            try:
                self._thread_local.conn.close()
            except Exception:
                pass

        self._thread_local = threading.local()

        with self._setup_lock:
            self._completed_phases.clear()

        with self._connections_lock:
            self._all_connections.clear()

        self._pid = current_pid

    def _create_thread_connection(self) -> None:
        """Create new thread-local connection."""
        conn = sqlite3.connect(self._db_path, isolation_level=None)

        with self._connections_lock:
            self._all_connections.add(conn)

        self._thread_local.conn = conn
        self._apply_connection_settings(conn)

    def _apply_connection_settings(self, conn: sqlite3.Connection) -> None:
        """Apply per-connection settings."""
        conn.execute(f"PRAGMA busy_timeout={self._config['BUSY_TIMEOUT']}")

        wal_autocheckpoint = self._config["WAL_AUTOCHECKPOINT"]
        if wal_autocheckpoint < 0:
            warnings.warn(
                f"Invalid WAL_AUTOCHECKPOINT '{wal_autocheckpoint}', using 1000",
                stacklevel=2,
            )
            wal_autocheckpoint = 1000
        conn.execute(f"PRAGMA wal_autocheckpoint={wal_autocheckpoint}")

        if SetupPhase.OPTIMIZATION in self._completed_phases:
            self._apply_optimization_settings(conn)
            self._thread_local.optimization_applied = True

    def _apply_optimization_settings(self, conn: sqlite3.Connection) -> None:
        """Apply optimization settings to connection."""
        cache_mb = self._config["CACHE_MB"]
        conn.execute(f"PRAGMA cache_size=-{cache_mb * 1024}")

        sync_mode = self._config["SYNC_MODE"]
        if sync_mode not in ("FULL", "NORMAL", "OFF"):
            warnings.warn(f"Invalid SYNC_MODE '{sync_mode}', using FULL", stacklevel=2)
            sync_mode = "FULL"
        conn.execute(f"PRAGMA synchronous={sync_mode}")

    def setup(self, phase: SetupPhase) -> None:
        """Run specific setup phase with file locking.

        Args:
            phase: Setup phase to execute
        """
        if phase in self._completed_phases:
            return

        lock_path = self._get_lock_path(phase)
        if not lock_path:
            return

        lock_file = self._acquire_lock_with_timeout(lock_path, timeout=10.0)

        try:
            self._execute_setup_under_lock(phase)
        finally:
            self._release_lock(lock_file, lock_path)

    def _get_lock_path(self, phase: SetupPhase) -> Optional[Path]:
        """Get lock file path for setup phase."""
        try:
            lock_path = Path(self._db_path).with_suffix(f".{phase.value}.lock")
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            self._created_files.add(lock_path)
            return lock_path
        except (ValueError, OSError, TypeError):
            return None

    def _acquire_lock_with_timeout(self, lock_path: Path, timeout: float) -> Any:
        """Acquire file lock with timeout."""
        start_time = time.monotonic()

        while time.monotonic() - start_time < timeout:
            lock_file = self._try_acquire_lock(lock_path)
            if lock_file is not None:
                return lock_file
            time.sleep(0.05)

        raise OperationalError(f"Timeout waiting for setup lock: {lock_path.name}")

    def _try_acquire_lock(self, lock_path: Path) -> Any:
        """Try to acquire lock using platform-appropriate method."""
        # Try Unix fcntl first
        if HAS_FCNTL:
            try:
                lock_file = open(lock_path, "w")
                try:
                    os.chmod(lock_path, 0o600)
                except OSError:
                    pass
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return lock_file
            except OSError:
                if "lock_file" in locals():
                    lock_file.close()

        # Try Windows msvcrt
        if HAS_MSVCRT:
            try:
                lock_file = open(lock_path, "a+b")  # type: ignore[assignment]
                lock_file.seek(0)
                try:
                    os.chmod(lock_path, 0o600)
                except OSError:
                    pass
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]
                return lock_file
            except OSError:
                if "lock_file" in locals():
                    lock_file.close()

        # Fallback to exclusive create
        if lock_path.exists():
            try:
                if time.time() - lock_path.stat().st_mtime > 10.0:
                    lock_path.unlink()  # Remove stale lock
            except OSError:
                pass
            return None

        try:
            lock_file = open(lock_path, "x")
            try:
                os.chmod(lock_path, 0o600)
            except OSError:
                pass
            return lock_file
        except FileExistsError:
            return None

    def _execute_setup_under_lock(self, phase: SetupPhase) -> None:
        """Execute setup phase with thread synchronization."""
        with self._setup_lock:
            if phase in self._completed_phases:
                return

            if self._is_phase_complete(phase):
                self._completed_phases.add(phase)
                return

            if phase == SetupPhase.CONNECTION:
                self._setup_connection_phase()
            elif phase == SetupPhase.OPTIMIZATION:
                self._setup_optimization_phase()

            self._mark_phase_complete(phase)

    def _setup_connection_phase(self) -> None:
        """Setup WAL mode and critical connection settings."""

        def do_setup() -> None:
            setup_conn = sqlite3.connect(self._db_path, isolation_level=None)
            try:
                setup_conn.execute("PRAGMA busy_timeout=10000")

                # Enable WAL mode if not already enabled
                cursor = setup_conn.execute("PRAGMA journal_mode")
                current_mode = cursor.fetchone()[0] if cursor else "delete"

                if current_mode.lower() != "wal":
                    cursor = setup_conn.execute("PRAGMA journal_mode=WAL")
                    if cursor:
                        result = cursor.fetchone()
                        if result and result[0].lower() != "wal":
                            raise RuntimeError(
                                f"Failed to enable WAL mode, got: {result}"
                            )
            finally:
                setup_conn.close()

        execute_with_retry(do_setup, max_retries=30, retry_delay=0.1)

    def _setup_optimization_phase(self) -> None:
        """Setup performance optimizations."""
        if hasattr(self._thread_local, "conn"):
            self._apply_optimization_settings(self._thread_local.conn)
            self._thread_local.optimization_applied = True

    def _is_phase_complete(self, phase: SetupPhase) -> bool:
        """Check if setup phase is already complete."""
        try:
            marker_path = Path(self._db_path).with_suffix(f".{phase.value}.done")
            return marker_path.exists()
        except (ValueError, OSError, TypeError):
            return False

    def _mark_phase_complete(self, phase: SetupPhase) -> None:
        """Mark setup phase as complete."""
        self._completed_phases.add(phase)
        try:
            marker_path = Path(self._db_path).with_suffix(f".{phase.value}.done")
            if not marker_path.exists():
                marker_path.touch(mode=0o600)
            else:
                marker_path.touch()
            self._created_files.add(marker_path)
        except (ValueError, OSError, TypeError):
            pass

    def _release_lock(self, lock_file: Any, lock_path: Path) -> None:
        """Release file lock."""
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

        with contextlib.suppress(Exception):
            lock_file.close()

        if not HAS_MSVCRT:
            with contextlib.suppress(OSError):
                lock_path.unlink()

    def setup_database(self) -> None:
        """Setup database with both connection and optimization phases."""
        self.setup(SetupPhase.CONNECTION)
        self.setup(SetupPhase.OPTIMIZATION)

    def close(self) -> None:
        """Close all connections and clean up resources."""
        with self._connections_lock:
            for conn in self._all_connections:
                try:
                    conn.close()
                except Exception:
                    pass
            self._all_connections.clear()

        if hasattr(self._thread_local, "conn"):
            with contextlib.suppress(Exception):
                delattr(self._thread_local, "conn")

        # Clean up marker files
        for file_path in self._created_files:
            try:
                if file_path.exists():
                    file_path.unlink()
            except (OSError, ValueError, TypeError):
                pass
        self._created_files.clear()

    def __enter__(self) -> SQLiteConnectionManager:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager."""
        self.close()

    def __del__(self) -> None:
        """Destructor cleanup."""
        try:
            self.close()
        except Exception:
            pass


# ==============================================================================
# CONVENIENCE FUNCTIONS
# ==============================================================================


def create_optimized_connection(
    db_path: str, config: Optional[Dict[str, Any]] = None
) -> sqlite3.Connection:
    """Create a single optimized SQLite connection.

    Args:
        db_path: Path to database file
        config: Optional configuration

    Returns:
        Optimized SQLite connection

    Note:
        This creates a connection without a manager context. For multi-threaded
        applications, use SQLiteConnectionManager instead.
    """
    # Create a new connection directly with optimizations
    validate_safe_path_components(db_path, "Database path")

    # Ensure parent directory exists
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    # Use temporary manager just for setup
    temp_manager = SQLiteConnectionManager(db_path, config)
    temp_manager.setup_database()
    temp_manager.close()  # Close manager but keep setup effects

    # Create a standalone connection with settings applied
    conn = sqlite3.connect(db_path, isolation_level=None)

    # Apply connection settings directly
    config = config or load_sqlite_config()
    conn.execute(f"PRAGMA busy_timeout={config['BUSY_TIMEOUT']}")
    conn.execute(f"PRAGMA wal_autocheckpoint={config['WAL_AUTOCHECKPOINT']}")
    conn.execute(f"PRAGMA cache_size=-{config['CACHE_MB'] * 1024}")

    sync_mode = config["SYNC_MODE"]
    if sync_mode not in ("FULL", "NORMAL", "OFF"):
        sync_mode = "FULL"
    conn.execute(f"PRAGMA synchronous={sync_mode}")

    return conn


def setup_wal_mode(db_path: str) -> None:
    """Setup WAL mode on a database file.

    Args:
        db_path: Path to database file
    """
    manager = SQLiteConnectionManager(db_path)
    manager.setup(SetupPhase.CONNECTION)
