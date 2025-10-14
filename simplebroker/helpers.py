"""Helper functions and classes for SimpleBroker."""

import os
import sqlite3
import threading
import time
from collections.abc import Callable
from pathlib import Path, PurePath
from typing import TypeVar

from ._constants import (
    MAX_PROJECT_TRAVERSAL_DEPTH,
    SIMPLEBROKER_MAGIC,
    _validate_safe_path_components,
)
from ._exceptions import DatabaseError, OperationalError, StopException

T = TypeVar("T")


def interruptible_sleep(
    seconds: float,
    stop_event: threading.Event | None = None,
    chunk_size: float = 0.1,
) -> bool:
    """Sleep for the specified duration, but can be interrupted by a stop event.

    This function provides a more responsive alternative to time.sleep() that can be
    interrupted by a threading.Event. Even without a stop_event, it sleeps in chunks
    to allow for better thread responsiveness and signal handling.

    Args:
        seconds: Number of seconds to sleep
        stop_event: Optional threading.Event that can interrupt the sleep
        chunk_size: Maximum duration of each sleep chunk (default: 0.1 seconds)

    Returns:
        True if the full sleep duration completed, False if interrupted by stop_event

    Example:
        # In a loop that needs to be stoppable
        stop_event = threading.Event()
        while not stop_event.is_set():
            # Do work...
            if not interruptible_sleep(5.0, stop_event):
                break  # Sleep was interrupted, exit loop
    """
    if seconds <= 0:
        return True

    # Create a dummy event if none provided
    event = stop_event or threading.Event()

    # For short sleeps, do it in one go
    if seconds <= chunk_size:
        return not event.wait(timeout=seconds)

    # For longer sleeps, chunk it up
    start_time = time.perf_counter()
    target_end_time = start_time + seconds

    while time.perf_counter() < target_end_time:
        remaining = target_end_time - time.perf_counter()
        if remaining <= 0:
            break

        if event.wait(timeout=min(chunk_size, remaining)):
            # Only return False if it was the actual stop_event that was set
            return stop_event is None or not stop_event.is_set()

    return True


def _execute_with_retry(
    operation: Callable[[], T],
    *,
    max_retries: int = 10,
    retry_delay: float = 0.05,
    stop_event: threading.Event | None = None,
) -> T:
    """Execute a database operation with retry logic for locked database errors.

    Args:
        operation: A callable that performs the database operation
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries (exponential backoff applied)
        stop_event: Optional threading.Event that can interrupt the retry loop

    Returns:
        The result of the operation

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
        except OperationalError as e:
            msg = str(e).lower()
            if any(marker in msg for marker in locked_markers):
                if attempt < max_retries - 1:
                    # exponential back-off + 0-25 ms jitter using time-based pseudo-random
                    jitter = (time.time() * 1000) % 25 / 1000  # 0-25ms jitter
                    wait = retry_delay * (2**attempt) + jitter
                    if not interruptible_sleep(wait, stop_event):
                        # Sleep was interrupted, raise exception to exit retry loop
                        raise StopException("Retry interrupted by stop event") from None
                    continue
            # If not a locked error or last attempt, re-raise
            raise

    # This should never be reached, but satisfies mypy
    raise AssertionError("Unreachable code")


def _is_filesystem_root(path: Path) -> bool:
    """Check if path represents a filesystem root.

    Args:
        path: Path to check if it is a root directory

    Returns:
        True if path is a root directory, False otherwise

    Security Note:
        Stops at filesystem root to prevent infinite loops.
    """
    p = Path(path).resolve()
    return p.parent == p


def is_ancestor(possible_ancestor: str | Path, possible_descendant: str | Path) -> bool:
    """Check if possible_ancestor is an ancestor of possible_descendant."""
    path_ancestor = Path(possible_ancestor).resolve()
    path_descendant = Path(possible_descendant).resolve()

    try:
        path_descendant.relative_to(path_ancestor)
        return True
    except ValueError:
        return False


def _validate_sqlite_database(file_path: Path, verify_magic: bool = True) -> None:
    """Validate that a file is a valid SQLite database and raise detailed errors.

    Args:
        file_path: Path to the file to validate
        verify_magic: Whether to verify SimpleBroker magic string

    Raises:
        DatabaseError: If the file is not a valid SQLite database with specific reason
    """
    # Verify arg types
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    # Verify file existence and that it's a file
    if not file_path.exists():
        raise DatabaseError(f"Database file does not exist: {file_path}")

    if not file_path.is_file():
        raise DatabaseError(f"Path exists but is not a regular file: {file_path}")

    # Check permissions
    if not os.access(file_path.parent, os.R_OK | os.W_OK):
        raise DatabaseError(f"Parent directory is not accessible: {file_path.parent}")

    if not os.access(file_path, os.R_OK | os.W_OK):
        raise DatabaseError(f"Database file is not readable/writable: {file_path}")

    # First check the header (fast)
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
    try:
        conn = sqlite3.connect(f"file:{file_path}?mode=ro", uri=True)
        cursor = conn.cursor()
        # PRAGMA integrity_check is more thorough but slower
        # PRAGMA schema_version is faster and sufficient for most cases
        cursor.execute("PRAGMA schema_version")
        cursor.fetchone()

        if verify_magic:
            cursor.execute("SELECT value FROM meta WHERE key = 'magic'")
            magic_row = cursor.fetchone()
            if magic_row is None:
                raise DatabaseError(
                    f"Database is missing SimpleBroker metadata: {file_path}"
                )
            if magic_row[0] != SIMPLEBROKER_MAGIC:
                raise DatabaseError(
                    f"Database has incorrect magic string (not a SimpleBroker database): {file_path}"
                )

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
        try:
            conn.close()
        except Exception:
            pass  # Ignore close errors


def _is_valid_sqlite_db(file_path: Path, verify_magic: bool = True) -> bool:
    """Check if a file is a valid SQLite database.

    Args:
        file_path: Path to the file to check
        verify_magic: Whether to verify SimpleBroker magic string

    Returns:
        True if the file is a valid SQLite database, False otherwise
    """
    try:
        _validate_sqlite_database(file_path, verify_magic)
        return True
    except DatabaseError:
        return False


def _find_project_database(
    search_filename: str,
    starting_dir: Path,
    max_depth: int = MAX_PROJECT_TRAVERSAL_DEPTH,
) -> Path | None:
    """Search upward through directory hierarchy for SimpleBroker project database.

    Args:
        search_filename: Database filename to search for (e.g., ".broker.db")
        starting_dir: Directory to start search from (typically cwd)
        max_depth: Maximum levels to traverse (security limit)

    Returns:
        Absolute path to found database, or None if not found

    Security Features:
        - Respects max_depth to prevent infinite loops
        - Validates database authenticity via magic string
        - Stops at filesystem boundaries (root, home, etc.)
        - Uses existing path resolution for symlink safety

    Raises:
        ValueError: If starting_dir doesn't exist or max_depth exceeded
    """
    if not starting_dir.exists():
        raise ValueError(f"Starting directory does not exist: {starting_dir}")

    current_dir = starting_dir.resolve()  # Use existing symlink resolution
    depth = 0

    while depth < max_depth:
        # Check for filesystem root directory
        if _is_filesystem_root(current_dir):
            break

        candidate_path = current_dir / search_filename
        if _is_valid_sqlite_db(candidate_path):
            return candidate_path.resolve()
        else:
            # If the candidate path is not a valid SQLite DB, continue search
            current_dir = current_dir.parent
            depth += 1
            continue
    return None


def _is_ancestor_of_working_directory(db_path: Path, working_dir: Path) -> bool:
    """Verify that db_path is in the ancestor chain of working_dir.

    Args:
        db_path: Resolved database path from project scoping
        working_dir: Current working directory

    Returns:
        True if db_path.parent is an ancestor of working_dir

    Security Note:
        Prevents project scoping from accessing sibling directories
        or unrelated paths outside the legitimate parent chain.
    """
    return is_ancestor(db_path.parent, working_dir)


def _validate_working_directory(working_dir: Path) -> None:
    """Validate that working directory exists and is accessible.

    Args:
        working_dir: Directory path to validate

    Raises:
        ValueError: If directory validation fails
    """
    if not working_dir.exists():
        raise ValueError(f"Directory not found: {working_dir}")
    if not working_dir.is_dir():
        # Provide more helpful error message for common mistake
        if working_dir.is_file():
            raise ValueError(f"Path is a file, not a directory: {working_dir}")
        else:
            raise ValueError(f"Not a directory: {working_dir}")


def _is_compound_db_name(db_name: str) -> tuple[bool, list[str]]:
    """Detect if database name contains path components and split them.

    Only supports a single directory level (e.g., "some/name.db").
    Deeper nesting is not allowed for security and simplicity.

    Args:
        db_name: Database name from BROKER_DEFAULT_DB_NAME

    Returns:
        tuple of (is_compound, path_components)
        - is_compound: True if db_name contains exactly one directory separator
        - path_components: list of path parts (empty if not compound)

    Examples:
        _is_compound_db_name("broker.db") -> (False, [])
        _is_compound_db_name("some/name.db") -> (True, ["some", "name.db"])

    Raises:
        ValueError: If database name contains dangerous characters or more than one directory level
    """
    # First validate for security
    _validate_safe_path_components(db_name, "Database name")

    db_name = db_name.replace("\\", "/")  # Normalize path separators
    pure_path = PurePath(db_name)
    parts = list(pure_path.parts)

    # Check for nested directories (more than 2 parts)
    if len(parts) > 2:
        raise ValueError(
            f"Database name must not contain nested directories: {db_name}. "
            f"Only single directory level is supported (e.g., 'dir/name.db')"
        )

    # If there are exactly 2 parts, it's compound
    is_compound = len(parts) == 2
    return is_compound, parts if is_compound else []


def _create_compound_db_directories(base_dir: Path, db_name: str) -> None:
    """Create intermediate directories for compound database names.

    Args:
        base_dir: Base directory where database will be located
        db_name: Database name (may be compound like "some/name.db")

    Raises:
        ValueError: If directory creation fails
    """
    is_compound, parts = _is_compound_db_name(db_name)

    if not is_compound:
        return  # Nothing to create

    # Create intermediate directories (exclude the final filename)
    intermediate_parts = parts[:-1]  # All parts except the database filename

    if intermediate_parts:
        intermediate_path = base_dir
        for part in intermediate_parts:
            intermediate_path = intermediate_path / part

        try:
            intermediate_path.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            raise ValueError(
                f"Cannot create intermediate directories {intermediate_path}: {e}"
            ) from e


def ensure_compound_db_path(base_dir: Path, db_name: str) -> Path:
    """Ensure compound database path exists and return full database path.

    Args:
        base_dir: Base directory (e.g., /home/vanl/dev/)
        db_name: Database name (e.g., ".config/broker.db")

    Returns:
        Full database path (e.g., /home/vanl/dev/.config/broker.db)

    Raises:
        ValueError: If directory creation fails or db_name is invalid
    """
    is_compound, parts = _is_compound_db_name(db_name)

    if not is_compound:
        return base_dir / db_name

    # Create subdirectory and return full path
    subdir_path = base_dir / parts[0]
    try:
        subdir_path.mkdir(parents=True, exist_ok=True)
    except (OSError, PermissionError) as e:
        raise ValueError(
            f"Cannot create compound subdirectory {subdir_path}: {e}"
        ) from e

    return subdir_path / parts[1]


def _validate_database_parent_directory(db_path: Path) -> None:
    """Validate that database parent directory exists and has proper permissions.

    Args:
        db_path: Database file path to validate parent directory of

    Raises:
        ValueError: If parent directory validation fails
    """
    # Check if parent directory exists
    if not db_path.parent.exists():
        raise ValueError(f"Parent directory not found: {db_path.parent}")

    # Check if parent directory is accessible (executable/writable)
    if not os.access(db_path.parent, os.X_OK):
        raise ValueError(f"Parent directory is not accessible: {db_path.parent}")

    if not os.access(db_path.parent, os.W_OK):
        raise ValueError(f"Parent directory is not writable: {db_path.parent}")


def _resolve_symlinks_safely(path: Path, max_depth: int = 40) -> Path:
    """Safely resolve symlinks with protection against infinite loops.

    Args:
        path: Path to resolve
        max_depth: Maximum symlink resolution depth to prevent infinite loops

    Returns:
        Resolved path with all symlinks followed

    Raises:
        RuntimeError: If symlink resolution fails
    """
    try:
        resolved_path = path.resolve()

        # On Windows, resolve() might not fully resolve symlink chains
        # Keep resolving until we reach a non-symlink or hit an error
        depth = 0
        while resolved_path.is_symlink() and depth < max_depth:
            try:
                # Read the symlink target and resolve it
                if hasattr(resolved_path, "readlink"):
                    # Python 3.9+
                    target = resolved_path.readlink()
                else:
                    # Python 3.8 and older
                    target = Path(os.readlink(str(resolved_path)))

                if target.is_absolute():
                    resolved_path = target.resolve()
                else:
                    # Relative symlink - resolve relative to parent
                    resolved_path = (resolved_path.parent / target).resolve()
                depth += 1
            except (OSError, RuntimeError):
                # If we can't read/resolve the symlink, use what we have
                break

        return resolved_path
    except (RuntimeError, OSError) as e:
        raise RuntimeError(f"Failed to resolve symlinks for {path}: {e}") from e


def _validate_path_containment(
    db_path: Path, working_dir: Path, used_project_scope: bool
) -> None:
    """Validate that database path is properly contained within allowed boundaries.

    Args:
        db_path: Resolved database path to validate
        working_dir: Resolved working directory
        used_project_scope: Whether project scoping was used

    Raises:
        ValueError: If path containment validation fails
    """
    # Check if the database path is within the working directory
    # Exception: Allow parent paths when using legitimate project scoping
    containment_check = True
    if hasattr(db_path, "is_relative_to"):
        containment_check = not db_path.is_relative_to(working_dir)
    else:
        # Fallback for older Python versions - try relative_to and catch exception
        try:
            db_path.relative_to(working_dir)
            containment_check = False
        except ValueError:
            containment_check = True

    if containment_check and not used_project_scope:
        raise ValueError("Database file must be within the working directory")
    elif used_project_scope:
        # Additional validation for project-scoped paths
        if not _is_ancestor_of_working_directory(db_path, working_dir):
            raise ValueError(
                "Project-scoped database path must be in parent directory chain"
            )


def _validate_path_traversal_prevention(filename: str) -> None:
    """Validate that filename doesn't contain path traversal attempts.

    Args:
        filename: Database filename to validate

    Raises:
        ValueError: If path traversal attempt is detected

    Note:
        This function is deprecated in favor of _validate_safe_path_components
        but maintained for backward compatibility.
    """
    _validate_safe_path_components(filename, "Database filename")


# ~
