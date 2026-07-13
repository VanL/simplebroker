"""Helper functions and classes for SimpleBroker."""

import os
import threading
import time
from collections.abc import Callable, Mapping
from pathlib import Path, PurePath
from typing import Any, TypeVar

from ._backends import get_configured_backend
from ._constants import (
    MAX_PROJECT_TRAVERSAL_DEPTH,
    _validate_safe_path_components,
    load_config,
)
from ._exceptions import OperationalError, StopException
from ._retry import (
    RetryInterrupted,
    RetryState,
    Stop,
    bounded_jitter,
    execute_retry,
    expo,
    interruptible_sleep,
    stop_after_attempt,
    stop_after_delay,
    stop_any,
    stop_never,
)

T = TypeVar("T")

_config = load_config()
db_backend = get_configured_backend(_config)
SETUP_RETRY_MAX_ELAPSED = 10.0
SETUP_RETRY_DELAY = 0.05
SETUP_RETRY_MAX_DELAY = 0.25
SETUP_BUSY_TIMEOUT_CAP_MS = 250
SETUP_PHASE_LOCK_TIMEOUT = max(20.0, SETUP_RETRY_MAX_ELAPSED * 2.0)
_LOCKED_ERROR_MARKERS = (
    "database is locked",
    "database table is locked",
    "database schema is locked",
    "database is busy",
    "database busy",
)


def _build_retry_stop(
    *,
    max_retries: int | None,
    max_elapsed: float | None,
    progress_token: Callable[[], object | None] | None = None,
) -> Stop:
    stops: list[Stop] = []
    if max_retries is not None:
        stops.append(stop_after_attempt(max_retries))
    if max_elapsed is not None:
        if progress_token is None:
            stops.append(stop_after_delay(max_elapsed))
        else:
            stops.append(_StopAfterProgressStall(max_elapsed, progress_token))
    if not stops:
        return stop_never()
    if len(stops) == 1:
        return stops[0]
    return stop_any(*stops)


class _StopAfterProgressStall(Stop):
    """Stop after one idle window without a changed external progress token."""

    def __init__(
        self,
        idle_timeout: float,
        progress_token: Callable[[], object | None],
    ) -> None:
        self.idle_timeout = idle_timeout
        self._progress_token = progress_token
        self._last_progress_elapsed = 0.0
        self._observed_token: object | None = None
        self._has_observed_token = False

    def _observe_progress(self, *, elapsed: float) -> None:
        try:
            token = self._progress_token()
        except Exception:
            return
        if token is None:
            return
        if self._has_observed_token and token != self._observed_token:
            self._last_progress_elapsed = elapsed
        self._observed_token = token
        self._has_observed_token = True

    def __call__(self, state: RetryState) -> bool:
        self._observe_progress(elapsed=state.elapsed)
        return state.elapsed - self._last_progress_elapsed >= self.idle_timeout


def _execute_with_retry(
    operation: Callable[[], T],
    *,
    max_retries: int | None = 10,
    retry_delay: float = 0.05,
    stop_event: threading.Event | None = None,
    max_elapsed: float | None = None,
    max_retry_delay: float | None = None,
    progress_token: Callable[[], object | None] | None = None,
) -> T:
    """Execute a database operation with retry logic for locked database errors.

    Args:
        operation: A callable that performs the database operation
        max_retries: Maximum number of retry attempts. None means the elapsed
            deadline is the only retry bound.
        retry_delay: Initial delay between retries (exponential backoff applied)
        stop_event: Optional threading.Event that can interrupt the retry loop
        max_elapsed: Optional elapsed-time retry budget in seconds
        max_retry_delay: Optional cap for each retry sleep
        progress_token: Optional external progress marker. When it changes,
            max_elapsed is treated as an idle timeout instead of a fixed
            wall-clock deadline.

    Returns:
        The result of the operation

    Raises:
        The last exception if all retries fail
    """
    if max_retries is None and max_elapsed is None:
        raise ValueError("max_retries=None requires max_elapsed")

    def retry_on(exc: Exception) -> bool:
        if not isinstance(exc, OperationalError):
            return False
        return _is_locked_operational_error(exc)

    def stop_checked_operation() -> T:
        if stop_event is not None and stop_event.is_set():
            raise RetryInterrupted
        return operation()

    try:
        return execute_retry(
            stop_checked_operation,
            retry_on=retry_on,
            wait_gen=expo,
            wait_gen_kwargs={
                "base": 2,
                "factor": retry_delay,
                "max_value": max_retry_delay,
            },
            jitter=bounded_jitter,
            stop=_build_retry_stop(
                max_retries=max_retries,
                max_elapsed=max_elapsed,
                progress_token=progress_token,
            ),
            # execute_retry's max_delay is a fixed total-runtime clamp. A
            # progress-aware stop owns the deadline instead so forward motion
            # can refresh the idle window.
            max_delay=max_elapsed if progress_token is None else None,
            sleep=interruptible_sleep,
            stop_event=stop_event,
        )
    except RetryInterrupted:
        raise StopException("Retry interrupted by stop event") from None


def _is_locked_operational_error(exc: OperationalError) -> bool:
    """Return whether an OperationalError represents lock or busy contention.

    Backends whose drivers do not emit SQLite-style messages mark
    contention explicitly via ``OperationalError.retryable``; the message
    markers remain the fallback for plain SQLite errors.
    """
    retryable = getattr(exc, "retryable", None)
    if retryable is not None:
        return bool(retryable)
    message = str(exc).lower()
    return any(marker in message for marker in _LOCKED_ERROR_MARKERS)


def _is_watcher_operational_retry(exc: Exception) -> bool:
    if not isinstance(exc, OperationalError):
        return False
    if getattr(exc, "retryable", None) is False:
        return False
    return True


def _execute_watcher_operational_retry(
    operation: Callable[[], T],
    *,
    max_retries: int = 5,
    retry_delay: float = 0.05,
    stop_event: threading.Event | None = None,
    before_sleep: Callable[[RetryState, Exception, float], None] | None = None,
) -> T:
    try:
        return execute_retry(
            operation,
            retry_on=_is_watcher_operational_retry,
            wait_gen=expo,
            wait_gen_kwargs={"base": 2, "factor": retry_delay},
            jitter=bounded_jitter,
            stop=stop_after_attempt(max_retries),
            sleep=interruptible_sleep,
            stop_event=stop_event,
            before_sleep=before_sleep,
        )
    except RetryInterrupted:
        raise StopException("Retry interrupted by stop event") from None


def _execute_connection_retry(
    operation: Callable[[], T],
    *,
    max_retries: int = 3,
    stop_event: threading.Event | None = None,
    before_sleep: Callable[[RetryState, Exception, float], None] | None = None,
) -> T:
    def retry_on(exc: Exception) -> bool:
        return not isinstance(exc, StopException)

    try:
        return execute_retry(
            operation,
            retry_on=retry_on,
            wait_gen=expo,
            wait_gen_kwargs={"base": 2, "factor": 2.0, "max_value": None},
            jitter=None,
            stop=stop_after_attempt(max_retries),
            sleep=interruptible_sleep,
            stop_event=stop_event,
            before_sleep=before_sleep,
        )
    except RetryInterrupted:
        raise StopException("Connection interrupted") from None


def setup_busy_timeout_ms(config: Mapping[str, Any]) -> int:
    """Return the short busy timeout used by SQLite setup operations."""

    busy_timeout = int(config["BROKER_BUSY_TIMEOUT"])
    return max(0, min(busy_timeout, SETUP_BUSY_TIMEOUT_CAP_MS))


class SetupProgressBudget:
    """Track idle time between successful setup operations."""

    def __init__(self, idle_timeout: float | None = None) -> None:
        self.idle_timeout = (
            SETUP_RETRY_MAX_ELAPSED if idle_timeout is None else idle_timeout
        )
        self._last_progress = time.monotonic()

    def remaining(self) -> float:
        """Return seconds left before setup is considered idle."""

        return self.idle_timeout - (time.monotonic() - self._last_progress)

    def record_progress(self) -> None:
        """Refresh the idle budget after a setup operation succeeds."""

        self._last_progress = time.monotonic()


def _setup_phase_context(phase: str, target: str) -> str:
    return f"Setup phase {phase!r} for {target!r}"


def _setup_idle_timeout_error(
    *,
    phase: str,
    target: str,
    idle_timeout: float,
    detail: str,
) -> OperationalError:
    return OperationalError(
        f"{_setup_phase_context(phase, target)} made no progress for "
        f"{idle_timeout:.1f}s: {detail}"
    )


def execute_setup_with_retry(
    operation: Callable[[], T],
    *,
    phase: str,
    target: str,
    stop_event: threading.Event | None = None,
    progress_budget: SetupProgressBudget | None = None,
) -> T:
    """Run setup work with bounded retry progress and contextual errors."""

    max_elapsed = SETUP_RETRY_MAX_ELAPSED
    if progress_budget is not None:
        max_elapsed = progress_budget.remaining()
        if max_elapsed <= 0:
            raise _setup_idle_timeout_error(
                phase=phase,
                target=target,
                idle_timeout=progress_budget.idle_timeout,
                detail="setup idle timeout expired",
            )

    try:
        result = _execute_with_retry(
            operation,
            max_retries=None,
            retry_delay=SETUP_RETRY_DELAY,
            stop_event=stop_event,
            max_elapsed=max_elapsed,
            max_retry_delay=SETUP_RETRY_MAX_DELAY,
        )
    except StopException:
        raise
    except OperationalError as exc:
        if progress_budget is not None:
            remaining = progress_budget.remaining()
            if remaining <= 0 and _is_locked_operational_error(exc):
                raise _setup_idle_timeout_error(
                    phase=phase,
                    target=target,
                    idle_timeout=progress_budget.idle_timeout,
                    detail=str(exc),
                ) from exc
            raise OperationalError(
                f"{_setup_phase_context(phase, target)} failed: {exc}"
            ) from exc

        if not _is_locked_operational_error(exc):
            raise OperationalError(
                f"{_setup_phase_context(phase, target)} failed: {exc}"
            ) from exc

        raise OperationalError(
            f"{_setup_phase_context(phase, target)} could not make progress within "
            f"{SETUP_RETRY_MAX_ELAPSED:.1f}s: {exc}"
        ) from exc
    if progress_budget is not None:
        progress_budget.record_progress()
    return result


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
    """Compatibility wrapper for SQLite database validation."""
    db_backend.validate_database(file_path, verify_magic)


def _is_valid_sqlite_db(file_path: Path, verify_magic: bool = True) -> bool:
    """Compatibility wrapper for SQLite database validation checks."""
    return db_backend.is_valid_database(file_path, verify_magic)


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
