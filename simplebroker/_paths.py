"""Filesystem path security and project-database discovery.

Every function here answers one of two questions: is this path safe to use,
and where is the project database for this directory. Path validation runs
before any file is opened, so a rejected path never reaches a backend.
"""

import os
from pathlib import Path, PurePath

from ._backends import get_backend
from ._constants import MAX_PROJECT_TRAVERSAL_DEPTH, _validate_safe_path_components


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


def _same_filesystem(current: Path, parent: Path) -> bool:
    """Return whether an upward discovery step stays on one filesystem."""
    try:
        return current.stat().st_dev == parent.stat().st_dev
    except OSError:
        return False


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
    get_backend().validate_database(file_path, verify_magic)


def _is_valid_sqlite_db(file_path: Path, verify_magic: bool = True) -> bool:
    """Compatibility wrapper for SQLite database validation checks."""
    return get_backend().is_valid_database(file_path, verify_magic)


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
        - Stops at the filesystem root and before crossing mount boundaries
        - Resolves the physical starting path and follows its parent chain

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
            parent = current_dir.parent
            if not _same_filesystem(current_dir, parent):
                break
            current_dir = parent
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
                target = resolved_path.readlink()

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
    containment_check = not db_path.is_relative_to(working_dir)

    if containment_check and not used_project_scope:
        raise ValueError("Database file must be within the working directory")
    # Additional validation for project-scoped paths
    if used_project_scope and not _is_ancestor_of_working_directory(
        db_path, working_dir
    ):
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
