"""Cross-platform test utilities for SimpleBroker tests."""

import platform
import tempfile
from pathlib import Path


def create_platform_absolute_path(
    relative_parts: str, base_dir: str | None = None
) -> str:
    """Create a platform-appropriate absolute path for testing.

    This function creates absolute paths that work correctly on both Unix and Windows
    platforms, avoiding the issues with hardcoded Unix paths like '/tmp/test' that
    are treated as relative paths on Windows.

    Args:
        relative_parts: Path parts to append to the base directory (e.g., "test/dir")
        base_dir: Optional base directory. If None, uses a temporary directory.

    Returns:
        Platform-appropriate absolute path as string

    Examples:
        >>> path = create_platform_absolute_path("test/dir")  # Uses temp dir
        >>> path = create_platform_absolute_path("project", "/home/user")  # Uses custom base
    """
    if base_dir is None:
        # Use a guaranteed absolute path from the system
        base = Path.cwd()
    else:
        base = Path(base_dir)

    # Ensure the base is absolute
    if not base.is_absolute():
        base = base.resolve()

    # Append the relative parts
    return str(base / relative_parts)


def create_temp_absolute_path(relative_parts: str) -> str:
    """Create a temporary absolute path for testing.

    This is a convenience function that creates absolute paths within the system's
    temporary directory, ensuring they work across platforms.

    Args:
        relative_parts: Path parts to append to temp directory

    Returns:
        Absolute path within temporary directory
    """
    temp_root = Path(tempfile.gettempdir())
    return str(temp_root / relative_parts)


def get_platform_drive_letter() -> str:
    """Get a valid drive letter for the current platform.

    Returns:
        Drive letter (e.g., "C") on Windows, "/" on Unix systems
    """
    if platform.system() == "Windows":
        # Try to get the current drive letter, fallback to C
        current_path = Path.cwd()
        drive = str(current_path).split(":")[0] if ":" in str(current_path) else "C"
        return drive
    else:
        return ""  # Unix systems don't use drive letters


def create_windows_absolute_path(relative_parts: str, drive: str | None = None) -> str:
    """Create a Windows-style absolute path for testing.

    Args:
        relative_parts: Path parts (e.g., "temp/test")
        drive: Drive letter (defaults to current drive or C)

    Returns:
        Windows absolute path like "C:\\temp\\test"
    """
    if drive is None:
        drive = get_platform_drive_letter() or "C"
    # Fix Python 3.8 compatibility - can't use backslashes in f-strings
    backslash = "\\"
    windows_parts = relative_parts.replace("/", backslash)
    return f"{drive}:{backslash}{windows_parts}"


def normalize_path_for_platform(path: str) -> str:
    """Normalize a path string for the current platform.

    This function handles path separator normalization and ensures
    the path is appropriate for the current operating system.

    Args:
        path: Path string to normalize

    Returns:
        Platform-normalized path string
    """
    return str(Path(path))


def is_platform_absolute(path: str) -> bool:
    """Check if a path is absolute on the current platform.

    This provides a reliable cross-platform way to test absolute paths,
    accounting for Windows drive letters vs Unix root paths.

    Args:
        path: Path string to check

    Returns:
        True if path is absolute on current platform
    """
    return Path(path).is_absolute()


def create_dangerous_path(base_path: str, dangerous_char: str) -> str:
    """Create a path with dangerous characters for security testing.

    Args:
        base_path: Base path to build upon
        dangerous_char: Dangerous character to include (e.g., "|", "&")

    Returns:
        Path with dangerous character embedded
    """
    base = Path(base_path)
    return str(base / f"test{dangerous_char}dir")


def create_absolute_test_path(relative_path: str = "test") -> str:
    """Create a guaranteed absolute path for cross-platform testing.

    This function creates absolute paths that work on both Unix and Windows,
    avoiding hardcoded paths like '/tmp/test' that fail on Windows.

    Args:
        relative_path: Relative path to append to a temporary base

    Returns:
        Cross-platform absolute path

    Example:
        >>> path = create_absolute_test_path("project/database")
        # Returns something like "/tmp/tmpXXXXXX/project/database" on Unix
        # or "C:\\Users\\...\\AppData\\Local\\Temp\\tmpXXXXXX\\project\\database" on Windows
    """
    temp_base = Path(tempfile.gettempdir())
    return str(temp_base / "simplebroker_test" / relative_path)


def skip_if_platform(platform_name: str, reason: str = "Platform-specific test"):
    """Decorator to skip tests on specific platforms.

    Args:
        platform_name: Platform to skip on (e.g., "Windows", "Darwin", "Linux")
        reason: Reason for skipping

    Returns:
        pytest skip decorator

    Example:
        @skip_if_platform("Windows", "Unix-specific shell behavior")
        def test_unix_specific_feature():
            ...
    """
    import pytest

    return pytest.mark.skipif(platform.system() == platform_name, reason=reason)


def run_only_on_platform(platform_name: str, reason: str = "Platform-specific test"):
    """Decorator to run tests only on specific platforms.

    Args:
        platform_name: Platform to run on (e.g., "Windows", "Darwin", "Linux")
        reason: Reason for platform restriction

    Returns:
        pytest skip decorator

    Example:
        @run_only_on_platform("Windows", "Windows drive letter behavior")
        def test_windows_specific_feature():
            ...
    """
    import pytest

    return pytest.mark.skipif(platform.system() != platform_name, reason=reason)
