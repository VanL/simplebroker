"""Constants and configuration for SimpleBroker.

This module centralizes all constants and environment variable configuration
for SimpleBroker. Constants are immutable values that control various aspects
of the system's behavior, from message size limits to timing parameters.

Environment Variables:
    See the load_config() function for a complete list of supported environment
    variables and their default values.

Usage:
    from simplebroker._constants import MAX_MESSAGE_SIZE, load_config

    # Use constants directly
    if len(message) > MAX_MESSAGE_SIZE:
        raise ValueError("Message too large")

    # Load configuration once at module level
    _config = load_config()
    timeout = _config["BROKER_BUSY_TIMEOUT"]

    Note that functions that use _config values all take a config parameter,
    which defaults to _config if not provided.
"""

import os
import platform
import re
import warnings
from pathlib import PurePath
from typing import Any, Final

# ==============================================================================
# VERSION INFORMATION
# ==============================================================================

__version__: Final[str] = "2.8.3"
"""Current version of SimpleBroker."""

# ==============================================================================
# PROGRAM IDENTIFICATION
# ==============================================================================

PROG_NAME: Final[str] = "simplebroker"
"""Program name used in CLI help and error messages."""

ALIAS_PREFIX: Final[str] = "@"
"""Prefix used to denote explicit alias references in the CLI."""

# ==============================================================================
# DATABASE CONFIGURATION
# ==============================================================================

DEFAULT_DB_NAME: Final[str] = ".broker.db"
"""Default database filename created in current directory if not specified."""

SIMPLEBROKER_MAGIC: Final[str] = "simplebroker-v1"
"""Magic string stored in database to verify it's a SimpleBroker database."""

SCHEMA_VERSION: Final[int] = 4
"""Current database schema version for migration compatibility."""

# ==============================================================================
# EXIT CODES
# ==============================================================================

EXIT_SUCCESS: Final[int] = 0
"""Exit code for successful operations."""

EXIT_ERROR: Final[int] = 1
"""Exit code for errors in processing."""

EXIT_QUEUE_EMPTY: Final[int] = 2
"""Exit code when queue is empty or no messages match criteria."""

# ==============================================================================
# MESSAGE AND QUEUE CONSTRAINTS
# ==============================================================================

MAX_MESSAGE_SIZE: Final[int] = 10 * 1024 * 1024  # 10MB limit
"""Maximum allowed message size in bytes (default: 10MB).

Can be overridden with BROKER_MAX_MESSAGE_SIZE environment variable.
Messages larger than this will be rejected with a ValueError.
"""

MAX_QUEUE_NAME_LENGTH: Final[int] = 512
"""Maximum allowed length for queue names in characters."""

# ==============================================================================
# TIMESTAMP AND ID GENERATION
# ==============================================================================
# SimpleBroker uses hybrid timestamps that combine physical time with a logical
# counter to ensure uniqueness even under extreme concurrency.

TIMESTAMP_EXACT_NUM_DIGITS: Final[int] = 19
"""Exact number of digits required for message ID timestamps in string form."""

PHYSICAL_TIME_BITS: Final[int] = 52
"""Number of bits used for microseconds since epoch (supports until ~2113)."""

LOGICAL_COUNTER_BITS: Final[int] = 12
"""Number of bits used for the monotonic counter to handle sub-microsecond events."""

LOGICAL_COUNTER_MASK: Final[int] = (1 << LOGICAL_COUNTER_BITS) - 1
"""Bitmask for extracting the logical counter from a hybrid timestamp."""

MAX_LOGICAL_COUNTER: Final[int] = 1 << LOGICAL_COUNTER_BITS
"""Maximum value for logical counter (4096) before time must advance."""

UNIX_NATIVE_BOUNDARY: Final[int] = 2**44
"""Boundary for distinguishing Unix timestamps from native format (~17.6 trillion, year 2527)."""

SQLITE_MAX_INT64: Final[int] = 2**63
"""Maximum value for SQLite's signed 64-bit integer - timestamps must be less than this."""

# ==============================================================================
# TIME UNIT CONVERSIONS
# ==============================================================================

MS_PER_SECOND: Final[int] = 1000
"""Milliseconds per second."""

US_PER_SECOND: Final[int] = 1_000_000
"""Microseconds per second."""

MS_PER_US: Final[int] = 1000
"""Microseconds per millisecond."""

NS_PER_US: Final[int] = 1000
"""Nanoseconds per microsecond."""

NS_PER_SECOND: Final[int] = 1_000_000_000
"""Nanoseconds per second."""

WAIT_FOR_NEXT_INCREMENT: Final[float] = 0.000_001
"""Sleep duration in seconds (1μs) when waiting for clock to advance during timestamp collision."""

MAX_ITERATIONS: Final[int] = 100_000
"""Maximum iterations waiting for time to advance before concluding clock is broken."""

# ==============================================================================
# BATCH SIZE SETTINGS
# ==============================================================================

PEEK_BATCH_SIZE: Final[int] = 1000
"""Default batch size for peek operations.

Peek operations are non-transactional, so larger batches improve performance
without holding database locks. This is separate from GENERATOR_BATCH_SIZE
which is used for transactional claim/move operations.
"""

# ==============================================================================
# WATCHER SETTINGS
# ==============================================================================

MAX_TOTAL_RETRY_TIME: Final[int] = 300  # 5 minutes max
"""Maximum time in seconds to retry watcher initialization before giving up."""

# ==============================================================================
# DATABASE RUNNER PHASES
# ==============================================================================


class ConnectionPhase:
    """Database setup phases for SQLRunner implementations."""

    CONNECTION = "connection"
    """Basic connectivity and critical settings (e.g., enabling WAL mode)."""

    OPTIMIZATION = "optimization"
    """Performance settings (cache size, synchronous mode, etc.)."""


# ==============================================================================
# PROJECT SCOPING CONSTANTS
# ==============================================================================

MAX_PROJECT_TRAVERSAL_DEPTH: Final[int] = 100
"""Maximum directory levels to traverse when searching for project databases.

This limit prevents infinite loops and performance issues in pathological
directory structures. Set to match reasonable project depth expectations.
"""

# ==============================================================================
# PATH SECURITY VALIDATION
# ==============================================================================

# Common dangerous characters across all platforms
_COMMON_DANGEROUS_CHARS = [
    "\0",  # Null byte - can truncate paths
    "\r",
    "\n",  # Line endings - can cause injection
    "\t",  # Tab - can cause parsing issues
    "\x7f",  # DEL character
]

# Unix/Mac shell metacharacters (excluding backslash - it's allowed as path separator)
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

# Windows dangerous characters
_WINDOWS_DANGEROUS_CHARS = [
    ":",
    "*",
    "?",
    '"',
    "<",
    ">",
    "|",
    # Note: backslash is allowed on Windows as it's the native path separator
]

# Create platform-specific character lists
_unix_chars = _COMMON_DANGEROUS_CHARS + _UNIX_SHELL_CHARS
_windows_chars = _COMMON_DANGEROUS_CHARS + _WINDOWS_DANGEROUS_CHARS

# Pre-compile regex patterns for maximum performance
_UNIX_DANGEROUS_REGEX = re.compile(f"[{re.escape(''.join(_unix_chars))}]")
_WINDOWS_DANGEROUS_REGEX = re.compile(f"[{re.escape(''.join(_windows_chars))}]")

# Windows reserved names (case-insensitive)
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


def _validate_safe_path_components(path: str, context: str = "path") -> None:
    """Validate that path components don't contain dangerous characters or reserved names.

    This function provides comprehensive security validation for user-supplied paths,
    preventing various attack vectors including path traversal, shell injection,
    and Windows reserved names.

    Args:
        path: Path string to validate (can be filename or compound path)
        context: Description of what is being validated for error messages

    Raises:
        ValueError: If path contains dangerous characters or reserved names

    Security Features:
        - Prevents path traversal attacks (..)
        - Blocks null bytes and control characters
        - Prevents shell injection on Unix/Mac
        - Blocks Windows reserved names (CON, PRN, AUX, etc.)
        - Validates each path component separately
        - Uses pre-compiled regex for maximum performance
        - Allows Windows drive letters (e.g., C:, D:)
    """
    if not isinstance(path, str) or not path:
        raise ValueError(f"{context} must be a non-empty string")

    # Normalize path separators for consistent processing
    normalized_path = path.replace("\\", "/")
    pure_path = PurePath(normalized_path)

    # Use pre-compiled platform-specific regex for dangerous character detection
    is_windows = platform.system() == "Windows"
    dangerous_regex = _WINDOWS_DANGEROUS_REGEX if is_windows else _UNIX_DANGEROUS_REGEX

    # Check for dangerous characters using optimized regex
    if dangerous_regex.search(path):
        # On Windows, check if colon is part of a legitimate drive letter
        if is_windows and ":" in path:
            # Windows drive letter pattern: C:, D:, etc. at the beginning of absolute paths
            drive_pattern = re.compile(r"^[A-Za-z]:")

            # If it's just a drive letter, allow it
            if drive_pattern.match(path):
                # Check if there are other dangerous characters besides the drive colon
                path_without_drive = path[2:]  # Remove "C:" part
                if dangerous_regex.search(path_without_drive):
                    match = dangerous_regex.search(path_without_drive)
                    dangerous_char = match.group() if match else "unknown"
                    raise ValueError(
                        f"{context} contains dangerous character '{dangerous_char}': {path}. "
                        f"Path components must not contain shell metacharacters or control characters."
                    )
                # Drive letter is OK, continue with other validations
            else:
                # Colon is not part of drive letter, it's dangerous
                match = dangerous_regex.search(path)
                dangerous_char = match.group() if match else "unknown"
                raise ValueError(
                    f"{context} contains dangerous character '{dangerous_char}': {path}. "
                    f"Path components must not contain shell metacharacters or control characters."
                )
        else:
            # Not Windows or no colon, regular dangerous character
            match = dangerous_regex.search(path)
            dangerous_char = match.group() if match else "unknown"
            raise ValueError(
                f"{context} contains dangerous character '{dangerous_char}': {path}. "
                f"Path components must not contain shell metacharacters or control characters."
            )

    # Check each path component
    for part in pure_path.parts:
        if not part:  # Empty component (e.g., double slashes)
            continue

        # Check for parent directory references
        if part == "..":
            raise ValueError(
                f"{context} must not contain parent directory references: {path}"
            )

        # Check for current directory references (usually not dangerous but suspicious)
        if part == ".":
            raise ValueError(
                f"{context} must not contain current directory references: {path}"
            )

        # Windows reserved name validation using pre-compiled set
        if is_windows:
            # Remove extension for reserved name check
            name_without_ext = part.split(".")[0].upper()
            if name_without_ext in _WINDOWS_RESERVED_NAMES:
                raise ValueError(
                    f"{context} contains Windows reserved name '{part}': {path}. "
                    f"Avoid names like CON, PRN, AUX, NUL, COM1-9, LPT1-9."
                )

        # Check for names that start or end with spaces/periods (problematic on Windows)
        if part.startswith(" ") or part.endswith(" "):
            raise ValueError(
                f"{context} component cannot start or end with spaces: '{part}' in {path}"
            )

        if is_windows and (part.startswith(".") and part != "." and part != ".."):
            # On Windows, leading dots can be problematic in some contexts
            pass  # Allow hidden files but we already blocked . and ..

        # Check for excessively long path components
        if len(part) > 255:  # Most filesystems have 255 byte filename limits
            raise ValueError(
                f"{context} component too long (max 255 chars): '{part[:50]}...' in {path}"
            )

    # Also check for current directory in the original path before PurePath processing
    # (PurePath normalizes some patterns away)
    if (
        "/./" in normalized_path
        or normalized_path.startswith("./")
        or normalized_path == "."
    ):
        raise ValueError(
            f"{context} must not contain current directory references: {path}"
        )

    # Check total path length (Windows has 260 char limit, Unix varies but 1024 is safe)
    max_path_length = 260 if is_windows else 1024
    if len(path) > max_path_length:
        raise ValueError(
            f"{context} too long (max {max_path_length} chars): {len(path)} chars in {path[:50]}..."
        )


def _parse_bool(value: str) -> bool:
    """Parse environment variable string to boolean.

    Args:
        value: String value from environment variable

    Returns:
        True for "1", "true", "yes", "on" (case-insensitive), False otherwise

    Examples:
        >>> _parse_bool("1")
        True
        >>> _parse_bool("TRUE")
        True
        >>> _parse_bool("false")
        False
        >>> _parse_bool("")
        False
    """
    if not value:
        return False
    return value.lower().strip() in ("1", "true", "yes", "on")


def load_config() -> dict[str, Any]:
    """Load configuration from environment variables.

    This function reads all SimpleBroker environment variables and returns
    a configuration dictionary with validated values. It's designed to be
    called once at module initialization to avoid repeated environment lookups.

    Returns:
        dict: Configuration dictionary with the following keys:

        SQLite Performance Settings:
            BROKER_BUSY_TIMEOUT (int): SQLite busy timeout in milliseconds.
                Default: 5000 (5 seconds)
                Controls how long SQLite waits when database is locked.

            BROKER_CACHE_MB (int): SQLite page cache size in megabytes.
                Default: 10
                Larger values improve performance for repeated queries.
                Recommended: 10-50 MB for typical use, 100+ MB for heavy use.

            BROKER_SYNC_MODE (str): SQLite synchronous mode.
                Default: "FULL"
                Options:
                - "FULL": Maximum durability, safe against power loss
                - "NORMAL": ~25% faster writes, small risk on power loss
                - "OFF": Fastest but unsafe - testing only

            BROKER_WAL_AUTOCHECKPOINT (int): WAL checkpoint threshold in pages.
                Default: 1000 (≈1MB with 1KB pages)
                Controls when WAL data is moved to main database.

        Message Processing:
            BROKER_MAX_MESSAGE_SIZE (int): Maximum message size in bytes.
                Default: 10485760 (10MB)
                Messages larger than this are rejected.

            BROKER_READ_COMMIT_INTERVAL (int): Messages per transaction in --all mode.
                Default: 1 (exactly-once delivery)
                Higher values improve performance but risk redelivery on failure.

            BROKER_GENERATOR_BATCH_SIZE (int): Batch size for generator methods.
                Default: 100
                Controls how many messages are fetched at once by claim/move generators.
                Higher values reduce query overhead but use more memory.

        Vacuum Settings:
            BROKER_AUTO_VACUUM (int): Enable automatic vacuum of claimed messages.
                Default: 1 (enabled)
                Set to 0 to disable automatic cleanup.

            BROKER_AUTO_VACUUM_INTERVAL (int): Write operations between vacuum checks.
                Default: 100
                Lower values = more frequent cleanup, higher values = better performance.

            BROKER_VACUUM_THRESHOLD (float): Percentage of claimed messages to trigger vacuum.
                Default: 0.1 (10%)
                Vacuum runs when claimed messages exceed this percentage of total.

            BROKER_VACUUM_BATCH_SIZE (int): Messages to delete per vacuum batch.
                Default: 1000
                Larger batches are faster but hold locks longer.

            BROKER_VACUUM_LOCK_TIMEOUT (int): Seconds before vacuum lock is considered stale.
                Default: 300 (5 minutes)
                Prevents stuck vacuum operations from blocking others.

        Watcher Settings:
            BROKER_SKIP_IDLE_CHECK (bool): Skip idle queue optimization check.
                Default: False
                Set to "1" to disable two-phase detection.

            BROKER_JITTER_FACTOR (float): Jitter factor for polling intervals.
                Default: 0.15 (15%)
                Prevents synchronized polling across multiple watchers.

            BROKER_INITIAL_CHECKS (int): Burst mode checks with zero delay.
                Default: 100
                Higher values = faster response to new messages.

            BROKER_MAX_INTERVAL (float): Maximum polling interval in seconds.
                Default: 0.1 (100ms)
                Lower values = more responsive but higher CPU usage.

            BROKER_BURST_SLEEP (float): Sleep between burst mode checks.
                Default: 0.00001 (10μs)
                Tiny delay to prevent CPU spinning.

        Debug:
            BROKER_DEBUG (bool): Enable debug output.
                Default: False
                Shows additional diagnostic information.

        Logging:
            BROKER_LOGGING_ENABLED (bool): Enable logging output.
                Default: False (disabled)
                Set to "1" to enable logging throughout SimpleBroker.
                When enabled, logs will be written using Python's logging module.
                Configure logging levels and handlers in your application as needed.

        Project Scoping:
            BROKER_DEFAULT_DB_LOCATION (str): Default directory for database files.
                Default: "" (current working directory)
                Overrides current working directory default.
                Must be an absolute path. If a relative path is provided,
                a warning will be issued and the value will be ignored (reset to "").

            BROKER_DEFAULT_DB_NAME (str): Default database filename.
                Default: ".broker.db"
                Used for both project scoping search and fallback creation.
                Can be a compound path (e.g. "subdir/.broker.db"), but
                SimpleBroker will

            BROKER_PROJECT_SCOPE (bool): Enable git-like upward database search.
                Default: False
                Set to "1", "true", "yes", or "on" to enable.
                When enabled, searches upward through directory hierarchy
                to find existing databases before creating new ones.

    """
    config = {
        # SQLite performance settings
        "BROKER_BUSY_TIMEOUT": int(os.environ.get("BROKER_BUSY_TIMEOUT", "5000")),
        "BROKER_CACHE_MB": int(os.environ.get("BROKER_CACHE_MB", "10")),
        "BROKER_SYNC_MODE": os.environ.get("BROKER_SYNC_MODE", "FULL").upper(),
        "BROKER_WAL_AUTOCHECKPOINT": int(
            os.environ.get("BROKER_WAL_AUTOCHECKPOINT", "1000"),
        ),
        # Message processing
        "BROKER_MAX_MESSAGE_SIZE": int(
            os.environ.get("BROKER_MAX_MESSAGE_SIZE", str(MAX_MESSAGE_SIZE)),
        ),
        "BROKER_READ_COMMIT_INTERVAL": int(
            os.environ.get("BROKER_READ_COMMIT_INTERVAL", "1"),
        ),
        "BROKER_GENERATOR_BATCH_SIZE": int(
            os.environ.get("BROKER_GENERATOR_BATCH_SIZE", "100"),
        ),
        # Vacuum settings
        "BROKER_AUTO_VACUUM": int(os.environ.get("BROKER_AUTO_VACUUM", "1")),
        "BROKER_AUTO_VACUUM_INTERVAL": int(
            os.environ.get("BROKER_AUTO_VACUUM_INTERVAL", "100"),
        ),
        "BROKER_VACUUM_THRESHOLD": float(
            os.environ.get("BROKER_VACUUM_THRESHOLD", "10"),
        )
        / 100,
        "BROKER_VACUUM_BATCH_SIZE": int(
            os.environ.get("BROKER_VACUUM_BATCH_SIZE", "1000"),
        ),
        "BROKER_VACUUM_LOCK_TIMEOUT": int(
            os.environ.get("BROKER_VACUUM_LOCK_TIMEOUT", "300"),
        ),
        # Watcher settings
        "BROKER_SKIP_IDLE_CHECK": os.environ.get("BROKER_SKIP_IDLE_CHECK", "0") == "1",
        "BROKER_JITTER_FACTOR": float(os.environ.get("BROKER_JITTER_FACTOR", "0.15")),
        "BROKER_INITIAL_CHECKS": int(
            os.environ.get("BROKER_INITIAL_CHECKS", "100"),
        ),
        "BROKER_MAX_INTERVAL": float(
            os.environ.get("BROKER_MAX_INTERVAL", "0.1"),
        ),
        "BROKER_BURST_SLEEP": float(
            os.environ.get("BROKER_BURST_SLEEP", "0.00001"),
        ),
        # Debug
        "BROKER_DEBUG": bool(os.environ.get("BROKER_DEBUG")),
        # Logging
        "BROKER_LOGGING_ENABLED": os.environ.get("BROKER_LOGGING_ENABLED", "0") == "1",
        # Project scoping configuration
        "BROKER_DEFAULT_DB_LOCATION": os.environ.get("BROKER_DEFAULT_DB_LOCATION", ""),
        "BROKER_DEFAULT_DB_NAME": os.environ.get(
            "BROKER_DEFAULT_DB_NAME", DEFAULT_DB_NAME
        ),
        "BROKER_PROJECT_SCOPE": _parse_bool(
            os.environ.get("BROKER_PROJECT_SCOPE", "0")
        ),
    }

    # Validate SYNC_MODE
    if config["BROKER_SYNC_MODE"] not in ("FULL", "NORMAL", "OFF"):
        config["BROKER_SYNC_MODE"] = "FULL"

    # Validate project scoping configuration
    db_location = config["BROKER_DEFAULT_DB_LOCATION"]
    if isinstance(db_location, str) and db_location:
        # First validate for security (dangerous characters)
        try:
            _validate_safe_path_components(db_location, "BROKER_DEFAULT_DB_LOCATION")
        except ValueError as e:
            raise ValueError(
                f"BROKER_DEFAULT_DB_LOCATION validation failed: {e}"
            ) from e

        # Then check that it's an absolute path
        if not os.path.isabs(db_location):
            # Issue warning and ignore non-absolute paths
            warnings.warn(
                f"BROKER_DEFAULT_DB_LOCATION must be an absolute path. "
                f"Ignoring relative path: {db_location}",
                UserWarning,
                stacklevel=2,
            )
            config["BROKER_DEFAULT_DB_LOCATION"] = ""

    # Validate BROKER_DEFAULT_DB_NAME format (but don't create directories)
    db_name = config["BROKER_DEFAULT_DB_NAME"]
    if isinstance(db_name, str) and db_name:
        from pathlib import PurePath

        # First validate for security (dangerous characters)
        try:
            _validate_safe_path_components(db_name, "BROKER_DEFAULT_DB_NAME")
        except ValueError as e:
            raise ValueError(f"BROKER_DEFAULT_DB_NAME validation failed: {e}") from e

        # Check if it's an absolute path (must come first)
        if os.path.isabs(db_name):
            raise ValueError(
                f"BROKER_DEFAULT_DB_NAME must be a relative path, not absolute: {db_name}. "
                f"Use BROKER_DEFAULT_DB_LOCATION to specify the directory instead."
            )

        # Then check for nested directories
        parts = list(PurePath(db_name).parts)
        if len(parts) > 2:
            raise ValueError(
                f"Database name must not contain nested directories: {db_name}. "
                f"Only single directory level is supported (e.g., 'dir/name.db')"
            )

    return config


# ~
