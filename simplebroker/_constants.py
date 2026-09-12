"""Constants and configuration for SimpleBroker.

This module centralizes all constants and environment variable configuration
for SimpleBroker. Constants are immutable values that control various aspects
of the system's behavior, from message size limits to timing parameters.

Environment Variables:
    See the load_config() function for a complete list of supported environment
    variables and their default values.

Usage:
    from simplebroker._constants import MAX_MESSAGE_SIZE, snapshot_config

    # Use constants directly
    if len(message) > MAX_MESSAGE_SIZE:
        raise ValueError("Message too large")

    # Resolve one immutable receipt at an ownership boundary.
    config = snapshot_config()
    timeout = config["BROKER_BUSY_TIMEOUT"]
"""

import platform
import re
import unicodedata
from pathlib import PurePath
from typing import TYPE_CHECKING, Any, Final

# ==============================================================================
# VERSION INFORMATION
# ==============================================================================

__version__: Final[str] = "8.1.1"
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

DEFAULT_PROJECT_CONFIG_NAME: Final[str] = ".broker.toml"
"""Default project configuration filename."""

SIMPLEBROKER_MAGIC: Final[str] = "simplebroker-v1"
"""Magic string stored in database to verify it's a SimpleBroker database."""

SCHEMA_VERSION: Final[int] = 6
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

EXIT_INTERRUPTED: Final[int] = 130
"""Exit code when an unhandled keyboard interrupt reaches the CLI wrapper."""

# ==============================================================================
# MESSAGE AND QUEUE CONSTRAINTS
# ==============================================================================

MAX_MESSAGE_SIZE: Final[int] = 10 * 1024 * 1024  # 10MB limit
"""Maximum allowed message size in bytes (default: 10MB).

Can be overridden with BROKER_MAX_MESSAGE_SIZE environment variable.
Messages larger than this will be rejected with a ValueError.
"""

# Longest accepted queue name, in characters.
MAX_QUEUE_NAME_LENGTH: Final[int] = 512

# Vacuum eligibility's absolute claimed-row limit ([SB-OPS-6]: "more
# than 10,000 claimed messages" fires regardless of ratio).
VACUUM_CLAIMED_ABSOLUTE_LIMIT: Final[int] = 10_000

# A compound default database name is exactly one directory plus one
# filename (for example ".weft/broker.db"): two path parts.
COMPOUND_DB_NAME_PARTS: Final[int] = 2

# Longest single path component accepted from configuration; matches the
# common POSIX/NTFS per-component filename limit in bytes.
_MAX_PATH_COMPONENT_LENGTH: Final[int] = 255
_WINDOWS_MAX_PATH_LENGTH: Final[int] = 260

# Safe-display bounds: ASCII control characters below space (0x20) and
# DEL (0x7f) are hex-escaped before a rejected value is shown.
_ASCII_PRINTABLE_MIN: Final[int] = 32
_ASCII_DEL: Final[int] = 127
"""Maximum allowed length for queue names in characters."""

# ==============================================================================
# TIMESTAMP AND ID GENERATION
# ==============================================================================
# SimpleBroker uses hybrid timestamps that combine physical time with a logical
# counter to ensure uniqueness even under extreme concurrency.

TIMESTAMP_EXACT_NUM_DIGITS: Final[int] = 19
"""Exact number of digits required for message ID timestamps in string form."""

PHYSICAL_TIME_BITS: Final[int] = 52
"""Nominal physical-width constant retained for compatibility.

Generated IDs retain ``time.time_ns()`` magnitude and clear the low 12 bits;
they do not encode a 52-bit microsecond counter.
"""

LOGICAL_COUNTER_BITS: Final[int] = 12
"""Low bits reserved for ordering within one 4,096-nanosecond time grain."""

LOGICAL_COUNTER_MASK: Final[int] = (1 << LOGICAL_COUNTER_BITS) - 1
"""Bitmask for extracting the logical counter from a hybrid timestamp."""

MAX_LOGICAL_COUNTER: Final[int] = 1 << LOGICAL_COUNTER_BITS
"""Exclusive logical-counter bound (4096) before physical time must advance."""

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

DEFAULT_LOAD_MAX_FUTURE_SKEW_SECONDS: Final[int] = 300
"""Default allowed future dump-header skew before load refuses mutation."""

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

    SCHEMA = "schema"
    """Schema bootstrap and migrations."""

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

# NUL, the ASCII C0 controls, and DEL are unsafe across all platforms.
_COMMON_DANGEROUS_CHARS = [*(chr(value) for value in range(32)), "\x7f"]

# POSIX characters interpreted by owned pattern or expansion consumers.
_POSIX_PATH_CONSUMER_CHARS = [
    "[",
    "]",
    "*",
    "?",
    "~",
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
_unix_chars = _COMMON_DANGEROUS_CHARS + _POSIX_PATH_CONSUMER_CHARS
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


def _reject_dangerous_path_characters(
    path: str,
    context: str,
    *,
    is_windows: bool,
    dangerous_regex: re.Pattern[str],
) -> None:
    """Reject the first unsafe character while allowing a Windows drive colon."""
    unicode_control = next(
        (character for character in path if unicodedata.category(character) == "Cc"),
        None,
    )
    if unicode_control is not None:
        raise ValueError(
            f"{context} contains dangerous character '{unicode_control}': {path}. "
            "Path components must not contain reserved, internally interpreted, "
            "or control characters."
        )

    match = dangerous_regex.search(path)
    if match is None:
        return

    if is_windows and ":" in path and re.match(r"^[A-Za-z]:", path):
        match = dangerous_regex.search(path[2:])
        if match is None:
            return

    dangerous_char = match.group()
    raise ValueError(
        f"{context} contains dangerous character '{dangerous_char}': {path}. "
        "Path components must not contain reserved, internally interpreted, "
        "or control characters."
    )


def _validate_path_component(
    part: str,
    path: str,
    context: str,
    *,
    is_windows: bool,
) -> None:
    """Validate one already-separated path component."""
    if part == "..":
        raise ValueError(
            f"{context} must not contain parent directory references: {path}"
        )
    if part == ".":
        raise ValueError(
            f"{context} must not contain current directory references: {path}"
        )

    if is_windows and part.split(".")[0].upper() in _WINDOWS_RESERVED_NAMES:
        raise ValueError(
            f"{context} contains Windows reserved name '{part}': {path}. "
            "Avoid names like CON, PRN, AUX, NUL, COM1-9, LPT1-9."
        )

    if part.startswith(" ") or part.endswith(" "):
        raise ValueError(
            f"{context} component cannot start or end with spaces: '{part}' in {path}"
        )
    if len(part) > _MAX_PATH_COMPONENT_LENGTH:
        raise ValueError(
            f"{context} component too long (max 255 chars): '{part[:50]}...' in {path}"
        )


def _validate_safe_path_components(path: str, context: str = "path") -> None:
    """Validate lexical path components and platform-reserved names.

    This check rejects lexical ``.`` / ``..`` components and disallowed
    characters. It does not resolve symlinks or establish physical containment.

    Args:
        path: Path string to validate (can be filename or compound path)
        context: Description of what is being validated for error messages

    Raises:
        ValueError: If path contains dangerous characters or reserved names

    Validation checks:
        - Rejects lexical traversal components (..)
        - Blocks null bytes and control characters
        - Blocks the configured punctuation set
        - Blocks Windows reserved names (CON, PRN, AUX, etc.)
        - Validates each path component separately
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

    _reject_dangerous_path_characters(
        path,
        context,
        is_windows=is_windows,
        dangerous_regex=dangerous_regex,
    )

    # Check each path component
    for part in pure_path.parts:
        if not part:  # Empty component (e.g., double slashes)
            continue
        _validate_path_component(
            part,
            path,
            context,
            is_windows=is_windows,
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

    # POSIX limits depend on the filesystem and system call. Windows retains
    # the existing product rule for paths that do not use extended syntax.
    if is_windows and len(path) > _WINDOWS_MAX_PATH_LENGTH:
        raise ValueError(
            f"{context} too long (max {_WINDOWS_MAX_PATH_LENGTH} chars): "
            f"{len(path)} chars in {path[:50]}..."
        )


# Historical private-module imports remain available without an import cycle.
if TYPE_CHECKING:
    from . import config as _configuration

    _CONFIG_FIELDS = _configuration._CONFIG_FIELDS
    _CONFIG_NORMALIZERS = _configuration._CONFIG_NORMALIZERS
    ResolvedConfig = _configuration.ResolvedConfig
    _normalize_config_value = _configuration._normalize_config_value
    _overlay_config = _configuration._overlay_config
    _parse_bool = _configuration._parse_bool
    _parse_debug_flag = _configuration._parse_debug_flag
    _parse_load_max_future_skew = _configuration._parse_load_max_future_skew
    _parse_project_scope = _configuration._parse_project_scope
    _parse_strict_one_bool = _configuration._parse_strict_one_bool
    _parse_vacuum_threshold = _configuration._parse_vacuum_threshold
    _safe_config_value_display = _configuration._safe_config_value_display
    _validate_config = _configuration._validate_config
    load_config = _configuration.load_config
    resolve_config = _configuration.resolve_config
    resolve_isolated_config = _configuration.resolve_isolated_config
    snapshot_config = _configuration.snapshot_config


def __getattr__(name: str) -> Any:
    if name in {
        "_CONFIG_NORMALIZERS",
        "_parse_bool",
        "_normalize_config_value",
        "_overlay_config",
        "_parse_vacuum_threshold",
        "load_config",
        "resolve_isolated_config",
        "_parse_debug_flag",
        "snapshot_config",
        "_parse_project_scope",
        "_CONFIG_FIELDS",
        "_validate_config",
        "ResolvedConfig",
        "resolve_config",
        "_safe_config_value_display",
        "_parse_load_max_future_skew",
        "_parse_strict_one_bool",
    }:
        from . import config

        return getattr(config, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
