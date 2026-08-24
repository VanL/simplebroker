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

import os
import platform
import re
import warnings
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import PurePath
from types import MappingProxyType
from typing import Any, Final, overload

from ._exceptions import InvalidConfigError

# ==============================================================================
# VERSION INFORMATION
# ==============================================================================

__version__: Final[str] = "7.4.1"
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

SCHEMA_VERSION: Final[int] = 5
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


def _reject_dangerous_path_characters(
    path: str,
    context: str,
    *,
    is_windows: bool,
    dangerous_regex: re.Pattern[str],
) -> None:
    """Reject the first unsafe character while allowing a Windows drive colon."""
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
        "Path components must not contain shell metacharacters or control characters."
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
    if len(part) > 255:
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


def _parse_strict_one_bool(value: Any) -> bool:
    """Return True only for canonical truthy override values.

    This matches the environment parsing contract used by flags that only accept
    ``"1"`` from environment variables while still accepting typed booleans from
    callers that build override dictionaries directly.
    """

    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    return str(value) == "1"


def _parse_debug_flag(value: Any) -> bool:
    """Mirror ``bool(os.environ.get(...))`` while accepting typed booleans."""

    if isinstance(value, bool):
        return value
    return bool(value)


def _parse_vacuum_threshold(value: Any) -> float:
    """Normalize string percentages or typed ratio/percentage overrides."""

    if isinstance(value, str):
        return float(value) / 100

    numeric = float(value)
    if numeric > 1:
        return numeric / 100
    return numeric


def _parse_project_scope(value: Any) -> bool:
    """Normalize project-scope values from the environment or typed overrides."""
    return value if isinstance(value, bool) else _parse_bool(str(value))


def _parse_load_max_future_skew(value: Any) -> int:
    """Accept only integer values, while allowing integer environment strings."""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise TypeError(
            "BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS must be a non-negative integer"
        )
    try:
        normalized = int(value)
    except ValueError as exc:
        raise ValueError(
            "BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS must be a non-negative integer"
        ) from exc
    if normalized < 0:
        raise ValueError(
            "BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS must be a non-negative integer"
        )
    return normalized


@dataclass(frozen=True, slots=True)
class _ConfigField:
    """One configuration key's environment default and shared coercion."""

    default: str
    normalize: Callable[[Any], Any]
    expected: str


_CONFIG_FIELDS: Final[dict[str, _ConfigField]] = {
    "BROKER_BUSY_TIMEOUT": _ConfigField(
        "5000", int, "an integer number of milliseconds"
    ),
    "BROKER_CACHE_MB": _ConfigField("10", int, "an integer number of megabytes"),
    "BROKER_SYNC_MODE": _ConfigField(
        "FULL", lambda value: str(value).upper(), "FULL, NORMAL, or OFF"
    ),
    "BROKER_WAL_AUTOCHECKPOINT": _ConfigField("1000", int, "an integer page count"),
    "BROKER_MAX_MESSAGE_SIZE": _ConfigField(
        str(MAX_MESSAGE_SIZE), int, "an integer byte count"
    ),
    "BROKER_READ_COMMIT_INTERVAL": _ConfigField("1", int, "an integer message count"),
    "BROKER_GENERATOR_BATCH_SIZE": _ConfigField("100", int, "an integer message count"),
    "BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS": _ConfigField(
        str(DEFAULT_LOAD_MAX_FUTURE_SKEW_SECONDS),
        _parse_load_max_future_skew,
        "a non-negative integer number of seconds",
    ),
    "BROKER_AUTO_VACUUM": _ConfigField("1", int, "an integer flag"),
    "BROKER_AUTO_VACUUM_INTERVAL": _ConfigField(
        "100", int, "an integer mutation count"
    ),
    "BROKER_VACUUM_THRESHOLD": _ConfigField(
        "10", _parse_vacuum_threshold, "a numeric percentage"
    ),
    "BROKER_VACUUM_BATCH_SIZE": _ConfigField("1000", int, "an integer message count"),
    "BROKER_SKIP_IDLE_CHECK": _ConfigField(
        "0", _parse_strict_one_bool, "a boolean flag"
    ),
    "BROKER_JITTER_FACTOR": _ConfigField("0.15", float, "a numeric ratio"),
    "BROKER_INITIAL_CHECKS": _ConfigField("100", int, "an integer check count"),
    "BROKER_MAX_INTERVAL": _ConfigField("0.1", float, "a numeric number of seconds"),
    "BROKER_BURST_SLEEP": _ConfigField("0.00001", float, "a numeric number of seconds"),
    "BROKER_DEBUG": _ConfigField("", _parse_debug_flag, "a boolean flag"),
    "BROKER_LOGGING_ENABLED": _ConfigField(
        "0", _parse_strict_one_bool, "a boolean flag"
    ),
    "BROKER_DEFAULT_DB_LOCATION": _ConfigField(
        "", str, "an absolute directory path or empty string"
    ),
    "BROKER_DEFAULT_DB_NAME": _ConfigField(
        DEFAULT_DB_NAME, str, "a relative database path with at most one directory"
    ),
    "BROKER_PROJECT_CONFIG_PATH": _ConfigField(
        "", str, "an absolute directory or one relative directory"
    ),
    "BROKER_PROJECT_CONFIG_NAME": _ConfigField(
        DEFAULT_PROJECT_CONFIG_NAME,
        str,
        "a relative config path with at most one directory",
    ),
    "BROKER_PROJECT_SCOPE": _ConfigField("0", _parse_project_scope, "a boolean flag"),
    "BROKER_BACKEND": _ConfigField("sqlite", str, "a backend name"),
    "BROKER_BACKEND_HOST": _ConfigField("localhost", str, "a host name"),
    "BROKER_BACKEND_PORT": _ConfigField("5432", int, "an integer port"),
    "BROKER_BACKEND_USER": _ConfigField("postgres", str, "a user name"),
    "BROKER_BACKEND_PASSWORD": _ConfigField("", str, "a password string"),
    "BROKER_BACKEND_DATABASE": _ConfigField("simplebroker", str, "a database name"),
    "BROKER_BACKEND_SCHEMA": _ConfigField("simplebroker_pg_v1", str, "a schema name"),
    "BROKER_BACKEND_TARGET": _ConfigField("", str, "a backend target string"),
}
"""Canonical defaults and coercion shared by environment and overrides."""

_CONFIG_NORMALIZERS: Final[dict[str, Callable[[Any], Any]]] = {
    key: field.normalize for key, field in _CONFIG_FIELDS.items()
}


@dataclass(frozen=True, slots=True, init=False, repr=False)
class ResolvedConfig(Mapping[str, Any]):
    """Read-only, complete configuration resolved without ambient input.

    Construction uses the same canonical defaults, coercion, and validation as
    :func:`resolve_isolated_config` for recognized keys and preserves additional
    keys as opaque extension data. Converting it to an ordinary mapping discards
    its ambient-free marker guarantee.
    """

    _values: Mapping[str, Any]

    def __init__(self, values: Mapping[str, Any]) -> None:
        object.__setattr__(
            self,
            "_values",
            MappingProxyType(_resolve_isolated_values(values, preserve_unknown=True)),
        )

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


_SENSITIVE_CONFIG_KEYS: Final = frozenset(
    {"BROKER_BACKEND_PASSWORD", "BROKER_BACKEND_TARGET"}
)
_CONFIG_VALUE_DISPLAY_LIMIT: Final = 160


def _safe_config_value_display(key: str, value: Any) -> str:
    if key in _SENSITIVE_CONFIG_KEYS:
        return "<redacted>"
    if type(value) in (str, bytes, int, float, bool, type(None)):
        display = repr(value)
    else:
        display = f"<{type(value).__name__}>"
    display = "".join(
        f"\\x{ord(char):02x}" if ord(char) < 32 or ord(char) == 127 else char
        for char in display
    )
    if len(display) > _CONFIG_VALUE_DISPLAY_LIMIT:
        display = display[: _CONFIG_VALUE_DISPLAY_LIMIT - 3] + "..."
    return display


def _invalid_config_error(key: str, value: Any, *, source: str) -> InvalidConfigError:
    return InvalidConfigError(
        key=key,
        source=source,
        expected=_CONFIG_FIELDS[key].expected,
        value_display=_safe_config_value_display(key, value),
    )


def _normalize_config_value(key: str, value: Any, *, source: str) -> Any:
    normalization_value = value
    if isinstance(value, str) and type(value) is not str:
        normalization_value = str.__str__(value)
    try:
        return _CONFIG_FIELDS[key].normalize(normalization_value)
    except (TypeError, ValueError) as exc:
        raise _invalid_config_error(key, value, source=source) from exc


def _unknown_config_error(key: object, value: Any) -> InvalidConfigError:
    display_key = key if isinstance(key, str) else repr(key)
    return InvalidConfigError(
        key=display_key,
        source="override",
        expected="a recognized canonical BROKER_* configuration key",
        value_display=_safe_config_value_display(display_key, value),
    )


def _reject_unknown_config_keys(overrides: Mapping[str, Any]) -> None:
    """Reject the first non-canonical override key."""
    unknown = [key for key in overrides if key not in _CONFIG_FIELDS]
    if unknown:
        key = unknown[0]
        raise _unknown_config_error(key, overrides[key])


def _resolve_isolated_values(
    overrides: Mapping[str, Any],
    *,
    preserve_unknown: bool,
) -> dict[str, Any]:
    """Resolve canonical defaults plus ambient-free explicit values."""
    if not preserve_unknown:
        _reject_unknown_config_keys(overrides)

    config = {
        key: _normalize_config_value(key, field.default, source="default")
        for key, field in _CONFIG_FIELDS.items()
    }
    for key, value in overrides.items():
        config[key] = (
            _normalize_config_value(key, value, source="override")
            if key in _CONFIG_FIELDS
            else value
        )

    if config["BROKER_SYNC_MODE"] not in ("FULL", "NORMAL", "OFF"):
        config["BROKER_SYNC_MODE"] = "FULL"

    _validate_config(config, source="override")
    return config


def resolve_isolated_config(
    overrides: Mapping[str, Any],
    *,
    preserve_unknown: bool = False,
) -> ResolvedConfig:
    """Resolve explicit overrides from canonical defaults without ambient input."""
    if not preserve_unknown:
        _reject_unknown_config_keys(overrides)
    return ResolvedConfig(overrides)


@overload
def resolve_config(  # type: ignore[overload-overlap]
    overrides: ResolvedConfig,
) -> ResolvedConfig: ...


@overload
def resolve_config(
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]: ...


def resolve_config(
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any] | ResolvedConfig:
    """Resolve fresh ambient config or reuse an exact config snapshot.

    ``None`` and ordinary mappings start from the current environment, then
    apply caller overrides with the canonical normalization and validation
    rules. An exact :class:`ResolvedConfig` is already complete, so it is
    returned unchanged without reading the environment. Subclasses are
    reconstructed and revalidated without an ambient read.
    """

    if type(overrides) is ResolvedConfig:
        return overrides
    if isinstance(overrides, ResolvedConfig):
        return ResolvedConfig(overrides)

    config = load_config()
    if overrides is None:
        return config

    for key, value in overrides.items():
        normalizer = _CONFIG_NORMALIZERS.get(key)
        config[key] = (
            _normalize_config_value(key, value, source="override")
            if normalizer is not None
            else value
        )

    if config["BROKER_SYNC_MODE"] not in ("FULL", "NORMAL", "OFF"):
        config["BROKER_SYNC_MODE"] = "FULL"

    _validate_config(config, source="override")
    return config


def snapshot_config(
    config: Mapping[str, Any] | None = None,
) -> ResolvedConfig:
    """Resolve and retain one complete configuration snapshot."""
    if type(config) is ResolvedConfig:
        return config
    return ResolvedConfig(resolve_config(config))


def _overlay_config(
    base: ResolvedConfig,
    overrides: Mapping[str, Any] | None,
) -> ResolvedConfig:
    """Apply ambient-free per-call overrides to an owned snapshot."""
    if overrides is None:
        return base
    if type(overrides) is ResolvedConfig:
        return overrides
    if isinstance(overrides, ResolvedConfig):
        return ResolvedConfig(overrides)
    values = dict(base)
    values.update(overrides)
    return ResolvedConfig(values)


def _validate_default_database_location(config: dict[str, Any]) -> None:
    db_location = config["BROKER_DEFAULT_DB_LOCATION"]
    if not isinstance(db_location, str) or not db_location:
        return
    try:
        _validate_safe_path_components(db_location, "BROKER_DEFAULT_DB_LOCATION")
    except ValueError as exc:
        raise ValueError(
            f"BROKER_DEFAULT_DB_LOCATION validation failed: {exc}"
        ) from exc
    if not os.path.isabs(db_location):
        warnings.warn(
            f"BROKER_DEFAULT_DB_LOCATION must be an absolute path. "
            f"Ignoring relative path: {db_location}",
            UserWarning,
            stacklevel=3,
        )
        config["BROKER_DEFAULT_DB_LOCATION"] = ""


def _validate_default_database_name(config: dict[str, Any]) -> None:
    db_name = config["BROKER_DEFAULT_DB_NAME"]
    if not isinstance(db_name, str) or not db_name:
        return
    try:
        _validate_safe_path_components(db_name, "BROKER_DEFAULT_DB_NAME")
    except ValueError as exc:
        raise ValueError(f"BROKER_DEFAULT_DB_NAME validation failed: {exc}") from exc
    if os.path.isabs(db_name):
        raise ValueError(
            f"BROKER_DEFAULT_DB_NAME must be a relative path, not absolute: {db_name}. "
            f"Use BROKER_DEFAULT_DB_LOCATION to specify the directory instead."
        )
    if len(PurePath(db_name).parts) > 2:
        raise ValueError(
            f"Database name must not contain nested directories: {db_name}. "
            f"Only single directory level is supported (e.g., 'dir/name.db')"
        )


def _validate_project_config_location(config: dict[str, Any]) -> None:
    project_config_path = config["BROKER_PROJECT_CONFIG_PATH"]
    if not isinstance(project_config_path, str) or not project_config_path:
        return
    try:
        _validate_safe_path_components(
            project_config_path,
            "BROKER_PROJECT_CONFIG_PATH",
        )
    except ValueError as exc:
        raise ValueError(
            f"BROKER_PROJECT_CONFIG_PATH validation failed: {exc}"
        ) from exc
    if not os.path.isabs(project_config_path):
        parts = PurePath(project_config_path.replace("\\", "/")).parts
        if len(parts) > 1:
            raise ValueError(
                "BROKER_PROJECT_CONFIG_PATH must be an absolute path or a "
                f"single relative directory: {project_config_path}"
            )


def _validate_project_config_name(config: dict[str, Any]) -> None:
    project_config_name = config["BROKER_PROJECT_CONFIG_NAME"]
    if not isinstance(project_config_name, str) or not project_config_name:
        return
    try:
        _validate_safe_path_components(
            project_config_name,
            "BROKER_PROJECT_CONFIG_NAME",
        )
    except ValueError as exc:
        raise ValueError(
            f"BROKER_PROJECT_CONFIG_NAME validation failed: {exc}"
        ) from exc
    if os.path.isabs(project_config_name):
        raise ValueError(
            "BROKER_PROJECT_CONFIG_NAME must be a relative path, not "
            f"absolute: {project_config_name}. Use BROKER_PROJECT_CONFIG_PATH "
            "to specify the directory instead."
        )
    name_parts = PurePath(project_config_name.replace("\\", "/")).parts
    if len(name_parts) > 2:
        raise ValueError(
            f"Project config name must not contain nested directories: "
            f"{project_config_name}. Only single directory level is supported "
            "(e.g., 'dir/broker.toml')"
        )
    project_config_path = config["BROKER_PROJECT_CONFIG_PATH"]
    if (
        isinstance(project_config_path, str)
        and project_config_path
        and not os.path.isabs(project_config_path)
    ):
        path_parts = PurePath(project_config_path.replace("\\", "/")).parts
        if len(path_parts) + len(name_parts) > 2:
            raise ValueError(
                "BROKER_PROJECT_CONFIG_PATH and BROKER_PROJECT_CONFIG_NAME "
                "must not combine into nested directories. Only single "
                "directory level is supported (e.g., 'dir/broker.toml')"
            )


def _validate_config(config: dict[str, Any], *, source: str) -> None:
    validators = (
        ("BROKER_DEFAULT_DB_LOCATION", _validate_default_database_location),
        ("BROKER_DEFAULT_DB_NAME", _validate_default_database_name),
        ("BROKER_PROJECT_CONFIG_PATH", _validate_project_config_location),
        ("BROKER_PROJECT_CONFIG_NAME", _validate_project_config_name),
    )
    for key, validator in validators:
        try:
            validator(config)
        except ValueError as exc:
            raise _invalid_config_error(key, config[key], source=source) from exc


def load_config() -> dict[str, Any]:
    """Load configuration from environment variables.

    This function reads all SimpleBroker environment variables and returns
    a configuration dictionary with validated values. Each call is a fresh
    read for an explicit resolution boundary. Library handles that need stable
    process behavior should retain and reuse :func:`snapshot_config` instead
    of caching this result at module import.

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
                - "NORMAL": May improve write throughput, small risk on power loss
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

            BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS (int): Maximum physical seconds
                a dump header may lead local wall time before load refuses by
                default. Default: 300. Must be non-negative.

        Vacuum Settings:
            BROKER_AUTO_VACUUM (int): Enable automatic vacuum of claimed messages.
                Default: 1 (enabled)
                Set to 0 to disable opportunistic cleanup checks.

            BROKER_AUTO_VACUUM_INTERVAL (int): Successful committed message mutations
                between checks on one long-lived broker core.
                Default: 100
                Values below 1 are normalized internally to 1, preserving the
                historical check-after-every-mutation behavior. The schedule is
                in-memory and per core; it is not a timer or background process.

            BROKER_VACUUM_THRESHOLD (float): Claimed-message ratio that triggers
                vacuum. Default: 0.1 (10%). String/environment inputs are
                percentages; typed numeric inputs in [0, 1] are ratios and
                typed values over 1 are percentages.

            BROKER_VACUUM_BATCH_SIZE (int): Messages to delete per vacuum batch.
                Default: 1000
                Larger batches are faster but hold locks longer.

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
                Can be a compound path (e.g. "subdir/.broker.db"), but it
                must remain relative and contain at most one directory level.

            BROKER_PROJECT_CONFIG_PATH (str): Optional directory prefix for
                project config discovery.
                Default: "" (search directly in each candidate project directory)
                Relative values are resolved beneath each candidate project
                directory. Absolute values point at one configured directory.

            BROKER_PROJECT_CONFIG_NAME (str): Project config filename.
                Default: ".broker.toml"
                Used for project config discovery and explicit-root resolution.
                Can be a compound path (e.g. ".weft/broker.toml") with one
                directory level.

            BROKER_PROJECT_SCOPE (bool): Enable git-like upward database search.
                Default: False
                Set to "1", "true", "yes", or "on" to enable.
                When enabled, searches upward through directory hierarchy
                to find existing databases before creating new ones.

        Backend Selection:
            BROKER_BACKEND (str): Backend name.
                Default: "sqlite"

            BROKER_BACKEND_HOST (str): Postgres host.
                Default: "localhost"

            BROKER_BACKEND_PORT (int): Postgres port.
                Default: 5432

            BROKER_BACKEND_USER (str): Postgres user.
                Default: "postgres"

            BROKER_BACKEND_PASSWORD (str): Postgres password.
                Default: ""

            BROKER_BACKEND_DATABASE (str): Postgres database name.
                Default: "simplebroker"

            BROKER_BACKEND_SCHEMA (str): Postgres schema name.
                Default: "simplebroker_pg_v1"

            BROKER_BACKEND_TARGET (str): Full Postgres DSN/conninfo string.
                Default: ""

    """
    config = {
        key: _normalize_config_value(
            key,
            os.environ.get(key, field.default),
            source="environment",
        )
        for key, field in _CONFIG_FIELDS.items()
    }

    # Validate SYNC_MODE
    if config["BROKER_SYNC_MODE"] not in ("FULL", "NORMAL", "OFF"):
        config["BROKER_SYNC_MODE"] = "FULL"

    _validate_config(config, source="environment")
    return config


# ~
