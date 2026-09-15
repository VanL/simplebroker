"""Shared constants, field definitions and explicit configuration resolution.

External env/TOML keys carry a namespace. Resolved keys are uppercase and
unprefixed; values retain the units declared in DEFAULT_CONFIG.
"""

from __future__ import annotations

import json
import math
import os
import platform
import re
import tomllib
import unicodedata
import warnings
from collections.abc import Callable, Iterable, Iterator, Mapping
from copy import copy
from dataclasses import dataclass
from pathlib import PurePath
from types import MappingProxyType
from typing import Any, Final


class BrokerError(Exception):
    """Base exception for all SimpleBroker errors."""


class InvalidConfigError(BrokerError, ValueError):
    """A recognized configuration value could not be parsed or validated."""

    __slots__ = ("_expected", "_key", "_source", "_value_display")

    def __init__(
        self,
        *,
        key: str,
        source: str,
        expected: str,
        value_display: str,
    ) -> None:
        self._key = key
        self._source = source
        self._expected = expected
        self._value_display = value_display
        super().__init__(
            f"invalid configuration {key}={value_display}: expected {expected}"
        )

    @property
    def key(self) -> str:
        return self._key

    @property
    def source(self) -> str:
        return self._source

    @property
    def expected(self) -> str:
        return self._expected

    @property
    def value_display(self) -> str:
        return self._value_display


# ==============================================================================
# VERSION INFORMATION
# ==============================================================================

__version__: Final[str] = "8.3.0"
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
"""Maximum allowed length for queue names in characters."""

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

# ==============================================================================
# TIMESTAMP AND ID GENERATION
# ==============================================================================
# SimpleBroker uses hybrid timestamps that combine physical time with a logical
# counter to ensure uniqueness even under extreme concurrency.

TIMESTAMP_EXACT_NUM_DIGITS: Final[int] = 19
"""Exact number of digits required for message ID timestamps in string form."""

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


def _path_diagnostic_preview(value: str) -> str:
    """Keep rejected path/name previews bounded without changing admission."""
    if len(value) > _MAX_PATH_COMPONENT_LENGTH:
        return value[:_MAX_PATH_COMPONENT_LENGTH] + "..."
    return value


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
            f"{context} contains dangerous character '{unicode_control}': "
            f"{_path_diagnostic_preview(path)}. "
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
        f"{context} contains dangerous character '{dangerous_char}': "
        f"{_path_diagnostic_preview(path)}. "
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
            f"{context} must not contain parent directory references: "
            f"{_path_diagnostic_preview(path)}"
        )
    if part == ".":
        raise ValueError(
            f"{context} must not contain current directory references: "
            f"{_path_diagnostic_preview(path)}"
        )

    if is_windows and part.split(".")[0].upper() in _WINDOWS_RESERVED_NAMES:
        raise ValueError(
            f"{context} contains Windows reserved name "
            f"'{_path_diagnostic_preview(part)}': {_path_diagnostic_preview(path)}. "
            "Avoid names like CON, PRN, AUX, NUL, COM1-9, LPT1-9."
        )

    if part.startswith(" ") or part.endswith(" "):
        raise ValueError(
            f"{context} component cannot start or end with spaces: "
            f"'{_path_diagnostic_preview(part)}' in {_path_diagnostic_preview(path)}"
        )
    if len(part) > _MAX_PATH_COMPONENT_LENGTH:
        raise ValueError(
            f"{context} component too long (max 255 chars): "
            f"'{_path_diagnostic_preview(part)}'"
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
            f"{context} must not contain current directory references: "
            f"{_path_diagnostic_preview(path)}"
        )

    # POSIX limits depend on the filesystem and system call. Windows retains
    # the existing product rule for paths that do not use extended syntax.
    if is_windows and len(path) > _WINDOWS_MAX_PATH_LENGTH:
        raise ValueError(
            f"{context} too long (max {_WINDOWS_MAX_PATH_LENGTH} chars): "
            f"{len(path)} chars in {_path_diagnostic_preview(path)}"
        )


# ==============================================================================
# CONFIGURATION DEFAULTS AND VALIDATORS
# ==============================================================================

# Typed defaults in the units declared below; embedders copy the field table
# to replace these defaults without changing the package's shared values.
DEFAULT_BUSY_TIMEOUT_MS: Final[int] = 5000
DEFAULT_CACHE_MB: Final[int] = 10
DEFAULT_SYNC_MODE: Final[str] = "FULL"
DEFAULT_WAL_AUTOCHECKPOINT: Final[int] = 1000
DEFAULT_READ_COMMIT_INTERVAL: Final[int] = 1
DEFAULT_GENERATOR_BATCH_SIZE: Final[int] = 100
DEFAULT_AUTO_VACUUM: Final[int] = 1
DEFAULT_AUTO_VACUUM_INTERVAL: Final[int] = 100
PERCENT_SCALE: Final[int] = 100
DEFAULT_VACUUM_THRESHOLD_PCT: Final[float] = 10.0
DEFAULT_VACUUM_BATCH_SIZE: Final[int] = 1000
DEFAULT_SKIP_IDLE_CHECK: Final[bool] = False
DEFAULT_JITTER_FACTOR: Final[float] = 0.15
DEFAULT_INITIAL_CHECKS: Final[int] = 100
DEFAULT_MAX_INTERVAL: Final[float] = 0.1
DEFAULT_BURST_SLEEP: Final[float] = 0.00001
DEFAULT_DEBUG: Final[bool] = False
DEFAULT_LOGGING_ENABLED: Final[bool] = False
DEFAULT_DB_LOCATION: Final[str] = ""
DEFAULT_PROJECT_CONFIG_PATH: Final[str] = ""
DEFAULT_PROJECT_SCOPE: Final[bool] = False
DEFAULT_BACKEND: Final[str] = "sqlite"
DEFAULT_BACKEND_HOST: Final[str] = "localhost"
DEFAULT_BACKEND_PORT: Final[int] = 5432
DEFAULT_BACKEND_USER: Final[str] = "postgres"
DEFAULT_BACKEND_PASSWORD: Final[str] = ""
DEFAULT_BACKEND_DATABASE: Final[str] = "simplebroker"
DEFAULT_BACKEND_SCHEMA: Final[str] = "simplebroker_pg_v1"
DEFAULT_BACKEND_TARGET: Final[str] = ""


def _percent(value: Any) -> float:
    """Accept a percentage between zero and 100, stored as a percentage."""
    if isinstance(value, bool):
        raise TypeError("percentage must be a number, not a boolean")
    result = float(value)
    if not 0 <= result <= PERCENT_SCALE:
        raise ValueError("percentage must be between 0 and 100")
    return result


def _strict_one_bool(value: Any) -> bool:
    """Accept the existing strict-one flag grammar and typed booleans."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    return str(value) == "1"


def _debug_flag(value: Any) -> bool:
    """A nonempty debug value enables debugging, including the string '0'."""
    return bool(value)


def _project_scope(value: Any) -> bool:
    """Accept typed booleans or the standard 1/true/yes/on grammar."""
    if isinstance(value, bool):
        return value
    return str(value).lower().strip() in ("1", "true", "yes", "on")


def _load_max_future_skew(value: Any) -> int:
    """Require non-negative integer seconds, without rounding or booleans."""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise TypeError("must be a non-negative integer")
    result = int(value)
    if result < 0:
        raise ValueError("must be a non-negative integer")
    return result


def _sync_mode(value: Any) -> str:
    """Normalize SQLite sync mode, retaining the FULL fallback."""
    result = str(value).upper()
    return result if result in ("FULL", "NORMAL", "OFF") else DEFAULT_SYNC_MODE


def _db_location_path(value: Any) -> str:
    """Accept an empty string or an absolute host directory, preserving spelling."""
    result = str(value)
    if result and not os.path.isabs(result):
        raise ValueError("DEFAULT_DB_LOCATION must be an absolute path")
    return result


def _validate_db_name_component(name: str, context: str = "Database name") -> None:
    """Admit one SQLite name component under [SB-CLI-2], including old checks."""
    allowed = "use only ASCII letters, digits, dot, dash, and underscore"
    try:
        _validate_safe_path_components(name, context)
    except ValueError as error:
        raise ValueError(f"{error}; {allowed}") from error
    if re.fullmatch(r"[A-Za-z0-9._-]+", name) is None:
        raise ValueError(f"{context} must {allowed}: {name!r}")


def _validate_sqlite_filename(path: str, context: str = "Database filename") -> None:
    """Validate a filesystem target's terminal name, without constraining parents."""
    # Unlike Path.name, basename preserves a terminal '.' or empty component.
    _validate_db_name_component(os.path.basename(path), context)


def _db_name_path(value: Any) -> str:
    """Accept a database filename with at most one relative directory."""
    result = str(value)
    _validate_safe_path_components(result, "DEFAULT_DB_NAME")
    if os.path.isabs(result):
        raise ValueError("database name must be relative")
    parts = result.replace("\\", "/").split("/")
    for part in parts:
        _validate_db_name_component(part, "DEFAULT_DB_NAME")
    if len(parts) > COMPOUND_DB_NAME_PARTS:
        raise ValueError("database name must not contain nested directories")
    return result


def _project_config_path(value: Any) -> str:
    """Accept an absolute directory or one relative directory."""
    result = str(value)
    if result and not os.path.isabs(result):
        _validate_safe_path_components(result, "PROJECT_CONFIG_PATH")
        if len(PurePath(result.replace("\\", "/")).parts) > 1:
            raise ValueError("must be an absolute path or a single relative directory")
    return result


def _project_config_name(value: Any) -> str:
    """Accept a config filename with at most one relative directory."""
    result = str(value)
    if result:
        _validate_safe_path_components(result, "PROJECT_CONFIG_NAME")
        if os.path.isabs(result):
            raise ValueError("project config name must be relative")
        if len(PurePath(result.replace("\\", "/")).parts) > COMPOUND_DB_NAME_PARTS:
            raise ValueError("project config name must not contain nested directories")
    return result


@dataclass(frozen=True, slots=True)
class ConfigField:
    """One default, its declared unit or description, and optional validator."""

    default: Any
    description: str
    validator: Callable[[Any], Any] | None = None
    sensitive: bool = False


# The external namespace used when no base or explicit prefix is supplied.
DEFAULT_PREFIX: Final[str] = "BROKER"

# The single field table pairs defaults with their meaning and one coercer.
DEFAULT_CONFIG: Final[Mapping[str, ConfigField]] = MappingProxyType(
    {
        "BUSY_TIMEOUT": ConfigField(
            DEFAULT_BUSY_TIMEOUT_MS, "an integer number of milliseconds", int
        ),
        "CACHE_MB": ConfigField(
            DEFAULT_CACHE_MB, "an integer number of megabytes", int
        ),
        "SYNC_MODE": ConfigField(DEFAULT_SYNC_MODE, "FULL, NORMAL, or OFF", _sync_mode),
        "WAL_AUTOCHECKPOINT": ConfigField(
            DEFAULT_WAL_AUTOCHECKPOINT, "an integer page count", int
        ),
        "MAX_MESSAGE_SIZE": ConfigField(MAX_MESSAGE_SIZE, "an integer byte count", int),
        "READ_COMMIT_INTERVAL": ConfigField(
            DEFAULT_READ_COMMIT_INTERVAL, "an integer message count", int
        ),
        "GENERATOR_BATCH_SIZE": ConfigField(
            DEFAULT_GENERATOR_BATCH_SIZE, "an integer message count", int
        ),
        "LOAD_MAX_FUTURE_SKEW_SECONDS": ConfigField(
            DEFAULT_LOAD_MAX_FUTURE_SKEW_SECONDS,
            "a non-negative integer number of seconds",
            _load_max_future_skew,
        ),
        "AUTO_VACUUM": ConfigField(DEFAULT_AUTO_VACUUM, "an integer flag", int),
        "AUTO_VACUUM_INTERVAL": ConfigField(
            DEFAULT_AUTO_VACUUM_INTERVAL, "an integer mutation count", int
        ),
        "VACUUM_THRESHOLD": ConfigField(
            DEFAULT_VACUUM_THRESHOLD_PCT,
            "a number between 0 and 100, excluding booleans",
            _percent,
        ),
        "VACUUM_BATCH_SIZE": ConfigField(
            DEFAULT_VACUUM_BATCH_SIZE, "an integer message count", int
        ),
        "SKIP_IDLE_CHECK": ConfigField(
            DEFAULT_SKIP_IDLE_CHECK, "a boolean flag", _strict_one_bool
        ),
        "JITTER_FACTOR": ConfigField(DEFAULT_JITTER_FACTOR, "a numeric ratio", float),
        "INITIAL_CHECKS": ConfigField(
            DEFAULT_INITIAL_CHECKS, "an integer check count", int
        ),
        "MAX_INTERVAL": ConfigField(
            DEFAULT_MAX_INTERVAL, "a numeric number of seconds", float
        ),
        "BURST_SLEEP": ConfigField(
            DEFAULT_BURST_SLEEP, "a numeric number of seconds", float
        ),
        "DEBUG": ConfigField(DEFAULT_DEBUG, "a boolean flag", _debug_flag),
        "LOGGING_ENABLED": ConfigField(
            DEFAULT_LOGGING_ENABLED, "a boolean flag", _strict_one_bool
        ),
        "DEFAULT_DB_LOCATION": ConfigField(
            DEFAULT_DB_LOCATION,
            "an absolute directory path or empty string",
            _db_location_path,
        ),
        "DEFAULT_DB_NAME": ConfigField(
            DEFAULT_DB_NAME,
            "a relative database path with at most one directory; components use only ASCII letters, digits, dot, dash, and underscore",
            _db_name_path,
        ),
        "PROJECT_CONFIG_PATH": ConfigField(
            DEFAULT_PROJECT_CONFIG_PATH,
            "an absolute directory or one relative directory",
            _project_config_path,
        ),
        "PROJECT_CONFIG_NAME": ConfigField(
            DEFAULT_PROJECT_CONFIG_NAME,
            "a relative config path with at most one directory",
            _project_config_name,
        ),
        "PROJECT_SCOPE": ConfigField(
            DEFAULT_PROJECT_SCOPE, "a boolean flag", _project_scope
        ),
        "BACKEND": ConfigField(DEFAULT_BACKEND, "a backend name", str),
        "BACKEND_HOST": ConfigField(DEFAULT_BACKEND_HOST, "a host name", str),
        "BACKEND_PORT": ConfigField(DEFAULT_BACKEND_PORT, "an integer port", int),
        "BACKEND_USER": ConfigField(DEFAULT_BACKEND_USER, "a user name", str),
        "BACKEND_PASSWORD": ConfigField(
            DEFAULT_BACKEND_PASSWORD, "a password string", str, sensitive=True
        ),
        "BACKEND_DATABASE": ConfigField(
            DEFAULT_BACKEND_DATABASE, "a database name", str
        ),
        "BACKEND_SCHEMA": ConfigField(DEFAULT_BACKEND_SCHEMA, "a schema name", str),
        "BACKEND_TARGET": ConfigField(
            DEFAULT_BACKEND_TARGET, "a backend target string", str, sensitive=True
        ),
    }
)


class Config(Mapping[str, Any]):
    """A shallow, read-only copy of resolved values in their declared units."""

    def __init__(
        self,
        values: Mapping[str, Any],
        *,
        prefix: str = DEFAULT_PREFIX,
        defaults: Mapping[str, ConfigField] = DEFAULT_CONFIG,
    ) -> None:
        self._values = MappingProxyType(dict(values))
        self._prefix = prefix
        # Derivation needs the field validators as well as resolved values.
        # Keep a shallow declaration snapshot, not another configuration object.
        self._defaults = MappingProxyType(dict(defaults))

    def __getstate__(self) -> Any:
        """Use ordinary pickle for values, declarations and subclass state."""
        state = super().__getstate__()
        attributes, slots = state if isinstance(state, tuple) else (state, None)
        attributes = dict(attributes)
        attributes["_values"] = dict(self._values)
        attributes["_defaults"] = dict(self._defaults)
        return attributes if slots is None else (attributes, slots)

    def __setstate__(self, state: Any) -> None:
        attributes, slots = state if isinstance(state, tuple) else (state, None)
        attributes = dict(attributes)
        attributes["_values"] = MappingProxyType(attributes["_values"])
        attributes["_defaults"] = MappingProxyType(attributes["_defaults"])
        self.__dict__.update(attributes)
        if slots is not None:
            for name, value in slots.items():
                setattr(self, name, value)

    @property
    def prefix(self) -> str:
        """External namespace inherited by an explicitly derived configuration."""
        return self._prefix

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


# External suffixes and declarations use ASCII uppercase identifier spelling.
_CONFIG_NAME = re.compile(r"[A-Z][A-Z0-9_]*\Z")
_CONFIG_VALUE_DISPLAY_LIMIT: Final[int] = 160


def _safe_config_value_display(value: Any, *, sensitive: bool = False) -> str:
    if sensitive:
        return "<redacted>"
    if type(value) in (str, bytes, int, float, bool, type(None)):
        display = repr(value)
    else:
        display = f"<{type(value).__name__}>"
    display = "".join(
        f"\\x{ord(char):02x}"
        if ord(char) < _ASCII_PRINTABLE_MIN or ord(char) == _ASCII_DEL
        else char
        for char in display
    )
    if len(display) > _CONFIG_VALUE_DISPLAY_LIMIT:
        display = display[: _CONFIG_VALUE_DISPLAY_LIMIT - 3] + "..."
    return display


def _check_whole_config(
    values: dict[str, Any],
    *,
    prefix: str,
    source: str,
    defaults: Mapping[str, ConfigField],
) -> None:
    """Apply the combined project-config path rule to the final values."""
    path = values.get("PROJECT_CONFIG_PATH")
    name = values.get("PROJECT_CONFIG_NAME")
    if (
        isinstance(path, str)
        and path
        and not os.path.isabs(path)
        and isinstance(name, str)
        and name
        and len(PurePath(path.replace("\\", "/")).parts)
        + len(PurePath(name.replace("\\", "/")).parts)
        > COMPOUND_DB_NAME_PARTS
    ):
        field = defaults.get("PROJECT_CONFIG_NAME")
        raise InvalidConfigError(
            key=f"{prefix}_PROJECT_CONFIG_NAME",
            source=source,
            expected="PROJECT_CONFIG_PATH and PROJECT_CONFIG_NAME not to combine into nested directories",
            value_display=_safe_config_value_display(
                name, sensitive=bool(field and field.sensitive)
            ),
        )


def _validated_value(field: ConfigField, value: Any, *, key: str, source: str) -> Any:
    """Return a field's normalized value or raise with safe diagnostics."""
    assert field.validator is not None
    try:
        # Built-in coercers can invoke a str subclass's hostile repr while
        # formatting a parse error. Keep diagnostics owned here.
        normalized = str.__str__(value) if isinstance(value, str) else value
        return field.validator(normalized)
    except (TypeError, ValueError, OverflowError) as exc:
        raise InvalidConfigError(
            key=key,
            source=source,
            expected=field.description,
            value_display=_safe_config_value_display(value, sensitive=field.sensitive),
        ) from exc


def _source_phrase(source: str) -> str:
    """Name a configuration source for a one-line diagnostic."""
    return {
        "default": "defaults",
        "environment": "the environment",
        "file": "the TOML file",
        "override": "overrides",
        "TOML mapping": "the TOML mapping",
    }.get(source, source)


def _warn_invalid(failure: InvalidConfigError) -> None:
    """Report an invalid value as its source is applied."""
    warnings.warn(
        f"ignoring invalid {failure.key}={failure.value_display} from "
        f"{_source_phrase(failure.source)} (expected {failure.expected})",
        UserWarning,
        stacklevel=1,
    )


def _external_config_key(
    supplied_key: Any,
    value: Any,
    *,
    marker: str,
    location: str,
    defaults: Mapping[str, ConfigField],
    strict: bool = False,
) -> str | None:
    """Select one external namespace suffix, warning only for declared near misses."""
    selected = isinstance(supplied_key, str) and supplied_key.startswith(marker)
    key = supplied_key[len(marker) :] if selected else ""
    if selected and _CONFIG_NAME.fullmatch(key):
        return key
    if strict:
        raise ValueError(
            f"{location} configuration key {_safe_config_value_display(supplied_key)} "
            f"must match {marker}[A-Z][A-Z0-9_]*"
        )
    if not selected:
        return None
    candidate = key.upper()
    if candidate in defaults:
        field = defaults[candidate]
        warnings.warn(
            f"ignoring {supplied_key} from {_source_phrase(location)}: did you mean "
            f"{marker}{candidate}? "
            f"(value {_safe_config_value_display(value, sensitive=field.sensitive)})",
            UserWarning,
            stacklevel=1,
        )
    return None


def _read_config_toml(
    path: os.PathLike[str] | str, *, prefix: str
) -> Mapping[str, Any]:
    """Read an explicitly supplied document with config error metadata."""
    try:
        with open(path, "rb") as stream:
            return tomllib.load(stream)
    except (OSError, ValueError) as exc:
        raise InvalidConfigError(
            key=f"{prefix}_CONFIG_FILE",
            source="file",
            expected="a readable TOML document",
            value_display=_safe_config_value_display(os.fspath(path)),
        ) from exc


def _validate_config_names(names: Iterable[Any], source: str) -> None:
    """Reject malformed caller-controlled names without restricting custom fields."""
    for key in names:
        if not isinstance(key, str) or not _CONFIG_NAME.fullmatch(key):
            raise ValueError(
                f"{source} configuration key {_safe_config_value_display(key)} "
                "must match [A-Z][A-Z0-9_]* (uppercase unprefixed name)"
            )


def _require_mapping(supplied: object, source: str) -> Mapping[str, Any]:
    """Name the caller's argument when a configuration source is not a mapping."""
    if not isinstance(supplied, Mapping):
        parameter = {"environment": "env", "file": "toml"}.get(source, source)
        raise TypeError(
            f"{parameter} configuration must be a mapping, "
            f"not {type(supplied).__name__}"
        )
    return supplied


def _from_supplied_config(
    config: object,
    *,
    prefix: str | None,
    defaults: Mapping[str, ConfigField] | None,
    override: Mapping[str, Any] | None,
) -> Config:
    """Return an explicitly supplied Config, or derive from it with an override."""
    if not isinstance(config, Config):
        raise TypeError(
            f"config must be a Config, not {type(config).__name__}; "
            "build one with resolve_config()"
        )
    if prefix is not None and prefix != config.prefix:
        raise ValueError(
            f"prefix {prefix!r} differs from the supplied config's prefix "
            f"{config.prefix!r}"
        )
    if defaults is not None and dict(defaults) != dict(config._defaults):
        raise ValueError(
            "defaults differ from the supplied config's field declarations"
        )
    if override is not None:
        _require_mapping(override, "override")
    if not override:
        return config
    # An already-resolved config absorbed its TOML and environment when it was
    # built, so deriving reads neither again and does not revalidate its values.
    return _resolve_sources(
        config.prefix,
        defaults=config._defaults,
        derive_from=config,
        toml=None,
        env=None,
        override=override,
    )


def resolve_config(
    prefix: str | None = None,
    *,
    defaults: Mapping[str, ConfigField] | None = None,
    toml: os.PathLike[str] | str | Mapping[str, Any] | None = None,
    env: Mapping[str, str] | None = None,
    override: Mapping[str, Any] | None = None,
    config: Config | None = None,
) -> Config:
    """Resolve defaults < TOML < environment < overrides.

    Environment input is explicit. TOML and env select only names under the
    supplied prefix. Override uses that same namespace and rejects invalid names.
    Every supplied value is validated; an invalid value warns as its source is
    applied. If any invalid value remains after all sources, the first raises.
    An explicitly supplied ``config`` is returned unchanged without reading TOML
    or the environment, or derived from when an override is also supplied; its
    namespace and field declarations are inherited and cannot be rebound.
    """
    if config is not None:
        return _from_supplied_config(
            config, prefix=prefix, defaults=defaults, override=override
        )
    if defaults is not None and defaults is not DEFAULT_CONFIG:
        _validate_config_names(defaults, "defaults")
    return _resolve_sources(
        DEFAULT_PREFIX if prefix is None else prefix,
        defaults=DEFAULT_CONFIG if defaults is None else defaults,
        derive_from=None,
        toml=toml,
        env=env,
        override=override,
    )


def _resolve_sources(
    prefix: str,
    *,
    defaults: Mapping[str, ConfigField],
    derive_from: Config | None,
    toml: os.PathLike[str] | str | Mapping[str, Any] | None,
    env: Mapping[str, str] | None,
    override: Mapping[str, Any] | None,
) -> Config:
    """Apply defaults or a resolved config, then TOML, environment and override."""
    values: dict[str, Any] = {}
    origins: dict[str, str] = {}
    failures: dict[str, InvalidConfigError] = {}
    marker = prefix + "_"
    sources = (
        (
            "default",
            {
                key: copy(field.default)
                if isinstance(field.default, (dict, list, set, bytearray))
                else field.default
                for key, field in defaults.items()
            }
            if derive_from is None
            else {},
        ),
        ("config", derive_from),
        ("file", toml),
        ("environment", env),
        ("override", override),
    )
    for source, supplied in sources:
        if supplied is None:
            continue
        location = "TOML mapping" if source == "file" else source
        if source == "file" and not isinstance(supplied, Mapping):
            location = os.fspath(supplied)
            supplied = _read_config_toml(supplied, prefix=prefix)
        supplied = _require_mapping(supplied, source)
        for supplied_key, value in supplied.items():
            key = supplied_key
            if source in ("file", "environment", "override"):
                selected_key = _external_config_key(
                    supplied_key,
                    value,
                    marker=marker,
                    location=location,
                    defaults=defaults,
                    strict=source == "override",
                )
                if selected_key is None:
                    continue
                key = selected_key
            failures.pop(key, None)
            # Values carried by an already-resolved config are not revalidated.
            field = defaults.get(key) if source != "config" else None
            if field is not None and field.validator is not None:
                try:
                    value = _validated_value(
                        field, value, key=marker + key, source=source
                    )
                except InvalidConfigError as failure:
                    _warn_invalid(failure)
                    failures[key] = failure
                    continue
            values[key] = value
            origins[key] = source
    if failures:
        raise next(iter(failures.values()))
    _check_whole_config(
        values,
        prefix=prefix,
        source=origins.get("PROJECT_CONFIG_NAME", "default"),
        defaults=defaults,
    )
    return Config(values, prefix=prefix, defaults=defaults)


def _check_json_value(value: Any, active: set[int]) -> None:
    """Reject values that JSON would coerce, lose, or cannot represent."""
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("config transport requires finite floats")
        return
    if not isinstance(value, (list, dict)):
        raise TypeError("config transport contains an unsupported JSON value")
    identity = id(value)
    if identity in active:
        raise ValueError("config transport contains a cyclic container")
    active.add(identity)
    try:
        if isinstance(value, dict):
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError("config transport requires string object keys")
                _check_json_value(item, active)
        else:
            for item in value:
                _check_json_value(item, active)
    finally:
        active.remove(identity)


def serialize_config(config: Config) -> str:
    """Transport all resolved values and their prefix as JSON, without validators.

    The payload may contain credentials. It is not a redacted diagnostic.
    """
    payload = {"prefix": config.prefix, "values": dict(config)}
    _check_json_value(payload, set())
    return json.dumps(payload, allow_nan=False, sort_keys=True, separators=(",", ":"))


def deserialize_config(
    payload: str | Mapping[str, Any],
    *,
    defaults: Mapping[str, ConfigField] = DEFAULT_CONFIG,
) -> Config:
    """Rebuild JSON config data using receiver-owned fields and validators.

    No ambient environment or TOML is read. Additional envelope metadata is
    ignored; it cannot select declarations or executable code.
    """
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, Mapping):
        raise ValueError("config transport requires an object envelope")  # noqa: TRY004 approved [DOM-10.1.1] [RUFF-SUP-038] exception
    prefix = payload.get("prefix")
    values = payload.get("values")
    if not isinstance(prefix, str) or not isinstance(values, Mapping):
        raise ValueError(  # noqa: TRY004 approved [DOM-10.1.1] [RUFF-SUP-038] exception
            "config transport requires a string prefix and object values"
        )
    values = dict(values)
    _check_json_value(values, set())
    return resolve_config(
        prefix,
        defaults=defaults,
        override={prefix + "_" + key: value for key, value in values.items()},
    )
