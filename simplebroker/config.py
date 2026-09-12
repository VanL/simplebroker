"""Shared configuration resolution and canonical snapshots.

External prefixes are a naming boundary. All configuration values are stored
once under canonical names; legacy views preserve the published broker API.
"""

from __future__ import annotations

import os
import re
import tomllib
import warnings
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, replace
from datetime import date, time
from pathlib import PurePath
from types import MappingProxyType
from typing import Any, Final, overload

from ._constants import (
    _ASCII_DEL,
    _ASCII_PRINTABLE_MIN,
    COMPOUND_DB_NAME_PARTS,
    DEFAULT_DB_NAME,
    DEFAULT_LOAD_MAX_FUTURE_SKEW_SECONDS,
    DEFAULT_PROJECT_CONFIG_NAME,
    MAX_MESSAGE_SIZE,
    _validate_safe_path_components,
)
from ._exceptions import InvalidConfigError


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


# Canonical BROKER_* field registry: default, coercion, and expected
# form for every recognized configuration key.
_CONFIG_FIELDS: Final[dict[str, _ConfigField]] = {
    "busy_timeout": _ConfigField("5000", int, "an integer number of milliseconds"),
    "cache_mb": _ConfigField("10", int, "an integer number of megabytes"),
    "sync_mode": _ConfigField(
        "FULL", lambda value: str(value).upper(), "FULL, NORMAL, or OFF"
    ),
    "wal_autocheckpoint": _ConfigField("1000", int, "an integer page count"),
    "max_message_size": _ConfigField(
        str(MAX_MESSAGE_SIZE), int, "an integer byte count"
    ),
    "read_commit_interval": _ConfigField("1", int, "an integer message count"),
    "generator_batch_size": _ConfigField("100", int, "an integer message count"),
    "load_max_future_skew_seconds": _ConfigField(
        str(DEFAULT_LOAD_MAX_FUTURE_SKEW_SECONDS),
        _parse_load_max_future_skew,
        "a non-negative integer number of seconds",
    ),
    "auto_vacuum": _ConfigField("1", int, "an integer flag"),
    "auto_vacuum_interval": _ConfigField("100", int, "an integer mutation count"),
    "vacuum_threshold": _ConfigField(
        "10", _parse_vacuum_threshold, "a numeric percentage"
    ),
    "vacuum_batch_size": _ConfigField("1000", int, "an integer message count"),
    "skip_idle_check": _ConfigField("0", _parse_strict_one_bool, "a boolean flag"),
    "jitter_factor": _ConfigField("0.15", float, "a numeric ratio"),
    "initial_checks": _ConfigField("100", int, "an integer check count"),
    "max_interval": _ConfigField("0.1", float, "a numeric number of seconds"),
    "burst_sleep": _ConfigField("0.00001", float, "a numeric number of seconds"),
    "debug": _ConfigField("", _parse_debug_flag, "a boolean flag"),
    "logging_enabled": _ConfigField("0", _parse_strict_one_bool, "a boolean flag"),
    "default_db_location": _ConfigField(
        "", str, "an absolute directory path or empty string"
    ),
    "default_db_name": _ConfigField(
        DEFAULT_DB_NAME, str, "a relative database path with at most one directory"
    ),
    "project_config_path": _ConfigField(
        "", str, "an absolute directory or one relative directory"
    ),
    "project_config_name": _ConfigField(
        DEFAULT_PROJECT_CONFIG_NAME,
        str,
        "a relative config path with at most one directory",
    ),
    "project_scope": _ConfigField("0", _parse_project_scope, "a boolean flag"),
    "backend": _ConfigField("sqlite", str, "a backend name"),
    "backend_host": _ConfigField("localhost", str, "a host name"),
    "backend_port": _ConfigField("5432", int, "an integer port"),
    "backend_user": _ConfigField("postgres", str, "a user name"),
    "backend_password": _ConfigField("", str, "a password string"),
    "backend_database": _ConfigField("simplebroker", str, "a database name"),
    "backend_schema": _ConfigField("simplebroker_pg_v1", str, "a schema name"),
    "backend_target": _ConfigField("", str, "a backend target string"),
}
"""Canonical defaults and coercion shared by environment and overrides."""

# Per-key coercion callables derived from the field registry.
_CONFIG_NORMALIZERS: Final[dict[str, Callable[[Any], Any]]] = {
    key: field.normalize for key, field in _CONFIG_FIELDS.items()
}


# Keys whose values are redacted before any diagnostic display.
_SENSITIVE_CONFIG_KEYS: Final = frozenset({"backend_password", "backend_target"})
# Longest rejected-value excerpt shown in a diagnostic, in characters.
_CONFIG_VALUE_DISPLAY_LIMIT: Final = 160


def _safe_config_value_display(key: str, value: Any) -> str:
    if key in _SENSITIVE_CONFIG_KEYS:
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


def _invalid_config_error(key: str, value: Any, *, source: str) -> InvalidConfigError:
    return InvalidConfigError(
        key="BROKER_" + key.upper(),
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


def _validate_default_database_location(config: dict[str, Any]) -> None:
    db_location = config["default_db_location"]
    if not isinstance(db_location, str) or not db_location:
        return
    try:
        _validate_safe_path_components(db_location, "default_db_location")
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
        config["default_db_location"] = ""


def _validate_default_database_name(config: dict[str, Any]) -> None:
    db_name = config["default_db_name"]
    if not isinstance(db_name, str) or not db_name:
        return
    try:
        _validate_safe_path_components(db_name, "default_db_name")
    except ValueError as exc:
        raise ValueError(f"BROKER_DEFAULT_DB_NAME validation failed: {exc}") from exc
    if os.path.isabs(db_name):
        raise ValueError(
            f"BROKER_DEFAULT_DB_NAME must be a relative path, not absolute: {db_name}. "
            f"Use BROKER_DEFAULT_DB_LOCATION to specify the directory instead."
        )
    if len(PurePath(db_name).parts) > COMPOUND_DB_NAME_PARTS:
        raise ValueError(
            f"Database name must not contain nested directories: {db_name}. "
            f"Only single directory level is supported (e.g., 'dir/name.db')"
        )


def _validate_project_config_location(config: dict[str, Any]) -> None:
    project_config_path = config["project_config_path"]
    if not isinstance(project_config_path, str) or not project_config_path:
        return
    try:
        _validate_safe_path_components(
            project_config_path,
            "project_config_path",
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
    project_config_name = config["project_config_name"]
    if not isinstance(project_config_name, str) or not project_config_name:
        return
    try:
        _validate_safe_path_components(
            project_config_name,
            "project_config_name",
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
    if len(name_parts) > COMPOUND_DB_NAME_PARTS:
        raise ValueError(
            f"Project config name must not contain nested directories: "
            f"{project_config_name}. Only single directory level is supported "
            "(e.g., 'dir/broker.toml')"
        )
    project_config_path = config["project_config_path"]
    if (
        isinstance(project_config_path, str)
        and project_config_path
        and not os.path.isabs(project_config_path)
    ):
        path_parts = PurePath(project_config_path.replace("\\", "/")).parts
        if len(path_parts) + len(name_parts) > COMPOUND_DB_NAME_PARTS:
            raise ValueError(
                "BROKER_PROJECT_CONFIG_PATH and BROKER_PROJECT_CONFIG_NAME "
                "must not combine into nested directories. Only single "
                "directory level is supported (e.g., 'dir/broker.toml')"
            )


def _validate_config(config: dict[str, Any], *, source: str) -> None:
    validators = (
        ("default_db_location", _validate_default_database_location),
        ("default_db_name", _validate_default_database_name),
        ("project_config_path", _validate_project_config_location),
        ("project_config_name", _validate_project_config_name),
    )
    for key, validator in validators:
        try:
            validator(config)
        except ValueError as exc:
            raise _invalid_config_error(key, config[key], source=source) from exc


def _identity(value: Any) -> Any:
    return value


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, (tuple, list)):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(v) for v in value)
    if isinstance(value, (str, bytes, int, float, bool, date, time, type(None))):
        return value
    raise TypeError("configuration values must be scalars or containers")


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {k: _thaw(v) for k, v in value.items()}
    if isinstance(value, tuple):
        return [_thaw(v) for v in value]
    if isinstance(value, frozenset):
        return {_thaw_member(v) for v in value}
    return value


def _thaw_member(value: Any) -> Any:
    if isinstance(value, tuple):
        return tuple(_thaw_member(v) for v in value)
    if isinstance(value, frozenset):
        return frozenset(_thaw_member(v) for v in value)
    return _thaw(value)


@dataclass(frozen=True, slots=True, repr=False)
class ConfigField:
    """A canonical default, documentation and value validator.

    Validators return the canonical value or raise ValueError/TypeError.
    Parsers convert external env/TOML values before canonical validation.
    """

    default: Any
    description: str
    validator: Callable[[Any], Any] = _identity
    parser: Callable[[Any], Any] | None = None
    sensitive: bool = False
    dependencies: tuple[str, ...] = ()
    default_factory: Callable[[Mapping[str, Any]], Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "default", _freeze(self.default))
        object.__setattr__(self, "dependencies", tuple(self.dependencies))

    def __repr__(self) -> str:
        return (
            f"ConfigField(description={self.description!r}, sensitive={self.sensitive})"
        )


def _dependency_order(copied: Mapping[str, ConfigField]) -> list[str]:
    order: list[str] = []
    visiting: set[str] = set()

    def visit(name: str) -> None:
        if name in order:
            return
        if name in visiting:
            raise ValueError(f"cyclic config default dependency: {name}")
        if name not in copied:
            raise ValueError(f"unknown config default dependency: {name}")
        visiting.add(name)
        for dependency in copied[name].dependencies:
            visit(dependency)
        visiting.remove(name)
        order.append(name)

    for name in copied:
        visit(name)
    return order


@dataclass(frozen=True, init=False, repr=False)
class ConfigSchema(Mapping[str, ConfigField]):
    """Immutable field declarations and source-selection policy.

    source_order is highest priority first; validate_base preserves the broker's
    existing ambient-base-before-options failure order.
    """

    _fields: Mapping[str, ConfigField]
    source_order: tuple[str, ...]
    validate_base: bool
    _order: tuple[str, ...]

    def __init__(
        self,
        fields: Mapping[str, ConfigField],
        *,
        source_order: tuple[str, ...] = ("options", "file", "env"),
        validate_base: bool = True,
    ) -> None:
        copied = dict(fields)
        for name, field in copied.items():
            if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
                raise ValueError(f"invalid canonical config field: {name!r}")
            if not isinstance(field, ConfigField):
                raise TypeError("schema entries must be ConfigField instances")
        if set(source_order) != {"options", "file", "env"} or len(source_order) != len(
            {"options", "file", "env"}
        ):
            raise ValueError("source_order must contain options, file and env once")
        order = _dependency_order(copied)
        object.__setattr__(self, "_fields", MappingProxyType(copied))
        object.__setattr__(self, "source_order", tuple(source_order))
        object.__setattr__(self, "validate_base", validate_base)
        object.__setattr__(self, "_order", tuple(order))

    def __getitem__(self, key: str) -> ConfigField:
        return self._fields[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._fields)

    def __len__(self) -> int:
        return len(self._fields)

    def derive(
        self,
        *,
        defaults: Mapping[str, Any] | None = None,
        fields: Mapping[str, ConfigField] | None = None,
        parsers: Mapping[str, Callable[[Any], Any]] | None = None,
        source_order: tuple[str, ...] | None = None,
        validate_base: bool | None = None,
    ) -> ConfigSchema:
        result = dict(self)
        for name, default in (defaults or {}).items():
            result[name] = replace(
                result[name], default=default, default_factory=None, dependencies=()
            )
        for name, parser in (parsers or {}).items():
            result[name] = replace(result[name], parser=parser)
        for name, field in (fields or {}).items():
            if name in result:
                raise ValueError(f"config field already declared: {name}")
            result[name] = field
        return ConfigSchema(
            result,
            source_order=self.source_order if source_order is None else source_order,
            validate_base=self.validate_base
            if validate_base is None
            else validate_base,
        )


def _canonical_validator(name: str, default: Any) -> Callable[[Any], Any]:
    def validate(value: Any) -> Any:
        expected = type(default)
        if expected is float and type(value) in (int, float):
            return float(value)
        if type(value) is not expected:
            raise TypeError(f"expected {expected.__name__}")
        if name == "load_max_future_skew_seconds" and value < 0:
            raise ValueError("expected a non-negative integer")
        if name == "sync_mode" and value not in ("FULL", "NORMAL", "OFF"):
            return "FULL"
        return value

    return validate


CONFIG_DEFAULTS = ConfigSchema(
    {
        name: ConfigField(
            default=field.normalize(field.default),
            description=field.expected,
            validator=_canonical_validator(name, field.normalize(field.default)),
            parser=field.normalize,
            sensitive=name in _SENSITIVE_CONFIG_KEYS,
        )
        for name, field in _CONFIG_FIELDS.items()
    }
)


def _field_value(
    schema: ConfigSchema,
    name: str,
    value: Any,
    *,
    prefix: str,
    source: str,
    external: bool = False,
) -> Any:
    field = schema[name]
    original = value
    try:
        if external and field.parser is not None:
            # File percentages use external units even though TOML supplies numbers.
            if name == "vacuum_threshold" and field.parser is _parse_vacuum_threshold:
                value = float(value) / 100
            else:
                value = field.parser(value)
        return field.validator(value)
    except (TypeError, ValueError) as exc:
        key = f"{prefix}_{name.upper()}" if external else name
        raise InvalidConfigError(
            key=key,
            source=source,
            expected=field.description,
            value_display="<redacted>"
            if field.sensitive
            else _safe_config_value_display(name, original),
        ) from exc


def _derived_value(field: ConfigField, name: str, values: Mapping[str, Any]) -> Any:
    assert field.default_factory is not None
    try:
        return field.default_factory(MappingProxyType(dict(values)))
    except (TypeError, ValueError) as exc:
        raise InvalidConfigError(
            key=name,
            source="derived",
            expected=field.description,
            value_display="<redacted>" if field.sensitive else "<derived default>",
        ) from exc


def _check_core(
    values: dict[str, Any],
    *,
    source: str,
    prefix: str,
    sources: Mapping[str, str] | None = None,
) -> None:
    if not set(CONFIG_DEFAULTS).issubset(values):
        return
    try:
        _validate_config(values, source=source)
    except InvalidConfigError as exc:
        name = exc.key.removeprefix("BROKER_").lower()
        raise InvalidConfigError(
            key=f"{prefix}_{name.upper()}",
            source=sources.get(name, source) if sources is not None else source,
            expected=exc.expected,
            value_display=exc.value_display,
        ) from exc


@dataclass(frozen=True, init=False, repr=False)
class _ConfigReceipt(Mapping[str, Any]):
    """Shared canonical backing, with no ambient input or per-product copies."""

    _values: Mapping[str, Any]

    def __len__(self) -> int:
        return len(self._values)


@dataclass(frozen=True, init=False, repr=False)
class ConfigSnapshot(_ConfigReceipt):
    """Complete canonical settings; selected-prefix reads alias the same values."""

    schema: ConfigSchema
    prefix: str
    sources: Mapping[str, str]

    def __init__(
        self,
        values: Mapping[str, Any],
        *,
        schema: ConfigSchema = CONFIG_DEFAULTS,
        prefix: str = "BROKER",
    ) -> None:
        _check_prefix(prefix)
        if set(values) != set(schema):
            raise ValueError("snapshot requires exactly the declared schema fields")
        normalized = {
            name: _field_value(
                schema, name, values[name], prefix=prefix, source="snapshot"
            )
            for name in schema
        }
        _check_core(normalized, source="snapshot", prefix=prefix)
        object.__setattr__(
            self,
            "_values",
            MappingProxyType({k: _freeze(v) for k, v in normalized.items()}),
        )
        object.__setattr__(self, "schema", schema)
        object.__setattr__(self, "prefix", prefix)
        object.__setattr__(
            self, "sources", MappingProxyType(dict.fromkeys(schema, "snapshot"))
        )

    def __getitem__(self, key: str) -> Any:
        if key in self._values:
            return self._values[key]
        if key.startswith(self.prefix + "_"):
            name = key[len(self.prefix) + 1 :].lower()
            if key == self.prefix + "_" + name.upper():
                return self._values[name]
        # Existing broker consumers/plugins may still use their published spelling.
        if key.startswith("BROKER_") and key[7:].lower() in CONFIG_DEFAULTS:
            return self._values[key[7:].lower()]
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    @classmethod
    def from_values(
        cls,
        values: Mapping[str, Any],
        *,
        schema: ConfigSchema = CONFIG_DEFAULTS,
        prefix: str = "BROKER",
    ) -> ConfigSnapshot:
        return cls(values, schema=schema, prefix=prefix)

    def to_values(self) -> dict[str, Any]:
        return {k: _thaw(v) for k, v in self._values.items()}

    def with_options(self, options: Mapping[str, Any]) -> ConfigSnapshot:
        unknown = set(options) - set(self.schema)
        if unknown:
            raise ValueError(f"unknown option: {next(iter(unknown))}")
        values: dict[str, Any] = {}
        sources = dict(self.sources)
        changed: set[str] = set()
        for name in self.schema._order:
            field = self.schema[name]
            if name in options:
                value = _field_value(
                    self.schema,
                    name,
                    options[name],
                    prefix=self.prefix,
                    source="override",
                )
                sources[name] = "override"
            elif (
                field.default_factory is not None
                and sources[name] in ("default", "derived")
                and changed.intersection(field.dependencies)
            ):
                value = _field_value(
                    self.schema,
                    name,
                    _derived_value(field, name, values),
                    prefix=self.prefix,
                    source="derived",
                )
                sources[name] = "derived"
            else:
                value = self[name]
            values[name] = value
            if value != self[name]:
                changed.add(name)
        result = ConfigSnapshot(values, schema=self.schema, prefix=self.prefix)
        object.__setattr__(result, "sources", MappingProxyType(sources))
        return result


@dataclass(frozen=True, init=False, repr=False)
class ResolvedConfig(_ConfigReceipt):
    """Historical BROKER mapping view, retaining shallow opaque extra values."""

    _extras: Mapping[str, Any]

    def __init__(self, values: Mapping[str, Any]) -> None:
        canonical, extras = _legacy_values(values)
        object.__setattr__(self, "_values", MappingProxyType(canonical))
        object.__setattr__(self, "_extras", MappingProxyType(extras))

    def __getitem__(self, key: str) -> Any:
        if key in self._extras:
            return self._extras[key]
        if key in self._values:
            return self._values[key]
        if key.startswith("BROKER_"):
            return self._values[key[7:].lower()]
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        yield from ("BROKER_" + name.upper() for name in self._values)
        yield from self._extras

    def __len__(self) -> int:
        return len(self._values) + len(self._extras)


def _legacy_values(
    overrides: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    values = {name: field.default for name, field in CONFIG_DEFAULTS.items()}
    extras: dict[str, Any] = {}
    for key, value in overrides.items():
        name = (
            key[7:].lower()
            if isinstance(key, str) and key.startswith("BROKER_")
            else ""
        )
        if name in CONFIG_DEFAULTS and key == "BROKER_" + name.upper():
            values[name] = _normalize_config_value(name, value, source="override")
        else:
            extras[key] = value
    if values["sync_mode"] not in ("FULL", "NORMAL", "OFF"):
        values["sync_mode"] = "FULL"
    _validate_config(values, source="override")
    return values, extras


def _check_prefix(prefix: str) -> None:
    if not re.fullmatch(r"[A-Z][A-Z0-9_]*[A-Z0-9]|[A-Z]", prefix):
        raise ValueError(
            "prefix must be an uppercase identifier without a trailing underscore"
        )


def build_config(
    prefix: str,
    env: Mapping[str, str] | None = None,
    config_file: os.PathLike[str] | str | Mapping[str, Any] | None = None,
    options: Mapping[str, Any] | None = None,
    *,
    defaults: ConfigSchema = CONFIG_DEFAULTS,
) -> ConfigSnapshot:
    """Resolve declared namespaced settings; never read implicit process state."""
    _check_prefix(prefix)
    schema = defaults
    file_values: Mapping[str, Any] = {}
    if isinstance(config_file, Mapping):
        file_values = config_file
    elif config_file is not None:
        with open(config_file, "rb") as stream:
            file_values = tomllib.load(stream)
    inputs: dict[str, Mapping[str, Any]] = {
        "env": env or {},
        "file": file_values,
        "options": options or {},
    }
    if set(inputs["options"]) - set(schema):
        raise ValueError("options contain undeclared configuration fields")

    if schema.validate_base and options:
        _resolve_sources(schema, prefix, inputs, False)
    values, sources = _resolve_sources(schema, prefix, inputs, True)
    result = ConfigSnapshot(values, schema=schema, prefix=prefix)
    object.__setattr__(result, "sources", MappingProxyType(sources))
    return result


def _resolve_sources(
    schema: ConfigSchema,
    prefix: str,
    inputs: Mapping[str, Mapping[str, Any]],
    include_options: bool,
) -> tuple[dict[str, Any], dict[str, str]]:
    values: dict[str, Any] = {}
    sources: dict[str, str] = {}
    for name in schema._order:
        field = schema[name]
        for source in schema.source_order:
            if source == "options" and not include_options:
                continue
            key = name if source == "options" else prefix + "_" + name.upper()
            if key in inputs[source]:
                label = {"env": "environment", "options": "override", "file": "file"}[
                    source
                ]
                values[name] = _field_value(
                    schema,
                    name,
                    inputs[source][key],
                    prefix=prefix,
                    source=label,
                    external=source != "options",
                )
                sources[name] = label
                break
        else:
            if field.default_factory is None:
                value, label = field.default, "default"
            else:
                value, label = (
                    _derived_value(field, name, values),
                    "derived",
                )
            values[name] = _field_value(
                schema, name, value, prefix=prefix, source=label
            )
            sources[name] = label
    _check_core(
        values,
        source="override" if include_options and inputs["options"] else "environment",
        prefix=prefix,
        sources=sources,
    )
    return values, sources


class _NamingView(Mapping[str, Any]):
    def __init__(self, values: Mapping[str, Any], *, legacy: bool) -> None:
        self._backing = values
        self.legacy = legacy

    def __iter__(self) -> Iterator[str]:
        seen: set[str] = set()
        for key in self._backing:
            name = (
                key[7:].lower()
                if key.startswith("BROKER_") and key[7:].lower() in CONFIG_DEFAULTS
                else key
            )
            output = (
                "BROKER_" + name.upper()
                if self.legacy and name in CONFIG_DEFAULTS
                else name
            )
            if output not in seen:
                seen.add(output)
                yield output

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def __getitem__(self, key: str) -> Any:
        name = (
            key[7:].lower()
            if key.startswith("BROKER_") and key[7:].lower() in CONFIG_DEFAULTS
            else key
        )
        if name in CONFIG_DEFAULTS:
            if isinstance(self._backing, _ConfigReceipt):
                # Canonical access must not confuse a legacy opaque lower-case extra.
                return self._backing._values[name]
            prefixed = "BROKER_" + name.upper()
            if prefixed in self._backing:
                return self._backing[prefixed]
        return self._backing[name]


def canonical_config(values: Mapping[str, Any]) -> Mapping[str, Any]:
    """Internal naming view without resolution, normalization or copying."""
    return _NamingView(values, legacy=False)


def legacy_config(values: Mapping[str, Any]) -> Mapping[str, Any]:
    """Published plugin naming view over the same retained configuration."""
    if isinstance(values, ConfigSnapshot) or (
        isinstance(values, _NamingView) and not values.legacy
    ):
        return _NamingView(values, legacy=True)
    return values


def _recognize(values: Mapping[str, Any]) -> ConfigSnapshot | ResolvedConfig | None:
    if isinstance(values, _NamingView):
        return _recognize(values._backing)
    if isinstance(values, ConfigSnapshot):
        receipt = (
            values
            if type(values) is ConfigSnapshot
            else ConfigSnapshot.from_values(
                dict(values), schema=values.schema, prefix=values.prefix
            )
        )
        for name in CONFIG_DEFAULTS:
            if name not in receipt:
                raise ValueError(f"missing broker config field: {name}")
            canonical = _field_value(
                CONFIG_DEFAULTS,
                name,
                receipt[name],
                prefix=receipt.prefix,
                source="snapshot",
            )
            if canonical != receipt[name]:
                raise _invalid_config_error(name, receipt[name], source="snapshot")
        return receipt
    if type(values) is ResolvedConfig:
        return values
    if isinstance(values, ResolvedConfig):
        return ResolvedConfig(values)
    return None


@overload
def resolve_config(overrides: ConfigSnapshot) -> ConfigSnapshot: ...  # type: ignore[overload-overlap]
@overload
def resolve_config(overrides: ResolvedConfig) -> ResolvedConfig: ...  # type: ignore[overload-overlap]
@overload
def resolve_config(overrides: Mapping[str, Any] | None = None) -> dict[str, Any]: ...


def resolve_config(
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any] | ResolvedConfig | ConfigSnapshot:
    if overrides is not None:
        receipt = _recognize(overrides)
        if receipt is not None:
            return receipt
    config = load_config()
    if overrides is not None:
        config.update(overrides)
        return dict(ResolvedConfig(config))
    return config


@overload
def snapshot_config(config: ConfigSnapshot) -> ConfigSnapshot: ...  # type: ignore[overload-overlap]
@overload
def snapshot_config(config: Mapping[str, Any] | None = None) -> ResolvedConfig: ...


def snapshot_config(
    config: Mapping[str, Any] | None = None,
) -> ConfigSnapshot | ResolvedConfig:
    if config is not None:
        receipt = _recognize(config)
        if receipt is not None:
            return receipt
    return ResolvedConfig(resolve_config(config))


def resolve_isolated_config(
    overrides: Mapping[str, Any], *, preserve_unknown: bool = False
) -> ResolvedConfig:
    if not preserve_unknown:
        for key, value in overrides.items():
            if key not in {"BROKER_" + name.upper() for name in CONFIG_DEFAULTS}:
                raise InvalidConfigError(
                    key=key,
                    source="override",
                    expected="a recognized canonical BROKER_* configuration key",
                    value_display=_safe_config_value_display(key, value),
                )
    return ResolvedConfig(overrides)


def _overlay_config(
    base: Mapping[str, Any], overrides: Mapping[str, Any] | None
) -> Any:
    if overrides is None:
        return base
    receipt = _recognize(overrides)
    if receipt is not None:
        return receipt
    if isinstance(base, ConfigSnapshot):
        options = {
            key[7:].lower()
            if key.startswith("BROKER_") and key[7:].lower() in CONFIG_DEFAULTS
            else key: value
            for key, value in overrides.items()
        }
        # Historical operation options still accept typed/string broker coercions.
        options = {
            key: _normalize_config_value(key, value, source="override")
            if key in CONFIG_DEFAULTS
            else value
            for key, value in options.items()
        }
        return base.with_options(options)
    values = dict(base)
    values.update(overrides)
    return ResolvedConfig(values)


def load_config() -> dict[str, Any]:
    """Read current BROKER env and return the historical mutable prefixed dict."""
    return dict(
        legacy_config(build_config("BROKER", env=os.environ, defaults=CONFIG_DEFAULTS))
    )
