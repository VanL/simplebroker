"""Internal project-config parsing for backend-aware CLI resolution."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ._backend_plugins import get_backend_plugin
from ._constants import (
    DEFAULT_PROJECT_CONFIG_NAME,
    MAX_PROJECT_TRAVERSAL_DEPTH,
    load_config,
    resolve_config,
)
from ._targets import ResolvedTarget

PROJECT_CONFIG_FILENAME = DEFAULT_PROJECT_CONFIG_NAME
SUPPORTED_PROJECT_CONFIG_VERSION = 1
_SECTION_RE = re.compile(r"^\[(?P<section>[A-Za-z0-9_]+)\]$")
_BASIC_STRING_ESCAPES = {
    "b": "\b",
    "t": "\t",
    "n": "\n",
    "f": "\f",
    "r": "\r",
    '"': '"',
    "\\": "\\",
}
_HEX_RE = re.compile(r"^[0-9A-Fa-f]+$")


def _strip_comments(line: str) -> str:
    """Strip TOML-style comments while preserving quoted strings."""
    in_quotes = False
    escaped = False
    result: list[str] = []

    for char in line:
        if char == '"' and not escaped:
            in_quotes = not in_quotes
        if char == "#" and not in_quotes:
            break
        result.append(char)
        escaped = char == "\\" and not escaped

    return "".join(result).strip()


def _parse_value(raw_value: str) -> Any:
    """Parse a restricted subset of TOML values used by SimpleBroker."""
    value = raw_value.strip()
    if not value:
        raise ValueError("Empty value is not allowed in .broker.toml")

    if value.startswith('"') and value.endswith('"'):
        return _parse_basic_string(value[1:-1])
    if value in {"true", "false"}:
        return value == "true"
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    raise ValueError(f"Unsupported TOML value: {raw_value}")


def _parse_basic_string(value: str) -> str:
    """Decode the TOML basic-string escapes supported by .broker.toml."""
    result: list[str] = []
    index = 0

    while index < len(value):
        char = value[index]
        if char != "\\":
            if _is_disallowed_basic_string_char(char):
                raise ValueError(f"Invalid control character in TOML string: {char!r}")
            result.append(char)
            index += 1
            continue

        index += 1
        if index >= len(value):
            raise ValueError("Invalid TOML string escape: trailing backslash")

        escape = value[index]
        if escape in _BASIC_STRING_ESCAPES:
            result.append(_BASIC_STRING_ESCAPES[escape])
            index += 1
            continue

        if escape in {"u", "U"}:
            length = 4 if escape == "u" else 8
            start = index + 1
            end = start + length
            digits = value[start:end]
            if len(digits) != length or _HEX_RE.fullmatch(digits) is None:
                raise ValueError(f"Invalid TOML Unicode escape: \\{escape}{digits}")
            codepoint = int(digits, 16)
            if codepoint > 0x10FFFF or 0xD800 <= codepoint <= 0xDFFF:
                raise ValueError(
                    f"Invalid TOML Unicode scalar value: \\{escape}{digits}"
                )
            result.append(chr(codepoint))
            index = end
            continue

        raise ValueError(f"Invalid TOML string escape: \\{escape}")

    return "".join(result)


def _is_disallowed_basic_string_char(char: str) -> bool:
    codepoint = ord(char)
    return codepoint <= 0x08 or 0x0A <= codepoint <= 0x1F or codepoint == 0x7F


def _parse_project_config_text(text: str) -> dict[str, Any]:
    """Parse the supported .broker.toml subset into a dictionary."""
    parsed: dict[str, Any] = {}
    section: str | None = None

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = _strip_comments(raw_line)
        if not line:
            continue

        section_match = _SECTION_RE.match(line)
        if section_match:
            section = section_match.group("section")
            parsed.setdefault(section, {})
            continue

        if "=" not in line:
            raise ValueError(f"Invalid .broker.toml line {line_number}: {raw_line!r}")

        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid empty key on line {line_number}")

        value = _parse_value(raw_value)
        if section is None:
            parsed[key] = value
        else:
            table = parsed.setdefault(section, {})
            if not isinstance(table, dict):
                raise ValueError(f"Invalid table assignment on line {line_number}")
            table[key] = value

    return parsed


def load_project_config(config_path: Path) -> dict[str, Any]:
    """Load and validate a .broker.toml file."""
    data = _parse_project_config_text(config_path.read_text(encoding="utf-8"))

    version = data.get("version")
    backend = data.get("backend")
    target = data.get("target")

    if version != SUPPORTED_PROJECT_CONFIG_VERSION:
        raise ValueError(
            "Unsupported .broker.toml version "
            f"{version!r}; expected {SUPPORTED_PROJECT_CONFIG_VERSION}"
        )
    if not isinstance(backend, str) or not backend:
        raise ValueError(".broker.toml requires a non-empty string 'backend'")
    if not isinstance(target, str) or not target:
        raise ValueError(".broker.toml requires a non-empty string 'target'")

    backend_options = data.get("backend_options", {})
    if backend_options is None:
        backend_options = {}
    if not isinstance(backend_options, dict):
        raise ValueError("'backend_options' must be a table in .broker.toml")

    return {
        "version": version,
        "backend": backend,
        "target": target,
        "backend_options": backend_options,
    }


def _config_dict(config: Mapping[str, Any] | None) -> dict[str, Any]:
    return dict(load_config()) if config is None else resolve_config(config)


def project_config_path_for_directory(
    directory: Path,
    *,
    config: Mapping[str, Any] | None = None,
) -> Path:
    """Return the configured project config path rooted at a directory."""

    config_dict = _config_dict(config)
    config_path_prefix = str(config_dict.get("BROKER_PROJECT_CONFIG_PATH", ""))
    config_name = str(
        config_dict.get("BROKER_PROJECT_CONFIG_NAME", PROJECT_CONFIG_FILENAME)
    )
    root = directory.resolve()

    if config_path_prefix:
        prefix = Path(config_path_prefix).expanduser()
        if prefix.is_absolute():
            return (prefix / config_name).resolve(strict=False)
        return (root / prefix / config_name).resolve(strict=False)

    return (root / config_name).resolve(strict=False)


def find_project_config(
    starting_dir: Path,
    *,
    config: Mapping[str, Any] | None = None,
    max_depth: int = MAX_PROJECT_TRAVERSAL_DEPTH,
) -> Path | None:
    """Search upward for the configured project TOML file."""
    config_dict = _config_dict(config)
    config_path_prefix = str(config_dict.get("BROKER_PROJECT_CONFIG_PATH", ""))

    if config_path_prefix and Path(config_path_prefix).expanduser().is_absolute():
        candidate = project_config_path_for_directory(starting_dir, config=config_dict)
        return candidate if candidate.is_file() else None

    current_dir = starting_dir.resolve()
    depth = 0

    while depth < max_depth:
        candidate = project_config_path_for_directory(current_dir, config=config_dict)
        if candidate.is_file():
            return candidate
        if current_dir.parent == current_dir:
            return None
        current_dir = current_dir.parent
        depth += 1

    return None


def resolve_project_target(
    config_path: Path,
    *,
    config: Mapping[str, Any] | None = None,
) -> ResolvedTarget:
    """Resolve a project config into an internal target object.

    The project file owns backend selection and target-shaping fields for the
    resolved target. Ambient env/config is only supplemental for backend data
    the project file should not store, such as passwords.
    """
    config_data = load_project_config(config_path)
    backend_name = config_data["backend"]
    plugin = get_backend_plugin(backend_name)
    target = config_data["target"]
    backend_options = dict(config_data["backend_options"])

    if backend_name == "sqlite":
        target = str((config_path.parent / target).expanduser().resolve())
    else:
        config_dict = (
            dict(load_config()) if config is None else resolve_config(dict(config))
        )
        # Project config owns the target. Backends receive it through the TOML
        # arguments rather than through ambient BROKER_BACKEND_TARGET.
        config_dict["BROKER_BACKEND_TARGET"] = ""
        resolved = plugin.init_backend(
            config_dict,
            toml_target=target,
            toml_options=backend_options,
        )
        target = str(resolved["target"])
        backend_options = dict(resolved["backend_options"])

    return ResolvedTarget(
        backend_name=backend_name,
        target=target,
        backend_options=backend_options,
        project_root=config_path.parent,
        config_path=config_path,
        used_project_scope=True,
        legacy_sqlite_path_mode=False,
    )


__all__ = [
    "PROJECT_CONFIG_FILENAME",
    "SUPPORTED_PROJECT_CONFIG_VERSION",
    "find_project_config",
    "load_project_config",
    "project_config_path_for_directory",
    "resolve_project_target",
]
