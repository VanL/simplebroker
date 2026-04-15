"""Internal project-config parsing for backend-aware CLI resolution."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from ._backend_plugins import get_backend_plugin
from ._constants import MAX_PROJECT_TRAVERSAL_DEPTH
from ._targets import ResolvedTarget

PROJECT_CONFIG_FILENAME = ".broker.toml"
SUPPORTED_PROJECT_CONFIG_VERSION = 1
_SECTION_RE = re.compile(r"^\[(?P<section>[A-Za-z0-9_]+)\]$")


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
        return bytes(value[1:-1], "utf-8").decode("unicode_escape")
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
            raise ValueError(
                f"Invalid .broker.toml line {line_number}: {raw_line!r}"
            )

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


def find_project_config(
    starting_dir: Path,
    *,
    max_depth: int = MAX_PROJECT_TRAVERSAL_DEPTH,
) -> Path | None:
    """Search upward for .broker.toml."""
    current_dir = starting_dir.resolve()
    depth = 0

    while depth < max_depth:
        candidate = current_dir / PROJECT_CONFIG_FILENAME
        if candidate.is_file():
            return candidate
        if current_dir.parent == current_dir:
            return None
        current_dir = current_dir.parent
        depth += 1

    return None


def resolve_project_target(config_path: Path) -> ResolvedTarget:
    """Resolve a project config into an internal target object."""
    config_data = load_project_config(config_path)
    backend_name = config_data["backend"]
    plugin = get_backend_plugin(backend_name)
    target = config_data["target"]
    backend_options = dict(config_data["backend_options"])

    if backend_name == "sqlite":
        target = str((config_path.parent / target).expanduser().resolve())
    else:
        from ._constants import load_config

        resolved = plugin.init_backend(
            load_config(),
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
    "resolve_project_target",
]
