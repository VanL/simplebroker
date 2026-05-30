"""Internal project-config parsing for backend-aware CLI resolution."""

from __future__ import annotations

import tomllib
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


def _validated_backend_options(raw_options: object) -> dict[str, Any]:
    if not isinstance(raw_options, dict):
        raise ValueError("'backend_options' must be a table in .broker.toml")

    options: dict[str, Any] = {}
    for key, value in raw_options.items():
        if not isinstance(value, str | int | float | bool):
            raise ValueError(
                "'backend_options' values must be strings, integers, floats, "
                "or booleans in .broker.toml"
            )
        options[str(key)] = value
    return options


def load_project_config(config_path: Path) -> dict[str, Any]:
    """Load and validate a .broker.toml file."""
    with config_path.open("rb") as config_file:
        data = tomllib.load(config_file)

    version = data.get("version")
    backend = data.get("backend")
    target = data.get("target")
    backend_options = _validated_backend_options(data.get("backend_options", {}))

    if version != SUPPORTED_PROJECT_CONFIG_VERSION:
        raise ValueError(
            "Unsupported .broker.toml version "
            f"{version!r}; expected {SUPPORTED_PROJECT_CONFIG_VERSION}"
        )
    if not isinstance(backend, str) or not backend:
        raise ValueError(".broker.toml requires a non-empty string 'backend'")
    if not isinstance(target, str) or not target:
        raise ValueError(".broker.toml requires a non-empty string 'target'")

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
