"""Internal project-config parsing for backend-aware CLI resolution."""

from __future__ import annotations

import json
import sys
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ._backend_plugins import get_backend_plugin
from ._constants import (
    DEFAULT_PROJECT_CONFIG_NAME,
    MAX_PROJECT_TRAVERSAL_DEPTH,
    ResolvedConfig,
    _overlay_config,
    _validate_safe_path_components,
    snapshot_config,
)
from ._targets import BrokerTarget, _backend_target_has_password

PROJECT_CONFIG_FILENAME = DEFAULT_PROJECT_CONFIG_NAME
SUPPORTED_PROJECT_CONFIG_VERSION = 1


def _same_filesystem(current: Path, parent: Path) -> bool:
    """Return whether an upward discovery step stays on one filesystem."""
    try:
        return current.stat().st_dev == parent.stat().st_dev
    except OSError:
        return False


def _warn_for_inline_project_config_password(config_path: Path, target: str) -> None:
    """Warn about a recognized inline password without exposing its value."""
    if _backend_target_has_password(target):
        print(
            f"simplebroker: warning: {config_path} embeds a backend password; "
            "store secrets in BROKER_BACKEND_PASSWORD or another environment variable",
            file=sys.stderr,
        )


def _validated_backend_options(raw_options: object) -> dict[str, Any]:
    match raw_options:
        case dict() as raw_options_dict:
            return dict(raw_options_dict)
        case _:
            raise ValueError("'backend_options' must be a table in .broker.toml")


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

    _warn_for_inline_project_config_password(config_path, target)

    return {
        "version": version,
        "backend": backend,
        "target": target,
        "backend_options": backend_options,
    }


def _config_snapshot(config: Mapping[str, Any] | None) -> ResolvedConfig:
    return snapshot_config(config)


def _require_lossless_backend_options(
    backend_name: str,
    backend_options: dict[str, Any],
) -> None:
    """Enforce the existing JSON target-transport boundary after plugin return."""

    def is_exact_json_value(value: Any) -> bool:
        value_type = type(value)
        if value is None or value_type in {bool, int, float, str}:
            return True
        if value_type is list:
            return all(is_exact_json_value(item) for item in value)
        if value_type is dict:
            return all(
                type(key) is str and is_exact_json_value(item)
                for key, item in value.items()
            )
        return False

    failure: BaseException | None
    try:
        encoded = json.dumps(
            backend_options,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        if not is_exact_json_value(backend_options):
            raise TypeError("backend_options contain a non-JSON-native type")
        restored = json.loads(encoded)
    except (TypeError, ValueError, OverflowError) as exc:
        failure = exc
    else:
        if restored == backend_options:
            return
        failure = None
    raise ValueError(
        f"Backend plugin '{backend_name}' returned backend_options that are not "
        "lossless through BrokerTarget serialization; normalize TOML-native "
        "values or reject them in the plugin"
    ) from failure


def project_config_path_for_directory(
    directory: Path,
    *,
    config: Mapping[str, Any] | None = None,
) -> Path:
    """Return the configured project config path rooted at a directory."""

    config_dict = _config_snapshot(config)
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
    config_dict = _config_snapshot(config)
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
        parent = current_dir.parent
        if not _same_filesystem(current_dir, parent):
            return None
        current_dir = parent
        depth += 1

    return None


def resolve_project_target(
    config_path: Path,
    *,
    config: Mapping[str, Any] | None = None,
) -> BrokerTarget:
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

    config_dict = _overlay_config(
        snapshot_config(config),
        {"BROKER_BACKEND_TARGET": ""},
    )
    # The selected plugin owns option validation and normalization, including
    # for SQLite. Project values are not replaced by ambient target state.
    resolved = plugin.init_backend(
        config_dict,
        toml_target=target,
        toml_options=backend_options,
    )
    target = str(resolved["target"])
    backend_options = dict(resolved["backend_options"])
    _require_lossless_backend_options(backend_name, backend_options)

    if backend_name == "sqlite":
        resolved_target = (config_path.parent / target).expanduser().resolve()
        _validate_safe_path_components(
            str(resolved_target), ".broker.toml sqlite target"
        )
        target = str(resolved_target)

    return BrokerTarget(
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
