"""Public project-scoping helpers for backend-agnostic consumers."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from ._backend_plugins import get_backend_plugin
from ._constants import load_config
from ._project_config import (
    PROJECT_CONFIG_FILENAME,
    find_project_config,
    resolve_project_target,
)
from ._targets import ResolvedTarget
from .helpers import _find_project_database, _is_valid_sqlite_db

BrokerTarget = ResolvedTarget


def _config_dict(config: dict[str, Any] | None) -> dict[str, Any]:
    return dict(load_config()) if config is None else dict(config)


def _root_from_relative_target(target_path: Path, relative_target: Path) -> Path:
    if relative_target.is_absolute():
        return target_path.parent.resolve(strict=False)

    root = target_path
    for _ in relative_target.parts:
        root = root.parent
    return root.resolve(strict=False)


def _sqlite_target(
    target_path: Path,
    *,
    root: Path,
    used_project_scope: bool,
    config_path: Path | None = None,
) -> BrokerTarget:
    get_backend_plugin("sqlite")
    return ResolvedTarget(
        backend_name="sqlite",
        target=str(target_path.resolve(strict=False)),
        backend_options={},
        project_root=root,
        config_path=config_path,
        used_project_scope=used_project_scope,
        legacy_sqlite_path_mode=True,
    )


def resolve_broker_target(
    starting_dir: str | Path | None = None,
    *,
    config: dict[str, Any] | None = None,
) -> BrokerTarget | None:
    """Discover an existing SimpleBroker target from a directory.

    Resolution order:
    1. Upward `.simplebroker.toml` search
    2. Legacy upward sqlite database discovery using `BROKER_DEFAULT_DB_NAME`

    Returns `None` when no existing broker target is found.
    """

    config_dict = _config_dict(config)
    start_dir = (
        Path.cwd().resolve()
        if starting_dir is None
        else Path(starting_dir).expanduser().resolve()
    )

    config_path = find_project_config(start_dir)
    if config_path is not None:
        return resolve_project_target(config_path)

    default_target = Path(str(config_dict["BROKER_DEFAULT_DB_NAME"]))
    if default_target.is_absolute():
        absolute_target = default_target.resolve(strict=False)
        if _is_valid_sqlite_db(absolute_target):
            return _sqlite_target(
                absolute_target,
                root=absolute_target.parent,
                used_project_scope=True,
            )
        return None

    discovered = _find_project_database(str(default_target), start_dir)
    if discovered is None:
        return None

    root = _root_from_relative_target(discovered, default_target)
    return _sqlite_target(discovered, root=root, used_project_scope=True)


def target_for_directory(
    directory: str | Path,
    *,
    config: dict[str, Any] | None = None,
) -> BrokerTarget:
    """Return the broker target rooted at an explicit directory.

    If `directory/.simplebroker.toml` exists, it defines the target. Otherwise,
    the configured default sqlite target path is resolved relative to `directory`.
    """

    config_dict = _config_dict(config)
    root = Path(directory).expanduser().resolve()

    config_path = root / PROJECT_CONFIG_FILENAME
    if config_path.is_file():
        return resolve_project_target(config_path)

    default_target = Path(str(config_dict["BROKER_DEFAULT_DB_NAME"]))
    target_path = (
        default_target.resolve(strict=False)
        if default_target.is_absolute()
        else (root / default_target).resolve(strict=False)
    )
    return _sqlite_target(
        target_path,
        root=root,
        used_project_scope=False,
    )


def broker_root(
    starting_dir: str | Path | None = None,
    *,
    config: dict[str, Any] | None = None,
) -> Path | None:
    """Return the discovered broker project root, if one exists."""

    target = resolve_broker_target(starting_dir, config=config)
    if target is None:
        return None
    return target.project_root


def serialize_broker_target(target: BrokerTarget) -> str:
    """Serialize a broker target for transport across process boundaries."""

    payload = {
        "backend_name": target.backend_name,
        "target": target.target,
        "backend_options": dict(target.backend_options),
        "project_root": (
            str(target.project_root.resolve(strict=False))
            if target.project_root is not None
            else None
        ),
        "config_path": (
            str(target.config_path.resolve(strict=False))
            if target.config_path is not None
            else None
        ),
        "used_project_scope": target.used_project_scope,
        "legacy_sqlite_path_mode": target.legacy_sqlite_path_mode,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def deserialize_broker_target(
    payload: str | Mapping[str, Any],
) -> BrokerTarget:
    """Reconstruct a broker target from :func:`serialize_broker_target` output."""

    raw: Mapping[str, Any]
    if isinstance(payload, str):
        decoded = json.loads(payload)
        if not isinstance(decoded, dict):
            raise ValueError("Broker target payload must decode to an object")
        raw = cast(Mapping[str, Any], decoded)
    else:
        raw = payload

    backend_name = raw.get("backend_name")
    target = raw.get("target")
    if not isinstance(backend_name, str) or not backend_name:
        raise ValueError("Broker target payload missing backend_name")
    if not isinstance(target, str) or not target:
        raise ValueError("Broker target payload missing target")

    backend_options = raw.get("backend_options", {})
    if not isinstance(backend_options, dict):
        raise ValueError("Broker target payload backend_options must be an object")

    project_root_raw = raw.get("project_root")
    config_path_raw = raw.get("config_path")
    project_root = (
        Path(project_root_raw).expanduser().resolve(strict=False)
        if isinstance(project_root_raw, str) and project_root_raw
        else None
    )
    config_path = (
        Path(config_path_raw).expanduser().resolve(strict=False)
        if isinstance(config_path_raw, str) and config_path_raw
        else None
    )

    return ResolvedTarget(
        backend_name=backend_name,
        target=target,
        backend_options=dict(cast(dict[str, Any], backend_options)),
        project_root=project_root,
        config_path=config_path,
        used_project_scope=bool(raw.get("used_project_scope", False)),
        legacy_sqlite_path_mode=bool(raw.get("legacy_sqlite_path_mode", False)),
    )


__all__ = [
    "BrokerTarget",
    "broker_root",
    "deserialize_broker_target",
    "resolve_broker_target",
    "serialize_broker_target",
    "target_for_directory",
]
