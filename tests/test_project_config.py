"""Tests for .broker.toml project configuration."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from simplebroker._constants import load_config
from simplebroker._project_config import load_project_config, resolve_project_target
from simplebroker.db import BrokerDB
from simplebroker.project import (
    broker_root,
    deserialize_broker_target,
    resolve_broker_target,
    serialize_broker_target,
    target_for_directory,
)

from .conftest import run_cli


def _write_project_config(
    path: Path,
    *,
    backend: str,
    target: str,
    backend_options: dict[str, str] | None = None,
) -> None:
    lines = [
        "version = 1",
        f'backend = "{backend}"',
        f'target = "{target}"',
        "",
    ]
    if backend_options:
        lines.append("[backend_options]")
        for key, value in backend_options.items():
            lines.append(f'{key} = "{value}"')
        lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")


def _project_backend_config(*, sqlite_target: str) -> tuple[str, str, dict[str, str]]:
    if os.environ.get("BROKER_TEST_BACKEND") == "postgres":
        dsn = os.environ["SIMPLEBROKER_PG_TEST_DSN"]
        return "postgres", dsn, {"schema": f"pytest_{uuid.uuid4().hex[:12]}"}
    return "sqlite", sqlite_target, {}


@pytest.mark.sqlite_only
def test_load_project_config_and_resolve_relative_sqlite_target(tmp_path) -> None:
    """Relative sqlite targets should resolve from the config file directory."""
    config_path = tmp_path / ".broker.toml"
    _write_project_config(config_path, backend="sqlite", target="data/queue.db")

    config_data = load_project_config(config_path)
    resolved = resolve_project_target(config_path)

    assert config_data["backend"] == "sqlite"
    assert config_data["target"] == "data/queue.db"
    assert resolved.backend_name == "sqlite"
    assert resolved.target_path == (tmp_path / "data" / "queue.db").resolve()


def test_project_config_preferred_over_legacy_project_database(workdir: Path) -> None:
    """Project config should beat legacy upward .broker.db discovery."""
    project_root = workdir / "project"
    nested = project_root / "src" / "child"
    nested.mkdir(parents=True)

    legacy_db = project_root / ".broker.db"
    with BrokerDB(str(legacy_db)) as db:
        db.write("tasks", "legacy message")

    backend, target, backend_options = _project_backend_config(
        sqlite_target="data/config.db"
    )
    _write_project_config(
        project_root / ".broker.toml",
        backend=backend,
        target=target,
        backend_options=backend_options,
    )

    env = {"BROKER_PROJECT_SCOPE": "1"}

    code, stdout, stderr = run_cli("init", cwd=nested, env=env)
    assert code == 0, stderr

    if backend == "sqlite":
        config_db = project_root / "data" / "config.db"
        assert config_db.exists()

    code, stdout, stderr = run_cli(
        "write", "tasks", "config message", cwd=nested, env=env
    )
    assert code == 0, stderr

    code, stdout, stderr = run_cli("read", "tasks", cwd=nested, env=env)
    assert code == 0, stderr
    assert stdout == "config message"

    code, stdout, stderr = run_cli(
        "-f",
        str(legacy_db),
        "read",
        "tasks",
        cwd=nested,
        env={"BROKER_PROJECT_SCOPE": "0"},
    )
    assert code == 0, stderr
    assert stdout == "legacy message"


def test_project_config_roundtrip_from_nested_directory(workdir: Path) -> None:
    """CLI should round-trip through a project config from a child directory."""
    project_root = workdir / "project"
    nested = project_root / "a" / "b"
    nested.mkdir(parents=True)

    backend, target, backend_options = _project_backend_config(
        sqlite_target="var/app.db"
    )
    _write_project_config(
        project_root / ".broker.toml",
        backend=backend,
        target=target,
        backend_options=backend_options,
    )

    env = {"BROKER_PROJECT_SCOPE": "1"}

    code, stdout, stderr = run_cli("init", cwd=nested, env=env)
    assert code == 0, stderr
    if backend == "sqlite":
        assert (project_root / "var" / "app.db").exists()

    code, stdout, stderr = run_cli("write", "jobs", "hello", cwd=nested, env=env)
    assert code == 0, stderr

    code, stdout, stderr = run_cli("--status", cwd=nested, env=env)
    assert code == 0, stderr
    assert "total_messages: 1" in stdout

    code, stdout, stderr = run_cli("read", "jobs", cwd=nested, env=env)
    assert code == 0, stderr
    assert stdout == "hello"


@pytest.mark.sqlite_only
def test_public_resolve_broker_target_discovers_upward_sqlite_project(
    workdir: Path,
) -> None:
    """Public project discovery should find legacy sqlite targets upward."""

    project_root = workdir / "project"
    nested = project_root / "deep" / "child"
    nested.mkdir(parents=True)

    db_path = project_root / ".broker.db"
    with BrokerDB(str(db_path)) as db:
        db.write("tasks", "payload")

    resolved = resolve_broker_target(
        nested,
        config={"BROKER_DEFAULT_DB_NAME": ".broker.db"},
    )

    assert resolved is not None
    assert resolved.backend_name == "sqlite"
    assert resolved.target_path == db_path.resolve()
    assert resolved.project_root == project_root.resolve()
    assert (
        broker_root(
            nested,
            config={"BROKER_DEFAULT_DB_NAME": ".broker.db"},
        )
        == project_root.resolve()
    )


@pytest.mark.sqlite_only
def test_public_target_for_directory_builds_default_sqlite_target(
    tmp_path: Path,
) -> None:
    """Explicit directory resolution should produce a backend-agnostic target."""

    target = target_for_directory(
        tmp_path,
        config={"BROKER_DEFAULT_DB_NAME": ".weft/broker.db"},
    )

    assert target.backend_name == "sqlite"
    assert target.target_path == (tmp_path / ".weft" / "broker.db").resolve()
    assert target.project_root == tmp_path.resolve()


def test_public_broker_target_roundtrip_serialization(tmp_path: Path) -> None:
    """Broker targets should serialize cleanly for subprocess transport."""

    original = target_for_directory(
        tmp_path,
        config={"BROKER_DEFAULT_DB_NAME": ".weft/broker.db"},
    )

    encoded = serialize_broker_target(original)
    decoded = deserialize_broker_target(encoded)

    assert decoded == original


def test_resolve_target_defaults_to_sqlite_without_toml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without toml and without BROKER_BACKEND, sqlite discovery still returns None."""
    monkeypatch.chdir(tmp_path)

    target = resolve_broker_target(tmp_path, config=load_config())

    assert target is None


def test_resolve_target_unknown_backend_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unknown backend names should produce a user-facing availability error."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BROKER_BACKEND", "mysql")

    with pytest.raises(
        RuntimeError, match="Requested backend 'mysql' is not available"
    ):
        target_for_directory(tmp_path, config=load_config())


def test_resolve_target_missing_postgres_plugin_has_install_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Missing postgres plugin should recommend the extension package."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BROKER_BACKEND", "postgres")
    config = load_config()

    def raise_unknown(name: str):
        raise RuntimeError(f"Unknown backend plugin: {name}")

    monkeypatch.setattr("simplebroker.project.get_backend_plugin", raise_unknown)

    with pytest.raises(
        RuntimeError,
        match=(
            r"Requested backend 'postgres' is not available\. "
            r"Install simplebroker-pg or simplebroker\[pg\]\."
        ),
    ):
        target_for_directory(tmp_path, config=config)


def test_toml_overrides_env_backend_in_public_helpers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A direct project config should win over BROKER_BACKEND env selection."""
    monkeypatch.setenv("BROKER_BACKEND", "postgres")
    _write_project_config(
        tmp_path / ".broker.toml", backend="sqlite", target="x.db"
    )

    target = target_for_directory(tmp_path, config=load_config())

    assert target.backend_name == "sqlite"
