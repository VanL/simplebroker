"""Regression tests for partial config handling in connection setup."""

from __future__ import annotations

from pathlib import Path

import pytest

from simplebroker import resolve_config, target_for_directory
from simplebroker._backends.sqlite.plugin import sqlite_backend_plugin
from simplebroker._constants import load_config
from simplebroker._runner import SQLiteRunner
from simplebroker._targets import ResolvedTarget
from simplebroker.db import BrokerCore, DBConnection


def test_resolve_config_normalizes_partial_override_values() -> None:
    """Public config normalization should produce complete typed broker config."""

    config = resolve_config(
        {
            "BROKER_AUTO_VACUUM_INTERVAL": "100",
            "BROKER_BACKEND_PORT": "5433",
            "BROKER_PROJECT_SCOPE": "1",
            "BROKER_LOGGING_ENABLED": "1",
            "BROKER_VACUUM_THRESHOLD": "20",
        }
    )

    assert config["BROKER_AUTO_VACUUM_INTERVAL"] == 100
    assert isinstance(config["BROKER_AUTO_VACUUM_INTERVAL"], int)
    assert config["BROKER_BACKEND_PORT"] == 5433
    assert isinstance(config["BROKER_BACKEND_PORT"], int)
    assert config["BROKER_PROJECT_SCOPE"] is True
    assert config["BROKER_LOGGING_ENABLED"] is True
    assert config["BROKER_VACUUM_THRESHOLD"] == 0.2
    assert isinstance(config["BROKER_VACUUM_THRESHOLD"], float)
    assert config["BROKER_MAX_MESSAGE_SIZE"] == load_config()["BROKER_MAX_MESSAGE_SIZE"]


def test_broker_core_merges_partial_config_with_defaults(tmp_path: Path) -> None:
    """Direct BrokerCore construction should accept override-only configs."""
    runner = SQLiteRunner(str(tmp_path / "test.db"))

    with BrokerCore(runner, config={"BROKER_AUTO_VACUUM_INTERVAL": "100"}) as core:
        assert core._vacuum_interval == 100
        assert isinstance(core._vacuum_interval, int)


def test_dbconnection_non_sqlite_target_accepts_partial_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-SQLite connection setup should not require a fully materialized config."""
    target = ResolvedTarget(
        backend_name="postgres",
        target=str(tmp_path / "test.db"),
        backend_options={},
        project_root=tmp_path,
    )

    monkeypatch.setattr(
        "simplebroker._backend_plugins.get_backend_plugin",
        lambda name="sqlite": sqlite_backend_plugin,
    )

    with DBConnection(
        target,
        config={"BROKER_AUTO_VACUUM_INTERVAL": "100"},
    ) as conn:
        core = conn.get_connection()
        assert isinstance(core, BrokerCore)
        assert core._vacuum_interval == 100
        assert isinstance(core._vacuum_interval, int)


def test_target_for_directory_normalizes_partial_backend_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Project helpers should pass typed config into backend init hooks."""

    seen: dict[str, object] = {}

    class DummyPlugin:
        def init_backend(self, config):  # type: ignore[no-untyped-def]
            seen.update(config)
            return {
                "target": str(config["BROKER_BACKEND_TARGET"]),
                "backend_options": {"schema": str(config["BROKER_BACKEND_SCHEMA"])},
            }

    monkeypatch.setattr(
        "simplebroker.project.get_backend_plugin",
        lambda name="sqlite": DummyPlugin(),
    )

    target = target_for_directory(
        tmp_path,
        config={
            "BROKER_BACKEND": "postgres",
            "BROKER_BACKEND_TARGET": "postgresql://broker@db.example.com/app",
            "BROKER_BACKEND_SCHEMA": "broker_schema",
            "BROKER_BACKEND_PORT": "5433",
            "BROKER_AUTO_VACUUM_INTERVAL": "100",
        },
    )

    assert target.backend_name == "postgres"
    assert seen["BROKER_BACKEND_PORT"] == 5433
    assert isinstance(seen["BROKER_BACKEND_PORT"], int)
    assert seen["BROKER_AUTO_VACUUM_INTERVAL"] == 100
    assert isinstance(seen["BROKER_AUTO_VACUUM_INTERVAL"], int)
