"""Regression tests for partial config handling in connection setup."""

from __future__ import annotations

from pathlib import Path

import pytest

from simplebroker._backends.sqlite.plugin import sqlite_backend_plugin
from simplebroker._constants import load_config
from simplebroker._runner import SQLiteRunner
from simplebroker._targets import ResolvedTarget
from simplebroker.db import BrokerCore, DBConnection


def test_broker_core_merges_partial_config_with_defaults(tmp_path: Path) -> None:
    """Direct BrokerCore construction should accept override-only configs."""
    runner = SQLiteRunner(str(tmp_path / "test.db"))

    with BrokerCore(runner, config={"BROKER_LOGGING_ENABLED": True}) as core:
        assert core._vacuum_interval == load_config()["BROKER_AUTO_VACUUM_INTERVAL"]


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
        config={"BROKER_LOGGING_ENABLED": True},
    ) as conn:
        assert isinstance(conn.get_connection(), BrokerCore)
