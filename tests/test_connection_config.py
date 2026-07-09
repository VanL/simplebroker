"""Regression tests for partial config handling in connection setup."""

from __future__ import annotations

from pathlib import Path

import pytest

from simplebroker import resolve_config, target_for_directory
from simplebroker._backends.sqlite.plugin import sqlite_backend_plugin
from simplebroker._constants import load_config
from simplebroker._runner import SQLiteRunner
from simplebroker._targets import BrokerTarget
from simplebroker.db import BrokerCore, DBConnection
from simplebroker.watcher import QueueWatcher

from .helper_scripts.broker_factory import make_broker


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
        assert core._config["BROKER_AUTO_VACUUM_INTERVAL"] == 100
        assert isinstance(core._config["BROKER_AUTO_VACUUM_INTERVAL"], int)


def test_dbconnection_non_sqlite_target_accepts_partial_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-SQLite connection setup should not require a fully materialized config."""
    target = BrokerTarget(
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
        assert core._config["BROKER_AUTO_VACUUM_INTERVAL"] == 100
        assert isinstance(core._config["BROKER_AUTO_VACUUM_INTERVAL"], int)


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


def test_watcher_default_strategy_uses_instance_config(tmp_path: Path) -> None:
    watcher = QueueWatcher(
        "jobs",
        lambda _message, _timestamp: None,
        db=tmp_path / "watcher.db",
        config={
            "BROKER_INITIAL_CHECKS": 7,
            "BROKER_MAX_INTERVAL": 2.5,
            "BROKER_BURST_SLEEP": 0.3,
            "BROKER_JITTER_FACTOR": 0.01,
        },
    )
    try:
        assert watcher._strategy._initial_checks == 7
        assert watcher._strategy._max_interval == 2.5
        assert watcher._strategy._burst_sleep == 0.3
        assert watcher._strategy._jitter_factor == 0.01
    finally:
        watcher.stop()


@pytest.mark.shared
def test_claim_generator_uses_instance_batch_size(broker_target) -> None:
    broker = make_broker(
        broker_target,
        config={
            "BROKER_GENERATOR_BATCH_SIZE": 1,
            "BROKER_AUTO_VACUUM": 0,
        },
    )
    generator = None
    try:
        for index in range(3):
            broker.write("jobs", f"message-{index}")

        generator = broker.claim_generator(
            "jobs",
            with_timestamps=False,
            delivery_guarantee="at_least_once",
        )
        assert next(generator) == "message-0"
        assert next(generator) == "message-1"
        generator.close()
        generator = None

        assert broker.peek_many("jobs", limit=10, with_timestamps=False) == [
            "message-1",
            "message-2",
        ]
    finally:
        if generator is not None:
            generator.close()
        broker.close()


@pytest.mark.shared
def test_move_generator_uses_instance_batch_size(broker_target) -> None:
    broker = make_broker(
        broker_target,
        config={
            "BROKER_GENERATOR_BATCH_SIZE": 1,
            "BROKER_AUTO_VACUUM": 0,
        },
    )
    generator = None
    try:
        for index in range(3):
            broker.write("source", f"message-{index}")

        generator = broker.move_generator(
            "source",
            "destination",
            with_timestamps=False,
            delivery_guarantee="at_least_once",
        )
        assert next(generator) == "message-0"
        assert next(generator) == "message-1"
        generator.close()
        generator = None

        assert broker.peek_many("source", limit=10, with_timestamps=False) == [
            "message-1",
            "message-2",
        ]
        assert broker.peek_many("destination", limit=10, with_timestamps=False) == [
            "message-0"
        ]
    finally:
        if generator is not None:
            generator.close()
        broker.close()


@pytest.mark.shared
@pytest.mark.parametrize("operation", ["claim", "move"])
def test_generator_explicit_config_overrides_instance_batch_size(
    broker_target, operation: str
) -> None:
    broker = make_broker(
        broker_target,
        config={
            "BROKER_GENERATOR_BATCH_SIZE": 1,
            "BROKER_AUTO_VACUUM": 0,
        },
    )
    generator = None
    try:
        for index in range(4):
            broker.write("source", f"message-{index}")

        if operation == "claim":
            generator = broker.claim_generator(
                "source",
                with_timestamps=False,
                delivery_guarantee="at_least_once",
                config={"BROKER_GENERATOR_BATCH_SIZE": 2},
            )
        else:
            generator = broker.move_generator(
                "source",
                "destination",
                with_timestamps=False,
                delivery_guarantee="at_least_once",
                config={"BROKER_GENERATOR_BATCH_SIZE": 2},
            )

        assert [next(generator) for _ in range(3)] == [
            "message-0",
            "message-1",
            "message-2",
        ]
        generator.close()
        generator = None

        assert broker.peek_many("source", limit=10, with_timestamps=False) == [
            "message-2",
            "message-3",
        ]
        expected_destination = ["message-0", "message-1"] if operation == "move" else []
        assert (
            broker.peek_many("destination", limit=10, with_timestamps=False)
            == expected_destination
        )
    finally:
        if generator is not None:
            generator.close()
        broker.close()
