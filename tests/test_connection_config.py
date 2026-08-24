"""Regression tests for partial config handling in connection setup."""
# mypy: disable-error-code=no-untyped-def

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from simplebroker import (
    Queue,
    open_broker,
    resolve_config,
    resolve_isolated_config,
    target_for_directory,
)
from simplebroker._backends.sqlite.plugin import sqlite_backend_plugin
from simplebroker._constants import load_config
from simplebroker._exceptions import InvalidConfigError, MessageError
from simplebroker._runner import SQLiteRunner
from simplebroker._targets import BrokerTarget
from simplebroker.db import BrokerCore, DBConnection
from simplebroker.watcher import PollingStrategy, QueueWatcher

from .helper_scripts.broker_factory import make_broker
from .helper_scripts.timing import scale_timeout_for_ci, wait_for_condition


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


def test_resolve_config_preserves_typed_debug_and_percentage_overrides() -> None:
    """Programmatic overrides should not need environment-style strings."""

    config = resolve_config(
        {
            "BROKER_DEBUG": False,
            "BROKER_VACUUM_THRESHOLD": 20,
        }
    )

    assert config["BROKER_DEBUG"] is False
    assert config["BROKER_VACUUM_THRESHOLD"] == 0.2

    assert resolve_config({"BROKER_DEBUG": "verbose"})["BROKER_DEBUG"] is True


def test_ephemeral_queue_keeps_constructor_snapshot_after_invalid_env_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_MAX_MESSAGE_SIZE", "5")
    queue = Queue("jobs", db_path=str(tmp_path / "snapshot.db"))

    monkeypatch.setenv("BROKER_BUSY_TIMEOUT", "not-an-integer")

    assert queue.write("12345") > 0
    with pytest.raises(MessageError, match=r"exceeds maximum allowed size \(5 bytes\)"):
        queue.write("123456")


def test_new_queue_observes_later_environment_while_existing_queue_stays_fixed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_MAX_MESSAGE_SIZE", "5")
    first = Queue("jobs", db_path=str(tmp_path / "first-snapshot.db"))
    monkeypatch.setenv("BROKER_MAX_MESSAGE_SIZE", "7")
    second = Queue("jobs", db_path=str(tmp_path / "second-snapshot.db"))

    with pytest.raises(MessageError, match=r"maximum allowed size \(5 bytes\)"):
        first.write("123456")
    assert second.write("1234567") > 0


def test_persistent_queue_keeps_snapshot_before_first_lazy_core_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_MAX_MESSAGE_SIZE", "5")
    queue = Queue(
        "jobs",
        db_path=str(tmp_path / "persistent-lazy-snapshot.db"),
        persistent=True,
    )
    monkeypatch.setenv("BROKER_BUSY_TIMEOUT", "not-an-integer")

    try:
        assert queue.write("12345") > 0
        with pytest.raises(MessageError, match=r"maximum allowed size \(5 bytes\)"):
            queue.write("123456")
    finally:
        queue.close()


def test_open_broker_samples_when_context_is_entered(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_MAX_MESSAGE_SIZE", "5")
    manager = open_broker(str(tmp_path / "entry-snapshot.db"))
    monkeypatch.setenv("BROKER_MAX_MESSAGE_SIZE", "7")

    with manager as broker:
        assert broker.write("jobs", "1234567") > 0
        with pytest.raises(
            MessageError,
            match=r"exceeds maximum allowed size \(7 bytes\)",
        ):
            broker.write("jobs", "12345678")


def test_dbconnection_keeps_constructor_snapshot_for_lazy_core_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_MAX_MESSAGE_SIZE", "5")
    connection = DBConnection(str(tmp_path / "lazy-core-snapshot.db"), config=None)
    monkeypatch.setenv("BROKER_BUSY_TIMEOUT", "not-an-integer")

    try:
        broker = connection.get_connection()
        assert broker.write("jobs", "12345") > 0
        with pytest.raises(
            MessageError,
            match=r"exceeds maximum allowed size \(5 bytes\)",
        ):
            broker.write("jobs", "123456")
    finally:
        connection.close()


def test_generator_override_inherits_core_snapshot_without_ambient_reread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = resolve_isolated_config(
        {
            "BROKER_AUTO_VACUUM": 0,
            "BROKER_GENERATOR_BATCH_SIZE": 1,
        }
    )
    runner = SQLiteRunner(str(tmp_path / "generator-overlay.db"), config=config)

    with BrokerCore(runner, config=config) as core:
        core.write("jobs", "one")
        core.write("jobs", "two")
        monkeypatch.setenv("BROKER_BUSY_TIMEOUT", "not-an-integer")

        messages: Any = core.claim_generator(
            "jobs",
            with_timestamps=False,
            delivery_guarantee="at_least_once",
            config={"BROKER_GENERATOR_BATCH_SIZE": 2},
        )
        try:
            assert next(messages) == "one"
        finally:
            messages.close()


def test_generator_reads_ordinary_override_on_first_iteration(
    tmp_path: Path,
) -> None:
    config = resolve_isolated_config({"BROKER_AUTO_VACUUM": 0})
    runner = SQLiteRunner(str(tmp_path / "generator-entry-snapshot.db"), config=config)

    with BrokerCore(runner, config=config) as core:
        core.write("jobs", "one")
        overrides: dict[str, Any] = {"BROKER_GENERATOR_BATCH_SIZE": 1}
        messages = core.claim_generator(
            "jobs",
            with_timestamps=False,
            delivery_guarantee="at_least_once",
            config=overrides,
        )
        overrides["BROKER_GENERATOR_BATCH_SIZE"] = "not-an-integer"

        with pytest.raises(InvalidConfigError, match="BROKER_GENERATOR_BATCH_SIZE"):
            next(messages)


def test_broker_core_merges_partial_config_with_defaults(tmp_path: Path) -> None:
    """A partial size override governs writes while other defaults remain usable."""
    runner = SQLiteRunner(str(tmp_path / "test.db"))

    with BrokerCore(runner, config={"BROKER_MAX_MESSAGE_SIZE": 5}) as core:
        core.write("jobs", "12345")
        with pytest.raises(
            MessageError, match=r"exceeds maximum allowed size \(5 bytes\)"
        ):
            core.write("jobs", "123456")

        assert core.peek_many("jobs", limit=10, with_timestamps=False) == ["12345"]


def test_dbconnection_non_sqlite_target_accepts_partial_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Partial config changes real operations through a resolved target."""
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
        config={"BROKER_MAX_MESSAGE_SIZE": 5},
    ) as conn:
        core = conn.get_connection()
        core.write("jobs", "12345")
        with pytest.raises(
            MessageError, match=r"exceeds maximum allowed size \(5 bytes\)"
        ):
            core.write("jobs", "123456")

        assert core.peek_many("jobs", limit=10, with_timestamps=False) == ["12345"]


def test_target_for_directory_normalizes_partial_backend_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Project helpers should pass typed config into backend init hooks."""

    seen: dict[str, object] = {}

    class DummyPlugin:
        def init_backend(self, config):
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


def test_target_discovery_samples_environment_for_each_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_DEFAULT_DB_NAME", "first.db")
    first = target_for_directory(tmp_path)
    monkeypatch.setenv("BROKER_DEFAULT_DB_NAME", "second.db")
    second = target_for_directory(tmp_path)

    assert Path(first.target) == tmp_path / "first.db"
    assert Path(second.target) == tmp_path / "second.db"


def test_watcher_instance_config_controls_live_polling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delays: list[float] = []
    burst_sleeps: list[float] = []
    original_get_delay = PollingStrategy._get_delay

    def recording_get_delay(strategy: PollingStrategy) -> float:
        delay = original_get_delay(strategy)
        delays.append(delay)
        burst_sleeps.append(strategy._burst_sleep)
        return delay

    monkeypatch.setattr(PollingStrategy, "_get_delay", recording_get_delay)
    watcher = QueueWatcher(
        "jobs",
        lambda _message, _timestamp: None,
        db=tmp_path / "watcher.db",
        config={
            "BROKER_INITIAL_CHECKS": 2,
            "BROKER_MAX_INTERVAL": 0.01,
            "BROKER_BURST_SLEEP": 0.0001,
            "BROKER_JITTER_FACTOR": 0,
        },
    )
    try:
        watcher.run_in_thread()
        assert wait_for_condition(
            lambda: len(delays) >= 5,
            timeout=scale_timeout_for_ci(5.0),
            message="configured watcher did not enter its live polling schedule",
        )
    finally:
        watcher.stop()

    assert delays[:3] == [0, 0, 0]
    assert delays[3] > 0
    assert burst_sleeps and set(burst_sleeps) == {0.0001}


def test_watcher_environment_config_controls_live_polling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_INITIAL_CHECKS", "0")
    monkeypatch.setenv("BROKER_MAX_INTERVAL", "0.007")
    monkeypatch.setenv("BROKER_BURST_SLEEP", "0.0001")
    monkeypatch.setenv("BROKER_JITTER_FACTOR", "0")

    delays: list[float] = []
    original_get_delay = PollingStrategy._get_delay

    def recording_get_delay(strategy: PollingStrategy) -> float:
        delay = original_get_delay(strategy)
        delays.append(delay)
        return delay

    monkeypatch.setattr(PollingStrategy, "_get_delay", recording_get_delay)
    watcher = QueueWatcher(
        "jobs",
        lambda _message, _timestamp: None,
        db=tmp_path / "environment-watcher.db",
    )
    try:
        watcher.run_in_thread()
        assert wait_for_condition(
            lambda: len(delays) >= 4,
            timeout=scale_timeout_for_ci(5.0),
            message="environment-configured watcher did not poll",
        )
    finally:
        watcher.stop()

    assert delays[0] == 0
    assert delays[1] > 0
    assert all(0 <= delay <= 0.007 for delay in delays)


def test_watcher_given_queue_adopts_queue_snapshot_and_overlays_without_ambient(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_INITIAL_CHECKS", "3")
    monkeypatch.setenv("BROKER_CACHE_MB", "17")
    queue = Queue(
        "jobs",
        db_path=str(tmp_path / "watcher-queue-snapshot.db"),
        persistent=True,
    )
    monkeypatch.setenv("BROKER_BUSY_TIMEOUT", "not-an-integer")

    inherited = QueueWatcher(queue, lambda _message, _timestamp: None)
    overlaid = QueueWatcher(
        queue,
        lambda _message, _timestamp: None,
        config={"BROKER_INITIAL_CHECKS": 9},
    )
    try:
        assert inherited._config is queue._config
        assert inherited._strategy._initial_checks == 3
        assert overlaid._strategy._initial_checks == 9
        assert overlaid._config["BROKER_CACHE_MB"] == 17
        assert overlaid._queue_obj._config is queue._config
    finally:
        inherited.stop()
        overlaid.stop()
        queue.close()


@pytest.mark.shared
def test_claim_generator_uses_instance_batch_size(broker_target) -> None:
    broker = make_broker(
        broker_target,
        config={
            "BROKER_GENERATOR_BATCH_SIZE": 1,
            "BROKER_AUTO_VACUUM": 0,
        },
    )
    generator: Any = None
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
        broker.shutdown()


@pytest.mark.shared
def test_move_generator_uses_instance_batch_size(broker_target) -> None:
    broker = make_broker(
        broker_target,
        config={
            "BROKER_GENERATOR_BATCH_SIZE": 1,
            "BROKER_AUTO_VACUUM": 0,
        },
    )
    generator: Any = None
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
        broker.shutdown()


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
    generator: Any = None
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
        broker.shutdown()
