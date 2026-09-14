"""Regression tests for explicit configuration ownership in connection setup."""
# mypy: disable-error-code=no-untyped-def

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from simplebroker import (
    Queue,
    open_broker,
    resolve_config,
    target_for_directory,
)
from simplebroker._backends.sqlite.plugin import sqlite_backend_plugin
from simplebroker._exceptions import MessageError
from simplebroker._runner import SQLiteRunner
from simplebroker._targets import BrokerTarget
from simplebroker.db import BrokerCore, DBConnection
from simplebroker.watcher import PollingStrategy, QueueWatcher

from .helper_scripts.broker_factory import make_broker


def test_resolve_config_normalizes_partial_override_values() -> None:
    """Public config normalization should produce complete typed broker config."""

    config = resolve_config(
        env=os.environ,
        override={
            "BROKER_AUTO_VACUUM_INTERVAL": "100",
            "BROKER_BACKEND_PORT": "5433",
            "BROKER_PROJECT_SCOPE": "1",
            "BROKER_LOGGING_ENABLED": "1",
            "BROKER_VACUUM_THRESHOLD": "20",
        },
    )

    assert config["AUTO_VACUUM_INTERVAL"] == 100
    assert isinstance(config["AUTO_VACUUM_INTERVAL"], int)
    assert config["BACKEND_PORT"] == 5433
    assert isinstance(config["BACKEND_PORT"], int)
    assert config["PROJECT_SCOPE"] is True
    assert config["LOGGING_ENABLED"] is True
    assert config["VACUUM_THRESHOLD"] == 20
    assert isinstance(config["VACUUM_THRESHOLD"], float)
    assert (
        config["MAX_MESSAGE_SIZE"] == resolve_config(env=os.environ)["MAX_MESSAGE_SIZE"]
    )


def test_resolve_config_preserves_typed_debug_and_percentage_overrides() -> None:
    """Programmatic overrides should not need environment-style strings."""

    config = resolve_config(
        env=os.environ,
        override={"BROKER_DEBUG": False, "BROKER_VACUUM_THRESHOLD": 20},
    )

    assert config["DEBUG"] is False
    assert config["VACUUM_THRESHOLD"] == 20

    assert (
        resolve_config(env=os.environ, override={"BROKER_DEBUG": "verbose"})["DEBUG"]
        is True
    )


def test_library_handles_without_config_ignore_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_MAX_MESSAGE_SIZE", "5")
    monkeypatch.setenv("BROKER_DEFAULT_DB_NAME", "env.db")

    assert Queue("jobs", db_path=str(tmp_path / "queue.db")).write("123456") > 0
    with open_broker(str(tmp_path / "broker.db")) as broker:
        assert broker.write("jobs", "123456") > 0
    assert Path(target_for_directory(tmp_path).target).name != "env.db"


def test_persistent_queue_keeps_snapshot_before_first_lazy_core_creation(
    tmp_path: Path,
) -> None:
    queue = Queue(
        "jobs",
        db_path=str(tmp_path / "persistent-lazy-snapshot.db"),
        persistent=True,
        config=resolve_config(override={"BROKER_MAX_MESSAGE_SIZE": 5}),
    )

    try:
        assert queue.write("12345") > 0
        with pytest.raises(MessageError, match=r"maximum allowed size \(5 bytes\)"):
            queue.write("123456")
    finally:
        queue.close()


def test_dbconnection_keeps_constructor_snapshot_for_lazy_core_creation(
    tmp_path: Path,
) -> None:
    connection = DBConnection(
        str(tmp_path / "lazy-core-snapshot.db"),
        config=resolve_config(override={"BROKER_MAX_MESSAGE_SIZE": 5}),
    )

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
    config = resolve_config(
        override={"BROKER_AUTO_VACUUM": 0, "BROKER_GENERATOR_BATCH_SIZE": 1}
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
            config=resolve_config(override={"BROKER_GENERATOR_BATCH_SIZE": 2}),
        )
        try:
            assert next(messages) == "one"
        finally:
            messages.close()


def test_generator_retains_explicit_config_on_first_iteration(tmp_path: Path) -> None:
    config = resolve_config(override={"BROKER_AUTO_VACUUM": 0})
    runner = SQLiteRunner(str(tmp_path / "generator-entry-snapshot.db"), config=config)
    supplied = {"GENERATOR_BATCH_SIZE": 1}
    operation_config = resolve_config(
        config=config,
        override={"BROKER_" + key: value for key, value in (supplied).items()},
    )
    with BrokerCore(runner, config=config) as core:
        core.write("jobs", "one")
        messages: Any = core.claim_generator(
            "jobs",
            with_timestamps=False,
            delivery_guarantee="at_least_once",
            config=operation_config,
        )
        supplied["GENERATOR_BATCH_SIZE"] = 2
        try:
            assert next(messages) == "one"
        finally:
            messages.close()


def test_broker_core_merges_partial_config_with_defaults(tmp_path: Path) -> None:
    """A resolved size override governs writes while other defaults remain usable."""
    runner = SQLiteRunner(str(tmp_path / "test.db"))

    with BrokerCore(
        runner, config=resolve_config(override={"BROKER_MAX_MESSAGE_SIZE": 5})
    ) as core:
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
        config=resolve_config(override={"BROKER_MAX_MESSAGE_SIZE": 5}),
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
                "target": str(config["BACKEND_TARGET"]),
                "backend_options": {"schema": str(config["BACKEND_SCHEMA"])},
            }

    monkeypatch.setattr(
        "simplebroker.project.get_backend_plugin",
        lambda name="sqlite": DummyPlugin(),
    )

    target = target_for_directory(
        tmp_path,
        config=resolve_config(
            override={
                "BROKER_BACKEND": "postgres",
                "BROKER_BACKEND_TARGET": "postgresql://broker@db.example.com/app",
                "BROKER_BACKEND_SCHEMA": "broker_schema",
                "BROKER_BACKEND_PORT": "5433",
                "BROKER_AUTO_VACUUM_INTERVAL": "100",
            }
        ),
    )

    assert target.backend_name == "postgres"
    assert seen["BACKEND_PORT"] == 5433
    assert isinstance(seen["BACKEND_PORT"], int)
    assert seen["AUTO_VACUUM_INTERVAL"] == 100
    assert isinstance(seen["AUTO_VACUUM_INTERVAL"], int)


def test_watcher_instance_config_maps_into_strategy_fields(
    tmp_path: Path,
) -> None:
    """Part (i) of the two-part polling-config proof (plan Task 5.8):
    QueueWatcher construction maps instance configuration into the
    strategy's fields. Part (ii) below proves those fields determine the
    delay schedule; together they replace two live-thread _get_delay
    spies that were the file's flake vector."""
    watcher = QueueWatcher(
        "jobs",
        lambda _message, _timestamp: None,
        db=tmp_path / "watcher.db",
        config=resolve_config(
            override={
                "BROKER_INITIAL_CHECKS": 2,
                "BROKER_MAX_INTERVAL": 0.01,
                "BROKER_BURST_SLEEP": 0.0001,
                "BROKER_JITTER_FACTOR": 0,
            }
        ),
    )
    try:
        strategy = watcher._strategy
        assert strategy._initial_checks == 2
        assert strategy._max_interval == 0.01
        assert strategy._burst_sleep == 0.0001
        assert strategy._jitter_factor == 0
    finally:
        watcher.stop()


def test_polling_strategy_fields_determine_delay_schedule() -> None:
    """Part (ii): the configured fields drive the delay schedule — no
    threads, direct _get_delay calls."""
    import threading

    strategy = PollingStrategy(
        threading.Event(),
        initial_checks=2,
        max_interval=0.01,
        burst_sleep=0.0001,
        jitter_factor=0,
    )
    delays = []
    for _ in range(6):
        strategy._check_count += 1
        delays.append(strategy._get_delay())

    assert delays[:2] == [0, 0]
    assert all(delay > 0 for delay in delays[2:])
    assert all(delay <= 0.01 for delay in delays)


def test_watcher_given_queue_adopts_queue_snapshot_and_overlays(
    tmp_path: Path,
) -> None:
    queue = Queue(
        "jobs",
        db_path=str(tmp_path / "watcher-queue-snapshot.db"),
        persistent=True,
        config=resolve_config(
            override={"BROKER_INITIAL_CHECKS": 3, "BROKER_CACHE_MB": 17}
        ),
    )

    inherited = QueueWatcher(queue, lambda _message, _timestamp: None)
    overlaid = QueueWatcher(
        queue,
        lambda _message, _timestamp: None,
        config=resolve_config(
            config=queue._config, override={"BROKER_INITIAL_CHECKS": 9}
        ),
    )
    try:
        assert inherited._config is queue._config
        assert inherited._strategy._initial_checks == 3
        assert overlaid._strategy._initial_checks == 9
        assert overlaid._config["CACHE_MB"] == 17
        assert overlaid._queue_obj._config is queue._config
    finally:
        inherited.stop()
        overlaid.stop()
        queue.close()


@pytest.mark.shared
def test_claim_generator_uses_instance_batch_size(broker_target) -> None:
    broker = make_broker(
        broker_target,
        config=resolve_config(
            override={"BROKER_GENERATOR_BATCH_SIZE": 1, "BROKER_AUTO_VACUUM": 0}
        ),
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
        config=resolve_config(
            override={"BROKER_GENERATOR_BATCH_SIZE": 1, "BROKER_AUTO_VACUUM": 0}
        ),
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
        config=resolve_config(
            override={"BROKER_GENERATOR_BATCH_SIZE": 1, "BROKER_AUTO_VACUUM": 0}
        ),
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
                config=resolve_config(override={"BROKER_GENERATOR_BATCH_SIZE": 2}),
            )
        else:
            generator = broker.move_generator(
                "source",
                "destination",
                with_timestamps=False,
                delivery_guarantee="at_least_once",
                config=resolve_config(override={"BROKER_GENERATOR_BATCH_SIZE": 2}),
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


@pytest.mark.parametrize("consumer", ["connection", "watcher"])
def test_explicit_config_is_retained_at_constructor(
    tmp_path: Path, consumer: str
) -> None:
    supplied = {"CUSTOM_METADATA": {"labels": ["original"]}}
    config = resolve_config(
        override={"BROKER_" + key: value for key, value in (supplied).items()}
    )
    path = tmp_path / "capture.db"
    if consumer == "connection":
        connection = DBConnection(str(path), config=config)
        try:
            supplied["CUSTOM_METADATA"]["labels"].append("changed")
            assert connection._config is config
            assert connection._config["CUSTOM_METADATA"] == {
                "labels": ["original", "changed"]
            }
        finally:
            connection.close()
    else:
        watcher = QueueWatcher("jobs", lambda *_: None, db=path, config=config)
        try:
            supplied["CUSTOM_METADATA"]["labels"].append("changed")
            assert watcher._config is config
            assert watcher._config["CUSTOM_METADATA"] == {
                "labels": ["original", "changed"]
            }
        finally:
            watcher.stop()
