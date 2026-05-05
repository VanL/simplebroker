from pathlib import Path

import pytest

from simplebroker import Queue
from simplebroker._constants import SCHEMA_VERSION
from simplebroker._phaselock import PhaseLockService
from simplebroker.db import BrokerDB
from simplebroker.watcher import QueueWatcher

pytestmark = [pytest.mark.sqlite_only]


def test_queue_uses_configured_default_db_name_when_db_path_omitted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    queue = Queue("probe", config={"BROKER_DEFAULT_DB_NAME": ".weft/broker.db"})
    queue.generate_timestamp()

    assert (tmp_path / ".weft" / "broker.db").is_file()
    assert PhaseLockService(tmp_path / ".weft" / "broker.db").has_phase(
        f"schema-v{SCHEMA_VERSION}"
    )
    assert not (tmp_path / ".broker.db").exists()
    assert not list((tmp_path / ".weft").glob("*.done"))
    assert not list(tmp_path.glob("*.done"))


def test_queue_empty_db_path_uses_configured_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    queue = Queue(
        "probe",
        db_path="",
        config={"BROKER_DEFAULT_DB_NAME": ".weft/broker.db"},
    )
    queue.generate_timestamp()

    assert (tmp_path / ".weft" / "broker.db").is_file()
    assert not (tmp_path / ".broker.db").exists()


def test_queue_uses_configured_default_db_location(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workdir = tmp_path / "work"
    broker_dir = tmp_path / "broker-home"
    workdir.mkdir()
    broker_dir.mkdir()
    monkeypatch.chdir(workdir)

    queue = Queue(
        "probe",
        config={
            "BROKER_DEFAULT_DB_LOCATION": str(broker_dir),
            "BROKER_DEFAULT_DB_NAME": "configured.db",
        },
    )
    queue.generate_timestamp()

    assert (broker_dir / "configured.db").is_file()
    assert not (workdir / "configured.db").exists()


def test_queue_explicit_db_path_overrides_config_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    queue = Queue(
        "probe",
        db_path="explicit.db",
        config={"BROKER_DEFAULT_DB_NAME": ".weft/broker.db"},
    )
    queue.generate_timestamp()

    assert (tmp_path / "explicit.db").is_file()
    assert PhaseLockService(tmp_path / "explicit.db").has_phase(
        f"schema-v{SCHEMA_VERSION}"
    )
    assert not list(tmp_path.glob("*.done"))
    assert not (tmp_path / ".weft" / "broker.db").exists()


def test_queue_passed_config_overrides_environment_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BROKER_DEFAULT_DB_NAME", "env.db")

    queue = Queue("probe", config={"BROKER_DEFAULT_DB_NAME": ".weft/broker.db"})
    queue.generate_timestamp()

    assert (tmp_path / ".weft" / "broker.db").is_file()
    assert not (tmp_path / "env.db").exists()


def test_queue_write_uses_configured_message_size_limit(tmp_path: Path) -> None:
    queue = Queue(
        "probe",
        db_path=str(tmp_path / "broker.db"),
        config={"BROKER_MAX_MESSAGE_SIZE": 3},
    )

    with pytest.raises(ValueError, match="maximum allowed size \\(3 bytes\\)"):
        queue.write("toolong")

    queue.write("ok")
    assert queue.read() == "ok"


def test_broker_broadcast_uses_configured_message_size_limit(tmp_path: Path) -> None:
    broker = BrokerDB(
        str(tmp_path / "broker.db"),
        config={"BROKER_MAX_MESSAGE_SIZE": 3},
    )
    try:
        broker.write("probe", "ok")
        with pytest.raises(ValueError, match="maximum allowed size \\(3 bytes\\)"):
            broker.broadcast("toolong")
    finally:
        broker.close()


def test_watcher_omitted_db_uses_configured_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    watcher = QueueWatcher(
        "probe",
        lambda _message, _timestamp: None,
        config={"BROKER_DEFAULT_DB_NAME": ".weft/broker.db"},
    )
    try:
        watcher._queue_obj.generate_timestamp()
    finally:
        watcher.stop()

    assert (tmp_path / ".weft" / "broker.db").is_file()
    assert not (tmp_path / ".broker.db").exists()


def test_watcher_dispatch_uses_configured_message_size_limit(tmp_path: Path) -> None:
    handled: list[tuple[str, int]] = []
    errors: list[tuple[Exception, str, int]] = []

    def error_handler(exc: Exception, message: str, timestamp: int) -> bool:
        errors.append((exc, message, timestamp))
        return True

    watcher = QueueWatcher(
        "probe",
        lambda message, timestamp: handled.append((message, timestamp)),
        db=str(tmp_path / "broker.db"),
        error_handler=error_handler,
        config={"BROKER_MAX_MESSAGE_SIZE": 3},
    )
    try:
        watcher._dispatch("toolong", 123, config=watcher._config)
    finally:
        watcher.stop()

    assert handled == []
    assert len(errors) == 1
    assert isinstance(errors[0][0], ValueError)
    assert "3 byte limit" in str(errors[0][0])
