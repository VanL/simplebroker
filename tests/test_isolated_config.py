"""Ambient-free snapshots remain owned across broker layers."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from simplebroker import (
    Config,
    Queue,
    QueueWatcher,
    load_lines,
    open_broker,
    resolve_config,
    target_for_directory,
)
from simplebroker.ext import SQLiteRunner


def _invalid_ambient(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_BUSY_TIMEOUT", "not-an-integer")


def _isolated(**overrides: object) -> Config:
    return resolve_config(
        override={
            "BROKER_" + key: value
            for key, value in (
                {"DEFAULT_DB_NAME": "isolated.db", "AUTO_VACUUM": "0", **overrides}
            ).items()
        }
    )


def test_isolated_complete_immutable(monkeypatch: pytest.MonkeyPatch) -> None:
    _invalid_ambient(monkeypatch)
    config = _isolated(BUSY_TIMEOUT="41")
    assert len(config) == 32
    assert config["BUSY_TIMEOUT"] == 41
    assert config["CACHE_MB"] == 10
    with pytest.raises(TypeError):
        config["CACHE_MB"] = 99  # type: ignore[index]


def test_unknown_namespaced_value_preserved() -> None:
    config = resolve_config(env={"BROKER_CUSTOM": "kept"})
    assert config["CUSTOM"] == "kept"
    assert len(config) == 33


def test_resolved_marker_survives_queue_project_broker_and_runner_layers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _isolated()
    _invalid_ambient(monkeypatch)

    target = target_for_directory(tmp_path, config=config)
    assert Path(target.target) == tmp_path / "isolated.db"

    with Queue("jobs", db_path=target, persistent=True, config=config) as queue:
        assert queue.write("queued") > 0
        assert queue.read() == "queued"

    broker_path = tmp_path / "broker.db"
    with open_broker(str(broker_path), config=config) as broker:
        broker.write("jobs", "opened")
        assert broker.claim_one("jobs", with_timestamps=False) == "opened"

    runner = SQLiteRunner(str(tmp_path / "runner.db"), config=config)
    try:
        assert runner.get_connection() is not None
    finally:
        runner.close()


def test_resolved_marker_survives_watcher_and_dump_load_layers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _isolated()
    _invalid_ambient(monkeypatch)

    handled = threading.Event()
    watcher = QueueWatcher(
        "jobs",
        lambda _body, _timestamp: handled.set(),
        db=tmp_path / "watcher.db",
        config=config,
    )
    try:
        watcher.run_in_thread()
        Queue("jobs", db_path=str(tmp_path / "watcher.db"), config=config).write("wake")
        assert handled.wait(3)
    finally:
        watcher.stop()

    header = json.dumps(
        {
            "type": "header",
            "format": "simplebroker-dump",
            "version": 1,
            "last_ts": "0000000000000000000",
        }
    )
    with open_broker(str(tmp_path / "load.db"), config=config) as broker:
        result = load_lines(broker, [header], config=config)
    assert result.messages == 0
    assert result.aliases == 0
