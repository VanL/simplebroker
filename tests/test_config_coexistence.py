"""Real broker and spawned-process use of application configuration."""

from __future__ import annotations

import multiprocessing
import os
from collections.abc import Callable
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any, cast

import pytest

from simplebroker import (
    DEFAULT_CONFIG,
    Config,
    ConfigField,
    Queue,
    deserialize_config,
    resolve_config,
    serialize_config,
)
from simplebroker.db import DBConnection
from simplebroker.watcher import QueueWatcher


def retention(value: Any) -> int:
    result = int(value)
    if result < 0:
        raise ValueError("retention must be nonnegative")
    return result


def defaults() -> dict[str, ConfigField]:
    fields = dict(DEFAULT_CONFIG)
    fields["RETENTION_DAYS"] = ConfigField(7, "retention days", retention)
    return fields


def spawn_consumer(payload: str, path: str, sender: Connection) -> None:
    try:
        os.environ["BROKER_CACHE_MB"] = "invalid"
        config = deserialize_config(payload, defaults=defaults())
        with Queue("spawn", db_path=path, persistent=True, config=config) as queue:
            queue.write("child")
            with queue.sidecar() as session:
                pragma = next(iter(session.run("PRAGMA cache_size", fetch=True)))[0]
            sender.send(
                (
                    config.prefix,
                    config["RETENTION_DAYS"],
                    config["CUSTOM"],
                    pragma,
                    queue.read(),
                )
            )
    finally:
        sender.close()


def test_namespaces_coexist() -> None:
    env = {
        "BROKER_CACHE_MB": "11",
        "WEFT_CACHE_MB": "22",
        "TAUT_CACHE_MB": "33",
        "WEFT_CUSTOM": "kept",
        "CACHE_MB": "99",
    }
    configs = [resolve_config(prefix, env=env) for prefix in ("BROKER", "WEFT", "TAUT")]
    assert [c["CACHE_MB"] for c in configs] == [11, 22, 33]
    assert configs[1]["CUSTOM"] == "kept"
    assert all("WEFT_CACHE_MB" not in c for c in configs)


@pytest.mark.parametrize("subclass", [False, True])
def test_snapshot_retained_at_construction_and_lazy_acquisition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, subclass: bool
) -> None:
    config = resolve_config(
        "WEFT",
        defaults=defaults(),
        env={"WEFT_CACHE_MB": "22", "WEFT_RETENTION_DAYS": "9"},
    )
    if subclass:

        class ApplicationConfig(Config):
            pass

        config = ApplicationConfig(config, prefix=config.prefix, defaults=defaults())
    monkeypatch.setenv("BROKER_CACHE_MB", "invalid")
    queue = Queue(
        "jobs", db_path=str(tmp_path / "queue.db"), persistent=True, config=config
    )
    watcher = QueueWatcher(queue, lambda *_: None, config=config)
    try:
        assert queue._config is config
        assert watcher._config is config
        monkeypatch.setenv("BROKER_BUSY_TIMEOUT", "invalid at acquisition")
        queue.write("retained")
        with queue.sidecar() as session:
            assert list(session.run("PRAGMA cache_size", fetch=True)) == [(-22 * 1024,)]
        assert queue.read() == "retained"
        assert watcher._config.prefix == "WEFT"
        assert watcher._config["RETENTION_DAYS"] == 9
    finally:
        watcher.stop()
        queue.close()


def test_spawn_round_trip_uses_declared_units(tmp_path: Path) -> None:
    config = resolve_config(
        "WEFT",
        defaults=defaults(),
        env={"WEFT_CACHE_MB": "22", "WEFT_RETENTION_DAYS": "9", "WEFT_CUSTOM": "kept"},
    )
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    child = context.Process(
        target=spawn_consumer,
        args=(serialize_config(config), str(tmp_path / "spawn.db"), sender),
    )
    child.start()
    sender.close()
    try:
        assert receiver.poll(20), "spawned consumer did not return"
        assert receiver.recv() == ("WEFT", 9, "kept", -22 * 1024, "child")
        child.join(10)
        assert child.exitcode == 0
    finally:
        receiver.close()
        if child.is_alive():
            child.terminate()
        child.join(10)


def _plain_mapping() -> Any:
    return cast(Any, {"CACHE_MB": 5})


@pytest.mark.parametrize(
    "construct",
    [
        lambda path: Queue("jobs", db_path=path, config=_plain_mapping()),
        lambda path: QueueWatcher(
            "jobs", lambda *_: None, db=path, config=_plain_mapping()
        ),
        lambda path: DBConnection(path, config=_plain_mapping()),
    ],
    ids=["queue", "watcher", "connection"],
)
def test_consumers_reject_a_non_config_at_construction(
    tmp_path: Path, construct: Callable[[str], object]
) -> None:
    target = tmp_path / "reject.db"
    with pytest.raises(TypeError, match="config must be a Config"):
        construct(str(target))
    assert not target.exists()


def test_per_call_config_must_be_a_config(tmp_path: Path) -> None:
    connection = DBConnection(str(tmp_path / "per-call.db"))
    try:
        with pytest.raises(TypeError, match="config must be a Config"):
            connection.get_connection(config=_plain_mapping())
    finally:
        connection.close()


def direct_config_consumer(config: Config, path: str, sender: Connection) -> None:
    try:
        os.environ["BROKER_CACHE_MB"] = "invalid"
        os.environ["WEFT_RETENTION_DAYS"] = "invalid"
        continued = resolve_config(
            config=config, override={"WEFT_RETENTION_DAYS": "10"}
        )
        with Queue("spawn", db_path=path, persistent=True, config=continued) as queue:
            queue.write("direct")
            with queue.sidecar() as session:
                cache = next(iter(session.run("PRAGMA cache_size", fetch=True)))[0]
            sender.send(
                (continued.prefix, continued["RETENTION_DAYS"], cache, queue.read())
            )
    finally:
        sender.close()


def test_direct_config_spawn_preserves_declarations_and_sqlite_settings(
    tmp_path: Path,
) -> None:
    config = resolve_config("WEFT", defaults=defaults(), override={"WEFT_CACHE_MB": 22})
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    child = context.Process(
        target=direct_config_consumer,
        args=(config, str(tmp_path / "direct.db"), sender),
    )
    child.start()
    sender.close()
    try:
        assert receiver.poll(20), "spawned Config consumer did not return"
        assert receiver.recv() == ("WEFT", 10, -22 * 1024, "direct")
        child.join(10)
        assert child.exitcode == 0
    finally:
        receiver.close()
        if child.is_alive():
            child.terminate()
        child.join(10)
