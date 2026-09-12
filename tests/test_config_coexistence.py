"""Real broker and spawned-process use of a composed application snapshot."""

from __future__ import annotations

import multiprocessing
import os
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

import pytest

from simplebroker import Queue
from simplebroker.config import (
    CONFIG_DEFAULTS,
    ConfigField,
    ConfigSchema,
    ConfigSnapshot,
    build_config,
    resolve_config,
    snapshot_config,
)
from simplebroker.watcher import QueueWatcher


def _retention(value: Any) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("retention must be a nonnegative integer")
    return value


def _schema() -> ConfigSchema:
    return CONFIG_DEFAULTS.derive(
        fields={"retention_days": ConfigField(7, "retention days", _retention, int)}
    )


def _spawn_consumer(values: dict[str, Any], path: str, sender: Connection) -> None:
    try:
        os.environ["BROKER_CACHE_MB"] = "invalid"
        os.environ["WEFT_RETENTION_DAYS"] = "invalid"
        config = ConfigSnapshot.from_values(values, schema=_schema(), prefix="WEFT")
        with Queue("spawn", db_path=path, persistent=True, config=config) as queue:
            queue.write("child")
            with queue.sidecar() as session:
                pragma = next(iter(session.run("PRAGMA cache_size", fetch=True)))[0]
            sender.send((config["retention_days"], pragma, queue.read()))
    finally:
        sender.close()


def test_prefixes_coexist_and_ignore_unknown_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = {
        "BROKER_CACHE_MB": "11",
        "WEFT_CACHE_MB": "22",
        "TAUT_CACHE_MB": "33",
        "BROKER_TYPO_XYZ": "invalid",
        "BROKER_VACUUM_LOCK_TIMEOUT": "invalid",
        "WEFTX_RETENTION_DAYS": "invalid",
        "WEFT_UNKNOWN": "invalid",
    }
    original = dict(env)
    configs = [
        build_config(prefix, env=env, defaults=_schema())
        for prefix in ("BROKER", "WEFT", "TAUT")
    ]
    assert [c["cache_mb"] for c in configs] == [11, 22, 33]
    assert env == original
    monkeypatch.setenv("BROKER_CACHE_MB", "invalid")
    assert build_config("BROKER")["cache_mb"] == CONFIG_DEFAULTS["cache_mb"].default
    assert (
        build_config("WEFT", env={"BROKER_CACHE_MB": "invalid"}, defaults=_schema())[
            "retention_days"
        ]
        == 7
    )
    assert configs[1]["BROKER_CACHE_MB"] == configs[1]["WEFT_CACHE_MB"] == 22
    with pytest.raises(KeyError):
        _ = configs[1]["TAUT_CACHE_MB"]


def test_one_composed_snapshot_reaches_queue_watcher_and_sqlite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = build_config(
        "WEFT",
        env={"WEFT_CACHE_MB": "22", "WEFT_RETENTION_DAYS": "9"},
        defaults=_schema(),
    )
    monkeypatch.setenv("BROKER_CACHE_MB", "invalid")
    monkeypatch.setenv("WEFT_RETENTION_DAYS", "invalid")
    assert resolve_config(config) is config
    assert snapshot_config(config) is config
    with Queue(
        "jobs", db_path=str(tmp_path / "queue.db"), persistent=True, config=config
    ) as queue:
        assert queue._config is config
        watcher = QueueWatcher(queue, lambda *_: None, config=config)
        try:
            assert watcher._config is config
            assert watcher._config["retention_days"] == 9
            queue.write("retained")
            with queue.sidecar() as session:
                assert list(session.run("PRAGMA cache_size", fetch=True)) == [
                    (-22 * 1024,)
                ]
            assert queue.read() == "retained"
        finally:
            watcher.stop()


def test_snapshot_subclass_supported_without_ambient_reread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ApplicationSnapshot(ConfigSnapshot):
        pass

    base = build_config("WEFT", env={"WEFT_CACHE_MB": "22"}, defaults=_schema())
    config = ApplicationSnapshot.from_values(
        base.to_values(), schema=base.schema, prefix="WEFT"
    )
    monkeypatch.setenv("BROKER_CACHE_MB", "invalid")
    with Queue("jobs", db_path=str(tmp_path / "subclass.db"), config=config) as queue:
        assert queue._config["retention_days"] == 7
        queue.write("subclass")
        assert queue.read() == "subclass"


def test_pure_application_schema_builds_but_is_not_a_broker_receipt(
    tmp_path: Path,
) -> None:
    schema = ConfigSchema(
        {"retention_days": ConfigField(7, "retention", _retention, int)}
    )
    config = build_config("APP", defaults=schema)
    assert config.to_values() == {"retention_days": 7}
    path = tmp_path / "must-not-open.db"
    with pytest.raises(ValueError):
        Queue("jobs", db_path=str(path), config=config)
    assert not path.exists()


def test_spawn_reconstructs_owned_schema_and_consumes_canonical_values(
    tmp_path: Path,
) -> None:
    config = build_config(
        "WEFT",
        env={"WEFT_CACHE_MB": "22", "WEFT_RETENTION_DAYS": "9"},
        defaults=_schema(),
    )
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    child = context.Process(
        target=_spawn_consumer,
        args=(config.to_values(), str(tmp_path / "spawn.db"), sender),
    )
    child.start()
    sender.close()
    try:
        assert receiver.poll(20), "spawned config consumer did not return"
        assert receiver.recv() == (9, -22 * 1024, "child")
        child.join(10)
        assert child.exitcode == 0
    finally:
        receiver.close()
        if child.is_alive():
            child.terminate()
        child.join(10)


def test_legacy_opaque_canonical_names_do_not_drive_broker_settings(
    tmp_path: Path,
) -> None:
    from simplebroker.config import ResolvedConfig, _overlay_config, canonical_config

    config = ResolvedConfig(
        {
            "BROKER_INITIAL_CHECKS": 13,
            "BROKER_CACHE_MB": 19,
            "initial_checks": "opaque polling data",
            "cache_mb": "opaque cache data",
        }
    )
    overlay = _overlay_config(config, {"initial_checks": "replacement opaque"})
    assert overlay["initial_checks"] == "replacement opaque"
    assert canonical_config(overlay)["initial_checks"] == 13
    with Queue("opaque", db_path=str(tmp_path / "opaque.db"), config=config) as queue:
        watcher = QueueWatcher(queue, lambda *_: None)
        try:
            assert watcher._create_strategy()._initial_checks == 13
            queue.write("ok")
            with queue.sidecar() as session:
                assert list(session.run("PRAGMA cache_size", fetch=True)) == [
                    (-19 * 1024,)
                ]
        finally:
            watcher.stop()


def test_custom_schema_cannot_bypass_broker_canonical_validation(
    tmp_path: Path,
) -> None:
    fields = dict(CONFIG_DEFAULTS)
    fields["sync_mode"] = ConfigField("NOTVALID", "custom mode")
    config = build_config("APP", defaults=ConfigSchema(fields))
    with pytest.raises(ValueError):
        Queue("invalid", db_path=str(tmp_path / "invalid.db"), config=config)
    assert not (tmp_path / "invalid.db").exists()


def test_plugin_view_preserves_receipt_across_all_resolver_gates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from simplebroker._project_config import _config_snapshot
    from simplebroker.config import _overlay_config, legacy_config

    config = build_config("WEFT", defaults=_schema(), options={"cache_mb": 31})
    view = legacy_config(config)
    monkeypatch.setenv("BROKER_CACHE_MB", "invalid")
    for resolve in (resolve_config, snapshot_config, _config_snapshot):
        assert resolve(view) is config
    assert _overlay_config(config, view) is config
    assert _overlay_config(config, {"BROKER_CACHE_MB": 32})["cache_mb"] == 32


def test_legacy_plugin_mapping_keeps_exact_opaque_keys() -> None:
    from simplebroker.config import legacy_config

    original: Any = {"BROKER_CACHE_MB": 19, "cache_mb": "opaque", 1: "numeric extra"}
    view = legacy_config(original)
    assert view is original
    assert dict(view) == original
    assert view["cache_mb"] == "opaque"
