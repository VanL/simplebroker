"""Public ambient-free embedding configuration contract [SB-API-2]."""

from __future__ import annotations

import json
import threading
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from simplebroker import (
    Queue,
    QueueWatcher,
    ResolvedConfig,
    load_lines,
    open_broker,
    resolve_config,
    resolve_isolated_config,
    snapshot_config,
    target_for_directory,
)
from simplebroker.ext import InvalidConfigError, SQLiteRunner

if TYPE_CHECKING:
    _ordinary_config_type: dict[str, Any] = resolve_config()
    _isolated_config_type: ResolvedConfig = resolve_config(resolve_isolated_config({}))
    _snapshot_config_type: ResolvedConfig = snapshot_config()


def _invalid_ambient(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_BUSY_TIMEOUT", "not-an-integer")


def _isolated(**overrides: object) -> ResolvedConfig:
    return resolve_isolated_config(
        {
            "BROKER_DEFAULT_DB_NAME": "isolated.db",
            "BROKER_AUTO_VACUUM": "0",
            **overrides,
        }
    )


def test_isolated_resolver_is_complete_immutable_and_ambient_free(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _invalid_ambient(monkeypatch)

    config = _isolated(BROKER_BUSY_TIMEOUT="41")

    assert isinstance(config, ResolvedConfig)
    assert isinstance(config, Mapping)
    assert len(config) == 32
    assert config["BROKER_BUSY_TIMEOUT"] == 41
    assert config["BROKER_CACHE_MB"] == 10
    with pytest.raises(TypeError):
        config["BROKER_BUSY_TIMEOUT"] = 42  # type: ignore[index]
    with pytest.raises(AttributeError):
        config._values = {}  # type: ignore[misc]


def test_isolated_resolver_rejects_unknown_and_direct_invalid_values() -> None:
    with pytest.raises(InvalidConfigError) as unknown:
        resolve_isolated_config({"BROKER_NOT_A_FIELD": "value"})
    assert unknown.value.key == "BROKER_NOT_A_FIELD"

    with pytest.raises(InvalidConfigError) as invalid:
        ResolvedConfig({"BROKER_BUSY_TIMEOUT": "not-an-integer"})
    assert invalid.value.key == "BROKER_BUSY_TIMEOUT"

    secret = resolve_isolated_config({"BROKER_BACKEND_PASSWORD": "do-not-print"})
    assert "do-not-print" not in repr(secret)


def test_isolated_resolver_preserves_unknown_only_when_requested() -> None:
    config = resolve_isolated_config(
        {"BROKER_EMBEDDER_METADATA": "kept"},
        preserve_unknown=True,
    )

    assert len(config) == 33
    assert config["BROKER_EMBEDDER_METADATA"] == "kept"
    assert config["BROKER_CACHE_MB"] == 10


def test_resolved_config_copies_top_level_but_leaves_opaque_values_owned() -> None:
    nested = {"mutable": "first"}
    source = {
        "BROKER_CACHE_MB": "13",
        "BROKER_EXTENSION_STATE": nested,
    }

    config = ResolvedConfig(source)
    source["BROKER_CACHE_MB"] = "99"
    source["BROKER_EXTENSION_STATE"] = {"replacement": True}
    nested["mutable"] = "second"

    assert config["BROKER_CACHE_MB"] == 13
    assert config["BROKER_EXTENSION_STATE"] is nested
    assert config["BROKER_EXTENSION_STATE"] == {"mutable": "second"}


def test_canonical_looking_extra_is_opaque_on_every_permissive_path() -> None:
    typo = {"BROKER_BUSY_TIMOUT": "1"}

    direct = ResolvedConfig(typo)
    isolated = resolve_isolated_config(typo, preserve_unknown=True)
    ordinary = snapshot_config(typo)

    for config in (direct, isolated, ordinary):
        assert config["BROKER_BUSY_TIMOUT"] == "1"
        assert config["BROKER_BUSY_TIMEOUT"] != 1


def test_resolved_config_subclass_is_revalidated_to_exact_marker() -> None:
    class DerivedResolvedConfig(ResolvedConfig):
        pass

    derived = DerivedResolvedConfig({"BROKER_CACHE_MB": "17"})

    resolved = resolve_config(derived)

    assert type(resolved) is ResolvedConfig
    assert resolved is not derived
    assert resolved["BROKER_CACHE_MB"] == 17


def test_ordinary_resolution_retains_ambient_base_and_unknown_passthrough(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_CACHE_MB", "17")
    config = resolve_config({"BROKER_EMBEDDER_METADATA": "kept"})
    assert type(config) is dict
    assert config["BROKER_CACHE_MB"] == 17
    assert config["BROKER_EMBEDDER_METADATA"] == "kept"

    _invalid_ambient(monkeypatch)
    with pytest.raises(InvalidConfigError, match="BROKER_BUSY_TIMEOUT"):
        resolve_config({"BROKER_BUSY_TIMEOUT": "41"})


def test_snapshot_factory_captures_ambient_once_and_reuses_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_CACHE_MB", "17")
    source = {"BROKER_EMBEDDER_METADATA": "first"}

    config = snapshot_config(source)
    source["BROKER_EMBEDDER_METADATA"] = "second"
    _invalid_ambient(monkeypatch)

    assert config["BROKER_CACHE_MB"] == 17
    assert config["BROKER_EMBEDDER_METADATA"] == "first"
    assert snapshot_config(config) is config
    assert resolve_config(config) is config


def test_opaque_extra_does_not_select_core_builtin_backend(tmp_path: Path) -> None:
    config = snapshot_config({"_BROKER_INTERNAL_BACKEND": "missing"})

    runner = SQLiteRunner(str(tmp_path / "opaque-extra.db"), config=config)
    try:
        assert runner.get_connection() is not None
    finally:
        runner.close()


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


def test_exact_marker_reuse_does_not_consult_later_ambient_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _isolated(BROKER_CACHE_MB="19")
    _invalid_ambient(monkeypatch)
    monkeypatch.setenv("BROKER_CACHE_MB", "not-an-integer")

    again = resolve_config(config)

    assert isinstance(again, ResolvedConfig)
    assert again is config
    assert again["BROKER_CACHE_MB"] == 19
    assert dict(again) == dict(config)
