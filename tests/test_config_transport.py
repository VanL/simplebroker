"""Data-only config transport and receiver-owned validation across processes."""

from __future__ import annotations

import json
import os
import pickle
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from simplebroker import (
    DEFAULT_CONFIG,
    Config,
    ConfigField,
    deserialize_config,
    resolve_config,
    serialize_config,
)
from simplebroker.ext import InvalidConfigError


def test_envelope_carries_all_values_without_declarations() -> None:
    calls: list[Any] = []

    def validate(value: Any) -> Any:
        calls.append(value)
        return value

    fields = dict(DEFAULT_CONFIG)
    fields["CUSTOM"] = ConfigField(["original"], "custom", validate)
    config = resolve_config("WEFT", defaults=fields)
    calls.clear()
    payload = serialize_config(config)
    decoded = json.loads(payload)
    assert decoded == {"prefix": "WEFT", "values": dict(config)}
    assert calls == []
    restored = deserialize_config(payload, defaults=fields)
    assert dict(restored) == dict(config)
    assert payload == json.dumps(
        decoded, allow_nan=False, sort_keys=True, separators=(",", ":")
    )
    decoded["values"]["CUSTOM"].append("detached")
    assert config["CUSTOM"] == ["original"]
    assert fields["CUSTOM"].validator is validate


@pytest.mark.parametrize("prefix", ["WEFT", "TAUT"])
def test_json_values_and_namespace_round_trip(prefix: str) -> None:
    class Integer(int):
        pass

    values = {
        "CUSTOM": {
            "null": None,
            "bool": False,
            "zero": 0,
            "float": 0.125,
            "text": "雪 café",
            "list": [True, 42, ""],
            "empty": {},
            "empty_list": [],
            "subtype": Integer(3),
        },
        "WEFT_CUSTOM": "do not strip twice",
    }
    config = Config(values, prefix=prefix, defaults={})
    restored = deserialize_config(serialize_config(config), defaults={})
    assert restored.prefix == prefix
    assert dict(restored) == values
    assert type(restored) is Config
    assert type(restored["CUSTOM"]["subtype"]) is int
    assert restored is not config


@pytest.mark.parametrize("direction", ["export", "mapping"])
@pytest.mark.parametrize(
    "value",
    [
        ("private-secret",),
        {"private-secret"},
        b"private-secret",
        datetime(2026, 1, 1, tzinfo=UTC),
        object(),
        {1: "private-secret"},
    ],
)
def test_unsupported_types_are_rejected_without_values(
    direction: str, value: Any
) -> None:
    with pytest.raises(TypeError) as error:
        if direction == "export":
            serialize_config(Config({"CUSTOM": value}))
        else:
            deserialize_config({"prefix": "BROKER", "values": {"CUSTOM": value}})
    assert "private-secret" not in str(error.value)


@pytest.mark.parametrize("direction", ["export", "mapping", "text"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_values_are_rejected(direction: str, value: float) -> None:
    envelope = {"prefix": "BROKER", "values": {"CUSTOM": value}}
    with pytest.raises(ValueError):
        if direction == "export":
            serialize_config(Config({"CUSTOM": value}))
        else:
            deserialize_config(
                json.dumps(envelope) if direction == "text" else envelope
            )


@pytest.mark.parametrize("direction", ["export", "mapping"])
@pytest.mark.parametrize("container", ["list", "dict"])
def test_cycles_fail_but_shared_acyclic_values_work(
    direction: str, container: str
) -> None:
    value: Any = [] if container == "list" else {}
    if container == "list":
        value.append(value)
    else:
        value["self"] = value
    with pytest.raises(ValueError):
        if direction == "export":
            serialize_config(Config({"CUSTOM": value}))
        else:
            deserialize_config({"prefix": "BROKER", "values": {"CUSTOM": value}})
    shared = ["same"]
    mapped = {"prefix": "BROKER", "values": {"CUSTOM": [shared, shared]}}
    assert deserialize_config(mapped)["CUSTOM"] == [["same"], ["same"]]
    config = Config({"CUSTOM": [shared, shared]})
    assert deserialize_config(serialize_config(config))["CUSTOM"] == [
        ["same"],
        ["same"],
    ]


@pytest.mark.parametrize(
    "payload",
    [
        "{",
        "null",
        "[]",
        "1",
        '"private-secret"',
        {},
        {"prefix": "WEFT"},
        {"values": {}},
        {"prefix": 1, "values": {}},
        {"prefix": "WEFT", "values": []},
    ],
)
def test_invalid_envelope_is_rejected_without_values(payload: Any) -> None:
    with pytest.raises(ValueError) as error:
        deserialize_config(payload)
    assert "private-secret" not in str(error.value)


def test_extra_envelope_metadata_cannot_select_declarations() -> None:
    payload = {
        "prefix": "APP",
        "values": {"CUSTOM": 8},
        "defaults": "private.module:loader",
        "class": "private.Class",
        "validators": {"CUSTOM": "callable"},
    }
    assert dict(deserialize_config(payload, defaults={})) == {"CUSTOM": 8}
    assert payload["values"] == {"CUSTOM": 8}


@pytest.mark.parametrize("key", ["lower", "", "1BAD", "BAD-KEY"])
def test_invalid_canonical_field_names_fail_receiver_resolution(key: str) -> None:
    with pytest.raises(ValueError):
        deserialize_config({"prefix": "WEFT", "values": {key: 1}})


def test_receiver_defaults_validators_and_later_overrides_are_local() -> None:
    sender = Config({"CUSTOM": "8"}, prefix="APP", defaults={})
    fields = {
        "CUSTOM": ConfigField(3, "integer", lambda value: int(value)),
        "OMITTED": ConfigField(12, "receiver default", int),
    }
    restored = deserialize_config(serialize_config(sender), defaults=fields)
    assert dict(restored) == {"CUSTOM": 8, "OMITTED": 12}
    assert resolve_config(config=restored, override={"APP_CUSTOM": "9"})["CUSTOM"] == 9
    with pytest.warns(UserWarning), pytest.raises(InvalidConfigError):
        resolve_config(config=restored, override={"APP_CUSTOM": "bad"})

    def small_integer(value: Any) -> int:
        result = int(value)
        if result >= 4:
            raise ValueError("expected small integer")
        return result

    with pytest.warns(UserWarning), pytest.raises(InvalidConfigError):
        deserialize_config(
            serialize_config(sender),
            defaults={"CUSTOM": ConfigField(3, "small integer", small_integer)},
        )


def test_receiver_ignores_ambient_and_retains_units(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_CACHE_MB", "invalid")
    monkeypatch.setenv("WEFT_CACHE_MB", "invalid")
    payload: dict[str, Any] = {
        "prefix": "WEFT",
        "values": {"CACHE_MB": "22", "VACUUM_THRESHOLD": 0.1},
    }
    restored = deserialize_config(payload)
    assert restored["CACHE_MB"] == 22
    assert restored["VACUUM_THRESHOLD"] == 0.1
    assert payload["values"]["CACHE_MB"] == "22"
    with pytest.warns(UserWarning), pytest.raises(InvalidConfigError):
        deserialize_config({"prefix": "WEFT", "values": {"VACUUM_THRESHOLD": True}})


def test_transport_preserves_secret_data_but_not_sender_class() -> None:
    class ApplicationConfig(Config):
        pass

    sender = ApplicationConfig({"BACKEND_PASSWORD": "private-secret"}, prefix="WEFT")
    payload = serialize_config(sender)
    assert json.loads(payload)["values"]["BACKEND_PASSWORD"] == "private-secret"
    restored = deserialize_config(payload)
    assert type(restored) is Config
    assert restored["BACKEND_PASSWORD"] == "private-secret"


_STARTUP_SCRIPT = """
import json
import sys
from simplebroker import DEFAULT_CONFIG, ConfigField, Queue, deserialize_config

def retention(value):
    result = int(value)
    if result < 0:
        raise ValueError("retention must be nonnegative")
    return result

fields = dict(DEFAULT_CONFIG)
fields["RETENTION_DAYS"] = ConfigField(7, "days", retention)
config = deserialize_config(sys.stdin.read(), defaults=fields)
with Queue("child", db_path=sys.argv[1], persistent=True, config=config) as queue:
    queue.write("startup")
    with queue.sidecar() as session:
        cache = next(iter(session.run("PRAGMA cache_size", fetch=True)))[0]
    print(json.dumps([config.prefix, config["RETENTION_DAYS"], cache, queue.read()]))
"""


@pytest.mark.parametrize("invalid", [False, True])
def test_json_stdin_startup_validates_before_opening_queue(
    tmp_path: Path, invalid: bool
) -> None:
    config = resolve_config(
        "WEFT",
        override={"WEFT_CACHE_MB": 22, "WEFT_RETENTION_DAYS": -1 if invalid else 9},
    )
    path = tmp_path / "child.db"
    result = subprocess.run(
        [sys.executable, "-c", _STARTUP_SCRIPT, str(path)],
        input=serialize_config(config),
        text=True,
        capture_output=True,
        env={**os.environ, "BROKER_CACHE_MB": "invalid", "WEFT_CACHE_MB": "invalid"},
        timeout=20,
        check=False,
    )
    if invalid:
        assert result.returncode != 0
        assert "InvalidConfigError" in result.stderr
        assert not path.exists()
    else:
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == ["WEFT", 9, -22 * 1024, "startup"]
        assert path.exists()


def test_mapping_reception_preserves_shallow_value_ownership() -> None:
    nested = {"items": [1, 2]}
    payload = {"prefix": "APP", "values": {"CUSTOM": nested}}
    restored = deserialize_config(payload, defaults={})
    assert restored["CUSTOM"] is nested
    assert payload == {"prefix": "APP", "values": {"CUSTOM": {"items": [1, 2]}}}


def _pickle_retention(value: Any) -> int:
    result = int(value)
    if result < 0:
        raise ValueError("retention must be nonnegative")
    return result


class _PickleApplicationConfig(Config):
    """Module-owned class whose ordinary additional state must survive pickle."""

    __slots__ = ("slot_metadata",)
    slot_metadata: list[str]
    label: str
    self_reference: _PickleApplicationConfig


@pytest.mark.parametrize("protocol", range(pickle.HIGHEST_PROTOCOL + 1))
def test_pickle_protocols_preserve_values_prefix_and_declarations(
    protocol: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    fields = dict(DEFAULT_CONFIG)
    fields["RETENTION_DAYS"] = ConfigField(7, "retention", _pickle_retention)
    config = resolve_config(
        "WEFT", defaults=fields, override={"WEFT_RETENTION_DAYS": 9}
    )
    monkeypatch.setenv("WEFT_RETENTION_DAYS", "invalid")
    monkeypatch.setenv("BROKER_CACHE_MB", "invalid")
    restored = pickle.loads(pickle.dumps(config, protocol=protocol))
    assert type(restored) is Config
    assert restored is not config
    assert restored.prefix == "WEFT"
    assert dict(restored) == dict(config)
    assert (
        resolve_config(config=restored, override={"WEFT_RETENTION_DAYS": "10"})[
            "RETENTION_DAYS"
        ]
        == 10
    )
    with pytest.warns(UserWarning), pytest.raises(InvalidConfigError):
        resolve_config(config=restored, override={"WEFT_RETENTION_DAYS": -1})
    with pytest.raises(TypeError):
        cast(Any, restored)["CACHE_MB"] = 1


def test_pickle_rejects_local_validators_but_json_does_not_transport_them() -> None:
    config = resolve_config(
        "APP", defaults={"COUNT": ConfigField(1, "count", lambda value: int(value))}
    )
    with pytest.raises((AttributeError, pickle.PicklingError)):
        pickle.dumps(config)
    assert (
        deserialize_config(
            serialize_config(config), defaults={"COUNT": ConfigField(2, "count", int)}
        )["COUNT"]
        == 1
    )


def test_pickle_does_not_revalidate_and_retains_non_json_values() -> None:
    # Direct Config construction carries resolved values without invoking validators.
    # A validator that would reject the current value must not run on unpickle.
    values = {
        "COUNT": -1,
        "BINARY": b"bytes",
        "TUPLE": (1, 2),
        "SET": {3},
        "WHEN": datetime(2026, 1, 1, tzinfo=UTC),
    }
    config = Config(
        values,
        prefix="APP",
        defaults={"COUNT": ConfigField(1, "count", _pickle_retention)},
    )
    restored = pickle.loads(pickle.dumps(config))
    assert dict(restored) == values
    assert restored.prefix == "APP"
    with pytest.raises(TypeError):
        serialize_config(config)


@pytest.mark.parametrize("protocol", [0, pickle.HIGHEST_PROTOCOL])
def test_pickle_subclass_dictionary_slots_and_cycles(protocol: int) -> None:
    config = _PickleApplicationConfig({"COUNT": 1}, prefix="APP", defaults={})
    config.slot_metadata = ["slot"]
    config.label = "dictionary attribute"
    config.self_reference = config
    restored = pickle.loads(pickle.dumps(config, protocol=protocol))
    assert type(restored) is _PickleApplicationConfig
    assert restored.slot_metadata == ["slot"]
    assert restored.label == "dictionary attribute"
    assert restored.self_reference is restored
    assert dict(restored) == {"COUNT": 1}
    assert restored.prefix == "APP"
