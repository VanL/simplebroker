"""Public shared-builder behavior, using a schema owned by a small embedder."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from simplebroker._exceptions import InvalidConfigError
from simplebroker.config import (
    CONFIG_DEFAULTS,
    ConfigField,
    ConfigSchema,
    ConfigSnapshot,
    build_config,
)


def nonnegative(value: Any) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("expected nonnegative integer")
    return value


def app_schema(**kwargs: Any) -> ConfigSchema:
    return CONFIG_DEFAULTS.derive(
        fields={
            "retention_days": ConfigField(
                7, "nonnegative retention days", nonnegative, int
            ),
        },
        **kwargs,
    )


@pytest.mark.parametrize(
    "source, expected",
    [("default", 7), ("environment", 8), ("file", 9), ("override", 10)],
)
def test_embedder_field_from_each_source(
    tmp_path: Path, source: str, expected: int
) -> None:
    path = tmp_path / "settings.toml"
    path.write_text("WEFT_RETENTION_DAYS = 9\n")
    config = build_config(
        "WEFT",
        env={"WEFT_RETENTION_DAYS": "8"} if source != "default" else {},
        config_file=path if source in ("file", "override") else None,
        options={"retention_days": 10} if source == "override" else None,
        defaults=app_schema(),
    )
    assert config["retention_days"] == expected
    assert config.sources["retention_days"] == source
    assert config["WEFT_RETENTION_DAYS"] == expected
    assert "WEFT_RETENTION_DAYS" in config
    assert config.get("WEFT_RETENTION_DAYS") == expected
    assert list(config).count("retention_days") == 1
    assert "WEFT_RETENTION_DAYS" not in config.to_values()


@pytest.mark.parametrize("source", ["env", "file", "options"])
def test_embedder_validator_rejects_invalid_selected_field(source: str) -> None:
    kwargs: dict[str, Any] = (
        {"env": {"WEFT_RETENTION_DAYS": "-1"}}
        if source == "env"
        else {
            "config_file" if source == "file" else "options": {
                "WEFT_RETENTION_DAYS" if source == "file" else "retention_days": -1
            }
        }
    )
    with pytest.raises(InvalidConfigError):
        build_config("WEFT", defaults=app_schema(), **kwargs)


def test_real_toml_coexists_with_project_target_format(tmp_path: Path) -> None:
    from simplebroker._project_config import load_project_config

    path = tmp_path / ".broker.toml"
    path.write_text("""version = 1
backend = "postgres"
target = "postgresql://localhost/example"
BROKER_CACHE_MB = 11
WEFT_CACHE_MB = 22
WEFT_RETENTION_DAYS = 9
WEFT_UNKNOWN = "ignored"
retention_days = -1
TAUT_RETENTION_DAYS = "invalid other namespace"
[backend_options]
schema = "example"
""")
    assert build_config("BROKER", config_file=path)["cache_mb"] == 11
    config = build_config("WEFT", config_file=path, defaults=app_schema())
    assert (config["cache_mb"], config["retention_days"]) == (22, 9)
    target = load_project_config(path)
    assert target["backend"] == "postgres"
    assert target["target"] == "postgresql://localhost/example"
    assert target["backend_options"]["schema"] == "example"


def test_derived_schema_does_not_change_shared_defaults() -> None:
    original = build_config("BROKER")
    schema = app_schema(defaults={"cache_mb": 22})
    assert build_config("WEFT", defaults=schema)["cache_mb"] == 22
    assert build_config("BROKER").to_values() == original.to_values()
    with pytest.raises(InvalidConfigError):
        build_config(
            "WEFT", defaults=CONFIG_DEFAULTS.derive(defaults={"cache_mb": "bad"})
        )
    with pytest.raises(ValueError):
        CONFIG_DEFAULTS.derive(fields={"cache_mb": ConfigField(1, "replacement")})


@pytest.mark.parametrize("validate_base", [True, False])
def test_source_validation_timing(validate_base: bool) -> None:
    schema = app_schema(validate_base=validate_base)
    kwargs: dict[str, Any] = {
        "env": {"WEFT_RETENTION_DAYS": "bad"},
        "options": {"retention_days": 3},
        "defaults": schema,
    }
    if validate_base:
        with pytest.raises(InvalidConfigError) as error:
            build_config("WEFT", **kwargs)
        assert error.value.source == "environment"
    else:
        assert build_config("WEFT", **kwargs)["retention_days"] == 3


def test_embedder_can_choose_documented_source_priority() -> None:
    schema = app_schema(source_order=("env", "options", "file"))
    assert (
        build_config(
            "WEFT",
            env={"WEFT_RETENTION_DAYS": "8"},
            config_file={"WEFT_RETENTION_DAYS": 9},
            options={"retention_days": 10},
            defaults=schema,
        )["retention_days"]
        == 8
    )


def dependency_schema() -> ConfigSchema:
    return ConfigSchema(
        {
            "base": ConfigField(2, "nonnegative integer", nonnegative, int),
            "twice": ConfigField(
                None,
                "nonnegative integer",
                nonnegative,
                int,
                dependencies=("base",),
                default_factory=lambda v: v["base"] * 2,
            ),
            "chain": ConfigField(
                None,
                "nonnegative integer",
                nonnegative,
                int,
                dependencies=("twice",),
                default_factory=lambda v: v["twice"] + 1,
            ),
        }
    )


def test_derived_provenance_and_explicit_equal_defaults() -> None:
    original = build_config("APP", defaults=dependency_schema())
    changed = original.with_options({"base": 3})
    assert changed.to_values() == {"base": 3, "twice": 6, "chain": 7}
    assert changed.sources == {
        "base": "override",
        "twice": "derived",
        "chain": "derived",
    }
    assert original.to_values() == {"base": 2, "twice": 4, "chain": 5}
    explicit = original.with_options({"twice": 4}).with_options({"base": 5})
    assert explicit.to_values() == {"base": 5, "twice": 4, "chain": 5}
    assert explicit.sources["twice"] == "override"
    assert changed.with_options({"base": 3}).to_values() == changed.to_values()
    restored = ConfigSnapshot.from_values(
        original.to_values(), schema=dependency_schema(), prefix="APP"
    )
    assert restored.with_options({"base": 10})["twice"] == 4


@pytest.mark.parametrize("source", ["env", "file"])
def test_supplied_derived_field_stays_supplied(source: str) -> None:
    kwargs: dict[str, Any] = {
        "env" if source == "env" else "config_file": {
            "APP_TWICE": "4" if source == "env" else 4
        }
    }
    config = build_config("APP", defaults=dependency_schema(), **kwargs).with_options(
        {"base": 8}
    )
    assert config["twice"] == 4
    assert config["chain"] == 5


def test_dependencies_reject_cycles_missing_fields_and_invalid_results() -> None:
    for dependency in ("missing", "value"):
        with pytest.raises(ValueError):
            ConfigSchema({"value": ConfigField(0, "value", dependencies=(dependency,))})
    schema = ConfigSchema(
        {
            "base": ConfigField(0, "integer", nonnegative),
            "derived": ConfigField(
                None,
                "nonnegative",
                nonnegative,
                dependencies=("base",),
                default_factory=lambda v: 1 - v["base"],
            ),
        }
    )
    with pytest.raises(InvalidConfigError) as error:
        build_config("APP", defaults=schema).with_options({"base": 2})
    assert error.value.source == "derived"


def test_canonical_transport_is_complete_and_does_not_rescale() -> None:
    config = build_config(
        "WEFT", env={"WEFT_VACUUM_THRESHOLD": "20"}, defaults=app_schema()
    )
    restored = ConfigSnapshot.from_values(
        config.to_values(), schema=config.schema, prefix="WEFT"
    )
    assert restored["vacuum_threshold"] == 0.2
    assert restored.with_options({"vacuum_threshold": 0.3})["vacuum_threshold"] == 0.3
    incomplete = restored.to_values()
    del incomplete["retention_days"]
    with pytest.raises(ValueError):
        ConfigSnapshot.from_values(incomplete, schema=config.schema)


def test_file_errors_are_not_silently_replaced_with_defaults(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        build_config("BROKER", config_file=tmp_path / "absent.toml")
    path = tmp_path / "bad.toml"
    path.write_text("BROKER_CACHE_MB = [")
    with pytest.raises(ValueError):
        build_config("BROKER", config_file=path)


@pytest.mark.parametrize("value", [False, 0, "", None])
def test_explicit_falsy_values_are_not_treated_as_omitted(value: Any) -> None:
    schema = ConfigSchema({"setting": ConfigField("fallback", "optional setting")})
    config = build_config("APP", defaults=schema, options={"setting": value})
    assert config["setting"] == value
    assert type(config["setting"]) is type(value)
    assert config.sources["setting"] == "override"


@pytest.mark.parametrize("source", ["env", "file"])
def test_external_percentage_units_normalize_exactly_once(source: str) -> None:
    kwargs: dict[str, Any] = {
        "env" if source == "env" else "config_file": {
            "APP_VACUUM_THRESHOLD": "20" if source == "env" else 20
        }
    }
    config = build_config("APP", **kwargs)
    assert config["vacuum_threshold"] == 0.2
    assert ConfigSnapshot.from_values(config.to_values())["vacuum_threshold"] == 0.2


def test_snapshot_nested_values_and_transport_are_detached() -> None:
    raw = {"labels": ["original"]}
    schema = ConfigSchema({"metadata": ConfigField(raw, "application metadata")})
    raw["labels"].append("mutated input")
    config = build_config("APP", defaults=schema)
    assert config["metadata"]["labels"] == ("original",)
    with pytest.raises(TypeError):
        config["metadata"]["labels"] = ()
    payload = config.to_values()
    payload["metadata"]["labels"].append("mutated transport")
    assert config["metadata"]["labels"] == ("original",)


def test_transport_preserves_hashable_set_members() -> None:
    schema = ConfigSchema({"pairs": ConfigField({("x", "y")}, "pairs")})
    snapshot = build_config("APP", defaults=schema)
    payload = snapshot.to_values()
    assert payload == {"pairs": {("x", "y")}}
    assert (
        ConfigSnapshot.from_values(payload, schema=schema)["pairs"] == snapshot["pairs"]
    )


def test_legacy_nonstring_opaque_keys_remain_preserved() -> None:
    from simplebroker.config import ResolvedConfig

    opaque: Any = {1: "opaque"}
    key: Any = 1
    config = ResolvedConfig(opaque)
    assert config[key] == "opaque"
    assert dict(config)[key] == "opaque"


@pytest.mark.parametrize("failure", ["factory", "validator"])
def test_sensitive_derived_failures_are_typed_and_redacted(failure: str) -> None:
    secret = "private-secret-value"

    def derive(_values: Any) -> str:
        if failure == "factory":
            raise ValueError(secret)
        return secret

    def validate(_value: Any) -> Any:
        raise ValueError(secret)

    schema = ConfigSchema(
        {
            "credential": ConfigField(
                None,
                "a valid credential",
                validate,
                sensitive=True,
                default_factory=derive,
            ),
        }
    )
    with pytest.raises(InvalidConfigError) as error:
        build_config("APP", defaults=schema)
    assert error.value.source == "derived"
    assert error.value.key == "credential"
    assert error.value.value_display == "<redacted>"
    assert secret not in str(error.value)
    assert secret not in repr(schema["credential"])


def test_file_validation_error_retains_selected_source(tmp_path: Path) -> None:
    path = tmp_path / "settings.toml"
    path.write_text('APP_DEFAULT_DB_NAME = "/absolute/database.db"\n')
    with pytest.raises(InvalidConfigError) as error:
        build_config("APP", config_file=path)
    assert error.value.key == "APP_DEFAULT_DB_NAME"
    assert error.value.source == "file"
