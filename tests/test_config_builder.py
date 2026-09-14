"""One resolver, declared units and extensible namespaced settings."""

from __future__ import annotations

import os
import warnings
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from simplebroker import DEFAULT_CONFIG, Config, ConfigField, resolve_config
from simplebroker.ext import InvalidConfigError

# Invalid values warn as they are applied; tests here assert the raise or the
# warning explicitly.
pytestmark = pytest.mark.filterwarnings("ignore:.*ignoring invalid")


def days(value: Any) -> int:
    result = int(value)
    if result < 0:
        raise ValueError("days must be nonnegative")
    return result


def app_defaults() -> dict[str, ConfigField]:
    fields = dict(DEFAULT_CONFIG)
    fields["RETENTION_DAYS"] = ConfigField(7, "days", days)
    return fields


@pytest.mark.parametrize("source", ["toml", "env", "override"])
@pytest.mark.parametrize(
    "value,expected", [("10", 10), (10, 10), (10.0, 10), ("0.1", 0.1), (0.1, 0.1)]
)
def test_declared_percentage_has_one_meaning(
    source: str, value: Any, expected: float
) -> None:
    key = "APP_VACUUM_THRESHOLD"
    kwargs: dict[str, Any] = {source: {key: value}}
    config = resolve_config("APP", **kwargs)
    assert config["VACUUM_THRESHOLD"] == expected
    rederived = resolve_config(
        config=config, override={key: config["VACUUM_THRESHOLD"]}
    )
    assert dict(rederived) == dict(config)


@pytest.mark.parametrize("source", ["toml", "env", "override"])
@pytest.mark.parametrize("value", ["0.15", 0.15, "15", 15])
def test_jitter_keeps_fractional_unit(source: str, value: Any) -> None:
    key = "BROKER_JITTER_FACTOR"
    kwargs: dict[str, Any] = {source: {key: value}}
    assert resolve_config(**kwargs)["JITTER_FACTOR"] == float(value)


@pytest.mark.parametrize("value", [-1, 101])
def test_percent_bounds(value: int) -> None:
    with pytest.raises(InvalidConfigError):
        resolve_config(override={"BROKER_VACUUM_THRESHOLD": value})


@pytest.mark.parametrize("source", ["toml", "env", "override"])
def test_custom_validator_at_every_source(source: str) -> None:
    key = "APP_RETENTION_DAYS"
    valid: dict[str, Any] = {source: {key: "8"}}
    invalid: dict[str, Any] = {source: {key: "-1"}}
    assert (
        resolve_config("APP", defaults=app_defaults(), **valid)["RETENTION_DAYS"] == 8
    )
    with pytest.raises(InvalidConfigError):
        resolve_config("APP", defaults=app_defaults(), **invalid)


def test_precedence_and_defaults() -> None:
    fields = app_defaults()
    assert resolve_config(defaults=fields)["RETENTION_DAYS"] == 7
    sources: dict[str, Any] = {
        "toml": {"APP_RETENTION_DAYS": 8},
        "env": {"APP_RETENTION_DAYS": "9"},
        "override": {"APP_RETENTION_DAYS": 11},
    }
    for last, expected in [("override", 11), ("env", 9), ("toml", 8)]:
        assert (
            resolve_config("APP", defaults=fields, **sources)["RETENTION_DAYS"]
            == expected
        )
        del sources[last]


@pytest.mark.parametrize(
    "source,label", [("toml", "the TOML file"), ("env", "the environment")]
)
def test_invalid_lower_source_warns_and_later_valid_value_wins(
    source: str, label: str
) -> None:
    key = "APP_RETENTION_DAYS"
    kwargs: dict[str, Any] = {source: {key: "bad"}, "override": {key: 9}}
    with pytest.warns(UserWarning, match=f"ignoring invalid {key}=.* from {label}"):
        config = resolve_config("APP", defaults=app_defaults(), **kwargs)
    assert config["RETENTION_DAYS"] == 9


@pytest.mark.parametrize("value", [False, 0, "", None])
def test_explicit_falsy_custom_values(value: Any) -> None:
    config = resolve_config(defaults={}, override={"BROKER_CUSTOM": value})
    assert config["CUSTOM"] is value


def test_real_toml_and_namespace_selection(tmp_path: Path) -> None:
    path = tmp_path / "settings.toml"
    path.write_text(
        'BROKER_CACHE_MB = 11\nWEFT_CACHE_MB = 22\nWEFT_CUSTOM = "kept"\nCACHE_MB = 99\nversion = 1\nbackend = "postgres"\ntarget = "postgresql://localhost/app"\n'
    )
    config = resolve_config("WEFT", toml=path)
    assert config["CACHE_MB"] == 22
    assert config["CUSTOM"] == "kept"
    assert "version" not in config
    from simplebroker._project_config import load_project_config

    assert load_project_config(path)["backend"] == "postgres"


@pytest.mark.parametrize("source", ["env", "toml"])
def test_parse_never_fails_on_name_and_custom_is_retained(source: str) -> None:
    raw = {
        "BROKER_CUSTOM": "kept",
        "BROKER_": "ignored",
        "BROKER_bad_custom": "ignored",
        "CACHE_MB": "ignored",
        "WEFT_CACHE_MB": "ignored",
        "BROKER_1BAD": "ignored",
    }
    with warnings.catch_warnings(record=True) as seen:
        kwargs: dict[str, Any] = {source: raw}
        config = resolve_config(**kwargs)
    assert not seen
    assert config["CUSTOM"] == "kept"
    assert config["CACHE_MB"] == 10
    assert set(config) == set(DEFAULT_CONFIG) | {"CUSTOM"}


def test_only_canonical_lookup_and_declaration() -> None:
    config = resolve_config()
    for key in ("BROKER_CACHE_MB", "WEFT_CACHE_MB", "cache_mb"):
        with pytest.raises(KeyError):
            _ = config[key]
    with pytest.raises(ValueError):
        resolve_config(defaults={"lower": ConfigField(1, "value", int)})


def test_warning_multiplicity_source_and_redaction(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "settings.toml"
    path.write_text('APP_cache_mb = 99\nAPP_backend_password = "secret-file"\n')
    with pytest.warns(UserWarning) as seen:
        config = resolve_config("APP", toml=path, env={"APP_cache_mb": "98"})
    assert len(seen) == 3
    texts = [str(w.message) for w in seen]
    assert sum("APP_cache_mb" in t for t in texts) == 2
    assert any(str(path) in t for t in texts)
    assert any("environment" in t for t in texts)
    assert all("secret-file" not in t for t in texts)
    assert config["CACHE_MB"] == 10
    assert capsys.readouterr().out == ""


def test_repeated_warning_uses_default_filter() -> None:
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("default")
        for _ in range(3):
            resolve_config(env={"BROKER_cache_mb": "99"})
    assert len(seen) == 1


def test_combined_constraint_checks_final_values() -> None:
    config = resolve_config(
        env={
            "BROKER_PROJECT_CONFIG_PATH": "dir",
            "BROKER_PROJECT_CONFIG_NAME": "sub/file.toml",
        },
        override={"BROKER_PROJECT_CONFIG_NAME": ".broker.toml"},
    )
    assert config["PROJECT_CONFIG_NAME"] == ".broker.toml"


@pytest.mark.parametrize("reverse", [False, True])
def test_combined_source_applies_as_a_whole(reverse: bool) -> None:
    fields = dict(DEFAULT_CONFIG)
    fields["PROJECT_CONFIG_NAME"] = replace(
        fields["PROJECT_CONFIG_NAME"], default="sub/file.toml"
    )
    pair = [("PROJECT_CONFIG_PATH", "dir"), ("PROJECT_CONFIG_NAME", "file.toml")]
    if reverse:
        pair.reverse()
    assert (
        resolve_config(
            defaults=fields, override={"BROKER_" + key: value for key, value in pair}
        )["PROJECT_CONFIG_PATH"]
        == "dir"
    )


def test_application_only_and_partial_broker_defaults() -> None:
    fields = {"RETENTION_DAYS": ConfigField(7, "days", days)}
    config = resolve_config(
        "APP",
        defaults=fields,
        toml={"APP_RETENTION_DAYS": 8},
        env={"APP_RETENTION_DAYS": "9"},
        override={"APP_RETENTION_DAYS": 11},
    )
    assert dict(config) == {"RETENTION_DAYS": 11}
    for key in ("PROJECT_CONFIG_PATH", "PROJECT_CONFIG_NAME", "DEFAULT_DB_LOCATION"):
        assert set(resolve_config(defaults={key: DEFAULT_CONFIG[key]})) == {key}


def test_sources_unchanged_and_frozen(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_CACHE_MB", "bad")
    fields = app_defaults()
    env = {"APP_CACHE_MB": "12"}
    override = {"APP_RETENTION_DAYS": 9}
    before = (dict(fields), dict(env), dict(override), dict(os.environ))
    config = resolve_config("APP", defaults=fields, env=env, override=override)
    assert before == (fields, env, override, dict(os.environ))
    assert resolve_config()["CACHE_MB"] == 10
    with pytest.raises(TypeError):
        DEFAULT_CONFIG["CACHE_MB"] = fields["CACHE_MB"]  # type: ignore[index]
    with pytest.raises(TypeError):
        config["CACHE_MB"] = 1  # type: ignore[index]
    with pytest.raises(TypeError):
        del config["CACHE_MB"]  # type: ignore[attr-defined]
    with pytest.raises((TypeError, AttributeError)):
        config.update({"CACHE_MB": 1})  # type: ignore[attr-defined]
    assert resolve_config(defaults=fields)["RETENTION_DAYS"] == 7


def test_file_errors(tmp_path: Path) -> None:
    with pytest.raises(InvalidConfigError) as error:
        resolve_config(toml=tmp_path / "absent.toml")
    assert error.value.source == "file"
    path = tmp_path / "bad.toml"
    path.write_text("BROKER_CACHE_MB = [")
    with pytest.raises(ValueError):
        resolve_config(toml=path)


def test_one_public_configuration_surface() -> None:
    import simplebroker
    import simplebroker._constants as constants

    assert not (Path(simplebroker.__file__).parent / "config.py").exists()
    assert isinstance(resolve_config(), Config)
    assert {"serialize_config", "deserialize_config"} <= set(simplebroker.__all__)
    assert set(ConfigField.__dataclass_fields__) == {
        "default",
        "description",
        "validator",
        "sensitive",
    }
    for name in (
        "build_config",
        "load_config",
        "snapshot_config",
        "resolve_isolated_config",
        "ConfigSnapshot",
        "ResolvedConfig",
        "ConfigSchema",
        "CONFIG_DEFAULTS",
        "canonical_config",
        "legacy_config",
        "_CONFIG_FIELDS",
        "integer",
        "text",
        "number",
        "db_location_path",
        "db_name_path",
        "project_config_path",
        "project_config_name",
        "load_max_future_skew",
        "project_scope",
        "debug_flag",
        "strict_one_bool",
        "percent",
        "sync_mode",
    ):
        assert not hasattr(constants, name)
        assert not hasattr(simplebroker, name)


def test_mutable_defaults_are_copied_per_build() -> None:
    original = ["base"]
    fields = {"LABELS": ConfigField(original, "labels")}
    first = resolve_config(defaults=fields)
    second = resolve_config(defaults=fields)
    first["LABELS"].append("changed")
    assert second["LABELS"] == ["base"]
    assert original == ["base"]


def test_mappingproxy_default_is_accepted() -> None:
    from types import MappingProxyType

    original = MappingProxyType({"label": "base"})
    config = resolve_config(defaults={"METADATA": ConfigField(original, "metadata")})
    assert config["METADATA"] == original


def test_invalid_default_source_and_sensitive_error() -> None:
    def reject(value: Any) -> Any:
        raise ValueError("must not echo private value")

    with pytest.raises(InvalidConfigError) as error:
        resolve_config(
            "APP",
            defaults={
                "SECRET": ConfigField("private", "secret", reject, sensitive=True)
            },
        )
    assert error.value.source == "default"
    assert error.value.key == "APP_SECRET"
    assert error.value.value_display == "<redacted>"
    assert "private" not in str(error.value)


def test_builtin_default_names_are_uppercase_identifiers() -> None:
    assert all(key.isidentifier() and key.isupper() for key in DEFAULT_CONFIG)


@pytest.mark.parametrize("source", ["override"])
@pytest.mark.parametrize(
    "key",
    [
        "cache_mb",
        "Cache_MB",
        "",
        "CACHE-MB",
        "CACHE MB",
        "1CACHE",
        "_CACHE",
        "CACHÉ",
        1,
        None,
    ],
)
def test_internal_sources_reject_malformed_keys(source: str, key: Any) -> None:
    kwargs: dict[str, Any] = {source: {key: "private-value"}}
    with pytest.raises(ValueError, match=rf"{source} configuration key") as error:
        resolve_config(**kwargs)
    assert "private-value" not in str(error.value)


@pytest.mark.parametrize("source", ["override"])
def test_internal_sources_preserve_custom_names(source: str) -> None:
    kwargs: dict[str, Any] = {source: {"APP_CUSTOM_2": 9, "APP_APP_METADATA": "kept"}}
    config = resolve_config("APP", **kwargs)
    assert config["CUSTOM_2"] == 9
    assert config["APP_METADATA"] == "kept"


@pytest.mark.parametrize("source", ["defaults", "toml", "env", "override"])
@pytest.mark.parametrize("value", [True, False, "true", "false", "True", "False"])
def test_percentage_rejects_booleans(source: str, value: Any) -> None:
    if source == "defaults":
        kwargs: dict[str, Any] = {
            "defaults": {
                "VACUUM_THRESHOLD": replace(
                    DEFAULT_CONFIG["VACUUM_THRESHOLD"], default=value
                )
            }
        }
    else:
        key = "BROKER_VACUUM_THRESHOLD"
        kwargs = {source: {key: value}}
    with pytest.raises(InvalidConfigError) as error:
        resolve_config(**kwargs)
    assert error.value.key == "BROKER_VACUUM_THRESHOLD"
    assert error.value.source == {
        "defaults": "default",
        "toml": "file",
        "env": "environment",
    }.get(source, source)


@pytest.mark.parametrize(
    "last_key,first_key",
    [
        ("PROJECT_CONFIG_NAME", "PROJECT_CONFIG_PATH"),
        ("PROJECT_CONFIG_PATH", "PROJECT_CONFIG_NAME"),
    ],
)
def test_cross_field_error_names_source_of_final_name(
    last_key: str, first_key: str
) -> None:
    values = {"PROJECT_CONFIG_PATH": "dir", "PROJECT_CONFIG_NAME": "sub/file.toml"}
    with pytest.raises(InvalidConfigError) as error:
        resolve_config(
            env={"BROKER_" + first_key: values[first_key]},
            override={"BROKER_" + last_key: values[last_key]},
        )
    name_source = "override" if last_key == "PROJECT_CONFIG_NAME" else "environment"
    assert error.value.source == name_source
    assert "PROJECT_CONFIG_PATH and PROJECT_CONFIG_NAME" in error.value.expected


@pytest.mark.parametrize(
    "key", ["CACHE_MB", "BROKER_CACHE_MB", "WEFT_", "WEFT_cache_mb", "WEFT_1BAD"]
)
def test_override_rejects_other_namespaces_and_malformed_names(key: str) -> None:
    with pytest.raises(ValueError, match="override configuration key"):
        resolve_config("WEFT", override={key: 12})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        assert resolve_config("WEFT", env={key: "12"})["CACHE_MB"] == 10


@pytest.mark.parametrize(
    ("source", "value"),
    [("env", ["BROKER_CACHE_MB"]), ("override", {"BROKER_CACHE_MB", 7})],
)
def test_non_mapping_source_raises_type_error(source: str, value: Any) -> None:
    with pytest.raises(TypeError, match=f"{source} configuration must be a mapping"):
        resolve_config(**{source: value})


def test_config_argument_returns_the_supplied_config_unread(tmp_path: Path) -> None:
    config = resolve_config(override={"BROKER_CACHE_MB": 42})
    assert resolve_config(config=config) is config
    assert resolve_config(config=config, override={}) is config
    unread = resolve_config(
        config=config,
        env={"BROKER_BUSY_TIMEOUT": "not-an-integer"},
        toml=tmp_path / "missing.toml",
    )
    assert unread is config
    assert resolve_config(config=None)["CACHE_MB"] == 10


def test_config_argument_with_override_derives_inheriting_namespace() -> None:
    config = resolve_config(
        "WEFT", defaults=app_defaults(), override={"WEFT_RETENTION_DAYS": 9}
    )
    derived = resolve_config(config=config, override={"WEFT_RETENTION_DAYS": "10"})
    assert derived is not config
    assert derived.prefix == "WEFT"
    assert derived["RETENTION_DAYS"] == 10
    assert config["RETENTION_DAYS"] == 9
    with pytest.raises(InvalidConfigError):
        resolve_config(config=derived, override={"WEFT_RETENTION_DAYS": -1})
    with pytest.raises(ValueError, match="override configuration key"):
        resolve_config(config=config, override={"BROKER_RETENTION_DAYS": 11})


@pytest.mark.parametrize("value", [{"CACHE_MB": 5}, {"BROKER_CACHE_MB": 5}, "BROKER"])
def test_config_argument_requires_a_config(value: Any) -> None:
    with pytest.raises(TypeError, match="config must be a Config"):
        resolve_config(config=value)


def test_config_argument_rejects_contradicting_inputs() -> None:
    config = resolve_config("WEFT", defaults=app_defaults())
    assert resolve_config("WEFT", config=config) is config
    assert resolve_config(config=config, defaults=app_defaults()) is config
    with pytest.raises(TypeError, match="base"):
        resolve_config(**cast(Any, {"base": config}))
    with pytest.raises(ValueError, match="prefix"):
        resolve_config("TAUT", config=config)
    with pytest.raises(ValueError, match="defaults"):
        resolve_config(config=config, defaults=DEFAULT_CONFIG)
    with pytest.raises(TypeError, match="override configuration must be a mapping"):
        resolve_config(config=config, override=cast(Any, {"WEFT_CACHE_MB", 5}))


def test_invalid_values_warn_as_applied_and_raise_only_when_final() -> None:
    env = {
        "BROKER_BUSY_TIMEOUT": "not-an-integer",
        "BROKER_DEFAULT_DB_LOCATION": "relative",
    }
    with pytest.warns(
        UserWarning, match="ignoring invalid .* from the environment"
    ) as record:
        config = resolve_config(
            env=env,
            override={"BROKER_BUSY_TIMEOUT": 5, "BROKER_DEFAULT_DB_LOCATION": ""},
        )
    assert len(record) == 2
    assert config["BUSY_TIMEOUT"] == 5

    with pytest.warns(UserWarning), pytest.raises(InvalidConfigError) as error:
        resolve_config(env={"BROKER_DEFAULT_DB_LOCATION": "relative"})
    assert error.value.source == "environment"
    assert error.value.key == "BROKER_DEFAULT_DB_LOCATION"


@pytest.mark.parametrize("literal", ["inf", "-inf", "nan", '"invalid"'])
@pytest.mark.parametrize("overridden", [False, True])
def test_numeric_coercion_failure_uses_warning_and_final_value_policy(
    tmp_path: Path, literal: str, overridden: bool
) -> None:
    path = tmp_path / "numeric.toml"
    path.write_text(f"BROKER_CACHE_MB = {literal}\n")
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        if overridden:
            config = resolve_config(toml=path, override={"BROKER_CACHE_MB": 32})
            assert config["CACHE_MB"] == 32
        else:
            with pytest.raises(InvalidConfigError) as caught:
                resolve_config(toml=path)
            assert caught.value.key == "BROKER_CACHE_MB"
            assert caught.value.source == "file"
        assert len(seen) == 1
        assert "BROKER_CACHE_MB" in str(seen[0].message)
        assert "the TOML file" in str(seen[0].message)


def test_override_numeric_overflow_retains_typed_error() -> None:
    with (
        pytest.warns(UserWarning, match="overrides"),
        pytest.raises(InvalidConfigError) as caught,
    ):
        resolve_config(override={"BROKER_CACHE_MB": float("inf")})
    assert caught.value.source == "override"
    assert caught.value.key == "BROKER_CACHE_MB"


def test_sensitive_validator_overflow_keeps_safe_metadata() -> None:
    secret = "numeric-test-secret"

    def overflow(value: Any) -> int:
        if isinstance(value, int):
            return value
        raise OverflowError(f"untrusted parse detail: {value}")

    defaults = {"TOKEN": ConfigField(1, "numeric token", overflow, sensitive=True)}
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        with pytest.raises(InvalidConfigError) as caught:
            resolve_config(defaults=defaults, override={"BROKER_TOKEN": secret})
    assert secret not in str(caught.value)
    assert secret not in caught.value.value_display
    assert len(seen) == 1
    assert secret not in str(seen[0].message)


def test_custom_runtime_error_is_not_invalid_configuration() -> None:
    failure = RuntimeError("application validator failed")

    def fail(value: Any) -> int:
        raise failure

    with pytest.raises(RuntimeError) as caught:
        resolve_config(
            defaults={"CUSTOM": ConfigField(1, "custom", fail)},
            override={"BROKER_CUSTOM": 2},
        )
    assert caught.value is failure
