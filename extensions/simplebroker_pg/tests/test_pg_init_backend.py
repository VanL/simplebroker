"""Tests for Postgres verify_env()/init_backend() env var resolution."""

from __future__ import annotations

from typing import Never

import psycopg
import pytest
from psycopg import conninfo as pg_conninfo
from simplebroker_pg.plugin import PostgresBackendPlugin, verify_env
from simplebroker_pg.validation import connect

from simplebroker._exceptions import DatabaseError

pytestmark = [pytest.mark.pg_only]


def test_init_backend_constructs_dsn_from_individual_vars() -> None:
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_HOST": "db.example.com",
        "BROKER_BACKEND_PORT": 5433,
        "BROKER_BACKEND_USER": "myuser",
        "BROKER_BACKEND_PASSWORD": "secret",
        "BROKER_BACKEND_DATABASE": "mydb",
        "BROKER_BACKEND_SCHEMA": "app_v1",
        "BROKER_BACKEND_TARGET": "",
    }

    result = plugin.init_backend(config)

    assert result["target"] == "postgresql://myuser:secret@db.example.com:5433/mydb"
    assert result["backend_options"] == {"schema": "app_v1"}


def test_init_backend_percent_encodes_reserved_characters() -> None:
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_HOST": "db.example.com",
        "BROKER_BACKEND_PORT": 5432,
        "BROKER_BACKEND_USER": "user:name",
        "BROKER_BACKEND_PASSWORD": "p@ss/w:rd",
        "BROKER_BACKEND_DATABASE": "db/name",
        "BROKER_BACKEND_SCHEMA": "app_v1",
        "BROKER_BACKEND_TARGET": "",
    }

    result = plugin.init_backend(config)

    assert result["target"] == (
        "postgresql://user%3Aname:p%40ss%2Fw%3Ard@db.example.com:5432/db%2Fname"
    )


def test_init_backend_omits_password_when_empty() -> None:
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_HOST": "localhost",
        "BROKER_BACKEND_PORT": 5432,
        "BROKER_BACKEND_USER": "postgres",
        "BROKER_BACKEND_PASSWORD": "",
        "BROKER_BACKEND_DATABASE": "simplebroker",
        "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
        "BROKER_BACKEND_TARGET": "",
    }

    result = plugin.init_backend(config)

    assert result["target"] == "postgresql://postgres@localhost:5432/simplebroker"


def test_init_backend_merges_password_into_existing_target() -> None:
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_TARGET": "postgresql://myuser@db.example.com:5432/mydb",
        "BROKER_BACKEND_PASSWORD": "secret",
        "BROKER_BACKEND_SCHEMA": "app_v1",
    }

    result = plugin.init_backend(config)
    parsed = pg_conninfo.conninfo_to_dict(result["target"])

    assert parsed["user"] == "myuser"
    assert parsed["password"] == "secret"
    assert parsed["host"] == "db.example.com"
    assert parsed["dbname"] == "mydb"
    assert result["backend_options"] == {"schema": "app_v1"}


def test_verify_env_rejects_invalid_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_BACKEND_SCHEMA", "not-valid!")

    with pytest.raises(DatabaseError, match="schema"):
        verify_env(
            {
                "BROKER_BACKEND_TARGET": "postgresql://x@y/z",
                "BROKER_BACKEND_SCHEMA": "not-valid!",
            }
        )


def test_verify_env_rejects_invalid_port() -> None:
    with pytest.raises(DatabaseError, match="BROKER_BACKEND_PORT"):
        verify_env(
            {
                "BROKER_BACKEND_HOST": "db.example.com",
                "BROKER_BACKEND_PORT": 70000,
                "BROKER_BACKEND_USER": "postgres",
                "BROKER_BACKEND_PASSWORD": "",
                "BROKER_BACKEND_DATABASE": "simplebroker",
                "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
                "BROKER_BACKEND_TARGET": "",
            }
        )


def test_init_backend_target_overrides_individual_vars() -> None:
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_HOST": "ignored",
        "BROKER_BACKEND_PORT": 9999,
        "BROKER_BACKEND_USER": "ignored",
        "BROKER_BACKEND_PASSWORD": "",
        "BROKER_BACKEND_DATABASE": "ignored",
        "BROKER_BACKEND_SCHEMA": "my_schema",
        "BROKER_BACKEND_TARGET": "postgresql://real@realhost:5432/realdb",
    }

    result = plugin.init_backend(config)

    assert result["target"] == "postgresql://real@realhost:5432/realdb"
    assert result["backend_options"] == {"schema": "my_schema"}


def test_init_backend_uses_defaults() -> None:
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_TARGET": "",
        "BROKER_BACKEND_HOST": "localhost",
        "BROKER_BACKEND_PORT": 5432,
        "BROKER_BACKEND_USER": "postgres",
        "BROKER_BACKEND_PASSWORD": "",
        "BROKER_BACKEND_DATABASE": "simplebroker",
        "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
    }

    result = plugin.init_backend(config)

    assert result["target"] == "postgresql://postgres@localhost:5432/simplebroker"
    assert result["backend_options"] == {"schema": "simplebroker_pg_v1"}


def test_init_backend_toml_target_used_as_fallback() -> None:
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_TARGET": "",
        "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
    }

    result = plugin.init_backend(
        config,
        toml_target="postgresql://toml@tomlhost/tomldb",
    )

    assert result["target"] == "postgresql://toml@tomlhost/tomldb"


def test_init_backend_env_target_overrides_toml_target() -> None:
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_TARGET": "postgresql://env@envhost/envdb",
        "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
    }

    result = plugin.init_backend(
        config,
        toml_target="postgresql://toml@tomlhost/tomldb",
    )

    assert result["target"] == "postgresql://env@envhost/envdb"


def test_init_backend_individual_env_parts_do_not_rewrite_toml_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_BACKEND_HOST", "envhost")
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_TARGET": "",
        "BROKER_BACKEND_HOST": "envhost",
        "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
    }

    result = plugin.init_backend(
        config,
        toml_target="postgresql://toml@tomlhost/tomldb",
    )

    assert result["target"] == "postgresql://toml@tomlhost/tomldb"


def test_init_backend_toml_schema_preserved_when_env_not_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("BROKER_BACKEND_SCHEMA", raising=False)
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_TARGET": "postgresql://x@y/z",
        "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
    }

    result = plugin.init_backend(
        config,
        toml_options={"schema": "from_toml"},
    )

    assert result["backend_options"]["schema"] == "from_toml"


def test_init_backend_env_schema_overrides_toml(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_BACKEND_SCHEMA", "from_env")
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_TARGET": "postgresql://x@y/z",
        "BROKER_BACKEND_SCHEMA": "from_env",
    }

    result = plugin.init_backend(
        config,
        toml_options={"schema": "from_toml"},
    )

    assert result["backend_options"]["schema"] == "from_env"


def test_connect_wraps_auth_errors_as_connection_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_auth_error(*args: object, **kwargs: object) -> Never:
        raise psycopg.OperationalError("password authentication failed")

    monkeypatch.setattr("simplebroker_pg.validation.psycopg.connect", raise_auth_error)

    with pytest.raises(
        DatabaseError,
        match="Could not connect to Postgres target: password authentication failed",
    ):
        connect("postgresql://postgres@localhost/simplebroker")
