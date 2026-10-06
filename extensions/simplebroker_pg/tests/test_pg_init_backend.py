"""Tests for Postgres verify_env()/init_backend() env var resolution."""

from __future__ import annotations

from typing import Never

import psycopg
import pytest
from psycopg import conninfo as pg_conninfo
from simplebroker_pg.plugin import PostgresBackendPlugin, verify_env
from simplebroker_pg.validation import connect

from simplebroker import resolve_config
from simplebroker._exceptions import DatabaseError, OperationalError
from simplebroker.ext import BACKEND_API_VERSION

pytestmark = [pytest.mark.pg_only]


def test_backend_plugin_declares_backend_api_version() -> None:
    plugin = PostgresBackendPlugin()

    assert plugin.backend_api_version == BACKEND_API_VERSION


def test_init_backend_constructs_dsn_from_individual_vars() -> None:
    plugin = PostgresBackendPlugin()
    config = resolve_config(
        override={
            "BROKER_BACKEND_HOST": "db.example.com",
            "BROKER_BACKEND_PORT": 5433,
            "BROKER_BACKEND_USER": "myuser",
            "BROKER_BACKEND_PASSWORD": "secret",
            "BROKER_BACKEND_DATABASE": "mydb",
            "BROKER_BACKEND_SCHEMA": "app_v1",
            "BROKER_BACKEND_TARGET": "",
        }
    )

    result = plugin.init_backend(config)

    assert result["target"] == "postgresql://myuser:secret@db.example.com:5433/mydb"
    assert result["backend_options"] == {"schema": "app_v1"}


def test_init_backend_percent_encodes_reserved_characters() -> None:
    plugin = PostgresBackendPlugin()
    config = resolve_config(
        override={
            "BROKER_BACKEND_HOST": "db.example.com",
            "BROKER_BACKEND_PORT": 5432,
            "BROKER_BACKEND_USER": "user:name",
            "BROKER_BACKEND_PASSWORD": "p@ss/w:rd",
            "BROKER_BACKEND_DATABASE": "db/name",
            "BROKER_BACKEND_SCHEMA": "app_v1",
            "BROKER_BACKEND_TARGET": "",
        }
    )

    result = plugin.init_backend(config)

    assert result["target"] == (
        "postgresql://user%3Aname:p%40ss%2Fw%3Ard@db.example.com:5432/db%2Fname"
    )


def test_init_backend_omits_password_when_empty() -> None:
    plugin = PostgresBackendPlugin()
    config = resolve_config(
        override={
            "BROKER_BACKEND_HOST": "localhost",
            "BROKER_BACKEND_PORT": 5432,
            "BROKER_BACKEND_USER": "postgres",
            "BROKER_BACKEND_PASSWORD": "",
            "BROKER_BACKEND_DATABASE": "simplebroker",
            "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
            "BROKER_BACKEND_TARGET": "",
        }
    )

    result = plugin.init_backend(config)

    assert result["target"] == "postgresql://postgres@localhost:5432/simplebroker"


def test_init_backend_merges_password_into_existing_target() -> None:
    plugin = PostgresBackendPlugin()
    config = resolve_config(
        override={
            "BROKER_BACKEND_TARGET": "postgresql://myuser@db.example.com:5432/mydb",
            "BROKER_BACKEND_PASSWORD": "secret",
            "BROKER_BACKEND_SCHEMA": "app_v1",
        }
    )

    result = plugin.init_backend(config)
    parsed = pg_conninfo.conninfo_to_dict(result["target"])

    assert parsed["user"] == "myuser"
    assert parsed["password"] == "secret"
    assert parsed["host"] == "db.example.com"
    assert parsed["dbname"] == "mydb"
    assert result["backend_options"] == {"schema": "app_v1"}


def test_verify_env_rejects_invalid_schema() -> None:
    with pytest.raises(DatabaseError, match="schema"):
        verify_env(
            resolve_config(
                override={
                    "BROKER_BACKEND_TARGET": "postgresql://x@y/z",
                    "BROKER_BACKEND_SCHEMA": "not-valid!",
                }
            )
        )


def test_verify_env_rejects_invalid_port() -> None:
    with pytest.raises(DatabaseError, match="BROKER_BACKEND_PORT"):
        verify_env(
            resolve_config(
                override={
                    "BROKER_BACKEND_HOST": "db.example.com",
                    "BROKER_BACKEND_PORT": 70000,
                    "BROKER_BACKEND_USER": "postgres",
                    "BROKER_BACKEND_PASSWORD": "",
                    "BROKER_BACKEND_DATABASE": "simplebroker",
                    "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
                    "BROKER_BACKEND_TARGET": "",
                }
            )
        )


def test_init_backend_target_overrides_individual_vars() -> None:
    plugin = PostgresBackendPlugin()
    config = resolve_config(
        override={
            "BROKER_BACKEND_HOST": "ignored",
            "BROKER_BACKEND_PORT": 9999,
            "BROKER_BACKEND_USER": "ignored",
            "BROKER_BACKEND_PASSWORD": "",
            "BROKER_BACKEND_DATABASE": "ignored",
            "BROKER_BACKEND_SCHEMA": "my_schema",
            "BROKER_BACKEND_TARGET": "postgresql://real@realhost:5432/realdb",
        }
    )

    result = plugin.init_backend(config)

    assert result["target"] == "postgresql://real@realhost:5432/realdb"
    assert result["backend_options"] == {"schema": "my_schema"}


def test_init_backend_uses_defaults() -> None:
    plugin = PostgresBackendPlugin()
    config = resolve_config(
        override={
            "BROKER_BACKEND_TARGET": "",
            "BROKER_BACKEND_HOST": "localhost",
            "BROKER_BACKEND_PORT": 5432,
            "BROKER_BACKEND_USER": "postgres",
            "BROKER_BACKEND_PASSWORD": "",
            "BROKER_BACKEND_DATABASE": "simplebroker",
            "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
        }
    )

    result = plugin.init_backend(config)

    assert result["target"] == "postgresql://postgres@localhost:5432/simplebroker"
    assert result["backend_options"] == {"schema": "simplebroker_pg_v1"}


def test_init_backend_toml_target_used_as_fallback() -> None:
    plugin = PostgresBackendPlugin()
    config = resolve_config(
        override={
            "BROKER_BACKEND_TARGET": "",
            "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
        }
    )

    result = plugin.init_backend(
        config,
        toml_target="postgresql://toml@tomlhost/tomldb",
    )

    assert result["target"] == "postgresql://toml@tomlhost/tomldb"


def test_init_backend_toml_target_overrides_env_target() -> None:
    plugin = PostgresBackendPlugin()
    config = resolve_config(
        override={
            "BROKER_BACKEND_TARGET": "postgresql://env@envhost/envdb",
            "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
        }
    )

    result = plugin.init_backend(
        config,
        toml_target="postgresql://toml@tomlhost/tomldb",
    )

    assert result["target"] == "postgresql://toml@tomlhost/tomldb"


def test_init_backend_individual_env_parts_do_not_rewrite_toml_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_BACKEND_HOST", "envhost")
    plugin = PostgresBackendPlugin()
    config = resolve_config(
        override={
            "BROKER_BACKEND_TARGET": "",
            "BROKER_BACKEND_HOST": "envhost",
            "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
        }
    )

    result = plugin.init_backend(
        config,
        toml_target="postgresql://toml@tomlhost/tomldb",
    )

    assert result["target"] == "postgresql://toml@tomlhost/tomldb"


def test_init_backend_toml_schema_preserved_when_env_not_set() -> None:
    plugin = PostgresBackendPlugin()
    config = resolve_config(
        override={
            "BROKER_BACKEND_TARGET": "postgresql://x@y/z",
            "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
        }
    )

    result = plugin.init_backend(
        config,
        toml_options={"schema": "from_toml"},
    )

    assert result["backend_options"]["schema"] == "from_toml"


def test_init_backend_toml_schema_overrides_env() -> None:
    plugin = PostgresBackendPlugin()
    config = resolve_config(
        override={
            "BROKER_BACKEND_TARGET": "postgresql://x@y/z",
            "BROKER_BACKEND_SCHEMA": "from_env",
        }
    )

    result = plugin.init_backend(
        config,
        toml_options={"schema": "from_toml"},
    )

    assert result["backend_options"]["schema"] == "from_toml"


def test_init_backend_toml_target_uses_default_schema_when_toml_schema_missing() -> (
    None
):
    plugin = PostgresBackendPlugin()
    config = resolve_config(
        override={
            "BROKER_BACKEND_TARGET": "postgresql://x@y/z",
            "BROKER_BACKEND_SCHEMA": "from_env",
        }
    )

    result = plugin.init_backend(
        config,
        toml_target="postgresql://toml@tomlhost/tomldb",
    )

    assert result["target"] == "postgresql://toml@tomlhost/tomldb"
    assert result["backend_options"]["schema"] == "simplebroker_pg_v1"


@pytest.mark.parametrize(
    ("driver_error", "expected_type", "capacity"),
    [
        (
            psycopg.errors.TooManyConnections("too many connections for role"),
            OperationalError,
            True,
        ),
        (psycopg.OperationalError("connection refused"), OperationalError, False),
        (
            psycopg.errors.InvalidPassword("password authentication failed"),
            OperationalError,
            False,
        ),
        (psycopg.Error("invalid connection request"), DatabaseError, False),
    ],
    ids=["capacity", "unstructured-operational", "authentication", "non-operational"],
)
def test_connect_preserves_driver_error_classification_and_cause(
    monkeypatch: pytest.MonkeyPatch,
    driver_error: psycopg.Error,
    expected_type: type[DatabaseError],
    capacity: bool,
) -> None:
    """Acquisition failures must stay distinct from invalid schema evidence."""

    def fail_connect(*args: object, **kwargs: object) -> Never:
        raise driver_error

    monkeypatch.setattr("simplebroker_pg.validation.psycopg.connect", fail_connect)

    with pytest.raises(
        DatabaseError,
        match="Could not connect to Postgres target:",
    ) as exc_info:
        connect("postgresql://postgres@localhost/simplebroker")

    assert type(exc_info.value) is expected_type
    assert exc_info.value.__cause__ is driver_error
    assert str(driver_error) in str(exc_info.value)
    if isinstance(exc_info.value, OperationalError):
        # Authentication is operational too, but is not guaranteed transient.
        assert exc_info.value.retryable is not True
        assert exc_info.value._connection_capacity is capacity


_CAPACITY_PREFIX = (
    'connection failed: connection to server at "127.0.0.1", port 5432 failed: FATAL:  '
)
_ROLE_CAPACITY = 'too many connections for role "worker"'
_NETWORK_FAILURE = (
    'connection failed: connection to server at "::1", port 5432 failed: '
    "Connection refused"
)
_ALL_FAILURES = "\nMultiple connection attempts failed. All failures were:\n"


@pytest.mark.parametrize(
    ("driver_error", "capacity"),
    [
        (psycopg.OperationalError(_CAPACITY_PREFIX + _ROLE_CAPACITY), True),
        (
            psycopg.OperationalError(
                _CAPACITY_PREFIX + 'too many connections for database "broker"'
            ),
            True,
        ),
        (
            psycopg.OperationalError(
                _CAPACITY_PREFIX + "sorry, too many clients already"
            ),
            True,
        ),
        (
            psycopg.OperationalError(
                _CAPACITY_PREFIX
                + "remaining connection slots are reserved for non-replication superuser connections"
            ),
            True,
        ),
        (
            psycopg.OperationalError(
                _CAPACITY_PREFIX
                + "remaining connection slots are reserved for roles with the SUPERUSER attribute"
            ),
            True,
        ),
        (
            psycopg.OperationalError(
                _CAPACITY_PREFIX
                + 'remaining connection slots are reserved for roles with privileges of the "pg_use_reserved_connections" role'
            ),
            True,
        ),
        (
            psycopg.OperationalError(
                'connection failed: connection to server on socket "/tmp/.s.PGSQL.5432" failed: FATAL:  '
                + _ROLE_CAPACITY
            ),
            True,
        ),
        (
            psycopg.OperationalError(
                _CAPACITY_PREFIX
                + _ROLE_CAPACITY
                + _ALL_FAILURES
                + "- host: 'localhost', port: '5432', hostaddr: '::1': "
                + _CAPACITY_PREFIX
                + _ROLE_CAPACITY
                + "\n"
                + "- host: 'localhost', port: '5432', hostaddr: '127.0.0.1': "
                + _CAPACITY_PREFIX
                + _ROLE_CAPACITY
            ),
            True,
        ),
        (
            psycopg.OperationalError(
                _CAPACITY_PREFIX
                + _ROLE_CAPACITY
                + _ALL_FAILURES
                + "- host: 'localhost', port: '5432', hostaddr: '::1': "
                + _NETWORK_FAILURE
                + "\n"
                + "- host: 'localhost', port: '5432', hostaddr: '127.0.0.1': "
                + _CAPACITY_PREFIX
                + _ROLE_CAPACITY
            ),
            False,
        ),
        (
            psycopg.errors.TooManyConnections(_NETWORK_FAILURE + _ALL_FAILURES),
            True,
        ),
        (
            psycopg.errors.InvalidPassword(_CAPACITY_PREFIX + _ROLE_CAPACITY),
            False,
        ),
        (
            psycopg.OperationalError(
                'connection failed: connection to server at "localhost" (::1), port 5432 failed: FATAL:  '
                + _ROLE_CAPACITY
            ),
            True,
        ),
        (
            psycopg.OperationalError(
                'connection failed: connection to server at "localhost" (127.0.0.1), port 5432 failed: FATAL:  '
                + _ROLE_CAPACITY
                + _ALL_FAILURES
                + "- host: 'localhost', port: '5432', hostaddr: '::1': "
                + 'connection failed: connection to server at "localhost" (::1), port 5432 failed: FATAL:  '
                + _ROLE_CAPACITY
                + "\n"
                + "- host: 'localhost', port: '5432', hostaddr: '127.0.0.1': "
                + 'connection failed: connection to server at "localhost" (127.0.0.1), port 5432 failed: FATAL:  '
                + _ROLE_CAPACITY
            ),
            True,
        ),
        (
            psycopg.OperationalError(
                'connection failed: connection to server at "localhost" (::1), port 5432 failed: FATAL:  '
                + _ROLE_CAPACITY
                + _ALL_FAILURES
                + "- host: 'localhost', port: '5432', hostaddr: '::1': "
                + _NETWORK_FAILURE
            ),
            False,
        ),
        (psycopg.OperationalError("too many connections for role"), False),
        (psycopg.OperationalError(_NETWORK_FAILURE), False),
        (
            psycopg.OperationalError(
                _CAPACITY_PREFIX + 'password authentication failed for user "worker"'
            ),
            False,
        ),
        (
            psycopg.OperationalError(
                _CAPACITY_PREFIX
                + 'database "sorry, too many clients already" does not exist'
            ),
            False,
        ),
        (
            psycopg.OperationalError(
                'connection failed: connection to server at "sorry, too many clients already", '
                "port 5432 failed: FATAL:  permission denied"
            ),
            False,
        ),
        (
            psycopg.OperationalError(
                _CAPACITY_PREFIX + "unrecognized capacity refusal"
            ),
            False,
        ),
        (psycopg.OperationalError(_CAPACITY_PREFIX + "zu viele Verbindungen"), False),
        (
            psycopg.OperationalError(
                _CAPACITY_PREFIX
                + "remaining connection slots are reserved for unknown users"
            ),
            False,
        ),
        (
            psycopg.OperationalError(
                _CAPACITY_PREFIX + _ROLE_CAPACITY + "\nunknown failure"
            ),
            False,
        ),
    ],
)
def test_connect_marks_only_confirmed_capacity_refusals(
    monkeypatch: pytest.MonkeyPatch,
    driver_error: psycopg.OperationalError,
    capacity: bool,
) -> None:
    """Capacity patience must not extend authentication or ambiguous startup failures."""

    def fail_connect(*args: object, **kwargs: object) -> Never:
        raise driver_error

    monkeypatch.setattr("simplebroker_pg.validation.psycopg.connect", fail_connect)
    with pytest.raises(OperationalError) as failure:
        connect("postgresql://postgres@localhost/simplebroker")
    assert failure.value._connection_capacity is capacity
    assert failure.value.__cause__ is driver_error
    assert failure.value.retryable is not True
