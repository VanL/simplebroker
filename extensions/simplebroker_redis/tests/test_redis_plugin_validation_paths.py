"""Focused Redis plugin and validation branch coverage."""

from __future__ import annotations

import threading
from typing import Never

import pytest
import redis
from simplebroker_redis.plugin import RedisBackendPlugin
from simplebroker_redis.validation import (
    NamespaceState,
    connect,
    inspect_namespace,
    key_prefix,
    require_namespace,
    validate_target,
)

from simplebroker._exceptions import DatabaseError

pytestmark = [pytest.mark.redis_only]


@pytest.mark.parametrize(
    "options, match",
    [
        ({"namespace": 123}, "must be a string"),
        ({"namespace": ""}, "must be non-empty"),
        ({"namespace": "bad namespace"}, "may not contain whitespace"),
        ({"namespace": "bad*namespace"}, "may not contain whitespace"),
    ],
)
def test_require_namespace_rejects_invalid_redis_namespaces(
    options: dict[str, object], match: str
) -> None:
    with pytest.raises(DatabaseError, match=match):
        require_namespace(options)


def test_connect_wraps_redis_connection_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_redis_error(*args: object, **kwargs: object) -> Never:
        raise redis.RedisError("connection refused")

    monkeypatch.setattr(
        "simplebroker_redis.validation.redis.Redis.from_url",
        raise_redis_error,
    )

    with pytest.raises(
        DatabaseError,
        match="Could not connect to Valkey/Redis target: connection refused",
    ):
        connect("redis://127.0.0.1:1/0")


def test_inspect_namespace_classifies_absent_foreign_and_partial_states(
    redis_url: str,
    redis_namespace: str,
) -> None:
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    prefix = key_prefix(redis_namespace)
    queues_key = f"{prefix}:queues"
    meta_key = f"{prefix}:meta"
    try:
        absent = inspect_namespace(
            redis_url,
            backend_options={"namespace": redis_namespace},
        )
        assert absent.state is NamespaceState.ABSENT
        assert absent.key_count == 0

        client.sadd(queues_key, "legacy")
        foreign = inspect_namespace(
            redis_url,
            backend_options={"namespace": redis_namespace},
        )
        assert foreign.state is NamespaceState.FOREIGN
        assert foreign.key_count == 1
        with pytest.raises(DatabaseError, match="not available for SimpleBroker init"):
            validate_target(
                redis_url,
                backend_options={"namespace": redis_namespace},
                verify_initialized=False,
            )

        client.delete(queues_key)
        client.hset(meta_key, mapping={"magic": "wrong", "schema_version": "bad"})
        partial = inspect_namespace(
            redis_url,
            backend_options={"namespace": redis_namespace},
        )
        assert partial.state is NamespaceState.PARTIAL_SIMPLEBROKER
        assert partial.schema_version is None
        with pytest.raises(DatabaseError, match="not SimpleBroker-managed"):
            validate_target(
                redis_url,
                backend_options={"namespace": redis_namespace},
            )
    finally:
        client.delete(queues_key, meta_key)
        client.close()


def test_validate_target_reports_absent_redis_namespace(
    redis_url: str,
    redis_namespace: str,
) -> None:
    with pytest.raises(DatabaseError, match="does not exist"):
        validate_target(redis_url, backend_options={"namespace": redis_namespace})


def test_redis_init_backend_builds_target_from_parts() -> None:
    result = RedisBackendPlugin().init_backend(
        {
            "BROKER_BACKEND_HOST": "redis.example.test",
            "BROKER_BACKEND_PORT": "6380",
            "BROKER_BACKEND_PASSWORD": "p@ss word",
            "BROKER_BACKEND_DATABASE": "2",
            "BROKER_BACKEND_SCHEMA": "tenant_1",
            "BROKER_BACKEND_TARGET": "",
        },
        toml_options={"max_connections": "3", "pool_timeout": "0.5"},
    )

    assert result["target"] == "redis://:p%40ss%20word@redis.example.test:6380/2"
    assert result["backend_options"] == {
        "namespace": "tenant_1",
        "max_connections": "3",
        "pool_timeout": "0.5",
    }


@pytest.mark.parametrize(
    "database, match",
    [
        ("not-a-number", "Redis DB number"),
        ("-1", "non-negative"),
    ],
)
def test_redis_init_backend_rejects_invalid_database_numbers(
    database: str, match: str
) -> None:
    with pytest.raises(DatabaseError, match=match):
        RedisBackendPlugin().init_backend(
            {
                "BROKER_BACKEND_HOST": "127.0.0.1",
                "BROKER_BACKEND_PORT": "6379",
                "BROKER_BACKEND_DATABASE": database,
                "BROKER_BACKEND_SCHEMA": "tenant_1",
                "BROKER_BACKEND_TARGET": "",
            }
        )


def test_redis_backend_options_schema_and_namespace_must_match() -> None:
    with pytest.raises(DatabaseError, match="namespace and schema options must match"):
        RedisBackendPlugin().init_backend(
            {},
            toml_options={"namespace": "tenant_a", "schema": "tenant_b"},
        )


def test_redis_initialize_target_is_idempotent(
    redis_url: str,
    redis_namespace: str,
) -> None:
    plugin = RedisBackendPlugin()
    try:
        plugin.initialize_target(
            redis_url, backend_options={"namespace": redis_namespace}
        )
        plugin.initialize_target(
            redis_url, backend_options={"namespace": redis_namespace}
        )
    finally:
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_redis_cleanup_absent_namespace_returns_false(
    redis_url: str,
    redis_namespace: str,
) -> None:
    assert (
        RedisBackendPlugin().cleanup_target(
            redis_url,
            backend_options={"namespace": redis_namespace},
        )
        is False
    )


def test_redis_activity_waiter_requires_target_without_runner() -> None:
    plugin = RedisBackendPlugin()

    assert (
        plugin.create_activity_waiter(
            target=None,
            backend_options={"namespace": "tenant_1"},
            queue_name="jobs",
            stop_event=threading.Event(),
        )
        is None
    )
    assert (
        plugin.create_activity_waiter_for_queues(
            target=None,
            backend_options={"namespace": "tenant_1"},
            queue_names=("jobs",),
            stop_event=threading.Event(),
        )
        is None
    )
