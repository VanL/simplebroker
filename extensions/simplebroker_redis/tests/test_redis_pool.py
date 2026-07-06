"""Valkey/Redis command connection-pool integration tests."""

from __future__ import annotations

import os
import time

import pytest
import redis
from simplebroker_redis import RedisRunner, get_backend_plugin
from simplebroker_redis import plugin as redis_plugin_module
from simplebroker_redis.core import RedisBrokerCore

from simplebroker._exceptions import DatabaseError, OperationalError

pytestmark = [pytest.mark.redis_only]


def test_runner_uses_blocking_connection_pool(
    redis_url: str, redis_namespace: str
) -> None:
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    try:
        runner.client.ping()
        assert isinstance(runner._pool, redis.BlockingConnectionPool)
    finally:
        runner.shutdown()


def test_runner_reuses_one_pool_for_multiple_client_accesses(
    redis_url: str, redis_namespace: str
) -> None:
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    try:
        first_client = runner.client
        first_pool = runner._pool
        assert first_client is runner.client
        assert first_pool is runner._pool
    finally:
        runner.shutdown()


def test_pool_options_from_backend_options(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    runner = plugin.create_runner(
        redis_url,
        backend_options={
            "namespace": redis_namespace,
            "max_connections": 3,
            "pool_timeout": 0.25,
        },
    )
    try:
        assert runner.pool_options.max_connections == 3
        assert runner.pool_options.timeout == 0.25
        runner.client.ping()
        assert runner._pool is not None
        assert runner._pool.max_connections == 3
    finally:
        runner.shutdown()


@pytest.mark.parametrize(
    "backend_options",
    [
        {"max_connections": 0},
        {"max_connections": True},
        {"max_connections": "nope"},
        {"pool_timeout": 0},
        {"pool_timeout": -1},
        {"pool_timeout": "nope"},
        {"unexpected": "value"},
    ],
)
def test_invalid_pool_options_raise_database_error(
    redis_url: str, redis_namespace: str, backend_options: dict[str, object]
) -> None:
    plugin = get_backend_plugin()
    options = {"namespace": redis_namespace, **backend_options}
    with pytest.raises(DatabaseError):
        plugin.create_runner(redis_url, backend_options=options)


def test_schema_and_namespace_must_not_disagree(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    with pytest.raises(DatabaseError):
        plugin.create_runner(
            redis_url,
            backend_options={
                "namespace": redis_namespace,
                "schema": f"{redis_namespace}_other",
            },
        )


def test_pool_exhaustion_is_bounded(redis_url: str, redis_namespace: str) -> None:
    plugin = get_backend_plugin()
    runner = plugin.create_runner(
        redis_url,
        backend_options={
            "namespace": redis_namespace,
            "max_connections": 1,
            "pool_timeout": 0.1,
        },
    )
    core = RedisBrokerCore(runner)
    held_connection = None
    try:
        core.write("jobs", "before")
        assert runner._pool is not None
        held_connection = runner._pool.get_connection()
        started = time.monotonic()
        with pytest.raises(OperationalError):
            core.write("jobs", "blocked")
        assert time.monotonic() - started < 1.0
    finally:
        if held_connection is not None and runner._pool is not None:
            runner._pool.release(held_connection)
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_shutdown_disconnects_pool(redis_url: str, redis_namespace: str) -> None:
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    runner.client.ping()
    first_pool = runner._pool
    assert first_pool is not None

    runner.shutdown()

    assert runner._pool is None
    assert runner._client is None

    runner.client.ping()
    try:
        assert runner._pool is not None
        assert runner._pool is not first_pool
    finally:
        runner.shutdown()


def test_fork_check_recreates_pool(redis_url: str, redis_namespace: str) -> None:
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    runner.client.ping()
    first_pool = runner._pool
    assert first_pool is not None

    runner._pid = -1
    runner.client.ping()

    try:
        assert runner._pid == os.getpid()
        assert runner._pool is not None
        assert runner._pool is not first_pool
    finally:
        runner.shutdown()


def test_shared_activity_registry_is_pid_scoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeListener:
        def __init__(self, target: str, namespace: str) -> None:
            self._target = target
            self._namespace = namespace
            self.closed = False

        def has_registrations(self) -> bool:
            return False

        def close(self) -> None:
            self.closed = True

    current_pid = 100
    monkeypatch.setattr(
        redis_plugin_module, "_SharedRedisActivityListener", FakeListener
    )
    monkeypatch.setattr(redis_plugin_module.os, "getpid", lambda: current_pid)
    registry = redis_plugin_module._RedisActivityRegistry()

    parent_first = registry.listener("redis://example.test/0", "namespace")
    parent_second = registry.listener("redis://example.test/0", "namespace")
    assert parent_second is parent_first

    current_pid = 200
    child = registry.listener("redis://example.test/0", "namespace")
    assert child is not parent_first


def test_activity_waiter_does_not_consume_command_pool_slot(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    runner = plugin.create_runner(
        redis_url,
        backend_options={
            "namespace": redis_namespace,
            "max_connections": 1,
            "pool_timeout": 0.25,
        },
    )
    waiter = plugin.create_activity_waiter(
        target=None,
        runner=runner,
        queue_name="jobs",
        stop_event=None,
    )
    assert waiter is not None
    core = RedisBrokerCore(runner)
    try:
        core.write("jobs", "payload")
        assert waiter.wait(2.0) is True
        assert core.claim_one("jobs", with_timestamps=False) == "payload"
    finally:
        waiter.close()
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})
