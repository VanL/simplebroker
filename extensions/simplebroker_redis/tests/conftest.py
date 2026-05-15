"""Shared fixtures for Valkey/Redis extension tests."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest
from simplebroker_redis import RedisRunner, get_backend_plugin

TEST_URL = os.environ.get("SIMPLEBROKER_VALKEY_TEST_URL") or os.environ.get(
    "SIMPLEBROKER_REDIS_TEST_URL"
)


def unique_namespace(prefix: str = "sbtest") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def redis_url() -> str:
    if not TEST_URL:
        pytest.skip("Set SIMPLEBROKER_VALKEY_TEST_URL to run Redis extension tests")
    return TEST_URL


@pytest.fixture
def redis_namespace() -> str:
    return unique_namespace()


@pytest.fixture
def redis_runner(redis_url: str, redis_namespace: str) -> Iterator[RedisRunner]:
    plugin = get_backend_plugin()
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    plugin.initialize_target(redis_url, backend_options={"namespace": redis_namespace})
    try:
        yield runner
    finally:
        runner.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})
