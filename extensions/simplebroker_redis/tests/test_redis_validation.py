"""Redis namespace validation tests."""

from __future__ import annotations

import pytest
from simplebroker_redis.plugin import RedisBackendPlugin
from simplebroker_redis.validation import (
    is_namespace_key,
    key_prefix,
    require_namespace,
)

from simplebroker._exceptions import DatabaseError
from simplebroker.ext import BACKEND_API_VERSION


@pytest.mark.redis_only
def test_backend_plugin_declares_backend_api_version() -> None:
    plugin = RedisBackendPlugin()

    assert plugin.backend_api_version == BACKEND_API_VERSION


@pytest.mark.parametrize("namespace", ["parent:child", "parent:", ":child"])
def test_require_namespace_rejects_colons(namespace: str) -> None:
    with pytest.raises(DatabaseError, match="letters, numbers, _, -, or \\."):
        require_namespace({"namespace": namespace})


def test_require_namespace_allows_non_delimiter_punctuation() -> None:
    assert (
        require_namespace({"namespace": "tenant_1.jobs-prod"}) == "tenant_1.jobs-prod"
    )


def test_is_namespace_key_rejects_colon_extended_namespace_keys() -> None:
    prefix = key_prefix("parent")
    token = "0123456789abcdef0123456789abcdef"

    assert is_namespace_key(prefix, f"{prefix}:meta")
    assert is_namespace_key(prefix, f"{prefix}:q:jobs.pending:reserved")
    assert is_namespace_key(prefix, f"{prefix}:batches:{token}:meta")

    assert not is_namespace_key(prefix, f"{prefix}:child:meta")
    assert not is_namespace_key(prefix, f"{prefix}:q:q:jobs:reserved")
    assert not is_namespace_key(prefix, f"{prefix}:batches:batches:{token}:meta")


class _StaleApiPlugin(RedisBackendPlugin):
    """Simulates an extension built against an older backend API."""

    backend_api_version = BACKEND_API_VERSION - 1


@pytest.mark.parametrize("method", ["create_core", "create_core_from_runner"])
def test_stale_plugin_fails_handshake_before_any_connection(method: str) -> None:
    """A hand-instantiated stale plugin is rejected BEFORE Redis is touched.

    RedisBrokerCore opens its connection in __init__, so the backend API
    handshake must fire first. The target below points at a closed port: if
    construction were attempted, we would see a connection error instead of
    the handshake RuntimeError.
    """
    plugin = _StaleApiPlugin()

    with pytest.raises(RuntimeError, match="backend API"):
        if method == "create_core":
            plugin.create_core(
                "redis://127.0.0.1:1/0",
                backend_options={"namespace": "handshake_test"},
            )
        else:
            plugin.create_core_from_runner(runner=None)  # type: ignore[arg-type]
