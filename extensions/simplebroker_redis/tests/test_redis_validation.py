"""Redis namespace validation tests."""

from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from simplebroker_redis import validation as redis_validation
from simplebroker_redis.plugin import RedisBackendPlugin
from simplebroker_redis.validation import (
    NamespaceInspection,
    NamespaceState,
    inspect_namespace,
    is_namespace_key,
    key_prefix,
    require_namespace,
    validate_target,
)

from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker._exceptions import DatabaseError
from simplebroker._targets import BrokerTarget
from simplebroker.db import _initialize_project_backend_target
from simplebroker.ext import BACKEND_API_VERSION

pytestmark = [pytest.mark.redis_only]


def test_backend_plugin_declares_backend_api_version() -> None:
    plugin = RedisBackendPlugin()

    assert plugin.backend_api_version == BACKEND_API_VERSION


def test_two_project_initializers_share_the_config_phase_lock(
    redis_url: str,
    tmp_path: Path,
) -> None:
    plugin = RedisBackendPlugin()
    namespace = f"phase_{uuid.uuid4().hex}"
    config_path = tmp_path / ".broker.toml"
    config_path.write_text("version = 1\n", encoding="utf-8")
    target = BrokerTarget(
        backend_name="redis",
        target=redis_url,
        backend_options={"namespace": namespace},
        project_root=tmp_path,
        config_path=config_path,
        used_project_scope=True,
    )
    ready = threading.Barrier(2)

    def initialize() -> None:
        ready.wait()
        _initialize_project_backend_target(target, config={})

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(initialize) for _ in range(2)]
            for future in futures:
                future.result()

        inspection = inspect_namespace(
            redis_url,
            backend_options={"namespace": namespace},
        )
        assert inspection.state is NamespaceState.OWNED
        assert inspection.current_shape_ready
    finally:
        plugin.cleanup_target(
            redis_url,
            backend_options={"namespace": namespace},
        )


@pytest.mark.parametrize("namespace", ["parent:child", "parent:", ":child"])
def test_require_namespace_rejects_colons(namespace: str) -> None:
    with pytest.raises(DatabaseError, match="letters, numbers, _, -, or \\."):
        require_namespace({"namespace": namespace})


def test_require_namespace_allows_non_delimiter_punctuation() -> None:
    assert (
        require_namespace({"namespace": "tenant_1.jobs-prod"}) == "tenant_1.jobs-prod"
    )


def test_require_namespace_rejects_values_over_key_schema_limit() -> None:
    with pytest.raises(DatabaseError, match="1-128 chars"):
        require_namespace({"namespace": "n" * 129})


def test_is_namespace_key_rejects_colon_extended_namespace_keys() -> None:
    prefix = key_prefix("parent")
    token = "0123456789abcdef0123456789abcdef"

    assert is_namespace_key(prefix, f"{prefix}:meta")
    assert is_namespace_key(prefix, f"{prefix}:q:jobs.pending:reserved")
    assert is_namespace_key(prefix, f"{prefix}:batches:{token}:meta")

    assert not is_namespace_key(prefix, f"{prefix}:child:meta")
    assert not is_namespace_key(prefix, f"{prefix}:q:q:jobs:reserved")
    assert not is_namespace_key(prefix, f"{prefix}:batches:batches:{token}:meta")


def test_is_namespace_key_validates_activity_queue_keys_and_prefix() -> None:
    prefix = key_prefix("tenant")

    assert not is_namespace_key(prefix, "simplebroker:other:meta")
    assert is_namespace_key(prefix, f"{prefix}:activity:q:jobs")
    assert not is_namespace_key(prefix, f"{prefix}:activity:q:bad queue")
    assert not is_namespace_key(prefix, f"{prefix}:unknown:key:shape")


def test_validate_target_allows_absent_namespace_for_init(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        redis_validation,
        "inspect_namespace",
        lambda *args, **kwargs: NamespaceInspection("tenant", NamespaceState.ABSENT, 0),
    )

    validate_target("redis://example/0", verify_initialized=False)


def test_inspect_namespace_keeps_an_older_owned_version_out_of_partial_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Client:
        def hgetall(self, key: str) -> dict[str, str]:
            return {
                "magic": SIMPLEBROKER_MAGIC,
                "schema_version": "0",
            }

        def scan_iter(self, pattern: str) -> list[str]:
            return ["simplebroker:tenant:meta"]

        def close(self) -> None:
            return None

    monkeypatch.setattr(redis_validation, "connect", lambda target: Client())

    inspection = inspect_namespace(
        "redis://example/0",
        backend_options={"namespace": "tenant"},
    )

    assert inspection.state is NamespaceState.OWNED
    assert inspection.schema_version == 0


@pytest.mark.parametrize(
    ("version", "current_shape_ready", "error_match"),
    [
        (0, False, "older than supported version 1; no migration"),
        (1, True, None),
        (1, False, "current metadata shape is incomplete"),
        (2, True, "newer than supported version 1"),
    ],
)
@pytest.mark.parametrize("verify_initialized", [False, True])
def test_owned_redis_version_and_readiness_transition_table(
    monkeypatch: pytest.MonkeyPatch,
    verify_initialized: bool,
    version: int,
    current_shape_ready: bool,
    error_match: str | None,
) -> None:
    monkeypatch.setattr(
        redis_validation,
        "inspect_namespace",
        lambda *args, **kwargs: NamespaceInspection(
            "tenant",
            NamespaceState.OWNED,
            1,
            schema_version=version,
            current_shape_ready=current_shape_ready,
        ),
    )

    if error_match is None:
        validate_target(
            "redis://example/0",
            verify_initialized=verify_initialized,
        )
    else:
        with pytest.raises(DatabaseError, match=error_match):
            validate_target(
                "redis://example/0",
                verify_initialized=verify_initialized,
            )


@pytest.mark.parametrize(
    ("state", "verify_initialized", "match"),
    [
        (NamespaceState.FOREIGN, False, "not available for SimpleBroker init"),
        (NamespaceState.ABSENT, True, "does not exist"),
        (NamespaceState.FOREIGN, True, "not SimpleBroker-managed"),
    ],
)
def test_validate_target_reports_namespace_ownership_errors(
    monkeypatch: pytest.MonkeyPatch,
    state: NamespaceState,
    verify_initialized: bool,
    match: str,
) -> None:
    monkeypatch.setattr(
        redis_validation,
        "inspect_namespace",
        lambda *args, **kwargs: NamespaceInspection("tenant", state, 1),
    )

    with pytest.raises(DatabaseError, match=match):
        validate_target(
            "redis://example/0",
            verify_initialized=verify_initialized,
        )


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
