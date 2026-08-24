"""Focused Redis plugin and activity-listener contracts."""

from __future__ import annotations

import os
import subprocess
import threading
from pathlib import Path
from typing import Any

import pytest
import simplebroker_redis.plugin as redis_plugin_module
from simplebroker_redis.plugin import RedisBackendPlugin
from simplebroker_redis.runner import RedisRunner
from simplebroker_redis.validation import NamespaceInspection, NamespaceState

from simplebroker import snapshot_config
from simplebroker._exceptions import DatabaseError, OperationalError

pytestmark = [pytest.mark.redis_only]
ROOT = Path(__file__).resolve().parents[3]


def test_public_context_manager_annotations_remain_override_compatible(
    tmp_path: Path,
) -> None:
    """Keep the Redis export compatible with downstream context subclasses."""
    probe = tmp_path / "redis_context_manager_compatibility.py"
    probe.write_text(
        """\
from typing import Any, Literal

from simplebroker_redis.core import RedisBrokerCore


class CustomRedisCore(RedisBrokerCore):
    def __enter__(self) -> RedisBrokerCore:
        return super().__enter__()

    def __exit__(
        self, exc_type: Any, exc_val: Any, exc_tb: str
    ) -> Literal[False]:
        return super().__exit__(exc_type, exc_val, exc_tb)
""",
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            "mypy",
            "--config-file",
            str(ROOT / "pyproject.toml"),
            "--no-incremental",
            str(probe),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def _listener() -> Any:
    listener = object.__new__(redis_plugin_module._SharedRedisActivityListener)
    listener._target = "redis://example/0"
    listener._namespace = "tenant"
    listener._prefix = "simplebroker:tenant"
    listener._stop_event = threading.Event()
    listener._lock = threading.RLock()
    listener._ready = threading.Event()
    listener._conditions = {}
    listener._versions = {}
    listener._refcounts = {}
    listener._wildcard_version = 0
    listener._error = None
    return listener


def test_database_and_namespace_defaults_keep_legacy_config_compatible() -> None:
    assert redis_plugin_module._database_number(None) == 0
    assert redis_plugin_module._database_number("simplebroker") == 0
    assert (
        redis_plugin_module._namespace_from_options(
            {},
            {"namespace": "tenant", "schema": "tenant"},
        )
        == "tenant"
    )


def test_plugin_runner_receipt_keeps_marker_out_of_redundant_config_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class RecordingRunner:
        def __init__(self, target: str, **kwargs: Any) -> None:
            captured["target"] = target
            captured.update(kwargs)

    monkeypatch.setattr(redis_plugin_module, "RedisRunner", RecordingRunner)
    config = snapshot_config({"BROKER_BUSY_TIMEOUT": 1250})

    RedisBackendPlugin().create_runner(
        "redis://example/0",
        backend_options={"namespace": "tenant"},
        config=config,
    )

    assert captured["pool_options"].timeout == 1.25
    assert "config" not in captured


def test_direct_runner_snapshots_environment_when_pool_options_are_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_BUSY_TIMEOUT", "1250")
    runner = RedisRunner("redis://example/0", namespace="tenant")
    monkeypatch.setenv("BROKER_BUSY_TIMEOUT", "9750")

    assert runner.pool_options.timeout == 1.25


def test_cleanup_reuses_one_snapshot_for_runner_and_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = snapshot_config({"EXTENSION_RECEIPT": "kept"})
    runner_config: list[object] = []
    core_config: list[object] = []

    monkeypatch.setattr(
        redis_plugin_module,
        "inspect_namespace",
        lambda *args, **kwargs: NamespaceInspection(
            "tenant",
            NamespaceState.OWNED,
            1,
        ),
    )

    class Client:
        def scan_iter(self, pattern: str) -> list[str]:
            return []

        def close(self) -> None:
            return None

    class Runner:
        stale_batch_seconds = 30

        def __init__(self, *args: object, **kwargs: object) -> None:
            runner_config.append(kwargs.get("config"))

        def close(self) -> None:
            return None

    class Core:
        def __init__(self, runner: object, *, config: object) -> None:
            core_config.append(config)

        def recover_stale_batches(self, *, max_age_seconds: int) -> None:
            return None

    monkeypatch.setattr(
        redis_plugin_module.redis.Redis,
        "from_url",
        lambda *args, **kwargs: Client(),
    )
    monkeypatch.setattr(redis_plugin_module, "RedisRunner", Runner)
    monkeypatch.setattr(redis_plugin_module, "RedisBrokerCore", Core)

    result = RedisBackendPlugin().cleanup_target(
        "redis://example/0",
        backend_options={"namespace": "tenant"},
        config=marker,
    )

    assert result is False
    assert runner_config == [marker]
    assert core_config == [marker]


def test_listener_channel_and_wait_state_transitions() -> None:
    listener = _listener()
    registration = listener.register("jobs")

    assert listener._channel("jobs") == "simplebroker:tenant:activity:q:jobs"

    listener._error = RuntimeError("listener failed")
    with pytest.raises(OperationalError, match="listener failed"):
        listener.wait(registration, 1.0, stop_event=threading.Event())

    listener._error = None
    registration.version = 3
    listener._versions["jobs"] = 2
    assert listener.wait(registration, 1.0, stop_event=threading.Event()) is True
    assert registration.version == 2

    listener.close()
    assert listener._stop_event.is_set()


def test_closed_activity_waiter_does_not_touch_listener() -> None:
    waiter = object.__new__(redis_plugin_module.RedisActivityWaiter)
    waiter._closed = True

    assert waiter.wait(1.0) is False


def test_activity_registry_releases_only_owned_inactive_listener() -> None:
    class FakeListener(redis_plugin_module._SharedRedisActivityListener):
        _target = "redis://example/0"
        _namespace = "tenant"

        def __init__(self, *, active: bool) -> None:
            self.active = active
            self.close_calls = 0

        def has_registrations(self) -> bool:
            return self.active

        def close(self) -> None:
            self.close_calls += 1

    registry = redis_plugin_module._RedisActivityRegistry()
    owned = FakeListener(active=True)
    key = (os.getpid(), owned._target, owned._namespace)
    registry._listeners[key] = owned

    registry.release(owned)
    assert registry._listeners[key] is owned

    unowned = FakeListener(active=False)
    registry.release(unowned)
    assert registry._listeners[key] is owned
    assert unowned.close_calls == 0

    owned.active = False
    registry.release(owned)
    assert key not in registry._listeners
    assert owned.close_calls == 1


def test_listener_run_reports_failure_and_wakes_registered_queues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listener = _listener()
    listener.register("jobs")

    class BrokenPubSub:
        def psubscribe(self, pattern: str) -> None:
            raise RuntimeError("subscribe failed")

        def close(self) -> None:
            return None

    class Client:
        def pubsub(self, *, ignore_subscribe_messages: bool) -> BrokenPubSub:
            return BrokenPubSub()

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        redis_plugin_module.redis.Redis,
        "from_url",
        lambda *args, **kwargs: Client(),
    )

    listener._run()

    assert isinstance(listener._error, RuntimeError)
    assert str(listener._error) == "subscribe failed"
    assert listener._ready.is_set()


def test_listener_run_ignores_non_queue_activity_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listener = _listener()

    class PubSub:
        def psubscribe(self, pattern: str) -> None:
            return None

        def get_message(self, *, timeout: float) -> dict[str, str]:
            listener._stop_event.set()
            return {"channel": "simplebroker:tenant:other"}

        def close(self) -> None:
            return None

    class Client:
        def pubsub(self, *, ignore_subscribe_messages: bool) -> PubSub:
            return PubSub()

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        redis_plugin_module.redis.Redis,
        "from_url",
        lambda *args, **kwargs: Client(),
    )

    listener._run()

    assert listener._error is None


@pytest.mark.parametrize("method", ["initialize_target", "cleanup_target"])
def test_target_mutations_refuse_foreign_namespace(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
) -> None:
    monkeypatch.setattr(
        redis_plugin_module,
        "inspect_namespace",
        lambda *args, **kwargs: NamespaceInspection(
            "tenant",
            NamespaceState.FOREIGN,
            1,
        ),
    )
    plugin = RedisBackendPlugin()

    with pytest.raises(DatabaseError, match="not available|not SimpleBroker-managed"):
        getattr(plugin, method)(
            "redis://example/0",
            backend_options={"namespace": "tenant"},
        )
