"""Valkey/Redis backend plugin for SimpleBroker."""

from __future__ import annotations

import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import redis

from simplebroker._backend_plugins import ActivityWaiter
from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker._exceptions import DatabaseError, OperationalError

from . import scripts
from ._constants import DEFAULT_NAMESPACE, DEFAULT_TARGET, REDIS_SCHEMA_VERSION
from .core import RedisBrokerCore
from .keys import RedisKeys
from .pool import POOL_OPTION_KEYS, pool_options_from_config
from .responses import response_int
from .runner import RedisRunner
from .validation import (
    NamespaceState,
    inspect_namespace,
    is_namespace_key,
    key_prefix,
    require_namespace,
    validate_target,
)


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def _database_number(value: object) -> int:
    raw = _text(value)
    if not raw or raw == "simplebroker":
        return 0
    try:
        number = int(raw)
    except ValueError as exc:
        raise DatabaseError(
            "BROKER_BACKEND_DATABASE must be a Redis DB number"
        ) from exc
    if number < 0:
        raise DatabaseError("BROKER_BACKEND_DATABASE must be non-negative")
    return number


def _target_from_parts(config: Mapping[str, Any]) -> str:
    host = _text(config.get("BROKER_BACKEND_HOST"), "127.0.0.1")
    port = _text(config.get("BROKER_BACKEND_PORT"), "6379")
    password = config.get("BROKER_BACKEND_PASSWORD")
    db = _database_number(config.get("BROKER_BACKEND_DATABASE"))
    auth = f":{quote(str(password), safe='')}@" if password else ""
    return f"redis://{auth}{host}:{port}/{db}"


def _namespace_from_options(
    config: Mapping[str, Any],
    toml_options: Mapping[str, Any] | None,
) -> str:
    options = dict(toml_options or {})
    if "namespace" in options and "schema" in options:
        namespace = _text(options["namespace"])
        schema = _text(options["schema"])
        if namespace != schema:
            raise DatabaseError("Redis namespace and schema options must match")
    if "namespace" in options or "schema" in options:
        return require_namespace(options)
    schema = _text(config.get("BROKER_BACKEND_SCHEMA"), DEFAULT_NAMESPACE)
    return require_namespace({"namespace": schema})


def _normalize_backend_options(
    config: Mapping[str, Any] | None,
    backend_options: Mapping[str, Any] | None,
) -> dict[str, Any]:
    resolved_config = config or {}
    options = dict(backend_options or {})
    namespace = _namespace_from_options(resolved_config, options)
    allowed = {"namespace", "schema", *POOL_OPTION_KEYS}
    unknown = sorted(str(key) for key in options if key not in allowed)
    if unknown:
        joined = ", ".join(unknown)
        raise DatabaseError(f"Unknown Redis backend option(s): {joined}")
    normalized = dict(options)
    normalized["namespace"] = namespace
    normalized.pop("schema", None)
    pool_options_from_config(resolved_config, normalized)
    return normalized


@dataclass(slots=True)
class _QueueWaiterRegistration:
    queue_name: str
    condition: threading.Condition
    version: int
    wildcard_version: int


class _SharedRedisActivityListener:
    """Process-local Pub/Sub listener for one target and namespace."""

    def __init__(self, target: str, namespace: str) -> None:
        self._target = target
        self._namespace = namespace
        self._prefix = key_prefix(namespace)
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._ready = threading.Event()
        self._conditions: dict[str, threading.Condition] = {}
        self._versions: dict[str, int] = {}
        self._refcounts: dict[str, int] = {}
        self._wildcard_version = 0
        self._error: Exception | None = None
        self._thread = threading.Thread(
            target=self._run,
            name=f"simplebroker-redis-listener-{namespace}",
            daemon=True,
        )
        self._thread.start()
        if not self._ready.wait(timeout=5.0):
            self.close()
            raise OperationalError("Redis activity listener did not start")
        if self._error is not None:
            self.close()
            raise OperationalError(str(self._error)) from self._error

    def _channel(self, queue_name: str | None = None) -> str:
        if queue_name is None:
            return f"{self._prefix}:activity:all"
        return f"{self._prefix}:activity:q:{queue_name}"

    def _run(self) -> None:
        client = redis.Redis.from_url(self._target, decode_responses=True)
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        try:
            pubsub.psubscribe(f"{self._prefix}:activity:*")
            self._ready.set()
            while not self._stop_event.is_set():
                message = pubsub.get_message(timeout=0.1)
                if not message:
                    continue
                channel = str(message.get("channel", ""))
                with self._lock:
                    if channel == self._channel(None):
                        continue
                    prefix = f"{self._prefix}:activity:q:"
                    if channel.startswith(prefix):
                        queue_name = channel[len(prefix) :]
                        if queue_name in self._versions:
                            self._versions[queue_name] += 1
                            self._conditions[queue_name].notify_all()
        except Exception as exc:
            if not self._stop_event.is_set():
                self._error = exc
                self._ready.set()
                with self._lock:
                    for condition in self._conditions.values():
                        condition.notify_all()
        finally:
            self._ready.set()
            pubsub.close()
            client.close()

    def register(self, queue_name: str) -> _QueueWaiterRegistration:
        with self._lock:
            refcount = self._refcounts.get(queue_name, 0)
            self._refcounts[queue_name] = refcount + 1
            if refcount == 0:
                self._conditions[queue_name] = threading.Condition(self._lock)
                self._versions[queue_name] = 0
            return _QueueWaiterRegistration(
                queue_name=queue_name,
                condition=self._conditions[queue_name],
                version=self._versions[queue_name],
                wildcard_version=self._wildcard_version,
            )

    def unregister(self, queue_name: str) -> None:
        with self._lock:
            refcount = self._refcounts.get(queue_name, 0)
            if refcount <= 1:
                self._refcounts.pop(queue_name, None)
                self._versions.pop(queue_name, None)
                self._conditions.pop(queue_name, None)
            else:
                self._refcounts[queue_name] = refcount - 1

    def has_registrations(self) -> bool:
        with self._lock:
            return bool(self._refcounts)

    def wait(self, registration: _QueueWaiterRegistration, timeout: float) -> bool:
        deadline = time.monotonic() + max(0.0, timeout)
        with self._lock:
            while True:
                if self._error is not None:
                    raise OperationalError(str(self._error)) from self._error
                current = self._versions.get(registration.queue_name, 0)
                if current != registration.version:
                    if current > registration.version:
                        registration.version += 1
                    else:
                        registration.version = current
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                registration.condition.wait(remaining)

    def close(self) -> None:
        self._stop_event.set()
        with self._lock:
            for condition in self._conditions.values():
                condition.notify_all()


class RedisActivityWaiter:
    def __init__(
        self,
        listener: _SharedRedisActivityListener,
        registration: _QueueWaiterRegistration,
    ) -> None:
        self._listener = listener
        self._registration = registration
        self._closed = False

    def wait(self, timeout: float) -> bool:
        if self._closed:
            return False
        return self._listener.wait(self._registration, timeout)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._listener.unregister(self._registration.queue_name)
        _activity_registry.release(self._listener)


class RedisMultiQueueActivityWaiter:
    def __init__(self, waiters: Sequence[RedisActivityWaiter]) -> None:
        self._waiters = list(waiters)

    def wait(self, timeout: float) -> bool:
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            for waiter in self._waiters:
                if waiter.wait(0):
                    return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(0.05, remaining))

    def close(self) -> None:
        for waiter in self._waiters:
            waiter.close()


class _RedisActivityRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._listeners: dict[tuple[str, str], _SharedRedisActivityListener] = {}

    def listener(self, target: str, namespace: str) -> _SharedRedisActivityListener:
        key = (target, namespace)
        with self._lock:
            listener = self._listeners.get(key)
            if listener is None:
                listener = _SharedRedisActivityListener(target, namespace)
                self._listeners[key] = listener
            return listener

    def release(self, listener: _SharedRedisActivityListener) -> None:
        with self._lock:
            if listener.has_registrations():
                return
            key = (listener._target, listener._namespace)
            if self._listeners.get(key) is listener:
                self._listeners.pop(key, None)
                listener.close()


_activity_registry = _RedisActivityRegistry()


class RedisBackendPlugin:
    """SimpleBroker backend plugin for Valkey/Redis."""

    name = "redis"
    schema_version = REDIS_SCHEMA_VERSION
    sql = None
    is_direct_backend = True

    def init_backend(
        self,
        config: Mapping[str, Any],
        *,
        toml_target: str = "",
        toml_options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        target = _text(toml_target) or _text(config.get("BROKER_BACKEND_TARGET"))
        if not target:
            target = _target_from_parts(config) if config else DEFAULT_TARGET
        backend_options = _normalize_backend_options(config, toml_options)
        return {
            "target": target,
            "backend_options": backend_options,
        }

    def create_runner(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> RedisRunner:
        normalized = _normalize_backend_options(config, backend_options)
        pool_options = pool_options_from_config(config, normalized)
        return RedisRunner(
            target,
            backend_options=normalized,
            config=dict(config or {}),
            pool_options=pool_options,
        )

    def create_core(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
        stop_event: threading.Event | None = None,
    ) -> RedisBrokerCore:
        runner = self.create_runner(
            target, backend_options=backend_options, config=config
        )
        return RedisBrokerCore(runner, config=config, stop_event=stop_event)

    def create_core_from_runner(
        self,
        runner: RedisRunner,
        *,
        config: Mapping[str, Any] | None = None,
        stop_event: threading.Event | None = None,
    ) -> RedisBrokerCore:
        return RedisBrokerCore(runner, config=config, stop_event=stop_event)

    def initialize_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> None:
        del config
        namespace = require_namespace(backend_options)
        inspection = inspect_namespace(target, backend_options={"namespace": namespace})
        if inspection.state not in {NamespaceState.ABSENT, NamespaceState.OWNED}:
            raise DatabaseError(
                f"Redis namespace '{namespace}' is not available for SimpleBroker "
                f"init: {inspection.state.value}"
            )
        if inspection.state is NamespaceState.OWNED:
            return
        client = redis.Redis.from_url(target, decode_responses=True)
        try:
            prefix = key_prefix(namespace)
            client.hset(
                f"{prefix}:meta",
                mapping={
                    "magic": SIMPLEBROKER_MAGIC,
                    "schema_version": str(REDIS_SCHEMA_VERSION),
                    "last_ts": "0",
                    "alias_version": "0",
                },
            )
        except redis.RedisError as exc:
            raise DatabaseError(str(exc)) from exc
        finally:
            client.close()

    def validate_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        verify_initialized: bool = True,
        config: Mapping[str, Any] | None = None,
    ) -> None:
        del config
        validate_target(
            target,
            backend_options=backend_options,
            verify_initialized=verify_initialized,
        )

    def cleanup_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> bool:
        del config
        namespace = require_namespace(backend_options)
        inspection = inspect_namespace(target, backend_options={"namespace": namespace})
        if inspection.state is NamespaceState.ABSENT:
            return False
        if inspection.state is not NamespaceState.OWNED:
            raise DatabaseError(
                f"Redis namespace '{namespace}' is not SimpleBroker-managed "
                f"({inspection.state.value})"
            )
        client = redis.Redis.from_url(target, decode_responses=True)
        try:
            runner = RedisRunner(target, namespace=namespace)
            core = RedisBrokerCore(runner)
            core.recover_stale_batches(max_age_seconds=runner.stale_batch_seconds)
            redis_keys = RedisKeys(namespace)
            prefix = redis_keys.prefix
            for reserved_key in client.scan_iter(f"{redis_keys.prefix}:q:*:reserved"):
                if not is_namespace_key(prefix, reserved_key):
                    continue
                if response_int(client.zcard(reserved_key)):
                    raise DatabaseError(
                        "Cannot clean up Redis namespace while an at_least_once "
                        "batch is active"
                    )
            matching_keys = [
                key
                for key in client.scan_iter(f"{prefix}:*")
                if is_namespace_key(prefix, key)
            ]
            if not matching_keys:
                return False
            unlink = getattr(client, "unlink", None)
            for index in range(0, len(matching_keys), 500):
                chunk = matching_keys[index : index + 500]
                if callable(unlink):
                    unlink(*chunk)
                else:
                    client.delete(*chunk)
            return True
        except redis.RedisError as exc:
            raise DatabaseError(str(exc)) from exc
        finally:
            try:
                runner.close()
            except UnboundLocalError:
                pass
            client.close()

    def check_version(self) -> None:
        return None

    def read_last_ts(self, runner: RedisRunner) -> int:
        try:
            raw = runner.client.hget(f"{key_prefix(runner.namespace)}:meta", "last_ts")
            return response_int(raw or 0)
        except redis.RedisError as exc:
            raise OperationalError(str(exc)) from exc

    def advance_last_ts(self, runner: RedisRunner, *, new_ts: int) -> bool:
        try:
            result = runner.client.eval(
                scripts.ADVANCE_LAST_TS,
                1,
                f"{key_prefix(runner.namespace)}:meta",
                str(new_ts),
            )
            return bool(result)
        except redis.RedisError as exc:
            raise OperationalError(str(exc)) from exc

    def write_last_ts(self, runner: RedisRunner, ts: int) -> None:
        try:
            runner.client.hset(
                f"{key_prefix(runner.namespace)}:meta", "last_ts", str(ts)
            )
        except redis.RedisError as exc:
            raise OperationalError(str(exc)) from exc

    def read_alias_version(self, runner: RedisRunner) -> int:
        try:
            raw = runner.client.hget(
                f"{key_prefix(runner.namespace)}:meta", "alias_version"
            )
            return response_int(raw or 0)
        except redis.RedisError as exc:
            raise OperationalError(str(exc)) from exc

    def create_activity_waiter(
        self,
        *,
        target: str | None,
        backend_options: Mapping[str, Any] | None = None,
        runner: RedisRunner | None = None,
        queue_name: str,
        stop_event: Any,
    ) -> ActivityWaiter | None:
        del stop_event
        if runner is not None:
            target = runner.target
            namespace = runner.namespace
        else:
            if target is None:
                return None
            namespace = require_namespace(backend_options)
        listener = _activity_registry.listener(target, namespace)
        return RedisActivityWaiter(listener, listener.register(queue_name))

    def create_activity_waiter_for_queues(
        self,
        *,
        target: str | None,
        backend_options: Mapping[str, Any] | None = None,
        runner: RedisRunner | None = None,
        queue_names: Sequence[str],
        stop_event: Any,
    ) -> ActivityWaiter | None:
        waiters = [
            self.create_activity_waiter(
                target=target,
                backend_options=backend_options,
                runner=runner,
                queue_name=queue_name,
                stop_event=stop_event,
            )
            for queue_name in queue_names
        ]
        typed_waiters = [
            waiter for waiter in waiters if isinstance(waiter, RedisActivityWaiter)
        ]
        if len(typed_waiters) != len(queue_names):
            return None
        return RedisMultiQueueActivityWaiter(typed_waiters)


_plugin = RedisBackendPlugin()


def get_backend_plugin() -> RedisBackendPlugin:
    return _plugin
