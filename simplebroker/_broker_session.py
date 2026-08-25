"""Process-local broker session sharing for persistent Queue handles."""

from __future__ import annotations

import atexit
import os
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from ._backend_plugins import BackendPlugin, BrokerConnection, get_backend_plugin
from ._constants import ResolvedConfig
from ._key_material import FrozenValue, freeze_key_material
from ._targets import BrokerTarget

_CLOSE_ACTIVE_OPERATION_TIMEOUT = 5.0

# Module-owned pid seam: tests patch this alias instead of the shared
# ``os.getpid``, which other threads and finalizers may observe.
_getpid = os.getpid


@dataclass(frozen=True)
class _SessionKey:
    pid: int
    backend_name: str
    target: str
    backend_options: FrozenValue
    config: FrozenValue


@dataclass(frozen=True)
class _SessionSpec:
    key: _SessionKey
    backend_name: str
    target: str
    backend_options: Mapping[str, Any]
    config: ResolvedConfig
    backend_plugin: BackendPlugin


class _SessionCoreFactory(Protocol):
    """Construct and close the concrete cores owned by one process session."""

    def create(
        self,
        stop_event: threading.Event | None,
    ) -> BrokerConnection: ...

    def close_core(self, core: BrokerConnection) -> None: ...

    def close(self) -> None: ...


_SessionCoreFactoryBuilder = Callable[[_SessionSpec], _SessionCoreFactory]


@dataclass
class _RegistryEntry:
    session: _ProcessBrokerSession
    refcount: int = 0


def _normalize_sqlite_target(target: str) -> str:
    path = Path(target).expanduser()
    try:
        return str(path.resolve())
    except (OSError, ValueError):
        return str(path)


def _target_parts(
    db_path: str | BrokerTarget,
) -> tuple[str, str, dict[str, Any], BackendPlugin]:
    if isinstance(db_path, BrokerTarget):
        target = db_path.target
        if db_path.backend_name == "sqlite":
            target = _normalize_sqlite_target(target)
        return (
            db_path.backend_name,
            target,
            dict(db_path.backend_options),
            db_path.plugin,
        )
    return (
        "sqlite",
        _normalize_sqlite_target(str(db_path)),
        {},
        get_backend_plugin("sqlite"),
    )


def _session_key(db_path: str | BrokerTarget, config: ResolvedConfig) -> _SessionKey:
    return _session_spec(db_path, config).key


def _session_spec(
    db_path: str | BrokerTarget,
    config: ResolvedConfig,
) -> _SessionSpec:
    backend_name, target, backend_options, backend_plugin = _target_parts(db_path)
    key = _SessionKey(
        pid=_getpid(),
        backend_name=backend_name,
        target=target,
        backend_options=freeze_key_material(backend_options),
        config=freeze_key_material(config),
    )
    return _SessionSpec(
        key=key,
        backend_name=backend_name,
        target=target,
        backend_options=dict(backend_options),
        config=config,
        backend_plugin=backend_plugin,
    )


class _ProcessBrokerSession:
    """Backend session shared by persistent queues for one target in one process."""

    def __init__(
        self,
        factory: _SessionCoreFactory,
    ) -> None:
        self._factory = factory
        self._thread_local = threading.local()
        self._lock = threading.RLock()
        self._operation_condition = threading.Condition(self._lock)
        self._active_operations = 0
        self._active_core_creations = 0
        self._cores: set[BrokerConnection] = set()
        self._closing = False
        self._closed = False

    def get_connection(
        self,
        stop_event: threading.Event | None,
        *,
        lease_operation: bool = True,
    ) -> BrokerConnection:
        """Return this thread's shared core, creating it if needed."""

        if lease_operation:
            self._begin_operation()
        creation_started = False
        try:
            with self._operation_condition:
                if self._closed or self._closing:
                    raise RuntimeError("Broker session is closed")

                core = cast(
                    BrokerConnection | None,
                    getattr(self._thread_local, "core", None),
                )
                if core is not None:
                    core.set_stop_event(stop_event)
                    return core

                self._active_core_creations += 1
                creation_started = True

            core = self._factory.create(stop_event)

            with self._operation_condition:
                discard_core = self._closed or self._closing
                if not discard_core:
                    self._thread_local.core = core
                    self._cores.add(core)
                    core.set_stop_event(stop_event)
                    return core

            self._factory.close_core(core)
            raise RuntimeError("Broker session is closed")
        except Exception:
            if lease_operation:
                self._end_operation()
            raise
        finally:
            if creation_started:
                self._end_core_creation()

    def _begin_operation(self) -> None:
        """Retain the session while a queue operation is using a core."""

        with self._operation_condition:
            if self._closed or self._closing:
                raise RuntimeError("Broker session is closed")
            self._active_operations += 1
            depth = int(getattr(self._thread_local, "operation_depth", 0))
            self._thread_local.operation_depth = depth + 1

    def _end_operation(self) -> None:
        """Release one active queue operation lease."""

        with self._operation_condition:
            depth = int(getattr(self._thread_local, "operation_depth", 0))
            if depth > 1:
                self._thread_local.operation_depth = depth - 1
            elif depth == 1:
                delattr(self._thread_local, "operation_depth")

            if self._active_operations <= 0:
                return

            self._active_operations -= 1
            if self._active_operations == 0:
                self._operation_condition.notify_all()

    def _end_core_creation(self) -> None:
        with self._operation_condition:
            if self._active_core_creations <= 0:
                return
            self._active_core_creations -= 1
            if self._active_core_creations == 0:
                self._operation_condition.notify_all()

    def cleanup_current_thread(self) -> None:
        """Recycle the current thread's cached core without releasing the session."""

        with self._lock:
            core = getattr(self._thread_local, "core", None)
            if core is None:
                return
            delattr(self._thread_local, "core")
            self._cores.discard(core)

        self._factory.close_core(core)

    def release_current_thread_connection(self) -> None:
        """Release this operation while keeping the backend checkout cached.

        Persistent queues multiplex queue operations through the same
        process-local session. Releasing the current thread's backend core
        after every operation turns ``persistent=True`` into connection churn
        for pool-backed backends. Explicit queue/session cleanup owns the
        actual close lifecycle.
        """

        self._end_operation()

    def close_all(self) -> None:
        """Close all owned resources for this session."""

        with self._operation_condition:
            if self._closed:
                return
            self._closing = True
            deadline = time.monotonic() + _CLOSE_ACTIVE_OPERATION_TIMEOUT
            while self._active_operations > 0 or self._active_core_creations > 0:
                # Daemon threads may not release leases during interpreter shutdown.
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._operation_condition.wait(timeout=remaining)
            self._closed = True
            cores = list(self._cores)
            self._cores.clear()
            if hasattr(self._thread_local, "core"):
                delattr(self._thread_local, "core")

        for core in cores:
            self._factory.close_core(core)
        self._factory.close()


class _ProcessBrokerSessionRegistry:
    """Reference-counted registry for process-local broker sessions."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: dict[_SessionKey, _RegistryEntry] = {}

    def acquire(
        self,
        db_path: str | BrokerTarget,
        *,
        config: ResolvedConfig,
        factory_builder: _SessionCoreFactoryBuilder,
    ) -> tuple[_SessionKey, _ProcessBrokerSession]:
        spec = _session_spec(db_path, config)
        key = spec.key
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                entry = _RegistryEntry(
                    session=_ProcessBrokerSession(factory_builder(spec))
                )
                self._entries[key] = entry
            entry.refcount += 1
            return key, entry.session

    def release(self, key: _SessionKey) -> None:
        session: _ProcessBrokerSession | None = None
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return
            entry.refcount -= 1
            if entry.refcount > 0:
                return
            session = entry.session
            del self._entries[key]

        session.close_all()

    def close_all(self) -> None:
        with self._lock:
            entries = list(self._entries.values())
            self._entries.clear()

        for entry in entries:
            entry.session.close_all()


_registry = _ProcessBrokerSessionRegistry()


def acquire_process_broker_session(
    db_path: str | BrokerTarget,
    *,
    config: ResolvedConfig,
    factory_builder: _SessionCoreFactoryBuilder,
) -> tuple[_SessionKey, _ProcessBrokerSession]:
    return _registry.acquire(
        db_path,
        config=config,
        factory_builder=factory_builder,
    )


def release_process_broker_session(key: _SessionKey) -> None:
    _registry.release(key)


def close_process_broker_sessions() -> None:
    _registry.close_all()


atexit.register(close_process_broker_sessions)


__all__ = [
    "acquire_process_broker_session",
    "close_process_broker_sessions",
    "release_process_broker_session",
]
