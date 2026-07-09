"""Process-local broker session sharing for persistent Queue handles."""

from __future__ import annotations

import atexit
import os
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from ._backend_plugins import BackendPlugin, BrokerConnection, get_backend_plugin
from ._constants import load_config, resolve_config
from ._runner import (
    close_owned_runner,
    lease_runner_thread_connection,
    release_runner_thread_connection,
)
from ._targets import BrokerTarget

if TYPE_CHECKING:
    from ._runner import SQLRunner

_config = load_config()
_CLOSE_ACTIVE_OPERATION_TIMEOUT = 5.0

_FrozenValue = (
    tuple[tuple[str, "_FrozenValue"], ...]
    | tuple["_FrozenValue", ...]
    | str
    | int
    | float
    | bool
    | None
)


@dataclass(frozen=True)
class _SessionKey:
    pid: int
    backend_name: str
    target: str
    backend_options: _FrozenValue
    config: _FrozenValue


@dataclass
class _RegistryEntry:
    session: _ProcessBrokerSession
    refcount: int = 0


def _freeze_for_key(value: Any) -> _FrozenValue:
    """Freeze common Python data structures into deterministic key material."""

    if isinstance(value, Mapping):
        return tuple(
            (str(key), _freeze_for_key(item))
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_for_key(item) for item in value)
    if isinstance(value, set):
        return tuple(sorted((_freeze_for_key(item) for item in value), key=repr))
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return repr(value)


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


def _session_key(db_path: str | BrokerTarget, config: Mapping[str, Any]) -> _SessionKey:
    backend_name, target, backend_options, _ = _target_parts(db_path)
    return _SessionKey(
        pid=os.getpid(),
        backend_name=backend_name,
        target=target,
        backend_options=_freeze_for_key(backend_options),
        config=_freeze_for_key(resolve_config(dict(config))),
    )


class _ProcessBrokerSession:
    """Backend session shared by persistent queues for one target in one process."""

    def __init__(
        self,
        db_path: str | BrokerTarget,
        *,
        config: Mapping[str, Any] = _config,
    ) -> None:
        self._config = resolve_config(dict(config))
        (
            self._backend_name,
            self._target,
            self._backend_options,
            self._backend_plugin,
        ) = _target_parts(db_path)
        self._thread_local = threading.local()
        self._lock = threading.RLock()
        self._operation_condition = threading.Condition(self._lock)
        self._active_operations = 0
        self._cores: set[BrokerConnection] = set()
        self._runner: SQLRunner | None = None
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
        with self._lock:
            if self._closed or self._closing:
                if lease_operation:
                    self._end_operation()
                raise RuntimeError("Broker session is closed")

            core = cast(
                BrokerConnection | None, getattr(self._thread_local, "core", None)
            )
            if core is not None:
                core.set_stop_event(stop_event)
                return core

        try:
            core = self._create_core(stop_event)
        except Exception:
            if lease_operation:
                self._end_operation()
            raise

        with self._lock:
            if self._closed or self._closing:
                try:
                    if self._backend_name == "sqlite":
                        core.shutdown()
                    else:
                        core.close()
                finally:
                    if lease_operation:
                        self._end_operation()
                raise RuntimeError("Broker session is closed")
            self._thread_local.core = core
            self._cores.add(core)
            core.set_stop_event(stop_event)
            return core

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

    def _create_core(self, stop_event: threading.Event | None) -> BrokerConnection:
        if self._backend_name == "sqlite":
            from .db import BrokerDB

            return BrokerDB(self._target, config=self._config, stop_event=stop_event)

        from .db import BrokerCore, _is_direct_backend

        if _is_direct_backend(self._backend_plugin):
            if self._runner is None:
                with self._lock:
                    if self._runner is None:
                        self._runner = self._backend_plugin.create_runner(
                            self._target,
                            backend_options=self._backend_options,
                            config=self._config,
                        )
            leased = lease_runner_thread_connection(self._runner)
            try:
                return self._backend_plugin.create_core_from_runner(
                    self._runner,
                    config=self._config,
                    stop_event=stop_event,
                )
            except Exception:
                if leased:
                    release_runner_thread_connection(self._runner)
                raise

        if self._runner is None:
            with self._lock:
                if self._runner is None:
                    self._runner = self._backend_plugin.create_runner(
                        self._target,
                        backend_options=self._backend_options,
                        config=self._config,
                    )
        leased = lease_runner_thread_connection(self._runner)
        try:
            return BrokerCore(
                self._runner,
                config=self._config,
                backend_plugin=self._backend_plugin,
                stop_event=stop_event,
            )
        except Exception:
            if leased:
                release_runner_thread_connection(self._runner)
            raise

    def cleanup_current_thread(self) -> None:
        """Recycle the current thread's cached core without releasing the session."""

        with self._lock:
            core = getattr(self._thread_local, "core", None)
            if core is None:
                return
            delattr(self._thread_local, "core")
            self._cores.discard(core)

        if self._backend_name == "sqlite":
            core.shutdown()
        else:
            core.close()

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
            while self._active_operations > 0:
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
            runner = self._runner
            self._runner = None

        if self._backend_name == "sqlite":
            for core in cores:
                core.shutdown()
            return

        for core in cores:
            core.close()

        if runner is not None:
            close_owned_runner(runner)


class _ProcessBrokerSessionRegistry:
    """Reference-counted registry for process-local broker sessions."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: dict[_SessionKey, _RegistryEntry] = {}

    def acquire(
        self,
        db_path: str | BrokerTarget,
        *,
        config: Mapping[str, Any] = _config,
    ) -> tuple[_SessionKey, _ProcessBrokerSession]:
        key = _session_key(db_path, config)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                entry = _RegistryEntry(
                    session=_ProcessBrokerSession(db_path, config=config)
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
    config: Mapping[str, Any] = _config,
) -> tuple[_SessionKey, _ProcessBrokerSession]:
    return _registry.acquire(db_path, config=config)


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
