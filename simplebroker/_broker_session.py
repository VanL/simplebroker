"""Process-local broker session sharing for persistent Queue handles."""

from __future__ import annotations

import atexit
import os
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ._backend_plugins import BackendPlugin, get_backend_plugin
from ._constants import load_config, resolve_config
from ._runner_lifecycle import close_owned_runner
from ._targets import ResolvedTarget

if TYPE_CHECKING:
    from ._runner import SQLRunner
    from .db import BrokerCore, BrokerDB

_config = load_config()

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
    db_path: str | ResolvedTarget,
) -> tuple[str, str, dict[str, Any], BackendPlugin]:
    if isinstance(db_path, ResolvedTarget):
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


def _session_key(
    db_path: str | ResolvedTarget, config: Mapping[str, Any]
) -> _SessionKey:
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
        db_path: str | ResolvedTarget,
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
        self._cores: set[BrokerCore | BrokerDB] = set()
        self._runner: SQLRunner | None = None
        self._closed = False

    def get_connection(
        self,
        stop_event: threading.Event | None,
    ) -> BrokerCore | BrokerDB:
        """Return this thread's shared core, creating it if needed."""

        with self._lock:
            if self._closed:
                raise RuntimeError("Broker session is closed")

            core = getattr(self._thread_local, "core", None)
            if core is None:
                core = self._create_core()
                self._thread_local.core = core
                self._cores.add(core)

            core.set_stop_event(stop_event)
            return core

    def _create_core(self) -> BrokerCore | BrokerDB:
        if self._backend_name == "sqlite":
            from .db import BrokerDB

            return BrokerDB(self._target, config=self._config)

        from .db import BrokerCore

        if self._runner is None:
            self._runner = self._backend_plugin.create_runner(
                self._target,
                backend_options=self._backend_options,
                config=self._config,
            )
        return BrokerCore(
            self._runner,
            config=self._config,
            backend_plugin=self._backend_plugin,
        )

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
        """Release a pooled backend checkout while keeping this thread's core."""

        if self._backend_name == "sqlite":
            return

        with self._lock:
            core = getattr(self._thread_local, "core", None)
        if core is not None:
            core.close()

    def close_all(self) -> None:
        """Close all owned resources for this session."""

        with self._lock:
            if self._closed:
                return
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

        if runner is not None:
            close_owned_runner(runner)


class _ProcessBrokerSessionRegistry:
    """Reference-counted registry for process-local broker sessions."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: dict[_SessionKey, _RegistryEntry] = {}

    def acquire(
        self,
        db_path: str | ResolvedTarget,
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
    db_path: str | ResolvedTarget,
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
