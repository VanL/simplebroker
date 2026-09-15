"""Public process-session lifetime handle ([SB-API-3], [SB-API-11])."""

from __future__ import annotations

import threading
import weakref
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Self

from . import _broker_session
from ._backend_plugins import BrokerConnection
from ._constants import Config, resolve_config
from ._targets import BrokerTarget
from .db import DBConnection, _build_process_session_core_factory
from .sbqueue import Queue, _canonicalize_queue_target, _default_target_from_config


class _ActiveOperationCloseError(RuntimeError):
    """Refuse scope close while this key has an operation on this thread."""


class BrokerSession:
    """Own a scoped lease on one process-shared broker session."""

    def __init__(self) -> None:
        raise TypeError(
            "BrokerSession cannot be constructed directly. "
            "Create a session with BrokerSession.connect()."
        )

    def _initialize(
        self,
        key: _broker_session._SessionKey,
        process_session: _broker_session._ProcessBrokerSession,
        target: str | BrokerTarget,
        config: Config,
    ) -> None:
        self._key = key
        self._process_session = process_session
        self._target = target
        self._config = config
        self._queues: list[Queue] = []
        self._lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._closing = False
        self._released = False
        self._finalizer = weakref.finalize(
            self,
            _broker_session.release_process_broker_session,
            key,
        )

    @classmethod
    def connect(
        cls,
        db_path: str | BrokerTarget | None = None,
        *,
        config: Config | None = None,
    ) -> BrokerSession:
        """Acquire a process-session lease for one resolved target."""
        resolved = resolve_config(config=config)
        unresolved_target = (
            _default_target_from_config(resolved)
            if db_path is None or db_path == ""
            else db_path
        )
        target = _canonicalize_queue_target(
            unresolved_target,
            config=resolved,
            runner=None,
        )
        key, process_session = _broker_session.acquire_process_broker_session(
            target,
            config=resolved,
            factory_builder=_build_process_session_core_factory,
        )
        session = object.__new__(cls)
        session._initialize(key, process_session, target, resolved)
        return session

    def _is_process_owner(self) -> bool:
        return self._key.pid == _broker_session._getpid()

    def _ensure_process_owner(self) -> None:
        if not self._is_process_owner():
            raise RuntimeError(
                "BrokerSession used in a forked process. "
                "Create a new session in the child process."
            )

    def _ensure_open_locked(self) -> None:
        if self._closing or self._released:
            raise RuntimeError(
                "BrokerSession is closed. Create a new session with "
                "BrokerSession.connect()."
            )

    def queue(self, name: str) -> Queue:
        """Create a scope-owned persistent Queue on this session's target."""
        self._ensure_process_owner()
        with self._lock:
            self._ensure_open_locked()
            queue = Queue(
                name,
                db_path=self._target,
                persistent=True,
                config=self._config,
            )
            queue._session = weakref.ref(self)
            self._queues.append(queue)
            return queue

    def recycle_thread(self) -> None:
        """Release this session's cache for the calling thread."""
        self._ensure_process_owner()
        if self._released:
            return
        self._process_session.cleanup_current_thread()

    @contextmanager
    def connection(self) -> Iterator[BrokerConnection]:
        """Yield a connection over this handle's shared process session."""
        self._ensure_process_owner()
        with self._lock:
            self._ensure_open_locked()
            conn = DBConnection(
                self._target,
                None,
                config=self._config,
                share_in_process=True,
            )
        try:
            with conn._operation_connection() as connection:
                yield connection
        finally:
            conn.close()

    def close(self) -> None:
        """Recycle this thread, close minted Queues, and release this lease."""
        if not self._is_process_owner():
            self._closing = True
            self._released = True
            self._finalizer.detach()
            return

        with self._close_lock:
            with self._lock:
                if self._released:
                    return
                if self._process_session.current_thread_operation_depth() > 0:
                    raise _ActiveOperationCloseError(
                        "BrokerSession cannot close while this thread has an open "
                        "Queue or connection operation on the same process-session "
                        "key. Close all such iterators and exit all such connection "
                        "contexts first."
                    )
                self._closing = True
                queues = list(self._queues)

            cleanup_failure: Exception | None = None
            cleanup_failure = _broker_session._capture_process_session_cleanup(
                cleanup_failure,
                self._process_session.cleanup_current_thread,
            )
            for queue in queues:
                cleanup_failure = _broker_session._capture_process_session_cleanup(
                    cleanup_failure,
                    queue.close,
                )

            def release_lease() -> None:
                try:
                    _broker_session.release_process_broker_session(self._key)
                finally:
                    with self._lock:
                        self._released = True
                    self._finalizer.detach()

            cleanup_failure = _broker_session._capture_process_session_cleanup(
                cleanup_failure,
                release_lease,
            )
            if cleanup_failure is not None:
                raise cleanup_failure

    def __enter__(self) -> Self:
        self._ensure_process_owner()
        with self._lock:
            self._ensure_open_locked()
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _traceback: object,
    ) -> None:
        try:
            self.close()
        except Exception as close_failure:
            if exc is None:
                raise
            _broker_session._attach_process_session_cleanup_failure(
                exc,
                close_failure,
            )

    @property
    def target(self) -> str | BrokerTarget:
        """Return the bound target without exposing mutable backend options."""
        if isinstance(self._target, BrokerTarget):
            return self._target._detached()
        return self._target

    @property
    def backend_name(self) -> str:
        return self._key.backend_name

    @property
    def config(self) -> Config:
        return self._config
