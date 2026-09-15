"""Process-local broker session sharing for persistent Queue handles."""

from __future__ import annotations

import atexit
import os
import sys
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast

from ._backend_plugins import BackendPlugin, BrokerConnection, get_backend_plugin
from ._constants import Config
from ._key_material import FrozenValue, freeze_key_material, snapshot_key_material
from ._targets import BrokerTarget, normalize_sqlite_target

_CLOSE_ACTIVE_OPERATION_TIMEOUT = 5.0
_PENDING_CLEANUP_EXPLICIT = object()
_PENDING_CLEANUP_LAST_USER = object()

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
    config: Config
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


@dataclass
class _CoreDisposalClaim:
    """Carry detached-core ownership across an interruptible handoff."""

    core: BrokerConnection | None = None


# Retain inherited resource graphs without invoking their parent-owned cleanup.
_ABANDONED_FORK_SESSION_ENTRIES: list[dict[_SessionKey, _RegistryEntry]] = []


def _stable_exception_message(failure: BaseException) -> str:
    """Render literal string arguments without invoking custom formatting."""

    string_args = [argument for argument in failure.args if type(argument) is str]
    return ": ".join(string_args) if string_args else "<message unavailable>"


def _retain_cleanup_failure(
    primary: Exception | None,
    failure: Exception,
) -> Exception:
    """Keep one cleanup failure primary and attach each later diagnostic."""

    if primary is None:
        return failure
    failure_notes = tuple(getattr(failure, "__notes__", ()))
    primary.add_note(
        "Additional process-session cleanup failure: "
        f"{type(failure).__qualname__}: {_stable_exception_message(failure)}"
    )
    for note in failure_notes:
        primary.add_note(f"Additional process-session cleanup diagnostic: {note}")
    return primary


def _attach_process_session_cleanup_failure(
    primary: BaseException,
    failure: Exception,
) -> None:
    """Attach deferred cleanup evidence without replacing the active failure."""

    failure_notes = tuple(getattr(failure, "__notes__", ()))
    primary.add_note(
        "Additional process-session cleanup failure: "
        f"{type(failure).__qualname__}: {_stable_exception_message(failure)}"
    )
    for note in failure_notes:
        primary.add_note(f"Additional process-session cleanup diagnostic: {note}")


def _first_present_failure(
    *failures: BaseException | None,
) -> BaseException | None:
    """Return the first failure by identity, without invoking truthiness."""

    return next((failure for failure in failures if failure is not None), None)


def _capture_process_session_cleanup(
    primary: Exception | None,
    action: Callable[[], None],
) -> Exception | None:
    """Run one cleanup while retaining arbitrary ordinary failure evidence."""

    try:
        action()
    except Exception as failure:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-035] exception
        return _retain_cleanup_failure(primary, failure)
    return primary


def _target_parts(
    db_path: str | BrokerTarget,
) -> tuple[str, str, dict[str, Any], BackendPlugin]:
    if isinstance(db_path, BrokerTarget):
        target = db_path.target
        if db_path.backend_name == "sqlite":
            target = normalize_sqlite_target(target)
        return (
            db_path.backend_name,
            target,
            dict(db_path.backend_options),
            db_path.plugin,
        )
    return (
        "sqlite",
        normalize_sqlite_target(str(db_path)),
        {},
        get_backend_plugin("sqlite"),
    )


def _session_key(db_path: str | BrokerTarget, config: Config) -> _SessionKey:
    return _session_spec(db_path, config).key


def _session_spec(
    db_path: str | BrokerTarget,
    config: Config,
) -> _SessionSpec:
    backend_name, target, backend_options, backend_plugin = _target_parts(db_path)
    backend_options_snapshot = cast(
        dict[str, Any], snapshot_key_material(backend_options)
    )
    key = _SessionKey(
        pid=_getpid(),
        backend_name=backend_name,
        target=target,
        backend_options=freeze_key_material(backend_options_snapshot),
        config=freeze_key_material((config.prefix, config._defaults, config)),
    )
    return _SessionSpec(
        key=key,
        backend_name=backend_name,
        target=target,
        backend_options=backend_options_snapshot,
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
        self._factory_close_deferred = False

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
        except Exception as failure:
            if lease_operation:
                self._end_operation(active_failure=failure)
            raise
        finally:
            if creation_started:
                active_failure = sys.exc_info()[1]
                try:
                    self._end_core_creation()
                except Exception as cleanup_failure:
                    if active_failure is None:
                        raise
                    _attach_process_session_cleanup_failure(
                        active_failure,
                        cleanup_failure,
                    )

    def _begin_operation(self) -> None:
        """Retain the session while a queue operation is using a core."""

        with self._operation_condition:
            if self._closed or self._closing:
                raise RuntimeError("Broker session is closed")
            self._active_operations += 1
            depth = int(getattr(self._thread_local, "operation_depth", 0))
            self._thread_local.operation_depth = depth + 1

    def _end_operation(self, *, active_failure: BaseException | None = None) -> None:
        """Release one active queue operation lease."""

        claim = _CoreDisposalClaim()
        release_operation = False
        cleanup_failure: Exception | None = None
        unwind_failure: BaseException | None = None
        try:
            with self._operation_condition:
                depth = self._pop_operation_depth_locked()
                if depth is None or self._active_operations <= 0:
                    return
                release_operation = True
                if depth == 1:
                    self._claim_pending_cleanup_locked(claim)

            if claim.core is not None:
                cleanup_failure = self._dispose_claimed_core(claim)
        except BaseException as failure:
            unwind_failure = failure
            raise
        finally:
            self._restore_claimed_core(claim)
            if release_operation:
                self._release_operation_hold()
            retry_failure = self._retry_closed_claims_after_operation(
                unwind_failure,
                cleanup_failure,
                active_failure,
            )
            if retry_failure is not None:
                cleanup_failure = retry_failure

        if cleanup_failure is not None:
            if active_failure is not None:
                _attach_process_session_cleanup_failure(
                    active_failure,
                    cleanup_failure,
                )
            else:
                raise cleanup_failure

    def _pop_operation_depth_locked(self) -> int | None:
        """Release and return this thread's current operation depth."""

        depth = self.current_thread_operation_depth()
        if depth <= 0:
            return None
        if depth > 1:
            self._thread_local.operation_depth = depth - 1
        else:
            delattr(self._thread_local, "operation_depth")
        return depth

    def _claim_current_thread_core_locked(
        self,
        claim: _CoreDisposalClaim,
    ) -> None:
        """Detach reusable TLS while retaining session ownership."""

        core = cast(
            BrokerConnection | None,
            getattr(self._thread_local, "core", None),
        )
        if core is None:
            return
        claim.core = core
        delattr(self._thread_local, "core")
        self._cores.discard(core)

    def _claim_pending_cleanup_locked(self, claim: _CoreDisposalClaim) -> None:
        """Consume a pending outermost cleanup after operation guards pass."""

        if getattr(self._thread_local, "cleanup_pending", None) is None:
            return
        if self._closed:
            delattr(self._thread_local, "cleanup_pending")
            if hasattr(self._thread_local, "core"):
                delattr(self._thread_local, "core")
            return
        self._claim_current_thread_core_locked(claim)
        delattr(self._thread_local, "cleanup_pending")

    def _restore_claimed_core(self, claim: _CoreDisposalClaim) -> None:
        """Return an unfinished detached claim to terminal session ownership."""

        core = claim.core
        if core is None:
            return
        with self._operation_condition:
            self._cores.add(core)
            claim.core = None

    def _dispose_claimed_core(self, claim: _CoreDisposalClaim) -> Exception | None:
        """Dispose one detached core; session ownership survives failure."""

        core = claim.core
        assert core is not None
        try:
            self._factory.close_core(core)
        except Exception as failure:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-035] exception
            self._restore_claimed_core(claim)
            return failure
        except BaseException:
            self._restore_claimed_core(claim)
            raise
        claim.core = None
        return None

    def _request_current_thread_cleanup_locked(
        self,
        claim: _CoreDisposalClaim,
        *,
        cause: object = _PENDING_CLEANUP_EXPLICIT,
    ) -> None:
        """Request cleanup while the operation condition is held."""

        if self._closed or self._closing:
            return
        core = cast(
            BrokerConnection | None,
            getattr(self._thread_local, "core", None),
        )
        if core is None:
            return
        depth = self.current_thread_operation_depth()
        if depth > 0:
            pending = getattr(self._thread_local, "cleanup_pending", None)
            if cause is _PENDING_CLEANUP_EXPLICIT or pending is None:
                self._thread_local.cleanup_pending = cause
            return
        # A raw drain hold keeps terminal factory close behind disposal without
        # changing this thread's operation depth.
        self._active_operations += 1
        hold_count = int(getattr(self._thread_local, "cleanup_disposal_hold_count", 0))
        self._thread_local.cleanup_disposal_hold_count = hold_count + 1
        self._claim_current_thread_core_locked(claim)

    def _release_operation_hold(self) -> None:
        """Release one active-operation or disposal drain hold."""

        with self._operation_condition:
            if self._active_operations <= 0:
                return
            self._active_operations -= 1
            if self._active_operations == 0:
                self._operation_condition.notify_all()

    def _release_cleanup_disposal_holds_since(self, initial_count: int) -> None:
        """Release idle-cleanup holds acquired after an invocation snapshot."""

        with self._operation_condition:
            current_count = int(
                getattr(self._thread_local, "cleanup_disposal_hold_count", 0)
            )
            release_count = current_count - initial_count
            if release_count <= 0:
                return
            if initial_count > 0:
                self._thread_local.cleanup_disposal_hold_count = initial_count
            else:
                delattr(self._thread_local, "cleanup_disposal_hold_count")
            self._active_operations = max(
                0,
                self._active_operations - release_count,
            )
            if self._active_operations == 0:
                self._operation_condition.notify_all()

    def _cleanup_current_thread(self, *, cause: object) -> None:
        """Request and complete one caller-thread cleanup when idle."""

        claim = _CoreDisposalClaim()
        active_failure: BaseException | None = None
        initial_hold_count = int(
            getattr(self._thread_local, "cleanup_disposal_hold_count", 0)
        )
        try:
            with self._operation_condition:
                self._request_current_thread_cleanup_locked(claim, cause=cause)
            if claim.core is not None:
                cleanup_failure = self._dispose_claimed_core(claim)
                if cleanup_failure is not None:
                    raise cleanup_failure
        except BaseException as failure:
            active_failure = failure
            raise
        finally:
            self._restore_claimed_core(claim)
            self._release_cleanup_disposal_holds_since(initial_hold_count)
            retry_failure = self._retry_closed_claims(active_failure)
            if retry_failure is not None:
                raise retry_failure

    def _retry_closed_claims(
        self,
        active_failure: BaseException | None,
    ) -> Exception | None:
        """Retry late claims after terminal timeout without replacing failure."""

        if not self._closed or not self._cores:
            return None
        retry_failure = _capture_process_session_cleanup(None, self.close_all)
        if retry_failure is None:
            return None
        if active_failure is None:
            return retry_failure
        _attach_process_session_cleanup_failure(active_failure, retry_failure)
        return None

    def _retry_closed_claims_after_operation(
        self,
        unwind_failure: BaseException | None,
        cleanup_failure: Exception | None,
        active_failure: BaseException | None,
    ) -> Exception | None:
        """Retry late claims using the established operation-failure order."""

        return self._retry_closed_claims(
            _first_present_failure(
                unwind_failure,
                cleanup_failure,
                active_failure,
            )
        )

    def current_thread_operation_depth(self) -> int:
        """Return this thread's active operation depth."""

        return int(getattr(self._thread_local, "operation_depth", 0))

    def _end_core_creation(self) -> None:
        close_factory = False
        with self._operation_condition:
            if self._active_core_creations <= 0:
                return
            self._active_core_creations -= 1
            if self._active_core_creations == 0:
                close_factory = self._factory_close_deferred
                self._factory_close_deferred = False
                self._operation_condition.notify_all()
        if close_factory:
            self._factory.close()

    def cleanup_current_thread(self) -> None:
        """Recycle the current thread's cached core without releasing the session."""

        self._cleanup_current_thread(cause=_PENDING_CLEANUP_EXPLICIT)

    def add_thread_user(self) -> None:
        """Register one connection manager using this thread's cached core."""

        if threading.current_thread() is threading.main_thread():
            return
        with self._operation_condition:
            if (
                getattr(self._thread_local, "cleanup_pending", None)
                is _PENDING_CLEANUP_LAST_USER
            ):
                delattr(self._thread_local, "cleanup_pending")
            count = int(getattr(self._thread_local, "user_count", 0))
            self._thread_local.user_count = count + 1

    def drop_thread_user(self) -> None:
        """Release this thread's core when its last registered manager closes."""

        if threading.current_thread() is threading.main_thread():
            return
        with self._operation_condition:
            count = int(getattr(self._thread_local, "user_count", 0))
            if count <= 0:
                return
            if count > 1:
                self._thread_local.user_count = count - 1
                return
            delattr(self._thread_local, "user_count")
        self._cleanup_current_thread(cause=_PENDING_CLEANUP_LAST_USER)

    def release_current_thread_connection(
        self,
        *,
        active_failure: BaseException | None = None,
    ) -> None:
        """Release this operation while keeping the backend checkout cached.

        Persistent queues multiplex queue operations through the same
        process-local session. Releasing the current thread's backend core
        after every operation turns ``persistent=True`` into connection churn
        for pool-backed backends. Explicit queue/session cleanup owns the
        actual close lifecycle.
        """

        self._end_operation(active_failure=active_failure)

    def _close_terminal_cores(
        self,
        cores: list[BrokerConnection],
        *,
        retain_failures: bool,
    ) -> Exception | None:
        """Close a terminal snapshot, retaining only retry-owned late claims."""

        cleanup_failure: Exception | None = None
        for index, core in enumerate(cores):
            try:
                self._factory.close_core(core)
            except Exception as failure:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-035] exception
                if retain_failures:
                    with self._operation_condition:
                        self._cores.add(core)
                cleanup_failure = _retain_cleanup_failure(cleanup_failure, failure)
            except BaseException:
                if retain_failures:
                    with self._operation_condition:
                        self._cores.update(cores[index:])
                raise
        return cleanup_failure

    def close_all(self) -> None:
        """Close all owned resources for this session."""

        with self._operation_condition:
            if self._closed:
                if not self._cores:
                    return
                cores = list(self._cores)
                self._cores.clear()
                defer_factory_close = True
                retrying_late_claims = True
            else:
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
                defer_factory_close = self._active_core_creations > 0
                retrying_late_claims = False
                self._factory_close_deferred = defer_factory_close
                if hasattr(self._thread_local, "core"):
                    delattr(self._thread_local, "core")

        cleanup_failure = self._close_terminal_cores(
            cores,
            retain_failures=retrying_late_claims,
        )
        if not defer_factory_close:
            cleanup_failure = _capture_process_session_cleanup(
                cleanup_failure,
                self._factory.close,
            )

        if cleanup_failure is not None:
            raise cleanup_failure


class _ProcessBrokerSessionRegistry:
    """Reference-counted registry for process-local broker sessions."""

    def __init__(self) -> None:
        self._pid = _getpid()
        self._lock = threading.RLock()
        self._entries: dict[_SessionKey, _RegistryEntry] = {}

    def _recover_after_fork_if_needed(self) -> None:
        """Replace process state before acquire, release, or shutdown takes a lock."""
        current_pid = _getpid()
        if current_pid == self._pid:
            return
        # First child access follows the runner's single-threaded recovery rule.
        # Closing or dropping this graph could enter a vanished parent's locks.
        if self._entries:
            _ABANDONED_FORK_SESSION_ENTRIES.append(self._entries)
        self._entries = {}
        self._lock = threading.RLock()
        self._pid = current_pid

    def acquire(
        self,
        db_path: str | BrokerTarget,
        *,
        config: Config,
        factory_builder: _SessionCoreFactoryBuilder,
    ) -> tuple[_SessionKey, _ProcessBrokerSession]:
        self._recover_after_fork_if_needed()
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
        self._recover_after_fork_if_needed()
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
        self._recover_after_fork_if_needed()
        with self._lock:
            entries = list(self._entries.values())
            self._entries.clear()

        cleanup_failure: Exception | None = None
        for entry in entries:
            cleanup_failure = _capture_process_session_cleanup(
                cleanup_failure,
                entry.session.close_all,
            )

        if cleanup_failure is not None:
            raise cleanup_failure


_registry = _ProcessBrokerSessionRegistry()


def acquire_process_broker_session(
    db_path: str | BrokerTarget,
    *,
    config: Config,
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
