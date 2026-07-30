"""Executable transition tables for phase-lock state."""

from __future__ import annotations

import ctypes
import ctypes.util
import errno
import os
import select
import signal
import threading
import time
from pathlib import Path
from typing import Any, Self, cast

import pytest

import simplebroker._phaselock as phaselock
from simplebroker._phaselock import (
    Phase,
    PhaseLockCancelled,
    PhaseLockService,
    PhaseLockTimeout,
)
from tests.helpers.state_machine_contracts import TransitionCase, fires_transition_table


def _case(
    machine: str,
    transition_id: str,
    start: str,
    event: str,
    next_state: str,
    effects: str,
    result: str,
) -> TransitionCase[str]:
    return TransitionCase(
        transition_id=transition_id,
        start_state=start,
        event=event,
        guard=f"{machine} starts in {start!r}; event {event!r} is enabled",
        next_state=next_state,
        effects=effects,
        expected_result=result,
        payload=transition_id,
    )


PHASE_LOCK_TRANSITIONS = (
    _case(
        "phase lock",
        "RUN_MISSING",
        "unmarked",
        "run phases",
        "marked",
        "run action and durably publish completion",
        "phase is completed",
    ),
    _case(
        "phase lock",
        "SKIP_MARKED",
        "marked",
        "run phases",
        "marked",
        "skip action",
        "phase is skipped",
    ),
    _case(
        "phase lock",
        "ACTION_FAILURE",
        "unmarked",
        "action raises",
        "unmarked",
        "release lock without publishing completion",
        "original error propagates",
    ),
    _case(
        "phase lock",
        "CANCEL_BEFORE_ACTION",
        "unmarked",
        "cancellation requested",
        "unmarked",
        "release acquired lock without running action",
        "PhaseLockCancelled propagates",
    ),
    _case(
        "phase lock",
        "CONTENDED_TIMEOUT",
        "held by another thread",
        "wait budget expires",
        "unacquired",
        "leave holder and completion state unchanged",
        "PhaseLockTimeout",
    ),
    _case(
        "phase lock",
        "EMPTY",
        "no requested phases",
        "run phases",
        "no requested phases",
        "avoid lock acquisition and actions",
        "empty completed and skipped sets",
    ),
    _case(
        "phase lock",
        "MARKED_WHILE_WAITING",
        "unmarked and lock held elsewhere",
        "marker appears while waiting",
        "marked without lock acquisition",
        "stop waiting and skip the completed action",
        "phase is skipped",
    ),
    _case(
        "phase lock",
        "XATTR_FALLBACK",
        "unmarked with unusable xattrs",
        "run phase",
        "marked in status sidecar",
        "run action then fall back to durable status marker",
        "phase completes and later run skips",
    ),
)


def _assert_marked_while_waiting(
    target: Path,
    phase: Phase,
    calls: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = PhaseLockService(
        target,
        use_xattrs=False,
        strict_marker_locking=False,
        timeout=1,
        retry_delay=0.001,
    )
    holder = PhaseLockService(target, use_xattrs=False, timeout=1)
    held = threading.Event()
    release = threading.Event()
    waiting = threading.Event()
    result_holder: list[Any] = []
    real_acquire = phaselock._AdvisoryLock._acquire_process_lock

    def observe_wait(self: object, *args: object, **kwargs: object) -> bool:
        if threading.current_thread().name == "phase-contender":
            waiting.set()
        return real_acquire(self, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        phaselock._AdvisoryLock,
        "_acquire_process_lock",
        observe_wait,
    )

    def hold() -> None:
        with holder.locked():
            held.set()
            release.wait(1)

    def contend() -> None:
        result_holder.append(service.run_phases([phase]))

    holder_thread = threading.Thread(target=hold)
    contender_thread = threading.Thread(target=contend, name="phase-contender")
    holder_thread.start()
    assert held.wait(1)
    contender_thread.start()
    assert waiting.wait(1)
    service._write_status_phases(["schema"])
    contender_thread.join(1)
    release.set()
    holder_thread.join(1)
    assert not contender_thread.is_alive()
    assert result_holder[0].skipped == ("schema",)
    assert calls == []


def _assert_xattr_fallback(
    target: Path,
    phase: Phase,
    calls: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _UnavailableXattrs:
        @staticmethod
        def get_value(path: Path, key: str) -> bytes:
            del path, key
            raise OSError(errno.ENOTSUP, "xattrs unavailable")

        @staticmethod
        def set_value(path: Path, key: str, value: bytes) -> None:
            del path, key, value
            raise OSError(errno.ENOTSUP, "xattrs unavailable")

    monkeypatch.setattr(phaselock, "_xattr_provider", _UnavailableXattrs)
    service = PhaseLockService(target, use_xattrs=True)
    first = service.run_phases([phase])
    second = service.run_phases([phase])
    assert first.completed == ("schema",)
    assert second.skipped == ("schema",)
    assert calls == ["run"]


def _assert_contended_timeout(
    target: Path,
    phase: Phase,
    calls: list[str],
) -> None:
    holder = PhaseLockService(
        target,
        use_xattrs=False,
        timeout=1,
        retry_delay=0.001,
    )
    contender = PhaseLockService(
        target,
        use_xattrs=False,
        timeout=0.02,
        retry_delay=0.001,
    )
    acquired = threading.Event()
    release = threading.Event()

    def hold() -> None:
        with holder.locked():
            acquired.set()
            release.wait(1)

    thread = threading.Thread(target=hold)
    thread.start()
    assert acquired.wait(1)
    try:
        with pytest.raises(PhaseLockTimeout):
            contender.run_phases([phase])
    finally:
        release.set()
        thread.join(1)
    assert calls == []


def _assert_basic_phase_transition(
    payload: str,
    target: Path,
    service: PhaseLockService,
    phase: Phase,
    calls: list[str],
) -> None:
    if payload == "SKIP_MARKED":
        service.run_phases([phase])
        calls.clear()
    if payload == "ACTION_FAILURE":
        with pytest.raises(RuntimeError, match="phase failed"):
            service.run_phases([phase])
        assert not service.has_phase("schema")
    elif payload == "CANCEL_BEFORE_ACTION":
        with pytest.raises(PhaseLockCancelled):
            service.run_phases([phase], should_cancel=lambda: True)
        assert calls == []
    elif payload == "CONTENDED_TIMEOUT":
        _assert_contended_timeout(target, phase, calls)
    else:
        result = service.run_phases([phase])
        if payload == "RUN_MISSING":
            assert result.completed == ("schema",)
            assert calls == ["run"]
        else:
            assert result.skipped == ("schema",)
            assert calls == []


@fires_transition_table("SM-PHASE-LOCK", PHASE_LOCK_TRANSITIONS)
def test_phase_lock_fires_transition_table(
    transition_case: TransitionCase[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / f"{transition_case.payload}.db"
    target.touch()
    service = PhaseLockService(target, use_xattrs=False)
    calls: list[str] = []

    def action() -> None:
        calls.append("run")
        if transition_case.payload == "ACTION_FAILURE":
            raise RuntimeError("phase failed")

    phase = Phase("schema", action)
    if transition_case.payload == "EMPTY":
        result = service.run_phases(())
        assert result.completed == ()
        assert result.skipped == ()
        assert calls == []
        return
    if transition_case.payload == "MARKED_WHILE_WAITING":
        _assert_marked_while_waiting(target, phase, calls, monkeypatch)
        return
    if transition_case.payload == "XATTR_FALLBACK":
        _assert_xattr_fallback(target, phase, calls, monkeypatch)
        return
    _assert_basic_phase_transition(
        transition_case.payload,
        target,
        service,
        phase,
        calls,
    )


DARWIN_XATTR_TRANSITIONS = (
    _case(
        "Darwin xattr discovery",
        "NON_DARWIN",
        "uninitialized",
        "discover on non-Darwin",
        "uninitialized",
        "avoid loading libc",
        "no provider",
    ),
    _case(
        "Darwin xattr discovery",
        "CACHE_FAILURE",
        "uninitialized",
        "libc loading fails",
        "failure cached",
        "cache unavailable provider",
        "subsequent lookup does not retry",
    ),
    _case(
        "Darwin xattr discovery",
        "CACHE_SUCCESS",
        "uninitialized",
        "libc symbols load",
        "provider cached",
        "configure and publish one provider",
        "subsequent lookup reuses provider",
    ),
    _case(
        "Darwin xattr discovery",
        "ERANGE_REPROBE",
        "provider cached",
        "value grows between size probe and read",
        "provider cached",
        "reprobe size and retry read",
        "complete expanded value is returned",
    ),
    _case(
        "Darwin xattr discovery",
        "CONCURRENT_FIRST_INIT",
        "uninitialized",
        "concurrent discovery calls",
        "provider cached",
        "publish one reusable provider",
        "all callers observe the cached provider",
    ),
    _case(
        "Darwin xattr discovery",
        "FORK_RESETS_DISCOVERY_LOCK",
        "parent discovery lock held",
        "process forks and child discovers provider",
        "child provider cached",
        "reset inherited discovery guard in child before discovery",
        "child discovery completes without deadlock",
    ),
)


class _CFunction:
    argtypes: list[object] | None = None
    restype: object | None = None

    def __call__(self, *args: object) -> int:
        return 0


class _HandlerCFunction(_CFunction):
    def __init__(self, handler: object) -> None:
        self._handler = handler

    def __call__(self, *args: object) -> int:
        return self._handler(*args)  # type: ignore[operator, no-any-return]


class _LibC:
    def __init__(self) -> None:
        self.getxattr = _CFunction()
        self.setxattr = _CFunction()


class _ObservedLock:
    def __init__(self, second_entered: threading.Event) -> None:
        self._lock = threading.Lock()
        self._second_entered = second_entered

    def __enter__(self) -> Self:
        if threading.current_thread().name == "darwin-provider-second":
            self._second_entered.set()
        self._lock.acquire()
        return self

    def __exit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None:
        del exc_type, exc_value, traceback
        self._lock.release()


def _assert_darwin_fork_probe() -> None:
    if not hasattr(os, "fork"):
        pytest.skip("real fork transition is unavailable on this platform")
    parent_lock = phaselock._DARWIN_XATTR_PROVIDER_LOCK
    parent_lock.acquire()
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(read_fd)
            provider = phaselock._darwin_xattr_provider()
            os.write(write_fd, b"1" if provider is not None else b"0")
        finally:
            os.close(write_fd)
            os._exit(0)
    os.close(write_fd)
    parent_lock.release()
    status: int | None = None
    payload = b""
    try:
        readable, _, _ = select.select([read_fd], [], [], 5.0)
        if readable:
            payload = os.read(read_fd, 1)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            waited_pid, candidate_status = os.waitpid(pid, os.WNOHANG)
            if waited_pid == pid:
                status = candidate_status
                break
            time.sleep(0.01)
        if status is None:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            status = os.waitpid(pid, 0)[1]
        exit_code = os.waitstatus_to_exitcode(status)
        assert readable and payload == b"1" and exit_code == 0, (
            "Darwin discovery child failed or deadlocked after fork; "
            f"pid={pid}, readable={bool(readable)}, payload={payload!r}, "
            f"status={status}, exit_code={exit_code}"
        )
    finally:
        os.close(read_fd)
        if status is None:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.waitpid(pid, 0)


def _install_darwin_loader(
    payload: str,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    list[int],
    list[int],
    bytes,
    threading.Event,
    threading.Event,
    threading.Event,
]:
    monkeypatch.setattr(
        phaselock,
        "_DARWIN_XATTR_PROVIDER",
        phaselock._DARWIN_XATTR_PROVIDER_UNSET,
    )
    monkeypatch.setattr(ctypes.util, "find_library", lambda name: "libc.dylib")
    value = b"expanded"
    calls: list[int] = []
    read_attempts: list[int] = []
    loader_entered = threading.Event()
    release_loader = threading.Event()
    second_lock_entered = threading.Event()
    if payload == "CONCURRENT_FIRST_INIT":
        monkeypatch.setattr(
            phaselock,
            "_DARWIN_XATTR_PROVIDER_LOCK",
            _ObservedLock(second_lock_entered),
        )

    def getxattr(
        path: bytes,
        key: bytes,
        buffer: object | None,
        size: int,
        position: int,
        options: int,
    ) -> int:
        del path, key, position, options
        if buffer is None:
            return 2 if not read_attempts else len(value)
        read_attempts.append(size)
        if len(read_attempts) == 1:
            ctypes.set_errno(errno.ERANGE)
            return -1
        ctypes.memmove(cast(Any, buffer), value, len(value))
        return len(value)

    def load_libc(*args: object, **kwargs: object) -> _LibC:
        del args, kwargs
        calls.append(1)
        if payload == "CONCURRENT_FIRST_INIT" and len(calls) == 1:
            loader_entered.set()
            assert release_loader.wait(2)
        if payload == "CACHE_FAILURE":
            raise OSError("missing libc")
        libc = _LibC()
        if payload == "ERANGE_REPROBE":
            libc.getxattr = _HandlerCFunction(getxattr)
        return libc

    monkeypatch.setattr(ctypes, "CDLL", load_libc)
    return (
        calls,
        read_attempts,
        value,
        loader_entered,
        release_loader,
        second_lock_entered,
    )


def _assert_concurrent_darwin_discovery(
    calls: list[int],
    loader_entered: threading.Event,
    release_loader: threading.Event,
    second_lock_entered: threading.Event,
) -> None:
    providers: list[object | None] = []
    second_finished = threading.Event()

    def discover(*, second: bool = False) -> None:
        providers.append(phaselock._darwin_xattr_provider())
        if second:
            second_finished.set()

    first = threading.Thread(target=discover, name="darwin-provider-first")
    second = threading.Thread(
        target=lambda: discover(second=True),
        name="darwin-provider-second",
    )
    first.start()
    assert loader_entered.wait(1)
    second.start()
    try:
        assert second_lock_entered.wait(1)
        assert not second_finished.is_set(), (
            "second Darwin xattr discovery observed the provisional None cache "
            "instead of waiting for first initialization"
        )
    finally:
        release_loader.set()
        first.join(2)
        second.join(2)
    assert not first.is_alive()
    assert not second.is_alive()
    assert len(providers) == 2
    assert providers[0] is not None
    assert providers[1] is providers[0]
    assert len(calls) == 1


def _assert_darwin_discovery(
    payload: str,
    calls: list[int],
    read_attempts: list[int],
    value: bytes,
    loader_entered: threading.Event,
    release_loader: threading.Event,
    second_lock_entered: threading.Event,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if payload == "NON_DARWIN":
        monkeypatch.setattr(phaselock.sys, "platform", "linux")
        assert phaselock._darwin_xattr_provider() is None
        assert calls == []
        return

    monkeypatch.setattr(phaselock.sys, "platform", "darwin")
    if payload == "FORK_RESETS_DISCOVERY_LOCK":
        _assert_darwin_fork_probe()
        return
    if payload == "CONCURRENT_FIRST_INIT":
        _assert_concurrent_darwin_discovery(
            calls,
            loader_entered,
            release_loader,
            second_lock_entered,
        )
        return

    first = phaselock._darwin_xattr_provider()
    second = phaselock._darwin_xattr_provider()
    if payload == "CACHE_FAILURE":
        assert first is second is None
    else:
        assert first is second
        assert first is not None
        if payload == "ERANGE_REPROBE":
            assert first.get_value(Path("broker.db"), "user.key") == value
            assert len(read_attempts) == 2
    assert len(calls) == 1


@fires_transition_table("SM-DARWIN-XATTR", DARWIN_XATTR_TRANSITIONS)
def test_darwin_xattr_fires_transition_table(
    transition_case: TransitionCase[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        calls,
        read_attempts,
        value,
        loader_entered,
        release_loader,
        second_lock_entered,
    ) = _install_darwin_loader(
        transition_case.payload,
        monkeypatch,
    )
    _assert_darwin_discovery(
        transition_case.payload,
        calls,
        read_attempts,
        value,
        loader_entered,
        release_loader,
        second_lock_entered,
        monkeypatch,
    )
