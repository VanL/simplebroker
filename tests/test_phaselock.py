from __future__ import annotations

import errno
import os
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

import pytest

import simplebroker._phaselock as phaselock_module
from simplebroker._phaselock import (
    AdvisoryFileLock,
    Phase,
    PhaseLockCancelled,
    PhaseLockService,
    PhaseLockTimeout,
    PhaseLockUnavailable,
    PhaseRunResult,
    __version__,
)
from tests.helper_scripts.timing import scale_timeout_for_ci


def _real_xattrs_supported(target: Path) -> bool:
    service = PhaseLockService(target, use_xattrs=True)
    phase_name = "pytest-probe"
    return service.mark_phase(phase_name) and service.has_phase(phase_name)


def _default_xattrs_supported(target: Path) -> bool:
    service = PhaseLockService(target)
    phase_name = "pytest-default-probe"
    return service.mark_phase(phase_name) and service.has_phase(phase_name)


def _wait_for_file(
    path: Path,
    *,
    timeout: float = 5.0,
    proc: subprocess.Popen[str] | None = None,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if proc is not None and proc.poll() is not None:
            stdout, stderr = proc.communicate()
            raise AssertionError(
                f"process exited before creating {path}\n"
                f"returncode={proc.returncode}\nstdout={stdout}\nstderr={stderr}"
            )
        time.sleep(0.01)
    raise AssertionError(f"Timed out waiting for {path}")


def _write_status_file(path: Path, phases: tuple[str, ...] | list[str]) -> None:
    path.write_text("".join(f"{phase}\n" for phase in phases), encoding="utf-8")


def _read_status_file(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def test_version_is_1_0() -> None:
    assert __version__ == "1.0"


def test_xattr_env_mode_parses_enabled_disabled_and_unknown_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(phaselock_module.PHASELOCK_ENABLE_XATTRS, "YES")
    assert phaselock_module._xattr_env_mode() is True

    monkeypatch.setenv(phaselock_module.PHASELOCK_ENABLE_XATTRS, "fallback")
    assert phaselock_module._xattr_env_mode() is False

    monkeypatch.setenv(phaselock_module.PHASELOCK_ENABLE_XATTRS, "definitely")
    assert phaselock_module._xattr_env_mode() is None


def test_stdlib_xattr_provider_wraps_get_and_set_values_as_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "broker.db"
    calls: list[tuple[Path, str, bytes]] = []

    def getxattr(path: Path, key: str) -> bytearray:
        assert path == target
        assert key == "user.phaselock.phase"
        return bytearray(b"done")

    def setxattr(path: Path, key: str, value: bytes) -> None:
        calls.append((path, key, value))

    monkeypatch.setattr(phaselock_module.os, "getxattr", getxattr, raising=False)
    monkeypatch.setattr(phaselock_module.os, "setxattr", setxattr, raising=False)

    provider = phaselock_module._xattr_provider()

    assert provider is not None
    assert provider.get_value(target, "user.phaselock.phase") == b"done"
    provider.set_value(target, "user.phaselock.phase", cast(Any, bytearray(b"1")))
    assert calls == [(target, "user.phaselock.phase", b"1")]


def test_darwin_xattr_provider_returns_none_on_non_darwin_platform(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(phaselock_module.sys, "platform", "linux")
    monkeypatch.setattr(
        phaselock_module,
        "_DARWIN_XATTR_PROVIDER",
        phaselock_module._DARWIN_XATTR_PROVIDER_UNSET,
    )

    assert phaselock_module._darwin_xattr_provider() is None


def test_darwin_xattr_provider_caches_initialization_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ctypes

    monkeypatch.setattr(phaselock_module.sys, "platform", "darwin")
    monkeypatch.setattr(
        phaselock_module,
        "_DARWIN_XATTR_PROVIDER",
        phaselock_module._DARWIN_XATTR_PROVIDER_UNSET,
    )
    monkeypatch.setattr(
        ctypes,
        "CDLL",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("no libc")),
    )

    assert phaselock_module._darwin_xattr_provider() is None
    assert phaselock_module._DARWIN_XATTR_PROVIDER is None
    assert phaselock_module._darwin_xattr_provider() is None


class _FakeCFunction:
    def __init__(self, handler: Callable[..., int]) -> None:
        self._handler = handler
        self.argtypes: list[object] | None = None
        self.restype: object | None = None

    def __call__(self, *args: object) -> int:
        return self._handler(*args)


class _FakeLibc:
    def __init__(
        self,
        getxattr: _FakeCFunction,
        setxattr: _FakeCFunction,
    ) -> None:
        self.getxattr = getxattr
        self.setxattr = setxattr


def _install_fake_darwin_libc(
    monkeypatch: pytest.MonkeyPatch,
    getxattr_handler: Callable[..., int],
    setxattr_handler: Callable[..., int] | None = None,
) -> tuple[_FakeLibc, list[tuple[str | None, bool]], list[str]]:
    import ctypes
    import ctypes.util

    def default_setxattr(*args: object) -> int:
        return 0

    fake_libc = _FakeLibc(
        _FakeCFunction(getxattr_handler),
        _FakeCFunction(setxattr_handler or default_setxattr),
    )
    cdll_calls: list[tuple[str | None, bool]] = []
    find_library_calls: list[str] = []

    def find_library(name: str) -> str | None:
        find_library_calls.append(name)
        return "/fake/libc.dylib"

    def cdll(path: str | None, *, use_errno: bool = False) -> _FakeLibc:
        cdll_calls.append((path, use_errno))
        return fake_libc

    monkeypatch.setattr(phaselock_module.sys, "platform", "darwin")
    monkeypatch.setattr(
        phaselock_module,
        "_DARWIN_XATTR_PROVIDER",
        phaselock_module._DARWIN_XATTR_PROVIDER_UNSET,
    )
    monkeypatch.setattr(ctypes.util, "find_library", find_library)
    monkeypatch.setattr(ctypes, "CDLL", cdll)
    return fake_libc, cdll_calls, find_library_calls


def test_darwin_xattr_provider_configures_libc_signatures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ctypes

    fake_libc, cdll_calls, find_library_calls = _install_fake_darwin_libc(
        monkeypatch,
        lambda *args: 0,
    )

    provider = phaselock_module._darwin_xattr_provider()

    assert provider is not None
    assert find_library_calls == ["c"]
    assert cdll_calls == [("/fake/libc.dylib", True)]
    assert fake_libc.getxattr.argtypes == [
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_uint32,
        ctypes.c_int,
    ]
    assert fake_libc.getxattr.restype is ctypes.c_ssize_t
    assert fake_libc.setxattr.argtypes == [
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_uint32,
        ctypes.c_int,
    ]
    assert fake_libc.setxattr.restype is ctypes.c_int


def test_darwin_xattr_provider_gets_and_sets_values_through_libc(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ctypes

    target = tmp_path / "broker.db"
    value = b"done"
    get_calls: list[tuple[bytes, bytes, object | None, int, int, int]] = []
    set_calls: list[tuple[bytes, bytes, bytes, int, int, int]] = []

    def getxattr(
        path: bytes,
        key: bytes,
        buffer: object | None,
        size: int,
        position: int,
        options: int,
    ) -> int:
        get_calls.append((path, key, buffer, size, position, options))
        assert path == os.fsencode(target)
        assert key == b"user.phaselock.phase"
        if buffer is None:
            return len(value)
        ctypes.memmove(cast(Any, buffer), value, len(value))
        return len(value)

    def setxattr(
        path: bytes,
        key: bytes,
        buffer: object,
        size: int,
        position: int,
        options: int,
    ) -> int:
        set_calls.append(
            (
                path,
                key,
                ctypes.string_at(cast(Any, buffer), size),
                size,
                position,
                options,
            )
        )
        return 0

    _install_fake_darwin_libc(monkeypatch, getxattr, setxattr)
    provider = phaselock_module._darwin_xattr_provider()

    assert provider is not None
    assert provider.get_value(target, "user.phaselock.phase") == value
    provider.set_value(target, "user.phaselock.phase", cast(Any, bytearray(b"1")))
    assert [(call[3], call[4], call[5]) for call in get_calls] == [
        (0, 0, 0),
        (len(value), 0, 0),
    ]
    assert set_calls == [
        (
            os.fsencode(target),
            b"user.phaselock.phase",
            b"1",
            1,
            0,
            0,
        )
    ]


def test_darwin_xattr_provider_returns_empty_value_without_second_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "broker.db"
    calls: list[tuple[bytes, bytes, object | None, int, int, int]] = []

    def getxattr(
        path: bytes,
        key: bytes,
        buffer: object | None,
        size: int,
        position: int,
        options: int,
    ) -> int:
        calls.append((path, key, buffer, size, position, options))
        return 0

    _install_fake_darwin_libc(monkeypatch, getxattr)
    provider = phaselock_module._darwin_xattr_provider()

    assert provider is not None
    assert provider.get_value(target, "user.phaselock.empty") == b""
    assert calls == [(os.fsencode(target), b"user.phaselock.empty", None, 0, 0, 0)]


def test_darwin_xattr_provider_retries_get_when_value_grows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ctypes

    target = tmp_path / "broker.db"
    value = b"grown"
    calls: list[tuple[object | None, int]] = []

    def getxattr(
        path: bytes,
        key: bytes,
        buffer: object | None,
        size: int,
        position: int,
        options: int,
    ) -> int:
        calls.append((buffer, size))
        if len(calls) == 1:
            return 3
        if len(calls) == 2:
            ctypes.set_errno(getattr(errno, "ERANGE", 34))
            return -1
        if len(calls) == 3:
            return len(value)
        assert buffer is not None
        ctypes.memmove(cast(Any, buffer), value, len(value))
        return len(value)

    _install_fake_darwin_libc(monkeypatch, getxattr)
    provider = phaselock_module._darwin_xattr_provider()

    assert provider is not None
    assert provider.get_value(target, "user.phaselock.phase") == value
    assert [(buffer is None, size) for buffer, size in calls] == [
        (True, 0),
        (False, 3),
        (True, 0),
        (False, len(value)),
    ]


def test_darwin_xattr_provider_raises_when_read_fails_without_erange(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ctypes

    target = tmp_path / "broker.db"
    calls = 0

    def getxattr(
        path: bytes,
        key: bytes,
        buffer: object | None,
        size: int,
        position: int,
        options: int,
    ) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            return 4
        ctypes.set_errno(errno.EIO)
        return -1

    _install_fake_darwin_libc(monkeypatch, getxattr)
    provider = phaselock_module._darwin_xattr_provider()

    assert provider is not None
    with pytest.raises(OSError) as error:
        provider.get_value(target, "user.phaselock.phase")
    assert error.value.errno == errno.EIO
    assert error.value.filename == os.fspath(target)


def test_darwin_xattr_provider_raises_when_retry_size_probe_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ctypes

    target = tmp_path / "broker.db"
    calls = 0

    def getxattr(
        path: bytes,
        key: bytes,
        buffer: object | None,
        size: int,
        position: int,
        options: int,
    ) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            return 3
        if calls == 2:
            ctypes.set_errno(getattr(errno, "ERANGE", 34))
            return -1
        ctypes.set_errno(errno.EIO)
        return -1

    _install_fake_darwin_libc(monkeypatch, getxattr)
    provider = phaselock_module._darwin_xattr_provider()

    assert provider is not None
    with pytest.raises(OSError) as error:
        provider.get_value(target, "user.phaselock.phase")
    assert error.value.errno == errno.EIO
    assert error.value.filename == os.fspath(target)


def test_darwin_xattr_provider_raises_oserror_from_errno(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ctypes

    target = tmp_path / "broker.db"

    def getxattr(
        path: bytes,
        key: bytes,
        buffer: object | None,
        size: int,
        position: int,
        options: int,
    ) -> int:
        ctypes.set_errno(errno.EIO)
        return -1

    def setxattr(
        path: bytes,
        key: bytes,
        buffer: object,
        size: int,
        position: int,
        options: int,
    ) -> int:
        ctypes.set_errno(errno.EACCES)
        return -1

    _install_fake_darwin_libc(monkeypatch, getxattr, setxattr)
    provider = phaselock_module._darwin_xattr_provider()

    assert provider is not None
    with pytest.raises(OSError) as get_error:
        provider.get_value(target, "user.phaselock.phase")
    assert get_error.value.errno == errno.EIO
    assert get_error.value.filename == os.fspath(target)

    with pytest.raises(OSError) as set_error:
        provider.set_value(target, "user.phaselock.phase", b"1")
    assert set_error.value.errno == errno.EACCES
    assert set_error.value.filename == os.fspath(target)


def test_process_lock_key_falls_back_when_resolve_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "broker.lock"

    def fail_resolve(self: Path, *, strict: bool = False) -> Path:
        raise RuntimeError("cannot resolve")

    monkeypatch.setattr(Path, "resolve", fail_resolve)

    assert phaselock_module._process_lock_key(path) == path.absolute()


def test_darwin_ctypes_xattrs_are_used_when_stdlib_xattrs_are_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if sys.platform != "darwin":
        pytest.skip("Darwin-only ctypes xattr provider")

    import simplebroker._phaselock as phaselock_module

    monkeypatch.setattr(phaselock_module.os, "getxattr", None, raising=False)
    monkeypatch.setattr(phaselock_module.os, "setxattr", None, raising=False)
    target = tmp_path / "broker.db"
    target.touch()
    if not _default_xattrs_supported(target):
        pytest.skip("Darwin libc xattrs are not supported on this filesystem")

    calls: list[str] = []
    service = PhaseLockService(target)
    phases = (Phase("connection-v1", lambda: calls.append("connection")),)

    first = service.run_phases(phases)
    second = service.run_phases(phases)

    assert first.completed == ("connection-v1",)
    assert first.xattrs_available is True
    assert first.status_paths == ()
    assert second.completed == ()
    assert second.skipped == ("connection-v1",)
    assert calls == ["connection"]
    assert service.lock_path.exists()
    assert not service.status_base_path.exists()


def test_missing_stdlib_and_darwin_xattrs_falls_back_to_status_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import simplebroker._phaselock as phaselock_module

    monkeypatch.setattr(phaselock_module.os, "getxattr", None, raising=False)
    monkeypatch.setattr(phaselock_module.os, "setxattr", None, raising=False)
    monkeypatch.setattr(phaselock_module, "_darwin_xattr_provider", lambda: None)
    target = tmp_path / "broker.db"
    target.touch()
    calls: list[str] = []
    service = PhaseLockService(target)

    result = service.run_phases(
        (Phase("connection-v1", lambda: calls.append("connection")),)
    )

    assert result.completed == ("connection-v1",)
    assert result.xattrs_available is False
    assert result.status_paths == (service.status_base_path,)
    assert calls == ["connection"]
    assert _read_status_file(service.status_base_path) == ["connection-v1"]


def test_env_can_force_status_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import simplebroker._phaselock as phaselock_module

    monkeypatch.setenv(phaselock_module.PHASELOCK_ENABLE_XATTRS, "0")
    target = tmp_path / "broker.db"
    target.touch()
    calls: list[str] = []
    service = PhaseLockService(target)

    result = service.run_phases(
        (Phase("connection-v1", lambda: calls.append("connection")),)
    )

    assert result.completed == ("connection-v1",)
    assert result.xattrs_available is False
    assert result.status_paths == (service.status_base_path,)
    assert calls == ["connection"]
    assert _read_status_file(service.status_base_path) == ["connection-v1"]


def test_explicit_xattr_setting_overrides_env_fallback_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import simplebroker._phaselock as phaselock_module

    monkeypatch.setenv(phaselock_module.PHASELOCK_ENABLE_XATTRS, "0")
    target = tmp_path / "broker.db"
    target.touch()
    if not _real_xattrs_supported(target):
        pytest.skip("real xattrs are not supported on this runtime/filesystem")

    calls: list[str] = []
    service = PhaseLockService(target, use_xattrs=True)

    result = service.run_phases(
        (Phase("connection-v1", lambda: calls.append("connection")),)
    )

    assert result.completed == ("connection-v1",)
    assert result.xattrs_available is True
    assert result.status_paths == ()
    assert calls == ["connection"]
    assert not service.status_base_path.exists()


def test_default_namespace_is_generic(tmp_path: Path) -> None:
    service = PhaseLockService(tmp_path / "resource.db")

    assert service.attr_key("connection-v1") == "user.phaselock.connection-v1"


def test_empty_namespace_uses_raw_attr_name(tmp_path: Path) -> None:
    service = PhaseLockService(tmp_path / "broker.db", namespace=".")

    assert service.attr_key("connection-v1") == "connection-v1"
    assert service.attr_key("connection-v1", attr_name="custom") == "custom"


def test_has_phase_uses_status_file_when_xattrs_are_disabled(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(target, use_xattrs=False)
    _write_status_file(service.status_base_path, ["connection-v1"])

    assert service.has_phase("connection-v1")
    assert not service.has_phase("schema-v4")


def test_empty_phase_list_in_status_mode_returns_no_status_paths(
    tmp_path: Path,
) -> None:
    service = PhaseLockService(
        tmp_path / "broker.db",
        use_xattrs=False,
        strict_marker_locking=True,
    )

    result = service.run_phases(())

    assert result.completed == ()
    assert result.skipped == ()
    assert result.status_paths == ()
    assert not service.lock_path.exists()
    assert not service.status_base_path.exists()


@contextmanager
def _subprocess_holding_phase_lock(target: Path) -> Iterator[subprocess.Popen[str]]:
    ready = target.with_suffix(".ready")
    release = target.with_suffix(".release")
    script = textwrap.dedent(
        """
        import sys
        import time
        from pathlib import Path

        from simplebroker._phaselock import PhaseLockService

        target = Path(sys.argv[1])
        ready = Path(sys.argv[2])
        release = Path(sys.argv[3])

        service = PhaseLockService(target, timeout=5.0, retry_delay=0.01)
        with service.locked():
            ready.touch()
            while not release.exists():
                time.sleep(0.01)
        """
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", script, str(target), str(ready), str(release)],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_file(ready, proc=proc)
        yield proc
    finally:
        release.touch()
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5.0)
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            raise AssertionError(
                f"lock holder failed with {proc.returncode}\nstdout={stdout}\nstderr={stderr}"
            )


def test_real_xattr_runtime_marks_and_skips_completed_phases(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    if not _default_xattrs_supported(target):
        pytest.skip("real xattrs are not supported on this runtime/filesystem")

    calls: list[str] = []
    service = PhaseLockService(target)
    phases = (
        Phase("connection-v1", lambda: calls.append("connection")),
        Phase("schema-v4", lambda: calls.append("schema")),
    )

    first = service.run_phases(phases)
    second = service.run_phases(phases)

    assert first.completed == ("connection-v1", "schema-v4")
    assert first.skipped == ()
    assert first.xattrs_available is True
    assert second.completed == ()
    assert second.skipped == ("connection-v1", "schema-v4")
    assert calls == ["connection", "schema"]
    assert service.lock_path.exists()


def test_real_xattr_runtime_resumes_from_last_marked_phase(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    if not _default_xattrs_supported(target):
        pytest.skip("real xattrs are not supported on this runtime/filesystem")

    calls: list[str] = []
    service = PhaseLockService(target)

    def fail_schema() -> None:
        calls.append("schema-failed")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        service.run_phases(
            (
                Phase("connection-v1", lambda: calls.append("connection")),
                Phase("schema-v4", fail_schema),
            )
        )

    assert service.has_phase("connection-v1")
    assert not service.has_phase("schema-v4")
    assert service.lock_path.exists()

    result = service.run_phases(
        (
            Phase("connection-v1", lambda: calls.append("connection-again")),
            Phase("schema-v4", lambda: calls.append("schema")),
        )
    )

    assert result.completed == ("schema-v4",)
    assert result.skipped == ("connection-v1",)
    assert calls == ["connection", "schema-failed", "schema"]
    assert service.lock_path.exists()


def test_failure_while_lock_held_leaves_partial_xattrs_and_blocks_contenders(
    tmp_path: Path,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    if not _default_xattrs_supported(target):
        pytest.skip("real xattrs are not supported on this runtime/filesystem")

    ready = tmp_path / "phase2-ready"
    release = tmp_path / "release-phase2"
    script = textwrap.dedent(
        """
        import sys
        import time
        from pathlib import Path

        from simplebroker._phaselock import Phase, PhaseLockService

        target = Path(sys.argv[1])
        ready = Path(sys.argv[2])
        release = Path(sys.argv[3])

        service = PhaseLockService(target, timeout=5.0, retry_delay=0.01)

        def connection() -> None:
            pass

        def fail_schema_after_signal() -> None:
            ready.touch()
            while not release.exists():
                time.sleep(0.01)
            raise RuntimeError("schema setup failed")

        service.run_phases(
            (
                Phase("connection-v1", connection),
                Phase("schema-v4", fail_schema_after_signal),
            )
        )
        """
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", script, str(target), str(ready), str(release)],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    service = PhaseLockService(target, timeout=0.15, retry_delay=0.01)
    try:
        _wait_for_file(ready, proc=proc)

        assert service.has_phase("connection-v1")
        assert not service.has_phase("schema-v4")
        with pytest.raises(PhaseLockTimeout):
            service.run_phases(
                (
                    Phase("connection-v1", lambda: None),
                    Phase("schema-v4", lambda: None),
                )
            )
    finally:
        release.touch()

    stdout, stderr = proc.communicate(timeout=5.0)
    assert proc.returncode != 0, stdout
    assert "schema setup failed" in stderr

    calls: list[str] = []
    result = service.run_phases(
        (
            Phase("connection-v1", lambda: calls.append("connection-again")),
            Phase("schema-v4", lambda: calls.append("schema")),
        )
    )

    assert result.completed == ("schema-v4",)
    assert result.skipped == ("connection-v1",)
    assert calls == ["schema"]
    assert service.lock_path.exists()


def test_no_xattr_fallback_writes_single_status_file_and_no_done_files(
    tmp_path: Path,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()

    calls: list[str] = []
    service = PhaseLockService(target, use_xattrs=False)
    phases = (
        Phase("connection-v1", lambda: calls.append("connection")),
        Phase("schema-v4", lambda: calls.append("schema")),
    )

    first = service.run_phases(phases)
    second = service.run_phases(phases)

    assert first.completed == ("connection-v1", "schema-v4")
    assert first.xattrs_available is False
    expected_status_paths = (tmp_path / "broker.db.status",)
    assert first.status_paths == expected_status_paths
    assert second.completed == ()
    assert second.skipped == ("connection-v1", "schema-v4")
    assert second.status_paths == expected_status_paths
    assert calls == ["connection", "schema"]
    assert service.lock_path == tmp_path / "broker.db.lock"
    assert service.lock_path.exists()
    assert _read_status_file(service.status_base_path) == [
        "connection-v1",
        "schema-v4",
    ]
    assert sorted(tmp_path.glob("broker.db.status.*")) == []
    assert not list(tmp_path.glob("*.done"))


def test_no_xattr_action_failure_keeps_single_lock_file(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()

    service = PhaseLockService(target, use_xattrs=False)

    def fail() -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        service.run_phases((Phase("connection-v1", fail),))

    assert service.lock_path.exists()
    assert list(tmp_path.glob("*.lock")) == [service.lock_path]
    assert not service.status_base_path.exists()
    assert not list(tmp_path.glob("*.status.*"))
    assert not list(tmp_path.glob("*.done"))


def test_xattr_mark_failure_falls_back_to_status_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    missing_errno = 8765

    class FailingSetXattrProvider:
        def get_value(self, path: Path, key: str) -> bytes:
            raise OSError(missing_errno, "missing")

        def set_value(self, path: Path, key: str, value: bytes) -> None:
            raise OSError(errno.EPERM, "unsupported")

    monkeypatch.setattr(phaselock_module, "_MISSING_XATTR_ERRNOS", {missing_errno})
    monkeypatch.setattr(
        phaselock_module,
        "_xattr_provider",
        lambda: FailingSetXattrProvider(),
    )
    service = PhaseLockService(target, use_xattrs=True)
    calls: list[str] = []

    result = service.run_phases(
        (Phase("connection-v1", lambda: calls.append("connection")),)
    )

    assert result.completed == ("connection-v1",)
    assert result.skipped == ()
    assert result.xattrs_available is False
    assert result.status_paths == (service.status_base_path,)
    assert calls == ["connection"]
    assert _read_status_file(service.status_base_path) == ["connection-v1"]


def test_no_xattr_status_marker_resumes_from_last_completed_phase(
    tmp_path: Path,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    calls: list[str] = []
    service = PhaseLockService(target, use_xattrs=False)

    def fail_schema() -> None:
        calls.append("schema-failed")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        service.run_phases(
            (
                Phase("connection-v1", lambda: calls.append("connection")),
                Phase("schema-v4", fail_schema),
                Phase("optimization-v1", lambda: calls.append("optimization")),
            )
        )

    assert calls == ["connection", "schema-failed"]
    assert _read_status_file(service.status_base_path) == ["connection-v1"]

    result = service.run_phases(
        (
            Phase("connection-v1", lambda: calls.append("connection-again")),
            Phase("schema-v4", lambda: calls.append("schema")),
            Phase("optimization-v1", lambda: calls.append("optimization")),
        )
    )

    assert result.completed == ("schema-v4", "optimization-v1")
    assert result.skipped == ("connection-v1",)
    assert calls == ["connection", "schema-failed", "schema", "optimization"]
    assert _read_status_file(service.status_base_path) == [
        "connection-v1",
        "schema-v4",
        "optimization-v1",
    ]
    assert service.lock_path.exists()


def test_no_xattr_failure_while_lock_held_leaves_partial_status_marker(
    tmp_path: Path,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    ready = tmp_path / "fallback-phase2-ready"
    release = tmp_path / "fallback-release-phase2"
    script = textwrap.dedent(
        """
        import sys
        import time
        from pathlib import Path

        from simplebroker._phaselock import Phase, PhaseLockService

        target = Path(sys.argv[1])
        ready = Path(sys.argv[2])
        release = Path(sys.argv[3])

        service = PhaseLockService(
            target,
            timeout=5.0,
            retry_delay=0.01,
            use_xattrs=False,
        )

        def connection() -> None:
            pass

        def fail_schema_after_signal() -> None:
            ready.touch()
            while not release.exists():
                time.sleep(0.01)
            raise RuntimeError("schema setup failed")

        service.run_phases(
            (
                Phase("connection-v1", connection),
                Phase("schema-v4", fail_schema_after_signal),
            )
        )
        """
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", script, str(target), str(ready), str(release)],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    service = PhaseLockService(
        target,
        timeout=0.15,
        retry_delay=0.01,
        use_xattrs=False,
    )
    try:
        _wait_for_file(ready, proc=proc)

        assert _read_status_file(service.status_base_path) == ["connection-v1"]
        with pytest.raises(PhaseLockTimeout):
            service.run_phases(
                (
                    Phase("connection-v1", lambda: None),
                    Phase("schema-v4", lambda: None),
                )
            )
    finally:
        release.touch()

    stdout, stderr = proc.communicate(timeout=5.0)
    assert proc.returncode != 0, stdout
    assert "schema setup failed" in stderr

    calls: list[str] = []
    result = service.run_phases(
        (
            Phase("connection-v1", lambda: calls.append("connection-again")),
            Phase("schema-v4", lambda: calls.append("schema")),
        )
    )

    assert result.completed == ("schema-v4",)
    assert result.skipped == ("connection-v1",)
    assert calls == ["schema"]
    assert _read_status_file(service.status_base_path) == [
        "connection-v1",
        "schema-v4",
    ]
    assert service.lock_path.exists()


def test_no_xattr_status_file_keeps_independent_completed_phases(
    tmp_path: Path,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(target, use_xattrs=False)
    _write_status_file(
        service.status_base_path,
        ["connection-v1", "schema-v4"],
    )
    calls: list[str] = []

    result = service.run_phases(
        (
            Phase("connection-v1", lambda: calls.append("connection")),
            Phase("schema-v4", lambda: calls.append("schema")),
            Phase("optimization-v1", lambda: calls.append("optimization")),
        )
    )

    assert result.completed == ("optimization-v1",)
    assert result.skipped == ("connection-v1", "schema-v4")
    assert calls == ["optimization"]
    assert _read_status_file(service.status_base_path) == [
        "connection-v1",
        "schema-v4",
        "optimization-v1",
    ]


def test_no_xattr_sparse_status_does_not_imply_missing_phases(
    tmp_path: Path,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(target, use_xattrs=False)
    _write_status_file(service.status_base_path, ["connection-v1", "optimization-v1"])
    calls: list[str] = []

    result = service.run_phases(
        (
            Phase("connection-v1", lambda: calls.append("connection")),
            Phase("schema-v4", lambda: calls.append("schema")),
            Phase("optimization-v1", lambda: calls.append("optimization")),
        )
    )

    assert result.completed == ("schema-v4",)
    assert result.skipped == ("connection-v1", "optimization-v1")
    assert calls == ["schema"]
    assert _read_status_file(service.status_base_path) == [
        "connection-v1",
        "optimization-v1",
        "schema-v4",
    ]


def test_no_xattr_malformed_status_is_not_trusted(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(target, use_xattrs=False)
    service.status_base_path.write_bytes(b"connection-v1\nbad\x00phase\n")
    calls: list[str] = []

    result = service.run_phases((Phase("connection-v1", lambda: calls.append("ran")),))

    assert result.completed == ("connection-v1",)
    assert result.skipped == ()
    assert calls == ["ran"]
    assert _read_status_file(service.status_base_path) == ["connection-v1"]


def test_status_reader_deduplicates_and_ignores_blank_lines(tmp_path: Path) -> None:
    service = PhaseLockService(tmp_path / "broker.db", use_xattrs=False)
    service.status_base_path.write_text(
        "connection-v1\n\nschema-v4\nconnection-v1\n",
        encoding="utf-8",
    )

    entries, diagnostic = service._read_status_entries()

    assert entries == ["connection-v1", "schema-v4"]
    assert diagnostic is None


def test_status_reader_reports_oversized_and_invalid_utf8_status_files(
    tmp_path: Path,
) -> None:
    service = PhaseLockService(tmp_path / "broker.db", use_xattrs=False)
    service.status_base_path.write_bytes(
        b"a" * (phaselock_module._MAX_STATUS_BYTES + 1)
    )

    entries, diagnostic = service._read_status_entries()

    assert entries == []
    assert diagnostic == (
        "status_read_error=status file too large: "
        f"{phaselock_module._MAX_STATUS_BYTES + 1}"
    )

    service.status_base_path.write_bytes(b"\xff")

    entries, diagnostic = service._read_status_entries()
    phase_diagnostics = service._phase_diagnostics(
        (Phase("connection-v1", lambda: None),)
    )

    assert entries == []
    assert diagnostic is not None
    assert diagnostic.startswith("status_parse_error=")
    assert "status_parse_error=" in phase_diagnostics


def test_status_reader_reports_stat_and_read_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = PhaseLockService(tmp_path / "broker.db", use_xattrs=False)
    service.status_base_path.write_text("connection-v1\n", encoding="utf-8")
    real_stat: Callable[..., os.stat_result] = Path.stat
    real_read_bytes = Path.read_bytes

    def fail_stat(self: Path, *args: object, **kwargs: object) -> os.stat_result:
        if self == service.status_base_path:
            raise OSError("stat failed")
        return real_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", fail_stat)

    entries, diagnostic = service._read_status_entries()

    assert entries == []
    assert diagnostic is not None
    assert diagnostic.startswith("status_read_error=")

    monkeypatch.setattr(Path, "stat", real_stat)

    def fail_read_bytes(self: Path) -> bytes:
        if self == service.status_base_path:
            raise OSError("read failed")
        return real_read_bytes(self)

    monkeypatch.setattr(Path, "read_bytes", fail_read_bytes)

    entries, diagnostic = service._read_status_entries()

    assert entries == []
    assert diagnostic is not None
    assert diagnostic.startswith("status_read_error=")


def test_write_status_phases_deduplicates_entries(tmp_path: Path) -> None:
    service = PhaseLockService(tmp_path / "broker.db", use_xattrs=False)

    service._write_status_phases(["connection-v1", "connection-v1", "schema-v4"])

    assert _read_status_file(service.status_base_path) == [
        "connection-v1",
        "schema-v4",
    ]


def test_write_status_phases_removes_temp_file_when_fdopen_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = PhaseLockService(tmp_path / "broker.db", use_xattrs=False)

    def fail_fdopen(fd: int, mode: str) -> object:
        raise RuntimeError("fdopen failed")

    monkeypatch.setattr(phaselock_module.os, "fdopen", fail_fdopen)

    with pytest.raises(RuntimeError, match="fdopen failed"):
        service._write_status_phases(["connection-v1"])

    assert not service.status_base_path.exists()
    assert list(tmp_path.glob("broker.db.status.tmp.*")) == []


def test_discard_status_markers_removes_current_markers(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    service = PhaseLockService(target, use_xattrs=False)
    current_temp = tmp_path / "broker.db.status.tmp.1"

    service.status_base_path.touch()
    current_temp.touch()
    service.lock_path.touch()

    service.discard_status_markers()

    assert not service.status_base_path.exists()
    assert not current_temp.exists()
    assert service.lock_path.exists()


def test_no_xattr_existing_status_marker_does_not_bypass_held_lock(
    tmp_path: Path,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(
        target,
        timeout=1.0,
        retry_delay=0.01,
        use_xattrs=False,
        strict_marker_locking=True,
    )
    calls: list[str] = []
    result_holder: dict[str, object] = {}
    errors: list[BaseException] = []
    started = threading.Event()
    done = threading.Event()
    release = target.with_suffix(".release")

    _write_status_file(service.status_base_path, ["connection-v1"])

    def run_waiter() -> None:
        started.set()
        try:
            result_holder["result"] = service.run_phases(
                (Phase("connection-v1", lambda: calls.append("ran")),)
            )
        except BaseException as exc:
            errors.append(exc)
        finally:
            done.set()

    with _subprocess_holding_phase_lock(target):
        waiter = threading.Thread(target=run_waiter)
        waiter.start()
        assert started.wait(timeout=1.0)
        assert not done.wait(timeout=0.2)
        release.touch()
        assert done.wait(timeout=1.0)
        waiter.join(timeout=1.0)

    assert not errors
    result = result_holder["result"]
    assert isinstance(result, PhaseRunResult)
    assert result.completed == ()
    assert result.skipped == ("connection-v1",)
    assert calls == []
    assert not waiter.is_alive()


def test_strict_lock_wait_can_be_cancelled(tmp_path: Path) -> None:
    """Callers can stop waiting without weakening the marker barrier."""
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(
        target,
        timeout=5.0,
        retry_delay=0.01,
        use_xattrs=False,
        strict_marker_locking=True,
    )
    lock_held = threading.Event()
    release_lock = threading.Event()
    stop_waiting = threading.Event()
    done = threading.Event()
    errors: list[BaseException] = []
    calls: list[str] = []

    def hold_lock() -> None:
        with service.locked():
            lock_held.set()
            release_lock.wait(timeout=2.0)

    def wait_for_phase() -> None:
        try:
            service.run_phases(
                (Phase("connection-v1", lambda: calls.append("ran")),),
                should_cancel=stop_waiting.is_set,
            )
        except BaseException as exc:
            errors.append(exc)
        finally:
            done.set()

    holder = threading.Thread(target=hold_lock)
    waiter = threading.Thread(target=wait_for_phase)
    try:
        holder.start()
        assert lock_held.wait(timeout=1.0)
        waiter.start()
        assert not done.wait(timeout=0.1)

        stop_waiting.set()

        assert done.wait(timeout=1.0)
    finally:
        release_lock.set()
        holder.join(timeout=1.0)
        waiter.join(timeout=1.0)

    assert len(errors) == 1
    assert isinstance(errors[0], PhaseLockCancelled)
    assert "cancel" in str(errors[0]).lower()
    assert calls == []
    assert not holder.is_alive()
    assert not waiter.is_alive()


def test_no_xattr_waiter_does_not_skip_when_phase_marked_while_lock_is_held(
    tmp_path: Path,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(
        target,
        timeout=1.0,
        retry_delay=0.01,
        use_xattrs=False,
        strict_marker_locking=True,
    )
    calls: list[str] = []
    result_holder: dict[str, object] = {}
    errors: list[BaseException] = []
    started = threading.Event()
    done = threading.Event()
    release = target.with_suffix(".release")

    def run_waiter() -> None:
        started.set()
        try:
            result_holder["result"] = service.run_phases(
                (Phase("connection-v1", lambda: calls.append("ran")),)
            )
        except BaseException as exc:
            errors.append(exc)
        finally:
            done.set()

    def mark_phase_after_waiter_blocks() -> None:
        assert started.wait(timeout=1.0)
        _write_status_file(service.status_base_path, ["connection-v1"])

    with _subprocess_holding_phase_lock(target):
        waiter = threading.Thread(target=run_waiter)
        marker = threading.Thread(target=mark_phase_after_waiter_blocks)
        waiter.start()
        marker.start()
        marker.join(timeout=1.0)
        assert not marker.is_alive()
        assert not done.wait(timeout=0.2)
        release.touch()
        assert done.wait(timeout=1.0)
        waiter.join(timeout=1.0)

    assert not errors
    result = result_holder["result"]
    assert isinstance(result, PhaseRunResult)
    assert result.completed == ()
    assert result.skipped == ("connection-v1",)
    assert calls == []
    assert not waiter.is_alive()


def test_no_xattr_non_strict_waiter_skips_when_phase_marked_while_lock_is_held(
    tmp_path: Path,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(
        target,
        timeout=1.0,
        retry_delay=0.01,
        use_xattrs=False,
        strict_marker_locking=False,
    )
    calls: list[str] = []

    def mark_phase_after_waiter_blocks() -> None:
        time.sleep(0.05)
        _write_status_file(service.status_base_path, ["connection-v1"])

    with _subprocess_holding_phase_lock(target):
        marker = threading.Thread(target=mark_phase_after_waiter_blocks)
        marker.start()
        result = service.run_phases(
            (Phase("connection-v1", lambda: calls.append("ran")),)
        )
        marker.join(timeout=1.0)

    assert result.completed == ()
    assert result.skipped == ("connection-v1",)
    assert calls == []
    assert not marker.is_alive()


def test_process_local_lock_serializes_threads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    timeout = scale_timeout_for_ci(2.0)
    first_service = PhaseLockService(
        target,
        timeout=timeout,
        retry_delay=0.01,
        use_xattrs=False,
    )
    second_service = PhaseLockService(
        target,
        timeout=timeout,
        retry_delay=0.01,
        use_xattrs=False,
    )
    first_entered = threading.Event()
    release_first = threading.Event()
    second_waiting = threading.Event()
    second_done = threading.Event()
    calls: list[str] = []
    results: dict[str, PhaseRunResult] = {}
    errors: list[BaseException] = []

    real_acquire_process_lock = phaselock_module._AdvisoryLock._acquire_process_lock

    def observe_process_lock(self: Any, *args: Any, **kwargs: Any) -> bool:
        if threading.current_thread().name == "second-phase-runner":
            second_waiting.set()
        return real_acquire_process_lock(self, *args, **kwargs)

    monkeypatch.setattr(
        phaselock_module._AdvisoryLock,
        "_acquire_process_lock",
        observe_process_lock,
    )

    def first_action() -> None:
        calls.append("first")
        first_entered.set()
        assert release_first.wait(timeout=timeout)

    def run_first() -> None:
        try:
            first_service.run_phases((Phase("connection-v1", first_action),))
        except BaseException as exc:
            errors.append(exc)

    def run_second() -> None:
        try:
            results["second"] = second_service.run_phases(
                (Phase("connection-v1", lambda: calls.append("second")),)
            )
        except BaseException as exc:
            errors.append(exc)
        finally:
            second_done.set()

    first = threading.Thread(target=run_first)
    second = threading.Thread(target=run_second, name="second-phase-runner")
    try:
        first.start()
        assert first_entered.wait(timeout=timeout)

        second.start()
        assert second_waiting.wait(timeout=timeout)
        assert not second_done.is_set()
    finally:
        release_first.set()
        first.join(timeout=timeout)
        if second.ident is not None:
            second.join(timeout=timeout)

    assert not errors
    assert not first.is_alive()
    assert not second.is_alive()
    assert calls == ["first"]
    assert results["second"].completed == ()
    assert results["second"].skipped == ("connection-v1",)


def test_lock_timeout_when_another_process_holds_lock(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(target, timeout=0.15, retry_delay=0.01)
    calls: list[str] = []

    with _subprocess_holding_phase_lock(target):
        with pytest.raises(PhaseLockTimeout) as exc_info:
            service.run_phases((Phase("connection-v1", lambda: calls.append("ran")),))

    assert calls == []
    assert service.lock_path.exists()
    message = str(exc_info.value)
    assert "timeout=0.150s" in message
    assert "elapsed=" in message
    assert f"lock_path={service.lock_path}" in message
    assert "lock_size=" in message
    assert "target=" in message
    assert "missing=['connection-v1']" in message
    assert exc_info.value.cause is None


def test_lock_context_releases_after_exception(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(target, timeout=0.5, retry_delay=0.01)

    with pytest.raises(RuntimeError, match="boom"):
        with service.locked():
            raise RuntimeError("boom")

    with service.locked():
        assert service.lock_path.exists()


def test_advisory_file_lock_context_manager_releases_after_exception(
    tmp_path: Path,
) -> None:
    lock_path = tmp_path / "resource.lock"
    lock = AdvisoryFileLock(lock_path, timeout=0.5, retry_delay=0.01)

    with pytest.raises(RuntimeError, match="boom"):
        with lock:
            assert lock.locked
            raise RuntimeError("boom")

    assert not lock.locked
    with AdvisoryFileLock(lock_path, timeout=0.5, retry_delay=0.01) as second_lock:
        assert second_lock.locked


def test_advisory_file_lock_rejects_same_instance_reentrant_context(
    tmp_path: Path,
) -> None:
    lock_path = tmp_path / "resource.lock"
    lock = AdvisoryFileLock(lock_path, timeout=0.5, retry_delay=0.01)

    with lock:
        with pytest.raises(RuntimeError, match="does not support re-entrant"):
            with lock:
                pass
        assert lock.locked

    assert not lock.locked

    result: list[bool] = []
    errors: list[BaseException] = []

    def acquire_from_other_thread() -> None:
        second_lock = AdvisoryFileLock(lock_path, timeout=0.5, retry_delay=0.01)
        try:
            if second_lock.acquire():
                result.append(True)
                second_lock.release()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=acquire_from_other_thread)
    thread.start()
    thread.join(timeout=1.0)

    assert not thread.is_alive()
    assert errors == []
    assert result == [True]


def test_advisory_lock_unavailable_without_lock_primitives(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = tmp_path / "broker.lock"
    lock = phaselock_module._AdvisoryLock(lock_path, timeout=0.01, retry_delay=0.0)

    monkeypatch.setattr(phaselock_module, "fcntl", None)
    monkeypatch.setattr(phaselock_module, "msvcrt", None)

    with pytest.raises(PhaseLockUnavailable):
        lock.acquire()

    with lock_path.open("a+b") as lock_file:
        with pytest.raises(PhaseLockUnavailable):
            lock._try_lock(lock_file)


def test_advisory_lock_open_errors_timeout_with_diagnostics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock = phaselock_module._AdvisoryLock(
        tmp_path / "broker.lock",
        timeout=0.0,
        retry_delay=0.0,
    )

    def fail_open(self: Path, *args: object, **kwargs: object) -> object:
        raise OSError(errno.EACCES, "permission denied")

    monkeypatch.setattr(Path, "open", fail_open)

    with pytest.raises(PhaseLockTimeout) as exc_info:
        lock.acquire(diagnostics=lambda: "diagnostic=present")

    message = str(exc_info.value)
    assert "last_error=" in message
    assert "diagnostic=present" in message


def test_windows_permission_denied_open_is_classified_as_structural_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Windows lock-file open failure must not look like contention."""
    lock = phaselock_module._AdvisoryLock(
        tmp_path / "broker.lock",
        timeout=0.0,
        retry_delay=0.0,
    )
    denied = PermissionError(errno.EACCES, "permission denied")

    def fail_open(self: Path, *args: object, **kwargs: object) -> object:
        raise denied

    # Exercise the Windows availability branch even when this test runs on a
    # POSIX development host. The open fails before the msvcrt primitive is
    # called; the contract under test is the structural failure classification.
    monkeypatch.setattr(phaselock_module, "fcntl", None)
    monkeypatch.setattr(phaselock_module, "msvcrt", object())
    monkeypatch.setattr(Path, "open", fail_open)

    with pytest.raises(PhaseLockTimeout) as exc_info:
        lock.acquire()

    assert exc_info.value.cause is denied


def test_advisory_lock_stops_after_lock_attempt_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock = phaselock_module._AdvisoryLock(
        tmp_path / "broker.lock",
        timeout=1.0,
        retry_delay=0.0,
    )
    stop_calls = 0

    def stop_waiting() -> bool:
        nonlocal stop_calls
        stop_calls += 1
        return stop_calls >= 3

    def fail_try_lock(self: object, lock_file: object) -> None:
        raise BlockingIOError(errno.EAGAIN, "busy")

    monkeypatch.setattr(phaselock_module._AdvisoryLock, "_try_lock", fail_try_lock)

    assert lock.acquire(should_stop_waiting=stop_waiting) is False
    assert lock._file is None
    assert not lock._process_locked


def test_process_local_lock_timeout_includes_diagnostics(tmp_path: Path) -> None:
    lock_path = tmp_path / "broker.lock"
    first_lock = phaselock_module._AdvisoryLock(
        lock_path,
        timeout=1.0,
        retry_delay=0.01,
    )
    errors: list[BaseException] = []

    assert first_lock.acquire()

    def contend() -> None:
        contender = phaselock_module._AdvisoryLock(
            lock_path,
            timeout=0.05,
            retry_delay=0.001,
        )
        try:
            contender.acquire(diagnostics=lambda: "process_lock=busy")
        except BaseException as exc:
            errors.append(exc)

    try:
        thread = threading.Thread(target=contend)
        thread.start()
        thread.join(timeout=1.0)
    finally:
        first_lock.release()

    assert not thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], PhaseLockTimeout)
    assert "process_lock=busy" in str(errors[0])


def test_timeout_message_reports_missing_lock_stat_and_last_error(
    tmp_path: Path,
) -> None:
    lock = phaselock_module._AdvisoryLock(
        tmp_path / "missing.lock",
        timeout=1.0,
        retry_delay=0.01,
    )

    message = lock._timeout_message(
        time.monotonic(),
        last_error=OSError("open failed"),
        diagnostics=lambda: (_ for _ in ()).throw(RuntimeError("diagnostic failed")),
    )

    assert "lock_stat_error=" in message
    assert "last_error=" in message
    assert "diagnostic failed" not in message


def test_advisory_lock_uses_msvcrt_when_fcntl_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int]] = []

    class FakeMsvcrt:
        LK_NBLCK = 7
        LK_UNLCK = 8

        def locking(self, fd: int, mode: int, nbytes: int) -> None:
            calls.append((mode, nbytes))

    monkeypatch.setattr(phaselock_module, "fcntl", None)
    monkeypatch.setattr(phaselock_module, "msvcrt", FakeMsvcrt())
    lock = phaselock_module._AdvisoryLock(
        tmp_path / "broker.lock",
        timeout=0.5,
        retry_delay=0.01,
    )

    assert lock.acquire()
    lock.release()

    assert calls == [(FakeMsvcrt.LK_NBLCK, 1), (FakeMsvcrt.LK_UNLCK, 1)]


def test_advisory_lock_release_without_file_is_noop(tmp_path: Path) -> None:
    lock = phaselock_module._AdvisoryLock(
        tmp_path / "broker.lock",
        timeout=0.5,
        retry_delay=0.01,
    )

    lock.release()
    lock._release_process_lock()

    assert lock._file is None
    assert not lock._process_locked


def test_lock_file_is_prepared_for_byte_range_locking(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(target, timeout=0.5, retry_delay=0.01)

    with service.locked():
        assert service.lock_path.exists()
        assert service.lock_path.stat().st_size >= 1


@pytest.mark.skipif(os.name != "nt", reason="msvcrt is Windows-only")
def test_msvcrt_lock_blocks_second_process_on_windows(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(target, timeout=0.15, retry_delay=0.01)

    with _subprocess_holding_phase_lock(target):
        with pytest.raises(PhaseLockTimeout):
            with service.locked():
                pass


def test_empty_phase_or_attr_name_is_rejected(tmp_path: Path) -> None:
    service = PhaseLockService(tmp_path / "broker.db")

    with pytest.raises(ValueError, match="non-empty"):
        service.attr_key("")
    with pytest.raises(ValueError, match="NUL"):
        service.attr_key("bad\x00phase")


def test_xattr_get_and_set_failure_modes_update_availability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    unsupported_errno = 9876

    monkeypatch.setattr(
        phaselock_module, "_UNSUPPORTED_XATTR_ERRNOS", {unsupported_errno}
    )

    disabled = PhaseLockService(target, use_xattrs=False)
    assert disabled._get_xattr("key") is None
    assert disabled._set_xattr("key", b"value") is False
    assert disabled.xattrs_available is False

    monkeypatch.setattr(phaselock_module, "_xattr_provider", lambda: None)
    missing_provider = PhaseLockService(target, use_xattrs=True)
    assert missing_provider._get_xattr("key") is None
    assert missing_provider._set_xattr("key", b"value") is False
    assert missing_provider.xattrs_available is False

    class MissingTargetProvider:
        def get_value(self, path: Path, key: str) -> bytes:
            raise FileNotFoundError(path)

        def set_value(self, path: Path, key: str, value: bytes) -> None:
            raise FileNotFoundError(path)

    monkeypatch.setattr(
        phaselock_module,
        "_xattr_provider",
        lambda: MissingTargetProvider(),
    )
    missing_target = PhaseLockService(target, use_xattrs=True)
    assert missing_target._get_xattr("key") is None
    assert missing_target._set_xattr("key", b"value") is False

    class UnsupportedProvider:
        def get_value(self, path: Path, key: str) -> bytes:
            raise OSError(unsupported_errno, "unsupported")

        def set_value(self, path: Path, key: str, value: bytes) -> None:
            raise OSError(unsupported_errno, "unsupported")

    monkeypatch.setattr(
        phaselock_module,
        "_xattr_provider",
        lambda: UnsupportedProvider(),
    )
    unsupported = PhaseLockService(target, use_xattrs=True)
    assert unsupported._get_xattr("key") is None
    assert unsupported._set_xattr("key", b"value") is False
    assert unsupported.xattrs_available is False

    class UnexpectedProvider:
        def get_value(self, path: Path, key: str) -> bytes:
            raise OSError(errno.EIO, "io failed")

        def set_value(self, path: Path, key: str, value: bytes) -> None:
            raise OSError(errno.EIO, "io failed")

    monkeypatch.setattr(
        phaselock_module,
        "_xattr_provider",
        lambda: UnexpectedProvider(),
    )
    unexpected = PhaseLockService(target, use_xattrs=True)
    assert unexpected._get_xattr("key") is None
    assert unexpected._set_xattr("key", b"value") is False
    assert unexpected.xattrs_available is False


def test_same_stem_targets_get_distinct_sidecar_paths(tmp_path: Path) -> None:
    """Same-stem targets must not collide on lock/status sidecar paths.

    ``mydb.db`` and ``mydb.backup`` share a stem; deriving the sidecar with
    ``with_suffix`` collapses both onto ``mydb.lock``/``mydb.status``. Sidecars
    must instead append to the full target name so they stay distinct.
    """
    db_service = PhaseLockService(tmp_path / "mydb.db")
    backup_service = PhaseLockService(tmp_path / "mydb.backup")

    assert db_service.lock_path != backup_service.lock_path
    assert db_service.status_base_path != backup_service.status_base_path


def test_xattrless_status_not_inherited_across_same_stem_targets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On xattr-less filesystems, same-stem targets must not share status markers.

    With status-file fallback, completing a phase for ``mydb.db`` must not make
    ``mydb.backup`` appear to have that phase completed (which would skip its
    setup). A colliding status path is exactly that hazard.
    """
    monkeypatch.setenv(phaselock_module.PHASELOCK_ENABLE_XATTRS, "0")

    db_target = tmp_path / "mydb.db"
    db_target.touch()
    db_service = PhaseLockService(db_target)
    db_result = db_service.run_phases((Phase("connection-v1", lambda: None),))
    assert db_result.completed == ("connection-v1",)
    assert db_service.has_phase("connection-v1")

    backup_service = PhaseLockService(tmp_path / "mydb.backup")
    assert not backup_service.has_phase("connection-v1")
