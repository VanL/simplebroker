from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from simplebroker._phaselock import (
    Phase,
    PhaseLockService,
    PhaseLockTimeout,
    PhaseRunResult,
)


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

    monkeypatch.setenv(phaselock_module._ENABLE_PHASELOCK_XATTRS, "0")
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

    monkeypatch.setenv(phaselock_module._ENABLE_PHASELOCK_XATTRS, "0")
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
        if proc.returncode != 0:
            stdout, stderr = proc.communicate()
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
    expected_status_paths = (tmp_path / "broker.status",)
    assert first.status_paths == expected_status_paths
    assert second.completed == ()
    assert second.skipped == ("connection-v1", "schema-v4")
    assert second.status_paths == expected_status_paths
    assert calls == ["connection", "schema"]
    assert service.lock_path == tmp_path / "broker.lock"
    assert service.lock_path.exists()
    assert _read_status_file(service.status_base_path) == [
        "connection-v1",
        "schema-v4",
    ]
    assert sorted(tmp_path.glob("broker.status.*")) == []
    assert sorted(tmp_path.glob("broker.setup.status.*")) == []
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


def test_no_xattr_legacy_status_sidecars_are_ignored_then_cleaned(
    tmp_path: Path,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(target, use_xattrs=False)
    service.status_path_for_phase("connection-v1").touch(mode=0o600)
    service.status_path_for_phase("schema-v4").touch(mode=0o600)
    calls: list[str] = []

    result = service.run_phases(
        (
            Phase("connection-v1", lambda: calls.append("connection")),
            Phase("schema-v4", lambda: calls.append("schema")),
            Phase("optimization-v1", lambda: calls.append("optimization")),
        )
    )

    assert result.completed == ("connection-v1", "schema-v4", "optimization-v1")
    assert result.skipped == ()
    assert calls == ["connection", "schema", "optimization"]
    assert _read_status_file(service.status_base_path) == [
        "connection-v1",
        "schema-v4",
        "optimization-v1",
    ]
    assert not service.status_path_for_phase("connection-v1").exists()
    assert not service.status_path_for_phase("schema-v4").exists()


def test_no_xattr_legacy_status_sidecars_clean_when_new_status_skips(
    tmp_path: Path,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(
        target,
        use_xattrs=False,
        strict_marker_locking=True,
    )
    _write_status_file(service.status_base_path, ["connection-v1", "schema-v4"])
    service.status_path_for_phase("connection-v1").touch(mode=0o600)
    calls: list[str] = []

    result = service.run_phases(
        (
            Phase("connection-v1", lambda: calls.append("connection")),
            Phase("schema-v4", lambda: calls.append("schema")),
        )
    )

    assert result.completed == ()
    assert result.skipped == ("connection-v1", "schema-v4")
    assert calls == []
    assert not service.status_path_for_phase("connection-v1").exists()


def test_no_xattr_legacy_lock_is_ignored_then_cleaned(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(target, use_xattrs=False)
    legacy_lock_path = target.with_suffix(".setup.lock")
    legacy_lock_path.touch(mode=0o600)

    result = service.run_phases((Phase("connection-v1", lambda: None),))

    assert result.completed == ("connection-v1",)
    assert not legacy_lock_path.exists()
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


def test_process_local_lock_serializes_threads(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    first_service = PhaseLockService(
        target,
        timeout=1.0,
        retry_delay=0.01,
        use_xattrs=False,
    )
    second_service = PhaseLockService(
        target,
        timeout=1.0,
        retry_delay=0.01,
        use_xattrs=False,
    )
    first_entered = threading.Event()
    release_first = threading.Event()
    calls: list[str] = []
    results: dict[str, object] = {}

    def first_action() -> None:
        calls.append("first")
        first_entered.set()
        assert release_first.wait(timeout=1.0)

    def run_first() -> None:
        first_service.run_phases((Phase("connection-v1", first_action),))

    def run_second() -> None:
        results["second"] = second_service.run_phases(
            (Phase("connection-v1", lambda: calls.append("second")),)
        )

    first = threading.Thread(target=run_first)
    first.start()
    assert first_entered.wait(timeout=1.0)

    second = threading.Thread(target=run_second)
    second.start()
    time.sleep(0.05)
    assert second.is_alive()

    release_first.set()
    first.join(timeout=1.0)
    second.join(timeout=1.0)

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


def test_lock_context_releases_after_exception(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(target, timeout=0.5, retry_delay=0.01)

    with pytest.raises(RuntimeError, match="boom"):
        with service.locked():
            raise RuntimeError("boom")

    with service.locked():
        assert service.lock_path.exists()


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
