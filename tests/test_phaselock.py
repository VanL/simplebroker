from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from simplebroker._phaselock import Phase, PhaseLockService, PhaseLockTimeout


def _real_xattrs_supported(target: Path) -> bool:
    if not callable(getattr(os, "getxattr", None)):
        return False
    if not callable(getattr(os, "setxattr", None)):
        return False

    key = "user.simplebroker.pytest.probe"
    try:
        os.setxattr(target, key, b"1")
        return os.getxattr(target, key) == b"1"
    except OSError:
        return False
    finally:
        remove = getattr(os, "removexattr", None)
        if callable(remove):
            try:
                remove(target, key)
            except OSError:
                pass


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
    if not _real_xattrs_supported(target):
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
    assert not service.lock_path.exists()


def test_real_xattr_runtime_resumes_from_last_marked_phase(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    if not _real_xattrs_supported(target):
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
    assert not service.lock_path.exists()


def test_failure_while_lock_held_leaves_partial_xattrs_and_blocks_contenders(
    tmp_path: Path,
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    if not _real_xattrs_supported(target):
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
    assert not service.lock_path.exists()


def test_no_xattr_fallback_keeps_one_status_file_and_no_done_files(
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
    assert first.status_path == tmp_path / "broker.setup.status.schema-v4"
    assert second.completed == ()
    assert second.skipped == ("connection-v1", "schema-v4")
    assert calls == ["connection", "schema"]
    assert not service.lock_path.exists()
    assert list(tmp_path.glob("*.status.*")) == [first.status_path]
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
    assert list(tmp_path.glob("*.status.*")) == [
        tmp_path / "broker.setup.status.connection-v1"
    ]

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
    assert list(tmp_path.glob("*.status.*")) == [
        tmp_path / "broker.setup.status.optimization-v1"
    ]
    assert not service.lock_path.exists()


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

        assert list(tmp_path.glob("*.status.*")) == [
            tmp_path / "broker.setup.status.connection-v1"
        ]
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
    assert list(tmp_path.glob("*.status.*")) == [
        tmp_path / "broker.setup.status.schema-v4"
    ]
    assert not service.lock_path.exists()


def test_no_xattr_status_marker_cleans_older_status_files(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(target, use_xattrs=False)
    (tmp_path / "broker.setup.status.connection-v1").touch()
    (tmp_path / "broker.setup.status.schema-v4").touch()
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
    assert list(tmp_path.glob("*.status.*")) == [
        tmp_path / "broker.setup.status.optimization-v1"
    ]


def test_lock_timeout_when_another_process_holds_lock(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(target, timeout=0.15, retry_delay=0.01)
    calls: list[str] = []

    with _subprocess_holding_phase_lock(target):
        with pytest.raises(PhaseLockTimeout):
            service.run_phases((Phase("connection-v1", lambda: calls.append("ran")),))

    assert calls == []
    assert service.lock_path.exists()


def test_lock_context_releases_after_exception(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    service = PhaseLockService(target, timeout=0.5, retry_delay=0.01)

    with pytest.raises(RuntimeError, match="boom"):
        with service.locked():
            raise RuntimeError("boom")

    with service.locked():
        assert service.lock_path.exists()


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
