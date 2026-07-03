"""SQLite setup contention integration tests."""

from __future__ import annotations

import multiprocessing
import os
import sqlite3
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any

import pytest

from simplebroker import Queue
from simplebroker._phaselock import PhaseLockService

from .helper_scripts.timing import scale_timeout_for_ci


def _python_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    return env


def _wait_for_path(
    path: Path,
    *,
    timeout: float,
    proc: subprocess.Popen[str] | None = None,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if proc is not None and proc.poll() is not None:
            stdout, stderr = proc.communicate(timeout=5.0)
            raise AssertionError(
                f"Process exited before creating {path}: "
                f"returncode={proc.returncode}\nstdout={stdout}\nstderr={stderr}"
            )
        time.sleep(0.01)
    raise AssertionError(f"Timed out waiting for {path}")


def _communicate_success(proc: subprocess.Popen[str], *, timeout: float) -> str:
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, stderr = proc.communicate(timeout=5.0)
        raise AssertionError(
            f"Process timed out: args={proc.args!r}\nstdout={stdout}\nstderr={stderr}"
        ) from None

    assert proc.returncode == 0, (
        f"Process failed with {proc.returncode}: args={proc.args!r}\n"
        f"stdout={stdout}\nstderr={stderr}"
    )
    return stdout


def test_concurrent_first_writes_serialize_setup(tmp_path: Path) -> None:
    db_path = tmp_path / "broker.db"
    start_path = tmp_path / "start"
    process_count = 6 if sys.platform == "win32" else 8
    messages = [f"msg-{index}" for index in range(process_count)]
    script = textwrap.dedent(
        """
        import sys
        import time
        from pathlib import Path

        from simplebroker import Queue

        db_path = Path(sys.argv[1])
        message = sys.argv[2]
        start_path = Path(sys.argv[3])

        deadline = time.monotonic() + 15.0
        while not start_path.exists():
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for {start_path}")
            time.sleep(0.005)

        queue = Queue("setup_contention", persistent=True, db_path=str(db_path))
        try:
            queue.write(message)
        finally:
            queue.close()
        print(message, flush=True)
        """
    )

    procs = [
        subprocess.Popen(
            [sys.executable, "-c", script, str(db_path), message, str(start_path)],
            cwd=tmp_path,
            env=_python_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        for message in messages
    ]

    try:
        start_path.touch()
        for proc in procs:
            _communicate_success(proc, timeout=scale_timeout_for_ci(20.0))
    finally:
        for proc in procs:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5.0)

    queue = Queue("setup_contention", persistent=True, db_path=str(db_path))
    try:
        stored = list(queue.read(all_messages=True))
    finally:
        queue.close()

    assert sorted(stored) == sorted(messages)

    service = PhaseLockService(db_path)
    assert service.lock_path.exists()


def test_first_write_retries_during_temporary_setup_lock(tmp_path: Path) -> None:
    db_path = tmp_path / "broker.db"
    ready_path = tmp_path / "holder-ready"
    release_path = tmp_path / "holder-release"
    writer_started_path = tmp_path / "writer-started"
    holder_script = textwrap.dedent(
        """
        import sqlite3
        import sys
        import time
        from pathlib import Path

        db_path = Path(sys.argv[1])
        ready_path = Path(sys.argv[2])
        release_path = Path(sys.argv[3])

        conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=5.0)
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS preexisting(id INTEGER PRIMARY KEY)")
            conn.execute("BEGIN EXCLUSIVE")
            ready_path.touch()
            while not release_path.exists():
                time.sleep(0.01)
            conn.rollback()
        finally:
            conn.close()
        """
    )
    writer_script = textwrap.dedent(
        """
        import sys
        from pathlib import Path

        from simplebroker import Queue

        db_path = Path(sys.argv[1])
        started_path = Path(sys.argv[2])
        started_path.touch()

        queue = Queue("setup_contention", persistent=True, db_path=str(db_path))
        try:
            queue.write("locked-setup-message")
        finally:
            queue.close()
        print("done", flush=True)
        """
    )

    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            holder_script,
            str(db_path),
            str(ready_path),
            str(release_path),
        ],
        cwd=tmp_path,
        env=_python_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )

    writer: subprocess.Popen[str] | None = None
    try:
        _wait_for_path(
            ready_path,
            timeout=scale_timeout_for_ci(5.0),
            proc=holder,
        )
        writer = subprocess.Popen(
            [
                sys.executable,
                "-c",
                writer_script,
                str(db_path),
                str(writer_started_path),
            ],
            cwd=tmp_path,
            env=_python_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        _wait_for_path(
            writer_started_path,
            timeout=scale_timeout_for_ci(5.0),
            proc=writer,
        )
        time.sleep(1.0)
        release_path.touch()
        _communicate_success(holder, timeout=scale_timeout_for_ci(10.0))
        _communicate_success(writer, timeout=scale_timeout_for_ci(20.0))
    finally:
        release_path.touch()
        for proc in (writer, holder):
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5.0)

    queue = Queue("setup_contention", persistent=True, db_path=str(db_path))
    try:
        assert list(queue.read(all_messages=True)) == ["locked-setup-message"]
    finally:
        queue.close()


def _mixed_version_child_new_paths(
    db_path: str, barrier: Any, result_queue: Any
) -> None:
    """Child A: full runner setup using the new (post-fix) sidecar paths."""
    import os as _os

    from simplebroker import Queue as _Queue

    try:
        queue = _Queue("mixed_version", persistent=True, db_path=db_path)
        try:
            # Barrier immediately BEFORE the first queue op (setup is lazy and
            # runs when the first operation opens the connection).
            barrier.wait(timeout=30.0)
            queue.write("child-a")
            assert queue.read() == "child-a"
        finally:
            queue.close()
        result_queue.put(("child-a", 0, _os.getpid()))
    except BaseException as exc:  # pragma: no cover - child failure path
        result_queue.put(("child-a", 1, repr(exc)))
        raise


def _mixed_version_child_old_paths(
    db_path: str, barrier: Any, result_queue: Any
) -> None:
    """Child B: simulate a pre-fix process using old with_suffix sidecar paths.

    Monkeypatch SQLiteRunner._phase_lock_service to override the returned
    service's lock_path/status_base_path to the old with_suffix-style values,
    so the two children coordinate through DIFFERENT lock/status files.
    """
    import os as _os
    from pathlib import Path as _Path

    import simplebroker._runner as _runner_module
    from simplebroker import Queue as _Queue

    original = _runner_module.SQLiteRunner._phase_lock_service

    def _old_style(self: Any) -> Any:
        service = original(self)
        target = _Path(self._db_path)
        # Pre-fix derivation: with_suffix collapses onto the stem.
        service.lock_path = target.with_suffix(".lock")
        service.status_base_path = target.with_suffix(".status")
        return service

    _runner_module.SQLiteRunner._phase_lock_service = _old_style  # type: ignore[method-assign]

    try:
        queue = _Queue("mixed_version", persistent=True, db_path=db_path)
        try:
            barrier.wait(timeout=30.0)
            queue.write("child-b")
            assert queue.read() == "child-b"
        finally:
            queue.close()
        result_queue.put(("child-b", 0, _os.getpid()))
    except BaseException as exc:  # pragma: no cover - child failure path
        result_queue.put(("child-b", 1, repr(exc)))
        raise
    finally:
        _runner_module.SQLiteRunner._phase_lock_service = original  # type: ignore[method-assign]


def test_mixed_version_lock_paths_setup_is_idempotent_safe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mixed lock-path window (old vs new) must still yield a WAL DB + schema.

    During an upgrade, an old process may hold mydb.lock while a new one holds
    mydb.db.lock, so cross-version setup serialization is lost. With
    PHASELOCK_ENABLE_XATTRS=0 each scheme writes to its own status file, so
    neither child can observe the other's markers and BOTH must run full setup.
    SQLite serializes the underlying DDL, so the accepted mixed-version window
    is idempotent-safe: prove it rather than assume it.
    """
    monkeypatch.setenv("PHASELOCK_ENABLE_XATTRS", "0")

    db_path = tmp_path / "mydb.db"

    # Consistent spawn context: Process AND Barrier from the SAME context.
    ctx = multiprocessing.get_context("spawn")
    barrier = ctx.Barrier(2)
    result_queue: Any = ctx.Queue()

    child_a = ctx.Process(
        target=_mixed_version_child_new_paths,
        args=(str(db_path), barrier, result_queue),
    )
    child_b = ctx.Process(
        target=_mixed_version_child_old_paths,
        args=(str(db_path), barrier, result_queue),
    )

    child_a.start()
    child_b.start()

    results: dict[str, tuple[int, object]] = {}
    deadline = time.monotonic() + scale_timeout_for_ci(45.0)
    while len(results) < 2 and time.monotonic() < deadline:
        try:
            name, code, detail = result_queue.get(timeout=1.0)
        except Exception:
            continue
        results[name] = (code, detail)

    child_a.join(timeout=scale_timeout_for_ci(10.0))
    child_b.join(timeout=scale_timeout_for_ci(10.0))

    try:
        assert results.get("child-a", (None,))[0] == 0, (
            f"child-a failed: {results.get('child-a')}"
        )
        assert results.get("child-b", (None,))[0] == 0, (
            f"child-b failed: {results.get('child-b')}"
        )
        assert child_a.exitcode == 0, f"child-a exitcode={child_a.exitcode}"
        assert child_b.exitcode == 0, f"child-b exitcode={child_b.exitcode}"
    finally:
        for proc in (child_a, child_b):
            if proc.is_alive():  # pragma: no cover - cleanup
                proc.terminate()
                proc.join(timeout=5.0)

    # BOTH children must have executed full setup -> both status files exist.
    old_status = db_path.with_suffix(".status")  # child B, pre-fix scheme
    new_status = Path(str(db_path) + ".status")  # child A, post-fix scheme
    assert old_status.exists(), "child-b (old scheme) status file missing"
    assert new_status.exists(), "child-a (new scheme) status file missing"

    # Database is in WAL mode with an intact schema (fresh Queue can r/w).
    conn = sqlite3.connect(str(db_path))
    try:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    finally:
        conn.close()
    assert str(mode).lower() == "wal", f"journal_mode={mode!r}"

    verify = Queue("mixed_version_verify", persistent=True, db_path=str(db_path))
    try:
        verify.write("intact")
        assert verify.read() == "intact"
    finally:
        verify.close()
