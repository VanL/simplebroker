"""SQLite setup contention integration tests."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

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
