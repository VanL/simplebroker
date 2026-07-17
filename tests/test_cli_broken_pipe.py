"""Black-box pipe-closure contract for streaming CLI commands."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from simplebroker import Queue
from simplebroker.project import target_for_directory

from .conftest import run_cli


def _spawn_broker(workdir: Path, *args: str) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(Path(__file__).parents[1]), env.get("PYTHONPATH", "")])
    )
    env["PYTHONUNBUFFERED"] = "1"
    return subprocess.Popen(
        [sys.executable, "-m", "simplebroker.cli", *args],
        cwd=workdir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _close_consumer_and_wait(process: subprocess.Popen[str]) -> tuple[int | None, str]:
    assert process.stdout is not None
    process.stdout.close()
    try:
        returncode = process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        returncode = None
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
    assert process.stderr is not None
    stderr = process.stderr.read()
    process.stderr.close()
    return returncode, stderr


def _seed_large_queue(workdir: Path, *, count: int = 128) -> None:
    with Queue("bulk", db_path=target_for_directory(workdir), persistent=True) as queue:
        for index in range(count):
            queue.write(f"{index:04d}:" + "x" * 8192)


def test_watch_stops_claiming_after_stdout_consumer_exits(workdir: Path) -> None:
    assert run_cli("write", "jobs", "m1", cwd=workdir)[0] == 0
    process = _spawn_broker(workdir, "--quiet", "watch", "jobs")
    assert process.stdout is not None
    assert process.stdout.readline().strip() == "m1"
    process.stdout.close()

    for message in ("m2", "m3", "m4", "m5"):
        assert run_cli("write", "jobs", message, cwd=workdir)[0] == 0

    try:
        returncode = process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        returncode = None
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=2)
    assert process.stderr is not None
    stderr = process.stderr.read()
    process.stderr.close()

    assert returncode == 0, stderr
    assert stderr == ""
    code, remaining, error = run_cli("peek", "jobs", "--all", cwd=workdir)
    assert code == 0, error
    assert remaining.splitlines() == ["m3", "m4", "m5"]


def test_peek_all_pipe_closure_is_clean_and_preserves_queue(workdir: Path) -> None:
    _seed_large_queue(workdir)
    process = _spawn_broker(workdir, "peek", "bulk", "--all")
    assert process.stdout is not None
    assert process.stdout.readline().startswith("0000:")

    returncode, stderr = _close_consumer_and_wait(process)

    assert returncode == 0, stderr
    assert stderr == ""
    with Queue("bulk", db_path=target_for_directory(workdir)) as queue:
        assert queue.stats().pending == 128


def test_read_all_pipe_closure_is_clean_and_leaves_unread_messages(
    workdir: Path,
) -> None:
    _seed_large_queue(workdir)
    process = _spawn_broker(workdir, "read", "bulk", "--all")
    assert process.stdout is not None
    assert process.stdout.readline().startswith("0000:")

    returncode, stderr = _close_consumer_and_wait(process)

    assert returncode == 0, stderr
    assert stderr == ""
    with Queue("bulk", db_path=target_for_directory(workdir)) as queue:
        assert 0 < queue.stats().pending < 128


def test_read_all_pipe_closure_rolls_back_active_at_least_once_batch(
    workdir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BROKER_READ_COMMIT_INTERVAL", "128")
    _seed_large_queue(workdir)
    process = _spawn_broker(workdir, "read", "bulk", "--all")
    assert process.stdout is not None
    assert process.stdout.readline().startswith("0000:")

    returncode, stderr = _close_consumer_and_wait(process)

    assert returncode == 0, stderr
    assert stderr == ""
    with Queue("bulk", db_path=target_for_directory(workdir)) as queue:
        assert queue.stats().pending == 128


def test_dump_pipe_closure_is_clean(workdir: Path) -> None:
    _seed_large_queue(workdir)
    process = _spawn_broker(workdir, "dump")
    assert process.stdout is not None
    assert '"type":"header"' in process.stdout.readline().replace(" ", "")

    returncode, stderr = _close_consumer_and_wait(process)

    assert returncode == 0, stderr
    assert stderr == ""


@pytest.mark.skipif(os.name == "nt", reason="POSIX signal contract")
def test_watch_sigterm_is_a_clean_shutdown(workdir: Path) -> None:
    assert run_cli("write", "jobs", "ready", cwd=workdir)[0] == 0
    process = _spawn_broker(workdir, "--quiet", "watch", "jobs", "--peek")
    assert process.stdout is not None
    assert process.stdout.readline().strip() == "ready"

    process.send_signal(signal.SIGTERM)
    returncode = process.wait(timeout=10)
    process.stdout.close()
    assert process.stderr is not None
    stderr = process.stderr.read()
    process.stderr.close()

    assert returncode == 0
    assert stderr == ""
