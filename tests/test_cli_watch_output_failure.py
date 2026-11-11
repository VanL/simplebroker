"""A failed CLI output sink is terminal, unlike a recoverable user callback."""

from __future__ import annotations

import errno
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from simplebroker import Queue, commands

from .helper_scripts.timing import scale_timeout_for_ci, wait_for_condition

pytestmark = pytest.mark.sqlite_only
ROOT = Path(__file__).resolve().parents[1]
PROBE = ROOT / "tests/helper_scripts/cli_watch_output_probe.py"


def _environment() -> dict[str, str]:
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            str(path)
            for path in (
                ROOT,
                ROOT / "extensions/simplebroker_pg",
                ROOT / "extensions/simplebroker_redis",
            )
        ),
        "BROKER_PROJECT_SCOPE": "0",
    }
    # Qualify the normal CLI's default logging, not an artificially quiet logger.
    environment.pop("BROKER_LOGGING_ENABLED", None)
    return environment


def _reap(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.kill()
    process.communicate(timeout=scale_timeout_for_ci(10))


@pytest.mark.skipif(os.name != "posix", reason="real terminal hangup needs a POSIX pty")
@pytest.mark.parametrize("watch_mode", ["consume", "peek", "move"])
@pytest.mark.parametrize("json_output", [False, True], ids=["plain", "json"])
def test_watch_terminal_hangup_stops_before_later_messages(
    tmp_path: Path, watch_mode: str, json_output: bool
) -> None:
    _run_terminal_hangup(tmp_path, watch_mode, json_output, dead_stderr=False)


@pytest.mark.skipif(os.name != "posix", reason="real terminal hangup needs a POSIX pty")
def test_watch_both_terminal_streams_fail_without_flush_exit(tmp_path: Path) -> None:
    _run_terminal_hangup(tmp_path, "consume", True, dead_stderr=True)


@pytest.mark.skipif(os.name != "posix", reason="real terminal hangup needs a POSIX pty")
@pytest.mark.parametrize("failure_stage", ["terminal-flush", "banner"])
def test_watch_stderr_failure_during_cleanup_or_banner(
    tmp_path: Path, failure_stage: str
) -> None:
    _run_terminal_hangup(
        tmp_path, "consume", True, dead_stderr=True, failure_stage=failure_stage
    )


def _run_terminal_hangup(
    tmp_path: Path,
    mode: str,
    json_output: bool,
    *,
    dead_stderr: bool,
    failure_stage: str = "terminal",
) -> None:
    import pty

    target = tmp_path / "broker.db"
    ready, report = tmp_path / "ready", tmp_path / "report"
    with Queue("jobs", db_path=str(target)) as queue:
        queue.write("first failed delivery")
    arguments = ["--file", str(target)]
    if failure_stage != "banner":
        arguments.append("--quiet")
    arguments.extend(["watch", "jobs"])
    if mode == "peek":
        arguments.append("--peek")
    elif mode == "move":
        arguments.extend(["--move", "done"])
    if json_output:
        arguments.append("--json")
    master, slave = pty.openpty()
    process = subprocess.Popen(
        [
            sys.executable,
            str(PROBE),
            failure_stage,
            str(ready),
            str(report),
            *arguments,
        ],
        cwd=tmp_path,
        env=_environment(),
        stdin=subprocess.PIPE,
        stdout=slave,
        stderr=slave if dead_stderr else subprocess.PIPE,
        text=True,
    )
    os.close(slave)
    try:
        assert wait_for_condition(
            lambda: ready.exists() or process.poll() is not None,
            timeout=scale_timeout_for_ci(15),
        )
        assert ready.exists(), process.communicate()
        os.close(master)
        master = -1
        assert process.stdin is not None
        process.stdin.write("release\n")
        process.stdin.flush()
        try:
            _out, error = process.communicate(timeout=scale_timeout_for_ci(5))
        except subprocess.TimeoutExpired:
            pytest.fail("watch remained alive after its terminal output failed")
        assert process.returncode == 1, error
        result = json.loads(report.read_text())
        assert result["stdout_closed"] is (failure_stage != "banner")
        assert result["stdout_pair"]
        if dead_stderr:
            assert result["stderr_closed"]
            assert result["stderr_pair"]
        else:
            assert error is not None
            assert "Traceback" not in error
            assert "Exception ignored" not in error
            if json_output:
                diagnostic = json.loads(error)
                assert diagnostic["error"] == "ERROR"
                assert diagnostic["retryable"] is False
            else:
                assert "simplebroker: error:" in error
        # Process exit is the admission boundary witness. Later writes must
        # survive regardless of polling cadence or backend scheduling.
        with Queue("jobs", db_path=str(target)) as queue:
            for index in range(20):
                queue.write(f"later-{index}")
            unclaimed = mode == "peek" or failure_stage == "banner"
            assert queue.stats().pending == (21 if unclaimed else 20)
        with Queue("done", db_path=str(target)) as destination:
            assert destination.stats().pending == (1 if mode == "move" else 0)
    finally:
        if master >= 0:
            os.close(master)
        _reap(process)


@pytest.mark.skipif(
    os.name != "posix", reason="launch-time descriptor absence is POSIX"
)
@pytest.mark.parametrize("descriptor", [1, 2])
@pytest.mark.parametrize("json_output", [False, True], ids=["plain", "json"])
@pytest.mark.parametrize(
    "malformed_selector", [False, True], ids=["valid", "malformed"]
)
def test_watch_absent_stream_refused_before_postgres_target_resolution(
    tmp_path: Path, descriptor: int, json_output: bool, malformed_selector: bool
) -> None:
    marker, report = tmp_path / "target-open", tmp_path / "report"
    environment = _environment()
    environment.update(
        BROKER_BACKEND="postgres",
        BROKER_BACKEND_TARGET="postgresql://127.0.0.1:1/postgres?connect_timeout=1",
    )
    arguments = ["--quiet", "watch", "jobs"]
    if malformed_selector:
        arguments.extend(["--after", "invalid"])
    if json_output:
        arguments.append("--json")
    result = subprocess.run(
        [
            sys.executable,
            str(PROBE),
            f"absent-{descriptor}",
            str(marker),
            str(report),
            *arguments,
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=scale_timeout_for_ci(15),
    )
    assert result.returncode == 1, result.stderr
    assert not marker.exists(), (
        "watch resolved a target before rejecting missing streams"
    )
    assert result.stdout == ""
    data = json.loads(report.read_text())
    assert data["stdout_was_none" if descriptor == 1 else "stderr_was_none"]
    if descriptor == 2:
        assert result.stderr == ""
    elif json_output:
        assert json.loads(result.stderr)["error"] == "ERROR"
    else:
        assert "stdout" in result.stderr
    assert "Exception ignored" not in result.stderr


@pytest.mark.skipif(
    os.name != "posix", reason="launch-time descriptor absence is POSIX"
)
@pytest.mark.parametrize("descriptor", [1, 2])
def test_direct_watch_absent_stream_refused_before_acquiring_target(
    tmp_path: Path, descriptor: int
) -> None:
    target = tmp_path / "broker.db"
    marker, report = tmp_path / "marker", tmp_path / "report"
    result = subprocess.run(
        [
            sys.executable,
            str(PROBE),
            f"absent-{descriptor}-direct",
            str(marker),
            str(report),
            str(target),
        ],
        cwd=tmp_path,
        env=_environment(),
        capture_output=True,
        text=True,
        check=False,
        timeout=scale_timeout_for_ci(10),
    )
    assert result.returncode == 1, result.stderr
    assert not marker.exists(), "direct watch resolved inputs before stream rejection"
    assert not target.exists()
    assert result.stdout == result.stderr == ""
    data = json.loads(report.read_text())
    assert data["stdout_was_none" if descriptor == 1 else "stderr_was_none"]


def test_watch_closed_python_stdout_refused_without_target_creation(
    tmp_path: Path,
) -> None:
    marker, report = tmp_path / "marker", tmp_path / "report"
    target = tmp_path / "broker.db"
    result = subprocess.run(
        [
            sys.executable,
            str(PROBE),
            "closed",
            str(marker),
            str(report),
            "--file",
            str(target),
            "--quiet",
            "watch",
            "jobs",
        ],
        cwd=tmp_path,
        env=_environment(),
        capture_output=True,
        text=True,
        check=False,
        timeout=scale_timeout_for_ci(15),
    )
    assert result.returncode == 1, result.stderr
    assert not target.exists()
    assert "Exception ignored" not in result.stderr


class _CallerOutput:
    closed = False

    def __init__(self, failure: Exception) -> None:
        self.failure = failure
        self.close_calls = 0

    def write(self, _value: str) -> int:
        raise self.failure

    def flush(self) -> None:
        raise self.failure

    def close(self) -> None:
        self.close_calls += 1


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_direct_watch_preserves_error_and_caller_stream_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_fails: bool
) -> None:
    target = tmp_path / "broker.db"
    with Queue("jobs", db_path=str(target)) as queue:
        queue.write("payload")
    failure = OSError(errno.EIO, "output failed")
    output = _CallerOutput(failure)
    original = sys.__stdout__
    stopped = []
    stop = commands.QueueWatcher.stop

    def observed_stop(self: Any, *args: Any, **kwargs: Any) -> None:
        stopped.append(self)
        stop(self, *args, **kwargs)
        if cleanup_fails:
            raise RuntimeError("cleanup failed")

    monkeypatch.setattr(commands.QueueWatcher, "stop", observed_stop)
    monkeypatch.setattr(sys, "stdout", output)

    # A bounded callback counter makes the baseline failure explicit rather
    # than leaving an in-process watcher running indefinitely.
    def stop_after_wrong_continuation(self: Any) -> None:
        if self._queue_obj.stats().pending == 0:
            self.stop(join=False)

    monkeypatch.setattr(
        commands.QueueWatcher, "_process_messages", stop_after_wrong_continuation
    )
    try:
        with pytest.raises(OSError) as raised:
            commands.cmd_watch(str(target), "jobs", quiet=True)
        assert raised.value is failure
        assert stopped
        assert output.close_calls == 0
        assert sys.__stdout__ is original
        assert sys.stdout is not output
        seen = set()
        error: BaseException | None = failure
        while error is not None:
            assert id(error) not in seen, "exception cause/context contains a cycle"
            seen.add(id(error))
            error = error.__cause__ or error.__context__
        if cleanup_fails:
            assert any("cleanup failed" in note for note in failure.__notes__)
    finally:
        if sys.stdout is not output and sys.stdout is not original:
            sys.stdout.close()
