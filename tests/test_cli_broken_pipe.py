"""Black-box pipe-closure contract for streaming CLI commands."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from simplebroker import Queue
from simplebroker.project import target_for_directory

from .conftest import run_cli


def _spawn_broker(
    workdir: Path, *args: str, unbuffered: bool = True
) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(Path(__file__).parents[1]), env.get("PYTHONPATH", "")])
    )
    if unbuffered:
        env["PYTHONUNBUFFERED"] = "1"
    else:
        env.pop("PYTHONUNBUFFERED", None)
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


def test_default_buffered_list_pipe_closure_is_controlled_error(
    workdir: Path,
) -> None:
    assert run_cli("write", "listed", "payload", cwd=workdir)[0] == 0

    process = _spawn_broker(workdir, "list", unbuffered=False)
    returncode, stderr = _close_consumer_and_wait(process)

    assert returncode == 1
    assert stderr.startswith("simplebroker: error:")
    assert "stdout" in stderr.lower()
    assert "rerun" in stderr.lower()
    assert "Traceback" not in stderr
    assert "Exception ignored" not in stderr


@pytest.mark.parametrize(
    ("output_flag", "json_output"),
    [("--json", True), ("--timestamps", False)],
)
def test_default_buffered_write_pipe_closure_reports_durable_mutation(
    workdir: Path,
    output_flag: str,
    json_output: bool,
) -> None:
    process = _spawn_broker(
        workdir,
        "write",
        "written",
        "payload",
        output_flag,
        unbuffered=False,
    )
    returncode, stderr = _close_consumer_and_wait(process)

    assert returncode == 1
    if json_output:
        assert stderr.startswith("{")
        message = json.loads(stderr)["message"]
    else:
        assert stderr.startswith("simplebroker: error:")
        message = stderr
    assert "inspect broker state before retrying" in message
    assert "Traceback" not in stderr
    assert "Exception ignored" not in stderr
    with Queue("written", db_path=target_for_directory(workdir)) as queue:
        assert queue.peek() == "payload"


def _assert_controlled_finite_output_error(
    process: subprocess.Popen[str],
    *,
    json_output: bool,
) -> str:
    returncode, stderr = _close_consumer_and_wait(process)

    assert returncode == 1
    assert "Traceback" not in stderr
    assert "Exception ignored" not in stderr
    if json_output:
        diagnostic = json.loads(stderr)
        assert diagnostic["error"] == "ERROR"
        assert diagnostic["retryable"] is False
        message = str(diagnostic["message"])
    else:
        assert stderr.startswith("simplebroker: error:")
        message = stderr
    assert "stdout" in message.lower()
    return message


@pytest.mark.parametrize(
    "args",
    [(), ("--quiet",), ("--version",), ("list", "--help")],
)
def test_help_and_version_pipe_closure_are_controlled_errors(
    workdir: Path,
    args: tuple[str, ...],
) -> None:
    process = _spawn_broker(workdir, *args, unbuffered=False)

    _assert_controlled_finite_output_error(process, json_output=False)


def test_alias_list_pipe_closure_is_a_controlled_error(workdir: Path) -> None:
    assert run_cli("alias", "add", "worker", "jobs", cwd=workdir)[0] == 0
    process = _spawn_broker(workdir, "alias", "list", unbuffered=False)

    _assert_controlled_finite_output_error(process, json_output=False)


@pytest.mark.parametrize(
    ("args", "json_output"),
    [
        (("list", "--json"), True),
        (("list", "--stats"), False),
        (("exists", "finite", "--json"), True),
        (("exists", "missing", "--json"), True),
        (("stats", "finite"), False),
        (("stats", "finite", "--json"), True),
        (("--status",), False),
        (("--status", "--json"), True),
    ],
)
def test_finite_metadata_pipe_closure_is_a_controlled_error(
    workdir: Path,
    args: tuple[str, ...],
    json_output: bool,
) -> None:
    assert run_cli("write", "finite", "payload", cwd=workdir)[0] == 0
    process = _spawn_broker(workdir, *args, unbuffered=False)

    _assert_controlled_finite_output_error(process, json_output=json_output)


def test_rename_pipe_closure_reports_durable_mutation(workdir: Path) -> None:
    assert run_cli("write", "old-name", "payload", cwd=workdir)[0] == 0
    process = _spawn_broker(
        workdir,
        "rename",
        "old-name",
        "new-name",
        "--json",
        unbuffered=False,
    )

    message = _assert_controlled_finite_output_error(process, json_output=True)

    assert "inspect broker state before retrying" in message
    with Queue("new-name", db_path=target_for_directory(workdir)) as queue:
        assert queue.peek() == "payload"


def test_rename_no_match_pipe_closure_overrides_queue_empty(workdir: Path) -> None:
    process = _spawn_broker(
        workdir,
        "rename",
        "missing-name",
        "new-name",
        "--json",
        unbuffered=False,
    )

    message = _assert_controlled_finite_output_error(process, json_output=True)

    assert "inspect broker state before retrying" in message


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


def test_short_default_buffered_read_all_rolls_back_before_commit(
    workdir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BROKER_READ_COMMIT_INTERVAL", "128")
    with Queue(
        "short", db_path=target_for_directory(workdir), persistent=True
    ) as queue:
        for index in range(5):
            queue.write(f"message-{index}")

    process = _spawn_broker(
        workdir,
        "read",
        "short",
        "--all",
        unbuffered=False,
    )
    returncode, stderr = _close_consumer_and_wait(process)

    assert returncode == 0, stderr
    assert stderr == ""
    with Queue("short", db_path=target_for_directory(workdir)) as queue:
        stats = queue.stats()
        assert stats.pending == 5
        assert stats.claimed == 0


def test_exact_message_pipe_closure_is_clean(workdir: Path) -> None:
    with Queue(
        "exact", db_path=target_for_directory(workdir), persistent=True
    ) as queue:
        message_id = queue.write("payload")

    process = _spawn_broker(workdir, "read", "exact", "-m", str(message_id))
    returncode, stderr = _close_consumer_and_wait(process)

    assert returncode == 0, stderr
    assert stderr == ""


def test_exact_message_json_pipe_closure_is_clean(workdir: Path) -> None:
    with Queue(
        "exact-json", db_path=target_for_directory(workdir), persistent=True
    ) as queue:
        message_id = queue.write("payload")

    process = _spawn_broker(
        workdir,
        "read",
        "exact-json",
        "-m",
        str(message_id),
        "--json",
    )
    returncode, stderr = _close_consumer_and_wait(process)

    assert returncode == 0, stderr
    assert stderr == ""


@pytest.mark.parametrize("json_output", [False, True])
def test_exact_move_pipe_closure_is_clean(workdir: Path, json_output: bool) -> None:
    with Queue(
        "move-source", db_path=target_for_directory(workdir), persistent=True
    ) as queue:
        message_id = queue.write("payload")

    args = ["move", "move-source", "move-dest", "-m", str(message_id)]
    if json_output:
        args.append("--json")
    process = _spawn_broker(workdir, *args)
    returncode, stderr = _close_consumer_and_wait(process)

    assert returncode == 0, stderr
    assert stderr == ""
    with Queue("move-dest", db_path=target_for_directory(workdir)) as destination:
        assert destination.peek(message_id=message_id) == "payload"


def test_move_all_pipe_closure_is_clean_after_atomic_move(workdir: Path) -> None:
    with Queue(
        "move-all-source",
        db_path=target_for_directory(workdir),
        persistent=True,
    ) as source:
        for index in range(5):
            source.write(f"payload-{index}")

    process = _spawn_broker(
        workdir,
        "move",
        "move-all-source",
        "move-all-dest",
        "--all",
    )
    returncode, stderr = _close_consumer_and_wait(process)

    assert returncode == 0, stderr
    assert stderr == ""
    with Queue("move-all-dest", db_path=target_for_directory(workdir)) as destination:
        assert destination.stats().pending == 5


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
    try:
        returncode = process.wait(timeout=10)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=2)
        process.stdout.close()
        assert process.stderr is not None
        stderr = process.stderr.read()
        process.stderr.close()

    assert returncode == 0
    assert stderr == ""
