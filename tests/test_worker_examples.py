"""Black-box contracts for the published shell worker examples."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from simplebroker import Queue

REPO_ROOT = Path(__file__).resolve().parents[1]
SAFE_WORKER = REPO_ROOT / "examples" / "safe_worker.sh"
RESILIENT_WORKER = REPO_ROOT / "examples" / "resilient_worker.sh"
EXAMPLES_README = REPO_ROOT / "examples" / "README.md"
AGENT_KERNEL = REPO_ROOT / "docs" / "agent-kernel.md"
MESSAGE_ID = "1722783600000000000"
MAX_MESSAGE_ID = str(2**63 - 1)


class _WorkerEnv(dict[str, str]):
    """Environment mapping whose test-failure representation hides host secrets."""

    def __repr__(self) -> str:
        return "<worker test environment>"


def _write_executable(path: Path, source: str) -> None:
    path.write_text(source, encoding="utf-8")
    path.chmod(0o755)


@pytest.fixture
def worker_env(tmp_path: Path) -> dict[str, str]:
    if os.name == "nt":
        pytest.skip("published workers are illustrative Bash examples")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    broker_log = tmp_path / "broker.log"
    broker_argv_log = tmp_path / "broker-argv.log"
    handler_log = tmp_path / "handler.log"
    handler_argc_log = tmp_path / "handler-argc.log"
    sleep_log = tmp_path / "sleep.log"

    _write_executable(
        bin_dir / "broker",
        f"""#!/bin/bash
set -u
operation=
for argument in "$@"; do
    case "$argument" in
        watch|peek|delete) operation="$argument" ;;
    esac
done
printf '%s\n' "$operation" >> "$BROKER_CALL_LOG"
printf '%s' "$operation" >> "$BROKER_ARGV_LOG"
for argument in "$@"; do
    printf '\t%s' "$argument" >> "$BROKER_ARGV_LOG"
done
printf '\n' >> "$BROKER_ARGV_LOG"

count=0
if [ -f "$BROKER_STATE" ]; then
    count=$(< "$BROKER_STATE")
fi
count=$((count + 1))
printf '%s\n' "$count" > "$BROKER_STATE"

case "$BROKER_SCENARIO:$operation" in
    message:*|delete_failure:watch|delete_failure:peek)
        printf '{{"message":"do work","timestamp":"%s"}}\n' "${{BROKER_MESSAGE_ID:-{MESSAGE_ID}}}"
        ;;
    delete_failure:delete)
        echo "simulated delete failure" >&2
        exit 1
        ;;
    large_message:delete)
        echo "simulated delete failure after large message" >&2
        exit 1
        ;;
    peek_failure:watch|peek_failure:peek)
        echo "simulated backend failure" >&2
        exit 1
        ;;
    empty_then_error:watch|empty_then_error:peek)
        if [ "$count" -eq 1 ]; then
            exit 2
        fi
        echo "simulated backend failure after idle" >&2
        exit 1
        ;;
    empty_success:watch|empty_success:peek)
        exit 0
        ;;
    invalid_json:watch|invalid_json:peek)
        printf '%s\n' '{{"message":'
        ;;
    numeric_timestamp:watch|numeric_timestamp:peek)
        printf '%s\n' '{{"message":"do work","timestamp":{MESSAGE_ID}}}'
        ;;
    trailing_newlines:watch|trailing_newlines:peek)
        printf '%s\n' '{{"message":"line one\\n\\n","timestamp":"{MESSAGE_ID}"}}'
        ;;
    nul_message:watch|nul_message:peek)
        printf '%s\n' '{{"message":"before\\u0000after","timestamp":"{MESSAGE_ID}"}}'
        ;;
    large_message:watch|large_message:peek)
        python3 -c 'import json; print(json.dumps({{"message": "x" * (3 * 1024 * 1024), "timestamp": "{MESSAGE_ID}"}}))'
        ;;
    *)
        echo "unexpected broker call: $*" >&2
        exit 1
        ;;
esac
""",
    )
    _write_executable(
        bin_dir / "handler-ok",
        """#!/bin/bash
printf '%s' "$#" > "$HANDLER_ARGC_LOG"
cat > "$HANDLER_CALL_LOG"
exit 0
""",
    )
    _write_executable(
        bin_dir / "handler-fail",
        """#!/bin/bash
printf '%s' "$#" > "$HANDLER_ARGC_LOG"
cat > "$HANDLER_CALL_LOG"
exit 7
""",
    )
    _write_executable(
        bin_dir / "handler-ignore-stdin",
        """#!/bin/bash
printf '%s' "$#" > "$HANDLER_ARGC_LOG"
printf 'handler succeeded without reading stdin' > "$HANDLER_CALL_LOG"
exit 0
""",
    )
    _write_executable(
        bin_dir / "sleep",
        """#!/bin/bash
printf '%s\n' "$*" >> "$SLEEP_CALL_LOG"
count=$(wc -l < "$SLEEP_CALL_LOG")
if [ "$count" -gt "${SLEEP_FAIL_AFTER:-999}" ]; then
    exit 99
fi
if [ "${SLEEP_SEND_TERM:-0}" -eq 1 ]; then
    kill -TERM "$PPID"
fi
exit 0
""",
    )

    env = _WorkerEnv(os.environ.copy())
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "BROKER_CALL_LOG": str(broker_log),
            "BROKER_ARGV_LOG": str(broker_argv_log),
            "BROKER_STATE": str(tmp_path / "broker.state"),
            "HANDLER_CALL_LOG": str(handler_log),
            "HANDLER_ARGC_LOG": str(handler_argc_log),
            "SLEEP_CALL_LOG": str(sleep_log),
            "BROKER_DB": str(tmp_path / "broker.db"),
            "CHECKPOINT_FILE": str(tmp_path / "checkpoint"),
            "BROKER_SCENARIO": "message",
            "BROKER_MESSAGE_ID": MESSAGE_ID,
            "SLEEP_FAIL_AFTER": "0",
        }
    )
    return env


def _run_worker(
    script: Path, tmp_path: Path, env: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(script)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def _calls(env: dict[str, str]) -> list[str]:
    path = Path(env["BROKER_CALL_LOG"])
    return path.read_text(encoding="utf-8").splitlines() if path.exists() else []


def _broker_argv(env: dict[str, str]) -> list[list[str]]:
    path = Path(env["BROKER_ARGV_LOG"])
    if not path.exists():
        return []
    return [line.split("\t") for line in path.read_text(encoding="utf-8").splitlines()]


def _set_handler(script: Path, env: dict[str, str], command: str) -> None:
    env["PROCESS_TASK" if script == SAFE_WORKER else "PROCESS_EVENT"] = command


def test_safe_worker_requires_handler_before_broker_call(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    result = _run_worker(SAFE_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "PROCESS_TASK" in result.stderr
    assert _calls(worker_env) == []


def test_safe_worker_rejects_handler_with_embedded_arguments(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    worker_env["PROCESS_TASK"] = "handler-ok --unsafe-split"

    result = _run_worker(SAFE_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "one executable" in result.stderr
    assert _calls(worker_env) == []


def test_safe_worker_stops_on_processing_failure(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    worker_env["PROCESS_TASK"] = "handler-fail"

    result = _run_worker(SAFE_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "next run" in result.stderr
    assert _calls(worker_env) == ["peek"]
    assert Path(worker_env["HANDLER_CALL_LOG"]).read_text(encoding="utf-8") == "do work"
    assert Path(worker_env["HANDLER_ARGC_LOG"]).read_text(encoding="utf-8") == "0"


def test_safe_worker_stops_and_warns_on_delete_failure(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    worker_env.update(
        {"PROCESS_TASK": "handler-ok", "BROKER_SCENARIO": "delete_failure"}
    )

    result = _run_worker(SAFE_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "simulated delete failure" in result.stderr
    assert "reprocessed" in result.stderr
    assert _calls(worker_env) == ["peek", "delete"]
    assert _broker_argv(worker_env) == [
        ["peek", "peek", "tasks", "--json"],
        ["delete", "delete", "tasks", "-m", MESSAGE_ID],
    ]


def test_safe_worker_preserves_peek_failure(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    worker_env.update({"PROCESS_TASK": "handler-ok", "BROKER_SCENARIO": "peek_failure"})

    result = _run_worker(SAFE_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "simulated backend failure" in result.stderr
    assert "No new messages" not in result.stdout
    assert _calls(worker_env) == ["peek"]


def test_safe_worker_treats_only_exit_two_as_empty(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    worker_env.update(
        {
            "PROCESS_TASK": "handler-ok",
            "BROKER_SCENARIO": "empty_then_error",
            "SLEEP_FAIL_AFTER": "1",
        }
    )

    result = _run_worker(SAFE_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert result.stdout.count("No new messages") == 1
    assert "simulated backend failure after idle" in result.stderr
    assert _calls(worker_env) == ["peek", "peek"]


def test_safe_worker_rejects_successful_empty_peek(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    worker_env.update(
        {"PROCESS_TASK": "handler-ok", "BROKER_SCENARIO": "empty_success"}
    )

    result = _run_worker(SAFE_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "empty output" in result.stderr
    assert "No new messages" not in result.stdout


def test_safe_worker_rejects_invalid_json(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    worker_env.update({"PROCESS_TASK": "handler-ok", "BROKER_SCENARIO": "invalid_json"})

    result = _run_worker(SAFE_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "invalid JSON" in result.stderr
    assert _calls(worker_env) == ["peek"]


@pytest.mark.parametrize("script", [SAFE_WORKER, RESILIENT_WORKER])
def test_worker_preserves_trailing_newlines_on_handler_stdin(
    script: Path, tmp_path: Path, worker_env: dict[str, str]
) -> None:
    _set_handler(script, worker_env, "handler-fail")
    worker_env["BROKER_SCENARIO"] = "trailing_newlines"

    result = _run_worker(script, tmp_path, worker_env)

    assert result.returncode == 1
    assert Path(worker_env["HANDLER_CALL_LOG"]).read_bytes() == b"line one\n\n"
    assert Path(worker_env["HANDLER_ARGC_LOG"]).read_text(encoding="utf-8") == "0"
    assert _calls(worker_env) == ["peek"]


@pytest.mark.parametrize("script", [SAFE_WORKER, RESILIENT_WORKER])
def test_worker_rejects_nul_before_handler_or_acknowledgement(
    script: Path, tmp_path: Path, worker_env: dict[str, str]
) -> None:
    _set_handler(script, worker_env, "handler-ok")
    worker_env["BROKER_SCENARIO"] = "nul_message"

    result = _run_worker(script, tmp_path, worker_env)

    assert result.returncode == 1
    assert "NUL payload" in result.stderr
    assert not Path(worker_env["HANDLER_CALL_LOG"]).exists()
    assert _calls(worker_env) == ["peek"]


@pytest.mark.parametrize("script", [SAFE_WORKER, RESILIENT_WORKER])
def test_worker_streams_large_valid_message_to_handler_stdin(
    script: Path, tmp_path: Path, worker_env: dict[str, str]
) -> None:
    _set_handler(script, worker_env, "handler-fail")
    worker_env["BROKER_SCENARIO"] = "large_message"

    result = _run_worker(script, tmp_path, worker_env)

    assert result.returncode == 1
    assert Path(worker_env["HANDLER_CALL_LOG"]).stat().st_size == 3 * 1024 * 1024
    assert Path(worker_env["HANDLER_ARGC_LOG"]).read_text(encoding="utf-8") == "0"
    assert _calls(worker_env) == ["peek"]


@pytest.mark.parametrize("script", [SAFE_WORKER, RESILIENT_WORKER])
def test_worker_uses_handler_status_when_success_handler_closes_stdin_early(
    script: Path, tmp_path: Path, worker_env: dict[str, str]
) -> None:
    _set_handler(script, worker_env, "handler-ignore-stdin")
    worker_env["BROKER_SCENARIO"] = "large_message"

    result = _run_worker(script, tmp_path, worker_env)

    assert result.returncode == 1
    assert "failed to delete" in result.stderr
    assert "Error processing" not in result.stderr
    assert Path(worker_env["HANDLER_ARGC_LOG"]).read_text(encoding="utf-8") == "0"
    assert _calls(worker_env) == ["peek", "delete"]


@pytest.mark.parametrize("script", [SAFE_WORKER, RESILIENT_WORKER])
@pytest.mark.parametrize(
    "message_id", ["0000000000000000000", "0000000000000000001", MAX_MESSAGE_ID]
)
def test_worker_accepts_full_public_id_range_as_canonical_json_string(
    script: Path,
    tmp_path: Path,
    worker_env: dict[str, str],
    message_id: str,
) -> None:
    _set_handler(script, worker_env, "handler-ok")
    worker_env.update(
        {"BROKER_MESSAGE_ID": message_id, "BROKER_SCENARIO": "delete_failure"}
    )

    result = _run_worker(script, tmp_path, worker_env)

    assert result.returncode == 1
    delete_argv = _broker_argv(worker_env)[1]
    assert delete_argv[-1] == message_id


@pytest.mark.parametrize("script", [SAFE_WORKER, RESILIENT_WORKER])
@pytest.mark.parametrize("message_id", ["-1", str(2**63), "10000000000000000000"])
def test_worker_rejects_strings_outside_public_id_range(
    script: Path,
    tmp_path: Path,
    worker_env: dict[str, str],
    message_id: str,
) -> None:
    _set_handler(script, worker_env, "handler-ok")
    worker_env["BROKER_MESSAGE_ID"] = message_id

    result = _run_worker(script, tmp_path, worker_env)

    assert result.returncode == 1
    assert "invalid" in result.stderr
    assert _calls(worker_env) == ["peek"]
    assert not Path(worker_env["HANDLER_CALL_LOG"]).exists()


@pytest.mark.parametrize("script", [SAFE_WORKER, RESILIENT_WORKER])
def test_worker_rejects_numeric_timestamp_token(
    script: Path, tmp_path: Path, worker_env: dict[str, str]
) -> None:
    _set_handler(script, worker_env, "handler-ok")
    worker_env["BROKER_SCENARIO"] = "numeric_timestamp"

    result = _run_worker(script, tmp_path, worker_env)

    assert result.returncode == 1
    assert "invalid message JSON" in result.stderr
    assert _calls(worker_env) == ["peek"]


@pytest.mark.parametrize("script", [SAFE_WORKER, RESILIENT_WORKER])
def test_worker_does_not_require_jq_1_7_for_message_id_precision(
    script: Path, tmp_path: Path, worker_env: dict[str, str]
) -> None:
    real_jq = shutil.which("jq")
    if real_jq is None:
        pytest.skip("jq is required to exercise the published worker")
    fake_bin = tmp_path / "jq-without-version"
    fake_bin.mkdir()
    _write_executable(
        fake_bin / "jq",
        f"""#!/bin/bash
if [ "${1:-}" = "--version" ]; then
    echo "worker must not inspect jq version" >&2
    exit 99
fi
exec "{real_jq}" "$@"
""",
    )
    _set_handler(script, worker_env, "handler-ok")
    worker_env["BROKER_SCENARIO"] = "delete_failure"
    worker_env["PATH"] = f"{fake_bin}:{worker_env['PATH']}"

    result = _run_worker(script, tmp_path, worker_env)

    assert result.returncode == 1
    assert "worker must not inspect jq version" not in result.stderr
    assert _calls(worker_env) == ["peek", "delete"]


def test_safe_worker_consumes_real_broker_string_id_without_precision_loss(
    tmp_path: Path,
    worker_env: dict[str, str],
) -> None:
    real_broker_bin = tmp_path / "real-broker-bin"
    real_broker_bin.mkdir()
    _write_executable(
        real_broker_bin / "broker",
        f"""#!/bin/bash
exec env PYTHONPATH={shlex.quote(str(REPO_ROOT))} \
    {shlex.quote(sys.executable)} -m simplebroker "$@"
""",
    )
    message_id = 1234567890123456789
    with Queue("tasks", db_path=str(tmp_path / ".broker.db")) as queue:
        queue.insert_messages([("real broker body", message_id)])

    handler = Path(worker_env["HANDLER_CALL_LOG"]).parent / "bin" / "handler-fail"
    worker_env["PROCESS_TASK"] = str(handler)
    worker_env["PATH"] = f"{real_broker_bin}:{os.environ['PATH']}"

    result = _run_worker(SAFE_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert f"Processing message ID: {message_id}" in result.stdout
    assert Path(worker_env["HANDLER_CALL_LOG"]).read_text(encoding="utf-8") == (
        "real broker body"
    )


def test_worker_docs_do_not_state_obsolete_jq_precision_floor() -> None:
    for path in (REPO_ROOT / "README.md", EXAMPLES_README, AGENT_KERNEL):
        text = path.read_text(encoding="utf-8")
        assert "jq 1.7+" not in text


def test_resilient_worker_preserves_operational_peek_failure(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    worker_env["BROKER_SCENARIO"] = "peek_failure"

    result = _run_worker(RESILIENT_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "simulated backend failure" in result.stderr
    assert "No new messages" not in result.stdout
    assert _calls(worker_env) == ["peek"]


def test_resilient_worker_treats_only_exit_two_as_empty(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    worker_env.update({"BROKER_SCENARIO": "empty_then_error", "SLEEP_FAIL_AFTER": "1"})

    result = _run_worker(RESILIENT_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert result.stdout.count("No new messages") == 1
    assert "simulated backend failure after idle" in result.stderr
    assert _calls(worker_env) == ["peek", "peek"]


def test_resilient_worker_rejects_successful_empty_peek(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    worker_env["BROKER_SCENARIO"] = "empty_success"

    result = _run_worker(RESILIENT_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "empty output" in result.stderr
    assert "No new messages" not in result.stdout


def test_resilient_worker_rejects_invalid_json(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    worker_env["BROKER_SCENARIO"] = "invalid_json"

    result = _run_worker(RESILIENT_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "invalid JSON" in result.stderr
    assert _calls(worker_env) == ["peek"]


def test_resilient_worker_preserves_delete_failure_and_exact_id(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    worker_env.update(
        {"PROCESS_EVENT": "handler-ok", "BROKER_SCENARIO": "delete_failure"}
    )

    result = _run_worker(RESILIENT_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "simulated delete failure" in result.stderr
    assert "reprocessed" in result.stderr
    assert _calls(worker_env) == ["peek", "delete"]
    delete_argv = _broker_argv(worker_env)[1]
    assert delete_argv[-4:] == ["delete", "events", "-m", MESSAGE_ID]
    peek_argv = _broker_argv(worker_env)[0]
    assert "--after" not in peek_argv


def test_resilient_worker_does_not_skip_id_behind_checkpoint(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    Path(worker_env["CHECKPOINT_FILE"]).write_text(MESSAGE_ID, encoding="utf-8")
    worker_env.update(
        {
            "BROKER_MESSAGE_ID": "0000000000000000001",
            "BROKER_SCENARIO": "delete_failure",
            "PROCESS_EVENT": "handler-ok",
        }
    )

    result = _run_worker(RESILIENT_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert _calls(worker_env) == ["peek", "delete"]
    peek_argv, delete_argv = _broker_argv(worker_env)
    assert "--after" not in peek_argv
    assert delete_argv[-1] == "0000000000000000001"


def test_resilient_worker_checkpoint_write_failure_is_fatal_after_ack(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    worker_env.update(
        {
            "PROCESS_EVENT": "handler-ok",
            "CHECKPOINT_FILE": str(tmp_path / "missing" / "checkpoint"),
        }
    )

    result = _run_worker(RESILIENT_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "failed to write checkpoint" in result.stderr
    assert _calls(worker_env) == ["peek", "delete"]
    assert not (tmp_path / "missing" / "checkpoint").exists()


@pytest.mark.parametrize("checkpoint", ["123", "not-an-id"])
def test_resilient_worker_rejects_corrupt_checkpoint_before_broker_call(
    tmp_path: Path, worker_env: dict[str, str], checkpoint: str
) -> None:
    Path(worker_env["CHECKPOINT_FILE"]).write_text(checkpoint, encoding="utf-8")

    result = _run_worker(RESILIENT_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "checkpoint" in result.stderr.lower()
    assert _calls(worker_env) == []


_get_effective_uid = getattr(os, "geteuid", None)


@pytest.mark.skipif(
    not callable(_get_effective_uid) or _get_effective_uid() == 0,
    reason="requires Unix mode permissions and a non-root effective user",
)
def test_resilient_worker_rejects_unreadable_checkpoint(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    checkpoint = Path(worker_env["CHECKPOINT_FILE"])
    checkpoint.write_text(MESSAGE_ID, encoding="utf-8")
    checkpoint.chmod(0)

    result = _run_worker(RESILIENT_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "checkpoint" in result.stderr.lower()
    assert _calls(worker_env) == []


def test_worker_test_module_collects_when_os_has_no_geteuid(tmp_path: Path) -> None:
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import os, runpy\n"
                "if hasattr(os, 'geteuid'):\n"
                "    del os.geteuid\n"
                f"runpy.run_path({str(Path(__file__))!r}, run_name='worker_probe')"
            ),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert probe.returncode == 0, probe.stderr


def test_resilient_worker_trap_makes_checkpoint_failure_explicit() -> None:
    source = RESILIENT_WORKER.read_text(encoding="utf-8")

    assert "save_checkpoint || exit 1; exit 0" in source


def test_resilient_worker_signal_checkpoint_failure_exits_nonzero(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    worker_env.update(
        {
            "BROKER_SCENARIO": "empty_then_error",
            "CHECKPOINT_FILE": str(tmp_path / "missing" / "checkpoint"),
            "SLEEP_FAIL_AFTER": "999",
            "SLEEP_SEND_TERM": "1",
        }
    )

    result = _run_worker(RESILIENT_WORKER, tmp_path, worker_env)

    assert result.returncode != 0
    assert "failed to write checkpoint" in result.stderr
    assert _calls(worker_env) == ["peek"]


def test_resilient_worker_rejects_broken_checkpoint_symlink(
    tmp_path: Path, worker_env: dict[str, str]
) -> None:
    Path(worker_env["CHECKPOINT_FILE"]).symlink_to(tmp_path / "missing-checkpoint")

    result = _run_worker(RESILIENT_WORKER, tmp_path, worker_env)

    assert result.returncode == 1
    assert "checkpoint" in result.stderr.lower()
    assert _calls(worker_env) == []
