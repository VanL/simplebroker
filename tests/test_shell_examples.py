"""Black-box contracts for the three interactive Bash demonstrations."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from simplebroker import Queue

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_README = REPO_ROOT / "examples" / "README.md"
QUEUE_MIGRATION = REPO_ROOT / "examples" / "queue_migration.sh"
DEAD_LETTER_QUEUE = REPO_ROOT / "examples" / "dead_letter_queue.sh"
WORK_STEALING = REPO_ROOT / "examples" / "work_stealing.sh"
MENU_SCRIPTS = (QUEUE_MIGRATION, DEAD_LETTER_QUEUE, WORK_STEALING)

OLD_ID = 1_699_999_999_000_000_000
NEW_ID = 1_700_000_001_000_000_000
RETRY_ID = "1700000000000000001"
SECOND_RETRY_ID = "1700000000000000002"


class _ShellEnv(dict[str, str]):
    """Environment mapping whose failure representation hides host secrets."""

    def __repr__(self) -> str:
        return "<shell example test environment>"


def _write_executable(path: Path, source: str) -> None:
    path.write_text(source, encoding="utf-8")
    path.chmod(0o755)


def _require_shell_tools() -> None:
    if os.name == "nt":
        pytest.skip("published menu scripts are illustrative Bash examples")
    if shutil.which("bash") is None or shutil.which("jq") is None:
        pytest.skip("Bash and jq are required to exercise the shell examples")


@pytest.fixture
def menu_env(tmp_path: Path) -> dict[str, str]:
    _require_shell_tools()
    bin_dir = tmp_path / "menu-bin"
    bin_dir.mkdir()
    call_log = tmp_path / "menu-broker.log"

    _write_executable(
        bin_dir / "broker",
        """#!/bin/bash
printf '%s' "$1" >> "$BROKER_CALL_LOG"
for argument in "$@"; do
    printf '\t%s' "$argument" >> "$BROKER_CALL_LOG"
done
printf '\n' >> "$BROKER_CALL_LOG"
exit 0
""",
    )
    _write_executable(
        bin_dir / "date",
        """#!/bin/bash
case " $* " in
    *" +%s "*) printf '%s\n' '1700000000' ;;
    *) printf '%s\n' '2026-08-24' ;;
esac
""",
    )

    env = _ShellEnv(os.environ.copy())
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "BROKER_CALL_LOG": str(call_log),
        }
    )
    return env


def _run_menu(
    script: Path,
    tmp_path: Path,
    env: dict[str, str],
    *args: str,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(script), *args],
        cwd=tmp_path,
        env=env,
        input=input_text,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )


@pytest.mark.parametrize(
    ("script", "selector", "expected_call_count"),
    [
        (QUEUE_MIGRATION, "9", 15),
        (DEAD_LETTER_QUEUE, "6", 4),
        (WORK_STEALING, "8", 60),
    ],
)
def test_menu_selector_argument_dispatches_without_reading_stdin(
    script: Path,
    selector: str,
    expected_call_count: int,
    tmp_path: Path,
    menu_env: dict[str, str],
) -> None:
    result = _run_menu(script, tmp_path, menu_env, selector)

    assert result.returncode == 0, result.stderr
    calls = Path(menu_env["BROKER_CALL_LOG"]).read_text(encoding="utf-8").splitlines()
    assert len(calls) == expected_call_count


@pytest.mark.parametrize(
    ("script", "selector"),
    [
        (QUEUE_MIGRATION, "9"),
        (DEAD_LETTER_QUEUE, "6"),
        (WORK_STEALING, "8"),
    ],
)
def test_menu_selector_prompts_only_when_argument_is_absent(
    script: Path,
    selector: str,
    tmp_path: Path,
    menu_env: dict[str, str],
) -> None:
    result = _run_menu(script, tmp_path, menu_env, input_text=f"{selector}\n")

    assert result.returncode == 0, result.stderr
    assert Path(menu_env["BROKER_CALL_LOG"]).is_file()


@pytest.mark.parametrize("script", MENU_SCRIPTS)
def test_menu_selector_rejects_invalid_argument(
    script: Path, tmp_path: Path, menu_env: dict[str, str]
) -> None:
    result = _run_menu(script, tmp_path, menu_env, "not-a-menu-item")

    assert result.returncode != 0
    assert "Invalid choice" in result.stderr
    assert not Path(menu_env["BROKER_CALL_LOG"]).exists()


def test_queue_migration_moves_only_messages_older_than_cutoff(
    tmp_path: Path,
) -> None:
    _require_shell_tools()
    bin_dir = tmp_path / "real-broker-bin"
    bin_dir.mkdir()
    _write_executable(
        bin_dir / "broker",
        f"""#!/bin/bash
exec env PYTHONPATH={shlex.quote(str(REPO_ROOT))} \
    {shlex.quote(sys.executable)} -m simplebroker "$@"
""",
    )

    db_path = tmp_path / ".broker.db"
    with Queue("source", db_path=str(db_path)) as source:
        source.insert_messages([("older", OLD_ID), ("newer", NEW_ID)])

    env = _ShellEnv(os.environ.copy())
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    result = _run_menu(
        QUEUE_MIGRATION,
        tmp_path,
        env,
        "3",
        input_text="source\ndestination\n1700000000\n",
    )

    assert result.returncode == 0, result.stderr
    with Queue("source", db_path=str(db_path)) as source:
        assert list(source.peek_generator(with_timestamps=True)) == [("newer", NEW_ID)]
    with Queue("destination", db_path=str(db_path)) as destination:
        assert list(destination.peek_generator(with_timestamps=True)) == [
            ("older", OLD_ID)
        ]


@pytest.fixture
def retry_env(tmp_path: Path) -> dict[str, str]:
    _require_shell_tools()
    bin_dir = tmp_path / "retry-bin"
    bin_dir.mkdir()
    temp_dir = tmp_path / "retry-tmp"
    temp_dir.mkdir()

    _write_executable(
        bin_dir / "broker",
        """#!/bin/bash
set -u
operation="$1"
printf '%s' "$operation" >> "$BROKER_ARGV_LOG"
for argument in "$@"; do
    printf '\t%s' "$argument" >> "$BROKER_ARGV_LOG"
done
printf '\n' >> "$BROKER_ARGV_LOG"

case "$operation" in
    peek)
        printf '%s\n' 'peek-start' >> "$BROKER_EVENT_LOG"
        if [ "${BROKER_SCENARIO:-success}" = "signal" ]; then
            kill -TERM "$PPID"
            exit 143
        fi
        cat "$BROKER_SNAPSHOT"
        printf '%s\n' 'peek-complete' >> "$BROKER_EVENT_LOG"
        if [ "${BROKER_SCENARIO:-success}" = "peek-empty" ]; then
            exit 2
        fi
        ;;
    write)
        printf '%s\n' 'write' >> "$BROKER_EVENT_LOG"
        cat > "$BROKER_WRITE_LOG"
        if [ "${BROKER_SCENARIO:-success}" = "write-failure" ]; then
            echo "simulated retry write failure" >&2
            exit 1
        fi
        ;;
    delete)
        printf '%s\n' 'delete' >> "$BROKER_EVENT_LOG"
        if [ "${BROKER_SCENARIO:-success}" = "delete-failure" ]; then
            echo "simulated retry delete failure" >&2
            exit 1
        fi
        ;;
    move)
        printf '%s\n' 'move' >> "$BROKER_EVENT_LOG"
        ;;
    *)
        echo "unexpected broker call: $*" >&2
        exit 99
        ;;
esac
""",
    )
    _write_executable(
        bin_dir / "date",
        """#!/bin/bash
printf '%s\n' "${DATE_NOW:-1000}"
""",
    )

    snapshot = tmp_path / "retry.jsonl"
    snapshot.write_text("", encoding="utf-8")
    env = _ShellEnv(os.environ.copy())
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "TMPDIR": str(temp_dir),
            "BROKER_ARGV_LOG": str(tmp_path / "retry-argv.log"),
            "BROKER_EVENT_LOG": str(tmp_path / "retry-events.log"),
            "BROKER_WRITE_LOG": str(tmp_path / "retry-write.log"),
            "BROKER_SNAPSHOT": str(snapshot),
            "BROKER_SCENARIO": "success",
            "DATE_NOW": "1000",
        }
    )
    return env


def _retry_record(payload: object, timestamp: str = RETRY_ID) -> str:
    message = payload if isinstance(payload, str) else json.dumps(payload)
    return json.dumps({"message": message, "timestamp": timestamp})


def _set_retry_records(env: dict[str, str], *records: str) -> None:
    content = "\n".join(records)
    if content:
        content += "\n"
    Path(env["BROKER_SNAPSHOT"]).write_text(content, encoding="utf-8")


def _run_retry_once(
    tmp_path: Path, env: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; process_retry_queue_once',
            "retry-example",
            str(DEAD_LETTER_QUEUE),
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def _log_lines(env: dict[str, str], key: str) -> list[str]:
    path = Path(env[key])
    return path.read_text(encoding="utf-8").splitlines() if path.exists() else []


def _assert_retry_snapshot_removed(env: dict[str, str]) -> None:
    assert list(Path(env["TMPDIR"]).glob("simplebroker-retry.*")) == []


def test_retry_scan_snapshots_before_reschedule_and_preserves_nested_message(
    tmp_path: Path, retry_env: dict[str, str]
) -> None:
    _set_retry_records(
        retry_env,
        _retry_record(
            {"original": "please fail\nsecond line", "next_retry": 0, "attempts": 1}
        ),
    )

    result = _run_retry_once(tmp_path, retry_env)

    assert result.returncode == 0, result.stderr
    assert _log_lines(retry_env, "BROKER_EVENT_LOG") == [
        "peek-start",
        "peek-complete",
        "write",
        "delete",
    ]
    assert _log_lines(retry_env, "BROKER_ARGV_LOG") == [
        "peek\tpeek\tretry_queue\t--all\t--json",
        "write\twrite\tretry_queue\t-",
        f"delete\tdelete\tretry_queue\t-m\t{RETRY_ID}",
    ]
    replacement = json.loads(
        Path(retry_env["BROKER_WRITE_LOG"]).read_text(encoding="utf-8")
    )
    assert replacement == {
        "original": "please fail\nsecond line",
        "next_retry": 1120,
        "attempts": 2,
    }
    _assert_retry_snapshot_removed(retry_env)


def test_retry_scan_moves_exhausted_retry_to_permanent_dlq(
    tmp_path: Path, retry_env: dict[str, str]
) -> None:
    _set_retry_records(
        retry_env,
        _retry_record({"original": "still fail", "next_retry": 0, "attempts": 3}),
    )

    result = _run_retry_once(tmp_path, retry_env)

    assert result.returncode == 0, result.stderr
    assert _log_lines(retry_env, "BROKER_EVENT_LOG") == [
        "peek-start",
        "peek-complete",
        "move",
    ]
    assert _log_lines(retry_env, "BROKER_ARGV_LOG")[-1] == (
        f"move\tmove\tretry_queue\tfailed\t-m\t{RETRY_ID}"
    )
    _assert_retry_snapshot_removed(retry_env)


def test_retry_scan_deletes_successful_retry_by_exact_id(
    tmp_path: Path, retry_env: dict[str, str]
) -> None:
    _set_retry_records(
        retry_env,
        _retry_record({"original": "works", "next_retry": 0, "attempts": 1}),
    )

    result = _run_retry_once(tmp_path, retry_env)

    assert result.returncode == 0, result.stderr
    assert _log_lines(retry_env, "BROKER_EVENT_LOG") == [
        "peek-start",
        "peek-complete",
        "delete",
    ]
    _assert_retry_snapshot_removed(retry_env)


@pytest.mark.parametrize(
    "bad_payload",
    [
        "not-json",
        "[]",
        {"original": 7, "next_retry": 0, "attempts": 1},
        {"original": "fail", "next_retry": -1, "attempts": 1},
        {"original": "fail", "next_retry": 1.5, "attempts": 1},
        {"original": "fail", "next_retry": "1", "attempts": 1},
        {"original": "fail", "next_retry": None, "attempts": 1},
        {"original": "fail", "next_retry": True, "attempts": 1},
        {"original": "fail", "next_retry": 0, "attempts": 0},
        {"original": "fail", "next_retry": 0, "attempts": 1.5},
        {"original": "fail", "next_retry": 0, "attempts": "1"},
        {"original": "fail", "next_retry": 0, "attempts": None},
        {"original": "fail", "next_retry": 0, "attempts": True},
    ],
)
def test_retry_scan_rejects_malformed_payload_before_any_mutation(
    bad_payload: object, tmp_path: Path, retry_env: dict[str, str]
) -> None:
    _set_retry_records(
        retry_env,
        _retry_record({"original": "first fail", "next_retry": 0, "attempts": 1}),
        _retry_record(bad_payload, SECOND_RETRY_ID),
    )

    result = _run_retry_once(tmp_path, retry_env)

    assert result.returncode != 0
    assert SECOND_RETRY_ID in result.stderr
    assert "left unmodified" in result.stderr
    assert _log_lines(retry_env, "BROKER_EVENT_LOG") == [
        "peek-start",
        "peek-complete",
    ]
    _assert_retry_snapshot_removed(retry_env)


@pytest.mark.parametrize(
    "bad_record",
    [
        "not-json",
        json.dumps({"timestamp": SECOND_RETRY_ID}),
        json.dumps({"message": "{}", "timestamp": 7}),
    ],
)
def test_retry_scan_rejects_malformed_outer_envelope(
    bad_record: str, tmp_path: Path, retry_env: dict[str, str]
) -> None:
    _set_retry_records(retry_env, bad_record)

    result = _run_retry_once(tmp_path, retry_env)

    assert result.returncode != 0
    assert "Malformed retry record" in result.stderr
    assert _log_lines(retry_env, "BROKER_EVENT_LOG") == [
        "peek-start",
        "peek-complete",
    ]
    _assert_retry_snapshot_removed(retry_env)


def test_retry_scan_accepts_integral_json_numbers(
    tmp_path: Path, retry_env: dict[str, str]
) -> None:
    _set_retry_records(
        retry_env,
        _retry_record({"original": "later", "next_retry": 2000.0, "attempts": 1.0}),
    )

    result = _run_retry_once(tmp_path, retry_env)

    assert result.returncode == 0, result.stderr
    assert _log_lines(retry_env, "BROKER_EVENT_LOG") == [
        "peek-start",
        "peek-complete",
    ]
    _assert_retry_snapshot_removed(retry_env)


def test_retry_scan_stops_without_delete_when_replacement_write_fails(
    tmp_path: Path, retry_env: dict[str, str]
) -> None:
    retry_env["BROKER_SCENARIO"] = "write-failure"
    _set_retry_records(
        retry_env,
        _retry_record({"original": "fail", "next_retry": 0, "attempts": 1}),
    )

    result = _run_retry_once(tmp_path, retry_env)

    assert result.returncode != 0
    assert "original left in place" in result.stderr
    assert _log_lines(retry_env, "BROKER_EVENT_LOG") == [
        "peek-start",
        "peek-complete",
        "write",
    ]
    _assert_retry_snapshot_removed(retry_env)


def test_retry_scan_reports_duplicate_risk_when_old_delete_fails(
    tmp_path: Path, retry_env: dict[str, str]
) -> None:
    retry_env["BROKER_SCENARIO"] = "delete-failure"
    _set_retry_records(
        retry_env,
        _retry_record({"original": "fail", "next_retry": 0, "attempts": 1}),
    )

    result = _run_retry_once(tmp_path, retry_env)

    assert result.returncode != 0
    assert "duplicate/retry risk" in result.stderr
    assert _log_lines(retry_env, "BROKER_EVENT_LOG") == [
        "peek-start",
        "peek-complete",
        "write",
        "delete",
    ]
    _assert_retry_snapshot_removed(retry_env)


def test_retry_snapshot_is_removed_when_signal_interrupts_peek(
    tmp_path: Path, retry_env: dict[str, str]
) -> None:
    retry_env["BROKER_SCENARIO"] = "signal"

    result = _run_retry_once(tmp_path, retry_env)

    assert result.returncode != 0
    _assert_retry_snapshot_removed(retry_env)


def test_menu_example_docs_keep_demo_scope_and_document_selector_arguments() -> None:
    text = EXAMPLES_README.read_text(encoding="utf-8")

    assert "demonstration purposes only" in text
    for example in (
        "./dead_letter_queue.sh 3",
        "./queue_migration.sh 3",
        "./work_stealing.sh 8",
    ):
        assert example in text
