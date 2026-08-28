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


def _real_broker_env(tmp_path: Path) -> dict[str, str]:
    bin_dir = tmp_path / "real-broker-bin"
    bin_dir.mkdir()
    _write_executable(
        bin_dir / "broker",
        f"""#!/bin/bash
exec env PYTHONPATH={shlex.quote(str(REPO_ROOT))} \
    {shlex.quote(sys.executable)} -m simplebroker "$@"
""",
    )
    env = _ShellEnv(os.environ.copy())
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    return env


@pytest.fixture
def shell_control_env(tmp_path: Path) -> dict[str, str]:
    _require_shell_tools()
    bin_dir = tmp_path / "shell-control-bin"
    bin_dir.mkdir()
    _write_executable(
        bin_dir / "broker",
        """#!/bin/bash
set -u
printf '%s' "$1" >> "$BROKER_CALL_LOG"
for argument in "${@:2}"; do
    printf '\t%s' "$argument" >> "$BROKER_CALL_LOG"
done
printf '\n' >> "$BROKER_CALL_LOG"

case "${BROKER_FAKE_MODE}:$1" in
    simple-empty:peek) exit 2 ;;
    simple-empty:move) exit 2 ;;
    empty-then-error:peek)
        count=0
        if [ -f "$BROKER_COUNT_FILE" ]; then count=$(cat "$BROKER_COUNT_FILE"); fi
        count=$((count + 1))
        printf '%s\n' "$count" > "$BROKER_COUNT_FILE"
        if [ "$count" -eq 1 ]; then exit 2; fi
        echo "simulated peek failure" >&2
        exit 1
        ;;
    move-empty-then-error:move)
        count=0
        if [ -f "$BROKER_COUNT_FILE" ]; then count=$(cat "$BROKER_COUNT_FILE"); fi
        count=$((count + 1))
        printf '%s\n' "$count" > "$BROKER_COUNT_FILE"
        if [ "$count" -eq 1 ]; then exit 2; fi
        echo "simulated move failure" >&2
        exit 1
        ;;
    controlled-move:stats) printf '%s\n' "$BROKER_STATS_JSON" ;;
    controlled-move:move) exit "$BROKER_MOVE_STATUS" ;;
    stats-valid:stats) printf '%s\n' "$BROKER_STATS_JSON" ;;
    stats-malformed:stats) printf '%s\n' '{"pending":"unknown"}' ;;
    stats-missing:stats) printf '%s\n' '{"queue":"jobs","total":0}' ;;
    stats-error:stats) echo "simulated stats failure" >&2; exit 1 ;;
    peek-empty:peek) exit 2 ;;
    peek-error:peek) echo "simulated peek failure" >&2; exit 1 ;;
    peek-malformed:peek) printf '%s\n' '{"message":7,"timestamp":"1700000000000000000"}' ;;
    peek-bad-id:peek) printf '%s\n' '{"message":"work","timestamp":"9999999999999999999"}' ;;
    replacement-delete-error:peek)
        printf '%s\n' '{"message":"fail","timestamp":"1700000000000000000"}'
        ;;
    replacement-delete-error:write) cat >/dev/null ;;
    replacement-delete-error:delete)
        echo "simulated source delete failure" >&2
        exit 1
        ;;
    rename:rename) exit 0 ;;
    bound:move) exit 0 ;;
    backup:stats) printf '%s\n' '{"queue":"source","pending":2,"claimed":1,"total":3,"exists":true}' ;;
    backup:dump)
        printf '%s\n' '{"format":"simplebroker-dump","version":1,"created_at_ns":1,"source_last_ts":1}'
        exit 0
        ;;
    dump-error:stats) printf '%s\n' "$BROKER_STATS_JSON" ;;
    dump-error:dump) echo 'partial dump'; exit 1 ;;
    *) echo "unexpected broker call: $*" >&2; exit 99 ;;
esac
""",
    )
    _write_executable(
        bin_dir / "sleep",
        """#!/bin/bash
exit 0
""",
    )
    env = _ShellEnv(os.environ.copy())
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "BROKER_CALL_LOG": str(tmp_path / "shell-control.log"),
            "BROKER_COUNT_FILE": str(tmp_path / "shell-control-count"),
            "BROKER_FAKE_MODE": "stats-valid",
            "BROKER_MOVE_STATUS": "0",
            "BROKER_STATS_JSON": (
                '{"queue":"jobs","pending":7,"claimed":2,"total":9,"exists":true}'
            ),
        }
    )
    return env


def _run_sourced_function(
    script: Path,
    tmp_path: Path,
    env: dict[str, str],
    function: str,
    *args: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; "$2" "${@:3}"',
            "shell-example",
            str(script),
            function,
            *args,
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


@pytest.mark.parametrize(
    "script",
    [DEAD_LETTER_QUEUE, QUEUE_MIGRATION, WORK_STEALING],
)
def test_queue_depth_uses_validated_json_stats(
    script: Path,
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = "stats-valid"

    result = _run_sourced_function(
        script, tmp_path, shell_control_env, "queue_depth", "jobs"
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "7"
    assert _log_lines(shell_control_env, "BROKER_CALL_LOG") == ["stats\tjobs\t--json"]


@pytest.mark.parametrize(
    ("mode", "expected_error"),
    [
        ("stats-malformed", "invalid stats"),
        ("stats-missing", "invalid stats"),
        ("stats-error", "could not read stats"),
    ],
)
@pytest.mark.parametrize(
    "script",
    [DEAD_LETTER_QUEUE, QUEUE_MIGRATION, WORK_STEALING],
)
def test_queue_depth_fails_instead_of_coercing_bad_stats_to_zero(
    script: Path,
    mode: str,
    expected_error: str,
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = mode

    result = _run_sourced_function(
        script, tmp_path, shell_control_env, "queue_depth", "jobs"
    )

    assert result.returncode != 0
    assert expected_error in result.stderr


def test_load_distribution_stops_on_stats_failure(
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = "stats-error"

    result = _run_sourced_function(
        WORK_STEALING,
        tmp_path,
        shell_control_env,
        "load_based_distribution",
    )

    assert result.returncode != 0
    assert "could not read stats" in result.stderr
    assert _log_lines(shell_control_env, "BROKER_CALL_LOG") == [
        "stats\tworker1-tasks\t--json"
    ]


def test_round_robin_idles_on_no_match_then_stops_on_move_failure(
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = "move-empty-then-error"

    result = _run_sourced_function(
        WORK_STEALING,
        tmp_path,
        shell_control_env,
        "round_robin_distribution",
    )

    assert result.returncode != 0
    assert "Failed to distribute work" in result.stderr
    assert Path(shell_control_env["BROKER_COUNT_FILE"]).read_text().strip() == "2"


@pytest.mark.parametrize(
    ("function", "args"),
    [
        ("gradual_migration", ("source", "dest", "1")),
        ("merge_queues", ("dest", "source")),
    ],
)
@pytest.mark.parametrize("move_status", [1, 2])
def test_migration_move_loops_distinguish_no_match_from_failure(
    function: str,
    args: tuple[str, ...],
    move_status: int,
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = "controlled-move"
    shell_control_env["BROKER_MOVE_STATUS"] = str(move_status)
    shell_control_env["BROKER_STATS_JSON"] = (
        '{"queue":"source","pending":0,"claimed":0,"total":0,"exists":false}'
    )

    result = _run_sourced_function(
        QUEUE_MIGRATION,
        tmp_path,
        shell_control_env,
        function,
        *args,
    )

    assert result.returncode == (0 if move_status == 2 else 1)


@pytest.mark.parametrize("move_status", [1, 2])
def test_batch_retry_distinguishes_no_match_from_failure(
    move_status: int,
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = "controlled-move"
    shell_control_env["BROKER_MOVE_STATUS"] = str(move_status)

    result = _run_sourced_function(
        DEAD_LETTER_QUEUE,
        tmp_path,
        shell_control_env,
        "batch_retry_dlq",
        "all",
    )

    assert result.returncode == (0 if move_status == 2 else 1)
    if move_status == 2:
        assert "No matching DLQ messages" in result.stdout
    else:
        assert "Failed to retry messages" in result.stderr


def test_simple_dlq_treats_empty_as_completion(
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = "simple-empty"

    result = _run_sourced_function(
        DEAD_LETTER_QUEUE,
        tmp_path,
        shell_control_env,
        "simple_dlq_pattern",
    )

    assert result.returncode == 0, result.stderr
    assert "No more messages to process" in result.stdout
    assert "No failed messages to retry" in result.stdout


@pytest.mark.parametrize(
    ("function", "args"),
    [
        ("dlq_with_retry_count", ()),
        ("process_with_delays", ("tasks",)),
    ],
)
def test_continuous_dlq_loops_idle_on_empty_but_stop_on_peek_failure(
    function: str,
    args: tuple[str, ...],
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = "empty-then-error"

    result = _run_sourced_function(
        DEAD_LETTER_QUEUE,
        tmp_path,
        shell_control_env,
        function,
        *args,
    )

    assert result.returncode != 0
    assert "Failed to peek" in result.stderr
    assert Path(shell_control_env["BROKER_COUNT_FILE"]).read_text().strip() == "2"


@pytest.mark.parametrize(
    ("script", "function", "args"),
    [
        (DEAD_LETTER_QUEUE, "simple_dlq_pattern", ()),
        (DEAD_LETTER_QUEUE, "dlq_with_retry_count", ()),
        (DEAD_LETTER_QUEUE, "process_with_delays", ("tasks",)),
        (QUEUE_MIGRATION, "filtered_migration", ("source", "dest", "match")),
        (QUEUE_MIGRATION, "split_queue", ("source", "dest-a", "dest-b")),
        (
            QUEUE_MIGRATION,
            "transform_migration",
            ("source", "dest", "tr", "a-z", "A-Z"),
        ),
        (WORK_STEALING, "simulate_worker", ("worker1", "0")),
    ],
)
@pytest.mark.parametrize("mode", ["peek-malformed", "peek-bad-id"])
def test_message_loops_reject_malformed_success_output(
    script: Path,
    function: str,
    args: tuple[str, ...],
    mode: str,
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = mode

    result = _run_sourced_function(
        script,
        tmp_path,
        shell_control_env,
        function,
        *args,
    )

    assert result.returncode != 0
    assert "invalid message JSON" in result.stderr


@pytest.mark.parametrize(
    ("function", "args"),
    [
        ("filtered_migration", ("source", "dest", "match")),
        ("split_queue", ("source", "dest-a", "dest-b")),
        ("transform_migration", ("source", "dest", "tr", "a-z", "A-Z")),
    ],
)
@pytest.mark.parametrize(
    ("mode", "expected_status"),
    [("peek-empty", 0), ("peek-error", 1)],
)
def test_migration_peek_loops_distinguish_empty_from_failure(
    function: str,
    args: tuple[str, ...],
    mode: str,
    expected_status: int,
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = mode

    result = _run_sourced_function(
        QUEUE_MIGRATION,
        tmp_path,
        shell_control_env,
        function,
        *args,
    )

    assert result.returncode == expected_status
    if expected_status:
        assert "Failed to peek" in result.stderr


def test_worker_simulator_idles_then_stops_on_peek_failure(
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = "empty-then-error"

    result = _run_sourced_function(
        WORK_STEALING,
        tmp_path,
        shell_control_env,
        "simulate_worker",
        "worker1",
        "0",
    )

    assert result.returncode != 0
    assert "Failed to peek" in result.stderr
    assert Path(shell_control_env["BROKER_COUNT_FILE"]).read_text().strip() == "2"


@pytest.mark.parametrize(
    ("function", "args"),
    [
        ("dlq_with_retry_count", ()),
        ("process_with_delays", ("tasks",)),
    ],
)
def test_dlq_replacement_stops_when_source_delete_fails(
    function: str,
    args: tuple[str, ...],
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = "replacement-delete-error"

    result = _run_sourced_function(
        DEAD_LETTER_QUEUE,
        tmp_path,
        shell_control_env,
        function,
        *args,
    )

    assert result.returncode != 0
    assert "duplicate/retry risk" in result.stderr
    assert [
        line.split("\t", 1)[0]
        for line in _log_lines(shell_control_env, "BROKER_CALL_LOG")
    ] == ["peek", "write", "delete"]


def test_migration_rename_uses_the_rename_operation(
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = "rename"

    result = _run_sourced_function(
        QUEUE_MIGRATION,
        tmp_path,
        shell_control_env,
        "rename_queue",
        "old",
        "new",
    )

    assert result.returncode == 0, result.stderr
    assert _log_lines(shell_control_env, "BROKER_CALL_LOG") == ["rename\told\tnew"]


def test_migration_rename_preserves_pending_and_claimed_state(
    tmp_path: Path,
) -> None:
    env = _real_broker_env(tmp_path)
    db_path = tmp_path / ".broker.db"
    with Queue("old", db_path=str(db_path)) as old:
        old.insert_messages([("claimed", OLD_ID), ("pending", NEW_ID)])
        assert old.read_one() == "claimed"

    result = _run_sourced_function(
        QUEUE_MIGRATION,
        tmp_path,
        env,
        "rename_queue",
        "old",
        "new",
    )

    assert result.returncode == 0, result.stderr
    with Queue("old", db_path=str(db_path)) as old:
        assert old.stats().total == 0
    with Queue("new", db_path=str(db_path)) as new:
        assert new.stats().pending == 1
        assert new.stats().claimed == 1
        assert new.peek_many(
            limit=10,
            with_timestamps=False,
            include_claimed=True,
        ) == ["claimed", "pending"]


def test_migration_rename_reports_missing_source_and_existing_destination(
    tmp_path: Path,
) -> None:
    env = _real_broker_env(tmp_path)

    missing = _run_sourced_function(
        QUEUE_MIGRATION,
        tmp_path,
        env,
        "rename_queue",
        "missing",
        "new",
    )

    assert missing.returncode == 2
    assert "does not exist" in missing.stderr

    db_path = tmp_path / ".broker.db"
    with Queue("old", db_path=str(db_path)) as old:
        old.write("old body")
    with Queue("new", db_path=str(db_path)) as new:
        new.write("new body")

    collision = _run_sourced_function(
        QUEUE_MIGRATION,
        tmp_path,
        env,
        "rename_queue",
        "old",
        "new",
    )

    assert collision.returncode == 1
    assert "destination may already exist" in collision.stderr
    with Queue("old", db_path=str(db_path)) as old:
        assert old.peek_many(limit=10, with_timestamps=False) == ["old body"]
    with Queue("new", db_path=str(db_path)) as new:
        assert new.peek_many(limit=10, with_timestamps=False) == ["new body"]


def test_filtered_migration_treats_dash_prefixed_filter_as_pattern(
    tmp_path: Path,
) -> None:
    env = _real_broker_env(tmp_path)
    db_path = tmp_path / ".broker.db"
    with Queue("source", db_path=str(db_path)) as source:
        source.insert_messages([("foo", OLD_ID), ("contains -efoo", NEW_ID)])

    result = _run_sourced_function(
        QUEUE_MIGRATION,
        tmp_path,
        env,
        "filtered_migration",
        "source",
        "dest",
        "-efoo",
    )

    assert result.returncode == 0, result.stderr
    with Queue("source", db_path=str(db_path)) as source:
        assert source.peek_many(limit=10, with_timestamps=False) == ["foo"]
    with Queue("dest", db_path=str(db_path)) as destination:
        assert destination.peek_many(limit=10, with_timestamps=False) == [
            "contains -efoo"
        ]


def test_filtered_migration_rejects_invalid_pattern_without_mutation(
    tmp_path: Path,
) -> None:
    env = _real_broker_env(tmp_path)
    db_path = tmp_path / ".broker.db"
    with Queue("source", db_path=str(db_path)) as source:
        source.write("untouched")

    result = _run_sourced_function(
        QUEUE_MIGRATION,
        tmp_path,
        env,
        "filtered_migration",
        "source",
        "dest",
        "[",
    )

    assert result.returncode == 1
    assert "Invalid filter pattern" in result.stderr
    with Queue("source", db_path=str(db_path)) as source:
        assert source.peek_many(limit=10, with_timestamps=False) == ["untouched"]
    with Queue("dest", db_path=str(db_path)) as destination:
        assert destination.stats().total == 0


@pytest.mark.parametrize(
    ("mode", "expected_dlq", "expected_tasks"),
    [
        ("all", [], ["old failure", "recent failure"]),
        ("recent", ["old failure"], ["recent failure"]),
    ],
)
def test_batch_retry_modes_select_expected_real_rows(
    mode: str,
    expected_dlq: list[str],
    expected_tasks: list[str],
    tmp_path: Path,
) -> None:
    env = _real_broker_env(tmp_path)
    db_path = tmp_path / ".broker.db"
    with Queue("dlq", db_path=str(db_path)) as dlq:
        dlq.insert_messages([("old failure", OLD_ID)])
        dlq.write("recent failure")

    result = _run_sourced_function(
        DEAD_LETTER_QUEUE,
        tmp_path,
        env,
        "batch_retry_dlq",
        mode,
    )

    assert result.returncode == 0, result.stderr
    with Queue("dlq", db_path=str(db_path)) as dlq:
        assert dlq.peek_many(limit=10, with_timestamps=False) == expected_dlq
    with Queue("tasks", db_path=str(db_path)) as tasks:
        assert tasks.peek_many(limit=10, with_timestamps=False) == expected_tasks


@pytest.mark.parametrize("bound", ["1700000000s", "1837025672140161024"])
def test_migration_bound_reaches_the_cli_unchanged(
    bound: str,
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = "bound"

    result = _run_sourced_function(
        QUEUE_MIGRATION,
        tmp_path,
        shell_control_env,
        "migrate_by_time",
        "source",
        "dest",
        bound,
    )

    assert result.returncode == 0, result.stderr
    assert _log_lines(shell_control_env, "BROKER_CALL_LOG") == [
        f"move\tsource\tdest\t--all\t--before\t{bound}"
    ]


def test_queue_export_uses_pending_only_dump_and_documents_load(
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = "backup"
    backup = tmp_path / "source.ndjson"

    result = _run_sourced_function(
        QUEUE_MIGRATION,
        tmp_path,
        shell_control_env,
        "backup_queue",
        "source",
        str(backup),
    )

    assert result.returncode == 0, result.stderr
    assert _log_lines(shell_control_env, "BROKER_CALL_LOG") == [
        "stats\tsource\t--json",
        "dump\t--include\tsource",
    ]
    assert "pending messages" in result.stdout
    assert "claimed rows and application sidecars are not included" in result.stdout
    assert f"broker load < {backup}" in result.stdout


def test_queue_export_reports_dump_failure(
    tmp_path: Path,
    shell_control_env: dict[str, str],
) -> None:
    shell_control_env["BROKER_FAKE_MODE"] = "dump-error"
    backup = tmp_path / "source.ndjson"

    result = _run_sourced_function(
        QUEUE_MIGRATION,
        tmp_path,
        shell_control_env,
        "backup_queue",
        "source",
        str(backup),
    )

    assert result.returncode != 0
    assert "may be incomplete" in result.stderr
    assert "Restore into a fresh target" not in result.stdout


def test_queue_export_round_trip_preserves_pending_ids_and_excludes_claimed(
    tmp_path: Path,
) -> None:
    env = _real_broker_env(tmp_path)
    source_db = tmp_path / ".broker.db"
    with Queue("source", db_path=str(source_db)) as source:
        source.insert_messages([("claimed", OLD_ID), ("pending", NEW_ID)])
        assert source.read_one() == "claimed"

    backup = tmp_path / "source.ndjson"
    exported = _run_sourced_function(
        QUEUE_MIGRATION,
        tmp_path,
        env,
        "backup_queue",
        "source",
        str(backup),
    )

    assert exported.returncode == 0, exported.stderr
    records = [json.loads(line) for line in backup.read_text().splitlines()]
    messages = [record for record in records if record["type"] == "message"]
    assert [record["body"] for record in messages] == ["pending"]
    assert [record["id"] for record in messages] == [str(NEW_ID)]

    restored_dir = tmp_path / "restored"
    restored_dir.mkdir()
    loaded = subprocess.run(
        ["broker", "load"],
        cwd=restored_dir,
        env=env,
        input=backup.read_text(),
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert loaded.returncode == 0, loaded.stderr
    with Queue("source", db_path=str(restored_dir / ".broker.db")) as restored:
        assert restored.stats().pending == 1
        assert restored.stats().claimed == 0
        assert restored.latest_pending_timestamp() == NEW_ID
        assert restored.peek_many(limit=10, with_timestamps=False) == ["pending"]


def test_queue_depth_reads_real_cli_stats(
    tmp_path: Path,
) -> None:
    env = _real_broker_env(tmp_path)
    with Queue("jobs", db_path=str(tmp_path / ".broker.db")) as jobs:
        for message in ("one", "two", "three"):
            jobs.write(message)
        assert jobs.read_one() == "one"

    result = _run_sourced_function(
        WORK_STEALING,
        tmp_path,
        env,
        "queue_depth",
        "jobs",
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "2"


def test_transform_stops_after_real_write_when_source_delete_fails(
    tmp_path: Path,
) -> None:
    bin_dir = tmp_path / "partial-mutation-bin"
    bin_dir.mkdir()
    db_path = tmp_path / "partial.db"
    _write_executable(
        bin_dir / "broker",
        f"""#!/bin/bash
if [ "$1" = "delete" ] && [ "$2" = "source" ] && [ "$4" = "$BROKER_FAIL_ID" ]; then
    echo "simulated source delete failure" >&2
    exit 1
fi
exec env PYTHONPATH={shlex.quote(str(REPO_ROOT))} \
    {shlex.quote(sys.executable)} -m simplebroker -f "$BROKER_REAL_DB" "$@"
""",
    )
    env = _ShellEnv(os.environ.copy())
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "BROKER_REAL_DB": str(db_path),
            "BROKER_FAIL_ID": str(OLD_ID),
        }
    )
    with Queue("source", db_path=str(db_path)) as source:
        source.insert_messages([("first", OLD_ID), ("second", NEW_ID)])

    result = _run_sourced_function(
        QUEUE_MIGRATION,
        tmp_path,
        env,
        "transform_migration",
        "source",
        "dest",
        "tr",
        "a-z",
        "A-Z",
    )

    assert result.returncode != 0
    assert "duplicate/retry risk" in result.stderr
    with Queue("source", db_path=str(db_path)) as source:
        assert source.peek_many(limit=10, with_timestamps=False) == ["first", "second"]
    with Queue("dest", db_path=str(db_path)) as destination:
        assert destination.peek_many(limit=10, with_timestamps=False) == ["FIRST"]


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


@pytest.mark.parametrize("bound", ["1700000000s", "1700000000000000000"])
def test_queue_migration_moves_only_messages_older_than_cutoff(
    bound: str,
    tmp_path: Path,
) -> None:
    _require_shell_tools()
    env = _real_broker_env(tmp_path)

    db_path = tmp_path / ".broker.db"
    with Queue("source", db_path=str(db_path)) as source:
        source.insert_messages([("older", OLD_ID), ("newer", NEW_ID)])

    result = _run_menu(
        QUEUE_MIGRATION,
        tmp_path,
        env,
        "3",
        input_text=f"source\ndestination\n{bound}\n",
    )

    assert result.returncode == 0, result.stderr
    with Queue("source", db_path=str(db_path)) as source:
        assert source.peek_many(limit=10, with_timestamps=False) == ["newer"]
        assert source.latest_pending_timestamp() == NEW_ID
    with Queue("destination", db_path=str(db_path)) as destination:
        assert destination.peek_many(limit=10, with_timestamps=False) == ["older"]
        assert destination.latest_pending_timestamp() == OLD_ID


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
