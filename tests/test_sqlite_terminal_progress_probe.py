"""Diagnostic contract for real SQLite terminal progress under Windows spawn."""

from __future__ import annotations

import json
import os
from multiprocessing.process import BaseProcess
from pathlib import Path

import pytest

from tests.helper_scripts import sqlite_terminal_progress_probe as terminal_probe
from tests.helper_scripts.sqlite_terminal_progress_probe import (
    PhaseRecord,
    run_sqlite_terminal_progress_probe,
)


def _assert_record_identity(
    records: list[PhaseRecord],
    *,
    operation: str,
    iteration: int,
) -> None:
    assert {record["operation"] for record in records} == {operation}
    assert {record["iteration"] for record in records} == {iteration}
    assert len({record["runner_id"] for record in records}) == 1
    assert len({record["process_id"] for record in records}) == 1
    assert len({record["thread_id"] for record in records}) == 1


def _assert_call_pair(
    records: list[PhaseRecord],
    *,
    phases: tuple[str, str],
) -> None:
    assert [record["phase"] for record in records] == list(phases)
    assert records[0]["call_id"] == records[1]["call_id"]
    assert records[0]["runner_id"] == records[1]["runner_id"]
    assert records[0]["operation"] == records[1]["operation"]
    assert records[0]["iteration"] == records[1]["iteration"]


def _assert_transaction(records: list[PhaseRecord]) -> None:
    assert len(records) == 4
    _assert_call_pair(records[:2], phases=("begin-entered", "begin-returned"))
    _assert_call_pair(records[2:], phases=("commit-entered", "commit-returned"))
    assert records[0]["call_id"] != records[2]["call_id"]
    assert len({record["runner_id"] for record in records}) == 1

    begin_entered, begin_returned, commit_entered, commit_returned = records
    assert begin_entered["in_transaction"] in {None, False}
    assert begin_entered["transaction_owner_id"] is None
    assert begin_entered["admitted_operations"] == 0
    assert begin_returned["in_transaction"] is True
    assert begin_returned["transaction_owner_id"] == begin_returned["thread_id"]
    assert begin_returned["admitted_operations"] == 0
    assert begin_returned["tracked_connections"] == 1
    assert commit_entered["in_transaction"] is True
    assert commit_entered["transaction_owner_id"] == commit_entered["thread_id"]
    assert commit_entered["admitted_operations"] == 0
    assert commit_entered["tracked_connections"] == 1
    assert commit_returned["in_transaction"] is False
    assert commit_returned["transaction_owner_id"] is None
    assert commit_returned["admitted_operations"] == 0
    assert commit_returned["tracked_connections"] == 1


def _assert_close(records: list[PhaseRecord]) -> None:
    assert len(records) == 2
    _assert_call_pair(records, phases=("close-entered", "close-returned"))
    close_entered, close_returned = records
    assert close_entered["in_transaction"] is False
    assert close_entered["transaction_owner_id"] is None
    assert close_entered["admitted_operations"] == 1
    assert close_entered["tracked_connections"] == 1
    assert close_returned["in_transaction"] is None
    assert close_returned["transaction_owner_id"] is None
    assert close_returned["admitted_operations"] == 1
    assert close_returned["tracked_connections"] == 1


def _assert_exact_terminal_grammar(
    records: list[PhaseRecord],
    *,
    iterations: int,
    separate_runner_idle: bool = False,
) -> None:
    expected_records = (24 if separate_runner_idle else 20) + 8 * iterations
    assert len(records) == expected_records
    assert [record["sequence"] for record in records] == list(
        range(1, len(records) + 1)
    )

    ready = records[0]
    assert ready["phase"] == "probe-ready"
    assert ready["operation"] == "ready"
    assert ready["iteration"] == -1
    assert ready["runner_id"] == -1

    create_table = records[1:19]
    _assert_record_identity(create_table, operation="create-table", iteration=-1)
    for offset in range(0, 16, 4):
        _assert_transaction(create_table[offset : offset + 4])
    assert create_table[0]["tracked_connections"] == 0
    assert all(record["tracked_connections"] == 1 for record in create_table[1:])
    _assert_close(create_table[16:])

    runner_ids = {create_table[0]["runner_id"]}
    workload_process_id = create_table[0]["process_id"]
    workload_thread_id = create_table[0]["thread_id"]
    offset = 19
    idle_runner_id: int | None = None
    idle_process_id: int | None = None
    idle_thread_id: int | None = None
    if separate_runner_idle:
        idle_ready = records[offset]
        assert idle_ready["phase"] == "probe-idle-ready"
        assert idle_ready["operation"] == "idle-ready"
        assert idle_ready["iteration"] == -1
        idle_runner_id = idle_ready["runner_id"]
        idle_process_id = idle_ready["process_id"]
        idle_thread_id = idle_ready["thread_id"]
        assert idle_runner_id >= 0
        assert idle_process_id == workload_process_id
        assert idle_thread_id != workload_thread_id
        assert idle_ready["in_transaction"] is False
        assert idle_ready["transaction_owner_id"] is None
        assert idle_ready["admitted_operations"] == 0
        assert idle_ready["tracked_connections"] == 1
        assert idle_runner_id not in runner_ids
        runner_ids.add(idle_runner_id)
        offset += 1

    for iteration in range(iterations):
        iteration_offset = offset + iteration * 8
        insert = records[iteration_offset : iteration_offset + 6]
        select = records[iteration_offset + 6 : iteration_offset + 8]
        _assert_record_identity(insert, operation="insert", iteration=iteration)
        _assert_transaction(insert[:4])
        _assert_close(insert[4:])
        _assert_record_identity(select, operation="select", iteration=iteration)
        _assert_close(select)
        assert insert[0]["process_id"] == workload_process_id
        assert insert[0]["thread_id"] == workload_thread_id
        assert select[0]["process_id"] == workload_process_id
        assert select[0]["thread_id"] == workload_thread_id
        insert_runner = insert[0]["runner_id"]
        select_runner = select[0]["runner_id"]
        assert insert_runner != select_runner
        assert insert_runner not in runner_ids
        assert select_runner not in runner_ids
        runner_ids.update((insert_runner, select_runner))

    offset += iterations * 8
    if separate_runner_idle:
        assert idle_runner_id is not None
        assert idle_process_id is not None
        assert idle_thread_id is not None
        idle_close = records[offset : offset + 2]
        _assert_record_identity(idle_close, operation="idle-close", iteration=-1)
        _assert_close(idle_close)
        assert idle_close[0]["runner_id"] == idle_runner_id
        assert idle_close[0]["process_id"] == idle_process_id
        assert idle_close[0]["thread_id"] == idle_thread_id

        idle_released = records[offset + 2]
        assert idle_released["phase"] == "probe-idle-released"
        assert idle_released["operation"] == "idle-released"
        assert idle_released["iteration"] == -1
        assert idle_released["runner_id"] == idle_runner_id
        assert idle_released["process_id"] == idle_process_id
        assert idle_released["thread_id"] == idle_thread_id
        assert idle_released["in_transaction"] is None
        assert idle_released["transaction_owner_id"] is None
        assert idle_released["admitted_operations"] == 0
        assert idle_released["tracked_connections"] == 0

    complete = records[-1]
    assert complete["phase"] == "probe-complete"
    assert complete["operation"] == "complete"
    assert complete["iteration"] == iterations - 1
    assert complete["runner_id"] == -1
    assert complete["tracked_connections"] == 0
    assert complete["transaction_owner_id"] is None
    assert complete["admitted_operations"] == 0


def test_ephemeral_sidecar_terminal_progress_in_spawn_child(tmp_path: Path) -> None:
    iterations = int(os.environ.get("SIMPLEBROKER_TERMINAL_PROBE_ITERATIONS", "32"))
    result = run_sqlite_terminal_progress_probe(
        str(tmp_path / "terminal-progress.db"),
        iterations=iterations,
    )
    records = result["records"]
    summary = {
        key: value
        for key, value in result.items()
        if key not in {"records", "open_terminal_calls"}
    }
    summary["record_count"] = len(records)
    summary["first_records"] = records[:20]
    summary["last_records"] = records[-8:]
    summary["open_terminal_calls"] = result["open_terminal_calls"]
    rendered = json.dumps(summary, indent=2, sort_keys=True)
    print(rendered)

    assert result["hard_cap_reached"] is False, rendered
    assert result["error"] is None, rendered
    assert result["completed"] is True, rendered
    assert result["process_exitcode"] == 0, rendered
    assert result["open_terminal_calls"] == [], rendered
    _assert_exact_terminal_grammar(records, iterations=iterations)


def test_ephemeral_sidecar_progress_with_idle_same_database_connection(
    tmp_path: Path,
) -> None:
    iterations = int(os.environ.get("SIMPLEBROKER_TERMINAL_PROBE_ITERATIONS", "32"))
    result = run_sqlite_terminal_progress_probe(
        str(tmp_path / "idle-connection-terminal-progress.db"),
        iterations=iterations,
        probe_mode="separate-runner-idle",
    )
    records = result["records"]
    rendered = json.dumps(
        {
            **{
                key: value
                for key, value in result.items()
                if key not in {"records", "open_terminal_calls"}
            },
            "record_count": len(records),
            "first_records": records[:40],
            "last_records": records[-12:],
            "open_terminal_calls": result["open_terminal_calls"],
        },
        indent=2,
        sort_keys=True,
    )
    print(rendered)

    assert result["hard_cap_reached"] is False, rendered
    assert result["error"] is None, rendered
    assert result["completed"] is True, rendered
    assert result["process_exitcode"] == 0, rendered
    assert result["open_terminal_calls"] == [], rendered
    _assert_exact_terminal_grammar(
        records,
        iterations=iterations,
        separate_runner_idle=True,
    )


def test_parent_preserves_acknowledged_phase_before_terminating_child(
    tmp_path: Path,
) -> None:
    result = run_sqlite_terminal_progress_probe(
        str(tmp_path / "blocked-terminal-progress.db"),
        iterations=1,
        observation_threshold=0.05,
        hard_cap=0.25,
        _test_block_after_phase="close-entered",
    )

    assert result["completed"] is False
    assert result["crossed_observation_threshold"] is True
    assert result["hard_cap_reached"] is True
    assert result["process_exitcode"] is not None
    assert len(result["open_terminal_calls"]) == 1
    open_call = result["open_terminal_calls"][0]
    assert open_call["phase"] == "close-entered"
    assert open_call["operation"] == "create-table"
    assert open_call["in_transaction"] is False
    assert result["records"][-1] == open_call


def test_parent_protocol_failure_reaps_child_without_masking_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    finished: list[tuple[BaseProcess, bool]] = []
    finish_process = terminal_probe._finish_probe_process

    def observed_finish(process: BaseProcess, *, terminate: bool) -> None:
        finish_process(process, terminate=terminate)
        finished.append((process, terminate))

    monkeypatch.setattr(terminal_probe, "_finish_probe_process", observed_finish)

    with pytest.raises(
        RuntimeError,
        match="injected parent protocol failure before acknowledgement",
    ):
        run_sqlite_terminal_progress_probe(
            str(tmp_path / "parent-protocol-failure.db"),
            iterations=1,
            _test_parent_failure_after_sequence=1,
        )

    assert len(finished) == 1
    process, terminate = finished[0]
    assert terminate is True
    assert process.is_alive() is False
    assert process.exitcode is not None
