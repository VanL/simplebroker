"""Spawn-isolated real-SQLite probe for terminal progress diagnostics."""

from __future__ import annotations

import faulthandler
import itertools
import multiprocessing as mp
import os
import sqlite3
import threading
import time
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypedDict, cast

if TYPE_CHECKING:
    from simplebroker import Queue

TerminalPhase = Literal[
    "begin-entered",
    "begin-returned",
    "commit-entered",
    "commit-returned",
    "close-entered",
    "close-returned",
    "probe-complete",
]


class PhaseRecord(TypedDict):
    sequence: int
    call_id: int
    phase: TerminalPhase
    operation: str
    iteration: int
    runner_id: int
    process_id: int
    thread_id: int
    child_monotonic: float
    child_elapsed: float
    in_transaction: bool | None
    transaction_owner_id: int | None
    admitted_operations: int
    tracked_connections: int


class ProbeResult(TypedDict):
    completed: bool
    crossed_observation_threshold: bool
    hard_cap_reached: bool
    process_exitcode: int | None
    error: str | None
    open_terminal_calls: list[PhaseRecord]
    records: list[PhaseRecord]
    parent_elapsed: float
    records_after_observation: int


class _OperationContext(TypedDict):
    operation: str
    iteration: int


_ENTERED_TO_RETURNED: dict[TerminalPhase, TerminalPhase] = {
    "begin-entered": "begin-returned",
    "commit-entered": "commit-returned",
    "close-entered": "close-returned",
}
_RETURNED_PHASES = frozenset(_ENTERED_TO_RETURNED.values())


def _connection_in_transaction(connection: sqlite3.Connection | None) -> bool | None:
    if connection is None:
        return None
    try:
        return bool(connection.in_transaction)
    except sqlite3.Error:
        return None


def _run_public_sidecar_workload(
    queue: Queue,
    iterations: int,
    operation_context: _OperationContext,
) -> None:
    operation_context["operation"] = "create-table"
    operation_context["iteration"] = -1
    with queue.sidecar(transaction=True) as session:
        session.run(
            "CREATE TABLE IF NOT EXISTS app_probe "
            "(k INTEGER PRIMARY KEY, v TEXT NOT NULL)"
        )

    for iteration in range(iterations):
        operation_context["operation"] = "insert"
        operation_context["iteration"] = iteration
        with queue.sidecar(transaction=True) as session:
            session.run(
                "INSERT INTO app_probe (k, v) VALUES (?, ?)",
                (iteration, f"value-{iteration}"),
            )

        operation_context["operation"] = "select"
        operation_context["iteration"] = iteration
        with queue.sidecar() as session:
            rows = list(
                session.run(
                    "SELECT v FROM app_probe WHERE k = ?",
                    (iteration,),
                    fetch=True,
                )
            )
        expected = [(f"value-{iteration}",)]
        if rows != expected:
            raise AssertionError(f"unexpected probe rows: {rows!r} != {expected!r}")


def _terminal_probe_child(
    channel: Connection,
    target: str,
    iterations: int,
    block_after_phase: TerminalPhase | None,
) -> None:
    """Run the public sidecar workload and synchronously publish each phase."""
    from simplebroker import Queue
    from simplebroker._runner import SQLiteRunner

    faulthandler.enable()
    started = time.monotonic()
    sequence = itertools.count(1)
    call_ids = itertools.count(1)
    operation_context = _OperationContext(operation="bootstrap", iteration=-1)

    original_begin = SQLiteRunner.begin_immediate
    original_commit = SQLiteRunner.commit
    original_close = SQLiteRunner._close_tracked_connection

    def publish(
        phase: TerminalPhase,
        *,
        runner: SQLiteRunner | None,
        call_id: int,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        owner = None if runner is None else runner._transaction_owner
        record: PhaseRecord = {
            "sequence": next(sequence),
            "call_id": call_id,
            "phase": phase,
            "operation": operation_context["operation"],
            "iteration": operation_context["iteration"],
            "runner_id": -1 if runner is None else runner.instance_id,
            "process_id": os.getpid(),
            "thread_id": threading.get_ident(),
            "child_monotonic": time.monotonic(),
            "child_elapsed": time.monotonic() - started,
            "in_transaction": _connection_in_transaction(connection),
            "transaction_owner_id": None if owner is None else owner.ident,
            "admitted_operations": (
                0 if runner is None else runner._transaction_admitted_operations
            ),
            "tracked_connections": (
                0 if runner is None else len(runner._all_connections)
            ),
        }
        channel.send(record)
        acknowledgement = channel.recv()
        if acknowledgement != ("ack", record["sequence"]):
            raise RuntimeError(
                "terminal probe received an invalid acknowledgement: "
                f"{acknowledgement!r}"
            )
        if phase in _ENTERED_TO_RETURNED:
            faulthandler.cancel_dump_traceback_later()
            faulthandler.dump_traceback_later(55.0, repeat=False)
        elif phase in _RETURNED_PHASES:
            faulthandler.cancel_dump_traceback_later()
        if phase == block_after_phase:
            threading.Event().wait()

    def observed_begin(runner: SQLiteRunner) -> None:
        call_id = next(call_ids)
        connection = cast(
            sqlite3.Connection | None,
            getattr(runner._thread_local, "conn", None),
        )
        publish(
            "begin-entered",
            runner=runner,
            call_id=call_id,
            connection=connection,
        )
        original_begin(runner)
        connection = cast(
            sqlite3.Connection | None,
            getattr(runner._thread_local, "conn", None),
        )
        publish(
            "begin-returned",
            runner=runner,
            call_id=call_id,
            connection=connection,
        )

    def observed_commit(runner: SQLiteRunner) -> None:
        call_id = next(call_ids)
        connection = cast(
            sqlite3.Connection | None,
            getattr(runner._thread_local, "conn", None),
        )
        publish(
            "commit-entered",
            runner=runner,
            call_id=call_id,
            connection=connection,
        )
        original_commit(runner)
        connection = cast(
            sqlite3.Connection | None,
            getattr(runner._thread_local, "conn", None),
        )
        publish(
            "commit-returned",
            runner=runner,
            call_id=call_id,
            connection=connection,
        )

    def observed_close(
        runner: SQLiteRunner,
        connection: sqlite3.Connection,
    ) -> bool:
        call_id = next(call_ids)
        publish(
            "close-entered",
            runner=runner,
            call_id=call_id,
            connection=connection,
        )
        closed = original_close(runner, connection)
        publish(
            "close-returned",
            runner=runner,
            call_id=call_id,
            connection=connection,
        )
        return closed

    runner_type: Any = SQLiteRunner
    runner_type.begin_immediate = observed_begin
    runner_type.commit = observed_commit
    runner_type._close_tracked_connection = observed_close

    try:
        queue = Queue("jobs", db_path=str(Path(target)), persistent=False)
        _run_public_sidecar_workload(queue, iterations, operation_context)
        operation_context["operation"] = "complete"
        operation_context["iteration"] = iterations - 1
        publish(
            "probe-complete",
            runner=None,
            call_id=next(call_ids),
        )
    finally:
        faulthandler.cancel_dump_traceback_later()
        channel.close()


def _open_terminal_calls(records: list[PhaseRecord]) -> list[PhaseRecord]:
    entered: dict[int, PhaseRecord] = {}
    for record in records:
        phase = record["phase"]
        if phase in _ENTERED_TO_RETURNED:
            entered[record["call_id"]] = record
        elif phase in _RETURNED_PHASES:
            entered_record = entered.get(record["call_id"])
            if entered_record is None:
                continue
            if _ENTERED_TO_RETURNED[entered_record["phase"]] == phase:
                entered.pop(record["call_id"])
    return list(entered.values())


def _raise_injected_parent_failure(
    record: PhaseRecord,
    failure_after_sequence: int | None,
) -> None:
    if record["sequence"] == failure_after_sequence:
        raise RuntimeError("injected parent protocol failure before acknowledgement")


def _collect_phase_records(
    parent_channel: Connection,
    process: BaseProcess,
    *,
    started: float,
    observation_threshold: float,
    hard_cap: float,
    test_parent_failure_after_sequence: int | None,
) -> tuple[list[PhaseRecord], bool, bool, bool, int]:
    records: list[PhaseRecord] = []
    completed = False
    crossed_observation_threshold = False
    hard_cap_reached = False
    records_after_observation = 0
    last_progress_at = started
    while True:
        now = time.monotonic()
        if now - started >= observation_threshold:
            crossed_observation_threshold = True
        missing_progress = now - last_progress_at
        if missing_progress >= hard_cap:
            hard_cap_reached = True
            break
        if not parent_channel.poll(min(0.1, hard_cap - missing_progress)):
            if not process.is_alive():
                break
            continue
        try:
            record = cast(PhaseRecord, parent_channel.recv())
        except EOFError:
            break
        records.append(record)
        _raise_injected_parent_failure(record, test_parent_failure_after_sequence)
        parent_channel.send(("ack", record["sequence"]))
        last_progress_at = time.monotonic()
        if last_progress_at - started >= observation_threshold:
            crossed_observation_threshold = True
        if crossed_observation_threshold:
            records_after_observation += 1
        if record["phase"] == "probe-complete":
            completed = True
            break
    return (
        records,
        completed,
        crossed_observation_threshold,
        hard_cap_reached,
        records_after_observation,
    )


def _finish_probe_process(process: BaseProcess, *, terminate: bool) -> None:
    if terminate and process.is_alive():
        process.terminate()
    process.join(timeout=2.0)
    if process.is_alive():
        process.kill()
        process.join(timeout=2.0)


def run_sqlite_terminal_progress_probe(
    target: str,
    *,
    iterations: int,
    observation_threshold: float = 15.0,
    hard_cap: float = 60.0,
    _test_block_after_phase: TerminalPhase | None = None,
    _test_parent_failure_after_sequence: int | None = None,
) -> ProbeResult:
    """Run the terminal probe with parent-owned observation and hard deadlines."""
    if iterations < 1:
        raise ValueError("iterations must be positive")
    if observation_threshold <= 0 or hard_cap <= observation_threshold:
        raise ValueError("hard_cap must be greater than observation_threshold > 0")

    context = mp.get_context("spawn")
    parent_channel, child_channel = context.Pipe(duplex=True)
    process = context.Process(
        target=_terminal_probe_child,
        args=(child_channel, target, iterations, _test_block_after_phase),
    )
    started = time.monotonic()
    process.start()
    child_channel.close()
    collection_completed = False
    hard_cap_reached = False
    try:
        (
            records,
            completed,
            crossed_observation_threshold,
            hard_cap_reached,
            records_after_observation,
        ) = _collect_phase_records(
            parent_channel,
            process,
            started=started,
            observation_threshold=observation_threshold,
            hard_cap=hard_cap,
            test_parent_failure_after_sequence=_test_parent_failure_after_sequence,
        )
        collection_completed = True
    finally:
        _finish_probe_process(
            process,
            terminate=hard_cap_reached or not collection_completed,
        )
        parent_channel.close()

    parent_elapsed = time.monotonic() - started
    error = None
    if not completed and not hard_cap_reached:
        error = "probe child exited before publishing probe-complete"
    return {
        "completed": completed,
        "crossed_observation_threshold": crossed_observation_threshold,
        "hard_cap_reached": hard_cap_reached,
        "process_exitcode": process.exitcode,
        "error": error,
        "open_terminal_calls": _open_terminal_calls(records),
        "records": records,
        "parent_elapsed": parent_elapsed,
        "records_after_observation": records_after_observation,
    }
