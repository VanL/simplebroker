"""Spawn-isolated real-SQLite probe for terminal progress diagnostics."""

from __future__ import annotations

import faulthandler
import itertools
import json
import multiprocessing as mp
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, field
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
    "probe-ready",
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
    timeout_stage: Literal["startup", "terminal-progress"] | None
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
        operation_context["operation"] = "ready"
        operation_context["iteration"] = -1
        publish(
            "probe-ready",
            runner=None,
            call_id=next(call_ids),
        )
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


def _print_probe_progress(
    event: str,
    *,
    elapsed: float,
    last_record: PhaseRecord | None,
) -> None:
    print(
        "[sqlite-terminal-probe] "
        + json.dumps(
            {
                "event": event,
                "elapsed": elapsed,
                "last_record": last_record,
            },
            sort_keys=True,
        ),
        flush=True,
    )


@dataclass
class _CollectionState:
    started: float
    records: list[PhaseRecord] = field(default_factory=list)
    completed: bool = False
    crossed_observation_threshold: bool = False
    hard_cap_reached: bool = False
    timeout_stage: Literal["startup", "terminal-progress"] | None = None
    records_after_observation: int = 0
    workload_started_at: float | None = None
    missing_progress_reported: bool = False
    last_progress_at: float = field(init=False)

    def __post_init__(self) -> None:
        self.last_progress_at = self.started

    def observe_time(
        self,
        now: float,
        *,
        observation_threshold: float,
        startup_cap: float,
        hard_cap: float,
    ) -> tuple[float, float]:
        workload_elapsed = (
            0.0 if self.workload_started_at is None else now - self.workload_started_at
        )
        if (
            self.workload_started_at is not None
            and workload_elapsed >= observation_threshold
        ):
            if not self.crossed_observation_threshold:
                _print_probe_progress(
                    "observation-threshold-crossed",
                    elapsed=workload_elapsed,
                    last_record=self.records[-1] if self.records else None,
                )
            self.crossed_observation_threshold = True

        missing_progress = now - self.last_progress_at
        active_cap = startup_cap if self.workload_started_at is None else hard_cap
        return missing_progress, active_cap

    def acknowledge(
        self,
        record: PhaseRecord,
        *,
        received_at: float,
        observation_threshold: float,
    ) -> None:
        self.last_progress_at = received_at
        self.missing_progress_reported = False
        if record["phase"] == "probe-ready":
            if self.workload_started_at is not None:
                raise RuntimeError("probe child published duplicate readiness")
            self.workload_started_at = received_at
            _print_probe_progress("ready", elapsed=0.0, last_record=record)
            return
        if self.workload_started_at is None:
            raise RuntimeError("probe child published work before readiness")
        if received_at - self.workload_started_at >= observation_threshold:
            self.crossed_observation_threshold = True
        if self.crossed_observation_threshold:
            self.records_after_observation += 1
        if record["phase"] == "probe-complete":
            self.completed = True


def _collect_phase_records(
    parent_channel: Connection,
    process: BaseProcess,
    *,
    started: float,
    observation_threshold: float,
    startup_cap: float,
    hard_cap: float,
    test_parent_failure_after_sequence: int | None,
) -> tuple[
    list[PhaseRecord],
    bool,
    bool,
    bool,
    Literal["startup", "terminal-progress"] | None,
    int,
]:
    state = _CollectionState(started=started)
    while True:
        now = time.monotonic()
        missing_progress, active_cap = state.observe_time(
            now,
            observation_threshold=observation_threshold,
            startup_cap=startup_cap,
            hard_cap=hard_cap,
        )
        if missing_progress >= active_cap:
            state.hard_cap_reached = True
            state.timeout_stage = (
                "startup" if state.workload_started_at is None else "terminal-progress"
            )
            break
        if (
            not state.missing_progress_reported
            and state.workload_started_at is not None
            and missing_progress >= 5.0
        ):
            _print_probe_progress(
                "missing-progress",
                elapsed=missing_progress,
                last_record=state.records[-1] if state.records else None,
            )
            state.missing_progress_reported = True
        if not parent_channel.poll(min(0.1, active_cap - missing_progress)):
            if not process.is_alive():
                break
            continue
        try:
            record = cast(PhaseRecord, parent_channel.recv())
        except EOFError:
            break
        state.records.append(record)
        _raise_injected_parent_failure(record, test_parent_failure_after_sequence)
        parent_channel.send(("ack", record["sequence"]))
        state.acknowledge(
            record,
            received_at=time.monotonic(),
            observation_threshold=observation_threshold,
        )
        if state.completed:
            break
    return (
        state.records,
        state.completed,
        state.crossed_observation_threshold,
        state.hard_cap_reached,
        state.timeout_stage,
        state.records_after_observation,
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
    startup_cap: float = 30.0,
    hard_cap: float = 60.0,
    _test_block_after_phase: TerminalPhase | None = None,
    _test_parent_failure_after_sequence: int | None = None,
) -> ProbeResult:
    """Run the terminal probe with parent-owned observation and hard deadlines."""
    if iterations < 1:
        raise ValueError("iterations must be positive")
    if startup_cap <= 0:
        raise ValueError("startup_cap must be positive")
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
            timeout_stage,
            records_after_observation,
        ) = _collect_phase_records(
            parent_channel,
            process,
            started=started,
            observation_threshold=observation_threshold,
            startup_cap=startup_cap,
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
        "timeout_stage": timeout_stage,
        "process_exitcode": process.exitcode,
        "error": error,
        "open_terminal_calls": _open_terminal_calls(records),
        "records": records,
        "parent_elapsed": parent_elapsed,
        "records_after_observation": records_after_observation,
    }
