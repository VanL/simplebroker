"""Executable transition contracts for the reference reactor example."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import pytest

from examples.reference_reactor import (  # type: ignore[import-untyped]
    PendingOutput,
    Reactor,
    WorkerResult,
)
from simplebroker import Queue
from simplebroker.ext import OperationalError
from tests.helpers.state_machine_contracts import (
    TransitionCase,
    fires_transition_table,
)

INBOX = "transition.inbox"
OTHER_INBOX = "transition.inbox.other"
OUTBOX = "transition.outbox"
CONTROL_IN = "transition.control.in"
CONTROL_OUT = "transition.control.out"


def _json_message_id(value: int) -> str:
    return f"{value:019d}"


def _make_reactor(
    db_path: Path,
    *,
    processor: Any | None = None,
) -> Reactor:
    options: dict[str, Any] = {}
    if processor is not None:
        options["processor"] = processor
    return Reactor(
        input_queues=[INBOX, OTHER_INBOX],
        output_queue=OUTBOX,
        control_in_queue=CONTROL_IN,
        control_out_queue=CONTROL_OUT,
        db=db_path,
        worker_count=1,
        **options,
    )


def _result_statuses(reactor: Reactor) -> list[str]:
    with reactor._metadata_queue.sidecar() as session:
        return [
            str(row[0])
            for row in session.run(
                "SELECT status FROM reactor_results ORDER BY input_ts",
                fetch=True,
            )
        ]


@dataclass(frozen=True, slots=True)
class ReactorPayload:
    mode: Literal[
        "claim-idle",
        "dispatch-input",
        "drain-local-result",
        "stop-before-turn",
        "foreign-turn",
        "bounded-run",
        "repeated-stop",
        "backlog-retry-recovery",
        "control-ping",
        "control-status",
        "control-stop",
        "control-non-object",
        "control-unknown",
        "control-output-failure",
        "control-checkpoint-failure",
    ]


REACTOR_TRANSITIONS = (
    TransitionCase(
        transition_id="idle-turn-claims-owner",
        start_state="constructed-unowned",
        event="process one idle turn",
        guard="no durable backlog, local result, or queue input is pending",
        next_state="idle-owned",
        effects="the calling thread becomes the sole drive owner",
        expected_result="the turn completes without dispatch",
        payload=ReactorPayload("claim-idle"),
    ),
    TransitionCase(
        transition_id="input-dispatches-to-worker",
        start_state="idle-owned",
        event="process an input queue row",
        guard="no output backlog blocks ordinary input",
        next_state="work-inflight",
        effects="a broker-free work item is recorded and queued to a worker",
        expected_result="the input timestamp is present in the inflight set",
        payload=ReactorPayload("dispatch-input"),
    ),
    TransitionCase(
        transition_id="local-result-drains-before-queue",
        start_state="local-result-pending",
        event="process one turn",
        guard="a worker result is already available",
        next_state="output-written",
        effects="the result is recorded durably and published before queue drain",
        expected_result="one output row is written and local activity clears",
        payload=ReactorPayload("drain-local-result"),
    ),
    TransitionCase(
        transition_id="stop-skips-input-dispatch",
        start_state="stop-requested-with-input",
        event="process one turn",
        guard="the reactor stop event is set",
        next_state="stop-requested-with-input",
        effects="durable backlog may drain but ordinary input dispatch is skipped",
        expected_result="the source timestamp is not added to inflight work",
        payload=ReactorPayload("stop-before-turn"),
    ),
    TransitionCase(
        transition_id="foreign-drive-rejected",
        start_state="idle-owned",
        event="a second thread processes a turn",
        guard="another thread already owns reactor turns",
        next_state="idle-owned",
        effects="no state is transferred to the foreign thread",
        expected_result="RuntimeError identifies the single-owner contract",
        payload=ReactorPayload("foreign-turn"),
    ),
    TransitionCase(
        transition_id="bounded-run-finalizes",
        start_state="constructed-unowned",
        event="run one bounded iteration",
        guard="max_iterations is one",
        next_state="stopped-closed",
        effects="the loop finalizer stops the reactor and closes owned resources",
        expected_result="resources are marked closed",
        payload=ReactorPayload("bounded-run"),
    ),
    TransitionCase(
        transition_id="repeated-stop-is-idempotent",
        start_state="stopped-closed",
        event="stop again",
        guard="resources were already closed by the first stop",
        next_state="stopped-closed",
        effects="worker stop and resource close are not repeated",
        expected_result="the second stop returns without error",
        payload=ReactorPayload("repeated-stop"),
    ),
    TransitionCase(
        transition_id="durable-backlog-retries-before-input",
        start_state="output-pending-with-input",
        event="one publication attempt fails and the next turn retries",
        guard="the pending output row remains durable between turns",
        next_state="output-written-with-work-inflight",
        effects="the failed turn skips input; recovery publishes before dispatch",
        expected_result="the input becomes inflight only after backlog recovery",
        payload=ReactorPayload("backlog-retry-recovery"),
    ),
    TransitionCase(
        transition_id="control-ping-responds-and-checkpoints",
        start_state="control-pending",
        event="plain-text PING is processed",
        guard="the control output and sidecar are writable",
        next_state="control-processed",
        effects="writes PONG, increments the handled count, checkpoints, and audits",
        expected_result="one successful PING response is durable",
        payload=ReactorPayload("control-ping"),
    ),
    TransitionCase(
        transition_id="control-status-bypasses-output-backpressure",
        start_state="output-blocked-with-status-and-input-pending",
        event="STATUS is processed while output replay fails",
        guard="the control lane remains readable while ordinary input is blocked",
        next_state="output-blocked-with-status-processed",
        effects="reports durable counts and checkpoints without dispatching input",
        expected_result="STATUS succeeds and exposes the blocked backlog",
        payload=ReactorPayload("control-status"),
    ),
    TransitionCase(
        transition_id="control-stop-responds-before-stop",
        start_state="control-pending",
        event="STOP is processed",
        guard="response publication and checkpointing succeed",
        next_state="stop-requested",
        effects="writes and records the response before setting the stop event",
        expected_result="the stopping response is observable at stop request time",
        payload=ReactorPayload("control-stop"),
    ),
    TransitionCase(
        transition_id="control-non-object-is-rejected",
        start_state="control-pending",
        event="a JSON array is processed",
        guard="JSON control payloads must be objects",
        next_state="control-processed",
        effects="writes an error response and records the invalid command checkpoint",
        expected_result="the response explains the object requirement",
        payload=ReactorPayload("control-non-object"),
    ),
    TransitionCase(
        transition_id="control-unknown-command-is-rejected",
        start_state="control-pending",
        event="an unknown object command is processed",
        guard="the command is neither PING, STATUS, nor STOP",
        next_state="control-processed",
        effects="writes an error response and records the command checkpoint",
        expected_result="the response names the unknown command",
        payload=ReactorPayload("control-unknown"),
    ),
    TransitionCase(
        transition_id="control-output-failure-preserves-input",
        start_state="control-pending",
        event="control response publication fails",
        guard="handled count and checkpoint have not advanced",
        next_state="control-pending",
        effects="does not count, checkpoint, or audit the control input",
        expected_result="the same input succeeds after output recovery",
        payload=ReactorPayload("control-output-failure"),
    ),
    TransitionCase(
        transition_id="control-checkpoint-failure-replays-response",
        start_state="control-response-written",
        event="sidecar checkpoint recording fails",
        guard="the handled count already advanced but the input remains pending",
        next_state="control-processed-after-replay",
        effects="replays the response, increments again, then checkpoints once",
        expected_result="two responses expose the at-least-once control boundary",
        payload=ReactorPayload("control-checkpoint-failure"),
    ),
)


def _assert_foreign_turn_rejected(reactor: Reactor) -> None:
    reactor.process_once()
    errors: list[BaseException] = []

    def drive_from_foreign_thread() -> None:
        try:
            reactor.process_once()
        except BaseException as exc:  # noqa: BLE001 - test captures thread result
            errors.append(exc)

    thread = threading.Thread(target=drive_from_foreign_thread)
    thread.start()
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert "single-owner" in str(errors[0])


def _write_control(reactor: Reactor, body: str) -> int:
    control = Queue(CONTROL_IN, db_path=reactor._metadata_queue.db_target)
    try:
        return control.write(body)
    finally:
        control.close()


def _control_responses(reactor: Reactor) -> list[dict[str, Any]]:
    return [
        json.loads(body)
        for body in reactor._control_out_queue.peek_many(
            10,
            with_timestamps=False,
        )
    ]


def _control_audit_details(reactor: Reactor) -> list[str]:
    with reactor._metadata_queue.sidecar() as session:
        return [
            str(row[0])
            for row in session.run(
                """
                SELECT detail
                FROM reactor_audit
                WHERE lane = ? AND event = 'control'
                ORDER BY event_id
                """,
                (CONTROL_IN,),
                fetch=True,
            )
        ]


def _assert_control_recorded(
    reactor: Reactor,
    *,
    timestamp: int,
    detail: str,
    handled: int = 1,
) -> None:
    assert reactor._control_messages_handled == handled
    assert reactor._load_checkpoints()[CONTROL_IN] == timestamp
    assert _control_audit_details(reactor) == [detail]


def _fire_backlog_retry_recovery(
    reactor: Reactor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pending = _pending_result(reactor, 301)
    source = Queue(INBOX, db_path=reactor._metadata_queue.db_target)
    try:
        input_timestamp = source.write("blocked-until-output-recovers")
    finally:
        source.close()

    insert_messages = reactor._output_queue.insert_messages
    attempts = 0

    def fail_one_turn(messages: object) -> Any:
        nonlocal attempts
        attempts += 1
        if attempts <= 3:
            raise OperationalError("transient output failure")
        return insert_messages(messages)

    monkeypatch.setattr(
        reactor._output_queue,
        "insert_messages",
        fail_one_turn,
    )
    reactor.process_once()
    assert attempts == 3
    assert _result_statuses(reactor) == ["output_pending"]
    assert (INBOX, input_timestamp) not in reactor._inflight

    reactor.process_once()
    assert attempts == 4
    assert _result_statuses(reactor) == ["output_written"]
    assert (INBOX, input_timestamp) in reactor._inflight
    assert (
        reactor._output_queue.peek_one(
            exact_timestamp=pending.output_message_id,
            include_claimed=True,
        )
        is not None
    )


def _fire_control_transition(
    reactor: Reactor,
    *,
    mode: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if mode == "control-ping":
        timestamp = _write_control(reactor, "PING")
        reactor.process_once()
        assert _control_responses(reactor) == [
            {
                "command": "PING",
                "input_timestamp": _json_message_id(timestamp),
                "message": "PONG",
                "ok": True,
                "request_id": None,
            }
        ]
        _assert_control_recorded(reactor, timestamp=timestamp, detail="PING")
    elif mode == "control-status":
        _pending_result(reactor, 302)
        source = Queue(INBOX, db_path=reactor._metadata_queue.db_target)
        try:
            input_timestamp = source.write("blocked-by-output")
        finally:
            source.close()
        timestamp = _write_control(
            reactor,
            json.dumps({"command": "STATUS", "request_id": "status-1"}),
        )

        def fail_output(_messages: object) -> None:
            raise OperationalError("output remains unavailable")

        monkeypatch.setattr(
            reactor._output_queue,
            "insert_messages",
            fail_output,
        )
        reactor.process_once()
        response = _control_responses(reactor)[0]
        assert response["ok"] is True
        assert response["request_id"] == "status-1"
        assert response["pending_output_backlog"] == 1
        assert response["output_backlog_blocked"] is True
        assert response["result_status_counts"] == {"output_pending": 1}
        assert response["checkpoints"][INBOX] == _json_message_id(302)
        assert (INBOX, input_timestamp) not in reactor._inflight
        _assert_control_recorded(reactor, timestamp=timestamp, detail="STATUS")
    elif mode == "control-stop":
        timestamp = _write_control(
            reactor,
            json.dumps({"command": "STOP", "request_id": "stop-1"}),
        )
        request_stop = reactor.request_stop
        observations: list[tuple[list[dict[str, Any]], int, int]] = []

        def observe_then_stop() -> None:
            observations.append(
                (
                    _control_responses(reactor),
                    reactor._control_messages_handled,
                    reactor._load_checkpoints()[CONTROL_IN],
                )
            )
            request_stop()

        monkeypatch.setattr(reactor, "request_stop", observe_then_stop)
        reactor.process_once()
        assert observations == [
            (
                [
                    {
                        "command": "STOP",
                        "input_timestamp": _json_message_id(timestamp),
                        "message": "stopping",
                        "ok": True,
                        "request_id": "stop-1",
                    }
                ],
                1,
                timestamp,
            )
        ]
        assert reactor._reactor_stop_event.is_set()
        _assert_control_recorded(reactor, timestamp=timestamp, detail="STOP")
    elif mode in {"control-non-object", "control-unknown"}:
        non_object = mode == "control-non-object"
        body = json.dumps(["PING"]) if non_object else json.dumps({"command": "BOGUS"})
        timestamp = _write_control(reactor, body)
        reactor.process_once()
        response = _control_responses(reactor)[0]
        assert response["ok"] is False
        if non_object:
            assert response["command"] == "<invalid>"
            assert "JSON object" in response["error"]
            detail = "<invalid>"
        else:
            assert response["command"] == "BOGUS"
            assert response["error"] == "unknown command: BOGUS"
            detail = "BOGUS"
        _assert_control_recorded(reactor, timestamp=timestamp, detail=detail)
    else:
        _fire_control_failure(reactor, mode=mode, monkeypatch=monkeypatch)


def _fire_control_failure(
    reactor: Reactor,
    *,
    mode: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timestamp = _write_control(reactor, "PING")
    if mode == "control-output-failure":
        write_response = reactor._control_out_queue.write

        def fail_response(_body: str) -> None:
            raise OperationalError("control output unavailable")

        monkeypatch.setattr(reactor._control_out_queue, "write", fail_response)
        with pytest.raises(OperationalError, match="control output unavailable"):
            reactor.process_once()
        assert reactor._control_messages_handled == 0
        assert CONTROL_IN not in reactor._load_checkpoints()
        assert _control_audit_details(reactor) == []

        monkeypatch.setattr(reactor._control_out_queue, "write", write_response)
        reactor.process_once()
        assert len(_control_responses(reactor)) == 1
        _assert_control_recorded(reactor, timestamp=timestamp, detail="PING")
    else:
        metadata_connection = reactor._metadata_queue.conn.get_connection()
        metadata_runner = metadata_connection._runner
        run_statement = metadata_runner.run
        failed = False

        def fail_audit_insert(
            sql: str,
            params: tuple[Any, ...] = (),
            *,
            fetch: bool = False,
        ) -> Any:
            nonlocal failed
            if "INSERT INTO reactor_audit" in sql and not failed:
                failed = True
                raise OperationalError("control audit unavailable")
            return run_statement(sql, params, fetch=fetch)

        monkeypatch.setattr(metadata_runner, "run", fail_audit_insert)
        with pytest.raises(OperationalError, match="control audit unavailable"):
            reactor.process_once()
        assert len(_control_responses(reactor)) == 1
        assert reactor._control_messages_handled == 1
        assert CONTROL_IN not in reactor._load_checkpoints()
        assert _control_audit_details(reactor) == []

        monkeypatch.setattr(metadata_runner, "run", run_statement)
        reactor.process_once()
        assert len(_control_responses(reactor)) == 2
        _assert_control_recorded(
            reactor,
            timestamp=timestamp,
            detail="PING",
            handled=2,
        )


def _fire_scheduling_transition(reactor: Reactor, mode: str) -> None:
    if mode == "claim-idle":
        reactor.process_once()
        assert reactor._drive_owner_ident == threading.get_ident()
        assert reactor._inflight == set()
    elif mode == "dispatch-input":
        source = Queue(INBOX, db_path=reactor._metadata_queue.db_target)
        try:
            timestamp = source.write("work")
        finally:
            source.close()
        reactor.process_once()
        assert (INBOX, timestamp) in reactor._inflight
    elif mode == "drain-local-result":
        source = Queue(INBOX, db_path=reactor._metadata_queue.db_target)
        try:
            source.write("queued-after-local-result")
        finally:
            source.close()
        reactor._worker_results.put(
            WorkerResult(
                source_queue=INBOX,
                timestamp=101,
                value={"ok": True},
            )
        )
        reactor.notify_reactor_activity()
        queue_drain_saw_written_result: list[bool] = []
        drain_queue = reactor._drain_queue

        def observe_queue_drain() -> None:
            queue_drain_saw_written_result.append(
                _result_statuses(reactor) == ["output_written"]
            )
            drain_queue()

        reactor._drain_queue = observe_queue_drain
        reactor.process_once()
        assert _result_statuses(reactor) == ["output_written"]
        assert reactor._outputs_published == 1
        assert queue_drain_saw_written_result == [True]
    elif mode == "stop-before-turn":
        source = Queue(INBOX, db_path=reactor._metadata_queue.db_target)
        try:
            timestamp = source.write("do-not-dispatch")
        finally:
            source.close()
        reactor.request_stop()
        reactor.process_once()
        assert (INBOX, timestamp) not in reactor._inflight
    elif mode == "foreign-turn":
        _assert_foreign_turn_rejected(reactor)
    elif mode == "bounded-run":
        reactor.run_until_stopped(poll_interval=0.0, max_iterations=1)
        assert reactor._resources_closed
    else:
        reactor.stop()
        reactor.stop()
        assert reactor._resources_closed


@fires_transition_table("SM-REACTOR", REACTOR_TRANSITIONS)
def test_reference_reactor_fires_transition_table(
    transition_case: TransitionCase[ReactorPayload],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fire scheduling and ownership transitions against a real reactor."""

    mode = transition_case.payload.mode
    work_release = threading.Event()

    def blocking_processor(_item: object) -> dict[str, bool]:
        assert work_release.wait(timeout=2.0)
        return {"released": True}

    reactor = _make_reactor(
        tmp_path / "reactor.db",
        processor=(
            blocking_processor
            if mode
            in {
                "dispatch-input",
                "drain-local-result",
                "backlog-retry-recovery",
                "control-status",
            }
            else None
        ),
    )
    try:
        if mode == "backlog-retry-recovery":
            _fire_backlog_retry_recovery(reactor, monkeypatch)
        elif mode.startswith("control-"):
            _fire_control_transition(
                reactor,
                mode=mode,
                monkeypatch=monkeypatch,
            )
        else:
            _fire_scheduling_transition(reactor, mode)
    finally:
        work_release.set()
        reactor.stop()


@dataclass(frozen=True, slots=True)
class ReactorOutputPayload:
    mode: Literal[
        "empty",
        "publish",
        "duplicate",
        "retry",
        "mark-retry",
        "route-error",
        "budget",
    ]


REACTOR_OUTPUT_TRANSITIONS = (
    TransitionCase(
        transition_id="empty-backlog-succeeds",
        start_state="output-idle",
        event="drain pending output",
        guard="no pending row exists",
        next_state="output-idle",
        effects="no queue or sidecar mutation occurs",
        expected_result="the drain reports complete",
        payload=ReactorOutputPayload("empty"),
    ),
    TransitionCase(
        transition_id="pending-output-publishes",
        start_state="output-pending",
        event="publish pending output",
        guard="the configured route matches and insertion succeeds",
        next_state="output-written",
        effects="the exact-ID row is inserted and sidecar status advances",
        expected_result="the publish reports success",
        payload=ReactorOutputPayload("publish"),
    ),
    TransitionCase(
        transition_id="existing-exact-id-confirms",
        start_state="output-pending",
        event="publish an already present exact ID",
        guard="the integrity conflict resolves to an existing output row",
        next_state="output-written",
        effects="the pending sidecar row is confirmed without a duplicate insert",
        expected_result="one output exists and status advances",
        payload=ReactorOutputPayload("duplicate"),
    ),
    TransitionCase(
        transition_id="operational-failure-retries",
        start_state="output-pending",
        event="publish pending output",
        guard="the backend reports OperationalError",
        next_state="output-pending",
        effects="the pending row remains durable and no written marker is stored",
        expected_result="the publish reports retryable failure",
        payload=ReactorOutputPayload("retry"),
    ),
    TransitionCase(
        transition_id="status-mark-failure-replays-exact-id",
        start_state="output-pending",
        event="mark output written after insertion",
        guard="the exact-ID insert succeeded but the sidecar update fails",
        next_state="output-pending-with-published-id",
        effects="retry confirms the existing exact ID then advances durable status",
        expected_result="one output exists and replay reaches output-written",
        payload=ReactorOutputPayload("mark-retry"),
    ),
    TransitionCase(
        transition_id="route-mismatch-is-terminal",
        start_state="output-pending",
        event="publish pending output",
        guard="the durable row names a different output queue",
        next_state="output-pending",
        effects="no output is inserted and no written marker is stored",
        expected_result="RuntimeError identifies the stored and configured routes",
        payload=ReactorOutputPayload("route-error"),
    ),
    TransitionCase(
        transition_id="budget-leaves-backlog",
        start_state="multiple-outputs-pending",
        event="drain one output",
        guard="more rows exist than max_outputs",
        next_state="output-pending",
        effects="one row is published and the sentinel row remains pending",
        expected_result="the drain reports incomplete",
        payload=ReactorOutputPayload("budget"),
    ),
)


def _pending_result(reactor: Reactor, timestamp: int) -> PendingOutput:
    pending = reactor._record_pending_result(
        WorkerResult(
            source_queue=INBOX,
            timestamp=timestamp,
            value={"timestamp": timestamp},
        )
    )
    assert pending is not None
    return pending


def test_pending_output_id_is_allocated_outside_sidecar_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reactor = _make_reactor(tmp_path / "reactor-output-boundary.db")
    transaction_states: list[bool] = []
    original_generate_timestamp = reactor._output_queue.generate_timestamp

    def generate_timestamp() -> int:
        assert reactor._metadata_queue.conn is not None
        core = reactor._metadata_queue.conn.get_core()
        connection = core._runner.get_connection()
        transaction_states.append(bool(connection.in_transaction))
        return cast(int, original_generate_timestamp())

    monkeypatch.setattr(
        reactor._output_queue,
        "generate_timestamp",
        generate_timestamp,
    )
    try:
        pending = _pending_result(reactor, 200)
        assert pending.output_message_id > 0
        assert transaction_states == [False]
    finally:
        reactor.stop()


@fires_transition_table("SM-REACTOR-OUTPUT", REACTOR_OUTPUT_TRANSITIONS)
def test_reference_reactor_output_fires_transition_table(
    transition_case: TransitionCase[ReactorOutputPayload],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fire pending-output transitions with real queue and sidecar state."""

    reactor = _make_reactor(tmp_path / "reactor-output.db")
    mode = transition_case.payload.mode
    try:
        if mode == "empty":
            assert reactor._drain_pending_outputs(max_outputs=1) is True
            assert _result_statuses(reactor) == []
        elif mode == "publish":
            pending = _pending_result(reactor, 201)
            assert reactor._try_publish_output(pending) is True
            assert _result_statuses(reactor) == ["output_written"]
            assert (
                reactor._output_queue.peek_one(
                    exact_timestamp=pending.output_message_id,
                    include_claimed=True,
                )
                is not None
            )
        elif mode == "duplicate":
            pending = _pending_result(reactor, 202)
            reactor._output_queue.insert_messages(
                [(pending.payload, pending.output_message_id)]
            )
            assert reactor._try_publish_output(pending) is True
            assert _result_statuses(reactor) == ["output_written"]
            rows = reactor._output_queue.peek_many(
                10,
                include_claimed=True,
                with_timestamps=True,
            )
            assert [timestamp for _body, timestamp in rows].count(
                pending.output_message_id
            ) == 1
        elif mode == "retry":
            pending = _pending_result(reactor, 203)

            def fail_insert(_messages: object) -> None:
                raise OperationalError("injected publication failure")

            monkeypatch.setattr(reactor._output_queue, "insert_messages", fail_insert)
            assert reactor._try_publish_output(pending) is False
            assert _result_statuses(reactor) == ["output_pending"]
        elif mode == "mark-retry":
            pending = _pending_result(reactor, 204)
            mark_output_written = reactor._mark_output_written

            def fail_mark(_pending: PendingOutput) -> None:
                raise OperationalError("injected status-mark failure")

            monkeypatch.setattr(reactor, "_mark_output_written", fail_mark)
            assert reactor._try_publish_output(pending) is False
            assert _result_statuses(reactor) == ["output_pending"]
            assert (
                reactor._output_queue.peek_one(
                    exact_timestamp=pending.output_message_id,
                    include_claimed=True,
                )
                is not None
            )

            monkeypatch.setattr(
                reactor,
                "_mark_output_written",
                mark_output_written,
            )
            assert reactor._try_publish_output(pending) is True
            assert _result_statuses(reactor) == ["output_written"]
            rows = reactor._output_queue.peek_many(
                10,
                include_claimed=True,
                with_timestamps=True,
            )
            assert [timestamp for _body, timestamp in rows].count(
                pending.output_message_id
            ) == 1
        elif mode == "route-error":
            pending = _pending_result(reactor, 205)
            with reactor._metadata_queue.sidecar(transaction=True) as session:
                session.run(
                    """
                    UPDATE reactor_results
                    SET output_queue = ?
                    WHERE source_queue = ? AND input_ts = ?
                    """,
                    (
                        "different.outbox",
                        pending.source_queue,
                        pending.input_timestamp,
                    ),
                )
            pending = reactor._pending_output_rows(limit=1)[0]
            with pytest.raises(RuntimeError, match="route mismatch"):
                reactor._publish_output(pending)
            assert _result_statuses(reactor) == ["output_pending"]
            assert (
                reactor._output_queue.peek_one(
                    exact_timestamp=pending.output_message_id,
                    include_claimed=True,
                )
                is None
            )
        else:
            _pending_result(reactor, 206)
            _pending_result(reactor, 207)
            assert reactor._drain_pending_outputs(max_outputs=1) is False
            assert _result_statuses(reactor) == [
                "output_written",
                "output_pending",
            ]
    finally:
        reactor.stop()
