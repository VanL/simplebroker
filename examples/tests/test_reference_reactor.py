from __future__ import annotations

import json
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from reference_reactor import (  # noqa: E402
    BaseReactor,
    PendingOutput,
    Reactor,
    WorkItem,
    _read_json_messages,
    _send_control,
    _wait_for_control_reply,
    _write_json,
)

from simplebroker import Queue  # noqa: E402
from simplebroker.ext import OperationalError  # noqa: E402

INBOX_A = "reactor.inbox.a"
INBOX_B = "reactor.inbox.b"
CONTROL_IN = "reactor.control.in"
CONTROL_OUT = "reactor.control.out"
OUTBOX = "reactor.outbox"
NEW_OUTBOX = "reactor.outbox.new"


@pytest.fixture(autouse=True)
def fail_on_thread_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[list[threading.ExceptHookArgs]]:
    exceptions: list[threading.ExceptHookArgs] = []

    def record_exception(args: threading.ExceptHookArgs) -> None:
        exceptions.append(args)

    monkeypatch.setattr(threading, "excepthook", record_exception)
    yield exceptions
    assert exceptions == []


def _make_reactor(
    db_path: Path,
    *,
    output_queue: str = OUTBOX,
    processor: Any | None = None,
    worker_count: int = 2,
) -> Reactor:
    kwargs: dict[str, Any] = {}
    if processor is not None:
        kwargs["processor"] = processor
    return Reactor(
        input_queues=[INBOX_A, INBOX_B],
        output_queue=output_queue,
        control_in_queue=CONTROL_IN,
        control_out_queue=CONTROL_OUT,
        db=db_path,
        worker_count=worker_count,
        **kwargs,
    )


def _wait_for_outputs(db_path: Path, count: int, *, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if len(_read_json_messages(OUTBOX, db_path)) >= count:
            return
        time.sleep(0.01)
    raise TimeoutError(f"timed out waiting for {count} output messages")


def _wait_for_control_reply_at_timestamp(
    db_path: Path,
    *,
    input_timestamp: int,
    timeout: float = 2.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for payload, _timestamp in _read_json_messages(CONTROL_OUT, db_path):
            if payload.get("input_timestamp") == input_timestamp:
                return payload
        time.sleep(0.01)
    raise TimeoutError(
        f"timed out waiting for control reply to input {input_timestamp}"
    )


def _stop_reactor(reactor: Reactor, thread: threading.Thread) -> None:
    reactor.stop()
    thread.join(timeout=2.0)
    assert not thread.is_alive()


def _sidecar_rows(
    db_path: Path,
    sql: str,
    params: tuple[Any, ...] = (),
) -> list[tuple[Any, ...]]:
    queue = Queue(INBOX_A, db_path=str(db_path))
    with queue.sidecar() as session:
        return list(session.run(sql, params, fetch=True))


def _seed_pending_result(
    db_path: Path,
    *,
    input_timestamp: int,
    output_queue: str = OUTBOX,
    output_message_id: int,
    payload: str,
) -> None:
    metadata_queue = Queue(INBOX_A, db_path=str(db_path), persistent=True)
    try:
        with metadata_queue.sidecar(transaction=True) as session:
            session.run(
                """
                INSERT INTO reactor_results
                    (
                        source_queue,
                        input_ts,
                        output_queue,
                        output_message_id,
                        payload,
                        status,
                        error,
                        updated_at_ns
                    )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    INBOX_A,
                    input_timestamp,
                    output_queue,
                    output_message_id,
                    payload,
                    "output_pending",
                    None,
                    time.time_ns(),
                ),
            )
    finally:
        metadata_queue.close()


@pytest.mark.parametrize(
    ("input_queues", "output_queue", "control_in", "control_out", "duplicate"),
    [
        ([INBOX_A, INBOX_A], OUTBOX, CONTROL_IN, CONTROL_OUT, INBOX_A),
        ([INBOX_A, INBOX_B], INBOX_A, CONTROL_IN, CONTROL_OUT, INBOX_A),
        ([INBOX_A, INBOX_B], OUTBOX, INBOX_A, CONTROL_OUT, INBOX_A),
        ([INBOX_A, INBOX_B], OUTBOX, CONTROL_IN, INBOX_A, INBOX_A),
        (
            [INBOX_A, INBOX_B],
            CONTROL_IN,
            CONTROL_IN,
            CONTROL_OUT,
            CONTROL_IN,
        ),
        (
            [INBOX_A, INBOX_B],
            CONTROL_OUT,
            CONTROL_IN,
            CONTROL_OUT,
            CONTROL_OUT,
        ),
        (
            [INBOX_A, INBOX_B],
            OUTBOX,
            CONTROL_IN,
            CONTROL_IN,
            CONTROL_IN,
        ),
    ],
)
def test_reactor_rejects_overlapping_queue_roles(
    tmp_path: Path,
    input_queues: list[str],
    output_queue: str,
    control_in: str,
    control_out: str,
    duplicate: str,
) -> None:
    db_path = tmp_path / "reactor.db"
    reactor: Reactor | None = None
    try:
        with pytest.raises(ValueError, match="distinct") as exc_info:
            reactor = Reactor(
                input_queues=input_queues,
                output_queue=output_queue,
                control_in_queue=control_in,
                control_out_queue=control_out,
                db=db_path,
                worker_count=1,
            )
    finally:
        if reactor is not None:
            reactor.stop()

    assert duplicate in str(exc_info.value)
    assert not db_path.exists()


def test_reactor_rejects_empty_input_queues(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="input_queues cannot be empty"):
        Reactor(
            input_queues=[],
            output_queue=OUTBOX,
            control_in_queue=CONTROL_IN,
            control_out_queue=CONTROL_OUT,
            db=tmp_path / "reactor.db",
        )


def test_base_reactor_centralizes_process_wait_stop_loop(tmp_path: Path) -> None:
    class OneTurnReactor(BaseReactor):
        def __init__(self, db_path: Path) -> None:
            self.turns = 0
            self.backlog_drains = 0
            super().__init__(
                queues=["base.reactor"],
                default_handler=self._unexpected_handler,
                db=db_path,
                persistent=True,
                check_interval=1,
            )

        @staticmethod
        def _unexpected_handler(message: str, timestamp: int) -> None:
            del message, timestamp
            raise AssertionError("base reactor test should not dispatch a handler")

        def _drain_reactor_backlog(self) -> bool:
            self.backlog_drains += 1
            return True

        def _drain_queue(self) -> None:
            self.turns += 1
            self.request_stop()

    reactor = OneTurnReactor(tmp_path / "reactor.db")
    reactor.run_until_stopped(poll_interval=0.0, max_iterations=3)

    assert reactor.turns == 1
    assert reactor.backlog_drains >= 2
    assert reactor._resources_closed


def test_worker_result_event_wakes_background_reactor(tmp_path: Path) -> None:
    db_path = tmp_path / "reactor.db"
    _write_json(Queue(INBOX_A, db_path=str(db_path)), {"id": 1})

    worker_started = threading.Event()

    def processor(item: WorkItem) -> dict[str, Any]:
        worker_started.set()
        time.sleep(0.05)
        return {"source_queue": item.source_queue, "timestamp": item.timestamp}

    reactor = _make_reactor(db_path, processor=processor, worker_count=1)
    thread = threading.Thread(
        target=reactor.run_until_stopped,
        kwargs={"poll_interval": 5.0},
        daemon=True,
    )
    thread.start()
    try:
        assert worker_started.wait(timeout=1.0)
        _wait_for_outputs(db_path, 1, timeout=1.0)
    finally:
        _stop_reactor(reactor, thread)


def test_input_activity_wakes_background_reactor(tmp_path: Path) -> None:
    db_path = tmp_path / "reactor.db"
    processed = threading.Event()

    def processor(item: WorkItem) -> dict[str, Any]:
        processed.set()
        return {"source_queue": item.source_queue, "timestamp": item.timestamp}

    reactor = _make_reactor(db_path, processor=processor, worker_count=1)
    thread = threading.Thread(
        target=reactor.run_until_stopped,
        kwargs={"poll_interval": 5.0},
        daemon=True,
    )
    thread.start()
    try:
        time.sleep(0.1)
        _write_json(Queue(INBOX_A, db_path=str(db_path)), {"id": 1})
        assert processed.wait(timeout=1.0)
        _wait_for_outputs(db_path, 1, timeout=1.0)
    finally:
        _stop_reactor(reactor, thread)


def test_reactor_turns_have_single_thread_owner(tmp_path: Path) -> None:
    db_path = tmp_path / "reactor.db"
    _write_json(Queue(INBOX_A, db_path=str(db_path)), {"id": 1})

    reactor = _make_reactor(db_path)
    thread = threading.Thread(target=reactor.run_until_stopped, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 1.0
        while reactor._drive_owner_ident is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert reactor._drive_owner_ident is not None

        with pytest.raises(RuntimeError, match="single-owner"):
            reactor.process_once()
    finally:
        _stop_reactor(reactor, thread)


def test_reactor_rejects_dynamic_queue_mutators(tmp_path: Path) -> None:
    reactor = _make_reactor(tmp_path / "reactor.db")
    try:
        with pytest.raises(NotImplementedError, match="fixed at construction"):
            reactor.add_queue("late.queue")
        with pytest.raises(NotImplementedError, match="fixed at construction"):
            reactor.remove_queue(INBOX_A)
    finally:
        reactor.stop()


def test_stop_during_startup_waits_for_drive_thread_before_closing(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "reactor.db"
    reactor = _make_reactor(db_path)
    strategy_entered = threading.Event()
    release_strategy = threading.Event()
    original_strategy_start = reactor._ensure_polling_strategy_started

    def delayed_strategy_start() -> None:
        strategy_entered.set()
        assert release_strategy.wait(timeout=2.0)
        original_strategy_start()

    reactor._ensure_polling_strategy_started = delayed_strategy_start  # type: ignore[method-assign]

    thread = reactor.start()
    assert strategy_entered.wait(timeout=2.0)
    stopper = threading.Thread(target=reactor.stop)
    stopper.start()
    time.sleep(0.05)
    assert stopper.is_alive()

    release_strategy.set()
    stopper.join(timeout=2.0)
    thread.join(timeout=2.0)
    assert not stopper.is_alive()
    assert not thread.is_alive()
    assert reactor._resources_closed


def test_stop_waits_for_manual_drive_thread_before_closing_queues(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "reactor.db"
    _write_json(Queue(INBOX_A, db_path=str(db_path)), {"id": 1})

    reactor = _make_reactor(db_path, worker_count=1)
    publish_entered = threading.Event()
    release_publish = threading.Event()
    original_publish = reactor._publish_output

    def delayed_publish(*args: Any, **kwargs: Any) -> Any:
        publish_entered.set()
        assert release_publish.wait(timeout=2.0)
        return original_publish(*args, **kwargs)

    reactor._publish_output = delayed_publish  # type: ignore[method-assign]

    thread = threading.Thread(target=reactor.run_until_stopped, daemon=True)
    thread.start()
    assert publish_entered.wait(timeout=2.0)

    stopper = threading.Thread(target=reactor.stop)
    stopper.start()
    time.sleep(0.05)
    assert stopper.is_alive()

    release_publish.set()
    stopper.join(timeout=2.0)
    thread.join(timeout=2.0)
    assert not stopper.is_alive()
    assert not thread.is_alive()
    assert len(_read_json_messages(OUTBOX, db_path)) == 1
    assert reactor._resources_closed


def test_manual_drive_thread_self_closes_after_external_stop_join_false(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "reactor.db"
    reactor = _make_reactor(db_path)

    thread = threading.Thread(target=reactor.run_until_stopped, daemon=True)
    thread.start()
    deadline = time.monotonic() + 1.0
    while reactor._drive_owner_ident is None and time.monotonic() < deadline:
        time.sleep(0.01)
    assert reactor._drive_owner_ident is not None

    reactor.stop(join=False)
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert reactor._resources_closed


def test_control_lane_is_peek_checkpointed_not_consumed(tmp_path: Path) -> None:
    db_path = tmp_path / "reactor.db"
    reactor = _make_reactor(db_path)
    thread = reactor.start()
    try:
        _send_control(db_path, "PING", "ping-1")
        ping_reply = _wait_for_control_reply(db_path, request_id="ping-1")
        assert ping_reply["ok"] is True
        assert ping_reply["message"] == "PONG"

        control_rows = cast(
            list[tuple[str, int]],
            Queue(CONTROL_IN, db_path=str(db_path)).peek_many(with_timestamps=True),
        )
        ping_timestamps = [
            timestamp
            for body, timestamp in control_rows
            if json.loads(body).get("request_id") == "ping-1"
        ]
        assert len(ping_timestamps) == 1

        _send_control(db_path, "STATUS", "status-1")
        status = _wait_for_control_reply(db_path, request_id="status-1")
        assert status["checkpoints"][CONTROL_IN] >= ping_timestamps[0]
    finally:
        _send_control(db_path, "STOP", "stop-control")
        _wait_for_control_reply(db_path, request_id="stop-control")
        thread.join(timeout=2.0)
        reactor.stop()


@pytest.mark.parametrize("body", ["[]", "null", "1", "true", '"PING"'])
def test_non_object_control_payload_returns_error_and_lane_progresses(
    tmp_path: Path,
    body: str,
) -> None:
    db_path = tmp_path / "reactor.db"
    reactor = _make_reactor(db_path)
    thread = reactor.start()
    try:
        with Queue(CONTROL_IN, db_path=str(db_path)) as control_queue:
            timestamp = control_queue.generate_timestamp()
            control_queue.insert_messages([(body, timestamp)])

        invalid_reply = _wait_for_control_reply_at_timestamp(
            db_path,
            input_timestamp=timestamp,
        )
        assert invalid_reply["ok"] is False
        assert invalid_reply["request_id"] is None
        assert "JSON object or plain-text command" in invalid_reply["error"]

        _send_control(db_path, "PING", "ping-after-invalid")
        ping_reply = _wait_for_control_reply(
            db_path,
            request_id="ping-after-invalid",
        )
        assert ping_reply["ok"] is True
        assert ping_reply["message"] == "PONG"

        checkpoint_rows = _sidecar_rows(
            db_path,
            """
            SELECT last_processed_ts
            FROM reactor_checkpoints
            WHERE queue_name = ?
            """,
            (CONTROL_IN,),
        )
        assert checkpoint_rows
        assert int(checkpoint_rows[0][0]) >= timestamp
    finally:
        _stop_reactor(reactor, thread)


def test_plain_text_control_command_remains_supported(tmp_path: Path) -> None:
    db_path = tmp_path / "reactor.db"
    reactor = _make_reactor(db_path)
    thread = reactor.start()
    try:
        with Queue(CONTROL_IN, db_path=str(db_path)) as control_queue:
            timestamp = control_queue.generate_timestamp()
            control_queue.insert_messages([("PING", timestamp)])

        reply = _wait_for_control_reply_at_timestamp(
            db_path,
            input_timestamp=timestamp,
        )
        assert reply["ok"] is True
        assert reply["command"] == "PING"
        assert reply["message"] == "PONG"
    finally:
        _stop_reactor(reactor, thread)


@pytest.mark.parametrize(
    ("command", "error_command"),
    [("", "<empty>"), ("DANCE", "DANCE")],
)
def test_unknown_object_control_command_returns_error(
    tmp_path: Path,
    command: str,
    error_command: str,
) -> None:
    db_path = tmp_path / "reactor.db"
    reactor = _make_reactor(db_path)
    thread = reactor.start()
    try:
        _send_control(db_path, command, "unknown-command")
        reply = _wait_for_control_reply(
            db_path,
            request_id="unknown-command",
        )
        assert reply["ok"] is False
        assert error_command in reply["error"]
    finally:
        _stop_reactor(reactor, thread)


def test_checkpointed_control_is_not_reprocessed_after_restart(tmp_path: Path) -> None:
    db_path = tmp_path / "reactor.db"
    reactor = _make_reactor(db_path)
    thread = reactor.start()
    try:
        _send_control(db_path, "PING", "ping-once")
        _wait_for_control_reply(db_path, request_id="ping-once")
    finally:
        _stop_reactor(reactor, thread)

    restarted = _make_reactor(db_path)
    restart_thread = restarted.start()
    try:
        _send_control(db_path, "PING", "restart-barrier")
        barrier_reply = _wait_for_control_reply(
            db_path,
            request_id="restart-barrier",
        )
        assert barrier_reply["message"] == "PONG"

        ping_replies = [
            payload
            for payload, _timestamp in _read_json_messages(CONTROL_OUT, db_path)
            if payload.get("request_id") == "ping-once"
        ]
        barrier_replies = [
            payload
            for payload, _timestamp in _read_json_messages(CONTROL_OUT, db_path)
            if payload.get("request_id") == "restart-barrier"
        ]
        assert len(ping_replies) == 1
        assert len(barrier_replies) == 1
    finally:
        _stop_reactor(restarted, restart_thread)


def test_pending_output_replay_waits_for_first_driven_turn(tmp_path: Path) -> None:
    db_path = tmp_path / "reactor.db"
    bootstrap = _make_reactor(db_path, worker_count=1)
    bootstrap.stop()

    output_queue = Queue(OUTBOX, db_path=str(db_path), persistent=True)
    try:
        output_message_id = output_queue.generate_timestamp()
    finally:
        output_queue.close()

    payload = json.dumps({"replayed": True}, sort_keys=True)
    _seed_pending_result(
        db_path,
        input_timestamp=1000,
        output_message_id=output_message_id,
        payload=payload,
    )

    class RequiresInitializedPublishReactor(Reactor):
        def __init__(self, **kwargs: Any) -> None:
            self.subclass_ready = False
            try:
                super().__init__(**kwargs)
            except BaseException:
                self.stop()
                raise
            self.subclass_ready = True

        def _publish_output(self, pending: PendingOutput) -> None:
            assert self.subclass_ready
            super()._publish_output(pending)

    reactor = RequiresInitializedPublishReactor(
        input_queues=[INBOX_A, INBOX_B],
        output_queue=OUTBOX,
        control_in_queue=CONTROL_IN,
        control_out_queue=CONTROL_OUT,
        db=db_path,
        worker_count=1,
    )
    try:
        assert _read_json_messages(OUTBOX, db_path) == []

        reactor.process_once()

        outputs = _read_json_messages(OUTBOX, db_path)
        assert outputs.count(({"replayed": True}, output_message_id)) == 1

        rows = _sidecar_rows(
            db_path,
            """
            SELECT status
            FROM reactor_results
            WHERE source_queue = ? AND input_ts = ?
            """,
            (INBOX_A, 1000),
        )
        assert rows == [("output_written",)]
    finally:
        reactor.stop()


def test_pending_output_rejects_configured_route_drift(tmp_path: Path) -> None:
    db_path = tmp_path / "reactor.db"
    bootstrap = _make_reactor(db_path, worker_count=1)
    bootstrap.stop()

    with Queue(OUTBOX, db_path=str(db_path), persistent=True) as output_queue:
        output_message_id = output_queue.generate_timestamp()

    payload = json.dumps({"replayed": True}, sort_keys=True)
    _seed_pending_result(
        db_path,
        input_timestamp=1000,
        output_queue=OUTBOX,
        output_message_id=output_message_id,
        payload=payload,
    )

    drifted = _make_reactor(
        db_path,
        output_queue=NEW_OUTBOX,
        worker_count=1,
    )
    try:
        with pytest.raises(RuntimeError) as exc_info:
            drifted.process_once()
        error = str(exc_info.value)
        assert OUTBOX in error
        assert NEW_OUTBOX in error
        assert _read_json_messages(OUTBOX, db_path) == []
        assert _read_json_messages(NEW_OUTBOX, db_path) == []
        assert _sidecar_rows(
            db_path,
            """
            SELECT status
            FROM reactor_results
            WHERE source_queue = ? AND input_ts = ?
            """,
            (INBOX_A, 1000),
        ) == [("output_pending",)]
    finally:
        drifted.stop()

    recovered = _make_reactor(db_path, output_queue=OUTBOX, worker_count=1)
    try:
        recovered.process_once()
        assert (
            _read_json_messages(OUTBOX, db_path).count(
                ({"replayed": True}, output_message_id)
            )
            == 1
        )
        assert _sidecar_rows(
            db_path,
            """
            SELECT status
            FROM reactor_results
            WHERE source_queue = ? AND input_ts = ?
            """,
            (INBOX_A, 1000),
        ) == [("output_written",)]
    finally:
        recovered.stop()


def test_pending_output_drain_budget_fetches_one_backlog_sentinel(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "reactor.db"
    reactor = _make_reactor(db_path, worker_count=1)
    try:
        with Queue(OUTBOX, db_path=str(db_path), persistent=True) as output_queue:
            output_ids = [output_queue.generate_timestamp() for _ in range(3)]

        for index, (input_timestamp, output_id) in enumerate(
            zip((1000, 2000, 3000), output_ids, strict=True),
            start=1,
        ):
            _seed_pending_result(
                db_path,
                input_timestamp=input_timestamp,
                output_message_id=output_id,
                payload=json.dumps({"replayed": index}, sort_keys=True),
            )

        first_two = reactor._pending_output_rows(limit=2)
        assert [pending.input_timestamp for pending in first_two] == [1000, 2000]

        assert reactor._drain_pending_outputs(max_outputs=0) is False
        assert _read_json_messages(OUTBOX, db_path) == []

        assert reactor._drain_pending_outputs(max_outputs=1) is False
        assert len(_read_json_messages(OUTBOX, db_path)) == 1
        assert reactor._pending_output_count() == 2

        assert reactor._drain_pending_outputs(max_outputs=1) is False
        assert len(_read_json_messages(OUTBOX, db_path)) == 2
        assert reactor._pending_output_count() == 1

        assert reactor._drain_pending_outputs(max_outputs=1) is True
        outputs = _read_json_messages(OUTBOX, db_path)
        assert len(outputs) == 3
        assert sorted(timestamp for _payload, timestamp in outputs) == sorted(
            output_ids
        )
        assert reactor._pending_output_count() == 0
        assert reactor._drain_pending_outputs(max_outputs=0) is True
        assert _sidecar_rows(
            db_path,
            """
            SELECT input_ts, status
            FROM reactor_results
            WHERE source_queue = ?
            ORDER BY input_ts
            """,
            (INBOX_A,),
        ) == [
            (1000, "output_written"),
            (2000, "output_written"),
            (3000, "output_written"),
        ]
    finally:
        reactor.stop()


def test_existing_output_exact_id_replay_marks_written_without_duplicate(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "reactor.db"
    bootstrap = _make_reactor(db_path, worker_count=1)
    bootstrap.stop()

    output_message_id = 1000
    payload = json.dumps({"already": "published"}, sort_keys=True)
    Queue(OUTBOX, db_path=str(db_path)).insert_messages([(payload, output_message_id)])
    _seed_pending_result(
        db_path,
        input_timestamp=1000,
        output_message_id=output_message_id,
        payload=payload,
    )

    reactor = _make_reactor(db_path, worker_count=1)
    try:
        reactor.process_once()
        outputs = _read_json_messages(OUTBOX, db_path)
        assert outputs.count(({"already": "published"}, output_message_id)) == 1
        rows = _sidecar_rows(
            db_path,
            """
            SELECT status
            FROM reactor_results
            WHERE source_queue = ? AND input_ts = ?
            """,
            (INBOX_A, 1000),
        )
        assert rows == [("output_written",)]
    finally:
        reactor.stop()


def test_pending_output_retries_in_process_after_transient_publish_failure(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "reactor.db"
    _write_json(Queue(INBOX_A, db_path=str(db_path)), {"id": 1})

    class FailOncePublishReactor(Reactor):
        def __init__(self, **kwargs: Any) -> None:
            self.publish_attempts = 0
            super().__init__(**kwargs)

        def _publish_output(self, *args: Any, **kwargs: Any) -> None:
            self.publish_attempts += 1
            if self.publish_attempts == 1:
                raise OperationalError("simulated transient publish failure")
            super()._publish_output(*args, **kwargs)

    reactor = FailOncePublishReactor(
        input_queues=[INBOX_A, INBOX_B],
        output_queue=OUTBOX,
        control_in_queue=CONTROL_IN,
        control_out_queue=CONTROL_OUT,
        db=db_path,
        worker_count=1,
    )
    try:
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            reactor.process_once()
            if _read_json_messages(OUTBOX, db_path):
                break
            reactor.wait_for_activity(0.01)

        outputs = _read_json_messages(OUTBOX, db_path)
        assert len(outputs) == 1
        assert reactor.publish_attempts >= 2
        rows = _sidecar_rows(
            db_path,
            """
            SELECT status
            FROM reactor_results
            WHERE source_queue = ?
            """,
            (INBOX_A,),
        )
        assert rows == [("output_written",)]
    finally:
        reactor.stop()


def test_output_backlog_blocks_new_input_but_not_control_lane(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "reactor.db"
    _write_json(Queue(INBOX_A, db_path=str(db_path)), {"id": "first"})

    class AlwaysFailPublishReactor(Reactor):
        def __init__(self, **kwargs: Any) -> None:
            self.publish_attempts = 0
            super().__init__(**kwargs)

        def _publish_output(self, *args: Any, **kwargs: Any) -> None:
            self.publish_attempts += 1
            raise OperationalError("simulated stuck output sink")

    reactor = AlwaysFailPublishReactor(
        input_queues=[INBOX_A, INBOX_B],
        output_queue=OUTBOX,
        control_in_queue=CONTROL_IN,
        control_out_queue=CONTROL_OUT,
        db=db_path,
        worker_count=1,
    )
    thread = reactor.start()
    try:
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            rows = _sidecar_rows(
                db_path,
                """
                SELECT status
                FROM reactor_results
                WHERE source_queue = ?
                """,
                (INBOX_A,),
            )
            if rows == [("output_pending",)]:
                break
            time.sleep(0.01)
        else:
            raise AssertionError("output backlog did not become pending")
        attempts_after_backlog = reactor.publish_attempts

        _write_json(Queue(INBOX_B, db_path=str(db_path)), {"id": "second"})

        _send_control(db_path, "STATUS", "status-while-blocked")
        status = _wait_for_control_reply(
            db_path,
            request_id="status-while-blocked",
        )
        assert status["ok"] is True
        assert status["output_backlog_blocked"] is True
        assert status["pending_output_backlog"] == 1
        assert reactor.publish_attempts > attempts_after_backlog

        inbox_b_seen = _sidecar_rows(
            db_path,
            """
            SELECT status
            FROM reactor_seen
            WHERE source_queue = ?
            """,
            (INBOX_B,),
        )
        assert inbox_b_seen == []

        _send_control(db_path, "STOP", "stop-while-blocked")
        stop_reply = _wait_for_control_reply(
            db_path,
            request_id="stop-while-blocked",
        )
        assert stop_reply["ok"] is True
        thread.join(timeout=2.0)
        assert not thread.is_alive()
    finally:
        reactor.stop()


def test_crash_after_result_record_replays_pending_output_on_restart(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "reactor.db"
    _write_json(Queue(INBOX_A, db_path=str(db_path)), {"id": 1})

    class SimulatedCrash(Exception):
        pass

    class CrashAfterRecordReactor(Reactor):
        def _publish_output(self, *args: Any, **kwargs: Any) -> None:
            raise SimulatedCrash

    reactor = CrashAfterRecordReactor(
        input_queues=[INBOX_A, INBOX_B],
        output_queue=OUTBOX,
        control_in_queue=CONTROL_IN,
        control_out_queue=CONTROL_OUT,
        db=db_path,
        worker_count=1,
    )
    try:
        deadline = time.monotonic() + 2.0
        with pytest.raises(SimulatedCrash):
            while True:
                if time.monotonic() >= deadline:
                    raise AssertionError("simulated crash did not occur")
                reactor.process_once()
                reactor.wait_for_activity(0.01)
    finally:
        reactor.stop()

    pending_rows = _sidecar_rows(
        db_path,
        """
        SELECT status
        FROM reactor_results
        WHERE source_queue = ?
        """,
        (INBOX_A,),
    )
    assert pending_rows == [("output_pending",)]

    restarted = _make_reactor(db_path, worker_count=1)
    try:
        restarted.process_once()
        outputs = _read_json_messages(OUTBOX, db_path)
        assert len(outputs) == 1
        rows = _sidecar_rows(
            db_path,
            """
            SELECT status
            FROM reactor_results
            WHERE source_queue = ?
            """,
            (INBOX_A,),
        )
        assert rows == [("output_written",)]
    finally:
        restarted.stop()


def test_processor_error_publishes_error_envelope_and_advances_checkpoint(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "reactor.db"
    _write_json(Queue(INBOX_A, db_path=str(db_path)), {"id": "poison"})

    def poison_processor(item: WorkItem) -> dict[str, Any]:
        raise ValueError(f"poison {item.timestamp}")

    reactor = _make_reactor(db_path, processor=poison_processor, worker_count=1)
    thread = reactor.start()
    try:
        _wait_for_outputs(db_path, 1)
        outputs = _read_json_messages(OUTBOX, db_path)
        assert len(outputs) == 1
        payload, _output_timestamp = outputs[0]
        assert payload["ok"] is False
        assert "poison" in payload["error"]

        rows = _sidecar_rows(
            db_path,
            """
            SELECT last_processed_ts
            FROM reactor_checkpoints
            WHERE queue_name = ?
            """,
            (INBOX_A,),
        )
        assert rows == [(payload["input_timestamp"],)]

        time.sleep(0.1)
        assert len(_read_json_messages(OUTBOX, db_path)) == 1
        seen_rows = _sidecar_rows(
            db_path,
            """
            SELECT status
            FROM reactor_seen
            WHERE source_queue = ? AND input_ts = ?
            """,
            (INBOX_A, payload["input_timestamp"]),
        )
        assert seen_rows == [("output_written",)]
    finally:
        _stop_reactor(reactor, thread)


def test_non_json_processor_result_publishes_error_and_advances_checkpoint(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "reactor.db"
    _write_json(Queue(INBOX_A, db_path=str(db_path)), {"id": "non-json"})

    def non_json_processor(item: WorkItem) -> dict[str, Any]:
        return {"timestamp": item.timestamp, "not_json": object()}

    reactor = _make_reactor(db_path, processor=non_json_processor, worker_count=1)
    thread = reactor.start()
    try:
        _wait_for_outputs(db_path, 1)
        outputs = _read_json_messages(OUTBOX, db_path)
        assert len(outputs) == 1
        payload, _output_timestamp = outputs[0]
        assert payload["ok"] is False
        assert "not JSON serializable" in payload["error"]

        rows = _sidecar_rows(
            db_path,
            """
            SELECT last_processed_ts
            FROM reactor_checkpoints
            WHERE queue_name = ?
            """,
            (INBOX_A,),
        )
        assert rows == [(payload["input_timestamp"],)]
        time.sleep(0.1)
        assert len(_read_json_messages(OUTBOX, db_path)) == 1
    finally:
        _stop_reactor(reactor, thread)


def test_per_queue_single_inflight_preserves_source_order(tmp_path: Path) -> None:
    db_path = tmp_path / "reactor.db"
    for index in range(3):
        _write_json(Queue(INBOX_A, db_path=str(db_path)), {"source": "a", "id": index})
    _write_json(Queue(INBOX_B, db_path=str(db_path)), {"source": "b", "id": 1})

    active_sources: set[str] = set()
    active_lock = threading.Lock()
    overlap_errors: list[str] = []
    cross_queue_overlap = threading.Event()

    def processor(item: WorkItem) -> dict[str, Any]:
        with active_lock:
            if item.source_queue in active_sources:
                overlap_errors.append(item.source_queue)
            active_sources.add(item.source_queue)
            if len(active_sources) > 1:
                cross_queue_overlap.set()
        try:
            time.sleep(0.02)
            return {"source_queue": item.source_queue, "timestamp": item.timestamp}
        finally:
            with active_lock:
                active_sources.remove(item.source_queue)

    reactor = _make_reactor(db_path, processor=processor, worker_count=4)
    thread = reactor.start()
    try:
        _wait_for_outputs(db_path, 4)
        assert overlap_errors == []
        assert cross_queue_overlap.is_set()

        outputs = [
            payload for payload, _timestamp in _read_json_messages(OUTBOX, db_path)
        ]
        inbox_a_timestamps = [
            payload["input_timestamp"]
            for payload in outputs
            if payload["source_queue"] == INBOX_A
        ]
        assert inbox_a_timestamps == sorted(inbox_a_timestamps)
    finally:
        _stop_reactor(reactor, thread)
