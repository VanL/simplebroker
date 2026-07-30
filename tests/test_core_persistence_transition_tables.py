"""Executable transition tables for core persistent-state machines."""

from __future__ import annotations

import json
import os
import select
import signal
import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock

import pytest

from simplebroker import open_broker
from simplebroker._backends.sqlite.schema import (
    ensure_schema_v5,
    initialize_database,
    messages_has_claimed_column,
    meta_table_exists,
    migrate_schema,
    pending_queue_ts_index_exists,
    ts_unique_index_exists,
)
from simplebroker._constants import SCHEMA_VERSION
from simplebroker._dump import LOAD_BATCH_SIZE, LoadResult, load_lines
from simplebroker._exceptions import IntegrityError, OperationalError, TimestampError
from simplebroker._runner import SetupPhase, SQLiteRunner
from simplebroker._timestamp import MAX_LOGICAL_COUNTER
from simplebroker.db import BrokerDB
from tests.helpers.state_machine_contracts import TransitionCase, fires_transition_table

pytestmark = pytest.mark.sqlite_only
_FORK_PROBE_TIMEOUT = 5.0


def _case(
    transition_id: str,
    start_state: str,
    event: str,
    next_state: str,
    effects: str,
    expected_result: str,
) -> TransitionCase[str]:
    return TransitionCase(
        transition_id=transition_id,
        start_state=start_state,
        event=event,
        guard=f"machine starts in {start_state!r}; event {event!r} is enabled",
        next_state=next_state,
        effects=effects,
        expected_result=expected_result,
        payload=transition_id,
    )


def _kill_and_reap(pid: int) -> int:
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    return os.waitpid(pid, 0)[1]


def _assert_fork_probe(pid: int, read_fd: int, *, label: str) -> None:
    deadline = time.monotonic() + _FORK_PROBE_TIMEOUT
    payload = b""
    status: int | None = None
    try:
        readable, _, _ = select.select([read_fd], [], [], _FORK_PROBE_TIMEOUT)
        if not readable:
            status = _kill_and_reap(pid)
            raise AssertionError(
                f"{label} child timed out before reporting; pid={pid}, status={status}"
            )
        payload = os.read(read_fd, 1)
        while time.monotonic() < deadline:
            waited_pid, candidate_status = os.waitpid(pid, os.WNOHANG)
            if waited_pid == pid:
                status = candidate_status
                break
            time.sleep(0.01)
        if status is None:
            status = _kill_and_reap(pid)
            raise AssertionError(
                f"{label} child reported but did not exit; "
                f"pid={pid}, payload={payload!r}, status={status}"
            )
        exit_code = os.waitstatus_to_exitcode(status)
        assert payload == b"1" and exit_code == 0, (
            f"{label} child failed; pid={pid}, payload={payload!r}, "
            f"status={status}, exit_code={exit_code}"
        )
    finally:
        os.close(read_fd)
        if status is None:
            _kill_and_reap(pid)


SQLITE_SCHEMA_TRANSITIONS = (
    _case(
        "BOOTSTRAP",
        "absent",
        "initialize",
        "current",
        "create schema and metadata atomically",
        "all current schema facts exist",
    ),
    _case(
        "MIGRATE_V1",
        "v1",
        "migrate",
        "current",
        "apply v2 through current in order",
        "versions 2, 3, 4, and 5 are published",
    ),
    _case(
        "REJECT_DUPLICATE_TIMESTAMPS",
        "v2 with duplicate timestamps",
        "migrate v3",
        "v2",
        "roll back the failed unique-index migration",
        "migration raises and v3 is not published",
    ),
    _case(
        "BOOTSTRAP_ROLLBACK",
        "absent",
        "bootstrap SQL fails",
        "absent",
        "roll back the explicit bootstrap transaction",
        "failure propagates and meta table is absent",
    ),
    _case(
        "CURRENT_REPAIR",
        "current version with missing pending index",
        "ensure current schema",
        "current and repaired",
        "recreate the missing current-version index",
        "version is unchanged and index exists",
    ),
    _case(
        "CURRENT_IDEMPOTENT",
        "current and complete",
        "migrate current schema again",
        "current and complete",
        "verify or recreate idempotent structures without version writes",
        "schema facts remain true",
    ),
)


def _create_v1(path: Path, *, duplicate_timestamps: bool = False) -> None:
    with closing(sqlite3.connect(path)) as conn:
        conn.execute(
            "CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "queue TEXT NOT NULL, body TEXT NOT NULL, ts INTEGER NOT NULL)"
        )
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL)")
        conn.execute("INSERT INTO meta (key, value) VALUES ('last_ts', 0)")
        if duplicate_timestamps:
            conn.executemany(
                "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
                [("q", "one", 7), ("q", "two", 7)],
            )
        conn.commit()


class _FailingBootstrapRunner:
    def __init__(self, runner: SQLiteRunner) -> None:
        self.runner = runner
        self.failed = False

    def run(self, sql: str, *args: object, **kwargs: object) -> object:
        if not self.failed and "CREATE TABLE IF NOT EXISTS messages" in sql:
            self.failed = True
            raise sqlite3.OperationalError("bootstrap fault")
        return self.runner.run(sql, *args, **kwargs)  # type: ignore[arg-type]

    def __getattr__(self, name: str) -> object:
        return getattr(self.runner, name)


@fires_transition_table("SM-SQLITE-SCHEMA", SQLITE_SCHEMA_TRANSITIONS)
def test_sqlite_schema_fires_transition_table(
    transition_case: TransitionCase[str],
    tmp_path: Path,
) -> None:
    path = tmp_path / f"{transition_case.payload}.db"
    runner = SQLiteRunner(str(path))
    versions: list[int] = []
    try:
        if transition_case.payload == "BOOTSTRAP":
            initialize_database(runner, run_with_retry=lambda operation: operation())
            assert messages_has_claimed_column(runner)
            assert pending_queue_ts_index_exists(runner)
            return
        if transition_case.payload == "BOOTSTRAP_ROLLBACK":
            failing_runner = _FailingBootstrapRunner(runner)
            with pytest.raises(sqlite3.OperationalError, match="bootstrap fault"):
                initialize_database(
                    failing_runner,  # type: ignore[arg-type]
                    run_with_retry=lambda operation: operation(),
                )
            assert not runner.get_connection().in_transaction
            assert not meta_table_exists(runner)
            return
        if transition_case.payload in {"CURRENT_REPAIR", "CURRENT_IDEMPOTENT"}:
            initialize_database(runner, run_with_retry=lambda operation: operation())
            if transition_case.payload == "CURRENT_REPAIR":
                runner.run("DROP INDEX idx_messages_pending_queue_ts")
                assert not pending_queue_ts_index_exists(runner)
                ensure_schema_v5(
                    runner,
                    current_version=SCHEMA_VERSION,
                    write_schema_version=lambda version: versions.append(version),
                )
            else:
                migrate_schema(
                    runner,
                    current_version=SCHEMA_VERSION,
                    write_schema_version=versions.append,
                )
            assert versions == []
            assert messages_has_claimed_column(runner)
            assert pending_queue_ts_index_exists(runner)
            return

        _create_v1(
            path, duplicate_timestamps=transition_case.payload.endswith("TIMESTAMPS")
        )
        runner.close()
        runner = SQLiteRunner(str(path))
        if transition_case.payload == "MIGRATE_V1":
            migrate_schema(
                runner,
                current_version=1,
                write_schema_version=versions.append,
            )
            assert versions == [2, 3, 4, 5]
            return

        with pytest.raises(RuntimeError, match="duplicate timestamps"):
            migrate_schema(
                runner,
                current_version=1,
                write_schema_version=versions.append,
            )
        assert versions == [2]
        assert not ts_unique_index_exists(runner)
    finally:
        runner.close()


DUMP_LOAD_TRANSITIONS = (
    _case(
        "HEADER_ONLY",
        "awaiting header",
        "valid header then EOF",
        "complete",
        "perform no mutations",
        "zero messages and aliases",
    ),
    _case(
        "DUPLICATE_HEADER",
        "body",
        "second header",
        "rejected",
        "stop parsing",
        "line-numbered duplicate-header error",
    ),
    _case(
        "BLANK_ONLY_EOF",
        "body",
        "blank records then EOF",
        "complete",
        "ignore blank records",
        "zero messages and aliases",
    ),
    _case(
        "BATCH_SIZE_MINUS_ONE",
        "body",
        "499 messages then EOF",
        "complete",
        "flush the final partial batch",
        "all messages persist",
    ),
    _case(
        "BATCH_SIZE",
        "body",
        "500 messages",
        "complete",
        "flush one full batch",
        "all messages persist",
    ),
    _case(
        "BATCH_SIZE_PLUS_ONE",
        "body",
        "501 messages then EOF",
        "complete",
        "flush a full and a partial batch",
        "all messages persist",
    ),
    _case(
        "LATER_ERROR_PRESERVES_FLUSHED_BATCH",
        "body after one full batch",
        "unknown record",
        "rejected after partial mutation",
        "retain the committed first batch",
        "error reports the later line",
    ),
    _case(
        "APPLY_ALIAS",
        "body with no aliases",
        "valid alias record",
        "body with alias applied",
        "apply alias immediately",
        "load result and destination expose the alias",
    ),
    _case(
        "MISSING_HEADER_EOF",
        "awaiting header",
        "blank input reaches EOF",
        "rejected",
        "perform no mutations",
        "missing-header error",
    ),
    _case(
        "MALFORMED_JSON",
        "body",
        "malformed JSON record",
        "rejected",
        "stop at malformed record",
        "line-numbered malformed-JSON error",
    ),
    _case(
        "NON_OBJECT_RECORD",
        "body",
        "JSON array record",
        "rejected",
        "reject non-object JSON",
        "line-numbered record-shape error",
    ),
    _case(
        "WRONG_FORMAT",
        "awaiting header",
        "header names another format",
        "rejected",
        "perform no mutations",
        "unrecognized-format error",
    ),
    _case(
        "WRONG_VERSION",
        "awaiting header",
        "header names unsupported version",
        "rejected",
        "perform no mutations",
        "unsupported-version error",
    ),
    _case(
        "INVALID_ALIAS_FIELDS",
        "body",
        "alias record has non-string field",
        "rejected",
        "do not apply alias",
        "line-numbered alias-field error",
    ),
    _case(
        "INVALID_MESSAGE_FIELDS",
        "body",
        "message record has non-string body",
        "rejected",
        "do not buffer message",
        "line-numbered message-field error",
    ),
    _case(
        "INVALID_MESSAGE_ID",
        "body",
        "message record has invalid ID",
        "rejected",
        "do not buffer message",
        "line-numbered message-ID error",
    ),
    _case(
        "DESTINATION_INSERT_FAILURE",
        "body with destination ID already present",
        "flush colliding message",
        "rejected with existing destination unchanged",
        "propagate destination integrity failure",
        "existing message remains the only row",
    ),
)


def _header() -> str:
    return json.dumps({"type": "header", "format": "simplebroker-dump", "version": 1})


def _message(index: int) -> str:
    return json.dumps(
        {"type": "message", "queue": "jobs", "body": str(index), "id": index + 1}
    )


def _dump_input(payload: str) -> tuple[list[str], int, int, str | None]:
    invalid_inputs = {
        "MISSING_HEADER_EOF": (["", "   \n"], "missing header"),
        "MALFORMED_JSON": ([_header(), "{"], "line 2: malformed JSON"),
        "NON_OBJECT_RECORD": (
            [_header(), "[]"],
            "line 2: record must be a JSON object",
        ),
        "WRONG_FORMAT": (
            [json.dumps({"type": "header", "format": "other", "version": 1})],
            "unrecognized dump format",
        ),
        "WRONG_VERSION": (
            [
                json.dumps(
                    {"type": "header", "format": "simplebroker-dump", "version": 2}
                )
            ],
            "unsupported dump version",
        ),
        "INVALID_ALIAS_FIELDS": (
            [
                _header(),
                json.dumps({"type": "alias", "alias": 1, "target": "jobs"}),
            ],
            "alias record requires string",
        ),
        "INVALID_MESSAGE_FIELDS": (
            [
                _header(),
                json.dumps({"type": "message", "queue": "jobs", "body": 1, "id": 1}),
            ],
            "message record requires string",
        ),
        "INVALID_MESSAGE_ID": (
            [
                _header(),
                json.dumps(
                    {
                        "type": "message",
                        "queue": "jobs",
                        "body": "bad",
                        "id": "bad",
                    }
                ),
            ],
            "invalid message ID",
        ),
    }
    if payload in invalid_inputs:
        invalid_lines, invalid_pattern = invalid_inputs[payload]
        return invalid_lines, 0, 0, invalid_pattern

    lines = [_header()]
    expected_count = 0
    expected_aliases = 0
    error_pattern: str | None = None
    if payload == "DUPLICATE_HEADER":
        lines.append(_header())
        error_pattern = "duplicate header"
    elif payload == "BLANK_ONLY_EOF":
        lines.extend(["", "   \n"])
    elif payload.startswith("BATCH_SIZE"):
        offset = {
            "BATCH_SIZE_MINUS_ONE": -1,
            "BATCH_SIZE": 0,
            "BATCH_SIZE_PLUS_ONE": 1,
        }[payload]
        expected_count = LOAD_BATCH_SIZE + offset
        lines.extend(_message(index) for index in range(expected_count))
    elif payload == "LATER_ERROR_PRESERVES_FLUSHED_BATCH":
        expected_count = LOAD_BATCH_SIZE
        lines.extend(_message(index) for index in range(expected_count))
        lines.append(json.dumps({"type": "unknown"}))
        error_pattern = "unknown record type"
    elif payload == "APPLY_ALIAS":
        lines.append(json.dumps({"type": "alias", "alias": "work", "target": "jobs"}))
        expected_aliases = 1
    elif payload == "DESTINATION_INSERT_FAILURE":
        lines.append(_message(0))
        expected_count = 1
    return lines, expected_count, expected_aliases, error_pattern


@fires_transition_table("SM-DUMP-LOAD", DUMP_LOAD_TRANSITIONS)
def test_dump_load_fires_transition_table(
    transition_case: TransitionCase[str],
    tmp_path: Path,
) -> None:
    payload = transition_case.payload
    lines, expected_count, expected_aliases, error_pattern = _dump_input(payload)

    with open_broker(str(tmp_path / f"{payload}.db")) as broker:
        if payload == "DESTINATION_INSERT_FAILURE":
            broker.insert_messages([("jobs", "existing", 1)])
            with pytest.raises(IntegrityError):
                load_lines(broker, lines)
        elif error_pattern is not None:
            with pytest.raises(ValueError, match=error_pattern):
                load_lines(broker, lines)
        else:
            assert load_lines(broker, lines) == LoadResult(
                messages=expected_count,
                aliases=expected_aliases,
            )
        persisted = list(broker.peek_generator("jobs", with_timestamps=False))
        if payload == "APPLY_ALIAS":
            assert broker.resolve_alias("work") == "jobs"
    assert len(persisted) == expected_count


TIMESTAMP_GENERATOR_TRANSITIONS = (
    _case(
        "LAZY_INITIALIZE",
        "uninitialized",
        "generate",
        "initialized",
        "read and advance durable last_ts",
        "positive timestamp is cached",
    ),
    _case(
        "CLOCK_REGRESSION",
        "initialized",
        "wall clock moves backward",
        "logical advance",
        "retain physical component and increment logical component",
        "next timestamp remains greater",
    ),
    _case(
        "PHYSICAL_ADVANCE",
        "initialized at one physical tick",
        "wall clock advances",
        "new physical tick",
        "reset logical counter to zero",
        "timestamp physical component advances",
    ),
    _case(
        "LOGICAL_INCREMENT",
        "initialized within one physical tick",
        "clock is unchanged",
        "same physical tick with next logical value",
        "increment logical counter",
        "timestamp remains monotonic",
    ),
    _case(
        "LOGICAL_OVERFLOW_WAIT_SUCCESS",
        "logical counter at final value",
        "clock advances during bounded wait",
        "new physical tick",
        "wait then reset logical counter",
        "generation succeeds at the advanced clock",
    ),
    _case(
        "REFRESH",
        "stale local cache",
        "refresh",
        "synchronized",
        "read durable last_ts",
        "cache equals durable value",
    ),
    _case(
        "FORK_RESET",
        "initialized in parent",
        "PID changes before generate",
        "initialized in child identity",
        "clear local counter and reload durable state",
        "generated timestamp remains monotonic",
    ),
    _case(
        "LOGICAL_OVERFLOW_FAILURE",
        "logical counter exhausted",
        "clock does not advance within wait budget",
        "failed",
        "stop after bounded clock probes",
        "TimestampError",
    ),
    _case(
        "CAS_LOSS_RETRY",
        "initialized",
        "another connection wins compare-and-set",
        "synchronized and stored",
        "reload winner and retry",
        "new timestamp exceeds competing value",
    ),
    _case(
        "RESERVE_REFRESH",
        "initialized",
        "reserve candidates then refresh",
        "synchronized to durable state",
        "advance only local cache then reload durable value",
        "reservation is ordered and refresh discards the local gap",
    ),
    _case(
        "CONCURRENT_CAS",
        "two initialized generators",
        "generate concurrently",
        "both stored",
        "resolve CAS contention through durable reload",
        "timestamps are unique and durable maximum wins",
    ),
)


def _fire_timestamp_local_transition(
    payload: str,
    core: BrokerDB,
    first: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generator = core._timestamp_gen
    if payload == "LAZY_INITIALIZE":
        assert not generator._initialized
        generated = generator.generate()
        assert generator.get_cached_last_ts() == generated > 0
    elif payload == "REFRESH":
        generator._last_ts = 0
        assert generator.refresh_last_ts() == first
    else:
        physical, _ = generator._decode_hybrid_timestamp(first)
        generator._last_ts = physical | (MAX_LOGICAL_COUNTER - 1)
        monkeypatch.setattr("simplebroker._timestamp.MAX_ITERATIONS", 0)
        monkeypatch.setattr("simplebroker._timestamp.time.time_ns", lambda: physical)
        with pytest.raises(TimestampError, match="Logical counter exhausted"):
            generator.generate()


def _fire_timestamp_clock_transition(
    payload: str,
    core: BrokerDB,
    first: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generator = core._timestamp_gen
    physical, logical = generator._decode_hybrid_timestamp(first)
    if payload == "PHYSICAL_ADVANCE":
        monkeypatch.setattr(
            "simplebroker._timestamp.time.time_ns",
            lambda: physical + MAX_LOGICAL_COUNTER,
        )
        generated = generator.generate()
        next_physical, next_logical = generator._decode_hybrid_timestamp(generated)
        assert next_physical > physical
        assert next_logical == 0
    elif payload in {"CLOCK_REGRESSION", "LOGICAL_INCREMENT"}:
        now = physical - 1 if payload == "CLOCK_REGRESSION" else physical
        monkeypatch.setattr("simplebroker._timestamp.time.time_ns", lambda: now)
        generated = generator.generate()
        next_physical, next_logical = generator._decode_hybrid_timestamp(generated)
        assert next_physical == physical
        assert next_logical == logical + 1
    else:
        generator._last_ts = physical | (MAX_LOGICAL_COUNTER - 1)
        clock_values = iter((physical, physical + MAX_LOGICAL_COUNTER))
        monkeypatch.setattr(
            "simplebroker._timestamp.time.time_ns",
            lambda: next(clock_values),
        )
        monkeypatch.setattr("simplebroker._timestamp.random.uniform", lambda *_: 0)
        monkeypatch.setattr("simplebroker._timestamp.time.sleep", lambda _: None)
        generated = generator.generate()
        next_physical, next_logical = generator._decode_hybrid_timestamp(generated)
        assert next_physical > physical
        assert next_logical == 0


def _assert_timestamp_fork_reset(core: BrokerDB, first: int) -> None:
    if not hasattr(os, "fork"):
        pytest.skip("real fork transition is unavailable on this platform")
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(read_fd)
            generated = core._timestamp_gen.generate()
            state = (
                generated > first
                and core._timestamp_gen._pid == os.getpid()
                and core._timestamp_gen._initialized
            )
            os.write(write_fd, b"1" if state else b"0")
        finally:
            os.close(write_fd)
            os._exit(0)
    os.close(write_fd)
    _assert_fork_probe(pid, read_fd, label="timestamp fork reset")


def _skip_unavailable_fork_transition(payload: str) -> None:
    if payload == "FORK_RESET" and not hasattr(os, "fork"):
        pytest.skip("real fork transition is unavailable on this platform")


def _fire_timestamp_coordination_transition(
    payload: str,
    core: BrokerDB,
    first: int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generator = core._timestamp_gen
    if payload == "CAS_LOSS_RETRY":
        competing = first + 10_000
        original_advance = generator._backend_plugin.advance_last_ts
        first_attempt = True

        def advance_after_competitor(runner: object, *, new_ts: int) -> bool:
            nonlocal first_attempt
            if first_attempt:
                first_attempt = False
                core._runner.run(
                    "UPDATE meta SET value = ? WHERE key = 'last_ts'",
                    (competing,),
                )
                core._runner.commit()
            return original_advance(runner, new_ts=new_ts)  # type: ignore[arg-type]

        monkeypatch.setattr(
            generator._backend_plugin,
            "advance_last_ts",
            advance_after_competitor,
        )
        assert generator.generate() > competing
        assert not first_attempt
    elif payload == "RESERVE_REFRESH":
        durable = generator.refresh_last_ts()
        candidates = generator._reserve_candidates(3)
        assert candidates == sorted(candidates)
        assert len(set(candidates)) == 3
        assert candidates[0] > durable
        assert generator.refresh_last_ts() == durable
    elif payload == "CONCURRENT_CAS":
        other = BrokerDB(str(tmp_path / f"{payload}.db"))
        barrier = threading.Barrier(2)
        values: list[int] = []

        def generate_with(candidate: BrokerDB) -> None:
            barrier.wait(timeout=2)
            values.append(candidate.generate_timestamp())

        threads = [
            threading.Thread(target=generate_with, args=(candidate,))
            for candidate in (core, other)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(2)
            assert not thread.is_alive()
        assert len(values) == 2
        assert len(set(values)) == 2
        assert generator.refresh_last_ts() == max(values)
        other.close()
    else:
        _assert_timestamp_fork_reset(core, first)


@fires_transition_table("SM-TIMESTAMP-GENERATOR", TIMESTAMP_GENERATOR_TRANSITIONS)
def test_timestamp_generator_fires_transition_table(
    transition_case: TransitionCase[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _skip_unavailable_fork_transition(transition_case.payload)
    core = BrokerDB(str(tmp_path / f"{transition_case.payload}.db"))
    try:
        payload = transition_case.payload
        first = 0 if payload == "LAZY_INITIALIZE" else core._timestamp_gen.generate()
        if payload in {
            "LAZY_INITIALIZE",
            "REFRESH",
            "LOGICAL_OVERFLOW_FAILURE",
        }:
            _fire_timestamp_local_transition(
                payload,
                core,
                first,
                monkeypatch,
            )
        elif payload in {
            "CLOCK_REGRESSION",
            "PHYSICAL_ADVANCE",
            "LOGICAL_INCREMENT",
            "LOGICAL_OVERFLOW_WAIT_SUCCESS",
        }:
            _fire_timestamp_clock_transition(payload, core, first, monkeypatch)
        else:
            _fire_timestamp_coordination_transition(
                payload,
                core,
                first,
                tmp_path,
                monkeypatch,
            )
    finally:
        core.close()


SQLITE_RUNNER_TRANSITIONS = (
    _case(
        "CREATE_REUSE",
        "no thread connection",
        "get connection twice",
        "thread connection open",
        "create one owned connection",
        "both calls return the same connection",
    ),
    _case(
        "CLOSE",
        "thread connection open",
        "close",
        "closed",
        "close tracked connection",
        "old connection rejects use",
    ),
    _case(
        "REPEATED_CLOSE",
        "closed",
        "close",
        "closed",
        "perform no duplicate teardown",
        "no error",
    ),
    _case(
        "FORK_RESET",
        "parent connection open",
        "PID changes before get",
        "child connection open",
        "discard inherited connection and create a new one",
        "new connection is distinct",
    ),
    _case(
        "PER_THREAD",
        "main-thread connection open",
        "worker thread gets connection",
        "two thread-owned connections",
        "create a distinct worker connection",
        "connections are distinct and both usable",
    ),
    _case(
        "BEGIN_COMMIT",
        "idle",
        "owner begins and commits",
        "idle",
        "reserve owner until commit succeeds",
        "transaction closes and admission reopens",
    ),
    _case(
        "BEGIN_ROLLBACK",
        "idle",
        "owner begins and rolls back",
        "idle",
        "reserve owner until rollback succeeds",
        "transaction closes and admission reopens",
    ),
    _case(
        "BEGIN_FAILURE",
        "idle",
        "begin fails",
        "idle",
        "discard the new owner claim",
        "later operations remain admissible",
    ),
    _case(
        "COMMIT_FAILURE_ROLLBACK",
        "owner transaction active",
        "commit fails, then owner rolls back",
        "idle",
        "retain owner authority through rollback",
        "original commit error propagates and rollback reopens admission",
    ),
    _case(
        "TERMINAL_WITHOUT_TRANSACTION",
        "idle",
        "commit and rollback",
        "idle",
        "preserve SQLite terminal no-op behavior",
        "no owner state is created or over-released",
    ),
    _case(
        "FOREIGN_TERMINAL_REJECTED",
        "owner transaction active",
        "foreign thread commits and rolls back",
        "owner transaction active",
        "reject foreign settlement",
        "both foreign calls fail and the owner can roll back",
    ),
    _case(
        "FOREIGN_ADMISSION_TIMEOUT",
        "owner transaction active",
        "foreign thread runs SQL past the admission budget",
        "owner transaction active",
        "wait without operation lock, then fail retryably",
        "owner remains able to roll back",
    ),
    _case(
        "OWNER_CLOSE",
        "owner transaction active",
        "owner closes runner",
        "idle with connections closed",
        "settle the owner while closing tracked connections",
        "old connection rejects use",
    ),
    _case(
        "FORK_ACTIVE_RESET",
        "parent transaction active",
        "child touches runner after fork",
        "child idle with fresh connection",
        "discard inherited owner and synchronization state",
        "child does not reuse or close the parent connection",
    ),
    _case(
        "SETUP_MARKER_SUCCESS",
        "phase incomplete",
        "setup connection phase",
        "phase complete",
        "run phase and publish completion marker",
        "is_setup_complete is true",
    ),
    _case(
        "SETUP_MARKER_FAILURE",
        "phase incomplete",
        "setup action fails",
        "phase incomplete",
        "do not publish completion marker",
        "original failure propagates",
    ),
)


def _assert_runner_fork_reset(runner: SQLiteRunner) -> None:
    if not hasattr(os, "fork"):
        pytest.skip("real fork transition is unavailable on this platform")
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(read_fd)
            connection = runner.get_connection()
            state = (
                connection.execute("SELECT 1").fetchone() == (1,)
                and runner._pid == os.getpid()
                and runner._transaction_owner is None
                and runner._transaction_admitted_operations == 0
            )
            os.write(write_fd, b"1" if state else b"0")
        finally:
            os.close(write_fd)
            runner.close()
            os._exit(0)
    os.close(write_fd)
    _assert_fork_probe(pid, read_fd, label="SQLite runner fork reset")


_SQLITE_TRANSACTION_TRANSITIONS = {
    "BEGIN_COMMIT",
    "BEGIN_ROLLBACK",
    "BEGIN_FAILURE",
    "COMMIT_FAILURE_ROLLBACK",
    "TERMINAL_WITHOUT_TRANSACTION",
    "FOREIGN_TERMINAL_REJECTED",
    "FOREIGN_ADMISSION_TIMEOUT",
    "OWNER_CLOSE",
}
_SQLITE_FOREIGN_TRANSACTION_TRANSITIONS = {
    "FOREIGN_TERMINAL_REJECTED",
    "FOREIGN_ADMISSION_TIMEOUT",
    "OWNER_CLOSE",
}


def _fire_sqlite_foreign_transaction_transition(
    payload: str,
    runner: SQLiteRunner,
    first: sqlite3.Connection,
) -> None:
    runner.begin_immediate()
    if payload == "FOREIGN_TERMINAL_REJECTED":
        errors: list[OperationalError] = []

        def settle_from_foreign_thread() -> None:
            for terminal in (runner.commit, runner.rollback):
                try:
                    terminal()
                except OperationalError as exc:
                    errors.append(exc)

        thread = threading.Thread(target=settle_from_foreign_thread)
        thread.start()
        thread.join(2)
        assert not thread.is_alive()
        assert len(errors) == 2
        assert all(error.retryable is False for error in errors)
        assert runner._transaction_owner is threading.current_thread()
        runner.rollback()
    elif payload == "FOREIGN_ADMISSION_TIMEOUT":
        errors: list[OperationalError] = []

        def run_from_foreign_thread() -> None:
            try:
                runner.run("SELECT 1", fetch=True)
            except OperationalError as exc:
                errors.append(exc)

        thread = threading.Thread(target=run_from_foreign_thread)
        thread.start()
        thread.join(2)
        assert not thread.is_alive()
        assert len(errors) == 1
        assert errors[0].retryable is True
        assert runner._transaction_owner is threading.current_thread()
        runner.rollback()
    else:
        assert payload == "OWNER_CLOSE"
        runner.close()
        assert runner._transaction_owner is None
        with pytest.raises(sqlite3.ProgrammingError):
            first.execute("SELECT 1")


def _fire_sqlite_transaction_transition(
    payload: str,
    runner: SQLiteRunner,
    first: sqlite3.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if payload in _SQLITE_FOREIGN_TRANSACTION_TRANSITIONS:
        _fire_sqlite_foreign_transaction_transition(payload, runner, first)
    elif payload == "BEGIN_COMMIT":
        runner.begin_immediate()
        assert runner._transaction_owner is threading.current_thread()
        runner.commit()
        assert runner._transaction_owner is None
        assert not first.in_transaction
    elif payload == "BEGIN_ROLLBACK":
        runner.begin_immediate()
        assert runner._transaction_owner is threading.current_thread()
        runner.rollback()
        assert runner._transaction_owner is None
        assert not first.in_transaction
    elif payload == "BEGIN_FAILURE":
        failed_connection = Mock()
        failed_connection.execute.side_effect = sqlite3.OperationalError(
            "begin failed"
        )
        with monkeypatch.context() as scoped:
            scoped.setattr(runner, "get_connection", lambda: failed_connection)
            with pytest.raises(OperationalError, match="begin failed"):
                runner.begin_immediate()
        assert runner._transaction_owner is None
        assert list(runner.run("SELECT 1", fetch=True)) == [(1,)]
    elif payload == "COMMIT_FAILURE_ROLLBACK":
        runner.begin_immediate()
        failed_connection = Mock()
        failed_connection.commit.side_effect = sqlite3.OperationalError(
            "commit failed"
        )
        with monkeypatch.context() as scoped:
            scoped.setattr(runner, "get_connection", lambda: failed_connection)
            with pytest.raises(OperationalError, match="commit failed"):
                runner.commit()
        assert runner._transaction_owner is threading.current_thread()
        runner.rollback()
        assert runner._transaction_owner is None
    elif payload == "TERMINAL_WITHOUT_TRANSACTION":
        runner.commit()
        runner.rollback()
        assert runner._transaction_owner is None
        assert runner._transaction_admitted_operations == 0


def _fire_sqlite_connection_transition(
    payload: str,
    runner: SQLiteRunner,
    first: sqlite3.Connection,
) -> None:
    if payload == "CREATE_REUSE":
        assert runner.get_connection() is first
    elif payload in {"FORK_RESET", "FORK_ACTIVE_RESET"}:
        if payload == "FORK_ACTIVE_RESET":
            runner.begin_immediate()
        _assert_runner_fork_reset(runner)
        if payload == "FORK_ACTIVE_RESET":
            runner.rollback()
    elif payload == "PER_THREAD":
        worker_connections: list[sqlite3.Connection] = []

        def get_worker_connection() -> None:
            connection = runner.get_connection()
            connection.execute("SELECT 1").fetchone()
            worker_connections.append(connection)

        thread = threading.Thread(target=get_worker_connection)
        thread.start()
        thread.join(2)
        assert not thread.is_alive()
        assert len(worker_connections) == 1
        assert worker_connections[0] is not first
        runner.close()
        for connection in (first, worker_connections[0]):
            with pytest.raises(sqlite3.ProgrammingError):
                connection.execute("SELECT 1")
    else:
        runner.close()
        if payload == "CLOSE":
            with pytest.raises(sqlite3.ProgrammingError):
                first.execute("SELECT 1")
        else:
            runner.close()


@fires_transition_table("SM-SQLITE-RUNNER", SQLITE_RUNNER_TRANSITIONS)
def test_sqlite_runner_fires_transition_table(
    transition_case: TransitionCase[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _skip_unavailable_fork_transition(transition_case.payload)
    config = (
        {"BROKER_BUSY_TIMEOUT": 25}
        if transition_case.payload == "FOREIGN_ADMISSION_TIMEOUT"
        else None
    )
    runner = SQLiteRunner(
        str(tmp_path / f"{transition_case.payload}.db"),
        config=config,
    )
    first = runner.get_connection()
    if transition_case.payload in _SQLITE_TRANSACTION_TRANSITIONS:
        _fire_sqlite_transaction_transition(
            transition_case.payload,
            runner,
            first,
            monkeypatch,
        )
    elif transition_case.payload == "SETUP_MARKER_SUCCESS":
        runner.setup(SetupPhase.CONNECTION)
        assert runner.is_setup_complete(SetupPhase.CONNECTION)
    elif transition_case.payload == "SETUP_MARKER_FAILURE":

        def fail_phase(
            phase: SetupPhase,
            stop_event: threading.Event | None,
        ) -> None:
            del phase, stop_event
            raise RuntimeError("setup failed")

        monkeypatch.setattr(runner, "_execute_builtin_setup_phase", fail_phase)
        with pytest.raises(RuntimeError, match="setup failed"):
            runner.setup(SetupPhase.CONNECTION)
        assert not runner.is_setup_complete(SetupPhase.CONNECTION)
    else:
        _fire_sqlite_connection_transition(transition_case.payload, runner, first)
    runner.close()
