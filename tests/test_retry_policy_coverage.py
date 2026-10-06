"""Focused coverage for retry and setup-policy edge cases."""
# mypy: disable-error-code=no-untyped-def

from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

import pytest

from simplebroker import Queue, _retry_policy
from simplebroker._exceptions import OperationalError, StopException
from simplebroker._retry import DEFAULT_MIN_RETRY_SLEEP_S, interruptible_sleep
from simplebroker._retry_policy import (
    SetupProgressBudget,
    _execute_connection_retry,
    _execute_watcher_operational_retry,
    _execute_with_retry,
    _is_locked_operational_error,
    execute_setup_with_retry,
)
from simplebroker._runner import SetupPhase, SQLiteRunner
from tests.helpers.state_machine_contracts import (
    TransitionCase,
    fires_transition_table,
)


class SetupBudgetEvent(StrEnum):
    """Executable events for the setup-progress budget contract."""

    CREATE = "create"
    OBSERVE = "observe"
    ADVANCE_AND_OBSERVE = "advance-and-observe"
    RECORD_PROGRESS = "record-progress"


@dataclass(frozen=True, slots=True)
class SetupBudgetPayload:
    """Machine-local inputs and expected observations for one transition."""

    event: SetupBudgetEvent
    elapsed_before_event: float
    elapsed_during_event: float
    expected_remaining: float


SETUP_PROGRESS_BUDGET_TRANSITIONS = (
    TransitionCase(
        transition_id="INITIALIZE",
        start_state="uninitialized",
        event="create budget",
        guard="an idle timeout is configured",
        next_state="tracking",
        effects="the current monotonic time becomes the progress baseline",
        expected_result="the full idle budget remains",
        payload=SetupBudgetPayload(
            event=SetupBudgetEvent.CREATE,
            elapsed_before_event=0.0,
            elapsed_during_event=0.0,
            expected_remaining=5.0,
        ),
    ),
    TransitionCase(
        transition_id="OBSERVE-ACTIVE",
        start_state="tracking",
        event="observe remaining time before timeout",
        guard="elapsed idle time is below the timeout",
        next_state="tracking",
        effects="the progress baseline is unchanged",
        expected_result="the remaining budget reflects elapsed time",
        payload=SetupBudgetPayload(
            event=SetupBudgetEvent.ADVANCE_AND_OBSERVE,
            elapsed_before_event=0.0,
            elapsed_during_event=2.0,
            expected_remaining=3.0,
        ),
    ),
    TransitionCase(
        transition_id="EXPIRE",
        start_state="tracking",
        event="observe remaining time at timeout",
        guard="elapsed idle time equals the timeout",
        next_state="expired",
        effects="the progress baseline is unchanged",
        expected_result="no idle budget remains",
        payload=SetupBudgetPayload(
            event=SetupBudgetEvent.ADVANCE_AND_OBSERVE,
            elapsed_before_event=0.0,
            elapsed_during_event=5.0,
            expected_remaining=0.0,
        ),
    ),
    TransitionCase(
        transition_id="REMAIN-EXPIRED",
        start_state="expired",
        event="observe remaining time after timeout",
        guard="elapsed idle time exceeds the timeout",
        next_state="expired",
        effects="the progress baseline is unchanged",
        expected_result="the remaining budget is negative",
        payload=SetupBudgetPayload(
            event=SetupBudgetEvent.OBSERVE,
            elapsed_before_event=6.0,
            elapsed_during_event=0.0,
            expected_remaining=-1.0,
        ),
    ),
    TransitionCase(
        transition_id="REFRESH-ACTIVE",
        start_state="tracking",
        event="record successful progress",
        guard="the budget has not expired",
        next_state="tracking",
        effects="the current monotonic time replaces the progress baseline",
        expected_result="the full idle budget remains",
        payload=SetupBudgetPayload(
            event=SetupBudgetEvent.RECORD_PROGRESS,
            elapsed_before_event=2.0,
            elapsed_during_event=0.0,
            expected_remaining=5.0,
        ),
    ),
    TransitionCase(
        transition_id="REFRESH-EXPIRED",
        start_state="expired",
        event="record successful progress",
        guard="the budget has expired",
        next_state="tracking",
        effects="the current monotonic time replaces the progress baseline",
        expected_result="the full idle budget remains",
        payload=SetupBudgetPayload(
            event=SetupBudgetEvent.RECORD_PROGRESS,
            elapsed_before_event=6.0,
            elapsed_during_event=0.0,
            expected_remaining=5.0,
        ),
    ),
)


class DeterministicClock:
    """Controllable monotonic clock used at the public clock seam."""

    def __init__(self, initial: float = 100.0) -> None:
        self.now = initial

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class PeekFaultRunner:
    """Delegate to real SQLite while injecting failures at the peek query."""

    def __init__(self, db_path: Path) -> None:
        self._inner = SQLiteRunner(str(db_path))
        self.peek_attempts = 0
        self._remaining_failures = 0
        self._failure: OperationalError | None = None

    def fail_peek(self, error: OperationalError, *, times: int = 1) -> None:
        self.peek_attempts = 0
        self._remaining_failures = times
        self._failure = error

    def run(
        self, sql: str, params: tuple[Any, ...] = (), *, fetch: bool = False
    ) -> list[tuple[Any, ...]]:
        if "SELECT body, ts FROM messages" in sql:
            self.peek_attempts += 1
            if self._remaining_failures:
                self._remaining_failures -= 1
                assert self._failure is not None
                raise self._failure
        return list(self._inner.run(sql, params, fetch=fetch))

    def begin_immediate(self) -> None:
        self._inner.begin_immediate()

    def commit(self) -> None:
        self._inner.commit()

    def rollback(self) -> None:
        self._inner.rollback()

    def close(self) -> None:
        self._inner.close()

    def setup(self, phase: SetupPhase) -> None:
        self._inner.setup(phase)

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        return self._inner.is_setup_complete(phase)


def test_public_peek_one_retries_an_explicitly_retryable_runner_failure(
    tmp_path: Path,
) -> None:
    """The public peek path shares the core's bounded retry policy."""

    runner = PeekFaultRunner(tmp_path / "peek-retry.db")
    queue = Queue("jobs", db_path=str(tmp_path / "decoy.db"), runner=runner)
    try:
        queue.write("first")
        retryable = OperationalError("backend-specific contention")
        retryable.retryable = True
        runner.fail_peek(retryable)

        assert queue.peek_one() == "first"
        assert runner.peek_attempts == 2
    finally:
        queue.close()
        runner.close()


def test_public_peek_many_retries_an_explicitly_retryable_runner_failure(
    tmp_path: Path,
) -> None:
    """Batch peek uses the same bounded retry owner as single-message peek."""

    runner = PeekFaultRunner(tmp_path / "peek-many-retry.db")
    queue = Queue("jobs", db_path=str(tmp_path / "decoy.db"), runner=runner)
    try:
        queue.write("first")
        queue.write("second")
        retryable = OperationalError("backend-specific contention")
        retryable.retryable = True
        runner.fail_peek(retryable)

        assert queue.peek_many() == ["first", "second"]
        assert runner.peek_attempts == 2
    finally:
        queue.close()
        runner.close()


def test_public_peek_does_not_retry_an_explicitly_nonretryable_runner_failure(
    tmp_path: Path,
) -> None:
    """Peek propagates a classified permanent failure after one attempt."""

    runner = PeekFaultRunner(tmp_path / "peek-no-retry.db")
    queue = Queue("jobs", db_path=str(tmp_path / "decoy.db"), runner=runner)
    try:
        queue.write("first")
        nonretryable = OperationalError("database is locked permanently")
        nonretryable.retryable = False
        runner.fail_peek(nonretryable)

        with pytest.raises(OperationalError) as exc_info:
            queue.peek_one()

        assert exc_info.value is nonretryable
        assert runner.peek_attempts == 1
    finally:
        queue.close()
        runner.close()


@fires_transition_table("SM-SETUP-BUDGET", SETUP_PROGRESS_BUDGET_TRANSITIONS)
def test_setup_progress_budget_fires_transition_table(
    transition_case: TransitionCase[SetupBudgetPayload],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fire every declared setup-budget transition against the real owner."""

    clock = DeterministicClock()
    monkeypatch.setattr(_retry_policy, "_monotonic", clock)
    payload = transition_case.payload

    budget = SetupProgressBudget(idle_timeout=5.0)
    clock.advance(payload.elapsed_before_event)
    if transition_case.start_state == "uninitialized":
        assert payload.event is SetupBudgetEvent.CREATE
    else:
        initial_remaining = budget.remaining()
        initial_state = "tracking" if initial_remaining > 0 else "expired"
        assert initial_state == transition_case.start_state

    if payload.event is SetupBudgetEvent.ADVANCE_AND_OBSERVE:
        clock.advance(payload.elapsed_during_event)
    elif payload.event is SetupBudgetEvent.RECORD_PROGRESS:
        budget.record_progress()

    remaining = budget.remaining()
    next_state = "tracking" if remaining > 0 else "expired"

    assert remaining == pytest.approx(payload.expected_remaining)
    assert next_state == transition_case.next_state


def test_interruptible_sleep_handles_zero_and_interrupts() -> None:
    assert interruptible_sleep(0) is True

    class StopEvent:
        def __init__(self) -> None:
            self.waits = 0

        def wait(self, timeout: float) -> bool:
            self.waits += 1
            return True

        def is_set(self) -> bool:
            return True

    stop_event = StopEvent()
    assert interruptible_sleep(1.0, stop_event) is False  # type: ignore[arg-type]
    assert stop_event.waits == 1


def test_interruptible_sleep_short_sleep_uses_single_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Event:
        def __init__(self) -> None:
            self.timeouts: list[float] = []

        def wait(self, timeout: float) -> bool:
            self.timeouts.append(timeout)
            return False

    # Pass the recording event as the real stop_event argument instead of
    # replacing the shared threading.Event globally, which any concurrent
    # thread creating an Event in the window would observe.
    event = Event()

    assert interruptible_sleep(0.05, event, chunk_size=0.1) is True  # type: ignore[arg-type]
    assert event.timeouts == [0.05]


def test_execute_with_retry_retries_locked_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    attempts = 0

    def fake_sleep(wait: float, stop_event=None) -> bool:
        sleeps.append(wait)
        return True

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OperationalError("database is locked")
        return "ok"

    monkeypatch.setattr(_retry_policy, "interruptible_sleep", fake_sleep)

    assert _execute_with_retry(operation, max_retries=3, retry_delay=0) == "ok"
    assert attempts == 2
    assert len(sleeps) == 1


def test_execute_with_retry_stops_when_sleep_is_interrupted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_sleep(wait: float, stop_event=None) -> bool:
        return False

    monkeypatch.setattr(_retry_policy, "interruptible_sleep", fake_sleep)

    with pytest.raises(StopException, match="Retry interrupted"):
        _execute_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("database is busy")),
            max_retries=2,
            retry_delay=0,
        )


def test_execute_with_retry_does_not_start_after_stop_is_requested() -> None:
    stop_event = threading.Event()
    stop_event.set()
    operation_called = False

    def operation() -> str:
        nonlocal operation_called
        operation_called = True
        return "unexpected"

    with pytest.raises(StopException, match="Retry interrupted"):
        _execute_with_retry(operation, stop_event=stop_event)

    assert not operation_called


def test_execute_with_retry_does_not_retry_unrelated_operational_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _retry_policy,
        "interruptible_sleep",
        lambda wait, stop_event=None: pytest.fail("unexpected retry"),
    )

    with pytest.raises(OperationalError, match="syntax error"):
        _execute_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("syntax error"))
        )


def test_execute_with_retry_uses_elapsed_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic_time = 0.0
    sleeps: list[float] = []

    def fake_monotonic() -> float:
        return monotonic_time

    def fake_sleep(wait: float, stop_event=None) -> bool:
        nonlocal monotonic_time
        sleeps.append(wait)
        monotonic_time += wait
        return True

    monkeypatch.setattr("simplebroker._retry._monotonic", fake_monotonic)
    monkeypatch.setattr(_retry_policy, "interruptible_sleep", fake_sleep)

    with pytest.raises(OperationalError, match="database is locked"):
        _execute_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("database is locked")),
            max_retries=None,
            retry_delay=0.1,
            max_retry_delay=0.1,
            max_elapsed=0.15,
        )

    assert sleeps
    assert all(wait <= 0.1 for wait in sleeps)
    assert monotonic_time <= 0.15


def test_execute_with_retry_rejects_an_unbounded_retry_policy() -> None:
    """A missing attempt limit must be paired with an elapsed-time limit."""

    operation_called = False

    def operation() -> None:
        nonlocal operation_called
        operation_called = True

    with pytest.raises(ValueError, match="max_retries=None requires max_elapsed"):
        _execute_with_retry(operation, max_retries=None, max_elapsed=None)

    assert not operation_called


def test_execute_with_retry_refreshes_idle_budget_on_external_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Slow forward progress must outlive one fixed elapsed window."""

    monotonic_time = 0.0
    attempts = 0

    def fake_monotonic() -> float:
        return monotonic_time

    def fake_sleep(wait: float, stop_event=None) -> bool:
        nonlocal monotonic_time
        monotonic_time += wait
        return True

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 5:
            raise OperationalError("database is locked")
        return "ok"

    monkeypatch.setattr("simplebroker._retry._monotonic", fake_monotonic)
    monkeypatch.setattr(_retry_policy, "interruptible_sleep", fake_sleep)
    monkeypatch.setattr(_retry_policy, "bounded_jitter", lambda wait: wait)

    assert (
        _execute_with_retry(
            operation,
            max_retries=None,
            retry_delay=0.1,
            max_retry_delay=0.1,
            max_elapsed=0.15,
            progress_token=lambda: attempts // 2,
        )
        == "ok"
    )
    assert attempts == 5
    assert monotonic_time > 0.15


def test_execute_with_retry_does_not_probe_progress_without_contention() -> None:
    """The forward-progress detector must not tax the uncontended fast path."""

    progress_probes = 0

    def progress_token() -> int:
        nonlocal progress_probes
        progress_probes += 1
        return 1

    assert (
        _execute_with_retry(
            lambda: "ok",
            max_retries=None,
            max_elapsed=0.15,
            progress_token=progress_token,
        )
        == "ok"
    )
    assert progress_probes == 0


def test_execute_with_retry_progress_budget_still_stops_when_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stable progress token must not turn retries into an infinite wait."""

    monotonic_time = 0.0

    def fake_monotonic() -> float:
        return monotonic_time

    def fake_sleep(wait: float, stop_event=None) -> bool:
        nonlocal monotonic_time
        monotonic_time += wait
        return True

    monkeypatch.setattr("simplebroker._retry._monotonic", fake_monotonic)
    monkeypatch.setattr(_retry_policy, "interruptible_sleep", fake_sleep)
    monkeypatch.setattr(_retry_policy, "bounded_jitter", lambda wait: wait)

    with pytest.raises(OperationalError, match="database is locked"):
        _execute_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("database is locked")),
            max_retries=None,
            retry_delay=0.1,
            max_retry_delay=0.1,
            max_elapsed=0.15,
            progress_token=lambda: 7,
        )

    assert 0.15 <= monotonic_time <= 0.25


def test_execute_with_retry_elapsed_budget_still_honors_stop_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop_event = threading.Event()

    def fake_sleep(wait: float, stop_event=None) -> bool:
        return False

    monkeypatch.setattr(_retry_policy, "interruptible_sleep", fake_sleep)

    with pytest.raises(StopException, match="Retry interrupted"):
        _execute_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("database is busy")),
            max_retries=None,
            retry_delay=0.01,
            max_elapsed=1.0,
            stop_event=stop_event,
        )


def test_watcher_operational_retry_does_not_retry_stop_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    def capture(wait: float, stop_event=None) -> bool:
        sleeps.append(wait)
        return True

    monkeypatch.setattr(_retry_policy, "interruptible_sleep", capture)

    def fail() -> None:
        raise StopException("stop during operation")

    with pytest.raises(StopException):
        _execute_watcher_operational_retry(fail, max_retries=5)

    assert sleeps == []


def test_execute_setup_with_retry_refreshes_progress_budget_after_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic_time = 0.0
    first_attempts = 0
    second_ran = False

    def fake_monotonic() -> float:
        return monotonic_time

    def fake_sleep(wait: float, stop_event=None) -> bool:
        nonlocal monotonic_time
        monotonic_time += wait
        return True

    monkeypatch.setattr(_retry_policy, "_monotonic", fake_monotonic)
    monkeypatch.setattr(_retry_policy, "interruptible_sleep", fake_sleep)
    monkeypatch.setattr(_retry_policy, "SETUP_RETRY_MAX_ELAPSED", 0.15)

    def first_operation() -> str:
        nonlocal first_attempts
        first_attempts += 1
        if monotonic_time < 0.14:
            raise OperationalError("database is locked")
        return "first"

    def second_operation() -> str:
        nonlocal second_ran
        second_ran = True
        return "second"

    budget = SetupProgressBudget()

    assert (
        execute_setup_with_retry(
            first_operation,
            phase="schema",
            target="test.db",
            progress_budget=budget,
        )
        == "first"
    )
    assert first_attempts > 1
    assert (
        execute_setup_with_retry(
            second_operation,
            phase="schema",
            target="test.db",
            progress_budget=budget,
        )
        == "second"
    )
    assert second_ran


def test_execute_setup_with_retry_fails_when_no_operation_makes_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic_time = 0.0

    def fake_monotonic() -> float:
        return monotonic_time

    def fake_sleep(wait: float, stop_event=None) -> bool:
        nonlocal monotonic_time
        monotonic_time += wait
        return True

    monkeypatch.setattr(_retry_policy, "_monotonic", fake_monotonic)
    monkeypatch.setattr(_retry_policy, "interruptible_sleep", fake_sleep)
    monkeypatch.setattr(_retry_policy, "SETUP_RETRY_MAX_ELAPSED", 0.15)

    budget = SetupProgressBudget()

    with pytest.raises(OperationalError) as exc_info:
        execute_setup_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("database is locked")),
            phase="schema",
            target="test.db",
            progress_budget=budget,
        )

    message = str(exc_info.value)
    assert "made no progress" in message
    assert "database is locked" in message


def test_execute_setup_with_retry_does_not_refresh_budget_on_failed_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic_time = 0.0
    second_ran = False

    def fake_monotonic() -> float:
        return monotonic_time

    def fake_sleep(wait: float, stop_event=None) -> bool:
        nonlocal monotonic_time
        monotonic_time += wait
        return True

    def second_operation() -> str:
        nonlocal second_ran
        second_ran = True
        return "second"

    monkeypatch.setattr(_retry_policy, "_monotonic", fake_monotonic)
    monkeypatch.setattr(_retry_policy, "interruptible_sleep", fake_sleep)
    monkeypatch.setattr(_retry_policy, "SETUP_RETRY_MAX_ELAPSED", 0.15)

    budget = SetupProgressBudget()

    with pytest.raises(OperationalError, match="made no progress"):
        execute_setup_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("database is locked")),
            phase="schema",
            target="test.db",
            progress_budget=budget,
        )

    with pytest.raises(OperationalError, match="setup idle timeout expired"):
        execute_setup_with_retry(
            second_operation,
            phase="schema",
            target="test.db",
            progress_budget=budget,
        )

    assert not second_ran


def test_execute_setup_with_retry_reports_immediate_setup_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_retry_policy, "SETUP_RETRY_MAX_ELAPSED", 0.15)

    budget = SetupProgressBudget()

    with pytest.raises(OperationalError) as exc_info:
        execute_setup_with_retry(
            lambda: (_ for _ in ()).throw(OperationalError("syntax error")),
            phase="schema",
            target="test.db",
            progress_budget=budget,
        )

    message = str(exc_info.value)
    assert "failed" in message
    assert "syntax error" in message
    assert "made no progress" not in message


def test_execute_with_retry_uses_bounded_jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    def fake_sleep(wait: float, stop_event=None) -> bool:
        sleeps.append(wait)
        return True

    monkeypatch.setattr(_retry_policy, "interruptible_sleep", fake_sleep)
    jitter_values = iter([0.006, 0.015, 0.03])

    def fake_uniform(low: float, high: float) -> float:
        value = next(jitter_values)
        assert low == DEFAULT_MIN_RETRY_SLEEP_S
        assert low <= value <= high
        return value

    monkeypatch.setattr("simplebroker._retry._uniform", fake_uniform)

    def always_locked() -> None:
        raise OperationalError("database is locked")

    with pytest.raises(OperationalError):
        _execute_with_retry(always_locked, max_retries=4, retry_delay=0.01)

    assert sleeps == pytest.approx([0.006, 0.015, 0.03], rel=1e-9)


class TestRetryableClassification:
    """OperationalError.retryable overrides marker matching (backend-neutral)."""

    def test_default_falls_back_to_sqlite_markers(self):
        assert _is_locked_operational_error(OperationalError("database is locked"))
        assert not _is_locked_operational_error(OperationalError("syntax error"))

    def test_retryable_true_forces_retry_without_markers(self):
        exc = OperationalError("canceling statement due to lock timeout")
        exc.retryable = True
        assert _is_locked_operational_error(exc)

    def test_retryable_false_blocks_retry_despite_markers(self):
        exc = OperationalError("database is locked")
        exc.retryable = False
        assert not _is_locked_operational_error(exc)

    def test_stop_exception_is_never_retryable(self):
        assert not _is_locked_operational_error(
            StopException("Operation interrupted by stop event")
        )

    def test_execute_with_retry_honors_retryable_flag(self):
        attempts = []

        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                exc = OperationalError("pg-style contention, no sqlite words")
                exc.retryable = True
                raise exc
            return "done"

        assert _execute_with_retry(flaky, retry_delay=0.001) == "done"
        assert len(attempts) == 3


@pytest.fixture
def connection_retry_clock(monkeypatch: pytest.MonkeyPatch):
    """Control time and RNG without substituting the retry engine."""
    clock = DeterministicClock(initial=0.0)
    waits: list[float] = []

    def sleep(wait: float, stop_event=None) -> bool:
        waits.append(wait)
        clock.advance(wait)
        return True

    monkeypatch.setattr("simplebroker._retry._monotonic", clock)
    monkeypatch.setattr("simplebroker._retry._uniform", lambda floor, upper: upper)
    monkeypatch.setattr(_retry_policy, "interruptible_sleep", sleep)
    return clock, waits


def capacity_refusal() -> OperationalError:
    error = OperationalError("connection slots exhausted")
    error._connection_capacity = True
    return error


def test_connection_capacity_retries_past_three_until_success(connection_retry_clock):
    """A draining server can admit a caller after the ordinary attempt limit."""
    clock, waits = connection_retry_clock
    attempts = 0

    def open_connection() -> str:
        nonlocal attempts
        attempts += 1
        if attempts <= 4:
            raise capacity_refusal()
        return "connected"

    assert (
        _execute_connection_retry(open_connection, capacity_wait_seconds=30)
        == "connected"
    )
    assert attempts == 5
    assert clock.now < 30
    assert waits == [2, 4, 5, 5]


@pytest.mark.parametrize("budget", [1, 30])
def test_connection_capacity_deadline_prevents_another_attempt(
    connection_retry_clock, budget: int
):
    """Even a clipped final sleep must not start a post-deadline connect."""
    clock, waits = connection_retry_clock
    starts: list[float] = []
    failure = capacity_refusal()

    def open_connection() -> None:
        starts.append(clock.now)
        raise failure

    with pytest.raises(OperationalError) as caught:
        _execute_connection_retry(open_connection, capacity_wait_seconds=budget)

    assert caught.value is failure
    assert clock.now == budget
    assert all(start < budget for start in starts)
    assert waits[-1] == (1 if budget == 1 else 4)
    if budget == 1:
        assert len(starts) == 1
    else:
        assert len(starts) > 3


def test_slow_first_capacity_refusal_does_not_sleep_after_deadline(
    connection_retry_clock,
):
    clock, waits = connection_retry_clock
    attempts = 0
    failure = capacity_refusal()

    def open_connection() -> None:
        nonlocal attempts
        attempts += 1
        clock.advance(31)
        raise failure

    with pytest.raises(OperationalError) as caught:
        _execute_connection_retry(open_connection, capacity_wait_seconds=30)
    assert caught.value is failure
    assert attempts == 1
    assert waits == []


@pytest.mark.parametrize("budget", [None, 0])
def test_connection_capacity_disabled_preserves_three_attempts(
    connection_retry_clock, budget
):
    _, waits = connection_retry_clock
    attempts = 0
    failure = capacity_refusal()

    def open_connection() -> None:
        nonlocal attempts
        attempts += 1
        raise failure

    with pytest.raises(OperationalError) as caught:
        _execute_connection_retry(open_connection, capacity_wait_seconds=budget)
    assert caught.value is failure
    assert attempts == 3
    assert waits == [2, 4]


@pytest.mark.parametrize(
    "message", ["password authentication failed", "network unavailable"]
)
def test_capacity_budget_does_not_extend_unclassified_failures(
    connection_retry_clock, message
):
    _, waits = connection_retry_clock
    attempts = 0
    failure = OperationalError(message)

    def open_connection() -> None:
        nonlocal attempts
        attempts += 1
        raise failure

    with pytest.raises(OperationalError) as caught:
        _execute_connection_retry(open_connection, capacity_wait_seconds=30)
    assert caught.value is failure
    assert attempts == 3
    assert waits == [2, 4]


@pytest.mark.parametrize("capacity_first", [False, True])
def test_mixed_connection_failures_keep_total_attempt_count(
    connection_retry_clock, capacity_first
):
    _, waits = connection_retry_clock
    ordinary = OperationalError("network unavailable")
    capacity = capacity_refusal()
    failures = (
        [capacity, capacity, ordinary]
        if capacity_first
        else [ordinary, capacity, ordinary]
    )
    attempts = 0

    def open_connection() -> None:
        nonlocal attempts
        failure = failures[min(attempts, 2)]
        attempts += 1
        raise failure

    with pytest.raises(OperationalError) as caught:
        _execute_connection_retry(open_connection, capacity_wait_seconds=30)
    assert caught.value is ordinary
    assert attempts == 3
    assert waits == [2, 4]


def test_mixed_connection_failure_keeps_original_deadline(connection_retry_clock):
    clock, waits = connection_retry_clock
    ordinary = OperationalError("network unavailable")
    attempts = 0

    def open_connection() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            clock.advance(8)
            raise capacity_refusal()
        raise ordinary

    with pytest.raises(OperationalError) as caught:
        _execute_connection_retry(open_connection, capacity_wait_seconds=11)
    assert caught.value is ordinary
    assert attempts == 2
    assert clock.now == 11
    assert waits == [2, 1]


def test_connection_success_in_flight_can_finish_after_capacity_deadline(
    connection_retry_clock,
):
    clock, _ = connection_retry_clock
    attempts = 0

    def open_connection() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise capacity_refusal()
        clock.advance(30)
        return "connected"

    assert (
        _execute_connection_retry(open_connection, capacity_wait_seconds=3)
        == "connected"
    )
    assert attempts == 2
    assert clock.now > 3


@pytest.mark.parametrize("choose_upper", [False, True])
def test_connection_capacity_jitter_floor_and_cap(
    connection_retry_clock, monkeypatch, choose_upper
):
    """Sample both RNG endpoints through real jitter and the real retry engine."""
    _, waits = connection_retry_clock
    ranges: list[tuple[float, float]] = []
    attempts = 0

    def uniform(floor: float, upper: float) -> float:
        ranges.append((floor, upper))
        return upper if choose_upper else floor

    def open_connection() -> str:
        nonlocal attempts
        attempts += 1
        if attempts <= 4:
            raise capacity_refusal()
        return "connected"

    monkeypatch.setattr("simplebroker._retry._uniform", uniform)
    assert (
        _execute_connection_retry(open_connection, capacity_wait_seconds=30)
        == "connected"
    )
    assert ranges == [(1, 2), (1, 4), (1, 5), (1, 5)]
    assert waits == ([2, 4, 5, 5] if choose_upper else [1, 1, 1, 1])


def test_connection_capacity_long_budget_keeps_backoff_finite(connection_retry_clock):
    """More than 1024 retries must not overflow an uncapped exponential."""
    _, waits = connection_retry_clock
    attempts = 0

    def open_connection() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 1100:
            raise capacity_refusal()
        return "connected"

    assert (
        _execute_connection_retry(open_connection, capacity_wait_seconds=10000)
        == "connected"
    )
    assert attempts == 1100
    assert max(waits) == 5


@pytest.mark.parametrize("sleep_completed", [False, True])
def test_connection_capacity_sleep_remains_interruptible(
    connection_retry_clock, monkeypatch, sleep_completed: bool
):
    _, waits = connection_retry_clock
    attempts = 0
    stop_event = threading.Event()

    def interrupted_sleep(wait: float, event) -> bool:
        assert event is stop_event
        waits.append(wait)
        stop_event.set()
        return sleep_completed

    def open_connection() -> None:
        nonlocal attempts
        attempts += 1
        if attempts > 1:
            return
        raise capacity_refusal()

    monkeypatch.setattr(_retry_policy, "interruptible_sleep", interrupted_sleep)
    with pytest.raises(StopException, match="Connection interrupted"):
        _execute_connection_retry(
            open_connection, capacity_wait_seconds=30, stop_event=stop_event
        )
    assert attempts == 1
    assert waits == [2]
