"""Focused tests for Redis's definite-conflict retry adapter."""

from __future__ import annotations

import threading
from collections.abc import Callable

import pytest
import simplebroker_redis.core as redis_core_module
from simplebroker_redis import scripts
from simplebroker_redis.core import RedisBrokerCore
from simplebroker_redis.runner import RedisRunner

import simplebroker._retry as retry_module
from simplebroker._exceptions import OperationalError

pytestmark = [pytest.mark.redis_only]


def _no_sleep(seconds: float, stop_event: object) -> bool:
    del seconds, stop_event
    return True


def test_safe_conflicts_continue_beyond_three_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts <= 4:
            raise redis_core_module._RedisConflict
        return "committed"

    monkeypatch.setattr(redis_core_module, "_retry_sleep", _no_sleep)

    assert (
        redis_core_module._execute_conflict_retry(
            operation,
            exhaustion_message="conflicts exhausted",
        )
        == "committed"
    )
    assert attempts == 5


def test_safe_conflict_sleep_is_capped_at_250_ms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    sleeps: list[float] = []

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts <= 12:
            raise redis_core_module._RedisConflict
        return "committed"

    def record_sleep(seconds: float, stop_event: object) -> bool:
        del stop_event
        sleeps.append(seconds)
        return True

    monkeypatch.setattr(redis_core_module, "apply_jitter", lambda seconds: seconds)
    monkeypatch.setattr(redis_core_module, "_retry_sleep", record_sleep)

    assert (
        redis_core_module._execute_conflict_retry(
            operation,
            exhaustion_message="conflicts exhausted",
        )
        == "committed"
    )
    assert sleeps
    assert max(sleeps) == pytest.approx(0.25)


def test_elapsed_budget_preserves_public_exhaustion_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise redis_core_module._RedisConflict

    monkeypatch.setattr(redis_core_module, "_CONFLICT_RETRY_MAX_ELAPSED", 0.0)
    monkeypatch.setattr(redis_core_module, "_retry_sleep", _no_sleep)

    with pytest.raises(RuntimeError, match="conflicts exhausted"):
        redis_core_module._execute_conflict_retry(
            operation,
            exhaustion_message="conflicts exhausted",
        )
    assert attempts == 1


def test_elapsed_budget_accumulates_real_retry_waits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    clock = 0.0
    sleeps: list[float] = []

    def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise redis_core_module._RedisConflict

    def monotonic() -> float:
        return clock

    def advance_clock(seconds: float, stop_event: object) -> bool:
        nonlocal clock
        del stop_event
        sleeps.append(seconds)
        clock += seconds
        return True

    monkeypatch.setattr(redis_core_module, "_CONFLICT_RETRY_MAX_ELAPSED", 0.003)
    monkeypatch.setattr(redis_core_module, "apply_jitter", lambda seconds: seconds)
    monkeypatch.setattr(redis_core_module, "_retry_sleep", advance_clock)
    monkeypatch.setattr(retry_module, "_monotonic", monotonic)

    with pytest.raises(RuntimeError, match="conflicts exhausted"):
        redis_core_module._execute_conflict_retry(
            operation,
            exhaustion_message="conflicts exhausted",
        )

    assert attempts == 3
    assert sleeps == pytest.approx([0.001, 0.002])


def test_stale_fence_retries_do_not_sleep_or_consume_id_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    sleeps: list[float] = []

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts <= 2:
            raise redis_core_module._RedisConflict(backoff=False)
        if attempts == 3:
            raise redis_core_module._RedisConflict
        return "committed"

    def record_sleep(seconds: float, stop_event: object) -> bool:
        del stop_event
        sleeps.append(seconds)
        return True

    monkeypatch.setattr(redis_core_module, "apply_jitter", lambda seconds: seconds)
    monkeypatch.setattr(redis_core_module, "_retry_sleep", record_sleep)

    assert (
        redis_core_module._execute_conflict_retry(
            operation,
            exhaustion_message="conflicts exhausted",
        )
        == "committed"
    )
    assert sleeps == pytest.approx([0.001])


def test_interrupted_conflict_wait_uses_public_exhaustion_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop_event = threading.Event()
    observed_events: list[threading.Event | None] = []

    def operation() -> None:
        raise redis_core_module._RedisConflict

    def interrupt(seconds: float, event: threading.Event | None) -> bool:
        assert seconds > 0
        observed_events.append(event)
        return False

    monkeypatch.setattr(redis_core_module, "apply_jitter", lambda seconds: seconds)
    monkeypatch.setattr(redis_core_module, "_retry_sleep", interrupt)

    with pytest.raises(RuntimeError, match="conflicts exhausted"):
        redis_core_module._execute_conflict_retry(
            operation,
            exhaustion_message="conflicts exhausted",
            stop_event=stop_event,
        )

    assert observed_events == [stop_event]


def test_stale_fence_retry_observes_stop_before_zero_delay_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop_event = threading.Event()
    attempts = 0
    sleeps: list[float] = []

    def operation() -> None:
        nonlocal attempts
        attempts += 1
        stop_event.set()
        raise redis_core_module._RedisConflict(backoff=False)

    def record_sleep(seconds: float, event: threading.Event | None) -> bool:
        del event
        sleeps.append(seconds)
        return True

    monkeypatch.setattr(redis_core_module, "_retry_sleep", record_sleep)

    with pytest.raises(RuntimeError, match="conflicts exhausted"):
        redis_core_module._execute_conflict_retry(
            operation,
            exhaustion_message="conflicts exhausted",
            stop_event=stop_event,
        )

    assert attempts == 1
    assert sleeps == []


def test_eval_id_conflict_wait_uses_core_stop_event(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop_event = threading.Event()
    core = RedisBrokerCore(redis_runner, stop_event=stop_event)
    eval_calls = 0
    observed_events: list[threading.Event | None] = []

    def evaluate(script: str, *args: object) -> tuple[int]:
        nonlocal eval_calls
        del args
        assert script == scripts.WRITE_MESSAGE
        eval_calls += 1
        return (-1,)

    def interrupt(seconds: float, event: threading.Event | None) -> bool:
        assert seconds > 0
        observed_events.append(event)
        return False

    monkeypatch.setattr(core._timestamp_gen, "_reserve_candidates", lambda count: [1])
    monkeypatch.setattr(core._client, "eval", evaluate)
    monkeypatch.setattr(core, "_maybe_recover_stale_batches", lambda: None)
    monkeypatch.setattr(core, "_resync_timestamp_generator", lambda: None)
    monkeypatch.setattr(redis_core_module, "apply_jitter", lambda seconds: seconds)
    monkeypatch.setattr(redis_core_module, "_retry_sleep", interrupt)

    try:
        with pytest.raises(RuntimeError, match="repeated timestamp conflicts"):
            core.write("jobs", "message")
        assert eval_calls == 1
        assert observed_events == [stop_event]
    finally:
        core.close()


@pytest.mark.parametrize(
    "error_factory",
    [
        pytest.param(
            lambda: OperationalError("transport outcome is unknown"),
            id="translated-transport-error",
        ),
        pytest.param(lambda: ValueError("unrelated failure"), id="unrelated-error"),
    ],
)
def test_non_conflict_errors_are_attempted_once(
    monkeypatch: pytest.MonkeyPatch,
    error_factory: Callable[[], Exception],
) -> None:
    attempts = 0

    def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise error_factory()

    monkeypatch.setattr(redis_core_module, "_retry_sleep", _no_sleep)

    with pytest.raises(Exception, match="unknown|unrelated"):
        redis_core_module._execute_conflict_retry(
            operation,
            exhaustion_message="conflicts exhausted",
        )
    assert attempts == 1
