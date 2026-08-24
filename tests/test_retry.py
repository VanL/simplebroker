"""Tests for the vendorable retry engine."""

from __future__ import annotations

import ast
import contextvars
import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import simplebroker._retry as retry_module
from simplebroker._retry import (
    DEFAULT_MIN_RETRY_SLEEP_S,
    RetryState,
    apply_jitter,
    execute_retry,
    expo,
    get_attempt_number,
    interruptible_sleep,
    remove_backoff,
    stop_after_attempt,
    stop_after_delay,
    stop_all,
    stop_any,
    stop_never,
    stop_when_event_set,
)
from simplebroker._retry import test_config as retry_test_config

_ALLOWED_STDLIB_ROOTS = frozenset(sys.stdlib_module_names) | {"__future__"}
_THREAD_TEST_TIMEOUT = 5.0


def _capture_retry_sleeps() -> list[float]:
    sleeps: list[float] = []

    def capture(seconds: float, _event: threading.Event | None) -> bool:
        sleeps.append(seconds)
        return True

    def fail() -> None:
        raise RuntimeError("retry once")

    with pytest.raises(RuntimeError, match="retry once"):
        execute_retry(
            fail,
            retry_on=lambda exc: isinstance(exc, RuntimeError),
            wait_gen_kwargs={"factor": 1.0},
            jitter=None,
            stop=stop_after_attempt(2),
            sleep=capture,
        )
    return sleeps


def test_retry_module_is_stdlib_only() -> None:
    source = Path(retry_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                assert root in _ALLOWED_STDLIB_ROOTS, root
        elif isinstance(node, ast.ImportFrom):
            assert node.module is not None
            root = node.module.split(".")[0]
            assert root in _ALLOWED_STDLIB_ROOTS, node.module


def test_interruptible_sleep_returns_true_when_completed() -> None:
    assert interruptible_sleep(0.02) is True


def test_interruptible_sleep_returns_false_when_event_set() -> None:
    event = threading.Event()
    event.set()
    assert interruptible_sleep(1.0, event) is False


def test_apply_jitter_enforces_floor() -> None:
    assert apply_jitter(0.0) == DEFAULT_MIN_RETRY_SLEEP_S
    assert apply_jitter(0.001) == DEFAULT_MIN_RETRY_SLEEP_S


def test_apply_jitter_spans_up_to_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "random.uniform",
        lambda low, high: (low + high) / 2,
    )
    assert apply_jitter(0.05) == pytest.approx(0.0275, rel=1e-9)


def test_expo_yields_zero_then_exponential_values() -> None:
    gen = expo(base=2, factor=0.05, max_value=0.2)
    assert next(gen) == 0.0
    assert next(gen) == 0.05
    assert next(gen) == 0.10
    assert next(gen) == 0.20
    assert next(gen) == 0.20


def test_stop_after_attempt_limits_tries() -> None:
    state = RetryState(tries=10, elapsed=0.0)
    assert stop_after_attempt(10)(state) is True
    state.tries = 9
    assert stop_after_attempt(10)(state) is False


def test_stop_after_delay() -> None:
    state = RetryState(tries=1, elapsed=31.0)
    assert stop_after_delay(30.0)(state) is True


def test_stop_when_event_set() -> None:
    event = threading.Event()
    assert stop_when_event_set(event)(RetryState()) is False
    event.set()
    assert stop_when_event_set(event)(RetryState()) is True


def test_stop_any_and_all() -> None:
    a = stop_after_attempt(2)
    b = stop_after_delay(0.01)
    state = RetryState(tries=2, elapsed=0.0)
    assert (a | b)(state) is True
    state.tries = 1
    assert (a & b)(state) is False
    assert stop_any(a, b)(state) is False
    state.elapsed = 0.02
    assert stop_all(a, b)(state) is False


def test_stop_never_is_false() -> None:
    assert stop_never()(RetryState()) is False


def test_execute_retry_succeeds_on_first_try() -> None:
    assert execute_retry(lambda: 42, retry_on=lambda e: False) == 42


def test_execute_retry_retries_then_succeeds() -> None:
    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("fail")
        return "ok"

    with remove_backoff():
        assert (
            execute_retry(
                flaky,
                retry_on=lambda e: isinstance(e, ValueError),
                stop=stop_after_attempt(5),
            )
            == "ok"
        )
    assert calls["n"] == 3


def test_execute_retry_gives_up_after_max_attempts() -> None:
    def always_fail() -> None:
        raise RuntimeError("nope")

    with remove_backoff(), pytest.raises(RuntimeError, match="nope"):
        execute_retry(
            always_fail,
            retry_on=lambda e: True,
            stop=stop_after_attempt(3),
        )


def test_execute_retry_does_not_sleep_after_final_attempt() -> None:
    sleeps: list[float] = []

    def capture(seconds: float, _event: threading.Event | None) -> bool:
        sleeps.append(seconds)
        return True

    def fail() -> None:
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        execute_retry(
            fail,
            retry_on=lambda e: True,
            stop=stop_after_attempt(4),
            sleep=capture,
        )
    assert len(sleeps) == 3


def test_execute_retry_runs_once_even_if_stop_event_preset() -> None:
    event = threading.Event()
    event.set()
    calls = {"n": 0}

    def once() -> int:
        calls["n"] += 1
        return 7

    assert execute_retry(once, retry_on=lambda e: False, stop_event=event) == 7
    assert calls["n"] == 1


def test_execute_retry_does_not_retry_base_exceptions() -> None:
    calls = {"n": 0}

    def exit_op() -> None:
        calls["n"] += 1
        raise SystemExit(1)

    with pytest.raises(SystemExit):
        execute_retry(
            exit_op,
            retry_on=lambda e: True,
            stop=stop_after_attempt(5),
        )
    assert calls["n"] == 1


def test_execute_retry_coerces_stop_none_to_never() -> None:
    calls = {"n": 0}

    def flaky() -> int:
        calls["n"] += 1
        if calls["n"] < 2:
            raise ValueError("x")
        return 1

    with remove_backoff():
        assert execute_retry(flaky, retry_on=lambda e: True, stop=None) == 1


def test_execute_retry_clamps_sleep_to_max_delay_remaining(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    def capture(seconds: float, _event: threading.Event | None) -> bool:
        sleeps.append(seconds)
        return True

    def fail() -> None:
        raise OSError("locked")

    monkeypatch.setattr("simplebroker._retry.time.monotonic", lambda: 0.0)

    with pytest.raises(OSError):
        execute_retry(
            fail,
            retry_on=lambda e: True,
            wait_gen_kwargs={"base": 2, "factor": 1.0},
            jitter=None,
            stop=stop_after_attempt(2),
            max_delay=0.3,
            sleep=capture,
        )
    assert sleeps == [0.3]


def test_execute_retry_honors_delay_budget_via_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic_time = 0.0
    calls = 0

    def fake_monotonic() -> float:
        return monotonic_time

    def locked() -> None:
        nonlocal calls, monotonic_time
        calls += 1
        monotonic_time += 0.03
        raise OSError("locked")

    monkeypatch.setattr("simplebroker._retry.time.monotonic", fake_monotonic)

    with remove_backoff(), pytest.raises(OSError):
        execute_retry(
            locked,
            retry_on=lambda e: True,
            stop=stop_after_delay(0.05),
        )
    assert calls == 2


def test_get_attempt_number_inside_operation() -> None:
    seen: list[int | None] = []

    def record() -> None:
        seen.append(get_attempt_number())
        if len(seen) < 2:
            raise ValueError("again")

    with remove_backoff():
        execute_retry(
            record,
            retry_on=lambda e: isinstance(e, ValueError),
            stop=stop_after_attempt(5),
        )
    assert seen == [1, 2]
    assert get_attempt_number() is None


def test_remove_backoff_zeroes_sleep() -> None:
    sleeps: list[float] = []

    def fake_sleep(seconds: float, _event: threading.Event | None) -> bool:
        sleeps.append(seconds)
        return True

    def fail() -> None:
        raise ValueError("x")

    with remove_backoff(), pytest.raises(ValueError):
        execute_retry(
            fail,
            retry_on=lambda e: True,
            stop=stop_after_attempt(2),
            sleep=fake_sleep,
        )
    assert sleeps == []


def test_nested_retry_config_restores_outer_multiplier() -> None:
    assert _capture_retry_sleeps() == [1.0]
    with retry_test_config(sleep_multiplier=0.25):
        assert _capture_retry_sleeps() == [0.25]
        with retry_test_config(sleep_multiplier=0.5):
            assert _capture_retry_sleeps() == [0.5]
        assert _capture_retry_sleeps() == [0.25]
    assert _capture_retry_sleeps() == [1.0]


def test_retry_config_restores_multiplier_after_exception() -> None:
    with (
        pytest.raises(LookupError, match="leave scope"),
        retry_test_config(sleep_multiplier=0.25),
    ):
        assert _capture_retry_sleeps() == [0.25]
        raise LookupError("leave scope")

    assert _capture_retry_sleeps() == [1.0]


def test_fresh_context_uses_default_retry_multiplier() -> None:
    with retry_test_config(sleep_multiplier=0.25):
        assert _capture_retry_sleeps() == [0.25]
        assert contextvars.Context().run(_capture_retry_sleeps) == [1.0]
        assert _capture_retry_sleeps() == [0.25]


def test_copied_context_inherits_without_mutating_parent() -> None:
    worker_override_active = threading.Event()
    parent_observed = threading.Event()
    worker_observations: list[list[float]] = []

    def worker() -> None:
        worker_observations.append(_capture_retry_sleeps())
        with retry_test_config(sleep_multiplier=0.5):
            worker_observations.append(_capture_retry_sleeps())
            worker_override_active.set()
            assert parent_observed.wait(timeout=_THREAD_TEST_TIMEOUT)
        worker_observations.append(_capture_retry_sleeps())

    with retry_test_config(sleep_multiplier=0.25):
        copied_context = contextvars.copy_context()
        with ThreadPoolExecutor(max_workers=1) as executor:
            worker_future = executor.submit(copied_context.run, worker)
            assert worker_override_active.wait(timeout=_THREAD_TEST_TIMEOUT)
            parent_sleep = _capture_retry_sleeps()
            parent_observed.set()
            worker_future.result(timeout=_THREAD_TEST_TIMEOUT)

        assert parent_sleep == [0.25]
        assert _capture_retry_sleeps() == [0.25]

    assert worker_observations == [[0.25], [0.5], [0.25]]
    assert _capture_retry_sleeps() == [1.0]


def test_retry_config_isolated_across_overlapping_fresh_contexts() -> None:
    first_entered = threading.Event()
    both_entered = threading.Barrier(2)
    both_observed = threading.Barrier(2)
    first_exited = threading.Event()
    observations: dict[str, list[list[float]]] = {"first": [], "second": []}

    def first_worker() -> None:
        with retry_test_config(sleep_multiplier=0.25):
            first_entered.set()
            both_entered.wait(timeout=_THREAD_TEST_TIMEOUT)
            observations["first"].append(_capture_retry_sleeps())
            both_observed.wait(timeout=_THREAD_TEST_TIMEOUT)
        first_exited.set()

    def second_worker() -> None:
        assert first_entered.wait(timeout=_THREAD_TEST_TIMEOUT)
        with retry_test_config(sleep_multiplier=0.5):
            both_entered.wait(timeout=_THREAD_TEST_TIMEOUT)
            observations["second"].append(_capture_retry_sleeps())
            both_observed.wait(timeout=_THREAD_TEST_TIMEOUT)
            assert first_exited.wait(timeout=_THREAD_TEST_TIMEOUT)
            observations["second"].append(_capture_retry_sleeps())

    first_context = contextvars.Context()
    second_context = contextvars.Context()
    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(first_context.run, first_worker)
        second_future = executor.submit(second_context.run, second_worker)
        first_future.result(timeout=_THREAD_TEST_TIMEOUT)
        second_future.result(timeout=_THREAD_TEST_TIMEOUT)

    assert observations == {
        "first": [[0.25]],
        "second": [[0.5], [0.5]],
    }
    assert first_context.run(_capture_retry_sleeps) == [1.0]
    assert second_context.run(_capture_retry_sleeps) == [1.0]
    assert _capture_retry_sleeps() == [1.0]


def test_hot_loop_warning_logs_after_rapid_retries(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="simplebroker._retry")
    sleeps: list[float] = []

    def fast_sleep(seconds: float, _event: threading.Event | None) -> bool:
        sleeps.append(seconds)
        return True

    def fail() -> None:
        raise ValueError("again")

    monkeypatch.setattr("simplebroker._retry.time.monotonic", lambda: 1.0)

    with pytest.raises(ValueError):
        execute_retry(
            fail,
            retry_on=lambda e: True,
            stop=stop_after_attempt(7),
            sleep=fast_sleep,
            jitter=None,
            wait_gen_kwargs={"base": 2, "factor": 0.01},
        )
    assert len(sleeps) >= 5
    assert any("Hot loop" in record.message for record in caplog.records)
