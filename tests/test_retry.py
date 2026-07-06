"""Tests for the vendorable retry engine."""

from __future__ import annotations

import ast
import logging
import sys
import threading
from pathlib import Path

import pytest

import simplebroker._retry as retry_module
from simplebroker._retry import (
    DEFAULT_MIN_RETRY_SLEEP_S,
    RetryState,
    __version__,
    apply_jitter,
    bounded_jitter,
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

_ALLOWED_STDLIB_ROOTS = frozenset(sys.stdlib_module_names) | {"__future__"}


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


def test_version_is_non_empty_string() -> None:
    assert isinstance(__version__, str)
    assert __version__


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


def test_bounded_jitter_delegates_to_apply_jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[float, float]] = []

    def capture(base: float, *, floor: float = DEFAULT_MIN_RETRY_SLEEP_S) -> float:
        calls.append((base, floor))
        return 0.02

    monkeypatch.setattr("simplebroker._retry.apply_jitter", capture)
    assert bounded_jitter(0.05) == 0.02
    assert calls == [(0.05, DEFAULT_MIN_RETRY_SLEEP_S)]


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

    with remove_backoff():
        with pytest.raises(RuntimeError, match="nope"):
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

    with remove_backoff():
        with pytest.raises(OSError):
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

    with remove_backoff():
        with pytest.raises(ValueError):
            execute_retry(
                fail,
                retry_on=lambda e: True,
                stop=stop_after_attempt(2),
                sleep=fake_sleep,
            )
    assert sleeps == []


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
