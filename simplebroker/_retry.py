# Copyright (c) 2025 Van Lindberg
# SPDX-License-Identifier: MIT

"""Small sync retry engine with bounded jitter and composable stop conditions.

This module is intentionally standalone: it depends only on the Python standard
library and can be copied into another project. It does not know about databases,
queues, or application-specific exceptions.
"""

from __future__ import annotations

import contextlib
import contextvars
import logging
import random
import threading
import time
from collections.abc import Callable, Generator, Iterator
from dataclasses import dataclass
from typing import Any, TypeVar

T = TypeVar("T")

__version__ = "1.0"

DEFAULT_MIN_RETRY_SLEEP_S = 0.005

_logger = logging.getLogger("simplebroker._retry")
_logger.addHandler(logging.NullHandler())


def interruptible_sleep(
    seconds: float,
    stop_event: threading.Event | None = None,
    *,
    chunk_size: float = 0.1,
) -> bool:
    """Sleep for the specified duration, but allow interruption by a stop event."""

    if seconds <= 0:
        return True

    event = stop_event or threading.Event()

    if seconds <= chunk_size:
        return not event.wait(timeout=seconds)

    start_time = time.perf_counter()
    target_end_time = start_time + seconds

    while time.perf_counter() < target_end_time:
        remaining = target_end_time - time.perf_counter()
        if remaining <= 0:
            break

        if event.wait(timeout=min(chunk_size, remaining)):
            return stop_event is None or not stop_event.is_set()

    return True


def apply_jitter(
    base_wait: float,
    *,
    floor: float = DEFAULT_MIN_RETRY_SLEEP_S,
) -> float:
    upper = max(floor, base_wait)
    return floor if upper <= floor else random.uniform(floor, upper)


def bounded_jitter(
    base_wait: float,
    *,
    floor: float = DEFAULT_MIN_RETRY_SLEEP_S,
) -> float:
    return apply_jitter(base_wait, floor=floor)


class Stop:
    def __call__(self, state: RetryState) -> bool:
        raise NotImplementedError

    def __or__(self, other: Stop) -> Stop:
        return stop_any(self, other)

    def __and__(self, other: Stop) -> Stop:
        return stop_all(self, other)


@dataclass
class RetryState:
    tries: int = 0
    start_time: float = 0.0
    elapsed: float = 0.0


class stop_after_attempt(Stop):
    def __init__(self, max_attempts: int) -> None:
        self.max_attempts = max_attempts

    def __call__(self, state: RetryState) -> bool:
        return state.tries >= self.max_attempts


class stop_after_delay(Stop):
    def __init__(self, max_delay: float) -> None:
        self.max_delay = max_delay

    def __call__(self, state: RetryState) -> bool:
        return state.elapsed >= self.max_delay


class stop_when_event_set(Stop):
    def __init__(self, event: threading.Event) -> None:
        self.event = event

    def __call__(self, state: RetryState) -> bool:
        del state
        return self.event.is_set()


class _StopAny(Stop):
    def __init__(self, stops: tuple[Stop, ...]) -> None:
        self._stops = stops

    def __call__(self, state: RetryState) -> bool:
        return any(stop(state) for stop in self._stops)


class _StopAll(Stop):
    def __init__(self, stops: tuple[Stop, ...]) -> None:
        self._stops = stops

    def __call__(self, state: RetryState) -> bool:
        return all(stop(state) for stop in self._stops)


def stop_any(*stops: Stop) -> Stop:
    return _StopAny(stops)


def stop_all(*stops: Stop) -> Stop:
    return _StopAll(stops)


class stop_never(Stop):
    def __call__(self, state: RetryState) -> bool:
        del state
        return False


class Wait:
    def __init__(self, gen_func: Callable[..., Generator[float, Any, None]]) -> None:
        self._gen_func = gen_func

    def __call__(self, **kwargs: Any) -> Generator[float, Any, None]:
        return self._gen_func(**kwargs)


def _expo(
    *,
    base: float = 2,
    factor: float = 1,
    max_value: float | None = None,
) -> Generator[float, Any, None]:
    yield 0.0
    n = 0
    while True:
        wait = factor * base**n
        if max_value is None or wait < max_value:
            yield wait
            n += 1
        else:
            yield max_value


expo = Wait(_expo)


class RetryInterrupted(Exception):
    """Raised when a retry sleep is interrupted by stop_event."""


_retry_context: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "_retry_context", default=None
)


@contextlib.contextmanager
def _attempt_context(attempt_number: int) -> Iterator[None]:
    token = _retry_context.set(attempt_number)
    try:
        yield
    finally:
        _retry_context.reset(token)


def is_retrying() -> bool:
    return _retry_context.get() is not None


def get_attempt_number() -> int | None:
    return _retry_context.get()


_TEST_CONFIG: dict[str, float] = {"sleep_multiplier": 1.0}


@contextlib.contextmanager
def test_config(*, sleep_multiplier: float | None = None) -> Iterator[None]:
    old = _TEST_CONFIG["sleep_multiplier"]
    if sleep_multiplier is not None:
        _TEST_CONFIG["sleep_multiplier"] = sleep_multiplier
    try:
        yield
    finally:
        _TEST_CONFIG["sleep_multiplier"] = old


@contextlib.contextmanager
def remove_backoff() -> Iterator[None]:
    with test_config(sleep_multiplier=0.0):
        yield


def _init_wait_gen(
    wait_gen: Wait | None,
    wait_gen_kwargs: dict[str, Any] | None,
) -> Generator[float, Any, None]:
    wait = (wait_gen or expo)(**(wait_gen_kwargs or {}))
    wait.send(None)
    return wait


_hot_loop_data: dict[str, float | int] = {"last_retry": 0.0, "count": 0}
_hot_loop_lock = threading.Lock()


def _check_hot_loop() -> None:
    now = time.monotonic()
    with _hot_loop_lock:
        prev = float(_hot_loop_data["last_retry"])
        if prev > 0 and now - prev < 0.1:
            _hot_loop_data["count"] = int(_hot_loop_data["count"]) + 1
        else:
            _hot_loop_data["count"] = 0
        _hot_loop_data["last_retry"] = now
        if int(_hot_loop_data["count"]) >= 5:
            _logger.warning(
                "Hot loop detected: %d retries in quick succession. "
                "Add jitter or increase backoff.",
                _hot_loop_data["count"],
            )
            _hot_loop_data["count"] = 0


def execute_retry(
    operation: Callable[[], T],
    *,
    retry_on: Callable[[Exception], bool],
    wait_gen: Wait | None = None,
    wait_gen_kwargs: dict[str, Any] | None = None,
    jitter: Callable[[float], float] | None = bounded_jitter,
    stop: Stop | None = None,
    max_delay: float | None = None,
    sleep: Callable[[float, threading.Event | None], bool] | None = None,
    stop_event: threading.Event | None = None,
    before_sleep: Callable[[RetryState, Exception, float], None] | None = None,
) -> T:
    if stop is None:
        stop = stop_never()
    sleep_fn = interruptible_sleep if sleep is None else sleep
    wait = _init_wait_gen(wait_gen, wait_gen_kwargs)
    start = time.monotonic()
    state = RetryState(start_time=start)

    while True:
        state.tries += 1
        with _attempt_context(state.tries):
            try:
                return operation()
            except Exception as exc:
                if not retry_on(exc):
                    raise
                state.elapsed = time.monotonic() - start
                if stop(state):
                    raise
                base_wait = wait.send(None)
                sleep_seconds = jitter(base_wait) if jitter else base_wait
                if max_delay is not None:
                    remaining = max_delay - state.elapsed
                    if remaining <= 0:
                        raise
                    sleep_seconds = min(sleep_seconds, remaining)
                sleep_seconds *= _TEST_CONFIG["sleep_multiplier"]
                if sleep_seconds > 0:
                    if before_sleep is not None:
                        before_sleep(state, exc, sleep_seconds)
                    _check_hot_loop()
                    if not sleep_fn(sleep_seconds, stop_event):
                        raise RetryInterrupted from None


__all__ = [
    "DEFAULT_MIN_RETRY_SLEEP_S",
    "RetryInterrupted",
    "RetryState",
    "Stop",
    "Wait",
    "__version__",
    "apply_jitter",
    "bounded_jitter",
    "execute_retry",
    "expo",
    "get_attempt_number",
    "interruptible_sleep",
    "is_retrying",
    "remove_backoff",
    "stop_after_attempt",
    "stop_after_delay",
    "stop_all",
    "stop_any",
    "stop_never",
    "stop_when_event_set",
    "test_config",
]

# ~
