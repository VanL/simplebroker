"""Terminal cleanup contracts for PostgreSQL activity waiters."""

from __future__ import annotations

import threading

import pytest
import simplebroker_pg.runner as pg_runner_module

pytestmark = [pytest.mark.pg_only]


class _Listener:
    def __init__(
        self,
        events: list[str],
        *,
        unregister_error: BaseException | None = None,
    ) -> None:
        self.events = events
        self.unregister_error = unregister_error

    def register_queue(self, queue_name: str) -> tuple[int, int]:
        self.events.append(f"register:{queue_name}")
        return 0, 0

    def unregister_queue(self, queue_name: str) -> None:
        self.events.append(f"unregister:{queue_name}")
        if self.unregister_error is not None:
            raise self.unregister_error

    def register_queue_set(
        self,
        queue_names: tuple[str, ...],
    ) -> tuple[int, dict[str, int], int]:
        self.events.append(f"register-set:{','.join(queue_names)}")
        return 7, dict.fromkeys(queue_names, 0), 0

    def unregister_queue_set(self, fan_in_id: int) -> None:
        self.events.append(f"unregister-set:{fan_in_id}")
        if self.unregister_error is not None:
            raise self.unregister_error


class _Registry:
    def __init__(
        self,
        listener: _Listener,
        events: list[str],
        *,
        release_error: BaseException | None = None,
    ) -> None:
        self.listener = listener
        self.events = events
        self.release_error = release_error

    def acquire(self, dsn: str, *, schema: str) -> _Listener:
        self.events.append(f"acquire:{dsn}:{schema}")
        return self.listener

    def release(self, dsn: str, *, schema: str) -> None:
        self.events.append(f"release:{dsn}:{schema}")
        if self.release_error is not None:
            raise self.release_error


def test_postgres_single_waiter_preserves_first_error_and_notes_release_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    first_error = RuntimeError("unregister failed")
    later_error = ValueError("release failed")
    listener = _Listener(events, unregister_error=first_error)
    registry = _Registry(listener, events, release_error=later_error)
    monkeypatch.setattr(pg_runner_module, "_activity_registry", registry)
    waiter = pg_runner_module.PostgresActivityWaiter(
        "postgresql://example/test",
        schema="broker_data",
        queue_name="jobs",
        stop_event=threading.Event(),
    )

    with pytest.raises(RuntimeError, match="unregister failed") as raised:
        waiter.close()

    assert raised.value is first_error
    assert raised.value.__notes__ == ["cleanup failure: ValueError: release failed"]
    assert events == [
        "acquire:postgresql://example/test:broker_data",
        "register:jobs",
        "unregister:jobs",
        "release:postgresql://example/test:broker_data",
    ]

    waiter.close()
    assert len(events) == 4


def test_postgres_multi_waiter_error_is_terminal_after_release_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    first_error = RuntimeError("fan-in unregister failed")
    listener = _Listener(events, unregister_error=first_error)
    registry = _Registry(listener, events)
    monkeypatch.setattr(pg_runner_module, "_activity_registry", registry)
    waiter = pg_runner_module.PostgresMultiQueueActivityWaiter(
        "postgresql://example/test",
        schema="broker_data",
        queue_names=("alpha", "beta"),
        stop_event=threading.Event(),
    )

    with pytest.raises(RuntimeError, match="fan-in unregister failed") as raised:
        waiter.close()

    assert raised.value is first_error
    assert events == [
        "acquire:postgresql://example/test:broker_data",
        "register-set:alpha,beta",
        "unregister-set:7",
        "release:postgresql://example/test:broker_data",
    ]
    waiter.close()
    assert len(events) == 4


@pytest.mark.parametrize("multi", [False, True], ids=["single", "multi"])
def test_postgres_waiter_successful_close_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
    multi: bool,
) -> None:
    events: list[str] = []
    listener = _Listener(events)
    registry = _Registry(listener, events)
    monkeypatch.setattr(pg_runner_module, "_activity_registry", registry)
    waiter: (
        pg_runner_module.PostgresActivityWaiter
        | pg_runner_module.PostgresMultiQueueActivityWaiter
    )
    if multi:
        waiter = pg_runner_module.PostgresMultiQueueActivityWaiter(
            "postgresql://example/test",
            schema="broker_data",
            queue_names=("alpha", "beta"),
            stop_event=threading.Event(),
        )
    else:
        waiter = pg_runner_module.PostgresActivityWaiter(
            "postgresql://example/test",
            schema="broker_data",
            queue_name="jobs",
            stop_event=threading.Event(),
        )
    baseline = len(events)

    waiter.close()
    after_first = list(events)
    waiter.close()

    assert len(after_first) == baseline + 2
    assert events == after_first


def test_postgres_multi_waiter_base_exception_is_terminal_and_stops_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    listener = _Listener(events, unregister_error=KeyboardInterrupt())
    registry = _Registry(listener, events)
    monkeypatch.setattr(pg_runner_module, "_activity_registry", registry)
    waiter = pg_runner_module.PostgresMultiQueueActivityWaiter(
        "postgresql://example/test",
        schema="broker_data",
        queue_names=("alpha", "beta"),
        stop_event=threading.Event(),
    )

    with pytest.raises(KeyboardInterrupt):
        waiter.close()

    assert events == [
        "acquire:postgresql://example/test:broker_data",
        "register-set:alpha,beta",
        "unregister-set:7",
    ]
    waiter.close()
    assert len(events) == 3
