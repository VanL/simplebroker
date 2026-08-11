"""Terminal cleanup contracts for Redis activity waiters."""

from __future__ import annotations

import threading

import pytest
import simplebroker_redis.plugin as redis_plugin_module

pytestmark = [pytest.mark.redis_only]


class _Listener:
    def __init__(
        self,
        name: str,
        events: list[str],
        *,
        unregister_error: BaseException | None = None,
    ) -> None:
        self.name = name
        self.events = events
        self.unregister_error = unregister_error

    def unregister(self, queue_name: str) -> None:
        self.events.append(f"unregister:{self.name}:{queue_name}")
        if self.unregister_error is not None:
            raise self.unregister_error


class _Registry:
    def __init__(
        self,
        events: list[str],
        *,
        release_error: BaseException | None = None,
        release_errors: dict[str, BaseException] | None = None,
    ) -> None:
        self.events = events
        self.release_error = release_error
        self.release_errors = release_errors or {}

    def release(self, listener: _Listener) -> None:
        self.events.append(f"release:{listener.name}")
        release_error = self.release_errors.get(listener.name, self.release_error)
        if release_error is not None:
            raise release_error


def _waiter(listener: _Listener, queue_name: str):
    registration = redis_plugin_module._QueueWaiterRegistration(
        queue_name=queue_name,
        condition=threading.Condition(),
        version=0,
        wildcard_version=0,
    )
    return redis_plugin_module.RedisActivityWaiter(
        listener,  # type: ignore[arg-type]
        registration,
        threading.Event(),
    )


def test_redis_multi_waiter_closes_every_child_and_is_terminal_after_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    first_error = RuntimeError("first unregister failed")
    first = _waiter(
        _Listener("first", events, unregister_error=first_error),
        "alpha",
    )
    second = _waiter(_Listener("second", events), "beta")
    waiter = redis_plugin_module.RedisMultiQueueActivityWaiter(
        [first, second],
        threading.Event(),
    )
    monkeypatch.setattr(redis_plugin_module, "_activity_registry", _Registry(events))

    with pytest.raises(RuntimeError, match="first unregister failed") as raised:
        waiter.close()

    assert raised.value is first_error
    assert events == [
        "unregister:first:alpha",
        "release:first",
        "unregister:second:beta",
        "release:second",
    ]
    waiter.close()
    assert events == [
        "unregister:first:alpha",
        "release:first",
        "unregister:second:beta",
        "release:second",
    ]


def test_redis_single_waiter_preserves_first_error_and_notes_later_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    first_error = RuntimeError("unregister failed")
    later_error = ValueError("release failed")
    waiter = _waiter(
        _Listener("single", events, unregister_error=first_error),
        "jobs",
    )
    monkeypatch.setattr(
        redis_plugin_module,
        "_activity_registry",
        _Registry(events, release_error=later_error),
    )

    with pytest.raises(RuntimeError, match="unregister failed") as raised:
        waiter.close()

    assert raised.value is first_error
    assert raised.value.__notes__ == ["cleanup failure: ValueError: release failed"]
    assert events == ["unregister:single:jobs", "release:single"]

    waiter.close()
    assert events == ["unregister:single:jobs", "release:single"]


def test_redis_multi_waiter_notes_later_child_failure_in_cleanup_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    first_error = RuntimeError("first child failed")
    later_error = ValueError("second child failed")
    first = _waiter(
        _Listener("first", events, unregister_error=first_error),
        "alpha",
    )
    second = _waiter(
        _Listener("second", events, unregister_error=later_error),
        "beta",
    )
    waiter = redis_plugin_module.RedisMultiQueueActivityWaiter(
        [first, second],
        threading.Event(),
    )
    monkeypatch.setattr(redis_plugin_module, "_activity_registry", _Registry(events))

    with pytest.raises(RuntimeError, match="first child failed") as raised:
        waiter.close()

    assert raised.value is first_error
    assert raised.value.__notes__ == [
        "cleanup failure: ValueError: second child failed"
    ]
    assert events == [
        "unregister:first:alpha",
        "release:first",
        "unregister:second:beta",
        "release:second",
    ]


def test_redis_multi_waiter_preserves_later_child_secondary_failure_note(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    first_error = RuntimeError("first child unregister failed")
    later_error = ValueError("second child unregister failed")
    secondary_error = LookupError("second child release failed")
    first = _waiter(
        _Listener("first", events, unregister_error=first_error),
        "alpha",
    )
    second = _waiter(
        _Listener("second", events, unregister_error=later_error),
        "beta",
    )
    waiter = redis_plugin_module.RedisMultiQueueActivityWaiter(
        [first, second],
        threading.Event(),
    )
    monkeypatch.setattr(
        redis_plugin_module,
        "_activity_registry",
        _Registry(events, release_errors={"second": secondary_error}),
    )

    with pytest.raises(RuntimeError, match="first child unregister failed") as raised:
        waiter.close()

    assert raised.value is first_error
    assert raised.value.__notes__ == [
        "cleanup failure: ValueError: second child unregister failed",
        "cleanup failure: LookupError: second child release failed",
    ]
    assert events == [
        "unregister:first:alpha",
        "release:first",
        "unregister:second:beta",
        "release:second",
    ]


def test_redis_single_waiter_successful_close_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    waiter = _waiter(_Listener("single", events), "jobs")
    monkeypatch.setattr(redis_plugin_module, "_activity_registry", _Registry(events))

    waiter.close()
    waiter.close()

    assert events == ["unregister:single:jobs", "release:single"]


def test_redis_multi_waiter_successful_close_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    first = _waiter(_Listener("first", events), "alpha")
    second = _waiter(_Listener("second", events), "beta")
    waiter = redis_plugin_module.RedisMultiQueueActivityWaiter(
        [first, second],
        threading.Event(),
    )
    monkeypatch.setattr(redis_plugin_module, "_activity_registry", _Registry(events))

    waiter.close()
    waiter.close()

    assert events == [
        "unregister:first:alpha",
        "release:first",
        "unregister:second:beta",
        "release:second",
    ]


def test_redis_multi_waiter_base_exception_is_terminal_and_stops_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    first = _waiter(
        _Listener("first", events, unregister_error=KeyboardInterrupt()),
        "alpha",
    )
    second = _waiter(_Listener("second", events), "beta")
    waiter = redis_plugin_module.RedisMultiQueueActivityWaiter(
        [first, second],
        threading.Event(),
    )
    monkeypatch.setattr(redis_plugin_module, "_activity_registry", _Registry(events))

    with pytest.raises(KeyboardInterrupt):
        waiter.close()

    assert events == ["unregister:first:alpha"]
    waiter.close()
    assert events == ["unregister:first:alpha"]
