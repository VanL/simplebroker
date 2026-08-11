"""Terminal cleanup contracts for Redis activity waiters."""

from __future__ import annotations

import threading

import pytest
import simplebroker_redis.plugin as redis_plugin_module

from tests.helpers.state_machine_contracts import (
    TransitionCase,
    fires_transition_table,
)

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


def _waiter(
    listener: _Listener,
    queue_name: str,
) -> redis_plugin_module.RedisActivityWaiter:
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


ACTIVITY_WAITER_TRANSITIONS = (
    TransitionCase(
        transition_id="CLOSE_SUCCESS",
        start_state="open",
        event="close",
        guard="owner has serialized wait and close",
        next_state="closed",
        effects="mark terminal, then run every owned cleanup action once",
        expected_result="return without error",
        payload="success",
    ),
    TransitionCase(
        transition_id="CLOSE_ORDINARY_FAILURE",
        start_state="open",
        event="close",
        guard="an independently safe action remains after an ordinary failure",
        next_state="closed",
        effects="continue safe cleanup and retain the first failure",
        expected_result="raise the first ordinary failure",
        payload="ordinary_failure",
    ),
    TransitionCase(
        transition_id="CLOSE_NESTED_FAILURES",
        start_state="open",
        event="close",
        guard="later child cleanup has primary and secondary failures",
        next_state="closed",
        effects="flatten all later cleanup evidence in execution order",
        expected_result="raise the first failure with ordered notes",
        payload="nested_failures",
    ),
    TransitionCase(
        transition_id="CLOSE_INTERRUPTED",
        start_state="open",
        event="close",
        guard="cleanup raises BaseException outside Exception",
        next_state="closed",
        effects="stop the current cleanup attempt immediately",
        expected_result="propagate the interruption",
        payload="interrupt",
    ),
    TransitionCase(
        transition_id="CLOSE_AGAIN",
        start_state="closed",
        event="close",
        guard="the first close attempt already ran",
        next_state="closed",
        effects="perform no cleanup action",
        expected_result="return without error",
        payload="repeat",
    ),
)


@fires_transition_table("SM-ACTIVITY-WAITER", ACTIVITY_WAITER_TRANSITIONS)
def test_activity_waiter_fires_transition_table(
    monkeypatch: pytest.MonkeyPatch,
    transition_case: TransitionCase[str],
) -> None:
    events: list[str] = []
    scenario = transition_case.payload
    first_error: BaseException | None = None
    release_errors: dict[str, BaseException] = {}

    if scenario == "interrupt":
        first_error = KeyboardInterrupt()
    elif scenario in {"ordinary_failure", "nested_failures"}:
        first_error = RuntimeError("first child unregister failed")

    first = _waiter(
        _Listener("first", events, unregister_error=first_error),
        "alpha",
    )
    second_error: BaseException | None = None
    if scenario == "nested_failures":
        second_error = ValueError("second child unregister failed")
        release_errors["second"] = LookupError("second child release failed")
    second = _waiter(
        _Listener("second", events, unregister_error=second_error),
        "beta",
    )
    waiter = redis_plugin_module.RedisMultiQueueActivityWaiter(
        [first, second],
        threading.Event(),
    )
    monkeypatch.setattr(
        redis_plugin_module,
        "_activity_registry",
        _Registry(events, release_errors=release_errors),
    )

    if scenario in {"success", "repeat"}:
        waiter.close()
        after_first = list(events)
        if scenario == "success":
            assert after_first == [
                "unregister:first:alpha",
                "release:first",
                "unregister:second:beta",
                "release:second",
            ]
    elif scenario == "ordinary_failure":
        with pytest.raises(RuntimeError, match="first child unregister failed"):
            waiter.close()
        after_first = list(events)
        assert after_first[-2:] == ["unregister:second:beta", "release:second"]
    elif scenario == "nested_failures":
        with pytest.raises(
            RuntimeError, match="first child unregister failed"
        ) as raised:
            waiter.close()
        after_first = list(events)
        assert raised.value.__notes__ == [
            "cleanup failure: ValueError: second child unregister failed",
            "cleanup failure: LookupError: second child release failed",
        ]
    else:
        assert scenario == "interrupt"
        with pytest.raises(KeyboardInterrupt):
            waiter.close()
        after_first = list(events)
        assert after_first == ["unregister:first:alpha"]

    assert waiter._closed is True
    waiter.close()
    assert events == after_first


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
