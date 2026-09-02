"""Executable transition contracts for Redis extension state machines."""

from __future__ import annotations

import os
import queue
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from typing import cast

import pytest
import redis
import simplebroker_redis.plugin as redis_plugin_module
import simplebroker_redis.runner as redis_runner_module
from simplebroker_redis import scripts
from simplebroker_redis.core import RedisBrokerCore
from simplebroker_redis.keys import RedisKeys, encode_id
from simplebroker_redis.plugin import (
    _SharedRedisActivityListener,
)
from simplebroker_redis.runner import RedisRunner

from simplebroker._exceptions import IntegrityError, OperationalError, QueueNameError
from simplebroker._timestamp import TimestampError
from tests.helper_scripts import drive_until
from tests.helpers.state_machine_contracts import (
    TransitionCase,
    fires_transition_table,
)

pytestmark = [pytest.mark.redis_only]

_CLOSED = object()


class _ScriptedPubSub:
    def __init__(self) -> None:
        self.events: queue.Queue[object] = queue.Queue()
        self.patterns: list[str] = []
        self.closed = False

    def psubscribe(self, pattern: str) -> None:
        self.patterns.append(pattern)

    def get_message(self, *, timeout: float) -> dict[str, str] | None:
        try:
            event = self.events.get(timeout=timeout)
        except queue.Empty:
            return None
        if event is _CLOSED:
            return None
        if isinstance(event, BaseException):
            raise event
        assert isinstance(event, dict)
        return event

    def send(self, channel: str) -> None:
        self.events.put({"channel": channel})

    def fail(self, error: BaseException) -> None:
        self.events.put(error)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.events.put(_CLOSED)


class _ScriptedRedisClient:
    def __init__(self, pubsub: _ScriptedPubSub) -> None:
        self._pubsub = pubsub
        self.closed = False

    def pubsub(self, *, ignore_subscribe_messages: bool) -> _ScriptedPubSub:
        assert ignore_subscribe_messages
        return self._pubsub

    def close(self) -> None:
        self.closed = True


@contextmanager
def _started_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[
    tuple[_SharedRedisActivityListener, _ScriptedPubSub, _ScriptedRedisClient]
]:
    pubsub = _ScriptedPubSub()
    client = _ScriptedRedisClient(pubsub)
    monkeypatch.setattr(
        redis_plugin_module.redis.Redis,
        "from_url",
        lambda *args, **kwargs: client,
    )
    listener = _SharedRedisActivityListener(
        "redis://transport.invalid/0",
        "transition_contract",
    )
    try:
        yield listener, pubsub, client
    finally:
        listener.close()


def _redis_listener_starts(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, pubsub, _client):
        assert listener._ready.is_set()
        assert listener._error is None
        assert pubsub.patterns == ["simplebroker:transition_contract:activity:*"]


def _redis_listener_start_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    class _BrokenPubSub(_ScriptedPubSub):
        def psubscribe(self, pattern: str) -> None:
            del pattern
            raise RuntimeError("subscribe failed")

    pubsub = _BrokenPubSub()
    client = _ScriptedRedisClient(pubsub)
    monkeypatch.setattr(
        redis_plugin_module.redis.Redis,
        "from_url",
        lambda *args, **kwargs: client,
    )
    with pytest.raises(OperationalError, match="subscribe failed"):
        _SharedRedisActivityListener(
            "redis://transport.invalid/0",
            "transition_contract",
        )


def _redis_listener_start_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    class _SlowPubSub(_ScriptedPubSub):
        def psubscribe(self, pattern: str) -> None:
            time.sleep(0.05)
            super().psubscribe(pattern)

    pubsub = _SlowPubSub()
    client = _ScriptedRedisClient(pubsub)
    monkeypatch.setattr(
        redis_plugin_module.redis.Redis,
        "from_url",
        lambda *args, **kwargs: client,
    )

    with pytest.raises(OperationalError, match="did not start"):
        _SharedRedisActivityListener(
            "redis://transport.invalid/0",
            "transition_contract",
            startup_timeout=0.01,
        )

    assert pubsub.closed
    assert client.closed


def _redis_listener_registers(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, _pubsub, _client):
        first = listener.register("jobs")
        second = listener.register("jobs")
        assert first.version == second.version == 0
        assert listener._refcounts == {"jobs": 2}
        assert set(listener._conditions) == {"jobs"}


def _redis_listener_routes_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, pubsub, _client):
        registration = listener.register("jobs")
        pubsub.send(listener._channel("jobs"))
        assert listener.wait(
            registration,
            1.0,
            stop_event=threading.Event(),
        )
        assert registration.version == 1


def _redis_listener_ignores_global(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, pubsub, _client):
        registration = listener.register("jobs")
        pubsub.send(listener._channel(None))
        assert not listener.wait(
            registration,
            0.05,
            stop_event=threading.Event(),
        )
        assert registration.version == 0


def _redis_listener_ignores_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, pubsub, _client):
        registration = listener.register("jobs")
        pubsub.send(listener._channel("unknown"))
        assert not listener.wait(
            registration,
            0.05,
            stop_event=threading.Event(),
        )
        assert listener._versions == {"jobs": 0}


def _redis_listener_publishes_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, pubsub, _client):
        registration = listener.register("jobs")
        pubsub.fail(RuntimeError("pubsub read failed"))
        drive_until(
            lambda: listener._error is not None,
            timeout=1.0,
            interval=0.005,
            message="Redis listener transition did not become observable",
            diagnostics=lambda: {
                "error": repr(listener._error),
                "ready": listener._ready.is_set(),
                "stopped": listener._stop_event.is_set(),
                "thread_alive": listener._thread.is_alive(),
            },
        )
        with pytest.raises(OperationalError, match="pubsub read failed"):
            listener.wait(
                registration,
                0.1,
                stop_event=threading.Event(),
            )


def _redis_listener_unregisters_one(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, _pubsub, _client):
        listener.register("jobs")
        listener.register("jobs")
        listener.unregister("jobs")
        assert listener._refcounts == {"jobs": 1}
        assert set(listener._conditions) == {"jobs"}


def _redis_listener_unregisters_last(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, _pubsub, _client):
        listener.register("jobs")
        listener.unregister("jobs")
        listener.unregister("jobs")
        assert listener._refcounts == {}
        assert listener._conditions == {}
        assert listener._versions == {}


def _redis_listener_stop_wakes_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, pubsub, client):
        registration = listener.register("jobs")
        result: list[bool] = []
        thread = threading.Thread(
            target=lambda: result.append(
                listener.wait(
                    registration,
                    5.0,
                    stop_event=threading.Event(),
                )
            )
        )
        thread.start()
        listener.close()
        thread.join(timeout=1.0)
        assert not thread.is_alive()
        assert result == [False]
        assert not listener._thread.is_alive()
        assert pubsub.closed
        assert client.closed


def _redis_listener_close_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, pubsub, client):
        listener.close()
        listener.close()
        assert listener._stop_event.is_set()
        assert not listener._thread.is_alive()
        assert pubsub.closed
        assert client.closed


def _redis_waiter_close_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    with _started_listener(monkeypatch) as (listener, _pubsub, _client):
        releases: list[_SharedRedisActivityListener] = []
        monkeypatch.setattr(
            redis_plugin_module._activity_registry,
            "release",
            releases.append,
        )
        waiter = redis_plugin_module.RedisActivityWaiter(
            listener,
            listener.register("jobs"),
            threading.Event(),
        )
        waiter.close()
        waiter.close()
        assert listener._refcounts == {}
        assert releases == [listener]


REDIS_ACTIVITY_LISTENER_TRANSITIONS = (
    TransitionCase(
        transition_id="START-READY",
        start_state="starting",
        event="pattern subscription succeeds",
        guard="external Pub/Sub transport is available",
        next_state="ready",
        effects="subscribes to the namespace activity pattern and publishes readiness",
        expected_result="listener accepts queue registrations",
        payload=_redis_listener_starts,
    ),
    TransitionCase(
        transition_id="START-FAILURE",
        start_state="starting",
        event="pattern subscription fails",
        guard="stop was not requested",
        next_state="failed",
        effects="stores the error, publishes readiness, and closes transports",
        expected_result="OperationalError reaches the creator",
        payload=_redis_listener_start_fails,
    ),
    TransitionCase(
        transition_id="START-TIMEOUT",
        start_state="starting",
        event="subscription does not publish readiness before the deadline",
        guard="the startup timeout expires while the transport is still opening",
        next_state="failed-and-closed",
        effects="requests stop, waits for worker completion, and closes transports",
        expected_result="OperationalError identifies startup timeout",
        payload=_redis_listener_start_times_out,
    ),
    TransitionCase(
        transition_id="REGISTER-REFCOUNT",
        start_state="ready without queue registrations",
        event="the same queue is registered twice",
        guard="listener is ready",
        next_state="ready with two queue references",
        effects="creates one condition/version owner and increments its refcount",
        expected_result="both registrations share the current version",
        payload=_redis_listener_registers,
    ),
    TransitionCase(
        transition_id="NOTIFY-QUEUE",
        start_state="ready with jobs registered",
        event="jobs channel message arrives",
        guard="channel names an active queue",
        next_state="ready with jobs version advanced",
        effects="increments the queue version and wakes its waiters",
        expected_result="wait reports one activity transition",
        payload=_redis_listener_routes_queue,
    ),
    TransitionCase(
        transition_id="NOTIFY-GLOBAL-IGNORED",
        start_state="ready with jobs registered",
        event="global activity channel message arrives",
        guard="queue-specific versions are the wake-up source",
        next_state="ready with versions unchanged",
        effects="does not consume a queue transition",
        expected_result="jobs wait times out",
        payload=_redis_listener_ignores_global,
    ),
    TransitionCase(
        transition_id="NOTIFY-UNKNOWN",
        start_state="ready with jobs registered",
        event="unknown queue channel message arrives",
        guard="queue has no registration",
        next_state="ready with versions unchanged",
        effects="does not create queue state",
        expected_result="jobs wait times out",
        payload=_redis_listener_ignores_unknown,
    ),
    TransitionCase(
        transition_id="READ-FAILURE",
        start_state="ready with jobs registered",
        event="Pub/Sub read raises",
        guard="stop was not requested",
        next_state="failed",
        effects="stores the failure and wakes registered waiters",
        expected_result="wait translates and raises the stored failure",
        payload=_redis_listener_publishes_failure,
    ),
    TransitionCase(
        transition_id="UNREGISTER-DECREMENT",
        start_state="ready with two jobs references",
        event="one jobs registration closes",
        guard="another jobs registration remains",
        next_state="ready with one jobs reference",
        effects="decrements the refcount and preserves queue state",
        expected_result="remaining waiter stays registered",
        payload=_redis_listener_unregisters_one,
    ),
    TransitionCase(
        transition_id="UNREGISTER-LAST",
        start_state="ready with one jobs reference",
        event="last jobs registration closes",
        guard="no other jobs registration remains",
        next_state="ready without jobs state",
        effects="removes condition, version, and refcount; unknown close is a no-op",
        expected_result="no jobs registration remains",
        payload=_redis_listener_unregisters_last,
    ),
    TransitionCase(
        transition_id="STOP-WAIT",
        start_state="ready with a blocked waiter",
        event="listener closes",
        guard="no activity is pending",
        next_state="stopped",
        effects="sets stop and wakes registered conditions",
        expected_result="blocked wait returns false",
        payload=_redis_listener_stop_wakes_wait,
    ),
    TransitionCase(
        transition_id="LISTENER-CLOSE-IDEMPOTENT",
        start_state="ready",
        event="listener close repeats",
        guard="the first close already completed the worker",
        next_state="stopped",
        effects="preserves closed transports and a completed worker thread",
        expected_result="both close calls complete",
        payload=_redis_listener_close_is_idempotent,
    ),
    TransitionCase(
        transition_id="WAITER-CLOSE-IDEMPOTENT",
        start_state="ready with one jobs waiter",
        event="waiter close repeats",
        guard="first close unregisters its queue",
        next_state="ready without jobs state",
        effects="releases the registry exactly once",
        expected_result="second close is a no-op",
        payload=_redis_waiter_close_is_idempotent,
    ),
)


@fires_transition_table(
    "SM-REDIS-ACTIVITY-LISTENER",
    REDIS_ACTIVITY_LISTENER_TRANSITIONS,
)
def test_redis_activity_listener_fires_transition_table(
    transition_case: TransitionCase[Callable[[pytest.MonkeyPatch], None]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transition_case.payload(monkeypatch)


class _TrackingPool:
    def __init__(self, generation: int) -> None:
        self.generation = generation
        self.disconnect_calls = 0

    def disconnect(self) -> None:
        self.disconnect_calls += 1


class _TrackingClient:
    def __init__(self, pool: _TrackingPool) -> None:
        self.pool = pool
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


@dataclass(slots=True)
class _RunnerResources:
    pools: list[_TrackingPool]
    clients: list[_TrackingClient]


def _runner_with_tracking_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[RedisRunner, _RunnerResources]:
    runner = RedisRunner(
        "redis://transport.invalid/0",
        namespace="transition_contract",
    )
    resources = _RunnerResources([], [])

    def create_pool() -> _TrackingPool:
        pool = _TrackingPool(len(resources.pools) + 1)
        resources.pools.append(pool)
        return pool

    def create_client(*, connection_pool: _TrackingPool) -> _TrackingClient:
        client = _TrackingClient(connection_pool)
        resources.clients.append(client)
        return client

    monkeypatch.setattr(runner, "_create_pool", create_pool)
    monkeypatch.setattr(redis_runner_module.redis, "Redis", create_client)
    return runner, resources


def _runner_lazy_create(monkeypatch: pytest.MonkeyPatch) -> None:
    runner, resources = _runner_with_tracking_resources(monkeypatch)
    assert runner._pool is None
    assert runner._client is None
    assert runner.client is resources.clients[0]
    assert len(resources.pools) == len(resources.clients) == 1


def _runner_reuses(monkeypatch: pytest.MonkeyPatch) -> None:
    runner, resources = _runner_with_tracking_resources(monkeypatch)
    first = runner.client
    assert runner.client is first
    assert len(resources.pools) == len(resources.clients) == 1


def _runner_concurrent_first_access(monkeypatch: pytest.MonkeyPatch) -> None:
    runner, resources = _runner_with_tracking_resources(monkeypatch)
    barrier = threading.Barrier(8)
    clients: list[object] = []

    def access() -> None:
        barrier.wait()
        clients.append(runner.client)

    threads = [threading.Thread(target=access) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=1.0)
        assert not thread.is_alive()
    assert len({id(client) for client in clients}) == 1
    assert len(resources.pools) == len(resources.clients) == 1


def _runner_creation_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    runner, _resources = _runner_with_tracking_resources(monkeypatch)
    monkeypatch.setattr(
        runner,
        "_create_pool",
        lambda: (_ for _ in ()).throw(RuntimeError("pool creation failed")),
    )
    with pytest.raises(RuntimeError, match="pool creation failed"):
        _ = runner.client
    assert runner._pool is None
    assert runner._client is None


def _runner_client_creation_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    runner, resources = _runner_with_tracking_resources(monkeypatch)
    attempts = 0

    def create_client(*, connection_pool: _TrackingPool) -> _TrackingClient:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("client creation failed")
        client = _TrackingClient(connection_pool)
        resources.clients.append(client)
        return client

    monkeypatch.setattr(redis_runner_module.redis, "Redis", create_client)

    with pytest.raises(RuntimeError, match="client creation failed"):
        _ = runner.client
    assert runner._pool is None
    assert runner._client is None
    assert len(resources.pools) == 1
    assert resources.pools[0].disconnect_calls == 1

    assert runner.client is resources.clients[0]
    assert len(resources.pools) == 2
    assert resources.pools[1].disconnect_calls == 0


def _runner_fork_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    runner, resources = _runner_with_tracking_resources(monkeypatch)
    first_client = cast(_TrackingClient, runner.client)
    first_pool = resources.pools[0]
    runner._pid = -1
    second_client = runner.client
    assert second_client is not first_client
    assert first_client.close_calls == 0
    assert first_pool.disconnect_calls == 0
    assert any(
        first_client is resource
        for resource in redis_runner_module._ABANDONED_FORK_REDIS_RESOURCES
    )
    assert any(
        first_pool is resource
        for resource in redis_runner_module._ABANDONED_FORK_REDIS_RESOURCES
    )
    assert runner._pid == os.getpid()
    assert len(resources.pools) == len(resources.clients) == 2


def _runner_closes_owned_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    runner, resources = _runner_with_tracking_resources(monkeypatch)
    client = cast(_TrackingClient, runner.client)
    pool = resources.pools[0]
    runner.close()
    assert runner._client is None
    assert runner._pool is None
    assert client.close_calls == 1
    assert pool.disconnect_calls == 1


def _runner_close_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    runner, resources = _runner_with_tracking_resources(monkeypatch)
    client = cast(_TrackingClient, runner.client)
    pool = resources.pools[0]
    runner.close()
    runner.close()
    assert client.close_calls == 1
    assert pool.disconnect_calls == 1


def _runner_recreates_after_close(monkeypatch: pytest.MonkeyPatch) -> None:
    runner, resources = _runner_with_tracking_resources(monkeypatch)
    first = runner.client
    runner.close()
    second = runner.client
    assert second is not first
    assert len(resources.pools) == len(resources.clients) == 2


REDIS_RUNNER_TRANSITIONS = (
    TransitionCase(
        transition_id="LAZY-CREATE",
        start_state="unopened",
        event="client is first requested",
        guard="process ID still owns the runner",
        next_state="open",
        effects="creates one owned pool and one client under the init lock",
        expected_result="the client is returned",
        payload=_runner_lazy_create,
    ),
    TransitionCase(
        transition_id="REUSE",
        start_state="open",
        event="client is requested again",
        guard="process ID still owns the runner",
        next_state="open",
        effects="preserves the owned pool and client",
        expected_result="the identical client is returned",
        payload=_runner_reuses,
    ),
    TransitionCase(
        transition_id="CONCURRENT-FIRST-ACCESS",
        start_state="unopened",
        event="several threads request the client together",
        guard="all calls share one runner",
        next_state="open",
        effects="double-checked locking creates one pool and one client",
        expected_result="every caller receives the identical client",
        payload=_runner_concurrent_first_access,
    ),
    TransitionCase(
        transition_id="CREATE-FAILURE",
        start_state="unopened",
        event="pool creation fails",
        guard="no resource was installed",
        next_state="unopened",
        effects="leaves both resource slots empty",
        expected_result="creation failure propagates",
        payload=_runner_creation_failure,
    ),
    TransitionCase(
        transition_id="CLIENT-CONSTRUCTOR-FAILURE",
        start_state="pool-created-client-unopened",
        event="Redis client construction raises",
        guard="the new pool has not been published as runner state",
        next_state="unopened",
        effects="disconnects the partial pool and clears both resource slots",
        expected_result="the failure propagates and the next access creates a fresh pair",
        payload=_runner_client_creation_failure,
    ),
    TransitionCase(
        transition_id="FORK-RESET",
        start_state="open in parent process",
        event="child process first requests the client",
        guard="stored process ID differs",
        next_state="open in child process",
        effects="abandons inherited handles, updates PID, and creates new owned handles",
        expected_result="child receives a distinct client and pool",
        payload=_runner_fork_reset,
    ),
    TransitionCase(
        transition_id="CLOSE-OWNED",
        start_state="open",
        event="close is requested",
        guard="runner owns a client and pool",
        next_state="closed",
        effects="clears slots before closing the client and disconnecting the pool",
        expected_result="both resources close once",
        payload=_runner_closes_owned_resources,
    ),
    TransitionCase(
        transition_id="CLOSE-IDEMPOTENT",
        start_state="closed",
        event="close repeats",
        guard="resource slots are empty",
        next_state="closed",
        effects="does not close old resources again",
        expected_result="repeated close succeeds",
        payload=_runner_close_is_idempotent,
    ),
    TransitionCase(
        transition_id="REOPEN-AFTER-CLOSE",
        start_state="closed",
        event="client is requested",
        guard="runner remains usable",
        next_state="open",
        effects="creates a new owned client and pool",
        expected_result="new resources replace the closed generation",
        payload=_runner_recreates_after_close,
    ),
)


@fires_transition_table("SM-REDIS-RUNNER", REDIS_RUNNER_TRANSITIONS)
def test_redis_runner_fires_transition_table(
    transition_case: TransitionCase[Callable[[pytest.MonkeyPatch], None]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transition_case.payload(monkeypatch)


@dataclass(frozen=True, slots=True)
class _WriteProtocolScenario:
    responses: tuple[tuple[object, ...] | BaseException, ...] = ()
    reservation_error: bool = False
    keep_newest: int | None = None
    expected_error: str | None = None
    expected_conflicts: int = 0
    expected_sleeps: int = 0
    expected_resyncs: int = 0
    expected_refreshes: int = 0


def _write_script_protocol(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
    scenario: _WriteProtocolScenario,
) -> None:
    core = RedisBrokerCore(redis_runner)
    responses = list(scenario.responses)
    eval_calls: list[tuple[object, ...]] = []
    reserve_calls: list[int] = []
    sleeps: list[float] = []
    resyncs: list[None] = []
    refreshes: list[None] = []
    publishes: list[str | None] = []

    def reserve(count: int) -> list[int]:
        assert count == 1
        if scenario.reservation_error:
            try:
                raise OperationalError("reservation transport failed")
            except OperationalError as cause:
                raise TimestampError("reservation failed") from cause
        candidate = 100 + len(reserve_calls)
        reserve_calls.append(candidate)
        return [candidate]

    def evaluate(script: str, *args: object) -> object:
        assert script == scripts.WRITE_MESSAGE
        eval_calls.append(args)
        response = responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    monkeypatch.setattr(core._timestamp_gen, "_reserve_candidates", reserve)
    monkeypatch.setattr(core._client, "eval", evaluate)
    monkeypatch.setattr(core, "_maybe_recover_stale_batches", lambda: None)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    monkeypatch.setattr(
        core, "_resync_timestamp_generator", lambda: resyncs.append(None)
    )

    def refresh_last_ts() -> int:
        refreshes.append(None)
        return 0

    monkeypatch.setattr(core._timestamp_gen, "refresh_last_ts", refresh_last_ts)
    monkeypatch.setattr(core, "_publish", publishes.append)

    expectation = (
        pytest.raises(Exception, match=scenario.expected_error)
        if scenario.expected_error
        else nullcontext()
    )
    try:
        with expectation:
            result = core.write(
                "jobs",
                "message",
                keep_newest=scenario.keep_newest,
            )
            assert result == reserve_calls[-1]
        assert len(eval_calls) == len(scenario.responses)
        assert len(reserve_calls) == (
            0 if scenario.reservation_error else len(scenario.responses)
        )
        assert core._ts_conflict_count == scenario.expected_conflicts
        assert len(sleeps) == scenario.expected_sleeps
        assert len(resyncs) == scenario.expected_resyncs
        assert len(refreshes) == scenario.expected_refreshes
        assert publishes == ([] if scenario.expected_error else ["jobs"])
        for call, candidate in zip(eval_calls, reserve_calls, strict=True):
            expected_tail = (
                "jobs",
                str(candidate),
                encode_id(candidate),
                "message",
            )
            if scenario.keep_newest is not None:
                expected_tail = (*expected_tail, str(scenario.keep_newest))
            assert call[-len(expected_tail) :] == expected_tail
    finally:
        core.close()


def _write_case(
    scenario: _WriteProtocolScenario,
) -> Callable[[RedisRunner, pytest.MonkeyPatch], None]:
    def run(redis_runner: RedisRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _write_script_protocol(redis_runner, monkeypatch, scenario)

    return run


REDIS_WRITE_TRANSITIONS = (
    TransitionCase(
        transition_id="LUA-SUCCESS",
        start_state="candidate reserved locally",
        event="Lua returns 1",
        guard="namespace, stale fence, and duplicate preflight pass",
        next_state="complete",
        effects="publishes the post-commit queue activity hint",
        expected_result="returns the committed candidate",
        payload=_write_case(_WriteProtocolScenario(responses=((1,),))),
    ),
    TransitionCase(
        transition_id="EXISTING-ID-SLEEP",
        start_state="executing first Lua attempt",
        event="Lua returns -1 then succeeds",
        guard="one conflict retry remains before resync",
        next_state="complete",
        effects="records conflict, sleeps, reserves again, and retries",
        expected_result="returns the second candidate",
        payload=_write_case(
            _WriteProtocolScenario(
                responses=((-1,), (1,)),
                expected_conflicts=1,
                expected_sleeps=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="EXISTING-ID-RESYNC",
        start_state="executing after one ID conflict",
        event="Lua returns -1 again then succeeds",
        guard="the shared conflict budget permits a final attempt",
        next_state="complete",
        effects="resynchronizes monotonically, reserves again, and retries",
        expected_result="returns the third candidate",
        payload=_write_case(
            _WriteProtocolScenario(
                responses=((-1,), (-1,), (1,)),
                expected_conflicts=2,
                expected_sleeps=1,
                expected_resyncs=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="EXISTING-ID-TERMINAL",
        start_state="executing after two ID conflicts",
        event="Lua returns -1 a third time",
        guard="the shared conflict budget is exhausted",
        next_state="failed",
        effects="records the third conflict without another retry",
        expected_result="RuntimeError reports repeated conflicts",
        payload=_write_case(
            _WriteProtocolScenario(
                responses=((-1,), (-1,), (-1,)),
                expected_error="repeated timestamp conflicts",
                expected_conflicts=3,
                expected_sleeps=1,
                expected_resyncs=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="STALE-FENCE-REFRESH",
        start_state="executing first Lua attempt",
        event="Lua returns -6 then succeeds",
        guard="another writer advanced persisted high-water",
        next_state="complete",
        effects="refreshes persisted state, reserves again, and retries",
        expected_result="returns the fresh second candidate",
        payload=_write_case(
            _WriteProtocolScenario(
                responses=((-6,), (1,)),
                expected_conflicts=1,
                expected_refreshes=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="KEEP-STALE-FENCE-REFRESH",
        start_state="executing first keep-write Lua attempt",
        event="Lua returns -6 then succeeds",
        guard="another writer advanced persisted high-water",
        next_state="complete",
        effects="retries the complete write-and-claim unit with a fresh candidate",
        expected_result="returns only the surviving attempt's candidate",
        payload=_write_case(
            _WriteProtocolScenario(
                responses=((-6,), (1,)),
                keep_newest=2,
                expected_conflicts=1,
                expected_refreshes=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="STALE-FENCE-SECOND",
        start_state="executing after one stale fence",
        event="Lua returns -6 again then succeeds",
        guard="the shared conflict budget permits a final attempt",
        next_state="complete",
        effects="refreshes a second time and retries",
        expected_result="returns the third candidate",
        payload=_write_case(
            _WriteProtocolScenario(
                responses=((-6,), (-6,), (1,)),
                expected_conflicts=2,
                expected_refreshes=2,
            )
        ),
    ),
    TransitionCase(
        transition_id="MIXED-CONFLICT-TERMINAL",
        start_state="executing after one ID conflict and one stale fence",
        event="Lua returns -1 as the third shared conflict",
        guard="both result codes consume one common budget",
        next_state="failed",
        effects="stops without another resync or reservation",
        expected_result="RuntimeError reports repeated conflicts",
        payload=_write_case(
            _WriteProtocolScenario(
                responses=((-1,), (-6,), (-1,)),
                expected_error="repeated timestamp conflicts",
                expected_conflicts=3,
                expected_sleeps=1,
                expected_refreshes=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="NAMESPACE-MISSING",
        start_state="executing Lua",
        event="Lua returns -2",
        guard="namespace metadata is absent",
        next_state="failed",
        effects="does not retry or publish",
        expected_result="OperationalError reports the missing namespace",
        payload=_write_case(
            _WriteProtocolScenario(
                responses=((-2,),),
                expected_error="namespace is not initialized",
            )
        ),
    ),
    TransitionCase(
        transition_id="KEEP-INVALID-INTERNAL",
        start_state="executing keep-write Lua",
        event="Lua returns -5",
        guard="Python and Lua keep contracts disagree",
        next_state="failed",
        effects="does not retry or publish",
        expected_result="OperationalError reports the invalid internal keep value",
        payload=_write_case(
            _WriteProtocolScenario(
                responses=((-5,),),
                keep_newest=2,
                expected_error="invalid keep value",
            )
        ),
    ),
    TransitionCase(
        transition_id="KEEP-RESERVATION-CONFLICT",
        start_state="executing keep-write Lua",
        event="Lua returns -7",
        guard="a displaced pending ID belongs to an active batch",
        next_state="failed",
        effects="does not consume timestamp retry budget or publish",
        expected_result="retryable OperationalError asks the caller to retry",
        payload=_write_case(
            _WriteProtocolScenario(
                responses=((-7,),),
                keep_newest=2,
                expected_error="active at-least-once batch",
            )
        ),
    ),
    TransitionCase(
        transition_id="KEEP-MISSING-BODY",
        start_state="executing keep-write Lua",
        event="Lua returns -8",
        guard="a displaced pending ID has no body",
        next_state="failed",
        effects="does not retry or publish",
        expected_result="IntegrityError reports corrupt stored state",
        payload=_write_case(
            _WriteProtocolScenario(
                responses=((-8,),),
                keep_newest=2,
                expected_error="missing its stored body",
            )
        ),
    ),
    TransitionCase(
        transition_id="UNEXPECTED-CODE",
        start_state="executing Lua",
        event="Lua returns an unknown status",
        guard="Python and Lua protocols disagree",
        next_state="failed",
        effects="does not reinterpret or retry the status",
        expected_result="OperationalError includes the unknown code",
        payload=_write_case(
            _WriteProtocolScenario(
                responses=((99,),),
                expected_error="Unexpected Redis write result: 99",
            )
        ),
    ),
    TransitionCase(
        transition_id="EMPTY-RESPONSE",
        start_state="executing Lua",
        event="Lua returns no status element",
        guard="the response violates the script protocol",
        next_state="failed",
        effects="does not crash with an indexing error",
        expected_result="OperationalError identifies the empty response",
        payload=_write_case(
            _WriteProtocolScenario(
                responses=((),),
                expected_error="empty response",
            )
        ),
    ),
    TransitionCase(
        transition_id="RESERVATION-FAILURE",
        start_state="reserving local candidate",
        event="reservation fails from an operational cause",
        guard="Lua has not started",
        next_state="failed",
        effects="does not retry an unstarted write",
        expected_result="OperationalError preserves the cause",
        payload=_write_case(
            _WriteProtocolScenario(
                reservation_error=True,
                expected_error="reservation transport failed",
            )
        ),
    ),
    TransitionCase(
        transition_id="TRANSPORT-AMBIGUOUS",
        start_state="executing Lua",
        event="Redis transport raises without a script result",
        guard="server commit outcome is ambiguous",
        next_state="failed",
        effects="translates once and does not retry",
        expected_result="OperationalError preserves transport text",
        payload=_write_case(
            _WriteProtocolScenario(
                responses=(redis.RedisError("eval failed"),),
                expected_error="eval failed",
            )
        ),
    ),
)


@fires_transition_table("SM-REDIS-WRITE", REDIS_WRITE_TRANSITIONS)
def test_redis_write_fires_transition_table(
    transition_case: TransitionCase[Callable[[RedisRunner, pytest.MonkeyPatch], None]],
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transition_case.payload(redis_runner, monkeypatch)


@dataclass(frozen=True, slots=True)
class _BroadcastProtocolScenario:
    responses: tuple[tuple[object, ...] | BaseException, ...]
    exact: bool = True
    expected_count: int | None = None
    expected_error: str | None = None
    expected_conflicts: int = 0
    expected_sleeps: int = 0
    expected_resyncs: int = 0
    expected_refreshes: int = 0


def _broadcast_guard(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
    *,
    kwargs: dict[str, object],
    error_type: type[BaseException],
    match: str,
) -> None:
    del monkeypatch
    core = RedisBrokerCore(redis_runner)
    try:
        with pytest.raises(error_type, match=match):
            core.broadcast("message", **kwargs)  # type: ignore[arg-type]
    finally:
        core.close()


def _broadcast_zero_exact(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del monkeypatch
    core = RedisBrokerCore(redis_runner)
    try:
        before = redis_runner.backend_plugin.read_last_ts(redis_runner)
        assert core.broadcast("message", queue_names=[]) == 0
        assert redis_runner.backend_plugin.read_last_ts(redis_runner) == before
    finally:
        core.close()


def _broadcast_all_no_targets(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del monkeypatch
    core = RedisBrokerCore(redis_runner)
    try:
        before = redis_runner.backend_plugin.read_last_ts(redis_runner)
        assert core.broadcast("message") == 0
        assert redis_runner.backend_plugin.read_last_ts(redis_runner) == before
    finally:
        core.close()


def _broadcast_all_targets(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del monkeypatch
    core = RedisBrokerCore(redis_runner)
    try:
        core.write("alpha", "seed")
        core.write("beta", "seed")
        assert core.broadcast("announcement") == 2
        assert core.peek_many("alpha", limit=10, with_timestamps=False)[-1] == (
            "announcement"
        )
        assert core.peek_many("beta", limit=10, with_timestamps=False)[-1] == (
            "announcement"
        )
    finally:
        core.close()


def _broadcast_exact_filters_missing(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del monkeypatch
    core = RedisBrokerCore(redis_runner)
    try:
        core.write("jobs", "seed")
        assert (
            core.broadcast(
                "announcement",
                queue_names=("jobs", "missing", "jobs"),
            )
            == 1
        )
        assert core.peek_many("jobs", limit=10, with_timestamps=False) == [
            "seed",
            "announcement",
        ]
        assert not core.queue_exists("missing")
    finally:
        core.close()


def _broadcast_exact_creates_missing(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del monkeypatch
    core = RedisBrokerCore(redis_runner)
    try:
        assert (
            core.broadcast(
                "announcement",
                queue_names=("jobs", "other"),
                create_missing=True,
            )
            == 2
        )
        assert core.peek_one("jobs", with_timestamps=False) == "announcement"
        assert core.peek_one("other", with_timestamps=False) == "announcement"
    finally:
        core.close()


def _broadcast_pattern_no_match(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del monkeypatch
    core = RedisBrokerCore(redis_runner)
    try:
        core.write("jobs", "seed")
        before = redis_runner.backend_plugin.read_last_ts(redis_runner)
        assert core.broadcast("announcement", pattern="missing*") == 0
        assert redis_runner.backend_plugin.read_last_ts(redis_runner) == before
    finally:
        core.close()


def _broadcast_pattern_matches(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del monkeypatch
    core = RedisBrokerCore(redis_runner)
    try:
        core.write("jobs-a", "seed")
        core.write("jobs-b", "seed")
        core.write("other", "seed")
        assert core.broadcast("announcement", pattern="jobs-*") == 2
        assert core.peek_many("jobs-a", limit=10, with_timestamps=False) == [
            "seed",
            "announcement",
        ]
        assert core.peek_many("jobs-b", limit=10, with_timestamps=False) == [
            "seed",
            "announcement",
        ]
        assert core.peek_many("other", limit=10, with_timestamps=False) == ["seed"]
    finally:
        core.close()


def _broadcast_empty_pattern_uses_all_selector(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del monkeypatch
    core = RedisBrokerCore(redis_runner)
    try:
        core.write("alpha", "seed")
        core.write("beta", "seed")
        assert core.broadcast("announcement", pattern="") == 2
        assert core.peek_many("alpha", limit=10, with_timestamps=False)[-1] == (
            "announcement"
        )
        assert core.peek_many("beta", limit=10, with_timestamps=False)[-1] == (
            "announcement"
        )
    finally:
        core.close()


def _broadcast_pattern_misses_queue_created_after_snapshot(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = RedisBrokerCore(redis_runner)
    writer = RedisBrokerCore(redis_runner)
    original_queue_names = core._queue_names
    snapshot_taken = False

    def snapshot_then_create() -> set[str]:
        nonlocal snapshot_taken
        queues = original_queue_names()
        if not snapshot_taken:
            snapshot_taken = True
            writer.write("jobs-new", "new-seed")
        return queues

    monkeypatch.setattr(core, "_queue_names", snapshot_then_create)
    try:
        core.write("jobs-old", "old-seed")
        assert core.broadcast("announcement", pattern="jobs-*") == 1
        assert core.peek_many("jobs-old", limit=10, with_timestamps=False) == [
            "old-seed",
            "announcement",
        ]
        assert core.peek_many("jobs-new", limit=10, with_timestamps=False) == [
            "new-seed"
        ]
    finally:
        writer.close()
        core.close()


def _broadcast_pattern_recreates_queue_deleted_after_snapshot(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = RedisBrokerCore(redis_runner)
    deleting_core = RedisBrokerCore(redis_runner)
    core.write("jobs", "seed")
    original_generate = core.generate_timestamp
    deleted = False

    def delete_then_generate() -> int:
        nonlocal deleted
        if not deleted:
            deleted = True
            assert deleting_core.delete("jobs") == 1
        return original_generate()

    monkeypatch.setattr(core, "generate_timestamp", delete_then_generate)
    try:
        assert core.broadcast("announcement", pattern="job*") == 1
        assert core.queue_exists("jobs")
        assert core.peek_many("jobs", limit=10, with_timestamps=False) == [
            "announcement"
        ]
    finally:
        deleting_core.close()
        core.close()


def _broadcast_candidate_reservation_fails(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = RedisBrokerCore(redis_runner)
    before = redis_runner.backend_plugin.read_last_ts(redis_runner)
    cause = OperationalError("candidate store unavailable")
    failure = TimestampError("candidate reservation failed")
    failure.__cause__ = cause
    monkeypatch.setattr(
        core._timestamp_gen,
        "_reserve_candidates",
        lambda count: (_ for _ in ()).throw(failure),
    )
    try:
        with pytest.raises(OperationalError, match="candidate store unavailable"):
            core.broadcast(
                "announcement",
                queue_names=("jobs",),
                create_missing=True,
            )
        assert not core.queue_exists("jobs")
        assert redis_runner.backend_plugin.read_last_ts(redis_runner) == before
    finally:
        core.close()


def _broadcast_all_lua_rejects_stale_candidates_without_mutation(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del monkeypatch
    core = RedisBrokerCore(redis_runner)
    advancing_core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    try:
        core.write("jobs", "seed")
        candidates = core._timestamp_gen._reserve_candidates(1)
        advanced = advancing_core.generate_timestamp()
        while advanced <= candidates[-1]:
            advanced = advancing_core.generate_timestamp()

        before_last_ts = redis_runner.backend_plugin.read_last_ts(redis_runner)
        before_bodies = core._client.hlen(keys.bodies)
        before_all_ids = core._client.zcard(keys.all_ids)
        before_pending = core._client.zcard(keys.pending("jobs"))

        result = core._client.eval(
            scripts.BROADCAST_MESSAGE,
            4,
            keys.meta,
            keys.bodies,
            keys.all_ids,
            keys.queues,
            str(candidates[-1]),
            encode_id(candidates[-1]),
            "1",
            "announcement",
            keys.key("q", ""),
            "all",
            "0",
            encode_id(candidates[0]),
        )

        assert result == [-6]
        assert redis_runner.backend_plugin.read_last_ts(redis_runner) == before_last_ts
        assert core._client.hlen(keys.bodies) == before_bodies
        assert core._client.zcard(keys.all_ids) == before_all_ids
        assert core._client.zcard(keys.pending("jobs")) == before_pending
        assert core.peek_many("jobs", limit=10, with_timestamps=False) == ["seed"]
    finally:
        advancing_core.close()
        core.close()


def _broadcast_all_refreshes_after_external_advance(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = RedisBrokerCore(redis_runner)
    advancing_core = RedisBrokerCore(redis_runner)
    core.write("jobs", "seed")
    original_reserve = core._timestamp_gen._reserve_candidates
    reservations = 0

    def reserve_then_advance(count: int) -> list[int]:
        nonlocal reservations
        reservations += 1
        candidates = original_reserve(count)
        if reservations == 1:
            advanced = advancing_core.generate_timestamp()
            while advanced <= candidates[-1]:
                advanced = advancing_core.generate_timestamp()
        return candidates

    monkeypatch.setattr(
        core._timestamp_gen,
        "_reserve_candidates",
        reserve_then_advance,
    )
    try:
        assert core.broadcast("announcement") == 1
        assert reservations == 2
        assert core._ts_conflict_count == 1
        assert core.peek_many("jobs", limit=10, with_timestamps=False) == [
            "seed",
            "announcement",
        ]
    finally:
        advancing_core.close()
        core.close()


@dataclass(frozen=True, slots=True)
class _PatternProtocolScenario:
    conflicts: int = 0
    timestamp_error: bool = False
    expected_error: str | None = None
    expected_sleeps: int = 0
    expected_resyncs: int = 0


def _broadcast_pattern_protocol(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
    scenario: _PatternProtocolScenario,
) -> None:
    core = RedisBrokerCore(redis_runner)
    core.write("jobs", "seed")
    original_insert = core.insert_messages
    insert_attempts = 0
    sleeps: list[float] = []
    resyncs: list[None] = []

    def insert_with_conflicts(records: object) -> None:
        nonlocal insert_attempts
        insert_attempts += 1
        if insert_attempts <= scenario.conflicts:
            raise IntegrityError(f"pattern conflict {insert_attempts}")
        original_insert(records)  # type: ignore[arg-type]

    monkeypatch.setattr(core, "insert_messages", insert_with_conflicts)
    monkeypatch.setattr(
        redis_plugin_module.time,
        "sleep",
        lambda seconds: sleeps.append(seconds),
    )
    monkeypatch.setattr(
        core,
        "_resync_timestamp_generator",
        lambda: resyncs.append(None),
    )
    if scenario.timestamp_error:
        monkeypatch.setattr(
            core,
            "generate_timestamp",
            lambda: (_ for _ in ()).throw(TimestampError("clock failed")),
        )

    expectation = (
        pytest.raises(
            (RuntimeError, TimestampError),
            match=scenario.expected_error,
        )
        if scenario.expected_error
        else nullcontext()
    )
    try:
        with expectation:
            assert core.broadcast("announcement", pattern="job*") == 1
        assert insert_attempts == (
            0 if scenario.timestamp_error else min(scenario.conflicts + 1, 3)
        )
        assert core._ts_conflict_count == scenario.conflicts
        assert len(sleeps) == scenario.expected_sleeps
        assert len(resyncs) == scenario.expected_resyncs
    finally:
        core.close()


def _pattern_case(
    scenario: _PatternProtocolScenario,
) -> Callable[[RedisRunner, pytest.MonkeyPatch], None]:
    def run(redis_runner: RedisRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _broadcast_pattern_protocol(redis_runner, monkeypatch, scenario)

    return run


def _broadcast_script_protocol(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
    scenario: _BroadcastProtocolScenario,
) -> None:
    core = RedisBrokerCore(redis_runner)
    responses = iter(scenario.responses)
    eval_calls = 0
    sleeps: list[float] = []
    resyncs: list[None] = []
    refreshes: list[None] = []
    success_refreshes: list[None] = []

    def eval_script(*args: object, **kwargs: object) -> list[object]:
        nonlocal eval_calls
        del args, kwargs
        eval_calls += 1
        response = next(responses)
        if isinstance(response, BaseException):
            raise response
        return list(response)

    monkeypatch.setattr(core._client, "eval", eval_script)
    monkeypatch.setattr(
        redis_plugin_module.time,
        "sleep",
        lambda seconds: sleeps.append(seconds),
    )
    monkeypatch.setattr(
        core,
        "_resync_timestamp_generator",
        lambda: resyncs.append(None),
    )
    monkeypatch.setattr(
        core._timestamp_gen,
        "refresh_last_ts",
        lambda: refreshes.append(None),
    )
    monkeypatch.setattr(
        core,
        "refresh_last_timestamp",
        lambda: success_refreshes.append(None),
    )
    call_kwargs = (
        {"queue_names": ("jobs",), "create_missing": True} if scenario.exact else {}
    )
    expectation = (
        pytest.raises(
            (OperationalError, RuntimeError),
            match=scenario.expected_error,
        )
        if scenario.expected_error
        else nullcontext()
    )
    try:
        with expectation:
            result = core.broadcast("message", **call_kwargs)  # type: ignore[arg-type]
            assert result == scenario.expected_count
        assert eval_calls == len(scenario.responses)
        assert core._ts_conflict_count == scenario.expected_conflicts
        assert len(sleeps) == scenario.expected_sleeps
        assert len(resyncs) == scenario.expected_resyncs
        assert len(refreshes) == scenario.expected_refreshes
        assert len(success_refreshes) == (scenario.expected_count is not None)
    finally:
        core.close()


def _protocol_case(
    scenario: _BroadcastProtocolScenario,
) -> Callable[[RedisRunner, pytest.MonkeyPatch], None]:
    def run(redis_runner: RedisRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _broadcast_script_protocol(redis_runner, monkeypatch, scenario)

    return run


def _guard_case(
    kwargs: dict[str, object],
    error_type: type[BaseException],
    match: str,
) -> Callable[[RedisRunner, pytest.MonkeyPatch], None]:
    def run(redis_runner: RedisRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _broadcast_guard(
            redis_runner,
            monkeypatch,
            kwargs=kwargs,
            error_type=error_type,
            match=match,
        )

    return run


REDIS_BROADCAST_TRANSITIONS = (
    TransitionCase(
        transition_id="REJECT-MIXED-SELECTORS",
        start_state="validating",
        event="pattern and exact queues are both supplied",
        guard="selectors are mutually exclusive",
        next_state="rejected",
        effects="does not reserve timestamps or invoke Lua",
        expected_result="ValueError identifies the conflicting selectors",
        payload=_guard_case(
            {"pattern": "*", "queue_names": ("jobs",)},
            ValueError,
            "cannot be used together",
        ),
    ),
    TransitionCase(
        transition_id="REJECT-CREATE-MISSING-TYPE",
        start_state="validating",
        event="create_missing is not boolean",
        guard="option type is invalid",
        next_state="rejected",
        effects="does not reserve timestamps or invoke Lua",
        expected_result="TypeError identifies create_missing",
        payload=_guard_case(
            {"create_missing": 1},
            TypeError,
            "must be a boolean",
        ),
    ),
    TransitionCase(
        transition_id="REJECT-CREATE-WITHOUT-QUEUES",
        start_state="validating",
        event="create_missing is requested without exact queues",
        guard="exact-create needs an explicit target set",
        next_state="rejected",
        effects="does not reserve timestamps or invoke Lua",
        expected_result="ValueError identifies the missing queues",
        payload=_guard_case(
            {"create_missing": True},
            ValueError,
            "requires queue_names",
        ),
    ),
    TransitionCase(
        transition_id="REJECT-STRING-QUEUE-SEQUENCE",
        start_state="validating",
        event="queue_names is one string",
        guard="a string is not a queue sequence",
        next_state="rejected",
        effects="does not split the string into names",
        expected_result="TypeError identifies the sequence contract",
        payload=_guard_case(
            {"queue_names": "jobs"},
            TypeError,
            "not a string",
        ),
    ),
    TransitionCase(
        transition_id="REJECT-INVALID-QUEUE",
        start_state="validating",
        event="an exact queue name is invalid",
        guard="queue validation precedes timestamp reservation",
        next_state="rejected",
        effects="does not invoke Lua",
        expected_result="QueueNameError reaches the caller",
        payload=_guard_case(
            {"queue_names": ("bad queue",)},
            QueueNameError,
            "Invalid queue name",
        ),
    ),
    TransitionCase(
        transition_id="EMPTY-EXACT",
        start_state="validating",
        event="empty exact target set is supplied",
        guard="there are no target queues",
        next_state="complete-no-op",
        effects="does not advance the timestamp high-water mark",
        expected_result="returns zero",
        payload=_broadcast_zero_exact,
    ),
    TransitionCase(
        transition_id="ALL-NO-TARGETS",
        start_state="selecting-all",
        event="Lua selects no registered queues",
        guard="namespace is initialized but has no queues",
        next_state="complete-no-op",
        effects="does not advance metadata or publish activity",
        expected_result="returns zero",
        payload=_broadcast_all_no_targets,
    ),
    TransitionCase(
        transition_id="ALL-SUCCESS",
        start_state="selecting-all",
        event="Lua selects every registered queue",
        guard="timestamp capacity covers the atomic target set",
        next_state="complete",
        effects="inserts one message per queue and publishes queue activity",
        expected_result="returns the exact affected count",
        payload=_broadcast_all_targets,
    ),
    TransitionCase(
        transition_id="EXACT-FILTER-MISSING",
        start_state="selecting-exact",
        event="requested set includes existing and missing queues",
        guard="create_missing is false",
        next_state="complete",
        effects="inserts only into queues registered at the atomic point",
        expected_result="returns one without resurrecting the missing queue",
        payload=_broadcast_exact_filters_missing,
    ),
    TransitionCase(
        transition_id="EXACT-CREATE-SUCCESS",
        start_state="selecting-exact-create",
        event="requested queues are missing",
        guard="create_missing is true",
        next_state="complete",
        effects="atomically creates targets and inserts their messages",
        expected_result="returns two and both queues are readable",
        payload=_broadcast_exact_creates_missing,
    ),
    TransitionCase(
        transition_id="PATTERN-NO-MATCH",
        start_state="selecting-pattern",
        event="pattern matches no registered queue",
        guard="queue snapshot is empty",
        next_state="complete-no-op",
        effects="does not allocate timestamps",
        expected_result="returns zero",
        payload=_broadcast_pattern_no_match,
    ),
    TransitionCase(
        transition_id="EMPTY-PATTERN-USES-ALL",
        start_state="validating-selectors",
        event="an empty pattern string is supplied",
        guard="empty pattern is the selector-free spelling",
        next_state="selecting-all",
        effects="uses atomic Lua target selection rather than a Python pattern snapshot",
        expected_result="every queue receives the broadcast",
        payload=_broadcast_empty_pattern_uses_all_selector,
    ),
    TransitionCase(
        transition_id="PATTERN-SUCCESS",
        start_state="selecting-pattern",
        event="pattern matches two registered queues",
        guard="Python snapshot is stable through insertion",
        next_state="complete",
        effects="uses the normal multi-insert owner for matched queues",
        expected_result="returns two and leaves unmatched queues unchanged",
        payload=_broadcast_pattern_matches,
    ),
    TransitionCase(
        transition_id="PATTERN-CREATED-AFTER-SNAPSHOT",
        start_state="pattern-snapshot-captured",
        event="a matching queue is created before insertion",
        guard="pattern target selection already completed in Python",
        next_state="complete",
        effects="does not add the newly created queue to the captured target set",
        expected_result="only the queue present in the snapshot receives the message",
        payload=_broadcast_pattern_misses_queue_created_after_snapshot,
    ),
    TransitionCase(
        transition_id="PATTERN-DELETED-AFTER-SNAPSHOT",
        start_state="pattern-snapshot-captured",
        event="a selected queue is deleted before insertion",
        guard="normal multi-insert recreates queues named by its input records",
        next_state="complete-with-recreation",
        effects="recreates the deleted queue with only the broadcast message",
        expected_result="the documented non-atomic pattern behavior remains visible",
        payload=_broadcast_pattern_recreates_queue_deleted_after_snapshot,
    ),
    TransitionCase(
        transition_id="CANDIDATE-RESERVATION-FAILURE",
        start_state="reserving-atomic-candidates",
        event="local candidate reservation fails from an operational cause",
        guard="Lua has not started",
        next_state="failed",
        effects="does not create targets, persist metadata, or invoke atomic insertion",
        expected_result="OperationalError preserves the reservation cause",
        payload=_broadcast_candidate_reservation_fails,
    ),
    TransitionCase(
        transition_id="PATTERN-CONFLICT-SLEEP",
        start_state="inserting-pattern-snapshot",
        event="the first multi-insert conflicts",
        guard="the pattern retry budget allows another attempt",
        next_state="complete",
        effects="records the conflict, sleeps briefly, regenerates IDs, and retries",
        expected_result="the second insert succeeds",
        payload=_pattern_case(
            _PatternProtocolScenario(
                conflicts=1,
                expected_sleeps=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="PATTERN-CONFLICT-RESYNC",
        start_state="inserting-pattern-snapshot after one conflict",
        event="the second multi-insert conflicts",
        guard="the final retry remains",
        next_state="complete",
        effects="resynchronizes timestamp state, regenerates IDs, and retries",
        expected_result="the third insert succeeds",
        payload=_pattern_case(
            _PatternProtocolScenario(
                conflicts=2,
                expected_sleeps=1,
                expected_resyncs=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="PATTERN-CONFLICT-TERMINAL",
        start_state="inserting-pattern-snapshot after two conflicts",
        event="the third multi-insert conflicts",
        guard="the retry budget is exhausted",
        next_state="failed",
        effects="records the third conflict and stops",
        expected_result="RuntimeError reports repeated conflicts",
        payload=_pattern_case(
            _PatternProtocolScenario(
                conflicts=3,
                expected_error="repeated timestamp conflicts",
                expected_sleeps=1,
                expected_resyncs=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="PATTERN-TIMESTAMP-FAILURE",
        start_state="allocating-pattern-timestamps",
        event="timestamp generation fails",
        guard="no insertion attempt has started",
        next_state="failed",
        effects="does not retry or mutate queue messages",
        expected_result="TimestampError reaches the caller",
        payload=_pattern_case(
            _PatternProtocolScenario(
                timestamp_error=True,
                expected_error="clock failed",
            )
        ),
    ),
    TransitionCase(
        transition_id="ALL-STALE-FENCE-LUA-NO-MUTATION",
        start_state="executing-lua with stale all-selector candidates",
        event="another core advances persisted last_ts before Lua executes",
        guard="the first reserved candidate is not newer than persisted metadata",
        next_state="retry-required",
        effects="returns -6 without changing metadata, bodies, indexes, or queue data",
        expected_result="real Valkey state is byte-for-byte logically unchanged",
        payload=_broadcast_all_lua_rejects_stale_candidates_without_mutation,
    ),
    TransitionCase(
        transition_id="ALL-STALE-FENCE-PUBLIC-RETRY",
        start_state="executing public all-selector broadcast",
        event="another core advances persisted last_ts after local reservation",
        guard="the refreshed retry budget is available",
        next_state="complete",
        effects="refreshes local timestamp state, reserves again, and retries Lua",
        expected_result="the public broadcast succeeds on its second reservation",
        payload=_broadcast_all_refreshes_after_external_advance,
    ),
    TransitionCase(
        transition_id="LUA-SUCCESS",
        start_state="executing-lua",
        event="Lua returns code 1 and affected queues",
        guard="script protocol is well formed",
        next_state="complete",
        effects="refreshes the timestamp and publishes affected queue activity",
        expected_result="returns affected queue count",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((1, "jobs"),),
                expected_count=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="CAPACITY-GROW-FIRST",
        start_state="executing-lua with initial all-target capacity",
        event="Lua returns -4 then succeeds",
        guard="atomic queue set outgrew the first reservation",
        next_state="complete",
        effects="grows capacity and retries without a conflict penalty",
        expected_result="second Lua attempt succeeds",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-4, 20), (1,)),
                exact=False,
                expected_count=0,
            )
        ),
    ),
    TransitionCase(
        transition_id="CAPACITY-GROW-SECOND",
        start_state="executing-lua after one capacity growth",
        event="Lua returns -4 again then succeeds",
        guard="atomic queue set outgrew two reservations",
        next_state="complete",
        effects="performs the second allowed growth and retries",
        expected_result="third Lua attempt succeeds",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-4, 20), (-4, 40), (1,)),
                exact=False,
                expected_count=0,
            )
        ),
    ),
    TransitionCase(
        transition_id="CAPACITY-FAIL-THIRD",
        start_state="executing-lua after two capacity growths",
        event="Lua returns -4 a third time",
        guard="growth retry budget is exhausted",
        next_state="failed",
        effects="stops retrying",
        expected_result="RuntimeError reports repeated queue growth",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-4, 20), (-4, 40), (-4, 80)),
                exact=False,
                expected_error="repeated queue growth",
            )
        ),
    ),
    TransitionCase(
        transition_id="CAPACITY-EXACT-IMPOSSIBLE",
        start_state="executing-lua exact selector",
        event="Lua returns -4",
        guard="exact capacity must equal requested count",
        next_state="failed",
        effects="does not treat an impossible protocol result as retryable",
        expected_result="OperationalError reports the selector mismatch",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-4, 2),),
                expected_error="capacity result for exact selector",
            )
        ),
    ),
    TransitionCase(
        transition_id="CONFLICT-EXISTING-FIRST",
        start_state="executing-lua",
        event="Lua returns -1 then succeeds",
        guard="first generated ID conflicts with stored data",
        next_state="complete",
        effects="records conflict, sleeps briefly, and retries",
        expected_result="second attempt succeeds",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-1,), (1, "jobs")),
                expected_count=1,
                expected_conflicts=1,
                expected_sleeps=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="CONFLICT-DUPLICATE-FIRST",
        start_state="executing-lua",
        event="Lua returns -3 then succeeds",
        guard="first candidate batch contains a duplicate ID",
        next_state="complete",
        effects="records conflict, sleeps briefly, and retries",
        expected_result="second attempt succeeds",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-3,), (1, "jobs")),
                expected_count=1,
                expected_conflicts=1,
                expected_sleeps=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="CONFLICT-EXISTING-RESYNC-SECOND",
        start_state="executing-lua after one timestamp conflict",
        event="Lua returns -1 a second time then succeeds",
        guard="conflict retry budget still allows resynchronization",
        next_state="complete",
        effects="resynchronizes the generator and retries",
        expected_result="third attempt succeeds",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-1,), (-1,), (1, "jobs")),
                expected_count=1,
                expected_conflicts=2,
                expected_sleeps=1,
                expected_resyncs=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="CONFLICT-DUPLICATE-RESYNC-SECOND",
        start_state="executing-lua after one timestamp conflict",
        event="Lua returns -3 a second time then succeeds",
        guard="conflict retry budget still allows resynchronization",
        next_state="complete",
        effects="resynchronizes the generator and retries",
        expected_result="third attempt succeeds",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-3,), (-3,), (1, "jobs")),
                expected_count=1,
                expected_conflicts=2,
                expected_sleeps=1,
                expected_resyncs=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="CONFLICT-EXISTING-FAIL-THIRD",
        start_state="executing-lua after two timestamp conflicts",
        event="Lua returns -1 a third time",
        guard="conflict retry budget is exhausted",
        next_state="failed",
        effects="records the conflict and stops retrying",
        expected_result="RuntimeError reports repeated conflicts",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-1,), (-1,), (-1,)),
                expected_error="repeated timestamp conflicts",
                expected_conflicts=3,
                expected_sleeps=1,
                expected_resyncs=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="CONFLICT-DUPLICATE-FAIL-THIRD",
        start_state="executing-lua after two timestamp conflicts",
        event="Lua returns -3 a third time",
        guard="conflict retry budget is exhausted",
        next_state="failed",
        effects="records the conflict and stops retrying",
        expected_result="RuntimeError reports repeated conflicts",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-3,), (-3,), (-3,)),
                expected_error="repeated timestamp conflicts",
                expected_conflicts=3,
                expected_sleeps=1,
                expected_resyncs=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="STALE-FENCE-FIRST",
        start_state="executing-lua",
        event="Lua returns -6 then succeeds",
        guard="persisted high-water mark overtook the reserved exact IDs",
        next_state="complete",
        effects="refreshes timestamp state and retries",
        expected_result="second attempt succeeds",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-6,), (1, "jobs")),
                expected_count=1,
                expected_conflicts=1,
                expected_refreshes=1,
            )
        ),
    ),
    TransitionCase(
        transition_id="STALE-FENCE-SECOND",
        start_state="executing-lua after one stale fence",
        event="Lua returns -6 again then succeeds",
        guard="refresh retry budget still allows one more attempt",
        next_state="complete",
        effects="refreshes timestamp state a second time",
        expected_result="third attempt succeeds",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-6,), (-6,), (1, "jobs")),
                expected_count=1,
                expected_conflicts=2,
                expected_refreshes=2,
            )
        ),
    ),
    TransitionCase(
        transition_id="STALE-FENCE-FAIL-THIRD",
        start_state="executing-lua after two stale fences",
        event="Lua returns -6 a third time",
        guard="refresh retry budget is exhausted",
        next_state="failed",
        effects="records the conflict and stops retrying",
        expected_result="RuntimeError reports repeated conflicts",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-6,), (-6,), (-6,)),
                expected_error="repeated timestamp conflicts",
                expected_conflicts=3,
                expected_refreshes=2,
            )
        ),
    ),
    TransitionCase(
        transition_id="NAMESPACE-MISSING",
        start_state="executing-lua",
        event="Lua returns -2",
        guard="namespace metadata is absent at the atomic point",
        next_state="failed",
        effects="does not retry",
        expected_result="OperationalError reports uninitialized namespace",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-2,),),
                expected_error="namespace is not initialized",
            )
        ),
    ),
    TransitionCase(
        transition_id="MALFORMED-ARGUMENTS",
        start_state="executing-lua",
        event="Lua returns -5",
        guard="script rejects its argument shape",
        next_state="failed",
        effects="does not retry",
        expected_result="OperationalError reports malformed script arguments",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((-5,),),
                expected_error="Malformed Redis broadcast",
            )
        ),
    ),
    TransitionCase(
        transition_id="IMPOSSIBLE-CODE",
        start_state="executing-lua",
        event="Lua returns an unknown status code",
        guard="Python and Lua protocol disagree",
        next_state="failed",
        effects="does not reinterpret or retry the impossible code",
        expected_result="OperationalError includes the unknown code",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=((99,),),
                expected_error="Unexpected Redis broadcast result: 99",
            )
        ),
    ),
    TransitionCase(
        transition_id="TRANSPORT-FAILURE",
        start_state="executing-lua",
        event="Redis transport raises",
        guard="script did not return a protocol result",
        next_state="failed",
        effects="translates the external Redis error",
        expected_result="OperationalError preserves transport text",
        payload=_protocol_case(
            _BroadcastProtocolScenario(
                responses=(redis.RedisError("eval failed"),),
                expected_error="eval failed",
            )
        ),
    ),
)


@fires_transition_table("SM-REDIS-BROADCAST", REDIS_BROADCAST_TRANSITIONS)
def test_redis_broadcast_fires_transition_table(
    transition_case: TransitionCase[Callable[[RedisRunner, pytest.MonkeyPatch], None]],
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transition_case.payload(redis_runner, monkeypatch)
