"""Basic Valkey/Redis backend integration tests."""

from __future__ import annotations

import threading
import time

import pytest
import redis
from simplebroker_redis import RedisRunner, get_backend_plugin
from simplebroker_redis.core import RedisBrokerCore
from simplebroker_redis.keys import RedisKeys, encode_id
from simplebroker_redis.plugin import RedisMultiQueueActivityWaiter
from simplebroker_redis.validation import key_prefix

from simplebroker import Queue, create_activity_waiter_for_queues
from simplebroker.ext import PollingStrategy

pytestmark = [pytest.mark.redis_only]


def test_runner_queue_round_trip(redis_runner: RedisRunner) -> None:
    queue = Queue("jobs", runner=redis_runner, persistent=True)
    try:
        queue.write("hello")
        assert queue.read() == "hello"
        assert queue.read() is None
    finally:
        queue.close()


def test_plugin_core_round_trip(redis_url: str, redis_namespace: str) -> None:
    plugin = get_backend_plugin()
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    try:
        core.write("jobs", "hello")
        assert core.peek_one("jobs", with_timestamps=False) == "hello"
        assert core.claim_one("jobs", with_timestamps=False) == "hello"
    finally:
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_broadcast_is_atomic_when_generated_ids_collide(
    redis_runner: RedisRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        core.write("alpha", "seed-alpha")
        core.write("beta", "seed-beta")
        colliding_ts = core.generate_timestamp()
        monkeypatch.setattr(core, "generate_timestamp", lambda: colliding_ts)

        with pytest.raises(RuntimeError, match="timestamp conflicts"):
            core.broadcast("announcement")

        assert core.peek_many("alpha", limit=10, with_timestamps=False) == [
            "seed-alpha"
        ]
        assert core.peek_many("beta", limit=10, with_timestamps=False) == ["seed-beta"]
    finally:
        core.shutdown()


def test_broadcast_success_with_pattern(redis_runner: RedisRunner) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        core.write("alpha", "seed-alpha")
        core.write("alerts", "seed-alerts")
        core.write("beta", "seed-beta")

        assert core.broadcast("announcement", pattern="a*") == 2

        assert core.peek_many("alpha", limit=10, with_timestamps=False) == [
            "seed-alpha",
            "announcement",
        ]
        assert core.peek_many("alerts", limit=10, with_timestamps=False) == [
            "seed-alerts",
            "announcement",
        ]
        assert core.peek_many("beta", limit=10, with_timestamps=False) == ["seed-beta"]
    finally:
        core.shutdown()


def test_cleanup_preserves_colon_extended_namespace_keys(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    parent_prefix = key_prefix(redis_namespace)
    child_meta = f"{parent_prefix}:child:meta"
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    try:
        plugin.initialize_target(
            redis_url, backend_options={"namespace": redis_namespace}
        )
        client.hset(child_meta, mapping={"owner": "legacy-child"})

        assert plugin.cleanup_target(
            redis_url, backend_options={"namespace": redis_namespace}
        )

        assert not client.exists(f"{parent_prefix}:meta")
        assert client.hgetall(child_meta) == {"owner": "legacy-child"}
    finally:
        client.delete(child_meta)
        client.close()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_delete_message_ids_removes_redis_storage(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    keys = RedisKeys(redis_namespace)
    try:
        core.write("jobs", "one")
        core.write("jobs", "two")
        core.write("jobs", "three")
        timestamps = dict(core.peek_many("jobs", limit=10))

        assert (
            core.claim_one(
                "jobs", exact_timestamp=timestamps["two"], with_timestamps=False
            )
            == "two"
        )
        assert core._runner.client.hlen(keys.bodies) == 3

        deleted = core.delete_message_ids(
            "jobs", [timestamps["two"], timestamps["three"]]
        )

        assert deleted == 2
        assert core._runner.client.hlen(keys.bodies) == 1
        for timestamp in (timestamps["two"], timestamps["three"]):
            encoded = encode_id(timestamp)
            assert core._runner.client.zscore(keys.pending("jobs"), encoded) is None
            assert core._runner.client.zscore(keys.claimed("jobs"), encoded) is None
            assert core._runner.client.zscore(keys.all_ids, encoded) is None
    finally:
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_delete_from_queues_removes_redis_storage(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    keys = RedisKeys(redis_namespace)
    try:
        core.write("alpha", "alpha1")
        core.write("alpha", "alpha2")
        core.write("beta", "beta1")
        core.write("gamma", "gamma1")
        alpha_timestamps = dict(core.peek_many("alpha", limit=10))
        beta_timestamps = dict(core.peek_many("beta", limit=10))

        assert (
            core.claim_one(
                "alpha",
                exact_timestamp=alpha_timestamps["alpha1"],
                with_timestamps=False,
            )
            == "alpha1"
        )
        assert core._runner.client.hlen(keys.bodies) == 4

        deleted = core.delete_from_queues(["alpha", "beta"])

        assert deleted == 3
        assert core._runner.client.hlen(keys.bodies) == 1
        for timestamp in (
            alpha_timestamps["alpha1"],
            alpha_timestamps["alpha2"],
            beta_timestamps["beta1"],
        ):
            encoded = encode_id(timestamp)
            assert core._runner.client.hget(keys.bodies, encoded) is None
            assert core._runner.client.zscore(keys.pending("alpha"), encoded) is None
            assert core._runner.client.zscore(keys.claimed("alpha"), encoded) is None
            assert core._runner.client.zscore(keys.pending("beta"), encoded) is None
            assert core._runner.client.zscore(keys.claimed("beta"), encoded) is None
            assert core._runner.client.zscore(keys.all_ids, encoded) is None
        assert core.peek_many("gamma", limit=10, with_timestamps=False) == ["gamma1"]
    finally:
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_delete_from_queues_redis_before_timestamp_is_strict(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    keys = RedisKeys(redis_namespace)
    try:
        core.write("alpha", "old-alpha")
        core.write("beta", "old-beta")
        core.write("gamma", "old-gamma")
        core.write("alpha", "boundary-alpha")
        boundary_ts = dict(core.peek_many("alpha", limit=10))["boundary-alpha"]
        core.write("alpha", "new-alpha")
        core.write("beta", "new-beta")
        alpha_timestamps = dict(core.peek_many("alpha", limit=10))
        beta_timestamps = dict(core.peek_many("beta", limit=10))

        deleted = core.delete_from_queues(
            ["alpha", "beta"],
            before_timestamp=boundary_ts,
        )

        assert deleted == 2
        assert core.peek_many("alpha", limit=10, with_timestamps=False) == [
            "boundary-alpha",
            "new-alpha",
        ]
        assert core.peek_many("beta", limit=10, with_timestamps=False) == ["new-beta"]
        assert core.peek_many("gamma", limit=10, with_timestamps=False) == ["old-gamma"]
        for timestamp in (
            alpha_timestamps["old-alpha"],
            beta_timestamps["old-beta"],
        ):
            encoded = encode_id(timestamp)
            assert core._runner.client.hget(keys.bodies, encoded) is None
            assert core._runner.client.zscore(keys.all_ids, encoded) is None
        assert core._runner.client.hget(keys.bodies, encode_id(boundary_ts)) == (
            "boundary-alpha"
        )
    finally:
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_list_queues_redis_returns_names_and_tracks_claimed_lifecycle(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    try:
        core.write("jobs.pending", "one")
        core.write("jobs.claimed", "two")
        core.write("events", "three")

        assert core.claim_one("jobs.claimed", with_timestamps=False) == "two"

        assert core.list_queues() == ["events", "jobs.claimed", "jobs.pending"]
        assert core.list_queues(prefix="jobs.") == ["jobs.claimed", "jobs.pending"]
        assert core.list_queues(pattern="*.pending") == ["jobs.pending"]

        core.vacuum()

        assert core.list_queues() == ["events", "jobs.pending"]
    finally:
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_find_message_ids_redis_literal_search_preserves_storage(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    keys = RedisKeys(redis_namespace)
    try:
        core.write("jobs", "tenant:acme one")
        core.write("jobs", "tenant:globex one")
        core.write("jobs", "tenant:acme two")
        timestamps = dict(core.peek_many("jobs", limit=10))

        ids = core.find_message_ids(
            "jobs",
            body_contains="tenant:acme",
            limit=10,
        )

        assert ids == [
            timestamps["tenant:acme one"],
            timestamps["tenant:acme two"],
        ]
        assert core._runner.client.hlen(keys.bodies) == 3
        assert core.peek_many("jobs", limit=10, with_timestamps=False) == [
            "tenant:acme one",
            "tenant:globex one",
            "tenant:acme two",
        ]
    finally:
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_find_message_ids_redis_claimed_visibility(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    try:
        core.write("jobs", "target pending")
        core.write("jobs", "target claimed")
        timestamps = dict(core.peek_many("jobs", limit=10))

        assert (
            core.claim_one(
                "jobs",
                exact_timestamp=timestamps["target claimed"],
                with_timestamps=False,
            )
            == "target claimed"
        )

        assert core.find_message_ids("jobs", body_contains="target", limit=10) == [
            timestamps["target pending"]
        ]
        assert core.find_message_ids(
            "jobs",
            body_contains="target",
            limit=10,
            include_claimed=True,
        ) == [
            timestamps["target pending"],
            timestamps["target claimed"],
        ]
    finally:
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_find_message_ids_redis_treats_patterns_as_literals(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    try:
        core.write("jobs", "progress 100% ready")
        core.write("jobs", "progress 100x ready")
        core.write("jobs", "code a_c ready")
        core.write("jobs", "code abc ready")
        timestamps = dict(core.peek_many("jobs", limit=10))

        assert core.find_message_ids("jobs", body_contains="100%", limit=10) == [
            timestamps["progress 100% ready"]
        ]
        assert core.find_message_ids("jobs", body_contains="a_c", limit=10) == [
            timestamps["code a_c ready"]
        ]
    finally:
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_find_message_ids_redis_timestamp_bounds_are_strict(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    try:
        core.write("jobs", "target old")
        core.write("jobs", "target boundary")
        boundary_ts = dict(core.peek_many("jobs", limit=10))["target boundary"]
        core.write("jobs", "target new")
        timestamps = dict(core.peek_many("jobs", limit=10))

        assert core.find_message_ids(
            "jobs",
            body_contains="target",
            limit=10,
            before_timestamp=boundary_ts,
        ) == [timestamps["target old"]]
        assert core.find_message_ids(
            "jobs",
            body_contains="target",
            limit=10,
            after_timestamp=boundary_ts,
        ) == [timestamps["target new"]]
    finally:
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_activity_waiter_receives_queue_scoped_publish(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    waiter = plugin.create_activity_waiter(
        target=redis_url,
        backend_options={"namespace": redis_namespace},
        queue_name="jobs",
        stop_event=None,
    )
    assert waiter is not None
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    try:
        assert waiter.wait(0.05) is False
        core.write("other", "miss")
        assert waiter.wait(0.05) is False

        core.write("jobs", "hit")
        assert waiter.wait(2.0) is True
        assert core.claim_one("jobs", with_timestamps=False) == "hit"
    finally:
        waiter.close()
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_activity_waiter_preserves_multiple_queue_notifications(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    waiter = plugin.create_activity_waiter(
        target=redis_url,
        backend_options={"namespace": redis_namespace},
        queue_name="jobs",
        stop_event=None,
    )
    assert waiter is not None
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    try:
        for index in range(3):
            core.write("jobs", f"hit-{index}")

        assert waiter.wait(2.0) is True
        assert waiter.wait(2.0) is True
        assert waiter.wait(2.0) is True
        assert waiter.wait(0.05) is False
    finally:
        waiter.close()
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_polling_strategy_replaces_redis_waiter_for_dynamic_queue_set(
    redis_runner: RedisRunner,
) -> None:
    """The strategy should replace fixed Redis registrations before cleanup."""
    queue_a = Queue("alpha", runner=redis_runner, persistent=True)
    queue_b = Queue("beta", runner=redis_runner, persistent=True)
    queue_c = Queue("charlie", runner=redis_runner, persistent=True)
    stop_event = threading.Event()
    strategy = PollingStrategy(stop_event)
    old_waiter: RedisMultiQueueActivityWaiter | None = None
    candidate: RedisMultiQueueActivityWaiter | None = None
    displaced: RedisMultiQueueActivityWaiter | None = None

    try:
        created_old = create_activity_waiter_for_queues(
            [queue_a, queue_b],
            stop_event=stop_event,
        )
        assert created_old is not None
        assert isinstance(created_old, RedisMultiQueueActivityWaiter)
        old_waiter = created_old
        strategy.start(activity_waiter=old_waiter)

        created_candidate = create_activity_waiter_for_queues(
            [queue_a, queue_c],
            stop_event=stop_event,
        )
        assert created_candidate is not None
        assert isinstance(created_candidate, RedisMultiQueueActivityWaiter)
        candidate = created_candidate
        replaced = strategy.replace_activity_waiter(candidate)
        assert isinstance(replaced, RedisMultiQueueActivityWaiter)
        displaced = replaced

        assert displaced is old_waiter
        assert all(child._closed is False for child in old_waiter._waiters)
        assert all(child._closed is False for child in candidate._waiters)

        queue_b.write("removed")
        strategy._next_native_idle_poll_at = time.monotonic() + 2.0
        strategy.wait_for_activity()
        assert strategy.consume_native_activity_hint() is False

        displaced.close()
        assert all(child._closed is True for child in displaced._waiters)
        assert all(child._closed is False for child in candidate._waiters)

        queue_c.write("added")
        strategy.wait_for_activity()
        assert strategy.consume_native_activity_hint() is True

        strategy.close()
        assert all(child._closed is True for child in candidate._waiters)
    finally:
        strategy.close()
        if displaced is not None:
            displaced.close()
        if old_waiter is not None:
            old_waiter.close()
        if candidate is not None:
            candidate.close()
        queue_a.close()
        queue_b.close()
        queue_c.close()


def test_activity_waiter_stop_event_breaks_wait_promptly(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    stop_event = threading.Event()
    waiter = plugin.create_activity_waiter(
        target=redis_url,
        backend_options={"namespace": redis_namespace},
        queue_name="jobs",
        stop_event=stop_event,
    )
    assert waiter is not None
    results: list[bool] = []
    thread = threading.Thread(target=lambda: results.append(waiter.wait(5.0)))
    try:
        thread.start()
        time.sleep(0.1)
        stop_event.set()
        thread.join(0.75)

        assert not thread.is_alive()
        assert results == [False]
    finally:
        waiter.close()
        thread.join(5.0)
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_multi_queue_activity_waiter_stop_event_breaks_wait_promptly(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    stop_event = threading.Event()
    waiter = plugin.create_activity_waiter_for_queues(
        target=redis_url,
        backend_options={"namespace": redis_namespace},
        queue_names=("alpha", "beta"),
        stop_event=stop_event,
    )
    assert waiter is not None
    results: list[bool] = []
    thread = threading.Thread(target=lambda: results.append(waiter.wait(5.0)))
    try:
        thread.start()
        time.sleep(0.1)
        stop_event.set()
        thread.join(0.75)

        assert not thread.is_alive()
        assert results == [False]
    finally:
        waiter.close()
        thread.join(5.0)
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})
