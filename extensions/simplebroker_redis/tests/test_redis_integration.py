"""Basic Valkey/Redis backend integration tests."""

from __future__ import annotations

import pytest
from simplebroker_redis import RedisRunner, get_backend_plugin
from simplebroker_redis.keys import RedisKeys, encode_id

from simplebroker import Queue

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
