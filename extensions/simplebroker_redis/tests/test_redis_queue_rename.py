"""Valkey/Redis-specific queue rename behavior."""

from __future__ import annotations

import pytest
from simplebroker_redis import RedisRunner, get_backend_plugin
from simplebroker_redis.core import RedisBrokerCore
from simplebroker_redis.keys import RedisKeys, encode_id

from simplebroker._exceptions import OperationalError

pytestmark = [pytest.mark.redis_only]


def test_redis_rename_cleans_old_keys_and_queue_set(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    try:
        core.write("old", "pending")
        core.write("old", "claimed")
        timestamps = dict(core.peek_many("old", limit=10))

        assert (
            core.claim_one(
                "old",
                exact_timestamp=timestamps["claimed"],
                with_timestamps=False,
            )
            == "claimed"
        )

        result = core.rename_queue("old", "new")

        assert result.messages_renamed == 2
        assert core._runner.client.zcard(keys.pending("old")) == 0
        assert core._runner.client.zcard(keys.claimed("old")) == 0
        assert core._runner.client.zcard(keys.reserved("old")) == 0
        assert set(core._runner.client.zrange(keys.pending("new"), 0, -1)) == {
            encode_id(timestamps["pending"])
        }
        assert set(core._runner.client.zrange(keys.claimed("new"), 0, -1)) == {
            encode_id(timestamps["claimed"])
        }
        assert "old" not in core._runner.client.smembers(keys.queues)
        assert "new" in core._runner.client.smembers(keys.queues)
        for timestamp in timestamps.values():
            encoded = encode_id(timestamp)
            assert core._runner.client.hget(keys.bodies, encoded) is not None
            assert core._runner.client.zscore(keys.all_ids, encoded) is not None
    finally:
        core.close()


def test_redis_rename_rejects_active_reserved_source(
    redis_url: str,
    redis_namespace: str,
) -> None:
    plugin = get_backend_plugin()
    runner = RedisRunner(redis_url, namespace=redis_namespace, stale_batch_seconds=300)
    renaming_runner = RedisRunner(
        redis_url, namespace=redis_namespace, stale_batch_seconds=300
    )
    core = RedisBrokerCore(runner)
    renaming_core = RedisBrokerCore(renaming_runner)
    generator = None
    try:
        core.write("old", "payload")
        generator = core.claim_generator(
            "old",
            delivery_guarantee="at_least_once",
            batch_size=1,
            with_timestamps=False,
        )
        assert next(generator) == "payload"

        with pytest.raises(OperationalError, match="at_least_once"):
            renaming_core.rename_queue("old", "new")

        generator.close()
        generator = None
        assert renaming_core.peek_many("old", limit=10, with_timestamps=False) == [
            "payload"
        ]
        assert renaming_core.queue_exists("new") is False
    finally:
        if generator is not None:
            generator.close()
        renaming_core.shutdown()
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_redis_rename_rejects_reserved_target_collision(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    try:
        core.write("old", "payload")
        core._runner.client.zadd(keys.reserved("new"), {encode_id(1): 0})

        with pytest.raises(ValueError, match="Target queue already exists"):
            core.rename_queue("old", "new")

        assert core.peek_many("old", limit=10, with_timestamps=False) == ["payload"]
        assert core._runner.client.zscore(keys.reserved("new"), encode_id(1)) == 0.0
    finally:
        core._runner.client.delete(keys.reserved("new"))
        core.close()


def test_redis_rename_retargets_aliases_and_bumps_version(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    try:
        core.add_alias("alias", "old")
        core.write("old", "payload")
        alias_version = int(core._runner.client.hget(keys.meta, "alias_version") or 0)

        result = core.rename_queue("old", "new")

        assert result.aliases_retargeted == 1
        assert core.resolve_alias("alias") == "new"
        assert int(core._runner.client.hget(keys.meta, "alias_version") or 0) > (
            alias_version
        )
    finally:
        core.close()


def test_redis_rename_publishes_old_and_new_activity(
    redis_url: str,
    redis_namespace: str,
) -> None:
    plugin = get_backend_plugin()
    core = plugin.create_core(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    old_waiter = plugin.create_activity_waiter(
        target=redis_url,
        backend_options={"namespace": redis_namespace},
        queue_name="old",
        stop_event=None,
    )
    new_waiter = plugin.create_activity_waiter(
        target=redis_url,
        backend_options={"namespace": redis_namespace},
        queue_name="new",
        stop_event=None,
    )
    assert old_waiter is not None
    assert new_waiter is not None
    try:
        core.write("old", "payload")
        assert old_waiter.wait(2.0) is True

        core.rename_queue("old", "new")

        assert old_waiter.wait(2.0) is True
        assert new_waiter.wait(2.0) is True
    finally:
        old_waiter.close()
        new_waiter.close()
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_redis_rename_missing_source_does_not_create_new_keys(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    try:
        result = core.rename_queue("missing", "new")

        assert result.messages_renamed == 0
        assert core._runner.client.exists(
            keys.pending("new"), keys.claimed("new"), keys.reserved("new")
        ) == 0
        assert "new" not in core._runner.client.smembers(keys.queues)
    finally:
        core.close()
