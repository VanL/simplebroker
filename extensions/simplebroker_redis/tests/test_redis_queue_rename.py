"""Valkey/Redis-specific queue rename behavior."""

from __future__ import annotations

import pytest
from simplebroker_redis import RedisRunner, get_backend_plugin
from simplebroker_redis.core import RedisBrokerCore

from simplebroker._exceptions import OperationalError

pytestmark = [pytest.mark.redis_only]


def test_redis_rename_preserves_pending_claimed_ids_and_removes_old_queue(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        core.insert_messages([("old", "pending", 10), ("old", "claimed", 20)])

        assert (
            core.claim_one(
                "old",
                exact_timestamp=20,
                with_timestamps=False,
            )
            == "claimed"
        )

        result = core.rename_queue("old", "new")

        assert result.messages_renamed == 2
        assert core.peek_many(
            "new", limit=10, with_timestamps=True, include_claimed=True
        ) == [("pending", 10), ("claimed", 20)]
        assert core.peek_many(
            "new", limit=10, with_timestamps=True, include_claimed=False
        ) == [("pending", 10)]
        assert (
            core.peek_many("old", limit=10, with_timestamps=True, include_claimed=True)
            == []
        )
        assert core.queue_exists("old") is False
        assert core.queue_exists("new") is True
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
    redis_url: str,
    redis_namespace: str,
) -> None:
    source_runner = RedisRunner(
        redis_url, namespace=redis_namespace, stale_batch_seconds=300
    )
    renaming_runner = RedisRunner(
        redis_url, namespace=redis_namespace, stale_batch_seconds=300
    )
    source_core = RedisBrokerCore(source_runner)
    renaming_core = RedisBrokerCore(renaming_runner)
    generator = None
    try:
        renaming_core.write("old", "source-payload")
        source_core.write("new", "reserved-target")
        generator = source_core.claim_generator(
            "new",
            delivery_guarantee="at_least_once",
            batch_size=1,
            with_timestamps=False,
        )
        assert next(generator) == "reserved-target"

        with pytest.raises(ValueError, match="Target queue already exists"):
            renaming_core.rename_queue("old", "new")

        assert renaming_core.peek_many(
            "old", limit=10, with_timestamps=False, include_claimed=True
        ) == ["source-payload"]
        assert renaming_core.peek_many(
            "new", limit=10, with_timestamps=False, include_claimed=True
        ) == ["reserved-target"]
    finally:
        if generator is not None:
            generator.close()
        renaming_core.shutdown()
        source_core.shutdown()
        get_backend_plugin().cleanup_target(
            redis_url, backend_options={"namespace": redis_namespace}
        )


def test_redis_rename_retargets_aliases_and_bumps_version(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        core.add_alias("alias", "old")
        core.write("old", "payload")
        alias_version = core.get_alias_version()

        result = core.rename_queue("old", "new")

        assert result.aliases_retargeted == 1
        assert core.resolve_alias("alias") == "new"
        assert core.get_alias_version() > alias_version
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
    try:
        result = core.rename_queue("missing", "new")

        assert result.messages_renamed == 0
        assert core.queue_exists("new") is False

        core.write("new", "created-after-miss")
        assert core.peek_many(
            "new", limit=10, with_timestamps=False, include_claimed=True
        ) == ["created-after-miss"]
    finally:
        core.close()
