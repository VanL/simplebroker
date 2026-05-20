"""Valkey/Redis state-transition integration tests."""

from __future__ import annotations

import pytest
from simplebroker_redis import RedisRunner, get_backend_plugin
from simplebroker_redis.core import RedisBrokerCore
from simplebroker_redis.keys import RedisKeys, encode_id

from simplebroker._exceptions import DatabaseError, OperationalError

pytestmark = [pytest.mark.redis_only]


def test_claim_generator_rolls_back_unfinished_batch(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        core.write("jobs", "one")
        core.write("jobs", "two")

        generator = core.claim_generator(
            "jobs",
            delivery_guarantee="at_least_once",
            batch_size=2,
            with_timestamps=False,
        )
        assert next(generator) == "one"

        generator.close()

        assert core.claim_many("jobs", 2, with_timestamps=False) == ["one", "two"]
    finally:
        core.close()


def test_move_generator_commits_reserved_batch(redis_runner: RedisRunner) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        core.write("source", "payload")

        generator = core.move_generator(
            "source",
            "dest",
            delivery_guarantee="at_least_once",
            batch_size=1,
            with_timestamps=False,
        )
        assert list(generator) == ["payload"]

        assert core.peek_one("source", with_timestamps=False) is None
        assert core.peek_one("dest", with_timestamps=False) == "payload"
    finally:
        core.close()


def test_cleanup_refuses_active_reserved_batch(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    core = RedisBrokerCore(runner)
    generator = None
    try:
        core.write("jobs", "payload")
        generator = core.claim_generator(
            "jobs",
            delivery_guarantee="at_least_once",
            batch_size=1,
            with_timestamps=False,
        )
        assert next(generator) == "payload"

        with pytest.raises(DatabaseError):
            plugin.cleanup_target(
                redis_url, backend_options={"namespace": redis_namespace}
            )
    finally:
        if generator is not None:
            generator.close()
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_stale_batch_recovery_releases_reserved_messages(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    runner = RedisRunner(redis_url, namespace=redis_namespace, stale_batch_seconds=300)
    core = RedisBrokerCore(runner)
    generator = None
    try:
        core.write("jobs", "payload")
        generator = core.claim_generator(
            "jobs",
            delivery_guarantee="at_least_once",
            batch_size=1,
            with_timestamps=False,
        )
        assert next(generator) == "payload"

        recovering_runner = RedisRunner(
            redis_url,
            namespace=redis_namespace,
            stale_batch_seconds=0,
        )
        recovering_core = RedisBrokerCore(recovering_runner)
        try:
            assert recovering_core.claim_one("jobs", with_timestamps=False) == "payload"
        finally:
            recovering_core.shutdown()
    finally:
        if generator is not None:
            generator.close()
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_delete_from_queues_refuses_active_reserved_batch(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    runner = RedisRunner(redis_url, namespace=redis_namespace, stale_batch_seconds=300)
    core = RedisBrokerCore(runner)
    deleting_runner = RedisRunner(
        redis_url, namespace=redis_namespace, stale_batch_seconds=300
    )
    deleting_core = RedisBrokerCore(deleting_runner)
    generator = None
    try:
        core.write("alpha", "alpha-pending")
        core.write("beta", "beta-reserved")
        generator = core.claim_generator(
            "beta",
            delivery_guarantee="at_least_once",
            batch_size=1,
            with_timestamps=False,
        )
        assert next(generator) == "beta-reserved"

        with pytest.raises(OperationalError):
            deleting_core.delete_from_queues(["alpha", "beta"])

        assert deleting_core.peek_many("alpha", limit=10, with_timestamps=False) == [
            "alpha-pending"
        ]
        assert (
            deleting_core._runner.client.zcard(
                RedisKeys(redis_namespace).reserved("beta")
            )
            == 1
        )
    finally:
        if generator is not None:
            generator.close()
        deleting_core.shutdown()
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_delete_from_queues_before_timestamp_checks_only_matching_reserved_ids(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    runner = RedisRunner(redis_url, namespace=redis_namespace, stale_batch_seconds=300)
    core = RedisBrokerCore(runner)
    deleting_runner = RedisRunner(
        redis_url, namespace=redis_namespace, stale_batch_seconds=300
    )
    deleting_core = RedisBrokerCore(deleting_runner)
    keys = RedisKeys(redis_namespace)
    matching_generator = None
    nonmatching_generator = None
    try:
        core.write("alpha", "old-reserved")
        old_reserved_ts = dict(core.peek_many("alpha", limit=10))["old-reserved"]
        matching_generator = core.claim_generator(
            "alpha",
            delivery_guarantee="at_least_once",
            batch_size=1,
            with_timestamps=False,
        )
        assert next(matching_generator) == "old-reserved"

        with pytest.raises(OperationalError):
            deleting_core.delete_from_queues(
                ["alpha"],
                before_timestamp=old_reserved_ts + 1,
            )
        assert (
            deleting_core._runner.client.hget(keys.bodies, encode_id(old_reserved_ts))
            == "old-reserved"
        )
        matching_generator.close()
        matching_generator = None

        core.write("beta", "old-delete")
        old_delete_ts = dict(core.peek_many("beta", limit=10))["old-delete"]
        core.write("beta", "new-reserved")
        new_reserved_ts = dict(core.peek_many("beta", limit=10))["new-reserved"]
        nonmatching_generator = core.claim_generator(
            "beta",
            delivery_guarantee="at_least_once",
            batch_size=1,
            after_timestamp=old_delete_ts,
            with_timestamps=False,
        )
        assert next(nonmatching_generator) == "new-reserved"

        deleted = deleting_core.delete_from_queues(
            ["beta"],
            before_timestamp=new_reserved_ts,
        )

        assert deleted == 1
        assert (
            deleting_core._runner.client.hget(keys.bodies, encode_id(old_delete_ts))
            is None
        )
        assert (
            deleting_core._runner.client.zscore(keys.all_ids, encode_id(old_delete_ts))
            is None
        )
        assert (
            deleting_core._runner.client.zscore(
                keys.reserved("beta"), encode_id(new_reserved_ts)
            )
            is not None
        )
        assert (
            deleting_core._runner.client.hget(keys.bodies, encode_id(new_reserved_ts))
            == "new-reserved"
        )
    finally:
        if matching_generator is not None:
            matching_generator.close()
        if nonmatching_generator is not None:
            nonmatching_generator.close()
        deleting_core.shutdown()
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})


def test_find_message_ids_skips_active_reserved_batch(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    runner = RedisRunner(redis_url, namespace=redis_namespace, stale_batch_seconds=300)
    core = RedisBrokerCore(runner)
    searching_runner = RedisRunner(
        redis_url, namespace=redis_namespace, stale_batch_seconds=300
    )
    searching_core = RedisBrokerCore(searching_runner)
    keys = RedisKeys(redis_namespace)
    generator = None
    try:
        core.write("jobs", "target reserved")
        reserved_ts = dict(core.peek_many("jobs", limit=10))["target reserved"]
        generator = core.claim_generator(
            "jobs",
            delivery_guarantee="at_least_once",
            batch_size=1,
            with_timestamps=False,
        )
        assert next(generator) == "target reserved"

        assert (
            searching_core.find_message_ids(
                "jobs",
                body_contains="target",
                limit=10,
                include_claimed=True,
            )
            == []
        )
        assert (
            searching_core._runner.client.zscore(
                keys.reserved("jobs"), encode_id(reserved_ts)
            )
            is not None
        )
    finally:
        if generator is not None:
            generator.close()
        searching_core.shutdown()
        core.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})
