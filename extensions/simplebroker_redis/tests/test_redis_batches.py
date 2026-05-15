"""Valkey/Redis state-transition integration tests."""

from __future__ import annotations

import pytest
from simplebroker_redis import RedisRunner, get_backend_plugin
from simplebroker_redis.core import RedisBrokerCore

from simplebroker._exceptions import DatabaseError

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
