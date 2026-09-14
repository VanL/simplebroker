"""Valkey/Redis state-transition integration tests."""

from __future__ import annotations

from typing import Any

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


@pytest.mark.parametrize(
    "metadata",
    [
        "fresh",
        "missing",
        "malformed",
        "no-source",
        "no-created",
        "cutoff",
        "older",
        "disabled",
    ],
)
def test_recovery_age_and_metadata_admission(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
    metadata: str,
) -> None:
    import time

    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
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
        (meta_key,) = list(
            redis_runner.client.scan_iter(keys.key("batches", "*", "meta"))
        )
        now = time.time_ns()
        cutoff = now - 300_000_000_000
        client = redis_runner.client
        if metadata == "missing":
            client.delete(meta_key)
        elif metadata == "no-source":
            client.hdel(meta_key, "source")
        elif metadata == "no-created":
            client.hdel(meta_key, "created_ns")
        else:
            value = {
                "fresh": str(cutoff + 1),
                "malformed": "not-a-number",
                "cutoff": str(cutoff),
                "older": str(cutoff - 1),
                "disabled": str(cutoff - 1),
            }[metadata]
            client.hset(meta_key, "created_ns", value)
        monkeypatch.setattr("simplebroker_redis.core.time.time_ns", lambda: now)
        expected = int(metadata in {"cutoff", "older"})
        assert (
            core.recover_stale_batches(
                max_age_seconds=-1 if metadata == "disabled" else 300
            )
            == expected
        )
        assert client.zcard(keys.reserved("jobs")) == 1 - expected
        assert (
            core.recover_stale_batches(
                max_age_seconds=-1 if metadata == "disabled" else 300
            )
            == 0
        )
    finally:
        if generator is not None:
            generator.close()
        core.close()


@pytest.mark.parametrize("change", ["source", "created_ns", "deleted"])
def test_recovery_revalidates_metadata_after_scan(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
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
        (meta_key,) = list(
            redis_runner.client.scan_iter(keys.key("batches", "*", "meta"))
        )
        # Adjacent integers above 2**53 must remain different, even in Lua.
        redis_runner.client.hset(meta_key, "created_ns", "1000000000000000000")
        real_hgetall = redis_runner.client.hgetall

        def change_after_snapshot(key: Any) -> Any:
            result = real_hgetall(key)
            if key == meta_key:
                if change == "deleted":
                    redis_runner.client.delete(key)
                else:
                    redis_runner.client.hset(
                        key,
                        change,
                        "other" if change == "source" else "1000000000000000001",
                    )
            return result

        monkeypatch.setattr(redis_runner.client, "hgetall", change_after_snapshot)
        assert core.recover_stale_batches(max_age_seconds=300) == 0
        assert redis_runner.client.zcard(keys.reserved("jobs")) == 1
    finally:
        if generator is not None:
            generator.close()
        core.close()


@pytest.mark.parametrize("operation", ["claim", "move"])
@pytest.mark.parametrize("first", ["commit", "recovery", "rollback"])
def test_recovery_and_batch_completion_order(
    redis_runner: RedisRunner,
    operation: str,
    first: str,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    generator = None
    try:
        core.write("jobs", "payload")
        factory = core.claim_generator if operation == "claim" else core.move_generator
        args = ("jobs",) if operation == "claim" else ("jobs", "dest")
        generator = factory(
            *args,
            delivery_guarantee="at_least_once",
            batch_size=1,
            with_timestamps=False,
        )
        assert next(generator) == "payload"
        if first == "commit":
            assert list(generator) == []
            assert core.recover_stale_batches(max_age_seconds=0) == 0
            assert core.peek_one("jobs", with_timestamps=False) is None
            assert redis_runner.client.zcard(keys.claimed("jobs")) == int(
                operation == "claim"
            )
            assert core.peek_one("dest", with_timestamps=False) == (
                "payload" if operation == "move" else None
            )
        elif first == "rollback":
            generator.close()
            assert core.recover_stale_batches(max_age_seconds=0) == 0
            assert core.peek_one("jobs", with_timestamps=False) == "payload"
        else:
            assert core.recover_stale_batches(max_age_seconds=0) == 1
            with pytest.raises(OperationalError, match="stale or invalid"):
                list(generator)
            assert core.peek_one("jobs", with_timestamps=False) == "payload"
            assert core.peek_one("dest", with_timestamps=False) is None
        assert redis_runner.client.zcard(keys.reserved("jobs")) == 0
    finally:
        if generator is not None:
            generator.close()
        core.close()


def test_concurrent_recovery_counts_live_ids_once(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import concurrent.futures
    import threading

    from simplebroker_redis import scripts

    core = RedisBrokerCore(redis_runner)
    generator = None
    ready = threading.Barrier(2)
    real_eval = redis_runner.client.eval

    def simultaneous_recovery(script: str, *args: Any, **kwargs: Any) -> Any:
        if script == scripts.RECOVER_STALE_BATCH:
            ready.wait(timeout=10)
        return real_eval(script, *args, **kwargs)

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
        monkeypatch.setattr(redis_runner.client, "eval", simultaneous_recovery)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(core.recover_stale_batches, max_age_seconds=0)
                for _ in range(2)
            ]
            assert sorted(f.result(timeout=15) for f in futures) == [0, 2]
        assert core.recover_stale_batches(max_age_seconds=0) == 0
        assert core.peek_many("jobs", limit=10, with_timestamps=False) == ["one", "two"]
    finally:
        if generator is not None:
            generator.close()
        core.close()


def test_recovery_preserves_neighbor_namespace(
    redis_url: str,
    redis_namespace: str,
) -> None:
    namespace_other = redis_namespace + "_other"
    runners = [
        RedisRunner(redis_url, namespace=namespace)
        for namespace in (redis_namespace, namespace_other)
    ]
    cores = [RedisBrokerCore(runner) for runner in runners]
    generators = []
    try:
        for core in cores:
            core.write("jobs", "payload")
            generator = core.claim_generator(
                "jobs",
                delivery_guarantee="at_least_once",
                batch_size=1,
                with_timestamps=False,
            )
            generators.append(generator)
            assert next(generator) == "payload"
        assert cores[0].recover_stale_batches(max_age_seconds=0) == 1
        assert cores[0].peek_one("jobs", with_timestamps=False) == "payload"
        assert cores[1].peek_one("jobs", with_timestamps=False) == "payload"
        assert runners[1].client.zcard(RedisKeys(namespace_other).reserved("jobs")) == 1
    finally:
        for generator in generators:
            generator.close()
        for core in cores:
            core.shutdown()
        for namespace in (redis_namespace, namespace_other):
            get_backend_plugin().cleanup_target(
                redis_url, backend_options={"namespace": namespace}
            )
