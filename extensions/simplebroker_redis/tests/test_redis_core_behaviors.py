"""Focused Redis direct-core behavior tests."""

from __future__ import annotations

import time
from typing import NoReturn

import pytest
import redis
from simplebroker_redis import RedisRunner
from simplebroker_redis.core import RedisBrokerCore
from simplebroker_redis.keys import RedisKeys

from simplebroker._exceptions import IntegrityError, MessageError, OperationalError

pytestmark = [pytest.mark.redis_only]


def test_redis_manual_vacuum_translates_client_errors(
    redis_runner: RedisRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = RedisBrokerCore(redis_runner, config={"BROKER_AUTO_VACUUM": 0})
    try:
        core.write("jobs", "payload")
        assert core.claim_one("jobs", with_timestamps=False) == "payload"

        def fail_eval(*_args: object, **_kwargs: object) -> NoReturn:
            raise redis.RedisError("injected vacuum failure")

        monkeypatch.setattr(core._client, "eval", fail_eval)

        with pytest.raises(
            OperationalError, match="injected vacuum failure"
        ) as exc_info:
            core.vacuum()

        assert isinstance(exc_info.value.__cause__, redis.RedisError)
        assert core.count_claimed_messages() == 1
    finally:
        core.close()


def test_redis_insert_messages_rejects_duplicate_explicit_ids(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        core.insert_messages([])
        core.insert_messages([("jobs", "one", 10)])

        with pytest.raises(IntegrityError, match="message ID already exists"):
            core.insert_messages([("jobs", "duplicate existing", 10)])

        with pytest.raises(IntegrityError, match="duplicate message ID"):
            core.insert_messages(
                [
                    ("jobs", "duplicate first", 20),
                    ("jobs", "duplicate second", 20),
                ]
            )

        assert core.peek_many("jobs", limit=10, with_timestamps=False) == ["one"]
    finally:
        core.close()


def test_redis_move_methods_cover_exact_bounds_and_claimed_rows(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        core.insert_messages(
            [
                ("source", "old", 10),
                ("source", "middle", 20),
                ("source", "new", 30),
            ]
        )
        assert (
            core.claim_one("source", exact_timestamp=20, with_timestamps=False)
            == "middle"
        )

        assert (
            core.move_one(
                "source",
                "dest",
                exact_timestamp=20,
                require_unclaimed=True,
                with_timestamps=False,
            )
            is None
        )
        assert (
            core.move_one(
                "source",
                "dest",
                exact_timestamp=20,
                require_unclaimed=False,
                with_timestamps=False,
            )
            == "middle"
        )
        assert core.move_many(
            "source",
            "dest",
            1,
            before_timestamp=30,
            with_timestamps=False,
        ) == ["old"]
        assert list(
            core.move_generator(
                "source",
                "dest",
                delivery_guarantee="exactly_once",
                with_timestamps=False,
            )
        ) == ["new"]

        assert core.peek_many("source", limit=10, with_timestamps=False) == []
        assert core.peek_many("dest", limit=10, with_timestamps=False) == [
            "old",
            "middle",
            "new",
        ]
    finally:
        core.close()


def test_redis_generators_reject_reentrant_mutation_during_reserved_batch(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    generator = None
    try:
        core.insert_messages(
            [
                ("jobs", "old", 10),
                ("jobs", "middle", 20),
                ("jobs", "new", 30),
            ]
        )
        generator = core.claim_generator(
            "jobs",
            delivery_guarantee="at_least_once",
            batch_size=2,
            with_timestamps=False,
        )
        assert next(generator) == "old"

        with pytest.raises(RuntimeError, match="at_least_once claim_generator"):
            core.write("jobs", "blocked")

        assert core.has_pending_messages("jobs") is True
        assert core.has_pending_messages("jobs", after_timestamp=30) is False
        generator.close()
        generator = None

        assert list(
            core.claim_generator(
                "jobs",
                delivery_guarantee="exactly_once",
                with_timestamps=False,
            )
        ) == ["old", "middle", "new"]
    finally:
        if generator is not None:
            generator.close()
        core.close()


def test_redis_queue_stats_status_and_delete_include_claimed_rows(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        core.write("alpha", "a1")
        core.write("alpha", "a2")
        core.write("beta", "b1")
        assert core.claim_one("alpha", with_timestamps=False) == "a1"

        alpha = core.get_queue_stat("alpha")
        assert (alpha.queue, alpha.pending, alpha.claimed, alpha.total) == (
            "alpha",
            1,
            1,
            2,
        )
        assert core.queue_exists("alpha") is True
        assert core.queue_exists_and_has_messages("alpha") is True
        assert core.list_queue_stats(prefix="a") == [alpha]
        assert [item.queue for item in core.list_queue_stats(pattern="*ta")] == ["beta"]
        assert core.get_queue_stats() == [("alpha", 1, 2), ("beta", 1, 1)]
        assert core.get_overall_stats() == (1, 3)
        assert core.count_claimed_messages() == 1

        status = core.status()
        assert status["total_messages"] == 3
        assert status["last_timestamp"] >= 3
        assert status["db_size"] == 0

        assert core.delete("beta") == 1
        assert core.queue_exists("beta") is False
        assert core.delete() == 2
        assert core.get_overall_stats() == (0, 0)
    finally:
        core.close()


def test_redis_core_rejects_invalid_inputs(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(
        redis_runner,
        config={"BROKER_MAX_MESSAGE_SIZE": 5},
    )
    try:
        with pytest.raises(MessageError, match="exceeds maximum"):
            core.write("jobs", "123456")
        with pytest.raises(MessageError, match="UTF-8 encodable"):
            core.write("jobs", "\ud800")
        with pytest.raises(ValueError, match="limit must be at least 1"):
            core.peek_many("jobs", limit=0)
        with pytest.raises(ValueError, match="limit must be at least 1"):
            core.claim_many("jobs", 0)
        with pytest.raises(ValueError, match="limit must be at least 1"):
            core.move_many("source", "dest", 0)
        with pytest.raises(ValueError, match="Source and target"):
            core.move_one("same", "same")
        with pytest.raises(ValueError, match="Source and target"):
            list(core.move_generator("same", "same"))
        with pytest.raises(TypeError, match="not a string"):
            core.delete_from_queues("jobs")

        assert core.delete_message_ids("jobs", []) == 0
        assert core.delete_from_queues([]) == 0
        assert core.broadcast("hi", pattern="missing*") == 0
        assert core.get_data_version() is None
    finally:
        core.close()


def test_redis_core_internal_state_edges_are_safe(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        assert core._qkey("jobs", "custom") == (
            f"simplebroker:{redis_runner.namespace}:q:jobs:custom"
        )

        core._publish(None)
        core._pid = -1
        core._check_fork_safety()
        assert core._pid > 0

        core._active_generator_batch = "claim"
        core._active_generator_batch_owner = -1
        core._assert_no_reentrant_mutation_during_batch("write")
        core._set_active_generator_batch(None)

        core._commit_claim_batch("jobs", "unused", [])
        core._commit_move_batch("jobs", "other", "unused", [])

        with pytest.raises(OperationalError, match="invalid Redis claim batch"):
            core._commit_claim_batch("jobs", "missing-token", [("payload", 1)])
        with pytest.raises(OperationalError, match="invalid Redis move batch"):
            core._commit_move_batch(
                "jobs",
                "other",
                "missing-token",
                [("payload", 1)],
            )

        assert core.recover_stale_batches(max_age_seconds=-1) == 0
        redis_runner.stale_batch_seconds = -1
        core._maybe_recover_stale_batches()

        core._resync_timestamp_generator()
        assert core.get_conflict_metrics()["ts_resync_count"] == 1
    finally:
        core.close()


def test_redis_alias_api_validates_and_updates_alias_metadata(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        core.write("existing", "payload")

        with pytest.warns(RuntimeWarning, match="already exists with messages"):
            core.add_alias("existing", "target")

        assert core.has_alias("existing") is True
        assert core.resolve_alias("existing") == "target"
        assert core.canonicalize_queue("existing") == "target"
        assert core.canonicalize_queue("plain") == "plain"
        assert core.list_aliases() == [("existing", "target")]
        assert core.aliases_for_target("target") == ["existing"]
        assert core.get_alias_version() > 0

        with pytest.raises(ValueError, match="already exists"):
            core.add_alias("existing", "other")
        with pytest.raises(ValueError, match="Cannot target another alias"):
            core.add_alias("other", "existing")
        with pytest.raises(ValueError, match="must differ"):
            core.add_alias("same", "same")
        with pytest.raises(ValueError, match="cannot be empty"):
            core.add_alias("", "target")
        with pytest.raises(ValueError, match="'@' prefix"):
            core.add_alias("@bad", "target")
        with pytest.raises(ValueError, match="'@' prefix"):
            core.add_alias("bad", "@target")
        with pytest.raises(ValueError, match="target cannot be empty"):
            core.add_alias("bad", "")

        core.remove_alias("existing")
        assert core.has_alias("existing") is False
        assert core.list_aliases() == []
    finally:
        core.close()


def test_redis_recover_stale_batches_ignores_unrecoverable_metadata(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    client = redis_runner.client
    try:
        client.hset(
            keys.batch_meta("00000000000000000000000000000000"),
            mapping={"source": "jobs"},
        )
        client.hset(
            keys.batch_meta("11111111111111111111111111111111"),
            mapping={"source": "jobs", "created_ns": "not-an-int"},
        )
        client.hset(
            keys.batch_meta("22222222222222222222222222222222"),
            mapping={"source": "jobs", "created_ns": str(time.time_ns() + 10**12)},
        )

        assert core.recover_stale_batches(max_age_seconds=0) == 0
    finally:
        core.close()
