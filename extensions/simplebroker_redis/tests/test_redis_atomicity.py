"""Concurrency and validation regressions for the Valkey/Redis backend."""

from __future__ import annotations

import threading

import pytest
from simplebroker_redis import RedisRunner, scripts
from simplebroker_redis.core import RedisBrokerCore
from simplebroker_redis.keys import RedisKeys, encode_id

from simplebroker._exceptions import OperationalError, QueueNameError

pytestmark = [pytest.mark.redis_only]


def _seed_reserved_prefix(
    core: RedisBrokerCore,
    queue: str,
    *,
    reserved_count: int = 2048,
) -> None:
    core.insert_messages(
        (queue, f"reserved-{timestamp}", timestamp)
        for timestamp in range(1, reserved_count + 2)
    )
    core._client.zadd(
        RedisKeys(core._runner.namespace).reserved(queue),
        {encode_id(timestamp): 0 for timestamp in range(1, reserved_count + 1)},
    )


def test_peek_generator_validates_queue_name(redis_runner: RedisRunner) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        with pytest.raises(QueueNameError):
            list(core.peek_generator("bad queue"))
    finally:
        core.close()


@pytest.mark.parametrize("after_timestamp", [True, "abc"])
def test_has_pending_validates_after_timestamp(
    redis_runner: RedisRunner,
    after_timestamp: object,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        with pytest.raises(TypeError):
            core.has_pending_messages("jobs", after_timestamp=after_timestamp)  # type: ignore[arg-type]
    finally:
        core.close()


def test_claim_skips_thousands_of_reserved_head_ids(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        _seed_reserved_prefix(core, "jobs")

        assert core.claim_one("jobs", with_timestamps=False) == "reserved-2049"
    finally:
        core._client.delete(RedisKeys(redis_runner.namespace).reserved("jobs"))
        core.close()


def test_move_skips_thousands_of_reserved_head_ids(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        _seed_reserved_prefix(core, "source")

        assert (
            core.move_one("source", "target", with_timestamps=False) == "reserved-2049"
        )
    finally:
        core._client.delete(RedisKeys(redis_runner.namespace).reserved("source"))
        core.close()


def test_batch_reservation_skips_thousands_of_reserved_head_ids(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    generator = None
    try:
        _seed_reserved_prefix(core, "jobs")
        generator = core.claim_generator(
            "jobs",
            delivery_guarantee="at_least_once",
            batch_size=1,
            with_timestamps=False,
        )

        assert next(generator) == "reserved-2049"
    finally:
        if generator is not None:
            generator.close()
        core._client.delete(RedisKeys(redis_runner.namespace).reserved("jobs"))
        core.close()


def test_delete_write_race_never_orphans_redis_storage(
    redis_runner: RedisRunner,
) -> None:
    writer = RedisBrokerCore(redis_runner)
    deleter = RedisBrokerCore(redis_runner)
    start = threading.Barrier(2)
    failures: list[BaseException] = []

    def write_messages() -> None:
        try:
            start.wait()
            for index in range(3000):
                writer.write("jobs", f"message-{index}")
        except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] exception
            failures.append(exc)

    thread = threading.Thread(target=write_messages)
    try:
        thread.start()
        start.wait()
        for _ in range(500):
            deleter.delete("jobs")
        thread.join(timeout=10)
        assert not thread.is_alive()
        assert failures == []

        keys = RedisKeys(redis_runner.namespace)
        body_ids = set(deleter._client.hkeys(keys.bodies))
        all_ids = set(deleter._client.zrange(keys.all_ids, 0, -1))
        live_ids = set(deleter._client.zrange(keys.pending("jobs"), 0, -1))
        live_ids.update(deleter._client.zrange(keys.claimed("jobs"), 0, -1))

        assert body_ids == live_ids
        assert all_ids == live_ids
    finally:
        thread.join(timeout=10)
        deleter.close()
        writer.close()


def test_delete_queue_script_removes_every_storage_reference(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    try:
        core.insert_messages(
            [
                ("jobs", "pending", 1),
                ("jobs", "claimed", 2),
            ]
        )
        assert core.claim_one("jobs", exact_timestamp=2) == ("claimed", 2)

        deleted = core._client.eval(
            scripts.DELETE_QUEUE,
            6,
            keys.pending("jobs"),
            keys.claimed("jobs"),
            keys.reserved("jobs"),
            keys.bodies,
            keys.all_ids,
            keys.queues,
            "jobs",
        )

        assert deleted == 2
        assert core._client.hlen(keys.bodies) == 0
        assert core._client.zcard(keys.all_ids) == 0
        assert core._client.exists(keys.pending("jobs")) == 0
        assert core._client.exists(keys.claimed("jobs")) == 0
        assert not core._client.sismember(keys.queues, "jobs")
    finally:
        core.close()


def test_delete_all_preflights_every_reserved_queue_before_deleting(
    redis_runner: RedisRunner,
) -> None:
    reserving = RedisBrokerCore(redis_runner)
    deleting = RedisBrokerCore(redis_runner)
    generator = None
    try:
        reserving.write("alpha", "keep")
        reserving.write("beta", "reserved")
        generator = reserving.claim_generator(
            "beta",
            delivery_guarantee="at_least_once",
            batch_size=1,
            with_timestamps=False,
        )
        assert next(generator) == "reserved"

        with pytest.raises(OperationalError, match="at_least_once batch"):
            deleting.delete()

        assert deleting.peek_one("alpha", with_timestamps=False) == "keep"
    finally:
        if generator is not None:
            generator.close()
        deleting.close()
        reserving.close()


def test_delete_queue_script_rechecks_reservation_without_partial_mutation(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    encoded = encode_id(1)
    try:
        core.insert_messages([("jobs", "reserved", 1)])
        core._client.zadd(keys.reserved("jobs"), {encoded: 0})

        result = core._client.eval(
            scripts.DELETE_QUEUE,
            6,
            keys.pending("jobs"),
            keys.claimed("jobs"),
            keys.reserved("jobs"),
            keys.bodies,
            keys.all_ids,
            keys.queues,
            "jobs",
        )

        assert result == -1
        assert core._client.hget(keys.bodies, encoded) == "reserved"
        assert core._client.zscore(keys.pending("jobs"), encoded) is not None
        assert core._client.zscore(keys.all_ids, encoded) is not None
    finally:
        core._client.delete(keys.reserved("jobs"))
        core.close()


def test_vacuum_script_keeps_nonempty_queue_registered(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    try:
        core.insert_messages(
            [
                ("jobs", "claimed", 1),
                ("jobs", "pending", 2),
            ]
        )
        assert core.claim_one("jobs", exact_timestamp=1) == ("claimed", 1)

        removed = core._client.eval(
            scripts.VACUUM_CLAIMED,
            6,
            keys.claimed("jobs"),
            keys.pending("jobs"),
            keys.reserved("jobs"),
            keys.bodies,
            keys.all_ids,
            keys.queues,
            "jobs",
            "100",
        )

        assert removed == 1
        assert core._client.sismember(keys.queues, "jobs")
        assert core.peek_one("jobs", with_timestamps=False) == "pending"
    finally:
        core.close()


def test_patternless_broadcast_does_not_resurrect_deleted_queue(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broadcaster = RedisBrokerCore(redis_runner)
    deleting = RedisBrokerCore(redis_runner)
    try:
        broadcaster.write("jobs", "seed")
        original_generate = broadcaster.generate_timestamp
        deleted = False

        def delete_before_first_timestamp() -> int:
            nonlocal deleted
            if not deleted:
                deleted = True
                assert deleting.delete("jobs") == 1
            return original_generate()

        monkeypatch.setattr(
            broadcaster,
            "generate_timestamp",
            delete_before_first_timestamp,
        )

        assert broadcaster.broadcast("announcement") == 0
        assert broadcaster.queue_exists("jobs") is False
    finally:
        deleting.close()
        broadcaster.close()


def test_exact_broadcast_does_not_resurrect_deleted_queue(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broadcaster = RedisBrokerCore(redis_runner)
    deleting = RedisBrokerCore(redis_runner)
    try:
        broadcaster.write("jobs", "seed")
        original_reserve = broadcaster._timestamp_gen._reserve_candidates
        deleted = False

        def delete_before_reservation(count: int) -> list[int]:
            nonlocal deleted
            if not deleted:
                deleted = True
                assert deleting.delete("jobs") == 1
            return original_reserve(count)

        monkeypatch.setattr(
            broadcaster._timestamp_gen,
            "_reserve_candidates",
            delete_before_reservation,
        )

        assert broadcaster.broadcast("announcement", queue_names=["jobs"]) == 0
        assert broadcaster.queue_exists("jobs") is False
    finally:
        deleting.close()
        broadcaster.close()


def test_exact_create_broadcast_resurrects_queue_deleted_before_atomic_point(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broadcaster = RedisBrokerCore(redis_runner)
    deleting = RedisBrokerCore(redis_runner)
    try:
        broadcaster.write("jobs", "seed")
        original_reserve = broadcaster._timestamp_gen._reserve_candidates
        deleted = False

        def delete_before_reservation(count: int) -> list[int]:
            nonlocal deleted
            if not deleted:
                deleted = True
                assert deleting.delete("jobs") == 1
            return original_reserve(count)

        monkeypatch.setattr(
            broadcaster._timestamp_gen,
            "_reserve_candidates",
            delete_before_reservation,
        )

        assert (
            broadcaster.broadcast(
                "announcement",
                queue_names=["jobs"],
                create_missing=True,
            )
            == 1
        )
        assert broadcaster.queue_exists("jobs") is True
        assert broadcaster.peek_many("jobs", limit=10, with_timestamps=False) == [
            "announcement"
        ]
    finally:
        deleting.close()
        broadcaster.close()


def test_patternless_broadcast_includes_queue_created_during_setup(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broadcaster = RedisBrokerCore(redis_runner)
    writer = RedisBrokerCore(redis_runner)
    try:
        broadcaster.write("existing", "seed")
        original_generate = broadcaster.generate_timestamp
        created = False

        def create_before_first_timestamp() -> int:
            nonlocal created
            if not created:
                created = True
                writer.write("new", "new-seed")
            return original_generate()

        monkeypatch.setattr(
            broadcaster,
            "generate_timestamp",
            create_before_first_timestamp,
        )

        assert broadcaster.broadcast("announcement") == 2
        assert broadcaster.peek_many("new", limit=10, with_timestamps=False) == [
            "new-seed",
            "announcement",
        ]
    finally:
        writer.close()
        broadcaster.close()


def test_patternless_broadcast_wakes_queue_waiter(
    redis_url: str,
    redis_namespace: str,
) -> None:
    from simplebroker_redis import get_backend_plugin

    plugin = get_backend_plugin()
    plugin.initialize_target(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    core = RedisBrokerCore(runner)
    core.write("jobs", "seed")
    waiter = plugin.create_activity_waiter(
        target=redis_url,
        backend_options={"namespace": redis_namespace},
        queue_name="jobs",
        stop_event=None,
    )
    assert waiter is not None
    try:
        assert waiter.wait(0.05) is False

        assert core.broadcast("announcement") == 1

        assert waiter.wait(2.0) is True
    finally:
        waiter.close()
        core.shutdown()
        plugin.cleanup_target(
            redis_url,
            backend_options={"namespace": redis_namespace},
        )


def test_exact_broadcast_wakes_only_selected_existing_queues(
    redis_url: str,
    redis_namespace: str,
) -> None:
    from simplebroker_redis import get_backend_plugin

    plugin = get_backend_plugin()
    plugin.initialize_target(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    core = RedisBrokerCore(runner)
    core.write("selected", "seed")
    core.write("other", "seed")
    selected_waiter = plugin.create_activity_waiter(
        target=redis_url,
        backend_options={"namespace": redis_namespace},
        queue_name="selected",
        stop_event=None,
    )
    other_waiter = plugin.create_activity_waiter(
        target=redis_url,
        backend_options={"namespace": redis_namespace},
        queue_name="other",
        stop_event=None,
    )
    assert selected_waiter is not None
    assert other_waiter is not None
    try:
        assert selected_waiter.wait(0.05) is False
        assert other_waiter.wait(0.05) is False

        assert (
            core.broadcast(
                "announcement",
                queue_names=["selected", "missing", "selected"],
            )
            == 1
        )

        assert selected_waiter.wait(2.0) is True
        assert other_waiter.wait(0.05) is False
    finally:
        other_waiter.close()
        selected_waiter.close()
        core.shutdown()
        plugin.cleanup_target(
            redis_url,
            backend_options={"namespace": redis_namespace},
        )


def test_exact_create_broadcast_wakes_new_queue(
    redis_url: str,
    redis_namespace: str,
) -> None:
    from simplebroker_redis import get_backend_plugin

    plugin = get_backend_plugin()
    plugin.initialize_target(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    core = RedisBrokerCore(runner)
    waiter = plugin.create_activity_waiter(
        target=redis_url,
        backend_options={"namespace": redis_namespace},
        queue_name="created",
        stop_event=None,
    )
    assert waiter is not None
    try:
        assert waiter.wait(0.05) is False

        assert (
            core.broadcast(
                "announcement",
                queue_names=["created"],
                create_missing=True,
            )
            == 1
        )

        assert waiter.wait(2.0) is True
        assert core.queue_exists("created") is True
        assert core.peek_one("created", with_timestamps=False) == "announcement"
    finally:
        waiter.close()
        core.shutdown()
        plugin.cleanup_target(
            redis_url,
            backend_options={"namespace": redis_namespace},
        )


def test_empty_exact_create_broadcast_does_not_wake_queue(
    redis_url: str,
    redis_namespace: str,
) -> None:
    from simplebroker_redis import get_backend_plugin

    plugin = get_backend_plugin()
    plugin.initialize_target(
        redis_url,
        backend_options={"namespace": redis_namespace},
    )
    runner = RedisRunner(redis_url, namespace=redis_namespace)
    core = RedisBrokerCore(runner)
    waiter = plugin.create_activity_waiter(
        target=redis_url,
        backend_options={"namespace": redis_namespace},
        queue_name="missing",
        stop_event=None,
    )
    assert waiter is not None
    try:
        assert waiter.wait(0.05) is False

        assert (
            core.broadcast(
                "announcement",
                queue_names=(),
                create_missing=True,
            )
            == 0
        )

        assert waiter.wait(0.05) is False
        assert core.queue_exists("missing") is False
    finally:
        waiter.close()
        core.shutdown()
        plugin.cleanup_target(
            redis_url,
            backend_options={"namespace": redis_namespace},
        )


def test_broadcast_script_rejects_insufficient_timestamp_batch_atomically(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    try:
        core.write("alpha", "seed-alpha")
        core.write("beta", "seed-beta")
        timestamp = core.generate_timestamp()

        result = core._client.eval(
            scripts.BROADCAST_MESSAGE,
            4,
            keys.meta,
            keys.bodies,
            keys.all_ids,
            keys.queues,
            str(timestamp),
            encode_id(timestamp),
            "1",
            "announcement",
            keys.key("q", ""),
            "all",
            "0",
            encode_id(timestamp),
        )

        assert result == [-4, 2]
        assert core.peek_many("alpha", limit=10, with_timestamps=False) == [
            "seed-alpha"
        ]
        assert core.peek_many("beta", limit=10, with_timestamps=False) == ["seed-beta"]
    finally:
        core.close()


def test_broadcast_script_selects_queues_at_atomic_insertion_point(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    try:
        core.write("alpha", "seed-alpha")
        core.write("deleted", "seed-deleted")
        assert core.delete("deleted") == 1
        timestamps = [core.generate_timestamp(), core.generate_timestamp()]

        result = core._client.eval(
            scripts.BROADCAST_MESSAGE,
            4,
            keys.meta,
            keys.bodies,
            keys.all_ids,
            keys.queues,
            str(timestamps[-1]),
            encode_id(timestamps[-1]),
            "2",
            "announcement",
            keys.key("q", ""),
            "all",
            "0",
            *(encode_id(timestamp) for timestamp in timestamps),
        )

        assert result == [1, "alpha"]
        assert core.peek_many("alpha", limit=10, with_timestamps=False) == [
            "seed-alpha",
            "announcement",
        ]
        assert core.queue_exists("deleted") is False
    finally:
        core.close()


@pytest.mark.parametrize(
    "arguments",
    [
        ["1", encode_id(1), "1", "body", "prefix", "exact", "1"],
        [
            "1",
            encode_id(1),
            "1",
            "body",
            "prefix",
            "all",
            "0",
            encode_id(1),
            encode_id(2),
        ],
        [
            "1",
            encode_id(1),
            "1",
            "body",
            "prefix",
            "all",
            "1",
            "jobs",
            encode_id(1),
        ],
        ["1", encode_id(1), "-1", "body", "prefix", "all", "0"],
        [
            "1",
            encode_id(1),
            "1.5",
            "body",
            "prefix",
            "all",
            "0",
            encode_id(1),
        ],
        ["1", encode_id(1), "0", "body", "prefix", "exact", "-1"],
        [
            "1",
            encode_id(1),
            "1",
            "body",
            "prefix",
            "exact",
            "0.5",
            "jobs",
            encode_id(1),
        ],
        ["1", encode_id(1), "0", "body", "prefix", "unknown", "0"],
        [
            "1",
            encode_id(1),
            "2",
            "body",
            "prefix",
            "exact",
            "1",
            "jobs",
            encode_id(1),
            encode_id(2),
        ],
        [
            "1",
            encode_id(1),
            "2",
            "body",
            "prefix",
            "exact_create",
            "1",
            "new",
            encode_id(1),
            encode_id(2),
        ],
    ],
)
def test_broadcast_script_rejects_malformed_layout(
    redis_runner: RedisRunner,
    arguments: list[str],
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    try:
        core.write("jobs", "seed")
        last_ts_before = core._client.hget(keys.meta, "last_ts")
        bodies_before = core._client.hgetall(keys.bodies)
        all_ids_before = core._client.zrange(keys.all_ids, 0, -1)

        result = core._client.eval(
            scripts.BROADCAST_MESSAGE,
            4,
            keys.meta,
            keys.bodies,
            keys.all_ids,
            keys.queues,
            *arguments,
        )

        assert result == [-5]
        assert core.peek_many("jobs", limit=10, with_timestamps=False) == ["seed"]
        assert not core._client.sismember(keys.queues, "new")
        assert core._client.exists(keys.pending("new")) == 0
        assert core._client.hget(keys.meta, "last_ts") == last_ts_before
        assert core._client.hgetall(keys.bodies) == bodies_before
        assert core._client.zrange(keys.all_ids, 0, -1) == all_ids_before
    finally:
        core.close()


def test_exact_broadcast_uses_request_capacity_not_registry_size(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        for index in range(12):
            core.write(f"queue-{index}", "seed")

        assert core.broadcast("announcement", queue_names=["missing", "queue-7"]) == 1
        assert core.peek_many("queue-7", limit=10, with_timestamps=False) == [
            "seed",
            "announcement",
        ]
        assert core.peek_many("queue-8", limit=10, with_timestamps=False) == ["seed"]
    finally:
        core.close()


def test_exact_broadcast_script_rejects_candidates_not_above_persisted_last_ts(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    try:
        core.write("jobs", "seed")

        result = core._client.eval(
            scripts.BROADCAST_MESSAGE,
            4,
            keys.meta,
            keys.bodies,
            keys.all_ids,
            keys.queues,
            "1",
            encode_id(1),
            "1",
            "announcement",
            keys.key("q", ""),
            "exact",
            "1",
            "jobs",
            encode_id(1),
        )

        assert result == [-6]
        assert core.peek_many("jobs", limit=10, with_timestamps=False) == ["seed"]
    finally:
        core.close()


@pytest.mark.parametrize(
    ("candidate_ids", "expected"),
    [
        ([encode_id(10), encode_id(11)], [-1]),
        ([encode_id(11), encode_id(11)], [-3]),
    ],
)
def test_exact_create_script_rejects_candidate_conflicts_before_mutation(
    redis_runner: RedisRunner,
    candidate_ids: list[str],
    expected: list[int],
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    try:
        core.insert_messages([("existing", "seed", 10)])
        state_before = {
            "last_ts": core._client.hget(keys.meta, "last_ts"),
            "queues": core._client.smembers(keys.queues),
            "bodies": core._client.hgetall(keys.bodies),
            "all_ids": core._client.zrange(keys.all_ids, 0, -1),
        }

        result = core._client.eval(
            scripts.BROADCAST_MESSAGE,
            4,
            keys.meta,
            keys.bodies,
            keys.all_ids,
            keys.queues,
            "11",
            encode_id(11),
            "2",
            "announcement",
            keys.key("q", ""),
            "exact_create",
            "2",
            "new-a",
            "new-b",
            *candidate_ids,
        )

        assert result == expected
        assert core._client.smembers(keys.queues) == state_before["queues"]
        assert not core._client.sismember(keys.queues, "new-a")
        assert not core._client.sismember(keys.queues, "new-b")
        assert core._client.exists(keys.pending("new-a")) == 0
        assert core._client.exists(keys.pending("new-b")) == 0
        assert core._client.hget(keys.meta, "last_ts") == state_before["last_ts"]
        assert core._client.hgetall(keys.bodies) == state_before["bodies"]
        assert core._client.zrange(keys.all_ids, 0, -1) == state_before["all_ids"]
    finally:
        core.close()


def test_exact_create_script_rejects_stale_candidates_before_mutation(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    try:
        core.insert_messages([("existing", "seed", 10)])
        state_before = {
            "last_ts": core._client.hget(keys.meta, "last_ts"),
            "queues": core._client.smembers(keys.queues),
            "bodies": core._client.hgetall(keys.bodies),
            "all_ids": core._client.zrange(keys.all_ids, 0, -1),
        }

        result = core._client.eval(
            scripts.BROADCAST_MESSAGE,
            4,
            keys.meta,
            keys.bodies,
            keys.all_ids,
            keys.queues,
            "2",
            encode_id(2),
            "2",
            "announcement",
            keys.key("q", ""),
            "exact_create",
            "2",
            "new-a",
            "new-b",
            encode_id(1),
            encode_id(2),
        )

        assert result == [-6]
        assert core._client.smembers(keys.queues) == state_before["queues"]
        assert not core._client.sismember(keys.queues, "new-a")
        assert not core._client.sismember(keys.queues, "new-b")
        assert core._client.exists(keys.pending("new-a")) == 0
        assert core._client.exists(keys.pending("new-b")) == 0
        assert core._client.hget(keys.meta, "last_ts") == state_before["last_ts"]
        assert core._client.hgetall(keys.bodies) == state_before["bodies"]
        assert core._client.zrange(keys.all_ids, 0, -1) == state_before["all_ids"]
    finally:
        core.close()


def test_exact_create_script_rejects_uninitialized_namespace_before_mutation(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    magic = core._client.hget(keys.meta, "magic")
    try:
        last_ts_before = core._client.hget(keys.meta, "last_ts")
        core._client.hdel(keys.meta, "magic")

        result = core._client.eval(
            scripts.BROADCAST_MESSAGE,
            4,
            keys.meta,
            keys.bodies,
            keys.all_ids,
            keys.queues,
            "1",
            encode_id(1),
            "1",
            "announcement",
            keys.key("q", ""),
            "exact_create",
            "1",
            "new",
            encode_id(1),
        )

        assert result == [-2]
        assert not core._client.sismember(keys.queues, "new")
        assert core._client.exists(keys.pending("new")) == 0
        assert core._client.hlen(keys.bodies) == 0
        assert core._client.zcard(keys.all_ids) == 0
        assert core._client.hget(keys.meta, "last_ts") == last_ts_before
    finally:
        if magic is not None:
            core._client.hset(keys.meta, "magic", magic)
        core.close()


def test_exact_create_rejects_pending_layout_before_registry_or_message_mutation(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    keys = RedisKeys(redis_runner.namespace)
    try:
        core._client.set(keys.pending("new-a"), "wrong-type")
        state_before = {
            "last_ts": core._client.hget(keys.meta, "last_ts"),
            "bodies": core._client.hgetall(keys.bodies),
            "all_ids": core._client.zrange(keys.all_ids, 0, -1),
        }

        with pytest.raises(OperationalError, match="WRONGTYPE"):
            core.broadcast(
                "announcement",
                queue_names=["new-a", "new-b"],
                create_missing=True,
            )

        assert not core._client.sismember(keys.queues, "new-a")
        assert not core._client.sismember(keys.queues, "new-b")
        assert core._client.get(keys.pending("new-a")) == "wrong-type"
        assert core._client.exists(keys.pending("new-b")) == 0
        assert core._client.hget(keys.meta, "last_ts") == state_before["last_ts"]
        assert core._client.hgetall(keys.bodies) == state_before["bodies"]
        assert core._client.zrange(keys.all_ids, 0, -1) == state_before["all_ids"]
    finally:
        core._client.delete(keys.pending("new-a"))
        core.close()


def test_patternless_broadcast_retries_when_queue_set_outgrows_timestamp_batch(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broadcaster = RedisBrokerCore(redis_runner)
    writer = RedisBrokerCore(redis_runner)
    try:
        broadcaster.write("existing", "seed")
        original_generate = broadcaster.generate_timestamp
        grew = False

        def grow_before_first_timestamp() -> int:
            nonlocal grew
            if not grew:
                grew = True
                for index in range(10):
                    writer.write(f"new-{index}", "seed")
            return original_generate()

        monkeypatch.setattr(
            broadcaster,
            "generate_timestamp",
            grow_before_first_timestamp,
        )

        assert broadcaster.broadcast("announcement") == 11
        for queue in ["existing", *(f"new-{index}" for index in range(10))]:
            assert (
                broadcaster.peek_many(queue, limit=10, with_timestamps=False)[-1]
                == "announcement"
            )
    finally:
        writer.close()
        broadcaster.close()
