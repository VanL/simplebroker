"""Focused Redis direct-core behavior tests."""

from __future__ import annotations

import gc
import threading
import time
import warnings
import weakref
from collections.abc import Iterator
from contextlib import ExitStack
from typing import NoReturn

import pytest
import redis
from redis.typing import EncodableT, KeyT
from simplebroker_redis import RedisRunner, get_backend_plugin
from simplebroker_redis import core as redis_core_module
from simplebroker_redis.core import RedisBrokerCore
from simplebroker_redis.keys import RedisKeys

from simplebroker import BrokerTarget, Queue, resolve_config
from simplebroker._exceptions import (
    IntegrityError,
    MessageError,
    OperationalError,
    TimestampError,
)

pytestmark = [pytest.mark.redis_only]


def test_process_write_lock_registry_uses_weak_plain_locks() -> None:
    registry = redis_core_module._ProcessWriteLockRegistry()

    lock = registry.get("redis://example/0", "tenant")
    retained = weakref.ref(lock)

    assert type(lock) is type(threading.Lock())
    assert registry.get("redis://example/0", "tenant") is lock
    assert registry.get("redis://example/0", "other") is not lock

    del lock
    gc.collect()

    assert retained() is None
    assert len(registry._locks) == 0
    replacement = registry.get("redis://example/0", "tenant")
    assert type(replacement) is type(threading.Lock())
    assert len(registry._locks) == 1


def test_queue_move_rejects_mutated_descriptor_namespace(
    redis_url: str, redis_namespace: str
) -> None:
    plugin = get_backend_plugin()
    other_namespace = f"{redis_namespace}_other"
    target = BrokerTarget("redis", redis_url, {"namespace": redis_namespace})
    config = resolve_config(override={})
    try:
        with ExitStack() as stack:
            source = stack.enter_context(
                Queue("source", db_path=target, persistent=True, config=config)
            )
            message_id = source.write("original payload")
            target.backend_options["namespace"] = other_namespace
            destination = stack.enter_context(
                Queue("destination", db_path=target, persistent=True, config=config)
            )
            same_target_destination = stack.enter_context(
                Queue(
                    "destination",
                    db_path=BrokerTarget(
                        "redis", redis_url, {"namespace": redis_namespace}
                    ),
                    config=config,
                )
            )

            with pytest.raises(ValueError, match="different broker targets"):
                source.move(destination)

            assert source.peek_one(with_timestamps=True) == (
                "original payload",
                message_id,
            )
            assert destination.peek() is None
            assert same_target_destination.peek() is None
            assert source.move(same_target_destination) == {
                "message": "original payload",
                "timestamp": message_id,
            }
            assert same_target_destination.peek() == "original payload"
            assert destination.peek() is None
    finally:
        for namespace in (redis_namespace, other_namespace):
            plugin.cleanup_target(redis_url, backend_options={"namespace": namespace})


def test_redis_timestamp_advance_transport_failure_is_ambiguous_after_real_eval(
    redis_runner: RedisRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = RedisBrokerCore(redis_runner)
    original_eval = core._client.eval
    floor = 1_000

    def eval_then_disconnect(
        script: str,
        numkeys: int,
        *keys_and_args: KeyT | EncodableT,
    ) -> NoReturn:
        original_eval(script, numkeys, *keys_and_args)
        raise redis.ConnectionError("injected disconnect after EVAL")

    try:
        monkeypatch.setattr(core._client, "eval", eval_then_disconnect)
        with pytest.raises(TimestampError, match="outcome is unknown") as exc_info:
            core.advance_last_timestamp(floor)
        assert exc_info.value.outcome_ambiguous is True

        monkeypatch.setattr(core._client, "eval", original_eval)
        assert core.refresh_last_timestamp() >= floor
    finally:
        core.close()


def test_keep_write_transport_failure_after_real_eval_leaves_whole_unit_committed(
    redis_runner: RedisRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = RedisBrokerCore(redis_runner)
    observer = RedisBrokerCore(redis_runner)
    original_eval = core._client.eval

    def eval_then_disconnect(
        script: str,
        numkeys: int,
        *keys_and_args: KeyT | EncodableT,
    ) -> NoReturn:
        original_eval(script, numkeys, *keys_and_args)
        raise redis.ConnectionError("injected disconnect after keep EVAL")

    try:
        core.write("jobs", "old-1")
        core.write("jobs", "old-2")
        monkeypatch.setattr(core._client, "eval", eval_then_disconnect)

        with pytest.raises(OperationalError, match="disconnect after keep EVAL"):
            core.write("jobs", "new", keep_newest=1)

        assert observer.peek_many("jobs", limit=10, with_timestamps=False) == ["new"]
        assert observer.peek_many(
            "jobs",
            limit=10,
            with_timestamps=False,
            include_claimed=True,
        ) == ["old-1", "old-2", "new"]
        stats = observer.get_queue_stat("jobs")
        assert (stats.pending, stats.claimed, stats.total) == (1, 2, 3)
    finally:
        observer.close()
        core.close()


def test_keep_write_publish_failure_leaves_whole_unit_committed(
    redis_runner: RedisRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = RedisBrokerCore(redis_runner)
    observer = RedisBrokerCore(redis_runner)
    waiter = get_backend_plugin().create_activity_waiter(
        target=redis_runner.target,
        backend_options={"namespace": redis_runner.namespace},
        queue_name="jobs",
        stop_event=threading.Event(),
    )
    assert waiter is not None

    def fail_publish(*_args: object, **_kwargs: object) -> NoReturn:
        raise OperationalError("injected publish failure")

    try:
        core.write("jobs", "old-1")
        core.write("jobs", "old-2")
        assert waiter.wait(2.0) is True
        monkeypatch.setattr(core._client, "publish", fail_publish)

        core.write("jobs", "new", keep_newest=1)

        assert waiter.wait(2.0) is True
        assert observer.peek_many("jobs", limit=10, with_timestamps=False) == ["new"]
        stats = observer.get_queue_stat("jobs")
        assert (stats.pending, stats.claimed, stats.total) == (1, 2, 3)
    finally:
        waiter.close()
        observer.close()
        core.close()


def test_redis_manual_vacuum_translates_client_errors(
    redis_runner: RedisRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = RedisBrokerCore(
        redis_runner, config=resolve_config(override={"BROKER_AUTO_VACUUM": 0})
    )
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
            "new",
        ]
        assert core.peek_many(
            "dest", limit=10, with_timestamps=True, include_claimed=True
        ) == [("old", 10), ("middle", 20), ("new", 30)]
        assert core.get_queue_stats() == [("dest", 2, 3)]
    finally:
        core.close()


def test_redis_move_pending_by_id_preserves_pending_state(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        core.insert_messages([("source", "pending", 10)])

        assert core.move_one(
            "source",
            "dest",
            exact_timestamp=10,
            require_unclaimed=False,
            with_timestamps=True,
        ) == ("pending", 10)

        assert core.peek_many("dest", limit=10, with_timestamps=False) == ["pending"]
        assert core.get_queue_stats() == [("dest", 1, 1)]
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
        config=resolve_config(override={"BROKER_MAX_MESSAGE_SIZE": 5}),
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


def test_redis_namespaces_isolate_public_queue_state(
    redis_url: str, redis_namespace: str
) -> None:
    first_namespace = f"{redis_namespace}-first"
    second_namespace = f"{redis_namespace}-second"
    plugin = get_backend_plugin()

    with ExitStack() as stack:
        stack.callback(
            plugin.cleanup_target,
            redis_url,
            backend_options={"namespace": first_namespace},
        )
        stack.callback(
            plugin.cleanup_target,
            redis_url,
            backend_options={"namespace": second_namespace},
        )
        first = RedisBrokerCore(RedisRunner(redis_url, namespace=first_namespace))
        stack.callback(first.shutdown)
        second = RedisBrokerCore(RedisRunner(redis_url, namespace=second_namespace))
        stack.callback(second.shutdown)

        first.write("jobs", "first-only")
        second.write("jobs", "second-only")

        assert first.peek_many("jobs", limit=10, with_timestamps=False) == [
            "first-only"
        ]
        assert second.peek_many("jobs", limit=10, with_timestamps=False) == [
            "second-only"
        ]


def test_redis_reserved_batch_rejects_unknown_token_without_mutation(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        core.insert_messages([("jobs", "first", 10), ("jobs", "second", 20)])
        before = core.peek_many(
            "jobs", limit=10, with_timestamps=True, include_claimed=True
        )

        with pytest.raises(OperationalError, match="invalid Redis claim batch"):
            core._commit_claim_batch(
                "jobs",
                "00000000000000000000000000000000",
                [("first", 10)],
            )

        assert (
            core.peek_many("jobs", limit=10, with_timestamps=True, include_claimed=True)
            == before
        )
        assert core.count_claimed_messages() == 0
    finally:
        core.close()


def test_fork_safety_replaces_inherited_write_lock(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = RedisBrokerCore(redis_runner)
    inherited_lock = core._write_lock
    inherited_lock.acquire()
    try:
        parent_pid = core._pid
        monkeypatch.setattr("simplebroker_redis.core.os.getpid", lambda: parent_pid + 1)
        core._check_fork_safety()

        assert core._write_lock is not inherited_lock
        assert core._write_lock.acquire(blocking=False)
        core._write_lock.release()
    finally:
        inherited_lock.release()
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
        # Aliases resolve only behind the "@" sigil; a plain name is literal.
        assert core.canonicalize_queue("existing") == "existing"
        assert core.canonicalize_queue("@existing") == "target"
        assert core.canonicalize_queue("plain") == "plain"
        assert core.list_aliases() == [("existing", "target")]
        assert core.aliases_for_target("target") == ["existing"]
        assert core.get_alias_version() > 0

        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
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


def test_redis_add_alias_translates_client_errors(
    redis_runner: RedisRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:

        def fail_eval(*_args: object, **_kwargs: object) -> NoReturn:
            raise redis.RedisError("injected alias failure")

        monkeypatch.setattr(core._client, "eval", fail_eval)

        with pytest.raises(
            OperationalError, match="injected alias failure"
        ) as exc_info:
            core.add_alias("alias", "target")

        assert isinstance(exc_info.value.__cause__, redis.RedisError)
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


@pytest.mark.parametrize("persistent", [False, True])
@pytest.mark.parametrize("method", ["move", "move_one", "move_many", "move_generator"])
def test_queue_move_rejects_config_derived_namespaces(
    redis_url: str, redis_namespace: str, persistent: bool, method: str
) -> None:
    other = f"{redis_namespace}_other"
    plugin = get_backend_plugin()
    try:
        with ExitStack() as stack:
            source = stack.enter_context(
                Queue(
                    "source",
                    db_path=BrokerTarget("redis", redis_url),
                    persistent=persistent,
                    config=resolve_config(
                        override={"BROKER_BACKEND_SCHEMA": redis_namespace}
                    ),
                )
            )
            destination = stack.enter_context(
                Queue(
                    "destination",
                    db_path=BrokerTarget("redis", redis_url),
                    persistent=persistent,
                    config=resolve_config(override={"BROKER_BACKEND_SCHEMA": other}),
                )
            )
            wrong_destination = stack.enter_context(
                Queue(
                    "destination",
                    db_path=BrokerTarget(
                        "redis", redis_url, {"namespace": redis_namespace}
                    ),
                )
            )
            message_id = source.write("namespace-owned payload")
            with pytest.raises(ValueError, match="different broker targets"):
                if method == "move_many":
                    source.move_many(destination, 1)
                elif method == "move_generator":
                    list(source.move_generator(destination))
                else:
                    getattr(source, method)(destination)
            assert source.peek_one(with_timestamps=True) == (
                "namespace-owned payload",
                message_id,
            )
            assert destination.peek() is None
            assert wrong_destination.peek() is None
    finally:
        for namespace in (redis_namespace, other):
            plugin.cleanup_target(redis_url, backend_options={"namespace": namespace})


@pytest.mark.parametrize(
    "options", [{}, {"schema": "selected"}, {"namespace": "selected"}]
)
def test_queue_effective_namespace_matches_explicit_target(
    redis_url: str, redis_namespace: str, options: dict[str, str]
) -> None:
    selected = dict.fromkeys(options, redis_namespace)
    config = resolve_config(
        override={
            "BROKER_BACKEND_SCHEMA": redis_namespace
            if not selected
            else f"{redis_namespace}_ignored",
            "BROKER_CACHE_MB": 11,
        }
    )
    original = BrokerTarget("redis", redis_url, selected)
    try:
        with (
            Queue("source", db_path=original, config=config) as source,
            Queue(
                "destination",
                db_path=BrokerTarget(
                    "redis", redis_url, {"namespace": redis_namespace}
                ),
                config=resolve_config(override={"BROKER_CACHE_MB": 17}),
            ) as destination,
        ):
            assert source._config is config
            bound = source.db_target
            assert isinstance(bound, BrokerTarget)
            assert bound.backend_options == {"namespace": redis_namespace}
            assert original.backend_options == selected
            bound.backend_options["namespace"] = "detached"
            message_id = source.write("same namespace")
            assert source.move(destination) == {
                "message": "same namespace",
                "timestamp": message_id,
            }
            assert destination.peek_one(with_timestamps=True) == (
                "same namespace",
                message_id,
            )
    finally:
        get_backend_plugin().cleanup_target(
            redis_url, backend_options={"namespace": redis_namespace}
        )


def test_redis_vacuum_drains_multiple_batches_and_honors_stop_event(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from simplebroker._exceptions import StopException

    stop_event = threading.Event()
    core = RedisBrokerCore(
        redis_runner,
        config=resolve_config(override={"BROKER_AUTO_VACUUM": 0}),
        stop_event=stop_event,
    )
    keys = RedisKeys(redis_runner.namespace)
    try:
        core.insert_messages([("jobs", f"msg-{idx}", idx + 1) for idx in range(2500)])
        assert len(core.claim_many("jobs", 2500, with_timestamps=False)) == 2500
        assert core.count_claimed_messages() == 2500

        core.vacuum()

        assert core.count_claimed_messages() == 0
        assert core.get_overall_stats() == (0, 0)
        assert core._client.hlen(keys.bodies) == 0
        assert core._client.zcard(keys.all_ids) == 0
        assert "jobs" not in core._client.smembers(keys.queues)

        core.insert_messages(
            [("jobs", f"msg2-{idx}", 10_000 + idx) for idx in range(2500)]
        )
        assert len(core.claim_many("jobs", 2500, with_timestamps=False)) == 2500

        real_eval = core._client.eval
        vacuum_evals = 0

        def stop_after_first_vacuum(script: str, numkeys: int, *args: object) -> object:
            nonlocal vacuum_evals
            result = real_eval(script, numkeys, *args)  # type: ignore[arg-type]
            if script == redis_core_module.scripts.VACUUM_CLAIMED:
                vacuum_evals += 1
                if vacuum_evals == 1:
                    stop_event.set()
            return result

        monkeypatch.setattr(core._client, "eval", stop_after_first_vacuum)
        with pytest.raises(StopException):
            core.vacuum()
        stop_event.clear()
        monkeypatch.setattr(core._client, "eval", real_eval)
        assert core.count_claimed_messages() == 1500
        core.vacuum()
        assert core.count_claimed_messages() == 0
    finally:
        core.close()


def _make_broken_redis_client(injected_exc: redis.RedisError) -> object:
    from contextlib import nullcontext

    class BrokenPipeTarget:
        def zscore(self, *_: object, **__: object) -> None:
            return None

        def execute(self) -> NoReturn:
            raise injected_exc

    class BrokenClient:
        def __getattr__(self, _name: str) -> object:
            if _name == "pipeline":
                return lambda *_, **__: nullcontext(BrokenPipeTarget())
            if _name == "scan_iter":

                def _broken_scan(*_: object, **__: object) -> Iterator[str]:
                    raise injected_exc
                    yield ""  # pragma: no cover

                return _broken_scan

            def _fail(*_: object, **__: object) -> NoReturn:
                raise injected_exc

            return _fail

    return BrokenClient()


@pytest.mark.parametrize(
    ("injected_exc", "expected_retryable"),
    [
        (redis.ConnectionError("conn drop"), None),
        (redis.BusyLoadingError("loading"), None),
        (redis.TimeoutError("timed out"), None),
        (redis.ResponseError("BUSY Redis is busy running a script"), True),
    ],
)
def test_redis_core_translates_client_errors_preserving_transport_ambiguity(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
    injected_exc: redis.RedisError,
    expected_retryable: bool | None,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        core.write("jobs", "seed")
        core.add_alias("alias", "jobs")
        monkeypatch.setattr(
            redis_runner, "_client", _make_broken_redis_client(injected_exc)
        )
        redis_runner._last_recovery_check_ns = 0

        calls = (
            core.list_queues,
            core.get_queue_stats,
            core.get_overall_stats,
            core.count_claimed_messages,
            core.get_alias_version,
            core.list_aliases,
            core.get_meta,
            lambda: core.peek_one("jobs"),
            lambda: core.peek_many("jobs", limit=5),
            lambda: core.claim_one("jobs"),
            lambda: core.find_message_ids("jobs", body_contains="seed"),
            lambda: core.has_pending_messages("jobs"),
            lambda: core.latest_pending_timestamp("jobs"),
            lambda: core.recover_stale_batches(max_age_seconds=0),
            lambda: core.resolve_alias("alias"),
            lambda: core.has_alias("alias"),
            lambda: core.remove_alias("alias"),
            lambda: core.broadcast("msg"),
        )
        for call in calls:
            redis_runner._last_recovery_check_ns = 0
            with pytest.raises(OperationalError) as exc_info:
                call()
            assert getattr(exc_info.value, "retryable", None) is expected_retryable
    finally:
        core.close()


def test_redis_generators_isolate_active_batches_across_threads_and_fork(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = RedisBrokerCore(redis_runner)
    gen_a = None
    try:
        core.insert_messages(
            [
                ("q_a", "a1", 10),
                ("q_a", "a2", 20),
                ("q_b", "b1", 30),
                ("q_b", "b2", 40),
            ]
        )
        gen_a = core.claim_generator(
            "q_a",
            delivery_guarantee="at_least_once",
            batch_size=2,
            with_timestamps=False,
        )
        assert next(gen_a) == "a1"

        with pytest.raises(
            RuntimeError,
            match=r"Queue\(\.\.\., persistent=False\), or an independent open_broker\(\)",
        ):
            core.write("q_a", "blocked_same_thread")

        sibling_results: list[object] = []
        sibling_error: list[BaseException] = []

        def run_sibling() -> None:
            try:
                claimed = list(
                    core.claim_generator(
                        "q_b",
                        delivery_guarantee="at_least_once",
                        batch_size=2,
                        with_timestamps=False,
                    )
                )
                core.write("q_b", "from_sibling")
                sibling_results.append(claimed)
            except (
                RuntimeError,
                OperationalError,
                AssertionError,
            ) as exc:  # pragma: no cover
                sibling_error.append(exc)

        worker = threading.Thread(target=run_sibling)
        worker.start()
        worker.join(timeout=5.0)
        assert not worker.is_alive()
        assert sibling_error == []
        assert sibling_results == [["b1", "b2"]]

        # Sibling batch completion must NOT clear Thread A's active batch guard.
        with pytest.raises(
            RuntimeError,
            match=r"at_least_once claim_generator batch is being yielded",
        ):
            core.write("q_a", "still_blocked_same_thread")

        assert list(gen_a) == ["a2"]
        gen_a = None

        # Simulate fork while a parent batch entry exists: child clears inherited guards.
        core._set_active_generator_batch("claim")
        parent_pid = core._pid
        monkeypatch.setattr("simplebroker_redis.core.os.getpid", lambda: parent_pid + 1)
        core._check_fork_safety()
        core._assert_no_reentrant_mutation_during_batch("write")
    finally:
        if gen_a is not None:
            gen_a.close()
        core.close()


@pytest.mark.parametrize("bad_batch_size", [0, -1, True, "10"])
@pytest.mark.parametrize("delivery", ["exactly_once", "at_least_once"])
def test_redis_generator_batch_size_validation_and_defaults(
    redis_runner: RedisRunner,
    bad_batch_size: object,
    delivery: str,
) -> None:
    core = RedisBrokerCore(
        redis_runner,
        config=resolve_config(override={"BROKER_GENERATOR_BATCH_SIZE": 2}),
    )
    try:
        with pytest.raises(ValueError, match="batch_size must be a positive integer"):
            next(
                core.claim_generator(
                    "jobs",
                    delivery_guarantee=delivery,  # type: ignore[arg-type]
                    batch_size=bad_batch_size,  # type: ignore[arg-type]
                )
            )
        with pytest.raises(ValueError, match="batch_size must be a positive integer"):
            next(
                core.move_generator(
                    "jobs",
                    "dest",
                    delivery_guarantee=delivery,  # type: ignore[arg-type]
                    batch_size=bad_batch_size,  # type: ignore[arg-type]
                )
            )
        with pytest.raises(ValueError, match="batch_size must be a positive integer"):
            next(
                core.peek_generator(
                    "jobs",
                    batch_size=bad_batch_size,  # type: ignore[arg-type]
                )
            )
    finally:
        core.close()


def test_redis_generator_batch_size_none_preserves_operation_defaults(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(
        redis_runner,
        config=resolve_config(override={"BROKER_GENERATOR_BATCH_SIZE": 2}),
    )
    observer = RedisBrokerCore(redis_runner)
    gen = None
    try:
        core.insert_messages(
            [("jobs", "m1", 10), ("jobs", "m2", 20), ("jobs", "m3", 30)]
        )
        assert len(list(core.peek_generator("jobs", batch_size=None))) == 3
        gen = core.claim_generator(
            "jobs",
            delivery_guarantee="at_least_once",
            batch_size=None,
            with_timestamps=False,
        )
        assert next(gen) == "m1"
        assert next(gen) == "m2"
        # Batch 1 commits on advancing to batch 2
        assert next(gen) == "m3"
        assert observer.count_claimed_messages() == 2
        gen.close()
        gen = None
        assert observer.count_claimed_messages() == 2
    finally:
        if gen is not None:
            gen.close()
        observer.close()
        core.close()


def test_redis_alias_mutations_increment_alias_version_monotonically_with_frozen_clock(
    redis_runner: RedisRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        assert "_alias_cache" not in core.__dict__
        assert "_load_aliases_locked" not in RedisBrokerCore.__dict__

        monkeypatch.setattr("simplebroker_redis.core.time.time_ns", lambda: 1_000_000)
        v0 = core.get_alias_version()

        core.add_alias("a1", "q_old")
        v1 = core.get_alias_version()
        assert v1 == v0 + 1

        core.write("q_old", "payload")
        core.rename_queue("q_old", "q_new")
        v2 = core.get_alias_version()
        assert v2 == v1 + 1

        core.remove_alias("a1")
        v3 = core.get_alias_version()
        assert v3 == v2 + 1
    finally:
        core.close()


@pytest.mark.parametrize("operation", ["claim", "move"])
def test_redis_foreign_thread_generator_close_unblocks_owner_thread(
    redis_runner: RedisRunner,
    operation: str,
) -> None:
    core = RedisBrokerCore(redis_runner)
    gen = None
    try:
        core.insert_messages([("src", "m1", 10), ("src", "m2", 20)])
        gen = (
            core.claim_generator(
                "src",
                delivery_guarantee="at_least_once",
                batch_size=2,
                with_timestamps=False,
            )
            if operation == "claim"
            else core.move_generator(
                "src",
                "dst",
                delivery_guarantee="at_least_once",
                batch_size=2,
                with_timestamps=False,
            )
        )
        assert next(gen) == "m1"

        close_errors: list[Exception] = []

        def close_from_foreign_thread() -> None:
            try:
                assert gen is not None
                gen.close()
            except (
                AssertionError,
                LookupError,
                OSError,
                RuntimeError,
                TypeError,
                ValueError,
            ) as exc:  # pragma: no cover - captured for assertion
                close_errors.append(exc)

        closer = threading.Thread(target=close_from_foreign_thread)
        closer.start()
        closer.join(timeout=5.0)
        assert not closer.is_alive()
        assert close_errors == []
        gen = None

        new_ts = core.write("src", "m3")
        assert isinstance(new_ts, int) and new_ts > 20
        assert core._active_generator_batch is None
    finally:
        if gen is not None:
            gen.close()
        core.close()
