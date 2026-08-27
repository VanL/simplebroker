"""Real Valkey/Redis proofs for bounded public message-ID order."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Literal

import pytest
from simplebroker_redis import RedisRunner, get_backend_plugin
from simplebroker_redis.core import RedisBrokerCore

pytestmark = [pytest.mark.redis_only]


def _insert_exact(
    core: RedisBrokerCore,
    queue: str,
    timestamps: list[int],
) -> None:
    core.insert_messages(
        [(queue, f"id-{timestamp}", timestamp) for timestamp in timestamps]
    )


def test_redis_one_many_bounds_and_live_order_use_public_ids(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        _insert_exact(core, "peek", [300, 100, 200])
        assert core.peek_one("peek", order="oldest") == ("id-100", 100)
        assert core.peek_one("peek", order="newest") == ("id-300", 300)
        assert core.peek_many("peek", 2, order="oldest") == [
            ("id-100", 100),
            ("id-200", 200),
        ]
        assert core.peek_many("peek", 2, order="newest") == [
            ("id-300", 300),
            ("id-200", 200),
        ]
        assert core.peek_many(
            "peek",
            3,
            after_timestamp=100,
            before_timestamp=300,
            order="newest",
        ) == [("id-200", 200)]
        assert list(core.peek_generator("peek")) == [
            ("id-100", 100),
            ("id-200", 200),
            ("id-300", 300),
        ]

        _insert_exact(core, "claim", [600, 400, 500])
        assert core.claim_many("claim", 2, order="newest") == [
            ("id-600", 600),
            ("id-500", 500),
        ]

        _insert_exact(core, "move", [900, 700, 800])
        assert core.move_many("move", "archive", 2, order="newest") == [
            ("id-900", 900),
            ("id-800", 800),
        ]
        assert core.peek_many("archive", 10) == [
            ("id-800", 800),
            ("id-900", 900),
        ]
    finally:
        core.close()


def test_redis_newest_include_claimed_merges_both_states(
    redis_runner: RedisRunner,
) -> None:
    core = RedisBrokerCore(redis_runner)
    try:
        _insert_exact(core, "jobs", [300, 100, 200])
        assert core.claim_one("jobs", order="oldest") == ("id-100", 100)
        assert core.peek_many("jobs", 3, include_claimed=True, order="newest") == [
            ("id-300", 300),
            ("id-200", 200),
            ("id-100", 100),
        ]
    finally:
        core.close()


@pytest.mark.parametrize("operation", ["claim", "move"])
def test_redis_newest_lua_resumes_below_reserved_windows(
    redis_runner: RedisRunner,
    operation: Literal["claim", "move"],
) -> None:
    core = RedisBrokerCore(redis_runner)
    token = ""
    reserved_rows: list[tuple[str, int]] = []
    try:
        # A limit-one Lua call examines at most 256 candidates before it
        # returns a cursor. Reserve 300 higher IDs while leaving ID 1 eligible
        # so descending continuation must use an exclusive upper bound.
        _insert_exact(core, "jobs", [1, *range(1000, 1300)])
        token, reserved_rows = core._begin_batch(
            "jobs",
            batch_size=300,
            after_timestamp=1,
            before_timestamp=None,
            exact_timestamp=None,
            op="claim",
        )
        assert len(reserved_rows) == 300

        if operation == "claim":
            assert core.claim_one("jobs", order="newest") == ("id-1", 1)
        else:
            assert core.move_one("jobs", "archive", order="newest") == (
                "id-1",
                1,
            )
    finally:
        if token:
            core._rollback_batch("jobs", token, reserved_rows)
        core.close()


def test_redis_concurrent_newest_claims_select_distinct_highest_ids(
    redis_url: str,
    redis_namespace: str,
) -> None:
    plugin = get_backend_plugin()
    plugin.initialize_target(redis_url, backend_options={"namespace": redis_namespace})
    seed_runner = RedisRunner(redis_url, namespace=redis_namespace)
    seed = RedisBrokerCore(seed_runner)
    runners = [RedisRunner(redis_url, namespace=redis_namespace) for _ in range(2)]
    cores = [RedisBrokerCore(runner) for runner in runners]
    ready = threading.Barrier(2)
    try:
        _insert_exact(seed, "jobs", [100, 200, 300])

        def claim(core: RedisBrokerCore) -> tuple[str, int] | None:
            ready.wait()
            result = core.claim_one("jobs", order="newest")
            assert result is None or isinstance(result, tuple)
            return result

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(claim, cores))

        assert {result[1] for result in results if result is not None} == {200, 300}
        assert seed.peek_one("jobs") == ("id-100", 100)
    finally:
        for core in cores:
            core.shutdown()
        seed.shutdown()
        plugin.cleanup_target(redis_url, backend_options={"namespace": redis_namespace})
