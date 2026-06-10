"""include_claimed peek semantics on the Redis backend.

Redis keeps claimed rows in a per-queue "claimed" ZSET parallel to "pending"
(bodies shared). include_claimed=True must merge both, ordered by message ID.
"""

from __future__ import annotations

from typing import cast

import pytest
from simplebroker_redis import RedisRunner

from simplebroker import Queue

pytestmark = [pytest.mark.redis_only]


def _seeded_queue(redis_runner: RedisRunner) -> tuple[Queue, list[int]]:
    queue = Queue("jobs", runner=redis_runner, persistent=True)
    for i in range(4):
        queue.write(f"m{i}")
    rows = cast("list[tuple[str, int]]", queue.peek_many(4, with_timestamps=True))
    ids = [ts for _body, ts in rows]
    return queue, ids


def test_default_peek_excludes_claimed(redis_runner: RedisRunner) -> None:
    queue, _ids = _seeded_queue(redis_runner)
    try:
        assert queue.read() == "m0"
        assert queue.peek_many(10) == ["m1", "m2", "m3"]
    finally:
        queue.close()


def test_include_claimed_merges_zsets_in_id_order(
    redis_runner: RedisRunner,
) -> None:
    queue, ids = _seeded_queue(redis_runner)
    try:
        assert queue.read() == "m0"
        assert queue.read() == "m1"
        rows = cast(
            "list[tuple[str, int]]",
            queue.peek_many(10, with_timestamps=True, include_claimed=True),
        )
        assert [body for body, _ in rows] == ["m0", "m1", "m2", "m3"]
        assert [ts for _, ts in rows] == ids
        # limit and bounds apply to the merged stream
        assert queue.peek_many(2, include_claimed=True) == ["m0", "m1"]
        rows = cast(
            "list[tuple[str, int]]",
            queue.peek_many(
                10, with_timestamps=True, include_claimed=True, after_timestamp=ids[1]
            ),
        )
        assert [body for body, _ in rows] == ["m2", "m3"]
    finally:
        queue.close()


def test_exact_id_and_generator(redis_runner: RedisRunner) -> None:
    queue, ids = _seeded_queue(redis_runner)
    try:
        assert queue.read() == "m0"
        assert queue.peek_one(exact_timestamp=ids[0]) is None
        assert queue.peek_one(exact_timestamp=ids[0], include_claimed=True) == "m0"
        with queue.get_connection() as conn:
            bodies = list(
                conn.peek_generator(
                    "jobs", batch_size=1, with_timestamps=False, include_claimed=True
                )
            )
        assert bodies == ["m0", "m1", "m2", "m3"]
    finally:
        queue.close()
