"""Shared tests for the latest pending timestamp API."""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.shared]


def test_latest_pending_timestamp_empty_queue_returns_none(
    queue_factory,
    broker,
) -> None:
    queue = queue_factory("jobs")

    assert queue.latest_pending_timestamp() is None
    assert broker.latest_pending_timestamp("jobs") is None


def test_latest_pending_timestamp_returns_newest_pending_timestamp(
    queue_factory,
    broker,
) -> None:
    queue = queue_factory("jobs")
    queue.insert_messages(
        [
            ("old", 10),
            ("middle", 20),
            ("newest", 30),
        ]
    )

    assert queue.latest_pending_timestamp() == 30
    assert broker.latest_pending_timestamp("jobs") == 30


def test_latest_pending_timestamp_ignores_claimed_messages(queue_factory) -> None:
    queue = queue_factory("jobs")
    queue.insert_messages(
        [
            ("old", 10),
            ("middle", 20),
            ("claimed newest", 30),
        ]
    )

    assert queue.read_one(exact_timestamp=30, with_timestamps=True) == (
        "claimed newest",
        30,
    )

    assert queue.latest_pending_timestamp() == 20


def test_latest_pending_timestamp_claimed_only_queue_returns_none(
    queue_factory,
) -> None:
    queue = queue_factory("jobs")
    queue.insert_messages(
        [
            ("old", 10),
            ("newest", 20),
        ]
    )

    assert queue.read_many(2, with_timestamps=True) == [("old", 10), ("newest", 20)]

    assert queue.latest_pending_timestamp() is None


def test_latest_pending_timestamp_is_queue_scoped(queue_factory) -> None:
    jobs = queue_factory("jobs")
    other = queue_factory("other")
    jobs.insert_messages([("jobs message", 10)])
    other.insert_messages([("other message", 40)])

    assert jobs.latest_pending_timestamp() == 10
    assert other.latest_pending_timestamp() == 40


def test_latest_pending_timestamp_ignores_generated_timestamp_without_row(
    queue_factory,
) -> None:
    queue = queue_factory("jobs")
    generated = queue.generate_timestamp()
    queue.insert_messages([("real row", 10)])

    assert generated > 10
    assert queue.latest_pending_timestamp() == 10


def test_latest_pending_timestamp_does_not_mutate_queue(
    queue_factory,
) -> None:
    queue = queue_factory("jobs")
    queue.insert_messages(
        [
            ("old", 10),
            ("newest", 20),
        ]
    )

    before_peek = queue.peek_many(limit=10, with_timestamps=True)
    before_stats = queue.stats()

    assert queue.latest_pending_timestamp() == 20

    assert queue.peek_many(limit=10, with_timestamps=True) == before_peek
    assert queue.stats() == before_stats
    assert queue.read_many(10, with_timestamps=True) == before_peek
