"""Tests for targeted queue metadata APIs."""

from __future__ import annotations

import pytest

from simplebroker import QueueStats

pytestmark = [pytest.mark.shared]


def _stats_by_name(stats: list[QueueStats]) -> dict[str, QueueStats]:
    return {item.queue: item for item in stats}


def test_queue_stats_for_pending_messages(queue_factory, broker) -> None:
    queue = queue_factory("jobs")
    for message in ("one", "two", "three"):
        queue.write(message)

    queue_stats = queue.stats()
    broker_stats = broker.get_queue_stat("jobs")

    assert queue_stats == QueueStats(queue="jobs", pending=3, claimed=0, total=3)
    assert broker_stats == queue_stats
    assert queue_stats.exists is True
    assert queue.exists() is True
    assert broker.queue_exists("jobs") is True


def test_queue_stats_include_claimed_messages(queue_factory) -> None:
    queue = queue_factory("jobs")
    for message in ("one", "two", "three"):
        queue.write(message)

    assert queue.read() == "one"

    stats = queue.stats()
    assert stats == QueueStats(queue="jobs", pending=2, claimed=1, total=3)
    assert stats.exists is True
    assert stats.pending + stats.claimed == stats.total
    assert queue.has_pending() is True


def test_missing_queue_stats_are_zero(queue_factory, broker) -> None:
    queue = queue_factory("missing")

    stats = queue.stats()
    assert stats == QueueStats(queue="missing", pending=0, claimed=0, total=0)
    assert stats.exists is False
    assert queue.exists() is False
    assert broker.queue_exists("missing") is False
    assert broker.get_queue_stat("missing") == stats


def test_vacuum_removes_claimed_only_queue_existence(queue_factory, broker) -> None:
    queue = queue_factory("jobs")
    queue.write("one")

    assert queue.read() == "one"
    assert queue.exists() is True
    assert queue.stats() == QueueStats(queue="jobs", pending=0, claimed=1, total=1)

    broker.vacuum()

    assert queue.exists() is False
    assert queue.stats() == QueueStats(queue="jobs", pending=0, claimed=0, total=0)


def test_list_queue_stats_with_prefix(queue_factory, broker) -> None:
    for name in ("weft.jobs.a", "weft.jobs.b", "weft.events.a", "other"):
        queue_factory(name).write(f"message for {name}")

    assert queue_factory("weft.jobs.b").read() == "message for weft.jobs.b"

    stats = broker.list_queue_stats(prefix="weft.jobs.")

    assert [item.queue for item in stats] == ["weft.jobs.a", "weft.jobs.b"]
    by_name = _stats_by_name(stats)
    assert by_name["weft.jobs.a"] == QueueStats(
        queue="weft.jobs.a", pending=1, claimed=0, total=1
    )
    assert by_name["weft.jobs.b"] == QueueStats(
        queue="weft.jobs.b", pending=0, claimed=1, total=1
    )


def test_list_queue_stats_with_pattern(queue_factory, broker) -> None:
    for name in ("weft.jobs.a", "weft.jobs.b", "weft.events.a", "other"):
        queue_factory(name).write(f"message for {name}")

    job_stats = broker.list_queue_stats(pattern="weft.jobs.*")
    suffix_stats = broker.list_queue_stats(pattern="*.a")

    assert [item.queue for item in job_stats] == ["weft.jobs.a", "weft.jobs.b"]
    assert [item.queue for item in suffix_stats] == [
        "weft.events.a",
        "weft.jobs.a",
    ]


def test_list_queue_stats_rejects_prefix_and_pattern(broker) -> None:
    with pytest.raises(ValueError, match="prefix.*pattern|pattern.*prefix"):
        broker.list_queue_stats(prefix="weft.", pattern="weft.*")


def test_list_queues_returns_names_only_and_includes_claimed(
    queue_factory, broker
) -> None:
    queue_factory("jobs.pending").write("one")
    queue_factory("jobs.claimed").write("two")
    queue_factory("events").write("three")

    assert broker.claim_one("jobs.claimed", with_timestamps=False) == "two"

    queues = broker.list_queues()

    assert queues == ["events", "jobs.claimed", "jobs.pending"]
    assert all(isinstance(item, str) for item in queues)
    assert broker.get_queue_stats() == [
        ("events", 1, 1),
        ("jobs.claimed", 0, 1),
        ("jobs.pending", 1, 1),
    ]


def test_list_queues_filters_by_prefix(queue_factory, broker) -> None:
    for name in ("weft.jobs.a", "weft.jobs.b", "weft.events.a", "other"):
        queue_factory(name).write(f"message for {name}")

    assert broker.list_queues(prefix="weft.jobs.") == [
        "weft.jobs.a",
        "weft.jobs.b",
    ]


def test_list_queues_filters_by_pattern(queue_factory, broker) -> None:
    for name in ("weft.jobs.a", "weft.jobs.b", "weft.events.a", "other"):
        queue_factory(name).write(f"message for {name}")

    assert broker.list_queues(pattern="weft.jobs.*") == [
        "weft.jobs.a",
        "weft.jobs.b",
    ]
    assert broker.list_queues(pattern="*.a") == [
        "weft.events.a",
        "weft.jobs.a",
    ]


def test_list_queues_rejects_prefix_and_pattern(broker) -> None:
    with pytest.raises(ValueError, match="prefix.*pattern|pattern.*prefix"):
        broker.list_queues(prefix="weft.", pattern="weft.*")


def test_list_queues_drops_queue_after_rows_removed(queue_factory, broker) -> None:
    queue = queue_factory("jobs")
    queue.write("one")

    assert broker.list_queues() == ["jobs"]

    assert broker.delete("jobs") == 1
    assert broker.list_queues() == []
