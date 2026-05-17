"""Shared tests for physical batch deletion by message ID."""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.shared]


def _timestamp_by_body(rows: list[tuple[str, int]]) -> dict[str, int]:
    return dict(rows)


def test_delete_message_ids_deletes_claimed_and_pending_rows(broker) -> None:
    for index in range(1, 6):
        broker.write("jobs", f"message{index}")

    timestamps = _timestamp_by_body(broker.peek_many("jobs", limit=10))
    claimed_ts = timestamps["message2"]
    pending_ts = timestamps["message3"]

    assert (
        broker.claim_one("jobs", exact_timestamp=claimed_ts, with_timestamps=False)
        == "message2"
    )
    before = broker.get_queue_stat("jobs")
    assert before.pending == 4
    assert before.claimed == 1
    assert before.total == 5

    deleted = broker.delete_message_ids(
        "jobs", [claimed_ts, pending_ts, 999999999999999999]
    )

    assert deleted == 2
    after = broker.get_queue_stat("jobs")
    assert after.pending == 3
    assert after.claimed == 0
    assert after.pending + after.claimed == after.total
    assert broker.peek_many("jobs", limit=10, with_timestamps=False) == [
        "message1",
        "message4",
        "message5",
    ]


def test_delete_message_ids_counts_duplicates_once(broker) -> None:
    broker.write("jobs", "message1")
    broker.write("jobs", "message2")
    timestamps = _timestamp_by_body(broker.peek_many("jobs", limit=10))

    deleted = broker.delete_message_ids(
        "jobs", [timestamps["message1"], timestamps["message1"]]
    )

    assert deleted == 1
    assert broker.get_queue_stat("jobs").total == 1
    assert broker.peek_many("jobs", limit=10, with_timestamps=False) == ["message2"]


def test_delete_message_ids_empty_batch_is_noop(broker) -> None:
    broker.write("jobs", "message1")
    before = broker.get_queue_stat("jobs")

    assert broker.delete_message_ids("jobs", []) == 0

    after = broker.get_queue_stat("jobs")
    assert (after.pending, after.claimed, after.total) == (
        before.pending,
        before.claimed,
        before.total,
    )


def test_delete_message_ids_does_not_cross_queues(broker) -> None:
    broker.write("a", "message-a")
    broker.write("b", "message-b")
    timestamps = _timestamp_by_body(broker.peek_many("a", limit=10))

    assert broker.delete_message_ids("b", [timestamps["message-a"]]) == 0
    assert broker.peek_many("a", limit=10, with_timestamps=False) == ["message-a"]
    assert broker.peek_many("b", limit=10, with_timestamps=False) == ["message-b"]


def test_queue_delete_many_uses_physical_batch_delete(queue_factory) -> None:
    q = queue_factory("jobs")
    q.write("message1")
    q.write("message2")
    q.write("message3")
    timestamps = _timestamp_by_body(list(q.peek_generator(with_timestamps=True)))

    deleted = q.delete_many([timestamps["message1"], timestamps["message3"]])

    assert deleted == 2
    assert list(q.peek_generator(with_timestamps=False)) == ["message2"]
