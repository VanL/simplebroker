"""Shared tests for finding message IDs by message body substring."""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.shared]


def _timestamp_by_body(rows: list[tuple[str, int]]) -> dict[str, int]:
    return dict(rows)


def test_find_message_ids_finds_literal_substring_in_one_queue_only(broker) -> None:
    broker.write("jobs", "tenant:acme invoice 1")
    broker.write("jobs", "tenant:acme invoice 2")
    broker.write("jobs", "tenant:globex invoice 1")
    broker.write("other", "tenant:acme invoice 3")
    timestamps = _timestamp_by_body(broker.peek_many("jobs", limit=10))

    ids = broker.find_message_ids(
        "jobs",
        body_contains="tenant:acme",
        limit=10,
    )

    assert ids == [
        timestamps["tenant:acme invoice 1"],
        timestamps["tenant:acme invoice 2"],
    ]
    assert broker.peek_many("jobs", limit=10, with_timestamps=False) == [
        "tenant:acme invoice 1",
        "tenant:acme invoice 2",
        "tenant:globex invoice 1",
    ]


def test_find_message_ids_uses_literal_matching_not_patterns(broker) -> None:
    broker.write("jobs", "progress 100% ready")
    broker.write("jobs", "progress 100x ready")
    broker.write("jobs", "code a_c ready")
    broker.write("jobs", "code abc ready")
    timestamps = _timestamp_by_body(broker.peek_many("jobs", limit=10))

    assert broker.find_message_ids("jobs", body_contains="100%", limit=10) == [
        timestamps["progress 100% ready"]
    ]
    assert broker.find_message_ids("jobs", body_contains="a_c", limit=10) == [
        timestamps["code a_c ready"]
    ]


def test_find_message_ids_is_case_sensitive(broker) -> None:
    broker.write("jobs", "Needle")
    broker.write("jobs", "needle")
    timestamps = _timestamp_by_body(broker.peek_many("jobs", limit=10))

    ids = broker.find_message_ids("jobs", body_contains="Needle", limit=10)

    assert ids == [timestamps["Needle"]]


def test_find_message_ids_claimed_visibility(broker) -> None:
    broker.write("jobs", "target pending")
    broker.write("jobs", "target claimed")
    timestamps = _timestamp_by_body(broker.peek_many("jobs", limit=10))

    assert (
        broker.claim_one(
            "jobs",
            exact_timestamp=timestamps["target claimed"],
            with_timestamps=False,
        )
        == "target claimed"
    )

    visible_ids = broker.find_message_ids("jobs", body_contains="target", limit=10)
    all_ids = broker.find_message_ids(
        "jobs",
        body_contains="target",
        limit=10,
        include_claimed=True,
    )

    assert visible_ids == [timestamps["target pending"]]
    assert all_ids == [
        timestamps["target pending"],
        timestamps["target claimed"],
    ]


def test_find_message_ids_timestamp_bounds_are_strict(broker) -> None:
    broker.write("jobs", "target old")
    broker.write("jobs", "target boundary")
    boundary_ts = _timestamp_by_body(broker.peek_many("jobs", limit=10))[
        "target boundary"
    ]
    broker.write("jobs", "target new")
    timestamps = _timestamp_by_body(broker.peek_many("jobs", limit=10))

    before_ids = broker.find_message_ids(
        "jobs",
        body_contains="target",
        limit=10,
        before_timestamp=boundary_ts,
    )
    after_ids = broker.find_message_ids(
        "jobs",
        body_contains="target",
        limit=10,
        after_timestamp=boundary_ts,
    )
    between_ids = broker.find_message_ids(
        "jobs",
        body_contains="target",
        limit=10,
        after_timestamp=boundary_ts,
        before_timestamp=boundary_ts,
    )

    assert before_ids == [timestamps["target old"]]
    assert after_ids == [timestamps["target new"]]
    assert between_ids == []


def test_find_message_ids_limit_caps_returned_matches(broker) -> None:
    for index in range(5):
        broker.write("jobs", f"target {index}")
    timestamps = _timestamp_by_body(broker.peek_many("jobs", limit=10))

    ids = broker.find_message_ids("jobs", body_contains="target", limit=2)

    assert ids == [
        timestamps["target 0"],
        timestamps["target 1"],
    ]


def test_find_message_ids_missing_queue_and_no_matches_return_empty(broker) -> None:
    broker.write("jobs", "ordinary message")

    assert broker.find_message_ids("missing", body_contains="target", limit=10) == []
    assert broker.find_message_ids("jobs", body_contains="absent", limit=10) == []


@pytest.mark.parametrize(
    ("kwargs", "exception"),
    [
        ({"body_contains": "target"}, ValueError),
        ({"body_contains": 123}, TypeError),
        ({"body_contains": ""}, ValueError),
        ({"body_contains": "   "}, ValueError),
        ({"body_contains": "ab"}, ValueError),
        ({"body_contains": "a b"}, ValueError),
        ({"body_contains": "x" * 1025}, ValueError),
        ({"body_contains": "target", "limit": 0}, ValueError),
        ({"body_contains": "target", "limit": 1001}, ValueError),
        ({"body_contains": "target", "limit": True}, TypeError),
        ({"body_contains": "target", "before_timestamp": -1}, ValueError),
    ],
)
def test_find_message_ids_validation_fails_before_scan(
    broker,
    kwargs: dict[str, object],
    exception: type[Exception],
) -> None:
    broker.write("jobs", "target message")
    queue = "bad queue" if kwargs == {"body_contains": "target"} else "jobs"

    with pytest.raises(exception):
        broker.find_message_ids(queue, **kwargs)

    assert broker.peek_many("jobs", limit=10, with_timestamps=False) == [
        "target message"
    ]
