"""Shared tests for deleting messages from multiple queues."""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.shared]


def _timestamp_by_body(rows: list[tuple[str, int]]) -> dict[str, int]:
    return dict(rows)


def test_delete_from_queues_deletes_selected_queues_only(broker) -> None:
    broker.write("alpha", "alpha-1")
    broker.write("alpha", "alpha-2")
    broker.write("beta", "beta-1")
    broker.write("gamma", "gamma-1")

    alpha_timestamps = _timestamp_by_body(broker.peek_many("alpha", limit=10))
    assert (
        broker.claim_one(
            "alpha",
            exact_timestamp=alpha_timestamps["alpha-1"],
            with_timestamps=False,
        )
        == "alpha-1"
    )

    deleted = broker.delete_from_queues(["alpha", "beta"])

    assert deleted == 3
    assert broker.peek_many("alpha", limit=10, with_timestamps=False) == []
    assert broker.peek_many("beta", limit=10, with_timestamps=False) == []
    assert broker.peek_many("gamma", limit=10, with_timestamps=False) == ["gamma-1"]
    assert broker.get_queue_stat("alpha").total == 0
    assert broker.get_queue_stat("beta").total == 0
    assert broker.get_queue_stat("gamma").total == 1


def test_delete_from_queues_before_timestamp_is_strict(broker) -> None:
    broker.write("alpha", "old-alpha")
    broker.write("beta", "old-beta")
    broker.write("gamma", "old-gamma")
    broker.write("alpha", "boundary-alpha")
    boundary_ts = _timestamp_by_body(broker.peek_many("alpha", limit=10))[
        "boundary-alpha"
    ]
    broker.write("alpha", "new-alpha")
    broker.write("beta", "new-beta")

    deleted = broker.delete_from_queues(
        ["alpha", "beta"],
        before_timestamp=boundary_ts,
    )

    assert deleted == 2
    assert broker.peek_many("alpha", limit=10, with_timestamps=False) == [
        "boundary-alpha",
        "new-alpha",
    ]
    assert broker.peek_many("beta", limit=10, with_timestamps=False) == ["new-beta"]
    assert broker.peek_many("gamma", limit=10, with_timestamps=False) == ["old-gamma"]


def test_delete_from_queues_counts_duplicate_queue_names_once(broker) -> None:
    broker.write("alpha", "alpha-1")
    broker.write("beta", "beta-1")

    deleted = broker.delete_from_queues(["alpha", "alpha", "beta", "alpha"])

    assert deleted == 2
    assert broker.get_queue_stat("alpha").total == 0
    assert broker.get_queue_stat("beta").total == 0


def test_delete_from_queues_empty_input_is_noop(broker) -> None:
    broker.write("alpha", "alpha-1")

    deleted = broker.delete_from_queues([])

    assert deleted == 0
    assert broker.peek_many("alpha", limit=10, with_timestamps=False) == ["alpha-1"]


def test_delete_from_queues_rejects_bare_string_before_mutation(broker) -> None:
    broker.write("alpha", "alpha-1")

    with pytest.raises(TypeError):
        broker.delete_from_queues("alpha")

    assert broker.peek_many("alpha", limit=10, with_timestamps=False) == ["alpha-1"]


def test_delete_from_queues_invalid_queue_fails_before_mutation(broker) -> None:
    broker.write("alpha", "alpha-1")
    broker.write("beta", "beta-1")

    with pytest.raises(ValueError):
        broker.delete_from_queues(["alpha", "bad queue name", "beta"])

    assert broker.peek_many("alpha", limit=10, with_timestamps=False) == ["alpha-1"]
    assert broker.peek_many("beta", limit=10, with_timestamps=False) == ["beta-1"]


def test_delete_from_queues_invalid_before_timestamp_fails_before_mutation(
    broker,
) -> None:
    broker.write("alpha", "alpha-1")

    with pytest.raises(ValueError):
        broker.delete_from_queues(["alpha"], before_timestamp=-1)

    assert broker.peek_many("alpha", limit=10, with_timestamps=False) == ["alpha-1"]
