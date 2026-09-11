"""Real-backend live keyset traversal proofs for [SB-DELIVERY-4]."""

from __future__ import annotations

from contextlib import closing
from itertools import islice
from typing import Any

import pytest

from simplebroker._constants import PEEK_BATCH_SIZE, SQLITE_MAX_INT64

from .helper_scripts.broker_factory import active_backend

pytestmark = pytest.mark.shared

# Small pages force several transitions without turning behavior tests into load tests.
PAGE_SIZE = 3


@pytest.mark.parametrize(
    "count", [0, 1, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, 3 * PAGE_SIZE + 1]
)
@pytest.mark.parametrize("with_timestamps", [False, True])
def test_fixed_data_pages(
    queue_factory: Any, count: int, with_timestamps: bool
) -> None:
    q = queue_factory("pages")
    expected = [(f"body-{i}", 10 * (i + 1)) for i in range(count)]
    q.insert_messages(reversed(expected))
    with q.get_connection() as core:
        rows = list(
            core.peek_generator(
                q.name, batch_size=PAGE_SIZE, with_timestamps=with_timestamps
            )
        )
    assert rows == (expected if with_timestamps else [body for body, _ in expected])
    assert list(q.peek_generator(with_timestamps=with_timestamps)) == rows


@pytest.mark.parametrize(
    "after,before,expected",
    [
        (None, None, [10, 20, 30, 40, 50, 60, 70]),
        (20, None, [30, 40, 50, 60, 70]),
        (None, 60, [10, 20, 30, 40, 50]),
        (10, 70, [20, 30, 40, 50, 60]),
        (30, 30, []),
        (50, 20, []),
    ],
)
def test_open_bounds_across_pages(
    queue_factory: Any, after: int | None, before: int | None, expected: list[int]
) -> None:
    q = queue_factory("bounds")
    q.insert_messages((str(i), i) for i in range(10, 80, 10))
    with q.get_connection() as core:
        rows = list(
            core.peek_generator(
                q.name,
                batch_size=PAGE_SIZE,
                after_timestamp=after,
                before_timestamp=before,
            )
        )
    assert [row[1] for row in rows] == expected


@pytest.mark.parametrize("mutation", ["delete", "move"])
@pytest.mark.parametrize("with_timestamps", [False, True])
def test_removing_returned_rows_does_not_skip_later_pages(
    queue_factory: Any, mutation: str, with_timestamps: bool
) -> None:
    source = queue_factory("source")
    mutator = queue_factory("source", persistent=False)
    destination = queue_factory("destination")
    expected = [(str(i), i) for i in range(1, 3 * PAGE_SIZE + 2)]
    source.insert_messages(expected)
    with (
        source.get_connection() as core,
        closing(
            core.peek_generator(
                source.name, batch_size=PAGE_SIZE, with_timestamps=with_timestamps
            )
        ) as stream,
    ):
        visited = []
        for row in stream:
            message_id = row[1] if isinstance(row, tuple) else int(row)
            visited.append(message_id)
            if mutation == "delete":
                assert mutator.delete(message_id=message_id)
            else:
                assert mutator.move(destination, message_id=message_id)
    assert visited == [message_id for _, message_id in expected]
    assert source.peek_one() is None


@pytest.mark.parametrize("arrival", ["insert", "move", "write"])
def test_live_arrivals_respect_cursor_not_arrival_time(
    queue_factory: Any, arrival: str
) -> None:
    q = queue_factory("arrivals")
    other = queue_factory("arrivals", persistent=False)
    staging = queue_factory("staging")
    q.insert_messages([(str(i), i) for i in (20, 40, 60, 80)])
    if arrival == "move":
        staging.insert_messages([("older", 10), ("ahead", 70)])
    with (
        q.get_connection() as core,
        closing(core.peek_generator(q.name, batch_size=PAGE_SIZE)) as stream,
    ):
        assert [row[1] for row in islice(stream, PAGE_SIZE)] == [20, 40, 60]
        if arrival == "insert":
            other.insert_messages([("older", 10), ("ahead", 70)])
            expected_tail = [70, 80]
        elif arrival == "move":
            staging.move(other, message_id=10)
            staging.move(other, message_id=70)
            expected_tail = [70, 80]
        else:
            new_id = other.write("new")
            expected_tail = [80, new_id]
        assert [row[1] for row in stream] == expected_tail
    if arrival != "write":
        assert q.peek_one(exact_timestamp=10) == "older"


def test_buffered_rows_are_observations_not_reservations(queue_factory: Any) -> None:
    q = queue_factory("buffer")
    other = queue_factory("buffer", persistent=False)
    q.insert_messages([(str(i), i) for i in range(1, 6)])
    with (
        q.get_connection() as core,
        closing(core.peek_generator(q.name, batch_size=PAGE_SIZE)) as stream,
    ):
        assert next(stream) == ("1", 1)
        assert other.delete(message_id=2)
        assert list(stream) == [("2", 2), ("3", 3), ("4", 4), ("5", 5)]
    assert q.peek_one(exact_timestamp=2) is None


@pytest.mark.parametrize("exact", [20, "0000000000000000020", 99])
@pytest.mark.parametrize("include_claimed", [False, True])
@pytest.mark.parametrize("claimed", [False, True])
def test_exact_id_terminates_with_single_row_pages(
    queue_factory: Any, exact: int | str, include_claimed: bool, claimed: bool
) -> None:
    q = queue_factory("exact")
    q.insert_messages([("ten", 10), ("twenty", 20)])
    if claimed:
        q.read_one(exact_timestamp=20)
    expected = (
        [("twenty", 20)]
        if (include_claimed or not claimed) and int(exact) == 20
        else []
    )
    with (
        q.get_connection() as core,
        closing(
            core.peek_generator(
                q.name,
                batch_size=1,
                exact_timestamp=exact,
                after_timestamp=30,
                before_timestamp=5,
                include_claimed=include_claimed,
            )
        ) as stream,
    ):
        # Bounded consumption catches repeating exact-ID implementations without hanging.
        assert list(islice(stream, 2)) == expected
        assert list(islice(stream, 1)) == []
    assert (
        list(
            q.peek_generator(
                exact_timestamp=exact,
                with_timestamps=True,
                include_claimed=include_claimed,
            )
        )
        == expected
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"exact_timestamp": "bad"},
        {"after_timestamp": "bad"},
        {"before_timestamp": "bad"},
        {"exact_timestamp": 20, "after_timestamp": "bad"},
    ],
)
def test_invalid_selection_keeps_lazy_failure_and_cleanup(
    queue_factory: Any, kwargs: dict[str, Any]
) -> None:
    q = queue_factory("invalid")
    stream = q.peek_generator(**kwargs)
    with pytest.raises((ValueError, TypeError)):
        next(stream)
    stream.close()
    assert q.write("still usable")


def test_legacy_zero_and_signed_ceiling_cursor(broker: Any, queue_factory: Any) -> None:
    # These legacy rows cannot be created through today's public insertion policy.
    # Native fixture setup is necessary; all traversal still executes real backends.
    expected = [("zero", 0), ("high", SQLITE_MAX_INT64 - 1)]
    for body, message_id in expected:
        if active_backend() == "redis":
            encoded = f"{message_id:019d}"
            broker._client.hset(broker._keys.bodies, encoded, body)
            broker._client.zadd(broker._keys.all_ids, {encoded: 0})
            broker._client.zadd(broker._keys.pending("legacy"), {encoded: 0})
            broker._client.sadd(broker._keys.queues, "legacy")
        else:
            broker._runner.run(
                "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
                ("legacy", body, message_id),
            )
            broker._runner.commit()
    assert list(broker.peek_generator("legacy", batch_size=1)) == expected
    for row in expected:
        with closing(
            broker.peek_generator("legacy", batch_size=1, exact_timestamp=row[1])
        ) as stream:
            assert list(islice(stream, 2)) == [row]
    q = queue_factory("legacy")
    assert list(q.peek_generator(with_timestamps=True)) == expected
    assert (
        list(q.peek_generator(with_timestamps=True, after_timestamp=0)) == expected[1:]
    )


def test_claimed_merge_and_state_change_do_not_repeat_ids(queue_factory: Any) -> None:
    q = queue_factory("states")
    other = queue_factory("states", persistent=False)
    expected = [(str(i), i) for i in range(1, 11)]
    q.insert_messages(expected)
    for message_id in (2, 5, 6, 8):
        q.read_one(exact_timestamp=message_id)
    with (
        q.get_connection() as core,
        closing(
            core.peek_generator(q.name, batch_size=PAGE_SIZE, include_claimed=True)
        ) as stream,
    ):
        prefix = list(islice(stream, PAGE_SIZE))
        assert prefix == expected[:PAGE_SIZE]
        other.read_one(exact_timestamp=1)
        other.read_one(exact_timestamp=7)
        assert prefix + list(stream) == expected


def test_short_page_ends_without_waiting_for_later_writes(queue_factory: Any) -> None:
    q = queue_factory("short")
    q.insert_messages([("one", 1)])
    with (
        q.get_connection() as core,
        closing(core.peek_generator(q.name, batch_size=PAGE_SIZE)) as stream,
    ):
        assert next(stream) == ("one", 1)
        q.write("later")
        assert list(stream) == []
    assert q.peek_many(2) == ["one", "later"]


def test_public_scan_observes_committed_changes_from_independent_core(
    queue_factory: Any, broker: Any
) -> None:
    # The broker fixture owns its own runner/client, separate from Queue sessions.
    q = queue_factory("independent")
    q.insert_messages((str(i), i) for i in range(1, PEEK_BATCH_SIZE + 2))
    with closing(q.peek_generator(with_timestamps=True)) as stream:
        assert [row[1] for row in islice(stream, PEEK_BATCH_SIZE)] == list(
            range(1, PEEK_BATCH_SIZE + 1)
        )
        assert broker.delete_message_ids(q.name, [1]) == 1
        new_id = broker.write(q.name, "ordinary append")
        assert list(stream) == [
            (str(PEEK_BATCH_SIZE + 1), PEEK_BATCH_SIZE + 1),
            ("ordinary append", new_id),
        ]
