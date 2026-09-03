"""Public behavior for the write-time pending window [SB-DELIVERY-9]."""

from __future__ import annotations

import concurrent.futures
import threading
from typing import Any, cast

import pytest

from simplebroker import Queue, commands

pytestmark = [pytest.mark.shared]


def test_write_keep_newest_claims_older_pending_rows(queue_factory: Any) -> None:
    queue = queue_factory("snapshots")
    first_id = queue.write("first")
    second_id = queue.write("second")

    newest_id = queue.write("newest", keep_newest=2)

    assert queue.peek_many(10, with_timestamps=True) == [
        ("second", second_id),
        ("newest", newest_id),
    ]
    assert queue.peek_many(10, with_timestamps=True, include_claimed=True) == [
        ("first", first_id),
        ("second", second_id),
        ("newest", newest_id),
    ]
    stats = queue.stats()
    assert (stats.pending, stats.claimed, stats.total) == (2, 1, 3)


def test_write_keep_newest_is_noop_below_window(queue_factory: Any) -> None:
    queue = queue_factory("snapshots")
    first_id = queue.write("first")

    newest_id = queue.write("newest", keep_newest=3)

    assert queue.peek_many(10, with_timestamps=True) == [
        ("first", first_id),
        ("newest", newest_id),
    ]
    stats = queue.stats()
    assert (stats.pending, stats.claimed, stats.total) == (2, 0, 2)


def test_write_keep_newest_one_on_empty_queue_keeps_the_new_row(
    queue_factory: Any,
) -> None:
    queue = queue_factory("snapshots")

    newest_id = queue.write("newest", keep_newest=1)

    assert queue.peek_many(10, with_timestamps=True) == [("newest", newest_id)]
    stats = queue.stats()
    assert (stats.pending, stats.claimed, stats.total) == (1, 0, 1)


def test_write_keep_newest_is_noop_with_exactly_n_minus_one_prior_rows(
    queue_factory: Any,
) -> None:
    queue = queue_factory("snapshots")
    prior_ids = [queue.write(f"prior-{index}") for index in range(2)]

    newest_id = queue.write("newest", keep_newest=3)

    assert queue.peek_many(10, with_timestamps=True) == [
        ("prior-0", prior_ids[0]),
        ("prior-1", prior_ids[1]),
        ("newest", newest_id),
    ]
    assert queue.stats().claimed == 0


def test_write_keep_newest_claims_every_row_outside_a_much_smaller_window(
    queue_factory: Any,
) -> None:
    queue = queue_factory("snapshots")
    for index in range(20):
        queue.write(f"prior-{index}")

    queue.write("newest", keep_newest=3)

    assert queue.peek_many(10) == ["prior-18", "prior-19", "newest"]
    stats = queue.stats()
    assert (stats.pending, stats.claimed, stats.total) == (3, 18, 21)


def test_write_keep_newest_excludes_existing_claimed_rows(queue_factory: Any) -> None:
    queue = queue_factory("snapshots")
    first_id = queue.write("first")
    assert queue.read_one(with_timestamps=True) == ("first", first_id)
    second_id = queue.write("second")

    newest_id = queue.write("newest", keep_newest=1)

    assert queue.peek_many(10, with_timestamps=True) == [("newest", newest_id)]
    assert queue.peek_many(10, with_timestamps=True, include_claimed=True) == [
        ("first", first_id),
        ("second", second_id),
        ("newest", newest_id),
    ]
    stats = queue.stats()
    assert (stats.pending, stats.claimed, stats.total) == (1, 2, 3)


def test_write_keep_newest_orders_by_public_id_not_insertion_time(
    queue_factory: Any,
) -> None:
    queue = queue_factory("snapshots")
    generated = queue.write("generated-first")
    queue.insert_messages([("low-id-inserted-later", 1)])

    newest = queue.write("newest", keep_newest=2)

    assert queue.peek_many(10, with_timestamps=True) == [
        ("generated-first", generated),
        ("newest", newest),
    ]
    assert queue.peek_many(10, with_timestamps=True, include_claimed=True) == [
        ("low-id-inserted-later", 1),
        ("generated-first", generated),
        ("newest", newest),
    ]


@pytest.mark.parametrize(
    ("value", "error_type"),
    [
        (True, TypeError),
        ("2", TypeError),
        (0, ValueError),
        (10_000, ValueError),
    ],
)
def test_write_keep_newest_rejects_invalid_python_values(
    queue_factory: Any,
    value: object,
    error_type: type[Exception],
) -> None:
    queue = queue_factory("snapshots")
    write = cast(Any, queue.write)

    with pytest.raises(error_type):
        write("not-written", keep_newest=value)

    assert queue.stats().total == 0


@pytest.mark.parametrize("value", [None, 1, 9999])
def test_write_keep_newest_accepts_complete_python_boundary_matrix(
    queue_factory: Any,
    value: int | None,
) -> None:
    queue = queue_factory("snapshots")

    message_id = queue.write("written", keep_newest=value)

    assert queue.peek_one(with_timestamps=True) == ("written", message_id)


@pytest.mark.parametrize("value", [-1, 1.0, b"1", object()])
def test_write_keep_newest_rejects_remaining_python_types_and_ranges(
    queue_factory: Any,
    value: object,
) -> None:
    queue = queue_factory("snapshots")
    write = cast(Any, queue.write)

    with pytest.raises((TypeError, ValueError)):
        write("not-written", keep_newest=value)

    assert queue.stats().total == 0


def test_queue_write_validates_keep_before_opening_corrupt_target(
    tmp_path: Any,
) -> None:
    target = tmp_path / "corrupt.db"
    target.write_text("not sqlite", encoding="utf-8")
    queue = Queue("snapshots", db_path=target)
    write = cast(Any, queue.write)
    try:
        with pytest.raises(TypeError, match="keep_newest"):
            write("not-written", keep_newest="2")
    finally:
        queue.close()


def test_cmd_write_validates_keep_before_config_stdin_and_target() -> None:
    class _ExplodingConfig(dict[str, object]):
        def items(self) -> Any:
            raise AssertionError("config was observed")

    with pytest.raises(TypeError, match="keep_newest"):
        commands.cmd_write(
            "missing.db",
            "snapshots",
            None,
            keep_newest=cast(Any, "2"),
            config=_ExplodingConfig(),
        )


def test_later_ordinary_write_may_grow_past_the_prior_window(
    queue_factory: Any,
) -> None:
    queue = queue_factory("snapshots")
    queue.write("old")
    queue.write("window", keep_newest=1)

    queue.write("later-ordinary")

    assert queue.peek_many(10) == ["window", "later-ordinary"]


def test_vacuum_may_physically_remove_rows_claimed_by_keep(
    queue_factory: Any,
) -> None:
    queue = queue_factory("snapshots")
    queue.write("old-1")
    queue.write("old-2")
    queue.write("new", keep_newest=1)
    assert queue.stats().claimed == 2

    with queue.get_connection() as connection:
        connection.vacuum()

    stats = queue.stats()
    assert (stats.pending, stats.claimed, stats.total) == (1, 0, 1)
    assert queue.peek_one(with_timestamps=False) == "new"


@pytest.mark.parametrize("competing_operation", ["claim", "write"])
def test_keep_write_is_serial_with_consumer_and_ordinary_writer(
    queue_factory: Any,
    competing_operation: str,
) -> None:
    keeper = cast(Queue, queue_factory("jobs"))
    competitor = cast(Queue, queue_factory("jobs"))
    original_ids = [keeper.write(f"original-{index}") for index in range(8)]
    start = threading.Barrier(2)

    def keep() -> int:
        start.wait(timeout=5)
        return keeper.write("kept-write", keep_newest=5)

    def compete() -> object:
        start.wait(timeout=5)
        if competing_operation == "claim":
            return competitor.read_one()
        return competitor.write("ordinary-write")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        keep_future = executor.submit(keep)
        competing_future = executor.submit(compete)
        kept_id = keep_future.result(timeout=10)
        competing_result = competing_future.result(timeout=10)

    assert kept_id > original_ids[-1]
    assert competing_result is not None
    pending_rows = keeper.peek_many(20, with_timestamps=True)
    all_rows = keeper.peek_many(
        20,
        with_timestamps=True,
        include_claimed=True,
    )
    expected_total = 9 if competing_operation == "claim" else 10
    expected_pending_counts = {4, 5} if competing_operation == "claim" else {5, 6}
    assert len(all_rows) == expected_total
    assert len(pending_rows) in expected_pending_counts
    assert pending_rows == all_rows[-len(pending_rows) :]


def test_sustained_keep_writers_with_different_windows_finish_serially(
    queue_factory: Any,
) -> None:
    first = queue_factory("jobs")
    second = queue_factory("jobs")
    start = threading.Barrier(2)

    def write_window(queue: Any, keep_newest: int) -> list[int]:
        start.wait(timeout=5)
        return [
            queue.write(
                f"keep-{keep_newest}-{index}",
                keep_newest=keep_newest,
            )
            for index in range(15)
        ]

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(write_window, first, 3)
        second_future = executor.submit(write_window, second, 7)
        ids = first_future.result(timeout=15) + second_future.result(timeout=15)

    assert len(set(ids)) == 30
    pending = first.peek_many(40, with_timestamps=True)
    all_rows = first.peek_many(40, with_timestamps=True, include_claimed=True)
    assert 3 <= len(pending) <= 7
    assert len(all_rows) == 30
    assert pending == all_rows[-len(pending) :]
