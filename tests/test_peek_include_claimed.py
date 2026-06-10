"""Behavior tests for include_claimed on the public peek surface.

Claimed rows are consumed-but-not-vacuumed messages. These tests create them
the real way (write + read) against real SQLite databases under tmp_path.
No mocks: assert returned rows, ordering, and state — never internal calls.
"""

from __future__ import annotations

from pathlib import Path

from simplebroker import Queue


def _db(tmp_path: Path) -> str:
    return str(tmp_path / "broker.db")


def _seed(q: Queue, n: int) -> list[int]:
    """Write n messages m0..m{n-1}; return their message IDs in write order."""
    ids: list[int] = []
    for i in range(n):
        q.write(f"m{i}")
    rows = q.peek_many(n, with_timestamps=True)
    assert len(rows) == n
    ids = [ts for _body, ts in rows]
    return ids


def test_default_peek_excludes_claimed(tmp_path: Path) -> None:
    q = Queue("jobs", db_path=_db(tmp_path))
    _seed(q, 3)
    assert q.read() == "m0"  # claims m0; the row lingers until vacuum

    bodies = q.peek_many(10)
    assert bodies == ["m1", "m2"]  # byte-identical to pre-flag behavior


def test_include_claimed_returns_superset_in_id_order(tmp_path: Path) -> None:
    q = Queue("jobs", db_path=_db(tmp_path))
    ids = _seed(q, 4)
    assert q.read() == "m0"
    assert q.read() == "m1"  # two claimed, two pending

    rows = q.peek_many(10, with_timestamps=True, include_claimed=True)
    assert [body for body, _ in rows] == ["m0", "m1", "m2", "m3"]
    assert [ts for _, ts in rows] == ids  # strict message-ID order


def test_limit_and_bounds_apply_to_merged_stream(tmp_path: Path) -> None:
    q = Queue("jobs", db_path=_db(tmp_path))
    ids = _seed(q, 4)
    assert q.read() == "m0"

    # limit counts claimed rows too
    rows = q.peek_many(2, with_timestamps=True, include_claimed=True)
    assert [body for body, _ in rows] == ["m0", "m1"]

    # after_timestamp applies to the merged stream
    rows = q.peek_many(
        10, with_timestamps=True, include_claimed=True, after_timestamp=ids[0]
    )
    assert [body for body, _ in rows] == ["m1", "m2", "m3"]


def test_exact_id_peek_finds_claimed_row_only_with_flag(tmp_path: Path) -> None:
    q = Queue("jobs", db_path=_db(tmp_path))
    ids = _seed(q, 2)
    assert q.read() == "m0"

    assert q.peek_one(exact_timestamp=ids[0]) is None
    assert q.peek_one(exact_timestamp=ids[0], include_claimed=True) == "m0"


def test_generator_paginates_across_claimed_boundary(tmp_path: Path) -> None:
    q = Queue("jobs", db_path=_db(tmp_path))
    _seed(q, 5)
    assert q.read() == "m0"
    assert q.read() == "m1"

    # Queue.peek_generator has no batch_size knob; drive the connection-level
    # generator where batch_size=1 forces offset pagination through the
    # merged stream. Then confirm the Queue-level generator agrees.
    with q.get_connection() as conn:
        bodies = list(
            conn.peek_generator(
                "jobs", batch_size=1, with_timestamps=False, include_claimed=True
            )
        )
    assert bodies == ["m0", "m1", "m2", "m3", "m4"]
    assert list(q.peek_generator(include_claimed=True)) == bodies
    assert list(q.peek_generator()) == ["m2", "m3", "m4"]


def test_peeking_claimed_rows_mutates_nothing(tmp_path: Path) -> None:
    q = Queue("jobs", db_path=_db(tmp_path))
    _seed(q, 3)
    assert q.read() == "m0"

    before = q.stats()
    q.peek_many(10, include_claimed=True)
    q.peek_one(include_claimed=True)
    after = q.stats()
    assert (before.pending, before.claimed) == (after.pending, after.claimed) == (2, 1)
    assert q.read() == "m1"  # delivery order untouched


def test_vacuum_removes_claimed_rows_from_flagged_peeks(tmp_path: Path) -> None:
    """Pins the documented race: claimed rows are deletion-pending."""
    q = Queue("jobs", db_path=_db(tmp_path))
    _seed(q, 2)
    assert q.read() == "m0"

    with q.get_connection() as conn:
        conn.vacuum()

    assert q.peek_many(10, include_claimed=True) == ["m1"]


def test_queue_peek_high_level_mirrors_flag(tmp_path: Path) -> None:
    q = Queue("jobs", db_path=_db(tmp_path))
    _seed(q, 2)
    assert q.read() == "m0"

    assert q.peek() == "m1"
    assert q.peek(include_claimed=True) == "m0"
    all_rows = q.peek(all_messages=True, include_claimed=True)
    assert list(all_rows) == ["m0", "m1"]  # type: ignore[arg-type]
