"""SQLite storage invariants for physical batch deletion."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from simplebroker.db import BrokerDB

pytestmark = [pytest.mark.sqlite_only]


def _counts(db_path: Path) -> tuple[int, int]:
    with sqlite3.connect(str(db_path)) as conn:
        total = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        claimed = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE claimed = 1"
        ).fetchone()[0]
    return total, claimed


def _counts_by_queue(db_path: Path) -> dict[str, int]:
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT queue, COUNT(*) FROM messages GROUP BY queue ORDER BY queue"
        ).fetchall()
    return {str(queue): int(count) for queue, count in rows}


def test_delete_message_ids_physically_removes_sqlite_rows(workdir: Path) -> None:
    db_path = workdir / "test.db"

    with BrokerDB(str(db_path)) as broker:
        for index in range(1, 5):
            broker.write("jobs", f"message{index}")
        timestamps = dict(broker.peek_many("jobs", limit=10))
        assert (
            broker.claim_one(
                "jobs",
                exact_timestamp=timestamps["message2"],
                with_timestamps=False,
            )
            == "message2"
        )

    assert _counts(db_path) == (4, 1)

    with BrokerDB(str(db_path)) as broker:
        deleted = broker.delete_message_ids(
            "jobs",
            [timestamps["message2"], timestamps["message3"]],
        )

    assert deleted == 2
    assert _counts(db_path) == (2, 0)


def test_delete_from_queues_physically_removes_sqlite_rows(workdir: Path) -> None:
    db_path = workdir / "test.db"

    with BrokerDB(str(db_path)) as broker:
        broker.write("alpha", "alpha1")
        broker.write("alpha", "alpha2")
        broker.write("beta", "beta1")
        broker.write("gamma", "gamma1")
        timestamps = dict(broker.peek_many("alpha", limit=10))
        assert (
            broker.claim_one(
                "alpha",
                exact_timestamp=timestamps["alpha1"],
                with_timestamps=False,
            )
            == "alpha1"
        )

    assert _counts(db_path) == (4, 1)

    with BrokerDB(str(db_path)) as broker:
        deleted = broker.delete_from_queues(["alpha", "beta"])

    assert deleted == 3
    assert _counts_by_queue(db_path) == {"gamma": 1}
    assert _counts(db_path) == (1, 0)


def test_delete_from_queues_sqlite_temp_table_insert_chunks(workdir: Path) -> None:
    db_path = workdir / "test.db"
    queue_names = [f"queue_{index}" for index in range(501)]

    with BrokerDB(str(db_path)) as broker:
        for queue in queue_names:
            broker.write(queue, queue)
        deleted = broker.delete_from_queues(queue_names)

    assert deleted == len(queue_names)
    assert _counts(db_path) == (0, 0)
