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
