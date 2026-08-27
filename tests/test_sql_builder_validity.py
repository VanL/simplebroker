"""Runtime SQLite builders used by the advanced async example are executable."""

import sqlite3

import pytest

from simplebroker._sql.sqlite import (
    build_claim_batch_query,
    build_claim_single_query,
    build_move_by_timestamp_query,
    build_peek_query,
)

pytestmark = pytest.mark.sqlite_only


@pytest.fixture
def conn():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE messages (queue TEXT NOT NULL, body TEXT NOT NULL, "
        "ts INTEGER PRIMARY KEY, "
        "claimed INTEGER DEFAULT 0)"
    )
    conn.execute("INSERT INTO messages (queue, body, ts) VALUES ('src', 'm1', 1)")
    conn.commit()
    yield conn
    conn.close()


def test_build_peek_query_executes(conn):
    rows = conn.execute(
        build_peek_query(["queue = ?", "claimed = 0"]), ("src", 5, 0)
    ).fetchall()
    assert rows == [("m1", 1)]


def test_build_claim_single_query_executes(conn):
    rows = conn.execute(
        build_claim_single_query(["queue = ?", "claimed = 0"]), ("src",)
    ).fetchall()
    assert rows == [("m1", 1)]


def test_build_claim_batch_query_executes(conn):
    rows = conn.execute(
        build_claim_batch_query(["queue = ?", "claimed = 0"]), ("src", 10)
    ).fetchall()
    assert rows == [("m1", 1)]


def test_build_move_by_timestamp_query_executes(conn):
    rows = conn.execute(
        build_move_by_timestamp_query(["ts = ?", "queue = ?"]),
        ("dest", 1, "src"),
    ).fetchall()
    assert rows == [("m1", 1)]
    assert conn.execute(
        "SELECT queue, claimed FROM messages WHERE ts = 1"
    ).fetchone() == ("dest", 0)
