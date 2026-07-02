"""The legacy SQL builders must emit executable SQLite.

These builders are consumed by examples/async_pooled_broker.py, which sits
outside every CI gate (ruff, mypy, pytest all exclude examples/) --
build_move_by_id_query shipped invalid SQL (RETURNING ... ORDER BY)
without anything noticing.  Executing each builder's output here pins
validity inside the gated suite.
"""

import sqlite3

import pytest

from simplebroker._sql.sqlite import (
    build_claim_batch_query,
    build_claim_single_query,
    build_move_by_id_query,
    build_peek_query,
)

pytestmark = pytest.mark.sqlite_only


@pytest.fixture
def conn():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "queue TEXT NOT NULL, body TEXT NOT NULL, ts INTEGER NOT NULL UNIQUE, "
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


def test_build_move_by_id_query_executes(conn):
    """RED pre-fix: the generated SQL places ORDER BY after RETURNING,
    which SQLite rejects with a syntax error."""
    rows = conn.execute(
        build_move_by_id_query(["id = ?", "queue = ?"]), ("dest", 1, "src")
    ).fetchall()
    assert rows == [(1, "m1", 1)]
    assert conn.execute(
        "SELECT queue, claimed FROM messages WHERE id = 1"
    ).fetchone() == ("dest", 0)
