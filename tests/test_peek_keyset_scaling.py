"""Public SQLite scan work grows linearly without timing-sensitive assertions."""

from __future__ import annotations

import sqlite3

import pytest

from .peek_pagination_benchmark import dataset, seed_sqlite_messages, sqlite_steps

pytestmark = [pytest.mark.sqlite_only]


def test_sqlite_fixture_seeding_uses_one_explicit_transaction() -> None:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.execute(
        "CREATE TABLE messages(id INTEGER PRIMARY KEY, queue TEXT, body TEXT, ts INTEGER)"
    )
    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    try:
        seed_sqlite_messages(connection, 3)
        row_count = connection.execute("SELECT count(*) FROM messages").fetchone()
    finally:
        connection.close()

    transaction_statements = [
        statement
        for statement in statements
        if statement in {"BEGIN IMMEDIATE", "COMMIT", "ROLLBACK"}
    ]
    assert transaction_statements == ["BEGIN IMMEDIATE", "COMMIT"]
    assert row_count == (3,)


def test_sqlite_fixture_seeding_rolls_back_partial_failure() -> None:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.execute(
        "CREATE TABLE messages("
        "id INTEGER PRIMARY KEY, queue TEXT, body TEXT, ts INTEGER CHECK(ts < 3))"
    )
    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            seed_sqlite_messages(connection, 3)
        row_count = connection.execute("SELECT count(*) FROM messages").fetchone()
        in_transaction = connection.in_transaction
    finally:
        connection.close()

    transaction_statements = [
        statement
        for statement in statements
        if statement in {"BEGIN IMMEDIATE", "COMMIT", "ROLLBACK"}
    ]
    assert transaction_statements == ["BEGIN IMMEDIATE", "ROLLBACK"]
    assert row_count == (0,)
    assert not in_transaction


def test_public_peek_doubled_rows_have_linear_vm_work() -> None:
    steps = []
    for count in (10000, 20000):
        with dataset("sqlite", count, None) as (queue, runner, _version):
            steps.append(sqlite_steps(queue, runner, count))
    # OFFSET is about 3.4x here; keyset is about 2x. Fixed-size VM sampling
    # avoids making Python callback overhead the Windows bottleneck. Fixture
    # construction and teardown are outside it, and every ordered ID is checked.
    assert steps[1] < steps[0] * 2.8, steps
