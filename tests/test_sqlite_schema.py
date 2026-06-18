"""SQLite schema bootstrap and migration tests."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from simplebroker._backends.sqlite.schema import (
    ensure_schema_v2,
    ensure_schema_v3,
    ensure_schema_v4,
    ensure_schema_v5,
    initialize_database,
    messages_has_claimed_column,
    meta_table_exists,
    migrate_schema,
    pending_queue_ts_index_exists,
    ts_unique_index_exists,
)
from simplebroker._constants import SCHEMA_VERSION, SIMPLEBROKER_MAGIC
from simplebroker._runner import SQLiteRunner

pytestmark = [pytest.mark.sqlite_only]


def _runner(db_path: Path) -> SQLiteRunner:
    return SQLiteRunner(str(db_path))


def _run_direct(operation):
    return operation()


def _create_v1_messages_table(db_path: Path) -> None:
    with closing(sqlite3.connect(db_path)) as conn:
        conn.execute("""
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                body TEXT NOT NULL,
                ts INTEGER NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE meta (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )
        """)
        conn.execute("INSERT INTO meta (key, value) VALUES ('last_ts', 0)")
        conn.commit()


def test_initialize_database_bootstraps_core_schema_and_metadata(
    tmp_path: Path,
) -> None:
    runner = _runner(tmp_path / "broker.db")
    try:
        assert meta_table_exists(runner) is False

        initialize_database(runner, run_with_retry=_run_direct)

        assert meta_table_exists(runner) is True
        assert messages_has_claimed_column(runner) is True
        assert pending_queue_ts_index_exists(runner) is True
        rows = dict(runner.run("SELECT key, value FROM meta", fetch=True))
        assert rows["magic"] == SIMPLEBROKER_MAGIC
        assert int(rows["schema_version"]) == SCHEMA_VERSION
        assert int(rows["alias_version"]) == 0
        aliases = list(
            runner.run(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='queue_aliases'",
                fetch=True,
            )
        )
        assert aliases == [("queue_aliases",)]
    finally:
        runner.close()


def test_migrate_schema_applies_v2_v3_v4_and_v5_in_order(tmp_path: Path) -> None:
    db_path = tmp_path / "old.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        migrate_schema(
            runner,
            current_version=1,
            write_schema_version=versions.append,
        )

        assert versions == [2, 3, 4, 5]
        assert messages_has_claimed_column(runner) is True
        assert ts_unique_index_exists(runner) is True
        assert pending_queue_ts_index_exists(runner) is True
        assert list(
            runner.run(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='queue_aliases'",
                fetch=True,
            )
        ) == [("queue_aliases",)]
    finally:
        runner.close()


def test_migrate_schema_v4_to_v5_creates_pending_queue_ts_index(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v4.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        ensure_schema_v3(runner, current_version=2, write_schema_version=lambda _: None)
        ensure_schema_v4(runner, current_version=3, write_schema_version=lambda _: None)

        assert pending_queue_ts_index_exists(runner) is False

        migrate_schema(
            runner,
            current_version=4,
            write_schema_version=versions.append,
        )

        assert versions == [5]
        assert pending_queue_ts_index_exists(runner) is True
    finally:
        runner.close()


def test_ensure_schema_v5_backfills_pending_queue_ts_index_idempotently(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v5.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        ensure_schema_v3(runner, current_version=2, write_schema_version=lambda _: None)
        ensure_schema_v4(runner, current_version=3, write_schema_version=lambda _: None)

        ensure_schema_v5(
            runner,
            current_version=5,
            write_schema_version=versions.append,
        )
        ensure_schema_v5(
            runner,
            current_version=5,
            write_schema_version=versions.append,
        )

        assert versions == []
        assert pending_queue_ts_index_exists(runner) is True
    finally:
        runner.close()


def test_latest_pending_timestamp_query_uses_pending_queue_ts_index(
    tmp_path: Path,
) -> None:
    runner = _runner(tmp_path / "broker.db")
    try:
        initialize_database(runner, run_with_retry=_run_direct)

        rows = list(
            runner.run(
                """
                EXPLAIN QUERY PLAN
                SELECT ts
                FROM messages
                WHERE queue = ? AND claimed = 0
                ORDER BY ts DESC
                LIMIT 1
                """,
                ("jobs",),
                fetch=True,
            )
        )

        plan_text = "\n".join(str(row) for row in rows)
        assert "idx_messages_pending_queue_ts" in plan_text
    finally:
        runner.close()


def test_ensure_schema_v2_adds_claimed_column_and_partial_index(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v1.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(
            runner, current_version=1, write_schema_version=versions.append
        )

        assert versions == [2]
        assert messages_has_claimed_column(runner) is True
        assert list(
            runner.run(
                "SELECT name FROM sqlite_master WHERE type='index' "
                "AND name='idx_messages_unclaimed'",
                fetch=True,
            )
        ) == [("idx_messages_unclaimed",)]
    finally:
        runner.close()


def test_ensure_schema_v3_reports_duplicate_timestamps(tmp_path: Path) -> None:
    db_path = tmp_path / "duplicates.db"
    _create_v1_messages_table(db_path)
    with closing(sqlite3.connect(db_path)) as conn:
        conn.executemany(
            "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
            [("jobs", "one", 42), ("jobs", "two", 42)],
        )
        conn.commit()

    runner = _runner(db_path)
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)

        with pytest.raises(RuntimeError, match="duplicate timestamps"):
            ensure_schema_v3(
                runner,
                current_version=2,
                write_schema_version=lambda _: None,
            )
    finally:
        runner.close()


def test_ensure_schema_v3_and_v4_skip_when_prior_versions_are_missing(
    tmp_path: Path,
) -> None:
    runner = _runner(tmp_path / "missing-prior.db")
    versions: list[int] = []
    try:
        ensure_schema_v3(
            runner, current_version=1, write_schema_version=versions.append
        )
        ensure_schema_v4(
            runner, current_version=2, write_schema_version=versions.append
        )

        assert versions == []
        assert ts_unique_index_exists(runner) is False
    finally:
        runner.close()


def test_ensure_schema_v4_backfills_alias_table_idempotently(tmp_path: Path) -> None:
    db_path = tmp_path / "v3.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        ensure_schema_v3(runner, current_version=2, write_schema_version=lambda _: None)

        ensure_schema_v4(
            runner, current_version=3, write_schema_version=versions.append
        )
        ensure_schema_v4(
            runner, current_version=4, write_schema_version=versions.append
        )

        assert versions == [4]
        assert list(
            runner.run(
                "SELECT value FROM meta WHERE key='alias_version'",
                fetch=True,
            )
        ) == [(0,)]
    finally:
        runner.close()
