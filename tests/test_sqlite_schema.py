"""SQLite schema bootstrap and migration tests."""

from __future__ import annotations

import sqlite3
import threading
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
from simplebroker._exceptions import IntegrityError, OperationalError
from simplebroker._runner import SQLiteRunner

pytestmark = [pytest.mark.sqlite_only]


def _runner(db_path: Path) -> SQLiteRunner:
    return SQLiteRunner(str(db_path))


def _run_direct(operation):
    return operation()


class _FailOnceRunner:
    """Delegate to a real runner while failing one named SQL operation."""

    def __init__(self, runner: SQLiteRunner, marker: str, error: Exception) -> None:
        self._runner = runner
        self._marker = marker
        self._error = error
        self._failed = False

    def run(self, sql, *args, **kwargs):
        if not self._failed and self._marker in sql:
            self._failed = True
            raise self._error
        return self._runner.run(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._runner, name)


class _FailCommitRunner:
    """Delegate to a real/race runner while injecting one commit failure."""

    def __init__(self, runner) -> None:
        self._runner = runner

    def commit(self) -> None:
        raise OperationalError("injected migration commit failure")

    def __getattr__(self, name):
        return getattr(self._runner, name)


class _BarrierAfterFirstRun:
    """Synchronize two real runners after one named read operation."""

    def __init__(
        self,
        runner: SQLiteRunner,
        marker: str,
        barrier: threading.Barrier,
    ) -> None:
        self._runner = runner
        self._marker = marker
        self._barrier = barrier
        self._waited = False

    def run(self, sql, *args, **kwargs):
        rows = self._runner.run(sql, *args, **kwargs)
        if not self._waited and self._marker in sql:
            self._waited = True
            self._barrier.wait(timeout=5.0)
        return rows

    def __getattr__(self, name):
        return getattr(self._runner, name)


class _CreateTsIndexBeforeFirstBegin:
    """Model another connection winning the v3 check-to-begin race."""

    def __init__(self, runner: SQLiteRunner, competitor: SQLiteRunner) -> None:
        self._runner = runner
        self._competitor = competitor
        self._prepared = False

    def begin_immediate(self) -> None:
        if not self._prepared:
            self._prepared = True
            self._competitor.run(
                "CREATE UNIQUE INDEX idx_messages_ts_unique ON messages(ts)"
            )
        self._runner.begin_immediate()

    def __getattr__(self, name):
        return getattr(self._runner, name)


class _CreateTsIndexBeforeCreate:
    """Model another connection winning the v3 repair race."""

    def __init__(self, runner: SQLiteRunner, competitor: SQLiteRunner) -> None:
        self._runner = runner
        self._competitor = competitor
        self._prepared = False

    def run(self, sql, *args, **kwargs):
        if not self._prepared and "CREATE UNIQUE INDEX idx_messages_ts_unique" in sql:
            self._prepared = True
            self._competitor.run(
                "CREATE UNIQUE INDEX idx_messages_ts_unique ON messages(ts)"
            )
        return self._runner.run(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._runner, name)


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


def test_initialize_database_uses_one_explicit_transaction(tmp_path: Path) -> None:
    runner = _runner(tmp_path / "broker.db")
    conn = runner.get_connection()
    statements: list[str] = []
    conn.set_trace_callback(statements.append)

    try:
        initialize_database(runner, run_with_retry=_run_direct)
    finally:
        conn.set_trace_callback(None)
        runner.close()

    transaction_statements = [
        statement
        for statement in statements
        if statement.lstrip().split(None, maxsplit=1)[0].upper()
        in {"BEGIN", "COMMIT", "ROLLBACK"}
    ]
    assert transaction_statements == ["BEGIN IMMEDIATE", "COMMIT"]


def test_initialize_database_rolls_back_bootstrap_errors(tmp_path: Path) -> None:
    runner = _runner(tmp_path / "bootstrap-error.db")
    failing_runner = _FailOnceRunner(
        runner,
        "CREATE TABLE IF NOT EXISTS messages",
        OperationalError("disk I/O error"),
    )
    try:
        with pytest.raises(OperationalError, match="disk I/O error"):
            initialize_database(failing_runner, run_with_retry=_run_direct)

        assert runner.get_connection().in_transaction is False
        assert meta_table_exists(runner) is False
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


def test_ensure_schema_v2_rolls_back_when_alter_fails(tmp_path: Path) -> None:
    db_path = tmp_path / "v1-alter-error.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    failing_runner = _FailOnceRunner(
        runner,
        "ALTER TABLE messages ADD COLUMN claimed",
        OperationalError("disk I/O error"),
    )
    versions: list[int] = []
    try:
        with pytest.raises(OperationalError, match="disk I/O error"):
            ensure_schema_v2(
                failing_runner,
                current_version=1,
                write_schema_version=versions.append,
            )

        assert runner.get_connection().in_transaction is False
        assert messages_has_claimed_column(runner) is False
        assert versions == []
    finally:
        runner.close()


def test_ensure_schema_v2_handles_concurrent_column_migration(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "concurrent-v2.db"
    _create_v1_messages_table(db_path)
    barrier = threading.Barrier(2)
    runners = [_runner(db_path), _runner(db_path)]
    wrapped = [
        _BarrierAfterFirstRun(
            runner,
            "pragma_table_info('messages')",
            barrier,
        )
        for runner in runners
    ]
    versions: list[list[int]] = [[], []]
    errors: list[BaseException] = []

    def migrate(index: int) -> None:
        try:
            ensure_schema_v2(
                wrapped[index],
                current_version=1,
                write_schema_version=versions[index].append,
            )
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=migrate, args=(index,)) for index in range(2)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10.0)

        assert all(not thread.is_alive() for thread in threads)
        assert errors == []
        assert versions == [[2], [2]]
        assert messages_has_claimed_column(runners[0]) is True
        assert all(not runner.get_connection().in_transaction for runner in runners)
    finally:
        for runner in runners:
            runner.close()


@pytest.mark.parametrize(
    ("current_version", "precreate_claimed", "expected_versions"),
    [(1, True, [2]), (2, False, [])],
)
def test_ensure_schema_v2_repairs_mismatched_version_and_column_state(
    tmp_path: Path,
    current_version: int,
    precreate_claimed: bool,
    expected_versions: list[int],
) -> None:
    db_path = tmp_path / f"mismatched-v2-{current_version}.db"
    _create_v1_messages_table(db_path)
    if precreate_claimed:
        with closing(sqlite3.connect(db_path)) as connection:
            connection.execute(
                "ALTER TABLE messages ADD COLUMN claimed INTEGER DEFAULT 0"
            )
            connection.commit()

    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(
            runner,
            current_version=current_version,
            write_schema_version=versions.append,
        )

        assert messages_has_claimed_column(runner) is True
        assert versions == expected_versions
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


def test_ensure_schema_v3_records_version_when_index_already_exists(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "preindexed.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        runner.run("CREATE UNIQUE INDEX idx_messages_ts_unique ON messages(ts)")

        ensure_schema_v3(
            runner,
            current_version=2,
            write_schema_version=versions.append,
        )

        assert versions == [3]
        assert ts_unique_index_exists(runner) is True
    finally:
        runner.close()


def test_ensure_schema_v3_rolls_back_other_integrity_errors(tmp_path: Path) -> None:
    db_path = tmp_path / "v2-integrity-error.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        failing_runner = _FailOnceRunner(
            runner,
            "CREATE UNIQUE INDEX idx_messages_ts_unique",
            IntegrityError("unrelated integrity failure"),
        )

        with pytest.raises(IntegrityError, match="unrelated integrity failure"):
            ensure_schema_v3(
                failing_runner,
                current_version=2,
                write_schema_version=versions.append,
            )

        assert runner.get_connection().in_transaction is False
        assert ts_unique_index_exists(runner) is False
        assert versions == []
    finally:
        runner.close()


def test_ensure_schema_v3_handles_index_created_after_preflight(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v3-index-race.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    competitor = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)

        ensure_schema_v3(
            _CreateTsIndexBeforeFirstBegin(runner, competitor),
            current_version=2,
            write_schema_version=versions.append,
        )

        assert versions == [3]
        assert ts_unique_index_exists(runner) is True
        assert runner.get_connection().in_transaction is False
    finally:
        competitor.close()
        runner.close()


def test_ensure_schema_v3_repair_rolls_back_when_commit_fails(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v3-index-race-commit-failure.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    competitor = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        race_runner = _CreateTsIndexBeforeFirstBegin(runner, competitor)

        with pytest.raises(OperationalError, match="migration commit failure"):
            ensure_schema_v3(
                _FailCommitRunner(race_runner),
                current_version=2,
                write_schema_version=versions.append,
            )

        assert runner.get_connection().in_transaction is False
        assert versions == [3]
    finally:
        competitor.close()
        runner.close()


def test_ensure_schema_v3_repair_handles_concurrent_index_creation(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v3-repair-race.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    competitor = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)

        ensure_schema_v3(
            _CreateTsIndexBeforeCreate(runner, competitor),
            current_version=3,
            write_schema_version=versions.append,
        )

        assert versions == []
        assert ts_unique_index_exists(runner) is True
    finally:
        competitor.close()
        runner.close()


@pytest.mark.parametrize("current_version", [2, 3])
def test_ensure_schema_v3_propagates_index_creation_errors(
    tmp_path: Path,
    current_version: int,
) -> None:
    db_path = tmp_path / f"v3-create-error-{current_version}.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        failing_runner = _FailOnceRunner(
            runner,
            "CREATE UNIQUE INDEX idx_messages_ts_unique",
            OperationalError("disk I/O error"),
        )

        with pytest.raises(OperationalError, match="disk I/O error"):
            ensure_schema_v3(
                failing_runner,
                current_version=current_version,
                write_schema_version=versions.append,
            )

        assert runner.get_connection().in_transaction is False
        assert ts_unique_index_exists(runner) is False
        assert versions == []
    finally:
        runner.close()


def test_ensure_schema_v3_v4_and_v5_skip_when_prior_versions_are_missing(
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
        ensure_schema_v5(
            runner, current_version=3, write_schema_version=versions.append
        )

        assert versions == []
        assert ts_unique_index_exists(runner) is False
    finally:
        runner.close()


@pytest.mark.parametrize("current_version", [3, 4])
def test_ensure_schema_v4_rolls_back_sql_errors(
    tmp_path: Path,
    current_version: int,
) -> None:
    db_path = tmp_path / f"v4-error-{current_version}.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        ensure_schema_v3(runner, current_version=2, write_schema_version=lambda _: None)
        failing_runner = _FailOnceRunner(
            runner,
            "CREATE INDEX IF NOT EXISTS idx_queue_aliases_target",
            OperationalError("disk I/O error"),
        )

        with pytest.raises(OperationalError, match="disk I/O error"):
            ensure_schema_v4(
                failing_runner,
                current_version=current_version,
                write_schema_version=versions.append,
            )

        assert runner.get_connection().in_transaction is False
        assert (
            list(
                runner.run(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name='queue_aliases'",
                    fetch=True,
                )
            )
            == []
        )
        assert versions == []
    finally:
        runner.close()


@pytest.mark.parametrize("current_version", [4, 5])
def test_ensure_schema_v5_rolls_back_sql_errors(
    tmp_path: Path,
    current_version: int,
) -> None:
    db_path = tmp_path / f"v5-error-{current_version}.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        ensure_schema_v3(runner, current_version=2, write_schema_version=lambda _: None)
        ensure_schema_v4(runner, current_version=3, write_schema_version=lambda _: None)
        failing_runner = _FailOnceRunner(
            runner,
            "CREATE INDEX IF NOT EXISTS idx_messages_pending_queue_ts",
            OperationalError("disk I/O error"),
        )

        with pytest.raises(OperationalError, match="disk I/O error"):
            ensure_schema_v5(
                failing_runner,
                current_version=current_version,
                write_schema_version=versions.append,
            )

        assert runner.get_connection().in_transaction is False
        assert pending_queue_ts_index_exists(runner) is False
        assert versions == []
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
