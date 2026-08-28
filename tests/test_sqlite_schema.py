"""SQLite schema bootstrap and migration tests."""

from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

import pytest

from simplebroker._backends.sqlite.schema import (
    ensure_schema_v2,
    ensure_schema_v3,
    ensure_schema_v4,
    ensure_schema_v5,
    ensure_schema_v6,
    initialize_database,
    messages_has_claimed_column,
    meta_table_exists,
    migrate_schema,
    pending_queue_ts_index_exists,
    ts_unique_index_exists,
)
from simplebroker._constants import SCHEMA_VERSION, SIMPLEBROKER_MAGIC
from simplebroker._exceptions import IntegrityError, OperationalError
from simplebroker._runner import SetupPhase, SQLiteRunner, SQLRunner

from .helper_scripts.sqlite_legacy_layouts import (
    create_sqlite_v1_layout,
    create_sqlite_v5_layout,
)

pytestmark = [pytest.mark.sqlite_only]


def _runner(db_path: Path) -> SQLiteRunner:
    return SQLiteRunner(str(db_path))


def _run_direct(operation):
    return operation()


def _write_schema_version(runner: SQLRunner, version: int) -> None:
    runner.run(
        "UPDATE meta SET value=? WHERE key='schema_version'",
        (version,),
    )


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


class _IgnoreOnceRunner:
    """Delegate to a real runner while ignoring one named SQL operation."""

    def __init__(self, runner: SQLiteRunner, marker: str) -> None:
        self._runner = runner
        self._marker = marker
        self._ignored = False

    def run(self, sql, *args, **kwargs):
        if not self._ignored and self._marker in sql:
            self._ignored = True
            return []
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


class _BarrierBeforeFirstBegin:
    """Synchronize two real runners immediately before their first write lock."""

    def __init__(
        self,
        runner: SQLiteRunner,
        barrier: threading.Barrier,
    ) -> None:
        self._runner = runner
        self._barrier = barrier
        self._waited = False

    def begin_immediate(self) -> None:
        if not self._waited:
            self._waited = True
            self._barrier.wait(timeout=5.0)
        self._runner.begin_immediate()

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


def _create_v1_messages_table(db_path: Path) -> None:
    create_sqlite_v1_layout(db_path)


def _create_v5_database(db_path: Path) -> None:
    """Create the literal last-release layout plus this suite's sidecar state."""
    create_sqlite_v5_layout(
        db_path,
        message_rows=[
            ("q", "id-300", 300, 0),
            ("q", "id-100", 100, 1),
            ("q", "id-200", 200, 0),
        ],
    )
    with closing(sqlite3.connect(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE sidecar_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                value TEXT NOT NULL
            );
            CREATE INDEX sidecar_jobs_value ON sidecar_jobs(value);
            CREATE TABLE sidecar_message_refs (
                message_ts INTEGER NOT NULL REFERENCES messages(ts),
                note TEXT NOT NULL
            );
            CREATE INDEX sidecar_message_refs_ts
                ON sidecar_message_refs(message_ts);
            CREATE VIEW sidecar_message_ts AS SELECT ts FROM messages;
            """
        )
        conn.execute("INSERT INTO sidecar_jobs (value) VALUES ('keep')")
        conn.execute(
            "INSERT INTO sidecar_message_refs (message_ts, note) "
            "VALUES (200, 'keep-ref')"
        )
        conn.commit()


def _sidecar_snapshot(conn: sqlite3.Connection) -> dict[str, object]:
    """Capture every caller-owned sidecar definition, row, and sequence."""
    return {
        "definition": conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='sidecar_jobs'"
        ).fetchone(),
        "index": conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='sidecar_jobs_value'"
        ).fetchone(),
        "rows": conn.execute("SELECT * FROM sidecar_jobs").fetchall(),
        "sequence": conn.execute(
            "SELECT name, seq FROM sqlite_sequence WHERE name='sidecar_jobs'"
        ).fetchall(),
        "ref_definition": conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='sidecar_message_refs'"
        ).fetchone(),
        "ref_index": conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='sidecar_message_refs_ts'"
        ).fetchone(),
        "ref_rows": conn.execute("SELECT * FROM sidecar_message_refs").fetchall(),
        "view_definition": conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='sidecar_message_ts'"
        ).fetchone(),
    }


def _owned_message_shape(conn: sqlite3.Connection) -> tuple[object, ...]:
    columns = conn.execute("PRAGMA table_info('messages')").fetchall()
    indexes = conn.execute(
        "SELECT name, \"unique\", partial FROM pragma_index_list('messages') "
        "ORDER BY name"
    ).fetchall()
    index_columns = {
        name: conn.execute(
            "SELECT name FROM pragma_index_info(?) ORDER BY seqno", (name,)
        ).fetchall()
        for name, _unique, _partial in indexes
    }
    return columns, indexes, index_columns


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


def test_fresh_schema_has_public_id_as_only_message_key(tmp_path: Path) -> None:
    db_path = tmp_path / "fresh-v6.db"
    runner = _runner(db_path)
    try:
        initialize_database(runner, run_with_retry=_run_direct)
        columns = list(runner.run("PRAGMA table_info('messages')", fetch=True))
        assert [row[1] for row in columns] == ["queue", "body", "ts", "claimed"]
        assert [(row[1], row[5]) for row in columns if row[5]] == [("ts", 1)]
    finally:
        runner.close()


def test_schema_v6_rebuild_preserves_sidecar_and_matches_fresh_shape(
    tmp_path: Path,
) -> None:
    migrated_path = tmp_path / "migrated.db"
    fresh_path = tmp_path / "fresh.db"
    _create_v5_database(migrated_path)

    with closing(sqlite3.connect(migrated_path)) as before:
        sidecar_before = _sidecar_snapshot(before)

    from simplebroker.db import BrokerDB

    with BrokerDB(str(migrated_path)) as broker:
        assert broker.peek_many("q", 10) == [("id-200", 200), ("id-300", 300)]
    with BrokerDB(str(fresh_path)):
        pass

    with (
        closing(sqlite3.connect(migrated_path)) as migrated,
        closing(sqlite3.connect(fresh_path)) as fresh,
    ):
        assert _owned_message_shape(migrated) == _owned_message_shape(fresh)
        assert migrated.execute(
            "SELECT queue, body, ts, claimed FROM messages ORDER BY ts"
        ).fetchall() == [
            ("q", "id-100", 100, 1),
            ("q", "id-200", 200, 0),
            ("q", "id-300", 300, 0),
        ]
        assert migrated.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone() == (6,)
        sidecar_after = _sidecar_snapshot(migrated)
        assert sidecar_after == sidecar_before
        assert migrated.execute(
            "SELECT ts FROM sidecar_message_ts ORDER BY ts"
        ).fetchall() == [(100,), (200,), (300,)]
        assert (
            migrated.execute(
                "SELECT name FROM sqlite_sequence WHERE name='messages'"
            ).fetchall()
            == []
        )


@pytest.mark.parametrize(
    "failure_marker",
    [
        "INSERT INTO simplebroker_messages_v6",
        "ALTER TABLE simplebroker_messages_v6",
    ],
)
def test_schema_v6_failure_rolls_back_owned_and_sidecar_state(
    tmp_path: Path,
    failure_marker: str,
) -> None:
    db_path = tmp_path / f"failure-{failure_marker.split()[0].lower()}.db"
    _create_v5_database(db_path)
    runner = _runner(db_path)
    failing = _FailOnceRunner(
        runner,
        failure_marker,
        OperationalError("database or disk is full"),
    )
    before = db_path.read_bytes()

    try:
        with pytest.raises(OperationalError, match="disk is full"):
            ensure_schema_v6(
                failing,
                current_version=5,
                write_schema_version=lambda version: _write_schema_version(
                    runner, version
                ),
            )
        assert runner.get_connection().in_transaction is False
    finally:
        runner.close()

    assert db_path.read_bytes() == before
    with closing(sqlite3.connect(db_path)) as conn:
        assert conn.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone() == (5,)
        assert conn.execute("SELECT * FROM sidecar_jobs").fetchall() == [(1, "keep")]
        assert conn.execute("SELECT * FROM sidecar_message_refs").fetchall() == [
            (200, "keep-ref")
        ]
        assert conn.execute(
            "SELECT name FROM pragma_table_info('messages') WHERE name='id'"
        ).fetchall() == [("id",)]


def test_schema_v6_migrates_despite_unsupported_caller_objects(
    tmp_path: Path,
) -> None:
    """Unsupported attachments never block migration.

    Anything outside the broker schema belongs in a sidecar: caller objects
    attached to ``messages`` are unsupported and do not survive the rebuild,
    while detached caller state (tables, rows, views and foreign keys as
    definitions) is preserved even when a private-id dependency leaves its
    definition broken. Migration must not refuse over them.
    """
    db_path = tmp_path / "dependent-sidecar.db"
    _create_v5_database(db_path)
    with closing(sqlite3.connect(db_path)) as conn:
        conn.execute("CREATE VIEW sidecar_message_ids AS SELECT id FROM messages")
        conn.execute(
            "CREATE TABLE sidecar_private_refs "
            "(message_id INTEGER REFERENCES messages(id))"
        )
        conn.execute("INSERT INTO sidecar_private_refs VALUES (1)")
        conn.execute("CREATE INDEX caller_body_idx ON messages(body)")
        conn.execute(
            "CREATE TRIGGER caller_audit AFTER INSERT ON messages BEGIN SELECT 1; END"
        )
        conn.commit()
        sidecar_rows_before = conn.execute("SELECT * FROM sidecar_jobs").fetchall()
        message_rows_before = conn.execute(
            "SELECT queue, body, ts, claimed FROM messages ORDER BY ts"
        ).fetchall()

    from simplebroker.db import BrokerDB

    db = BrokerDB(str(db_path))
    db.close()

    with closing(sqlite3.connect(db_path)) as conn:
        version = int(
            conn.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone()[0]
        )
        assert version == SCHEMA_VERSION
        columns = [
            row[1] for row in conn.execute("PRAGMA table_info('messages')").fetchall()
        ]
        assert columns == ["queue", "body", "ts", "claimed"]
        assert (
            conn.execute(
                "SELECT queue, body, ts, claimed FROM messages ORDER BY ts"
            ).fetchall()
            == message_rows_before
        )
        assert conn.execute("SELECT * FROM sidecar_jobs").fetchall() == (
            sidecar_rows_before
        )
        assert conn.execute("SELECT COUNT(*) FROM sidecar_private_refs").fetchone() == (
            1,
        )
        with pytest.raises(sqlite3.OperationalError, match="foreign key mismatch"):
            conn.execute("PRAGMA foreign_key_check('sidecar_private_refs')").fetchall()
        with pytest.raises(sqlite3.OperationalError, match="no such column: id"):
            conn.execute("SELECT * FROM sidecar_message_ids").fetchall()
        remaining = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE name IN "
                "('caller_body_idx', 'caller_audit', 'sidecar_message_ids')"
            ).fetchall()
        }
        # Attached objects were dropped with the old table; the detached view
        # definition survives (and is simply broken until its owner fixes it).
        assert remaining == {"sidecar_message_ids"}


def test_open_tolerates_unsupported_columns_and_objects_added_after_v6(
    tmp_path: Path,
) -> None:
    """Steady-state validation ignores unsupported extras instead of failing."""
    from simplebroker.db import BrokerDB

    db_path = tmp_path / "extras.db"
    with BrokerDB(str(db_path)) as broker:
        broker.write("jobs", "before-extra")

    with closing(sqlite3.connect(db_path)) as conn:
        conn.execute("ALTER TABLE messages ADD COLUMN caller_note TEXT")
        conn.execute(
            "UPDATE messages SET caller_note='unsupported-but-tolerated' "
            "WHERE body='before-extra'"
        )
        conn.execute("CREATE INDEX caller_claimed_idx ON messages(claimed)")
        conn.execute(
            "CREATE TRIGGER caller_after_insert AFTER INSERT ON messages "
            "BEGIN SELECT 1; END"
        )
        conn.commit()

    for _ in range(2):
        with BrokerDB(str(db_path)) as broker:
            assert broker.peek_one("jobs") is not None

    with closing(sqlite3.connect(db_path)) as conn:
        remaining = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE name IN "
                "('caller_claimed_idx', 'caller_after_insert')"
            ).fetchall()
        }
    assert remaining == {"caller_claimed_idx", "caller_after_insert"}


def test_schema_v6_preserves_public_id_foreign_key_and_pragma_state(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "public-id-reference.db"
    _create_v5_database(db_path)
    runner = _runner(db_path)
    try:
        runner.run("PRAGMA foreign_keys = ON")
        assert list(runner.run("PRAGMA foreign_keys", fetch=True)) == [(1,)]
        assert list(runner.run("PRAGMA legacy_alter_table", fetch=True)) == [(0,)]

        ensure_schema_v6(
            runner,
            current_version=5,
            write_schema_version=lambda version: _write_schema_version(runner, version),
        )

        assert list(runner.run("PRAGMA foreign_keys", fetch=True)) == [(1,)]
        assert list(runner.run("PRAGMA legacy_alter_table", fetch=True)) == [(0,)]
        assert list(runner.run("PRAGMA foreign_key_check", fetch=True)) == []
        assert list(runner.run("SELECT * FROM sidecar_message_refs", fetch=True)) == [
            (200, "keep-ref")
        ]
    finally:
        runner.close()


def test_schema_v6_failure_restores_foreign_key_pragma(tmp_path: Path) -> None:
    db_path = tmp_path / "public-id-reference-failure.db"
    _create_v5_database(db_path)
    runner = _runner(db_path)
    failing = _FailOnceRunner(
        runner,
        "ALTER TABLE simplebroker_messages_v6",
        OperationalError("injected migration failure"),
    )
    try:
        runner.run("PRAGMA foreign_keys = ON")

        with pytest.raises(OperationalError, match="injected migration failure"):
            ensure_schema_v6(
                failing,
                current_version=5,
                write_schema_version=lambda version: _write_schema_version(
                    runner, version
                ),
            )

        assert list(runner.run("PRAGMA foreign_keys", fetch=True)) == [(1,)]
        assert list(runner.run("PRAGMA legacy_alter_table", fetch=True)) == [(0,)]
        assert list(runner.run("PRAGMA foreign_key_check", fetch=True)) == []
        assert list(runner.run("SELECT * FROM sidecar_message_refs", fetch=True)) == [
            (200, "keep-ref")
        ]
        assert list(
            runner.run("SELECT value FROM meta WHERE key='schema_version'", fetch=True)
        ) == [(5,)]
    finally:
        runner.close()


def test_fresh_schema_uses_existing_unique_constraint_without_redundant_index(
    tmp_path: Path,
) -> None:
    runner = _runner(tmp_path / "fresh.db")
    try:
        initialize_database(runner, run_with_retry=_run_direct)
        migrate_schema(
            runner,
            current_version=SCHEMA_VERSION,
            write_schema_version=lambda _version: None,
        )

        unique_ts_indexes = []
        for (name,) in runner.run(
            "SELECT name FROM pragma_index_list('messages') "
            'WHERE "unique" = 1 AND partial = 0',
            fetch=True,
        ):
            columns = [
                row[0]
                for row in runner.run(
                    "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
                    (name,),
                    fetch=True,
                )
            ]
            if columns == ["ts"]:
                unique_ts_indexes.append(name)

        assert unique_ts_indexes == []
        assert ts_unique_index_exists(runner) is True
    finally:
        runner.close()


def test_schema_v3_accepts_equivalent_unique_index_with_another_name(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "equivalent-index.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        runner.run("CREATE UNIQUE INDEX custom_unique_ts ON messages(ts)")

        ensure_schema_v3(
            runner,
            current_version=2,
            write_schema_version=versions.append,
        )

        assert versions == [3]
        assert ts_unique_index_exists(runner) is True
        assert runner.run(
            "SELECT COUNT(*) FROM sqlite_master "
            "WHERE type = 'index' AND name = 'idx_messages_ts_unique'",
            fetch=True,
        ) == [(0,)]
    finally:
        runner.close()


def test_schema_v3_rejects_conflicting_owned_index_name(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "conflicting-index.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        runner.run("CREATE INDEX idx_messages_ts_unique ON messages(queue)")

        with pytest.raises(RuntimeError, match="idx_messages_ts_unique.*conflicts"):
            ensure_schema_v3(
                runner,
                current_version=2,
                write_schema_version=versions.append,
            )

        assert versions == []
        assert ts_unique_index_exists(runner) is False
    finally:
        runner.close()


def test_schema_v3_rejects_conflicting_owned_name_even_with_equivalent_index(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "conflicting-and-equivalent-index.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        runner.run("CREATE UNIQUE INDEX custom_unique_ts ON messages(ts)")
        runner.run("CREATE INDEX idx_messages_ts_unique ON messages(queue)")

        with pytest.raises(RuntimeError, match="idx_messages_ts_unique.*conflicts"):
            ensure_schema_v3(
                runner,
                current_version=2,
                write_schema_version=versions.append,
            )

        assert versions == []
    finally:
        runner.close()


@pytest.mark.parametrize(
    "failure_marker",
    [
        "CREATE TABLE IF NOT EXISTS messages",
        "CREATE TABLE IF NOT EXISTS meta",
        "VALUES ('schema_version', ?)",
    ],
    ids=["early", "middle", "late"],
)
def test_initialize_database_bootstrap_is_atomic_at_each_failure_point(
    tmp_path: Path, failure_marker: str
) -> None:
    runner = _runner(tmp_path / f"broker-{failure_marker.count(' ')}.db")
    failing_runner = _FailOnceRunner(
        runner,
        failure_marker,
        OperationalError("injected bootstrap failure"),
    )
    try:
        with pytest.raises(OperationalError, match="injected bootstrap failure"):
            initialize_database(failing_runner, run_with_retry=_run_direct)

        visible_objects = runner.run(
            "SELECT name FROM sqlite_master "
            "WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%'",
            fetch=True,
        )
        assert visible_objects == []

        initialize_database(failing_runner, run_with_retry=_run_direct)
        assert messages_has_claimed_column(runner) is True
        assert pending_queue_ts_index_exists(runner) is True
        metadata = dict(runner.run("SELECT key, value FROM meta", fetch=True))
        assert metadata["magic"] == SIMPLEBROKER_MAGIC
        assert int(metadata["schema_version"]) == SCHEMA_VERSION
        assert int(metadata["alias_version"]) == 0
    finally:
        runner.close()


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


def test_migrate_schema_applies_v2_through_v6_in_order(tmp_path: Path) -> None:
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

        assert versions == [2, 3, 4, 5, 6]
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

        assert versions == [5, 6]
        assert pending_queue_ts_index_exists(runner) is True
    finally:
        runner.close()


def test_ensure_schema_v5_runs_once_on_exact_preceding_version(
    tmp_path: Path,
) -> None:
    """The v4-to-v5 step installs its index once; reruns at v5 are no-ops."""
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
            current_version=4,
            write_schema_version=versions.append,
        )
        ensure_schema_v5(
            runner,
            current_version=5,
            write_schema_version=versions.append,
        )

        assert versions == [5]
        assert pending_queue_ts_index_exists(runner) is True
    finally:
        runner.close()


@pytest.mark.parametrize("direction", ["ASC", "DESC"])
def test_bounded_pending_selection_uses_queue_ts_index(
    tmp_path: Path, direction: str
) -> None:
    runner = _runner(tmp_path / "broker.db")
    try:
        initialize_database(runner, run_with_retry=_run_direct)

        rows = list(
            runner.run(
                f"""
                EXPLAIN QUERY PLAN
                SELECT ts
                FROM messages
                WHERE queue = ? AND claimed = 0
                ORDER BY ts {direction}
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


def test_ensure_schema_v2_adds_claimed_column(
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
        assert (
            list(
                runner.run(
                    "SELECT name FROM sqlite_master WHERE type='index' "
                    "AND name='idx_messages_unclaimed'",
                    fetch=True,
                )
            )
            == []
        )
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


def test_ensure_schema_v2_rolls_back_schema_and_durable_version_on_commit_failure(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v2-commit-failure.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        runner.run("INSERT INTO meta (key, value) VALUES ('schema_version', 1)")

        def record_schema_version(version: int) -> None:
            versions.append(version)
            runner.run(
                "UPDATE meta SET value = ? WHERE key = 'schema_version'",
                (version,),
            )

        with pytest.raises(OperationalError, match="migration commit failure"):
            ensure_schema_v2(
                _FailCommitRunner(runner),
                current_version=1,
                write_schema_version=record_schema_version,
            )

        assert runner.get_connection().in_transaction is False
        assert versions == [2]
        assert messages_has_claimed_column(runner) is False
        assert (
            list(
                runner.run(
                    "SELECT name FROM sqlite_master WHERE type='index' "
                    "AND name='idx_messages_unclaimed'",
                    fetch=True,
                )
            )
            == []
        )
        assert list(
            runner.run(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                fetch=True,
            )
        ) == [(1,)]
    finally:
        runner.close()


def test_ensure_schema_v2_propagates_non_operational_duplicate_column_error(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v1-misleading-duplicate-column.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    failing_runner = _FailOnceRunner(
        runner,
        "ALTER TABLE messages ADD COLUMN claimed",
        ValueError("misleading duplicate column name"),
    )
    versions: list[int] = []
    try:
        with pytest.raises(ValueError, match="misleading duplicate column name"):
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
    wrapped = [_BarrierBeforeFirstBegin(runner, barrier) for runner in runners]
    versions: list[list[int]] = [[], []]

    def migrate(index: int) -> None:
        ensure_schema_v2(
            wrapped[index],
            current_version=1,
            write_schema_version=versions[index].append,
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(migrate, index) for index in range(2)]
            for future in futures:
                future.result(timeout=10.0)

        assert versions == [[2], [2]]
        assert messages_has_claimed_column(runners[0]) is True
        assert all(not runner.get_connection().in_transaction for runner in runners)
    finally:
        for runner in runners:
            runner.close()


@pytest.mark.parametrize(
    ("current_version", "precreate_claimed", "expected_versions", "expected_column"),
    [(1, True, [2], True), (2, False, [], False)],
)
def test_ensure_schema_v2_runs_only_on_exact_preceding_version(
    tmp_path: Path,
    current_version: int,
    precreate_claimed: bool,
    expected_versions: list[int],
    expected_column: bool,
) -> None:
    """The v1-to-v2 step is idempotent at v1 and a strict no-op elsewhere."""
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

        assert messages_has_claimed_column(runner) is expected_column
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


def test_ensure_schema_v3_does_not_relabel_misleading_integrity_error(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v2-misleading-integrity-error.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        failing_runner = _FailOnceRunner(
            runner,
            "CREATE UNIQUE INDEX idx_messages_ts_unique",
            IntegrityError("UNIQUE constraint failed: unrelated injected constraint"),
        )

        with pytest.raises(
            IntegrityError,
            match="unrelated injected constraint",
        ):
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


def test_ensure_schema_v3_does_not_publish_version_from_already_exists_prose(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v2-misleading-already-exists.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        failing_runner = _FailOnceRunner(
            runner,
            "CREATE UNIQUE INDEX idx_messages_ts_unique",
            OperationalError("misleading already exists"),
        )

        with pytest.raises(OperationalError, match="misleading already exists"):
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


def test_ensure_schema_v3_requires_named_index_postcondition_before_version(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v2-missing-index-postcondition.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)

        with pytest.raises(
            RuntimeError,
            match="Failed to ensure the timestamp unique index",
        ):
            ensure_schema_v3(
                _IgnoreOnceRunner(
                    runner,
                    "CREATE UNIQUE INDEX idx_messages_ts_unique",
                ),
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


def test_ensure_schema_v3_rolls_back_schema_and_durable_version_on_commit_failure(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v3-commit-failure.db"
    _create_v1_messages_table(db_path)
    runner = _runner(db_path)
    versions: list[int] = []
    try:
        ensure_schema_v2(runner, current_version=1, write_schema_version=lambda _: None)
        runner.run("INSERT INTO meta (key, value) VALUES ('schema_version', 2)")

        def record_schema_version(version: int) -> None:
            versions.append(version)
            runner.run(
                "UPDATE meta SET value = ? WHERE key = 'schema_version'",
                (version,),
            )

        with pytest.raises(OperationalError, match="migration commit failure"):
            ensure_schema_v3(
                _FailCommitRunner(runner),
                current_version=2,
                write_schema_version=record_schema_version,
            )

        assert runner.get_connection().in_transaction is False
        assert versions == [3]
        assert list(
            runner.run(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                fetch=True,
            )
        ) == [(2,)]
        assert ts_unique_index_exists(runner) is False
    finally:
        runner.close()


def test_ensure_schema_v3_repair_handles_index_created_before_write_lock(
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
            _CreateTsIndexBeforeFirstBegin(runner, competitor),
            current_version=2,
            write_schema_version=versions.append,
        )

        assert versions == [3]
        assert ts_unique_index_exists(runner) is True
    finally:
        competitor.close()
        runner.close()


@pytest.mark.parametrize("current_version", [2])
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


@pytest.mark.parametrize("current_version", [3])
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


@pytest.mark.parametrize("current_version", [4])
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


class _RecordingRunner:
    """Delegate to a real runner while recording SQL and transaction calls."""

    def __init__(self, runner: SQLiteRunner) -> None:
        self._runner = runner
        self.statements: list[str] = []
        self.begin_immediate_calls = 0

    def run(self, sql, *args, **kwargs):
        self.statements.append(" ".join(str(sql).split()))
        return self._runner.run(sql, *args, **kwargs)

    def begin_immediate(self) -> None:
        self.begin_immediate_calls += 1
        self._runner.begin_immediate()

    def commit(self) -> None:
        self._runner.commit()

    def rollback(self) -> None:
        self._runner.rollback()

    def close(self) -> None:
        self._runner.close()

    def setup(self, phase: SetupPhase) -> None:
        self._runner.setup(phase)

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        return self._runner.is_setup_complete(phase)


def test_steady_state_open_runs_no_index_drop_and_no_write_transaction(
    tmp_path: Path,
) -> None:
    """A healthy v6 reopen must not drop, rebuild, or write-lock anything.

    Guards the open-time regression where the canonical ``idx_messages_queue_ts``
    name appeared in the legacy drop list and was dropped and rebuilt on every
    database open.
    """
    runner = _runner(tmp_path / "steady.db")
    try:
        initialize_database(runner, run_with_retry=_run_direct)
        migrate_schema(
            runner,
            current_version=SCHEMA_VERSION,
            write_schema_version=lambda version: _write_schema_version(runner, version),
        )

        recording = _RecordingRunner(runner)
        initialize_database(recording, run_with_retry=_run_direct)
        migrate_schema(
            recording,
            current_version=SCHEMA_VERSION,
            write_schema_version=lambda version: _write_schema_version(
                recording, version
            ),
        )

        dropped = [s for s in recording.statements if "DROP INDEX" in s.upper()]
        assert dropped == []
        # Bootstrap probe-gates on the schema_version row, the migration
        # ladder runs no step at the current version, and the validator is
        # read-only when healthy: a steady-state open is fully read-only.
        assert recording.begin_immediate_calls == 0
    finally:
        runner.close()


def test_rebuild_table_ddl_derives_from_canonical_builder() -> None:
    """The v6 rebuild shape has exactly one authored source."""
    from simplebroker._sql import CREATE_MESSAGES_TABLE, create_messages_table_sql

    assert CREATE_MESSAGES_TABLE == create_messages_table_sql()
    rebuild = create_messages_table_sql("simplebroker_messages_v6", if_not_exists=False)
    assert "CREATE TABLE simplebroker_messages_v6 (" in rebuild
    assert "IF NOT EXISTS" not in rebuild
    canonical_body = CREATE_MESSAGES_TABLE.split("(", 1)[1]
    rebuild_body = rebuild.split("(", 1)[1]
    assert canonical_body == rebuild_body
