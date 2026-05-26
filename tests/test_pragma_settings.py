"""Tests for SQLite PRAGMA settings and environment variable configuration."""

import os
import sqlite3
from typing import cast

import pytest

from simplebroker._backends.sqlite import runtime as sqlite_runtime
from simplebroker._constants import load_config
from simplebroker._runner import SetupPhase, SQLiteRunner
from simplebroker.db import BrokerCore, BrokerDB


class TrackingCursor:
    def __init__(self, row: tuple[str, ...] | None = None) -> None:
        self.row = row
        self.closed = False

    def fetchone(self) -> tuple[str, ...] | None:
        return self.row

    def close(self) -> None:
        self.closed = True


class TrackingConnection:
    def __init__(self) -> None:
        self.cursors: list[TrackingCursor] = []
        self.closed = False

    def execute(self, sql: str) -> TrackingCursor:
        row = None
        if sql == "PRAGMA journal_mode":
            row = ("delete",)
        elif sql == "PRAGMA journal_mode=WAL":
            row = ("wal",)
        cursor = TrackingCursor(row)
        self.cursors.append(cursor)
        return cursor

    def close(self) -> None:
        self.closed = True


def test_default_pragma_settings(tmp_path) -> None:
    """Test that default PRAGMA settings are applied correctly."""
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path)) as db:
        # Check cache size - default should be 10MB = 10240KiB
        result = db._runner.run("PRAGMA cache_size", fetch=True)
        cache_size = result[0][0]
        assert cache_size == -10240  # Negative means KiB

        # Check synchronous mode - default should be FULL (2)
        result = db._runner.run("PRAGMA synchronous", fetch=True)
        sync_mode = result[0][0]
        assert sync_mode == 2  # FULL = 2

        # Check that composite index exists
        result = db._runner.run(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_messages_queue_ts_id'",
            fetch=True,
        )
        assert len(result) > 0

        # Check that old indexes don't exist
        for old_index in ["idx_messages_queue_ts", "idx_queue_id", "idx_queue_ts"]:
            result = db._runner.run(
                "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
                (old_index,),
                fetch=True,
            )
            assert len(result) == 0


def test_brokercore_initializes_wal_mode(tmp_path) -> None:
    """BrokerCore should run SQLite connection-phase setup, including WAL mode."""
    db_path = tmp_path / "test.db"
    runner = SQLiteRunner(str(db_path))

    with BrokerCore(runner) as db:
        db.write("test_queue", "message")
        result = runner.run("PRAGMA journal_mode", fetch=True)
        assert result[0][0].lower() == "wal"


def test_sqlite_runner_uses_constructor_config(tmp_path) -> None:
    """SQLiteRunner should apply config passed through its constructor."""
    db_path = tmp_path / "test.db"
    runner = SQLiteRunner(
        str(db_path),
        config={
            "BROKER_BUSY_TIMEOUT": 1234,
            "BROKER_CACHE_MB": 25,
            "BROKER_WAL_AUTOCHECKPOINT": 5000,
        },
    )

    with BrokerCore(runner):
        assert runner.run("PRAGMA busy_timeout", fetch=True)[0][0] == 1234
        assert runner.run("PRAGMA cache_size", fetch=True)[0][0] == -25600
        assert runner.run("PRAGMA wal_autocheckpoint", fetch=True)[0][0] == 5000


def test_sqlite_runner_restores_optimization_settings_after_fork_detection(
    tmp_path,
) -> None:
    """Inherited runners should recover optimization state before opening child conns."""
    db_path = tmp_path / "test.db"
    runner = SQLiteRunner(str(db_path), config={"BROKER_CACHE_MB": 25})

    with BrokerCore(runner):
        assert runner.run("PRAGMA cache_size", fetch=True)[0][0] == -25600

        runner._pid = os.getpid() - 1
        conn = runner.get_connection()

        assert SetupPhase.OPTIMIZATION in runner._completed_phases
        assert conn.execute("PRAGMA cache_size").fetchone()[0] == -25600


def test_sqlite_runtime_closes_connection_setting_cursors() -> None:
    """Setup PRAGMA cursors should not wait for connection-close finalization."""
    config = load_config()
    tracker = TrackingConnection()
    conn = cast(sqlite3.Connection, tracker)

    sqlite_runtime.apply_connection_settings(
        conn,
        config=config,
        optimization_complete=True,
    )

    assert tracker.cursors
    assert all(cursor.closed for cursor in tracker.cursors)


def test_sqlite_connection_phase_closes_setup_cursors(monkeypatch, tmp_path) -> None:
    """Connection-phase setup should finalize every cursor before closing."""
    config = load_config()
    conn = TrackingConnection()

    monkeypatch.setattr(sqlite_runtime, "check_version", lambda: None)
    monkeypatch.setattr(sqlite_runtime.sqlite3, "connect", lambda *args, **kwargs: conn)

    sqlite_runtime.setup_connection_phase(str(tmp_path / "test.db"), config=config)

    assert conn.closed
    assert conn.cursors
    assert all(cursor.closed for cursor in conn.cursors)


def test_custom_cache_size(tmp_path) -> None:
    """Test that BROKER_CACHE_MB config works."""
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path), config={"BROKER_CACHE_MB": 25}) as db:
        result = db._runner.run("PRAGMA cache_size", fetch=True)
        cache_size = result[0][0]
        assert cache_size == -25600  # 25MB = 25600KiB


def test_custom_sync_mode_normal(tmp_path) -> None:
    """Test that BROKER_SYNC_MODE=NORMAL works."""
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path), config={"BROKER_SYNC_MODE": "NORMAL"}) as db:
        result = db._runner.run("PRAGMA synchronous", fetch=True)
        sync_mode = result[0][0]
        assert sync_mode == 1  # NORMAL = 1


def test_custom_sync_mode_off(tmp_path) -> None:
    """Test that BROKER_SYNC_MODE=OFF works."""
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path), config={"BROKER_SYNC_MODE": "OFF"}) as db:
        result = db._runner.run("PRAGMA synchronous", fetch=True)
        sync_mode = result[0][0]
        assert sync_mode == 0  # OFF = 0


def test_invalid_sync_mode_defaults_to_full(tmp_path) -> None:
    """Test that invalid BROKER_SYNC_MODE defaults to FULL."""
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path), config={"BROKER_SYNC_MODE": "INVALID"}) as db:
        result = db._runner.run("PRAGMA synchronous", fetch=True)
        sync_mode = result[0][0]
        assert sync_mode == 2  # FULL = 2


def test_sync_mode_case_insensitive(tmp_path) -> None:
    """Test that BROKER_SYNC_MODE is case-insensitive."""
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path), config={"BROKER_SYNC_MODE": "normal"}) as db:
        result = db._runner.run("PRAGMA synchronous", fetch=True)
        sync_mode = result[0][0]
        assert sync_mode == 1  # NORMAL = 1


def test_write_with_normal_sync_works(tmp_path) -> None:
    """Test that writes work correctly with NORMAL sync mode."""
    db_path = tmp_path / "test.db"

    # Write messages
    with BrokerDB(str(db_path), config={"BROKER_SYNC_MODE": "NORMAL"}) as db:
        for i in range(10):
            db.write("test_queue", f"message {i}")

    # Read them back to verify
    with BrokerDB(str(db_path)) as db:
        messages = db.peek_many("test_queue", limit=10, with_timestamps=False)
        assert len(messages) == 10
        for i, msg in enumerate(messages):
            assert msg == f"message {i}"


def test_custom_wal_autocheckpoint(tmp_path) -> None:
    """Test that BROKER_WAL_AUTOCHECKPOINT config works."""
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path), config={"BROKER_WAL_AUTOCHECKPOINT": 5000}) as db:
        result = db._runner.run("PRAGMA wal_autocheckpoint", fetch=True)
        autocheckpoint = result[0][0]
        assert autocheckpoint == 5000


def test_invalid_wal_autocheckpoint_defaults(tmp_path) -> None:
    """Test that invalid BROKER_WAL_AUTOCHECKPOINT defaults to 1000 with warning."""
    db_path = tmp_path / "test.db"

    with (
        pytest.warns(
            UserWarning,
            match="Invalid BROKER_WAL_AUTOCHECKPOINT '-100'",
        ),
        BrokerDB(str(db_path), config={"BROKER_WAL_AUTOCHECKPOINT": -100}) as db,
    ):
        result = db._runner.run("PRAGMA wal_autocheckpoint", fetch=True)
        autocheckpoint = result[0][0]
        assert autocheckpoint == 1000  # Default value


def test_wal_autocheckpoint_zero_disables(tmp_path) -> None:
    """Test that BROKER_WAL_AUTOCHECKPOINT=0 disables automatic checkpoints."""
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path), config={"BROKER_WAL_AUTOCHECKPOINT": 0}) as db:
        result = db._runner.run("PRAGMA wal_autocheckpoint", fetch=True)
        autocheckpoint = result[0][0]
        assert autocheckpoint == 0  # Disabled


def test_index_migration_from_old_database(tmp_path) -> None:
    """Test that old indexes are properly removed when opening existing database."""
    db_path = tmp_path / "test.db"

    # Create database with old schema
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue TEXT NOT NULL,
            body TEXT NOT NULL,
            ts INTEGER NOT NULL
        )
    """)
    # Create old indexes
    conn.execute("CREATE INDEX idx_messages_queue_ts ON messages(queue, ts)")
    conn.execute("CREATE INDEX idx_queue_id ON messages(queue, id)")
    conn.execute("""
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value INTEGER NOT NULL
        )
    """)
    conn.execute("INSERT INTO meta (key, value) VALUES ('last_ts', 0)")
    conn.commit()
    conn.close()

    # Now open with BrokerDB - should remove old indexes and create new one
    with BrokerDB(str(db_path)) as db:
        # Check that old indexes are gone
        result = db._runner.run(
            "SELECT name FROM sqlite_master WHERE type='index' AND name IN ('idx_messages_queue_ts', 'idx_queue_id')",
            fetch=True,
        )
        assert len(result) == 0

        # Check that new composite index exists
        result = db._runner.run(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_messages_queue_ts_id'",
            fetch=True,
        )
        assert len(result) > 0
