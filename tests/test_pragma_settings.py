"""Tests for SQLite PRAGMA settings and environment variable configuration."""

import sqlite3

import pytest

from simplebroker.db import BrokerDB


def test_default_pragma_settings(tmp_path):
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


def test_custom_cache_size(tmp_path, monkeypatch):
    """Test that BROKER_CACHE_MB environment variable works."""
    monkeypatch.setenv("BROKER_CACHE_MB", "25")
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path)) as db:
        result = db._runner.run("PRAGMA cache_size", fetch=True)
        cache_size = result[0][0]
        assert cache_size == -25600  # 25MB = 25600KiB


def test_custom_sync_mode_normal(tmp_path, monkeypatch):
    """Test that BROKER_SYNC_MODE=NORMAL works."""
    monkeypatch.setenv("BROKER_SYNC_MODE", "NORMAL")
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path)) as db:
        result = db._runner.run("PRAGMA synchronous", fetch=True)
        sync_mode = result[0][0]
        assert sync_mode == 1  # NORMAL = 1


def test_custom_sync_mode_off(tmp_path, monkeypatch):
    """Test that BROKER_SYNC_MODE=OFF works."""
    monkeypatch.setenv("BROKER_SYNC_MODE", "OFF")
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path)) as db:
        result = db._runner.run("PRAGMA synchronous", fetch=True)
        sync_mode = result[0][0]
        assert sync_mode == 0  # OFF = 0


def test_invalid_sync_mode_defaults_to_full(tmp_path, monkeypatch):
    """Test that invalid BROKER_SYNC_MODE defaults to FULL with warning."""
    monkeypatch.setenv("BROKER_SYNC_MODE", "INVALID")
    db_path = tmp_path / "test.db"

    with pytest.warns(RuntimeWarning, match="Invalid BROKER_SYNC_MODE 'INVALID'"):
        with BrokerDB(str(db_path)) as db:
            result = db._runner.run("PRAGMA synchronous", fetch=True)
            sync_mode = result[0][0]
            assert sync_mode == 2  # FULL = 2


def test_sync_mode_case_insensitive(tmp_path, monkeypatch):
    """Test that BROKER_SYNC_MODE is case-insensitive."""
    monkeypatch.setenv("BROKER_SYNC_MODE", "normal")
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path)) as db:
        result = db._runner.run("PRAGMA synchronous", fetch=True)
        sync_mode = result[0][0]
        assert sync_mode == 1  # NORMAL = 1


def test_write_with_normal_sync_works(tmp_path, monkeypatch):
    """Test that writes work correctly with NORMAL sync mode."""
    monkeypatch.setenv("BROKER_SYNC_MODE", "NORMAL")
    db_path = tmp_path / "test.db"

    # Write messages
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            db.write("test_queue", f"message {i}")

    # Read them back to verify
    with BrokerDB(str(db_path)) as db:
        messages = db.read("test_queue", all_messages=True)
        assert len(messages) == 10
        for i, msg in enumerate(messages):
            assert msg == f"message {i}"


def test_custom_wal_autocheckpoint(tmp_path, monkeypatch):
    """Test that BROKER_WAL_AUTOCHECKPOINT environment variable works."""
    monkeypatch.setenv("BROKER_WAL_AUTOCHECKPOINT", "5000")
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path)) as db:
        # The setting should be applied to the connection used by the runner
        result = db._runner.run("PRAGMA wal_autocheckpoint", fetch=True)
        autocheckpoint = result[0][0]
        assert autocheckpoint == 5000


def test_invalid_wal_autocheckpoint_defaults(tmp_path, monkeypatch):
    """Test that invalid BROKER_WAL_AUTOCHECKPOINT defaults to 1000 with warning."""
    monkeypatch.setenv("BROKER_WAL_AUTOCHECKPOINT", "-100")
    db_path = tmp_path / "test.db"

    with pytest.warns(UserWarning, match="Invalid BROKER_WAL_AUTOCHECKPOINT '-100'"):
        with BrokerDB(str(db_path)) as db:
            result = db._runner.run("PRAGMA wal_autocheckpoint", fetch=True)
            autocheckpoint = result[0][0]
            assert autocheckpoint == 1000  # Default value


def test_wal_autocheckpoint_zero_disables(tmp_path, monkeypatch):
    """Test that BROKER_WAL_AUTOCHECKPOINT=0 disables automatic checkpoints."""
    monkeypatch.setenv("BROKER_WAL_AUTOCHECKPOINT", "0")
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path)) as db:
        result = db._runner.run("PRAGMA wal_autocheckpoint", fetch=True)
        autocheckpoint = result[0][0]
        assert autocheckpoint == 0  # Disabled


def test_index_migration_from_old_database(tmp_path):
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
