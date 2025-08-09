"""Tests for SQLite PRAGMA settings and environment variable configuration."""

import sqlite3

import pytest

from simplebroker.db import BrokerDB


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


def test_custom_cache_size(tmp_path, monkeypatch) -> None:
    """Test that BROKER_CACHE_MB environment variable works."""
    # Import _config from the runner module
    from simplebroker._runner import _config

    # Save original value
    original_cache_mb = _config["BROKER_CACHE_MB"]

    # Update the config directly
    _config["BROKER_CACHE_MB"] = 25

    try:
        db_path = tmp_path / "test.db"

        with BrokerDB(str(db_path)) as db:
            result = db._runner.run("PRAGMA cache_size", fetch=True)
            cache_size = result[0][0]
            assert cache_size == -25600  # 25MB = 25600KiB
    finally:
        # Restore original value
        _config["BROKER_CACHE_MB"] = original_cache_mb


def test_custom_sync_mode_normal(tmp_path, monkeypatch) -> None:
    """Test that BROKER_SYNC_MODE=NORMAL works."""
    # Import _config from the runner module
    from simplebroker._runner import _config

    # Save original value
    original_sync_mode = _config["BROKER_SYNC_MODE"]

    # Update the config directly
    _config["BROKER_SYNC_MODE"] = "NORMAL"

    try:
        db_path = tmp_path / "test.db"

        with BrokerDB(str(db_path)) as db:
            result = db._runner.run("PRAGMA synchronous", fetch=True)
            sync_mode = result[0][0]
            assert sync_mode == 1  # NORMAL = 1
    finally:
        # Restore original value
        _config["BROKER_SYNC_MODE"] = original_sync_mode


def test_custom_sync_mode_off(tmp_path, monkeypatch) -> None:
    """Test that BROKER_SYNC_MODE=OFF works."""
    # Import _config from the runner module
    from simplebroker._runner import _config

    # Save original value
    original_sync_mode = _config["BROKER_SYNC_MODE"]

    # Update the config directly
    _config["BROKER_SYNC_MODE"] = "OFF"

    try:
        db_path = tmp_path / "test.db"

        with BrokerDB(str(db_path)) as db:
            result = db._runner.run("PRAGMA synchronous", fetch=True)
            sync_mode = result[0][0]
            assert sync_mode == 0  # OFF = 0
    finally:
        # Restore original value
        _config["BROKER_SYNC_MODE"] = original_sync_mode


def test_invalid_sync_mode_defaults_to_full(tmp_path, monkeypatch) -> None:
    """Test that invalid BROKER_SYNC_MODE defaults to FULL with warning."""
    # Import _config from the runner module
    from simplebroker._runner import _config

    # Save original value
    original_sync_mode = _config["BROKER_SYNC_MODE"]

    # Update the config directly with invalid value
    _config["BROKER_SYNC_MODE"] = "INVALID"

    try:
        db_path = tmp_path / "test.db"

        with pytest.warns(RuntimeWarning, match="Invalid BROKER_SYNC_MODE 'INVALID'"):
            with BrokerDB(str(db_path)) as db:
                result = db._runner.run("PRAGMA synchronous", fetch=True)
                sync_mode = result[0][0]
                assert sync_mode == 2  # FULL = 2
    finally:
        # Restore original value
        _config["BROKER_SYNC_MODE"] = original_sync_mode


def test_sync_mode_case_insensitive(tmp_path, monkeypatch) -> None:
    """Test that BROKER_SYNC_MODE is case-insensitive."""
    # Import _config from the runner module
    from simplebroker._runner import _config

    # Save original value
    original_sync_mode = _config["BROKER_SYNC_MODE"]

    # Update the config directly - need to uppercase since load_config does that
    _config["BROKER_SYNC_MODE"] = "NORMAL"

    try:
        db_path = tmp_path / "test.db"

        with BrokerDB(str(db_path)) as db:
            result = db._runner.run("PRAGMA synchronous", fetch=True)
            sync_mode = result[0][0]
            assert sync_mode == 1  # NORMAL = 1
    finally:
        # Restore original value
        _config["BROKER_SYNC_MODE"] = original_sync_mode


def test_write_with_normal_sync_works(tmp_path, monkeypatch) -> None:
    """Test that writes work correctly with NORMAL sync mode."""
    # Import _config from the runner module
    from simplebroker._runner import _config

    # Save original value
    original_sync_mode = _config["BROKER_SYNC_MODE"]

    # Update the config directly
    _config["BROKER_SYNC_MODE"] = "NORMAL"

    db_path = tmp_path / "test.db"

    # Write messages
    with BrokerDB(str(db_path)) as db:
        for i in range(10):
            db.write("test_queue", f"message {i}")

    # Read them back to verify
    with BrokerDB(str(db_path)) as db:
        messages = db.peek_many("test_queue", limit=10, with_timestamps=False)
        assert len(messages) == 10
        for i, msg in enumerate(messages):
            assert msg == f"message {i}"

    # Restore original value
    _config["BROKER_SYNC_MODE"] = original_sync_mode


def test_custom_wal_autocheckpoint(tmp_path, monkeypatch) -> None:
    """Test that BROKER_WAL_AUTOCHECKPOINT environment variable works."""
    # Import _config from the runner module
    from simplebroker._runner import _config

    # Save original value
    original_wal_autocheckpoint = _config["BROKER_WAL_AUTOCHECKPOINT"]

    # Update the config directly
    _config["BROKER_WAL_AUTOCHECKPOINT"] = 5000

    try:
        db_path = tmp_path / "test.db"

        with BrokerDB(str(db_path)) as db:
            # The setting should be applied to the connection used by the runner
            result = db._runner.run("PRAGMA wal_autocheckpoint", fetch=True)
            autocheckpoint = result[0][0]
            assert autocheckpoint == 5000
    finally:
        # Restore original value
        _config["BROKER_WAL_AUTOCHECKPOINT"] = original_wal_autocheckpoint


def test_invalid_wal_autocheckpoint_defaults(tmp_path, monkeypatch) -> None:
    """Test that invalid BROKER_WAL_AUTOCHECKPOINT defaults to 1000 with warning."""
    # Import _config from the runner module
    from simplebroker._runner import _config

    # Save original value
    original_wal_autocheckpoint = _config["BROKER_WAL_AUTOCHECKPOINT"]

    # Update the config directly
    _config["BROKER_WAL_AUTOCHECKPOINT"] = -100

    try:
        db_path = tmp_path / "test.db"

        with pytest.warns(
            UserWarning,
            match="Invalid BROKER_WAL_AUTOCHECKPOINT '-100'",
        ), BrokerDB(str(db_path)) as db:
            result = db._runner.run("PRAGMA wal_autocheckpoint", fetch=True)
            autocheckpoint = result[0][0]
            assert autocheckpoint == 1000  # Default value
    finally:
        # Restore original value
        _config["BROKER_WAL_AUTOCHECKPOINT"] = original_wal_autocheckpoint


def test_wal_autocheckpoint_zero_disables(tmp_path, monkeypatch) -> None:
    """Test that BROKER_WAL_AUTOCHECKPOINT=0 disables automatic checkpoints."""
    # Import _config from the runner module
    from simplebroker._runner import _config

    # Save original value
    original_wal_autocheckpoint = _config["BROKER_WAL_AUTOCHECKPOINT"]

    # Update the config directly
    _config["BROKER_WAL_AUTOCHECKPOINT"] = 0

    try:
        db_path = tmp_path / "test.db"

        with BrokerDB(str(db_path)) as db:
            result = db._runner.run("PRAGMA wal_autocheckpoint", fetch=True)
            autocheckpoint = result[0][0]
            assert autocheckpoint == 0  # Disabled
    finally:
        # Restore original value
        _config["BROKER_WAL_AUTOCHECKPOINT"] = original_wal_autocheckpoint


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
