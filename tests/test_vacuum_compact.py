"""Tests for the vacuum compact functionality."""

import sqlite3
from pathlib import Path
from unittest.mock import patch

from simplebroker.db import BrokerDB

from .conftest import run_cli


def test_vacuum_with_compact_flag(workdir: Path):
    """Test that vacuum with compact=True runs SQLite VACUUM command."""
    db_path = workdir / "test.db"

    # Create database with some messages
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            db.write("test_queue", f"message{i}")

    # Claim messages
    with BrokerDB(str(db_path)) as db:
        messages = db.claim_many("test_queue", limit=5, with_timestamps=False)
        assert len(messages) == 5

    # Mock the runner to track SQL commands
    with BrokerDB(str(db_path)) as db:
        original_run = db._runner.run
        sql_commands = []

        def track_sql(sql, params=(), *, fetch=False):
            sql_commands.append(sql.strip())
            return original_run(sql, params, fetch=fetch)

        with patch.object(db._runner, "run", side_effect=track_sql):
            db.vacuum(compact=True)

        # Check that VACUUM command was executed
        assert "VACUUM" in sql_commands


def test_vacuum_without_compact_flag(workdir: Path):
    """Test that vacuum without compact=True doesn't run SQLite VACUUM command."""
    db_path = workdir / "test.db"

    # Create database with some messages
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            db.write("test_queue", f"message{i}")

    # Claim messages
    with BrokerDB(str(db_path)) as db:
        messages = db.claim_many("test_queue", limit=5, with_timestamps=False)
        assert len(messages) == 5

    # Mock the runner to track SQL commands
    with BrokerDB(str(db_path)) as db:
        original_run = db._runner.run
        sql_commands = []

        def track_sql(sql, params=(), *, fetch=False):
            sql_commands.append(sql.strip())
            return original_run(sql, params, fetch=fetch)

        with patch.object(db._runner, "run", side_effect=track_sql):
            db.vacuum(compact=False)

        # Check that VACUUM command was NOT executed
        assert "VACUUM" not in sql_commands


def test_cli_vacuum_with_compact(workdir: Path):
    """Test CLI vacuum command with --compact flag."""
    db_path = workdir / "test.db"

    # Create database with some messages
    returncode, stdout, stderr = run_cli(
        "-f", str(db_path), "write", "test_queue", "message1", cwd=workdir
    )
    assert returncode == 0

    # Read to claim the message
    returncode, stdout, stderr = run_cli(
        "-f", str(db_path), "read", "test_queue", cwd=workdir
    )
    assert returncode == 0

    # Run vacuum with compact
    returncode, stdout, stderr = run_cli(
        "-f", str(db_path), "--vacuum", "--compact", cwd=workdir
    )
    assert returncode == 0
    assert "compacted" in stdout.lower()


def test_cli_compact_requires_vacuum(workdir: Path):
    """Test that --compact flag requires --vacuum."""
    db_path = workdir / "test.db"

    # Try to use --compact without --vacuum
    returncode, stdout, stderr = run_cli("-f", str(db_path), "--compact", cwd=workdir)
    assert returncode == 1
    assert "--compact can only be used with --vacuum" in stderr


def test_cli_vacuum_exclusive_with_commands(workdir: Path):
    """Test that --vacuum cannot be used with other commands."""
    db_path = workdir / "test.db"

    # Try to use --vacuum with a command
    returncode, stdout, stderr = run_cli(
        "-f", str(db_path), "--vacuum", "list", cwd=workdir
    )
    assert returncode == 1
    assert "--vacuum cannot be used with commands" in stderr


def test_compact_with_no_claimed_messages(workdir: Path):
    """Test compact runs even when no claimed messages exist."""
    db_path = workdir / "test.db"

    # Create database with unclaimed messages only
    with BrokerDB(str(db_path)) as db:
        db.write("test_queue", "message")

    # Run vacuum with compact when no claimed messages
    with BrokerDB(str(db_path)) as db:
        original_run = db._runner.run
        sql_commands = []

        def track_sql(sql, params=(), *, fetch=False):
            sql_commands.append(sql.strip())
            return original_run(sql, params, fetch=fetch)

        with patch.object(db._runner, "run", side_effect=track_sql):
            db.vacuum(compact=True)

        # VACUUM should still run even with no claimed messages
        assert "VACUUM" in sql_commands


def test_vacuum_compact_database_size_reduction(workdir: Path):
    """Test that VACUUM actually reduces database size after deleting claimed messages."""
    db_path = workdir / "test.db"

    # Create many messages
    with BrokerDB(str(db_path)) as db:
        for i in range(100):
            db.write("test_queue", f"message{i}" * 100)  # Larger messages

    # Claim all messages
    with BrokerDB(str(db_path)) as db:
        messages = db.claim_many("test_queue", limit=1000, with_timestamps=False)
        assert len(messages) == 100

    # Get size after claiming
    size_with_claimed = db_path.stat().st_size

    # Run vacuum without compact (just delete claimed)
    with BrokerDB(str(db_path)) as db:
        db.vacuum(compact=False)

    # Size should not change much after just deleting rows
    size_after_delete = db_path.stat().st_size
    assert size_after_delete >= size_with_claimed * 0.9  # Still mostly the same size

    # Now run with compact
    with BrokerDB(str(db_path)) as db:
        db.vacuum(compact=True)

    # Size should be significantly reduced after VACUUM
    size_after_vacuum = db_path.stat().st_size
    assert size_after_vacuum < size_with_claimed * 0.5  # Should be much smaller


def test_new_database_gets_auto_vacuum_incremental(workdir: Path):
    """Test that new databases are created with auto_vacuum=INCREMENTAL."""
    db_path = workdir / "test.db"

    # Create a new database
    with BrokerDB(str(db_path)) as db:
        db.write("test_queue", "message1")

    # Check auto_vacuum setting
    conn = sqlite3.connect(str(db_path))
    cursor = conn.execute("PRAGMA auto_vacuum")
    auto_vacuum_mode = cursor.fetchone()[0]
    conn.close()

    # Should be 2 (INCREMENTAL)
    assert auto_vacuum_mode == 2


def test_vacuum_compact_sets_auto_vacuum(workdir: Path):
    """Test that vacuum with compact sets auto_vacuum=INCREMENTAL for existing databases."""
    db_path = workdir / "test.db"

    # Create database without auto_vacuum (simulate old database)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE messages (id INTEGER, queue TEXT, body TEXT, ts INTEGER, claimed INTEGER)"
    )
    conn.close()

    # Verify auto_vacuum is not set
    conn = sqlite3.connect(str(db_path))
    cursor = conn.execute("PRAGMA auto_vacuum")
    assert cursor.fetchone()[0] == 0  # NONE
    conn.close()

    # Create BrokerDB instance and run vacuum with compact
    with BrokerDB(str(db_path)) as db:
        db.vacuum(compact=True)

    # Check auto_vacuum setting after compact
    conn = sqlite3.connect(str(db_path))
    cursor = conn.execute("PRAGMA auto_vacuum")
    auto_vacuum_mode = cursor.fetchone()[0]
    conn.close()

    # Should now be 2 (INCREMENTAL)
    assert auto_vacuum_mode == 2


def test_automatic_vacuum_runs_incremental_vacuum(workdir: Path):
    """Test that automatic vacuum runs incremental_vacuum when auto_vacuum is set."""
    db_path = workdir / "test.db"

    # Create database (will have auto_vacuum=INCREMENTAL)
    with BrokerDB(str(db_path)) as db:
        for i in range(5):
            db.write("test_queue", f"message{i}")

    # Claim messages
    with BrokerDB(str(db_path)) as db:
        messages = db.claim_many("test_queue", limit=5, with_timestamps=False)
        assert len(messages) == 5

    # Mock the runner to track SQL commands during automatic vacuum
    with BrokerDB(str(db_path)) as db:
        original_run = db._runner.run
        sql_commands = []

        def track_sql(sql, params=(), *, fetch=False):
            sql_commands.append(sql.strip())
            return original_run(sql, params, fetch=fetch)

        with patch.object(db._runner, "run", side_effect=track_sql):
            # Call the internal vacuum method (simulating automatic vacuum)
            db._vacuum_claimed_messages(compact=False)

        # Check that incremental_vacuum was called
        assert "PRAGMA incremental_vacuum(100)" in sql_commands
        # Should NOT have run full VACUUM
        assert "VACUUM" not in sql_commands
