"""Tests for the vacuum compact functionality."""

import json
import sqlite3
from pathlib import Path

from simplebroker.db import BrokerDB

from .conftest import run_cli


def _seed_cli_vacuum_target(db_path: Path, *, claimed: bool) -> None:
    """Create the database state needed by a CLI vacuum assertion."""
    with BrokerDB(str(db_path)) as db:
        db.write("test_queue", "message1")
        if claimed:
            messages = db.claim_many("test_queue", limit=1, with_timestamps=False)
            assert messages == ["message1"]


def test_cli_vacuum_with_compact(workdir: Path):
    """Test CLI vacuum command with --compact flag."""
    db_path = workdir / "test.db"
    _seed_cli_vacuum_target(db_path, claimed=True)

    # Run vacuum with compact
    returncode, stdout, stderr = run_cli(
        "-f", str(db_path), "--vacuum", "--compact", cwd=workdir
    )
    assert returncode == 0
    assert stdout == ""
    assert "compacted" in stderr.lower()


def test_cli_vacuum_status_goes_to_stderr(workdir: Path):
    """Vacuum status is diagnostic output, not stdout payload."""
    db_path = workdir / "test.db"
    _seed_cli_vacuum_target(db_path, claimed=True)

    returncode, stdout, stderr = run_cli("-f", str(db_path), "--vacuum", cwd=workdir)

    assert returncode == 0
    assert stdout == ""
    assert "Vacuumed 1 claimed messages" in stderr


def test_cli_vacuum_json_establishes_error_mode_without_success_payload(
    workdir: Path,
):
    db_path = workdir / "test.db"
    _seed_cli_vacuum_target(db_path, claimed=True)

    returncode, stdout, stderr = run_cli(
        "-f", str(db_path), "--vacuum", "--json", cwd=workdir
    )

    assert returncode == 0
    assert stdout == ""
    assert "Vacuumed 1 claimed messages" in stderr


def test_cli_vacuum_json_quiet_success_is_silent(workdir: Path):
    db_path = workdir / "test.db"
    _seed_cli_vacuum_target(db_path, claimed=True)

    returncode, stdout, stderr = run_cli(
        "-f",
        str(db_path),
        "--quiet",
        "--vacuum",
        "--json",
        cwd=workdir,
    )

    assert returncode == 0
    assert stdout == ""
    assert stderr == ""


def test_cli_vacuum_compact_json_keeps_success_output_unchanged(workdir: Path):
    db_path = workdir / "test.db"
    _seed_cli_vacuum_target(db_path, claimed=True)

    returncode, stdout, stderr = run_cli(
        "-f",
        str(db_path),
        "--vacuum",
        "--compact",
        "--json",
        cwd=workdir,
    )

    assert returncode == 0
    assert stdout == ""
    assert "compacted" in stderr.lower()


def test_cli_vacuum_json_structures_post_parse_target_error(workdir: Path):
    db_path = workdir / "missing" / "broker.db"

    returncode, stdout, stderr = run_cli(
        "-f", str(db_path), "--vacuum", "--json", cwd=workdir
    )

    assert returncode == 1
    assert stdout == ""
    payload = json.loads(stderr)
    assert set(payload) == {"error", "message", "retryable"}
    assert payload["error"] == "INVALID_ARGUMENT"
    assert isinstance(payload["message"], str)
    assert payload["retryable"] is False
    assert "Traceback" not in stderr


def test_cli_vacuum_json_structures_corrupt_target_error(workdir: Path):
    db_path = workdir / "corrupt.db"
    db_path.write_bytes(b"this is not a sqlite database\n")

    returncode, stdout, stderr = run_cli(
        "-f", str(db_path), "--vacuum", "--json", cwd=workdir
    )

    assert returncode == 1
    assert stdout == ""
    payload = json.loads(stderr)
    assert set(payload) == {"error", "message", "retryable"}
    assert payload["error"] == "ERROR"
    assert isinstance(payload["message"], str)
    assert payload["retryable"] is False
    assert "Traceback" not in stderr


def test_cli_vacuum_json_after_explicit_marker_does_not_establish_mode(
    workdir: Path,
):
    returncode, stdout, stderr = run_cli("--vacuum", "--", "--json", cwd=workdir)

    assert returncode == 1
    assert stdout == ""
    assert not stderr.startswith("{")
    assert "--json" in stderr
    assert "error" in stderr.lower()


def test_cli_vacuum_quiet_suppresses_status(workdir: Path):
    """Quiet mode suppresses vacuum status without changing the exit code."""
    db_path = workdir / "test.db"
    _seed_cli_vacuum_target(db_path, claimed=True)

    returncode, stdout, stderr = run_cli(
        "-f", str(db_path), "--quiet", "--vacuum", cwd=workdir
    )

    assert returncode == 0
    assert stdout == ""
    assert stderr == ""


def test_cli_vacuum_no_claimed_status_goes_to_stderr(workdir: Path):
    """The no-op vacuum message is status output on stderr."""
    db_path = workdir / "test.db"
    _seed_cli_vacuum_target(db_path, claimed=False)

    returncode, stdout, stderr = run_cli("-f", str(db_path), "--vacuum", cwd=workdir)

    assert returncode == 0
    assert stdout == ""
    assert "No claimed messages to vacuum" in stderr


def test_cli_compact_requires_vacuum(workdir: Path):
    """Test that --compact flag requires --vacuum."""
    db_path = workdir / "test.db"

    # Try to use --compact without --vacuum
    returncode, _stdout, stderr = run_cli("-f", str(db_path), "--compact", cwd=workdir)
    assert returncode == 1
    assert "--compact can only be used with --vacuum" in stderr


def test_cli_vacuum_exclusive_with_commands(workdir: Path):
    """Test that --vacuum cannot be used with other commands."""
    db_path = workdir / "test.db"

    # Try to use --vacuum with a command
    returncode, _stdout, stderr = run_cli(
        "-f", str(db_path), "--vacuum", "list", cwd=workdir
    )
    assert returncode == 1
    assert "--vacuum cannot be used with commands" in stderr


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
