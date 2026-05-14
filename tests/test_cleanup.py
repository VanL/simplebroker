"""
Tests for the --cleanup functionality.

Test cases:
- Cleanup of existing database
- Cleanup of non-existent database (should succeed)
- Cleanup with custom -d and -f options
- Cleanup with --quiet flag
- Cleanup exits without processing commands
"""

import os
import sqlite3
import subprocess
import sys
from contextlib import closing

import pytest

from simplebroker._backend_plugins import get_backend_plugin
from simplebroker._project_config import PROJECT_CONFIG_FILENAME, load_project_config

from .conftest import build_cli_env, run_cli
from .helper_scripts.timing import wait_for_condition


def _uses_sqlite_backend() -> bool:
    return os.environ.get("BROKER_TEST_BACKEND", "sqlite") == "sqlite"


def _write_sqlite_meta_db(db_path, *, magic: str | None) -> None:
    with closing(sqlite3.connect(str(db_path))) as conn:
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        if magic is not None:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('magic', ?)",
                (magic,),
            )
        conn.commit()


def test_cleanup_existing_database(workdir):
    """Test cleaning up an existing database."""
    # Create a database by writing a message
    rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0

    # Verify database exists
    db_path = workdir / ".broker.db"
    if _uses_sqlite_backend():
        assert db_path.exists()

    # Clean it up
    rc, out, err = run_cli("--cleanup", cwd=workdir)
    assert rc == 0
    assert out == ""
    assert "Database cleaned up" in err
    if _uses_sqlite_backend():
        assert wait_for_condition(
            lambda: not db_path.exists(), timeout=1.0, interval=0.05
        )
    else:
        # On PG, --cleanup drops the schema.  Re-init then verify empty.
        rc, _, _ = run_cli("init", cwd=workdir)
        assert rc == 0
        rc, out, err = run_cli("list", cwd=workdir)
        assert rc == 0, err
        assert out == ""


def test_cleanup_nonexistent_database(workdir):
    """Test cleaning up a non-existent database."""
    # Ensure database doesn't exist
    db_path = workdir / ".broker.db"
    if _uses_sqlite_backend():
        assert not db_path.exists()
    else:
        config = load_project_config(workdir / PROJECT_CONFIG_FILENAME)
        backend_name = str(config["backend"])
        get_backend_plugin(backend_name).cleanup_target(
            str(config["target"]),
            backend_options=dict(config["backend_options"]),
        )

        proc = subprocess.run(
            [sys.executable, "-m", "simplebroker.cli", "--cleanup"],
            cwd=workdir,
            capture_output=True,
            text=True,
            env=build_cli_env(),
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        assert proc.returncode == 0
        assert proc.stdout == ""
        assert "Database not found, nothing to clean up" in proc.stderr
        return

    # Cleanup should succeed with appropriate message
    rc, out, err = run_cli("--cleanup", cwd=workdir)
    assert rc == 0
    assert out == ""
    assert "Database not found, nothing to clean up" in err


@pytest.mark.sqlite_only
def test_cleanup_rejects_plain_file(workdir):
    """Cleanup must not delete a non-SimpleBroker file at the target path."""
    db_path = workdir / ".broker.db"
    db_path.write_text("not a sqlite database", encoding="utf-8")

    rc, out, err = run_cli("--cleanup", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert "not a valid SQLite database" in err
    assert db_path.read_text(encoding="utf-8") == "not a sqlite database"


@pytest.mark.sqlite_only
def test_cleanup_rejects_sqlite_db_without_simplebroker_magic(workdir):
    """Cleanup must not delete a SQLite database without SimpleBroker metadata."""
    db_path = workdir / ".broker.db"
    _write_sqlite_meta_db(db_path, magic=None)

    rc, out, err = run_cli("--cleanup", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert "missing SimpleBroker metadata" in err
    assert db_path.exists()


@pytest.mark.sqlite_only
def test_cleanup_rejects_sqlite_db_with_wrong_magic(workdir):
    """Cleanup must not delete a SQLite database that belongs to another app."""
    db_path = workdir / ".broker.db"
    _write_sqlite_meta_db(db_path, magic="not-simplebroker")

    rc, out, err = run_cli("--cleanup", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert "incorrect magic string" in err
    assert db_path.exists()


@pytest.mark.sqlite_only
def test_project_config_sqlite_cleanup_rejects_foreign_db(workdir):
    """Project-config SQLite cleanup uses the same validation-before-delete rule."""
    db_path = workdir / "configured.db"
    _write_sqlite_meta_db(db_path, magic="not-simplebroker")
    (workdir / PROJECT_CONFIG_FILENAME).write_text(
        "\n".join(
            [
                "version = 1",
                'backend = "sqlite"',
                'target = "configured.db"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    rc, out, err = run_cli(
        "--cleanup",
        cwd=workdir,
        env={"BROKER_PROJECT_SCOPE": "1", "BROKER_TEST_BACKEND": "sqlite"},
    )

    assert rc == 1
    assert out == ""
    assert "incorrect magic string" in err
    assert db_path.exists()


def test_cleanup_with_quiet(workdir):
    """Test cleanup with --quiet flag."""
    # Create database
    rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0

    # Cleanup with quiet flag - no output expected
    rc, out, err = run_cli("--quiet", "--cleanup", cwd=workdir)
    assert rc == 0
    assert out == ""
    assert err == ""

    # Verify database was removed
    db_path = workdir / ".broker.db"
    if _uses_sqlite_backend():
        assert wait_for_condition(
            lambda: not db_path.exists(), timeout=1.0, interval=0.05
        )
    else:
        # On PG, --cleanup drops the schema.  Re-init then verify empty.
        rc, _, _ = run_cli("init", cwd=workdir)
        assert rc == 0
        rc, out, err = run_cli("list", cwd=workdir)
        assert rc == 0, err
        assert out == ""


@pytest.mark.sqlite_only
def test_cleanup_with_custom_location(tmp_path):
    """Test cleanup with custom -d and -f options."""
    # Create custom directory
    custom_dir = tmp_path / "custom"
    custom_dir.mkdir()
    custom_file = "mydata.db"

    # Create database in custom location
    rc, _, _ = run_cli(
        "-d",
        str(custom_dir),
        "-f",
        custom_file,
        "write",
        "test",
        "message",
        cwd=tmp_path,  # Still need a cwd for run_cli
    )
    assert rc == 0

    # Verify custom database exists
    custom_db_path = custom_dir / custom_file
    assert custom_db_path.exists()

    # Cleanup with same options
    rc, out, err = run_cli(
        "-d", str(custom_dir), "-f", custom_file, "--cleanup", cwd=tmp_path
    )
    assert rc == 0
    assert wait_for_condition(
        lambda: not custom_db_path.exists(), timeout=1.0, interval=0.05
    )
    assert out == ""
    assert "Database cleaned up" in err
    assert str(custom_db_path) in err


def test_cleanup_exits_before_commands(workdir):
    """Test that cleanup exits without processing commands."""
    # This should cleanup and NOT write a message
    rc, out, err = run_cli("--cleanup", "write", "test", "message", cwd=workdir)

    assert rc == 0
    assert out == ""
    assert "Database cleaned up" in err or "Database not found" in err

    # Verify no database was created (write command was not executed)
    db_path = workdir / ".broker.db"
    if _uses_sqlite_backend():
        assert not db_path.exists()

    # Double-check by trying to read - should get empty queue error
    if _uses_sqlite_backend():
        rc, _, _ = run_cli("read", "test", cwd=workdir)
        assert rc == 2  # EXIT_QUEUE_EMPTY
    else:
        # On PG, the schema was dropped by --cleanup, so read fails with
        # "schema does not exist" (rc=1), not EXIT_QUEUE_EMPTY (rc=2).
        rc, _, _ = run_cli("read", "test", cwd=workdir)
        assert rc in (1, 2)


@pytest.mark.sqlite_only
def test_cleanup_permission_error(workdir, monkeypatch):
    """Test cleanup handles permission errors gracefully."""
    import os

    # Create a database
    rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0

    db_path = workdir / ".broker.db"
    assert db_path.exists()

    # Make database read-only
    os.chmod(db_path, 0o444)

    # Try to cleanup - should get permission error
    rc, _, err = run_cli("--cleanup", cwd=workdir)

    # Check if file still exists before trying to restore permissions
    if db_path.exists():
        # Restore permissions before assertions (cleanup)
        os.chmod(db_path, 0o644)

    # On some systems (especially CI), permission changes might not prevent deletion
    # So we check for either success or permission error
    if rc == 1:
        assert "Permission denied" in err or "error:" in err
    else:
        # If it succeeded despite read-only, that's OK too
        assert rc == 0
        assert not db_path.exists()


def test_cleanup_order_with_other_flags(workdir):
    """Test that cleanup works correctly when mixed with other global flags."""
    # Create database
    rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0

    # Various flag orderings should all work
    flag_combinations = [
        ["--cleanup", "--quiet"],
        ["--quiet", "--cleanup"],
        ["-q", "--cleanup"],
        ["--cleanup", "-q"],
    ]

    db_path = workdir / ".broker.db"
    for flags in flag_combinations:
        # Re-create database if needed
        if _uses_sqlite_backend() and not db_path.exists():
            rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
            assert rc == 0
        elif not _uses_sqlite_backend():
            # On PG, re-init the schema after previous cleanup dropped it
            rc, _, _ = run_cli("init", cwd=workdir)
            assert rc == 0
            rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
            assert rc == 0

        # Run cleanup with flags
        rc, out, err = run_cli(*flags, cwd=workdir)
        assert rc == 0
        assert out == ""  # All combinations include --quiet
        assert err == ""
        if _uses_sqlite_backend():
            assert wait_for_condition(
                lambda: not db_path.exists(), timeout=1.0, interval=0.05
            )
        else:
            # On PG, --cleanup drops the schema.  Re-init then verify empty.
            rc, _, _ = run_cli("init", cwd=workdir)
            assert rc == 0
            rc, out, err = run_cli("list", cwd=workdir)
            assert rc == 0, err
            assert out == ""
