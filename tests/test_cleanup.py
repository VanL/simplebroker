"""
Tests for the --cleanup functionality.

Test cases:
- Cleanup of existing database
- Cleanup of non-existent database (should succeed)
- Cleanup with custom -d and -f options
- Cleanup with --quiet flag
- Cleanup exits without processing commands
"""

from .conftest import run_cli


def test_cleanup_existing_database(workdir):
    """Test cleaning up an existing database."""
    # Create a database by writing a message
    rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0

    # Verify database exists
    db_path = workdir / ".broker.db"
    assert db_path.exists()

    # Clean it up
    rc, out, _ = run_cli("--cleanup", cwd=workdir)
    assert rc == 0
    assert not db_path.exists()
    assert "Database cleaned up" in out


def test_cleanup_nonexistent_database(workdir):
    """Test cleaning up a non-existent database."""
    # Ensure database doesn't exist
    db_path = workdir / ".broker.db"
    assert not db_path.exists()

    # Cleanup should succeed with appropriate message
    rc, out, _ = run_cli("--cleanup", cwd=workdir)
    assert rc == 0
    assert "Database not found" in out


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
    assert not db_path.exists()


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
    rc, out, _ = run_cli(
        "-d", str(custom_dir), "-f", custom_file, "--cleanup", cwd=tmp_path
    )
    assert rc == 0
    assert not custom_db_path.exists()
    assert "Database cleaned up" in out
    assert str(custom_db_path) in out


def test_cleanup_exits_before_commands(workdir):
    """Test that cleanup exits without processing commands."""
    # This should cleanup and NOT write a message
    rc, out, _ = run_cli("--cleanup", "write", "test", "message", cwd=workdir)

    assert rc == 0
    # Should see cleanup message, not write command output
    assert "Database cleaned up" in out or "Database not found" in out

    # Verify no database was created (write command was not executed)
    db_path = workdir / ".broker.db"
    assert not db_path.exists()

    # Double-check by trying to read - should get no such queue error
    rc, _, _ = run_cli("read", "test", cwd=workdir)
    assert rc == 4  # EXIT_NO_SUCH_QUEUE


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

    for flags in flag_combinations:
        # Re-create database if needed
        db_path = workdir / ".broker.db"
        if not db_path.exists():
            rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
            assert rc == 0

        # Run cleanup with flags
        rc, out, err = run_cli(*flags, cwd=workdir)
        assert rc == 0
        assert out == ""  # All combinations include --quiet
        assert err == ""
        assert not db_path.exists()
