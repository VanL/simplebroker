"""Test absolute path handling in -f flag."""

from pathlib import Path

from .conftest import run_cli


def test_absolute_path_in_f_flag(workdir: Path):
    """Test that -f accepts absolute paths and extracts dir/file correctly."""
    # Create a subdirectory
    subdir = workdir / "data"
    subdir.mkdir()

    # Use absolute path for database
    db_path = subdir / "mydb.db"
    abs_path = str(db_path.resolve())

    # Write using absolute path
    code, stdout, stderr = run_cli(
        "-f", abs_path, "write", "test", "message1", cwd=workdir
    )
    assert code == 0, f"Write failed: {stderr}"

    # Read using absolute path
    code, stdout, stderr = run_cli("-f", abs_path, "read", "test", cwd=workdir)
    assert code == 0
    assert stdout.strip() == "message1"

    # Verify database was created in the right place
    assert db_path.exists()


def test_absolute_path_with_explicit_dir(workdir: Path):
    """Test that -f with absolute path and -d with consistent dir works."""
    # Create a subdirectory
    subdir = workdir / "data"
    subdir.mkdir()

    # Use absolute path for database
    db_path = subdir / "mydb.db"
    abs_path = str(db_path.resolve())

    # Write using absolute path with explicit matching -d
    code, stdout, stderr = run_cli(
        "-d", str(subdir), "-f", abs_path, "write", "test", "message1", cwd=workdir
    )
    assert code == 0, f"Write failed: {stderr}"

    # Verify database was created
    assert db_path.exists()


def test_absolute_path_with_inconsistent_dir(workdir: Path):
    """Test that -f with absolute path and -d with different dir fails."""
    # Create two subdirectories
    subdir1 = workdir / "data1"
    subdir2 = workdir / "data2"
    subdir1.mkdir()
    subdir2.mkdir()

    # Use absolute path pointing to subdir1
    db_path = subdir1 / "mydb.db"
    abs_path = str(db_path.resolve())

    # Try to use with -d pointing to different directory
    code, stdout, stderr = run_cli(
        "-d", str(subdir2), "-f", abs_path, "write", "test", "message1", cwd=workdir
    )
    assert code == 1
    assert "Inconsistent paths" in stderr
    assert "conflicts with directory" in stderr


def test_cleanup_with_absolute_path(workdir: Path):
    """Test that --cleanup works with absolute paths."""
    # Create a subdirectory
    subdir = workdir / "data"
    subdir.mkdir()

    # Create database with absolute path
    db_path = subdir / "cleanup.db"
    abs_path = str(db_path.resolve())

    code, stdout, stderr = run_cli(
        "-f", abs_path, "write", "test", "message", cwd=workdir
    )
    assert code == 0
    assert db_path.exists()

    # Cleanup using absolute path
    code, stdout, stderr = run_cli("-f", abs_path, "--cleanup", cwd=workdir)
    assert code == 0
    assert not db_path.exists()
    assert "Database cleaned up" in stdout


def test_relative_path_still_works(workdir: Path):
    """Test that relative paths still work as before."""
    # Traditional usage with relative path
    code, stdout, stderr = run_cli(
        "-f", "relative.db", "write", "test", "message", cwd=workdir
    )
    assert code == 0

    # Verify database in current directory
    db_path = workdir / "relative.db"
    assert db_path.exists()

    # Read back
    code, stdout, stderr = run_cli("-f", "relative.db", "read", "test", cwd=workdir)
    assert code == 0
    assert stdout.strip() == "message"
