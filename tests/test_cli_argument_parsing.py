"""Test CLI argument parsing edge cases."""

from pathlib import Path

from .conftest import run_cli


def test_equals_form_for_global_options(workdir: Path):
    """Test --dir= and --file= equals form."""
    # Create a subdirectory
    subdir = workdir / "subdir"
    subdir.mkdir()

    # Test --dir= form
    code, stdout, stderr = run_cli(
        f"--dir={subdir}", "write", "test_queue", "message1", cwd=workdir
    )
    assert code == 0

    # Verify the database was created in the subdirectory
    assert (subdir / ".broker.db").exists()

    # Test --file= form
    code, stdout, stderr = run_cli(
        "--file=custom.db", "write", "test_queue", "message2", cwd=workdir
    )
    assert code == 0

    # Verify the custom database was created
    assert (workdir / "custom.db").exists()

    # Test both together
    code, stdout, stderr = run_cli(
        f"--dir={subdir}",
        "--file=another.db",
        "write",
        "test_queue",
        "message3",
        cwd=workdir,
    )
    assert code == 0

    # Verify the database was created with custom name in subdirectory
    assert (subdir / "another.db").exists()


def test_global_option_value_not_mistaken_for_subcommand(workdir: Path):
    """Test that values like 'write' or 'read' aren't mistaken for subcommands."""
    # Create files with subcommand names
    write_file = workdir / "write"
    write_file.touch()
    read_file = workdir / "read.txt"
    read_file.touch()

    # Test -f with a value that matches a subcommand name
    code, stdout, stderr = run_cli(
        "write", "-f", "read.txt", "test_queue", "message1", cwd=workdir
    )
    assert code == 0

    # Should create read.txt as database, not confuse it with read command
    import sqlite3

    conn = sqlite3.connect(read_file)
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    conn.close()
    assert ("messages",) in tables

    # Test with 'write' as filename after the write command
    code, stdout, stderr = run_cli(
        "write", "-f", "write", "test_queue", "message2", cwd=workdir
    )
    assert code == 0

    # Verify it used 'write' as the database file
    conn = sqlite3.connect(write_file)
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    conn.close()
    assert ("messages",) in tables


def test_global_options_after_subcommand(workdir: Path):
    """Test that global options work when placed after subcommand."""
    subdir = workdir / "after_cmd"
    subdir.mkdir()

    # Global options after subcommand should still work
    code, stdout, stderr = run_cli(
        "write", "test_queue", "message1", f"--dir={subdir}", cwd=workdir
    )
    assert code == 0
    assert (subdir / ".broker.db").exists()

    # Mixed order
    code, stdout, stderr = run_cli(
        "write", "-f", "mixed.db", "test_queue", "message2", "-q", cwd=workdir
    )
    assert code == 0
    assert (workdir / "mixed.db").exists()

    # Verify quiet flag worked (no output)
    assert stdout == ""
    assert stderr == ""


def test_complex_argument_combinations(workdir: Path):
    """Test complex combinations of arguments."""
    subdir = workdir / "complex"
    subdir.mkdir()

    # Everything mixed together
    code, stdout, stderr = run_cli(
        "write",
        "test_queue",
        f"--dir={subdir}",
        "-f",
        "complex.db",
        "test message",
        "-q",
        cwd=workdir,
    )
    assert code == 0
    assert (subdir / "complex.db").exists()

    # Read it back with different order
    code, stdout, stderr = run_cli(
        f"--dir={subdir}", "read", "-f", "complex.db", "test_queue", cwd=workdir
    )
    assert code == 0
    assert stdout.strip() == "test message"
