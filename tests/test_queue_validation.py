"""
Tests for queue name validation.

Ensures that invalid queue names are properly rejected.
"""

import os
import tempfile

import pytest

from simplebroker.db import BrokerDB

from .conftest import run_cli


def test_queue_name_starting_with_hyphen(workdir):
    """Test that queue names starting with hyphen are rejected."""
    # Note: Single hyphen "-" is special - it means stdin for the message parameter
    # in write command, so it gets interpreted differently by argparse

    # Name starting with hyphen (looks like a flag to argparse)
    rc, _, err = run_cli("write", "-myqueue", "message", cwd=workdir)
    assert rc == 1
    # This will be caught by argparse as an unknown option
    assert "error" in err.lower()

    # Test with direct DB access to verify validation works
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        with BrokerDB(db_path) as db:
            # Test single hyphen
            with pytest.raises(ValueError, match="Invalid queue name"):
                db.write("-", "message")

            # Test double hyphen
            with pytest.raises(ValueError, match="Invalid queue name"):
                db.write("--", "message")

            # Test name starting with hyphen
            with pytest.raises(ValueError, match="Invalid queue name"):
                db.write("-myqueue", "message")
    finally:
        os.unlink(db_path)


def test_queue_name_starting_with_period(workdir):
    """Test that queue names starting with period are rejected."""
    # Single period
    rc, _, err = run_cli("write", ".", "message", cwd=workdir)
    assert rc == 1
    assert "Invalid queue name" in err

    # Name starting with period
    rc, _, err = run_cli("write", ".myqueue", "message", cwd=workdir)
    assert rc == 1
    assert "Invalid queue name" in err


def test_valid_queue_names_with_special_chars(workdir):
    """Test that valid queue names with allowed special characters work."""
    # Queue with underscore at start
    rc, _, _ = run_cli("write", "_myqueue", "test1", cwd=workdir)
    assert rc == 0
    rc, out, _ = run_cli("read", "_myqueue", cwd=workdir)
    assert rc == 0
    assert out == "test1"

    # Queue with period in middle
    rc, _, _ = run_cli("write", "my.queue", "test2", cwd=workdir)
    assert rc == 0
    rc, out, _ = run_cli("read", "my.queue", cwd=workdir)
    assert rc == 0
    assert out == "test2"

    # Queue with hyphen in middle
    rc, _, _ = run_cli("write", "my-queue", "test3", cwd=workdir)
    assert rc == 0
    rc, out, _ = run_cli("read", "my-queue", cwd=workdir)
    assert rc == 0
    assert out == "test3"

    # Queue with mixed special chars
    rc, _, _ = run_cli("write", "my_queue-2.0", "test4", cwd=workdir)
    assert rc == 0
    rc, out, _ = run_cli("read", "my_queue-2.0", cwd=workdir)
    assert rc == 0
    assert out == "test4"


def test_empty_queue_name(workdir):
    """Test that empty queue name is rejected."""
    rc, _, err = run_cli("write", "", "message", cwd=workdir)
    assert rc == 1
    assert "error" in err.lower()


def test_queue_names_in_other_commands(workdir):
    """Test that queue name validation works in all commands that accept queue names."""
    # Test names that start with period (hyphen-starting names are caught by argparse)
    invalid_names = [".", ".invalid"]

    for name in invalid_names:
        # Test in read command
        rc, _, err = run_cli("read", name, cwd=workdir)
        assert rc == 1, f"read command should reject queue name '{name}'"
        assert "Invalid queue name" in err

        # Test in peek command
        rc, _, err = run_cli("peek", name, cwd=workdir)
        assert rc == 1, f"peek command should reject queue name '{name}'"
        assert "Invalid queue name" in err

        # Test in purge command
        rc, _, err = run_cli("purge", name, cwd=workdir)
        assert rc == 1, f"purge command should reject queue name '{name}'"
        assert "Invalid queue name" in err

    # Test with direct DB access for hyphen-starting names
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        with BrokerDB(db_path) as db:
            # Test that read/peek operations also validate queue names
            with pytest.raises(ValueError, match="Invalid queue name"):
                list(db.stream_read_with_timestamps("-"))

            with pytest.raises(ValueError, match="Invalid queue name"):
                list(db.stream_read_with_timestamps("--"))

            with pytest.raises(ValueError, match="Invalid queue name"):
                list(db.stream_read_with_timestamps("-invalid"))

            with pytest.raises(ValueError, match="Invalid queue name"):
                db.purge("-")
    finally:
        os.unlink(db_path)


def test_special_characters_not_allowed(workdir):
    """Test that other special characters are rejected."""
    invalid_names = [
        "queue@name",  # @ not allowed
        "queue#name",  # # not allowed
        "queue name",  # space not allowed
        "queue/name",  # / not allowed
        "queue\\name",  # \ not allowed
        "queue:name",  # : not allowed
        "queue;name",  # ; not allowed
        "queue|name",  # | not allowed
        "queue&name",  # & not allowed
        "queue*name",  # * not allowed
        "queue?name",  # ? not allowed
        "queue[name]",  # [] not allowed
        "queue{name}",  # {} not allowed
    ]

    for name in invalid_names:
        rc, _, err = run_cli("write", name, "message", cwd=workdir)
        assert rc == 1, f"Should reject queue name '{name}'"
        assert "Invalid queue name" in err


def test_queue_name_length_limit(workdir):
    """Test that queue names exceeding length limit are rejected."""
    # Create a name that's exactly at the limit (512 chars)
    max_length_name = "a" * 512
    rc, _, _ = run_cli("write", max_length_name, "test", cwd=workdir)
    assert rc == 0

    # Create a name that exceeds the limit
    too_long_name = "a" * 513
    rc, _, err = run_cli("write", too_long_name, "test", cwd=workdir)
    assert rc == 1
    assert "Invalid queue name" in err
    assert "exceeds" in err
