"""Test portability and Windows compatibility fixes."""

import unittest.mock
import warnings
from pathlib import Path

from simplebroker.db import BrokerDB


def test_chmod_windows_compatibility(tmp_path):
    """Test that chmod failures are handled gracefully."""
    db_path = tmp_path / "test.db"

    # Mock os.chmod to raise OSError (simulating Windows permission issue)
    with unittest.mock.patch("os.chmod", side_effect=OSError("Permission denied")):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # Should not crash, just warn
            db = BrokerDB(str(db_path))
            db.close()

            # Verify warning was issued
            assert len(w) == 1
            assert issubclass(w[0].category, RuntimeWarning)
            assert "Could not set file permissions" in str(w[0].message)
            assert str(db_path) in str(w[0].message)


def test_path_resolve_edge_case(tmp_path):
    """Test that Path.resolve() failures are handled gracefully."""
    # Create a path that exists
    test_file = tmp_path / "test.db"

    # Mock Path.resolve to raise OSError
    with unittest.mock.patch.object(
        Path, "resolve", side_effect=OSError("Invalid path")
    ):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # Should fall back to expanduser without crashing
            db = BrokerDB(str(test_file))
            db.close()

            # Verify warning was issued
            assert len(w) == 1
            assert issubclass(w[0].category, RuntimeWarning)
            assert "Could not resolve path" in str(w[0].message)


def test_chmod_called_on_new_database(tmp_path):
    """Test that chmod is called for new databases on all platforms."""
    db_path = tmp_path / "new.db"

    with unittest.mock.patch("os.chmod") as mock_chmod:
        db = BrokerDB(str(db_path))
        db.close()

        # Verify chmod was called with correct permissions
        # Now also called for marker files, so check database file specifically
        chmod_calls = [
            call for call in mock_chmod.call_args_list if call[0][0] == db_path
        ]
        assert len(chmod_calls) == 1
        assert chmod_calls[0][0] == (db_path, 0o600)


def test_chmod_not_called_on_existing_database(tmp_path):
    """Test that chmod is not called for existing database file itself."""
    db_path = tmp_path / "existing.db"

    # Create the database first
    db = BrokerDB(str(db_path))
    db.close()

    # Now open it again
    with unittest.mock.patch("os.chmod") as mock_chmod:
        db = BrokerDB(str(db_path))
        db.close()

        # Verify chmod was NOT called on the database file itself
        # (it may be called on lock files, which is expected)
        db_chmod_calls = [
            call for call in mock_chmod.call_args_list if call[0][0] == str(db_path)
        ]
        assert len(db_chmod_calls) == 0, (
            f"chmod should not be called on existing database file, but was called with {db_chmod_calls}"
        )


def test_normal_operation_still_works(tmp_path):
    """Test that normal database operations still work after our changes."""
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path)) as db:
        # Test basic operations
        db.write("test-queue", "Hello, World!")
        messages = db.read("test-queue")
        assert messages == ["Hello, World!"]

        # Test queue listing
        db.write("queue1", "msg1")
        db.write("queue2", "msg2")
        queues = db.list_queues()
        assert len(queues) == 2
        assert ("queue1", 1) in queues
        assert ("queue2", 1) in queues
