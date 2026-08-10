"""Tests for internal SQL building functions and database internals."""

import tempfile
from pathlib import Path

import pytest

from simplebroker._sql import RetrieveQuerySpec, build_retrieve_query
from simplebroker.db import BrokerDB


class TestSQLBuilders:
    """Test SQL query building functions."""

    def test_build_retrieve_query_invalid_operation(self):
        """Test that invalid operations raise errors."""
        with pytest.raises(ValueError, match="Invalid operation"):
            build_retrieve_query(
                "invalid",  # type: ignore[arg-type]
                RetrieveQuerySpec(
                    queue="jobs",
                    limit=1,
                    offset=0,
                    exact_timestamp=None,
                    after_timestamp=None,
                    require_unclaimed=True,
                    target_queue=None,
                ),
            )

    def test_build_retrieve_query_move_requires_a_target_queue(self) -> None:
        """A move must not produce SQL that can bind a missing destination."""

        with pytest.raises(ValueError, match="requires target_queue"):
            build_retrieve_query(
                "move",
                RetrieveQuerySpec(
                    queue="jobs",
                    limit=1,
                    offset=0,
                    exact_timestamp=None,
                    after_timestamp=None,
                    require_unclaimed=True,
                    target_queue=None,
                ),
            )


class TestRetrieveMethod:
    """Test the internal _retrieve method."""

    def test_retrieve_peek_operation(self):
        """Test _retrieve with peek operation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with BrokerDB(str(db_path)) as db:
                # Add test messages
                db.write("test_queue", "message1")
                db.write("test_queue", "message2")
                db.write("test_queue", "message3")

                # Peek messages
                results = db._retrieve("test_queue", operation="peek", limit=2)

                assert len(results) == 2
                assert results[0][0] == "message1"
                assert results[1][0] == "message2"

                # Messages should still be there
                results2 = db._retrieve("test_queue", operation="peek", limit=10)
                assert len(results2) == 3

    def test_retrieve_claim_operation(self):
        """Test _retrieve with claim operation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with BrokerDB(str(db_path)) as db:
                # Add test messages
                db.write("test_queue", "message1")
                db.write("test_queue", "message2")
                db.write("test_queue", "message3")

                # Claim messages with exactly-once
                results = db._retrieve(
                    "test_queue", operation="claim", limit=2, commit_before_yield=True
                )

                assert len(results) == 2
                assert results[0][0] == "message1"
                assert results[1][0] == "message2"

                # Messages should be gone
                results2 = db._retrieve("test_queue", operation="peek", limit=10)
                assert len(results2) == 1
                assert results2[0][0] == "message3"

    def test_retrieve_move_operation(self):
        """Test _retrieve with move operation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with BrokerDB(str(db_path)) as db:
                # Add test messages
                db.write("source", "message1")
                db.write("source", "message2")
                db.write("source", "message3")

                # Move messages
                results = db._retrieve(
                    "source",
                    operation="move",
                    target_queue="dest",
                    limit=2,
                    commit_before_yield=True,
                )

                assert len(results) == 2
                assert results[0][0] == "message1"
                assert results[1][0] == "message2"

                # Check source has 1 left
                source_results = db._retrieve("source", operation="peek", limit=10)
                assert len(source_results) == 1
                assert source_results[0][0] == "message3"

                # Check dest has 2
                dest_results = db._retrieve("dest", operation="peek", limit=10)
                assert len(dest_results) == 2
                assert dest_results[0][0] == "message1"
                assert dest_results[1][0] == "message2"

    def test_retrieve_with_exact_timestamp(self):
        """Test _retrieve with exact_timestamp filter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with BrokerDB(str(db_path)) as db:
                # Add test messages
                db.write("test_queue", "message1")
                db.write("test_queue", "message2")
                db.write("test_queue", "message3")

                # Get timestamp of second message
                all_messages = db._retrieve("test_queue", operation="peek", limit=10)
                target_ts = all_messages[1][1]

                # Retrieve specific message
                results = db._retrieve(
                    "test_queue", operation="peek", exact_timestamp=target_ts, limit=1
                )

                assert len(results) == 1
                assert results[0][0] == "message2"
                assert results[0][1] == target_ts

    def test_retrieve_with_after_timestamp(self):
        """Test _retrieve with after_timestamp filter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with BrokerDB(str(db_path)) as db:
                # Add test messages
                db.write("test_queue", "old1")
                db.write("test_queue", "old2")

                # Get timestamp of last old message
                old_messages = db._retrieve("test_queue", operation="peek", limit=10)
                cutoff_ts = old_messages[-1][1]

                # Add new messages
                db.write("test_queue", "new1")
                db.write("test_queue", "new2")

                # Retrieve only new messages
                results = db._retrieve(
                    "test_queue", operation="peek", after_timestamp=cutoff_ts, limit=10
                )

                assert len(results) == 2
                assert results[0][0] == "new1"
                assert results[1][0] == "new2"

    def test_retrieve_invalid_parameters(self):
        """Test _retrieve with invalid parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with BrokerDB(str(db_path)) as db:
                # Invalid queue name
                with pytest.raises(ValueError, match="Invalid queue name"):
                    db._retrieve(".invalid", operation="peek", limit=1)

                # Move without target_queue
                with pytest.raises(ValueError, match="target_queue is required"):
                    db._retrieve("source", operation="move", limit=1)

                # Invalid target queue name for move
                with pytest.raises(ValueError, match="Invalid queue name"):
                    db._retrieve(
                        "source", operation="move", target_queue="-invalid", limit=1
                    )
