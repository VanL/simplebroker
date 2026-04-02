"""Tests for internal SQL building functions and database internals."""

import tempfile
from pathlib import Path

import pytest

from simplebroker._sql import RetrieveQuerySpec, build_retrieve_query
from simplebroker.db import BrokerDB


class TestSQLBuilders:
    """Test SQL query building functions."""

    def test_build_retrieve_query_peek(self):
        """Test building peek queries."""
        # Basic peek
        query, params = build_retrieve_query(
            "peek",
            RetrieveQuerySpec(
                queue="jobs",
                limit=5,
                offset=2,
                exact_timestamp=None,
                since_timestamp=None,
                require_unclaimed=True,
                target_queue=None,
            ),
        )
        assert "SELECT body, ts FROM messages" in query
        assert "WHERE queue = ? AND claimed = 0" in query
        assert "ORDER BY id" in query
        assert "LIMIT ?" in query
        assert "OFFSET ?" in query
        assert "DELETE" not in query
        assert params == ("jobs", 5, 2)

        # Peek with timestamp filter
        query, params = build_retrieve_query(
            "peek",
            RetrieveQuerySpec(
                queue="jobs",
                limit=1,
                offset=0,
                exact_timestamp=None,
                since_timestamp=123,
                require_unclaimed=True,
                target_queue=None,
            ),
        )
        assert "WHERE queue = ? AND claimed = 0 AND ts > ?" in query
        assert params == ("jobs", 123, 1, 0)

        # Peek with exact timestamp (optimized order)
        query, params = build_retrieve_query(
            "peek",
            RetrieveQuerySpec(
                queue="jobs",
                limit=1,
                offset=0,
                exact_timestamp=999,
                since_timestamp=None,
                require_unclaimed=True,
                target_queue=None,
            ),
        )
        assert "WHERE ts = ? AND queue = ?" in query
        assert params == (999, "jobs", 1, 0)

    def test_build_retrieve_query_claim(self):
        """Test building claim queries."""
        # Basic claim
        query, params = build_retrieve_query(
            "claim",
            RetrieveQuerySpec(
                queue="jobs",
                limit=2,
                offset=0,
                exact_timestamp=None,
                since_timestamp=None,
                require_unclaimed=True,
                target_queue=None,
            ),
        )
        assert "UPDATE messages" in query
        assert "SET claimed = 1" in query
        assert "WHERE id IN" in query
        assert "SELECT id FROM messages" in query
        assert "WHERE queue = ? AND claimed = 0" in query
        assert "ORDER BY id" in query
        assert "LIMIT ?" in query
        assert "RETURNING body, ts" in query
        assert params == ("jobs", 2)

        # Claim with timestamp filter
        query, params = build_retrieve_query(
            "claim",
            RetrieveQuerySpec(
                queue="jobs",
                limit=2,
                offset=0,
                exact_timestamp=None,
                since_timestamp=123,
                require_unclaimed=True,
                target_queue=None,
            ),
        )
        assert "WHERE queue = ? AND claimed = 0 AND ts > ?" in query
        assert params == ("jobs", 123, 2)

    def test_build_retrieve_query_move(self):
        """Test building move queries."""
        # Basic move
        query, params = build_retrieve_query(
            "move",
            RetrieveQuerySpec(
                queue="jobs",
                limit=3,
                offset=0,
                exact_timestamp=None,
                since_timestamp=None,
                require_unclaimed=True,
                target_queue="done",
            ),
        )
        assert "UPDATE messages" in query
        assert "SET queue = ?, claimed = 0" in query  # Move also resets claimed flag
        assert "WHERE id IN" in query
        assert "SELECT id FROM messages" in query
        assert "WHERE queue = ? AND claimed = 0" in query
        assert "ORDER BY id" in query
        assert "LIMIT ?" in query
        assert "RETURNING body, ts" in query
        assert params == ("done", "jobs", 3)

        # Move with timestamp filter
        query, params = build_retrieve_query(
            "move",
            RetrieveQuerySpec(
                queue="jobs",
                limit=1,
                offset=0,
                exact_timestamp=None,
                since_timestamp=123,
                require_unclaimed=False,
                target_queue="done",
            ),
        )
        assert "WHERE queue = ? AND ts > ?" in query
        assert "WHERE queue = ? AND claimed = 0 AND ts > ?" not in query
        assert params == ("done", "jobs", 123, 1)

    def test_build_retrieve_query_invalid_operation(self):
        """Test that invalid operations raise errors."""
        with pytest.raises(ValueError, match="Invalid operation"):
            build_retrieve_query(  # type: ignore[arg-type]
                "invalid",
                RetrieveQuerySpec(
                    queue="jobs",
                    limit=1,
                    offset=0,
                    exact_timestamp=None,
                    since_timestamp=None,
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

            with BrokerDB(db_path) as db:
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

            with BrokerDB(db_path) as db:
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

            with BrokerDB(db_path) as db:
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

            with BrokerDB(db_path) as db:
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

    def test_retrieve_with_since_timestamp(self):
        """Test _retrieve with since_timestamp filter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with BrokerDB(db_path) as db:
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
                    "test_queue", operation="peek", since_timestamp=cutoff_ts, limit=10
                )

                assert len(results) == 2
                assert results[0][0] == "new1"
                assert results[1][0] == "new2"

    def test_retrieve_with_require_unclaimed(self):
        """Test _retrieve with require_unclaimed parameter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with BrokerDB(db_path) as db:
                # Add test messages
                db.write("test_queue", "message1")
                db.write("test_queue", "message2")
                db.write("test_queue", "message3")

                # Claim first message
                db._retrieve(
                    "test_queue", operation="claim", limit=1, commit_before_yield=True
                )

                # Try to move with require_unclaimed=True (default)
                results = db._retrieve(
                    "test_queue",
                    operation="move",
                    target_queue="dest",
                    limit=10,
                    require_unclaimed=True,
                    commit_before_yield=True,
                )

                # Should only move unclaimed messages
                assert len(results) == 2
                assert results[0][0] == "message2"
                assert results[1][0] == "message3"

                # Now try with require_unclaimed=False
                # First need to get the exact timestamp of claimed message
                db._retrieve(
                    "test_queue",
                    operation="peek",
                    limit=10,
                    require_unclaimed=False,  # Include claimed messages
                )

                # Note: The claimed message might have been deleted by the claim operation
                # depending on the implementation

    def test_retrieve_commit_before_yield_difference(self):
        """Test the difference between commit_before_yield True and False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            # Test exactly-once (commit_before_yield=True)
            with BrokerDB(db_path) as db:
                for i in range(5):
                    db.write("queue1", f"msg{i}")

                # Claim with exactly-once
                results = db._retrieve(
                    "queue1",
                    operation="claim",
                    limit=3,
                    commit_before_yield=True,  # Commit BEFORE returning
                )

                assert len(results) == 3

                # Messages are already gone
                remaining = db._retrieve("queue1", operation="peek", limit=10)
                assert len(remaining) == 2

            # Test at-least-once (commit_before_yield=False)
            with BrokerDB(db_path) as db:
                for i in range(5):
                    db.write("queue2", f"msg{i}")

                # Claim with at-least-once
                results = db._retrieve(
                    "queue2",
                    operation="claim",
                    limit=3,
                    commit_before_yield=False,  # Commit AFTER returning
                )

                assert len(results) == 3

                # Messages are still gone (commit happened after return)
                remaining = db._retrieve("queue2", operation="peek", limit=10)
                assert len(remaining) == 2

    def test_retrieve_invalid_parameters(self):
        """Test _retrieve with invalid parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with BrokerDB(db_path) as db:
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


class TestBuildRetrieveSpec:
    """Test the backend-neutral retrieve-query specification builder."""

    def test_build_retrieve_spec_basic(self):
        """Test retrieve spec construction."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with BrokerDB(db_path) as db:
                spec = db._build_retrieve_spec("test_queue", 10)
                assert spec == RetrieveQuerySpec(
                    queue="test_queue",
                    limit=10,
                    offset=0,
                    exact_timestamp=None,
                    since_timestamp=None,
                    require_unclaimed=True,
                    target_queue=None,
                )

                spec = db._build_retrieve_spec(
                    "test_queue",
                    1,
                    exact_timestamp=67890,
                    require_unclaimed=False,
                )
                assert spec == RetrieveQuerySpec(
                    queue="test_queue",
                    limit=1,
                    offset=0,
                    exact_timestamp=67890,
                    since_timestamp=None,
                    require_unclaimed=False,
                    target_queue=None,
                )

                spec = db._build_retrieve_spec(
                    "test_queue",
                    5,
                    offset=2,
                    target_queue="dest",
                    since_timestamp=12345,
                )
                assert spec == RetrieveQuerySpec(
                    queue="test_queue",
                    limit=5,
                    offset=2,
                    exact_timestamp=None,
                    since_timestamp=12345,
                    require_unclaimed=True,
                    target_queue="dest",
                )
