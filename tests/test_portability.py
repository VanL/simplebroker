"""Test portability and Windows compatibility fixes."""

import os
import stat
import subprocess
import sys
import unittest.mock
import warnings
from pathlib import Path

import pytest

from simplebroker.db import BrokerDB

pytestmark = [pytest.mark.sqlite_only]


def test_path_resolve_edge_case(tmp_path):
    """Test that Path.resolve() failures are handled gracefully."""
    # Create a path that exists
    test_file = tmp_path / "test.db"

    # Mock Path.resolve to raise OSError
    with (
        unittest.mock.patch.object(
            Path, "resolve", side_effect=OSError("Invalid path")
        ),
        warnings.catch_warnings(record=True) as w,
    ):
        warnings.simplefilter("always")
        # Should fall back to expanduser without crashing
        db = BrokerDB(str(test_file))
        try:
            pass  # Database created successfully despite path resolution failure
        finally:
            db.close()

        # Verify warning was issued
        assert len(w) == 1
        assert issubclass(w[0].category, RuntimeWarning)
        assert "Could not resolve path" in str(w[0].message)


@pytest.mark.skipif(os.name == "nt", reason="POSIX umask contract")
@pytest.mark.parametrize(
    ("umask", "expected_mode"),
    [(0o000, 0o644), (0o002, 0o644), (0o077, 0o600)],
)
def test_database_creation_respects_operator_umask(
    tmp_path: Path, umask: int, expected_mode: int
) -> None:
    """Fresh SQLite mode follows SQLite's request filtered by process umask."""
    db_path = tmp_path / f"new-{umask:o}.db"
    probe = """
import os
import stat
import sys

from simplebroker.db import BrokerDB

os.umask(int(sys.argv[2], 8))
with BrokerDB(sys.argv[1]) as db:
    db.write("jobs", "usable")
with BrokerDB(sys.argv[1]) as db:
    body = db.claim_one("jobs", with_timestamps=False)
print(f"{stat.S_IMODE(os.stat(sys.argv[1]).st_mode):04o} {body}")
"""

    completed = subprocess.run(
        [sys.executable, "-c", probe, str(db_path), f"{umask:o}"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == f"{expected_mode:04o} usable"


@pytest.mark.skipif(os.name == "nt", reason="POSIX mode preservation contract")
def test_reopen_preserves_existing_database_mode(tmp_path: Path) -> None:
    """Reopening preserves an operator-selected mode on an existing database."""
    db_path = tmp_path / "existing.db"

    with BrokerDB(str(db_path)) as db:
        db.write("jobs", "before-reopen")
    db_path.chmod(0o660)

    with BrokerDB(str(db_path)) as reopened:
        assert reopened.claim_one("jobs", with_timestamps=False) == "before-reopen"

    assert stat.S_IMODE(db_path.stat().st_mode) == 0o660


def test_normal_operation_still_works(tmp_path):
    """Test that normal database operations still work after our changes."""
    db_path = tmp_path / "test.db"

    with BrokerDB(str(db_path)) as db:
        # Test basic operations
        db.write("test-queue", "Hello, World!")
        message = db.claim_one("test-queue", with_timestamps=False)
        assert message == "Hello, World!"

        # Test queue listing
        db.write("queue1", "msg1")
        db.write("queue2", "msg2")
        queues = db.list_queues()
        assert len(queues) == 3
        assert "queue1" in queues
        assert "queue2" in queues
        assert "test-queue" in queues
