"""``validate_database()`` must not break the process's SQLite locks.

Root cause of a downstream WAL marooning incident (taut-summon, 2026-07-10):
the 16-byte header check raw-opened the database file with ``open()``. Per
POSIX advisory-lock semantics, closing ANY file descriptor to a file drops
ALL of the process's fcntl locks on that inode — including a live WAL
connection's long-lived shared database lock. SQLite caches its lock state
and never re-acquires, so the next external "last closer" checkpoint-deleted
the wal/shm generation beneath the still-attached holder. The holder then
served a permanently frozen view (frozen ``PRAGMA data_version``, blind
reads), and its own eventual graceful close deleted peers' committed-but-
uncheckpointed rows. SQLite documents this hazard class in
https://www.sqlite.org/howtocorrupt.html (POSIX advisory locking).

The rule this test pins: validation may stat, connect read-only via SQLite,
and query — but it must never ``open()`` the database file directly while
any connection in the process may hold locks on it.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from simplebroker import Queue
from simplebroker._backends.sqlite.validation import validate_database

pytestmark = [
    pytest.mark.sqlite_only,
    pytest.mark.skipif(
        sys.platform == "win32",
        reason="POSIX advisory-lock semantics",
    ),
]

_WRITER_SNIPPET = (
    "import sys\n"
    "from simplebroker import Queue\n"
    "q = Queue('q', db_path=sys.argv[1])\n"
    "q.write(sys.argv[2])\n"
    "q.close()\n"
)


def _write_from_other_process(db_path: str, body: str) -> None:
    subprocess.run(
        [sys.executable, "-c", _WRITER_SNIPPET, db_path, body],
        check=True,
        capture_output=True,
        timeout=30,
    )


def _generation(db_path: str) -> int | None:
    """Inode of the -shm sidecar, or None when it has been deleted."""
    try:
        return os.stat(db_path + "-shm").st_ino
    except FileNotFoundError:
        return None


def test_validate_database_preserves_live_wal_locks(workdir: Path) -> None:
    db_path = str(workdir / "guarded.db")
    seed = Queue("q", db_path=db_path)
    seed.write("seed")
    seed.close()

    holder = Queue("q", db_path=db_path, persistent=True)
    try:
        assert holder.read_one() == "seed"  # attach this thread's core
        generation = _generation(db_path)
        assert generation is not None

        # Control: a held generation survives an external writer's close.
        _write_from_other_process(db_path, "one")
        assert _generation(db_path) == generation
        assert holder.read_one() == "one"

        # The operation under test. It must not strip this process's locks.
        validate_database(Path(db_path))

        # If validation stripped the WAL shared lock, this external close
        # becomes a "last closer", deletes the generation beneath the
        # holder, and the holder goes permanently blind.
        _write_from_other_process(db_path, "two")
        assert _generation(db_path) == generation, (
            "sidecar generation was deleted beneath a live holder: "
            "validation broke the process's WAL locks"
        )
        assert holder.read_one() == "two"
    finally:
        holder.close()
