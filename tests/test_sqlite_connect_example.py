"""Safety tests for the copyable SQLite connection example."""

from __future__ import annotations

import contextlib
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from examples.sqlite_connect import validate_database_path
from simplebroker import Queue

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
    "queue = Queue('q', db_path=sys.argv[1])\n"
    "queue.write(sys.argv[2])\n"
    "queue.close()\n"
)


def _write_from_other_process(db_path: str, body: str) -> None:
    subprocess.run(
        [sys.executable, "-c", _WRITER_SNIPPET, db_path, body],
        check=True,
        capture_output=True,
        timeout=30,
    )


def _wal_generation(db_path: str) -> int | None:
    try:
        return os.stat(db_path + "-shm").st_ino
    except FileNotFoundError:
        return None


def test_example_validation_preserves_live_wal_locks(tmp_path: Path) -> None:
    """The copyable validator must leave SQLite's own file locks intact."""

    db_path = str(tmp_path / "guarded.db")
    seed = Queue("q", db_path=db_path)
    seed.write("seed")
    seed.close()

    holder = Queue("q", db_path=db_path, persistent=True)
    try:
        assert holder.read_one() == "seed"
        generation = _wal_generation(db_path)
        assert generation is not None

        _write_from_other_process(db_path, "one")
        assert _wal_generation(db_path) == generation
        assert holder.read_one() == "one"

        validate_database_path(Path(db_path))

        _write_from_other_process(db_path, "two")
        assert _wal_generation(db_path) == generation
        assert holder.read_one() == "two"
    finally:
        holder.close()


def test_example_validation_handles_uri_special_characters_in_path(
    tmp_path: Path,
) -> None:
    """Validator URI construction must escape '#', '?', '%', and spaces."""

    special_dir = tmp_path / "dir #1?mode=rw%20"
    special_dir.mkdir(parents=True)
    db_file = special_dir / "guarded #2?x=1%20.db"
    with contextlib.closing(sqlite3.connect(db_file)) as seed_conn:
        seed_conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        seed_conn.execute(
            "INSERT INTO meta (key, value) VALUES ('magic', 'simplebroker-v1')"
        )
        seed_conn.commit()

    validate_database_path(
        db_file,
        check_magic=True,
        magic_string="simplebroker-v1",
    )
    assert [entry.name for entry in tmp_path.iterdir()] == [special_dir.name]
