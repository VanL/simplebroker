"""Test streaming functionality for memory efficiency."""

import json
from pathlib import Path

from simplebroker import Queue, target_for_directory

from .conftest import run_cli


def _bulk_write(workdir: Path, queue_name: str, count: int) -> None:
    """Seed messages through the Python API, backend-agnostically.

    ``target_for_directory(workdir)`` resolves the same broker the CLI
    subprocesses resolve in *workdir* (the SQLite default db, or the pg/redis
    project config the worker fixtures wrote there), so seeding takes one
    connection instead of one subprocess per message. This is what lets these
    tests run in the regular suite instead of being marked slow.
    """
    target = target_for_directory(workdir)
    q = Queue(queue_name, db_path=target)
    try:
        for i in range(count):
            q.write(f"message_{i}")
    finally:
        q.close()


def test_streaming_read_all(workdir: Path):
    """Test that read --all uses streaming and doesn't blow memory."""
    count = 1000
    _bulk_write(workdir, "test_queue", count)

    code, stdout, stderr = run_cli(
        "read", "test_queue", "--all", cwd=workdir, timeout=120.0
    )
    assert code == 0, stderr

    lines = stdout.strip().split("\n")
    assert len(lines) == count
    assert lines[0] == "message_0"
    assert lines[count - 1] == f"message_{count - 1}"

    code, stdout, stderr = run_cli("list", "--stats", cwd=workdir)
    assert code == 0
    assert stdout.splitlines()[0] == (f"test_queue: 0 ({count} total, {count} claimed)")


def test_streaming_peek_all(workdir: Path):
    """Test that peek --all uses streaming and doesn't blow memory."""
    count = 1000
    _bulk_write(workdir, "test_queue", count)

    code, stdout, stderr = run_cli(
        "peek", "test_queue", "--all", cwd=workdir, timeout=120.0
    )
    assert code == 0, stderr

    lines = stdout.strip().split("\n")
    assert len(lines) == count
    assert lines[0] == "message_0"
    assert lines[count - 1] == f"message_{count - 1}"

    code, stdout, stderr = run_cli("list", "--stats", cwd=workdir)
    assert code == 0
    assert f"test_queue: {count}" in stdout


def test_json_output_now_ndjson(workdir: Path):
    """Test that JSON output now uses line-delimited JSON (ndjson) format."""
    for i in range(5):
        code, _, stderr = run_cli("write", "test_queue", f"message_{i}", cwd=workdir)
        assert code == 0

    code, stdout, stderr = run_cli("read", "test_queue", "--all", "--json", cwd=workdir)
    assert code == 0

    lines = stdout.strip().split("\n")
    assert len(lines) == 5

    for i, line in enumerate(lines):
        data = json.loads(line)
        assert "message" in data
        assert data["message"] == f"message_{i}"
