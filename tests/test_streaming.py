"""Test streaming functionality for memory efficiency."""

import json
import os
from pathlib import Path
from typing import Any

import pytest

from .conftest import POSTGRES_TEST_BACKEND, run_cli


def _is_pg_backend() -> bool:
    return os.environ.get("BROKER_TEST_BACKEND") == POSTGRES_TEST_BACKEND


def _bulk_write_pg(runner: Any, queue: str, count: int) -> None:
    """Write messages directly via the PG runner (no subprocess overhead)."""
    from simplebroker._backend_plugins import get_backend_plugin
    from simplebroker.db import BrokerCore

    plugin = get_backend_plugin("postgres")
    core = BrokerCore(runner, backend_plugin=plugin)
    try:
        for i in range(count):
            core.write(queue, f"message_{i}")
    finally:
        core.close()


@pytest.mark.slow
def test_streaming_read_all(workdir: Path, pg_worker_runner):
    """Test that read --all uses streaming and doesn't blow memory."""
    count = 1000
    if _is_pg_backend():
        _bulk_write_pg(pg_worker_runner, "test_queue", count)
    else:
        for i in range(count):
            code, _, stderr = run_cli(
                "write", "test_queue", f"message_{i}", cwd=workdir
            )
            assert code == 0

    code, stdout, stderr = run_cli(
        "read", "test_queue", "--all", cwd=workdir, timeout=120.0
    )
    assert code == 0, stderr

    lines = stdout.strip().split("\n")
    assert len(lines) == count
    assert lines[0] == "message_0"
    assert lines[count - 1] == f"message_{count - 1}"

    code, stdout, stderr = run_cli("list", cwd=workdir)
    assert code == 0
    assert stdout.strip() == ""


@pytest.mark.slow
def test_streaming_peek_all(workdir: Path, pg_worker_runner):
    """Test that peek --all uses streaming and doesn't blow memory."""
    count = 1000
    if _is_pg_backend():
        _bulk_write_pg(pg_worker_runner, "test_queue", count)
    else:
        for i in range(count):
            code, _, stderr = run_cli(
                "write", "test_queue", f"message_{i}", cwd=workdir
            )
            assert code == 0

    code, stdout, stderr = run_cli(
        "peek", "test_queue", "--all", cwd=workdir, timeout=120.0
    )
    assert code == 0, stderr

    lines = stdout.strip().split("\n")
    assert len(lines) == count
    assert lines[0] == "message_0"
    assert lines[count - 1] == f"message_{count - 1}"

    code, stdout, stderr = run_cli("list", cwd=workdir)
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
