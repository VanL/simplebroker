"""Opportunistic pg<->redis dump/load pipe tests.

Neither standard harness brings up BOTH non-SQLite backends, so these
directions cannot run in routine CI — they are covered transitively
(pg->sqlite->redis) by the extension pipe suites. This module makes them
DIRECTLY runnable when both backends are available, and skips cleanly
otherwise.

Manual run (both Dockers up):
  SIMPLEBROKER_PG_TEST_DSN=... SIMPLEBROKER_VALKEY_TEST_URL=... \
    uv run pytest tests/test_cross_backend_dump_load.py -v

This module deliberately uses a local subprocess helper rather than the
conftest harness: the conftest helper auto-writes backend project configs
whenever the ambient BROKER_TEST_BACKEND is set, which would fight the
explicit per-side env below. (Without the literal conftest-helper token the
module auto-marks sqlite_only and is deselected under the single-backend
suite runs, where it could only skip anyway.)
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

PG_DSN = os.environ.get("SIMPLEBROKER_PG_TEST_DSN")
REDIS_URL = os.environ.get("SIMPLEBROKER_VALKEY_TEST_URL") or os.environ.get(
    "SIMPLEBROKER_REDIS_TEST_URL"
)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

pytestmark = [
    pytest.mark.skipif(
        not (PG_DSN and REDIS_URL),
        reason="pg<->redis pipes need BOTH backends: set SIMPLEBROKER_PG_TEST_DSN "
        "and SIMPLEBROKER_VALKEY_TEST_URL",
    ),
]


def _pipe_cli(
    *args: str,
    cwd: Path,
    env: dict[str, str],
    stdin: str | None = None,
) -> tuple[int, str, str]:
    full_env = os.environ.copy()
    full_env.pop("BROKER_BACKEND", None)
    full_env.update(env)
    paths = [str(PROJECT_ROOT)]
    if full_env.get("PYTHONPATH"):
        paths.append(full_env["PYTHONPATH"])
    full_env["PYTHONPATH"] = os.pathsep.join(paths)
    completed = subprocess.run(
        [sys.executable, "-m", "simplebroker.cli", *args],
        cwd=cwd,
        env=full_env,
        input=stdin,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return completed.returncode, completed.stdout, completed.stderr


def _pg_env(schema: str) -> dict[str, str]:
    assert PG_DSN is not None
    return {
        "BROKER_BACKEND": "postgres",
        "BROKER_BACKEND_TARGET": PG_DSN,
        "BROKER_BACKEND_SCHEMA": schema,
    }


def _redis_env(namespace: str) -> dict[str, str]:
    assert REDIS_URL is not None
    return {
        "BROKER_BACKEND": "redis",
        "BROKER_BACKEND_TARGET": REDIS_URL,
        "BROKER_BACKEND_SCHEMA": namespace,
    }


def _seed(cwd: Path, env: dict[str, str]) -> None:
    assert _pipe_cli("write", "alpha", "a0", cwd=cwd, env=env)[0] == 0
    assert _pipe_cli("write", "alpha", "a1", cwd=cwd, env=env)[0] == 0
    assert _pipe_cli("write", "beta", "b0", cwd=cwd, env=env)[0] == 0
    assert _pipe_cli("alias", "add", "al", "alpha", cwd=cwd, env=env)[0] == 0


def _nonheader(dump_output: str) -> list[str]:
    lines = [ln for ln in dump_output.splitlines() if ln.strip()]
    assert json.loads(lines[0])["type"] == "header"
    return lines[1:]


def _pipe_round_trip(
    tmp_path: Path,
    src_env: dict[str, str],
    dst_env: dict[str, str],
    src_backend: str,
    dst_backend: str,
) -> None:
    src = tmp_path / "src"
    src.mkdir()
    _seed(src, src_env)

    code, dump_out, err = _pipe_cli("dump", cwd=src, env=src_env)
    assert code == 0, err
    assert json.loads(dump_out.splitlines()[0])["backend"] == src_backend

    dst = tmp_path / "dst"
    dst.mkdir()
    code, _out, err = _pipe_cli("load", cwd=dst, env=dst_env, stdin=dump_out)
    assert code == 0, err

    code, redump, err = _pipe_cli("dump", cwd=dst, env=dst_env)
    assert code == 0, err
    assert json.loads(redump.splitlines()[0])["backend"] == dst_backend
    assert _nonheader(redump) == _nonheader(dump_out)

    code, out, _err = _pipe_cli("read", "alpha", cwd=dst, env=dst_env)
    assert (code, out.strip()) == (0, "a0")


def test_postgres_to_redis_pipe(tmp_path: Path) -> None:
    _pipe_round_trip(
        tmp_path,
        _pg_env(f"dumppipe_{uuid.uuid4().hex[:12]}"),
        _redis_env(f"dumppipe_{uuid.uuid4().hex[:12]}"),
        "postgres",
        "redis",
    )


def test_redis_to_postgres_pipe(tmp_path: Path) -> None:
    _pipe_round_trip(
        tmp_path,
        _redis_env(f"dumppipe_{uuid.uuid4().hex[:12]}"),
        _pg_env(f"dumppipe_{uuid.uuid4().hex[:12]}"),
        "redis",
        "postgres",
    )
