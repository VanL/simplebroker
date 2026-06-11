"""Cross-backend dump/load pipes: SQLite <-> Redis via the env interface.

These tests run under ``bin/pytest-redis`` (which exports
``SIMPLEBROKER_VALKEY_TEST_URL``) and exercise the real user-facing
mechanism -- the load/dump side selects Redis via ``BROKER_BACKEND=redis``
+ ``BROKER_BACKEND_TARGET`` + ``BROKER_BACKEND_SCHEMA`` env vars (the
Redis plugin maps ``BROKER_BACKEND_SCHEMA`` to its namespace), no config
file -- i.e. literally ``broker dump | BROKER_BACKEND=redis broker load``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from simplebroker_redis import get_backend_plugin

TEST_URL = os.environ.get("SIMPLEBROKER_VALKEY_TEST_URL") or os.environ.get(
    "SIMPLEBROKER_REDIS_TEST_URL"
)
pytestmark = [
    pytest.mark.redis_only,
    pytest.mark.skipif(
        not TEST_URL,
        reason="Set SIMPLEBROKER_VALKEY_TEST_URL to run Redis extension tests",
    ),
]

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _run_cli(
    *args: str,
    cwd: Path,
    env: dict[str, str] | None = None,
    stdin: str | None = None,
) -> tuple[int, str, str]:
    full_env = os.environ.copy()
    full_env.pop("BROKER_BACKEND", None)  # explicit per-side selection only
    if env:
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


def _sqlite_env() -> dict[str, str]:
    return {"BROKER_BACKEND": "sqlite"}


def _redis_env(namespace: str) -> dict[str, str]:
    assert TEST_URL is not None
    return {
        "BROKER_BACKEND": "redis",
        "BROKER_BACKEND_TARGET": TEST_URL,
        "BROKER_BACKEND_SCHEMA": namespace,
    }


def _cleanup_namespace(namespace: str) -> None:
    assert TEST_URL is not None
    get_backend_plugin().cleanup_target(
        TEST_URL, backend_options={"namespace": namespace}
    )


def _seed(cwd: Path, env: dict[str, str]) -> None:
    assert _run_cli("write", "alpha", "a0", cwd=cwd, env=env)[0] == 0
    assert _run_cli("write", "alpha", "a1", cwd=cwd, env=env)[0] == 0
    assert _run_cli("write", "beta", "b0", cwd=cwd, env=env)[0] == 0
    assert _run_cli("alias", "add", "al", "alpha", cwd=cwd, env=env)[0] == 0


def _nonheader(dump_output: str) -> list[str]:
    lines = [ln for ln in dump_output.splitlines() if ln.strip()]
    assert json.loads(lines[0])["type"] == "header"
    return lines[1:]


def test_sqlite_to_redis_pipe(tmp_path: Path) -> None:
    namespace = f"dumppipe_{uuid.uuid4().hex[:12]}"
    redis_env = _redis_env(namespace)
    src = tmp_path / "src"
    src.mkdir()
    _seed(src, _sqlite_env())

    code, dump_out, err = _run_cli("dump", cwd=src, env=_sqlite_env())
    assert code == 0, err
    assert json.loads(dump_out.splitlines()[0])["backend"] == "sqlite"

    redis_dir = tmp_path / "redis"
    redis_dir.mkdir()
    try:
        code, _out, err = _run_cli("load", cwd=redis_dir, env=redis_env, stdin=dump_out)
        assert code == 0, err

        code, redump, err = _run_cli("dump", cwd=redis_dir, env=redis_env)
        assert code == 0, err
        assert json.loads(redump.splitlines()[0])["backend"] == "redis"
        assert _nonheader(redump) == _nonheader(dump_out)  # I-RT across backends

        code, out, _err = _run_cli("read", "alpha", cwd=redis_dir, env=redis_env)
        assert (code, out.strip()) == (0, "a0")
    finally:
        _cleanup_namespace(namespace)


def test_redis_to_sqlite_pipe(tmp_path: Path) -> None:
    namespace = f"dumppipe_{uuid.uuid4().hex[:12]}"
    redis_env = _redis_env(namespace)
    redis_dir = tmp_path / "redis"
    redis_dir.mkdir()
    try:
        _seed(redis_dir, redis_env)

        code, dump_out, err = _run_cli("dump", cwd=redis_dir, env=redis_env)
        assert code == 0, err
        assert json.loads(dump_out.splitlines()[0])["backend"] == "redis"

        dst = tmp_path / "dst"
        dst.mkdir()
        code, _out, err = _run_cli("load", cwd=dst, env=_sqlite_env(), stdin=dump_out)
        assert code == 0, err

        code, redump, err = _run_cli("dump", cwd=dst, env=_sqlite_env())
        assert code == 0, err
        assert json.loads(redump.splitlines()[0])["backend"] == "sqlite"
        assert _nonheader(redump) == _nonheader(dump_out)  # I-RT across backends

        code, out, _err = _run_cli("read", "alpha", cwd=dst, env=_sqlite_env())
        assert (code, out.strip()) == (0, "a0")
    finally:
        _cleanup_namespace(namespace)
