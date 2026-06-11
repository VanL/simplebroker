"""Cross-backend dump/load pipes: SQLite <-> Postgres via the env interface."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from simplebroker_pg import get_backend_plugin

TEST_DSN = os.environ.get("SIMPLEBROKER_PG_TEST_DSN")
pytestmark = [
    pytest.mark.pg_only,
    pytest.mark.skipif(
        not TEST_DSN,
        reason="Set SIMPLEBROKER_PG_TEST_DSN to run Postgres extension tests",
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


def _pg_env(schema: str) -> dict[str, str]:
    assert TEST_DSN is not None
    return {
        "BROKER_BACKEND": "postgres",
        "BROKER_BACKEND_TARGET": TEST_DSN,
        "BROKER_BACKEND_SCHEMA": schema,
    }


def _cleanup_schema(schema: str) -> None:
    assert TEST_DSN is not None
    get_backend_plugin().cleanup_target(
        TEST_DSN,
        backend_options={"schema": schema},
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


def test_sqlite_to_postgres_pipe(tmp_path: Path) -> None:
    schema = f"dumppipe_{uuid.uuid4().hex[:12]}"
    src = tmp_path / "src"
    src.mkdir()
    _seed(src, _sqlite_env())

    code, dump_out, err = _run_cli("dump", cwd=src, env=_sqlite_env())
    assert code == 0, err
    assert json.loads(dump_out.splitlines()[0])["backend"] == "sqlite"

    pg_dir = tmp_path / "pg"
    pg_dir.mkdir()
    try:
        code, _out, err = _run_cli(
            "load", cwd=pg_dir, env=_pg_env(schema), stdin=dump_out
        )
        assert code == 0, err

        code, redump, err = _run_cli("dump", cwd=pg_dir, env=_pg_env(schema))
        assert code == 0, err
        assert json.loads(redump.splitlines()[0])["backend"] == "postgres"
        assert _nonheader(redump) == _nonheader(dump_out)  # I-RT across backends

        code, out, _ = _run_cli("read", "alpha", cwd=pg_dir, env=_pg_env(schema))
        assert (code, out.strip()) == (0, "a0")
    finally:
        _cleanup_schema(schema)


def test_postgres_to_sqlite_pipe(tmp_path: Path) -> None:
    schema = f"dumppipe_{uuid.uuid4().hex[:12]}"
    pg_dir = tmp_path / "pg"
    pg_dir.mkdir()
    try:
        _seed(pg_dir, _pg_env(schema))

        code, dump_out, err = _run_cli("dump", cwd=pg_dir, env=_pg_env(schema))
        assert code == 0, err
    finally:
        _cleanup_schema(schema)

    dst = tmp_path / "dst"
    dst.mkdir()
    code, _out, err = _run_cli("load", cwd=dst, env=_sqlite_env(), stdin=dump_out)
    assert code == 0, err

    code, redump, err = _run_cli("dump", cwd=dst, env=_sqlite_env())
    assert code == 0, err
    assert _nonheader(redump) == _nonheader(dump_out)
