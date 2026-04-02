"""Integration tests for the Postgres SimpleBroker backend."""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from simplebroker_pg import PostgresRunner

from simplebroker import Queue

TEST_DSN = os.environ.get("SIMPLEBROKER_PG_TEST_DSN")
pytestmark = [
    pytest.mark.pg_only,
    pytest.mark.skipif(
        not TEST_DSN,
        reason="Set SIMPLEBROKER_PG_TEST_DSN to run Postgres extension tests",
    ),
]


def _schema_name(prefix: str = "sbtest") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _require_test_dsn() -> str:
    """Return the configured Postgres test DSN."""
    if TEST_DSN is None:
        raise RuntimeError("SIMPLEBROKER_PG_TEST_DSN is required for pg tests")
    return TEST_DSN


def _run_cli(
    *args: str,
    cwd: Path,
    env: dict[str, str] | None = None,
) -> tuple[int, str, str]:
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    project_paths = [str(Path(__file__).resolve().parents[3])]
    existing_pythonpath = full_env.get("PYTHONPATH")
    if existing_pythonpath:
        project_paths.append(existing_pythonpath)
    full_env["PYTHONPATH"] = os.pathsep.join(project_paths)
    full_env["PYTHONUNBUFFERED"] = "1"
    full_env["PYTHONIOENCODING"] = "utf-8"

    completed = subprocess.run(
        [sys.executable, "-m", "simplebroker.cli", *args],
        cwd=cwd,
        text=True,
        capture_output=True,
        env=full_env,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return completed.returncode, completed.stdout.strip(), completed.stderr.strip()


def test_postgres_runner_roundtrip() -> None:
    """Explicit runner injection should work end-to-end."""
    schema = _schema_name()
    dsn = _require_test_dsn()
    runner = PostgresRunner(dsn, schema=schema)
    queue = Queue("jobs", runner=runner, persistent=True)
    try:
        queue.write("hello")
        assert queue.read() == "hello"
    finally:
        queue.close()
        runner.backend_plugin.cleanup_target(
            dsn,
            backend_options={"schema": schema},
        )
        runner.close()


def test_postgres_cli_project_config_roundtrip(tmp_path: Path) -> None:
    """The core CLI should operate against a Postgres project config."""
    schema = _schema_name()
    project_root = tmp_path / "project"
    nested = project_root / "app" / "src"
    nested.mkdir(parents=True)

    config_path = project_root / ".simplebroker.toml"
    config_path.write_text(
        "\n".join(
            [
                "version = 1",
                'backend = "postgres"',
                f'target = "{TEST_DSN}"',
                "",
                "[backend_options]",
                f'schema = "{schema}"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    env = {"BROKER_PROJECT_SCOPE": "1"}

    code, stdout, stderr = _run_cli("init", cwd=nested, env=env)
    assert code == 0, stderr

    code, stdout, stderr = _run_cli("write", "jobs", "hello", cwd=nested, env=env)
    assert code == 0, stderr

    code, stdout, stderr = _run_cli("read", "jobs", cwd=nested, env=env)
    assert code == 0, stderr
    assert stdout == "hello"

    code, stdout, stderr = _run_cli("--cleanup", cwd=nested, env=env)
    assert code == 0, stderr
