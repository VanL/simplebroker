"""Integration tests for the Postgres SimpleBroker backend."""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from psycopg import conninfo as pg_conninfo
from simplebroker_pg import PostgresRunner, get_backend_plugin

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
    timeout: float | None = None,
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
        timeout=timeout,
    )
    return completed.returncode, completed.stdout.strip(), completed.stderr.strip()


def _dsn_parts() -> dict[str, str]:
    return {
        key: str(value)
        for key, value in pg_conninfo.conninfo_to_dict(_require_test_dsn()).items()
    }


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

    env = {"BROKER_PROJECT_SCOPE": "1", "BROKER_BACKEND": "sqlite"}

    code, stdout, stderr = _run_cli("init", cwd=nested, env=env)
    assert code == 0, stderr

    code, stdout, stderr = _run_cli("write", "jobs", "hello", cwd=nested, env=env)
    assert code == 0, stderr

    code, stdout, stderr = _run_cli("read", "jobs", cwd=nested, env=env)
    assert code == 0, stderr
    assert stdout == "hello"

    code, stdout, stderr = _run_cli("--cleanup", cwd=nested, env=env)
    assert code == 0, stderr


def test_postgres_cli_env_selected_backend_roundtrip(tmp_path: Path) -> None:
    """The CLI should operate against env-selected Postgres without any toml."""
    schema = _schema_name()
    project_root = tmp_path / "project"
    project_root.mkdir()

    env = {
        "BROKER_BACKEND": "postgres",
        "BROKER_BACKEND_TARGET": _require_test_dsn(),
        "BROKER_BACKEND_SCHEMA": schema,
    }

    code, stdout, stderr = _run_cli("init", cwd=project_root, env=env)
    assert code == 0, stderr

    code, stdout, stderr = _run_cli("write", "jobs", "hello", cwd=project_root, env=env)
    assert code == 0, stderr

    code, stdout, stderr = _run_cli("read", "jobs", cwd=project_root, env=env)
    assert code == 0, stderr
    assert stdout == "hello"

    code, stdout, stderr = _run_cli("--cleanup", cwd=project_root, env=env)
    assert code == 0, stderr


def test_postgres_cli_env_selected_backend_init_exits_cleanly(tmp_path: Path) -> None:
    """Env-selected Postgres init should not leave pool workers alive at exit."""

    schema = _schema_name()
    project_root = tmp_path / "project"
    project_root.mkdir()

    env = {
        "BROKER_BACKEND": "postgres",
        "BROKER_BACKEND_TARGET": _require_test_dsn(),
        "BROKER_BACKEND_SCHEMA": schema,
    }

    try:
        code, stdout, stderr = _run_cli(
            "init",
            cwd=project_root,
            env=env,
            timeout=10.0,
        )
        assert code == 0, stderr
        assert "Initialized SimpleBroker target:" in stdout
    finally:
        get_backend_plugin().cleanup_target(
            _require_test_dsn(),
            backend_options={"schema": schema},
        )


def test_postgres_owned_runner_subprocess_exits_cleanly(tmp_path: Path) -> None:
    """Owned Postgres runners should shut down their pool threads on process exit."""

    schema = _schema_name()
    project_root = tmp_path / "project"
    project_root.mkdir()

    env = os.environ.copy()
    env.update(
        {
            "BROKER_BACKEND": "postgres",
            "BROKER_BACKEND_TARGET": _require_test_dsn(),
            "BROKER_BACKEND_SCHEMA": schema,
        }
    )

    script = """
from pathlib import Path

from simplebroker import Queue, target_for_directory

target = target_for_directory(Path.cwd())
with Queue("jobs", db_path=target) as queue:
    queue.write("hello")
"""

    try:
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=project_root,
            text=True,
            capture_output=True,
            env=env,
            encoding="utf-8",
            errors="replace",
            check=False,
            timeout=10,
        )
        assert completed.returncode == 0, completed.stderr
    finally:
        get_backend_plugin().cleanup_target(
            _require_test_dsn(),
            backend_options={"schema": schema},
        )


def test_postgres_cli_password_env_merges_into_target(tmp_path: Path) -> None:
    """BROKER_BACKEND_PASSWORD should work with a passwordless target string."""
    parts = _dsn_parts()
    password = parts.get("password")
    if not password:
        pytest.skip("test DSN does not include a password to inject")

    schema = _schema_name()
    project_root = tmp_path / "project"
    project_root.mkdir()

    env = {
        "BROKER_BACKEND": "postgres",
        "BROKER_BACKEND_TARGET": pg_conninfo.make_conninfo(
            host=parts["host"],
            port=parts["port"],
            user=parts["user"],
            dbname=parts["dbname"],
        ),
        "BROKER_BACKEND_PASSWORD": password,
        "BROKER_BACKEND_SCHEMA": schema,
    }

    code, stdout, stderr = _run_cli("init", cwd=project_root, env=env)
    assert code == 0, stderr

    code, stdout, stderr = _run_cli("write", "jobs", "hello", cwd=project_root, env=env)
    assert code == 0, stderr

    code, stdout, stderr = _run_cli("read", "jobs", cwd=project_root, env=env)
    assert code == 0, stderr
    assert stdout == "hello"

    code, stdout, stderr = _run_cli("--cleanup", cwd=project_root, env=env)
    assert code == 0, stderr


def test_postgres_cli_missing_database_is_not_backend_unavailable(
    tmp_path: Path,
) -> None:
    """A bad target should fail as a connection error, not availability."""
    parts = _dsn_parts()
    schema = _schema_name()
    project_root = tmp_path / "project"
    project_root.mkdir()

    env = {
        "BROKER_BACKEND": "postgres",
        "BROKER_BACKEND_HOST": parts["host"],
        "BROKER_BACKEND_PORT": parts["port"],
        "BROKER_BACKEND_USER": parts["user"],
        "BROKER_BACKEND_DATABASE": f"missing_{uuid.uuid4().hex[:12]}",
        "BROKER_BACKEND_SCHEMA": schema,
    }
    if "password" in parts:
        env["BROKER_BACKEND_PASSWORD"] = parts["password"]

    code, stdout, stderr = _run_cli("init", cwd=project_root, env=env)
    assert code != 0
    assert "not available" not in stderr
    assert "Could not connect to Postgres target" in stderr
