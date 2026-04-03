"""
Shared fixtures / helpers for the SimpleBroker test-suite.

The guiding idea is "black-box, thin, but meaningful": all interaction goes
through the real command-line entry point (`python -m simplebroker.cli …`)
exactly how an end-user would invoke it.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from simplebroker import Queue
from simplebroker._project_config import PROJECT_CONFIG_FILENAME, find_project_config
from simplebroker._targets import ResolvedTarget

from .helper_scripts.broker_factory import (
    make_broker,
    make_queue,
    make_target,
)

# Import cleanup fixtures
from .helper_scripts.cleanup import cleanup_at_exit, cleanup_watchers

# Import subprocess utilities
from .helper_scripts.managed_subprocess import (
    ManagedProcess,
    managed_subprocess,
    run_subprocess,
)
from .helper_scripts.timing import scale_timeout_for_ci

# Import watcher patching
from .helper_scripts.watcher_patch import patch_watchers

# Import coverage subprocess helper if coverage is active
if os.environ.get("COVERAGE_PROCESS_START"):
    from .coverage_subprocess import run_with_coverage
else:
    run_with_coverage = None

if TYPE_CHECKING:
    from collections.abc import Iterator

    from simplebroker._backend_plugins import BackendPlugin


PROJECT_ROOT = Path(__file__).resolve().parents[1]

POSTGRES_TEST_BACKEND = "postgres"
_SQLITE_ONLY_RUN_CLI_MODULE_REASONS = {
    "test_absolute_path.py": "Exercises SQLite file path semantics via -f/-d and asserts on real database files.",
    "test_cli_argument_parsing.py": "Validates --dir/--file path parsing by asserting specific SQLite file creation locations.",
    "test_commands_init.py": "Tests SQLite database bootstrap, file validation, and on-disk schema details.",
    "test_edge_cases.py": "Contains SQLite file and sqlite3-backed assertions alongside CLI coverage.",
    "test_performance.py": "Contains SQLite-backed performance checks and sqlite3-specific assertions.",
    "test_symlink_security.py": "Validates symlink and path-containment rules for SQLite file targets.",
    "test_vacuum_compact.py": "Asserts SQLite VACUUM behavior, sqlite3 PRAGMAs, and file-size changes.",
}


def _test_backend_name(env: dict[str, str] | None = None) -> str:
    """Return the active backend name for CLI black-box tests."""
    if env and env.get("BROKER_TEST_BACKEND"):
        return env["BROKER_TEST_BACKEND"]
    return os.environ.get("BROKER_TEST_BACKEND", "sqlite")


def _postgres_schema_name(root: Path) -> str:
    """Derive a stable per-test schema name from the temp project root."""
    digest = hashlib.sha1(str(root.resolve()).encode("utf-8")).hexdigest()[:16]
    return f"pytest_{digest}"


def _config_root_from_args(args: tuple[object, ...], cwd: Path) -> Path:
    """Return the directory where a PG test project config should live."""
    arg_list = [str(arg) for arg in args]
    for index, arg in enumerate(arg_list):
        if arg.startswith("--dir="):
            dir_arg = arg.split("=", 1)[1]
            return (
                (cwd / dir_arg).resolve()
                if not Path(dir_arg).is_absolute()
                else Path(dir_arg)
            )
        if arg in {"-d", "--dir"} and index + 1 < len(arg_list):
            dir_arg = arg_list[index + 1]
            return (
                (cwd / dir_arg).resolve()
                if not Path(dir_arg).is_absolute()
                else Path(dir_arg)
            )
    return cwd.resolve()


def _ensure_postgres_project_config(
    config_root: Path,
    *,
    dsn: str,
) -> Path:
    """Create a per-test PG project config unless one already exists above cwd."""
    existing = find_project_config(config_root)
    if existing is not None:
        return existing

    config_root.mkdir(parents=True, exist_ok=True)
    # Prefer worker schema from env (set by session-scoped fixture under xdist)
    schema = os.environ.get("SIMPLEBROKER_PG_TEST_SCHEMA") or _postgres_schema_name(
        config_root
    )
    config_path = config_root / PROJECT_CONFIG_FILENAME
    config_path.write_text(
        "\n".join(
            [
                "version = 1",
                'backend = "postgres"',
                f'target = "{dsn}"',
                "",
                "[backend_options]",
                f'schema = "{schema}"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def _initialize_postgres_project(config_path: Path, *, dsn: str) -> None:
    """Ensure the temporary PG target schema exists for black-box CLI tests."""
    from simplebroker._backend_plugins import get_backend_plugin
    from simplebroker._project_config import load_project_config

    config = load_project_config(config_path)
    backend_options = dict(config.get("backend_options", {}))
    get_backend_plugin(POSTGRES_TEST_BACKEND).initialize_target(
        dsn,
        backend_options=backend_options,
    )


def _cleanup_postgres_projects(root: Path) -> None:
    """Drop any temporary PG schemas created under a test workdir."""
    if _test_backend_name() != POSTGRES_TEST_BACKEND:
        return

    dsn = os.environ.get("SIMPLEBROKER_PG_TEST_DSN")
    if not dsn:
        return

    from simplebroker._backend_plugins import get_backend_plugin
    from simplebroker._project_config import load_project_config

    plugin = get_backend_plugin(POSTGRES_TEST_BACKEND)
    cleaned_schemas: set[str] = set()
    for config_path in root.rglob(PROJECT_CONFIG_FILENAME):
        try:
            config = load_project_config(config_path)
        except Exception:
            continue
        if config.get("backend") != POSTGRES_TEST_BACKEND:
            continue
        schema = str(config.get("backend_options", {}).get("schema", "")).strip()
        if not schema or schema in cleaned_schemas:
            continue
        plugin.cleanup_target(
            dsn,
            backend_options={"schema": schema},
        )
        cleaned_schemas.add(schema)


# --------------------------------------------------------------------------- #
# Postgres worker-scoped fixtures (one schema per xdist worker)
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="session")
def pg_worker_dsn() -> str | None:
    """Return the PG test DSN, or None when not running against Postgres."""
    if _test_backend_name() != POSTGRES_TEST_BACKEND:
        return None
    dsn = os.environ.get("SIMPLEBROKER_PG_TEST_DSN")
    if not dsn:
        raise RuntimeError(
            "BROKER_TEST_BACKEND=postgres requires SIMPLEBROKER_PG_TEST_DSN"
        )
    return dsn


@pytest.fixture(scope="session")
def pg_worker_schema(worker_id: str, pg_worker_dsn: str | None) -> str | None:
    """Derive a unique schema name for this xdist worker."""
    if pg_worker_dsn is None:
        return None
    schema = f"pytest_worker_{worker_id}"
    os.environ["SIMPLEBROKER_PG_TEST_SCHEMA"] = schema
    return schema


@pytest.fixture(scope="session")
def pg_worker_tmpdir(
    tmp_path_factory: pytest.TempPathFactory,
    pg_worker_dsn: str | None,
    pg_worker_schema: str | None,
) -> Path | None:
    """Single tmpdir per worker with a .simplebroker.toml pointing at the worker schema."""
    if pg_worker_dsn is None or pg_worker_schema is None:
        return None
    root = tmp_path_factory.mktemp("pg_worker")
    config_path = root / PROJECT_CONFIG_FILENAME
    config_path.write_text(
        "\n".join(
            [
                "version = 1",
                'backend = "postgres"',
                f'target = "{pg_worker_dsn}"',
                "",
                "[backend_options]",
                f'schema = "{pg_worker_schema}"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return root


@pytest.fixture(scope="session")
def pg_worker_plugin(
    pg_worker_dsn: str | None,
) -> BackendPlugin | None:
    """Return the PG backend plugin singleton, or None for SQLite runs."""
    if pg_worker_dsn is None:
        return None
    from simplebroker._backend_plugins import get_backend_plugin

    return get_backend_plugin(POSTGRES_TEST_BACKEND)


@pytest.fixture(scope="session")
def pg_worker_runner(
    pg_worker_dsn: str | None,
    pg_worker_schema: str | None,
    pg_worker_plugin: BackendPlugin | None,
) -> Iterator[Any]:
    """
    Session-scoped runner: initializes the worker schema once and tears it down
    at the end.  Yields the runner for use in per-test TRUNCATE resets.
    """
    if pg_worker_dsn is None or pg_worker_schema is None or pg_worker_plugin is None:
        yield None
        return

    from simplebroker_pg import PostgresRunner

    runner = PostgresRunner(pg_worker_dsn, schema=pg_worker_schema)
    _ensure_pg_schema_initialized(runner, pg_worker_plugin)

    try:
        yield runner
    finally:
        # Teardown: drop the entire worker schema
        try:
            pg_worker_plugin.cleanup_target(
                pg_worker_dsn,
                backend_options={"schema": pg_worker_schema},
            )
        except Exception:
            pass
        if hasattr(runner, "shutdown"):
            runner.shutdown()
        else:
            runner.close()


@pytest.fixture(autouse=True, scope="session")
def _pg_worker_bootstrap(pg_worker_runner: Any) -> None:
    """Autouse: ensure the PG worker schema is initialized in every worker.

    By depending on pg_worker_runner this forces the entire session-scoped
    fixture chain (dsn → schema → tmpdir → plugin → runner) to resolve even
    for tests that never explicitly request ``workdir``.
    """


def _ensure_pg_schema_initialized(runner: Any, plugin: Any) -> None:
    """Create (or re-create) the broker tables inside the worker schema."""
    from typing import cast

    from simplebroker._backend_plugins import BackendPlugin
    from simplebroker.db import BrokerCore

    core = BrokerCore(runner, backend_plugin=cast(BackendPlugin, plugin))
    core.close()


def _reset_pg_tables(runner: Any, plugin: Any) -> None:
    """TRUNCATE all broker tables and re-seed the meta row for test isolation.

    If the tables are missing (e.g. a prior test ran ``--cleanup`` which drops
    the schema), re-initialize the full schema first.
    """
    from simplebroker_pg._constants import POSTGRES_SCHEMA_VERSION

    from simplebroker._constants import SIMPLEBROKER_MAGIC
    from simplebroker._exceptions import OperationalError

    try:
        runner.run("TRUNCATE messages RESTART IDENTITY CASCADE")
        runner.run("TRUNCATE aliases")
        runner.run("DELETE FROM meta")
    except OperationalError:
        # Tables were dropped (e.g. by --cleanup).  Re-create everything.
        _ensure_pg_schema_initialized(runner, plugin)
        return

    runner.run(
        "INSERT INTO meta (singleton, magic, schema_version, last_ts, alias_version) "
        "VALUES (TRUE, ?, ?, 0, 0)",
        (SIMPLEBROKER_MAGIC, POSTGRES_SCHEMA_VERSION),
    )


# --------------------------------------------------------------------------- #
# Backend-agnostic fixtures (broker_factory)
# --------------------------------------------------------------------------- #


@pytest.fixture
def broker_target(
    tmp_path: Path,
    pg_worker_dsn: str | None,
    pg_worker_schema: str | None,
    pg_worker_runner,
    pg_worker_plugin,
) -> ResolvedTarget:
    """Backend-agnostic resolved target for the active backend."""
    if _test_backend_name() == POSTGRES_TEST_BACKEND:
        assert pg_worker_dsn is not None
        assert pg_worker_schema is not None
        assert pg_worker_runner is not None
        _reset_pg_tables(pg_worker_runner, pg_worker_plugin)
        return make_target(
            tmp_path,
            backend="postgres",
            pg_dsn=pg_worker_dsn,
            pg_schema=pg_worker_schema,
        )
    return make_target(tmp_path, backend="sqlite")


@pytest.fixture
def broker(broker_target: ResolvedTarget) -> Iterator:
    """Backend-agnostic BrokerCore instance."""
    core = make_broker(broker_target)
    try:
        yield core
    finally:
        core.close()


@pytest.fixture
def queue_factory(broker_target: ResolvedTarget):
    """Factory that creates Queue instances bound to the active backend.

    Returns a callable: ``queue_factory("queue_name")`` -> ``Queue``.
    All queues created are closed automatically at teardown.
    """
    created: list = []

    def _factory(name: str, *, persistent: bool = True) -> Queue:
        q = make_queue(name, broker_target, persistent=persistent)
        created.append(q)
        return q

    yield _factory

    for q in created:
        try:
            q.close()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture
def workdir(
    tmp_path: Path,
    monkeypatch,
    pg_worker_tmpdir: Path | None,
    pg_worker_runner,
    pg_worker_plugin,
) -> Iterator[Path]:
    """
    Per-test temporary working directory.

    SQLite: each test gets its own tmp_path with a fresh database.
    Postgres: all tests in a worker share a single tmpdir + schema.
        Tables are TRUNCATEd between tests for isolation.
    """
    if _test_backend_name() == POSTGRES_TEST_BACKEND:
        assert pg_worker_tmpdir is not None
        assert pg_worker_runner is not None
        _reset_pg_tables(pg_worker_runner, pg_worker_plugin)
        monkeypatch.chdir(pg_worker_tmpdir)
        monkeypatch.setenv("BROKER_PROJECT_SCOPE", "1")
        yield pg_worker_tmpdir
    else:
        monkeypatch.chdir(tmp_path)
        yield tmp_path
        # Best-effort cleanup – ignore in-use errors on Windows.
        shutil.rmtree(tmp_path, ignore_errors=True)


# --------------------------------------------------------------------------- #
# Helper(s)
# --------------------------------------------------------------------------- #
def build_cli_env(env: dict[str, str] | None = None) -> dict[str, str]:
    """Build a subprocess environment for invoking the in-repo CLI."""
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    full_env["PYTHONIOENCODING"] = "utf-8"
    full_env["PYTHONUNBUFFERED"] = "1"
    project_paths = [str(PROJECT_ROOT)]
    existing_pythonpath = full_env.get("PYTHONPATH")
    if existing_pythonpath:
        project_paths.append(existing_pythonpath)
    full_env["PYTHONPATH"] = os.pathsep.join(project_paths)
    return full_env


def run_cli(
    *args,
    cwd: Path,
    stdin: str | None = None,
    timeout: float | None = None,
    env: dict[str, str] | None = None,
) -> tuple[int, str, str]:
    """
    Execute the SimpleBroker CLI (`python -m simplebroker.cli …`) inside *cwd*.

    Parameters
    ----------
    *args
        Individual CLI arguments, e.g. ``run_cli("write", "q", "msg", cwd=dir)``.
        All items are converted to str.
    cwd
        Directory where the command is executed (and where the DB lives).
    stdin
        If given, string passed to the process' standard input.
    timeout
        Safety valve – kill the process if it takes longer (seconds). Defaults to
        12s on Windows and 6s on other platforms.

    Returns
    -------
    (return_code, stdout, stderr)
        All output is stripped of trailing new-lines for convenience.
    """
    cmd = [sys.executable, "-m", "simplebroker.cli", *map(str, args)]

    full_env = build_cli_env(env)

    if _test_backend_name(full_env) == POSTGRES_TEST_BACKEND:
        dsn = full_env.get("SIMPLEBROKER_PG_TEST_DSN")
        if not dsn:
            raise RuntimeError(
                "BROKER_TEST_BACKEND=postgres requires SIMPLEBROKER_PG_TEST_DSN"
            )
        config_root = _config_root_from_args(args, cwd)
        _ensure_postgres_project_config(config_root, dsn=dsn)
        # Schema is pre-initialized by the session-scoped pg_worker_runner
        # fixture.  Each CLI subprocess will run its own idempotent
        # initialize_database() via BrokerCore.__init__.
        full_env.setdefault("BROKER_PROJECT_SCOPE", "1")

    # Use coverage-wrapped subprocess if available, otherwise normal subprocess
    run_func = run_with_coverage if run_with_coverage else subprocess.run

    if timeout is None:
        timeout = scale_timeout_for_ci(12.0 if sys.platform == "win32" else 6.0)

    run_kwargs: dict[str, Any] = {
        "cwd": cwd,
        "capture_output": True,
        "timeout": timeout,
        "env": full_env,
    }

    if stdin is None:
        run_kwargs.update(
            {
                "text": True,
                "encoding": "utf-8",
                "errors": "replace",
            }
        )
    else:
        # Feed raw bytes so black-box stdin tests are not affected by the
        # platform's text-mode newline translation, especially on Windows.
        run_kwargs["input"] = stdin.encode("utf-8")

    completed = run_func(cmd, **run_kwargs)

    if stdin is None:
        stdout = completed.stdout
        stderr = completed.stderr
    else:
        stdout = completed.stdout.decode("utf-8", errors="replace")
        stderr = completed.stderr.decode("utf-8", errors="replace")

    return (
        completed.returncode,
        stdout.strip(),
        stderr.strip(),
    )


@cache
def _module_uses_run_cli(path: str) -> bool:
    """Return True when a test module exercises the real CLI harness."""
    text = Path(path).read_text(encoding="utf-8")
    return "run_cli(" in text


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark tests by backend scope.

    Rule of thumb:
    - modules that exercise the real CLI via run_cli are shared
    - unless they still assert SQLite file/catalog behavior
    - everything else in the core suite is SQLite-specific
    """
    for item in items:
        if item.get_closest_marker("shared") or item.get_closest_marker("sqlite_only"):
            continue
        if item.path.name in _SQLITE_ONLY_RUN_CLI_MODULE_REASONS:
            item.add_marker(pytest.mark.sqlite_only)
        elif _module_uses_run_cli(str(item.path)):
            item.add_marker(pytest.mark.shared)
        else:
            item.add_marker(pytest.mark.sqlite_only)


# --------------------------------------------------------------------------- #
# Export subprocess utilities for use in tests
# --------------------------------------------------------------------------- #
__all__ = [
    "build_cli_env",
    "run_cli",
    "workdir",
    "managed_subprocess",
    "run_subprocess",
    "ManagedProcess",
    "cleanup_watchers",
    "cleanup_at_exit",
    "patch_watchers",
]
