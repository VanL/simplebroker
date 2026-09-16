"""Integration tests for the Postgres SimpleBroker backend."""

from __future__ import annotations

import contextlib
import os
import subprocess
import sys
import threading
import uuid
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, cast

import pytest
from psycopg import conninfo as pg_conninfo
from simplebroker_pg import PostgresRunner, get_backend_plugin
from simplebroker_pg.validation import connect

from simplebroker import BrokerSession, BrokerTarget, Config, Queue
from simplebroker._runner import SetupPhase, SQLRunner
from simplebroker.db import BrokerCore

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


def test_postgres_runner_quotes_mixed_case_search_path() -> None:
    """Mixed-case schemas must remain the active schema for unqualified SQL."""
    schema = _schema_name("SbTest")
    dsn = _require_test_dsn()
    plugin = get_backend_plugin()
    runner = PostgresRunner(dsn, schema=schema)
    core: BrokerCore | None = None

    try:
        core = BrokerCore(runner, backend_plugin=plugin)
        current_schema = next(iter(runner.run("SELECT current_schema()", fetch=True)))[
            0
        ]
        assert current_schema == schema

        core.write("jobs", "hello")
        assert core.claim_one("jobs", with_timestamps=False) == "hello"

        with connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    ORDER BY table_name
                    """,
                (schema,),
            )
            assert {row[0] for row in cur.fetchall()} >= {
                "aliases",
                "messages",
                "meta",
            }
    finally:
        if core is not None:
            core.close()
        runner.close()
        plugin.cleanup_target(dsn, backend_options={"schema": schema})


def test_postgres_runner_skips_schema_ddl_after_bootstrap() -> None:
    """New runners should avoid catalog DDL once broker metadata exists."""
    schema = _schema_name()
    dsn = _require_test_dsn()
    plugin = get_backend_plugin()
    first_runner = PostgresRunner(dsn, schema=schema)
    second_runner = PostgresRunner(dsn, schema=schema)
    seen_sql: list[str] = []

    try:
        first_core = BrokerCore(first_runner, backend_plugin=plugin)
        first_core.close()

        original_run = second_runner.run

        def tracked_run(
            sql: str,
            params: tuple[Any, ...] = (),
            *,
            fetch: bool = False,
        ) -> Iterable[tuple[Any, ...]]:
            seen_sql.append(sql)
            return original_run(sql, params, fetch=fetch)

        second_runner.run = tracked_run  # type: ignore[method-assign]

        second_core = BrokerCore(second_runner, backend_plugin=plugin)
        second_core.close()
    finally:
        first_runner.shutdown()
        second_runner.shutdown()
        plugin.cleanup_target(
            dsn,
            backend_options={"schema": schema},
        )

    ddl_statements = [
        sql
        for sql in seen_sql
        if any(
            marker in sql.upper()
            for marker in ("CREATE SCHEMA", "CREATE TABLE", "CREATE INDEX")
        )
    ]
    assert ddl_statements == []


def test_wrapped_postgres_runner_with_resolved_target_uses_pg_waiter() -> None:
    """A non-backend-aware wrapper should still use the Postgres target plugin."""

    class WrappedRunner:
        def __init__(self, runner: PostgresRunner) -> None:
            self._runner = runner

        def run(
            self,
            sql: str,
            params: tuple[Any, ...] = (),
            *,
            fetch: bool = False,
        ) -> Any:
            return self._runner.run(sql, params, fetch=fetch)

        def begin_immediate(self) -> None:
            self._runner.begin_immediate()

        def commit(self) -> None:
            self._runner.commit()

        def rollback(self) -> None:
            self._runner.rollback()

        def close(self) -> None:
            self._runner.close()

        def setup(self, phase: SetupPhase) -> None:
            self._runner.setup(phase)

        def is_setup_complete(self, phase: SetupPhase) -> bool:
            return self._runner.is_setup_complete(phase)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._runner, name)

    schema = _schema_name()
    dsn = _require_test_dsn()
    plugin = get_backend_plugin()
    runner = PostgresRunner(dsn, schema=schema)
    wrapped_runner = WrappedRunner(runner)
    target = BrokerTarget(
        "postgres",
        dsn,
        backend_options={"schema": schema},
    )
    queue = Queue("jobs", db_path=target, runner=wrapped_runner, persistent=True)

    try:
        waiter = queue.create_activity_waiter(stop_event=threading.Event())
        assert waiter is not None
        assert waiter.__class__.__name__ == "PostgresActivityWaiter"

        queue.write("hello")
        assert queue.read() == "hello"
    finally:
        queue.close()
        runner.shutdown()
        plugin.cleanup_target(dsn, backend_options={"schema": schema})


def test_postgres_cli_project_config_roundtrip(tmp_path: Path) -> None:
    """The core CLI should operate against a Postgres project config."""
    schema = _schema_name()
    project_root = tmp_path / "project"
    nested = project_root / "app" / "src"
    nested.mkdir(parents=True)

    config_path = project_root / ".broker.toml"
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


def test_postgres_project_persistent_queues_share_plugin_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Project bootstrap is isolated; persistent queues share one runtime runner."""
    schema = _schema_name()
    dsn = _require_test_dsn()
    project_root = tmp_path / "project"
    project_root.mkdir()
    config_path = project_root / ".broker.toml"
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

    plugin = get_backend_plugin()
    original_create_runner = plugin.create_runner
    create_runner_calls = 0
    runner_ids: list[int] = []

    def tracked_create_runner(
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Config | None = None,
    ) -> SQLRunner:
        nonlocal create_runner_calls
        create_runner_calls += 1
        runner = original_create_runner(
            target,
            backend_options=backend_options,
            config=config,
        )
        runner_ids.append(id(runner))
        return runner

    monkeypatch.setattr(plugin, "create_runner", tracked_create_runner)
    monkeypatch.chdir(project_root)

    try:
        with contextlib.ExitStack() as stack:
            queues = [
                stack.enter_context(Queue(name, persistent=True))
                for name in ("alpha", "beta", "gamma")
            ]
            for index, queue in enumerate(queues):
                queue.write(f"message-{index}")
            for index, queue in enumerate(queues):
                assert queue.read() == f"message-{index}"
    finally:
        get_backend_plugin().cleanup_target(
            dsn,
            backend_options={"schema": schema},
        )

    # The phase-lock owner uses one short-lived bootstrap runner. The three
    # persistent handles then share exactly one process-session runner.
    assert create_runner_calls == 2
    assert len(set(runner_ids)) == 2


def test_broker_session_queues_and_connection_share_one_postgres_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = _require_test_dsn()
    schema = _schema_name("broker_session")
    target = BrokerTarget("postgres", dsn, {"schema": schema})
    plugin = get_backend_plugin()
    original_create_runner = plugin.create_runner
    create_runner_calls = 0
    created_runners: list[PostgresRunner] = []

    def tracked_create_runner(
        target_value: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Config | None = None,
    ) -> SQLRunner:
        nonlocal create_runner_calls
        create_runner_calls += 1
        runner = cast(
            PostgresRunner,
            original_create_runner(
                target_value,
                backend_options=backend_options,
                config=config,
            ),
        )
        created_runners.append(runner)
        return runner

    monkeypatch.setattr(plugin, "create_runner", tracked_create_runner)
    try:
        with BrokerSession.connect(target) as session:
            session.queue("first").write("one")
            session.queue("second").write("two")
            with session.connection() as connection:
                assert sorted(connection.list_queues()) == ["first", "second"]
            assert len(created_runners) == 1
            assert created_runners[0]._lease_entries == {}
            assert not hasattr(created_runners[0]._thread_local, "conn")
    finally:
        plugin.cleanup_target(dsn, backend_options={"schema": schema})

    # The explicit target needs no project bootstrap runner. Both queues and
    # the temporary connection share one process-session runner.
    assert create_runner_calls == 1


def test_broker_session_threads_progress_while_claim_iterator_is_suspended() -> None:
    dsn = _require_test_dsn()
    schema = _schema_name("thread_progress")
    target = BrokerTarget("postgres", dsn, {"schema": schema})
    claim_open = threading.Event()
    release_claim = threading.Event()
    write_finished = threading.Event()
    errors: list[BaseException] = []

    try:
        with BrokerSession.connect(target) as session:
            seed = session.queue("source")
            seed.write("payload")
            seed.close()

            def hold_claim() -> None:
                queue = session.queue("source")
                iterator = queue.read_generator(
                    delivery_guarantee="at_least_once",
                )
                try:
                    assert next(iterator) == "payload"
                    claim_open.set()
                    assert release_claim.wait(timeout=5.0)
                except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
                    errors.append(exc)
                finally:
                    iterator.close()
                    queue.close()

            def write_other_queue() -> None:
                try:
                    assert claim_open.wait(timeout=5.0)
                    queue = session.queue("other")
                    try:
                        queue.write("independent")
                    finally:
                        queue.close()
                except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
                    errors.append(exc)
                finally:
                    write_finished.set()

            claim_thread = threading.Thread(target=hold_claim)
            writer_thread = threading.Thread(target=write_other_queue)
            claim_thread.start()
            writer_thread.start()
            try:
                assert claim_open.wait(timeout=5.0)
                assert write_finished.wait(timeout=5.0)
                assert errors == []
            finally:
                release_claim.set()
                claim_thread.join(timeout=5.0)
                writer_thread.join(timeout=5.0)
            assert not claim_thread.is_alive()
            assert not writer_thread.is_alive()
            assert session.queue("other").read() == "independent"
    finally:
        get_backend_plugin().cleanup_target(dsn, backend_options={"schema": schema})


def test_suspended_claim_iterator_retains_then_returns_pool_checkout() -> None:
    dsn = _require_test_dsn()
    schema = _schema_name("iterator_checkout")
    target = BrokerTarget("postgres", dsn, {"schema": schema})
    queue = Queue("source", db_path=target, persistent=True)
    iterator = None

    try:
        queue.write("payload")
        assert queue.conn is not None
        core = cast(BrokerCore, queue.conn.get_core())
        runner = cast(PostgresRunner, core._runner)
        assert not hasattr(runner._thread_local, "conn")

        iterator = queue.read_generator(delivery_guarantee="at_least_once")
        assert next(iterator) == "payload"

        assert runner._thread_local.in_transaction is True
        assert hasattr(runner._thread_local, "conn")
        suspended_stats = runner._pool.get_stats()
        assert suspended_stats["pool_available"] == suspended_stats["pool_size"] - 1

        iterator.close()
        iterator = None

        assert not hasattr(runner._thread_local, "conn")
        settled_stats = runner._pool.get_stats()
        assert settled_stats["pool_available"] == settled_stats["pool_size"]
    finally:
        if iterator is not None:
            iterator.close()
        queue.close()
        get_backend_plugin().cleanup_target(dsn, backend_options={"schema": schema})


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
        assert stdout == ""
        assert "Initialized SimpleBroker target:" in stderr
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

    code, _stdout, stderr = _run_cli("init", cwd=project_root, env=env)
    assert code != 0
    assert "not available" not in stderr
    assert "Could not connect to Postgres target" in stderr
