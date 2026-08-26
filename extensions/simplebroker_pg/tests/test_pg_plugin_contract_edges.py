"""Focused Postgres plugin contracts that do not require a live server."""

from __future__ import annotations

import threading
from collections.abc import Iterable
from typing import Any

import pytest
import simplebroker_pg.plugin as pg_plugin_module
import simplebroker_pg.validation as pg_validation
from simplebroker_pg.plugin import PostgresBackendPlugin
from simplebroker_pg.runner import RunnerMetaState
from simplebroker_pg.validation import SchemaInspection, SchemaState

import simplebroker.db as db_module
from simplebroker._exceptions import DatabaseError
from simplebroker._runner import SetupPhase

pytestmark = [pytest.mark.pg_only]


class RecordingRunner:
    schema = "broker_data"

    def __init__(self, responses: Iterable[list[tuple[Any, ...]]] = ()) -> None:
        self._responses = iter(responses)
        self.calls: list[tuple[str, tuple[Any, ...], bool]] = []

    def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> list[tuple[Any, ...]]:
        self.calls.append((sql, params, fetch))
        return next(self._responses, [])

    def begin_immediate(self) -> None:
        return None

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None

    def setup(self, phase: SetupPhase) -> None:
        del phase

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        del phase
        return True


class CachingRunner(RecordingRunner):
    def __init__(self) -> None:
        super().__init__()
        self.state: RunnerMetaState | None = None
        self.updates: list[dict[str, object]] = []

    def get_meta_cache(self) -> RunnerMetaState | None:
        return self.state

    def prime_meta_cache(self, state: RunnerMetaState) -> None:
        self.state = state

    def update_meta_cache(self, **kwargs: object) -> None:
        self.updates.append(kwargs)


@pytest.mark.parametrize(
    ("call", "match"),
    [
        (lambda: pg_plugin_module._require_text(7, name="host"), "non-empty string"),
        (lambda: pg_plugin_module._require_port(" "), "integer between"),
        (lambda: pg_plugin_module._require_port(object()), "integer between"),
    ],
)
def test_plugin_scalar_validation_rejects_wrong_types_and_empty_values(
    call: Any,
    match: str,
) -> None:
    with pytest.raises(DatabaseError, match=match):
        call()

    assert pg_plugin_module._optional_text(None, name="target") == ""


def test_meta_helpers_support_optional_cache_protocol() -> None:
    runner = RecordingRunner([[], [], [], [], []])

    assert pg_plugin_module._cached_meta(runner) is None
    pg_plugin_module._prime_meta(runner, RunnerMetaState("magic", 5, 1, 2))
    pg_plugin_module._update_meta(runner, last_ts=10)
    assert pg_plugin_module._load_meta_state(runner) is None
    assert pg_plugin_module._load_live_meta_state(runner) is None

    plugin = PostgresBackendPlugin()
    assert plugin.read_magic(runner) is None
    assert plugin.select_meta_items(runner) == []
    plugin.write_schema_version(runner, 4)

    caching_runner = CachingRunner()
    state = RunnerMetaState("magic", 5, 1, 2)
    pg_plugin_module._prime_meta(caching_runner, state)
    pg_plugin_module._update_meta(caching_runner, schema_version=4)

    assert pg_plugin_module._cached_meta(caching_runner) is state
    assert caching_runner.updates == [
        {
            "magic": None,
            "schema_version": 4,
            "last_ts": None,
            "alias_version": None,
        }
    ]


def test_initialize_target_closes_runner_when_core_construction_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ShutdownRunner:
        def __init__(self) -> None:
            self.shutdown_calls = 0

        def shutdown(self) -> None:
            self.shutdown_calls += 1

    class CloseRunner:
        def __init__(self) -> None:
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

    def fail_core(*args: object, **kwargs: object) -> None:
        raise RuntimeError("core construction failed")

    monkeypatch.setattr(
        pg_plugin_module,
        "inspect_schema",
        lambda *args, **kwargs: SchemaInspection(
            schema="broker_data",
            state=SchemaState.ABSENT,
            objects=frozenset(),
        ),
    )
    monkeypatch.setattr(db_module, "BrokerCore", fail_core)
    plugin = PostgresBackendPlugin()

    shutdown_runner = ShutdownRunner()
    monkeypatch.setattr(
        plugin, "create_runner", lambda *args, **kwargs: shutdown_runner
    )
    with pytest.raises(RuntimeError, match="core construction failed"):
        plugin.initialize_target(
            "postgresql://example/test",
            backend_options={"schema": "broker_data"},
        )
    assert shutdown_runner.shutdown_calls == 1

    close_runner = CloseRunner()
    monkeypatch.setattr(plugin, "create_runner", lambda *args, **kwargs: close_runner)
    with pytest.raises(RuntimeError, match="core construction failed"):
        plugin.initialize_target(
            "postgresql://example/test",
            backend_options={"schema": "broker_data"},
        )
    assert close_runner.close_calls == 1


def test_initialize_target_passes_one_config_snapshot_to_runner_and_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from simplebroker import snapshot_config

    class Runner:
        pass

    marker = snapshot_config({"EXTENSION_RECEIPT": "kept"})
    runner = Runner()
    runner_config: list[object] = []
    core_config: list[object] = []

    monkeypatch.setattr(
        pg_plugin_module,
        "inspect_schema",
        lambda *args, **kwargs: SchemaInspection(
            schema="broker_data",
            state=SchemaState.ABSENT,
            objects=frozenset(),
        ),
    )
    plugin = PostgresBackendPlugin()

    def create_runner(*args: object, **kwargs: object) -> object:
        runner_config.append(kwargs.get("config"))
        return runner

    class Core:
        def __init__(self, *args: object, **kwargs: object) -> None:
            core_config.append(kwargs.get("config"))

        def shutdown(self) -> None:
            return None

    monkeypatch.setattr(plugin, "create_runner", create_runner)
    monkeypatch.setattr(db_module, "BrokerCore", Core)

    plugin.initialize_target(
        "postgresql://example/test",
        backend_options={"schema": "broker_data"},
        config=marker,
    )

    assert runner_config == [marker]
    assert core_config == [marker]


@pytest.mark.parametrize("state", [SchemaState.EMPTY, SchemaState.OWNED])
def test_initialize_target_admits_empty_and_current_owned_schema(
    monkeypatch: pytest.MonkeyPatch,
    state: SchemaState,
) -> None:
    """EMPTY initializes and current OWNED initialization remains idempotent."""
    shutdowns: list[str] = []

    class Core:
        def __init__(self, *args: object, **kwargs: object) -> None:
            return None

        def shutdown(self) -> None:
            shutdowns.append("core")

    monkeypatch.setattr(
        pg_plugin_module,
        "inspect_schema",
        lambda *args, **kwargs: SchemaInspection(
            schema="broker_data",
            state=state,
            objects=(
                frozenset(pg_validation.REQUIRED_TABLES)
                if state is SchemaState.OWNED
                else frozenset()
            ),
            schema_version=(
                PostgresBackendPlugin.schema_version
                if state is SchemaState.OWNED
                else None
            ),
            current_shape_ready=state is SchemaState.OWNED,
        ),
    )
    monkeypatch.setattr(db_module, "BrokerCore", Core)
    plugin = PostgresBackendPlugin()
    monkeypatch.setattr(plugin, "create_runner", lambda *args, **kwargs: object())

    plugin.initialize_target(
        "postgresql://example/test",
        backend_options={"schema": "broker_data"},
    )
    assert shutdowns == ["core"]


def test_initialize_target_rejects_current_incomplete_owned_schema_before_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        pg_plugin_module,
        "inspect_schema",
        lambda *args, **kwargs: SchemaInspection(
            schema="broker_data",
            state=SchemaState.OWNED,
            objects=frozenset({"meta", "messages"}),
            schema_version=PostgresBackendPlugin.schema_version,
            current_shape_ready=False,
        ),
    )
    plugin = PostgresBackendPlugin()

    def unexpected_runner(*args: object, **kwargs: object) -> object:
        raise AssertionError("runner creation must follow readiness admission")

    monkeypatch.setattr(plugin, "create_runner", unexpected_runner)

    with pytest.raises(DatabaseError, match="current schema shape is incomplete"):
        plugin.initialize_target(
            "postgresql://example/test",
            backend_options={"schema": "broker_data"},
        )


def test_prepare_queue_operation_locks_unknown_operation_by_queue() -> None:
    runner = RecordingRunner()

    PostgresBackendPlugin().prepare_queue_operation(
        runner,
        operation="delete",
        queue="jobs",
    )

    assert runner.calls[0][0] == "SELECT pg_advisory_xact_lock(?)"
    assert isinstance(runner.calls[0][1][0], int)


def test_vacuum_compacts_after_deleting_claimed_batches() -> None:  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-022] exception
    class VacuumRunner(RecordingRunner):
        def __init__(self) -> None:
            super().__init__()
            self.deleted_counts = iter([1, 0])
            self.events: list[str] = []

        def lease_thread_connection(self) -> None:
            self.events.append("lease")

        def release_thread_connection(self) -> None:
            self.events.append("release")

        def begin_immediate(self) -> None:
            self.events.append("begin")

        def commit(self) -> None:
            self.events.append("commit")

        def rollback(self) -> None:
            self.events.append("rollback")

        def run(
            self,
            sql: str,
            params: tuple[Any, ...] = (),
            *,
            fetch: bool = False,
        ) -> list[tuple[Any, ...]]:
            self.calls.append((sql, params, fetch))
            normalized = " ".join(sql.split())
            if "pg_try_advisory_lock" in normalized:
                return [(True,)]
            if "SELECT COUNT(*) FROM deleted" in normalized:
                return [(next(self.deleted_counts),)]
            if "pg_advisory_unlock" in normalized:
                return [(True,)]
            return []

    runner = VacuumRunner()

    PostgresBackendPlugin().vacuum(
        runner,
        compact=True,
        config={"BROKER_VACUUM_BATCH_SIZE": 100},
    )

    assert runner.events == ["lease", "begin", "commit", "begin", "rollback", "release"]
    compact_sql = [sql for sql, _, _ in runner.calls if sql.startswith("VACUUM")]
    assert len(compact_sql) == 3


def test_vacuum_lock_contention_needs_no_optional_runner_lease() -> None:
    runner = RecordingRunner([[]])

    PostgresBackendPlugin().vacuum(
        runner,
        compact=False,
        config={"BROKER_VACUUM_BATCH_SIZE": 100},
    )

    assert len(runner.calls) == 1


def test_activity_waiters_accept_structural_runner_and_explicit_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[tuple[str, str, tuple[str, ...]]] = []

    monkeypatch.setattr(
        pg_plugin_module,
        "PostgresActivityWaiter",
        lambda dsn, *, schema, queue_name, stop_event: created.append(
            (dsn, schema, (queue_name,))
        ),
    )
    monkeypatch.setattr(
        pg_plugin_module,
        "PostgresMultiQueueActivityWaiter",
        lambda dsn, *, schema, queue_names, stop_event: created.append(
            (dsn, schema, queue_names)
        ),
    )

    runner = type(
        "StructuralRunner",
        (),
        {"dsn": "postgresql://runner/test", "schema": "runner_schema"},
    )()
    plugin = PostgresBackendPlugin()
    stop_event = threading.Event()

    plugin.create_activity_waiter(
        target=None,
        runner=runner,
        queue_name="jobs",
        stop_event=stop_event,
    )
    plugin.create_activity_waiter_for_queues(
        target=None,
        runner=runner,
        queue_names=("jobs", "other"),
        stop_event=stop_event,
    )
    plugin.create_activity_waiter_for_queues(
        target="postgresql://target/test",
        backend_options={"schema": "target_schema"},
        queue_names=("jobs",),
        stop_event=stop_event,
    )

    assert created == [
        ("postgresql://runner/test", "runner_schema", ("jobs",)),
        ("postgresql://runner/test", "runner_schema", ("jobs", "other")),
        ("postgresql://target/test", "target_schema", ("jobs",)),
    ]
