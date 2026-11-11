"""Reject schema spellings that PostgreSQL would silently map to another target.

The pure entrance matrix proves rejection before resource acquisition. Real
storage and notification controls separately prove the 63-byte identity boundary.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from simplebroker_pg import PostgresRunner, get_backend_plugin
from simplebroker_pg import runner as pg_runner_module
from simplebroker_pg import validation as pg_validation
from simplebroker_pg.plugin import PostgresBackendPlugin, verify_env
from simplebroker_pg.runner import (
    PostgresActivityWaiter,
    PostgresMultiQueueActivityWaiter,
)
from simplebroker_pg.validation import inspect_schema, quote_ident, require_schema_name

from simplebroker import BrokerTarget, Queue, resolve_config
from simplebroker._exceptions import DatabaseError
from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


@pytest.mark.parametrize("length", [62, 63])
def test_schema_boundary_preserves_valid_spelling(length: int) -> None:
    schema = "Tenant_" + "A" * (length - 7)
    assert require_schema_name({"schema": schema}) == schema
    # The non-public rule belongs to schemas, not arbitrary SQL identifiers.
    assert quote_ident("public") == '"public"'


_ENTRANCES = (
    "require",
    "verify",
    "init",
    "create_runner",
    "initialize",
    "validate",
    "inspect",
    "cleanup",
    "runner",
    "waiter",
    "multi_waiter",
    "plugin_waiter",
    "plugin_multi_waiter",
    "structural_core",
    "structural_waiter",
    "structural_multi_waiter",
    "lazy_queue_waiter",
)


@pytest.mark.parametrize("entrance", _ENTRANCES)
@pytest.mark.parametrize("schema", ["T" * 64, "public"], ids=["overlong", "public"])
def test_invalid_schema_rejected_before_resource_acquisition(
    monkeypatch: pytest.MonkeyPatch, entrance: str, schema: str
) -> None:
    """Every public route must reject before storage, attribution, or listener use.

    These spies stand outside schema validation. They make accidental pool,
    listener, or connection construction fail even without a reachable server.
    """
    dsn = "postgresql://audit@127.0.0.1:1/unreachable"
    plugin = PostgresBackendPlugin()
    options = {"schema": schema}
    config = resolve_config()
    stop = threading.Event()

    def forbidden(*args: object, **kwargs: object) -> Any:
        raise AssertionError("invalid schema reached backend resource acquisition")

    structural: Any = SimpleNamespace(
        dsn=dsn,
        schema=schema,
        setup=lambda _phase: None,
        begin_immediate=forbidden,
        run=forbidden,
    )

    monkeypatch.setattr(pg_validation, "connect", forbidden)
    monkeypatch.setattr(pg_runner_module, "ConnectionPool", forbidden)
    monkeypatch.setattr(pg_runner_module._activity_registry, "acquire", forbidden)
    monkeypatch.setattr(pg_runner_module._owner, "capture", forbidden)
    queue = Queue("jobs", db_path=BrokerTarget("postgres", dsn, options))
    calls: dict[str, Callable[[], object]] = {
        "require": lambda: require_schema_name(options),
        "verify": lambda: verify_env(config, toml_target=dsn, toml_options=options),
        "init": lambda: plugin.init_backend(
            config, toml_target=dsn, toml_options=options
        ),
        "create_runner": lambda: plugin.create_runner(dsn, backend_options=options),
        "initialize": lambda: plugin.initialize_target(dsn, backend_options=options),
        "validate": lambda: plugin.validate_target(dsn, backend_options=options),
        "inspect": lambda: inspect_schema(dsn, backend_options=options),
        "cleanup": lambda: plugin.cleanup_target(dsn, backend_options=options),
        "runner": lambda: PostgresRunner(dsn, schema=schema),
        "waiter": lambda: PostgresActivityWaiter(
            dsn, schema=schema, queue_name="jobs", stop_event=stop
        ),
        "multi_waiter": lambda: PostgresMultiQueueActivityWaiter(
            dsn, schema=schema, queue_names=("jobs",), stop_event=stop
        ),
        "plugin_waiter": lambda: plugin.create_activity_waiter(
            target=dsn, backend_options=options, queue_name="jobs", stop_event=stop
        ),
        "plugin_multi_waiter": lambda: plugin.create_activity_waiter_for_queues(
            target=dsn, backend_options=options, queue_names=("jobs",), stop_event=stop
        ),
        "structural_core": lambda: BrokerCore(
            structural, backend_plugin=get_backend_plugin()
        ),
        "structural_waiter": lambda: plugin.create_activity_waiter(
            target=None, runner=structural, queue_name="jobs", stop_event=stop
        ),
        "structural_multi_waiter": lambda: plugin.create_activity_waiter_for_queues(
            target=None, runner=structural, queue_names=("jobs",), stop_event=stop
        ),
        "lazy_queue_waiter": lambda: queue.create_activity_waiter(stop_event=stop),
    }
    try:
        with pytest.raises(
            DatabaseError, match="public" if schema == "public" else "63"
        ):
            calls[entrance]()
    finally:
        queue.close()


@pytest.mark.parametrize("operation", ["read", "write", "cleanup"])
@pytest.mark.parametrize("suffix", ["A", "B"])
def test_overlong_schema_cannot_access_or_delete_prefix_target(
    pg_dsn: str, pg_schema: str, operation: str, suffix: str
) -> None:
    """Real PG truncation must never let a rejected name touch the valid target."""
    schema = pg_schema.ljust(63, "x")
    plugin = PostgresBackendPlugin()
    with Queue(
        "jobs", db_path=BrokerTarget("postgres", pg_dsn, {"schema": schema})
    ) as q:
        q.write("private-prefix-message")
        try:
            long_options = {"schema": schema + suffix}
            if operation == "cleanup":
                with pytest.raises(DatabaseError, match="63"):
                    plugin.cleanup_target(pg_dsn, backend_options=long_options)
            else:
                with Queue(
                    "jobs", db_path=BrokerTarget("postgres", pg_dsn, long_options)
                ) as other:
                    # Queue acquisition preserves its existing retry/error wrapper.
                    with pytest.raises(RuntimeError, match="63") as failure:
                        if operation == "read":
                            other.peek()
                        else:
                            other.write("wrong-target-message")
                    assert isinstance(failure.value.__cause__, DatabaseError)
            assert list(q.peek_generator()) == ["private-prefix-message"]
        finally:
            q.close()
            plugin.cleanup_target(pg_dsn, backend_options={"schema": schema})


def test_distinct_63_byte_schemas_keep_messages_and_notifications_isolated(
    pg_dsn: str, pg_schema: str
) -> None:
    schemas = [pg_schema.ljust(62, "x") + suffix for suffix in ("A", "B")]
    plugin = PostgresBackendPlugin()
    queues = [
        Queue("jobs", db_path=BrokerTarget("postgres", pg_dsn, {"schema": schema}))
        for schema in schemas
    ]
    waiters = []
    try:
        for q in queues:
            q.stats()
            waiter = q.create_activity_waiter(stop_event=threading.Event())
            assert waiter is not None
            waiters.append(waiter)
        queues[0].write("tenant-A")
        assert waiters[0].wait(5.0)
        assert queues[1].peek() is None
        # Notification smoke check; the durable row assertions own isolation.
        assert not waiters[1].wait(0.1)
        queues[1].write("tenant-B")
        assert waiters[1].wait(5.0)
        assert queues[0].peek() == "tenant-A"
        assert queues[1].peek() == "tenant-B"
    finally:
        for waiter in waiters:
            waiter.close()
        for q in queues:
            q.close()
        for schema in schemas:
            plugin.cleanup_target(pg_dsn, backend_options={"schema": schema})
