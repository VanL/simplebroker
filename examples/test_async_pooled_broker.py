"""Firing tests for the advanced pooled-async SQLite example."""

import asyncio
import sqlite3
from collections.abc import AsyncGenerator, Mapping
from contextlib import AbstractContextManager, closing
from pathlib import Path
from typing import Any, cast

import aiosqlite
import async_pooled_broker as pooled_module
import pytest
from async_pooled_broker import AsyncQueue, PooledAsyncSQLiteRunner, async_broker

from simplebroker import Queue, open_broker
from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker.ext import BrokerConnection


def _initialize_through_public_example_api(db_path: Path) -> None:
    async def initialize() -> None:
        async with async_broker(str(db_path)) as broker:
            await AsyncQueue("schema_probe", broker).write("probe")

    asyncio.run(initialize())


def _schema_state(db_path: Path) -> tuple[int, set[str]]:
    with closing(sqlite3.connect(db_path)) as connection:
        version_row = connection.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()
        index_rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index'"
        ).fetchall()

    assert version_row is not None
    return int(version_row[0]), {str(row[0]) for row in index_rows}


def _owned_message_shape(
    db_path: Path,
) -> tuple[
    dict[str, tuple[str, int, object, int]],
    dict[str, tuple[int, int, tuple[str, ...]]],
]:
    """Return the supported message-table shape from SQLite's catalog."""

    with closing(sqlite3.connect(db_path)) as connection:
        columns = {
            str(row[1]): (str(row[2]).upper(), int(row[3]), row[4], int(row[5]))
            for row in connection.execute("PRAGMA table_info(messages)")
        }
        indexes: dict[str, tuple[int, int, tuple[str, ...]]] = {}
        for row in connection.execute("PRAGMA index_list(messages)"):
            name = str(row[1])
            if not name.startswith("idx_messages_"):
                continue
            index_columns = tuple(
                str(column[0])
                for column in connection.execute(
                    "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
                    (name,),
                )
            )
            indexes[name] = (int(row[2]), int(row[4]), index_columns)
    return columns, indexes


def _create_literal_v5_database(db_path: Path) -> None:
    """Create the previous release's table shape without current schema DDL."""

    with closing(sqlite3.connect(db_path)) as connection:
        connection.executescript(
            """
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                body TEXT NOT NULL,
                ts INTEGER NOT NULL UNIQUE,
                claimed INTEGER DEFAULT 0
            );
            CREATE TABLE meta (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
            CREATE TABLE queue_aliases (
                alias TEXT PRIMARY KEY,
                target TEXT NOT NULL
            );
            CREATE INDEX idx_queue_aliases_target ON queue_aliases(target);
            CREATE INDEX idx_messages_queue_ts_id ON messages(queue, ts, id);
            CREATE INDEX idx_messages_unclaimed
                ON messages(queue, claimed, id) WHERE claimed = 0;
            CREATE INDEX idx_messages_pending_queue_ts
                ON messages(queue, ts) WHERE claimed = 0;
            INSERT INTO messages (queue, body, ts, claimed)
                VALUES ('legacy', 'before migration', 100, 0);
            """
        )
        connection.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            (
                ("last_ts", 100),
                ("magic", SIMPLEBROKER_MAGIC),
                ("schema_version", 5),
                ("alias_version", 0),
            ),
        )
        connection.commit()


def _seed_sidecar(db_path: Path) -> None:
    with closing(sqlite3.connect(db_path)) as connection:
        connection.executescript(
            """
            CREATE TABLE task_monitor (
                task_key TEXT PRIMARY KEY,
                state TEXT NOT NULL
            );
            CREATE INDEX task_monitor_state_idx ON task_monitor(state);
            INSERT INTO task_monitor (task_key, state) VALUES ('task-1', 'ready');
            """
        )
        connection.commit()


def _sidecar_snapshot(
    db_path: Path,
) -> tuple[
    tuple[tuple[str, str], ...],
    tuple[str, ...],
    tuple[tuple[str, str], ...],
]:
    with closing(sqlite3.connect(db_path)) as connection:
        definitions = connection.execute(
            "SELECT name, sql FROM sqlite_master "
            "WHERE name IN ('task_monitor', 'task_monitor_state_idx') "
            "ORDER BY name"
        ).fetchall()
        rows = connection.execute(
            "SELECT task_key, state FROM task_monitor ORDER BY task_key"
        ).fetchall()
        indexed_columns = connection.execute(
            "SELECT name FROM pragma_index_info('task_monitor_state_idx') "
            "ORDER BY seqno"
        ).fetchall()
    return (
        tuple((str(name), str(sql)) for name, sql in definitions),
        tuple(str(name) for (name,) in indexed_columns),
        tuple((str(key), str(state)) for key, state in rows),
    )


def test_async_example_ensures_complete_v6_schema_on_fresh_and_stamped_db(
    tmp_path: Path,
) -> None:
    fresh_db = tmp_path / "fresh.db"
    _initialize_through_public_example_api(fresh_db)

    stamped_db = tmp_path / "stamped.db"
    _initialize_through_public_example_api(stamped_db)
    with closing(sqlite3.connect(stamped_db)) as connection:
        connection.execute("DROP INDEX IF EXISTS idx_messages_pending_queue_ts")
        connection.commit()

    _initialize_through_public_example_api(stamped_db)

    for db_path in (fresh_db, stamped_db):
        version, indexes = _schema_state(db_path)
        assert version == 6
        assert "idx_messages_pending_queue_ts" in indexes


def test_async_example_migrates_literal_v5_to_canonical_sync_v6(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "literal-v5.db"
    fresh_sync_path = tmp_path / "fresh-sync.db"
    _create_literal_v5_database(db_path)

    _initialize_through_public_example_api(db_path)

    fresh_sync_queue = Queue("expected", db_path=str(fresh_sync_path))
    try:
        fresh_sync_queue.write("initialize canonical schema")
    finally:
        fresh_sync_queue.close()
    assert _owned_message_shape(db_path) == _owned_message_shape(fresh_sync_path)

    queue = Queue("legacy", db_path=str(db_path))
    try:
        assert queue.peek_one(exact_timestamp=100) == "before migration"
    finally:
        queue.close()


def test_async_context_uses_canonical_setup_and_preserves_sidecar(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "literal-v5-sidecar.db"
    _create_literal_v5_database(db_path)
    _seed_sidecar(db_path)
    expected_sidecar = _sidecar_snapshot(db_path)

    setup_configs: list[object] = []
    runtime_configs: list[object] = []
    real_open_broker = open_broker

    def traced_open_broker(
        target: str,
        *,
        config: Mapping[str, Any],
    ) -> AbstractContextManager[BrokerConnection]:
        setup_configs.append(config)
        return real_open_broker(target, config=config)

    class TracedRunner(pooled_module.PooledAsyncSQLiteRunner):
        def __init__(
            self,
            *args: Any,
            config: Mapping[str, Any],
            **kwargs: Any,
        ) -> None:
            runtime_configs.append(config)
            super().__init__(*args, config=config, **kwargs)

    monkeypatch.setattr(
        pooled_module,
        "open_broker",
        traced_open_broker,
        raising=False,
    )
    monkeypatch.setattr(pooled_module, "PooledAsyncSQLiteRunner", TracedRunner)

    async def open_twice() -> None:
        for _ in range(2):
            async with async_broker(
                str(db_path),
                config={"BROKER_BUSY_TIMEOUT": 4321},
            ):
                pass

    asyncio.run(open_twice())

    assert len(setup_configs) == 2
    assert len(runtime_configs) == 2
    assert all(
        setup_config is runtime_config
        for setup_config, runtime_config in zip(
            setup_configs,
            runtime_configs,
            strict=True,
        )
    )
    assert _sidecar_snapshot(db_path) == expected_sidecar
    assert _schema_state(db_path)[0] == 6


def test_async_setup_failure_does_not_construct_pool(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "future-schema.db"
    queue = Queue("jobs", db_path=str(db_path))
    try:
        queue.write("initialize")
    finally:
        queue.close()
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute("UPDATE meta SET value = 999 WHERE key = 'schema_version'")
        connection.commit()

    pool_constructed = False

    class PoolMustNotBeConstructed:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            nonlocal pool_constructed
            pool_constructed = True

    monkeypatch.setattr(
        pooled_module,
        "SQLiteConnectionPool",
        PoolMustNotBeConstructed,
    )

    async def open_incompatible_target() -> None:
        async with async_broker(str(db_path)):
            pass

    with pytest.raises(RuntimeError, match="newer than supported"):
        asyncio.run(open_incompatible_target())
    assert not pool_constructed


def test_generated_ids_follow_commit_order_under_forced_interleaving(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "commit-order.db"

    async def exercise() -> tuple[list[str], list[str]]:
        async with async_broker(str(db_path), max_connections=2) as broker:
            await broker._ensure_initialized()
            real_execute = broker._execute_with_retry
            first_write_reached_boundary = asyncio.Event()
            release_first_write = asyncio.Event()
            first_task: asyncio.Task[None] | None = None

            async def barrier_execute(
                operation: Any,
                **kwargs: Any,
            ) -> Any:
                if asyncio.current_task() is first_task:
                    first_write_reached_boundary.set()
                    await release_first_write.wait()
                return await real_execute(operation, **kwargs)

            monkeypatch.setattr(broker, "_execute_with_retry", barrier_execute)
            completion_order: list[str] = []

            async def write(label: str) -> None:
                await broker.write("jobs", label)
                completion_order.append(label)

            first_task = asyncio.create_task(write("first-started"))
            await first_write_reached_boundary.wait()
            second_task = asyncio.create_task(write("second-started"))
            await second_task
            release_first_write.set()
            await first_task

        queue = Queue("jobs", db_path=str(db_path))
        try:
            public_id_order = queue.peek_many(10)
        finally:
            queue.close()
        return completion_order, public_id_order

    completion_order, public_id_order = asyncio.run(exercise())
    assert completion_order == ["second-started", "first-started"]
    assert public_id_order == completion_order


def test_claim_batch_normalizes_reversed_opaque_runner_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "row-order.db"

    async def exercise() -> list[str]:
        async with async_broker(str(db_path), max_connections=2) as broker:
            queue = AsyncQueue("jobs", broker)
            await queue.write("one")
            await queue.write("two")
            await queue.write("three")

            real_run = broker._runner.run

            async def reversing_run(
                sql: str,
                params: tuple[Any, ...] = (),
                *,
                fetch: bool = False,
            ) -> list[tuple[Any, ...]]:
                records = await real_run(sql, params, fetch=fetch)
                return list(reversed(records)) if len(records) > 1 else records

            monkeypatch.setattr(broker._runner, "run", reversing_run)
            return [message async for message in queue.stream(commit_interval=3)]

    assert asyncio.run(exercise()) == ["one", "two", "three"]


def test_open_batch_rejects_reentrant_delete_and_rolls_back(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "reentrant-batch.db"

    async def exercise() -> tuple[str, bool, list[tuple[str, int, int]]]:
        async with async_broker(str(db_path), max_connections=2) as broker:
            source = AsyncQueue("source", broker)
            unrelated = AsyncQueue("unrelated", broker)
            for body in ("one", "two", "three"):
                await source.write(body)
            await unrelated.write("keep")

            stream = cast(
                AsyncGenerator[str, None],
                source.stream(commit_interval=3),
            )
            first = await anext(stream)
            rejected = False
            try:
                await broker.delete("unrelated")
            except RuntimeError as exc:
                rejected = "batch stream" in str(exc)
            finally:
                await stream.aclose()

            return first, rejected, await broker.get_queue_stats()

    first, rejected, stats = asyncio.run(exercise())
    by_queue = {queue: (pending, total) for queue, pending, total in stats}
    assert first == "one"
    assert rejected
    assert by_queue == {"source": (3, 3), "unrelated": (1, 1)}


def test_cross_task_batch_close_clears_parent_and_inherited_child_state(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "batch-child-context.db"

    async def exercise() -> tuple[
        list[tuple[str, int, int]],
        list[tuple[str, int, int]],
    ]:
        async with async_broker(str(db_path), max_connections=2) as broker:
            source = AsyncQueue("source", broker)
            for body in ("one", "two", "three"):
                await source.write(body)

            stream = cast(
                AsyncGenerator[str, None],
                source.stream(commit_interval=3),
            )
            assert await anext(stream) == "one"

            release_child = asyncio.Event()

            async def inspect_after_close() -> list[tuple[str, int, int]]:
                await release_child.wait()
                return await broker.get_queue_stats()

            child = asyncio.create_task(inspect_after_close())
            await asyncio.create_task(stream.aclose())
            parent_stats = await broker.get_queue_stats()
            release_child.set()
            return parent_stats, await child

    parent_stats, child_stats = asyncio.run(exercise())
    for stats in (parent_stats, child_stats):
        assert {queue: (pending, total) for queue, pending, total in stats} == {
            "source": (3, 3)
        }


def test_task_created_before_batch_can_close_it_and_restore_messages(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "preexisting-closer.db"

    async def exercise() -> list[tuple[str, int, int]]:
        async with async_broker(str(db_path), max_connections=2) as broker:
            source = AsyncQueue("source", broker)
            for body in ("one", "two", "three"):
                await source.write(body)

            stream = cast(
                AsyncGenerator[str, None],
                source.stream(commit_interval=3),
            )
            close_batch = asyncio.Event()

            async def close_later() -> None:
                await close_batch.wait()
                await stream.aclose()

            closer = asyncio.create_task(close_later())
            assert await anext(stream) == "one"
            close_batch.set()
            await closer
            return await broker.get_queue_stats()

    stats = asyncio.run(exercise())
    assert {queue: (pending, total) for queue, pending, total in stats} == {
        "source": (3, 3)
    }


def test_cancelled_begin_releases_connection_and_allows_next_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "cancelled-begin.db"
    original_execute = aiosqlite.Connection.execute

    async def exercise() -> None:
        runner = PooledAsyncSQLiteRunner(str(db_path), max_connections=1)
        begin_started = asyncio.Event()
        never_finish = asyncio.Event()

        async def interrupt_begin(
            connection: aiosqlite.Connection,
            sql: str,
            parameters: Any = None,
        ) -> Any:
            if sql == "BEGIN IMMEDIATE":
                begin_started.set()
                await never_finish.wait()
            if parameters is None:
                return await original_execute(connection, sql)
            return await original_execute(connection, sql, parameters)

        monkeypatch.setattr(aiosqlite.Connection, "execute", interrupt_begin)
        interrupted = asyncio.create_task(runner.begin_immediate())
        await begin_started.wait()
        interrupted.cancel()
        with pytest.raises(asyncio.CancelledError):
            await interrupted

        monkeypatch.setattr(aiosqlite.Connection, "execute", original_execute)
        await asyncio.wait_for(runner.begin_immediate(), timeout=2)
        await runner.rollback()
        await runner.close()

    asyncio.run(exercise())
