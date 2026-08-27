"""Firing tests for the advanced pooled-async SQLite example."""

import asyncio
import sqlite3
from contextlib import closing
from pathlib import Path

from async_pooled_broker import AsyncQueue, async_broker

from simplebroker import Queue
from simplebroker._constants import SIMPLEBROKER_MAGIC


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


def test_async_example_documents_only_canonical_sync_modes() -> None:
    readme = (Path(__file__).parent / "ASYNC_README.md").read_text(encoding="utf-8")

    assert "Sync mode: FULL, NORMAL, or OFF" in readme
    assert "EXTRA" not in readme
