"""Firing tests for the advanced pooled-async SQLite example."""

import asyncio
import sqlite3
from contextlib import closing
from pathlib import Path

from async_pooled_broker import AsyncQueue, async_broker


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


def test_async_example_ensures_complete_v5_schema_on_fresh_and_stamped_db(
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
        assert version == 5
        assert "idx_messages_pending_queue_ts" in indexes


def test_async_example_documents_only_canonical_sync_modes() -> None:
    readme = (Path(__file__).parent / "ASYNC_README.md").read_text(encoding="utf-8")

    assert "Sync mode: FULL, NORMAL, or OFF" in readme
    assert "EXTRA" not in readme
