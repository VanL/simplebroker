"""Frozen legacy SQLite layouts for migration and admission tests.

These embed the literal DDL of past schema eras. They intentionally do not
import current schema constants or builders: their value is staying fixed
while current code moves to the next version. When a new schema version
lands, freeze its predecessor here rather than re-inlining DDL per test.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Sequence
from contextlib import closing
from pathlib import Path

from simplebroker._constants import SIMPLEBROKER_MAGIC

# Era: schema v1 (no claimed column, no unique timestamp constraint).
SQLITE_V1_LAYOUT_DDL = """
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue TEXT NOT NULL,
    body TEXT NOT NULL,
    ts INTEGER NOT NULL
);
CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""

# Era: schema v5 messages table (private surrogate key, unique timestamp).
SQLITE_V5_MESSAGES_TABLE_DDL = """
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue TEXT NOT NULL,
    body TEXT NOT NULL,
    ts INTEGER NOT NULL UNIQUE,
    claimed INTEGER DEFAULT 0
);
"""

# Era: complete broker-owned schema v5 layout (tables plus owned indexes).
SQLITE_V5_BROKER_DDL = (
    SQLITE_V5_MESSAGES_TABLE_DDL
    + """
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
"""
)


def create_sqlite_v1_layout(
    db_path: Path | str,
    *,
    last_ts: int = 0,
    messages: Iterable[tuple[str, str, int]] = (),
) -> None:
    """Create a pre-versioned (v1-era) database file."""
    with closing(sqlite3.connect(db_path)) as conn:
        conn.executescript(SQLITE_V1_LAYOUT_DDL)
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('last_ts', ?)",
            (last_ts,),
        )
        conn.executemany(
            "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
            list(messages),
        )
        conn.commit()


def create_sqlite_v5_layout(
    db_path: Path | str,
    *,
    last_ts: int = 300,
    message_rows: Sequence[tuple[str, str, int, int]] = (),
) -> None:
    """Create a literal last-release (v5) broker layout with v5 metadata."""
    with closing(sqlite3.connect(db_path)) as conn:
        conn.executescript(SQLITE_V5_BROKER_DDL)
        conn.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            [
                ("last_ts", last_ts),
                ("magic", SIMPLEBROKER_MAGIC),
                ("schema_version", 5),
                ("alias_version", 0),
            ],
        )
        conn.executemany(
            "INSERT INTO messages (queue, body, ts, claimed) VALUES (?, ?, ?, ?)",
            list(message_rows),
        )
        conn.commit()
