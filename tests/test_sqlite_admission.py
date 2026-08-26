"""SQLite ownership admission before any connection or schema setup writes."""

from __future__ import annotations

import os
import sqlite3
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from typing import Any, cast

import pytest

import simplebroker._backends.sqlite.plugin as sqlite_plugin_module
import simplebroker._phaselock as phaselock_module
import simplebroker.db as db_module
from simplebroker._constants import SCHEMA_VERSION, SIMPLEBROKER_MAGIC
from simplebroker._exceptions import DatabaseError
from simplebroker._runner import SQLiteRunner
from simplebroker.db import BrokerCore, BrokerDB


def _raw_database_state(db_path: Path) -> dict[str, bytes]:
    """Snapshot the database namespace without opening any SQLite file."""
    return {
        db_path.name: db_path.read_bytes(),
        **{
            candidate.name: candidate.read_bytes()
            for candidate in db_path.parent.glob(f"{db_path.name}*")
            if candidate != db_path and candidate.is_file()
        },
    }


def test_admission_retries_when_concurrent_bootstrap_creates_meta(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "concurrent-meta.db"
    runner = SQLiteRunner(str(db_path))
    connection = runner.get_connection()

    class CreateMetaAfterMissingTable:
        def __init__(self) -> None:
            self.injected = False

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
            if "LEFT JOIN meta" in sql and not self.injected:
                self.injected = True
                try:
                    return connection.execute(sql, params)
                except sqlite3.OperationalError:
                    with closing(sqlite3.connect(db_path)) as initializer:
                        initializer.execute(
                            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)"
                        )
                    raise
            return connection.execute(sql, params)

    proxy = CreateMetaAfterMissingTable()
    monkeypatch.setattr(runner, "get_connection", lambda: proxy)
    try:
        snapshot = db_module._read_explicit_sqlite_magic_before_setup(runner)
    finally:
        runner.close()

    assert proxy.injected is True
    assert snapshot.magic is None
    assert snapshot.stored_version is None
    assert snapshot.schema_cookie == 1


def test_broker_db_rejects_explicit_foreign_magic_before_any_setup_write(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "foreign.db"
    with closing(sqlite3.connect(db_path)) as connection:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        connection.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        connection.execute("CREATE TABLE foreign_records (value TEXT)")
        connection.execute("INSERT INTO meta VALUES ('magic', 'another-product')")
        connection.execute("INSERT INTO foreign_records VALUES ('keep-me')")
        connection.commit()

    before = _raw_database_state(db_path)
    assert set(before) == {db_path.name}

    with pytest.raises(RuntimeError, match="Database magic string mismatch"):
        BrokerDB(str(db_path))

    assert _raw_database_state(db_path) == before


def test_broker_db_rejects_foreign_magic_from_an_active_wal_before_setup(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "foreign-live.db"
    with closing(sqlite3.connect(db_path)) as connection:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        connection.execute("CREATE TABLE foreign_records (value TEXT)")
        connection.execute("INSERT INTO meta VALUES ('magic', 'another-product')")
        connection.execute("INSERT INTO foreign_records VALUES ('keep-me')")
        connection.commit()
        before_tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
        assert Path(f"{db_path}-wal").exists()
        assert Path(f"{db_path}-shm").exists()

        with pytest.raises(RuntimeError, match="Database magic string mismatch"):
            BrokerDB(str(db_path))

        assert not Path(f"{db_path}.lock").exists()
        assert (
            connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            ).fetchall()
            == before_tables
        )
        assert connection.execute(
            "SELECT value FROM meta WHERE key = 'magic'"
        ).fetchone() == ("another-product",)
        assert connection.execute("SELECT value FROM foreign_records").fetchone() == (
            "keep-me",
        )


def test_foreign_magic_is_checked_on_the_normal_runner_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "foreign.db"
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        connection.execute("INSERT INTO meta VALUES ('magic', 'another-product')")
        connection.commit()

    real_connect = cast(Callable[..., sqlite3.Connection], sqlite3.connect)
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def tracking_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        calls.append((args, kwargs))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", tracking_connect)

    with pytest.raises(RuntimeError, match="Database magic string mismatch"):
        BrokerDB(str(db_path))

    assert len(calls) == 1
    assert calls[0][0] == (str(db_path),)
    assert calls[0][1]["isolation_level"] is None
    assert "uri" not in calls[0][1]


def test_newer_owned_database_is_rejected_before_schema_setup_mutates_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "newer.db"
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
            CREATE TABLE meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            CREATE TABLE queue_aliases (
                alias TEXT PRIMARY KEY,
                target TEXT NOT NULL
            );
            CREATE INDEX idx_messages_queue_ts ON messages(queue, ts);
            """
        )
        connection.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            [
                ("magic", SIMPLEBROKER_MAGIC),
                ("schema_version", SCHEMA_VERSION + 1),
                ("last_ts", 0),
                ("alias_version", 0),
            ],
        )
        connection.execute(
            "INSERT INTO messages (queue, body, ts) VALUES ('q', 'keep', 7)"
        )
        connection.commit()

        before = {
            "objects": connection.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
            ).fetchall(),
            "meta": connection.execute(
                "SELECT key, value FROM meta ORDER BY key"
            ).fetchall(),
            "messages": connection.execute(
                "SELECT id, queue, body, ts, claimed FROM messages ORDER BY id"
            ).fetchall(),
        }

    real_connect = cast(Callable[..., sqlite3.Connection], sqlite3.connect)
    connections: list[tuple[tuple[object, ...], dict[str, object]]] = []
    setup_phases: list[object] = []

    def tracking_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        connections.append((args, kwargs))
        return real_connect(*args, **kwargs)

    def unexpected_setup(*args: object, **kwargs: object) -> None:
        setup_phases.append((args, kwargs))

    monkeypatch.setattr(sqlite3, "connect", tracking_connect)
    monkeypatch.setattr(SQLiteRunner, "setup_with_stop_event", unexpected_setup)

    with pytest.raises(RuntimeError, match="newer than supported"):
        BrokerDB(str(db_path))

    assert len(connections) == 1
    assert connections[0][0] == (str(db_path),)
    assert setup_phases == []

    with closing(sqlite3.connect(db_path)) as connection:
        after = {
            "objects": connection.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
            ).fetchall(),
            "meta": connection.execute(
                "SELECT key, value FROM meta ORDER BY key"
            ).fetchall(),
            "messages": connection.execute(
                "SELECT id, queue, body, ts, claimed FROM messages ORDER BY id"
            ).fetchall(),
        }

    assert after == before


@pytest.mark.parametrize(
    ("version_values", "error_type", "message"),
    [
        (["not-an-integer"], DatabaseError, "must contain an integer"),
        ([0], RuntimeError, "must be a positive integer"),
        ([SCHEMA_VERSION, SCHEMA_VERSION], RuntimeError, "is duplicated"),
    ],
)
def test_malformed_owned_version_is_rejected_before_setup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    version_values: list[object],
    error_type: type[Exception],
    message: str,
) -> None:
    db_path = tmp_path / "malformed-version.db"
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute("CREATE TABLE meta (key TEXT, value BLOB)")
        connection.execute(
            "INSERT INTO meta VALUES ('magic', ?)", (SIMPLEBROKER_MAGIC,)
        )
        connection.executemany(
            "INSERT INTO meta VALUES ('schema_version', ?)",
            [(value,) for value in version_values],
        )
        connection.execute("CREATE TABLE messages (body TEXT)")
        connection.execute("INSERT INTO messages VALUES ('keep-me')")
        connection.commit()
        before = connection.iterdump()
        before_dump = "\n".join(before)

    setup_phases: list[object] = []

    def unexpected_setup(*args: object, **kwargs: object) -> None:
        setup_phases.append((args, kwargs))

    monkeypatch.setattr(SQLiteRunner, "setup_with_stop_event", unexpected_setup)

    with pytest.raises(error_type, match=message):
        BrokerDB(str(db_path))

    assert setup_phases == []
    with closing(sqlite3.connect(db_path)) as connection:
        assert "\n".join(connection.iterdump()) == before_dump


def test_unversioned_legacy_database_publishes_only_completed_migrations(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "legacy.db"
    with closing(sqlite3.connect(db_path)) as connection:
        connection.executescript(
            """
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                body TEXT NOT NULL,
                ts INTEGER NOT NULL
            );
            CREATE TABLE meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            INSERT INTO meta (key, value) VALUES ('last_ts', 7);
            INSERT INTO messages (queue, body, ts) VALUES ('q', 'one', 7);
            INSERT INTO messages (queue, body, ts) VALUES ('q', 'two', 7);
            """
        )
        connection.commit()

    with pytest.raises(RuntimeError, match="duplicate timestamps"):
        BrokerDB(str(db_path))

    with closing(sqlite3.connect(db_path)) as connection:
        assert connection.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone() == (2,)
        assert connection.execute(
            "SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name = 'claimed'"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master "
            "WHERE type = 'index' AND name = 'idx_messages_ts_unique'"
        ).fetchone() == (0,)


def test_unversioned_legacy_database_advances_through_completed_migrations(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "legacy-success.db"
    with closing(sqlite3.connect(db_path)) as connection:
        connection.executescript(
            """
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                body TEXT NOT NULL,
                ts INTEGER NOT NULL
            );
            CREATE TABLE meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            INSERT INTO meta (key, value) VALUES ('last_ts', 7);
            INSERT INTO messages (queue, body, ts) VALUES ('q', 'keep-me', 7);
            """
        )
        connection.commit()

    with BrokerDB(str(db_path)) as broker:
        assert broker.peek_one("q") == ("keep-me", 7)
        broker.add_alias("legacy", "q")
        assert broker.resolve_alias("legacy") == "q"
        assert broker.status()["total_messages"] == 1

    with closing(sqlite3.connect(db_path)) as connection:
        metadata = dict(connection.execute("SELECT key, value FROM meta"))
        assert metadata["schema_version"] == SCHEMA_VERSION
        assert metadata["schema_proof_version"] == 1
        assert (
            metadata["schema_proof_cookie"]
            == connection.execute("PRAGMA schema_version").fetchone()[0]
        )
        assert connection.execute(
            "SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name = 'claimed'"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT COUNT(*) FROM pragma_index_list('messages') WHERE \"unique\" = 1"
        ).fetchone() == (1,)


@pytest.mark.parametrize("marker_mode", ["xattr", "fallback"])
def test_schema_marker_with_changed_schema_cookie_runs_repair_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    marker_mode: str,
) -> None:
    xattrs: dict[tuple[Path, str], bytes] = {}
    if marker_mode == "xattr":
        missing_errno = next(iter(phaselock_module._MISSING_XATTR_ERRNOS))

        def get_value(path: Path, key: str) -> bytes:
            try:
                return xattrs[(Path(path), key)]
            except KeyError as exc:
                raise OSError(missing_errno, "missing xattr", path) from exc

        def set_value(path: Path, key: str, value: bytes) -> None:
            xattrs[(Path(path), key)] = value

        monkeypatch.setattr(
            phaselock_module,
            "_xattr_provider",
            lambda: phaselock_module._XattrProvider(
                get_value=get_value,
                set_value=set_value,
            ),
        )
    else:
        monkeypatch.setenv(phaselock_module.PHASELOCK_ENABLE_XATTRS, "0")
    db_path = tmp_path / "stale-marker.db"
    with closing(sqlite3.connect(db_path)) as connection:
        connection.executescript(
            """
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                body TEXT NOT NULL,
                ts INTEGER NOT NULL
            );
            CREATE TABLE meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            INSERT INTO meta (key, value) VALUES ('last_ts', 0);
            """
        )
        connection.commit()

    with BrokerDB(str(db_path)):
        pass
    if marker_mode == "xattr":
        assert (db_path, "user.simplebroker.schema-v5") in xattrs

    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute("DROP INDEX idx_messages_ts_unique")
        connection.commit()

    real_migrate = sqlite_plugin_module.migrate_schema
    migrations: list[int] = []

    def tracking_migrate(*args, **kwargs):
        migrations.append(int(kwargs["current_version"]))
        return real_migrate(*args, **kwargs)

    monkeypatch.setattr(sqlite_plugin_module, "migrate_schema", tracking_migrate)

    with BrokerDB(str(db_path)):
        pass
    with BrokerDB(str(db_path)):
        pass

    assert migrations == [SCHEMA_VERSION]

    with closing(sqlite3.connect(db_path)) as connection:
        indexes = connection.execute(
            "SELECT name, \"unique\", partial FROM pragma_index_list('messages')"
        ).fetchall()
        assert any(
            unique == 1
            and partial == 0
            and connection.execute(
                "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
                (name,),
            ).fetchall()
            == [("ts",)]
            for name, unique, partial in indexes
        )


def test_schema_snapshot_cannot_mix_cookie_and_proof_across_external_ddl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "ddl-race.db"
    with closing(sqlite3.connect(db_path)) as connection:
        connection.executescript(
            """
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                body TEXT NOT NULL,
                ts INTEGER NOT NULL
            );
            CREATE TABLE meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            INSERT INTO meta (key, value) VALUES ('last_ts', 0);
            """
        )
        connection.commit()
    with BrokerDB(str(db_path)):
        pass

    real_connect = cast(Callable[..., sqlite3.Connection], sqlite3.connect)
    ddl_ran = False

    class RacingConnection(sqlite3.Connection):
        def execute(self, sql, parameters=(), /):
            nonlocal ddl_ran
            normalized = " ".join(str(sql).lower().split())
            if not ddl_ran and normalized == "pragma schema_version":
                cursor = super().execute(sql, parameters)
                with closing(real_connect(db_path)) as external:
                    external.execute("DROP INDEX idx_messages_ts_unique")
                    external.commit()
                ddl_ran = True
                return cursor
            if not ddl_ran and "from pragma_schema_version" in normalized:
                with closing(real_connect(db_path)) as external:
                    external.execute("DROP INDEX idx_messages_ts_unique")
                    external.commit()
                ddl_ran = True
            return super().execute(sql, parameters)

    def racing_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        return real_connect(*args, **kwargs, factory=RacingConnection)

    monkeypatch.setattr(sqlite3, "connect", racing_connect)

    with BrokerDB(str(db_path)):
        pass

    assert ddl_ran
    with closing(real_connect(db_path)) as connection:
        indexes = connection.execute(
            "SELECT name, \"unique\", partial FROM pragma_index_list('messages')"
        ).fetchall()
        assert any(
            unique == 1
            and partial == 0
            and connection.execute(
                "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
                (name,),
            ).fetchall()
            == [("ts",)]
            for name, unique, partial in indexes
        )


def test_missing_database_proof_causes_one_slow_open_then_returns_to_fast_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "missing-proof.db"
    with BrokerDB(str(db_path)):
        pass

    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute(
            "DELETE FROM meta WHERE key IN "
            "('schema_proof_version', 'schema_proof_cookie')"
        )
        connection.commit()

    real_migrate = sqlite_plugin_module.migrate_schema
    migrations: list[int] = []

    def tracking_migrate(*args, **kwargs):
        migrations.append(int(kwargs["current_version"]))
        return real_migrate(*args, **kwargs)

    monkeypatch.setattr(sqlite_plugin_module, "migrate_schema", tracking_migrate)

    with BrokerDB(str(db_path)):
        pass
    assert migrations == [SCHEMA_VERSION]

    with BrokerDB(str(db_path)):
        pass
    assert migrations == [SCHEMA_VERSION]

    with closing(sqlite3.connect(db_path)) as connection:
        proof = dict(
            connection.execute(
                "SELECT key, value FROM meta WHERE key IN "
                "('schema_proof_version', 'schema_proof_cookie')"
            )
        )
        assert proof["schema_proof_version"] == 1
        assert (
            proof["schema_proof_cookie"]
            == connection.execute("PRAGMA schema_version").fetchone()[0]
        )


def test_valid_schema_proof_uses_only_the_scalar_admission_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "valid-proof.db"
    with BrokerDB(str(db_path)):
        pass

    real_connect = cast(Callable[..., sqlite3.Connection], sqlite3.connect)
    statements: list[str] = []
    connections = 0

    def tracking_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        nonlocal connections
        connections += 1
        connection = real_connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(sqlite3, "connect", tracking_connect)

    with BrokerDB(str(db_path)):
        pass

    normalized = [statement.strip().lower() for statement in statements]
    assert connections == 1
    assert normalized.count("pragma schema_version") == 0
    assert (
        sum(
            "select meta.key, meta.value, cookie.schema_version" in statement
            for statement in normalized
        )
        == 1
    )
    assert not any(
        token in statement
        for statement in normalized
        for token in (
            "sqlite_master",
            "pragma table_info",
            "pragma index_list",
            "create table",
            "create index",
            "drop index",
        )
    )


def test_proof_publication_failure_does_not_publish_a_false_fast_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "proof-failure.db"
    real_publish = BrokerCore._publish_sqlite_schema_proof
    publications = 0

    def fail_once(core: BrokerCore) -> None:
        nonlocal publications
        publications += 1
        if publications == 1:
            raise sqlite3.OperationalError("injected proof publication failure")
        real_publish(core)

    monkeypatch.setattr(BrokerCore, "_publish_sqlite_schema_proof", fail_once)

    with pytest.raises(sqlite3.OperationalError, match="proof publication failure"):
        BrokerDB(str(db_path))

    with BrokerDB(str(db_path)):
        pass

    assert publications == 2
    with closing(sqlite3.connect(db_path)) as connection:
        assert connection.execute(
            "SELECT value FROM meta WHERE key = 'schema_proof_version'"
        ).fetchone() == (1,)


def test_concurrent_stale_proof_has_one_repair_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "concurrent-proof.db"
    with BrokerDB(str(db_path)):
        pass
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute(
            "DELETE FROM meta WHERE key IN "
            "('schema_proof_version', 'schema_proof_cookie')"
        )
        connection.commit()

    real_read = db_module._read_explicit_sqlite_magic_before_setup
    initial_barrier = threading.Barrier(2)
    synchronized_reads = 0
    reads_lock = threading.Lock()

    def synchronized_read(runner: SQLiteRunner):
        nonlocal synchronized_reads
        snapshot = real_read(runner)
        should_wait = False
        with reads_lock:
            if snapshot.proof_version is None and synchronized_reads < 2:
                synchronized_reads += 1
                should_wait = True
        if should_wait:
            initial_barrier.wait(timeout=5)
        return snapshot

    real_migrate = sqlite_plugin_module.migrate_schema
    migrations = 0

    def tracking_migrate(*args, **kwargs):
        nonlocal migrations
        with reads_lock:
            migrations += 1
        return real_migrate(*args, **kwargs)

    monkeypatch.setattr(
        db_module,
        "_read_explicit_sqlite_magic_before_setup",
        synchronized_read,
    )
    monkeypatch.setattr(sqlite_plugin_module, "migrate_schema", tracking_migrate)

    def open_once() -> None:
        with BrokerDB(str(db_path)):
            pass

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(open_once) for _ in range(2)]
        for future in futures:
            future.result(timeout=10)

    assert synchronized_reads == 2
    assert migrations == 1


def test_compact_causes_one_conservative_repair_then_fast_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "compact-proof.db"
    with BrokerDB(str(db_path)) as broker:
        broker.vacuum(compact=True)

    real_migrate = sqlite_plugin_module.migrate_schema
    migrations: list[int] = []

    def tracking_migrate(*args, **kwargs):
        migrations.append(int(kwargs["current_version"]))
        return real_migrate(*args, **kwargs)

    monkeypatch.setattr(sqlite_plugin_module, "migrate_schema", tracking_migrate)

    with BrokerDB(str(db_path)):
        pass
    with BrokerDB(str(db_path)):
        pass

    assert migrations == [SCHEMA_VERSION]


def test_fallback_marker_cannot_prove_replacement_database_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(phaselock_module.PHASELOCK_ENABLE_XATTRS, "0")
    db_path = tmp_path / "broker.db"
    replacement = tmp_path / "replacement.db"
    with BrokerDB(str(db_path)):
        pass
    with BrokerDB(str(replacement)):
        pass
    with closing(sqlite3.connect(replacement)) as connection:
        connection.execute(
            "DELETE FROM meta WHERE key IN "
            "('schema_proof_version', 'schema_proof_cookie')"
        )
        connection.commit()

    os.replace(replacement, db_path)
    assert Path(f"{db_path}.status").exists()

    real_migrate = sqlite_plugin_module.migrate_schema
    migrations: list[int] = []

    def tracking_migrate(*args, **kwargs):
        migrations.append(int(kwargs["current_version"]))
        return real_migrate(*args, **kwargs)

    monkeypatch.setattr(sqlite_plugin_module, "migrate_schema", tracking_migrate)
    with BrokerDB(str(db_path)):
        pass

    assert migrations == [SCHEMA_VERSION]


def test_setup_magic_xattr_does_not_replace_stored_version_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values: dict[tuple[Path, str], bytes] = {}
    missing_errno = next(iter(phaselock_module._MISSING_XATTR_ERRNOS))

    def get_value(path: Path, key: str) -> bytes:
        try:
            return values[(Path(path), key)]
        except KeyError as exc:
            raise OSError(missing_errno, "missing xattr", path) from exc

    def set_value(path: Path, key: str, value: bytes) -> None:
        values[(Path(path), key)] = value

    provider = phaselock_module._XattrProvider(
        get_value=get_value,
        set_value=set_value,
    )
    monkeypatch.setattr(phaselock_module, "_xattr_provider", lambda: provider)

    db_path = tmp_path / "owned.db"
    with BrokerDB(str(db_path)):
        pass

    magic_key = "user.simplebroker.magic"
    assert values[(db_path, magic_key)] == SIMPLEBROKER_MAGIC.encode()

    real_read = db_module._read_explicit_sqlite_magic_before_setup
    reads: list[SQLiteRunner] = []

    def tracking_read(runner: SQLiteRunner):
        reads.append(runner)
        return real_read(runner)

    monkeypatch.setattr(
        db_module,
        "_read_explicit_sqlite_magic_before_setup",
        tracking_read,
    )
    runner = SQLiteRunner(str(db_path))
    core = BrokerCore(runner)
    core.shutdown()
    assert reads == [runner]


@pytest.mark.parametrize("initial_state", ["absent", "empty", "no-magic", "owned"])
def test_broker_db_keeps_legacy_or_owned_sqlite_bootstrap_behavior(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    initial_state: str,
) -> None:
    db_path = tmp_path / "bootstrap.db"
    if initial_state == "empty":
        db_path.touch()
    elif initial_state in {"no-magic", "owned"}:
        with closing(sqlite3.connect(db_path)) as connection:
            connection.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
            if initial_state == "owned":
                connection.execute(
                    "INSERT INTO meta VALUES ('magic', ?)", (SIMPLEBROKER_MAGIC,)
                )
            connection.commit()

    with BrokerDB(str(db_path)):
        pass

    real_migrate = sqlite_plugin_module.migrate_schema
    migrations: list[int] = []

    def tracking_migrate(*args, **kwargs):
        migrations.append(int(kwargs["current_version"]))
        return real_migrate(*args, **kwargs)

    monkeypatch.setattr(sqlite_plugin_module, "migrate_schema", tracking_migrate)
    with BrokerDB(str(db_path)):
        pass
    with BrokerDB(str(db_path)):
        pass
    assert migrations == []

    with closing(sqlite3.connect(db_path)) as connection:
        stored_magic = connection.execute(
            "SELECT value FROM meta WHERE key = 'magic'"
        ).fetchone()
    assert stored_magic == (SIMPLEBROKER_MAGIC,)
