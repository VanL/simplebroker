"""Explicit public-Queue pagination measurements, never a timing pytest gate.

Run this same helper with baseline and candidate interpreters (or PYTHONPATHs)::

    uv run --no-sync python tests/peek_pagination_benchmark.py --label candidate
    SIMPLEBROKER_PG_TEST_DSN=... uv run --no-sync python \
        tests/peek_pagination_benchmark.py --backend postgres --label candidate

Only use a dedicated test PG service. Each invocation owns a fresh UUID schema
and drops only that schema. SQLite uses a temporary file. Fixtures have identical
IDs and representative synthetic task-event bodies across runs; seeding is raw
SQL outside measurements. Every measured scan uses Queue.peek_generator().
Output includes source path/hash so a baseline label alone is never evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import os
import sqlite3
import statistics
import tempfile
import time
import uuid
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any

from simplebroker import Queue
from simplebroker._exceptions import BrokerError
from simplebroker._runner import SQLiteRunner
from simplebroker._sql._query_spec import RetrieveQuerySpec
from simplebroker._targets import BrokerTarget
from simplebroker.db import BrokerCore

QUEUE = "weft.log.tasks"
SQLITE_PROGRESS_INTERVAL = 100


def payload(index: int) -> str:
    """Deterministic representative event, without claiming production fidelity."""
    return json.dumps(
        {
            "tid": f"task-{index % 5000:05d}",
            "state": "completed" if index % 3 == 0 else "running",
            "timestamp": "2026-09-11T12:00:00Z",
            "name": "example.task",
            "message": "Synthetic retained task-log benchmark event",
            "sequence": index,
        },
        separators=(",", ":"),
    )


def seed_sqlite_messages(connection: sqlite3.Connection, count: int) -> None:
    """Seed one benchmark dataset in a single explicit transaction.

    SQLiteRunner connections intentionally use autocommit. Without this
    boundary, executemany durably commits every fixture row, which turns setup
    into thousands of filesystem syncs on Windows.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.executemany(
            "INSERT INTO messages(queue, body, ts) VALUES (?, ?, ?)",
            ((QUEUE, payload(i), i) for i in range(1, count + 1)),
        )
        connection.execute("ANALYZE")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


@contextmanager
def dataset(
    backend: str, count: int, dsn: str | None
) -> Iterator[tuple[Queue, Any, str]]:
    """Own isolated storage and an injected real runner, including cleanup."""
    with tempfile.TemporaryDirectory(prefix="simplebroker-peek-bench-") as temp:
        plugin: Any = None
        schema = "peek_bench_" + uuid.uuid4().hex
        runner: Any
        if backend == "sqlite":
            target = BrokerTarget("sqlite", str(Path(temp) / "broker.db"))
            runner = SQLiteRunner(target.target)
        else:
            from simplebroker_pg import PostgresRunner, get_backend_plugin

            if dsn is None:
                raise ValueError("Postgres requires a dedicated test DSN")
            plugin = get_backend_plugin()
            target = BrokerTarget("postgres", dsn, {"schema": schema})
            runner = PostgresRunner(dsn, schema=schema)
        try:
            with Queue(QUEUE, db_path=target, runner=runner) as queue:
                # Force real schema initialization before raw, fixed-ID seeding.
                list(queue.peek_generator())
                if backend == "sqlite":
                    connection = runner.get_connection()
                    seed_sqlite_messages(connection, count)
                    version = sqlite3.sqlite_version
                else:
                    import psycopg
                    from psycopg import sql

                    if dsn is None:
                        raise ValueError("Postgres requires a dedicated test DSN")
                    with (
                        psycopg.connect(dsn) as connection,
                        connection.cursor() as cursor,
                    ):
                        with cursor.copy(
                            sql.SQL(
                                "COPY {}.messages(queue, body, ts) FROM STDIN"
                            ).format(sql.Identifier(schema))
                        ) as copy:
                            for i in range(1, count + 1):
                                copy.write_row((QUEUE, payload(i), i))
                        cursor.execute(
                            sql.SQL("ANALYZE {}.messages").format(
                                sql.Identifier(schema)
                            )
                        )
                        cursor.execute("SELECT version()")
                        version_row = cursor.fetchone()
                        if version_row is None:
                            raise AssertionError("Postgres did not return its version")
                        version = str(version_row[0])
                yield queue, runner, version
        finally:
            runner.close()
            if plugin is not None:
                plugin.cleanup_target(dsn, backend_options={"schema": schema})


def scan(queue: Queue, count: int) -> dict[str, Any]:
    """Verify every ordered ID while timing the real public generator."""
    digest = hashlib.sha256()
    body_digest = hashlib.sha256()
    seen = 0
    started = time.perf_counter()
    with closing(queue.peek_generator(with_timestamps=True)) as messages:
        for body, timestamp in messages:
            seen += 1
            if timestamp != seen:
                raise AssertionError(("Unexpected message ID", timestamp, seen))
            digest.update(f"{timestamp}\n".encode())
            body_digest.update(body.encode())
    elapsed = time.perf_counter() - started
    if seen != count:
        raise AssertionError(("Unexpected message count", seen, count))
    return {
        "rows": seen,
        "id_sha256": digest.hexdigest(),
        "body_sha256": body_digest.hexdigest(),
        "seconds": elapsed,
    }


def sqlite_steps(queue: Queue, runner: Any, count: int) -> int:
    """Count fixed-size VM instruction blocks during the public scan.

    Crossing from SQLite into Python for every opcode makes the observer itself
    dominate on Windows. The returned estimate is within one interval of the
    executed instruction count, which is far below the linearity test's margin.
    """
    callbacks = 0

    def progress() -> int:
        nonlocal callbacks
        callbacks += 1
        return 0

    connection = runner.get_connection()
    connection.set_progress_handler(progress, SQLITE_PROGRESS_INTERVAL)
    try:
        scan(queue, count)
    finally:
        connection.set_progress_handler(None, 0)
    return callbacks * SQLITE_PROGRESS_INTERVAL


def pg_plans(runner: Any, count: int, batch_size: int) -> dict[str, Any]:
    """Explain equivalent early/late pages with production SQL builders."""
    from simplebroker_pg._sql import build_retrieve_query

    plans = {}
    for name, after, offset in (
        ("early", None, 0),
        ("late_offset", None, count - batch_size),
        ("late_keyset", count - batch_size, 0),
    ):
        query, params = build_retrieve_query(
            "peek",
            RetrieveQuerySpec(QUEUE, batch_size, offset=offset, after_timestamp=after),
        )
        result = runner.run(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query, params, fetch=True
        )
        plans[name] = result[0][0]
    return plans


def measure(backend: str, count: int, dsn: str | None, repeats: int) -> dict[str, Any]:
    """Report timing separately from instrumentation, on the same warmed dataset."""
    from simplebroker.db import PEEK_BATCH_SIZE

    with dataset(backend, count, dsn) as (queue, runner, version):
        scan(queue, count)  # warm caches before timing
        measurements = [scan(queue, count) for _ in range(repeats)]
        result: dict[str, Any] = {
            "backend": backend,
            "version": version,
            "rows": count,
            "batch_size": PEEK_BATCH_SIZE,
            "id_sha256": measurements[0]["id_sha256"],
            "body_sha256": measurements[0]["body_sha256"],
            "median_seconds": statistics.median(row["seconds"] for row in measurements),
            "seconds": [row["seconds"] for row in measurements],
        }
        calls: list[str] = []
        original_run = runner.run

        def traced_run(sql: str, *args: Any, **kwargs: Any) -> Any:
            calls.append(sql)
            return original_run(sql, *args, **kwargs)

        runner.run = traced_run
        try:
            scan(queue, count)
        finally:
            runner.run = original_run
        result["runner_calls"] = len(calls)
        result["page_queries"] = sum("SELECT body, ts" in sql for sql in calls)
        if backend == "sqlite":
            result["vm_steps"] = sqlite_steps(queue, runner, count)
        else:
            result["explain"] = pg_plans(runner, count, PEEK_BATCH_SIZE)
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("sqlite", "postgres"), default="sqlite")
    parser.add_argument("--sizes", nargs="+", type=int, default=[50000, 100000, 220000])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--label", required=True)
    args = parser.parse_args()
    dsn = os.environ.get("SIMPLEBROKER_PG_TEST_DSN")
    if args.backend == "postgres" and not dsn:
        parser.error("Postgres requires a dedicated SIMPLEBROKER_PG_TEST_DSN")
    if args.repeats < 1 or any(size < 1000 for size in args.sizes):
        parser.error("repeats must be positive and sizes must be at least 1000")
    # Translate expected input, storage, dependency and verification failures at
    # the CLI boundary. Programming errors retain their diagnostic traceback.
    runtime_errors: tuple[type[Exception], ...] = (
        AssertionError,
        BrokerError,
        ImportError,
        OSError,
        RuntimeError,
        ValueError,
        sqlite3.Error,
    )
    if args.backend == "postgres":
        try:
            from psycopg import Error as PostgresError
        except ImportError as exc:
            parser.exit(1, f"{exc}\n")
        # Raw COPY/setup and pool failures may escape the runner's translated
        # BrokerError path; PoolTimeout/PoolClosed inherit psycopg.Error too.
        runtime_errors += (PostgresError,)
    try:
        results = [measure(args.backend, n, dsn, args.repeats) for n in args.sizes]
    except runtime_errors as exc:
        parser.exit(1, f"{exc}\n")
    source = inspect.getsource(BrokerCore.peek_generator)
    print(
        json.dumps(
            {
                "label": args.label,
                "source_path": inspect.getfile(BrokerCore),
                "generator_sha256": hashlib.sha256(source.encode()).hexdigest(),
                "results": results,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
