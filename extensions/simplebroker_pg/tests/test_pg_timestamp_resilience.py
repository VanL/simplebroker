"""PostgreSQL timestamp-repair concurrency regressions."""

from __future__ import annotations

import threading
import warnings
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any

import pytest
from simplebroker_pg import PostgresRunner

from simplebroker.db import BrokerCore
from simplebroker.ext import TimestampError

pytestmark = [pytest.mark.pg_only]


def test_missing_meta_row_cannot_silently_drop_timestamp_floor(
    pg_core: BrokerCore,
) -> None:
    original_rows = list(
        pg_core._runner.run(
            "SELECT singleton, magic, schema_version, last_ts, alias_version "
            "FROM meta WHERE singleton = TRUE",
            fetch=True,
        )
    )
    assert len(original_rows) == 1
    try:
        pg_core._runner.run("DELETE FROM meta WHERE singleton = TRUE")
        pg_core._runner.commit()

        with pytest.raises(TimestampError, match="below requested floor") as caught:
            pg_core.advance_last_timestamp(100)
    finally:
        pg_core._runner.run(
            "INSERT INTO meta "
            "(singleton, magic, schema_version, last_ts, alias_version) "
            "VALUES (?, ?, ?, ?, ?)",
            tuple(original_rows[0]),
        )
        pg_core._runner.commit()

    assert caught.value.outcome_ambiguous is False


class _PausingTimestampRepairPlugin:
    """Pause the real repair immediately before its backend mutation."""

    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate
        self.mutation_reached = threading.Event()
        self.release_mutation = threading.Event()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    def _pause(self) -> None:
        self.mutation_reached.set()
        if not self.release_mutation.wait(timeout=5):
            raise AssertionError("timestamp repair mutation was not released")

    def advance_last_ts(self, runner: Any, *, new_ts: int) -> bool:
        self._pause()
        return bool(self._delegate.advance_last_ts(runner, new_ts=new_ts))

    def write_last_ts(self, runner: Any, ts: int) -> None:
        self._pause()
        self._delegate.write_last_ts(runner, ts)


@contextmanager
def _cores_for_same_schema(
    pg_core: BrokerCore,
    pg_dsn: str,
    pg_schema: str,
    pg_plugin: Any,
) -> Iterator[tuple[BrokerCore, BrokerCore, _PausingTimestampRepairPlugin]]:
    runner = PostgresRunner(pg_dsn, schema=pg_schema)
    contender = BrokerCore(runner, backend_plugin=pg_plugin)
    pausing_plugin = _PausingTimestampRepairPlugin(pg_plugin)
    pg_core._backend_plugin = pausing_plugin
    try:
        yield pg_core, contender, pausing_plugin
    finally:
        contender.close()
        runner.shutdown()


def test_resync_cannot_overwrite_concurrent_high_water_backward(
    pg_core: BrokerCore,
    pg_dsn: str,
    pg_schema: str,
    pg_plugin: Any,
) -> None:
    """A stale repair must preserve a later high-water committed elsewhere."""
    with _cores_for_same_schema(pg_core, pg_dsn, pg_schema, pg_plugin) as (
        resyncing,
        contender,
        pausing_plugin,
    ):
        stored_id = resyncing.write("jobs", "stored before repair")
        pg_plugin.write_last_ts(resyncing._runner, 0)
        resyncing._runner.commit()

        with (
            ThreadPoolExecutor(max_workers=1) as executor,
            warnings.catch_warnings(record=True) as caught,
        ):
            warnings.simplefilter("always", RuntimeWarning)
            repair = executor.submit(resyncing._resync_timestamp_generator)
            try:
                assert pausing_plugin.mutation_reached.wait(timeout=5)
                concurrent_winner = contender.generate_timestamp()
                assert concurrent_winner > stored_id
            finally:
                pausing_plugin.release_mutation.set()
            repair.result(timeout=5)

        repair_warnings = [
            str(item.message)
            for item in caught
            if "Timestamp generator resynchronized" in str(item.message)
        ]
        assert len(repair_warnings) == 1
        assert f"New: {concurrent_winner} " in repair_warnings[0]
        assert pg_plugin.read_last_ts(resyncing._runner) == concurrent_winner
        assert resyncing.get_cached_last_timestamp() == concurrent_winner
