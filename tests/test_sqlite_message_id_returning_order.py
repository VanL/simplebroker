"""SQLite public-ID order when ``RETURNING`` rows arrive out of order."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

import pytest

from simplebroker._backends.sqlite.plugin import SQLiteBackendPlugin
from simplebroker._runner import SQLiteRunner
from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.sqlite_only]


class _ReversingReturningRunner(SQLiteRunner):
    """Exercise SQLite's documented freedom to reorder RETURNING rows."""

    def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Iterable[tuple[Any, ...]]:
        rows = list(super().run(sql, params, fetch=fetch))
        if fetch and sql.lstrip().startswith("UPDATE messages") and "RETURNING" in sql:
            rows.reverse()
        return rows


def test_equivalent_sqlite_plugin_instance_uses_the_builtin_row_contract(
    tmp_path: Path,
) -> None:
    """ID normalization must not depend on one plugin object's identity."""
    runner = _ReversingReturningRunner(str(tmp_path / "fresh-plugin.db"))
    broker = BrokerCore(runner, backend_plugin=cast(Any, SQLiteBackendPlugin()))
    try:
        broker.insert_messages(
            [
                ("jobs", "inserted-first", 300),
                ("jobs", "inserted-second", 100),
            ]
        )

        assert broker.claim_many("jobs", 2) == [
            ("inserted-second", 100),
            ("inserted-first", 300),
        ]
    finally:
        broker.close()
        runner.close()


@pytest.mark.parametrize(
    ("order", "expected_ids"),
    [("oldest", [100, 200, 300]), ("newest", [300, 200, 100])],
)
def test_claim_many_normalizes_sqlite_returning_rows_by_public_id(
    tmp_path: Path,
    order: str,
    expected_ids: list[int],
) -> None:
    """Materialized claims ignore raw engine return order in both directions."""
    runner = _ReversingReturningRunner(str(tmp_path / "claim-many.db"))
    broker = BrokerCore(runner)
    try:
        broker.insert_messages(
            [
                ("jobs", "inserted-first", 300),
                ("jobs", "inserted-second", 100),
                ("jobs", "inserted-third", 200),
            ]
        )

        rows = broker.claim_many("jobs", 3, order=order)
        assert [timestamp for _body, timestamp in rows] == expected_ids
    finally:
        broker.close()
        runner.close()


def test_claim_generator_uses_ascending_ids_when_returning_rows_are_reversed(
    tmp_path: Path,
) -> None:
    """Transactional claim batches normalize before yielding public rows."""
    runner = _ReversingReturningRunner(str(tmp_path / "claim-generator.db"))
    broker = BrokerCore(runner)
    try:
        broker.insert_messages(
            [
                ("jobs", "inserted-first", 300),
                ("jobs", "inserted-second", 100),
                ("jobs", "inserted-third", 200),
            ]
        )

        assert list(
            broker.claim_generator(
                "jobs",
                delivery_guarantee="at_least_once",
                batch_size=3,
            )
        ) == [
            ("inserted-second", 100),
            ("inserted-third", 200),
            ("inserted-first", 300),
        ]
    finally:
        broker.close()
        runner.close()


@pytest.mark.parametrize(
    ("order", "expected_ids"),
    [("oldest", [100, 200, 300]), ("newest", [300, 200, 100])],
)
def test_move_many_normalizes_sqlite_returning_rows_by_public_id(
    tmp_path: Path,
    order: str,
    expected_ids: list[int],
) -> None:
    """Materialized moves expose the selected public-ID direction."""
    runner = _ReversingReturningRunner(str(tmp_path / "move-many.db"))
    broker = BrokerCore(runner)
    try:
        broker.insert_messages(
            [
                ("source", "inserted-first", 300),
                ("source", "inserted-second", 100),
                ("source", "inserted-third", 200),
            ]
        )

        rows = broker.move_many("source", "destination", 3, order=order)
        assert [timestamp for _body, timestamp in rows] == expected_ids
    finally:
        broker.close()
        runner.close()


def test_move_generator_uses_ascending_ids_when_returning_rows_are_reversed(
    tmp_path: Path,
) -> None:
    """Transactional move batches normalize before yielding public rows."""
    runner = _ReversingReturningRunner(str(tmp_path / "move-generator.db"))
    broker = BrokerCore(runner)
    try:
        broker.insert_messages(
            [
                ("source", "inserted-first", 300),
                ("source", "inserted-second", 100),
                ("source", "inserted-third", 200),
            ]
        )

        assert list(
            broker.move_generator(
                "source",
                "destination",
                delivery_guarantee="at_least_once",
                batch_size=3,
            )
        ) == [
            ("inserted-second", 100),
            ("inserted-third", 200),
            ("inserted-first", 300),
        ]
    finally:
        broker.close()
        runner.close()
