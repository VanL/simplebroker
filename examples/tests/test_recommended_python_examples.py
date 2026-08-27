"""Behavioral proofs for the recommended Python examples."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
import python_api
from async_wrapper import AsyncBroker

from simplebroker import Queue


def test_async_wrapper_exposes_oldest_and_newest_bounded_selection(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "wrapper-order.db"
    queue = Queue("jobs", db_path=str(db_path))
    try:
        queue.insert_messages(
            [
                ("inserted-first", 300),
                ("inserted-second", 100),
                ("inserted-third", 200),
            ]
        )
    finally:
        queue.close()

    async def exercise() -> tuple[str | None, str | None, str | None, str | None]:
        async with AsyncBroker(db_path) as broker:
            return (
                await broker.peek("jobs"),
                await broker.peek("jobs", order="newest"),
                await broker.pop("jobs", order="newest"),
                await broker.pop("jobs"),
            )

    assert asyncio.run(exercise()) == (
        "inserted-second",
        "inserted-first",
        "inserted-first",
        "inserted-second",
    )


def test_async_wrapper_early_stream_exit_leaves_later_rows_pending(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "wrapper-stream.db"

    async def consume_one() -> None:
        async with AsyncBroker(db_path) as broker:
            await broker.push("jobs", "one")
            await broker.push("jobs", "two")
            await broker.push("jobs", "three")

            stream = broker.stream_messages("jobs")
            await anext(stream)
            await stream.aclose()

    asyncio.run(consume_one())

    queue = Queue("jobs", db_path=str(db_path))
    try:
        assert queue.peek_many(10) == ["two", "three"]
    finally:
        queue.close()


def test_python_api_demonstrates_public_id_order_not_insertion_order(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "python-order.db"

    python_api.selection_order_usage(db_path)

    output = capsys.readouterr().out
    assert "Default oldest peek: id=100, body=inserted-second" in output
    assert "Explicit newest peek: id=300, body=inserted-first" in output
    assert "Explicit newest read: id=300, body=inserted-first" in output
    assert "Default oldest read: id=100, body=inserted-second" in output

    queue = Queue("selection_order", db_path=str(db_path))
    try:
        assert queue.peek_many(10) == ["inserted-third"]
    finally:
        queue.close()
