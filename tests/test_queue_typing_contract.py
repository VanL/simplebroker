"""Static-only typing contract fixtures for the public Queue surface."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import assert_type

from simplebroker import CloseableIterator, MovedMessage, Queue

ROOT = Path(__file__).resolve().parents[1]


def assert_high_level_queue_types(queue: Queue, runtime_bool: bool) -> None:
    assert_type(queue.read(), str | None)
    assert_type(queue.read(order="newest"), str | None)
    assert_type(queue.read(with_timestamps=True), tuple[str, int] | None)
    assert_type(queue.read(all_messages=True), CloseableIterator[str])
    assert_type(
        queue.read(all_messages=True, with_timestamps=True),
        CloseableIterator[tuple[str, int]],
    )
    assert_type(
        queue.read(all_messages=runtime_bool),
        str | tuple[str, int] | CloseableIterator[str | tuple[str, int]] | None,
    )
    queue.read(all_messages=True).close()
    with closing(queue.read(all_messages=True)) as read_messages:
        assert_type(read_messages, CloseableIterator[str])
    ordinary_read_iterator: Iterator[str] = queue.read(all_messages=True)
    assert_type(ordinary_read_iterator, Iterator[str])

    assert_type(queue.peek(), str | None)
    assert_type(queue.peek(order="newest"), str | None)
    assert_type(queue.peek(with_timestamps=True), tuple[str, int] | None)
    assert_type(queue.peek(all_messages=True), CloseableIterator[str])
    assert_type(
        queue.peek(all_messages=True, with_timestamps=True),
        CloseableIterator[tuple[str, int]],
    )
    assert_type(
        queue.peek(all_messages=runtime_bool),
        str | tuple[str, int] | CloseableIterator[str | tuple[str, int]] | None,
    )
    queue.peek(all_messages=True).close()

    assert_type(queue.move("destination"), MovedMessage | None)
    assert_type(queue.move("destination", order="newest"), MovedMessage | None)
    assert_type(
        queue.move("destination", all_messages=True),
        CloseableIterator[MovedMessage],
    )
    assert_type(
        queue.move("destination", all_messages=runtime_bool),
        MovedMessage | CloseableIterator[MovedMessage] | None,
    )
    queue.move("destination", all_messages=True).close()
    with closing(queue.move("destination", all_messages=True)) as moved_messages:
        assert_type(moved_messages, CloseableIterator[MovedMessage])
    ordinary_move_iterator: Iterator[MovedMessage] = queue.move(
        "destination", all_messages=True
    )
    assert_type(ordinary_move_iterator, Iterator[MovedMessage])


def assert_granular_queue_types(queue: Queue, runtime_bool: bool) -> None:
    assert_type(queue.read_one(), str | None)
    assert_type(queue.read_one(order="newest"), str | None)
    assert_type(queue.read_one(with_timestamps=True), tuple[str, int] | None)
    assert_type(
        queue.read_one(with_timestamps=runtime_bool), str | tuple[str, int] | None
    )
    assert_type(queue.read_many(5), list[str])
    assert_type(queue.read_many(5, order="newest"), list[str])
    assert_type(queue.read_many(5, with_timestamps=True), list[tuple[str, int]])
    assert_type(
        queue.read_many(5, with_timestamps=runtime_bool),
        list[str] | list[tuple[str, int]],
    )
    assert_type(queue.read_generator(), CloseableIterator[str])
    assert_type(
        queue.read_generator(with_timestamps=True),
        CloseableIterator[tuple[str, int]],
    )
    assert_type(
        queue.read_generator(with_timestamps=runtime_bool),
        CloseableIterator[str | tuple[str, int]],
    )
    queue.read_generator().close()
    with closing(queue.read_generator()) as read_messages:
        assert_type(read_messages, CloseableIterator[str])
    ordinary_read_iterator: Iterator[str] = queue.read_generator()
    assert_type(ordinary_read_iterator, Iterator[str])

    assert_type(queue.peek_one(), str | None)
    assert_type(queue.peek_one(order="newest"), str | None)
    assert_type(queue.peek_one(with_timestamps=True), tuple[str, int] | None)
    assert_type(
        queue.peek_one(with_timestamps=runtime_bool), str | tuple[str, int] | None
    )
    assert_type(queue.peek_many(), list[str])
    assert_type(queue.peek_many(order="newest"), list[str])
    assert_type(queue.peek_many(with_timestamps=True), list[tuple[str, int]])
    assert_type(
        queue.peek_many(with_timestamps=runtime_bool),
        list[str] | list[tuple[str, int]],
    )
    assert_type(queue.peek_generator(), CloseableIterator[str])
    assert_type(
        queue.peek_generator(with_timestamps=True),
        CloseableIterator[tuple[str, int]],
    )
    assert_type(
        queue.peek_generator(with_timestamps=runtime_bool),
        CloseableIterator[str | tuple[str, int]],
    )
    queue.peek_generator().close()
    ordinary_iterator: Iterator[str] = queue.peek_generator()
    assert_type(ordinary_iterator, Iterator[str])

    assert_type(queue.move_one("destination"), str | None)
    assert_type(queue.move_one("destination", order="newest"), str | None)
    assert_type(
        queue.move_one("destination", with_timestamps=True),
        tuple[str, int] | None,
    )
    assert_type(
        queue.move_one("destination", with_timestamps=runtime_bool),
        str | tuple[str, int] | None,
    )
    assert_type(queue.move_many("destination", 5), list[str])
    assert_type(queue.move_many("destination", 5, order="newest"), list[str])
    assert_type(
        queue.move_many("destination", 5, with_timestamps=True),
        list[tuple[str, int]],
    )
    assert_type(
        queue.move_many("destination", 5, with_timestamps=runtime_bool),
        list[str] | list[tuple[str, int]],
    )
    assert_type(queue.move_generator("destination"), CloseableIterator[str])
    assert_type(
        queue.move_generator("destination", with_timestamps=True),
        CloseableIterator[tuple[str, int]],
    )
    assert_type(
        queue.move_generator("destination", with_timestamps=runtime_bool),
        CloseableIterator[str | tuple[str, int]],
    )
    queue.move_generator("destination").close()
    with closing(queue.move_generator("destination")) as moved_messages:
        assert_type(moved_messages, CloseableIterator[str])
    ordinary_move_iterator: Iterator[str] = queue.move_generator("destination")
    assert_type(ordinary_move_iterator, Iterator[str])

    assert_type(
        queue.stream_messages(),
        CloseableIterator[tuple[str, int]],
    )
    queue.stream_messages().close()
    with closing(queue.stream_messages()) as streamed_messages:
        assert_type(streamed_messages, CloseableIterator[tuple[str, int]])
    ordinary_stream_iterator: Iterator[tuple[str, int]] = queue.stream_messages()
    assert_type(ordinary_stream_iterator, Iterator[tuple[str, int]])


def assert_delete_types(queue: Queue, message_id: int) -> None:
    assert_type(queue.delete(), bool)
    assert_type(queue.delete(message_id=message_id), bool)


def test_delete_none_fixture_is_rejected_by_mypy() -> None:
    fixture = ROOT / "tests" / "typecheck_fixtures" / "queue_delete_none.py"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--config-file",
            str(ROOT / "pyproject.toml"),
            "--no-incremental",
            "--show-error-codes",
            str(fixture),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert result.stderr == ""
    assert result.stdout.count("error:") == 1
    assert "[call-overload]" in result.stdout
    assert "message_id" in result.stdout


def test_generator_order_fixture_is_rejected_by_mypy() -> None:
    fixture = ROOT / "tests" / "typecheck_fixtures" / "queue_generator_order.py"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--config-file",
            str(ROOT / "pyproject.toml"),
            "--no-incremental",
            "--show-error-codes",
            str(fixture),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert result.stderr == ""
    assert result.stdout.count("error:") >= 3
    assert result.stdout.count('Unexpected keyword argument "order"') == 3
