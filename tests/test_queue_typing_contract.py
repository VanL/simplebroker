"""Static-only typing contract fixtures for the public Queue surface."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import assert_type

from simplebroker import MovedMessage, Queue

ROOT = Path(__file__).resolve().parents[1]


def assert_high_level_queue_types(queue: Queue, runtime_bool: bool) -> None:
    assert_type(queue.read(), str | None)
    assert_type(queue.read(with_timestamps=True), tuple[str, int] | None)
    assert_type(queue.read(all_messages=True), Iterator[str])
    assert_type(
        queue.read(all_messages=True, with_timestamps=True),
        Iterator[tuple[str, int]],
    )
    assert_type(
        queue.read(all_messages=runtime_bool),
        str | tuple[str, int] | Iterator[str | tuple[str, int]] | None,
    )

    assert_type(queue.peek(), str | None)
    assert_type(queue.peek(with_timestamps=True), tuple[str, int] | None)
    assert_type(queue.peek(all_messages=True), Iterator[str])
    assert_type(
        queue.peek(all_messages=True, with_timestamps=True),
        Iterator[tuple[str, int]],
    )
    assert_type(
        queue.peek(all_messages=runtime_bool),
        str | tuple[str, int] | Iterator[str | tuple[str, int]] | None,
    )

    assert_type(queue.move("destination"), MovedMessage | None)
    assert_type(queue.move("destination", all_messages=True), Iterator[MovedMessage])
    assert_type(
        queue.move("destination", all_messages=runtime_bool),
        MovedMessage | Iterator[MovedMessage] | None,
    )


def assert_granular_queue_types(queue: Queue, runtime_bool: bool) -> None:
    assert_type(queue.read_one(), str | None)
    assert_type(queue.read_one(with_timestamps=True), tuple[str, int] | None)
    assert_type(
        queue.read_one(with_timestamps=runtime_bool), str | tuple[str, int] | None
    )
    assert_type(queue.read_many(5), list[str])
    assert_type(queue.read_many(5, with_timestamps=True), list[tuple[str, int]])
    assert_type(
        queue.read_many(5, with_timestamps=runtime_bool),
        list[str] | list[tuple[str, int]],
    )
    assert_type(queue.read_generator(), Iterator[str])
    assert_type(queue.read_generator(with_timestamps=True), Iterator[tuple[str, int]])
    assert_type(
        queue.read_generator(with_timestamps=runtime_bool),
        Iterator[str | tuple[str, int]],
    )

    assert_type(queue.peek_one(), str | None)
    assert_type(queue.peek_one(with_timestamps=True), tuple[str, int] | None)
    assert_type(
        queue.peek_one(with_timestamps=runtime_bool), str | tuple[str, int] | None
    )
    assert_type(queue.peek_many(), list[str])
    assert_type(queue.peek_many(with_timestamps=True), list[tuple[str, int]])
    assert_type(
        queue.peek_many(with_timestamps=runtime_bool),
        list[str] | list[tuple[str, int]],
    )
    assert_type(queue.peek_generator(), Iterator[str])
    assert_type(queue.peek_generator(with_timestamps=True), Iterator[tuple[str, int]])
    assert_type(
        queue.peek_generator(with_timestamps=runtime_bool),
        Iterator[str | tuple[str, int]],
    )

    assert_type(queue.move_one("destination"), str | None)
    assert_type(
        queue.move_one("destination", with_timestamps=True),
        tuple[str, int] | None,
    )
    assert_type(
        queue.move_one("destination", with_timestamps=runtime_bool),
        str | tuple[str, int] | None,
    )
    assert_type(queue.move_many("destination", 5), list[str])
    assert_type(
        queue.move_many("destination", 5, with_timestamps=True),
        list[tuple[str, int]],
    )
    assert_type(
        queue.move_many("destination", 5, with_timestamps=runtime_bool),
        list[str] | list[tuple[str, int]],
    )
    assert_type(queue.move_generator("destination"), Iterator[str])
    assert_type(
        queue.move_generator("destination", with_timestamps=True),
        Iterator[tuple[str, int]],
    )
    assert_type(
        queue.move_generator("destination", with_timestamps=runtime_bool),
        Iterator[str | tuple[str, int]],
    )


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
