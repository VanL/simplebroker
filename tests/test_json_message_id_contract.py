"""Enumerable firing coverage for SimpleBroker-owned JSON identity fields."""

from __future__ import annotations

import ast
import json
import re
from collections import Counter
from pathlib import Path

import pytest

from simplebroker import Queue, dump_lines, format_message_id, open_broker
from simplebroker.commands import _output_message, cmd_status, cmd_write
from simplebroker.watcher import json_print_handler

UNSAFE_MESSAGE_ID = 1234567890123456789

ROOT = Path(__file__).resolve().parents[1]
IDENTITY_FIELD_NAMES = frozenset({"timestamp", "last_timestamp", "last_ts", "id"})


class _IdentityFieldVisitor(ast.NodeVisitor):
    def __init__(
        self,
        relative_path: str,
        observed: Counter[tuple[str, str, str, bool]],
    ) -> None:
        self.relative_path = relative_path
        self.observed = observed
        self.function_stack: list[str] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_Dict(self, node: ast.Dict) -> None:
        for key, value in zip(node.keys, node.values, strict=True):
            if (
                isinstance(key, ast.Constant)
                and isinstance(key.value, str)
                and key.value in IDENTITY_FIELD_NAMES
            ):
                is_formatted = (
                    isinstance(value, ast.Call)
                    and isinstance(value.func, ast.Name)
                    and value.func.id == "format_message_id"
                )
                self.observed[
                    (
                        self.relative_path,
                        self.function_stack[-1] if self.function_stack else "<module>",
                        key.value,
                        is_formatted,
                    )
                ] += 1
        self.generic_visit(node)


def _assert_identity_token(raw: str, field: str, expected: int) -> None:
    payload = json.loads(raw)
    value = payload[field]
    assert type(value) is str
    assert re.fullmatch(r"[0-9]{19}", value)
    assert value.isascii()
    assert int(value) == expected
    assert re.search(rf'"{field}"\s*:\s*"[0-9]{{19}}"', raw)


def test_core_identity_dict_fields_are_exhaustively_classified() -> None:
    """A new identity-looking core field must choose wire string or domain int."""
    observed: Counter[tuple[str, str, str, bool]] = Counter()

    for path in sorted((ROOT / "simplebroker").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        _IdentityFieldVisitor(path.relative_to(ROOT).as_posix(), observed).visit(tree)

    expected = Counter(
        {
            ("simplebroker/commands.py", "_output_message", "timestamp", True): 1,
            ("simplebroker/commands.py", "cmd_write", "timestamp", True): 1,
            (
                "simplebroker/commands.py",
                "cmd_status",
                "last_timestamp",
                True,
            ): 1,
            ("simplebroker/_dump.py", "dump_lines", "last_ts", True): 1,
            ("simplebroker/_dump.py", "dump_lines", "id", True): 1,
            (
                "simplebroker/watcher.py",
                "json_print_handler",
                "timestamp",
                True,
            ): 1,
            ("simplebroker/db.py", "status", "last_timestamp", False): 1,
            ("simplebroker/sbqueue.py", "move", "timestamp", False): 3,
            (
                "simplebroker/sbqueue.py",
                "dict_generator",
                "timestamp",
                False,
            ): 1,
        }
    )
    assert observed == expected


def test_shared_message_line_formats_id_without_rewriting_body(
    capsys: pytest.CaptureFixture[str],
) -> None:
    body = '{"timestamp":42,"id":7}'
    _output_message(body, UNSAFE_MESSAGE_ID, True, False, False)
    raw = capsys.readouterr().out

    _assert_identity_token(raw, "timestamp", UNSAFE_MESSAGE_ID)
    assert json.loads(raw)["message"] == body


def test_write_and_status_format_only_their_json_boundary(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db = str(tmp_path / "boundary.db")
    assert cmd_write(db, "q", "body", json_output=True) == 0
    write_raw = capsys.readouterr().out
    write_id = int(json.loads(write_raw)["timestamp"])
    assert write_id > 2**53
    _assert_identity_token(write_raw, "timestamp", write_id)

    assert cmd_status(db, json_output=True) == 0
    status_raw = capsys.readouterr().out
    _assert_identity_token(status_raw, "last_timestamp", write_id)
    status = json.loads(status_raw)
    assert type(status["total_messages"]) is int
    assert type(status["db_size"]) is int


def test_dump_formats_header_and_message_identity_fields(tmp_path: Path) -> None:
    db = str(tmp_path / "dump.db")
    with Queue("q", db_path=db) as queue:
        queue.insert_messages([("body", UNSAFE_MESSAGE_ID)])
    with open_broker(db) as broker:
        header_raw, message_raw = list(dump_lines(broker))

    _assert_identity_token(header_raw, "last_ts", UNSAFE_MESSAGE_ID + 1)
    _assert_identity_token(message_raw, "id", UNSAFE_MESSAGE_ID)


def test_watcher_helper_formats_message_identity(
    capsys: pytest.CaptureFixture[str],
) -> None:
    json_print_handler("body", UNSAFE_MESSAGE_ID)
    raw = capsys.readouterr().out
    _assert_identity_token(raw, "timestamp", UNSAFE_MESSAGE_ID)


def test_adjacent_unsafe_ids_remain_distinct_after_json_parse() -> None:
    first = json.loads(json.dumps({"timestamp": format_message_id(2**53)}))["timestamp"]
    second = json.loads(json.dumps({"timestamp": format_message_id(2**53 + 1)}))[
        "timestamp"
    ]
    assert first != second
