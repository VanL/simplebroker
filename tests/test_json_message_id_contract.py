"""Enumerable firing coverage for SimpleBroker-owned JSON identity fields."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from simplebroker import Queue, dump_lines, format_message_id, open_broker
from simplebroker.commands import _output_message, cmd_status, cmd_write
from simplebroker.watcher import json_print_handler

from .conftest import run_cli

UNSAFE_MESSAGE_ID = 1234567890123456789


def _assert_identity_token(raw: str, field: str, expected: int) -> None:
    payload = json.loads(raw)
    value = payload[field]
    assert type(value) is str
    assert re.fullmatch(r"[0-9]{19}", value)
    assert value.isascii()
    assert int(value) == expected
    assert re.search(rf'"{field}"\s*:\s*"[0-9]{{19}}"', raw)


@pytest.mark.sqlite_only
def test_public_json_identity_producers_preserve_message_ids(
    workdir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Public JSON producers preserve integer identity as 19 ASCII digits."""
    db = workdir / "identity.db"
    exact_ids = {
        "peek": UNSAFE_MESSAGE_ID,
        "read": UNSAFE_MESSAGE_ID + 1,
        "move": UNSAFE_MESSAGE_ID + 2,
        "dump": UNSAFE_MESSAGE_ID + 3,
    }
    for queue_name, message_id in exact_ids.items():
        with Queue(queue_name, db_path=str(db)) as queue:
            queue.insert_messages([(queue_name, message_id)])

    raw_outputs: list[tuple[str, str, int]] = []
    for command in ("peek", "read"):
        rc, out, err = run_cli("-f", str(db), command, command, "--json", cwd=workdir)
        assert rc == 0, err
        raw_outputs.append((out, "timestamp", exact_ids[command]))

    rc, out, err = run_cli(
        "-f", str(db), "move", "move", "moved", "--json", cwd=workdir
    )
    assert rc == 0, err
    raw_outputs.append((out, "timestamp", exact_ids["move"]))

    rc, out, err = run_cli("-f", str(db), "dump", cwd=workdir)
    assert rc == 0, err
    dump_lines_output = out.splitlines()
    with Queue("dump", db_path=str(db)) as queue:
        expected_last_ts = queue.last_ts
    assert expected_last_ts is not None
    raw_outputs.append((dump_lines_output[0], "last_ts", expected_last_ts))
    parsed_dump = [(line, json.loads(line)) for line in dump_lines_output]
    dump_record = next(
        line
        for line, record in parsed_dump
        if record.get("type") == "message" and record["queue"] == "dump"
    )
    raw_outputs.append((dump_record, "id", exact_ids["dump"]))

    rc, write_out, err = run_cli(
        "-f", str(db), "write", "write", "body", "--json", cwd=workdir
    )
    assert rc == 0, err
    write_id = int(json.loads(write_out)["timestamp"])
    raw_outputs.append((write_out, "timestamp", write_id))

    rc, status_out, err = run_cli("-f", str(db), "--status", "--json", cwd=workdir)
    assert rc == 0, err
    raw_outputs.append((status_out, "last_timestamp", write_id))

    json_print_handler("watch", exact_ids["peek"])
    raw_outputs.append((capsys.readouterr().out, "timestamp", exact_ids["peek"]))

    for raw, field, expected in raw_outputs:
        _assert_identity_token(raw, field, expected)


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
