"""Behavior tests for the dump/load API (format v1, filters, round trip).

Real SQLite brokers under tmp_path, public API only, no mocks. The CLI and
cross-backend coverage lives in test_cli_dump_load.py and the extension test
dirs; this module pins the format and the library surface.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from simplebroker import LoadResult, Queue, dump_lines, load_lines, open_broker


def _db(tmp_path: Path, name: str = "src.db") -> str:
    return str(tmp_path / name)


def _seed(db: str) -> None:
    """Two queues + an alias + one claimed (consumed) message."""
    qa = Queue("alpha", db_path=db)
    qb = Queue("beta", db_path=db)
    for i in range(3):
        qa.write(f"a{i}")
    qb.write("b0")
    qb.write("line1\nline2")  # newline in body: the reason this is ndjson
    assert qa.read() == "a0"  # a0 becomes claimed: must NOT appear in dumps
    with qa.get_connection() as conn:
        conn.add_alias("al", "alpha")


def _records(lines: list[str]) -> list[dict[str, object]]:
    return [json.loads(line) for line in lines]


def test_dump_format_header_aliases_messages_in_order(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _seed(db)
    with open_broker(db) as broker:
        lines = list(dump_lines(broker))
    recs = _records(lines)

    header = recs[0]
    assert header["type"] == "header"
    assert header["format"] == "simplebroker-dump"
    assert header["version"] == 1
    assert header["backend"] == "sqlite"
    assert isinstance(header["last_ts"], int)

    assert [r["type"] for r in recs] == [
        "header",
        "alias",
        "message",
        "message",
        "message",
        "message",
    ]
    assert recs[1] == {"alias": "al", "target": "alpha", "type": "alias"}

    msgs = recs[2:]
    # pending only (a0 was claimed), queues sorted, ascending IDs within queue
    assert [(m["queue"], m["body"]) for m in msgs] == [
        ("alpha", "a1"),
        ("alpha", "a2"),
        ("beta", "b0"),
        ("beta", "line1\nline2"),
    ]
    ids = [m["id"] for m in msgs]
    assert all(isinstance(i, int) for i in ids)
    assert ids[0] < ids[1] and ids[2] < ids[3]
    # deterministic serialization: keys sorted in every line
    for line in lines:
        assert line == json.dumps(json.loads(line), ensure_ascii=False, sort_keys=True)


def test_round_trip_fixed_point(tmp_path: Path) -> None:
    src, dst = _db(tmp_path, "src.db"), _db(tmp_path, "dst.db")
    _seed(src)
    with open_broker(src) as broker:
        original = list(dump_lines(broker))
    with open_broker(dst) as broker:
        result = load_lines(broker, original)
        assert isinstance(result, LoadResult)
        assert result.messages == 4
        assert result.aliases == 1
        redump = list(dump_lines(broker))
    assert redump[1:] == original[1:]  # non-header lines identical (I-RT)

    # the restored broker behaves: FIFO order preserved, alias restored
    q = Queue("alpha", db_path=dst)
    assert q.read() == "a1"
    with open_broker(dst) as broker:
        assert broker.resolve_alias("al") == "alpha"

    # the watermark contract end-to-end: a write AFTER a restore always gets
    # an ID above every restored ID (insert_messages advanced last_ts; the
    # HLC's monotonicity does the rest, even under clock skew)
    restored_ids = [r["id"] for r in _records(redump)[1:] if r["type"] == "message"]
    q.write("post-restore")
    with open_broker(dst) as broker:
        rows = _records(list(dump_lines(broker)))[1:]
    new_ids = [
        r["id"] for r in rows if r["type"] == "message" and r["body"] == "post-restore"
    ]
    assert new_ids and min(new_ids) > max(restored_ids)


def test_load_accepts_exact_string_message_id(tmp_path: Path) -> None:
    db = _db(tmp_path)
    lines = [
        json.dumps({"type": "header", "format": "simplebroker-dump", "version": 1}),
        json.dumps(
            {
                "type": "message",
                "queue": "jobs",
                "body": "restored",
                "id": "0000000000000001000",
            }
        ),
    ]

    with open_broker(db) as broker:
        result = load_lines(broker, lines)

    assert result == LoadResult(messages=1, aliases=0)
    assert Queue("jobs", db_path=db).peek(message_id=1000) == "restored"


def test_load_rejects_malformed_message_id_with_line_context(tmp_path: Path) -> None:
    db = _db(tmp_path)
    lines = [
        json.dumps({"type": "header", "format": "simplebroker-dump", "version": 1}),
        json.dumps(
            {"type": "message", "queue": "jobs", "body": "bad", "id": "not-an-id"}
        ),
    ]

    with open_broker(db) as broker:
        with pytest.raises(ValueError, match="line 2: invalid message ID"):
            load_lines(broker, lines)

    assert Queue("jobs", db_path=db).peek() is None


def test_dump_canonicalizes_shuffled_exact_id_inserts(tmp_path: Path) -> None:
    """Exact-ID inserts make rowid order diverge from ID order; dump sorts."""
    db = _db(tmp_path)
    with open_broker(db) as broker:
        ids = [broker.generate_timestamp() for _ in range(3)]
        broker.insert_messages(
            [("q", "m2", ids[2]), ("q", "m0", ids[0]), ("q", "m1", ids[1])]
        )
        msgs = _records(list(dump_lines(broker)))[1:]
    assert [m["body"] for m in msgs] == ["m0", "m1", "m2"]
    assert [m["id"] for m in msgs] == sorted(ids)


def test_include_exclude_filters(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _seed(db)
    with open_broker(db) as broker:
        only_alpha = _records(list(dump_lines(broker, include=["alph*"])))
        no_alpha = _records(list(dump_lines(broker, exclude=["alph*"])))
        both = _records(
            list(dump_lines(broker, include=["alpha", "beta"], exclude=["beta"]))
        )

    assert {r["queue"] for r in only_alpha if r["type"] == "message"} == {"alpha"}
    assert any(r["type"] == "alias" for r in only_alpha)  # target alpha matches

    assert {r["queue"] for r in no_alpha if r["type"] == "message"} == {"beta"}
    assert not any(r["type"] == "alias" for r in no_alpha)  # alias target excluded

    assert {r["queue"] for r in both if r["type"] == "message"} == {"alpha"}


def test_alias_matches_on_its_own_name(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _seed(db)  # alias "al" -> "alpha"
    with open_broker(db) as broker:
        # include by ALIAS name: the alias record dumps, its target's
        # messages do not (the queue name "alpha" matches no include)
        by_alias = _records(list(dump_lines(broker, include=["al"])))
        # exclude by ALIAS name: alias gone, target queue's messages remain
        drop_alias = _records(list(dump_lines(broker, exclude=["al"])))
        # exclude wins across the pair: included by target, excluded by name
        exclude_wins = _records(
            list(dump_lines(broker, include=["alph*"], exclude=["al"]))
        )

    assert [r["type"] for r in by_alias] == ["header", "alias"]
    assert not any(r["type"] == "alias" for r in drop_alias)
    assert {r["queue"] for r in drop_alias if r["type"] == "message"} == {
        "alpha",
        "beta",
    }
    assert not any(r["type"] == "alias" for r in exclude_wins)
    assert {r["queue"] for r in exclude_wins if r["type"] == "message"} == {"alpha"}


def test_filters_are_case_sensitive(tmp_path: Path) -> None:
    db = _db(tmp_path)
    Queue("Alpha", db_path=db).write("x")
    with open_broker(db) as broker:
        assert not [
            r
            for r in _records(list(dump_lines(broker, include=["alpha"])))
            if r["type"] == "message"
        ]


def test_empty_broker_dumps_header_only_and_loads(tmp_path: Path) -> None:
    src, dst = _db(tmp_path, "src.db"), _db(tmp_path, "dst.db")
    with open_broker(src) as broker:  # open_broker creates the (empty) database
        lines = list(dump_lines(broker))
    assert len(lines) == 1 and json.loads(lines[0])["type"] == "header"
    with open_broker(dst) as broker:
        result = load_lines(broker, lines)
    assert (result.messages, result.aliases) == (0, 0)


def test_load_rejects_bad_input(tmp_path: Path) -> None:
    db = _db(tmp_path)
    header = json.dumps(
        {"type": "header", "format": "simplebroker-dump", "version": 1},
        sort_keys=True,
    )

    with open_broker(db) as broker:
        with pytest.raises(ValueError, match="header"):
            load_lines(broker, ['{"type": "message"}'])  # no header first
        with pytest.raises(ValueError, match="version"):
            load_lines(
                broker,
                [
                    json.dumps(
                        {
                            "type": "header",
                            "format": "simplebroker-dump",
                            "version": 2,
                        }
                    )
                ],
            )
        with pytest.raises(ValueError, match="line 2"):
            load_lines(broker, [header, "not json"])
        with pytest.raises(ValueError, match="line 2"):
            load_lines(broker, [header, '{"type": "mystery"}'])
        # field validation is strict: no coercion of nulls, bools, or malformed IDs
        with pytest.raises(ValueError, match="line 2"):
            load_lines(
                broker,
                [header, '{"type": "message", "queue": "q", "body": null, "id": 1}'],
            )
        with pytest.raises(ValueError, match="invalid message ID"):
            load_lines(
                broker,
                [header, '{"type": "message", "queue": "q", "body": "b", "id": true}'],
            )
        with pytest.raises(ValueError, match="invalid message ID"):
            load_lines(
                broker,
                [header, '{"type": "message", "queue": "q", "body": "b", "id": "1"}'],
            )
        with pytest.raises(ValueError, match="line 2"):
            load_lines(broker, [header, '{"type": "alias", "alias": "a"}'])
        # blank lines are tolerated (trailing-newline friendliness)
        result = load_lines(broker, [header, "", "\n"])
        assert result.messages == 0


def test_reloading_same_dump_fails_loudly(tmp_path: Path) -> None:
    """Fresh-target semantics: duplicate IDs raise, never silently double-insert."""
    from simplebroker.ext import IntegrityError

    src, dst = _db(tmp_path, "src.db"), _db(tmp_path, "dst.db")
    Queue("q", db_path=src).write("once")
    with open_broker(src) as broker:
        lines = list(dump_lines(broker))
    with open_broker(dst) as broker:
        load_lines(broker, lines)
        with pytest.raises(IntegrityError):
            load_lines(broker, lines)
