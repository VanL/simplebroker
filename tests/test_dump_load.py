"""Behavior tests for the dump/load API (format v1, filters, round trip).

Real SQLite brokers under tmp_path, public API only, no mocks. The CLI and
cross-backend coverage lives in test_cli_dump_load.py and the extension test
dirs; this module pins the format and the library surface.
"""

from __future__ import annotations

import json
import re
import warnings
from pathlib import Path
from typing import Any, cast

import pytest

from simplebroker import (
    DumpClockSkewWarning,
    LoadResult,
    Queue,
    dump_lines,
    load_lines,
    open_broker,
)
from simplebroker._constants import LOGICAL_COUNTER_MASK, NS_PER_SECOND
from simplebroker.ext import OperationalError, TimestampError


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


def _load_header(last_ts: int = 0) -> str:
    return json.dumps(
        {
            "type": "header",
            "format": "simplebroker-dump",
            "version": 1,
            "last_ts": f"{last_ts:019d}",
        }
    )


def test_load_rejects_incompatible_broker_before_consuming_input() -> None:
    consumed = False

    def lines() -> Any:
        nonlocal consumed
        consumed = True
        yield _load_header()

    with pytest.raises(TypeError, match="advance_last_timestamp"):
        load_lines(object(), lines())  # type: ignore[arg-type]

    assert consumed is False


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
    assert isinstance(header["last_ts"], str)
    assert re.fullmatch(r"[0-9]{19}", header["last_ts"])

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
    assert all(isinstance(i, str) and re.fullmatch(r"[0-9]{19}", i) for i in ids)
    string_ids = [i for i in ids if isinstance(i, str)]
    assert string_ids[0] < string_ids[1] and string_ids[2] < string_ids[3]
    # deterministic serialization: keys sorted in every line
    for line in lines:
        assert line == json.dumps(json.loads(line), ensure_ascii=False, sort_keys=True)


@pytest.mark.shared
def test_dump_header_is_inclusive_message_id_bound(broker: Any) -> None:
    at_header = broker.write("jobs", "at header")
    lines = dump_lines(broker)
    header = json.loads(next(lines))
    assert int(header["last_ts"]) == at_header

    after_header = broker.write("jobs", "after header")
    assert after_header > at_header
    records = _records(list(lines))

    assert [(record["body"], int(cast(str, record["id"]))) for record in records] == [
        ("at header", at_header)
    ]


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
    restored_ids = [
        int(message_id)
        for record in _records(redump)[1:]
        if record["type"] == "message"
        if isinstance(message_id := record["id"], str)
    ]
    q.write("post-restore")
    with open_broker(dst) as broker:
        rows = _records(list(dump_lines(broker)))[1:]
    new_ids = [
        int(message_id)
        for record in rows
        if record["type"] == "message" and record["body"] == "post-restore"
        if isinstance(message_id := record["id"], str)
    ]
    assert new_ids and min(new_ids) > max(restored_ids)


def test_load_accepts_exact_string_message_id(tmp_path: Path) -> None:
    db = _db(tmp_path)
    lines = [
        _load_header(1000),
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


def test_load_warns_and_proceeds_at_future_skew_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    header = 1_700_000_000_000_000_000 & ~LOGICAL_COUNTER_MASK
    now_ns = header - 300 * NS_PER_SECOND
    monkeypatch.setattr("simplebroker._dump.time.time_ns", lambda: now_ns)

    with (
        open_broker(_db(tmp_path)) as broker,
        pytest.warns(DumpClockSkewWarning, match="300") as caught,
    ):
        result = load_lines(broker, [_load_header(header)])

    assert result == LoadResult(messages=0, aliases=0)
    warning = str(caught[0].message)
    assert f"{header:019d}" in warning
    assert "at most 4095 broker-global generated IDs" in warning


def test_load_clock_skew_uses_physical_grain_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now_ns = 1_700_000_000_000_000_000 & ~LOGICAL_COUNTER_MASK
    monkeypatch.setattr("simplebroker._dump.time.time_ns", lambda: now_ns)

    with open_broker(_db(tmp_path, "current.db")) as broker:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            load_lines(broker, [_load_header(now_ns | LOGICAL_COUNTER_MASK)])
        assert caught == []

    with open_broker(_db(tmp_path, "past.db")) as broker:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            load_lines(broker, [_load_header(now_ns - LOGICAL_COUNTER_MASK - 1)])
        assert caught == []

    with (
        open_broker(_db(tmp_path, "future.db")) as broker,
        pytest.warns(DumpClockSkewWarning, match="0.000"),
    ):
        load_lines(broker, [_load_header(now_ns + LOGICAL_COUNTER_MASK + 1)])


def test_load_rejects_excessive_future_skew_before_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    header = 1_700_000_000_000_000_000 & ~LOGICAL_COUNTER_MASK
    now_ns = header - 301 * NS_PER_SECOND
    monkeypatch.setattr("simplebroker._dump.time.time_ns", lambda: now_ns)
    lines = [
        _load_header(header),
        json.dumps({"type": "alias", "alias": "work", "target": "jobs"}),
    ]

    with open_broker(_db(tmp_path)) as broker:
        with (
            pytest.warns(DumpClockSkewWarning),
            pytest.raises(ValueError, match="exceeds configured maximum"),
        ):
            load_lines(broker, lines)
        assert broker.list_aliases() == []
        assert broker.refresh_last_timestamp() == 0


def test_load_force_warns_and_accepts_excessive_future_skew(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    header = 1_700_000_000_000_000_000 & ~LOGICAL_COUNTER_MASK
    now_ns = header - 301 * NS_PER_SECOND
    monkeypatch.setattr("simplebroker._dump.time.time_ns", lambda: now_ns)

    with (
        open_broker(_db(tmp_path)) as broker,
        pytest.warns(DumpClockSkewWarning),
    ):
        result = load_lines(broker, [_load_header(header)], force=True)

    assert result == LoadResult(messages=0, aliases=0)
    with open_broker(_db(tmp_path)) as broker:
        assert broker.refresh_last_timestamp() >= header


def test_load_typed_config_override_changes_skew_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    header = 1_700_000_000_000_000_000 & ~LOGICAL_COUNTER_MASK
    now_ns = header - NS_PER_SECOND
    monkeypatch.setattr("simplebroker._dump.time.time_ns", lambda: now_ns)

    with (
        open_broker(_db(tmp_path)) as broker,
        pytest.warns(DumpClockSkewWarning),
        pytest.raises(ValueError, match="configured maximum of 0 seconds"),
    ):
        load_lines(
            broker,
            [_load_header(header)],
            config={"BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS": 0},
        )


def test_load_accepts_legacy_integer_message_id(tmp_path: Path) -> None:
    db = _db(tmp_path)
    lines = [
        _load_header(1000),
        '{"type":"message","queue":"jobs","body":"legacy","id":1000}',
    ]

    with open_broker(db) as broker:
        result = load_lines(broker, lines)

    assert result == LoadResult(messages=1, aliases=0)
    assert Queue("jobs", db_path=db).peek(message_id=1000) == "legacy"


def test_load_accepts_legacy_integer_header_last_ts(tmp_path: Path) -> None:
    db = _db(tmp_path)
    header_floor = 1000
    header = json.dumps(
        {
            "type": "header",
            "format": "simplebroker-dump",
            "version": 1,
            "last_ts": header_floor,
        }
    )

    with open_broker(db) as broker:
        assert load_lines(broker, [header]) == LoadResult(messages=0, aliases=0)
        assert broker.refresh_last_timestamp() == header_floor


def test_header_only_load_restores_last_timestamp_floor(tmp_path: Path) -> None:
    db = _db(tmp_path)
    high_water = 1_700_000_000_000_000_000
    lines = [
        json.dumps(
            {
                "type": "header",
                "format": "simplebroker-dump",
                "version": 1,
                "last_ts": f"{high_water:019d}",
            }
        )
    ]

    with open_broker(db) as broker:
        result = load_lines(broker, lines)
        assert result == LoadResult(messages=0, aliases=0)
        assert broker.refresh_last_timestamp() >= high_water
        assert broker.write("jobs", "after restore") > high_water


def test_load_header_floor_persists_when_local_cache_is_ahead(tmp_path: Path) -> None:
    db = _db(tmp_path)
    with open_broker(db) as broker:
        internal_broker = cast(Any, broker)
        cached_candidate = internal_broker._timestamp_gen._reserve_candidates(1)[0]
        header_floor = cached_candidate - 1
        assert broker.refresh_last_timestamp() == 0
        internal_broker._timestamp_gen._last_ts = cached_candidate
        internal_broker._timestamp_gen._initialized = True

        assert load_lines(broker, [_load_header(header_floor)]) == LoadResult(
            messages=0,
            aliases=0,
        )
        assert broker.refresh_last_timestamp() >= header_floor


def test_load_header_floor_observes_concurrent_durable_winner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _db(tmp_path)
    header_floor = 1000
    competing_floor = 2000
    with open_broker(db) as broker:
        internal_broker = cast(Any, broker)
        plugin = internal_broker._timestamp_gen._backend_plugin
        original_advance = plugin.advance_last_ts

        def advance_then_compete(runner: Any, *, new_ts: int) -> bool:
            advanced = original_advance(runner, new_ts=new_ts)
            internal_broker._runner.run(
                "UPDATE meta SET value = ? WHERE key = 'last_ts'",
                (competing_floor,),
            )
            internal_broker._runner.commit()
            return bool(advanced)

        monkeypatch.setattr(plugin, "advance_last_ts", advance_then_compete)

        assert load_lines(broker, [_load_header(header_floor)]) == LoadResult(
            messages=0,
            aliases=0,
        )
        assert broker.get_cached_last_timestamp() == competing_floor
        assert broker.refresh_last_timestamp() == competing_floor


def test_load_header_floor_final_read_failure_is_outcome_ambiguous(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _db(tmp_path)
    header_floor = 1000
    with open_broker(db) as broker:
        assert broker.refresh_last_timestamp() == 0
        plugin = cast(Any, broker)._timestamp_gen._backend_plugin

        def fail_final_read(_runner: Any) -> int:
            raise OperationalError("forced final read failure")

        with monkeypatch.context() as patcher:
            patcher.setattr(plugin, "read_last_ts", fail_final_read)
            with pytest.raises(
                TimestampError,
                match="durable outcome is unknown",
            ):
                load_lines(broker, [_load_header(header_floor)])

        assert broker.refresh_last_timestamp() == header_floor


def test_claimed_future_exact_ids_survive_as_header_floor(tmp_path: Path) -> None:
    src = _db(tmp_path, "src.db")
    dst = _db(tmp_path, "dst.db")
    future_base = 2_000_000_000_000_000_000
    inserted_ids = [future_base + offset for offset in range(10)]
    with open_broker(src) as broker:
        broker.insert_messages(
            ("jobs", f"future-{offset}", message_id)
            for offset, message_id in enumerate(inserted_ids)
        )
        assert len(broker.claim_many("jobs", limit=10)) == 10
        lines = list(dump_lines(broker))
        header_floor = int(json.loads(lines[0])["last_ts"])

    assert len(lines) == 1
    assert header_floor > max(inserted_ids)
    with open_broker(dst) as broker:
        with pytest.warns(DumpClockSkewWarning):
            assert load_lines(broker, lines, force=True) == LoadResult(
                messages=0, aliases=0
            )
        assert broker.refresh_last_timestamp() >= header_floor
        assert broker.write("jobs", "after restore") > header_floor


@pytest.mark.shared
def test_load_rejects_records_newer_than_header_bound(broker: Any) -> None:
    header_floor = 1000
    newer_id = 2000
    lines = [
        json.dumps(
            {
                "type": "header",
                "format": "simplebroker-dump",
                "version": 1,
                "last_ts": f"{header_floor:019d}",
            }
        ),
        json.dumps(
            {
                "type": "message",
                "queue": "jobs",
                "body": "newer than header sample",
                "id": f"{newer_id:019d}",
            }
        ),
    ]

    with pytest.raises(ValueError, match=r"line 2: message id exceeds header"):
        load_lines(broker, lines)
    assert broker.peek_one("jobs", exact_timestamp=newer_id) is None


@pytest.mark.parametrize("last_ts", ["1", -1, True, None, 1.0, 2**63])
def test_load_rejects_invalid_header_last_ts_with_line_context(
    tmp_path: Path,
    last_ts: object,
) -> None:
    header = json.dumps(
        {
            "type": "header",
            "format": "simplebroker-dump",
            "version": 1,
            "last_ts": last_ts,
        }
    )

    with (
        open_broker(_db(tmp_path)) as broker,
        pytest.raises(ValueError, match="line 1: invalid header last_ts"),
    ):
        load_lines(broker, [header])


def test_load_rejects_header_without_last_ts(tmp_path: Path) -> None:
    header = json.dumps({"type": "header", "format": "simplebroker-dump", "version": 1})

    with (
        open_broker(_db(tmp_path)) as broker,
        pytest.raises(ValueError, match="line 1: header requires 'last_ts'"),
    ):
        load_lines(broker, [header])


@pytest.mark.parametrize(
    "id_token",
    [
        '"1"',
        "-1",
        "true",
        "null",
        "1.0",
        "1e3",
        str(2**63),
        "10000000000000000000",
    ],
)
def test_load_rejects_noncanonical_message_id_tokens_with_line_context(
    tmp_path: Path,
    id_token: str,
) -> None:
    header = _load_header()
    record = f'{{"type":"message","queue":"q","body":"b","id":{id_token}}}'

    with (
        open_broker(_db(tmp_path)) as broker,
        pytest.raises(ValueError, match="line 2: invalid message ID"),
    ):
        load_lines(broker, [header, record])


def test_load_rejects_malformed_message_id_with_line_context(tmp_path: Path) -> None:
    db = _db(tmp_path)
    lines = [
        _load_header(1000),
        json.dumps(
            {"type": "message", "queue": "jobs", "body": "bad", "id": "not-an-id"}
        ),
    ]

    with (
        open_broker(db) as broker,
        pytest.raises(ValueError, match="line 2: invalid message ID"),
    ):
        load_lines(broker, lines)

    assert Queue("jobs", db_path=db).peek() is None


def test_load_rejects_huge_json_integer_with_line_context(tmp_path: Path) -> None:
    header = _load_header()
    huge_integer = "9" * 5000
    target = tmp_path / "huge-integer.db"
    with (
        open_broker(str(target)) as broker,
        pytest.raises(ValueError, match="invalid dump input at line 2"),
    ):
        load_lines(
            broker,
            [
                header,
                '{"type":"message","queue":"q","body":"b","id":' + huge_integer + "}",
            ],
        )


@pytest.mark.shared
def test_load_rejects_reserved_zero_with_line_context_before_batch_flush(
    broker: Any,
) -> None:
    lines = [
        _load_header(1000),
        json.dumps({"type": "message", "queue": "jobs", "body": "valid", "id": 1000}),
        json.dumps({"type": "message", "queue": "jobs", "body": "legacy", "id": 0}),
    ]

    with pytest.raises(
        ValueError,
        match="line 3: message_id 0 is reserved",
    ):
        load_lines(broker, lines)

    assert broker.refresh_last_timestamp() == 0
    assert broker.peek_one("jobs", exact_timestamp=1000) is None
    assert broker.peek_one("jobs", exact_timestamp=0) is None


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
    assert [m["id"] for m in msgs] == [
        f"{message_id:019d}" for message_id in sorted(ids)
    ]


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
    header = _load_header()

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
