"""Structural and behavioral binds for ``[SB-SELECT-*]`` and ``[SB-CLI-5]``."""

from __future__ import annotations

import ast
import inspect
import re
from pathlib import Path

import pytest

from simplebroker import Queue, commands
from simplebroker._constants import EXIT_SUCCESS

from .conftest import run_cli

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs" / "specs" / "14-timestamp-selection.md"
CLI_SPEC = ROOT / "docs" / "specs" / "10-cli.md"
REGISTRY = ROOT / "docs" / "specs" / "product-section-registry.md"
SPEC_INDEX = ROOT / "docs" / "specs" / "00-specs-index.md"
README = ROOT / "README.md"
KERNEL = ROOT / "docs" / "agent-kernel.md"
LLMS = ROOT / "llms.txt"

AFFECTED_EVIDENCE = {
    "SB-SELECT-1": {
        "tests/test_timestamp_selection_contract_sb_select.py": {
            "test_strict_open_bounds_on_queue_api"
        },
        "tests/test_after_flag.py": {"test_after_boundary_is_strictly_greater"},
        "tests/test_generator_methods.py": {
            "TestGeneratorMethods::test_generator_with_after_timestamp"
        },
        "tests/test_watcher.py": {
            "TestQueueWatcher::test_explicit_zero_after_timestamp_excludes_legacy_zero"
        },
    },
    "SB-SELECT-4": {
        "tests/test_timestamp_selection_contract_sb_select.py": {
            "test_select_watch_progress"
        },
        "tests/test_watcher.py": {
            "TestQueueWatcher::test_peek_handler_failure_does_not_advance_checkpoint",
            "TestQueueWatcher::test_explicit_zero_after_timestamp_excludes_legacy_zero",
        },
    },
    "SB-SELECT-5": {
        "tests/test_sqlite_message_id_returning_order.py": {
            "test_claim_many_normalizes_sqlite_returning_rows_by_public_id",
            "test_claim_generator_uses_ascending_ids_when_returning_rows_are_reversed",
            "test_move_many_normalizes_sqlite_returning_rows_by_public_id",
            "test_move_generator_uses_ascending_ids_when_returning_rows_are_reversed",
        },
        "tests/test_timestamp_selection_contract_sb_select.py": {
            "test_bounded_peek_orders_by_public_message_id",
            "test_bounded_one_and_many_order_matrix",
            "test_invalid_or_unbounded_order_fails_before_target_acquisition",
            "test_generator_signatures_do_not_expose_order",
            "test_direct_command_accepts_normalized_newest_order",
            "test_direct_command_rejects_newest_all_before_target_resolution",
        },
    },
}


def _section(code: str) -> str:
    text = SPEC.read_text(encoding="utf-8")
    match = re.search(
        rf"^## .+ \[{re.escape(code)}\]\n(?P<body>.*?)(?=^## |\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"missing section {code}"
    return match.group("body")


def _verification_row(code: str) -> str:
    prefix = f"| [{code}] |"
    return next(
        line
        for line in SPEC.read_text(encoding="utf-8").splitlines()
        if line.startswith(prefix)
    )


def _cited_nodes(row: str) -> dict[str, set[str]]:
    citations: dict[str, set[str]] = {}
    python_citations = re.findall(r"`([^`]+\.py(?:[^`]*)?)`", row)
    for citation in python_citations:
        match = re.fullmatch(
            r"(?P<path>[^`]+\.py)::(?P<node>[A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)*)",
            citation,
        )
        assert match is not None, f"Python evidence must cite an AST node: {citation}"
        citations.setdefault(match.group("path"), set()).add(match.group("node"))
    return citations


def _test_nodes(relative_path: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    nodes = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            nodes.update(
                f"{node.name}::{child.name}"
                for child in node.body
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
            )
    return nodes


def test_select_clause_inventory_and_authority() -> None:
    text = SPEC.read_text(encoding="utf-8")
    codes = re.findall(r"^## .+ \[SB-SELECT-(\d+)\]$", text, re.MULTILINE)
    assert codes == ["1", "2", "3", "4", "5"]
    for number in ("1", "2", "3", "4"):
        assert f"[SB-SELECT-{number}]" in text
        assert f"| [SB-SELECT-{number}] |" in text

    registry = REGISTRY.read_text(encoding="utf-8")
    assert "14-timestamp-selection.md" in registry
    assert "`canonical-spec`" in registry
    assert "[SB-SELECT-1]" in registry
    assert "[SB-SELECT-5]" in registry
    assert "[SB-CLI-5]" in registry

    assert "14-timestamp-selection.md" in SPEC_INDEX.read_text(encoding="utf-8")

    for path in (README, KERNEL, LLMS):
        surface = path.read_text(encoding="utf-8")
        assert "docs/specs/14-timestamp-selection.md" in surface
        assert "[SB-SELECT-1]" in surface or "SB-SELECT" in surface

    cli = CLI_SPEC.read_text(encoding="utf-8")
    assert "[SB-CLI-5]" in cli
    assert "ISO 8601" in cli
    assert "1705329000s" in cli or "Unix seconds" in cli


def test_select_affected_evidence_rows_match_exact_executable_manifests() -> None:
    for code, manifest in AFFECTED_EVIDENCE.items():
        citations = _cited_nodes(_verification_row(code))
        assert citations == manifest
        for relative_path, nodes in citations.items():
            assert nodes <= _test_nodes(relative_path)


def test_select_predicates_are_strict_open_bounds() -> None:
    body = _section("SB-SELECT-1")
    assert "message_id > after_timestamp" in body
    assert "message_id < before_timestamp" in body


def test_select_filter_not_stream_offset() -> None:
    body = _section("SB-SELECT-2")
    assert "pure filter" in body.lower() or "pure filter" in body
    assert "not" in body.lower()
    assert "stream offset" in body.lower() or "complete stream" in body.lower()


def test_select_late_older_ids() -> None:
    body = _section("SB-SELECT-3")
    assert "move" in body.lower()
    assert "exact" in body.lower()
    assert "behind" in body.lower() or "not selected" in body.lower()


def test_select_watch_progress() -> None:
    body = _section("SB-SELECT-4")
    assert "as they come" in body.lower() or "as they" in body.lower()
    assert "progress" in body.lower()


def test_strict_open_bounds_on_queue_api(queue_factory) -> None:
    """[SB-SELECT-1] Integer bounds are strict open intervals."""
    q = queue_factory("select_bounds")
    ids = [q.write(f"m{i}") for i in range(3)]
    mid = ids[1]

    after_mid = q.peek_many(
        limit=10,
        after_timestamp=mid,
        with_timestamps=True,
    )
    assert [ts for _, ts in after_mid] == [ids[2]]

    before_mid = q.peek_many(
        limit=10,
        before_timestamp=mid,
        with_timestamps=True,
    )
    assert [ts for _, ts in before_mid] == [ids[0]]


def test_bounded_peek_orders_by_public_message_id(queue_factory) -> None:
    """[SB-SELECT-5] Bounded selection uses public IDs in either direction."""
    q = queue_factory("select_order")
    q.insert_messages(
        [
            ("inserted-first", 300),
            ("inserted-second", 100),
            ("inserted-third", 200),
        ]
    )

    assert q.peek_one(with_timestamps=True) == ("inserted-second", 100)
    assert q.peek_one(order="newest", with_timestamps=True) == (
        "inserted-first",
        300,
    )


def _insert_out_of_order(queue: Queue, *, base: int = 0) -> None:
    queue.insert_messages(
        [
            ("id-300", base + 300),
            ("id-100", base + 100),
            ("id-200", base + 200),
        ]
    )


def test_bounded_one_and_many_order_matrix(queue_factory) -> None:
    peek = queue_factory("select_peek_matrix")
    _insert_out_of_order(peek)
    assert [ts for _, ts in peek.peek_many(2, with_timestamps=True)] == [100, 200]
    assert [
        ts
        for _, ts in peek.peek_many(
            2,
            with_timestamps=True,
            order="newest",
        )
    ] == [300, 200]
    assert peek.peek(
        after_timestamp=100,
        before_timestamp=300,
        with_timestamps=True,
        order="newest",
    ) == ("id-200", 200)
    assert peek.peek_one(
        exact_timestamp=200,
        with_timestamps=True,
        order="newest",
    ) == ("id-200", 200)

    read_one = queue_factory("select_read_one")
    _insert_out_of_order(read_one, base=1_000)
    assert read_one.read_one(order="newest", with_timestamps=True) == (
        "id-300",
        1_300,
    )

    read_many = queue_factory("select_read_many")
    _insert_out_of_order(read_many, base=2_000)
    assert [
        ts
        for _, ts in read_many.read_many(
            2,
            with_timestamps=True,
            order="newest",
        )
    ] == [2_300, 2_200]

    move_one = queue_factory("select_move_one")
    _insert_out_of_order(move_one, base=3_000)
    assert move_one.move_one(
        "select_move_one_dest",
        order="newest",
        with_timestamps=True,
    ) == ("id-300", 3_300)

    move_many = queue_factory("select_move_many")
    _insert_out_of_order(move_many, base=4_000)
    assert [
        ts
        for _, ts in move_many.move_many(
            "select_move_many_dest",
            2,
            with_timestamps=True,
            order="newest",
        )
    ] == [4_300, 4_200]


def test_invalid_or_unbounded_order_fails_before_target_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = Queue("select_pre_target", db_path="must-not-open.db")

    def fail_acquisition(*args, **kwargs):
        raise AssertionError("invalid order reached target acquisition")

    monkeypatch.setattr(Queue, "get_connection", fail_acquisition)
    for operation in (queue.read_one, queue.peek_one):
        with pytest.raises(ValueError, match="'oldest'.*'newest'"):
            operation(order="NEWEST")
    with pytest.raises(ValueError, match="'oldest'.*'newest'"):
        queue.move_one("destination", order="NEWEST")
    for operation in (queue.read, queue.peek):
        with pytest.raises(ValueError, match="all_messages"):
            operation(all_messages=True, order="newest")
    with pytest.raises(ValueError, match="all_messages"):
        queue.move("destination", all_messages=True, order="newest")


def test_generator_signatures_do_not_expose_order() -> None:
    for method in (Queue.read_generator, Queue.peek_generator, Queue.move_generator):
        assert "order" not in inspect.signature(method).parameters


def test_direct_command_accepts_normalized_newest_order(
    workdir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db = workdir / "direct-order.db"
    with Queue("q", db_path=str(db)) as queue:
        _insert_out_of_order(queue)

    assert commands.cmd_peek(str(db), "q", order="newest") == EXIT_SUCCESS
    assert capsys.readouterr().out == "id-300\n"


def test_direct_command_rejects_newest_all_before_target_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_resolution(*args, **kwargs):
        raise AssertionError("invalid order reached target resolution")

    monkeypatch.setattr(commands, "_resolve_alias_name", fail_resolution)
    for command, args in (
        (commands.cmd_read, ("must-not-open.db", "q")),
        (commands.cmd_peek, ("must-not-open.db", "q")),
        (commands.cmd_move, ("must-not-open.db", "q", "dest")),
    ):
        with pytest.raises(ValueError, match="all_messages"):
            command(*args, all_messages=True, order="newest")


def test_move_behind_lower_bound_is_invisible_to_filter(queue_factory) -> None:
    """[SB-SELECT-3] Filter with L hides moved-in ids <= L until bound is lowered."""
    source = queue_factory("select_src")
    dest = queue_factory("select_dst")
    older_id = source.write("older")
    newer_id = dest.write("already-here")
    assert older_id < newer_id

    # Consumer bound sits at newer_id.
    assert dest.peek_many(limit=10, after_timestamp=newer_id) == []

    moved = source.move_one(dest.name, exact_timestamp=older_id, with_timestamps=True)
    assert moved is not None
    assert moved[1] == older_id

    # Unchanged lower bound does not select the moved older id.
    assert dest.peek_many(limit=10, after_timestamp=newer_id) == []
    # Without the high lower bound, the older message is still pending at dest.
    assert dest.peek_one(exact_timestamp=older_id, with_timestamps=True) == (
        "older",
        older_id,
    )


def test_cli_after_iso_string_parses(workdir: Path) -> None:
    """[SB-CLI-5] Documented ISO form is accepted on CLI."""
    db = workdir / "select.db"
    assert run_cli("-f", str(db), "write", "q", "hello", cwd=workdir)[0] == EXIT_SUCCESS
    rc, out, err = run_cli(
        "-f",
        str(db),
        "peek",
        "q",
        "--all",
        "--after",
        "1970-01-01T00:00:00Z",
        cwd=workdir,
    )
    assert rc == EXIT_SUCCESS, err
    assert "hello" in out


def test_cli_equivalent_iso_and_seconds_bounds_select_the_same_rows(
    workdir: Path,
) -> None:
    db = workdir / "select-equivalent.db"
    bound = 4_638_902_402_999_996_416
    with Queue("q", db_path=str(db)) as queue:
        queue.insert_messages(
            [
                ("at-bound", bound),
                ("next-grain", bound + 4_096),
            ]
        )

    outputs: list[str] = []
    for spelling in ("2117-01-01T00:00:03Z", "4638902403s"):
        rc, out, err = run_cli(
            "-f",
            str(db),
            "peek",
            "q",
            "--all",
            "--after",
            spelling,
            cwd=workdir,
        )
        assert rc == EXIT_SUCCESS, err
        outputs.append(out)

    assert outputs == ["next-grain", "next-grain"]
