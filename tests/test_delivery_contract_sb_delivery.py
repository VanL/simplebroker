"""Public-path bindings for the canonical ``[SB-DELIVERY-*]`` contract."""
# mypy: disable-error-code=no-untyped-def

from __future__ import annotations

import ast
import concurrent.futures
import re
import threading
from pathlib import Path

import pytest

from simplebroker import Queue
from simplebroker._constants import PEEK_BATCH_SIZE
from simplebroker._delivery import ACCEPTED_DELIVERY_GUARANTEES

pytestmark = [pytest.mark.shared]

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs" / "specs" / "11-delivery.md"
REGISTRY = ROOT / "docs" / "specs" / "product-section-registry.md"
README = ROOT / "README.md"
KERNEL = ROOT / "docs" / "agent-kernel.md"
LLMS = ROOT / "llms.txt"


def _section(code: str) -> str:
    text = SPEC.read_text(encoding="utf-8")
    match = re.search(rf"^## .+ \[{re.escape(code)}\]$", text, re.MULTILINE)
    assert match is not None
    following = re.search(r"^## ", text[match.end() :], re.MULTILINE)
    end = len(text) if following is None else match.end() + following.start()
    return text[match.start() : end]


def _verification_row(code: str) -> str:
    prefix = f"| [{code}] |"
    return next(
        line
        for line in SPEC.read_text(encoding="utf-8").splitlines()
        if line.startswith(prefix)
    )


def _test_functions(relative_path: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def test_delivery_contract_clause_inventory_and_bindings() -> None:
    """Every canonical delivery clause has an implementation and firing gate."""
    text = SPEC.read_text(encoding="utf-8")
    heading_codes = [
        int(code)
        for code in re.findall(r"^## .+ \[SB-DELIVERY-(\d+)\]$", text, re.MULTILINE)
    ]
    assert heading_codes == list(range(1, 9))

    verification = text.split("## Verification", 1)[1].split("## Related Plans", 1)[0]
    verification_codes = re.findall(
        r"^\| \[(SB-DELIVERY-\d+)\] \|", verification, re.MULTILINE
    )
    assert verification_codes == [f"SB-DELIVERY-{code}" for code in range(1, 9)]
    for code in range(1, 9):
        assert f"| [SB-DELIVERY-{code}] |" in verification
        assert "tests/test_delivery_contract_sb_delivery.py" in _verification_row(
            f"SB-DELIVERY-{code}"
        )

    assert "simplebroker/watcher.py" in text
    assert "simplebroker/sbqueue.py" in text
    assert "simplebroker/db.py" in text
    assert "tests/test_cross_thread_finalization_poisoning.py" in verification
    assert "tests/test_cross_thread_generator_probe.py" in verification
    assert "tests/test_cli_broken_pipe.py" in verification

    registry_row = next(
        line
        for line in REGISTRY.read_text(encoding="utf-8").splitlines()
        if line.startswith("| Delivery guarantees,")
    )
    assert "`canonical-spec`" in registry_row
    assert "`11-delivery.md`" in registry_row
    assert "[SB-DELIVERY-1]" in registry_row
    assert "[SB-DELIVERY-8]" in registry_row
    assert "tests/test_delivery_contract_sb_delivery.py" in registry_row
    assert "tests/test_cross_thread_finalization_poisoning.py" in registry_row
    assert "tests/test_cli_broken_pipe.py" in registry_row

    for path in (README, KERNEL, LLMS):
        assert "docs/specs/11-delivery.md" in path.read_text(encoding="utf-8")


def test_readme_and_kernel_delivery_ranges_reach_the_canonical_terminal_clause() -> (
    None
):
    """Enumerable delivery restatements cannot silently stop before clause 8."""
    terminal = max(
        int(code)
        for code in re.findall(
            r"^## .+ \[SB-DELIVERY-(\d+)\]$",
            SPEC.read_text(encoding="utf-8"),
            re.MULTILINE,
        )
    )
    range_pattern = re.compile(r"\[SB-DELIVERY-1\]`?\s*[–-]\s*`?\[SB-DELIVERY-(\d+)\]")

    for path in (README, KERNEL):
        terminal_clauses = {
            int(value)
            for value in range_pattern.findall(path.read_text(encoding="utf-8"))
        }
        assert terminal_clauses
        assert terminal_clauses == {terminal}


def test_watch_mode_clause_binds_all_modes_and_runtime_gates() -> None:
    """[SB-DELIVERY-2] All three outcomes bind to real watcher tests."""
    section = _section("SB-DELIVERY-2")
    assert "default consume mode" in section
    assert "`--peek`" in section
    assert "`--move DEST`" in section

    row = _verification_row("SB-DELIVERY-2")
    required = {
        "tests/test_watcher.py": {
            "test_peek_handler_failure_does_not_advance_checkpoint"
        },
        "tests/test_queue_move_watcher.py": {
            "test_handler_failure_isolation",
            "test_transaction_safety",
        },
    }
    for relative_path, functions in required.items():
        assert relative_path in row
        assert functions <= _test_functions(relative_path)


def test_claim_is_unavailable_before_caller_processing(queue_factory) -> None:
    """[SB-DELIVERY-1] A returned claim is already unavailable to rivals."""
    consumer = queue_factory("claim_source")
    observer = queue_factory("claim_source")
    message_id = consumer.write("payload")

    assert consumer.read_one(with_timestamps=True) == ("payload", message_id)
    assert observer.peek_one(with_timestamps=True) is None
    assert observer.read_one(with_timestamps=True) is None


def test_same_target_move_reservation_has_one_winner(queue_factory) -> None:
    """[SB-DELIVERY-3] Concurrent public moves cannot reserve one row twice."""
    source = queue_factory("move_source")
    worker_one = queue_factory("move_source")
    worker_two = queue_factory("move_source")
    destination_one = queue_factory("inflight_one")
    destination_two = queue_factory("inflight_two")
    source.write("payload")
    start = threading.Barrier(2)

    def move(worker, destination: str):
        start.wait()
        return worker.move_one(destination, with_timestamps=False)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = (
            executor.submit(move, worker_one, destination_one.name),
            executor.submit(move, worker_two, destination_two.name),
        )
        results = [future.result() for future in futures]

    assert sorted(result for result in results if result is not None) == ["payload"]
    assert source.peek_one() is None
    destination_messages = destination_one.peek_many(
        limit=2, with_timestamps=False
    ) + destination_two.peek_many(limit=2, with_timestamps=False)
    assert destination_messages == ["payload"]


def test_move_rejects_a_queue_on_another_target(queue_factory, tmp_path) -> None:
    """[SB-DELIVERY-3] Queue destinations cannot cross broker targets."""
    source = queue_factory("cross_target_source")
    destination = Queue(
        "cross_target_destination",
        db_path=str(tmp_path / "different-target.db"),
    )
    try:
        source.write("payload")
        with pytest.raises(ValueError, match="different broker targets"):
            source.move_one(destination)

        assert source.peek_one() == "payload"
        assert destination.peek_one() is None
    finally:
        destination.close()


def test_two_peekers_observe_same_id_without_mutation(queue_factory) -> None:
    """[SB-DELIVERY-4] Peek observes but does not reserve or mutate."""
    first = queue_factory("peek_source")
    second = queue_factory("peek_source")
    message_id = first.write("payload")

    expected = ("payload", message_id)
    assert first.peek_one(with_timestamps=True) == expected
    assert second.peek_one(with_timestamps=True) == expected
    assert first.peek_one(with_timestamps=True) == expected


def test_live_peek_stream_mutation_leaves_unvisited_messages(queue_factory) -> None:
    """[SB-DELIVERY-4] Removing rows shifts a live offset-paged peek stream."""
    source = queue_factory("offset_source")
    deleter = queue_factory("offset_source")
    source.insert_messages(
        (f"message-{index}", index + 1) for index in range(PEEK_BATCH_SIZE + 2)
    )

    visited: list[int] = []
    for _body, message_id in source.peek_generator(with_timestamps=True):
        visited.append(message_id)
        assert deleter.delete(message_id=message_id)

    assert visited
    assert source.peek_one(with_timestamps=True) is not None


def test_invalid_generator_selector_fails_on_iteration_without_mutation(
    queue_factory,
) -> None:
    """[SB-DELIVERY-5] Lazy validation precedes claim or destination mutation."""
    source = queue_factory("invalid_selector_source")
    destination = queue_factory("invalid_selector_destination")
    source.write("payload")

    generator = source.move_generator(
        destination,
        delivery_guarantee="typo",
    )
    try:
        with pytest.raises(ValueError, match="typo"):
            next(generator)
    finally:
        generator.close()

    assert source.peek_many(limit=2, with_timestamps=False) == ["payload"]
    assert destination.peek_one() is None


def test_delivery_selector_vocabulary_matches_implementation() -> None:
    """[SB-DELIVERY-5] The documented selector set is exact and closed."""
    documented = set(
        re.findall(
            r'`"(exactly_once|at_least_once)"`',
            _section("SB-DELIVERY-5"),
        )
    )
    assert tuple(ACCEPTED_DELIVERY_GUARANTEES) == (
        "exactly_once",
        "at_least_once",
    )
    assert documented == set(ACCEPTED_DELIVERY_GUARANTEES)


@pytest.mark.parametrize("operation", ["read", "move"])
def test_materialized_batches_commit_before_return(queue_factory, operation) -> None:
    """[SB-DELIVERY-5] Materialized results are committed when returned."""
    source = queue_factory("materialized_source")
    observer = queue_factory("materialized_source")
    destination = queue_factory("materialized_destination")
    source.write("one")
    source.write("two")

    if operation == "read":
        result = source.read_many(1, delivery_guarantee="at_least_once")
        assert destination.peek_one() is None
    else:
        result = source.move_many(
            destination,
            1,
            delivery_guarantee="at_least_once",
        )
        assert destination.peek_many(limit=2, with_timestamps=False) == ["one"]

    assert result == ["one"]
    assert observer.peek_many(limit=2, with_timestamps=False) == ["two"]


def test_early_close_replays_unfinished_at_least_once_batch(queue_factory) -> None:
    """[SB-DELIVERY-5] Closing an unfinished batch makes it available again."""
    source = queue_factory("retry_source")
    for body in ("one", "two", "three"):
        source.write(body)

    generator = source.read_generator(delivery_guarantee="at_least_once")
    assert next(generator) == "one"
    generator.close()

    assert source.peek_many(limit=10, with_timestamps=False) == [
        "one",
        "two",
        "three",
    ]


def test_foreign_thread_contract_binds_sql_and_redis_process_probes() -> None:
    """[SB-DELIVERY-6] SQL poison and Redis non-poison use real probes."""
    required = {
        "tests/test_cross_thread_finalization_poisoning.py": {
            "test_foreign_generator_finalization_publishes_poison"
        },
        "tests/test_cross_thread_generator_probe.py": {
            "test_sqlite_cross_thread_generator_probe"
        },
        "extensions/simplebroker_pg/tests/test_pg_cross_thread_generator_probe.py": {
            "test_postgres_cross_thread_generator_probe"
        },
        "extensions/simplebroker_redis/tests/test_redis_cross_thread_generator_probe.py": {
            "test_redis_cross_thread_generator_probe"
        },
    }
    row = _verification_row("SB-DELIVERY-6")
    for relative_path, functions in required.items():
        assert relative_path in row
        assert functions <= _test_functions(relative_path)

    pg_probe = (
        ROOT
        / "extensions/simplebroker_pg/tests/test_pg_cross_thread_generator_probe.py"
    ).read_text(encoding="utf-8")
    redis_probe = (
        ROOT
        / "extensions/simplebroker_redis/tests/test_redis_cross_thread_generator_probe.py"
    ).read_text(encoding="utf-8")
    assert 'result["poisoned_after_foreign_close"] is True' in pg_probe
    assert 'result["poisoned_after_foreign_close"] is False' in redis_probe


def test_closed_pipe_contract_binds_black_box_cli_effects() -> None:
    """[SB-DELIVERY-7] Closed-pipe claims and rollback use real CLI tests."""
    row = _verification_row("SB-DELIVERY-7")
    assert "tests/test_cli_broken_pipe.py" in row
    assert {
        "test_watch_stops_claiming_after_stdout_consumer_exits",
        "test_read_all_pipe_closure_rolls_back_active_at_least_once_batch",
    } <= _test_functions("tests/test_cli_broken_pipe.py")


def test_readme_dlq_recipe_preserves_pending_work_on_failure() -> None:
    """The human-entry DLQ recipe must not claim before its fallback is safe."""
    readme = README.read_text(encoding="utf-8")
    recipe = readme.split("<summary>Dead Letter Queue Pattern</summary>", 1)[1]
    recipe = recipe.split("</details>", 1)[0]

    assert "broker peek tasks --json" in recipe
    assert 'broker move tasks dlq -m "$msg_id"' in recipe
    assert 'broker delete tasks -m "$msg_id"' in recipe
    assert "process_task_json" in recipe
    assert "| python3 -c" in recipe
    assert "broker read tasks" not in recipe
    assert "broker write dlq" not in recipe
    assert 'echo "$msg"' not in recipe


def test_readme_newline_recipe_writes_an_actual_newline() -> None:
    """A quoted backslash-n operand would store two literal characters."""
    readme = README.read_text(encoding="utf-8")
    section = readme.split("### JSON for Safe Processing", 1)[1]
    section = section.split("### Filtering by message id", 1)[0]

    assert section.count("printf 'ERROR: Database connection failed\\n") == 2
    assert section.count("| broker write alerts -") == 2
    assert 'broker write alerts "ERROR:' not in section
