"""Executable transition contract for the async example delivery stream."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from examples.async_pooled_broker import (
    AsyncQueue,
    async_broker,
)
from simplebroker import Queue
from simplebroker.ext import OperationalError
from tests.helpers.state_machine_contracts import (
    TransitionCase,
    fires_transition_table,
)


@dataclass(frozen=True, slots=True)
class AsyncStreamPayload:
    """Inputs and observable result for one async-stream transition."""

    mode: Literal[
        "empty",
        "peek-one",
        "peek-all",
        "consume-one",
        "consume-all",
        "batch-commit",
        "early-close",
        "consumer-error",
        "commit-error",
    ]
    expected_yields: tuple[str, ...]
    expected_remaining: tuple[str, ...]
    expected_transaction_events: tuple[str, ...]


ASYNC_STREAM_TRANSITIONS = (
    TransitionCase(
        transition_id="empty-rolls-back",
        start_state="ready-empty",
        event="consume one",
        guard="no unclaimed row exists",
        next_state="ready-empty",
        effects="the empty claim transaction is rolled back",
        expected_result="the stream ends without yielding",
        payload=AsyncStreamPayload("empty", (), (), ("rollback",)),
    ),
    TransitionCase(
        transition_id="peek-one-preserves",
        start_state="ready-with-messages",
        event="peek one",
        guard="peek is true and all_messages is false",
        next_state="ready-with-messages",
        effects="one row is read without claiming it",
        expected_result="the first body is yielded and both rows remain",
        payload=AsyncStreamPayload("peek-one", ("one",), ("one", "two"), ()),
    ),
    TransitionCase(
        transition_id="peek-all-preserves",
        start_state="ready-with-messages",
        event="peek all",
        guard="peek and all_messages are true",
        next_state="ready-with-messages",
        effects="all matching rows are read without a write transaction",
        expected_result="both bodies are yielded and remain available",
        payload=AsyncStreamPayload(
            "peek-all",
            ("one", "two"),
            ("one", "two"),
            (),
        ),
    ),
    TransitionCase(
        transition_id="single-consume-commits",
        start_state="ready-with-messages",
        event="consume one",
        guard="all_messages is false",
        next_state="ready-with-one-message",
        effects="the first row is claimed and the transaction commits",
        expected_result="the first body is yielded and only the second remains",
        payload=AsyncStreamPayload("consume-one", ("one",), ("two",), ("commit",)),
    ),
    TransitionCase(
        transition_id="exactly-once-loop-commits",
        start_state="ready-with-messages",
        event="consume all",
        guard="all_messages is true and commit_interval equals one",
        next_state="ready-empty",
        effects="each single-row claim commits before its body is yielded",
        expected_result="both bodies are yielded and neither remains",
        payload=AsyncStreamPayload(
            "consume-all",
            ("one", "two"),
            (),
            ("commit", "commit", "rollback"),
        ),
    ),
    TransitionCase(
        transition_id="batch-consumption-commits",
        start_state="ready-with-messages",
        event="consume a complete batch",
        guard="all_messages is true and commit_interval is greater than one",
        next_state="ready-empty",
        effects="the claimed batch commits after its final body is yielded",
        expected_result="both bodies are yielded and neither remains",
        payload=AsyncStreamPayload(
            "batch-commit",
            ("one", "two"),
            (),
            ("commit", "rollback"),
        ),
    ),
    TransitionCase(
        transition_id="early-close-replays",
        start_state="batch-suspended",
        event="close after the first yield",
        guard="the batch transaction has not reached its commit point",
        next_state="closed-with-uncommitted-batch",
        effects="closing the broker connection rolls back the suspended batch",
        expected_result="both messages are replayable after the broker closes",
        payload=AsyncStreamPayload(
            "early-close",
            ("one",),
            ("one", "two"),
            ("rollback",),
        ),
    ),
    TransitionCase(
        transition_id="consumer-error-replays",
        start_state="batch-suspended",
        event="throw a consumer error after the first yield",
        guard="the batch transaction has not reached its commit point",
        next_state="failed-with-uncommitted-batch",
        effects="the consumer error propagates and connection close rolls back",
        expected_result="the original error is preserved and both rows replay",
        payload=AsyncStreamPayload(
            "consumer-error",
            ("one",),
            ("one", "two"),
            ("rollback",),
        ),
    ),
    TransitionCase(
        transition_id="commit-failure-rolls-back",
        start_state="batch-fully-yielded",
        event="commit batch",
        guard="the runner reports a commit failure",
        next_state="failed-ready-with-messages",
        effects="the batch is rolled back before the error propagates",
        expected_result="the commit error is raised and both rows remain",
        payload=AsyncStreamPayload(
            "commit-error",
            ("one", "two"),
            ("one", "two"),
            ("commit", "rollback"),
        ),
    ),
)


async def _collect(stream: object) -> list[str]:
    result: list[str] = []
    async for message in stream:  # type: ignore[attr-defined]
        result.append(message)
    return result


async def _read_regular_mode(
    queue: AsyncQueue,
    mode: str,
) -> list[str]:
    if mode == "empty":
        return await _collect(queue.stream(all_messages=False))
    if mode == "peek-one":
        return await _collect(queue.stream(peek=True, all_messages=False))
    if mode == "peek-all":
        return await _collect(queue.stream(peek=True))
    if mode == "consume-one":
        return await _collect(queue.stream(all_messages=False))
    if mode == "consume-all":
        return await _collect(queue.stream(commit_interval=1))
    return await _collect(queue.stream(commit_interval=2))


async def _interrupt_batch(
    queue: AsyncQueue,
    *,
    consumer_error: bool,
) -> tuple[list[str], BaseException | None]:
    stream = cast(AsyncGenerator[str, None], queue.stream(commit_interval=2))
    yielded = [await anext(stream)]
    if not consumer_error:
        await stream.aclose()
        return yielded, None

    expected_error = RuntimeError("consumer failed")
    try:
        await stream.athrow(expected_error)
    except RuntimeError as exc:
        return yielded, exc
    raise AssertionError("consumer error did not propagate")


async def _fail_batch_commit(
    queue: AsyncQueue,
) -> tuple[list[str], BaseException | None]:
    yielded: list[str] = []
    try:
        async for message in queue.stream(commit_interval=2):
            yielded.append(message)
    except OperationalError as exc:
        return yielded, exc
    raise AssertionError("injected commit failure did not propagate")


def _trace_transactions(
    queue: AsyncQueue,
    *,
    fail_first_commit: bool,
) -> tuple[list[str], Any, Any]:
    runner = queue._broker._runner
    real_commit = runner.commit
    real_rollback = runner.rollback
    events: list[str] = []
    commit_failed = False

    async def traced_commit() -> None:
        nonlocal commit_failed
        events.append("commit")
        if fail_first_commit and not commit_failed:
            commit_failed = True
            raise OperationalError("injected commit failure")
        await real_commit()

    async def traced_rollback() -> None:
        events.append("rollback")
        await real_rollback()

    runner.commit = traced_commit  # type: ignore[method-assign]
    runner.rollback = traced_rollback  # type: ignore[method-assign]
    return events, real_commit, real_rollback


def _assert_expected_async_error(
    mode: str,
    caught: BaseException | None,
) -> None:
    if mode == "consumer-error":
        assert isinstance(caught, RuntimeError)
        assert str(caught) == "consumer failed"
    elif mode == "commit-error":
        assert isinstance(caught, OperationalError)
        assert str(caught) == "injected commit failure"
    else:
        assert caught is None


async def _fire_async_stream_transition(
    db_path: Path,
    payload: AsyncStreamPayload,
) -> tuple[
    tuple[str, ...],
    tuple[str, ...],
    tuple[str, ...],
    BaseException | None,
]:
    yielded: list[str] = []
    caught: BaseException | None = None

    async with async_broker(str(db_path), max_connections=2) as broker:
        queue = AsyncQueue("jobs", broker)
        await broker._ensure_initialized()
        if payload.mode != "empty":
            await queue.write("one")
            await queue.write("two")

        runner = broker._runner
        events, real_commit, real_rollback = _trace_transactions(
            queue,
            fail_first_commit=payload.mode == "commit-error",
        )
        regular_modes = {
            "empty",
            "peek-one",
            "peek-all",
            "consume-one",
            "consume-all",
            "batch-commit",
        }
        if payload.mode in regular_modes:
            yielded = await _read_regular_mode(queue, payload.mode)
        elif payload.mode in {"early-close", "consumer-error"}:
            yielded, caught = await _interrupt_batch(
                queue,
                consumer_error=payload.mode == "consumer-error",
            )
        else:
            yielded, caught = await _fail_batch_commit(queue)
        events_at_boundary = tuple(events)
        runner.commit = real_commit  # type: ignore[method-assign]
        runner.rollback = real_rollback  # type: ignore[method-assign]

    persisted = Queue("jobs", db_path=str(db_path))
    try:
        remaining = tuple(
            str(message) for message in persisted.peek_many(10, with_timestamps=False)
        )
    finally:
        persisted.close()

    _assert_expected_async_error(payload.mode, caught)
    return tuple(yielded), remaining, events_at_boundary, caught


@fires_transition_table("SM-ASYNC-STREAM", ASYNC_STREAM_TRANSITIONS)
def test_async_stream_fires_transition_table(
    transition_case: TransitionCase[AsyncStreamPayload],
    tmp_path: Path,
) -> None:
    """Fire every declared async-stream transition against a real SQLite file."""

    yielded, remaining, transaction_events, _caught = asyncio.run(
        _fire_async_stream_transition(
            tmp_path / "async-stream.db", transition_case.payload
        )
    )
    assert yielded == transition_case.payload.expected_yields
    assert remaining == transition_case.payload.expected_remaining
    assert transaction_events == transition_case.payload.expected_transaction_events
