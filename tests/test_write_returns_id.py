"""Public contract: write() returns the committed message's timestamp ID.

Queue.write(), BrokerCore.write(), and every first-party backend
connection's write() return the exact 64-bit timestamp/message ID that the
atomic write committed. These tests run against SQLite by default and
against Postgres / Redis via `uv run bin/pytest-pg` / `uv run
bin/pytest-redis` (the `broker` and `queue_factory` fixtures resolve the
active backend).

No broker internals are mocked. The only sanctioned patch in this module is
the active backend's timestamp fault-injection seam: persisted
``broker._timestamp_gen.generate`` for SQL or local
``broker._timestamp_gen._reserve_candidates`` for Redis. These force the
timestamp-conflict retry path that cannot occur naturally; see
tests/test_timestamp_resilience.py for the precedent.
"""

import threading
import warnings
from concurrent.futures import ThreadPoolExecutor

import pytest

from .helper_scripts.broker_factory import active_backend

pytestmark = pytest.mark.shared


def test_broker_write_returns_committed_id(broker):
    ts = broker.write("contract", "hello")

    assert type(ts) is int
    rows = list(broker.peek_generator("contract", with_timestamps=True))
    assert rows == [("hello", ts)]


def test_broker_write_ids_strictly_increase(broker):
    ids = [broker.write("ordering", f"m{i}") for i in range(20)]

    assert ids == sorted(set(ids)), "IDs must be unique and monotonic"
    rows = list(broker.peek_generator("ordering", with_timestamps=True))
    assert [ts for _, ts in rows] == ids


def test_retry_path_returns_surviving_row_id(broker):
    """After forced ID conflicts, write() returns the retried commit's ID.

    The conflict ladder is: attempt -> conflict -> backoff -> attempt ->
    conflict -> generator resync -> attempt -> success. The returned ID must
    be the third (successful) attempt's ID, not the discarded conflicting
    one.

    The occupant's ID is read back via peek rather than taken from write()'s
    return, so the injected conflict is a real, valid ID even on the
    pre-change baseline where write() returns None.
    """
    broker.write("retry", "occupant")
    rows = list(broker.peek_generator("retry", with_timestamps=True))
    occupant_ts = rows[0][1]

    calls = 0
    if active_backend() == "redis":
        seam = "_reserve_candidates"
        original = broker._timestamp_gen._reserve_candidates

        def collide_twice(count):
            nonlocal calls
            calls += 1
            if calls <= 2:
                return [occupant_ts]
            return original(count)

    else:
        seam = "generate"
        original = broker._timestamp_gen.generate

        def collide_twice():
            nonlocal calls
            calls += 1
            if calls <= 2:
                return occupant_ts
            return original()

    setattr(broker._timestamp_gen, seam, collide_twice)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            second = broker.write("retry", "retried")
    finally:
        setattr(broker._timestamp_gen, seam, original)

    assert calls >= 3
    assert type(second) is int
    assert second != occupant_ts
    rows_after = dict(broker.peek_generator("retry", with_timestamps=True))
    assert rows_after == {"occupant": occupant_ts, "retried": second}


def test_retry_exhaustion_raises_without_returning(broker):
    """If every attempt conflicts, write() raises; no stale ID escapes."""
    broker.write("exhaust", "occupant")
    rows = list(broker.peek_generator("exhaust", with_timestamps=True))
    occupant_ts = rows[0][1]

    if active_backend() == "redis":
        seam = "_reserve_candidates"
        original = broker._timestamp_gen._reserve_candidates

        def conflict(count):
            return [occupant_ts]

    else:
        seam = "generate"
        original = broker._timestamp_gen.generate

        def conflict():
            return occupant_ts

    setattr(broker._timestamp_gen, seam, conflict)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            with pytest.raises(RuntimeError):
                broker.write("exhaust", "never-committed")
    finally:
        setattr(broker._timestamp_gen, seam, original)

    rows_after = list(broker.peek_generator("exhaust", with_timestamps=True))
    assert rows_after == [("occupant", occupant_ts)]


def test_queue_write_returns_committed_id(queue_factory):
    q = queue_factory("qcontract")

    ts = q.write("payload")

    assert type(ts) is int
    assert q.peek_one(exact_timestamp=ts) == "payload"


def test_write_return_id_remains_row_identity_after_global_last_ts_advances(
    queue_factory,
):
    writer = queue_factory("writer")
    allocator = queue_factory("allocator")

    written_id = writer.write("owned-row")
    generated_id = allocator.generate_timestamp()

    assert generated_id > written_id
    assert writer.refresh_last_ts() == generated_id
    assert writer.peek_one(
        exact_timestamp=written_id,
        with_timestamps=True,
    ) == ("owned-row", written_id)
    assert writer.peek_one(exact_timestamp=generated_id) is None
    assert allocator.peek_one(exact_timestamp=generated_id) is None


def test_concurrent_writers_get_their_own_ids(queue_factory):
    """Each concurrent writer's returned ID identifies its own row.

    One Queue instance per thread (Queue instances are not shared across
    threads). All threads write distinct bodies to the same queue; every
    returned ID must resolve, via exact-ID peek, to the body that writer
    sent — never to another writer's row.
    """
    n_threads = 4
    per_thread = 10
    queues = [queue_factory("conc") for _ in range(n_threads)]
    results: list[dict[int, str]] = [{} for _ in range(n_threads)]
    barrier = threading.Barrier(n_threads)

    def writer(idx: int) -> None:
        barrier.wait()
        for i in range(per_thread):
            body = f"w{idx}-{i}"
            results[idx][queues[idx].write(body)] = body

    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = [executor.submit(writer, idx) for idx in range(n_threads)]
        for future in futures:
            future.result(timeout=30)

    combined: dict[int, str] = {}
    for partial in results:
        combined.update(partial)
    assert len(combined) == n_threads * per_thread, "returned IDs must be distinct"

    verify = queues[0]
    for ts, body in combined.items():
        assert verify.peek_one(exact_timestamp=ts) == body
