"""Public contract: write() returns the committed message's timestamp ID.

Queue.write(), BrokerCore.write(), and every first-party backend
connection's write() return the exact 64-bit timestamp/message ID that the
atomic write committed. These tests run against SQLite by default and
against Postgres / Redis via `uv run bin/pytest-pg` / `uv run
bin/pytest-redis` (the `broker` and `queue_factory` fixtures resolve the
active backend).

No broker internals are mocked. The only sanctioned patch in this module is
the repo's established timestamp fault-injection seam
(``broker._timestamp_gen.generate``), used to force the timestamp-conflict
retry path that cannot occur naturally; see
tests/test_timestamp_resilience.py for the precedent.
"""

import threading
import warnings

import pytest

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

    original = broker._timestamp_gen.generate
    calls = 0

    def collide_twice():
        nonlocal calls
        calls += 1
        if calls <= 2:
            return occupant_ts
        return original()

    broker._timestamp_gen.generate = collide_twice
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            second = broker.write("retry", "retried")
    finally:
        broker._timestamp_gen.generate = original

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

    original = broker._timestamp_gen.generate
    broker._timestamp_gen.generate = lambda: occupant_ts
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            with pytest.raises(RuntimeError):
                broker.write("exhaust", "never-committed")
    finally:
        broker._timestamp_gen.generate = original

    rows_after = list(broker.peek_generator("exhaust", with_timestamps=True))
    assert rows_after == [("occupant", occupant_ts)]


def test_queue_write_returns_committed_id(queue_factory):
    q = queue_factory("qcontract")

    ts = q.write("payload")

    assert type(ts) is int
    assert q.peek_one(exact_timestamp=ts) == "payload"


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
    errors: list[BaseException] = []
    barrier = threading.Barrier(n_threads)

    def writer(idx: int) -> None:
        try:
            barrier.wait()
            for i in range(per_thread):
                body = f"w{idx}-{i}"
                results[idx][queues[idx].write(body)] = body
        except BaseException as exc:  # pragma: no cover - failure reporting
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not any(t.is_alive() for t in threads), "writer threads hung"

    assert not errors
    combined: dict[int, str] = {}
    for partial in results:
        combined.update(partial)
    assert len(combined) == n_threads * per_thread, "returned IDs must be distinct"

    verify = queues[0]
    for ts, body in combined.items():
        assert verify.peek_one(exact_timestamp=ts) == body
