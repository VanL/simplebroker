"""Characterization tests pinning the move-vs-checkpoint skip.

A ``move`` preserves the moved message's original timestamp (stable IDs, a
documented design property). Timestamp-checkpoint consumers filter on
``ts > last_seen``, so a message moved into a queue with a timestamp behind an
already-advanced checkpoint is permanently skipped. These tests pin that
behavior across every affected consumer shape so any future change to the skip
is deliberate. See the README "Moved messages and checkpoints" caveat.

These are GREEN-FIRST characterization tests (they pass on current ``main``);
they exist to lock the current semantics, not to drive a behavior change. Bare
timestamp preservation is already pinned by
``tests/test_move_by_id.py::test_move_by_id_preserves_timestamp`` and is not
re-tested here.
"""

from __future__ import annotations

import threading
import time

import pytest

from simplebroker.watcher import QueueWatcher

from .conftest import run_cli
from .helper_scripts.broker_factory import make_broker

pytestmark = [pytest.mark.shared]


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


class _Collector:
    """Thread-safe collector of (body, ts) delivered to a watcher handler."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.messages: list[tuple[str, int]] = []

    def handler(self, body: str, ts: int) -> None:
        with self._lock:
            self.messages.append((body, ts))

    def bodies(self) -> list[str]:
        with self._lock:
            return [b for b, _ in self.messages]

    def wait_for_body(self, body: str, timeout: float = 5.0) -> bool:
        """Deadline-bounded wait for a specific body to be delivered."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if any(b == body for b, _ in self.messages):
                    return True
            time.sleep(0.01)
        return False


def _first_ts(core, queue: str) -> int:
    """Return the timestamp of the first pending message in ``queue``."""
    for _body, ts in core.peek_generator(queue):
        return ts
    raise AssertionError(f"queue {queue!r} is empty")


# --------------------------------------------------------------------------- #
# Watcher-based characterization tests
# --------------------------------------------------------------------------- #


def test_peek_watcher_skips_message_moved_in_behind_checkpoint(broker, broker_target):
    """Peek-mode durable-checkpoint consumer skips a message moved in behind it.

    Advance a peek watcher's checkpoint past a fresh message's ts, then move an
    older-ts message into the same queue. A new watcher started at the advanced
    checkpoint must NOT deliver the moved (older-ts) message while it DOES
    deliver a freshly written one.
    """
    core = make_broker(broker_target)

    # Seed an old-ts message in src that we will later move into dst.
    core.write("src", "moved-old")
    moved_ts = _first_ts(core, "src")

    # Deliver one fresh message into dst so a peek watcher advances its
    # checkpoint past that message's ts (which is > moved_ts).
    core.write("dst", "fresh-1")

    collector1 = _Collector()
    watcher1 = QueueWatcher(
        "dst",
        collector1.handler,
        db=broker_target,
        peek=True,
    )
    thread1 = watcher1.run_in_thread()
    try:
        assert collector1.wait_for_body("fresh-1"), "peek watcher never saw fresh-1"
    finally:
        watcher1.stop(join=True, timeout=5.0)
        thread1.join(timeout=5.0)

    # The advanced checkpoint is the fresh message's ts.
    checkpoint = collector1.messages[-1][1]
    assert checkpoint > moved_ts

    # Move the old-ts message into dst. Its ts (moved_ts) is behind checkpoint.
    result = core.move_one("src", "dst")
    assert result is not None

    # A durable-checkpoint consumer resuming at the advanced checkpoint.
    collector2 = _Collector()
    watcher2 = QueueWatcher(
        "dst",
        collector2.handler,
        db=broker_target,
        peek=True,
        after_timestamp=checkpoint,
    )
    thread2 = watcher2.run_in_thread()
    try:
        # A freshly written message (ts > checkpoint) MUST be delivered.
        core.write("dst", "fresh-2")
        assert collector2.wait_for_body("fresh-2"), (
            "new watcher never delivered a fresh post-checkpoint message"
        )
    finally:
        watcher2.stop(join=True, timeout=5.0)
        thread2.join(timeout=5.0)

    bodies = collector2.bodies()
    # The moved (behind-checkpoint) message is permanently skipped.
    assert "moved-old" not in bodies
    assert "fresh-2" in bodies


def test_consume_watcher_without_filter_delivers_moved_message(broker, broker_target):
    """The documented safe pattern: consume mode with NO timestamp filter.

    A consume-mode watcher with no ``after_timestamp`` claims whatever is
    unclaimed regardless of ts, so a moved (older-ts) message IS delivered.
    """
    core = make_broker(broker_target)

    core.write("src", "moved-old")
    core.move_one("src", "dst")

    collector = _Collector()
    watcher = QueueWatcher(
        "dst",
        collector.handler,
        db=broker_target,
        peek=False,  # consume mode, no after_timestamp
    )
    thread = watcher.run_in_thread()
    try:
        assert collector.wait_for_body("moved-old"), (
            "unfiltered consume watcher failed to deliver the moved message"
        )
    finally:
        watcher.stop(join=True, timeout=5.0)
        thread.join(timeout=5.0)

    assert "moved-old" in collector.bodies()


def test_consume_watcher_with_after_skips_moved_message(broker, broker_target):
    """Consume mode WITH after_timestamp is also exposed to the move-skip.

    Post-Task-0 semantics: consume mode applies the strict ``after_timestamp``
    filter at the claim query, so a message moved in behind the checkpoint is
    NOT claimed/delivered, while a fresh post-checkpoint message IS.
    """
    core = make_broker(broker_target)

    # Old-ts message to be moved in behind the checkpoint.
    core.write("src", "moved-old")
    moved_ts = _first_ts(core, "src")

    # Establish a checkpoint strictly above moved_ts using a real message ts.
    core.write("dst", "checkpoint-msg")
    checkpoint = _first_ts(core, "dst")
    assert checkpoint > moved_ts
    # Drain the checkpoint-msg so it does not confound delivery assertions.
    assert core.claim_one("dst") is not None

    core.move_one("src", "dst")

    collector = _Collector()
    watcher = QueueWatcher(
        "dst",
        collector.handler,
        db=broker_target,
        peek=False,
        after_timestamp=checkpoint,
    )
    thread = watcher.run_in_thread()
    try:
        core.write("dst", "fresh")
        assert collector.wait_for_body("fresh"), (
            "filtered consume watcher never delivered the fresh message"
        )
    finally:
        watcher.stop(join=True, timeout=5.0)
        thread.join(timeout=5.0)

    bodies = collector.bodies()
    assert "moved-old" not in bodies
    assert "fresh" in bodies


# --------------------------------------------------------------------------- #
# CLI characterization tests (peek --after / read --after)
# --------------------------------------------------------------------------- #


def test_cli_peek_after_skips_moved_message(workdir):
    """`peek --after <ts>` skips a message moved into the queue behind <ts>."""
    # Seed an old-ts message in src.
    run_cli("write", "src", "moved-old", cwd=workdir)
    rc, out, _ = run_cli("peek", "src", "--timestamps", cwd=workdir)
    assert rc == 0
    moved_ts = int(out.split("\t")[0])

    # Fresh message in dst; capture its ts as the checkpoint (> moved_ts).
    run_cli("write", "dst", "checkpoint-msg", cwd=workdir)
    rc, out, _ = run_cli("peek", "dst", "--timestamps", cwd=workdir)
    assert rc == 0
    checkpoint = int(out.split("\t")[0])
    assert checkpoint > moved_ts

    # Move the old-ts message into dst (ts preserved, behind checkpoint).
    rc, _, _ = run_cli("move", "src", "dst", cwd=workdir)
    assert rc == 0

    # Write a fresh post-checkpoint message.
    run_cli("write", "dst", "fresh", cwd=workdir)

    # peek --after checkpoint: moved-old skipped, fresh returned, and the
    # checkpoint-msg boundary excluded.
    rc, out, _ = run_cli(
        "peek", "dst", "--all", "--after", str(checkpoint), cwd=workdir
    )
    assert rc == 0
    returned = out.splitlines()
    assert "moved-old" not in returned
    assert "fresh" in returned


def test_cli_read_after_skips_moved_message(workdir):
    """`read --after <ts>` skips a message moved into the queue behind <ts>."""
    run_cli("write", "src", "moved-old", cwd=workdir)
    rc, out, _ = run_cli("peek", "src", "--timestamps", cwd=workdir)
    assert rc == 0
    moved_ts = int(out.split("\t")[0])

    run_cli("write", "dst", "checkpoint-msg", cwd=workdir)
    rc, out, _ = run_cli("peek", "dst", "--timestamps", cwd=workdir)
    assert rc == 0
    checkpoint = int(out.split("\t")[0])
    assert checkpoint > moved_ts

    rc, _, _ = run_cli("move", "src", "dst", cwd=workdir)
    assert rc == 0

    run_cli("write", "dst", "fresh", cwd=workdir)

    rc, out, _ = run_cli(
        "read", "dst", "--all", "--after", str(checkpoint), cwd=workdir
    )
    assert rc == 0
    returned = out.splitlines()
    assert "moved-old" not in returned
    assert "fresh" in returned
