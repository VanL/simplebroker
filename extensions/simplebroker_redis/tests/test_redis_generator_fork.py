"""A child cannot settle or advance its parent's at-least-once reservation.

Existing batch tests cover same-process close, including a foreign thread.
These tests use an exec-isolated parent and real Redis state: ordinary child
exit must run Python finalization without inheriting the pytest worker itself.
The independent consumer and the parent's successful commit are the oracle,
not calls to a mocked rollback helper. [SB-API-5]
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from simplebroker_redis import RedisRunner, get_backend_plugin
from simplebroker_redis.core import RedisBrokerCore

from simplebroker import BrokerTarget, CloseableIterator, Queue

pytestmark = [
    pytest.mark.redis_only,
    pytest.mark.skipif(not hasattr(os, "fork"), reason="fork() is not available"),
]


def _reap(pid: int) -> int:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        waited, status = os.waitpid(pid, os.WNOHANG)
        if waited:
            assert os.WIFEXITED(status), f"child terminated with status {status}"
            return os.WEXITSTATUS(status)
        time.sleep(0.01)
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)
    raise AssertionError("child blocked on an inherited batch lock")


def _child_action(
    core: RedisBrokerCore,
    generator: CloseableIterator[tuple[str, int] | str],
    action: str,
    recover: bool,
) -> None:
    # Recovery deliberately changes the core's mutable pid. It cannot
    # transfer settlement authority for this already-suspended generator.
    if recover:
        assert core.peek_one("other") is None
    if action == "advance":
        try:
            next(generator)
        except RuntimeError as exc:
            assert "fork" in str(exc).lower()
        else:
            raise AssertionError("child advanced a parent-owned batch")
    elif action in {"close", "locked_close"}:
        generator.close()
    elif action == "fresh":
        core.write("child", "fresh")
        assert list(
            core.claim_generator(
                "child",
                delivery_guarantee="at_least_once",
                with_timestamps=False,
            )
        ) == ["fresh"]


def _generator_for_probe(
    core: RedisBrokerCore,
    url: str,
    namespace: str,
    kind: str,
    surface: str,
) -> tuple[CloseableIterator[tuple[str, int] | str], Queue | None]:
    if surface != "core":
        owner = Queue(
            "source",
            db_path=BrokerTarget("redis", url, {"namespace": namespace}),
            persistent=surface == "persistent",
        )
        iterator = (
            owner.read_generator(delivery_guarantee="at_least_once")
            if kind == "claim"
            else owner.move_generator("target", delivery_guarantee="at_least_once")
        )
        return iterator, owner
    generator = (
        core.claim_generator(
            "source",
            delivery_guarantee="at_least_once",
            batch_size=10,
            with_timestamps=False,
        )
        if kind == "claim"
        else core.move_generator(
            "source",
            "target",
            delivery_guarantee="at_least_once",
            batch_size=10,
            with_timestamps=False,
        )
    )
    return generator, None


def _probe(
    kind: str, action: str, recover: bool, final_row: bool, surface: str = "core"
) -> dict[str, Any] | None:
    url = os.environ["SB_FORK_TEST_URL"]
    namespace = os.environ["SB_FORK_TEST_NAMESPACE"]
    plugin = get_backend_plugin()
    plugin.initialize_target(url, backend_options={"namespace": namespace})
    core = RedisBrokerCore(RedisRunner(url, namespace=namespace))
    bodies = [f"body-{index}" for index in range(10)]
    for body in bodies:
        core.write("source", body)
    generator, owner = _generator_for_probe(core, url, namespace, kind, surface)
    consumed = 10 if final_row else 1
    assert [next(generator) for _ in range(consumed)] == bodies[:consumed]

    held = threading.Event()
    release = threading.Event()
    holder = None
    if action == "locked_close":

        def hold_lock() -> None:
            with core._lock:
                held.set()
                assert release.wait(25)

        holder = threading.Thread(target=hold_lock)
        holder.start()
        assert held.wait(5)

    pid = os.fork()
    if pid == 0:
        _child_action(core, generator, action, recover)
        # Returning normally drops the local generator, running its finally.
        # No os._exit(): ordinary child finalization is the reported trigger.
        return None

    observer = None
    try:
        assert _reap(pid) == 0
        release.set()
        if holder is not None:
            holder.join(5)
            assert not holder.is_alive()
        observer = RedisBrokerCore(RedisRunner(url, namespace=namespace))
        duplicate_delivery = observer.claim_many("source", 10, with_timestamps=False)
        assert duplicate_delivery == [], (
            f"child released parent batch: {duplicate_delivery}"
        )
        remaining = list(generator)
        assert remaining == bodies[consumed:]
        assert observer.peek_many("source", limit=10, with_timestamps=False) == []
        delivered = observer.peek_many("target", limit=10, with_timestamps=False)
        assert delivered == (bodies if kind == "move" else [])
        return {"parent_committed": True, "duplicates": duplicate_delivery}
    finally:
        release.set()
        if holder is not None:
            holder.join(5)
            assert not holder.is_alive()
        generator.close()
        if owner is not None:
            owner.close()
        if observer is not None:
            observer.shutdown()
        core.shutdown()
        plugin.cleanup_target(url, backend_options={"namespace": namespace})


@pytest.mark.parametrize("kind", ["claim", "move"])
@pytest.mark.parametrize(
    ("action", "recover", "final_row"),
    [
        ("exit", False, False),
        ("close", False, False),
        ("locked_close", False, False),
        ("close", True, False),
        ("advance", False, False),
        ("advance", False, True),
        ("advance", True, False),
        ("advance", True, True),
        ("fresh", True, False),
    ],
)
def test_child_cannot_settle_or_advance_parent_batch(
    redis_url: str,
    redis_namespace: str,
    kind: str,
    action: str,
    recover: bool,
    final_row: bool,
) -> None:
    _assert_probe(redis_url, redis_namespace, kind, action, recover, final_row)


@pytest.mark.parametrize("kind", ["claim", "move"])
@pytest.mark.parametrize("surface", ["persistent", "ephemeral"])
def test_public_queue_child_exit_keeps_parent_reservation(
    redis_url: str,
    redis_namespace: str,
    kind: str,
    surface: str,
) -> None:
    """The public iterator's outer connection context must also abandon safely."""
    _assert_probe(redis_url, redis_namespace, kind, "exit", False, False, surface)


def _assert_probe(
    redis_url: str,
    redis_namespace: str,
    kind: str,
    action: str,
    recover: bool,
    final_row: bool,
    surface: str = "core",
) -> None:
    env = dict(
        os.environ, SB_FORK_TEST_URL=redis_url, SB_FORK_TEST_NAMESPACE=redis_namespace
    )
    result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            kind,
            action,
            str(int(recover)),
            str(int(final_row)),
            surface,
        ],
        capture_output=True,
        text=True,
        env=env,
        timeout=45,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout) == {"parent_committed": True, "duplicates": []}


if __name__ == "__main__":
    outcome = _probe(
        sys.argv[1],
        sys.argv[2],
        bool(int(sys.argv[3])),
        bool(int(sys.argv[4])),
        sys.argv[5],
    )
    if outcome is not None:
        print(json.dumps(outcome))
