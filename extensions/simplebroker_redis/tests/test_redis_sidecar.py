"""Sidecar capability behavior on the Redis backend: it has none."""

from __future__ import annotations

import pytest
from simplebroker_redis import RedisRunner

from simplebroker import Queue
from simplebroker.ext import SidecarUnavailableError

pytestmark = [pytest.mark.redis_only]


def test_sidecar_raises_unavailable(redis_runner: RedisRunner) -> None:
    queue = Queue("jobs", runner=redis_runner, persistent=True)
    try:
        with pytest.raises(SidecarUnavailableError):
            with queue.sidecar():
                pass
        with pytest.raises(SidecarUnavailableError):
            with queue.sidecar(transaction=True):
                pass
    finally:
        queue.close()
