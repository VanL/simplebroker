"""Opt-in displacement-cost ladder for write-time pending windows."""

from __future__ import annotations

import time
from typing import Any

import pytest

pytestmark = [pytest.mark.shared, pytest.mark.benchmark]


@pytest.mark.parametrize("displaced", [0, 1, 1_000, 10_000, 100_000, 220_000])
def test_write_keep_newest_displacement_ladder(
    queue_factory: Any,
    displaced: int,
) -> None:
    keep_newest = 5
    queue = queue_factory(f"keep_benchmark_{displaced}")
    prior = displaced + keep_newest - 1
    queue.insert_messages(("seed", index + 1) for index in range(prior))

    started = time.perf_counter()
    queue.write("new", keep_newest=keep_newest)
    elapsed = time.perf_counter() - started

    stats = queue.stats()
    assert (stats.pending, stats.claimed, stats.total) == (
        keep_newest,
        displaced,
        displaced + keep_newest,
    )
    print(f"displaced={displaced} seconds={elapsed:.6f}")
