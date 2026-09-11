"""Public SQLite scan work grows linearly without timing-sensitive assertions."""

from __future__ import annotations

import pytest

from .peek_pagination_benchmark import dataset, sqlite_steps

pytestmark = [pytest.mark.sqlite_only]


def test_public_peek_doubled_rows_have_linear_vm_work() -> None:
    steps = []
    for count in (10000, 20000):
        with dataset("sqlite", count, None) as (queue, runner, _version):
            steps.append(sqlite_steps(queue, runner, count))
    # OFFSET is about 3.4x here; keyset is about 2x. Fixture construction and
    # teardown are outside the progress callback, and every ordered ID is checked.
    assert steps[1] < steps[0] * 2.8, steps
