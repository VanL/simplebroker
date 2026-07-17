"""Timestamp-bound validation for the public Queue.has_pending API."""

import pytest

from simplebroker import Queue


@pytest.mark.parametrize("invalid", [True, "abc"])
def test_has_pending_rejects_non_integer_timestamp_bounds(
    tmp_path, invalid: object
) -> None:
    queue = Queue("jobs", db_path=str(tmp_path / "broker.db"))

    with pytest.raises(TypeError, match="after_timestamp must be an int or None"):
        queue.has_pending(after_timestamp=invalid)  # type: ignore[arg-type]
