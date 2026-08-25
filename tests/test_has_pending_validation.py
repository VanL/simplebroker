"""Timestamp-bound validation for the public Queue.has_pending API."""

from typing import Any

import pytest

pytestmark = pytest.mark.shared


@pytest.mark.parametrize("invalid", [True, "abc"])
def test_has_pending_rejects_non_integer_timestamp_bounds(
    queue_factory: Any, invalid: object
) -> None:
    queue = queue_factory("jobs")

    with pytest.raises(TypeError, match="after_timestamp must be an int or None"):
        queue.has_pending(after_timestamp=invalid)
