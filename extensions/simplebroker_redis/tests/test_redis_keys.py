"""Redis key and message-ID encoding behavior."""

from __future__ import annotations

import pytest
from simplebroker_redis.keys import encode_id

pytestmark = [pytest.mark.redis_only]


@pytest.mark.parametrize(
    ("timestamp", "message"),
    [
        (-1, "timestamp must be non-negative"),
        (10**19, "timestamp exceeds Redis lexicographic encoding range"),
    ],
)
def test_redis_message_ids_reject_values_outside_the_sortable_range(
    timestamp: int,
    message: str,
) -> None:
    """Every encoded ID must fit the fixed-width Redis lexicographic ordering."""

    with pytest.raises(ValueError, match=message):
        encode_id(timestamp)
