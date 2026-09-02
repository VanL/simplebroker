"""Closed delivery-guarantee input contract.

See ``docs/specs/11-delivery.md`` [SB-DELIVERY-5].
"""

from typing import Final, Literal

DeliveryGuarantee = Literal["exactly_once", "at_least_once"]

ACCEPTED_DELIVERY_GUARANTEES: Final[tuple[DeliveryGuarantee, ...]] = (
    "exactly_once",
    "at_least_once",
)

# Operation-scoped snapshot windows are intentionally small; this bounds the
# retained-set index walk without imposing a queue-depth or displaced-row cap.
MAX_KEEP_NEWEST: Final[int] = 9999


def validate_delivery_guarantee(value: object) -> DeliveryGuarantee:
    """Return a valid delivery guarantee or reject the unknown value."""
    if value not in ACCEPTED_DELIVERY_GUARANTEES:
        accepted = ", ".join(repr(item) for item in ACCEPTED_DELIVERY_GUARANTEES)
        raise ValueError(
            f"Invalid delivery_guarantee {value!r}; expected one of: {accepted}"
        )
    return value


def validate_keep_newest(value: object) -> int | None:
    """Normalize the optional write-time pending window [SB-DELIVERY-9]."""
    if value is None:
        return None
    if type(value) is not int:
        raise TypeError("keep_newest must be an integer from 1 to 9999 or None")
    if value < 1 or value > MAX_KEEP_NEWEST:
        raise ValueError("keep_newest must be between 1 and 9999")
    return value
