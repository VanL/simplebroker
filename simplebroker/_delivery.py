"""Closed delivery-guarantee input contract.

See ``docs/specs/11-delivery-contract.md`` [SB-DELIVERY-5].
"""

from typing import Final, Literal

DeliveryGuarantee = Literal["exactly_once", "at_least_once"]

ACCEPTED_DELIVERY_GUARANTEES: Final[tuple[DeliveryGuarantee, ...]] = (
    "exactly_once",
    "at_least_once",
)


def validate_delivery_guarantee(value: object) -> DeliveryGuarantee:
    """Return a valid delivery guarantee or reject the unknown value."""
    if value not in ACCEPTED_DELIVERY_GUARANTEES:
        accepted = ", ".join(repr(item) for item in ACCEPTED_DELIVERY_GUARANTEES)
        raise ValueError(
            f"Invalid delivery_guarantee {value!r}; expected one of: {accepted}"
        )
    return value
