"""Canonical validation for exact broker message IDs."""

from __future__ import annotations

from ._constants import SQLITE_MAX_INT64
from ._exceptions import TimestampError
from ._timestamp import TimestampGenerator

MessageIdInput = int | str

INVALID_MESSAGE_ID_MESSAGE = (
    "invalid message ID: expected exactly 19 digits within range"
)


def normalize_message_id(value: object, *, name: str = "message_id") -> int:
    """Validate and normalize an exact message ID.

    Exact message IDs accept either an integer ID or the CLI form: exactly 19
    decimal digits within SQLite's signed integer range. Range timestamp bounds
    intentionally use separate validators and remain int-only in the Python API.
    """

    if isinstance(value, bool):
        raise TypeError(f"{name} must be an int message ID or exact 19-digit string")

    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"{name} must be non-negative")
        if value >= SQLITE_MAX_INT64:
            raise ValueError(f"{name} exceeds maximum timestamp value")
        return value

    if isinstance(value, str):
        try:
            return TimestampGenerator.validate(value, exact=True)
        except TimestampError as exc:
            raise ValueError(INVALID_MESSAGE_ID_MESSAGE) from exc

    raise TypeError(f"{name} must be an int message ID or exact 19-digit string")
