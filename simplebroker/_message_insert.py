"""Shared validation helpers for exact-ID message inserts."""

from __future__ import annotations

from collections.abc import Callable, Iterable

from ._exceptions import IntegrityError
from ._message_id import MessageIdInput, normalize_message_id
from ._timestamp import validate_timestamp_bound

MessageInsertRecord = tuple[str, str, MessageIdInput]
NormalizedMessageInsertRecord = tuple[str, str, int]


def normalize_insert_records(
    records: Iterable[MessageInsertRecord],
    *,
    validate_queue_name: Callable[[str], None],
    validate_message_size: Callable[[str], None],
) -> tuple[list[NormalizedMessageInsertRecord], int | None]:
    """Validate exact-ID insert records and return the required last_ts value."""

    normalized_records: list[NormalizedMessageInsertRecord] = []
    seen_ids: set[int] = set()
    max_message_id: int | None = None

    for record in records:
        try:
            queue, message, message_id = record
        except (TypeError, ValueError) as exc:
            raise TypeError(
                "insert records must be (queue, message, message_id) tuples"
            ) from exc

        validate_queue_name(queue)
        validate_message_size(message)
        normalized_id = normalize_message_id(message_id)

        if normalized_id in seen_ids:
            raise IntegrityError("duplicate message ID in insert batch")
        seen_ids.add(normalized_id)

        if max_message_id is None or normalized_id > max_message_id:
            max_message_id = normalized_id

        normalized_records.append((queue, message, normalized_id))

    if max_message_id is None:
        return normalized_records, None

    required_last_ts = validate_timestamp_bound(
        "insert high-water timestamp",
        max_message_id + 1,
    )
    if required_last_ts is None:
        raise TypeError("insert high-water timestamp must be an int")
    return normalized_records, required_last_ts
