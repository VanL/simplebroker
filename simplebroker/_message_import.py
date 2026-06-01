"""Shared validation helpers for exact-ID message writes."""

from __future__ import annotations

from collections.abc import Callable, Iterable

from ._exceptions import IntegrityError
from ._timestamp import validate_timestamp_bound

MessageImportRecord = tuple[str, str, int]


def normalize_message_id(message_id: int) -> int:
    """Validate and normalize a required message ID."""

    normalized_id = validate_timestamp_bound("message_id", message_id)
    if normalized_id is None:
        raise TypeError("message_id must be an int")
    return normalized_id


def normalize_import_records(
    records: Iterable[MessageImportRecord],
    *,
    validate_queue_name: Callable[[str], None],
    validate_message_size: Callable[[str], None],
) -> tuple[list[MessageImportRecord], int | None]:
    """Validate bulk import records and return the required last_ts high-water."""

    normalized_records: list[MessageImportRecord] = []
    seen_ids: set[int] = set()
    max_message_id: int | None = None

    for record in records:
        try:
            queue, message, message_id = record
        except (TypeError, ValueError) as exc:
            raise TypeError(
                "import records must be (queue, message, message_id) tuples"
            ) from exc

        validate_queue_name(queue)
        validate_message_size(message)
        normalized_id = normalize_message_id(message_id)

        if normalized_id in seen_ids:
            raise IntegrityError("duplicate message ID in import batch")
        seen_ids.add(normalized_id)

        if max_message_id is None or normalized_id > max_message_id:
            max_message_id = normalized_id

        normalized_records.append((queue, message, normalized_id))

    if max_message_id is None:
        return normalized_records, None

    required_last_ts = validate_timestamp_bound(
        "import high-water timestamp",
        max_message_id + 1,
    )
    if required_last_ts is None:
        raise TypeError("import high-water timestamp must be an int")
    return normalized_records, required_last_ts
