"""Validation helpers for message body search APIs."""

from __future__ import annotations

BODY_SEARCH_DEFAULT_LIMIT = 100
BODY_SEARCH_MAX_LIMIT = 1000
BODY_SEARCH_MIN_NON_WHITESPACE_CHARS = 3
BODY_SEARCH_MAX_NEEDLE_LENGTH = 1024
BODY_SEARCH_REDIS_SCAN_CHUNK_SIZE = 500


def validate_body_contains(value: str) -> str:
    """Validate a literal body substring search needle."""
    if isinstance(value, bool) or not isinstance(value, str):
        raise TypeError("body_contains must be a string")
    if len(value) > BODY_SEARCH_MAX_NEEDLE_LENGTH:
        raise ValueError(
            f"body_contains must be at most {BODY_SEARCH_MAX_NEEDLE_LENGTH} characters"
        )
    non_whitespace_count = sum(1 for char in value if not char.isspace())
    if non_whitespace_count < BODY_SEARCH_MIN_NON_WHITESPACE_CHARS:
        raise ValueError(
            "body_contains must contain at least "
            f"{BODY_SEARCH_MIN_NON_WHITESPACE_CHARS} non-whitespace characters"
        )
    return value


def validate_body_search_limit(value: int) -> int:
    """Validate the maximum number of message IDs returned by body search."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("limit must be an int")
    if value < 1:
        raise ValueError("limit must be at least 1")
    if value > BODY_SEARCH_MAX_LIMIT:
        raise ValueError(f"limit must be at most {BODY_SEARCH_MAX_LIMIT}")
    return value
