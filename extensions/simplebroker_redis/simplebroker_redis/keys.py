"""Key and ID helpers for the Valkey/Redis backend."""

from __future__ import annotations

from .validation import key_prefix


def encode_id(timestamp: int) -> str:
    if timestamp < 0:
        raise ValueError("timestamp must be non-negative")
    encoded = f"{timestamp:019d}"
    if len(encoded) > 19:
        raise ValueError("timestamp exceeds Redis lexicographic encoding range")
    return encoded


def decode_id(encoded: str) -> int:
    return int(encoded)


def min_bound(after_timestamp: int | None) -> str:
    return "-" if after_timestamp is None else f"({encode_id(after_timestamp)}"


def max_bound(before_timestamp: int | None) -> str:
    return "+" if before_timestamp is None else f"({encode_id(before_timestamp)}"


def exact_bound(timestamp: int) -> str:
    return f"[{encode_id(timestamp)}"


class RedisKeys:
    """Construct namespaced Redis keys consistently."""

    def __init__(self, namespace: str) -> None:
        self.prefix = key_prefix(namespace)

    def key(self, *parts: str) -> str:
        return ":".join((self.prefix, *parts))

    @property
    def meta(self) -> str:
        return self.key("meta")

    @property
    def queues(self) -> str:
        return self.key("queues")

    @property
    def aliases(self) -> str:
        return self.key("aliases")

    @property
    def bodies(self) -> str:
        return self.key("bodies")

    @property
    def all_ids(self) -> str:
        return self.key("all_ids")

    def pending(self, queue: str) -> str:
        return self.key("q", queue, "pending")

    def claimed(self, queue: str) -> str:
        return self.key("q", queue, "claimed")

    def reserved(self, queue: str) -> str:
        return self.key("q", queue, "reserved")

    def batch_ids(self, token: str) -> str:
        return self.key("batches", token, "ids")

    def batch_meta(self, token: str) -> str:
        return self.key("batches", token, "meta")

    @property
    def activity_all(self) -> str:
        return self.key("activity", "all")

    def activity_queue(self, queue: str) -> str:
        return self.key("activity", "q", queue)
