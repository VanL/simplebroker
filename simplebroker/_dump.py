"""Dump/load: a versioned ndjson backup and migration format.

The format (``simplebroker-dump`` v1) is line-delimited JSON: exactly one
``header`` line, then ``alias`` lines (sorted by alias), then ``message``
lines (queues sorted, ascending message-ID order; pending messages only).
Output is deterministic for a given broker state.

Everything here composes the public ``BrokerConnection`` surface only —
``list_queues``, ``peek_generator``, ``get_meta``, ``list_aliases``,
``add_alias``, ``insert_messages`` — so dump/load work identically on every
backend, and a dump from one backend loads into any other.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import TYPE_CHECKING, Any, Final, cast

from ._message_id import INVALID_MESSAGE_ID_MESSAGE, normalize_message_id

if TYPE_CHECKING:
    from ._backend_plugins import BrokerConnection

DUMP_FORMAT: Final[str] = "simplebroker-dump"
DUMP_VERSION: Final[int] = 1
LOAD_BATCH_SIZE: Final[int] = 1000


@dataclass(frozen=True, slots=True)
class LoadResult:
    """Counts of records applied by ``load_lines``."""

    messages: int
    aliases: int


def _selected(
    queue: str,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
) -> bool:
    """Apply include/exclude fnmatch globs (case-sensitive) to a queue name."""
    if include and not any(fnmatchcase(queue, glob) for glob in include):
        return False
    if exclude and any(fnmatchcase(queue, glob) for glob in exclude):
        return False
    return True


def _alias_selected(
    alias: str,
    target: str,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
) -> bool:
    """An alias rides on either of its names.

    It is included when the alias name OR the target matches the includes
    (or no includes were given), and excluded — exclude wins — when EITHER
    name matches an exclude glob.
    """
    names = (alias, target)
    if include and not any(fnmatchcase(n, g) for n in names for g in include):
        return False
    if exclude and any(fnmatchcase(n, g) for n in names for g in exclude):
        return False
    return True


def _line(record: dict[str, Any]) -> str:
    return json.dumps(record, ensure_ascii=False, sort_keys=True)


def dump_lines(
    broker: BrokerConnection,
    *,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
) -> Iterator[str]:
    """Yield the broker's contents as simplebroker-dump v1 ndjson lines.

    Pending messages only (claimed rows are already-consumed and deletion-
    pending; restoring them would re-deliver). Aliases are included when
    either their own name or their target queue passes the include/exclude
    filter. The dump is a logical export: each internal batch is consistent,
    but it is not a point-in-time snapshot under concurrent writers —
    quiesce writers if you need an exact snapshot. Messages are emitted in
    ascending message-ID order (dump buffers and sorts one queue at a time;
    memory scales with the largest queue's pending count). A queue whose
    messages are ALL claimed contributes no lines (and so does not exist in
    the restored broker) — consistent with pending-only semantics.

    Args:
        broker: A broker connection (e.g. from ``open_broker(...)``).
        include: fnmatch-style globs; when given, only queues matching at
            least one glob dump. Aliases match on either their own name or
            their target.
        exclude: fnmatch-style globs; matching queues are omitted, and
            exclude always wins over include. Aliases are excluded when
            either their name or their target matches.
    """
    meta = broker.get_meta()
    yield _line(
        {
            "type": "header",
            "format": DUMP_FORMAT,
            "version": DUMP_VERSION,
            "backend": _backend_name(broker),
            "last_ts": int(meta.get("last_ts", 0)),
        }
    )

    for alias, target in sorted(broker.list_aliases()):
        if _alias_selected(alias, target, include, exclude):
            yield _line({"type": "alias", "alias": alias, "target": target})

    for queue in sorted(broker.list_queues()):
        if not _selected(queue, include, exclude):
            continue
        # The broker's internal iteration order is physical insertion order
        # (rowid), which equals message-ID order for normally written brokers
        # but can differ after exact-ID insert_messages calls — and Redis
        # always iterates in ID order. Dump canonicalizes: buffer one queue's
        # pending rows and sort by message ID, the durable, backend-portable
        # ordering. Memory scales with the largest queue's pending count.
        rows = [
            cast("tuple[str, int]", row)
            for row in broker.peek_generator(queue, with_timestamps=True)
        ]
        rows.sort(key=lambda item: item[1])
        for body, message_id in rows:
            yield _line(
                {"type": "message", "queue": queue, "body": body, "id": message_id}
            )


def _backend_name(broker: BrokerConnection) -> str:
    """Best-effort backend label for the header (informational only).

    There is no public backend accessor on broker handles; both BrokerCore
    and RedisBrokerCore carry ``_backend_plugin`` (whose ``name`` is
    "sqlite"/"postgres"/"redis"). This label is diagnostics-only metadata —
    load ignores it — so a best-effort internal read with an "unknown"
    fallback is acceptable here (this module ships inside simplebroker; the
    purity rule constrains *data operations* to public protocol members).
    """
    plugin = getattr(broker, "_backend_plugin", None)
    name = getattr(plugin, "name", None)
    return str(name) if name else "unknown"


def _error(line_number: int, problem: str) -> ValueError:
    return ValueError(f"invalid dump input at line {line_number}: {problem}")


def load_lines(broker: BrokerConnection, lines: Iterable[str]) -> LoadResult:
    """Apply simplebroker-dump v1 lines to a broker.

    Streams the input: alias records are applied immediately; message
    records are applied in atomic batches of ``LOAD_BATCH_SIZE`` via
    ``insert_messages`` (which restores exact message IDs and advances the
    broker's ID watermark). Load targets a fresh destination: duplicate
    message IDs raise ``IntegrityError`` rather than double-inserting, so a
    failed load should be retried into a clean database.

    Raises:
        ValueError: On a missing/invalid header, malformed JSON, unknown
            record types, or missing fields (with the 1-based line number).
        IntegrityError: On duplicate message IDs at the destination.
    """
    messages = 0
    aliases = 0
    batch: list[tuple[str, str, int]] = []
    header_seen = False

    def flush() -> None:
        nonlocal messages
        if batch:
            broker.insert_messages(list(batch))
            messages += len(batch)
            batch.clear()

    for line_number, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise _error(line_number, f"malformed JSON ({exc.msg})") from exc
        if not isinstance(record, dict):
            raise _error(line_number, "record must be a JSON object")

        kind = record.get("type")
        if not header_seen:
            if kind != "header":
                raise _error(line_number, "first record must be the dump header")
            if record.get("format") != DUMP_FORMAT:
                raise _error(line_number, "unrecognized dump format")
            if record.get("version") != DUMP_VERSION:
                raise _error(
                    line_number,
                    f"unsupported dump version {record.get('version')!r} "
                    f"(supported: {DUMP_VERSION})",
                )
            header_seen = True
            continue

        if kind == "alias":
            alias = record.get("alias")
            target = record.get("target")
            if not isinstance(alias, str) or not isinstance(target, str):
                raise _error(
                    line_number,
                    "alias record requires string 'alias' and 'target' fields",
                )
            broker.add_alias(alias, target)
            aliases += 1
        elif kind == "message":
            queue = record.get("queue")
            body = record.get("body")
            message_id = record.get("id")
            if not isinstance(queue, str) or not isinstance(body, str):
                raise _error(
                    line_number,
                    "message record requires string 'queue' and 'body' fields",
                )
            try:
                normalized_id = normalize_message_id(message_id)
            except (TypeError, ValueError) as exc:
                raise _error(line_number, INVALID_MESSAGE_ID_MESSAGE) from exc
            batch.append((queue, body, normalized_id))
            if len(batch) >= LOAD_BATCH_SIZE:
                flush()
        elif kind == "header":
            raise _error(line_number, "duplicate header")
        else:
            raise _error(line_number, f"unknown record type {kind!r}")

    if not header_seen:
        raise ValueError(
            "invalid dump input: missing header (is this a simplebroker dump?)"
        )
    flush()
    return LoadResult(messages=messages, aliases=aliases)


# ~
