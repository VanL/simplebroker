"""Dump/load: a versioned ndjson backup and migration format.

The format (``simplebroker-dump`` v1) is line-delimited JSON: exactly one
``header`` line, then ``alias`` lines (sorted by alias), then ``message``
lines (queues sorted, ascending message-ID order; pending messages only).
Output is deterministic for a given broker state.

Everything here composes the public ``BrokerConnection`` surface only —
``list_queues``, ``peek_generator``, ``get_meta``, ``list_aliases``,
``add_alias``, ``insert_messages``, ``advance_last_timestamp`` — so dump/load
work identically on every backend, and a dump from one backend loads into any
other.
"""

from __future__ import annotations

import json
import time
import warnings
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import TYPE_CHECKING, Any, Final, cast

from ._constants import (
    LOGICAL_COUNTER_MASK,
    MAX_LOGICAL_COUNTER,
    NS_PER_SECOND,
    SQLITE_MAX_INT64,
    resolve_config,
)
from ._message_id import (
    INVALID_MESSAGE_ID_MESSAGE,
    format_message_id,
    normalize_message_id,
)
from ._message_insert import RESERVED_MESSAGE_ID_MESSAGE

if TYPE_CHECKING:
    from ._backend_plugins import BrokerConnection

DUMP_FORMAT: Final[str] = "simplebroker-dump"
DUMP_VERSION: Final[int] = 1
LOAD_BATCH_SIZE: Final[int] = 1000


class DumpClockSkewWarning(UserWarning):
    """A dump watermark is physically ahead of the loading host's clock."""


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
    return not (exclude and any(fnmatchcase(queue, glob) for glob in exclude))


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
    return not (exclude and any(fnmatchcase(n, g) for n in names for g in exclude))


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
    header_last_ts = int(meta.get("last_ts", 0))
    snapshot_bound = (
        header_last_ts + 1 if header_last_ts < SQLITE_MAX_INT64 - 1 else None
    )
    yield _line(
        {
            "type": "header",
            "format": DUMP_FORMAT,
            "version": DUMP_VERSION,
            "backend": _backend_name(broker),
            "last_ts": format_message_id(header_last_ts),
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
            for row in broker.peek_generator(
                queue,
                with_timestamps=True,
                before_timestamp=snapshot_bound,
            )
        ]
        rows.sort(key=lambda item: item[1])
        for body, message_id in rows:
            yield _line(
                {
                    "type": "message",
                    "queue": queue,
                    "body": body,
                    "id": format_message_id(message_id),
                }
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


def _check_future_skew(
    header_last_ts: int,
    *,
    now_ns: int,
    max_future_skew_ns: int,
    force: bool,
    line_number: int,
) -> None:
    """Warn on a future header and reject excessive skew unless forced."""
    physical_ns = header_last_ts & ~LOGICAL_COUNTER_MASK
    future_skew_ns = physical_ns - now_ns
    if future_skew_ns <= 0:
        return

    remaining_ids = max(
        0,
        (MAX_LOGICAL_COUNTER - 1) - (header_last_ts & LOGICAL_COUNTER_MASK),
    )
    skew_seconds = future_skew_ns / NS_PER_SECOND
    warnings.warn(
        "dump header last_ts "
        f"{header_last_ts:019d} is {skew_seconds:.3f} seconds ahead "
        "of local wall time; apparent clock skew leaves at most "
        f"{remaining_ids} broker-global generated IDs before writes "
        "wait for wall time and may raise TimestampError",
        DumpClockSkewWarning,
        stacklevel=3,
    )
    if future_skew_ns > max_future_skew_ns and not force:
        raise _error(
            line_number,
            "dump header future skew exceeds configured maximum "
            f"of {max_future_skew_ns / NS_PER_SECOND:g} seconds",
        )


def load_lines(  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-009] exception
    broker: BrokerConnection,
    lines: Iterable[str],
    *,
    force: bool = False,
    config: Mapping[str, Any] | None = None,
) -> LoadResult:
    """Apply simplebroker-dump v1 lines to a broker.

    Validates backend capability and the header's future-clock skew before
    destination mutation. Streams the remaining input: alias records are
    applied immediately; message
    records are applied in atomic batches of ``LOAD_BATCH_SIZE`` via
    ``insert_messages`` (which restores exact message IDs and advances the
    broker's ID watermark). Load is intended for a fresh destination but does
    not enforce freshness. Duplicate message IDs raise ``IntegrityError``
    rather than double-inserting. Earlier mutations remain applied on a later
    error, so retry a failed load into a clean database.

    Args:
        broker: Current backend-v7 broker connection.
        lines: Iterable of v1 NDJSON records.
        force: Bypass only excessive future-skew refusal; warnings still fire.
        config: Optional typed configuration overrides resolved through the
            standard SimpleBroker config path.

    Raises:
        TypeError: If the broker lacks the required timestamp-advance method.
        ValueError: On a missing/invalid header, excessive future skew without
            force, a message ID above the header bound, malformed JSON, unknown
            record types, or missing fields (with the 1-based line number).
        IntegrityError: On duplicate message IDs at the destination.
        TimestampError: If the final monotone header-floor operation cannot
            confirm durable high-water. Earlier aliases and flushed message
            batches remain applied; ``outcome_ambiguous`` distinguishes whether
            the durable floor may already have advanced.
    """
    advance_last_timestamp = getattr(broker, "advance_last_timestamp", None)
    if not callable(advance_last_timestamp):
        raise TypeError(
            "broker must provide callable advance_last_timestamp() for dump load"
        )
    resolved_config = resolve_config(config)
    max_future_skew_ns = (
        int(resolved_config["BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS"]) * NS_PER_SECOND
    )

    messages = 0
    aliases = 0
    batch: list[tuple[str, str, int]] = []
    header_last_ts: int | None = None

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
        except ValueError as exc:
            raise _error(
                line_number, "malformed JSON (numeric value is too large)"
            ) from exc
        if not isinstance(record, dict):
            raise _error(line_number, "record must be a JSON object")

        kind = record.get("type")
        if header_last_ts is None:
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
            if "last_ts" not in record:
                raise _error(line_number, "header requires 'last_ts'")
            try:
                header_last_ts = normalize_message_id(record["last_ts"])
            except (TypeError, ValueError) as exc:
                raise _error(line_number, "invalid header last_ts") from exc
            _check_future_skew(
                header_last_ts,
                now_ns=time.time_ns(),
                max_future_skew_ns=max_future_skew_ns,
                force=force,
                line_number=line_number,
            )
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
            if normalized_id == 0:
                raise _error(line_number, RESERVED_MESSAGE_ID_MESSAGE)
            if normalized_id > header_last_ts:
                raise _error(line_number, "message id exceeds header last_ts")
            batch.append((queue, body, normalized_id))
            if len(batch) >= LOAD_BATCH_SIZE:
                flush()
        elif kind == "header":
            raise _error(line_number, "duplicate header")
        else:
            raise _error(line_number, f"unknown record type {kind!r}")

    if header_last_ts is None:
        raise ValueError(
            "invalid dump input: missing header (is this a simplebroker dump?)"
        )
    flush()
    advance_last_timestamp(header_last_ts)
    return LoadResult(messages=messages, aliases=aliases)


# ~
