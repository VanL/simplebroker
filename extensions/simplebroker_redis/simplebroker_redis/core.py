"""Direct Valkey/Redis broker core for SimpleBroker."""

from __future__ import annotations

import os
import threading
import time
import uuid
import warnings
from collections.abc import Generator, Mapping, Sequence
from fnmatch import fnmatchcase
from typing import Any, Literal, overload

import redis

from simplebroker._constants import (
    ALIAS_PREFIX,
    PEEK_BATCH_SIZE,
    load_config,
    resolve_config,
)
from simplebroker._exceptions import IntegrityError, OperationalError, TimestampError
from simplebroker._message_search import (
    BODY_SEARCH_DEFAULT_LIMIT,
    BODY_SEARCH_REDIS_SCAN_CHUNK_SIZE,
    validate_body_contains,
    validate_body_search_limit,
)
from simplebroker._timestamp import TimestampGenerator, validate_timestamp_bound
from simplebroker.db import (
    _literal_prefix_from_fnmatch,
    _validate_queue_name_cached,
    _validate_queue_prefix,
)
from simplebroker.metadata import QueueStats

from . import scripts
from .keys import RedisKeys, decode_id, encode_id, exact_bound, max_bound, min_bound
from .responses import response_dict, response_int, response_list, response_set
from .runner import RedisRunner
from .validation import is_namespace_key

_config = load_config()


def _translate_redis_error(exc: redis.RedisError) -> OperationalError:
    return OperationalError(str(exc))


class RedisBrokerCore:
    """Direct broker implementation backed by Valkey/Redis structures."""

    def __init__(
        self,
        runner: RedisRunner,
        *,
        config: Mapping[str, Any] | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        self._runner = runner
        self._config = resolve_config(config)
        self._stop_event = stop_event or threading.Event()
        self._lock = threading.RLock()
        self._pid = os.getpid()
        self._keys = RedisKeys(runner.namespace)
        self._prefix = self._keys.prefix
        self._max_message_size = int(self._config["BROKER_MAX_MESSAGE_SIZE"])
        self._vacuum_interval = int(self._config["BROKER_AUTO_VACUUM_INTERVAL"])
        self._write_count = 0
        self._active_generator_batch: Literal["claim", "move"] | None = None
        self._active_generator_batch_owner: int | None = None
        self._ts_conflict_count = 0
        self._ts_resync_count = 0
        self._backend_plugin = runner.backend_plugin
        self._alias_cache: dict[str, str] = {}
        self._alias_cache_version = -1
        with runner._init_lock:
            if not runner._target_initialized:
                self._backend_plugin.initialize_target(
                    runner.target,
                    backend_options={"namespace": runner.namespace},
                    config=self._config,
                )
                runner._target_initialized = True
        self._timestamp_gen = TimestampGenerator(
            runner, backend_plugin=self._backend_plugin
        )

    @property
    def _client(self) -> redis.Redis:
        return self._runner.client

    def _key(self, *parts: str) -> str:
        return self._keys.key(*parts)

    def _qkey(self, queue: str, state: str) -> str:
        if state == "pending":
            return self._keys.pending(queue)
        if state == "claimed":
            return self._keys.claimed(queue)
        if state == "reserved":
            return self._keys.reserved(queue)
        return self._key("q", queue, state)

    def _activity_channel(self, queue: str | None = None) -> str:
        if queue is None:
            return self._keys.activity_all
        return self._keys.activity_queue(queue)

    def _queue_names(self) -> set[Any]:
        return response_set(self._client.smembers(self._key("queues")))

    def _publish(self, queue: str | None) -> None:
        channel = self._activity_channel(queue)
        self._client.publish(channel, "*" if queue is None else queue)
        if queue is not None:
            self._client.publish(self._activity_channel(None), queue)

    def _check_fork_safety(self) -> None:
        current_pid = os.getpid()
        if current_pid != self._pid:
            self._pid = current_pid

    def set_stop_event(self, stop_event: threading.Event | None) -> None:
        self._stop_event = stop_event or threading.Event()

    def _validate_queue_name(self, queue: str) -> None:
        error = _validate_queue_name_cached(queue)
        if error:
            raise ValueError(error)

    def _validate_message_size(self, message: str) -> None:
        message_size = len(message.encode("utf-8"))
        if message_size > self._max_message_size:
            raise ValueError(
                f"Message size ({message_size} bytes) exceeds maximum allowed size "
                f"({self._max_message_size} bytes). Adjust BROKER_MAX_MESSAGE_SIZE if needed."
            )

    def _set_active_generator_batch(
        self, operation: Literal["claim", "move"] | None
    ) -> None:
        if operation is None:
            self._active_generator_batch = None
            self._active_generator_batch_owner = None
            return
        self._active_generator_batch = operation
        self._active_generator_batch_owner = threading.get_ident()

    def _assert_no_reentrant_mutation_during_batch(self, operation_name: str) -> None:
        if self._active_generator_batch is None:
            return
        if self._active_generator_batch_owner != threading.get_ident():
            return
        raise RuntimeError(
            f"Cannot perform {operation_name} while an at_least_once "
            f"{self._active_generator_batch}_generator batch is being yielded from "
            "this BrokerDB instance. Use delivery_guarantee='exactly_once' or a "
            "separate BrokerDB/Queue instance."
        )

    def generate_timestamp(self) -> int:
        self._assert_no_reentrant_mutation_during_batch("generate_timestamp")
        return self._timestamp_gen.generate()

    get_ts = generate_timestamp

    def get_cached_last_timestamp(self) -> int:
        return self._timestamp_gen.get_cached_last_ts()

    def refresh_last_timestamp(self) -> int:
        return self._timestamp_gen.refresh_last_ts()

    def write(self, queue: str, message: str) -> None:
        self._check_fork_safety()
        self._validate_queue_name(queue)
        self._validate_message_size(message)
        self._assert_no_reentrant_mutation_during_batch("write")
        self._write_message(queue, message)

    def import_message(self, queue: str, message: str, *, message_id: int) -> None:
        self._check_fork_safety()
        self._validate_queue_name(queue)
        self._validate_message_size(message)
        self._assert_no_reentrant_mutation_during_batch("import_message")
        normalized_id = validate_timestamp_bound("message_id", message_id)
        if normalized_id is None:
            raise TypeError("message_id must be an int")

        current_last_ts = self.refresh_last_timestamp()
        if normalized_id >= current_last_ts:
            raise ValueError("imported message_id must be lower than current last_ts")

        encoded = encode_id(normalized_id)
        try:
            result = response_list(
                self._client.eval(
                    scripts.WRITE_MESSAGE,
                    5,
                    self._keys.meta,
                    self._keys.bodies,
                    self._keys.all_ids,
                    self._keys.pending(queue),
                    self._keys.queues,
                    queue,
                    encoded,
                    message,
                )
            )
        except redis.RedisError as exc:
            raise _translate_redis_error(exc) from exc

        code = int(result[0])
        if code == 1:
            self._publish(queue)
            return
        if code == -1:
            raise IntegrityError("message ID already exists")
        if code == -2:
            raise OperationalError("Redis namespace is not initialized")
        raise OperationalError(f"Unexpected Redis import result: {code}")

    def _write_message(self, queue: str, message: str) -> int:
        for attempt in range(3):
            try:
                timestamp = self.generate_timestamp()
            except TimestampError as exc:
                if isinstance(exc.__cause__, OperationalError):
                    raise OperationalError(str(exc.__cause__)) from exc
                raise
            encoded = encode_id(timestamp)
            try:
                result = response_list(
                    self._client.eval(
                        scripts.WRITE_MESSAGE,
                        5,
                        self._keys.meta,
                        self._keys.bodies,
                        self._keys.all_ids,
                        self._keys.pending(queue),
                        self._keys.queues,
                        queue,
                        encoded,
                        message,
                    )
                )
            except redis.RedisError as exc:
                raise _translate_redis_error(exc) from exc
            code = int(result[0])
            if code == 1:
                self._publish(queue)
                return timestamp
            if code == -2:
                raise OperationalError("Redis namespace is not initialized")
            if code != -1:
                raise OperationalError(f"Unexpected Redis write result: {code}")
            self._ts_conflict_count += 1
            if attempt == 0:
                time.sleep(0.001)
            elif attempt == 1:
                self._resync_timestamp_generator()
        raise RuntimeError("Failed to write message after repeated timestamp conflicts")

    def get_conflict_metrics(self) -> dict[str, int]:
        return {
            "ts_conflict_count": self._ts_conflict_count,
            "ts_resync_count": self._ts_resync_count,
        }

    def reset_conflict_metrics(self) -> None:
        self._ts_conflict_count = 0
        self._ts_resync_count = 0

    def _resync_timestamp_generator(self) -> None:
        max_ts = self._backend_plugin.read_last_ts(self._runner)
        raw = response_list(
            self._client.execute_command(
                "ZREVRANGEBYLEX",
                self._keys.all_ids,
                "+",
                "-",
                "LIMIT",
                0,
                1,
            )
        )
        if raw:
            max_ts = max(max_ts, decode_id(str(raw[0])))
        self._backend_plugin.write_last_ts(self._runner, max_ts)
        self._timestamp_gen.refresh_last_ts()
        self._ts_resync_count += 1

    def _rows_from_flat(self, flat: list[Any]) -> list[tuple[str, int]]:
        return [
            (str(flat[index]), decode_id(str(flat[index + 1])))
            for index in range(0, len(flat), 2)
        ]

    def _zrange_pending(
        self,
        queue: str,
        *,
        limit: int,
        offset: int = 0,
        exact_timestamp: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
    ) -> list[str]:
        pending = self._qkey(queue, "pending")
        if exact_timestamp is not None:
            encoded = encode_id(exact_timestamp)
            return (
                [encoded] if self._client.zscore(pending, encoded) is not None else []
            )
        return [
            str(encoded)
            for encoded in response_list(
                self._client.zrangebylex(
                    pending,
                    min_bound(after_timestamp),
                    max_bound(before_timestamp),
                    start=offset,
                    num=limit,
                )
            )
        ]

    def _peek_rows(
        self,
        queue: str,
        *,
        limit: int,
        offset: int = 0,
        exact_timestamp: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
    ) -> list[tuple[str, int]]:
        ids = self._zrange_pending(
            queue,
            limit=limit,
            offset=offset,
            exact_timestamp=exact_timestamp,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
        )
        if not ids:
            return []
        bodies = response_list(self._client.hmget(self._key("bodies"), ids))
        return [
            (str(body), decode_id(encoded))
            for encoded, body in zip(ids, bodies, strict=False)
            if body is not None
        ]

    def _claim_rows(
        self,
        queue: str,
        *,
        limit: int,
        exact_timestamp: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
    ) -> list[tuple[str, int]]:
        self._maybe_recover_stale_batches()
        minb = (
            exact_bound(exact_timestamp)
            if exact_timestamp is not None
            else min_bound(after_timestamp)
        )
        maxb = (
            exact_bound(exact_timestamp)
            if exact_timestamp is not None
            else max_bound(before_timestamp)
        )
        try:
            flat = response_list(
                self._client.eval(
                    scripts.CLAIM_MESSAGES,
                    5,
                    self._qkey(queue, "pending"),
                    self._qkey(queue, "claimed"),
                    self._qkey(queue, "reserved"),
                    self._key("bodies"),
                    self._key("queues"),
                    queue,
                    str(limit),
                    minb,
                    maxb,
                )
            )
        except redis.RedisError as exc:
            raise _translate_redis_error(exc) from exc
        return self._rows_from_flat(flat)

    def _move_rows(
        self,
        source_queue: str,
        target_queue: str,
        *,
        limit: int,
        exact_timestamp: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        require_unclaimed: bool = True,
    ) -> list[tuple[str, int]]:
        self._maybe_recover_stale_batches()
        minb = (
            exact_bound(exact_timestamp)
            if exact_timestamp is not None
            else min_bound(after_timestamp)
        )
        maxb = (
            exact_bound(exact_timestamp)
            if exact_timestamp is not None
            else max_bound(before_timestamp)
        )
        try:
            flat = response_list(
                self._client.eval(
                    scripts.MOVE_MESSAGES,
                    6,
                    self._qkey(source_queue, "pending"),
                    self._qkey(source_queue, "claimed"),
                    self._qkey(source_queue, "reserved"),
                    self._qkey(target_queue, "pending"),
                    self._key("bodies"),
                    self._key("queues"),
                    source_queue,
                    target_queue,
                    str(limit),
                    minb,
                    maxb,
                    encode_id(exact_timestamp) if exact_timestamp is not None else "",
                    "1" if require_unclaimed else "0",
                )
            )
        except redis.RedisError as exc:
            raise _translate_redis_error(exc) from exc
        rows = self._rows_from_flat(flat)
        if rows:
            self._publish(target_queue)
        return rows

    def claim_one(
        self,
        queue: str,
        *,
        exact_timestamp: int | None = None,
        with_timestamps: bool = True,
    ) -> tuple[str, int] | str | None:
        self._validate_queue_name(queue)
        self._assert_no_reentrant_mutation_during_batch("claim operation")
        rows = self._claim_rows(queue, limit=1, exact_timestamp=exact_timestamp)
        if not rows:
            return None
        return rows[0] if with_timestamps else rows[0][0]

    def claim_many(
        self,
        queue: str,
        limit: int,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
    ) -> list[tuple[str, int]] | list[str]:
        del delivery_guarantee
        if limit < 1:
            raise ValueError("limit must be at least 1")
        self._validate_queue_name(queue)
        self._assert_no_reentrant_mutation_during_batch("claim operation")
        rows = self._claim_rows(
            queue,
            limit=limit,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
        )
        return rows if with_timestamps else [body for body, _ in rows]

    def claim_generator(
        self,
        queue: str,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        batch_size: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: int | None = None,
        config: dict[str, Any] = _config,
    ) -> Generator[tuple[str, int] | str, None, None]:
        self._validate_queue_name(queue)
        if delivery_guarantee == "exactly_once":
            while True:
                rows = self._claim_rows(
                    queue,
                    limit=1,
                    exact_timestamp=exact_timestamp,
                    after_timestamp=after_timestamp,
                    before_timestamp=before_timestamp,
                )
                if not rows:
                    return
                row = rows[0]
                yield row if with_timestamps else row[0]
            return
        effective_batch_size = batch_size or config["BROKER_GENERATOR_BATCH_SIZE"]
        yield from self._claim_batch_generator(
            queue,
            with_timestamps=with_timestamps,
            batch_size=effective_batch_size,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
            exact_timestamp=exact_timestamp,
        )

    def _begin_batch(
        self,
        queue: str,
        *,
        batch_size: int,
        after_timestamp: int | None,
        before_timestamp: int | None,
        exact_timestamp: int | None,
        op: Literal["claim", "move"],
        target_queue: str | None = None,
    ) -> tuple[str, list[tuple[str, int]]]:
        self.recover_stale_batches(max_age_seconds=self._runner.stale_batch_seconds)
        token = uuid.uuid4().hex
        minb = (
            exact_bound(exact_timestamp)
            if exact_timestamp is not None
            else min_bound(after_timestamp)
        )
        maxb = (
            exact_bound(exact_timestamp)
            if exact_timestamp is not None
            else max_bound(before_timestamp)
        )
        try:
            flat = response_list(
                self._client.eval(
                    scripts.BEGIN_BATCH,
                    6,
                    self._keys.pending(queue),
                    self._keys.reserved(queue),
                    self._keys.bodies,
                    self._keys.batch_ids(token),
                    self._keys.batch_meta(token),
                    self._keys.queues,
                    queue,
                    token,
                    op,
                    target_queue or "",
                    str(time.time_ns()),
                    str(batch_size),
                    minb,
                    maxb,
                )
            )
        except redis.RedisError as exc:
            raise _translate_redis_error(exc) from exc
        return token, self._rows_from_flat(flat)

    def _commit_claim_batch(
        self, queue: str, token: str, rows: list[tuple[str, int]]
    ) -> None:
        if not rows:
            return
        try:
            result = self._client.eval(
                scripts.COMMIT_CLAIM_BATCH,
                6,
                self._keys.pending(queue),
                self._keys.claimed(queue),
                self._keys.reserved(queue),
                self._keys.batch_ids(token),
                self._keys.batch_meta(token),
                self._keys.queues,
                queue,
            )
        except redis.RedisError as exc:
            raise _translate_redis_error(exc) from exc
        if response_int(result) < 0:
            raise OperationalError("Cannot commit stale or invalid Redis claim batch")

    def _commit_move_batch(
        self,
        source_queue: str,
        target_queue: str,
        token: str,
        rows: list[tuple[str, int]],
    ) -> None:
        if not rows:
            return
        try:
            result = self._client.eval(
                scripts.COMMIT_MOVE_BATCH,
                7,
                self._keys.pending(source_queue),
                self._keys.reserved(source_queue),
                self._keys.pending(target_queue),
                self._keys.batch_ids(token),
                self._keys.batch_meta(token),
                self._keys.queues,
                self._keys.claimed(source_queue),
                source_queue,
                target_queue,
            )
        except redis.RedisError as exc:
            raise _translate_redis_error(exc) from exc
        result_int = response_int(result)
        if result_int < 0:
            raise OperationalError("Cannot commit stale or invalid Redis move batch")
        if result_int:
            self._publish(target_queue)

    def _rollback_batch(
        self, queue: str, token: str, rows: list[tuple[str, int]]
    ) -> None:
        del rows
        try:
            self._client.eval(
                scripts.ROLLBACK_BATCH,
                3,
                self._keys.reserved(queue),
                self._keys.batch_ids(token),
                self._keys.batch_meta(token),
            )
        except redis.RedisError as exc:
            raise _translate_redis_error(exc) from exc

    def _claim_batch_generator(
        self,
        queue: str,
        *,
        with_timestamps: bool,
        batch_size: int,
        after_timestamp: int | None,
        before_timestamp: int | None,
        exact_timestamp: int | None,
    ) -> Generator[tuple[str, int] | str, None, None]:
        self._validate_queue_name(queue)
        while True:
            token, rows = self._begin_batch(
                queue,
                batch_size=batch_size,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                exact_timestamp=exact_timestamp,
                op="claim",
            )
            if not rows:
                return
            completed = False
            self._set_active_generator_batch("claim")
            try:
                for row in rows:
                    yield row if with_timestamps else row[0]
                completed = True
            finally:
                self._set_active_generator_batch(None)
                if completed:
                    self._commit_claim_batch(queue, token, rows)
                else:
                    self._rollback_batch(queue, token, rows)

    def peek_one(
        self,
        queue: str,
        *,
        exact_timestamp: int | None = None,
        with_timestamps: bool = True,
    ) -> tuple[str, int] | str | None:
        self._validate_queue_name(queue)
        rows = self._peek_rows(queue, limit=1, exact_timestamp=exact_timestamp)
        if not rows:
            return None
        return rows[0] if with_timestamps else rows[0][0]

    @overload
    def peek_many(
        self,
        queue: str,
        limit: int = PEEK_BATCH_SIZE,
        *,
        with_timestamps: Literal[True] = True,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
    ) -> list[tuple[str, int]]: ...

    @overload
    def peek_many(
        self,
        queue: str,
        limit: int = PEEK_BATCH_SIZE,
        *,
        with_timestamps: Literal[False],
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
    ) -> list[str]: ...

    @overload
    def peek_many(
        self,
        queue: str,
        limit: int = PEEK_BATCH_SIZE,
        *,
        with_timestamps: bool,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
    ) -> list[tuple[str, int]] | list[str]: ...

    def peek_many(
        self,
        queue: str,
        limit: int = PEEK_BATCH_SIZE,
        *,
        with_timestamps: bool = True,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
    ) -> list[tuple[str, int]] | list[str]:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        self._validate_queue_name(queue)
        rows = self._peek_rows(
            queue,
            limit=limit,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
        )
        return rows if with_timestamps else [body for body, _ in rows]

    def peek_generator(
        self,
        queue: str,
        *,
        with_timestamps: bool = True,
        batch_size: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: int | None = None,
    ) -> Generator[tuple[str, int] | str, None, None]:
        effective_batch_size = batch_size or PEEK_BATCH_SIZE
        offset = 0
        while True:
            rows = self._peek_rows(
                queue,
                limit=effective_batch_size,
                offset=offset,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                exact_timestamp=exact_timestamp,
            )
            if not rows:
                return
            for row in rows:
                yield row if with_timestamps else row[0]
            offset += len(rows)
            if len(rows) < effective_batch_size:
                return

    def move_one(
        self,
        source_queue: str,
        target_queue: str,
        *,
        exact_timestamp: int | None = None,
        require_unclaimed: bool = True,
        with_timestamps: bool = True,
    ) -> tuple[str, int] | str | None:
        if source_queue == target_queue:
            raise ValueError("Source and target queues cannot be the same")
        self._validate_queue_name(source_queue)
        self._validate_queue_name(target_queue)
        self._assert_no_reentrant_mutation_during_batch("move operation")
        rows = self._move_rows(
            source_queue,
            target_queue,
            limit=1,
            exact_timestamp=exact_timestamp,
            require_unclaimed=require_unclaimed,
        )
        if not rows:
            return None
        return rows[0] if with_timestamps else rows[0][0]

    def move_many(
        self,
        source_queue: str,
        target_queue: str,
        limit: int,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        require_unclaimed: bool = True,
    ) -> list[tuple[str, int]] | list[str]:
        del delivery_guarantee
        if source_queue == target_queue:
            raise ValueError("Source and target queues cannot be the same")
        if limit < 1:
            raise ValueError("limit must be at least 1")
        self._validate_queue_name(source_queue)
        self._validate_queue_name(target_queue)
        self._assert_no_reentrant_mutation_during_batch("move operation")
        rows = self._move_rows(
            source_queue,
            target_queue,
            limit=limit,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
            require_unclaimed=require_unclaimed,
        )
        return rows if with_timestamps else [body for body, _ in rows]

    def move_generator(
        self,
        source_queue: str,
        target_queue: str,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        batch_size: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: int | None = None,
        config: dict[str, Any] = _config,
    ) -> Generator[tuple[str, int] | str, None, None]:
        if source_queue == target_queue:
            raise ValueError("Source and target queues cannot be the same")
        self._validate_queue_name(source_queue)
        self._validate_queue_name(target_queue)
        if delivery_guarantee == "exactly_once":
            while True:
                rows = self._move_rows(
                    source_queue,
                    target_queue,
                    limit=1,
                    exact_timestamp=exact_timestamp,
                    after_timestamp=after_timestamp,
                    before_timestamp=before_timestamp,
                )
                if not rows:
                    return
                row = rows[0]
                yield row if with_timestamps else row[0]
            return
        effective_batch_size = batch_size or config["BROKER_GENERATOR_BATCH_SIZE"]
        while True:
            token, rows = self._begin_batch(
                source_queue,
                batch_size=effective_batch_size,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                exact_timestamp=exact_timestamp,
                op="move",
                target_queue=target_queue,
            )
            if not rows:
                return
            completed = False
            self._set_active_generator_batch("move")
            try:
                for row in rows:
                    yield row if with_timestamps else row[0]
                completed = True
            finally:
                self._set_active_generator_batch(None)
                if completed:
                    self._commit_move_batch(source_queue, target_queue, token, rows)
                else:
                    self._rollback_batch(source_queue, token, rows)

    def list_queues(
        self,
        *,
        prefix: str | None = None,
        pattern: str | None = None,
    ) -> list[str]:
        self._check_fork_safety()
        if prefix is not None and pattern is not None:
            raise ValueError("prefix and pattern cannot be used together")
        if prefix is not None:
            _validate_queue_prefix(prefix)

        queues = sorted(str(queue) for queue in self._queue_names())
        if prefix is not None:
            queues = [queue for queue in queues if queue.startswith(prefix)]
        if pattern is not None:
            literal_prefix = _literal_prefix_from_fnmatch(pattern)
            if literal_prefix:
                _validate_queue_prefix(literal_prefix)
            queues = [queue for queue in queues if fnmatchcase(queue, pattern)]
        return queues

    def get_queue_stats(self) -> list[tuple[str, int, int]]:
        return [
            (stats.queue, stats.pending, stats.total)
            for stats in self.list_queue_stats()
        ]

    def queue_exists(self, queue: str) -> bool:
        self._validate_queue_name(queue)
        return bool(
            response_int(self._client.zcard(self._qkey(queue, "pending")))
            or response_int(self._client.zcard(self._qkey(queue, "claimed")))
        )

    def get_queue_stat(self, queue: str) -> QueueStats:
        self._validate_queue_name(queue)
        pending = response_int(self._client.zcard(self._qkey(queue, "pending")))
        claimed = response_int(self._client.zcard(self._qkey(queue, "claimed")))
        return QueueStats(
            queue=queue, pending=pending, claimed=claimed, total=pending + claimed
        )

    def list_queue_stats(
        self,
        *,
        prefix: str | None = None,
        pattern: str | None = None,
    ) -> list[QueueStats]:
        if prefix is not None and pattern is not None:
            raise ValueError("prefix and pattern cannot be used together")
        if prefix is not None:
            _validate_queue_prefix(prefix)
        queues = sorted(str(queue) for queue in self._queue_names())
        if prefix is not None:
            queues = [queue for queue in queues if queue.startswith(prefix)]
        if pattern is not None:
            literal_prefix = _literal_prefix_from_fnmatch(pattern)
            if literal_prefix:
                _validate_queue_prefix(literal_prefix)
            queues = [queue for queue in queues if fnmatchcase(queue, pattern)]
        return [self.get_queue_stat(queue) for queue in queues]

    def get_overall_stats(self) -> tuple[int, int]:
        claimed = 0
        total = 0
        for stats in self.list_queue_stats():
            claimed += stats.claimed
            total += stats.total
        return claimed, total

    def count_claimed_messages(self) -> int:
        return sum(stats.claimed for stats in self.list_queue_stats())

    def status(self) -> dict[str, int]:
        _, total = self.get_overall_stats()
        return {
            "total_messages": total,
            "last_timestamp": self._runner.backend_plugin.read_last_ts(self._runner),
            "db_size": 0,
        }

    def _refuse_reserved(self, queue: str | None = None) -> None:
        self.recover_stale_batches(max_age_seconds=self._runner.stale_batch_seconds)
        queues = (
            [queue]
            if queue is not None
            else [str(item) for item in self._queue_names()]
        )
        for name in queues:
            if response_int(self._client.zcard(self._qkey(name, "reserved"))):
                raise OperationalError(
                    "Cannot delete queue while an at_least_once batch is active"
                )

    def delete(self, queue: str | None = None) -> int:
        self._assert_no_reentrant_mutation_during_batch("delete")
        self._refuse_reserved(queue)
        queues = (
            [queue]
            if queue is not None
            else [str(item) for item in self._queue_names()]
        )
        deleted = 0
        for name in queues:
            self._validate_queue_name(name)
            pending = response_list(
                self._client.zrange(self._qkey(name, "pending"), 0, -1)
            )
            claimed = response_list(
                self._client.zrange(self._qkey(name, "claimed"), 0, -1)
            )
            ids = pending + claimed
            deleted += len(ids)
            with self._client.pipeline(transaction=True) as pipe:
                if ids:
                    pipe.hdel(self._key("bodies"), *ids)
                    pipe.zrem(self._keys.all_ids, *ids)
                pipe.delete(self._qkey(name, "pending"))
                pipe.delete(self._qkey(name, "claimed"))
                pipe.delete(self._qkey(name, "reserved"))
                pipe.srem(self._key("queues"), name)
                pipe.execute()
        return deleted

    def delete_message_ids(self, queue: str, message_ids: Sequence[int]) -> int:
        self._validate_queue_name(queue)
        self._assert_no_reentrant_mutation_during_batch("delete_message_ids")
        if not message_ids:
            return 0

        deduped = tuple(dict.fromkeys(message_ids))
        if self._runner.stale_batch_seconds >= 0:
            self.recover_stale_batches(max_age_seconds=self._runner.stale_batch_seconds)
        encoded_ids = [encode_id(message_id) for message_id in deduped]
        try:
            result = self._client.eval(
                scripts.DELETE_MESSAGE_IDS,
                6,
                self._qkey(queue, "pending"),
                self._qkey(queue, "claimed"),
                self._qkey(queue, "reserved"),
                self._keys.bodies,
                self._keys.all_ids,
                self._keys.queues,
                queue,
                *encoded_ids,
            )
        except redis.RedisError as exc:
            raise _translate_redis_error(exc) from exc
        result_int = response_int(result)
        if result_int < 0:
            raise OperationalError(
                "Cannot delete message while an at_least_once batch is active"
            )
        return result_int

    def delete_from_queues(
        self,
        queue_names: Sequence[str],
        *,
        before_timestamp: int | None = None,
    ) -> int:
        self._check_fork_safety()
        self._assert_no_reentrant_mutation_during_batch("delete_from_queues")
        if isinstance(queue_names, (str, bytes)):
            raise TypeError(
                "queue_names must be a sequence of queue names, not a string"
            )
        before_timestamp = validate_timestamp_bound(
            "before_timestamp", before_timestamp
        )

        deduped = tuple(dict.fromkeys(queue_names))
        for queue in deduped:
            self._validate_queue_name(queue)
        if not deduped:
            return 0

        self.recover_stale_batches(max_age_seconds=self._runner.stale_batch_seconds)
        keys = [self._keys.bodies, self._keys.all_ids, self._keys.queues]
        for queue in deduped:
            keys.extend(
                (
                    self._qkey(queue, "pending"),
                    self._qkey(queue, "claimed"),
                    self._qkey(queue, "reserved"),
                )
            )
        try:
            result = self._client.eval(
                scripts.DELETE_FROM_QUEUES,
                len(keys),
                *keys,
                str(len(deduped)),
                max_bound(before_timestamp),
                *deduped,
            )
        except redis.RedisError as exc:
            raise _translate_redis_error(exc) from exc
        result_int = response_int(result)
        if result_int < 0:
            raise OperationalError(
                "Cannot delete message while an at_least_once batch is active"
            )
        return result_int

    def find_message_ids(
        self,
        queue: str,
        *,
        body_contains: str,
        limit: int = BODY_SEARCH_DEFAULT_LIMIT,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        include_claimed: bool = False,
    ) -> list[int]:
        self._check_fork_safety()
        self._validate_queue_name(queue)
        body_contains = validate_body_contains(body_contains)
        limit = validate_body_search_limit(limit)
        after_timestamp = validate_timestamp_bound("after_timestamp", after_timestamp)
        before_timestamp = validate_timestamp_bound(
            "before_timestamp", before_timestamp
        )
        self.recover_stale_batches(max_age_seconds=self._runner.stale_batch_seconds)

        source_keys = [self._qkey(queue, "pending")]
        if include_claimed:
            source_keys.append(self._qkey(queue, "claimed"))
        source_bounds = {key: min_bound(after_timestamp) for key in source_keys}
        exhausted: set[str] = set()
        maxb = max_bound(before_timestamp)
        reserved_key = self._qkey(queue, "reserved")
        matches: list[int] = []

        while len(matches) < limit and len(exhausted) < len(source_keys):
            candidate_ids: set[str] = set()
            for source_key in source_keys:
                if source_key in exhausted:
                    continue
                ids = [
                    str(encoded)
                    for encoded in response_list(
                        self._client.zrangebylex(
                            source_key,
                            source_bounds[source_key],
                            maxb,
                            start=0,
                            num=BODY_SEARCH_REDIS_SCAN_CHUNK_SIZE,
                        )
                    )
                ]
                if not ids:
                    exhausted.add(source_key)
                    continue
                source_bounds[source_key] = f"({ids[-1]}"
                candidate_ids.update(ids)
                if len(ids) < BODY_SEARCH_REDIS_SCAN_CHUNK_SIZE:
                    exhausted.add(source_key)

            if not candidate_ids:
                continue

            ordered_ids = sorted(candidate_ids)
            bodies = response_list(self._client.hmget(self._keys.bodies, ordered_ids))
            with self._client.pipeline(transaction=False) as pipe:
                for encoded in ordered_ids:
                    pipe.zscore(reserved_key, encoded)
                reserved_scores = response_list(pipe.execute())

            for encoded, body, reserved_score in zip(
                ordered_ids, bodies, reserved_scores, strict=False
            ):
                if len(matches) >= limit:
                    break
                if reserved_score is not None or body is None:
                    continue
                if body_contains in str(body):
                    matches.append(decode_id(encoded))

        return matches

    def broadcast(self, message: str, *, pattern: str | None = None) -> int:
        self._validate_message_size(message)
        self._assert_no_reentrant_mutation_during_batch("broadcast")
        queues = sorted(str(queue) for queue in self._queue_names())
        if pattern:
            queues = [queue for queue in queues if fnmatchcase(queue, pattern)]
        if not queues:
            return 0
        for queue in queues:
            self._write_message(queue, message)
        return len(queues)

    def queue_exists_and_has_messages(self, queue: str) -> bool:
        return self.queue_exists(queue)

    def has_pending_messages(
        self, queue: str, after_timestamp: int | None = None
    ) -> bool:
        self._validate_queue_name(queue)
        reserved = self._qkey(queue, "reserved")
        offset = 0
        while True:
            ids = self._zrange_pending(
                queue,
                limit=64,
                offset=offset,
                after_timestamp=after_timestamp,
            )
            if not ids:
                return False
            if any(self._client.zscore(reserved, encoded) is None for encoded in ids):
                return True
            offset += len(ids)

    def get_data_version(self) -> int | None:
        return None

    def vacuum(self, compact: bool = False) -> None:
        del compact
        self._assert_no_reentrant_mutation_during_batch("vacuum")
        batch_size = int(self._config["BROKER_VACUUM_BATCH_SIZE"])
        for queue in [str(item) for item in self._queue_names()]:
            claimed_key = self._qkey(queue, "claimed")
            ids = response_list(self._client.zrange(claimed_key, 0, batch_size - 1))
            if not ids:
                continue
            with self._client.pipeline(transaction=True) as pipe:
                pipe.zrem(claimed_key, *ids)
                pipe.hdel(self._keys.bodies, *ids)
                pipe.zrem(self._keys.all_ids, *ids)
                pipe.execute()
            if not self.queue_exists(queue):
                self._client.srem(self._key("queues"), queue)

    def recover_stale_batches(self, *, max_age_seconds: int) -> int:
        if max_age_seconds < 0:
            return 0
        cutoff_ns = time.time_ns() - (max_age_seconds * 1_000_000_000)
        recovered = 0
        pattern = self._key("batches", "*", "meta")
        for meta_key in list(self._client.scan_iter(pattern)):
            if not is_namespace_key(self._prefix, meta_key):
                continue
            parts = str(meta_key).split(":")
            if len(parts) < 4:
                continue
            token = parts[-2]
            meta = response_dict(self._client.hgetall(meta_key))
            source = str(meta.get("source", ""))
            created_raw = meta.get("created_ns")
            if not source or created_raw is None:
                continue
            try:
                created_ns = int(created_raw)
            except (TypeError, ValueError):
                continue
            if created_ns > cutoff_ns:
                continue
            ids_key = self._keys.batch_ids(token)
            ids = list(response_set(self._client.smembers(ids_key)))
            with self._client.pipeline(transaction=True) as pipe:
                if ids:
                    pipe.zrem(self._keys.reserved(source), *ids)
                pipe.delete(ids_key)
                pipe.delete(meta_key)
                pipe.execute()
            recovered += len(ids)
        return recovered

    def _maybe_recover_stale_batches(self) -> None:
        max_age_seconds = self._runner.stale_batch_seconds
        if max_age_seconds < 0:
            return
        now_ns = time.monotonic_ns()
        min_interval_ns = 0 if max_age_seconds == 0 else 1_000_000_000
        if now_ns - self._runner._last_recovery_check_ns < min_interval_ns:
            return
        with self._runner._recovery_lock:
            if now_ns - self._runner._last_recovery_check_ns < min_interval_ns:
                return
            self._runner._last_recovery_check_ns = now_ns
            self.recover_stale_batches(max_age_seconds=max_age_seconds)

    def _load_aliases_locked(self) -> None:
        self._alias_cache = {
            str(alias): str(target)
            for alias, target in response_dict(
                self._client.hgetall(self._key("aliases"))
            ).items()
        }
        self._alias_cache_version = self.get_alias_version()

    def get_alias_version(self) -> int:
        raw = self._client.hget(self._key("meta"), "alias_version")
        return response_int(raw or 0)

    def resolve_alias(self, alias: str) -> str | None:
        value = self._client.hget(self._key("aliases"), alias)
        return str(value) if value is not None else None

    def canonicalize_queue(self, queue: str) -> str:
        return self.resolve_alias(queue) or queue

    def has_alias(self, alias: str) -> bool:
        return bool(self._client.hexists(self._key("aliases"), alias))

    def list_aliases(self) -> list[tuple[str, str]]:
        return sorted(
            (str(alias), str(target))
            for alias, target in response_dict(
                self._client.hgetall(self._key("aliases"))
            ).items()
        )

    def aliases_for_target(self, target: str) -> list[str]:
        return sorted(alias for alias, value in self.list_aliases() if value == target)

    def _validate_alias_target(self, alias: str, target: str) -> None:
        if alias == target:
            raise ValueError("Alias and target must differ")
        if not alias:
            raise ValueError("Alias name cannot be empty")
        if alias.startswith(ALIAS_PREFIX):
            raise ValueError("Alias names should not include the '@' prefix")
        if target.startswith(ALIAS_PREFIX):
            raise ValueError("Target names should not include the '@' prefix")
        if not target:
            raise ValueError("Alias target cannot be empty")

    def add_alias(self, alias: str, target: str) -> None:
        self._assert_no_reentrant_mutation_during_batch("add_alias")
        self._validate_alias_target(alias, target)
        aliases = dict(self.list_aliases())
        if alias in aliases:
            raise ValueError(f"Alias '{alias}' already exists")
        if target in aliases:
            raise ValueError("Cannot target another alias")
        if self.queue_exists_and_has_messages(alias):
            warnings.warn(
                (
                    f"Queue '{alias}' already exists with messages. "
                    f"The alias @{alias} will redirect to '{target}' while "
                    f"the queue {alias} remains accessible directly."
                ),
                RuntimeWarning,
                stacklevel=3,
            )
        with self._client.pipeline(transaction=True) as pipe:
            pipe.hset(self._key("aliases"), alias, target)
            pipe.hset(self._key("meta"), "alias_version", str(time.time_ns()))
            pipe.execute()

    def remove_alias(self, alias: str) -> None:
        self._assert_no_reentrant_mutation_during_batch("remove_alias")
        with self._client.pipeline(transaction=True) as pipe:
            pipe.hdel(self._key("aliases"), alias)
            pipe.hset(self._key("meta"), "alias_version", str(time.time_ns()))
            pipe.execute()

    def get_meta(self) -> dict[str, int | str]:
        meta = response_dict(self._client.hgetall(self._key("meta")))
        return {
            "magic": str(meta.get("magic", "")),
            "schema_version": int(meta.get("schema_version", 0)),
            "last_ts": int(meta.get("last_ts", 0)),
            "alias_version": int(meta.get("alias_version", 0)),
        }

    def close(self) -> None:
        self._runner.release_thread_connection()

    def shutdown(self) -> None:
        self._runner.shutdown()

    def __enter__(self) -> RedisBrokerCore:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        self.close()
        return False
