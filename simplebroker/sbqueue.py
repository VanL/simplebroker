"""User-friendly Queue API for SimpleBroker.

This module provides a simplified interface for working with individual message
queues without managing the underlying database connection.
"""

import logging
import threading
import weakref
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal, Protocol, TypedDict, TypeVar, Union, cast, overload

from ._backend_plugins import (
    ActivityWaiter,
    BackendAwareRunner,
    BackendPlugin,
    BrokerConnection,
    MultiQueueActivityWaiterHook,
    get_backend_plugin,
)
from ._constants import (
    DEFAULT_DB_NAME,
    PEEK_BATCH_SIZE,
    ResolvedConfig,
    snapshot_config,
)
from ._delivery import DeliveryGuarantee, validate_delivery_guarantee
from ._exceptions import QueueNameError
from ._key_material import FrozenValue, freeze_key_material, snapshot_key_material
from ._message_id import MessageIdInput
from ._message_search import BODY_SEARCH_DEFAULT_LIMIT
from ._runner import SQLRunner
from ._selection import validate_selection_order
from ._sidecar import SidecarSession
from ._targets import BrokerTarget
from .db import DBConnection, _validate_queue_name_cached
from .metadata import QueueStats
from .project import target_for_directory

logger = logging.getLogger(__name__)


class MovedMessage(TypedDict):
    """Public shape returned by the high-level :meth:`Queue.move` method."""

    message: str
    timestamp: int


_CloseableItem_co = TypeVar("_CloseableItem_co", covariant=True)


class CloseableIterator(Protocol[_CloseableItem_co]):
    """Single-use iterator with explicit synchronous cleanup."""

    def __iter__(self) -> "CloseableIterator[_CloseableItem_co]": ...

    def __next__(self) -> _CloseableItem_co: ...

    def close(self) -> None: ...


class _DeleteAllSentinel:
    """Private marker that distinguishes omission from an explicit value."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<argument omitted>"


_DELETE_ALL = _DeleteAllSentinel()
_IteratorItem = TypeVar("_IteratorItem")


def _close_iterator(iterator: object) -> None:
    """Close a generator-like iterator when it exposes explicit cleanup."""
    close = getattr(iterator, "close", None)
    if callable(close):
        close()


def _next_or_none_and_close(iterator: Iterator[_IteratorItem]) -> _IteratorItem | None:
    """Take one item from an owned iterator and release it deterministically."""
    try:
        return next(iterator)
    except StopIteration:
        return None
    finally:
        _close_iterator(iterator)


def _moved_message(message: str, timestamp: int) -> MovedMessage:
    """Build the existing ordinary dictionary with its public static shape."""
    return {"message": message, "timestamp": timestamp}


@dataclass(frozen=True)
class _ActivityWaiterIdentity:
    plugin: BackendPlugin
    backend_name: str
    target_key: str | None
    backend_options_key: FrozenValue | None
    runner_id: int | None
    target_arg: str | None
    backend_options_arg: dict[str, Any] | None
    runner_arg: SQLRunner | None

    @property
    def compatibility_key(
        self,
    ) -> tuple[str, str | None, FrozenValue | None, int | None]:
        return (
            self.backend_name,
            self.target_key,
            self.backend_options_key,
            self.runner_id,
        )


def _display_broker_target(target: str | BrokerTarget) -> str:
    """Return a connection-safe target string for Queue diagnostics."""
    if isinstance(target, BrokerTarget):
        return target.display_target
    return str(target)


def _normalize_sqlite_waiter_target(target: str) -> str:
    path = Path(target).expanduser()
    try:
        return str(path.resolve())
    except (OSError, ValueError):
        return str(path)


def _canonicalize_queue_target(target: str | BrokerTarget) -> str | BrokerTarget:
    """Freeze SQLite targets to their construction-time absolute path."""
    if isinstance(target, BrokerTarget):
        if target.backend_name != "sqlite":
            return target
        return replace(
            target,
            target=_normalize_sqlite_waiter_target(target.target),
        )
    return _normalize_sqlite_waiter_target(str(target))


def _default_target_from_config(config: Mapping[str, Any]) -> BrokerTarget:
    """Resolve the implicit Queue target from caller-provided configuration."""

    root = (
        Path(str(config["BROKER_DEFAULT_DB_LOCATION"]))
        if config.get("BROKER_DEFAULT_DB_LOCATION")
        and config.get("BROKER_BACKEND", "sqlite") == "sqlite"
        else Path.cwd()
    )
    return target_for_directory(root, config=config)


class Queue:
    """A user-friendly handle to a specific message queue.

    This class provides a simpler API for working with a single queue.
    By default, uses ephemeral connections (created per operation) for
    maximum safety and minimal lock contention. Set persistent=True for
    performance-critical scenarios where connection overhead matters.
    Persistent Queue handles for the same resolved backend target share
    process-local backend session state. Backends may still create separate
    physical connections per thread or per pool checkout.

    Args:
        name: The name of the queue
        db_path: Path or target for the broker database. ``None`` or ``""``
            resolves the target from configuration.
        persistent: If True, use process-local persistent session state for the
            resolved target. If False (default), use ephemeral connections.
        runner: Optional custom SQLRunner implementation for extensions

    Examples:
        >>> # Default ephemeral mode - recommended for most users
        >>> queue = Queue("tasks")
        >>> queue.write("Process order #123")
        >>> message = queue.read()
        >>> print(message)
        Process order #123

        >>> # Natural string representation
        >>> print(f"Processing {queue}")
        Processing tasks
        >>> logger.info(f"Watching {queue}...")
        INFO: Watching tasks...

        >>> # Debugging representation
        >>> repr(queue)
        Queue('tasks')
        >>> Queue("logs", db_path="/custom/path.db", persistent=True)
        Queue('logs', db_path='/custom/path.db', persistent=True)

        >>> # Persistent mode - for performance-critical code
        >>> with Queue("tasks", persistent=True) as queue:
        ...     for i in range(10000):
        ...         queue.write(f"task_{i}")
    """

    # Type annotations for instance attributes
    conn: DBConnection | None

    def __init__(
        self,
        name: str,
        *,
        db_path: str | BrokerTarget | None = None,
        persistent: bool = False,
        runner: SQLRunner | None = None,
        config: Mapping[str, Any] | None = None,
    ):
        """Initialize a Queue instance.

        Args:
            name: The name of the queue
            db_path: Path or target for the broker database. ``None`` or ``""``
                resolves the target from configuration.
            persistent: If True, maintain a persistent connection.
                       If False (default), use ephemeral connections.
            runner: Optional custom SQLRunner implementation for extensions.
                    Injected runners are caller-owned and are reused for the
                    lifetime of this Queue object.
        """
        queue_name_error = _validate_queue_name_cached(name)
        if queue_name_error is not None:
            raise QueueNameError(queue_name_error)
        self.name = name
        self._persistent = persistent
        self._runner = runner
        self._config = snapshot_config(config)
        self._uses_config_default_target = db_path is None or db_path == ""
        if self._uses_config_default_target:
            resolved_db_path: str | BrokerTarget = _default_target_from_config(
                self._config
            )
        else:
            assert db_path is not None
            resolved_db_path = db_path
        self._db_path: str | BrokerTarget = _canonicalize_queue_target(resolved_db_path)
        self._stop_event: threading.Event | None = None

        # Create DBConnection for persistent queues and injected-runner queues.
        # The built-in no-runner path keeps its current "get in, get out"
        # semantics only when persistent=False.
        if persistent or runner is not None:
            self.conn = DBConnection(
                self._db_path,
                runner,
                config=self._config,
                share_in_process=persistent and runner is None,
            )
        else:
            self.conn = None

        # Install finalizer for cleanup
        self._install_finalizer()

        # Cached last generated timestamp (meta.last_ts)
        self._last_ts: int | None = None
        self._activity_waiter: ActivityWaiter | None = None

    @property
    def db_target(self) -> str | BrokerTarget:
        """Return the configured broker target for this queue."""

        return self._db_path

    @property
    def backend_name(self) -> str:
        """Return the resolved backend plugin name without opening a connection."""

        return self._activity_waiter_identity().backend_name

    def _move_destination_name(self, destination: Union[str, "Queue"]) -> str:
        if not isinstance(destination, Queue):
            return destination

        source_identity = self._activity_waiter_identity().compatibility_key
        destination_identity = destination._activity_waiter_identity().compatibility_key
        if source_identity != destination_identity:
            raise ValueError(
                "Cannot move messages between different broker targets: "
                f"source={_display_broker_target(self.db_target)!r}, "
                f"destination={_display_broker_target(destination.db_target)!r}"
            )
        return destination.name

    @contextmanager
    def get_connection(self) -> Iterator[BrokerConnection]:
        """Get connection for operations - handles both persistent and ephemeral modes.

        This context manager consolidates the connection logic. It yields a
        persistent queue connection/session lease when available, or creates a
        new connection on the fly for the no-runner ephemeral path.

        Yields:
            BrokerConnection: Connection object for database operations
        """
        if self.conn is not None:
            assert self.conn is not None  # Type guard for mypy
            self.conn.set_stop_event(self._stop_event)
            try:
                yield self.conn.get_connection()
            finally:
                self.conn.release_connection_after_use()
        else:
            with DBConnection(self._db_path, self._runner, config=self._config) as conn:
                conn.set_stop_event(self._stop_event)
                yield conn.get_connection()

    def set_stop_event(self, stop_event: threading.Event | None) -> None:
        """Propagate stop event to connections used by this queue."""

        self._stop_event = stop_event
        if self.conn is not None:
            self.conn.set_stop_event(stop_event)

    @property
    def last_ts(self) -> int | None:
        """Return cached meta.last_ts, fetching lazily on first access."""

        if self._last_ts is None:
            # Cache remains None on failure; callers can request an explicit refresh.
            with suppress(Exception), self.get_connection() as connection:
                try:
                    self._last_ts = connection.get_cached_last_timestamp()
                except AttributeError:
                    # Older runners without hint support
                    self._last_ts = connection.refresh_last_timestamp()

        return self._last_ts

    def refresh_last_ts(self) -> int:
        """Refresh cached last timestamp using a lightweight meta-table read."""

        with self.get_connection() as connection:
            latest = connection.refresh_last_timestamp()
        self._last_ts = latest
        return latest

    @contextmanager
    def sidecar(self, *, transaction: bool = False) -> Iterator[SidecarSession]:
        """Open a sidecar-table session against this queue's database.

        Sidecar sessions are thread-affine: create, use, and exit them on the
        same thread. Foreign-thread finalization permanently poisons the
        underlying broker instance; restart the process.

        Connection lifetime follows this queue's mode: ephemeral queues open
        and close a connection for the session ("get in, get out");
        persistent queues reuse their held connection. See
        ``BrokerCore.sidecar`` for transaction semantics and
        ``simplebroker.ext.RESERVED_TABLE_NAMES`` for tables you must not
        touch.
        """
        with (
            self.get_connection() as connection,
            connection.sidecar(transaction=transaction) as session,
        ):
            yield session

    def _update_last_ts_hint(self, connection: BrokerConnection) -> None:
        """Update cached last_ts using the connection's generator state."""

        if self._last_ts is None:
            return

        try:
            candidate = connection.get_cached_last_timestamp()
        except AttributeError:
            return
        self._last_ts = candidate

    def _observe_timestamp(self, timestamp: int) -> None:
        """Advance cached last_ts based on an observed message timestamp."""

        if self._last_ts is None or timestamp > self._last_ts:
            self._last_ts = timestamp

    def write(self, message: str) -> int:
        """Write a message to this queue.

        Args:
            message: The message content to write

        Returns:
            The committed message's unique 64-bit timestamp/message ID —
            the same value read/peek report for this message and the ID
            accepted by exact-ID APIs such as ``peek_one(exact_timestamp=...)``
            and ``delete(message_id=...)``. Unlike ``queue.last_ts`` (a
            broker-global high-water mark that may already reflect another
            writer's later message), the returned value always identifies
            this write's own row.

        Raises:
            QueueNameError: If the queue name is invalid
            MessageError: If the message is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            timestamp: int = connection.write(self.name, message)
            self._update_last_ts_hint(connection)
            return timestamp

    def _insert_records_for_queue(
        self,
        records: Iterable[tuple[str, MessageIdInput]],
    ) -> Iterator[tuple[str, str, MessageIdInput]]:
        for record in records:
            try:
                message, message_id = record
            except (TypeError, ValueError) as exc:
                raise TypeError(
                    "queue insert records must be (message, message_id) tuples"
                ) from exc
            yield self.name, message, message_id

    def insert_messages(self, records: Iterable[tuple[str, MessageIdInput]]) -> None:
        """Insert pending messages into this queue with exact existing IDs.

        IDs may be ints or exact 19-digit strings.
        """
        with self.get_connection() as connection:
            connection.insert_messages(self._insert_records_for_queue(records))
            self._update_last_ts_hint(connection)

    def generate_timestamp(self) -> int:
        """Generate a broker-compatible timestamp using the underlying database.

        Returns:
            64-bit hybrid timestamp unique within the database.
        """
        with self.get_connection() as connection:
            timestamp = connection.generate_timestamp()
            self._last_ts = timestamp
            return timestamp

    # Convenience alias
    get_ts = generate_timestamp

    @overload
    def read(
        self,
        *,
        all_messages: Literal[False] = False,
        with_timestamps: Literal[False] = False,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        message_id: MessageIdInput | None = None,
        order: str = "oldest",
    ) -> str | None: ...

    @overload
    def read(
        self,
        *,
        all_messages: Literal[False] = False,
        with_timestamps: Literal[True],
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        message_id: MessageIdInput | None = None,
        order: str = "oldest",
    ) -> tuple[str, int] | None: ...

    @overload
    def read(
        self,
        *,
        all_messages: Literal[True],
        with_timestamps: Literal[False] = False,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        message_id: MessageIdInput | None = None,
        order: str = "oldest",
    ) -> CloseableIterator[str]: ...

    @overload
    def read(
        self,
        *,
        all_messages: Literal[True],
        with_timestamps: Literal[True],
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        message_id: MessageIdInput | None = None,
        order: str = "oldest",
    ) -> CloseableIterator[tuple[str, int]]: ...

    @overload
    def read(
        self,
        *,
        all_messages: bool = False,
        with_timestamps: bool = False,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        message_id: MessageIdInput | None = None,
        order: str = "oldest",
    ) -> str | tuple[str, int] | CloseableIterator[str | tuple[str, int]] | None: ...

    def read(
        self,
        *,
        all_messages: bool = False,
        with_timestamps: bool = False,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        message_id: MessageIdInput | None = None,
        order: str = "oldest",
    ) -> str | tuple[str, int] | CloseableIterator[str | tuple[str, int]] | None:
        """Read and remove message(s) from the queue (CLI-mirroring method).

        This is the high-level method that mirrors CLI behavior. For more precise
        control, use the granular methods: read_one(), read_many(), read_generator().

        With ``all_messages=True``, the returned closeable iterator is lazy:
        creating it starts no Queue operation. Create, advance, exhaust, and
        close it on the same thread. A caller that may stop early must close it
        before closing this Queue or a higher-level client.

        Args:
            all_messages: If True, read all messages as a closeable iterator
            with_timestamps: If True, include timestamps in results
            after_timestamp: Only read messages newer than this timestamp
            before_timestamp: Only read messages older than this timestamp
            message_id: Read specific message by ID as an int or exact 19-digit string
                (cannot be used with other filters)

        Returns:
            Depends on parameters:
            - Single message (str or tuple) if all_messages=False
            - Closeable iterator if all_messages=True
            - None if no messages match criteria

        Raises:
            ValueError: If conflicting parameters are provided
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        validated_order = validate_selection_order(order)
        if all_messages and validated_order != "oldest":
            raise ValueError("order='newest' cannot be used with all_messages=True")

        has_range_filter = after_timestamp is not None or before_timestamp is not None
        if message_id is not None and (all_messages or has_range_filter):
            raise ValueError(
                "message_id cannot be used with all_messages, after_timestamp, "
                "or before_timestamp"
            )

        if message_id is not None:
            # Read specific message by ID
            return self.read_one(
                exact_timestamp=message_id,
                with_timestamps=with_timestamps,
                order=validated_order,
            )
        elif all_messages:
            # Return generator for all messages
            return self.read_generator(
                with_timestamps=with_timestamps,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
            )
        else:
            # Read single message
            if has_range_filter:
                results = self.read_many(
                    1,
                    with_timestamps=with_timestamps,
                    after_timestamp=after_timestamp,
                    before_timestamp=before_timestamp,
                    order=validated_order,
                )
                return results[0] if results else None
            else:
                return self.read_one(
                    with_timestamps=with_timestamps,
                    order=validated_order,
                )

    # ========== Granular Read API (maps to internal claim methods) ==========

    @overload
    def read_one(
        self,
        *,
        exact_timestamp: MessageIdInput | None = None,
        with_timestamps: Literal[False] = False,
        order: str = "oldest",
    ) -> str | None: ...

    @overload
    def read_one(
        self,
        *,
        exact_timestamp: MessageIdInput | None = None,
        with_timestamps: Literal[True],
        order: str = "oldest",
    ) -> tuple[str, int] | None: ...

    @overload
    def read_one(
        self,
        *,
        exact_timestamp: MessageIdInput | None = None,
        with_timestamps: bool,
        order: str = "oldest",
    ) -> str | tuple[str, int] | None: ...

    def read_one(
        self,
        *,
        exact_timestamp: MessageIdInput | None = None,
        with_timestamps: bool = False,
        order: str = "oldest",
    ) -> str | tuple[str, int] | None:
        """Read and remove exactly one message from the queue.

        This method provides exactly-once delivery semantics: the message is
        committed before being returned.

        Args:
            exact_timestamp: If provided, read only message with this exact ID
            with_timestamps: If True, return (message, timestamp) tuple

        Returns:
            Message string or (message, timestamp) tuple if with_timestamps=True,
            None if queue is empty or message not found

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        validated_order = validate_selection_order(order)
        with self.get_connection() as connection:
            return connection.claim_one(
                self.name,
                exact_timestamp=exact_timestamp,
                with_timestamps=with_timestamps,
                order=validated_order,
            )

    @overload
    def read_many(
        self,
        limit: int,
        *,
        with_timestamps: Literal[False] = False,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        order: str = "oldest",
    ) -> list[str]: ...

    @overload
    def read_many(
        self,
        limit: int,
        *,
        with_timestamps: Literal[True],
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        order: str = "oldest",
    ) -> list[tuple[str, int]]: ...

    @overload
    def read_many(
        self,
        limit: int,
        *,
        with_timestamps: bool,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        order: str = "oldest",
    ) -> list[str] | list[tuple[str, int]]: ...

    def read_many(
        self,
        limit: int,
        *,
        with_timestamps: bool = False,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        order: str = "oldest",
    ) -> list[str] | list[tuple[str, int]]:
        """Read and remove multiple messages from the queue.

        Args:
            limit: Maximum number of messages to read
            with_timestamps: If True, return list of (message, timestamp) tuples
            delivery_guarantee: Delivery contract for materializing messages.
                Materialized batch APIs commit before returning, so
                ``"at_least_once"`` is satisfied by the stricter exactly-once
                behavior. Use ``read_generator()`` when you need retry-on-stop
                batch processing.
            after_timestamp: Only read messages newer than this timestamp
            before_timestamp: Only read messages older than this timestamp

        Returns:
            list of messages or list of (message, timestamp) tuples if with_timestamps=True

        Raises:
            ValueError: If limit < 1
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        validated_order = validate_selection_order(order)
        validated_delivery = validate_delivery_guarantee(delivery_guarantee)
        with self.get_connection() as connection:
            return connection.claim_many(
                self.name,
                limit,
                with_timestamps=with_timestamps,
                delivery_guarantee=validated_delivery,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                order=validated_order,
            )

    @overload
    def read_generator(
        self,
        *,
        with_timestamps: Literal[False] = False,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
    ) -> CloseableIterator[str]: ...

    @overload
    def read_generator(
        self,
        *,
        with_timestamps: Literal[True],
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
    ) -> CloseableIterator[tuple[str, int]]: ...

    @overload
    def read_generator(
        self,
        *,
        with_timestamps: bool,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
    ) -> CloseableIterator[str | tuple[str, int]]: ...

    def read_generator(
        self,
        *,
        with_timestamps: bool = False,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
    ) -> CloseableIterator[str | tuple[str, int]]:
        """Generator that reads and removes messages from the queue.

        The returned closeable iterator is lazy: creating it starts no Queue
        operation. Create, advance, exhaust, and close it on the same thread.
        Even exactly-once iteration retains the suspended outer Queue operation
        after each committed yield. A caller that may stop early must close the
        iterator before closing this Queue or a higher-level client.

        For SQL-backed at-least-once iteration, finalization from another
        thread permanently poisons the instance; restart the process.
        Redis/Valkey does not share the SQL poison mechanism, but its behavior
        does not make cross-thread use portable.

        This is memory-efficient for processing large queues.

        Args:
            with_timestamps: If True, yield (message, timestamp) tuples
            delivery_guarantee: Delivery semantics
                - exactly_once: Process one message at a time (safer, slower)
                - at_least_once: Commit each batch after it is fully yielded
            after_timestamp: Only read messages newer than this timestamp
            before_timestamp: Only read messages older than this timestamp
            exact_timestamp: Only read message with this exact ID

        Yields:
            Messages or (message, timestamp) tuples if with_timestamps=True

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        validated_delivery = validate_delivery_guarantee(delivery_guarantee)
        with self.get_connection() as connection:
            yield from connection.claim_generator(
                self.name,
                with_timestamps=with_timestamps,
                delivery_guarantee=validated_delivery,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                exact_timestamp=exact_timestamp,
            )

    @overload
    def peek(
        self,
        *,
        all_messages: Literal[False] = False,
        with_timestamps: Literal[False] = False,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        message_id: MessageIdInput | None = None,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> str | None: ...

    @overload
    def peek(
        self,
        *,
        all_messages: Literal[False] = False,
        with_timestamps: Literal[True],
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        message_id: MessageIdInput | None = None,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> tuple[str, int] | None: ...

    @overload
    def peek(
        self,
        *,
        all_messages: Literal[True],
        with_timestamps: Literal[False] = False,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        message_id: MessageIdInput | None = None,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> CloseableIterator[str]: ...

    @overload
    def peek(
        self,
        *,
        all_messages: Literal[True],
        with_timestamps: Literal[True],
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        message_id: MessageIdInput | None = None,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> CloseableIterator[tuple[str, int]]: ...

    @overload
    def peek(
        self,
        *,
        all_messages: bool = False,
        with_timestamps: bool = False,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        message_id: MessageIdInput | None = None,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> str | tuple[str, int] | CloseableIterator[str | tuple[str, int]] | None: ...

    def peek(
        self,
        *,
        all_messages: bool = False,
        with_timestamps: bool = False,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        message_id: MessageIdInput | None = None,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> str | tuple[str, int] | CloseableIterator[str | tuple[str, int]] | None:
        """View message(s) without removing them from the queue (CLI-mirroring method).

        This is the high-level method that mirrors CLI behavior. For more precise
        control, use the granular methods: peek_one(), peek_many(), peek_generator().

        With ``all_messages=True``, the returned closeable iterator is lazy:
        creating it starts no Queue operation. Its first advancement starts the
        operation, and callers must advance, exhaust, or close it on the same
        thread. A caller that may stop early must close the iterator before
        closing this Queue or a higher-level client.

        Args:
            all_messages: If True, peek at all messages as a generator
            with_timestamps: If True, include timestamps in results
            after_timestamp: Only peek at messages newer than this timestamp
            before_timestamp: Only peek at messages older than this timestamp
            message_id: Peek at specific message by ID as an int or exact 19-digit
                string (cannot be used with other filters)
            include_claimed: If True, also return claimed (consumed but not
                yet vacuumed) messages, merged in message-ID order. Claimed
                rows are deletion-pending: vacuum may remove them at any
                time, and seeing one says nothing about delivery state.
                Peeking never changes claim state.

        Returns:
            Depends on parameters:
            - Single message (str or tuple) if all_messages=False
            - Generator if all_messages=True
            - None if no messages match criteria

        Raises:
            ValueError: If conflicting parameters are provided
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        validated_order = validate_selection_order(order)
        if all_messages and validated_order != "oldest":
            raise ValueError("order='newest' cannot be used with all_messages=True")

        has_range_filter = after_timestamp is not None or before_timestamp is not None
        if message_id is not None and (all_messages or has_range_filter):
            raise ValueError(
                "message_id cannot be used with all_messages, after_timestamp, "
                "or before_timestamp"
            )

        if message_id is not None:
            # Peek at specific message by ID
            return self.peek_one(
                exact_timestamp=message_id,
                with_timestamps=with_timestamps,
                include_claimed=include_claimed,
                order=validated_order,
            )
        elif all_messages:
            # Return generator for all messages
            return self.peek_generator(
                with_timestamps=with_timestamps,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                include_claimed=include_claimed,
            )
        else:
            # Peek at single message
            if has_range_filter:
                results = self.peek_many(
                    1,
                    with_timestamps=with_timestamps,
                    after_timestamp=after_timestamp,
                    before_timestamp=before_timestamp,
                    include_claimed=include_claimed,
                    order=validated_order,
                )
                return results[0] if results else None
            else:
                return self.peek_one(
                    with_timestamps=with_timestamps,
                    include_claimed=include_claimed,
                    order=validated_order,
                )

    # ========== Granular Peek API ==========

    @overload
    def peek_one(
        self,
        *,
        exact_timestamp: MessageIdInput | None = None,
        with_timestamps: Literal[False] = False,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> str | None: ...

    @overload
    def peek_one(
        self,
        *,
        exact_timestamp: MessageIdInput | None = None,
        with_timestamps: Literal[True],
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> tuple[str, int] | None: ...

    @overload
    def peek_one(
        self,
        *,
        exact_timestamp: MessageIdInput | None = None,
        with_timestamps: bool,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> str | tuple[str, int] | None: ...

    def peek_one(
        self,
        *,
        exact_timestamp: MessageIdInput | None = None,
        with_timestamps: bool = False,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> str | tuple[str, int] | None:
        """Peek at exactly one message without removing it from the queue.

        Args:
            exact_timestamp: If provided, peek only at message with this exact ID
            with_timestamps: If True, return (message, timestamp) tuple
            include_claimed: If True, also return claimed (consumed but not
                yet vacuumed) messages, merged in message-ID order. Claimed
                rows are deletion-pending: vacuum may remove them at any
                time, and seeing one says nothing about delivery state.
                Peeking never changes claim state.

        Returns:
            Message string or (message, timestamp) tuple if with_timestamps=True,
            None if queue is empty or message not found

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        validated_order = validate_selection_order(order)
        with self.get_connection() as connection:
            return connection.peek_one(
                self.name,
                exact_timestamp=exact_timestamp,
                with_timestamps=with_timestamps,
                include_claimed=include_claimed,
                order=validated_order,
            )

    @overload
    def peek_many(
        self,
        limit: int = PEEK_BATCH_SIZE,
        *,
        with_timestamps: Literal[False] = False,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> list[str]: ...

    @overload
    def peek_many(
        self,
        limit: int = PEEK_BATCH_SIZE,
        *,
        with_timestamps: Literal[True],
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> list[tuple[str, int]]: ...

    @overload
    def peek_many(
        self,
        limit: int = PEEK_BATCH_SIZE,
        *,
        with_timestamps: bool,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> list[str] | list[tuple[str, int]]: ...

    def peek_many(
        self,
        limit: int = PEEK_BATCH_SIZE,
        *,
        with_timestamps: bool = False,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        include_claimed: bool = False,
        order: str = "oldest",
    ) -> list[str] | list[tuple[str, int]]:
        """Peek at multiple messages without removing them from the queue.

        Args:
            limit: Maximum number of messages to peek at (default: 1000)
            with_timestamps: If True, return list of (message, timestamp) tuples
            after_timestamp: Only peek at messages newer than this timestamp
            before_timestamp: Only peek at messages older than this timestamp
            include_claimed: If True, also return claimed (consumed but not
                yet vacuumed) messages, merged in message-ID order. Claimed
                rows are deletion-pending: vacuum may remove them at any
                time, and seeing one says nothing about delivery state.
                Peeking never changes claim state.

        Returns:
            list of messages or list of (message, timestamp) tuples if with_timestamps=True

        Raises:
            ValueError: If limit < 1
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        validated_order = validate_selection_order(order)
        with self.get_connection() as connection:
            return connection.peek_many(
                self.name,
                limit,
                with_timestamps=with_timestamps,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                include_claimed=include_claimed,
                order=validated_order,
            )

    @overload
    def peek_generator(
        self,
        *,
        with_timestamps: Literal[False] = False,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
        include_claimed: bool = False,
    ) -> CloseableIterator[str]: ...

    @overload
    def peek_generator(
        self,
        *,
        with_timestamps: Literal[True],
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
        include_claimed: bool = False,
    ) -> CloseableIterator[tuple[str, int]]: ...

    @overload
    def peek_generator(
        self,
        *,
        with_timestamps: bool,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
        include_claimed: bool = False,
    ) -> CloseableIterator[str | tuple[str, int]]: ...

    def peek_generator(
        self,
        *,
        with_timestamps: bool = False,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
        include_claimed: bool = False,
    ) -> CloseableIterator[str | tuple[str, int]]:
        """Generator that peeks at messages without removing them from the queue.

        This is memory-efficient for viewing large queues. The returned
        closeable iterator is lazy: creating it starts no Queue operation. Its
        first advancement starts the operation, and callers must advance,
        exhaust, or close it on the same thread. A caller that may stop early
        must close the iterator before closing this Queue or a higher-level
        client. Closing before first advancement starts no operation and makes
        the iterator terminal.

        Args:
            with_timestamps: If True, yield (message, timestamp) tuples
            after_timestamp: Only peek at messages newer than this timestamp
            before_timestamp: Only peek at messages older than this timestamp
            exact_timestamp: Only peek at message with this exact ID
            include_claimed: If True, also return claimed (consumed but not
                yet vacuumed) messages, merged in message-ID order. Claimed
                rows are deletion-pending: vacuum may remove them at any
                time, and seeing one says nothing about delivery state.
                Peeking never changes claim state.

        Yields:
            Messages or (message, timestamp) tuples if with_timestamps=True

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            yield from connection.peek_generator(
                self.name,
                with_timestamps=with_timestamps,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                exact_timestamp=exact_timestamp,
                include_claimed=include_claimed,
            )

    @overload
    def move(
        self,
        destination: Union[str, "Queue"],
        *,
        message_id: MessageIdInput | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        all_messages: Literal[False] = False,
        order: str = "oldest",
    ) -> MovedMessage | None: ...

    @overload
    def move(
        self,
        destination: Union[str, "Queue"],
        *,
        message_id: MessageIdInput | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        all_messages: Literal[True],
        order: str = "oldest",
    ) -> CloseableIterator[MovedMessage]: ...

    @overload
    def move(
        self,
        destination: Union[str, "Queue"],
        *,
        message_id: MessageIdInput | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        all_messages: bool,
        order: str = "oldest",
    ) -> MovedMessage | None | CloseableIterator[MovedMessage]: ...

    def move(  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-016] exception
        self,
        destination: Union[str, "Queue"],
        *,
        message_id: MessageIdInput | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        all_messages: bool = False,
        order: str = "oldest",
    ) -> MovedMessage | None | CloseableIterator[MovedMessage]:
        """Move messages from this queue to another (CLI-mirroring method).

        This is the high-level method that mirrors CLI behavior. For more precise
        control, use the granular methods: move_one(), move_many(), move_generator().

        With ``all_messages=True``, the returned closeable iterator is lazy:
        creating it starts no Queue operation. Create, advance, exhaust, and
        close it on the same thread. A caller that may stop early must close it
        before closing this Queue or a higher-level client.

        Args:
            destination: Target queue (name or Queue instance).
            message_id: If provided, move only this specific message by int ID
                or exact 19-digit string.
            after_timestamp: If provided, only move messages newer than this timestamp.
            before_timestamp: If provided, only move messages older than this timestamp.
            all_messages: If True, move all messages. Cannot be used with message_id.

        Returns:
            Depends on parameters:
            - Single dict with 'message' and 'timestamp' if moving one message
            - Closeable iterator of dicts if all_messages=True
            - None if no messages to move

        Raises:
            ValueError: If source and destination are the same, or if conflicting options are used.
            QueueNameError: If queue names are invalid
            OperationalError: If the database is locked/busy
        """
        validated_order = validate_selection_order(order)
        if all_messages and validated_order != "oldest":
            raise ValueError("order='newest' cannot be used with all_messages=True")

        # Get destination queue name
        dest_name = self._move_destination_name(destination)

        # Check for same source and destination
        if self.name == dest_name:
            raise ValueError("Source and destination queues cannot be the same")

        # Check for conflicting options
        has_range_filter = after_timestamp is not None or before_timestamp is not None
        if message_id is not None and (all_messages or has_range_filter):
            raise ValueError(
                "message_id cannot be used with all_messages, after_timestamp, "
                "or before_timestamp"
            )

        if message_id is not None:
            # Move specific message by ID
            result = self.move_one(
                dest_name,
                exact_timestamp=message_id,
                require_unclaimed=False,  # Allow moving claimed messages by ID
                with_timestamps=True,
                order=validated_order,
            )
            if result:
                return _moved_message(result[0], result[1])
            return None
        elif all_messages:
            # Return generator for all messages
            def dict_generator() -> CloseableIterator[MovedMessage]:
                generator = self.move_generator(
                    dest_name,
                    with_timestamps=True,
                    after_timestamp=after_timestamp,
                    before_timestamp=before_timestamp,
                )
                try:
                    for result in generator:
                        msg, ts = result
                        yield _moved_message(msg, ts)
                finally:
                    generator.close()

            return dict_generator()
        else:
            # Move single message
            if has_range_filter:
                results = self.move_many(
                    dest_name,
                    1,
                    with_timestamps=True,
                    after_timestamp=after_timestamp,
                    before_timestamp=before_timestamp,
                    order=validated_order,
                )
                if not results:
                    return None
                msg, ts = results[0]
                return _moved_message(msg, ts)
            else:
                result = self.move_one(
                    dest_name,
                    with_timestamps=True,
                    order=validated_order,
                )
                if result:
                    return _moved_message(result[0], result[1])
                return None

    # ========== Granular Move API ==========

    @overload
    def move_one(
        self,
        destination: Union[str, "Queue"],
        *,
        exact_timestamp: MessageIdInput | None = None,
        require_unclaimed: bool = True,
        with_timestamps: Literal[False] = False,
        order: str = "oldest",
    ) -> str | None: ...

    @overload
    def move_one(
        self,
        destination: Union[str, "Queue"],
        *,
        exact_timestamp: MessageIdInput | None = None,
        require_unclaimed: bool = True,
        with_timestamps: Literal[True],
        order: str = "oldest",
    ) -> tuple[str, int] | None: ...

    @overload
    def move_one(
        self,
        destination: Union[str, "Queue"],
        *,
        exact_timestamp: MessageIdInput | None = None,
        require_unclaimed: bool = True,
        with_timestamps: bool,
        order: str = "oldest",
    ) -> str | tuple[str, int] | None: ...

    def move_one(
        self,
        destination: Union[str, "Queue"],
        *,
        exact_timestamp: MessageIdInput | None = None,
        require_unclaimed: bool = True,
        with_timestamps: bool = False,
        order: str = "oldest",
    ) -> str | tuple[str, int] | None:
        """Move exactly one message from this queue to another.

        Atomic operation with exactly-once semantics.

        Args:
            destination: Target queue (name or Queue instance)
            exact_timestamp: If provided, move only message with this exact ID
            require_unclaimed: If True (default), only move unclaimed messages.
                             If False, move any message (including claimed).
            with_timestamps: If True, return (message, timestamp) tuple

        Returns:
            Message string or (message, timestamp) tuple if with_timestamps=True,
            None if no messages to move or message not found

        Raises:
            ValueError: If source and destination are the same
            QueueNameError: If queue names are invalid
            OperationalError: If the database is locked/busy
        """
        validated_order = validate_selection_order(order)
        dest_name = self._move_destination_name(destination)
        if self.name == dest_name:
            raise ValueError("Source and destination queues cannot be the same")

        with self.get_connection() as connection:
            return connection.move_one(
                self.name,
                dest_name,
                exact_timestamp=exact_timestamp,
                require_unclaimed=require_unclaimed,
                with_timestamps=with_timestamps,
                order=validated_order,
            )

    @overload
    def move_many(
        self,
        destination: Union[str, "Queue"],
        limit: int,
        *,
        with_timestamps: Literal[False] = False,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        require_unclaimed: bool = True,
        order: str = "oldest",
    ) -> list[str]: ...

    @overload
    def move_many(
        self,
        destination: Union[str, "Queue"],
        limit: int,
        *,
        with_timestamps: Literal[True],
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        require_unclaimed: bool = True,
        order: str = "oldest",
    ) -> list[tuple[str, int]]: ...

    @overload
    def move_many(
        self,
        destination: Union[str, "Queue"],
        limit: int,
        *,
        with_timestamps: bool,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        require_unclaimed: bool = True,
        order: str = "oldest",
    ) -> list[str] | list[tuple[str, int]]: ...

    def move_many(
        self,
        destination: Union[str, "Queue"],
        limit: int,
        *,
        with_timestamps: bool = False,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        require_unclaimed: bool = True,
        order: str = "oldest",
    ) -> list[str] | list[tuple[str, int]]:
        """Move multiple messages from this queue to another.

        Atomic materialized batch move operation.

        Args:
            destination: Target queue (name or Queue instance)
            limit: Maximum number of messages to move
            with_timestamps: If True, return list of (message, timestamp) tuples
            delivery_guarantee: Delivery contract for materializing messages.
                Materialized batch APIs commit before returning, so
                ``"at_least_once"`` is satisfied by the stricter exactly-once
                behavior. Use ``move_generator()`` when you need retry-on-stop
                batch processing.
            after_timestamp: Only move messages newer than this timestamp
            before_timestamp: Only move messages older than this timestamp
            require_unclaimed: If True (default), only move unclaimed messages

        Returns:
            list of messages or list of (message, timestamp) tuples if with_timestamps=True

        Raises:
            ValueError: If source and destination are the same or limit < 1
            QueueNameError: If queue names are invalid
            OperationalError: If the database is locked/busy
        """
        validated_order = validate_selection_order(order)
        validated_delivery = validate_delivery_guarantee(delivery_guarantee)
        dest_name = self._move_destination_name(destination)
        if self.name == dest_name:
            raise ValueError("Source and destination queues cannot be the same")

        with self.get_connection() as connection:
            return connection.move_many(
                self.name,
                dest_name,
                limit,
                with_timestamps=with_timestamps,
                delivery_guarantee=validated_delivery,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                require_unclaimed=require_unclaimed,
                order=validated_order,
            )

    @overload
    def move_generator(
        self,
        destination: Union[str, "Queue"],
        *,
        with_timestamps: Literal[False] = False,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
    ) -> CloseableIterator[str]: ...

    @overload
    def move_generator(
        self,
        destination: Union[str, "Queue"],
        *,
        with_timestamps: Literal[True],
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
    ) -> CloseableIterator[tuple[str, int]]: ...

    @overload
    def move_generator(
        self,
        destination: Union[str, "Queue"],
        *,
        with_timestamps: bool,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
    ) -> CloseableIterator[str | tuple[str, int]]: ...

    def move_generator(
        self,
        destination: Union[str, "Queue"],
        *,
        with_timestamps: bool = False,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
    ) -> CloseableIterator[str | tuple[str, int]]:
        """Generator that moves messages from this queue to another.

        The returned closeable iterator is lazy: creating it starts no Queue
        operation. Create, advance, exhaust, and close it on the same thread.
        Even exactly-once iteration retains the suspended outer Queue operation
        after each committed yield. A caller that may stop early must close the
        iterator before closing this Queue or a higher-level client.

        For SQL-backed at-least-once iteration, finalization from another
        thread permanently poisons the instance; restart the process.
        Redis/Valkey does not share the SQL poison mechanism, but its behavior
        does not make cross-thread use portable.

        Args:
            destination: Target queue (name or Queue instance)
            with_timestamps: If True, yield (message, timestamp) tuples
            delivery_guarantee: Delivery semantics
                - exactly_once: Process one message at a time (safer, slower)
                - at_least_once: Commit each batch after it is fully yielded
            after_timestamp: Only move messages newer than this timestamp
            before_timestamp: Only move messages older than this timestamp
            exact_timestamp: Only move message with this exact ID

        Yields:
            Messages or (message, timestamp) tuples if with_timestamps=True

        Raises:
            ValueError: If source and destination are the same
            QueueNameError: If queue names are invalid
            OperationalError: If the database is locked/busy
        """
        validated_delivery = validate_delivery_guarantee(delivery_guarantee)
        dest_name = self._move_destination_name(destination)
        if self.name == dest_name:
            raise ValueError("Source and destination queues cannot be the same")

        with self.get_connection() as connection:
            yield from connection.move_generator(
                self.name,
                dest_name,
                with_timestamps=with_timestamps,
                delivery_guarantee=validated_delivery,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                exact_timestamp=exact_timestamp,
            )

    @overload
    def delete(self) -> bool: ...

    @overload
    def delete(self, *, message_id: MessageIdInput) -> bool: ...

    def delete(
        self,
        *,
        message_id: MessageIdInput | None | _DeleteAllSentinel = _DELETE_ALL,
    ) -> bool:
        """Delete messages from this queue.

        Args:
            message_id: If provided, delete only the message with this specific
                int ID or exact 19-digit string ID. Omit the argument to delete
                all messages in the queue; explicit ``None`` is ambiguous and
                rejected.

        Returns:
            True if any messages were deleted, False otherwise.
            When message_id is provided, returns True only if that specific message was found and deleted.

        Raises:
            TypeError: If ``message_id=None`` is passed explicitly.
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        if message_id is None:
            raise TypeError(
                "message_id=None is ambiguous; pass an ID or call delete() "
                "without arguments to delete the queue"
            )

        with self.get_connection() as connection:
            if message_id is _DELETE_ALL:
                return connection.delete(self.name) > 0
            target_id = cast(MessageIdInput, message_id)
            return connection.delete_message_ids(self.name, [target_id]) > 0

    def delete_many(self, message_ids: Sequence[MessageIdInput]) -> int:
        """Physically delete exact message IDs from this queue.

        Claimed and unclaimed messages are both eligible for deletion. Missing
        IDs and IDs belonging to other queues are ignored.

        Args:
            message_ids: Message IDs to delete from this queue as ints or exact
                19-digit strings.

        Returns:
            Number of messages physically deleted.

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            return connection.delete_message_ids(self.name, message_ids)

    def find_message_ids(
        self,
        *,
        body_contains: str,
        limit: int = BODY_SEARCH_DEFAULT_LIMIT,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        include_claimed: bool = False,
    ) -> list[int]:
        """Find message IDs in this queue by literal body substring.

        Args:
            body_contains: Literal substring to search for.
            limit: Maximum number of message IDs to return.
            after_timestamp: Only find messages newer than this timestamp.
            before_timestamp: Only find messages older than this timestamp.
            include_claimed: If True, include claimed messages where supported.

        Returns:
            List of matching message IDs.
        """
        with self.get_connection() as connection:
            return connection.find_message_ids(
                self.name,
                body_contains=body_contains,
                limit=limit,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                include_claimed=include_claimed,
            )

    def __enter__(self) -> "Queue":  # noqa: PYI034 approved [DOM-10.1.1] [RUFF-SUP-001] exception
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:  # noqa: PYI036 approved [DOM-10.1.1] [RUFF-SUP-001] exception
        """Exit the context manager and close the runner."""
        self.close()

    def __str__(self) -> str:
        """Human-readable string representation.

        Returns just the queue name for natural usage in logs and messages.

        Examples:
            >>> queue = Queue("tasks")
            >>> print(f"Processing {queue}")
            Processing tasks
            >>> logger.info(f"Watching {queue}")
            INFO: Watching tasks
        """
        return self.name

    def __repr__(self) -> str:
        """Developer-friendly representation for debugging.

        Returns a string that could recreate the object (when possible).

        Examples:
            >>> Queue("tasks")
            Queue('tasks')
            >>> Queue("logs", db_path="/var/db/app.db")
            Queue('logs', db_path='/var/db/app.db')
        """
        parts = [repr(self.name)]

        db_repr = _display_broker_target(self._db_path)
        if not self._uses_config_default_target and db_repr != DEFAULT_DB_NAME:
            parts.append(f"db_path={db_repr!r}")
        if self._persistent:
            parts.append("persistent=True")

        return f"Queue({', '.join(parts)})"

    def has_pending(self, after_timestamp: int | None = None) -> bool:
        """Check if this queue has pending (unclaimed) messages.

        Args:
            after_timestamp: If provided, only check for messages newer than this timestamp.

        Returns:
            True if there are unclaimed messages, False otherwise.

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            return connection.has_pending_messages(self.name, after_timestamp)

    def latest_pending_timestamp(self) -> int | None:
        """Return the newest pending message timestamp in this queue.

        Returns:
            Largest timestamp for an unclaimed message in this queue, or
            ``None`` when the queue has no pending messages.

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the backend operation fails
        """
        with self.get_connection() as connection:
            return connection.latest_pending_timestamp(self.name)

    def exists(self) -> bool:
        """Return whether this queue has any messages, including claimed rows."""
        with self.get_connection() as connection:
            return connection.queue_exists(self.name)

    def stats(self) -> QueueStats:
        """Return pending, claimed, and total counts for this queue."""
        with self.get_connection() as connection:
            return connection.get_queue_stat(self.name)

    def get_data_version(self) -> int | None:
        """Get the database data version for change detection.

        Returns:
            Integer version if available, None for non-SQLite backends or errors.

        Notes:
            This is SQLite-specific and used for efficient polling to detect
            when the database has been modified by other processes.
        """
        with self.get_connection() as connection:
            return connection.get_data_version()

    def create_activity_waiter(
        self,
        *,
        stop_event: threading.Event,
    ) -> ActivityWaiter | None:
        """Create or reuse a backend-native waiter for watcher wakeups."""
        if self._activity_waiter is not None:
            return self._activity_waiter

        identity = self._activity_waiter_identity()

        waiter = identity.plugin.create_activity_waiter(
            target=identity.target_arg,
            backend_options=identity.backend_options_arg,
            runner=identity.runner_arg,
            queue_name=self.name,
            stop_event=stop_event,
        )
        if waiter is not None:
            self._activity_waiter = waiter
        return waiter

    def _detach_activity_waiter(
        self,
        *,
        expected: ActivityWaiter | None,
    ) -> ActivityWaiter | None:
        """Transfer the cached waiter without closing it on an exact match."""
        waiter = self._activity_waiter
        if waiter is not expected:
            return None
        self._activity_waiter = None
        return waiter

    def _activity_waiter_identity(self) -> _ActivityWaiterIdentity:
        """Return backend identity and hook arguments for activity waiters."""

        if self._runner is not None:
            if isinstance(self._runner, BackendAwareRunner):
                plugin = self._runner.backend_plugin
                backend_name = plugin.name
            elif isinstance(self._db_path, BrokerTarget):
                plugin = self._db_path.plugin
                backend_name = self._db_path.backend_name
            else:
                plugin = get_backend_plugin("sqlite")
                backend_name = "sqlite"
            return _ActivityWaiterIdentity(
                plugin=plugin,
                backend_name=backend_name,
                target_key=None,
                backend_options_key=None,
                runner_id=id(self._runner),
                target_arg=None,
                backend_options_arg=None,
                runner_arg=self._runner,
            )

        if isinstance(self._db_path, BrokerTarget):
            plugin = self._db_path.plugin
            target = self._db_path.target
            target_key = (
                _normalize_sqlite_waiter_target(target)
                if self._db_path.backend_name == "sqlite"
                else target
            )
            backend_options = cast(
                dict[str, Any],
                snapshot_key_material(self._db_path.backend_options),
            )
            return _ActivityWaiterIdentity(
                plugin=plugin,
                backend_name=self._db_path.backend_name,
                target_key=(
                    f"sqlite:{target_key}"
                    if self._db_path.backend_name == "sqlite"
                    else f"resolved:{target_key}"
                ),
                backend_options_key=freeze_key_material(backend_options),
                runner_id=None,
                target_arg=target,
                backend_options_arg=backend_options,
                runner_arg=None,
            )

        target = str(self._db_path)
        return _ActivityWaiterIdentity(
            plugin=get_backend_plugin("sqlite"),
            backend_name="sqlite",
            target_key=f"sqlite:{_normalize_sqlite_waiter_target(target)}",
            backend_options_key=freeze_key_material({}),
            runner_id=None,
            target_arg=target,
            backend_options_arg=None,
            runner_arg=None,
        )

    def stream_messages(
        self,
        *,
        peek: bool = False,
        all_messages: bool = True,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        batch_processing: bool = False,
        commit_interval: int = 1,
    ) -> CloseableIterator[tuple[str, int]]:
        """Stream messages with timestamps from the queue.

        The returned closeable iterator is lazy: creating it starts no Queue
        operation. Create, advance, exhaust, and close it on the same thread in
        every mode. A caller that may stop early must close it before closing
        this Queue or a higher-level client.

        For SQL-backed at-least-once batch iteration, finalization from another
        thread permanently poisons the instance; restart the process.
        Redis/Valkey does not share the SQL poison mechanism, but its behavior
        does not make cross-thread use portable.

        This is an iterator that yields messages as they are retrieved from the database.
        It's more memory-efficient than read_all for large queues.

        Args:
            peek: If True, don't remove messages from queue
            all_messages: If True, stream all available messages. If False, yield at
                         most one message.
            after_timestamp: Only retrieve messages newer than this timestamp
            before_timestamp: Only retrieve messages older than this timestamp
            batch_processing: If True and peek=False, allow at-least-once batch
                             processing. If False, consume one message at a time.
            commit_interval: Batch size for at-least-once processing when
                            batch_processing=True and peek=False. Ignored otherwise.

        Yields:
            tuples of (message_body, timestamp)

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            if peek:
                if all_messages:
                    generator = connection.peek_generator(
                        self.name,
                        with_timestamps=True,
                        after_timestamp=after_timestamp,
                        before_timestamp=before_timestamp,
                    )
                    # Type assertion after we know with_timestamps=True yields tuple[str, int]
                    try:
                        for result in generator:
                            yield result  # type: ignore[misc]
                    finally:
                        _close_iterator(generator)
                else:
                    generator = connection.peek_generator(
                        self.name,
                        with_timestamps=True,
                        after_timestamp=after_timestamp,
                        before_timestamp=before_timestamp,
                    )
                    try:
                        result = next(generator)
                    except StopIteration:
                        return
                    else:
                        assert isinstance(result, tuple)
                        yield result
                    finally:
                        _close_iterator(generator)
                return

            if not all_messages:
                generator = connection.claim_generator(
                    self.name,
                    with_timestamps=True,
                    delivery_guarantee="exactly_once",
                    after_timestamp=after_timestamp,
                    before_timestamp=before_timestamp,
                )
                try:
                    result = next(generator)
                except StopIteration:
                    return
                else:
                    assert isinstance(result, tuple)
                    yield result
                finally:
                    _close_iterator(generator)
                return

            delivery_guarantee: DeliveryGuarantee = (
                "at_least_once"
                if batch_processing and commit_interval > 1
                else "exactly_once"
            )
            batch_size = (
                commit_interval if delivery_guarantee == "at_least_once" else None
            )

            generator = connection.claim_generator(
                self.name,
                with_timestamps=True,
                delivery_guarantee=delivery_guarantee,
                batch_size=batch_size,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
            )
            # Type assertion after we know with_timestamps=True yields tuple[str, int]
            try:
                for result in generator:
                    yield result  # type: ignore[misc]
            finally:
                _close_iterator(generator)

    def cleanup_connections(self) -> None:
        """Clean up active database handles without releasing the queue lease.

        Watchers use this during stop/error recovery. The queue remains usable
        afterward; call close() to release persistent session ownership.
        """
        if self.conn:
            self.conn.cleanup()
        if self._activity_waiter is not None:
            self._activity_waiter.close()
            self._activity_waiter = None
        if hasattr(self, "_watcher_conn"):
            self._watcher_conn.cleanup()
            delattr(self, "_watcher_conn")

    def close(self) -> None:
        """Close the queue and release resources.

        This is called automatically when using the queue as a context manager.
        In ephemeral mode, this is a no-op as connections are closed after each
        operation.
        """
        if self._activity_waiter is not None:
            self._activity_waiter.close()
            self._activity_waiter = None
        if self.conn:
            if hasattr(self, "_finalizer"):
                self._finalizer.detach()
            self.conn.close()

    # ========== Persistent Mode Helpers ==========

    def _install_finalizer(self) -> None:
        """Install weakref finalizer for cleanup."""

        def cleanup(
            conn: DBConnection | None,
            config: ResolvedConfig,
        ) -> None:
            """Cleanup function called by finalizer."""
            try:
                if conn:
                    conn.close()
            except Exception as e:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-005] exception
                if config.get("BROKER_LOGGING_ENABLED", True):
                    logger.warning(f"Error during Queue finalizer cleanup: {e}")

        # Install finalizer with reference to connection
        self._finalizer = weakref.finalize(self, cleanup, self.conn, self._config)


def create_activity_waiter_for_queues(
    queues: Sequence[Queue],
    *,
    stop_event: threading.Event,
) -> ActivityWaiter | None:
    """Create one backend-native waiter for activity across multiple queues."""

    queue_list = list(queues)
    if not queue_list:
        raise ValueError("queues cannot be empty")

    identities = [queue._activity_waiter_identity() for queue in queue_list]
    first_identity = identities[0]
    first_key = first_identity.compatibility_key
    for identity in identities[1:]:
        if identity.compatibility_key != first_key:
            raise ValueError("queues cannot safely share one activity waiter")

    queue_names = tuple(dict.fromkeys(queue.name for queue in queue_list))
    create_waiter = getattr(
        first_identity.plugin,
        "create_activity_waiter_for_queues",
        None,
    )
    if create_waiter is None:
        return None
    if not callable(create_waiter):
        raise TypeError("backend create_activity_waiter_for_queues must be callable")

    hook = cast(MultiQueueActivityWaiterHook, create_waiter)
    return hook(
        target=first_identity.target_arg,
        backend_options=first_identity.backend_options_arg,
        runner=first_identity.runner_arg,
        queue_names=queue_names,
        stop_event=stop_event,
    )


# ~
