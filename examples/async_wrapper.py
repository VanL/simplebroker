#!/usr/bin/env python3
"""
Minimal async wrapper for SimpleBroker using only stdlib.

This example shows how to integrate SimpleBroker with asyncio applications
without any external dependencies. The wrapper runs SimpleBroker's synchronous
API in a thread pool and provides async methods for queue operations.

This demonstrates the RECOMMENDED approach for async usage - wrapping the
standard Queue API rather than reimplementing with internal APIs.

Key concepts:
- Thread pool executor for sync operations
- Async context manager for lifecycle
- asyncio.Event for coordination
- Type hints for better IDE support
- Uses the standard Queue and QueueWatcher public API
"""

import asyncio
import concurrent.futures
import functools
import logging
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, TypeVar, cast

from simplebroker import (
    BrokerTarget,
    Queue,
    QueueWatcher,
    open_broker,
    resolve_broker_target,
    resolve_config,
    target_for_directory,
)
from simplebroker.ext import BrokerConnection
from simplebroker.watcher import logger_handler

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Type variables for generic decorator
T = TypeVar("T")


def run_in_executor(func: Callable[..., T]) -> Callable[..., asyncio.Future[T]]:
    """Decorator to run sync methods in the thread pool executor."""

    @functools.wraps(func)
    async def wrapper(self: "AsyncBroker", *args: Any, **kwargs: Any) -> T:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor, functools.partial(func, self, *args, **kwargs)
        )

    return cast(Callable[..., asyncio.Future[T]], wrapper)


@dataclass(frozen=True)
class BrokerClient:
    """Small public-API client that owns target resolution."""

    target: BrokerTarget
    config: dict[str, Any]

    @classmethod
    def from_root(cls, root: Path | str, **overrides: Any) -> "BrokerClient":
        """Create a client using normal SimpleBroker target resolution."""
        config = resolve_config(overrides)
        return cls(target_for_directory(root, config=config), config)

    @classmethod
    def for_database(cls, db_path: Path | str, **overrides: Any) -> "BrokerClient":
        """Create a SQLite client bound to a specific broker database path."""
        path = Path(db_path).expanduser()
        root = path.parent if path.is_absolute() else (Path.cwd() / path).parent
        return cls.from_root(root, BROKER_DEFAULT_DB_NAME=path.name, **overrides)

    @classmethod
    def from_target(
        cls,
        target: BrokerTarget,
        *,
        config: dict[str, Any] | None = None,
        **overrides: Any,
    ) -> "BrokerClient":
        """Create a client from an already-resolved backend target."""
        resolved_config = resolve_config({**(config or {}), **overrides})
        return cls(target, resolved_config)

    @classmethod
    def discover(
        cls, root: Path | str | None = None, **overrides: Any
    ) -> "BrokerClient":
        """Discover a project/env target, or create the default target for root."""
        config = resolve_config(overrides)
        search_root = Path.cwd() if root is None else Path(root)
        target = resolve_broker_target(search_root, config=config)
        if target is None:
            target = target_for_directory(search_root, config=config)
        return cls(target, config)

    def queue(self, name: str, *, persistent: bool = False) -> Queue:
        """Return a Queue bound to this client's resolved target."""
        return Queue(
            name,
            db_path=self.target,
            persistent=persistent,
            config=self.config,
        )

    def broker(self) -> AbstractContextManager[BrokerConnection]:
        """Open a backend-agnostic broker handle for cross-queue operations."""
        return open_broker(self.target, config=self.config)


class AsyncBroker:
    """Async wrapper for SimpleBroker with thread pool execution."""

    def __init__(
        self,
        db_path: Path | str | None = None,
        max_workers: int = 4,
        *,
        client: BrokerClient | None = None,
    ):
        """
        Initialize the async broker.

        Args:
            db_path: Optional SQLite database path convenience
            max_workers: Maximum number of worker threads (default: 4)
            client: Optional preconfigured broker client
        """
        if client is None and db_path is None:
            raise ValueError("Provide either db_path or client")
        self.client = client or BrokerClient.for_database(cast(Path | str, db_path))
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._watchers: list[tuple[QueueWatcher, asyncio.Event]] = []

    @classmethod
    def from_root(
        cls, root: Path | str, max_workers: int = 4, **overrides: Any
    ) -> "AsyncBroker":
        """Create an async wrapper using normal multi-backend target resolution."""
        return cls(
            max_workers=max_workers,
            client=BrokerClient.from_root(root, **overrides),
        )

    async def __aenter__(self) -> "AsyncBroker":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit with cleanup."""
        # Stop all watchers
        for watcher, _ in self._watchers:
            watcher.stop()

        # Wait for watchers to finish
        await asyncio.sleep(0.1)  # Give watchers time to stop

        # Shutdown executor
        self._executor.shutdown(wait=True)

    @run_in_executor
    def push(self, queue: str, message: str) -> None:
        """Push a message to a queue (runs in thread pool)."""
        with self.client.queue(queue) as q:
            q.write(message)

    @run_in_executor
    def pop(self, queue: str) -> str | None:
        """Pop a message from a queue (runs in thread pool)."""
        with self.client.queue(queue) as q:
            # read() removes and returns the oldest message
            result = q.read()
        return result if isinstance(result, str) else None

    @run_in_executor
    def peek(self, queue: str) -> str | None:
        """Peek at the next message without removing it."""
        with self.client.queue(queue) as q:
            # peek() returns the oldest message without removing it
            result = q.peek()
        return result if isinstance(result, str) else None

    @run_in_executor
    def size(self, queue: str) -> int:
        """Get the size of a queue."""
        with self.client.broker() as broker:
            return broker.get_queue_stat(queue).pending

    async def watch_queue(
        self, queue: str, handler: Callable[[str, int], None], peek: bool = False
    ) -> asyncio.Event:
        """
        Start watching a queue with an async-friendly interface.

        Args:
            queue: Queue name to watch
            handler: Sync handler function (will be called in thread)
            peek: If True, don't consume messages

        Returns:
            asyncio.Event that will be set when the watcher stops
        """

        # Create a stop event for coordination
        stop_event = asyncio.Event()

        # Create watcher
        watcher = QueueWatcher(
            queue,  # Queue name comes first
            handler,  # Handler comes second
            db=self.client.target,  # Resolved target from the client
            config=self.client.config,
            peek=peek,
        )

        # Start watcher in background thread
        thread = watcher.run_in_thread()

        # Store watcher and event
        self._watchers.append((watcher, stop_event))

        # Create task to monitor thread completion
        async def monitor_thread() -> None:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, thread.join)
            stop_event.set()

        asyncio.create_task(monitor_thread())

        return stop_event

    async def stream_messages(
        self, queue: str, batch_size: int = 10
    ) -> AsyncGenerator[tuple[str, int], None]:
        """
        Async generator to stream messages from a queue.

        Yields:
            tuple of (message, timestamp) for each message
        """

        while True:
            # Fetch a batch of messages in thread pool
            messages = await self._fetch_batch(queue, batch_size)

            if not messages:
                # No messages available, wait a bit
                await asyncio.sleep(0.1)
                continue

            # Yield messages one by one
            for msg, ts in messages:
                yield msg, ts

    @run_in_executor
    def _fetch_batch(self, queue: str, batch_size: int) -> list[tuple[str, int]]:
        """Fetch a batch of messages (runs in thread pool)."""
        messages = []

        # Read messages one by one up to batch_size
        with self.client.queue(queue) as q:
            for _ in range(batch_size):
                result = q.read(with_timestamps=True)
                if result is None:
                    break
                if isinstance(result, tuple):
                    messages.append(result)

        return messages


async def example_producer(broker: AsyncBroker) -> None:
    """Example async producer."""
    for i in range(10):
        await broker.push("orders", f"Order #{i + 1}")
        logger.info(f"Produced: Order #{i + 1}")
        await asyncio.sleep(0.5)


async def example_consumer(broker: AsyncBroker) -> None:
    """Example async consumer using stream."""
    count = 0
    async for msg, ts in broker.stream_messages("orders"):
        logger.info(f"Consumed at {ts}: {msg}")
        count += 1
        if count >= 10:
            break


async def example_watcher(broker: AsyncBroker) -> None:
    """Example using the watcher pattern."""
    processed = []

    def handler(msg: str, ts: int) -> None:
        """Handler runs in thread pool."""
        # Use logger_handler for consistent logging plus our custom logic
        logger_handler(msg, ts)
        processed.append(msg)

    # Start watching
    stop_event = await broker.watch_queue("notifications", handler)

    # Produce some messages
    for i in range(5):
        await broker.push("notifications", f"Notification {i + 1}")
        await asyncio.sleep(0.2)

    # Wait a bit for processing
    await asyncio.sleep(1)

    # Stop watching (find the watcher)
    for watcher, event in broker._watchers:
        if event is stop_event:
            watcher.stop()
            break

    # Wait for watcher to stop
    await stop_event.wait()
    logger.info(f"Processed {len(processed)} notifications")


async def example_cross_queue_operation(broker: AsyncBroker) -> None:
    """Example showing cross-queue operations through the client.

    For operations that need to inspect or modify multiple queues, use the
    client's open_broker() handle instead of constructing BrokerDB directly.
    """
    loop = asyncio.get_event_loop()

    def get_stats() -> list[tuple[str, int, int]]:
        with broker.client.broker() as broker_handle:
            return broker_handle.get_queue_stats()

    stats = await loop.run_in_executor(broker._executor, get_stats)
    logger.info(f"Queue stats: {stats}")


async def main() -> None:
    """Main example showing different async patterns."""

    with TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "async_example.db"

        async with AsyncBroker(db_path) as broker:
            logger.info("Starting async broker example...")

            # Example 1: Producer/Consumer pattern
            logger.info("\n--- Producer/Consumer Pattern ---")
            await asyncio.gather(example_producer(broker), example_consumer(broker))

            # Example 2: Watcher pattern
            logger.info("\n--- Watcher Pattern ---")
            await example_watcher(broker)

            # Example 3: Concurrent operations
            logger.info("\n--- Concurrent Operations ---")
            tasks = [broker.push("concurrent", f"Message {i}") for i in range(5)]
            await asyncio.gather(*tasks)
            logger.info("Pushed 5 messages concurrently")

            # Check queue size
            size = await broker.size("concurrent")
            logger.info(f"Queue size: {size}")

            # Example 4: Cross-queue client handle
            logger.info("\n--- Client Cross-Queue Operation ---")
            await example_cross_queue_operation(broker)

    logger.info("\nAsync broker example completed!")


if __name__ == "__main__":
    asyncio.run(main())
