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
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Optional, ParamSpec, TypeVar, cast

# Use the public API - Queue and QueueWatcher
from simplebroker import Queue, QueueWatcher
from simplebroker.watcher import logger_handler

# For cross-queue operations requiring direct database access (advanced use)
# Removed DBConnection import - use BrokerDB directly

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Type variables for generic decorator
P = ParamSpec("P")
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


class AsyncBroker:
    """Async wrapper for SimpleBroker with thread pool execution."""

    def __init__(self, db_path: Path | str, max_workers: int = 4):
        """
        Initialize the async broker.

        Args:
            db_path: Path to the database file
            max_workers: Maximum number of worker threads (default: 4)
        """
        self.db_path = Path(db_path)
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._queues: dict[str, Queue] = {}  # Cache Queue instances by name
        self._watchers: list[tuple[QueueWatcher, asyncio.Event]] = []

    async def __aenter__(self) -> "AsyncBroker":
        """Async context manager entry."""
        # Queue instances will be created on-demand for each queue name
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit with cleanup."""
        # Stop all watchers
        for watcher, _ in self._watchers:
            watcher.stop()

        # Wait for watchers to finish
        await asyncio.sleep(0.1)  # Give watchers time to stop

        # Queue instances handle their own cleanup

        # Shutdown executor
        self._executor.shutdown(wait=True)

    def _get_queue(self, queue_name: str) -> Queue:
        """Get or create a Queue instance for the given queue name."""
        if queue_name not in self._queues:
            self._queues[queue_name] = Queue(queue_name, db_path=str(self.db_path))
        return self._queues[queue_name]

    @run_in_executor
    def push(self, queue: str, message: str) -> None:
        """Push a message to a queue (runs in thread pool)."""
        q = self._get_queue(queue)
        q.write(message)

    @run_in_executor
    def pop(self, queue: str) -> Optional[str]:
        """Pop a message from a queue (runs in thread pool)."""
        q = self._get_queue(queue)
        # read() removes and returns the oldest message
        result = q.read()
        return result if isinstance(result, str) else None

    @run_in_executor
    def peek(self, queue: str) -> Optional[str]:
        """Peek at the next message without removing it."""
        q = self._get_queue(queue)
        # peek() returns the oldest message without removing it
        result = q.peek()
        return result if isinstance(result, str) else None

    @run_in_executor
    def size(self, queue: str) -> int:
        """Get the size of a queue."""
        # Use database directly to get queue size
        from simplebroker.db import BrokerDB

        with BrokerDB(str(self.db_path)) as db:
            stats = db.get_queue_stats()
            for queue_name, unclaimed, _total in stats:
                if queue_name == queue:
                    return unclaimed
        return 0

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
            db=self.db_path,  # Database is a keyword argument
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
            Tuple of (message, timestamp) for each message
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
        q = self._get_queue(queue)

        # Read messages one by one up to batch_size
        # Note: The Queue API doesn't expose timestamps directly in the public API
        # For this example, we'll just return the message with a placeholder timestamp
        for _ in range(batch_size):
            result = q.read(with_timestamps=True)
            if result is None:
                break
            if isinstance(result, tuple):
                messages.append(result)
            elif isinstance(result, str):
                # Fallback if no timestamp available
                import time

                messages.append((result, int(time.time() * 1000000)))

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


async def example_cross_queue_operation(db_path: Path) -> None:
    """Example showing cross-queue operations using BrokerDB (advanced).

    For operations that need to work across multiple queues atomically,
    you can use the BrokerDB context manager. This is an advanced
    pattern for when the simple Queue API isn't sufficient.
    """
    from simplebroker.db import BrokerDB

    # Create a BrokerDB instance for advanced operations
    broker_db = BrokerDB(str(db_path))

    # Use BrokerDB directly for cross-queue operations
    with broker_db:
        # Example: Get queue statistics
        stats = broker_db.get_queue_stats()
        logger.info(f"Queue stats: {stats}")

    broker_db.close()


async def main() -> None:
    """Main example showing different async patterns."""

    # Create database
    db_path = Path("async_example.db")

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

    # Cleanup
    db_path.unlink(missing_ok=True)
    logger.info("\nAsync broker example completed!")


if __name__ == "__main__":
    asyncio.run(main())
