#!/usr/bin/env python3
"""
Minimal async wrapper for SimpleBroker using only stdlib.

This example shows how to integrate SimpleBroker with asyncio applications
without any external dependencies. The wrapper runs SimpleBroker's synchronous
watcher in a thread pool and provides async methods for queue operations.

Key concepts:
- Thread pool executor for sync operations
- Async context manager for lifecycle
- asyncio.Event for coordination
- Type hints for better IDE support
"""

import asyncio
import concurrent.futures
import functools
import logging
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Optional, ParamSpec, TypeVar, cast

from simplebroker import QueueWatcher
from simplebroker.db import BrokerDB

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
        self._db: Optional[BrokerDB] = None
        self._watchers: list[tuple[QueueWatcher, asyncio.Event]] = []

    async def __aenter__(self) -> "AsyncBroker":
        """Async context manager entry."""
        self._db = BrokerDB(str(self.db_path))
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit with cleanup."""
        # Stop all watchers
        for watcher, _ in self._watchers:
            watcher.stop()

        # Wait for watchers to finish
        await asyncio.sleep(0.1)  # Give watchers time to stop

        # Close database
        if self._db:
            self._db.close()

        # Shutdown executor
        self._executor.shutdown(wait=True)

    @run_in_executor
    def push(self, queue: str, message: str) -> None:
        """Push a message to a queue (runs in thread pool)."""
        if not self._db:
            raise RuntimeError("Broker not initialized")
        self._db.write(queue, message)

    @run_in_executor
    def pop(self, queue: str) -> Optional[str]:
        """Pop a message from a queue (runs in thread pool)."""
        if not self._db:
            raise RuntimeError("Broker not initialized")
        # Note: read() removes the message from the queue
        # Read one message (non-peek mode removes it)
        for msg in self._db.read(queue, peek=False):
            return msg
        return None

    @run_in_executor
    def peek(self, queue: str) -> Optional[str]:
        """Peek at the next message without removing it."""
        if not self._db:
            raise RuntimeError("Broker not initialized")
        # Read one message in peek mode
        for msg in self._db.read(queue, peek=True):
            return msg
        return None

    @run_in_executor
    def size(self, queue: str) -> int:
        """Get the size of a queue."""
        if not self._db:
            raise RuntimeError("Broker not initialized")
        # Get count from list_queues
        queues = self._db.list_queues()
        for q_name, count in queues:
            if q_name == queue:
                return count
        return 0  # Queue doesn't exist

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
        if not self._db:
            raise RuntimeError("Broker not initialized")

        # Create a stop event for coordination
        stop_event = asyncio.Event()

        # Create watcher
        watcher = QueueWatcher(
            self.db_path,  # Use path for thread safety
            queue,
            handler,
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
        if not self._db:
            raise RuntimeError("Broker not initialized")

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
        # Use stream_read_with_timestamps for better performance
        if self._db:
            for msg, ts in self._db.stream_read_with_timestamps(
                queue, all_messages=False, peek=False, commit_interval=1
            ):
                messages.append((msg, ts))
                if len(messages) >= batch_size:
                    break
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
        logger.info(f"Watcher received at {ts}: {msg}")
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
