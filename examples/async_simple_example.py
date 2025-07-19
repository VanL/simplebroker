#!/usr/bin/env python3
"""Simple example showing how to use the async pooled broker implementation.

This example demonstrates common use cases for the async SimpleBroker:
- Basic queue operations
- Worker pattern with async/await
- Error handling
- Graceful shutdown

Requirements:
    pip install aiosqlite aiosqlitepool
"""

import asyncio
import random
import signal
import sys

# Import the async implementation
from async_pooled_broker import AsyncBrokerCore, AsyncQueue, async_broker


async def producer(queue: AsyncQueue, producer_id: int, count: int = 10) -> None:
    """Simulate a producer that adds work items to the queue."""
    for i in range(count):
        work_item = f"Job-{producer_id}-{i:03d}"
        await queue.write(work_item)
        print(f"Producer {producer_id}: Added {work_item}")
        # Simulate variable production rate
        await asyncio.sleep(random.uniform(0.1, 0.3))
    print(f"Producer {producer_id}: Finished producing {count} items")


async def worker(
    queue: AsyncQueue, worker_id: int, shutdown_event: asyncio.Event
) -> None:
    """Simulate a worker that processes items from the queue."""
    processed = 0

    while not shutdown_event.is_set():
        try:
            # Try to get a message with a timeout
            msg = await asyncio.wait_for(queue.read(), timeout=1.0)

            if msg:
                # Simulate processing
                print(f"Worker {worker_id}: Processing {msg}")
                await asyncio.sleep(random.uniform(0.2, 0.5))
                processed += 1

                # Simulate occasional failures
                if random.random() < 0.1:
                    print(f"Worker {worker_id}: Failed to process {msg}, will retry")
                    # In a real app, you might move to a retry queue
                    await queue.write(f"RETRY:{msg}")

        except asyncio.TimeoutError:
            # No message available, check if we should shutdown
            if shutdown_event.is_set():
                break
            continue
        except Exception as e:
            print(f"Worker {worker_id}: Error processing message: {e}")

    print(f"Worker {worker_id}: Shutting down, processed {processed} items")


async def monitor(broker: AsyncBrokerCore, shutdown_event: asyncio.Event) -> None:
    """Monitor queue status periodically."""
    while not shutdown_event.is_set():
        try:
            queues = await broker.list_queues()
            if queues:
                print("\n--- Queue Status ---")
                for queue_name, count in queues:
                    print(f"  {queue_name}: {count} messages")
                print("-------------------\n")

            await asyncio.sleep(5.0)
        except asyncio.CancelledError:
            break


async def main() -> None:
    """Main application demonstrating async queue patterns."""
    print("SimpleBroker Async Example")
    print("==========================")
    print("Press Ctrl+C to shutdown gracefully\n")

    # Create shutdown event for graceful termination
    shutdown_event = asyncio.Event()

    # Setup signal handler for graceful shutdown
    def signal_handler() -> None:
        print("\nReceived shutdown signal, finishing current work...")
        shutdown_event.set()

    # Register signal handlers
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    # Use the async broker with connection pooling
    async with async_broker("work_queue.db", pool_size=10) as broker:
        # Create work queue
        work_queue = AsyncQueue("work", broker)

        # Clear any existing messages
        await broker.delete("work")

        # Start monitoring task
        monitor_task = asyncio.create_task(monitor(broker, shutdown_event))

        # Start workers
        worker_tasks = [
            asyncio.create_task(worker(work_queue, i, shutdown_event)) for i in range(3)
        ]

        # Start producers
        producer_tasks = [
            asyncio.create_task(producer(work_queue, i, count=20)) for i in range(2)
        ]

        # Wait for producers to finish
        await asyncio.gather(*producer_tasks)
        print(
            "\nAll producers finished, waiting for workers to process remaining items..."
        )

        # Wait a bit for workers to process remaining items
        await asyncio.sleep(5)

        # Signal shutdown
        shutdown_event.set()

        # Wait for workers to finish
        await asyncio.gather(*worker_tasks)

        # Cancel monitor
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

        # Final status
        print("\n=== Final Status ===")
        queues = await broker.list_queues()
        for queue_name, count in queues:
            print(f"{queue_name}: {count} messages remaining")

        # Process any retry messages
        retry_count = 0
        async for msg in work_queue.stream():
            if msg.startswith("RETRY:"):
                retry_count += 1

        if retry_count > 0:
            print(f"\nFound {retry_count} messages that need retry")


async def simple_example() -> None:
    """Very simple example for getting started."""
    print("\n=== Simple Async Example ===")

    # Create a broker and queue
    async with async_broker("simple.db") as broker:
        queue = AsyncQueue("my_queue", broker)

        # Write some messages
        await queue.write("Hello")
        await queue.write("World")
        await queue.write("From async SimpleBroker!")

        # Read them back
        while True:
            msg = await queue.read()
            if msg is None:
                break
            print(f"Got message: {msg}")


async def batch_processing_example() -> None:
    """Example showing batch processing for better performance."""
    print("\n=== Batch Processing Example ===")

    async with async_broker("batch.db") as broker:
        queue = AsyncQueue("batch_queue", broker)

        # Write many messages
        print("Writing 100 messages...")
        for i in range(100):
            await queue.write(f"Batch message {i:03d}")

        # Process in batches with commit_interval
        print("\nProcessing in batches of 10...")
        count = 0

        # Using commit_interval=10 for better performance
        # This provides at-least-once delivery semantics
        async for _msg in queue.stream(commit_interval=10):
            count += 1
            if count % 10 == 0:
                print(f"Processed {count} messages...")

        print(f"Total processed: {count}")


if __name__ == "__main__":
    # Run different examples
    choice = sys.argv[1] if len(sys.argv) > 1 else "main"

    if choice == "simple":
        asyncio.run(simple_example())
    elif choice == "batch":
        asyncio.run(batch_processing_example())
    else:
        asyncio.run(main())
