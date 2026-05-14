#!/usr/bin/env python3
"""
Python API examples for SimpleBroker - RECOMMENDED STARTING POINT.

This file demonstrates the standard public API for SimpleBroker using the
Queue and QueueWatcher classes. These are the primary interfaces that most
users should use.

Key classes:
- Queue: Primary interface for single-queue operations (write, read, peek, etc.)
- QueueWatcher: For watching queues and processing messages as they arrive

This example shows:
- Basic queue operations
- Error handling patterns
- Custom watchers and processors
- Integration patterns
"""

import json
import logging
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from simplebroker import Queue, QueueWatcher

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def basic_usage(db_path: Path) -> None:
    """Basic queue operations."""
    print("=== Basic Usage ===")

    # Context manager ensures proper cleanup
    with Queue("demo", db_path=str(db_path)) as q:
        # Write messages
        q.write("Hello, World!")
        q.write("Task 1")
        q.write("Task 2")

        # Read messages (removes them)
        print(f"Read: {q.read()}")  # "Hello, World!"

        # Peek at next message (doesn't remove)
        print(f"Peek: {q.peek()}")  # "Task 1"

        # Read all remaining messages
        result = q.read(all_messages=True)
        messages = list(result) if result else []
        print(f"Read all: {messages}")  # ["Task 1", "Task 2"]


def timestamp_usage(db_path: Path) -> None:
    """Working with timestamps as message IDs."""
    print("\n=== Timestamp Usage ===")

    with Queue("events", db_path=str(db_path)) as q:
        # Write returns timestamp
        # Note: write() doesn't return timestamps in the current API
        q.write("Event 1")
        q.write("Event 2")
        q.write("Event 3")

        # Messages are stored with automatic timestamps

        # Read messages in order
        msg = q.read()
        print(f"Read first message: {msg}")

        # Read all remaining messages
        result = q.read(all_messages=True)
        remaining = list(result) if result else []
        print(f"Remaining messages: {remaining}")


def error_handling_pattern(db_path: Path) -> None:
    """Robust error handling with retry logic."""
    print("\n=== Error Handling Pattern ===")

    error_queue = Queue("errors", db_path=str(db_path))
    retry_queue = Queue("retry", db_path=str(db_path))

    def process_with_retry(message: str, timestamp: int) -> None:
        """Process message with retry logic."""
        data = {}
        retry_count = 0

        try:
            # Parse message as JSON with retry count
            data = json.loads(message)
            retry_count = data.get("retry_count", 0)
        except json.JSONDecodeError:
            # If message is not JSON, treat it as plain text
            data = {"message": message, "retry_count": 0}
            retry_count = 0

        try:
            # Simulate processing that might fail
            if data.get("fail", False) and retry_count < 3:
                raise ValueError("Processing failed")

            print(f"Successfully processed: {data}")

        except Exception as e:
            logger.error(f"Error processing message {timestamp}: {e}")

            # Increment retry count
            if "retry_count" in data:
                data["retry_count"] += 1
            else:
                data["retry_count"] = 1

            if data["retry_count"] <= 3:
                # Retry later
                retry_queue.write(json.dumps(data))
                logger.info(
                    f"Message {timestamp} queued for retry (attempt {data['retry_count']})"
                )
            else:
                # Move to error queue after max retries
                error_queue.write(
                    json.dumps(
                        {
                            "original_message": message,
                            "error": str(e),
                            "timestamp": timestamp,
                            "retry_count": data["retry_count"],
                        }
                    )
                )
                logger.error(
                    f"Message {timestamp} moved to error queue after {data['retry_count']} retries"
                )

    # Example usage
    with Queue("tasks", db_path=str(db_path)) as q:
        q.write(json.dumps({"task": "process_order", "order_id": 123, "fail": True}))
        q.write(json.dumps({"task": "send_email", "to": "user@example.com"}))

        # Process all messages
        result = q.read(all_messages=True)
        messages = list(result) if result else []
        for i, msg in enumerate(messages):
            # Using index as a simple identifier
            if isinstance(msg, str):
                process_with_retry(msg, i)


def custom_watcher_example(db_path: Path) -> None:
    """Custom queue watcher with error handling."""
    print("\n=== Custom Watcher Example ===")

    class MessageProcessor:
        def __init__(self) -> None:
            self.processed_count = 0
            self.error_count = 0
            self.error_queue = Queue("processing_errors", db_path=str(db_path))

        def process(self, message: str, timestamp: int) -> None:
            """Process a single message."""
            logger.info(f"Processing message {timestamp}: {message}")

            # Simulate processing
            if "error" in message.lower():
                raise ValueError(
                    f"Cannot process message containing 'error': {message}"
                )

            # Simulate work
            time.sleep(0.1)
            self.processed_count += 1
            logger.info(f"Successfully processed message {timestamp}")

        def handle_error(
            self, exception: Exception, message: str, timestamp: int
        ) -> bool | None:
            """Handle processing errors."""
            logger.error(f"Error processing message {timestamp}: {exception}")
            self.error_count += 1

            # Save failed message for investigation
            error_data = {
                "message": message,
                "timestamp": timestamp,
                "error": str(exception),
                "error_type": type(exception).__name__,
                "failed_at": int(time.time() * 1000),
            }
            self.error_queue.write(json.dumps(error_data))

            # Continue watching unless it's a critical error
            if isinstance(exception, KeyboardInterrupt):
                logger.info("Stopping watcher due to keyboard interrupt")
                return False

            # Continue processing other messages
            return True

        def get_stats(self) -> dict[str, float]:
            """Get processing statistics."""
            return {
                "processed": self.processed_count,
                "errors": self.error_count,
                "error_rate": self.error_count
                / (self.processed_count + self.error_count)
                if (self.processed_count + self.error_count) > 0
                else 0,
            }

    # Set up processor and watcher
    processor = MessageProcessor()
    queue = Queue("stream", db_path=str(db_path))

    # Add some test messages
    queue.write("Good message 1")
    queue.write("This will cause an ERROR")
    queue.write("Good message 2")

    # QueueWatcher now uses queue-first API pattern
    watcher = QueueWatcher(
        "stream",  # Queue name comes first
        processor.process,  # Handler comes second
        db=str(db_path),  # Database as keyword argument
        error_handler=processor.handle_error,
        peek=False,  # Consume messages
    )

    # Watch for a short time (in practice, this would run continuously)

    # Use run_in_thread() which returns and starts a thread
    watcher_thread = watcher.run_in_thread()

    try:
        # Let it process for a bit
        time.sleep(1)
    finally:
        # Clean up: stop the watcher and wait for thread to finish
        watcher.stop()
        watcher_thread.join()

    # Print statistics
    stats = processor.get_stats()
    print(f"\nProcessing stats: {json.dumps(stats, indent=2)}")


def checkpoint_processing(db_path: Path) -> None:
    """Checkpoint-based processing for resumable workflows."""
    print("\n=== Checkpoint Processing ===")

    checkpoint_file = db_path.with_suffix(".checkpoint")

    def load_checkpoint() -> int:
        """Load last processed timestamp."""
        try:
            with open(checkpoint_file) as f:
                return int(f.read().strip())
        except FileNotFoundError:
            return 0

    def save_checkpoint(timestamp: int) -> None:
        """Save checkpoint timestamp."""
        with open(checkpoint_file, "w") as f:
            f.write(str(timestamp))

    def process_batch() -> None:
        """Process messages in batches with checkpointing."""
        checkpoint = load_checkpoint()
        logger.info(f"Starting from checkpoint: {checkpoint}")

        with Queue("batch_tasks", db_path=str(db_path)) as q:
            # Get all messages (checkpoint filtering would need to be done at DB level)
            result = q.read(all_messages=True)
            messages = list(result) if result else []

            if not messages:
                logger.info("No new messages to process")
                return

            logger.info(f"Processing {len(messages)} messages")

            for i, message in enumerate(messages):
                try:
                    # Process message
                    logger.info(f"Processing: {message}")
                    time.sleep(0.1)  # Simulate work

                    # Update checkpoint after successful processing
                    # Using index as a simple timestamp substitute
                    save_checkpoint(i)

                except Exception as e:
                    logger.error(f"Failed to process message at index {i}: {e}")
                    # Stop processing batch on error
                    break

            logger.info(f"Batch complete. Last checkpoint: {load_checkpoint()}")

    # Example usage
    with Queue("batch_tasks", db_path=str(db_path)) as q:
        # Add some messages
        for i in range(5):
            q.write(f"Batch task {i + 1}")

    # Process in batches (can be interrupted and resumed)
    process_batch()


if __name__ == "__main__":
    # Run all examples against an isolated temporary database.
    with TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "python_api_demo.db"
        basic_usage(path)
        timestamp_usage(path)
        error_handling_pattern(path)
        custom_watcher_example(path)
        checkpoint_processing(path)
