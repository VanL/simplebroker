#!/usr/bin/env python3
"""
Simple watcher example demonstrating default handlers.

This example shows how to use the built-in default handlers provided
by SimpleBroker for common use cases like debugging and monitoring.
"""

import time

from simplebroker import Queue, QueueWatcher
from simplebroker.watcher import (
    default_error_handler,
    json_print_handler,
    logger_handler,
    simple_print_handler,
)


def main() -> None:
    """Demonstrate different default handlers."""

    # Create a persistent queue for the watcher
    with Queue("demo", persistent=True) as queue:
        # Add some test messages
        queue.write("Hello World!")
        queue.write("Message with\nnewlines")
        queue.write("Special chars: <>&'\"")

    print("=== Example 1: Simple Print Handler ===")
    print("Basic handler that prints [timestamp] message format")

    # Use simple_print_handler - good for debugging
    watcher1 = QueueWatcher("demo", simple_print_handler)

    # Add a message and process it
    with Queue("demo", persistent=True) as queue:
        queue.write("Debug message 1")

    # Run watcher briefly to show output
    with watcher1:
        time.sleep(0.5)  # Let it process messages

    print("\n=== Example 2: JSON Print Handler ===")
    print("Structured JSON output - safe for shell processing")

    # Use json_print_handler - good for shell pipelines
    watcher2 = QueueWatcher("demo", json_print_handler)

    # Add messages with special characters
    with Queue("demo", persistent=True) as queue:
        queue.write('JSON safe: newlines\nand quotes"')
        queue.write("Another message")

    with watcher2:
        time.sleep(0.5)

    print("\n=== Example 3: Logger Handler ===")
    print("Uses Python logging system - integrates with your app logging")

    # Configure logging to see the output
    import logging

    logging.basicConfig(level=logging.INFO, format="%(name)s: %(message)s")

    # Use logger_handler - good for production apps
    watcher3 = QueueWatcher("demo", logger_handler)

    with Queue("demo", persistent=True) as queue:
        queue.write("Logged message 1")
        queue.write("Logged message 2")

    with watcher3:
        time.sleep(0.5)

    print("\n=== Example 4: Custom Handler Using Default as Base ===")
    print("You can easily build on the defaults")

    def custom_handler(msg: str, ts: int) -> None:
        """Custom handler that adds context before using default."""
        print(f"[CUSTOM] Processing at {time.strftime('%H:%M:%S')}")
        simple_print_handler(msg, ts)  # Use default as building block
        print("[CUSTOM] Completed processing")

    watcher4 = QueueWatcher("demo", custom_handler)

    with Queue("demo", persistent=True) as queue:
        queue.write("Custom handled message")

    with watcher4:
        time.sleep(0.5)

    # Clean up - read remaining messages to clear queue
    with Queue("demo", persistent=True) as queue:
        while queue.read() is not None:
            pass  # Clear any remaining messages

    print("\n=== Example 5: Error Handler Demonstration ===")
    print("Shows how to use the default error handler")

    def failing_handler(msg: str, ts: int) -> None:
        """Handler that fails on certain messages."""
        if "error" in msg.lower():
            raise ValueError(f"Simulated error processing: {msg}")
        print(f"Successfully processed: {msg}")

    # Use default_error_handler explicitly
    watcher5 = QueueWatcher(
        "demo", failing_handler, error_handler=default_error_handler
    )

    with Queue("demo", persistent=True) as queue:
        queue.write("Good message")
        queue.write("This will cause an ERROR")
        queue.write("Another good message")

    with watcher5:
        time.sleep(0.5)

    print("\nDemo complete! The default handlers provide:")
    print("• simple_print_handler: Basic [timestamp] message output")
    print("• json_print_handler: Safe structured JSON output")
    print("• logger_handler: Integration with Python logging")
    print("• default_error_handler: Error logging and continuation")
    print("All are importable from simplebroker.watcher")


if __name__ == "__main__":
    main()
