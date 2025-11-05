#!/usr/bin/env python3
"""
MultiQueueWatcher Example

A MultiQueueWatcher class that monitors multiple queues using a single thread
and shared database connection. Features:

1. Single database connection
2. Round-robin processing between active queues
3. Single-threaded design
4. BaseWatcher inheritance for polling, error handling, lifecycle management

Usage:
    python multi_queue_watcher.py

Creates queues with different message types, sets up queue-specific handlers,
starts the MultiQueueWatcher, sends messages to queues, and demonstrates
round-robin processing.
"""

from __future__ import annotations

import itertools
import json
import logging
import tempfile
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from simplebroker import Queue
from simplebroker.db import BrokerDB
from simplebroker.watcher import (
    BaseWatcher,
    PollingStrategy,
    default_error_handler,
    simple_print_handler,
)

# Configure logging to see what's happening
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MultiQueueWatcher(BaseWatcher):
    """Watches multiple queues and processes them with round-robin scheduling.

    Uses a single thread and shared database connection to monitor and process
    messages from multiple queues. Each queue can have its own handler and
    error handler, with fallback to defaults.

    Features:
    - Single database connection shared across all queues
    - Round-robin processing to prevent queue starvation
    - Per-queue handler and error handler support
    - Inherits BaseWatcher's polling strategy and lifecycle management
    - Active/inactive queue management
    """

    def __init__(
        self,
        queues: list[str],
        default_handler: Callable[[str, int], None] = simple_print_handler,
        queue_handlers: dict[str, Callable[[str, int], None]] | None = None,
        queue_error_handlers: dict[str, Callable[[Exception, str, int], bool | None]]
        | None = None,
        *,
        db: BrokerDB | str | Path | None = None,
        stop_event: threading.Event | None = None,
        error_handler: Callable[
            [Exception, str, int], bool | None
        ] = default_error_handler,
        persistent: bool = True,
        polling_strategy: PollingStrategy | None = None,
        yield_strategy: str = "round_robin",
        check_interval: int = 10,
    ) -> None:
        """Initialize a MultiQueueWatcher.

        Args:
            queues: list of queue names to monitor
            default_handler: Default function called with (message_body, timestamp)
            queue_handlers: Optional mapping of queue names to specific handlers
            queue_error_handlers: Optional mapping of queue names to specific error handlers
            db: Database instance or path (uses default if None)
            stop_event: Event to signal watcher shutdown
            error_handler: Default error handler (used when no queue-specific handler exists)
            persistent: Whether to use persistent Queue objects
            polling_strategy: Custom polling strategy (uses default if None)
            yield_strategy: Queue selection strategy ("round_robin" supported)
            check_interval: How often to refresh active queue list (iterations)
        """
        # Input validation
        if not queues:
            raise ValueError("queues list cannot be empty")

        if not callable(default_handler):
            raise TypeError(
                f"default_handler must be callable, got {type(default_handler).__name__}"
            )

        if queue_handlers:
            for queue_name, handler in queue_handlers.items():
                if not callable(handler):
                    raise TypeError(
                        f"handler for queue '{queue_name}' must be callable"
                    )

        if queue_error_handlers:
            for queue_name, error_handler_func in queue_error_handlers.items():
                if not callable(error_handler_func):
                    raise TypeError(
                        f"error_handler for queue '{queue_name}' must be callable"
                    )

        if not callable(error_handler):
            raise TypeError(
                f"error_handler must be callable, got {type(error_handler).__name__}"
            )

        # Store handlers before calling super().__init__() to prevent BaseWatcher from overriding
        self._handler = default_handler
        self._error_handler = error_handler

        # Store configuration
        self._persistent = persistent
        self._default_handler = default_handler
        self._default_error_handler = error_handler
        self._yield_strategy = yield_strategy
        self._check_interval = check_interval

        # Use first queue for BaseWatcher initialization
        # This establishes the shared database connection
        initial_queue = Queue(
            str(queues[0]),
            db_path=str(db) if db else "broker.db",
            persistent=persistent,
        )

        super().__init__(
            initial_queue,
            stop_event=stop_event,
            polling_strategy=polling_strategy,
        )

        # Determine database path for creating additional Queue objects
        db_path = None
        if isinstance(db, BrokerDB):
            db_path = str(db.db_path)
        elif isinstance(db, str | Path):
            db_path = str(db)
        else:
            # Use database path from inherited queue object
            if hasattr(initial_queue, "_db_path"):
                db_path = initial_queue._db_path
            else:
                from simplebroker._constants import DEFAULT_DB_NAME

                db_path = DEFAULT_DB_NAME

        # Create queue configuration dict
        self._queues: dict[str, dict[str, Any]] = {}

        # Create Queue objects for each queue (sharing same database)
        for queue_name in queues:
            handler = (queue_handlers or {}).get(queue_name, default_handler)
            error_handler = (queue_error_handlers or {}).get(queue_name, error_handler)

            if queue_name == initial_queue.name:
                # Reuse the initial queue object
                queue_obj = initial_queue
            else:
                # Create new Queue object sharing same database
                queue_obj = Queue(queue_name, db_path=db_path, persistent=persistent)

            if hasattr(queue_obj, "set_stop_event"):
                queue_obj.set_stop_event(self._stop_event)

            self._queues[queue_name] = {
                "queue": queue_obj,
                "handler": handler,
                "error_handler": error_handler,
            }

        # Multi-queue processing state
        self._active_queues: list[str] = []
        self._queue_iterator: itertools.cycle[str] = itertools.cycle(
            []
        )  # Empty cycle, will be replaced when queues become active
        self._check_counter = 0

        logger.info(
            f"MultiQueueWatcher initialized with {len(queues)} queues: {queues}"
        )

    def _has_pending_messages(self) -> bool:
        """Check if ANY queue has pending messages.

        This is called by BaseWatcher's polling strategy to determine
        if there's any work to do across all monitored queues.
        """
        return any(
            queue_info["queue"].has_pending() for queue_info in self._queues.values()
        )

    def _update_active_queues(self) -> None:
        """Update the list of queues that currently have messages.

        Uses a two-tier checking strategy:
        1. Check currently active queues first
        2. Periodically check inactive queues based on check_interval

        Balances responsiveness with efficiency.
        """
        # Check currently active queues first (hot path)
        still_active = []
        for queue_name in self._active_queues:
            if self._queues[queue_name]["queue"].has_pending():
                still_active.append(queue_name)

        # Periodically check inactive queues (cold path)
        if self._check_counter % self._check_interval == 0:
            for queue_name, queue_info in self._queues.items():
                if queue_name not in still_active:
                    if queue_info["queue"].has_pending():
                        still_active.append(queue_name)
                        logger.debug(f"Queue '{queue_name}' became active")

        # Update state if active queues changed
        if set(still_active) != set(self._active_queues):
            logger.debug(
                f"Active queues changed: {self._active_queues} -> {still_active}"
            )
            self._active_queues = still_active
            if self._active_queues:
                self._queue_iterator = itertools.cycle(self._active_queues)
            else:
                self._queue_iterator = itertools.cycle([])  # Empty cycle

        self._check_counter += 1

    def _drain_queue(self) -> None:
        """Process messages from all active queues using round-robin.

        Called by BaseWatcher when the polling strategy detects activity.
        Implements round-robin processing across all active queues.
        """
        self._update_active_queues()

        if not self._active_queues:
            return

        messages_processed = 0
        queues_to_remove = []

        # Process one message from each active queue (one round)
        for _ in range(len(self._active_queues)):
            try:
                queue_name = next(self._queue_iterator)
                queue_info = self._queues[queue_name]

                # Try to read one message
                result = queue_info["queue"].read_one(with_timestamps=True)
                if result:
                    body, timestamp = result

                    # Switch handler and error handler context for this message
                    original_handler = self._handler
                    original_error_handler = self._error_handler
                    self._handler = queue_info["handler"]
                    self._error_handler = queue_info["error_handler"]

                    try:
                        # Use inherited _dispatch for consistent error handling and size validation
                        self._dispatch(body, timestamp)
                        messages_processed += 1
                        logger.debug(
                            f"Processed message from queue '{queue_name}': {body[:50]}..."
                        )
                    finally:
                        # Restore original handlers
                        self._handler = original_handler
                        self._error_handler = original_error_handler
                else:
                    # Queue became empty during processing
                    queues_to_remove.append(queue_name)
                    logger.debug(f"Queue '{queue_name}' became empty")

            except StopIteration:
                break
            except Exception as e:
                # Log but continue processing other queues
                logger.exception(f"Error processing queue {queue_name}: {e}")

        # Remove empty queues from active list
        if queues_to_remove:
            self._active_queues = [
                q for q in self._active_queues if q not in queues_to_remove
            ]
            if self._active_queues:
                self._queue_iterator = itertools.cycle(self._active_queues)
            else:
                self._queue_iterator = itertools.cycle([])  # Empty cycle

        # Notify polling strategy if we processed any messages
        if messages_processed > 0:
            self._strategy.notify_activity()
            logger.debug(f"Processed {messages_processed} messages this round")

    def add_queue(
        self,
        queue_name: str,
        handler: Callable[[str, int], None] | None = None,
        error_handler: Callable[[Exception, str, int], bool | None] | None = None,
    ) -> None:
        """Dynamically add a new queue to the watcher.

        Args:
            queue_name: The name of the queue to add
            handler: Optional handler for this queue (uses default_handler if None)
            error_handler: Optional error handler for this queue (uses default if None)

        Raises:
            ValueError: If queue_name already exists
            TypeError: If handler or error_handler is not callable
        """
        if queue_name in self._queues:
            raise ValueError(f"Queue '{queue_name}' already exists")

        if handler is not None and not callable(handler):
            raise TypeError(f"handler must be callable, got {type(handler).__name__}")

        if error_handler is not None and not callable(error_handler):
            raise TypeError(
                f"error_handler must be callable, got {type(error_handler).__name__}"
            )

        # Use defaults if none provided
        if handler is None:
            handler = self._default_handler
        if error_handler is None:
            error_handler = self._default_error_handler

        # Determine database path from existing queues
        db_path = None
        if self._queues:
            # Use database path from any existing queue
            existing_queue = next(iter(self._queues.values()))["queue"]
            if hasattr(existing_queue, "_db_path"):
                db_path = existing_queue._db_path
            else:
                from simplebroker._constants import DEFAULT_DB_NAME

                db_path = DEFAULT_DB_NAME
        else:
            # Fallback to default (shouldn't happen in normal usage)
            from simplebroker._constants import DEFAULT_DB_NAME

            db_path = DEFAULT_DB_NAME

        # Create Queue object sharing same database
        queue_obj = Queue(queue_name, db_path=db_path, persistent=self._persistent)

        if hasattr(queue_obj, "set_stop_event"):
            queue_obj.set_stop_event(self._stop_event)

        # Add to queues dictionary
        self._queues[queue_name] = {
            "queue": queue_obj,
            "handler": handler,
            "error_handler": error_handler,
        }

        logger.info(f"Added queue '{queue_name}' to MultiQueueWatcher")

    def remove_queue(self, queue_name: str) -> None:
        """Dynamically remove a queue from the watcher.

        Args:
            queue_name: The name of the queue to remove

        Raises:
            ValueError: If queue_name doesn't exist
        """
        if queue_name not in self._queues:
            raise ValueError(f"Queue '{queue_name}' not found")

        # Remove from queues dictionary
        del self._queues[queue_name]

        # Remove from active queues if present and update iterator
        if queue_name in self._active_queues:
            self._active_queues.remove(queue_name)
            if self._active_queues:
                self._queue_iterator = itertools.cycle(self._active_queues)
            else:
                self._queue_iterator = itertools.cycle([])  # Empty cycle

        logger.info(f"Removed queue '{queue_name}' from MultiQueueWatcher")

    def list_queues(self) -> list[str]:
        """Get a list of all configured queue names.

        Returns:
            list of queue names
        """
        return list(self._queues.keys())

    def get_active_queues(self) -> list[str]:
        """Get a list of currently active queue names (queues with messages).

        Returns:
            list of active queue names
        """
        return self._active_queues.copy()


def create_sample_handlers() -> dict[str, Callable[[str, int], None]]:
    """Create different handlers for different types of queues."""

    def orders_handler(message: str, timestamp: int) -> None:
        """Handler for order processing queue."""
        try:
            order = json.loads(message)
            print(
                f"🛒 [ORDER] Processing order #{order.get('id', 'unknown')} "
                f"for ${order.get('total', 0):.2f} at {timestamp}"
            )
        except json.JSONDecodeError:
            print(f"🛒 [ORDER] Processing order: {message} at {timestamp}")

    def notifications_handler(message: str, timestamp: int) -> None:
        """Handler for notification queue."""
        try:
            notification = json.loads(message)
            print(
                f"📧 [NOTIFICATION] {notification.get('type', 'message').upper()}: "
                f"{notification.get('content', message)} at {timestamp}"
            )
        except json.JSONDecodeError:
            print(f"📧 [NOTIFICATION] {message} at {timestamp}")

    def analytics_handler(message: str, timestamp: int) -> None:
        """Handler for analytics events queue."""
        try:
            event = json.loads(message)
            print(
                f"📊 [ANALYTICS] Event '{event.get('event', 'unknown')}' "
                f"from user {event.get('user_id', 'anonymous')} at {timestamp}"
            )
        except json.JSONDecodeError:
            print(f"📊 [ANALYTICS] {message} at {timestamp}")

    def logs_handler(message: str, timestamp: int) -> None:
        """Handler for log processing queue."""
        print(f"📝 [LOGS] {message} at {timestamp}")

    return {
        "orders": orders_handler,
        "notifications": notifications_handler,
        "analytics": analytics_handler,
        "logs": logs_handler,
    }


def create_sample_messages() -> dict[str, list[str]]:
    """Create sample messages for different queues."""
    return {
        "orders": [
            json.dumps({"id": 1001, "total": 29.99, "items": ["coffee", "bagel"]}),
            json.dumps({"id": 1002, "total": 149.99, "items": ["laptop_stand"]}),
            json.dumps({"id": 1003, "total": 8.50, "items": ["tea", "cookie"]}),
        ],
        "notifications": [
            json.dumps({"type": "email", "content": "Welcome to our service!"}),
            json.dumps({"type": "sms", "content": "Your order has shipped"}),
            json.dumps({"type": "push", "content": "New feature available"}),
        ],
        "analytics": [
            json.dumps(
                {"event": "page_view", "user_id": "user123", "page": "/dashboard"}
            ),
            json.dumps(
                {"event": "button_click", "user_id": "user456", "button": "subscribe"}
            ),
            json.dumps({"event": "purchase", "user_id": "user123", "amount": 29.99}),
        ],
        "logs": [
            "INFO: Application started successfully",
            "DEBUG: Database connection established",
            "WARNING: High memory usage detected",
        ],
        "default": [
            "Generic message 1",
            "Generic message 2",
            "Generic message 3",
        ],
    }


def populate_queues_with_messages(db_path: str, messages: dict[str, list[str]]) -> None:
    """Populate queues with sample messages."""
    print("\n📦 Populating queues with sample messages...")

    for queue_name, queue_messages in messages.items():
        queue = Queue(queue_name, db_path=db_path, persistent=True)
        for message in queue_messages:
            queue.write(message)
        print(f"   Added {len(queue_messages)} messages to '{queue_name}' queue")


def demonstrate_round_robin_fairness(watcher: MultiQueueWatcher, db_path: str) -> None:
    """Demonstrate round-robin processing across queues."""
    print("\n⚖️  Demonstrating round-robin processing...")
    print("   Adding more messages to 'orders' queue while others are empty...")

    # Add more messages to orders queue while processing is happening
    orders_queue = Queue("orders", db_path=db_path, persistent=True)
    for i in range(5):
        order_message = json.dumps(
            {"id": 2000 + i, "total": 15.99 + i * 5, "items": [f"item_{i}"]}
        )
        orders_queue.write(order_message)
        time.sleep(0.1)  # Small delay to show interleaving

    print("   Orders queue now has more messages. Other queues continue processing.")


def main() -> None:
    """Main demonstration of MultiQueueWatcher."""
    print("🚀 MultiQueueWatcher Example")
    print("=" * 50)

    # Create temporary database for this example
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "multi_queue_example.db"

        print(f"📁 Using temporary database: {db_path}")

        # Create sample handlers and messages
        handlers = create_sample_handlers()
        messages = create_sample_messages()

        # Populate queues with initial messages
        populate_queues_with_messages(str(db_path), messages)

        # Create and configure MultiQueueWatcher
        queue_names = ["orders", "notifications", "analytics", "logs", "default"]

        print(f"\n🔧 Creating MultiQueueWatcher for queues: {queue_names}")

        watcher = MultiQueueWatcher(
            queues=queue_names,
            queue_handlers=handlers,
            db=str(db_path),
            check_interval=5,  # Check inactive queues every 5 iterations
        )

        print("\n▶️  Starting MultiQueueWatcher...")
        print("    Messages will be processed from all queues in round-robin order.")
        print("    (Press Ctrl+C to stop)\n")

        try:
            # Start the watcher in a background thread
            watcher.start()

            # Let it process initial messages
            time.sleep(2)

            # Demonstrate round-robin fairness
            demonstrate_round_robin_fairness(watcher, str(db_path))

            # Let it continue processing
            time.sleep(3)

            # Add a few more messages to show continued processing
            print("\n📨 Adding final batch of messages...")
            final_messages = {
                "notifications": [
                    json.dumps(
                        {"type": "alert", "content": "System maintenance scheduled"}
                    )
                ],
                "analytics": [json.dumps({"event": "logout", "user_id": "user789"})],
                "logs": ["INFO: All systems operational"],
            }
            populate_queues_with_messages(str(db_path), final_messages)

            # Final processing time
            time.sleep(2)

        except KeyboardInterrupt:
            print("\n\n⏹️  Stopping MultiQueueWatcher...")
        finally:
            watcher.stop()
            print("✅ MultiQueueWatcher stopped successfully")

        print("\n📊 Final Statistics:")
        print(f"   Active queues: {watcher._active_queues}")
        print(f"   Total check cycles: {watcher._check_counter}")
        print(f"   Database: {db_path}")


if __name__ == "__main__":
    main()
