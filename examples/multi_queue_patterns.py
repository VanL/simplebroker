#!/usr/bin/env python3
"""
MultiQueueWatcher Usage Patterns

This file demonstrates various patterns and use cases for MultiQueueWatcher:

1. Basic multi-queue setup
2. Priority queues simulation
3. Queue-specific error handling
4. Dynamic queue management
5. Load balancing patterns
6. Monitoring and metrics

Each pattern is a standalone function that can be run independently.
"""

from __future__ import annotations

import logging

# Import MultiQueueWatcher from the same directory
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional

from simplebroker import Queue

sys.path.insert(0, str(Path(__file__).parent))
from multi_queue_watcher import MultiQueueWatcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def pattern_1_basic_setup() -> None:
    """Pattern 1: Basic multi-queue setup with different handlers."""
    print("\n" + "=" * 60)
    print("PATTERN 1: Basic Multi-Queue Setup")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "basic.db"

        # Define different handlers for different message types
        def urgent_handler(msg: str, ts: int) -> None:
            print(f"🚨 URGENT: {msg}")

        def normal_handler(msg: str, ts: int) -> None:
            print(f"📝 Normal: {msg}")

        def audit_handler(msg: str, ts: int) -> None:
            print(f"🔍 Audit: {msg}")

        # Create queues with specific handlers
        watcher = MultiQueueWatcher(
            queues=["urgent", "normal", "audit"],
            queue_handlers={
                "urgent": urgent_handler,
                "normal": normal_handler,
                "audit": audit_handler,
            },
            db=str(db_path),
        )

        # Add some messages
        urgent_q = Queue("urgent", db_path=str(db_path), persistent=True)
        normal_q = Queue("normal", db_path=str(db_path), persistent=True)
        audit_q = Queue("audit", db_path=str(db_path), persistent=True)

        urgent_q.write("Critical system failure!")
        normal_q.write("User logged in")
        audit_q.write("Database query executed")
        normal_q.write("Email sent")
        urgent_q.write("Security breach detected!")

        # Process messages
        watcher.start()
        time.sleep(2)
        watcher.stop()

        print("✅ Pattern 1 complete: Different handlers for different queue types")


def pattern_2_priority_simulation() -> None:
    """Pattern 2: Simulating priority queues with weighted processing."""
    print("\n" + "=" * 60)
    print("PATTERN 2: Priority Queue Simulation")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "priority.db"

        # Create a custom MultiQueueWatcher that processes high-priority more often
        class PriorityMultiQueueWatcher(MultiQueueWatcher):
            def _drain_queue(self) -> None:
                """Override to process high-priority queues more frequently."""
                self._update_active_queues()

                if not self._active_queues:
                    return

                messages_processed = 0

                # Process high-priority queue multiple times per round
                if "high_priority" in self._active_queues:
                    for _ in range(3):  # Process 3 messages from high priority
                        high_queue_info = self._queues["high_priority"]
                        result = high_queue_info["queue"].read_one(with_timestamps=True)
                        if result:
                            body, timestamp = result
                            original_handler = self._handler
                            self._handler = high_queue_info["handler"]
                            try:
                                self._dispatch(body, timestamp)
                                messages_processed += 1
                            finally:
                                self._handler = original_handler
                        else:
                            break

                # Then process other queues normally
                other_queues = [q for q in self._active_queues if q != "high_priority"]
                for queue_name in other_queues:
                    queue_info = self._queues[queue_name]
                    result = queue_info["queue"].read_one(with_timestamps=True)
                    if result:
                        body, timestamp = result
                        original_handler = self._handler
                        self._handler = queue_info["handler"]
                        try:
                            self._dispatch(body, timestamp)
                            messages_processed += 1
                        finally:
                            self._handler = original_handler

                if messages_processed > 0:
                    self._strategy.notify_activity()

        def high_priority_handler(msg: str, ts: int) -> None:
            print(f"🔥 HIGH PRIORITY: {msg}")

        def low_priority_handler(msg: str, ts: int) -> None:
            print(f"🐌 Low Priority: {msg}")

        watcher = PriorityMultiQueueWatcher(
            queues=["high_priority", "low_priority"],
            queue_handlers={
                "high_priority": high_priority_handler,
                "low_priority": low_priority_handler,
            },
            db=str(db_path),
        )

        # Add messages to both queues
        high_q = Queue("high_priority", db_path=str(db_path), persistent=True)
        low_q = Queue("low_priority", db_path=str(db_path), persistent=True)

        for i in range(5):
            high_q.write(f"Critical task {i}")
            low_q.write(f"Background task {i}")

        print("Notice how high-priority messages are processed more frequently:")
        watcher.start()
        time.sleep(3)
        watcher.stop()

        print("✅ Pattern 2 complete: Priority simulation with weighted processing")


def pattern_3_error_handling() -> None:
    """Pattern 3: Queue-specific error handling strategies."""
    print("\n" + "=" * 60)
    print("PATTERN 3: Queue-Specific Error Handling")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "errors.db"

        # Handlers that might fail
        def critical_handler(msg: str, ts: int) -> None:
            print(f"💎 Critical: {msg}")
            if "fail" in msg.lower():
                raise ValueError(f"Critical handler failed for: {msg}")

        def resilient_handler(msg: str, ts: int) -> None:
            print(f"🛡️  Resilient: {msg}")
            if "fail" in msg.lower():
                print(f"⚠️  Resilient handler handled failure gracefully for: {msg}")

        # Queue-specific error handlers - much cleaner approach!
        def critical_error_handler(
            exc: Exception, message: str, timestamp: int
        ) -> bool:
            print(f"❌ CRITICAL ERROR: {exc}")
            print(f"   Message: {message}")
            # Return False to stop processing for critical errors
            return False

        def resilient_error_handler(
            exc: Exception, message: str, timestamp: int
        ) -> bool:
            print(f"⚠️  Recoverable error: {exc}")
            print(f"   Message: {message}")
            # Return True to continue processing
            return True

        watcher = MultiQueueWatcher(
            queues=["critical_tasks", "resilient_tasks"],
            queue_handlers={
                "critical_tasks": critical_handler,
                "resilient_tasks": resilient_handler,
            },
            queue_error_handlers={
                "critical_tasks": critical_error_handler,
                "resilient_tasks": resilient_error_handler,
            },
            db=str(db_path),
        )

        # Add messages that will succeed and fail
        critical_q = Queue("critical_tasks", db_path=str(db_path), persistent=True)
        resilient_q = Queue("resilient_tasks", db_path=str(db_path), persistent=True)

        critical_q.write("Process payment")
        resilient_q.write("Update user profile")
        critical_q.write("FAIL: Bad payment data")  # This will cause error
        resilient_q.write("FAIL: Bad profile data")  # This will be handled gracefully
        critical_q.write("Send confirmation email")

        print("Watch how different queues handle errors differently:")
        watcher.start()
        time.sleep(2)
        watcher.stop()

        print("✅ Pattern 3 complete: Queue-specific error handling")
        print("   This approach provides clean separation of error handling per queue")


def pattern_4_load_balancing() -> None:
    """Pattern 4: Load balancing across similar workers."""
    print("\n" + "=" * 60)
    print("PATTERN 4: Load Balancing Pattern")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "load_balance.db"

        # Simulate different worker types
        def worker_a_handler(msg: str, ts: int) -> None:
            print(f"👷‍♀️ Worker A processing: {msg}")
            time.sleep(0.1)  # Simulate work

        def worker_b_handler(msg: str, ts: int) -> None:
            print(f"👷‍♂️ Worker B processing: {msg}")
            time.sleep(0.1)  # Simulate work

        def worker_c_handler(msg: str, ts: int) -> None:
            print(f"🤖 Worker C processing: {msg}")
            time.sleep(0.1)  # Simulate work

        # Create watcher with multiple worker queues
        watcher = MultiQueueWatcher(
            queues=["worker_a_queue", "worker_b_queue", "worker_c_queue"],
            queue_handlers={
                "worker_a_queue": worker_a_handler,
                "worker_b_queue": worker_b_handler,
                "worker_c_queue": worker_c_handler,
            },
            db=str(db_path),
            check_interval=3,  # Check inactive queues more frequently
        )

        # Distribute work across worker queues
        worker_queues = [
            Queue("worker_a_queue", db_path=str(db_path), persistent=True),
            Queue("worker_b_queue", db_path=str(db_path), persistent=True),
            Queue("worker_c_queue", db_path=str(db_path), persistent=True),
        ]

        # Add work items in round-robin fashion to balance load
        tasks = [
            "Process image upload",
            "Generate report",
            "Send notification",
            "Update database",
            "Validate user input",
            "Archive old data",
            "Backup files",
            "Clean temp directory",
            "Update search index",
        ]

        print("Distributing tasks across worker queues:")
        for i, task in enumerate(tasks):
            worker_queue = worker_queues[i % len(worker_queues)]
            worker_queue.write(f"Task {i + 1}: {task}")
            print(f"   Assigned '{task}' to {worker_queue.name}")

        print("\nProcessing tasks with load balancing:")
        watcher.start()
        time.sleep(4)
        watcher.stop()

        print("✅ Pattern 4 complete: Load balancing across worker queues")


def pattern_5_monitoring() -> None:
    """Pattern 5: Adding monitoring and metrics to MultiQueueWatcher."""
    print("\n" + "=" * 60)
    print("PATTERN 5: Monitoring and Metrics")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "monitored.db"

        # Enhanced MultiQueueWatcher with metrics
        class MonitoredMultiQueueWatcher(MultiQueueWatcher):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)
                self.metrics: Dict[str, Any] = {
                    "total_processed": 0,
                    "queue_stats": {
                        queue: {"processed": 0, "errors": 0}
                        for queue in self._queues.keys()
                    },
                    "start_time": time.time(),
                }
                self.current_queue: Optional[str] = None

            def _dispatch(self, message: str, timestamp: int) -> None:
                """Override to collect metrics."""
                start_time = time.time()

                try:
                    super()._dispatch(message, timestamp)
                    self.metrics["total_processed"] += 1
                    if self.current_queue:
                        self.metrics["queue_stats"][self.current_queue][
                            "processed"
                        ] += 1
                except Exception:
                    if self.current_queue:
                        self.metrics["queue_stats"][self.current_queue]["errors"] += 1
                    raise
                finally:
                    processing_time = time.time() - start_time
                    if processing_time > 0.1:  # Log slow messages
                        print(
                            f"⚠️  Slow processing: {processing_time:.2f}s for queue {self.current_queue}"
                        )

            def _drain_queue(self) -> None:
                """Override to track current queue for metrics."""
                self._update_active_queues()

                if not self._active_queues:
                    return

                for queue_name in self._active_queues:
                    self.current_queue = queue_name
                    queue_info = self._queues[queue_name]

                    result = queue_info["queue"].read_one(with_timestamps=True)
                    if result:
                        body, timestamp = result
                        original_handler = self._handler
                        self._handler = queue_info["handler"]
                        try:
                            self._dispatch(body, timestamp)
                        finally:
                            self._handler = original_handler

                self.current_queue = None

                # Print metrics every 10 messages
                if (
                    self.metrics["total_processed"] % 10 == 0
                    and self.metrics["total_processed"] > 0
                ):
                    self.print_metrics()

            def print_metrics(self) -> None:
                """Print current metrics."""
                runtime = time.time() - self.metrics["start_time"]
                print(f"\n📊 METRICS UPDATE (Runtime: {runtime:.1f}s)")
                print(f"   Total processed: {self.metrics['total_processed']}")
                for queue, stats in self.metrics["queue_stats"].items():
                    print(
                        f"   {queue}: {stats['processed']} processed, {stats['errors']} errors"
                    )

        def default_handler(msg: str, ts: int) -> None:
            print(f"📨 Processing: {msg}")
            # Simulate some processing time
            time.sleep(0.05)

        watcher = MonitoredMultiQueueWatcher(
            queues=["queue1", "queue2", "queue3"],
            default_handler=default_handler,
            db=str(db_path),
        )

        # Add messages to create workload
        queues = [
            Queue("queue1", db_path=str(db_path), persistent=True),
            Queue("queue2", db_path=str(db_path), persistent=True),
            Queue("queue3", db_path=str(db_path), persistent=True),
        ]

        print("Adding messages to create workload...")
        for i in range(25):
            queue = queues[i % 3]
            queue.write(f"Message {i + 1} for processing")

        print("Processing with monitoring enabled:")
        watcher.start()
        time.sleep(3)
        watcher.stop()

        # Final metrics
        watcher.print_metrics()
        print("✅ Pattern 5 complete: Monitoring and metrics collection")


def main() -> None:
    """Run all pattern demonstrations."""
    print("🎭 MultiQueueWatcher Usage Patterns")
    print("This demonstrates various patterns and use cases for MultiQueueWatcher\n")

    try:
        pattern_1_basic_setup()
        pattern_2_priority_simulation()
        pattern_3_error_handling()
        pattern_4_load_balancing()
        pattern_5_monitoring()

        print("\n" + "=" * 60)
        print("All patterns completed")
        print("=" * 60)
        print("\nKey features:")
        print("✓ MultiQueueWatcher provides round-robin processing")
        print("✓ Each queue can have its own handler and error handling")
        print("✓ Single database connection shared across all queues")
        print("✓ Can be extended for priority queues, load balancing, monitoring")
        print("✓ Inherits BaseWatcher's polling and lifecycle management")

    except KeyboardInterrupt:
        print("\n\n⏹️  Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Error during demo: {e}")
        logger.exception("Demo error")


if __name__ == "__main__":
    main()
