#!/usr/bin/env python3
"""Helper script to test SIGINT handling in QueueWatcher."""

import sys
from pathlib import Path

# Add parent directory to path so we can import simplebroker
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from simplebroker.db import BrokerDB
from simplebroker.watcher import QueueWatcher


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: watcher_sigint_script.py <db_path> <ready_file>")
        sys.exit(1)

    db_path = Path(sys.argv[1])
    ready_file = Path(sys.argv[2])

    # Create database connection
    db = BrokerDB(str(db_path))

    # Simple handler that prints messages
    def handler(msg: str, ts: float) -> None:
        print(f"Received: {msg}")

    # Create watcher
    watcher = QueueWatcher(
        "sigint_test_queue",
        handler,
        db=db,
    )

    # Signal that we're ready and about to start
    ready_file.touch()

    try:
        # This should exit gracefully when receiving SIGINT
        watcher.run_forever()
        exit_code = 0
    except KeyboardInterrupt:
        # This shouldn't happen - watcher should handle it internally
        exit_code = 1
    finally:
        db.close()

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
