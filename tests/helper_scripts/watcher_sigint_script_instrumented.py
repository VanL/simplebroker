#!/usr/bin/env python3
"""Instrumented helper script to analyze race conditions in SIGINT handling."""

import sys
import time
from pathlib import Path

# Add parent directory to path so we can import simplebroker
sys.path.insert(0, str(Path(__file__).parent.parent))

from simplebroker.db import BrokerDB
from simplebroker.watcher import QueueWatcher


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: watcher_sigint_script_instrumented.py <db_path> <ready_file>")
        sys.exit(1)

    db_path = Path(sys.argv[1])
    ready_file = Path(sys.argv[2])

    # Step 1: Signal that we've started
    print("STEP_1_STARTED", flush=True)
    start_time = time.time()

    # Step 2: Create database connection (this might be slow)
    print("STEP_2_DB_INIT_START", flush=True)
    db_init_start = time.time()

    try:
        db = BrokerDB(str(db_path))
        db_init_end = time.time()
        print(f"STEP_2_DB_INIT_COMPLETE:{db_init_end - db_init_start:.3f}s", flush=True)
    except Exception as e:
        print(f"STEP_2_DB_INIT_FAILED:{e}", flush=True)
        sys.exit(1)

    # Step 3: Create watcher (this should be fast)
    print("STEP_3_WATCHER_INIT_START", flush=True)
    watcher_init_start = time.time()

    def handler(msg: str, ts: float) -> None:
        print(f"Received: {msg}")

    try:
        watcher = QueueWatcher(
            "sigint_test_queue",
            handler,
            db=db,
        )
        watcher_init_end = time.time()
        print(
            f"STEP_3_WATCHER_INIT_COMPLETE:{watcher_init_end - watcher_init_start:.3f}s",
            flush=True,
        )
    except Exception as e:
        print(f"STEP_3_WATCHER_INIT_FAILED:{e}", flush=True)
        db.close()
        sys.exit(1)

    # Step 4: Create ready file (this should be very fast)
    print("STEP_4_READY_FILE_START", flush=True)
    ready_file_start = time.time()

    try:
        ready_file.touch()
        ready_file_end = time.time()
        print(
            f"STEP_4_READY_FILE_COMPLETE:{ready_file_end - ready_file_start:.3f}s",
            flush=True,
        )
    except Exception as e:
        print(f"STEP_4_READY_FILE_FAILED:{e}", flush=True)
        db.close()
        sys.exit(1)

    # Step 5: Signal that we're ready for signals
    total_init_time = time.time() - start_time
    print(f"STEP_5_READY_FOR_SIGNALS:{total_init_time:.3f}s", flush=True)

    try:
        # This should exit gracefully when receiving SIGINT
        print("STEP_6_RUN_FOREVER_START", flush=True)
        watcher.run_forever()
        exit_code = 0
        print("STEP_6_RUN_FOREVER_COMPLETE", flush=True)
    except KeyboardInterrupt:
        # This shouldn't happen - watcher should handle it internally
        print("STEP_6_KEYBOARD_INTERRUPT", flush=True)
        exit_code = 1
    except Exception as e:
        print(f"STEP_6_EXCEPTION:{e}", flush=True)
        exit_code = 1
    finally:
        print("STEP_7_CLEANUP_START", flush=True)
        db.close()
        print("STEP_7_CLEANUP_COMPLETE", flush=True)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
