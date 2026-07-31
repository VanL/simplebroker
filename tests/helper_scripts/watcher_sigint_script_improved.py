"""Improved SIGINT test script with race condition mitigation."""

import signal
import sys
import time
from pathlib import Path
from typing import cast

# Add repository root to path so subprocesses import this checkout even when
# PYTHONPATH contains another SimpleBroker install.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from simplebroker.db import BrokerDB
from simplebroker.watcher import QueueWatcher


def main() -> None:  # noqa: C901 approved [DOM-10.1.1] [RUFF-SUP-031] exception
    if len(sys.argv) != 3:
        print("Usage: watcher_sigint_script_improved.py <db_path> <ready_file>")
        sys.exit(1)

    db_path = Path(sys.argv[1])
    ready_file = Path(sys.argv[2])

    # Strategy 1: Use unique database file to avoid contention
    # Append process ID to make database file unique
    import os

    unique_db_path = db_path.parent / f"{db_path.stem}_pid{os.getpid()}{db_path.suffix}"

    # Strategy 2: Retry database connection with exponential backoff
    max_retries = 5
    for attempt in range(max_retries):
        try:
            # Copy test data from original database if it exists
            if db_path.exists():
                # Read from original database
                with BrokerDB(str(db_path)) as original_db:
                    messages = list(
                        original_db.peek_generator(
                            "sigint_test_queue", with_timestamps=False
                        )
                    )

                # Write to unique database
                with BrokerDB(str(unique_db_path)) as unique_db:
                    for msg in messages:
                        unique_db.write("sigint_test_queue", cast(str, msg))

            # Create database connection with unique file
            db = BrokerDB(str(unique_db_path))
            break

        except Exception as e:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-008] exception
            if attempt < max_retries - 1:
                # Exponential backoff with jitter
                wait_time = (2**attempt) * 0.1 + (
                    time.time() % 0.05
                )  # 0.1s, 0.2s, 0.4s, 0.8s, 1.6s + jitter
                print(
                    f"Database init attempt {attempt + 1} failed: {e}, retrying in {wait_time:.3f}s",
                    flush=True,
                )
                time.sleep(wait_time)
            else:
                print(
                    f"Database initialization failed after {max_retries} attempts: {e}",
                    flush=True,
                )
                sys.exit(1)

    # Strategy 3: Use a dedicated message handler that signals readiness
    handler_called = False

    def handler(msg: str, ts: float) -> None:
        nonlocal handler_called
        handler_called = True
        print(f"Received: {msg}")

    # Create watcher
    try:
        watcher = QueueWatcher(
            "sigint_test_queue",
            handler,
            db=db,
        )
    except Exception as e:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-008] exception
        print(f"Watcher creation failed: {e}", flush=True)
        db.close()
        sys.exit(1)

    # Install a bootstrap handler before publishing readiness. run_forever()
    # replaces it with the watcher's handler and restores it on exit, but a
    # signal arriving in that narrow handoff window must still request the
    # same graceful stop.
    previous_sigint_handler = signal.getsignal(signal.SIGINT)

    def request_stop(_signum: int, _frame: object) -> None:
        watcher.stop(join=False)

    signal.signal(signal.SIGINT, request_stop)
    ready_file.touch()
    print("READY_FOR_SIGNALS", flush=True)

    try:
        # This should exit gracefully when receiving SIGINT
        watcher.run_forever()
        exit_code = 0
    except KeyboardInterrupt:
        # Treat external KeyboardInterrupt as graceful shutdown
        exit_code = 0
    except Exception as e:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-008] exception
        print(f"Watcher error: {e}", flush=True)
        exit_code = 1
    finally:
        # Strategy 5: Clean up unique database file
        try:
            db.close()
            if unique_db_path.exists():
                unique_db_path.unlink()
        except Exception as e:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-008] exception
            print(f"Cleanup error: {e}", flush=True)
        finally:
            signal.signal(signal.SIGINT, previous_sigint_handler)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
