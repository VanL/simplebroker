# SQLite Cross-Thread Close Hardening Plan

## Purpose

This is the implementation plan for making SimpleBroker more robust when a
persistent SQLite-backed `Queue` is used in one thread and closed from another.

The concrete failure:

- Weft runs tests under coverage.
- Some Weft CLI and harness tests create or use persistent SimpleBroker queues
  from one thread, then clean them up from another.
- Under coverage, SimpleBroker emits `ResourceWarning: unclosed database` from
  `gc.collect()` inside `simplebroker/db.py`.

The root cause is not coverage. Coverage changes object lifetime enough to make
the bug visible. The bug is that SimpleBroker currently attempts to close
SQLite connections from a thread that did not create them, swallows the
`sqlite3.ProgrammingError`, clears tracking, and then garbage collection reports
the still-open database handle.

Desired invariant:

```text
Closing a persistent SQLite Queue from a different thread must not leak a
sqlite3.Connection, and must not hide a failed close by dropping the only
tracked reference to that connection.
```

This plan assumes the implementer is skilled but new to this repository. It
also assumes they tend to over-mock tests and over-design APIs. Follow the tasks
in order. Keep the change small. Use red-green TDD.

## Locked Decisions

These are decisions for this fix. Do not reopen them unless a red test proves
the plan is technically impossible.

### Scope lock

- Fix SimpleBroker. Do not solve this only by changing Weft.
- Do not introduce a new public queue/session API.
- Do not add runtime dependencies.
- Do not change message ordering, visibility, delivery guarantees, SQL schema,
  or SQL query shape.
- Do not rewrite `BrokerCore`, `BrokerDB`, or the process-local session
  registry.
- Do not remove process-local persistent session sharing.

### Robustness lock

SimpleBroker should be robust against cross-thread close for SQLite persistent
queues.

The target behavior is:

- `Queue.close()` may be called from a different thread than the thread that
  used the queue.
- The close should release all known SQLite connections for that queue/session.
- It should be warning-clean under `-W always::ResourceWarning` and coverage.
- If an unexpected close failure happens, SimpleBroker should not silently drop
  tracking for the unclosed connection.

### Implementation direction lock

Use the smallest viable hardening:

1. Make internally owned SQLite connections safe to close from the session
   close path by opening them with `check_same_thread=False`.
2. Preserve SimpleBroker's existing access discipline:
   - each `BrokerCore` serializes operations on its own runner with
     `BrokerCore._lock`
   - persistent SQLite sessions may still create one `BrokerDB`/runner/core per
     participating thread
   - normal cross-thread queue operations therefore use separate SQLite
     connections, not one shared raw connection
3. Keep tracking strong references until close has actually succeeded.
4. Add tests that prove the real leak path is fixed.

Why this direction: Python cannot force another live thread to run cleanup code
without cooperation. A fully cooperative deferred-close queue would require a
larger lifecycle protocol, thread heartbeats, pending-close handoff state, and
new edge cases for dead threads. That is too much for this bug. SimpleBroker
already claims and relies on cross-thread cleanup in several places, and its
runner/core operations are already guarded at the core boundary. Making SQLite
connections cross-thread closeable is the direct fix for cleanup after worker
operations have quiesced.

### Deferral lock

Do not implement a broad deferred-close scheduler in this change.

It is acceptable to add narrow internal helpers that separate "close this
connection" from "forget this connection" so failures do not remove tracking.
It is not acceptable to add a new background cleanup thread, global finalizer
queue, public `drain_closes()` API, or thread mailbox system.

### Contract lock

After this fix, the public contract should be:

- SimpleBroker SQLite queue handles are safe to use across threads through the
  `Queue` API. Internally, persistent SQLite sessions may create separate
  per-thread cores/connections.
- A persistent queue/session may own multiple SQLite connections internally,
  typically one per participating thread.
- `Queue.close()` releases the persistent session lease and closes known
  resources after participating operations have returned.
- `Queue.cleanup_connections()` recycles current-thread handles without
  releasing the persistent session lease.

Do not promise lock-free concurrent SQLite use. Do not imply users should pass
raw sqlite connections across threads. This is an internal SimpleBroker runner
behavior.

Also do not promise that `Queue.close()` can safely race an operation that is
actively executing in another thread. The target is robust cleanup when the
close call happens from a different thread after prior operations have returned.
If tests reveal an in-flight close race, handle it as a separate lifecycle
problem rather than expanding this plan silently.

## Repository Primer

### Project shape

- Runtime package: `simplebroker/`
- Tests: `tests/`
- Planning docs: `docs/plans/`
- Tooling config: `pyproject.toml`
- SQLite backend runtime helpers: `simplebroker/_backends/sqlite/`

### Files to read before coding

Read these files in order:

- `pyproject.toml`
- `docs/plans/2026-05-04-process-local-broker-session-plan.md`
- `simplebroker/sbqueue.py`
- `simplebroker/db.py`
- `simplebroker/_broker_session.py`
- `simplebroker/_runner.py`
- `simplebroker/_runner_lifecycle.py`
- `tests/test_queue_connection_manager.py`
- `tests/test_queue_coverage.py`
- `tests/test_runner_error_handling.py`
- `tests/test_watcher_concurrency.py`

Why the prior plan matters: `simplebroker/_broker_session.py` exists because
persistent queues now share process-local session state. This bug is in that
new lifecycle surface. Do not undo that design.

### Current lifecycle map

Persistent queue creation:

```text
Queue(name, persistent=True)
  -> Queue.conn = DBConnection(..., share_in_process=True)
  -> DBConnection acquires a process-local _ProcessBrokerSession lease
```

Persistent queue operation:

```text
Queue.write/read/peek/...
  -> Queue.get_connection()
  -> DBConnection.get_connection()
  -> _ProcessBrokerSession.get_connection()
  -> current thread gets or creates one BrokerDB core
  -> BrokerDB owns one SQLiteRunner
  -> SQLiteRunner gets or creates one sqlite3.Connection for that thread
```

Close path that currently leaks:

```text
Queue.close() from thread B
  -> DBConnection.close()
  -> release_process_broker_session()
  -> _ProcessBrokerSession.close_all()
  -> calls core.shutdown() for cores created by thread A
  -> SQLiteRunner.close()
  -> conn.close() raises sqlite3.ProgrammingError because thread B != thread A
  -> SQLiteRunner.close() swallows the exception and clears _all_connections
  -> gc.collect() reports ResourceWarning for the unclosed connection
```

Relevant current code:

- `simplebroker/sbqueue.py`: `Queue.__init__`, `Queue.get_connection()`,
  `Queue.cleanup_connections()`, `Queue.close()`
- `simplebroker/db.py`: `DBConnection`, `BrokerCore.close()`,
  `BrokerCore.shutdown()`, `BrokerDB`
- `simplebroker/_broker_session.py`: `_ProcessBrokerSession.get_connection()`,
  `cleanup_current_thread()`, `close_all()`
- `simplebroker/_runner.py`: `SQLiteRunner.get_connection()`,
  `SQLiteRunner.close()`

## Engineering Rules

### Red-green TDD

For each behavior change:

1. Add a real test that fails on the current code.
2. Run only that targeted test and confirm it fails for the expected reason.
3. Make the smallest code change that turns it green.
4. Refactor only after the behavior is pinned.

Do not fix code first and add tests after. This bug is a lifecycle contract bug;
tests are the contract.

### Prefer real tests over mocks

Use real temporary SQLite databases and real `Queue` objects.

Good:

- `tmp_path`
- real `Queue(..., persistent=True)`
- real `threading.Thread`
- `warnings.catch_warnings(record=True)`
- `gc.collect()`
- `pytest.mark.parametrize` when the same invariant is tested across close
  paths

Bad:

- mocking `sqlite3.Connection`
- monkeypatching `BrokerDB.shutdown()`
- asserting that an internal method was called instead of proving the connection
  was closed
- tests that only inspect private sets without exercising actual queue
  operations

One narrow exception: it is acceptable to add a small test-only helper that
captures exceptions or warnings, but the resource under test should be the real
SQLite runner.

### DRY

- If warning capture is needed in multiple tests, add one small local helper in
  the test module.
- If connection close bookkeeping needs a helper, add one private helper in
  `SQLiteRunner`; do not duplicate close/remove logic across methods.
- If docs mention the same lifecycle contract in more than one place, keep it
  short and consistent rather than writing separate long explanations.

### YAGNI

Do not add:

- a background close worker
- a thread mailbox
- an async API
- a queue-level ownership registry
- a public close policy flag
- new config keys
- a general resource manager abstraction

This fix should probably be a small runner-level change plus targeted tests and
minor docs.

### Code style

- Use the repository's existing style: type hints, short private helpers, and
  explicit exception handling where resource cleanup is involved.
- Keep comments rare and useful. A comment explaining why
  `check_same_thread=False` is safe here is useful. A comment narrating every
  assignment is not.
- Use ASCII only unless the touched file already requires otherwise.
- Do not hide unexpected cleanup failures with a bare `except Exception: pass`
  if that means dropping the last tracked reference to a resource.
- Do not add dependencies.

## Tooling

Run commands from the repository root.

First-time setup:

```bash
uv sync --extra dev
```

Targeted tests while iterating:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py -k "cross_thread or persistent"
uv run pytest -q -n 0 tests/test_queue_coverage.py -k "finalizer or close"
uv run pytest -q -n 0 tests/test_runner_error_handling.py -k "close"
```

Coverage-specific reproduction gate:

```bash
uv run coverage run --concurrency=thread -m pytest -q -n 0 tests/test_queue_connection_manager.py -k "cross_thread"
uv run coverage report
```

Warning-clean gate:

```bash
uv run pytest -q -n 0 -W error::ResourceWarning tests/test_queue_connection_manager.py -k "cross_thread"
```

Broader SQLite lifecycle gate:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py tests/test_queue_coverage.py tests/test_runner_error_handling.py tests/test_watcher_concurrency.py
```

Full local gates:

```bash
uv run pytest -q -n 0
uv run ruff check .
uv run mypy simplebroker
```

Use `-n 0` while working on this. `pyproject.toml` enables pytest-xdist by
default, but parallel test output makes lifecycle warnings harder to reason
about.

## Task 0: Establish The Baseline

### Goal

Confirm the current code fails in the way this plan describes.

### Files to touch

- None

### Steps

1. Read the files in the repository primer.
2. Run the current targeted queue tests:

   ```bash
   uv run pytest -q -n 0 tests/test_queue_connection_manager.py tests/test_queue_coverage.py
   ```

3. Reproduce the leak shape with a scratch command or a temporary test. Do not
   commit the scratch command.

   Expected shape:

   ```python
   import gc
   import threading
   import warnings
   from simplebroker import Queue

   q = Queue("x", db_path=str(tmp_path / "q.db"), persistent=True)
   q.write("hello")
   list(q.peek_generator())

   caught = []

   def closer() -> None:
       with warnings.catch_warnings(record=True) as ws:
           warnings.simplefilter("always", ResourceWarning)
           q.close()
           gc.collect()
           caught.extend(ws)

   thread = threading.Thread(target=closer)
   thread.start()
   thread.join()

   assert caught
   ```

### Gate

- You can explain why same-thread close is clean and cross-thread close warns.
- You can point to the swallowed exception in `SQLiteRunner.close()`.
- You can point to the `gc.collect()` call that surfaces the warning.

## Task 1: Add A Red Regression Test For Persistent Queue Cross-Thread Close

### Goal

Pin the externally visible bug with a real integration test.

### Files to touch

- `tests/test_queue_connection_manager.py`

### Test design

Add a test named something like:

```python
def test_persistent_queue_cross_thread_close_does_not_warn(tmp_path) -> None:
    ...
```

Use real SQLite and real threads:

1. Create `Queue("tasks", db_path=str(tmp_path / "broker.db"), persistent=True)`.
2. Write at least one message.
3. Read or peek through a generator or normal operation so the current thread
   definitely creates and uses a SQLite connection.
4. Close the queue from a different `threading.Thread`.
5. Inside the close thread, capture `ResourceWarning` with
   `warnings.catch_warnings(record=True)`.
6. Call `gc.collect()` after `q.close()` while warnings are still captured.
7. Assert that no captured warning is a `ResourceWarning`.

Important details:

- Use `thread.join(timeout=...)` and assert the thread finished. A hung cleanup
  test should fail clearly.
- If the close thread raises, capture the exception and fail the test in the
  main thread. Do not let thread exceptions disappear into stderr.
- Do not mock SQLite. The point is to exercise the real `sqlite3.Connection`
  thread rule.

### Expected red failure

On current code, the test should fail because at least one `ResourceWarning` is
captured after cross-thread close.

### Gate

Run:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py -k "cross_thread_close"
```

The test must fail before implementation. If it passes before code changes,
the test is not exercising the leak.

## Task 2: Add A Red Regression Test For Session Close With Multiple Threads

### Goal

Prove the fix covers the process-local persistent session shape, not just a
single main-thread-created core.

### Files to touch

- `tests/test_queue_connection_manager.py`

### Test design

Add a test named something like:

```python
def test_persistent_queue_close_cleans_connections_created_by_worker_threads(
    tmp_path,
) -> None:
    ...
```

Use this shape:

1. Create one persistent queue on the main thread.
2. Start two worker threads.
3. Each worker uses that same queue for a real operation, such as:
   - `queue.write(f"message-{i}")`
   - `list(queue.peek_generator())`
4. Join both workers and assert no worker failed.
5. Close the queue on the main thread under `ResourceWarning` capture.
6. Call `gc.collect()` while capture is active.
7. Assert no `ResourceWarning`.

Why this test matters: `_ProcessBrokerSession` stores one core per participating
thread. The bug is most likely when `close_all()` iterates cores created by
other threads.

### Avoid bad assertions

Do not assert exact private `_cores` lengths unless you need that for debugging.
The public invariant is warning-clean resource cleanup after real operations.

### Gate

Run:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py -k "worker_threads"
```

The test should fail on current code.

## Task 3: Harden SQLiteRunner Connection Ownership

### Goal

Make internally owned SQLite connections closeable from SimpleBroker cleanup
paths, and stop dropping tracking for failed closes.

### Files to touch

- `simplebroker/_runner.py`

### Implementation steps

1. In `SQLiteRunner.get_connection()`, update the `sqlite3.connect(...)` call
   to pass `check_same_thread=False`.

2. Add a short comment near the connection creation explaining why this is safe:

   - SimpleBroker does not expose the raw connection.
   - each `BrokerCore` serializes operations against its own runner with its
     lock.
   - persistent SQLite cross-thread operations normally use separate internal
     connections.
   - The flag is needed so runner/session cleanup can close known connections
     from the owning queue/session close path.

3. Refactor `SQLiteRunner.close()` so it does not clear `_all_connections`
   until close has succeeded for each connection.

   A small private helper is fine:

   ```python
   def _close_tracked_connection(self, conn: sqlite3.Connection) -> bool:
       try:
           conn.close()
       except Exception:
           return False
       return True
   ```

   Then `close()` should:

   - copy or iterate the tracked set under `_connections_lock`
   - attempt to close each connection
   - remove only successfully closed connections
   - keep failed connections tracked

4. Decide how to report close failures.

   Preferred behavior:

   - If `BROKER_LOGGING_ENABLED` is true, log a warning for unexpected close
     failures.
   - Do not raise from `close()` during cleanup. Existing cleanup paths expect
     best-effort shutdown.
   - Do not silently discard the resource.

5. Clean up current-thread local storage after a successful close.

   Current behavior deletes `self._thread_local.conn` if present. Keep that, but
   do not assume it is the only tracked connection.

### Invariants

- Closing an already-closed runner remains idempotent.
- Closing from a non-owner thread does not raise.
- Connections that fail to close remain in `_all_connections`.
- No raw sqlite connection is exposed through a new public API.

### Gate

Run:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py -k "cross_thread or worker_threads"
uv run pytest -q -n 0 tests/test_runner_error_handling.py -k "close"
```

Both new red tests should now pass. Existing close error handling tests should
still pass.

## Task 4: Add A Direct Runner Test For Failed Close Tracking

### Goal

Prevent regression to the old "swallow and clear" behavior.

### Files to touch

- `tests/test_runner_error_handling.py`

### Test design

There is likely already a close-error test in this file. Extend it or add a
small adjacent test.

The invariant to test:

```text
If a tracked connection fails to close, SQLiteRunner.close() must not remove it
from _all_connections.
```

Use the lightest acceptable fake here because this is specifically testing
runner bookkeeping:

1. Create a real `SQLiteRunner`.
2. Get a real connection so setup paths are normal.
3. Replace only the tracked connection with a tiny object whose `close()`
   raises, or use the existing mock pattern if the file already does so.
4. Call `runner.close()`.
5. Assert the failed-close object is still in `_all_connections`.

Keep this test focused. Do not use it as the main proof that queue cleanup is
fixed. The integration tests from Tasks 1 and 2 are the main proof.

### Gate

Run:

```bash
uv run pytest -q -n 0 tests/test_runner_error_handling.py -k "close"
```

## Task 5: Audit BrokerCore Shutdown And Session Close Semantics

### Goal

Make sure the runner-level fix composes with higher-level lifecycle paths.

### Files to inspect

- `simplebroker/db.py`
- `simplebroker/_broker_session.py`
- `simplebroker/sbqueue.py`

### What to check

Check these paths manually:

- `Queue.close()` detaches the finalizer and calls `DBConnection.close()`.
- `DBConnection.close()` releases a shared process session lease when
  `share_in_process=True`.
- `release_process_broker_session()` calls `_ProcessBrokerSession.close_all()`
  only when the refcount reaches zero.
- `_ProcessBrokerSession.close_all()` calls `core.shutdown()` for SQLite cores.
- `BrokerCore.shutdown()` calls `close_owned_runner(self._runner)`.
- `close_owned_runner()` handles runner lifecycle consistently.

### Possible small fixes

Make only small fixes if inspection or tests reveal them:

- If a close path clears tracking before close succeeds, fix that.
- If a close path calls `gc.collect()` before closing known resources, reorder
  it.
- If a close path catches an exception and loses the resource, keep the resource
  tracked and log when configured.

Do not add a new lifecycle abstraction here. The primary fix should remain in
`SQLiteRunner`.

### Gate

Run:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py tests/test_queue_coverage.py tests/test_runner_error_handling.py
```

## Task 6: Add Coverage Warning Gate For The Regression Tests

### Goal

Confirm the original "under coverage" symptom is gone.

### Files to touch

- Usually none

If the new tests are too hard to select by `-k`, consider giving them clear
names containing `cross_thread_close` and `worker_thread_close`.

### Commands

Run:

```bash
uv run coverage run --concurrency=thread -m pytest -q -n 0 tests/test_queue_connection_manager.py -k "cross_thread_close or worker_thread"
uv run coverage report
```

Then run the warning-as-error version:

```bash
uv run pytest -q -n 0 -W error::ResourceWarning tests/test_queue_connection_manager.py -k "cross_thread_close or worker_thread"
```

### Gate

- Both commands pass.
- No `ResourceWarning: unclosed database` appears.
- If a warning appears from a different resource, investigate it. Do not filter
  it away unless it is proved unrelated and documented in the test.

## Task 7: Update Documentation And Comments

### Goal

Document the real internal thread contract without over-explaining it to normal
users.

### Files to touch

- `simplebroker/_runner.py`
- Possibly `simplebroker/sbqueue.py`
- Possibly `README.md` if it already has a persistent queue/threading section

### Guidance

At minimum, add or adjust internal comments near `sqlite3.connect()` in
`SQLiteRunner.get_connection()`.

Good comment:

```python
# SQLiteRunner owns these internal connections. Disable sqlite's same-thread
# restriction so session cleanup can release connections created by worker
# threads after their queue operations have returned.
```

Avoid broad user-facing claims such as:

```text
SimpleBroker supports unlimited concurrent SQLite usage.
```

That is not the contract. The contract is normal `Queue` API use with robust
cleanup of known internal connections after operations have returned.

If adding README text, keep it short:

```text
Persistent SQLite queues may create one internal connection per participating
thread. SimpleBroker closes known internal connections when the persistent
queue/session is closed after participating operations have returned.
```

### Gate

Run:

```bash
uv run ruff check simplebroker tests
```

## Task 8: Run Full Verification

### Goal

Prove the fix does not break normal queue, watcher, or runner behavior.

### Commands

Run:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py tests/test_queue_coverage.py tests/test_runner_error_handling.py tests/test_watcher_concurrency.py
uv run pytest -q -n 0
uv run ruff check .
uv run mypy simplebroker
```

If full tests are too slow locally, at minimum run the targeted lifecycle suite
and record that full tests were not run. Do not claim full verification unless
it actually completed.

### Optional Weft verification

If the Weft checkout is available next to this repo, run its warning-producing
coverage command against the local SimpleBroker checkout.

The exact command depends on Weft's tooling. The useful invariant is:

```text
Weft tests that previously warned under coverage no longer emit
simplebroker/db.py ResourceWarning for unclosed sqlite3.Connection objects.
```

Do not make the SimpleBroker PR depend on Weft being locally available.

## Task 9: Review The Diff With A Failure-Mode Checklist

### Goal

Catch lifecycle mistakes before review.

### Checklist

Review the final diff and answer each question:

- Did we add a red test for the exact observed cross-thread close warning?
- Did we add a multi-thread persistent session test, not just a single-thread
  close test?
- Does `SQLiteRunner.close()` remove a connection from tracking only after the
  close succeeds?
- Are unexpected close failures visible through logging when logging is enabled?
- Did we avoid changing public queue APIs?
- Did we avoid adding a background cleanup system?
- Did we avoid filtering or suppressing `ResourceWarning` in tests?
- Did we preserve idempotent close behavior?
- Did we keep process-local persistent session sharing intact?
- Did we run a coverage-based warning gate?

If any answer is "no", fix it before asking for review.

## Common Mistakes To Avoid

### Mistake: Mocking away SQLite

Do not use mocks for the main regression. This bug exists because Python's real
sqlite connection enforces thread ownership. A mock will miss the bug.

### Mistake: Filtering ResourceWarning

Do not solve the test failure by adding a warning filter. The point is to close
the connection, not hide the warning.

### Mistake: Clearing tracking too early

The old code clears `_all_connections` even when close fails. That is the
tracking bug. Remove only what is actually closed.

### Mistake: Overbuilding deferral

Do not build a cross-thread cleanup queue. It sounds conceptually neat, but it
requires cooperative thread polling and does not solve dead worker threads. It
is a materially larger design than this fix needs.

### Mistake: Removing thread-local connections entirely

Do not convert SQLiteRunner to one global connection shared by all threads.
SimpleBroker's current design is one connection per participating thread plus
core-level locking for each runner. Keep that shape.

### Mistake: Weakening tests for speed

Lifecycle tests can be small and still real. A temp SQLite DB, one queue, and
one or two threads are enough. Do not replace that with private state-only
tests.

## Expected Diff Shape

The final implementation should be small.

Likely touched files:

- `simplebroker/_runner.py`
- `tests/test_queue_connection_manager.py`
- `tests/test_runner_error_handling.py`
- maybe `README.md` or a short docstring/comment

If the diff starts touching many watcher files, queue API files, or backend
plugin interfaces, stop and reassess. That probably means the implementation is
drifting away from the agreed direction.

## Review Notes For The Implementer

The subtle point: `check_same_thread=False` is not a license for unsynchronized
raw SQLite use. It removes Python's guard that prevents a connection object from
being used or closed outside its creator thread. SimpleBroker still needs its
existing core-level locking and its existing per-thread connection shape. This
plan uses the flag to make cleanup honest after participating operations have
returned, not to change runtime concurrency semantics.

The other subtle point: cleanup code must be honest. Best-effort cleanup is
fine, but best-effort cleanup that forgets unclosed resources is not fine. If a
resource fails to close, keep it tracked and make the failure visible when
logging is enabled.

## Stop Conditions

Stop and ask for design review if any of these happen:

- `check_same_thread=False` causes real concurrent operation failures in the
  targeted watcher or queue tests.
- The fix requires changing queue message semantics.
- The fix requires adding a public API.
- The fix requires changing Weft to pass tests.
- The only way to pass is to suppress `ResourceWarning`.

Those outcomes would mean this plan is wrong or incomplete. Do not drift into a
different architecture without making that explicit.
