# Shared PG Watcher Review Remediation Plan

## Purpose

This document is the implementation plan for the follow-up review findings in
the shared-connection and Postgres watcher work.

The concrete goal is narrow:

```text
Multiple persistent Queue handles for the same resolved backend target in the
same process should share backend runner/pool state, and Postgres watchers
should use one shared LISTEN/NOTIFY listener without changing watcher delivery
semantics or silently losing wakeups.
```

This plan addresses these review findings:

1. `PostgresRunner.close()` lifecycle semantics need an explicit decision.
2. The shared Postgres activity listener registry is not fork-safe.
3. Native Postgres notifications currently reset watcher burst state before a
   database check proves rows are processable.
4. Pending-message precheck state is cleared before drain code can safely use it.
5. Postgres listener startup failures are delayed into watcher timeouts.
6. `simplebroker-pg` must require the paired SimpleBroker core version that
   introduced the lifecycle behavior it relies on.

The current working tree also includes related changes that this plan must now
account for:

1. `_SharedActivityListener` has queue-level registration and unregister logic.
   That is good listener hygiene and should be preserved.
2. Wrapped non-backend-aware runners can now inherit backend identity from a
   `ResolvedTarget`. That fixes a real Postgres activity-waiter resolution hole
   and should stay covered by tests.
3. `PostgresActivityWaiter.requires_pending_check()` and matching core watcher
   logic have been added as an optimization attempt. Treat this as provisional.
   A queue refcount is not proof that a message is still pending, because it
   excludes non-watcher consumers, other processes, and direct `Queue.read()`
   calls. Do not let this hook bypass the database check unless tests prove that
   every relevant race is safe.

Assume the implementer is a skilled developer with no SimpleBroker context and
questionable instincts around abstraction and tests. Follow the tasks in order.
Keep changes small. Use red-green TDD where there is a real bug to reproduce.
Do not broaden this into a watcher rewrite or a backend framework rewrite.

## Critical Contract Note

The first review finding needs triage before code changes.

`simplebroker/_runner.py` currently defines `SQLRunner.close()` as resource
cleanup. `README.md` and `extensions/simplebroker_pg/README.md` currently show
explicitly created `PostgresRunner` instances being closed by the caller. That
means this plan should not blindly make `PostgresRunner.close()` non-destructive
again.

The real invariant is:

```text
Caller-owned PostgresRunner.close() may close the pool.
Shared SimpleBroker internals must not call PostgresRunner.close() when they
only mean "return this thread's pooled checkout".
```

The right API split is:

- `PostgresRunner.release_thread_connection()`: internal additive hook used by
  `BrokerCore.close()` to return one thread's checkout.
- `PostgresRunner.shutdown()`: explicit full pool shutdown.
- `PostgresRunner.close()`: preserve the public `SQLRunner` cleanup contract,
  unless historical release notes prove this would be a breaking regression.

If the implementer finds old published docs or tests proving that
`PostgresRunner.close()` was intentionally non-destructive, stop and report that
evidence before changing this plan. Otherwise, keep `close()` destructive and
add tests that prove shared SimpleBroker paths use `release_thread_connection()`
instead of `close()`.

## Scope Locks

### In Scope

- Core watcher interpretation of backend-native activity as a hint, not proof.
- Postgres-specific listener lifecycle, startup, fork-safety, and error handling
  inside `extensions/simplebroker_pg`.
- Tests that reproduce the current Postgres watcher failures with the real PG
  backend.
- Tests that prove shared core paths do not shut down a shared Postgres pool
  during normal queue operations.
- Packaging dependency lower bound for `simplebroker-pg`.

### Out Of Scope

- Do not patch Weft.
- Do not add a public broker/session API.
- Do not grow the Postgres pool to hide lifecycle bugs.
- Do not add a new queue watcher API.
- Do not add new runtime dependencies.
- Do not make Postgres notifications durable. `LISTEN/NOTIFY` remains a wake-up
  optimization.
- Do not batch normal consuming watchers to make tests faster.
- Do not add `if backend == "postgres"` branches in `simplebroker/watcher.py`.
- Do not redesign the backend plugin system.

### PG-Specific Logic Lock

The Postgres-specific implementation belongs in `simplebroker_pg`:

- `PostgresActivityWaiter`
- `_SharedActivityListener`
- `_SharedActivityRegistry`
- Postgres listener tests
- Postgres packaging metadata

The core watcher may only rely on the generic `ActivityWaiter` protocol:

```python
class ActivityWaiter(Protocol):
    def wait(self, timeout: float) -> bool: ...
    def close(self) -> None: ...
```

The generic semantic is:

```text
ActivityWaiter.wait(True) means "backend activity may have happened".
It does not mean "this watcher definitely has a processable message".
```

If an implementation adds an optional narrowing hook such as
`requires_pending_check()`, keep it optional and private to the optimization.
Do not add it to the required `ActivityWaiter` protocol unless the project is
ready to make every backend implement the new method. The safe default for
unknown waiters is always "pending check required."

That distinction is the heart of the PG watcher failure.

## Repository Primer

### Runtime Files

- `simplebroker/_backend_plugins.py`
  - Defines the backend plugin contract and `ActivityWaiter`.
  - Do not add Postgres-specific names here.

- `simplebroker/_runner.py`
  - Defines the public `SQLRunner` protocol.
  - Read the `close()` contract before changing runner lifecycle code.

- `simplebroker/_runner_lifecycle.py`
  - Owns the helper that closes runner objects owned by SimpleBroker.
  - `close_owned_runner()` should keep preferring `shutdown()` when present.

- `simplebroker/_broker_session.py`
  - Process-local session registry for persistent queues.
  - This is where SimpleBroker owns one runner per resolved target.

- `simplebroker/db.py`
  - `BrokerCore.close()` should prefer `release_thread_connection()` when a
    runner provides it.
  - `BrokerCore.shutdown()` and process session shutdown may close owned runners.

- `simplebroker/sbqueue.py`
  - `Queue.get_connection()` releases one backend checkout after each operation
    for persistent process-shared queues.
  - `Queue.create_activity_waiter()` asks the backend plugin for native waiters.

- `simplebroker/watcher.py`
  - `PollingStrategy.wait_for_activity()` handles delay, burst, and native waits.
  - `QueueWatcher._process_messages()` waits, prechecks, and drains.
  - `QueueWatcher._has_pending_messages()` is the gate that must verify native
    wake hints with the database.
  - `QueueWatcher._drain_queue()` is the only place that should reset burst
    after actual progress.

- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
  - `PostgresRunner` owns the psycopg pool.
  - `_SharedActivityListener` owns the dedicated `LISTEN/NOTIFY` connection.
  - `_SharedActivityRegistry` shares listeners per process/DSN/schema.
  - `PostgresActivityWaiter` is the core-facing waiter.

- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
  - `create_activity_waiter()` builds `PostgresActivityWaiter`.

- `extensions/simplebroker_pg/pyproject.toml`
  - Extension version and dependency lower bounds.

### Tests To Read First

- `tests/test_process_broker_session.py`
- `tests/test_watcher_burst_mode.py`
- `tests/test_watcher.py`
- `tests/test_watcher_concurrency.py`
- `tests/test_watcher_thundering_herd.py`
- `tests/test_watcher_race_conditions.py`
- `tests/test_backend_plugin_resolution.py`
- `tests/test_custom_runner_integration.py`
- `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py`
- `extensions/simplebroker_pg/tests/test_pg_notify.py`
- `extensions/simplebroker_pg/tests/test_pg_integration.py`

Read the existing plan too:

- `docs/plans/2026-05-04-process-local-broker-session-plan.md`

That plan explains why this is a SimpleBroker ownership issue exposed by Weft,
not a Weft issue.

## Tooling

Use commands from the repository root.

Core targeted tests:

```bash
uv run pytest -q -n0 tests/test_process_broker_session.py
uv run pytest -q -n0 tests/test_watcher_burst_mode.py
uv run pytest -q -n0 tests/test_watcher.py::TestQueueWatcher::test_since_timestamp_database_filtering
uv run pytest -q -n0 tests/test_watcher_concurrency.py::TestWorkerPool::test_worker_pool_with_slow_handlers
```

Postgres targeted tests:

```bash
bin/pytest-pg tests/test_watcher_burst_mode.py::test_burst_mode_no_reset_on_empty_wake -n0
bin/pytest-pg tests/test_watcher.py::TestQueueWatcher::test_since_timestamp_database_filtering -n0
bin/pytest-pg tests/test_watcher_concurrency.py::TestWorkerPool::test_worker_pool_with_slow_handlers -n0
bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py -n0
bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_notify.py -n0
bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_integration.py::test_postgres_project_persistent_queues_share_plugin_runner -n0
```

Full gates:

```bash
uv run pytest -q -n0
bin/pytest-pg
uv run ruff check simplebroker extensions/simplebroker_pg tests
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg
git diff --check
```

Use `-n0` during debugging. The bugs in this plan involve thread/process
lifecycle and timing. Parallel test output makes root cause harder to see.

## Engineering Rules

### Red-Green TDD

For each real defect:

1. Add or strengthen a test that fails on the current code.
2. Run only that targeted test and confirm the failure is the intended one.
3. Make the smallest code change that turns it green.
4. Refactor only after the behavior is pinned.
5. Run the task gate before moving on.

Do not implement first and backfill tests. The failures here are subtle enough
that implementation-first will almost certainly preserve a blind spot.

### Test Design Rules

Prefer real behavior:

- Use real `Queue`, `QueueWatcher`, `BrokerCore`, and `PostgresRunner` where the
  bug crosses queue/watcher/runtime boundaries.
- Use `bin/pytest-pg` for Postgres notification behavior.
- Use polling helpers such as `wait_for_condition()` for eventual behavior.
- Keep timeouts modest; raising timeouts is not a fix.

Use small fakes only at hard seams:

- A fake pool is acceptable to prove runner close/release lifecycle.
- A fake activity listener is acceptable to unit-test registry keying and
  refcounting.
- A fake `ActivityWaiter` is acceptable to test core's generic waiter contract.

Avoid:

- Mocking `Queue.has_pending()` for watcher correctness.
- Mocking psycopg notifications for the main PG behavior tests.
- Asserting full SQL strings.
- Counting private calls when visible queue state proves the same behavior.

### Code Style

- Keep code ASCII unless the edited file already requires otherwise.
- Keep helper methods private.
- Prefer one narrowly named helper over repeated ad hoc flag manipulation.
- Do not add comments that narrate obvious code.
- Add short comments only for concurrency or fork-safety invariants.
- Do not import `simplebroker_pg` from core modules.

## Task 0: Baseline And Contract Triage

### Goal

Start from known behavior and make the `PostgresRunner.close()` decision before
changing runtime code.

### Files To Touch

None.

### Steps

1. Read:
   - `simplebroker/_runner.py`
   - `simplebroker/_runner_lifecycle.py`
   - `simplebroker/db.py`, especially `BrokerCore.close()` and shutdown paths
   - `extensions/simplebroker_pg/simplebroker_pg/runner.py`
   - `README.md`, search for `PostgresRunner`
   - `extensions/simplebroker_pg/README.md`
2. Run:

   ```bash
   uv run pytest -q -n0 tests/test_process_broker_session.py
   bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py -n0
   ```

3. Decide:
   - If docs and tests confirm `PostgresRunner.close()` is destructive, do not
     change `close()` to be non-destructive.
   - If old docs or tests prove published `PostgresRunner.close()` was
     intentionally non-destructive, stop and report the compatibility conflict.

### Gate

Do not proceed until the implementer can explain this lifecycle split:

```text
normal shared queue operation -> BrokerCore.close() -> release_thread_connection()
last shared session closes     -> close_owned_runner() -> shutdown()
caller-owned runner cleanup    -> runner.close() or runner.shutdown()
```

## Task 1: Pin Shared Runner Lifecycle Semantics

### Goal

Address the `PostgresRunner.close()` review concern without breaking the public
runner cleanup contract.

The risk is not that `PostgresRunner.close()` closes the pool. The risk is that
SimpleBroker internals might accidentally call `close()` when they only mean to
release one thread's pooled checkout.

### Files To Touch

- `tests/test_process_broker_session.py`
- `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py`
- Possibly `simplebroker/db.py` only if the tests fail
- Possibly `simplebroker/_broker_session.py` only if the tests fail
- Possibly `extensions/simplebroker_pg/simplebroker_pg/runner.py` only if the
  lifecycle hooks are wrong

### Task 1.1: Add A Core Lifecycle Regression Test

Add or strengthen a test in `tests/test_process_broker_session.py`.

Test the visible invariant:

```text
Closing or recycling one shared BrokerCore must release a thread checkout but
must not close the shared runner or pool.
```

Suggested shape:

1. Use the existing `CountingBackendPlugin` and `CountingSQLiteRunner` pattern.
2. Ensure the fake runner records:
   - `release_thread_connection()` calls
   - `close()` calls
   - `shutdown()` calls if implemented
3. Create two persistent `Queue` handles for the same non-SQLite resolved target.
4. Perform one operation on each queue.
5. Call `cleanup_connections()` on one queue.
6. Assert:
   - release count increased
   - close/shutdown count did not increase
   - the sibling queue remains usable
7. Close both queues.
8. Assert the owned runner was fully closed exactly once at final session release.

Do not assert private session dictionaries unless visible lifecycle state is not
enough.

### Task 1.2: Keep PG Runner Unit Tests Explicit

In `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py`, keep or add
tests for all three runner lifecycle surfaces:

1. `release_thread_connection()` returns the current thread connection and does
   not close the pool.
2. `close()` releases the current thread connection and closes the pool, unless
   Task 0 found hard evidence that this public contract must be different.
3. `shutdown()` releases the current thread connection and closes the pool.

The fake pool test is acceptable here because the unit under test is pool
lifecycle, not queue behavior.

### Implementation Guidance

- `BrokerCore.close()` should prefer a callable `runner.release_thread_connection`
  when present.
- `close_owned_runner()` should continue to prefer `shutdown()` and fall back to
  `close()`.
- Do not add another lifecycle method unless a test proves the existing split is
  insufficient.

### Gate

```bash
uv run pytest -q -n0 tests/test_process_broker_session.py
bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py -n0
```

## Task 2: Make The Shared PG Listener Fork-Safe

### Goal

Prevent a child process from reusing a listener object inherited from the parent
process. Listener threads and psycopg connections do not survive `fork()` in a
usable way.

The current working tree already adds queue-level listener registration:

- `_conditions: dict[str, threading.Condition]`
- `_versions: dict[str, int]`
- `_queue_refcounts: dict[str, int]`
- `_wildcard_version: int`
- `register_queue()`
- `unregister_queue()`

Preserve that shape while adding fork safety. Queue-level cleanup and fork-safe
registry identity solve different problems.

### Files To Touch

- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py`

### Red Test

Add a unit test for `_SharedActivityRegistry`.

Suggested shape:

1. Create a fresh `_SharedActivityRegistry`.
2. Monkeypatch `extensions.simplebroker_pg.simplebroker_pg.runner.os.getpid` to
   return `1001`.
3. Monkeypatch `_SharedActivityListener` with a tiny fake that records:
   - constructor calls
   - `close()` calls
4. Acquire `(dsn, schema)` once. Assert one listener exists.
5. Acquire `(dsn, schema)` again in the same fake pid. Assert the same listener
   is reused and refcount increments.
6. Change fake pid to `1002`.
7. Acquire the same `(dsn, schema)` again. Assert a new listener is created.
8. Release all three acquisitions. Assert each listener is closed exactly once
   when its own refcount reaches zero.

This is an acceptable fake because the bug is registry keying, not Postgres
notification behavior.

### Implementation

In `_SharedActivityRegistry`:

1. Include `os.getpid()` in the key:

   ```python
   key = (os.getpid(), dsn, schema)
   ```

2. Use the same key shape in `acquire()` and `release()`.
3. Keep listener refcounting unchanged.
4. Do not try to close the parent listener object from the child. The child
   should simply not reuse it. The parent process still owns its listener.

### Invariants

- Same pid, same DSN, same schema: one listener.
- Same pid, different schema: different listeners.
- Different pid, same DSN, same schema: different listeners.
- Releasing a child listener must not close the parent listener.
- Closing the last waiter for one queue removes that queue's listener state
  without removing other queues on the same listener.
- Notifications for queues without active waiters do not create retained queue
  state.

### Gate

```bash
bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py -n0
```

## Task 3: Fail Fast On PG Listener Startup Failure

### Goal

A `PostgresActivityWaiter` should not be created around a listener that failed
to connect, failed to `LISTEN`, or never became ready. Startup errors should
surface at waiter creation time.

### Files To Touch

- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py`
- Optionally `extensions/simplebroker_pg/tests/test_pg_notify.py` for one live
  integration sanity check

### Red Tests

Add unit tests in `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py`.

Test 1: startup connection error is raised immediately.

1. Monkeypatch `psycopg.connect` to raise a `psycopg.OperationalError` or a
   regular `RuntimeError`.
2. Construct `_SharedActivityListener("bad dsn", schema="simplebroker")`.
3. Assert construction raises a SimpleBroker `OperationalError` or the translated
   backend error, not a later timeout from `wait()`.

Test 2: startup timeout is raised immediately.

Use a private constructor parameter only if needed:

```python
_SharedActivityListener(dsn, schema=schema, startup_timeout=0.01)
```

If adding this private parameter, keep the public waiter/plugin API unchanged.
Monkeypatch `_run` or the thread target so `_ready` is never set, then assert
construction raises an `OperationalError` that names listener startup timeout.

Do not wait five real seconds in a unit test.

### Implementation

In `_SharedActivityListener.__init__`:

1. Start the listener thread.
2. Wait for readiness for the configured startup timeout.
3. If the wait times out:
   - set `_stop_event`
   - close `_conn` if it exists
   - notify conditions
   - join the thread briefly
   - raise `OperationalError("Postgres activity listener did not start ...")`
4. If `_error` is set after readiness:
   - close the listener
   - translate psycopg errors through `_translate_error()`
   - wrap non-psycopg errors in `OperationalError`
   - raise immediately

Keep the normal `wait()` error check too. Runtime listener failures after
successful startup should still wake waiters and raise on their next wait.

### Invariants

- A returned `PostgresActivityWaiter` has a listener that successfully issued
  `LISTEN`.
- Bad DSNs fail before watcher threads degrade into missed-wakeup timeouts.
- Runtime listener errors are still visible after startup.

### Gate

```bash
bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py -n0
bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_notify.py -n0
```

## Task 4: Treat Native Activity As A Hint Until DB Verification

### Goal

Fix the PG watcher failures by making core watcher logic respect the generic
`ActivityWaiter` contract:

```text
native wake -> run pending-message check -> burst only if drain made progress
```

A native notification must not by itself reset burst state or skip
`Queue.has_pending()`.

The current working tree adds `PostgresActivityWaiter.requires_pending_check()`.
Do not treat listener queue refcount as sufficient proof that the pending check
can be skipped. It only counts waiters managed by that listener. It does not
count:

- another process that consumes the message after the notification
- a direct `Queue.read()` or `BrokerCore.claim_one()` consumer
- an application using the same database without an activity waiter

The simplest acceptable fix is to remove the optimization and always run the DB
pending check for native activity. If the optimization stays, it must be backed
by tests that cover these races and it must still avoid burst reset until drain
actually finds messages.

### Files To Touch

- `simplebroker/watcher.py`
- `tests/test_watcher_burst_mode.py`
- `tests/test_watcher.py`
- `tests/test_watcher_concurrency.py`
- `tests/test_watcher_thundering_herd.py`
- Possibly `extensions/simplebroker_pg/tests/test_pg_notify.py` if an extension
  regression test is needed

### Existing Red Signals

The user already observed these PG failures:

```text
tests/test_watcher_burst_mode.py::test_burst_mode_no_reset_on_empty_wake
tests/test_watcher.py::TestQueueWatcher::test_since_timestamp_database_filtering
tests/test_watcher_concurrency.py::TestWorkerPool::test_worker_pool_with_slow_handlers
```

Before changing code, reproduce at least the first failure with:

```bash
bin/pytest-pg tests/test_watcher_burst_mode.py::test_burst_mode_no_reset_on_empty_wake -n0
```

If it does not fail locally, still add the deterministic generic ActivityWaiter
test below.

### Task 4.1: Add A Generic Core Test For Native Hints

Add a test that does not require Postgres and does not mock queue state.

Suggested shape:

1. Create a real temporary queue with no messages.
2. Use a tiny fake `ActivityWaiter` whose first `wait()` returns `True` and later
   waits return `False`.
3. Construct a real `Queue` object and monkeypatch only that queue object's
   `create_activity_waiter()` method to return the fake waiter. This keeps the
   queue storage and `has_pending()` behavior real while controlling the
   backend-native wake seam.
4. Start a `QueueWatcher` with that queue object and an instrumented
   `PollingStrategy`.
5. Wait until the fake waiter has been called at least once, then stop the
   watcher.
6. Assert:
   - no handler call occurs
   - the strategy does not record zero-delay burst reset from the empty native
     wake
   - a real `Queue.has_pending()` check happened, if the existing instrumentation
     can observe that without excessive patching

If this test becomes awkward, prefer the existing PG failure tests over a
contorted unit test. Do not build a fake watcher framework just for this.

Also strengthen the existing `test_native_activity_hint_still_checks_empty_queue`
style coverage. Manually setting `_strategy._native_activity_pending = True`
only proves the default path. Add a variant that exercises the real
`wait_for_activity()` path with a fake waiter. If `requires_pending_check()` is
kept, include a fake waiter that returns `False` from that hook and prove the
watcher still does not enter burst when the queue is empty.

### Task 4.2: Change PollingStrategy Native Wake Behavior

In `PollingStrategy.wait_for_activity()`:

Current bad behavior:

```python
if self._activity_waiter.wait(wait_timeout):
    self._native_activity_pending = True
    self._check_count = 0
    self._activity_burst_remaining = self._initial_checks * 10
    self._record_immediate_burst_delays(count=2)
    return
```

Target behavior:

```python
if self._activity_waiter.wait(wait_timeout):
    self._native_activity_pending = True
    return
```

Do not reset `_check_count` here. Do not set `_activity_burst_remaining` here.
Do not record immediate burst delays here.

If the current `_native_activity_requires_pending_check` field remains, it must
not change this rule. A native wake may record whether a pending check is
required, but it must not trigger burst before a drain has found work.

The waiter's job is to wake the loop. The drain result decides whether to reset
burst.

### Task 4.3: Change QueueWatcher Native Hint Handling

In `QueueWatcher._has_pending_messages()`:

Current bad behavior:

```python
if self._strategy.consume_native_activity_hint():
    return True
```

Target behavior:

1. Consume the native activity hint.
2. Continue to the real database `has_pending()` check by default.
3. If the DB check returns true, set `_pending_found_by_db_check = True`.
4. If the DB check returns false, return false and preserve the current backoff.

Pseudo-code:

```python
self._strategy.consume_native_activity_hint()

def check_func() -> bool:
    return self._queue_obj.has_pending(
        self._last_seen_ts if self._last_seen_ts > 0 else None
    )

result = bool(self._process_with_retry(check_func, "pending_messages_check"))
if result:
    self._pending_found_by_db_check = True
return result
```

The local activity hint path may still return `True`. A local hint is produced
after this same watcher already found messages, and it exists to drain backlog
quickly. Native backend hints are different because another worker may have
consumed the notified row first.

Do not add this shortcut without stronger proof:

```python
if self._strategy.consume_native_activity_hint():
    return True
```

That is the original bug. A listener-side "only one waiter" check is not enough
to make it safe.

### Task 4.4: Keep Burst Reset Attached To Actual Progress

In `QueueWatcher._drain_queue()`:

- Keep `self._strategy.notify_activity()` only under `if found_messages:`.
- Do not notify activity after an empty native wake.
- Do not change consuming mode to drain more than one message unless
  `batch_processing=True`.
- Preserve the existing startup backlog handling unless a targeted test proves
  it is wrong.

If active PG watchers become too slow after removing native pre-burst reset,
debug why local backlog hints are not firing after real progress. Do not restore
native burst before DB verification.

If `requires_pending_check()` remains in the implementation, add a task-local
decision note in the code review:

```text
Why is it safe to skip the DB pending check here, given non-watcher consumers
and other processes?
```

If that question cannot be answered with tests, remove the hook or ignore it in
core watcher logic for now. YAGNI beats a subtle correctness exception.

### Invariants

- Empty native wake does not reset burst.
- Native wake with processable rows leads to prompt processing.
- Peek watchers with `since_timestamp` process all rows after the timestamp.
- Competing consuming watchers do not assume a notified row still exists.
- Consuming watchers still process at most one message per ordinary non-batch
  pass.

### Gate

```bash
uv run pytest -q -n0 tests/test_watcher_burst_mode.py
uv run pytest -q -n0 tests/test_watcher.py::TestQueueWatcher::test_since_timestamp_database_filtering
uv run pytest -q -n0 tests/test_watcher_concurrency.py::TestWorkerPool::test_worker_pool_with_slow_handlers

bin/pytest-pg tests/test_watcher_burst_mode.py::test_burst_mode_no_reset_on_empty_wake -n0
bin/pytest-pg tests/test_watcher.py::TestQueueWatcher::test_since_timestamp_database_filtering -n0
bin/pytest-pg tests/test_watcher_concurrency.py::TestWorkerPool::test_worker_pool_with_slow_handlers -n0
```

## Task 5: Fix Pending Precheck Lifetime

### Goal

Make `_pending_messages_precheck_confirmed` valid for the whole drain that
follows a successful precheck.

### Files To Touch

- `simplebroker/watcher.py`
- `tests/test_watcher_burst_mode.py` or `tests/test_watcher.py`

### Red Test

Add a small subclass test only if there is no existing red coverage after Task 4.

Suggested shape:

1. Create a real queue with one message.
2. Create a `QueueWatcher` subclass whose `_drain_queue()` calls
   `super()._has_pending_messages()` before delegating to `super()._drain_queue()`.
3. Instrument `Queue.has_pending()` with the lightest possible wrapper around the
   real method.
4. Start the watcher.
5. Assert:
   - the message is processed
   - the precheck result is consumed inside the drain without a second DB
     pending check

This is one of the few places a narrow wrapper around a method is acceptable:
the bug is private state lifetime. Do not replace the queue with a fake queue.

### Implementation

In `QueueWatcher._drain_queue()`:

- Remove the eager reset at the top of the method.
- Rely on the `finally` block in `_process_messages()` to clear
  `_pending_messages_precheck_confirmed` after `_drain_queue()` returns.
- If initial drain or direct calls to `_drain_queue()` need a clean flag, clear it
  in the caller before setting up that path, not at the top of drain.

Before changing this, verify all direct `_drain_queue()` call sites in
`simplebroker/watcher.py`:

- Initial drain in `_run_with_retries()`.
- Main loop through `_process_messages()`.
- Tests or subclasses.

### Invariants

- A successful precheck remains valid during the immediate drain that follows it.
- The flag is always cleared after that drain completes or raises.
- Direct initial drain does not accidentally inherit stale precheck state.

### Gate

```bash
uv run pytest -q -n0 tests/test_watcher_burst_mode.py tests/test_watcher.py
bin/pytest-pg tests/test_watcher_burst_mode.py::test_burst_mode_no_reset_on_empty_wake -n0
```

## Task 6: Tighten simplebroker-pg Dependency Lower Bound

### Goal

Prevent users from installing `simplebroker-pg` with a SimpleBroker core version
that does not provide the lifecycle hooks and shared-session behavior this
extension release relies on.

### Files To Touch

- `extensions/simplebroker_pg/pyproject.toml`
- A packaging metadata test file:
  - prefer `tests/test_release_script.py` if extending release tooling coverage
  - otherwise add `tests/test_packaging_metadata.py`

### Red Test

Add a static metadata test:

1. Parse root `pyproject.toml`.
2. Parse `extensions/simplebroker_pg/pyproject.toml`.
3. Assert the extension dependency list contains:

   ```text
   simplebroker>=3.3.0,<4
   ```

For future-proofing, the test may derive `3.3.0` from the current root project
version if the release policy is that every `simplebroker-pg` release in this
monorepo requires the current core minor. Do not over-generalize if the release
policy is not explicit.

### Implementation

Change:

```toml
"simplebroker>=3.0.0,<4",
```

to:

```toml
"simplebroker>=3.3.0,<4",
```

Do not change psycopg dependency bounds unless a separate test proves they are
wrong.

### Gate

```bash
uv run pytest -q -n0 tests/test_release_script.py
uv run pytest -q -n0 tests/test_packaging_metadata.py
```

Run only the second command if that file exists after implementation.

## Task 7: Optional Documentation Cleanup

### Goal

Make docs match the final lifecycle decision.

### Files To Touch

- `README.md`
- `extensions/simplebroker_pg/README.md`
- `CHANGELOG.md` if the current changelog entry needs correction

### Steps

1. Search:

   ```bash
   rg -n "PostgresRunner|release_thread_connection|shutdown|runner.close|shared" README.md extensions/simplebroker_pg/README.md CHANGELOG.md
   ```

2. If `PostgresRunner.close()` remains destructive:
   - Keep examples that call `runner.close()` for caller-owned runners.
   - Mention `release_thread_connection()` only if there is already an internal
     lifecycle section. It is not a normal user API.
3. If Task 0 produced hard evidence that `close()` must be non-destructive:
   - Update docs to say `shutdown()` closes the pool.
   - Update examples to use `runner.shutdown()` where full cleanup is intended.
   - Explain the compatibility reason in `CHANGELOG.md`.

Do not add a large lifecycle essay. Keep docs scoped to what users need.

### Gate

```bash
uv run pytest -q -n0 tests/test_constants.py tests/test_release_script.py
```

## Task 8: Full Verification

### Goal

Prove the remediation works across SQLite/core and Postgres extension paths.

### Required Commands

Run the targeted PG failures first:

```bash
bin/pytest-pg tests/test_watcher_burst_mode.py::test_burst_mode_no_reset_on_empty_wake -n0
bin/pytest-pg tests/test_watcher.py::TestQueueWatcher::test_since_timestamp_database_filtering -n0
bin/pytest-pg tests/test_watcher_concurrency.py::TestWorkerPool::test_worker_pool_with_slow_handlers -n0
```

Run the listener and lifecycle suites:

```bash
bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py -n0
bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_notify.py -n0
bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_integration.py -n0
```

Run core watcher/session suites:

```bash
uv run pytest -q -n0 tests/test_process_broker_session.py
uv run pytest -q -n0 tests/test_watcher_burst_mode.py tests/test_watcher.py tests/test_watcher_concurrency.py
uv run pytest -q -n0 tests/test_backend_plugin_resolution.py tests/test_custom_runner_integration.py
```

Run the wrapped-runner PG regression introduced by the current working tree:

```bash
bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_integration.py::test_wrapped_postgres_runner_with_resolved_target_uses_pg_waiter -n0
```

Run full gates:

```bash
uv run pytest -q -n0
bin/pytest-pg
uv run ruff check simplebroker extensions/simplebroker_pg tests
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg
git diff --check
```

### Release Gate Invariants

Do not call the work done unless all of these are true:

- Same-target persistent queues still allocate one backend runner/pool per
  process session, not one per queue.
- `Queue.cleanup_connections()` releases one thread checkout without releasing
  the shared session lease.
- Final shared session release shuts down the owned runner exactly once.
- A caller-owned `runner=` remains caller-owned.
- Postgres activity listener registry does not reuse a parent-process listener
  in a child process.
- Postgres activity listener startup errors fail fast.
- Native activity wakeups do not skip DB pending checks.
- Empty native activity wakeups do not reset watcher burst mode.
- Active PG watchers still process backlog promptly.
- The three PG failures listed in Task 4 are green.
- Wrapped non-backend-aware runners with a Postgres `ResolvedTarget` resolve to
  the Postgres backend plugin for activity waiters.
- `simplebroker-pg` declares a SimpleBroker lower bound that includes the core
  lifecycle behavior it requires.

## Fresh-Eyes Review Checklist

After implementation and before finalizing, review the diff from scratch.

Ask these questions in order:

1. Did any core file import or name `simplebroker_pg`? If yes, remove it.
2. Did any core watcher branch special-case Postgres by backend name? If yes,
   replace it with generic `ActivityWaiter` semantics.
3. Does a native activity wake still have any path that returns `True` from
   `_has_pending_messages()` without touching the database? If yes, the main bug
   remains unless there is a dedicated race test proving the shortcut is safe.
4. Does `notify_activity()` run only after actual message progress? If no, idle
   watchers can still enter burst.
5. Does the shared listener registry key include the current pid? If no, fork
   safety is still broken.
6. Does listener construction raise on startup error or timeout? If no, failures
   will still appear as watcher timeouts.
7. Did the fix add sleeps instead of synchronization? If yes, replace them with
   event/polling helpers.
8. Did tests mock out queue state or `has_pending()` so heavily that they no
   longer prove real behavior? If yes, rewrite them around real queues.
9. Did any change increase default Postgres pool size? If yes, revert it.
10. Did any change make ordinary consuming watchers drain multiple messages per
    non-batch pass? If yes, revert it unless a separate design decision approves
    that semantic change.
11. Does the package metadata prevent incompatible core/extension installs? If
    no, fix the dependency lower bound.
12. If `requires_pending_check()` remains, does the test suite cover direct
    consumers and other processes racing with a native wake? If no, remove or
    ignore the hook for now.

If answering these questions pulls the implementation toward a materially
different design, stop and report the conflict instead of continuing.

## Expected Final Shape

At the end, the implementation should look boring:

- `simplebroker_pg.runner` owns PG listener details and pid-safe listener
  registry keys.
- `simplebroker.watcher` treats all native activity waiters as generic hints.
- Burst reset remains tied to actual queue progress.
- Core shared-session lifecycle uses `release_thread_connection()` for
  per-operation cleanup and `shutdown()` for owned-runner teardown.
- Packaging metadata requires the matching SimpleBroker core release.

Anything more elaborate is probably a design drift.

## Plan Self-Review

This section is the required fresh-eyes review of the plan itself.

### Ambiguity Found: `PostgresRunner.close()` Semantics

The initial review finding said to keep `PostgresRunner.close()` non-destructive.
That conflicts with the current `SQLRunner` protocol and docs. The plan now
resolves that by treating the actual invariant as "SimpleBroker internals must
not accidentally call destructive close for per-thread release." This avoids
moving the code away from the documented runner contract.

### Ambiguity Found: PG-Specific Logic Placement

The native wake bug is visible only with Postgres, but the core bug is generic:
`ActivityWaiter.wait(True)` was treated as proof of queue work. The plan now
keeps PG-specific listener code in `simplebroker_pg` and limits core changes to
the generic ActivityWaiter semantics.

### Ambiguity Found: Queue Refcount Optimization

The current working tree adds `requires_pending_check()` based on listener queue
refcount. That can be a useful future optimization, but it is not a correctness
proof because it does not account for non-watcher consumers or other processes.
The plan now requires either stronger race tests or removing/ignoring the hook
until the optimization is proven safe.

### Ambiguity Found: Red Tests For Private State

Fork safety and listener startup are private lifecycle bugs. The plan allows
small fakes only for those hard seams, and requires real PG watcher tests for
the behavior users observe. This avoids over-mocking the main queue semantics.

### Bad Direction Avoided: Increasing Pool Size

The failures are missed-wakeup/state-machine problems, not insufficient pool
capacity. The plan explicitly forbids increasing pool size as a fix.

### Bad Direction Avoided: Batching Consuming Watchers

Processing more messages per pass might hide slow PG tests, but it changes
crash-loss and fairness behavior. The plan keeps consuming watcher semantics
unchanged unless `batch_processing=True`.

### Final Re-Review Result

The plan remains aligned with the original issue: same-process shared backend
resources, one PG listener per process/target/schema, and correct watcher wake
semantics. It does not require Weft changes and does not introduce a broader
abstraction than the existing `ActivityWaiter` and runner lifecycle hooks.
