# PG Watcher Follow-Up Review Remediation Plan

## Purpose

This document is the implementation plan for three fresh-eyes review findings in the current SimpleBroker / `simplebroker_pg` work:

1. Shared Postgres activity listeners leak per-queue state.
2. Postgres native notifications bypass the pending-message check.
3. Explicit runner plugin resolution ignores `ResolvedTarget` backend context for non-backend-aware wrapped runners.

Assume the implementer is technically skilled but has no context for this repository and may over-engineer or over-mock if not constrained. Follow the tasks in order. Keep the changes small. Use red-green TDD. Do not redesign the watcher model, backend plugin system, or Postgres notification schema.

The goal is to fix the three concrete issues while preserving the current reliability tradeoff: watchers may be a bit slower, but consuming mode should still process at most one message per ordinary pass to limit crash-loss exposure and preserve fairness.

## Scope Locks

### In Scope

- Postgres shared listener resource cleanup.
- Watcher interpretation of backend-native activity hints.
- Explicit runner backend plugin resolution when a `ResolvedTarget` is also available.
- Regression tests that prove the above behavior with real `Queue`, `QueueWatcher`, `BrokerCore`, and Postgres extension objects where practical.

### Out Of Scope

- Do not grow the Postgres pool to hide lifecycle bugs.
- Do not change the public queue or watcher API.
- Do not add new environment variables or config knobs.
- Do not add a new backend registry or dependency injection layer.
- Do not change message delivery semantics.
- Do not batch normal consuming watchers to improve speed.
- Do not make Postgres notifications durable. `LISTEN/NOTIFY` remains a wake-up optimization, not a source of truth.

### Reliability Lock

For consuming watchers (`peek=False`):

- One normal watcher pass may claim at most one message unless `batch_processing=True`.
- A native notification may wake the watcher.
- A native notification must not by itself prove that a message is still pending.
- If two watchers listen on the same queue, one watcher must not blindly claim because the other watcher may have consumed the notified message first.

For peek watchers (`peek=True`):

- It is acceptable to use a same-watcher local backlog hint to avoid repeated pre-checks, because peek does not remove messages.
- The watcher still advances `_last_seen_ts` only after successful handler dispatch.

## Repository Primer

### Runtime Files

- `simplebroker/watcher.py`
  - `PollingStrategy` handles wait/backoff/burst behavior.
  - `QueueWatcher._process_messages()` does wait, optional pending check, then drain.
  - `QueueWatcher._has_pending_messages()` decides whether a drain should happen.
  - `QueueWatcher._drain_queue()` dispatches to peek or consume mode and informs the strategy when progress happened.

- `simplebroker/sbqueue.py`
  - `Queue.create_activity_waiter()` chooses the backend plugin and asks it for a native activity waiter.
  - This method must use the same backend resolution logic as `DBConnection` for runner-backed queues.

- `simplebroker/db.py`
  - `DBConnection` chooses the backend plugin for `BrokerCore`.
  - `_BorrowedRunner` wraps caller-owned runners and should preserve backend identity without taking ownership.

- `simplebroker/_backend_plugins.py`
  - `BackendAwareRunner` is the protocol for runners that can declare their backend plugin.
  - `resolve_runner_backend_plugin()` currently defaults non-aware runners to SQLite.

- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
  - `PostgresRunner` owns the psycopg pool.
  - `PostgresActivityWaiter` is the watcher-facing `LISTEN/NOTIFY` waiter.
  - `_SharedActivityListener` is the process-local listener shared across waiters for one DSN/schema.

- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
  - `PostgresBackendPlugin.create_activity_waiter()` builds `PostgresActivityWaiter`.

### Test Files To Read First

- `tests/test_watcher_thundering_herd.py`
- `tests/test_watcher_race_conditions.py`
- `tests/test_watcher_burst_mode.py`
- `tests/test_watcher_concurrency.py`
- `tests/test_process_broker_session.py`
- `tests/test_backend_plugin_resolution.py`
- `tests/test_custom_runner_integration.py`
- `extensions/simplebroker_pg/tests/test_pg_notify.py`
- `extensions/simplebroker_pg/tests/test_pg_integration.py`
- `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py`

### Tooling

Use commands from the repo root:

- SQLite/local targeted tests:
  - `uv run pytest -q -n0 tests/test_watcher_race_conditions.py tests/test_watcher_thundering_herd.py`
  - `uv run pytest -q -n0 tests/test_backend_plugin_resolution.py tests/test_custom_runner_integration.py tests/test_process_broker_session.py`
- Postgres targeted tests:
  - `uv run ./bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_notify.py -q`
  - `uv run ./bin/pytest-pg tests/test_watcher_race_conditions.py::test_concurrent_pre_checks tests/test_watcher_race_conditions.py::test_pre_check_database_contention -q`
  - `uv run ./bin/pytest-pg tests/test_watcher_thundering_herd.py tests/test_watcher_metrics.py tests/test_watcher_burst_mode.py tests/test_watcher_race_conditions.py tests/test_watcher_concurrency.py -q`
- Full gates:
  - `uv run pytest -q -n0`
  - `uv run ./bin/pytest-pg`
  - `uv run ruff check simplebroker extensions/simplebroker_pg tests examples`
  - `uv run mypy simplebroker extensions examples`
  - `git diff --check`

Use `-n0` while debugging SQLite tests. Use `pytest-pg` for Postgres behavior; do not simulate PG watcher behavior with SQLite-only tests.

## Engineering Rules

### Red-Green TDD

For each finding:

1. Add or strengthen a test that fails on the current code.
2. Run the targeted test and confirm it fails for the intended reason.
3. Make the smallest code change to pass.
4. Refactor only after the test is green.

Do not skip the red step. The current bug set is about subtle lifecycle and race behavior. Tests that only assert “method was called” are not enough.

### Test Design Rules

Prefer real behavior over mocks:

- Use real `Queue`, `QueueWatcher`, `BrokerCore`, and `PostgresRunner` where possible.
- Use `pytest-pg` for Postgres listener and notification behavior.
- Small internal fakes are acceptable only for narrow unit tests around backend plugin resolution or lifecycle seams, and only when paired with integration coverage for the same production contract.
- Inspecting private listener state is acceptable for the listener cleanup regression because the leak is private state. Keep that test isolated to `extensions/simplebroker_pg/tests/`.

Avoid:

- Mocking `Queue.has_pending()` to prove watcher correctness.
- Mocking psycopg notifications for the main PG behavior tests.
- Adding sleeps as assertions. Use polling helpers such as `wait_for_condition()` when waiting is unavoidable.
- Raising timeouts to make tests pass. If a test only passes with a longer timeout, find the resource or state-machine issue.

### Code Style

- Keep helper methods private unless a public contract already exists.
- Avoid new abstractions unless they remove real duplication between `DBConnection` and `Queue.create_activity_waiter()`.
- Use clear names over clever state flags.
- Do not add comments that narrate obvious code. Add comments only where a concurrency invariant would be hard to infer.
- Keep imports local only when they avoid a real cycle or heavyweight dependency.

## Finding 1: Shared PG Listener Leaks Per-Queue State

### Current Problem

`_SharedActivityListener` stores:

- `_conditions: defaultdict[str, threading.Condition]`
- `_versions: defaultdict[str, int]`

Current behavior creates or retains entries for queue names seen in waits or notifications. Closing a `PostgresActivityWaiter` only decrements the listener-level registry refcount. It does not unregister the individual queue. A long-running process that watches many dynamic queues can retain state for every queue until the last waiter for the entire DSN/schema closes.

### Target Design

Add queue-level registration to `_SharedActivityListener`.

Implement a small internal contract:

- `register_queue(queue_name: str) -> tuple[int, int]`
  - increments a per-queue waiter refcount
  - creates the queue condition if needed
  - returns the current queue version and current wildcard version
- `unregister_queue(queue_name: str) -> None`
  - decrements the per-queue waiter refcount
  - when the refcount reaches zero:
    - remove the queue condition
    - remove the queue version
    - remove the refcount entry
    - notify the condition before dropping it so any waiting thread exits promptly

Replace `defaultdict` with plain dicts so reads do not accidentally create entries.

Use a separate `_wildcard_version: int` instead of storing `"*"` in `_versions`. Wildcard is not a real queue and should not participate in queue cleanup.

When the listener receives a notification:

- If payload is `"*"`:
  - increment `_wildcard_version`
  - notify all currently registered queue conditions
- If payload is a queue with active waiters:
  - increment that queue version
  - notify that queue's condition
- If payload is a queue with no active waiters:
  - ignore it
  - do not create condition or version state

This is correct because `LISTEN/NOTIFY` is only a wake-up hint. A watcher created after a write must discover existing backlog through initial drain / pending checks, not through retained notification history.

### Files To Touch

- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `extensions/simplebroker_pg/tests/test_pg_notify.py`
- `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py`

### Task 1.1: Add a Failing Listener Cleanup Test

Add a PG-only test in `extensions/simplebroker_pg/tests/test_pg_notify.py`.

Suggested test shape:

1. Create one `PostgresRunner` and two persistent `Queue` objects for the same runner/schema:
   - `Queue("jobs", runner=runner, persistent=True)`
   - `Queue("other", runner=runner, persistent=True)`
2. Create waiters for both queues with a shared `threading.Event`.
3. Access the private listener through the waiter. This is acceptable here because the bug is private resource retention.
4. Assert:
   - listener has registered `jobs`
   - listener has registered `other`
   - queue refcounts are correct
5. Close the `jobs` waiter.
6. Assert:
   - `jobs` is no longer present in queue condition/version/refcount state
   - `other` is still present
7. Close the `other` waiter.
8. Assert:
   - no registered queues remain

Also test same-queue refcount:

1. Create two waiters for `"jobs"`.
2. Close one waiter.
3. Assert `"jobs"` remains registered with refcount 1.
4. Close the second waiter.
5. Assert `"jobs"` is removed.

Keep helper inspection local to the test file. Do not expose public production APIs just for tests.

Expected red failure on current code: no queue unregister behavior exists; private state remains or no refcount exists.

### Task 1.2: Implement Queue Registration

In `_SharedActivityListener`:

- Add `_queue_refcounts: dict[str, int]`.
- Change `_conditions` and `_versions` to plain dicts.
- Add `register_queue()` and `unregister_queue()`.
- Update `wait()` to assume the queue was registered. If the queue is missing because of a programming error or concurrent close, return `False` with current known versions rather than creating new state.

In `PostgresActivityWaiter.__init__()`:

- After acquiring the shared listener, call `register_queue(queue_name)`.
- Store the returned versions in `_last_queue_version` and `_last_wildcard_version`.
- This prevents a newly-created waiter from replaying stale notifications that happened before it existed.

In `PostgresActivityWaiter.close()`:

- Call `listener.unregister_queue(queue_name)` before releasing the listener from `_activity_registry`.
- Keep close idempotent.

### Task 1.3: Add No-State-On-Inactive-Notification Test

Add a test that proves notifications for queues without active waiters do not grow listener state.

Suggested shape:

1. Create a waiter for queue `"jobs"`.
2. Write to another queue `"noise"` using a separate queue/core in the same schema.
3. Write to `"jobs"` and wait until the registered `"jobs"` version advances. This proves the listener is actively receiving notifications.
4. Assert listener state still does not include `"noise"`.

Do not make this test timing fragile. Use a small wait loop around the known active `"jobs"` notification. Do not use a fixed sleep as the proof that the listener processed `"noise"`; the assertion should be that processing a later known active notification did not leave inactive queue state behind.

### Task 1.4: Move Runner Lifecycle Seam Tests Into The PG Suite

Keep the existing narrow seam tests in `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py`, including the fake-pool test for direct branch coverage of `release_thread_connection()` and `close()`.

But move the file under the `pytest-pg` extension suite:

- add `pytestmark = [pytest.mark.pg_only]` to the file
- run it through `uv run ./bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py -q`

This is intentional. The fake-pool test is acceptable as a tiny seam test, but it should live with the Postgres extension gate because the behavior is Postgres-runner lifecycle behavior.

### Task 1.5: Add True Postgres Runner Lifecycle Integration Tests

Add real integration coverage in `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py` using a real `PostgresRunner`, real psycopg pool, and the test Postgres DSN.

Use existing helper patterns from `extensions/simplebroker_pg/tests/test_pg_integration.py`:

- require the test DSN from the environment
- create a unique schema name
- clean up the schema through the plugin in `finally`

Required tests:

1. `release_thread_connection()` returns a real checked-out pool connection without closing the pool.
   - Create `runner = PostgresRunner(dsn, schema=schema)`.
   - Force an idle checked-out real connection with the runner's private `_get_thread_conn()` seam.
   - Do not use `begin_immediate()` just to hold a connection. Releasing a transaction-open connection would test rollback/pool cleanup side effects, not the intended watcher-idle release path.
   - Call `runner.release_thread_connection()`.
   - Run `runner.run("SELECT 1", fetch=True)` again.
   - Assert the second query succeeds.
   - If psycopg pool exposes stable stats in this version, you may assert stats too, but do not depend on unstable internals.

2. `close()` closes the real pool.
   - Create `runner = PostgresRunner(dsn, schema=schema)`.
   - Run `runner.run("SELECT 1", fetch=True)` once.
   - Call `runner.close()`.
   - Assert a later `runner.run("SELECT 1", fetch=True)` raises an exception.
   - The exact exception type can come from psycopg_pool, so keep the assertion broad enough to be stable but narrow enough to prove the pool is unusable.

3. `run_exclusive_setup()` still runs an operation once.
   - Keep the existing seam/unit-style test if it is simpler.
   - Do not force this test to hit Postgres unless doing so simplifies the file.

Do not delete the fake-pool seam tests. The point is layered evidence:

- fake-pool seam tests cover direct lifecycle branches cheaply
- real PG integration tests prove those branches match the actual pool behavior

Important test-design note: ordinary `runner.run(..., fetch=True)` is not enough to prove `release_thread_connection()` returned a checked-out connection because `run()` auto-returns connections outside an explicit transaction. The integration test may use `_get_thread_conn()` here because this file is specifically about runner lifecycle seams, and the same behavior is also covered against the real psycopg pool.

### Invariants For Finding 1

- Closing the last waiter for one queue removes that queue's listener state without closing the shared schema listener if other queues still have waiters.
- Closing one of multiple waiters for the same queue does not remove that queue's listener state.
- Notifications for inactive queues do not create retained state.
- Wildcard notifications still wake all currently active queue waiters.
- Listener close remains idempotent and does not hang if called while a waiter is blocked.

## Finding 2: Native Notifications Bypass Pending Check

### Current Problem

`QueueWatcher._has_pending_messages()` treats a backend-native activity hint as proof of pending work:

```python
if self._strategy.consume_native_activity_hint():
    return True
```

That is too strong. A Postgres notification means “something relevant may have happened.” It does not guarantee that this watcher can still claim or peek a message:

- another watcher on the same queue may have consumed it
- the notification may be stale relative to this watcher
- the notification may be replayed from a shared listener version counter
- a move/broadcast/wildcard notification may wake multiple watchers

The correct source of truth is the database pending check or the actual one-message read/peek. Do not trust `NOTIFY` as a delivery contract.

### Target Design

Separate “wake the watcher” from “skip the pending-message check.”

Recommended model:

- `PollingStrategy.wait_for_activity()` may set a native activity flag when the backend waiter returns true.
- `QueueWatcher._has_pending_messages()` may consume that native flag for instrumentation or state accounting, but it must still run `Queue.has_pending()` before returning true.
- Same-watcher local hints are separate:
  - A local immediate-wake hint is allowed after a successful drain so the loop does not sleep before checking for more visible backlog.
  - A local skip-precheck hint is allowed only in narrow cases where the plan explicitly accepts a direct read/peek attempt.

For consuming mode, prefer:

- after a successful consume, wake immediately
- then run the pending check again before the next consume
- keep one-message-per-pass

For startup backlog in consuming mode:

- do not depend on new PG notifications because messages may have been written before the watcher started
- use an immediate local wake after a successful initial consume so the watcher can run another pending check promptly

For peek mode:

- it is acceptable to skip the DB pending check after a successful peek and attempt the next one-message peek directly
- an empty peek is not destructive
- this preserves the performance improvement for `since_timestamp` peek watchers

### Files To Touch

- `simplebroker/watcher.py`
- `tests/test_watcher_race_conditions.py`
- `tests/test_watcher_thundering_herd.py`
- `tests/test_watcher_burst_mode.py`
- `tests/test_watcher.py`
- Possibly `tests/test_watcher_metrics.py`

### Task 2.1: Add Same-Queue Native Notification Regression Test

Add a Postgres-shared test that proves native notifications do not cause blind drains.

Suggested location: `tests/test_watcher_race_conditions.py` or `tests/test_watcher_thundering_herd.py`.

Suggested shape:

1. Create two instrumented consuming watchers on the same queue.
2. Track:
   - handler call counts
   - pending check count
   - drain count
   - empty drain count if the test subclass can observe it without mocking DB internals
3. Write one message to the queue.
4. Wait until exactly one handler call happens.
5. Assert:
   - total processed messages is 1
   - at least one native wake may have occurred, but only one watcher should perform a successful drain
   - the non-consuming watcher does not blindly claim an empty message just because it saw the notification

If existing instrumentation cannot observe empty drains cleanly, add a small subclass that increments a counter when `_drain_queue()` is called and no dispatch happened. Do not mock `Queue.has_pending()`.

Expected red failure on current code: a watcher that consumed a native hint can proceed to drain without a live pending check.

### Task 2.2: Change Native Hint Semantics

In `PollingStrategy`:

- Rename or document native activity methods so their meaning is clear:
  - `consume_native_activity_hint()` should mean “a native wake happened”
  - it must not mean “messages are pending”
- Consider a name like `consume_native_wake_hint()` if it makes the call site safer.

In `QueueWatcher._has_pending_messages()`:

- Remove the early `return True` for native activity.
- If a native wake hint exists, consume it and continue to the normal `Queue.has_pending()` check.
- Preserve `_pending_messages_precheck_confirmed` behavior for the double-precheck case inside one drain pass.

Avoid a broad rewrite of `PollingStrategy`. This task is about making the source of truth explicit, not changing the whole backoff algorithm.

### Task 2.3: Rework Local Activity Hints Conservatively

Review the current local hint flags. The implementation should have clear, separate meanings:

- “wake immediately without sleeping”
- “skip the pending check and try one peek/read directly”
- “a DB pending check already happened in this same pass”

Do not use one flag for all three meanings.

Recommended behavior:

- Native wake:
  - wake immediately
  - still run DB pending check
- Consuming mode after successful drain:
  - wake immediately
  - still run DB pending check
- Consuming mode after a DB pending check already returned true in the same pass:
  - skip duplicate pending check for that exact pass only
- Peek mode after successful dispatch:
  - may skip the next pending check and attempt one more peek directly

If you keep the existing flag names, add a short comment near each flag definition explaining the invariant. If the names are misleading, rename them.

### Task 2.4: Preserve Startup Backlog and Since-Timestamp Behavior

Add or keep targeted tests for:

- a consuming watcher started after messages already exist drains the startup backlog promptly
- a peek watcher with `since_timestamp` processes all newer messages promptly
- two active queues do not wake unrelated queue watchers into burst mode
- multiple queues under concurrent writes drain fully under PG xdist

Use existing tests where possible. Do not create duplicate test files if a small extension to an existing test covers the invariant.

### Invariants For Finding 2

- A Postgres native notification wakes relevant watchers but does not bypass the live pending check in consuming mode.
- Consuming mode still claims at most one message per non-batch pass.
- Startup backlog does not wait for future notifications.
- Peek mode remains fast enough for existing `since_timestamp` tests.
- Idle watchers for unrelated queues do not reset burst mode because another queue received a notification.
- The PG pool size remains unchanged.

## Finding 3: Explicit Runner Plugin Resolution Drops Target Context

### Current Problem

When `DBConnection` receives both:

- `runner=<custom runner>`
- `db_path=<ResolvedTarget for postgres>`

it resolves the backend plugin from the runner only. If the runner implements `BackendAwareRunner`, this works. If the runner is a wrapper around `PostgresRunner` but does not implement `BackendAwareRunner`, resolution falls back to SQLite and ignores the `ResolvedTarget` plugin.

The same risk exists in `Queue.create_activity_waiter()`: explicit runner handling can choose SQLite unless the runner itself is backend-aware.

This creates a footgun for extension authors who wrap runners for logging, tracing, metrics, retries, or tests.

### Target Design

Use one clear backend resolution order for runner-backed paths:

1. If the runner implements `BackendAwareRunner`, use `runner.backend_plugin`.
2. Else if `db_path` is a `ResolvedTarget`, use `db_path.plugin`.
3. Else default to SQLite.

This order keeps explicit backend-aware runners authoritative while still preserving useful target context for simple wrappers.

Do not add a public `backend_plugin=` argument to `Queue` or `DBConnection` in this plan. That is a larger API design decision and is not needed to fix this finding.

### Files To Touch

- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `simplebroker/_backend_plugins.py` only if adding a small helper avoids duplicated resolution logic
- `tests/test_backend_plugin_resolution.py`
- `tests/test_custom_runner_integration.py`
- `extensions/simplebroker_pg/tests/test_pg_integration.py`
- `extensions/simplebroker_pg/tests/test_pg_notify.py`

### Task 3.1: Add Backend Resolution Unit Tests

Add tests that prove the resolution order.

Suggested tests:

1. Non-aware runner plus SQLite path defaults to SQLite.
2. Backend-aware runner plus conflicting target uses the runner plugin.
3. Non-aware runner plus `ResolvedTarget` uses the target plugin.

Use small local test runners/plugins. Do not hit Postgres for these pure resolution tests.

Good test shape:

- Create a `RecordingRunner` that implements `SQLRunner` but not `BackendAwareRunner`.
- Create a `ResolvedTarget` with a counting/recording plugin.
- Build a `DBConnection(target, runner=recording_runner)`.
- Assert the resulting `BrokerCore` uses the target plugin. If direct access to `_backend_plugin` is needed, keep that assertion local and explicit.

### Task 3.2: Refactor Plugin Resolution To One Helper

Avoid duplicating resolution logic in `DBConnection` and `Queue.create_activity_waiter()`.

Add a private helper in the most appropriate existing module. Preferred options:

- `simplebroker/_backend_plugins.py`
  - if the helper can stay generic and avoid importing `ResolvedTarget` at runtime
- `simplebroker/db.py`
  - if the helper is DBConnection-specific
- `simplebroker/sbqueue.py`
  - only if the helper is trivial and not reused

Recommended helper shape:

```python
def resolve_runner_or_target_backend_plugin(
    runner: SQLRunner | None,
    resolved_target: ResolvedTarget | None,
) -> BackendPlugin:
    if runner is not None and isinstance(runner, BackendAwareRunner):
        return runner.backend_plugin
    if resolved_target is not None:
        return resolved_target.plugin
    if runner is not None:
        return get_backend_plugin("sqlite")
    ...
```

Keep the helper private unless there is already a public need. Ensure it validates `plugin.sql` the same way existing resolver paths do.

Update:

- `DBConnection.__init__()`
- `_BorrowedRunner.backend_plugin`
- `Queue.create_activity_waiter()`

For `_BorrowedRunner`, pass the resolved plugin into the wrapper so `runner.backend_plugin` remains meaningful even for non-aware wrappers with a target context.

### Task 3.3: Add PG Wrapped Runner Integration Test

Add a PG-only integration test that catches the original failure mode.

Suggested location: `extensions/simplebroker_pg/tests/test_pg_integration.py` or `extensions/simplebroker_pg/tests/test_pg_notify.py`.

Test shape:

1. Define a small wrapper around `PostgresRunner`:
   - delegates `run`, `begin_immediate`, `commit`, `rollback`, `setup`, `is_setup_complete`, and any needed attributes
   - intentionally does **not** expose `backend_plugin`
   - delegates `dsn` and `schema` through `__getattr__` or explicit properties if the PG plugin requires them
2. Build a `ResolvedTarget` for the same DSN/schema.
3. Use `Queue("jobs", db_path=target, runner=wrapped_runner, persistent=True)`.
4. Assert:
   - `queue.create_activity_waiter(stop_event=...)` returns a `PostgresActivityWaiter`, not `None`
   - `queue.write()` and `queue.read()` or watcher wakeup works end-to-end

This test should fail on the current code because the queue will fall back to SQLite plugin activity-waiter behavior for the wrapped runner.

### Invariants For Finding 3

- Direct `PostgresRunner` still resolves to the Postgres plugin.
- Wrapped Postgres runner plus Postgres `ResolvedTarget` resolves to the Postgres plugin.
- Wrapped runner without target context still defaults to SQLite for backward compatibility.
- Backend-aware runner remains authoritative.
- Injected runners remain caller-owned. `Queue.close()` must not close the supplied runner.

## Suggested Implementation Order

### Task A: Listener Cleanup

1. Add listener queue-refcount tests.
2. Implement `register_queue()` / `unregister_queue()`.
3. Update `PostgresActivityWaiter` to register on init and unregister on close.
4. Move `test_pg_runner_lifecycle.py` under the `pg_only` marker and add real PG lifecycle integration tests.
5. Run:
   - `uv run ./bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_notify.py -q`
   - `uv run ./bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py -q`

### Task B: Native Hint Semantics

1. Add same-queue multi-watcher regression test.
2. Change native hint handling so it wakes but does not skip `Queue.has_pending()` in consuming mode.
3. Clarify local hint names or comments.
4. Run:
   - `uv run ./bin/pytest-pg tests/test_watcher_race_conditions.py::test_concurrent_pre_checks tests/test_watcher_race_conditions.py::test_pre_check_database_contention -q`
   - `uv run ./bin/pytest-pg tests/test_watcher_thundering_herd.py tests/test_watcher_metrics.py tests/test_watcher_burst_mode.py tests/test_watcher_race_conditions.py tests/test_watcher_concurrency.py -q`
   - `uv run pytest -q -n0 tests/test_watcher_thundering_herd.py tests/test_watcher_race_conditions.py tests/test_watcher_burst_mode.py`

### Task C: Runner/Target Plugin Resolution

1. Add pure unit tests for resolution order.
2. Add PG wrapped-runner integration test.
3. Implement one shared helper or carefully duplicated minimal logic if a helper would add import cycles.
4. Run:
   - `uv run pytest -q -n0 tests/test_backend_plugin_resolution.py tests/test_custom_runner_integration.py tests/test_process_broker_session.py`
   - `uv run ./bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_integration.py extensions/simplebroker_pg/tests/test_pg_notify.py -q`

### Task D: Full Gates

Run all of these before considering the work done:

- `uv run ruff check simplebroker extensions/simplebroker_pg tests examples`
- `uv run mypy simplebroker extensions examples`
- `uv run pytest -q -n0`
- `uv run ./bin/pytest-pg`
- `git diff --check`

If `pytest-pg` fails only under xdist, do not mark it flaky without root cause. The reported failures in this area have been real resource-model issues.

## Manual Review Checklist

Before finalizing the implementation, review the diff with these questions:

- Did any code path increase the Postgres pool size? If yes, revert it.
- Does any consuming watcher skip a live pending check solely because of a native notification? If yes, fix it.
- Can `_SharedActivityListener` retain a queue name after the last waiter for that queue closes? If yes, fix it.
- Can a notification for an inactive queue create retained listener state? If yes, fix it.
- Does a wrapped Postgres runner with a Postgres `ResolvedTarget` get a Postgres activity waiter? If no, fix it.
- Does `Queue.close()` still avoid closing caller-owned injected runners? If no, fix it.
- Are tests using real queue/backend behavior rather than over-mocking? If not, rewrite them.
- Is the watcher state machine simpler or at least more explicit than before? If names are ambiguous, rename or add a short invariant comment.

## Known Traps

- `LISTEN/NOTIFY` is not a durable queue. Do not retain notification versions for queues with no active waiters to “avoid missing messages.” Initial drain and pending checks are responsible for already-visible messages.
- Postgres sends notifications after transaction commit. Tests that expect immediate notification delivery must wait with a condition, not a fixed sleep.
- A native wake is not a pending-message proof. The database is the source of truth.
- A same-watcher local hint is not the same as a backend-native wake. Keep those states separate.
- Wrapped runners are common in examples and extensions. Do not require every wrapper to implement `BackendAwareRunner` if the caller already supplied a `ResolvedTarget`.
- Private state assertions are acceptable for listener cleanup tests, but only because the bug is private memory retention. Do not spread private assertions into broad behavior tests.

## Fresh-Eyes Self Review

After drafting this plan, I reviewed it against the warning not to drift into a materially different direction.

### Ambiguity Fixed: Listener Version Initialization

A new waiter should initialize its last-seen versions from the listener's current versions. Otherwise it can replay old notifications that happened before it existed. This is now explicit in Task 1.2.

### Ambiguity Fixed: Inactive Queue Notifications

The plan now says to ignore notifications for queues with no active waiters. That is correct because notifications are hints, not storage. Backlog discovery belongs to initial drain and pending checks.

### Ambiguity Fixed: Native Wake Versus Pending Proof

The plan distinguishes:

- native wake hint
- local immediate wake
- local skip-precheck hint
- same-pass precheck confirmation

That prevents a recurrence of the current over-loaded flag behavior.

### Ambiguity Fixed: Runner Resolution Order

The resolution order is now explicit: backend-aware runner wins, then `ResolvedTarget`, then SQLite fallback. This preserves explicit runner authority while fixing wrapped-runner target context.

### Decision Updated: Lifecycle Tests Under Pytest-PG

The plan now keeps the existing fake-pool seam test, moves it under the `pg_only` extension suite, and requires real `PostgresRunner` integration tests in the same file. This aligns with the repository preference against mocks while preserving cheap branch coverage for lifecycle edge paths.

### Scope Check

The plan does not:

- grow the pool
- add public API
- change notification channel naming
- redesign `SQLRunner`
- batch consuming watchers
- add durable notification semantics

The plan remains materially aligned with the three review findings.

## Definition Of Done

The work is done only when:

- all three findings have a red-green regression test
- targeted SQLite and PG tests pass
- full SQLite and PG gates pass
- lint, type-check, and `git diff --check` pass
- the final diff does not introduce unrelated refactors
