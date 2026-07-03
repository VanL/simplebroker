# Process-Local Broker Session Sharing Plan

## Purpose

This document is the implementation plan for fixing persistent queue connection
fan-out in SimpleBroker.

The concrete problem:

- A Weft `MultiQueueWatcher` watches `N` queues.
- Weft creates `N` SimpleBroker `Queue` objects.
- Today, each persistent `Queue` owns its own `DBConnection`.
- For Postgres, each backend-owned `DBConnection` can create its own runner and
  pool.
- This scales backend pool allocation with queue count and can hit server-side
  Postgres connection limits.

The desired invariant:

```text
Multiple persistent Queue handles for the same resolved backend target in the
same process should not allocate one backend runner/pool per queue.
```

This is a SimpleBroker ownership issue. Weft exposes it because
`MultiQueueWatcher` naturally creates many queue handles, but Weft should not
need to know backend pool construction rules.

Assume the implementer is technically strong but new to this repository. Also
assume they tend to over-mock tests and over-design APIs unless constrained.
Follow this plan in order. Keep every step small. Use red-green TDD.

## Locked Decisions

These are decisions, not open questions. Do not revisit them while implementing
unless a test proves this plan is wrong.

### Scope lock

- Implement the fix in SimpleBroker first.
- Do not solve this by special-casing Weft.
- Do not require Weft callers to pass an injected Postgres runner.
- Do not introduce a broad dependency-injection framework.
- Do not add new runtime dependencies.
- Do not change queue message semantics, ordering, or SQL shapes unless a
  failing test proves the session-sharing change requires it.

### Public API lock

The first implementation should preserve the existing `Queue` API.

- `Queue("name", db_path=target, persistent=True)` should gain process-local
  sharing automatically.
- `Queue(..., runner=runner)` remains caller-owned and should not use the
  process-local registry.
- `Queue(..., persistent=False, runner=None)` should keep the current ephemeral
  "get in, get out" behavior.

Do not add a public `Broker` class in the first implementation. A public broker
facade is a reasonable future API, but it is not required to make the invariant
true. Adding it now would widen the review surface and blur the red-green loop.

### Ownership lock

The clean internal abstraction is a process-local broker session.

- A `Queue` is a named handle.
- A persistent `Queue` holds a lease on a process-local broker session.
- The session owns backend connection-manager state for one resolved target.
- Multiple persistent queues in the same process and same target share that
  session.
- Different processes never share live session objects.

### Thread lock

Do not share unsafe live DB handles across threads.

The target state is:

- same process, same resolved target, same thread: persistent queue handles can
  reuse the same backend session/core state
- same process, same resolved target, different threads: preserve the existing
  thread isolation model
- different processes: recreate all live handles in the child process
- different resolved targets or backend options: do not share
- injected runners: do not share through the registry
- ephemeral queues: do not share through the registry

This distinction matters for SQLite. SQLite does not have a server-side
connection limit, but its connections are still file handles with lock and cache
state. The SQLite target should improve from "one runner/connection per queue in
one watcher thread" to "one runner/connection per target per thread."

### Lifecycle lock

Do not make `Queue.cleanup_connections()` release a queue's session lease.

Current watcher code uses cleanup during stop/error recovery. That operation
should close or recycle active handles, but the queue object should remain
usable afterward. Releasing the lease belongs to `Queue.close()` and finalizer
paths.

If this distinction is unclear, stop and reread `simplebroker/watcher.py`,
especially `BaseWatcher._perform_stop()` and `BaseWatcher.run_forever()`.

## Repository Primer

### Project shape

- Runtime code: `simplebroker/`
- Core tests: `tests/`
- Built-in SQLite backend: `simplebroker/_backends/sqlite/`
- Built-in SQLite runner: `simplebroker/_runner.py`
- Postgres extension: `extensions/simplebroker_pg/`
- Planning docs: `docs/plans/`

### Files to read before coding

Read these in order:

- `pyproject.toml`
- `simplebroker/sbqueue.py`
- `simplebroker/db.py`
- `simplebroker/_runner.py`
- `simplebroker/_targets.py`
- `simplebroker/_backend_plugins.py`
- `simplebroker/_backends/sqlite/plugin.py`
- `simplebroker/_backends/sqlite/runtime.py`
- `simplebroker/watcher.py`
- `tests/test_queue_connection_manager.py`
- `tests/test_connection_config.py`
- `tests/test_backend_plugin_resolution.py`
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `extensions/simplebroker_pg/tests/test_pg_integration.py`
- `../weft/weft/core/tasks/multiqueue_watcher.py`

Why read the Weft file: not to patch it in this SimpleBroker change, but to
understand the real caller that makes this bug visible.

### Current runtime map

Current persistent queue path:

```text
Queue(name, persistent=True)
  -> Queue.conn = DBConnection(...)
  -> DBConnection.get_connection()
  -> DBConnection creates one thread-local BrokerDB or BrokerCore
  -> BrokerDB/BrokerCore owns or wraps a backend runner
```

Current Weft shape:

```text
MultiQueueWatcher(queue_configs=N)
  -> Queue(q1, persistent=True)
  -> Queue(q2, persistent=True)
  -> ...
  -> Queue(qN, persistent=True)
```

That means `N` queue handles become `N` connection managers today.

Target internal shape:

```text
Queue(name, persistent=True)
  -> DBConnection lease
  -> process-local BrokerSessionRegistry
  -> BrokerSession for same resolved target
  -> per-thread BrokerDB/BrokerCore as needed
  -> backend runner/pool allocation no longer scales with queue count
```

## Tooling

Use `uv` from the repository root.

First-time setup:

```bash
uv sync --extra dev
```

Targeted tests while iterating:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py
uv run pytest -q -n 0 tests/test_connection_config.py
uv run pytest -q -n 0 tests/test_backend_plugin_resolution.py
```

SQLite-focused regression suite:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py tests/test_watcher.py tests/test_watcher_cleanup.py
```

Full local gates:

```bash
uv run pytest -q -n 0
uv run ruff check .
uv run mypy simplebroker
```

Use `-n 0` while debugging connection/session behavior. Parallel pytest output
obscures lifecycle failures.

Postgres extension tests require a configured Postgres DSN. If available, run:

```bash
export BROKER_TEST_BACKEND=postgres
export SIMPLEBROKER_PG_TEST_DSN="postgresql://..."
uv run pytest -q -n 0 extensions/simplebroker_pg/tests
```

Do not block the core SimpleBroker implementation on local Postgres availability.
The red tests for this fix should use an instrumented backend plugin that runs
real SQLite under the SQLRunner interface.

## Engineering Rules

### Red-green TDD

For every behavior change:

1. Write or strengthen a real test first.
2. Run the targeted test and confirm it fails on current code for the expected
   reason.
3. Make the smallest implementation change that turns it green.
4. Refactor only after the behavior is pinned by tests.

Do not implement first and then backfill tests.

### Prefer real tests over mocks

Use real `Queue` objects and real database operations.

Good test tools:

- temporary SQLite files via `tmp_path`
- real `Queue`
- real `ResolvedTarget`
- a test-local backend plugin that delegates to the built-in SQLite backend
- a test-local `CountingSQLiteRunner` subclass only when counting runner
  allocation is the thing under test

Bad test tools:

- mocking `DBConnection.get_connection()`
- mocking `BrokerCore`
- asserting private implementation details unrelated to allocation or lifecycle
- using fake SQL behavior that does not exercise `BrokerCore`

The only acceptable "fake" for the first red test is a thin backend plugin that
uses real SQLite underneath and counts calls to `create_runner()`. That tests
the extension seam without needing a live Postgres server.

### DRY

- Do not duplicate connection lifecycle logic between private and shared paths.
- If two objects need the same target key, write one helper.
- If several tests need the same counting backend, put the helper in one local
  test class or fixture. Do not spread variants across files.
- Keep the registry small. It should not become a generic service locator.

### YAGNI

Do not add:

- a public `Broker` class in this first pass
- an async session API
- connection pool sizing configuration
- per-queue session options
- weak-reference magic beyond what is needed for cleanup
- a background janitor thread
- cross-process sharing

If a proposed change is not needed to satisfy a named invariant below, do not do
it.

## Required Invariants

The implementation is done only when these are true and tested.

1. **Same target sharing:** Multiple persistent `Queue` handles for the same
   resolved backend target in the same process allocate one backend runner/pool
   session, not one per queue.
2. **SQLite thread isolation:** The built-in SQLite backend must not share one
   `sqlite3.Connection` across threads.
3. **Target isolation:** Different targets do not share a session.
4. **Backend option isolation:** Same target string with different backend
   options does not share a session.
5. **Config isolation:** Same target and backend options with materially
   different resolved config does not share a session. Start conservative:
   include the full resolved config fingerprint in the key.
6. **Injected runner isolation:** `Queue(..., runner=runner)` does not use the
   process-local registry and remains caller-owned.
7. **Ephemeral isolation:** `Queue(..., persistent=False, runner=None)` remains
   ephemeral and does not use the process-local registry.
8. **Close semantics:** Closing one queue lease does not close the shared
   session while another queue lease for the same target is still alive.
9. **Final cleanup:** Once the last lease is closed, the shared session shuts
   down owned backend resources.
10. **Fork safety:** A child process must not reuse a parent process session
    object.
11. **Watcher compatibility:** `QueueWatcher` tests continue to pass. Watcher
    stop/error cleanup must not permanently poison the queue handle.

## Task 0: Baseline And Orientation

### Goal

Understand the current ownership model before changing it.

### Files to touch

None.

### Steps

1. Read all files listed in the repository primer.
2. Run:
   - `uv run pytest -q -n 0 tests/test_queue_connection_manager.py`
   - `uv run pytest -q -n 0 tests/test_watcher_cleanup.py`
3. Confirm you can explain:
   - why one persistent `Queue` reuses a connection within one thread
   - why two persistent `Queue` objects for the same target do not share today
   - why Postgres pools make this worse than SQLite
   - why an injected runner is caller-owned and should be excluded

### Gate

Do not code until you can describe the current path in one paragraph:

```text
Queue -> DBConnection -> BrokerDB/BrokerCore -> SQLRunner
```

## Task 1: Add The First Red Test

### Goal

Pin the core bug without mentioning Weft.

### Files to touch

- `tests/test_queue_connection_manager.py`

### Test design

Add a test-local backend plugin that delegates to the SQLite backend but counts
backend runner allocation.

Implementation sketch:

```python
class CountingBackendPlugin:
    name = "counting"
    sql = sqlite_backend.sql
    schema_version = sqlite_backend.schema_version

    def __init__(self):
        self.create_runner_calls = 0

    def create_runner(self, target, *, backend_options=None, config=None):
        self.create_runner_calls += 1
        return SQLiteRunner(target)

    # Delegate all other BackendPlugin methods to sqlite_backend.
```

Register the test plugin through the same entry-point resolution seam used in
`tests/test_backend_plugin_resolution.py`. Use a test-local `EntryPointsMock`
and `monkeypatch` on
`simplebroker._backend_plugins.metadata.entry_points`. Do not monkeypatch
`ResolvedTarget.plugin` or `DBConnection`.

Use a real `ResolvedTarget`:

```python
target = ResolvedTarget(
    backend_name="counting",
    target=str(tmp_path / "broker.db"),
    backend_options={"schema": "same"},
)
```

Use real queues:

```python
q1 = Queue("a", db_path=target, persistent=True)
q2 = Queue("b", db_path=target, persistent=True)
q3 = Queue("c", db_path=target, persistent=True)

q1.write("one")
q2.write("two")
q3.write("three")
```

Desired assertion:

```python
assert plugin.create_runner_calls == 1
```

Current code should fail with `3`.

### Important details

- Do not use a real Postgres server for this test.
- Do not mock `DBConnection`.
- Do not assert only object IDs. Assert visible allocation through the backend
  plugin extension seam.
- Clean up queues in `finally` or with `contextlib.ExitStack`.

### Gate

Run:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py -k same_resolved_target
```

Confirm it fails because runner allocation is `3`, not because the test plugin
is incomplete.

## Task 2: Add The SQLite Same-Thread Red Test

### Goal

Show the same architectural issue exists for SQLite in one watcher-like thread,
without building a fake watcher.

### Files to touch

- `tests/test_queue_connection_manager.py`

### Test design

Patch `SQLiteRunner.__init__` to record runner instance IDs, then create several
persistent queues with the same file path and touch each queue.

Desired test shape:

```python
queues = [
    Queue(f"q{i}", db_path=str(db_path), persistent=True)
    for i in range(3)
]

for queue in queues:
    queue.write("message")

assert len(unique_sqlite_runner_ids) == 1
```

Current code should fail with `3`.

### Why this is not over-mocking

This patch observes the real SQLite runner constructor. It does not fake queue
operations or database behavior. The queues still write to a real SQLite file.

### Gate

Run:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py -k sqlite_same_target
```

Confirm it fails for the expected runner count.

## Task 3: Add Session Key Tests

### Goal

Pin the sharing boundaries before implementing the registry.

### Files to touch

Prefer a new focused test file:

- `tests/test_process_broker_session.py`

If the new file imports too much from internals, keep these tests in
`tests/test_queue_connection_manager.py` instead. Do not scatter them.

### Behaviors to test

Add tests using the same counting backend plugin from Task 1.

1. Same target and same backend options share.
2. Same target but different backend options do not share.
3. Different target paths do not share.
4. Injected runners do not use the counting plugin registry path.
5. Ephemeral queues do not use the process-local registry.

The tests should exercise real queue operations, not only key construction.

### Gate

Run:

```bash
uv run pytest -q -n 0 tests/test_process_broker_session.py tests/test_queue_connection_manager.py -k "target or session or runner"
```

At this stage, sharing tests should be red. Isolation tests may already pass.

## Task 4: Introduce The Internal Session Module

### Goal

Add the process-local session abstraction without wiring `Queue` through it yet.

### Files to touch

- Add `simplebroker/_broker_session.py`
- Possibly update `simplebroker/db.py` imports only if needed for type checking

### New internal concepts

Use names close to these:

- `_SessionKey`
- `_ProcessBrokerSession`
- `_ProcessBrokerSessionRegistry`
- `_freeze_for_key()`

Keep these private. Prefix with `_` unless a name is intentionally imported by
tests.

### Session key requirements

The key must include:

- current process ID from `os.getpid()`
- backend name
- target string
- frozen backend options
- frozen resolved config

For SQLite string paths, normalize the target before keying:

- expand `~`
- resolve to an absolute path when possible
- fall back to the expanded path if `Path.resolve()` raises

This prevents `./broker.db` and `/abs/path/broker.db` from becoming separate
sessions for the same SQLite file.

Use a conservative full-config fingerprint first. If this blocks practical
sharing later, narrow it with a separate test and review.

`ResolvedTarget.backend_options` is a mutable dict, so the key must freeze it
recursively. A simple deterministic helper is enough:

- sort mapping keys by `str(key)`
- recursively freeze mappings, lists, tuples, and sets
- use scalar values directly when hashable
- fall back to `repr(value)` for unusual values

Do not use raw `json.dumps()` unless you handle non-JSON config values.

### Registry requirements

The registry must:

- live in process memory only
- be guarded by `threading.RLock` or `threading.Lock`
- return a lease/session for a key
- increment a reference count on acquire
- decrement the reference count on release
- remove and close the session when the count reaches zero
- register an `atexit` cleanup hook

Do not add a cleanup thread.

### Circular import rule

`simplebroker/db.py` will import the session module. Therefore
`simplebroker/_broker_session.py` must not import `BrokerDB`, `BrokerCore`, or
`DBConnection` from `simplebroker.db` at module import time.

Use one of these patterns:

- `if TYPE_CHECKING:` imports for type hints only
- local imports inside methods that create `BrokerDB` or `BrokerCore`
- small helper functions that import lazily

Do not "fix" a circular import by moving large classes around in `db.py`.

### Gate

At the end of this task, no production behavior should change.

Run:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py
uv run ruff check simplebroker/_broker_session.py
uv run mypy simplebroker
```

## Task 5: Implement `_ProcessBrokerSession`

### Goal

Move shared ownership into `_ProcessBrokerSession` while preserving per-thread
connection isolation.

### Files to touch

- `simplebroker/_broker_session.py`
- `simplebroker/db.py` only for imports or helper reuse if needed

### Design

The session should expose methods similar to:

```python
class _ProcessBrokerSession:
    def get_connection(self, stop_event: threading.Event | None):
        ...

    def cleanup_lease(self, lease_id: object) -> None:
        ...

    def release(self, lease_id: object) -> None:
        ...

    def close_all(self) -> None:
        ...
```

Do not copy all of `DBConnection` into this class. Extract or reuse small helper
functions where needed.

### SQLite behavior

For a SQLite target, the session should create at most one `BrokerDB` per thread
per session.

- Same process, same target, same thread: same `BrokerDB` and same
  `SQLiteRunner`.
- Same process, same target, different threads: different `BrokerDB` objects and
  different SQLite connections.

This preserves SQLite's thread-safety model.

The session owns these `BrokerDB` objects. On final session close, call
`shutdown()` on each owned `BrokerDB`.

### Non-SQLite behavior

For a non-SQLite `ResolvedTarget`, the session should create one backend runner
per session, not one per queue.

That runner may own a pool. For Postgres, this is the critical resource.

The session can create one `BrokerCore` per thread using the shared runner.
This avoids sharing one mutable `BrokerCore._stop_event` across watcher threads
while still sharing the backend runner/pool.

The session owns the shared runner. Individual `BrokerCore` objects created with
that shared runner must not each shut the runner down. On final session close:

1. discard cached per-thread cores
2. shut down the owned runner exactly once, preferring `shutdown()` when present
3. fall back to `close()` only if the runner has no `shutdown()`

### Stop-event behavior

Each `DBConnection` lease still owns its current stop event.

Before returning a `BrokerDB` or `BrokerCore`, set that core's stop event to the
lease's stop event.

Do not store a single stop event on the shared session. That would let one
watcher overwrite another watcher's cancellation state across threads.

### Generator/reentrancy behavior

Be careful with `at_least_once` generator state in `BrokerCore`.

If multiple queue handles in the same thread share the same core, the existing
same-thread reentrancy guard remains effective. That is preferable to sharing
only the runner while allowing two `BrokerCore` instances to accidentally nest
transactions on the same underlying connection.

If a test fails because an old error message says "use a separate Queue
instance," update the message and docs to say "use a separate broker target or
separate process/thread" only if that behavior is still correct. Do not hide the
semantic change.

### Cleanup behavior

Distinguish two operations:

- cleanup active handles for recovery
- release the queue lease

`cleanup_lease()` should not decrement the session reference count.

`release()` should decrement the reference count and close owned resources only
when the last lease is gone.

For shared sessions, `cleanup_lease()` should be conservative:

- close or recycle the calling thread's cached core if one exists
- keep the session lease alive
- keep the shared backend runner/pool alive

Do not make cleanup from one queue shut down the shared runner while sibling
queue leases still exist.

### Gate

Run:

```bash
uv run pytest -q -n 0 tests/test_process_broker_session.py
uv run pytest -q -n 0 tests/test_queue_connection_manager.py
```

The tests from Tasks 1 and 2 may still be red until `DBConnection` is wired.

## Task 6: Wire `DBConnection` To The Shared Session

### Goal

Make persistent queues use the process-local session while preserving existing
private behavior for other paths.

### Files to touch

- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `tests/test_queue_connection_manager.py`
- `tests/test_process_broker_session.py`

### API shape

Add an internal keyword to `DBConnection`:

```python
DBConnection(
    db_path,
    runner=None,
    config=config,
    share_in_process=False,
)
```

Default it to `False` so existing direct `DBConnection(...)` and `open_broker()`
callers keep private ownership unless explicitly changed.

In `Queue.__init__`, use:

```python
share_in_process = persistent and runner is None
self.conn = DBConnection(
    self._db_path,
    runner,
    config=self._config,
    share_in_process=share_in_process,
)
```

Do not share injected runners through the registry. The caller already supplied
the runner and owns it.

### `DBConnection` behavior

For shared mode:

- `get_connection()` delegates to the session.
- `set_stop_event()` stores the lease stop event and applies it on the next
  borrowed core.
- `cleanup()` asks the session to clean up this lease's active handles but does
  not release the lease.
- add a `close()` or `release()` method that releases the lease.
- `__exit__()` should call the release method for context-manager use.
- `__del__()` should call the release method.

For private mode:

- preserve existing behavior.
- `close()` may delegate to `cleanup()` for compatibility.

### `Queue` behavior

Update `Queue.close()` and the weakref finalizer path so they release the
`DBConnection` lease rather than only calling cleanup.

Keep `Queue.cleanup_connections()` as recovery cleanup, not final release.

This distinction is important for watcher restart/retry behavior.

### Gate

The first red tests should now turn green:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py -k "same_resolved_target or sqlite_same_target"
```

Then run:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py tests/test_process_broker_session.py
```

## Task 7: Add Lifecycle And Isolation Tests

### Goal

Prevent the obvious resource-management regressions.

### Files to touch

- `tests/test_process_broker_session.py`
- possibly `tests/test_queue_connection_manager.py`

### Required tests

1. **Closing one lease does not close shared resources**
   - Create two persistent queues for the same counting target.
   - Touch both.
   - Close the first queue.
   - Touch the second queue again.
   - Assert no new backend runner was allocated.

2. **Closing the last lease closes owned resources**
   - Use a counting runner that records `shutdown()` or `close()` calls.
   - Create two queues for the same target.
   - Touch both.
   - Close both.
   - Assert owned resources were closed once.
   - Create a third queue for the same target.
   - Touch it.
   - Assert a new backend runner was allocated.

3. **Cleanup does not release the lease**
   - Create a queue.
   - Touch it.
   - Call `queue.cleanup_connections()`.
   - Touch it again.
   - Assert the queue remains usable.
   - For shared non-SQLite, assert this does not allocate a second backend
     runner.

4. **SQLite cross-thread isolation**
   - Create several persistent queues for the same SQLite path.
   - Touch them in one thread and record the SQLite runner/connection identity.
   - Touch them in another thread and record identity.
   - Assert one identity per thread, not one identity total and not one per
     queue.

5. **Process isolation**
   - Use `multiprocessing.get_context("spawn")` if practical.
   - Parent and child each create a persistent queue for the same target.
   - Assert the child creates its own runner/session.
   - If a spawn test is too slow/flaky, add a narrower test that monkeypatches
     `os.getpid()` around session-key construction. Prefer the spawn test if it
     is stable.

### Gate

Run:

```bash
uv run pytest -q -n 0 tests/test_process_broker_session.py
uv run pytest -q -n 0 tests/test_queue_connection_manager.py
```

Do not proceed while lifecycle tests are flaky.

## Task 8: Run Watcher Compatibility Tests

### Goal

Catch cleanup and stop-event regressions in the real watcher path.

### Files to touch

Only production files if tests fail for a real reason.

### Commands

Run:

```bash
uv run pytest -q -n 0 tests/test_watcher.py
uv run pytest -q -n 0 tests/test_watcher_cleanup.py
uv run pytest -q -n 0 tests/test_watcher_edge_cases.py
```

### What to watch for

- watcher stop hangs
- cleanup warnings
- leaked SQLite file locks
- changed retry behavior
- data-version polling failures
- stop event from one watcher affecting another watcher

If a stop-event bug appears, do not patch around it in tests. Fix the lease/core
boundary so stop events are applied per borrowed core and do not live on the
shared session.

## Task 9: Add A Postgres-Extension Regression Test If PG Is Available

### Goal

Validate the real extension does not allocate one `PostgresRunner` or pool per
queue.

### Files to touch

- `extensions/simplebroker_pg/tests/test_pg_integration.py`
- or add `extensions/simplebroker_pg/tests/test_pg_session_sharing.py`

Prefer a new test file if the integration file is already broad.

### Test design

If a live Postgres DSN is available in CI or local test setup, patch
`PostgresRunner.__init__` or the plugin's `create_runner()` to count allocations
while still delegating to real behavior.

Use real queues:

```python
queues = [
    Queue(f"pg_session_{i}", db_path=resolved_pg_target, persistent=True)
    for i in range(5)
]

for queue in queues:
    queue.write("message")

assert postgres_runner_allocations == 1
```

Do not assert against `pg_stat_activity` unless the test environment already
has reliable privileges and isolation. Counting runner/pool construction is the
contract under test.

### Gate

Run the extension tests if configured:

```bash
BROKER_TEST_BACKEND=postgres \
SIMPLEBROKER_PG_TEST_DSN="postgresql://..." \
uv run pytest -q -n 0 extensions/simplebroker_pg/tests
```

If no Postgres DSN is available, document that this gate was not run. The core
counting-backend tests are still mandatory.

## Task 10: Documentation Updates

### Goal

Make the new ownership model clear for future contributors.

### Files to touch

- `README.md` if it discusses persistent connections
- `simplebroker/sbqueue.py` docstrings
- `simplebroker/db.py` docstrings
- `CHANGELOG.md` if this repository expects unreleased changes there
- this plan doc only if implementation discovers a materially different final
  shape

### Required doc points

Document these facts:

- Persistent queues share a process-local broker session for the same resolved
  target.
- Sharing is process-local only.
- SQLite still preserves per-thread connection isolation.
- Injected runners remain caller-owned and are not placed in the shared
  registry.
- Ephemeral queues remain ephemeral.

Do not oversell this as "one database connection per process." That would be
false for SQLite threads and may be false for pooled backends.

Correct wording:

```text
Persistent Queue handles for the same resolved target share process-local
backend session state. Backends may still create separate physical connections
per thread or per pool checkout.
```

## Task 11: Downstream Weft Follow-Up

### Goal

Verify the real product path after SimpleBroker is fixed.

### Files to inspect in Weft

- `../weft/weft/core/tasks/multiqueue_watcher.py`
- `../weft/tests/tasks/test_multiqueue_watcher.py`
- `../weft/tests/multiqueue_polling_benchmark.py`
- `../weft/docs/agent-context/runbooks/runtime-and-context-patterns.md`

### What to test in Weft

Add a Weft regression test after the SimpleBroker change is available to Weft:

- Instantiate `MultiQueueWatcher` with several queues.
- Use a SimpleBroker counting backend target or monkeypatch the pg plugin.
- Touch/drain multiple watched queues.
- Assert backend runner/pool allocation does not scale with queue count.

Also inspect cleanup:

- Weft's `MultiQueueWatcher` owns more than the initial queue.
- If it does not already clean up all owned queue handles on stop, add that as a
  separate Weft fix.
- Keep that separate from the SimpleBroker session-sharing PR unless the tests
  prove they must land together.

Do not patch Weft first as a workaround for SimpleBroker.

## Final Verification Gates

Before calling the implementation done, run:

```bash
uv run pytest -q -n 0 tests/test_queue_connection_manager.py
uv run pytest -q -n 0 tests/test_process_broker_session.py
uv run pytest -q -n 0 tests/test_watcher.py tests/test_watcher_cleanup.py tests/test_watcher_edge_cases.py
uv run pytest -q -n 0
uv run ruff check .
uv run mypy simplebroker
```

If Postgres is configured:

```bash
BROKER_TEST_BACKEND=postgres \
SIMPLEBROKER_PG_TEST_DSN="postgresql://..." \
uv run pytest -q -n 0 extensions/simplebroker_pg/tests
```

The implementation is not complete if:

- runner allocation still scales with queue count for same-target persistent
  queues
- SQLite shares one physical connection across threads
- injected runners are closed by SimpleBroker
- watcher cleanup permanently closes a queue's ability to reconnect
- process isolation relies on "we probably will not fork"
- tests pass only because internals were mocked

## Review Checklist For The Implementer

Before opening a PR, answer these in the PR description:

1. What is the session key?
2. Which paths use the process-local registry?
3. Which paths explicitly do not use it?
4. How is the last lease released?
5. How do we avoid sharing SQLite connections across threads?
6. How do we avoid sharing live objects across processes?
7. Which test proves pg-like runner/pool allocation no longer scales with queue
   count?
8. Which test proves `Queue.cleanup_connections()` does not release the lease?
9. Which test proves closing one queue does not close another queue's shared
   resources?
10. Which watcher tests were run?

If any answer is vague, the implementation is not ready.

## Fresh-Eyes Review Notes

This section records the plan review done before committing the plan document.

### Review pass 1: API scope

Risk found: adding a public `Broker` API during this fix would expand the work
and invite taste-driven API design.

Decision: keep the first implementation internal. The abstraction is a
process-local broker session. The existing `Queue` API gets the behavior.

### Review pass 2: stop-event ownership

Risk found: a shared session-level stop event would let one watcher overwrite
another watcher's cancellation path.

Decision: stop events stay on the `DBConnection` lease. The borrowed core gets
the lease stop event immediately before use.

### Review pass 3: cleanup versus release

Risk found: treating `cleanup_connections()` as lease release would break watcher
retry/reconnect behavior.

Decision: distinguish cleanup from release. Cleanup recycles active handles.
Release decrements the session lease and may close the shared session when the
last owner exits.

### Review pass 4: test realism

Risk found: a pure mock of `DBConnection` or `BrokerCore` would prove the test's
mock wiring, not the runtime contract.

Decision: use real queues and real SQLite-backed execution. Only instrument the
backend plugin and runner constructor at the allocation seam.

### Review pass 5: plan drift

Risk found: the plan could drift into a full broker API redesign or a Weft-only
fix.

Decision: the plan remains centered on the agreed invariant: same process, same
resolved target, persistent queues should not allocate one backend runner/pool
per queue.

### Review pass 6: implementability details

Risks found:

- the test plugin registration seam was implicit
- a new session module could accidentally create circular imports with `db.py`
- SQLite path aliases could produce separate keys for the same database file
- shared non-SQLite cores could each try to shut down the same runner

Decision: make plugin registration, lazy imports, SQLite path normalization, and
owned-runner shutdown rules explicit in the implementation tasks.
