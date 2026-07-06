# SimpleBroker watcher embedder lifecycle hooks plan

Date: 2026-07-06

Status: ready for implementation after review

Primary outcome: SimpleBroker stays single-queue and simple, but embedders such
as Weft and Taut no longer need to clone `BaseWatcher._run_with_retries()` just
to supply a multi-queue activity waiter.

Release order:

1. Land and publish SimpleBroker first.
2. Refactor Weft onto the released SimpleBroker API.
3. Refactor Taut after Weft, because Taut's `taut/watcher.py` is explicitly
   vendored/adapted from Weft's `weft/core/tasks/multiqueue_watcher.py`.

Version boundary:

- The SimpleBroker release carrying these hooks is expected to be `5.1.0`.
- Weft and Taut branches must not raise their dependency floors until
  `simplebroker==5.1.0` is published and verified from a clean environment.
- After publish, both downstream repos must raise `simplebroker>=5.0.2` to
  `simplebroker>=5.1.0`.
- Do not place these changes under the released `5.0.0`, `5.0.1`, or `5.0.2`
  changelog history. Implementation entries start under `[Unreleased]`; release
  prep moves them to the new `5.1.0` section.

If Taut has urgent delivery pressure, Taut can use the same downstream phase
before Weft. Do not let that create a permanent fork in the multi-queue watcher
shape. Reconcile Weft and Taut immediately after.

## Mental model

SimpleBroker owns queue primitives and the single-queue watcher lifecycle.
Embedders own application policy: multi-queue selection, per-queue dispatch
modes, membership refresh, cursors, priorities, and task semantics.

This plan adds three protected `BaseWatcher` hooks and one public
`PollingStrategy` ownership method. It does not add a `MultiQueueWatcher` to
SimpleBroker. That is deliberate YAGNI: Weft and Taut need different policy on
top of the same low-level watcher lifecycle.

Protected methods on `BaseWatcher` are still part of the documented subclassing
contract because `BaseWatcher` is exported from `simplebroker.ext`.

## Verified current state

Line numbers are from the local checkout on 2026-07-06. Re-run `nl -ba` before
editing if the file has moved.

| Repo | File | Current behavior |
|------|------|------------------|
| SimpleBroker | `simplebroker/watcher.py:707` | `BaseWatcher._run_with_retries()` owns the top-level watcher restart loop. |
| SimpleBroker | `simplebroker/watcher.py:721` | `_run_with_retries()` calls `self._check_stop()` at the top of its `try` body, before the strategy startup block. |
| SimpleBroker | `simplebroker/watcher.py:742` | `_run_with_retries()` creates the activity waiter inline with `queue.create_activity_waiter(stop_event=self._stop_event)`. |
| SimpleBroker | `simplebroker/watcher.py:747` | `_run_with_retries()` calls `self._strategy.start(...)` inline. |
| SimpleBroker | `simplebroker/watcher.py:573` | `stop()` → `_perform_stop()` closes the strategy after the watcher thread exits. `_run_with_retries()` has no strategy-closing `finally`. |
| SimpleBroker | `simplebroker/watcher.py:1195` | `PollingStrategy.start()` calls `self.close()` before installing a new activity waiter. |
| SimpleBroker | `simplebroker/watcher.py:1212` | `PollingStrategy.close()` closes `_activity_waiter` and sets it to `None`. |
| SimpleBroker | `simplebroker/sbqueue.py:1159` | `Queue.create_activity_waiter()` caches a single-queue waiter on `Queue._activity_waiter`. |
| SimpleBroker | `simplebroker/ext.py:56` | `BaseWatcher`, `PollingStrategy`, `StopWatching`, and `default_error_handler` are exported from the embedder surface. |
| Weft | `weft/core/tasks/multiqueue_watcher.py:501` | `MultiQueueWatcher` clones `_run_with_retries()` only to call `_start_strategy_for_configured_queues()`. |
| Weft | `weft/core/tasks/multiqueue_watcher.py:510` | A comment says the override exists because SimpleBroker 3.3.2 had no protected waiter factory hook. |
| Weft | `weft/core/tasks/multiqueue_watcher.py:392` | `_reset_multi_activity_waiter()` reaches into `self._strategy._activity_waiter`. |
| Taut | `taut/watcher.py:348` | `MultiQueueWatcher` clones the same top-level retry loop. |
| Taut | `taut/watcher.py:291` | `_reset_multi_activity_waiter()` reaches into `self._strategy._activity_waiter`. |
| Taut | `taut/watcher.py:1` | The file states it is vendored/adapted from Weft. |

## Non-goals

- Do not add a `MultiQueueWatcher` to SimpleBroker.
- Do not move Weft or Taut queue membership logic into SimpleBroker.
- Do not rewrite `BaseWatcher._run_with_retries()` over `execute_retry()`.
  That loop is crash recovery for the whole watcher body, not a normal
  operation retry. It intentionally preserves deterministic `2**retry_count`
  sleeps and `_check_retry_timeout()`.
- Do not change watcher crash-recovery pacing, stop behavior, or thread-local
  cleanup timing.
- Do not change backend activity waiter semantics. Waiters remain hints:
  `wait(timeout) == True` means "some watched queue may have changed", not
  "a message is guaranteed".
- Do not change any existing external CLI contention or thundering-herd test
  that uses the `run_cli` wrapper. Not for cleanup, not for timing tweaks, not
  for helper reshaping. Add focused tests instead.
- Do not weaken or rewrite existing watcher subprocess tests to make this pass.

## Target SimpleBroker API

Add these protected methods to `BaseWatcher` in `simplebroker/watcher.py`:

```python
def _start_strategy(self) -> None:
    """Initialize the polling strategy for this watcher instance."""

def _create_activity_waiter(self, queue: Queue) -> ActivityWaiter | None:
    """Return the activity waiter passed into PollingStrategy.start()."""

def _on_data_version_change(self, queue: Queue) -> None:
    """Refresh watcher-owned queue caches after data-version changes."""
```

Default behavior must match today's behavior:

- `_start_strategy()` contains the existing `if hasattr(self._strategy, "start")`
  block currently inside `_run_with_retries()`.
- `_create_activity_waiter(queue)` returns
  `queue.create_activity_waiter(stop_event=self._stop_event)`.
- `_on_data_version_change(queue)` calls `queue.refresh_last_ts()`.
- The initial `queue.refresh_last_ts()` before polling stays as a direct
  startup prime. Do not call `_on_data_version_change()` for that initial prime,
  because downstream code currently only runs its extra refresh work from the
  callback path.

The default `_start_strategy()` body must have this shape. Keep names and stop
checks close to this skeleton so reviewers can see that only the startup block
moved:

```python
def _start_strategy(self) -> None:
    if hasattr(self._strategy, "start"):
        queue = self._get_queue_for_data_version()
        self._check_stop()

        def data_version_getter(q: Queue = queue) -> int | None:
            return q.get_data_version()

        try:
            queue.refresh_last_ts()
        except Exception:
            logger.debug("Initial last_ts refresh failed", exc_info=True)
        self._check_stop()

        def on_data_version_change(q: Queue = queue) -> None:
            self._on_data_version_change(q)

        activity_waiter = self._create_activity_waiter(queue)
        self._check_stop()

        self._strategy.start(
            data_version_getter,
            on_data_version_change=on_data_version_change,
            activity_waiter=activity_waiter,
        )
        self._check_stop()
```

Do not leave a duplicate copy of this block in `_run_with_retries()`. After this
extraction, `_run_with_retries()` calls `self._start_strategy()` and then
continues with the existing initial drain, processing loop, and retry handling.

The `self._check_stop()` currently at the top of the retry-loop `try` body
(`simplebroker/watcher.py:721`) is not part of the extracted block. It stays in
`_run_with_retries()`, before the `self._start_strategy()` call, so watchers
whose strategy has no `start()` method still check for stop before the initial
drain.

Add this public method to `PollingStrategy`:

```python
def detach_activity_waiter(
    self,
    *,
    expected: ActivityWaiter | None = None,
) -> ActivityWaiter | None:
    """Detach and return the current activity waiter without closing it."""
```

Semantics:

- If `expected is None`, detach whatever current waiter is installed.
- If no waiter is installed, return `None` and leave all state unchanged.
  The state clearing below happens only when a waiter is actually detached.
- If `expected is not None` and the current waiter is not that exact object,
  return `None` and leave the strategy unchanged.
- If detaching, set `_activity_waiter = None`.
- Clear native-waiter state that belongs to the detached waiter:
  `_native_activity_pending = False` and `_activity_burst_remaining = 0`.
- Do not clear local activity hints. `_local_activity_pending`,
  `_local_activity_pending_for_drain`, and `_local_activity_empty_check` are
  same-watcher hints, not waiter ownership state.
- Do not close the detached waiter. The caller now owns it.

Update `PollingStrategy.close()` to call `detach_activity_waiter()` and close
the returned waiter if any.

Update `PollingStrategy.start()` so it does not close and reinstall the same
waiter object. The simplest correct pattern is:

```python
old_waiter = self.detach_activity_waiter()
if old_waiter is not None and old_waiter is not activity_waiter:
    old_waiter.close()
```

Then assign the new provider, callback, counters, and `self._activity_waiter`.

This fixes a real ownership edge: `Queue.create_activity_waiter()` caches a
waiter, while `PollingStrategy.start()` currently closes the previous strategy
waiter before installing the incoming one. If the incoming object is the same
cached waiter, the old code can close and then reuse it.

One intentional behavior change rides along with the rewrite: today `start()`
never resets `_native_activity_pending`, so a stale native hint can survive a
strategy restart. With the detach-based `start()`, detaching the previous
waiter clears that hint. Treat this as a required invariant, not an accident:
a strategy lifecycle must not begin with a native hint left over from a
previous waiter.

`PollingStrategy.start()` is lifecycle initialization, not a live waiter hot-swap
API. It is called when a watcher starts or restarts after the outer retry loop.
Clearing `_native_activity_pending` and `_activity_burst_remaining` while
detaching the previous waiter is intentional because those are stale hints from
the previous strategy lifecycle. Downstream multi-queue membership refreshes
must not call `start()` just to rotate waiters; they must use their own
`_reset_multi_activity_waiter()` plus `detach_activity_waiter(expected=waiter)`.

Known and accepted consequence: after a mid-run
`_reset_multi_activity_waiter()` (for example Weft's
`wait_for_activity(timeout)` failure path), the strategy runs without a native
waiter until the next watcher restart and falls back to data-version polling.
That matches today's behavior under the private-attribute workaround. This
plan adds no supported way to install a new waiter into a running strategy; if
an embedder ever needs that, design it in a follow-up plan instead of
hot-swapping ad hoc.

## Engineering rules for this work

- Red-green TDD: write the focused failing test first, verify it fails for the
  expected reason, then implement the smallest change.
- Prefer real `Queue` and watcher objects over mocks. Fakes are allowed for the
  `ActivityWaiter` protocol because the protocol is small and backend-specific.
- Do not mock `Queue`, `BrokerDB`, or the broker backend for watcher behavior.
  Use temp databases and real watcher instances.
- Keep changes local to watcher lifecycle and downstream multi-queue watcher
  cleanup. If a task starts requiring backend plugin rewrites, stop and reassess.
- No broad refactors. Extract only what removes the duplicated retry loop.
- Keep type hints concrete. The repo uses `from __future__ import annotations`;
  the `ActivityWaiter` type is available under `TYPE_CHECKING` in
  `simplebroker/watcher.py`.
- Keep code formatted by repo tools. Do not hand-format large unrelated blocks.

## Phase A: SimpleBroker implementation

### A0. Read before editing

Files to inspect:

- `simplebroker/watcher.py`
- `simplebroker/sbqueue.py`
- `simplebroker/ext.py`
- `tests/test_watcher.py`
- `tests/test_watcher_edge_cases.py`
- `tests/test_watcher_race_conditions.py`
- `tests/test_ext_imports.py`
- `README.md` sections:
  - "Advanced: Custom Extensions"
  - "Advanced: First-Party Backend Extensions"
  - "Development & Contributing"
  - "Releases"
- `CHANGELOG.md`
- `pyproject.toml`
- `simplebroker/_constants.py`

Useful search commands:

```bash
rg -n "_run_with_retries|_handle_retry|activity_waiter|PollingStrategy|create_activity_waiter" simplebroker tests
rg -n "simplebroker.ext|BaseWatcher|PollingStrategy|ActivityWaiter|create_activity_waiter_for_queues" README.md docs/plans
```

### A1. Red tests for `PollingStrategy` waiter ownership

Modify:

- `tests/test_watcher.py`

Add a small fake waiter class inside `TestPollingStrategy` or near the test:

```python
class FakeActivityWaiter:
    def __init__(self) -> None:
        self.close_calls = 0

    def wait(self, timeout: float) -> bool:
        del timeout
        return False

    def close(self) -> None:
        self.close_calls += 1
```

Add tests:

1. `test_detach_activity_waiter_returns_without_closing`
   - Create `PollingStrategy(threading.Event())`.
   - Call `strategy.start(activity_waiter=waiter)`.
   - Call `detached = strategy.detach_activity_waiter()`.
   - Assert `detached is waiter`.
   - Assert `waiter.close_calls == 0`.
   - Assert `strategy.uses_native_activity() is False`.
   - Assert a second `detach_activity_waiter()` returns `None`.

2. `test_detach_activity_waiter_expected_mismatch_is_noop`
   - Start with `waiter_a`.
   - Call `strategy.detach_activity_waiter(expected=waiter_b)`.
   - Assert return is `None`.
   - Assert `strategy.uses_native_activity() is True`.
   - Assert `waiter_a.close_calls == 0`.

3. `test_start_does_not_close_same_activity_waiter`
   - Start with `waiter`.
   - Call `strategy.start(activity_waiter=waiter)` again.
   - Assert `waiter.close_calls == 0`.
   - Assert `strategy.uses_native_activity() is True`.

4. `test_start_closes_replaced_activity_waiter`
   - Start with `waiter_a`.
   - Start with `waiter_b`.
   - Assert `waiter_a.close_calls == 1`.
   - Assert `waiter_b.close_calls == 0`.

Run only the red tests first:

```bash
uv run pytest -q -n 0 tests/test_watcher.py -k "detach_activity_waiter or start_does_not_close_same_activity_waiter or start_closes_replaced_activity_waiter"
```

Expected before implementation: failures because `detach_activity_waiter` does
not exist and because `start()` closes the old waiter unconditionally.

### A2. Implement `PollingStrategy.detach_activity_waiter()`

Modify:

- `simplebroker/watcher.py`

Implementation notes:

- Add the method near `uses_native_activity()`, before `start()`.
- Keep it small. No backend knowledge. No queue knowledge.
- `close()` must become:

```python
waiter = self.detach_activity_waiter()
if waiter is not None:
    waiter.close()
```

- `start()` must not call `self.close()` anymore, because `close()` always closes
  the current waiter. Use the old/new object comparison described above.
- Preserve all existing counter resets in `start()`:
  `_data_version_provider`, `_check_count`, `_data_version`,
  `_data_change_callback`, `_activity_waiter`, `_activity_burst_remaining`, and
  `_schedule_next_native_idle_poll(initial=True)`.
- Verify against the old `close()` body before editing. In the current code,
  `close()` only closes `_activity_waiter` and sets it to `None`; if that changes
  before implementation, update this plan or preserve the additional cleanup in
  `start()`.

Re-run:

```bash
uv run pytest -q -n 0 tests/test_watcher.py -k "detach_activity_waiter or start_does_not_close_same_activity_waiter or start_closes_replaced_activity_waiter"
```

Expected after implementation: pass.

### A3. Red tests for `BaseWatcher` lifecycle hooks

Modify:

- `tests/test_watcher.py`

Add tests that use real `QueueWatcher` subclasses. Do not mock broker or queue
methods.

Test 1: `_run_with_retries()` starts the strategy through an overridable hook.

Shape:

```python
class StrategyHookWatcher(QueueWatcher):
    def __init__(self, *args, **kwargs):
        self.start_strategy_calls = 0
        super().__init__(*args, **kwargs)

    def _start_strategy(self) -> None:
        self.start_strategy_calls += 1
        super()._start_strategy()

    def _process_messages(self) -> None:
        raise StopWatching
```

Then:

- Build with a real temp broker target.
- Call `watcher._run_with_retries()`.
- Assert `start_strategy_calls == 1`.

Before implementation this fails because `_run_with_retries()` has no
`_start_strategy()` hook to override.

Test 2: `_create_activity_waiter()` is used by strategy startup.

Shape:

- Define `HookedWaiter` with `wait()` and `close()` methods.
- Subclass `QueueWatcher`:
  - `_create_activity_waiter(self, queue)` stores `queue.name` and returns the
    fake waiter.
  - `_process_messages()` does not matter for this test and does not need to be
    overridden.
- Construct the watcher for one known queue name, for example
  `"hooked_queue"`. The `queue` argument passed to `_create_activity_waiter()`
  comes from `BaseWatcher._get_queue_for_data_version()`, which is the same
  queue object used for the data-version provider.
- Call `_start_strategy()` directly.
- Assert the hook saw `"hooked_queue"`.
- Assert `watcher._strategy.uses_native_activity() is True`.
- Call `watcher._strategy.close()` in cleanup if the test does not use a context
  manager.

Do not use `_run_with_retries()` for this test. Calling `_start_strategy()`
directly keeps the installed waiter observable and the assertion focused. Note
for the implementer: the strategy is closed by `stop()` via `_perform_stop()`
(`simplebroker/watcher.py:573`), not by any `finally` inside
`_run_with_retries()` — so clean up with `watcher._strategy.close()` as stated
above rather than relying on watcher shutdown.

Test 3: `_on_data_version_change()` is the callback used after data-version
changes.

Shape:

- Use a real SQLite temp database.
- Subclass `QueueWatcher` and override `_on_data_version_change()` to append the
  queue name, then call `super()._on_data_version_change(queue)`.
- Construct the watcher and the writer `Queue` from the same `broker_target`
  path, not from separate temporary paths. The writer can be a separate
  `Queue("watched_queue", db=broker_target)` instance so the test uses a real
  second handle pointed at the same SQLite database.
- Call `watcher._start_strategy()`.
- Call `watcher._strategy._check_data_version()` once to establish the baseline.
- Clear the recorded override calls after the baseline check. This is important:
  the first version check may call the callback while establishing the initial
  version, and that must not be enough to pass the test.
- Write a message through a separate `Queue` or broker handle pointed at the
  same database.
- Call `watcher._strategy._check_data_version()` again.
- Assert the override was called after the write, and assert the queue name is
  the watched queue name.

This is a protected-method test, but it uses real SQLite state rather than
mocking the data-version provider. That is intentional.

Test 4: default `_start_strategy()` does not close and reinstall a cached queue
waiter.

Shape:

- Use a real SQLite temp broker target and a real `QueueWatcher`.
- Monkeypatch only the SQLite backend plugin's `create_activity_waiter(...)`
  method to return one `FakeActivityWaiter`. This is the backend hook boundary;
  do not mock `Queue`, `QueueWatcher`, or `BrokerDB`.
- Call `watcher._start_strategy()` twice. This forces the default
  `_create_activity_waiter(queue)` path through
  `Queue.create_activity_waiter(...)`, including the `Queue._activity_waiter`
  cache.
- Assert the fake waiter's `close_calls == 0` after the second start.
- Assert `watcher._strategy.uses_native_activity() is True`.
- In cleanup, call `watcher._strategy.close()` and assert the fake waiter is
  closed exactly once.

This test is not redundant with the pure `PollingStrategy` tests in A1. The
production bug class is "close then reinstall the same object returned from
`Queue.create_activity_waiter()`'s cache"; a fake passed directly to
`PollingStrategy.start()` does not exercise that cache.

Run:

```bash
uv run pytest -q -n 0 tests/test_watcher.py -k "start_strategy or create_activity_waiter or data_version_change or cached_queue_activity_waiter"
```

Expected before implementation: failure on missing hook methods or ignored
overrides.

### A4. Extract `BaseWatcher._start_strategy()` and hooks

Modify:

- `simplebroker/watcher.py`

Implementation details:

1. Add `_create_activity_waiter(self, queue: Queue) -> ActivityWaiter | None`.
   Default implementation:

   ```python
   return queue.create_activity_waiter(stop_event=self._stop_event)
   ```

2. Add `_on_data_version_change(self, queue: Queue) -> None`.
   Default implementation:

   ```python
   queue.refresh_last_ts()
   ```

3. Add `_start_strategy(self) -> None`.
   Extract the existing strategy startup block from `_run_with_retries()`:

   - Preserve `if hasattr(self._strategy, "start")`.
   - Preserve `queue = self._get_queue_for_data_version()`.
   - Preserve `_check_stop()` calls around setup.
   - Preserve `data_version_getter(q: Queue = queue) -> int | None`.
   - Preserve the startup `queue.refresh_last_ts()` try/except and debug log.
   - Use the new `_on_data_version_change()` hook in the callback:

     ```python
     def on_data_version_change(q: Queue = queue) -> None:
         self._on_data_version_change(q)
     ```

   - Use the new `_create_activity_waiter(queue)` hook.
   - Preserve the call to `self._strategy.start(...)`.

4. Replace the inline block in `_run_with_retries()` with:

   ```python
   self._start_strategy()
   ```

   Keep the `self._check_stop()` that precedes the extracted block
   (currently `watcher.py:721`) in `_run_with_retries()`, immediately before
   this call.

5. Do not alter the rest of `_run_with_retries()`:

   - Initial drain stays inside `_in_initial_drain`.
   - `_process_messages()` call stays where it is.
   - `except (StopException, StopWatching)` stays.
   - `except KeyboardInterrupt` stays.
   - Generic `Exception` still increments `retry_count` and calls
     `_handle_retry()`.

Re-run:

```bash
uv run pytest -q -n 0 tests/test_watcher.py -k "start_strategy or create_activity_waiter or data_version_change"
uv run pytest -q -n 0 tests/test_watcher_edge_cases.py -k "retry or absolute_timeout"
```

### A5. Regression tests for crash recovery invariants

Modify only if existing tests do not already prove these:

- `tests/test_watcher_edge_cases.py`

Do not rewrite the crash-recovery tests. Extend them only if a specific
assertion is missing.

Required invariants:

- `_handle_retry()` still sleeps `2**retry_count`.
- `_run_with_retries()` still restarts the whole watcher body after a generic
  exception.
- Stop during retry sleep still exits cleanly.
- `TimeoutError` from `_check_retry_timeout()` is still possible and still
  includes `MAX_TOTAL_RETRY_TIME`.

Run:

```bash
uv run pytest -q -n 0 tests/test_watcher_edge_cases.py -k "retry or absolute_timeout"
```

### A6. Public embedder-surface docs

Modify:

- `README.md`
- `CHANGELOG.md`

README target area:

- The "Advanced watcher integrations" paragraph around `README.md:1916`.

Add a concise note after the multi-queue waiter section:

- `BaseWatcher` and `PollingStrategy` are exported from `simplebroker.ext`.
- Subclasses that need a custom native waiter must override
  `_create_activity_waiter(queue)` instead of copying `_run_with_retries()`.
- Subclasses that attach a caller-owned waiter and later close it themselves can
  call `PollingStrategy.detach_activity_waiter(expected=waiter)` before closing
  the waiter.
- Do not document multi-queue watcher policy as if SimpleBroker owns it.

CHANGELOG:

- Add entries under `[Unreleased]`.
- Do not edit `[5.0.0]`, `[5.0.1]`, or any already released section.
- Suggested entries:

```markdown
### Added
- Added protected `BaseWatcher` lifecycle hooks for embedders that need custom
  activity waiters without cloning the watcher retry loop.
- Added `PollingStrategy.detach_activity_waiter()` so embedder-owned activity
  waiters can be released from the strategy without private attribute access.

### Fixed
- `PollingStrategy.start()` no longer closes and reinstalls the same cached
  activity waiter object.
```

### A7. SimpleBroker verification gates

Run focused gates first:

```bash
uv run pytest -q -n 0 tests/test_watcher.py -k "PollingStrategy or start_strategy or activity_waiter or data_version"
uv run pytest -q -n 0 tests/test_watcher_edge_cases.py -k "retry or absolute_timeout"
uv run pytest -q -n 0 tests/test_watcher_race_conditions.py -k "native_activity_waiter"
uv run pytest -q -n 0 tests/test_ext_imports.py
```

Run broad gates before release:

```bash
uv run pytest
uv run ./bin/pytest-pg
uv run ./bin/pytest-redis
uv run ruff check simplebroker tests bin
uv run ruff format --check simplebroker tests bin
uv run mypy simplebroker bin/release.py
uv build
```

If `pytest-pg` or `pytest-redis` cannot run because Docker is unavailable, say so
in the PR and run the corresponding CI workflow before publishing.

### A8. SimpleBroker release prep and publish

This is a public embedder API addition, so use the next minor version unless the
maintainer has already chosen a different unreleased version. As of this plan,
the expected release is `5.1.0`.

Release files:

- `pyproject.toml`
- `simplebroker/_constants.py`
- `CHANGELOG.md`
- `README.md`
- lock files only if the release helper changes them

Rules:

- Do not put this change under any already released changelog heading.
- During the implementation PR, add the entry under `[Unreleased]`.
- During release prep, do not publish until `CHANGELOG.md` has a
  `## [5.1.0] - 2026-07-06` section, or the release helper has generated the
  equivalent dated section. Move the watcher-hook entries into that section.
  If other `[Unreleased]` entries are also part of this release, move them too.
  If any entry is not part of this release, leave it under `[Unreleased]` and
  document why in the release PR.
- Verify `pyproject.toml` and `simplebroker/_constants.py` match after version
  prep.

Dry run:

```bash
python bin/release.py --version 5.1.0 --dry-run
```

Publish through the helper, not by pushing tags by hand:

```bash
python bin/release.py --version 5.1.0
```

After the GitHub Actions release gate publishes to PyPI, verify from a clean
environment:

```bash
python -m venv /tmp/simplebroker-5.1.0-check
/tmp/simplebroker-5.1.0-check/bin/python -m pip install -U pip
/tmp/simplebroker-5.1.0-check/bin/python -m pip install "simplebroker==5.1.0"
/tmp/simplebroker-5.1.0-check/bin/python - <<'PY'
from simplebroker.ext import BaseWatcher, PollingStrategy

assert hasattr(BaseWatcher, "_create_activity_waiter")
assert hasattr(BaseWatcher, "_start_strategy")
assert hasattr(BaseWatcher, "_on_data_version_change")
assert hasattr(PollingStrategy, "detach_activity_waiter")
print("simplebroker watcher embedder hooks available")
PY
```

Do not start Weft or Taut dependency-floor branches until this published check
passes.

## Phase B: Weft refactor after SimpleBroker publish

Why Weft first: Taut's watcher says it is vendored/adapted from Weft. Refactor
the source implementation first so Taut can copy the simpler shape instead of
inventing another variant.

### B0. Prepare Weft branch and dependency floor

Repo:

- `/Users/van/Developer/weft`

Files to inspect:

- `weft/core/tasks/multiqueue_watcher.py`
- `tests/tasks/test_multiqueue_watcher.py`
- `tests/core/test_manager.py`
- `tests/core/test_queue_wait.py`
- `pyproject.toml`
- `uv.lock`
- `CHANGELOG.md`
- `README.md` testing and release sections
- `docs/specifications/04-SimpleBroker_Integration.md`
- `docs/plans/2026-05-05-simplebroker-multiqueue-waiter-integration-plan.md`

Dependency change:

- In `pyproject.toml`, raise `simplebroker>=5.0.2` to
  `simplebroker>=5.1.0`.
- Update `uv.lock` with the repo's toolchain:

```bash
cd /Users/van/Developer/weft
. ./.envrc
uv lock --upgrade-package simplebroker
```

Important: Weft's `.envrc` may put `../simplebroker` on `PYTHONPATH`. That is
useful for local integration while developing, but final dependency verification
must use the published `simplebroker==5.1.0` from a clean environment.

### B1. Red tests and code-shape gates

Modify:

- `tests/tasks/test_multiqueue_watcher.py`

Add or update tests only where behavior is not already covered.

Required test/gate signals:

1. Existing multi-queue watcher tests still process real broker messages.
   Do not replace them with mocks.
2. Add one code-shape assertion or architecture test that prevents the retry-loop
   clone from returning:

   ```python
   from weft.core.tasks.multiqueue_watcher import MultiQueueWatcher

   def test_multi_queue_watcher_uses_base_retry_loop() -> None:
       assert "_run_with_retries" not in MultiQueueWatcher.__dict__
   ```

   This is acceptable because the whole point of the phase is deleting a clone.
   Keep it narrow. Do not add broad tests that assert full source text.

3. Add a behavior test, or extend an existing one, proving a multi-queue waiter
   returned by `create_activity_waiter_for_queues(...)` is passed through the
   inherited SimpleBroker strategy startup. The test may monkeypatch only
   `weft.core.tasks.multiqueue_watcher.create_activity_waiter_for_queues`, because
   that is the backend hook boundary. It must still construct a real
   `MultiQueueWatcher` with real `Queue` objects and a temp broker target.

4. Add a reset/ownership test proving `_reset_multi_activity_waiter()` no longer
   touches `self._strategy._activity_waiter` directly. Prefer behavior:
   - install a fake waiter through the watcher hook,
   - call `_reset_multi_activity_waiter()`,
   - assert the fake waiter is closed once,
   - assert `watcher._strategy.uses_native_activity() is False`.

Also run this grep gate after implementation:

```bash
rg -n "def _run_with_retries|_strategy\\._activity_waiter" weft/core/tasks/multiqueue_watcher.py
```

Expected final output: no matches.

### B2. Implement Weft watcher refactor

Modify:

- `weft/core/tasks/multiqueue_watcher.py`

Required changes:

1. Delete `_start_strategy_for_configured_queues()`.
2. Delete `_run_with_retries()`.
   This is not optional. The point of the phase is that Weft inherits
   `BaseWatcher._run_with_retries()` from SimpleBroker. Do not replace the old
   clone with a smaller `_start_strategy()` override unless Weft has behavior
   that cannot be expressed with `_create_activity_waiter()` or
   `_on_data_version_change()`.
3. Inspect `_on_data_version_change(queue)` if present. If it only calls
   `queue.refresh_last_ts()`, delete it and inherit SimpleBroker's default. Keep
   it only if Weft adds behavior that is not present in SimpleBroker.
4. Add:

   ```python
   def _create_activity_waiter(self, queue: Queue) -> Any | None:
       del queue
       return self._ensure_multi_activity_waiter()
   ```

   Use the exact return type that fits the current file imports. If Weft imports
   `ActivityWaiter` from `simplebroker.ext`, use `ActivityWaiter | None`;
   otherwise `Any | None` is acceptable because the existing code stores waiter
   objects as `Any`.

5. Change `_reset_multi_activity_waiter()`:

   - Remove:

     ```python
     if getattr(self._strategy, "_activity_waiter", None) is waiter:
         self._strategy._activity_waiter = None
     ```

   - Replace with:

     ```python
     self._strategy.detach_activity_waiter(expected=waiter)
     ```

   - Then close `waiter` as today.

   Do not leave any direct read or write of `self._strategy._activity_waiter` in
   the file. The grep gate below is the enforceable check for this rule.

6. Do not change Weft's queue scheduling, active queue discovery, priority,
   reserve/read/peek modes, or `wait_for_activity(timeout)` logic.

7. Update comments that currently mention SimpleBroker 3.3.2 lacking a hook.
   Replace them with a note that the workaround is gone because SimpleBroker now
   exposes the hook.

### B3. Weft verification

Run focused checks:

```bash
cd /Users/van/Developer/weft
. ./.envrc
./.venv/bin/python -m pytest tests/tasks/test_multiqueue_watcher.py -q
./.venv/bin/python -m pytest tests/core/test_queue_wait.py -q
./.venv/bin/python -m pytest tests/core/test_manager.py -k "activity_waiter or multiqueue or watcher" -q
rg -n "def _run_with_retries|_strategy\\._activity_waiter" weft/core/tasks/multiqueue_watcher.py
```

The `rg` command must exit with no matches. If `rg` exits `1` because no matches
were found, that is success.

Run broader checks:

```bash
./.venv/bin/python -m pytest
./.venv/bin/python bin/pytest-pg --all
./.venv/bin/ruff check weft tests
./.venv/bin/ruff format --check weft tests
./.venv/bin/mypy weft extensions/weft_docker extensions/weft_macos_sandbox extensions/weft_microsandbox
uv build
```

Clean published-dependency smoke:

```bash
python -m venv /tmp/weft-simplebroker-5.1.0-check
/tmp/weft-simplebroker-5.1.0-check/bin/python -m pip install -U pip
/tmp/weft-simplebroker-5.1.0-check/bin/python -m pip install "simplebroker==5.1.0"
/tmp/weft-simplebroker-5.1.0-check/bin/python -m pip install -e "/Users/van/Developer/weft[dev]"
/tmp/weft-simplebroker-5.1.0-check/bin/python - <<'PY'
from simplebroker import __version__
from weft.core.tasks.multiqueue_watcher import MultiQueueWatcher

assert __version__ == "5.1.0"
assert "_run_with_retries" not in MultiQueueWatcher.__dict__
print("weft uses published SimpleBroker watcher hooks")
PY
```

If the clean smoke fails because optional Weft dev extras are not installable in
a throwaway environment, run an equivalent clean environment check that imports
`weft.core.tasks.multiqueue_watcher` with published SimpleBroker and document
the limitation.

### B4. Weft docs, changelog, and release

Modify as needed:

- `CHANGELOG.md`
- `README.md`
- `docs/specifications/04-SimpleBroker_Integration.md`

Changelog entry:

- State that Weft now requires `simplebroker>=5.1.0`.
- State that `MultiQueueWatcher` uses SimpleBroker's watcher lifecycle hooks
  instead of cloning the retry loop.
- Do not claim user-visible queue semantics changed.

Release:

```bash
cd /Users/van/Developer/weft
uv run python bin/release.py --dry-run
uv run python bin/release.py --version <next-weft-version>
```

Choose `<next-weft-version>` according to Weft's normal SemVer policy. This is
likely a patch release if no public Weft API changes, but the dependency floor
may justify a minor release if Weft treats dependency floors as minor.

## Phase C: Taut refactor after Weft

### C0. Prepare Taut branch and dependency floor

Repo:

- `/Users/van/Developer/taut`

Files to inspect:

- `taut/watcher.py`
- `tests/test_watcher.py`
- `tests/test_shared_contract.py`
- `pyproject.toml`
- `taut/_constants.py`
- `CHANGELOG.md`
- `README.md`
- `docs/implementation/04-taut-architecture.md`
- `docs/specs/02-taut-core.md`

Dependency change:

- In `pyproject.toml`, raise `simplebroker>=5.0.2` to
  `simplebroker>=5.1.0`.
- Taut currently has no `uv.lock` in the inspected checkout. If that changes,
  update the lock with `uv lock --upgrade-package simplebroker`.

### C1. Red tests and code-shape gates

Modify:

- `tests/test_watcher.py`

Required test/gate signals:

1. Keep existing external CLI watcher tests unchanged. In particular,
   `test_live_watcher_receives_message_from_cli_subprocess` must continue to
   use `run_cli`; do not turn it into an in-process shortcut.
2. Add a narrow code-shape test:

   ```python
   from taut.watcher import MultiQueueWatcher

   def test_multi_queue_watcher_uses_base_retry_loop() -> None:
       assert "_run_with_retries" not in MultiQueueWatcher.__dict__
   ```

3. Add or keep a behavior test proving a watched message written through a
   subprocess still wakes the live watcher. The current test at
   `tests/test_watcher.py:213` already does this. Do not weaken it.
4. Add a reset/ownership behavior test similar to Weft's if no current test
   covers it:
   - create a watcher,
   - force or monkeypatch `create_activity_waiter_for_queues` to return a fake
     waiter,
   - start strategy via the inherited hook,
   - call `_reset_multi_activity_waiter()`,
   - assert the fake waiter closed once,
   - assert the strategy no longer uses native activity.

Grep gate after implementation:

```bash
rg -n "def _run_with_retries|_strategy\\._activity_waiter" taut/watcher.py
```

Expected final output: no matches.

### C2. Implement Taut watcher refactor

Modify:

- `taut/watcher.py`

Required changes:

1. If the vendored/adapted header already records a source Weft commit, update
   it to the Weft commit that contains the new SimpleBroker hook refactor. If
   the header does not record a source commit today, do not invent one.
2. Delete `_start_strategy_for_configured_queues()`.
3. Delete `_run_with_retries()`.
   This is not optional. Taut should inherit SimpleBroker's retry loop through
   the vendored Weft-style watcher. Do not keep a local retry-loop clone, and do
   not introduce a local `_start_strategy()` override unless Taut needs behavior
   that cannot be represented by `_create_activity_waiter()` plus
   `_on_data_version_change()`.
4. Apply the same rule as Weft B2.3 to
   `MultiQueueWatcher._on_data_version_change(queue)` (`taut/watcher.py:321`):
   after the refactor it only duplicates the new SimpleBroker default
   (`queue.refresh_last_ts()`), so delete it.
   `TautWatcher._on_data_version_change()` (`taut/watcher.py:694`) stays: it
   refreshes memberships, and its `super()` call resolves to the new base
   default once the middle copy is gone. Preserve that behavior.
5. Add:

   ```python
   def _create_activity_waiter(self, queue: Queue) -> Any | None:
       del queue
       return self._ensure_multi_activity_waiter()
   ```

6. Change `_reset_multi_activity_waiter()` to call:

   ```python
   self._strategy.detach_activity_waiter(expected=waiter)
   ```

   Then close the waiter as today.
   Do not leave any direct read or write of `self._strategy._activity_waiter` in
   the file. The grep gate below is the enforceable check for this rule.

7. Do not change Taut cursor behavior, poison-message handling, membership
   refresh, notification queue behavior, or thread filtering.

### C3. Taut verification

Run focused checks:

```bash
cd /Users/van/Developer/taut
uv run pytest tests/test_watcher.py -q -n 0
uv run pytest tests/test_shared_contract.py -q
rg -n "def _run_with_retries|_strategy\\._activity_waiter" taut/watcher.py
```

The `rg` command must have no matches.

Run full project gates:

```bash
uv run pytest
uv run ./bin/pytest-pg --fast
uv run ruff check taut tests bin extensions/taut_pg/taut_pg extensions/taut_pg/tests
uv run ruff format --check taut tests bin extensions/taut_pg/taut_pg extensions/taut_pg/tests
uv run --extra dev mypy taut tests bin/release.py extensions/taut_pg/taut_pg extensions/taut_pg/tests --config-file pyproject.toml
uv build
uv build extensions/taut_pg
```

Clean published-dependency smoke:

```bash
python -m venv /tmp/taut-simplebroker-5.1.0-check
/tmp/taut-simplebroker-5.1.0-check/bin/python -m pip install -U pip
/tmp/taut-simplebroker-5.1.0-check/bin/python -m pip install "simplebroker==5.1.0"
/tmp/taut-simplebroker-5.1.0-check/bin/python -m pip install -e "/Users/van/Developer/taut[dev]"
/tmp/taut-simplebroker-5.1.0-check/bin/python - <<'PY'
from simplebroker import __version__
from taut.watcher import MultiQueueWatcher

assert __version__ == "5.1.0"
assert "_run_with_retries" not in MultiQueueWatcher.__dict__
print("taut uses published SimpleBroker watcher hooks")
PY
```

### C4. Taut docs, changelog, and release

Modify as needed:

- `CHANGELOG.md`
- `README.md`
- `docs/implementation/04-taut-architecture.md`
- `docs/specs/02-taut-core.md`

Changelog entry:

- State that Taut now requires `simplebroker>=5.1.0`.
- State that the vendored Weft-style watcher now uses SimpleBroker's watcher
  lifecycle hooks instead of cloning the retry loop.
- Do not claim chat semantics changed.

Release:

```bash
cd /Users/van/Developer/taut
uv run python bin/release.py --dry-run
uv run python bin/release.py --version <next-taut-version>
```

Taut is GitHub-release only in the inspected README. Do not add PyPI workflow
changes as part of this plan.

## Cross-repo acceptance criteria

SimpleBroker:

1. `BaseWatcher._run_with_retries()` no longer contains the strategy startup
   block inline. It calls `_start_strategy()`.
2. `BaseWatcher` exposes `_start_strategy()`, `_create_activity_waiter(queue)`,
   and `_on_data_version_change(queue)`.
3. `PollingStrategy.detach_activity_waiter()` exists, returns without closing,
   supports an `expected=` guard, and clears native waiter state.
4. `PollingStrategy.start()` does not close the same activity waiter object it
   is about to install.
5. Watcher crash recovery behavior is unchanged.
6. README and changelog document the embedder hooks under unreleased or the new
   release section, never under an old release.
7. SimpleBroker is published and clean-environment import checks prove the new
   API is available.

Weft:

1. `weft/core/tasks/multiqueue_watcher.py` has no `_run_with_retries()` override.
2. It has no `self._strategy._activity_waiter` reach-in.
3. It overrides `_create_activity_waiter()` to return the multi-queue waiter.
4. Existing message-processing, queue waiting, and manager tests still pass.
5. `pyproject.toml` floors SimpleBroker at the published version carrying the
   hooks.

Taut:

1. `taut/watcher.py` has no `_run_with_retries()` override.
2. It has no `self._strategy._activity_waiter` reach-in.
3. It overrides `_create_activity_waiter()` to return the multi-queue waiter.
4. Existing subprocess watcher tests still use the real CLI helper and pass.
5. Membership refresh and cursor behavior remain unchanged.
6. `pyproject.toml` floors SimpleBroker at the published version carrying the
   hooks.

## Rollback plan

Before SimpleBroker publish:

- Revert the SimpleBroker branch. Downstream repos remain unchanged.

After SimpleBroker publish, before Weft/Taut releases:

- Publish a SimpleBroker patch release if the hooks are broken but backwards
  compatible to fix.
- If the API shape is fundamentally wrong, publish a deprecation note and do
  not move Weft/Taut floors to that version. Design a replacement in a new plan.

After Weft or Taut release:

- Roll forward in the affected downstream repo. Do not lower the dependency
  floor unless the published SimpleBroker release is known bad and a fixed
  SimpleBroker version is not yet available.
- If a downstream refactor broke multi-queue behavior, reintroduce the local
  `_run_with_retries()` override temporarily only as a hotfix, with a follow-up
  issue to return to the hook API. Do not leave both designs in place.

## Stop conditions

Stop and ask for review if any of these happen:

- Implementing SimpleBroker hooks requires changing backend plugin APIs.
- The SimpleBroker test suite needs existing watcher contention or external CLI
  tests weakened.
- Weft or Taut needs new multi-queue policy to use the hook.
- `_run_with_retries()` starts getting rewritten over `_retry`.
- The plan starts adding a SimpleBroker `MultiQueueWatcher`.
- Clean-environment downstream smoke tests import the sibling SimpleBroker
  checkout instead of the published package and you cannot isolate them.

## Self-review after drafting

I reviewed this plan against the stated goal: keep SimpleBroker at the same or
lower complexity while giving embedders a nicer API.

Issues found and fixed in this version:

1. The first draft direction risked coupling this to an `_retry` rewrite. Fixed:
   `_run_with_retries()` remains the outer crash-recovery loop.
2. `_create_activity_waiter()` alone was not enough for Taut. Fixed: the plan
   includes `_on_data_version_change(queue)` because Taut refreshes membership
   from the data-version callback.
3. The detach API was ambiguous about ownership. Fixed: it now returns the waiter
   without closing, supports `expected=`, and explicitly says which state is
   cleared.
4. The same-waiter `PollingStrategy.start()` close/reinstall problem could have
   remained latent. Fixed: the plan requires red tests and implementation for
   that exact invariant.
5. Downstream order could have made Taut diverge from its source. Fixed: Weft is
   sequenced before Taut, with a note that urgent Taut work must be reconciled.
6. Test guidance risked over-mocking. Fixed: tests must use real queues and temp
   brokers, with fakes only for the `ActivityWaiter` protocol.
7. Release sequencing could have been hand-wavy. Fixed: SimpleBroker publish and
   clean-environment checks are mandatory before downstream dependency-floor
   changes.

Third-party Grok review follow-up:

1. Grok said `_start_strategy()` was too easy to implement incorrectly from
   bullets alone. Fixed: the plan now includes the full intended method
   skeleton, including `queue = self._get_queue_for_data_version()`, the
   startup `queue.refresh_last_ts()` prime, the callback calling
   `_on_data_version_change(q)`, `_create_activity_waiter(queue)`, and stop
   checks.
2. Grok said the fake-only waiter tests could miss the real cached
   `Queue.create_activity_waiter()` edge. Fixed: A1 now requires a real
   `QueueWatcher`/real temp broker test that monkeypatches only the backend
   activity-waiter hook and calls `_start_strategy()` twice through the Queue
   cache path.
3. Grok said `PollingStrategy.start()` dropping `self.close()` could leak any
   cleanup that `close()` performs. Fixed: A2 now requires checking the current
   `close()` body and preserving any future cleanup if `close()` grows before
   implementation.
4. Grok said same-waiter restart semantics were unclear. Fixed: the target API
   now states that `start()` is lifecycle initialization, not a live waiter
   hot-swap API, and explains why native waiter hints are cleared on detach.
5. Grok said Test 2 did not name the queue source. Fixed: A3 now says the queue
   passed to `_create_activity_waiter()` comes from
   `_get_queue_for_data_version()` and tells the implementer to assert the known
   watched queue name.
6. Grok said the data-version hook test could use the wrong handles. Fixed: A3
   now requires the watcher and writer `Queue` to share the same `broker_target`
   path.
7. Grok said downstream deletion of `_run_with_retries()` and the private
   `_activity_waiter` reach-in was missing. That was a false positive: B2 and C2
   already required both deletions, and the grep gates enforced them. To prevent
   the same misread, those phases now explicitly say the deletions are not
   optional and that downstream should not replace the clone with a smaller
   `_start_strategy()` override unless the hook API cannot express the behavior.
8. Grok said the test command was truncated. That was a false positive from the
   review prompt/output, not from this file. The relevant pytest commands in A1,
   A3, and A7 are complete fenced commands with full `-k` expressions.
9. Grok said release/version coupling was ambiguous. That was mostly already
   covered in A8, B0, C0, and the clean-environment smoke checks. The top-level
   release-order section now repeats the expected `5.1.0` version boundary and
   downstream `simplebroker>=5.1.0` floors so readers see it before the task
   details.

Remaining tradeoff:

- The plan adds protected hooks rather than a larger public class. This is the
  right tradeoff for now. It removes downstream loop clones without making
  SimpleBroker own embedder policy.
