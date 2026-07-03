# Watch `--after` and PG Rename Lock Fix Plan

Date: 2026-07-02

Status: implemented
Verified implemented at HEAD (5a59d52) during the 2026-07-03
checkpoint/lock plan's Task 0; watcher and PG rename halves both
confirmed.

Owner: SimpleBroker

## Purpose

Fix two confirmed correctness bugs:

1. `watch --after` ignores the timestamp filter in consume mode. A consume-mode
   watcher started at checkpoint `X` can claim and deliver rows with
   `ts <= X`, including the boundary row. This violates the strict
   `after_timestamp` contract: only `ts > X` is eligible. This is unintended
   consumption and reprocessing, not silent message destruction: the handler
   does receive the incorrectly claimed messages.
2. Postgres `rename_queue()` has the same ABBA lock inversion that was already
   fixed for `broadcast()`. Writers take the singleton `meta` row lock while
   allocating a timestamp, then need the `messages` table. Rename currently
   takes the `messages` table first and later updates `meta.alias_version` when
   aliases are retargeted. With aliases present, concurrent writer and rename
   can deadlock. `40P01` is retryable today, so the current user impact is a
   transient stall and retry, not data loss.

This plan assumes the implementer is a skilled Python developer but new to this
repository. Follow the tasks in order. Use red-green TDD. Keep the changes
surgical. Do not redesign watcher delivery, queue APIs, alias metadata, or
backend locking.

## Locked Decisions

Do not reopen these unless a failing test proves the plan cannot work.

### Watcher Semantics

- `after_timestamp` is an exclusive lower bound: only messages with
  `ts > after_timestamp` are eligible.
- The boundary message is excluded.
- Peek mode and consume mode must use the same eligibility predicate.
- Consume mode may still claim before the handler runs. Do not change delivery
  guarantees in this fix.
- Do not add a public `after_timestamp` parameter to `Queue.read_one()`. The
  queue already has `Queue.stream_messages(peek=False, after_timestamp=...)`,
  and the watcher can use that. Expanding `read_one()` is unnecessary surface
  area.
- Do not update README checkpoint examples as part of this fix unless a test
  shows the docs are false after the code change. The README already promotes
  strict checkpoint behavior; the implementation is wrong.

### Postgres Rename Lock Semantics

- Use the same lock order as `prepare_broadcast()`: singleton `meta` row first,
  then `messages` table.
- Use the existing `pg_sql.LOCK_LAST_TS_ROW` statement for the singleton row.
  The name mentions `last_ts`, but `SELECT ... FOR UPDATE` locks the whole
  singleton row, including `alias_version`.
- Keep the fix in the Postgres plugin's rename preparation path. Do not move
  alias version updates out of `BrokerCore.rename_queue()`.
- Do not special-case `retarget_aliases=False`. `prepare_queue_operation()` does
  not know whether aliases will be retargeted, and avoiding one row lock is not
  worth splitting the protocol.
- Do not add advisory locks for rename. The `messages` table lock already
  provides the serialization rename needs; the bug is order, not missing a new
  lock type.

## Repository Primer

### Runtime Files

- `simplebroker/watcher.py`
  - `QueueWatcher` is the class behind watch behavior.
  - `_last_seen_ts` is initialized from `after_timestamp`.
  - `_has_pending_messages()` already passes `_last_seen_ts` into
    `Queue.has_pending()`, but this is only a precheck.
  - `_process_peek_messages()` passes `after_timestamp=self._last_seen_ts`.
  - `_consume_one_message()` currently calls `read_one()` with no timestamp
    filter.
  - `_consume_all_messages()` currently calls
    `stream_messages(peek=False, all_messages=True)` with no timestamp filter.

- `simplebroker/sbqueue.py`
  - `Queue.stream_messages()` already supports `peek=False` with
    `after_timestamp`.
  - `Queue.read_one()` supports only `exact_timestamp`, not `after_timestamp`.
    Leave it that way.

- `simplebroker/db.py`
  - `BrokerCore.rename_queue()` starts the transaction, calls backend
    `prepare_queue_operation(operation="rename")`, renames messages, retargets
    aliases, then bumps alias version if aliases changed.
  - `_do_write_transaction()` allocates timestamps inside the write
    transaction. On Postgres, that touches the singleton `meta` row before the
    insert touches `messages`.
  - `_increment_alias_version_locked()` writes `meta.alias_version`.

- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
  - `prepare_queue_operation(operation="rename")` currently takes only
    `LOCK_RENAME_SCOPE`, which is the `messages` table lock.
  - `prepare_broadcast()` already takes `LOCK_LAST_TS_ROW` before
    `LOCK_BROADCAST_SCOPE`. Mirror this order for rename.

- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
  - `LOCK_LAST_TS_ROW = "SELECT last_ts FROM meta WHERE singleton = TRUE FOR UPDATE"`
  - `LOCK_RENAME_SCOPE = "LOCK TABLE messages IN SHARE ROW EXCLUSIVE MODE"`

### Test Files

- `tests/test_watcher.py`
  - Existing watcher coverage lives here.
  - The current `after_timestamp` watcher tests are peek-only. Add consume-mode
    tests beside them.
  - The file already defines `MessageCollector`, imports `wait_for_condition`,
    and uses real queues/databases. Use the same style.

- `tests/test_queue_api_comprehensive.py`
  - Already tests `Queue.stream_messages()` filters. Use it as confidence that
    the lower API works. Do not duplicate those tests unless you change the
    lower API, which this plan says not to do.

- `extensions/simplebroker_pg/tests/test_pg_queue_rename.py`
  - Existing Postgres rename tests live here.
  - Add the lock-order tests here.

### Tooling

Run all commands from the repo root unless the command says otherwise.

- Use `uv run pytest ...` for the normal test suite.
- Use `uv run ./bin/pytest-pg ...` for Postgres-backed behavior. This starts a
  temporary Docker Postgres container and wires `SIMPLEBROKER_PG_TEST_DSN`.
- Do not run bare `python` for Postgres probes. The repo may otherwise import a
  globally installed `simplebroker_pg` instead of the editable extension.
- Use `-n0 -v` while developing one test file. The default suite runs through
  pytest-xdist and is noisier for red-green work.
- Use `rg` for code search.

## Engineering Rules

- DRY: use the existing filtered `stream_messages()` path. Do not copy SQL
  predicates into the watcher.
- YAGNI: no new public flags, APIs, config options, lock abstractions, or
  migrations.
- Red-green TDD: each task starts with a test that fails on current `main`.
- Test real behavior. Do not mock `Queue`, `BrokerDB`, sqlite3, psycopg, or the
  Postgres lock manager.
- A tiny recording runner is allowed only for a narrow statement-order unit
  test, and only if paired with a real `pytest-pg` integration test.
- Avoid sleeps as assertions. Use helper polling with deadlines, thread events,
  and joins. Short waits inside a polling helper are acceptable.
- Keep comments rare. Add comments only for concurrency invariants or surprising
  choices, such as why the `last_ts` lock also protects `alias_version`.
- Keep source changes small enough that a reviewer can verify the lock and
  filter reasoning in one pass.

## Task 0: Required Reading

Before editing, read these exact spans:

- `simplebroker/watcher.py`
  - `QueueWatcher.__init__`
  - `QueueWatcher._has_pending_messages`
  - `QueueWatcher._process_peek_messages`
  - `QueueWatcher._consume_one_message`
  - `QueueWatcher._consume_all_messages`
- `simplebroker/sbqueue.py`
  - `Queue.read_one`
  - `Queue.read_generator`
  - `Queue.stream_messages`
- `tests/test_watcher.py`
  - the existing `test_after_parameter_in_peek_mode`
  - the existing `test_after_timestamp_database_filtering`
- `simplebroker/db.py`
  - `_do_write_transaction`
  - `rename_queue`
  - `_increment_alias_version_locked`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
  - `prepare_queue_operation`
  - `prepare_broadcast`
  - `prepare_alias_mutation`
- `extensions/simplebroker_pg/tests/test_pg_queue_rename.py`

Stop after this reading if any of the code has materially changed from the
assumptions above. Update the plan before coding.

## Task 1: Add Red Tests for Consume-Mode Watcher `after_timestamp`

Files to touch:

- `tests/test_watcher.py`

Goal: prove consume-mode watchers apply the same strict lower bound as peek
watchers.

### Test Design

Use real `BrokerDB`/`Queue`/`QueueWatcher` through the existing `broker`,
`broker_target`, and `MessageCollector` fixtures/helpers in `tests/test_watcher.py`.

Do not start a long-running CLI `broker watch` process for this task. The bug is
inside `QueueWatcher`, and a CLI process adds timing and subprocess noise
without increasing coverage.

Use `_drain_queue()` directly for the new tests. This is a private method, but
it is the smallest deterministic seam for the bug:

- the public run loop calls `_drain_queue()` during initial drain;
- the bug is that `_drain_queue()` dispatches to consume helpers that do not
  pass the filter;
- direct drain avoids negative timeout assertions.

### Add These Tests

Add the tests near the existing after-timestamp watcher tests.

```python
    def test_after_timestamp_filters_consume_single_message_drain(
        self, broker, broker_target
    ):
        """Consume mode must not claim rows at or before the checkpoint."""
        broker.write("test_queue", "old-1")
        broker.write("test_queue", "boundary")
        boundary_ts = list(broker.peek_generator("test_queue"))[-1][1]
        broker.write("test_queue", "new-1")

        collector = MessageCollector()
        watcher = QueueWatcher(
            "test_queue",
            collector.handler,
            db=broker_target,
            peek=False,
            after_timestamp=boundary_ts,
        )

        try:
            watcher._drain_queue()
        finally:
            watcher.stop(join=False)

        messages = collector.get_messages()
        assert len(messages) == 1
        assert messages[0][0] == "new-1"
        assert messages[0][1] > boundary_ts
        remaining = list(broker.peek_generator("test_queue", with_timestamps=False))
        assert remaining == ["old-1", "boundary"]
```

Then add the batch variant:

```python
    def test_after_timestamp_filters_consume_batch_drain(
        self, broker, broker_target
    ):
        """Batch consume mode must use the same exclusive after filter."""
        broker.write("test_queue", "old-1")
        broker.write("test_queue", "boundary")
        boundary_ts = list(broker.peek_generator("test_queue"))[-1][1]
        broker.write("test_queue", "new-1")
        broker.write("test_queue", "new-2")

        collector = MessageCollector()
        watcher = QueueWatcher(
            "test_queue",
            collector.handler,
            db=broker_target,
            peek=False,
            after_timestamp=boundary_ts,
            batch_processing=True,
        )

        try:
            watcher._drain_queue()
        finally:
            watcher.stop(join=False)

        messages = collector.get_messages()
        assert [body for body, _ in messages] == ["new-1", "new-2"]
        assert all(ts > boundary_ts for _, ts in messages)
        remaining = list(broker.peek_generator("test_queue", with_timestamps=False))
        assert remaining == ["old-1", "boundary"]
```

The invariant is more important than the exact test names:

- old rows remain pending;
- the boundary row remains pending;
- only rows with `ts > boundary_ts` reach the handler;
- those newer rows are consumed.

### Red Gate

Run:

```bash
uv run pytest tests/test_watcher.py -n0 -v -k "after_timestamp_filters_consume"
```

Expected pre-fix failure:

- single drain test receives `"old-1"` instead of `"new-1"`;
- batch drain test receives and consumes `"old-1"` and `"boundary"` too.

If either test passes before code changes, the test is not hitting the bug.
Stop and fix the test.

## Task 2: Fix Consume-Mode Watcher Filtering

Files to touch:

- `simplebroker/watcher.py`

Goal: route consume-mode watcher reads through the existing filtered stream
path.

### Implementation

Add a small private helper on `QueueWatcher`:

```python
    def _after_timestamp_filter(self) -> int | None:
        return self._last_seen_ts if self._last_seen_ts > 0 else None
```

Use it in peek and consume paths. This keeps the lower-bound expression in one
place and avoids three almost-identical call sites.

Change `_consume_one_message()` from `read_one()` to `stream_messages()`:

```python
        for body, ts in self._queue_obj.stream_messages(
            peek=False,
            all_messages=False,
            after_timestamp=self._after_timestamp_filter(),
            commit_interval=1,
        ):
            self._try_dispatch_message(body, ts)
            return True
        return False
```

Change `_consume_all_messages()` so the `stream_messages()` call includes the
same filter:

```python
            for body, ts in self._queue_obj.stream_messages(
                peek=False,
                all_messages=True,
                after_timestamp=self._after_timestamp_filter(),
                commit_interval=1,
            ):
```

Do not add Python-side `if ts <= self._last_seen_ts: continue` filtering. That
would still claim the wrong rows before dropping them. The filter must reach the
database claim query.

Do not update `_last_seen_ts` in consume mode as part of this fix. Consumed rows
cannot be re-read, and changing checkpoint advancement on handler success vs.
failure is a separate delivery-semantics decision.

### Green Gate

Run:

```bash
uv run pytest tests/test_watcher.py -n0 -v -k "after_timestamp_filters_consume or after_parameter_in_peek_mode or after_timestamp_database_filtering"
uv run pytest tests/test_queue_api_comprehensive.py -n0 -v -k "stream_messages"
```

Expected:

- the new consume tests pass;
- existing peek `after_timestamp` tests still pass;
- existing `Queue.stream_messages()` tests still pass.

## Task 3: Add Red Tests for PG Rename Lock Order

Files to touch:

- `extensions/simplebroker_pg/tests/test_pg_queue_rename.py`

Goal: prove rename takes the singleton `meta` row before the `messages` table.

### Test 3A: Narrow Statement-Order Unit Test

Add a tiny recording runner in `test_pg_queue_rename.py` or as a local class
inside the test. It only needs `run()` and `schema`. It must not fake database
results for behavior tests. This is acceptable because the tested contract is
the order of statements sent by `prepare_queue_operation()`.

Suggested shape:

```python
class _RecordingRunner:
    schema = "test_schema"

    def __init__(self) -> None:
        self.calls: list[tuple[str, bool]] = []

    def run(
        self,
        sql: str,
        params: tuple[object, ...] = (),
        *,
        fetch: bool = False,
    ) -> list[tuple[object, ...]]:
        del params
        self.calls.append((sql, fetch))
        return []
```

Then test:

```python
def test_postgres_prepare_rename_locks_meta_before_messages(
    pg_plugin: BackendPlugin,
) -> None:
    runner = _RecordingRunner()

    pg_plugin.prepare_queue_operation(runner, operation="rename", queue="old")

    assert runner.calls == [
        (pg_plugin.sql.LOCK_LAST_TS_ROW, True),
        (pg_plugin.sql.LOCK_RENAME_SCOPE, False),
    ]
```

This test should fail pre-fix because the current call list contains only
`LOCK_RENAME_SCOPE`.

### Test 3B: Real Postgres Lock-Manager Test

Add one `pytest-pg` integration test that exercises the production Postgres
plugin hook against a real PostgreSQL lock manager. The purpose is not to force
a deadlock every time. The purpose is to prove the lock-order invariant:

```text
When a writer already holds the singleton meta row, rename preparation must
wait before it has acquired the messages table lock.
```

Use real psycopg connections and a real `PostgresRunner`. This test is less
flaky than a full `rename_queue()` deadlock test because it targets the exact
lock-preparation hook and uses explicit thread events.

Suggested helper:

```python
def _lock_messages_nowait(pg_dsn: str, pg_schema: str) -> bool:
    with psycopg.connect(pg_dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {quote_ident(pg_schema)}, public")
            try:
                cur.execute("LOCK TABLE messages IN ROW EXCLUSIVE MODE NOWAIT")
            except psycopg.errors.LockNotAvailable:
                conn.rollback()
                return False
            conn.rollback()
            return True
```

Suggested test flow:

1. Seed production state with `pg_core.write("old", "payload")`. This ensures
   the schema and tables exist.
2. Create `rename_runner = PostgresRunner(pg_dsn, schema=pg_schema)`.
3. Open a raw psycopg transaction and set the schema search path.
4. In that raw transaction, run:

   ```sql
   SELECT last_ts FROM meta WHERE singleton = TRUE FOR UPDATE
   ```

   This simulates a writer after timestamp allocation and before insert.
5. Start a thread that begins a transaction on `rename_runner`, sets a
   `prepare_started` event, calls:

   ```python
   pg_plugin.prepare_queue_operation(
       rename_runner,
       operation="rename",
       queue="old",
   )
   ```

   then sets a `prepare_returned` event and waits on a `release_prepare` event
   before rolling back. The rollback releases any lock held by the prepare
   call.
6. Wait for `prepare_started`.
7. For a bounded polling window, repeatedly:
   - fail if `prepare_returned` is set while the raw writer still holds
     `meta`;
   - fail if `_lock_messages_nowait()` returns `False`.

   Either failure means rename preparation took the `messages` lock before it
   could acquire `meta`.
8. Roll back the raw writer transaction to release `meta`.
9. Set `release_prepare`, join the prepare thread, and shut down
   `rename_runner`.
10. Assert no error was captured from the prepare thread.

Use a deadline, not a bare sleep. Use `time.monotonic()` and a short poll
interval such as `0.01`.

Pre-fix behavior:

- Test 3A fails reliably.
- Test 3B fails because current rename preparation returns and holds the
  `messages` table lock even though the raw writer still holds `meta`.

### Red Gate

Run:

```bash
uv run ./bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_queue_rename.py -q -k "lock_order or meta_before_messages"
```

Expected pre-fix failure:

- statement-order test sees only `LOCK_RENAME_SCOPE`;
- real PG lock-manager test observes either `prepare_returned` or a blocked
  `messages` lock while the raw writer still holds `meta`.

## Task 4: Fix PG Rename Lock Order

Files to touch:

- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`

Goal: make rename's preparation order match writers and broadcast.

### Implementation

In `PostgresBackendPlugin.prepare_queue_operation()`, change the rename branch
from:

```python
        if operation == "rename":
            del queue
            runner.run(pg_sql.LOCK_RENAME_SCOPE)
            return
```

to:

```python
        if operation == "rename":
            del queue
            # The singleton meta row also stores alias_version. Rename may bump
            # it after retargeting aliases, so match writer/broadcast order:
            # meta row first, then messages table.
            runner.run(pg_sql.LOCK_LAST_TS_ROW, fetch=True)
            runner.run(pg_sql.LOCK_RENAME_SCOPE)
            return
```

Keep the comment short. The important fact is that `LOCK_LAST_TS_ROW` locks the
whole singleton row, not only the `last_ts` field.

Do not change `pg_sql.LOCK_RENAME_SCOPE`. Do not add a new lock mode. Do not
move the alias retarget code.

### Green Gate

Run:

```bash
uv run ./bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_queue_rename.py -q
uv run ./bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py -q
```

Expected:

- existing rename tests still pass;
- new lock-order tests pass;
- broadcast behavior is unchanged.

## Task 5: Update Changelog

Files to touch:

- `CHANGELOG.md`

Add entries under the current unreleased section. Keep them factual:

- `watch --after` now applies the strict checkpoint filter in consume mode, not
  only peek mode.
- Postgres `rename_queue()` now takes the singleton meta row before the
  `messages` table, matching writer/broadcast lock order and avoiding a
  retryable `40P01` deadlock when aliases are retargeted.

Do not bump package versions. Release tooling owns versions.

## Task 6: Full Validation Gates

Run the smallest gates first, then the wider gates.

### Targeted Gates

```bash
uv run pytest tests/test_watcher.py -n0 -v -k "after_timestamp_filters_consume or after_parameter_in_peek_mode or after_timestamp_database_filtering"
uv run pytest tests/test_queue_api_comprehensive.py -n0 -v -k "stream_messages"
uv run ./bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_queue_rename.py -q
uv run ./bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py -q
```

### Lint and Type Gates

```bash
uv run ruff format --check simplebroker/watcher.py tests/test_watcher.py extensions/simplebroker_pg/simplebroker_pg/plugin.py extensions/simplebroker_pg/tests/test_pg_queue_rename.py
uv run ruff check simplebroker/watcher.py tests/test_watcher.py extensions/simplebroker_pg/simplebroker_pg/plugin.py extensions/simplebroker_pg/tests/test_pg_queue_rename.py
uv run mypy simplebroker bin/release.py extensions/simplebroker_pg/simplebroker_pg
```

The extension tests are excluded from mypy by config; that is expected. The
gate above type-checks the runtime packages touched by this plan.

### Broader Regression Gates

```bash
uv run pytest
uv run ./bin/pytest-pg
git diff --check
```

`bin/pytest-pg` requires Docker. If Docker is unavailable, report that as a
blocked gate with the exact Docker error.

## Review Checklist for the Implementer

Before sending the PR or final report, verify these invariants by reading the
diff, not only by trusting green tests:

- `QueueWatcher._process_peek_messages()` still passes an `after_timestamp`
  filter.
- Both consume paths pass an `after_timestamp` filter into
  `Queue.stream_messages()`.
- No consume path filters in Python after claiming a row.
- `Queue.read_one()` public signature did not grow.
- Old and boundary rows remain pending in the new watcher tests.
- Postgres rename calls `LOCK_LAST_TS_ROW` before `LOCK_RENAME_SCOPE`.
- The PG lock-order tests cover both statement order and real PostgreSQL lock
  behavior.
- No new sleeps were added as assertions.
- No new dependencies, config flags, migrations, or public APIs were added.
- `CHANGELOG.md` changed, but version numbers did not.

## Suggested Commit Split

Use two or three commits:

1. `fix(watcher): apply after filter in consume mode`
   - `simplebroker/watcher.py`
   - `tests/test_watcher.py`
2. `fix(pg): align rename lock order with writers`
   - `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
   - `extensions/simplebroker_pg/tests/test_pg_queue_rename.py`
3. `docs: note watcher and pg rename fixes`
   - `CHANGELOG.md`

If you prefer one commit for a small PR, that is acceptable. Do not mix
unrelated cleanup into the fix.

## Fresh-Eyes Plan Review

Review this plan before implementation. These are the likely traps:

- A test that only checks `has_pending(after_timestamp)` is insufficient. That
  precheck already used the filter; the bug is in the later claim path.
- A test that filters after handler delivery is insufficient. The wrong rows
  would still be claimed.
- A fake Postgres runner alone is insufficient. Statement order matters, but
  the real lock manager must also be represented.
- A full `rename_queue()` deadlock test is easy to make flaky. The stable
  invariant is lower level: rename preparation must not acquire `messages`
  while another transaction holds `meta`.
- A "fix" that retries harder only hides the PG bug. `40P01` is already
  retryable; the fix is lock order.
- A "fix" that adds new APIs is likely overbuilding. The existing lower-level
  stream API already has the needed filter.

If an implementation attempt starts moving toward a new watcher delivery model,
new queue registry, new backend lock abstraction, or new public API, stop and
return to this plan. That is a materially different direction.
