# Queue Rename API and CLI Plan

Date: 2026-06-18

Status: proposed

Owner: SimpleBroker

## Purpose

Add a small admin operation for renaming a logical queue:

```python
result = broker.rename_queue(
    "jobs.pending",
    "jobs.ready",
    retarget_aliases=True,
)
```

Implementation-wise, a queue is currently the `queue` text label on message
rows. Renaming a queue means retagging all existing messages from the old name
to the new name. It is not a durable queue declaration, because SimpleBroker has
no queue registry table.

This plan covers SQLite, Postgres, Redis, the Python API, and the CLI. It
assumes the implementing engineer is skilled but new to SimpleBroker and may
otherwise overbuild or over-mock. Read the whole plan before coding. Follow the
tasks in order. Use red-green TDD. Keep the implementation boring.

## Locked Decisions

Do not reopen these decisions unless a red test proves the plan cannot work.

### Semantics

Queue rename means:

- Rename every message whose current queue name is `old_queue`.
- Include both pending and claimed messages.
- Preserve message IDs, message bodies, claimed state for SQL backends, and FIFO
  order.
- Reject if `new_queue` already has any broker state.
- Do not merge queues.
- Do not add a `force`, `overwrite`, `dry_run`, `pending_only`, or
  `claimed_only` option.
- Do not add a queue registry table or schema migration.

The old queue can be recreated immediately after rename by any producer writing
to the old name. That is acceptable. This feature renames existing state; it
does not reserve queue names forever.

Missing source behavior:

- If `old_queue` has no messages, return a result with `messages_renamed=0`.
- Do not retarget aliases when no messages were renamed.
- The Python API should not raise for a missing source.
- The CLI should exit `2` for a missing source, matching other "not found" CLI
  behavior.

Target collision behavior:

- If `new_queue` already has any messages, raise `ValueError` before mutation.
- For Redis, "any messages" means pending, claimed, or reserved state.
- Do not consider alias names a collision. Alias names use the `@` CLI prefix
  and already have their own namespace rules.

### Public API Shape

Add a result type:

```python
@dataclass(frozen=True)
class QueueRenameResult:
    old_queue: str
    new_queue: str
    messages_renamed: int
    aliases_retargeted: int

    @property
    def renamed(self) -> bool:
        return self.messages_renamed > 0
```

Add this broker/core method:

```python
class BrokerConnection:
    def rename_queue(
        self,
        old_queue: str,
        new_queue: str,
        *,
        retarget_aliases: bool = True,
    ) -> QueueRenameResult: ...
```

Expose `QueueRenameResult` from `simplebroker.__init__`.

When serializing CLI JSON, include the `renamed` property explicitly. A plain
`dataclasses.asdict(result)` call will not include properties.

Do not add `Queue.rename(...)`. A `Queue` instance represents one queue name. A
method that changes the logical name behind the handle invites state bugs: the
handle would still have `self.name == old_queue` unless it mutated itself. Keep
this as a broker/admin operation.

Do not add a top-level helper. `open_broker(...)` is already the public
cross-queue surface.

### Alias Behavior

Aliases are stored separately from messages.

Default behavior should be `retarget_aliases=True`:

- After renaming `old_queue` to `new_queue`, aliases whose target is `old_queue`
  should point at `new_queue`.
- Alias version metadata must bump only when aliases actually change.
- Alias caches in already-open SQL broker handles must refresh after the version
  bump.

When `retarget_aliases=False`:

- Leave aliases pointing at `old_queue`.
- This may leave aliases pointing at a queue with no messages. That is allowed
  today and should stay allowed.

When `old_queue` has no messages:

- Do not retarget aliases even if aliases point at `old_queue`.
- Rationale: this operation renames existing queue state. Alias-only retargeting
  is a different admin operation and should remain explicit through alias APIs.

Python API queue names are literal queue names. The API should not accept
`@alias` syntax because `@` is not a valid queue-name character.

CLI operands follow existing CLI alias conventions:

- `OLD` may be `@alias`; resolve it to the alias target before calling the API.
- `NEW` may be `@alias`; resolve it to the alias target before calling the API.
- If either alias is undefined, emit the same style of error as existing queue
  commands.
- The JSON payload should show canonical resolved names.

### CLI Shape

Add:

```bash
broker rename OLD NEW [--json] [--no-retarget-aliases]
```

Plain output:

- On success, print nothing and exit `0`.
- On missing source, print nothing and exit `2`.
- On validation, collision, or backend error, emit the existing
  `simplebroker: error: ...` message to stderr and exit `1`.

JSON output:

- On success or missing source, print one JSON object to stdout:

```json
{
  "old_queue": "jobs.pending",
  "new_queue": "jobs.ready",
  "messages_renamed": 3,
  "aliases_retargeted": 2,
  "renamed": true
}
```

- On missing source, print the same shape with zero counts and `"renamed":
  false`, then exit `2`.
- On errors, use the existing JSON error formatter on stderr.

### Concurrency Model

The user-visible guarantee is:

```text
Every message committed to old_queue before the rename transaction takes its
backend write lock is renamed. Writes that happen after the rename commits may
recreate old_queue.
```

SQLite:

- Use `BEGIN IMMEDIATE` through the existing `BrokerCore` transaction path.
- SQLite's writer lock is enough.

Postgres:

- Do not rely on the existing queue advisory lock. Plain writes do not take that
  advisory lock.
- In the rename transaction, lock `messages` with:

```sql
LOCK TABLE messages IN SHARE ROW EXCLUSIVE MODE
```

- This conflicts with concurrent inserts, updates, and deletes while the
  collision check and update run.
- This is acceptable because rename is an admin operation.

Redis:

- Implement the storage transition in one Lua script.
- Redis scripts are atomic, so writes happen before or after the rename, not
  halfway through it.
- Reject rename when the source queue has active reserved messages. Reserved
  batch metadata stores the source queue name; renaming under an active batch
  would break commit, rollback, and stale recovery.
- Treat target reserved state as a collision.

### Watcher and Activity Notifications

Rename is a queue-state change. Watchers waiting on the old queue should wake
and observe no pending messages. Watchers waiting on the new queue should wake
and observe the renamed messages.

Required behavior:

- SQLite: no backend-native notification is needed. The database changes and
  polling/data-version checks remain the correctness path.
- Postgres: after a successful message rename, notify both old and new queue
  names on the existing activity channel. A wildcard notification is optional,
  but queue-specific old and new notifications are required.
- Redis: after a successful message rename, call `_publish(old_queue)` and
  `_publish(new_queue)`. Do not rely on `_publish(None)` because the current
  Redis listener ignores the all-channel message.

Do not wake watchers if no messages were renamed.

### Scope Boundaries

Do not change:

- message ID generation
- read, claim, peek, move, broadcast, delete, dump, load, or vacuum semantics
- queue-name validation rules
- alias graph validation except for retargeting alias targets during rename
- Redis batch reservation design
- Postgres schema version
- SQLite schema version
- package dependency boundaries

Do not add:

- queue registry tables
- migration files
- force merge behavior
- dry-run behavior
- new runtime dependencies
- a fallback loop that renames one message at a time
- mocked backend behavior tests as the main proof

If implementation starts requiring a materially different API, a schema
migration, or a change to delivery semantics, stop and report the problem.

## Repository Primer

Runtime package:

- `simplebroker/`

Public API files:

- `simplebroker/metadata.py`
  - Add `QueueRenameResult` next to `QueueStats`.
- `simplebroker/__init__.py`
  - Export `QueueRenameResult`.
- `simplebroker/_backend_plugins.py`
  - Add `BrokerConnection.rename_queue(...)`.
  - Add SQL backend hooks for renaming messages and retargeting aliases.
- `simplebroker/db.py`
  - Add `BrokerCore.rename_queue(...)`.
  - Keep validation, transaction boundaries, collision checks, and alias-version
    handling here.
- `simplebroker/sbqueue.py`
  - Do not add a rename method.

CLI files:

- `simplebroker/cli.py`
  - Add the parser entry.
  - Add `rename` to `ArgumentProcessor.subcommands`.
  - Add dispatch near `move` and `delete`.
- `simplebroker/commands.py`
  - Add `cmd_rename(...)`.
  - Use `_resolve_alias_name(...)` for CLI operands.
  - Let `cli.main(...)` use `_emit_error(...)` for validation and backend
    exceptions, matching existing command dispatch style.

SQLite files:

- `simplebroker/_sql/sqlite.py`
  - Add SQL constants for message rename and alias retargeting.
- `simplebroker/_sql/__init__.py`
  - Re-export constants only if SQLite helper modules import through this
    compatibility module.
- `simplebroker/_backends/sqlite/maintenance.py`
  - Add helper functions that execute the SQLite updates and immediately read
    `changes()` on the same connection.
- `simplebroker/_backends/sqlite/plugin.py`
  - Delegate backend hooks to the maintenance helpers.

Postgres files:

- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
  - Add count-returning CTEs for message rename and alias retargeting.
  - Add notification SQL or include notifications in the rename CTE.
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
  - Implement the backend hooks.
  - Update `prepare_queue_operation(...)` so `operation == "rename"` locks the
    messages table, not just an advisory queue key.
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
  - Usually no change. Do not force prepared statements for rename unless a
    benchmark later proves this path is hot.

Redis files:

- `extensions/simplebroker_redis/simplebroker_redis/core.py`
  - Add `RedisBrokerCore.rename_queue(...)`.
  - Validate inputs, reject same-name rename, enforce reentrant batch guard,
    call stale-batch recovery, run the Lua script, and publish old/new activity.
- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
  - Add one `RENAME_QUEUE` Lua script.
- `extensions/simplebroker_redis/simplebroker_redis/keys.py`
  - Usually no change. Reuse `RedisKeys.pending`, `claimed`, `reserved`,
    `queues`, `aliases`, `meta`, and ID helpers.

Docs:

- `README.md`
  - Add a short `open_broker(...).rename_queue(...)` example near the advanced
    `open_broker` section that already documents `delete_from_queues`.
  - Add `rename` to the command reference table.
  - Add a short alias note: CLI operands may use `@alias`; the Python API uses
    literal queue names.
- `CHANGELOG.md`
  - Add an entry because this is public API and CLI surface.
- `extensions/simplebroker_pg/README.md`
  - Usually no change unless the backend docs have an API parity list when
    implementation starts.
- `extensions/simplebroker_redis/README.md`
  - Usually no change unless the backend docs have an API parity list when
    implementation starts.

Tests:

- `tests/test_queue_rename.py`
  - New shared API behavior tests marked `pytest.mark.shared`.
- `tests/test_cli_rename.py`
  - CLI behavior tests through the real CLI subprocess helper.
- `extensions/simplebroker_pg/tests/test_pg_queue_rename.py`
  - Postgres-specific integration tests for table locking and activity
    notifications.
- `extensions/simplebroker_redis/tests/test_redis_queue_rename.py`
  - Redis-specific integration tests for Lua invariants, reserved-batch
    rejection, key cleanup, and activity notifications.

Useful existing plans:

- `docs/plans/2026-05-13-targeted-queue-metadata-api-plan.md`
  - Good model for adding broker/core methods and shared tests.
- `docs/plans/2026-05-20-delete-from-queues-plan.md`
  - Good model for backend hooks, Redis Lua, and safety invariants.
- `docs/plans/2026-05-14-simplebroker-redis-second-backend-plan.md`
  - Good model for Redis direct-core behavior and no-mock test design.

Useful external docs to check while implementing:

- Python `dataclasses.dataclass`
- Python `collections.abc.Sequence`
- SQLite `UPDATE`, `changes()`, and transaction behavior
- PostgreSQL explicit locking: `LOCK TABLE`, especially
  `SHARE ROW EXCLUSIVE MODE`
- PostgreSQL `UPDATE ... RETURNING`, CTEs, and `pg_notify`
- Redis `EVAL`, `RENAME`, `EXISTS`, `ZCARD`, `HGETALL`, `HSET`, `SADD`, `SREM`
- redis-py `eval` and Pub/Sub behavior

## Code Style

Follow the existing repository style.

- Python 3.11+ syntax only.
- Use standard library only in the root package.
- Use `from __future__ import annotations` in new Python files.
- Use modern typing: `X | None`, `list[str]`, `dict[str, Any]`.
- Use `collections.abc` for abstract collection imports.
- Keep helper names direct: `rename_queue_messages`,
  `retarget_aliases_to_queue`.
- Keep SQL constants explicit and boring.
- Do not hide backend differences behind a clever generic query builder.
- Keep comments rare. Comment semantic edge cases, especially Redis reserved
  batches and Postgres locking.
- Do not use mocks for behavior coverage. Prefer real `BrokerCore`, `Queue`,
  CLI subprocesses, real Postgres, and real Valkey/Redis.
- Do not add abstractions unless at least two current call sites need them.
- Keep the final diff right-sized. The feature should touch many files because
  it spans backends, but each change should be small.

## Data Flow

Logical flow:

```text
Python API / CLI
      |
      v
BrokerCore.rename_queue(old, new, retarget_aliases=True)
      |
      +-- validate queue names and same-name guard
      +-- open write transaction or Redis Lua script
      +-- reject target collision
      +-- rename pending + claimed message state
      +-- optionally retarget aliases old -> new
      +-- bump alias_version if aliases changed
      +-- notify old and new queue waiters when messages changed
      |
      v
QueueRenameResult
```

SQL transaction shape:

```text
BEGIN IMMEDIATE or backend-equivalent write transaction
  prepare_queue_operation(operation="rename", queue=old)
  if any rows for new:
      rollback and raise ValueError
  update messages set queue = new where queue = old
  if messages_renamed > 0 and retarget_aliases:
      update aliases set target = new where target = old
      if aliases changed:
          write alias_version = time.time_ns()
          refresh local alias cache
COMMIT
```

Redis script shape:

```text
RENAME_QUEUE Lua script
  if old reserved set has entries:
      return ACTIVE_RESERVED
  if new pending/claimed/reserved has entries:
      return TARGET_EXISTS
  count old pending + old claimed
  if count == 0:
      return 0, 0
  move old pending key -> new pending key when non-empty
  move old claimed key -> new claimed key when non-empty
  remove old from queues set
  add new to queues set
  if retarget_aliases:
      HGETALL aliases and HSET targets old -> new
      bump meta.alias_version only when aliases changed
  return message count and alias count
```

Use `HGETALL` for alias retargeting in the Lua script unless profiling proves
alias tables can become huge. Alias count is expected to be small, and this
keeps the change simpler than cursor state or new secondary indexes.

## Implementation Tasks

### Task 1: Add red shared API tests first

Files to create or edit:

- `tests/test_queue_rename.py`
- `tests/conftest.py` only if an existing fixture cannot express the test
  cleanly. Prefer not to touch it.

Write behavior tests through the real `broker` fixture. Mark the module:

```python
pytestmark = [pytest.mark.shared]
```

Tests to write before implementation:

1. `test_rename_queue_moves_pending_and_claimed_messages`
   - Write three messages to `old`.
   - Claim/read one message so it becomes claimed.
   - Rename `old` to `new`.
   - Assert result fields:
     - `old_queue == "old"`
     - `new_queue == "new"`
     - `messages_renamed == 3`
     - `aliases_retargeted == 0`
     - `renamed is True`
   - Assert `old` has zero total.
   - Assert `new` has two pending and one claimed.
   - Assert pending message order under `new` is the same as before.

2. `test_rename_queue_rejects_existing_target_without_mutation`
   - Write to `old` and `new`.
   - Add an alias pointing at `old`.
   - Capture `alias_version`.
   - Rename `old` to `new` should raise `ValueError`.
   - Assert both queues are unchanged.
   - Assert the alias still points at `old`.
   - Assert `alias_version` did not change.

3. `test_rename_queue_missing_source_is_noop`
   - Write to `other`.
   - Rename `missing` to `new`.
   - Assert zero counts in result.
   - Assert `new` was not created.
   - Assert `other` is unchanged.

4. `test_rename_queue_rejects_same_name`
   - `broker.rename_queue("same", "same")` raises `ValueError`.

5. `test_rename_queue_validates_names_before_mutation`
   - Invalid old name raises before mutation.
   - Invalid new name raises before mutation.
   - Existing messages remain in place.

6. `test_rename_queue_retargets_aliases_by_default`
   - Add aliases `a1 -> old`, `a2 -> old`, and `other -> elsewhere`.
   - Write messages to `old`.
   - Capture `alias_version`.
   - Rename `old` to `new`.
   - Assert `a1` and `a2` point at `new`.
   - Assert `other` is unchanged.
   - Assert `aliases_retargeted == 2`.
   - Assert alias version increased.

7. `test_rename_queue_can_leave_aliases_behind`
   - Same setup as above.
   - Rename with `retarget_aliases=False`.
   - Assert aliases still point at `old`.
   - Assert `aliases_retargeted == 0`.

8. `test_rename_queue_missing_source_does_not_retarget_aliases`
   - Add `a1 -> old`.
   - Do not write messages to `old`.
   - Rename `old` to `new`.
   - Assert alias still points at `old`.
   - Assert alias version did not change.

Do not mock storage. These tests should fail because the API does not exist.

Run:

```bash
uv run --extra dev pytest tests/test_queue_rename.py -q
```

Expected state after this task: red tests fail on missing API.

### Task 2: Add result type and protocol surface

Files to edit:

- `simplebroker/metadata.py`
- `simplebroker/__init__.py`
- `simplebroker/_backend_plugins.py`

Implementation:

1. Add `QueueRenameResult` to `metadata.py`.
2. Export it in `simplebroker/__init__.py`.
3. Add `rename_queue(...)` to the `BrokerConnection` protocol.
4. Add SQL backend plugin hooks:

```python
def rename_queue_messages(
    self,
    runner: SQLRunner,
    *,
    old_queue: str,
    new_queue: str,
) -> int: ...

def retarget_aliases(
    self,
    runner: SQLRunner,
    *,
    old_target: str,
    new_target: str,
) -> int: ...
```

Do not implement Redis storage behavior through these SQL hooks. Redis is a
direct backend and implements the broker protocol directly in
`RedisBrokerCore`. If a type checker or protocol update forces stub methods on
`RedisBackendPlugin`, the stubs should raise `NotImplementedError` and must not
be called in normal Redis operation.

Run:

```bash
uv run --extra dev pytest tests/test_queue_rename.py -q
```

Expected state: still red, now failing because methods are not implemented.

### Task 3: Implement SQLite SQL helpers and plugin hooks

Files to edit:

- `simplebroker/_sql/sqlite.py`
- `simplebroker/_sql/__init__.py` if exports are needed
- `simplebroker/_backends/sqlite/maintenance.py`
- `simplebroker/_backends/sqlite/plugin.py`

Add SQL constants:

```sql
UPDATE messages SET queue = ? WHERE queue = ?
```

```sql
UPDATE queue_aliases SET target = ? WHERE target = ?
```

Implementation rules:

- Put row-count logic in `maintenance.py`.
- Read `changes()` immediately after each SQLite update, before any other
  statement can overwrite the count.
- Do not commit in the helper. The caller owns the transaction.
- Do not run one update per message.

Suggested helper names:

```python
def rename_queue_messages(
    runner: SQLRunner,
    *,
    old_queue: str,
    new_queue: str,
) -> int: ...

def retarget_aliases(
    runner: SQLRunner,
    *,
    old_target: str,
    new_target: str,
) -> int: ...
```

Run:

```bash
uv run --extra dev pytest tests/test_queue_rename.py -q
```

Expected state: still red until `BrokerCore.rename_queue(...)` exists.

### Task 4: Implement `BrokerCore.rename_queue(...)` for SQL backends

Files to edit:

- `simplebroker/db.py`

Implementation outline:

1. Import `QueueRenameResult`.
2. Add a private helper if needed:

```python
def _queue_exists_locked(self, queue: str) -> bool:
    rows = list(self._runner.run(self._sql.CHECK_QUEUE_EXISTS, (queue,), fetch=True))
    return bool(rows[0][0]) if rows else False
```

3. Add `rename_queue(...)` near other cross-queue/admin methods such as
   `delete_from_queues`, `broadcast`, and metadata helpers.
4. Validate:
   - fork safety
   - both queue names
   - `old_queue != new_queue`
   - no reentrant mutation during an active generator batch
5. In one transaction:
   - call `begin_immediate()`
   - call `self._backend_plugin.prepare_queue_operation(..., operation="rename",
     queue=old_queue)`
   - if `new_queue` exists, raise `ValueError` and roll back
   - if `old_queue` does not exist, roll back or commit no-op and return zero
     result. Prefer rollback because no mutation occurred.
   - call `rename_queue_messages`
   - if `messages_renamed > 0 and retarget_aliases`, call
     `prepare_alias_mutation` when the backend exposes it, then
     `retarget_aliases`
   - if aliases changed, call `_increment_alias_version_locked()` and
     `_load_aliases_locked()`
   - commit
   - roll back on every exception

Important details:

- Do not call public `queue_exists(...)` from inside the transaction. It has its
  own retry/lock path and is not the right internal primitive.
- Validate both names before starting the transaction.
- Return canonical input strings exactly as supplied to the API.
- Do not mutate aliases when `messages_renamed == 0`.

Run:

```bash
uv run --extra dev pytest tests/test_queue_rename.py -q
uv run --extra dev pytest tests/test_aliases_db.py tests/test_queue_metadata.py -q
```

Expected state: SQLite shared API tests pass.

### Task 5: Implement Postgres SQL hooks and locking

Files to edit:

- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`

Add count-returning CTEs:

```sql
WITH renamed AS (
    UPDATE messages
    SET queue = ?
    WHERE queue = ?
    RETURNING 1
)
SELECT COUNT(*) FROM renamed
```

```sql
WITH updated AS (
    UPDATE aliases
    SET target = ?
    WHERE target = ?
    RETURNING 1
)
SELECT COUNT(*) FROM updated
```

Update `PostgresBackendPlugin.prepare_queue_operation(...)`:

- If `operation == "rename"`, run `LOCK TABLE messages IN SHARE ROW EXCLUSIVE
  MODE` and return.
- Keep existing claim/move behavior unchanged.
- Keep advisory locking behavior for other operations unchanged.

Add plugin hook methods delegating to the new SQL.

Do not update `runner._should_prepare(...)` unless benchmark evidence says
rename is hot. It should not be hot.

Run:

```bash
bin/pytest-pg tests/test_queue_rename.py extensions/simplebroker_pg/tests/test_pg_queue_rename.py -q
```

At this point `extensions/simplebroker_pg/tests/test_pg_queue_rename.py` may not
exist yet. Create it in Task 8. If running before Task 8, run only the shared
test target with `bin/pytest-pg tests/test_queue_rename.py -q`.

Expected state: shared rename behavior passes under Postgres.

### Task 6: Implement Redis rename script and direct-core method

Files to edit:

- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`

Lua script requirements:

- Inputs should be explicit.
- Use `KEYS` for Redis keys and `ARGV` for queue names, flags, and alias
  version.
- Never concatenate queue names into Lua source.
- Return structured numeric status so Python can map errors consistently.

Suggested return shape:

```text
{status, messages_renamed, aliases_retargeted}

status:
  1  success or source missing
 -1  source has active reserved messages
 -2  target already has pending, claimed, or reserved state
```

Script logic:

1. Check `ZCARD old_reserved`; if greater than zero, return `-1`.
2. Check `ZCARD new_pending`, `new_claimed`, and `new_reserved`; if any greater
   than zero, return `-2`.
3. Count `old_pending + old_claimed`.
4. If count is zero, return success with zero counts.
5. If old pending exists, rename or move old pending state to new pending.
6. If old claimed exists, rename or move old claimed state to new claimed.
7. Delete old reserved key if it exists and is empty.
8. Update `queues` set: remove old and add new.
9. If `retarget_aliases` is true:
   - inspect the aliases hash
   - rewrite values equal to old to new
   - count updates
   - write `meta.alias_version` only when count is greater than zero
10. Return counts.

Prefer Redis `RENAME` for non-empty old pending/claimed keys only when the new
key is known not to exist. If using `ZUNIONSTORE` or member-copy logic instead,
preserve fixed-width lexicographic member IDs and keep scores at `0`.

Python method requirements:

- Validate names with `_validate_queue_name`.
- Reject `old_queue == new_queue`.
- Call `_check_fork_safety()`.
- Call `_assert_no_reentrant_mutation_during_batch("rename_queue")`.
- Call `recover_stale_batches(...)` before checking reserved state.
- Deduplicate no inputs because this method takes two names, not a list.
- Map Lua `-1` to `OperationalError("Cannot rename queue while an at_least_once batch is active")`.
- Map Lua `-2` to `ValueError("Target queue already exists")`.
- On success with `messages_renamed > 0`, call `_publish(old_queue)` and
  `_publish(new_queue)`.
- Return `QueueRenameResult`.

Run:

```bash
bin/pytest-redis tests/test_queue_rename.py extensions/simplebroker_redis/tests/test_redis_queue_rename.py -q
```

At this point `extensions/simplebroker_redis/tests/test_redis_queue_rename.py`
may not exist yet. Create it in Task 9. If running before Task 9, run only the
shared test target with `bin/pytest-redis tests/test_queue_rename.py -q`.

Expected state: shared rename behavior passes under Redis.

### Task 7: Add CLI command

Files to edit:

- `simplebroker/commands.py`
- `simplebroker/cli.py`
- `tests/test_cli_rename.py`

Parser work in `simplebroker/cli.py`:

1. Add a `rename` subparser:

```python
rename_parser = subparsers.add_parser("rename", help="rename a queue")
rename_parser.add_argument("old_queue", help="queue name to rename")
rename_parser.add_argument("new_queue", help="new queue name")
rename_parser.add_argument("--json", action="store_true", help="output JSON")
rename_parser.add_argument(
    "--no-retarget-aliases",
    action="store_true",
    help="leave aliases pointing at the old queue name",
)
```

2. Add `"rename"` to `ArgumentProcessor.subcommands`.
3. Dispatch near `move`:

```python
elif args.command == "rename":
    return commands.cmd_rename(...)
```

Command work in `simplebroker/commands.py`:

1. Resolve CLI aliases with `_resolve_alias_name(...)` for both operands.
2. Call `broker.rename_queue(...)` through `DBConnection`.
3. In `--json` mode, print the result object on stdout.
4. In plain mode, print nothing for success or missing source.
5. Return:
   - `EXIT_SUCCESS` when `result.renamed`
   - `EXIT_QUEUE_EMPTY` when not renamed
6. Let validation and backend exceptions propagate to `cli.main(...)` unless
   `cmd_rename(...)` needs to translate a known no-op result. The main dispatch
   already catches `(ValueError, DatabaseError)` and calls `_emit_error(...)`
   with the correct JSON mode.

Be careful with argparse:

- `--no-retarget-aliases` means pass `retarget_aliases=False`.
- Do not make `rename` a subcommand of `alias`.
- Do not protect `rename` operands as free-form strings. Invalid queue names
  should be reported as validation errors, not treated as message bodies.

CLI tests in `tests/test_cli_rename.py`:

- Use the existing `run_cli` helper. Do not call `commands.cmd_rename(...)`
  directly for CLI behavior.
- Test plain success exits `0` and produces no stdout.
- Test JSON success prints the exact keys.
- Test missing source exits `2`; JSON mode prints zero-count result.
- Test target collision exits `1`, emits error on stderr, and leaves both
  queues unchanged.
- Test `--no-retarget-aliases`.
- Test `@alias` source resolution.
- Test `@alias` destination resolution.
- Test undefined alias reports an error.
- Test invalid queue name reports an error and leaves data unchanged.
- Add at least one test proving `-d` and `-f` still work when placed before the
  `rename` subcommand. The CLI has custom argument rearrangement code.

Run:

```bash
uv run --extra dev pytest tests/test_cli_rename.py -q
```

Expected state: CLI tests pass for SQLite.

### Task 8: Add Postgres-specific tests

Files to create:

- `extensions/simplebroker_pg/tests/test_pg_queue_rename.py`

Use real Postgres. Do not mock psycopg.

Tests:

1. `test_postgres_rename_queue_shared_behavior`
   - Optional if the shared suite already covers Postgres through
     `bin/pytest-pg`. Prefer shared tests for general behavior.

2. `test_postgres_rename_notifies_old_and_new_waiters`
   - Create a fresh `BrokerCore`.
   - Write to `old`.
   - Create activity waiters for `old` and `new` using the backend plugin
     path already tested in `test_pg_notify.py`.
   - Drain any initial notification state if the helper requires it.
   - Rename `old` to `new`.
   - Assert both waiters wake.
   - Assert a waiter for an unrelated queue does not need to wake. Do not make
     this assertion timing-fragile; if existing waiter tests avoid negative
     assertions, follow that pattern.

3. `test_postgres_rename_blocks_concurrent_target_race`
   - This is the one concurrency test worth having.
   - Use two broker connections against the same schema.
   - Arrange one thread to attempt rename and another to write to `new` around
     the same time.
   - Assert the final state is one of the allowed serial outcomes:
     - rename wins first: `old` messages moved to `new`, later write to `new`
       appends after commit; or
     - write wins first: rename sees target collision and leaves `old` intact.
   - Assert no split state: old partially renamed, target duplicate loss, or
     missing messages.

If the concurrency test is flaky, do not keep a timing-based test. Replace it
with a transaction-level test that deliberately holds a lock using raw psycopg,
then verifies the second operation waits or serializes. Prefer deterministic
coordination with events over sleeps.

Run:

```bash
bin/pytest-pg tests/test_queue_rename.py extensions/simplebroker_pg/tests/test_pg_queue_rename.py -q
```

### Task 9: Add Redis-specific tests

Files to create:

- `extensions/simplebroker_redis/tests/test_redis_queue_rename.py`

Use real Valkey/Redis. Do not mock redis-py.

Tests:

1. `test_redis_rename_cleans_old_keys_and_queue_set`
   - Write pending and claimed messages to `old`.
   - Rename to `new`.
   - Use the real client and `RedisKeys` to assert:
     - old pending key is gone or empty
     - old claimed key is gone or empty
     - old reserved key is gone or empty
     - new pending/claimed keys contain the expected IDs
     - `queues` set contains `new` and not `old`
     - `bodies` and `all_ids` still contain every message ID

2. `test_redis_rename_rejects_active_reserved_source`
   - Start an `at_least_once` generator batch on `old` and pause before
     completion.
   - Attempt rename from a separate broker handle.
   - Assert `OperationalError`.
   - Assert source queue state still works after rolling back or closing the
     generator.

3. `test_redis_rename_rejects_reserved_target_collision`
   - Create active reserved state on `new`.
   - Attempt rename `old -> new`.
   - Assert `ValueError`.
   - Assert no mutation.

4. `test_redis_rename_retargets_aliases_and_bumps_version`
   - Same as shared alias test, but assert Redis `meta.alias_version` changed
     in the hash.

5. `test_redis_rename_publishes_old_and_new_activity`
   - Use Redis activity waiters from the plugin.
   - Assert waiters for old and new wake after rename.

6. `test_redis_rename_missing_source_does_not_create_new_keys`
   - Rename missing to new.
   - Assert no new pending, claimed, or reserved keys exist.
   - Assert `queues` set does not contain `new`.

Run:

```bash
bin/pytest-redis tests/test_queue_rename.py extensions/simplebroker_redis/tests/test_redis_queue_rename.py -q
```

### Task 10: Add or update property tests only if they buy coverage

Files to inspect:

- `tests/test_property_queue_names.py`
- `tests/test_property_queue_model.py`

Do not add Hypothesis tests just to look thorough. Add one only if it catches a
class of bugs the deterministic tests do not cover.

Good property candidates:

- For valid distinct queue names, rename preserves total message count and
  transfers all message IDs from old to new.
- Invalid old or new queue names never mutate existing messages.

Bad property candidates:

- Re-testing every deterministic alias scenario with generated strings.
- Mocking backend hooks and asserting call order.

Run, if property tests are added:

```bash
uv run --extra dev pytest tests/test_property_queue_names.py tests/test_property_queue_model.py -q
```

### Task 11: Update docs and changelog

Files to edit:

- `README.md`
- `CHANGELOG.md`

README updates:

1. Add `rename <old> <new>` to the command table.
2. Add a short CLI example near `move` or aliases:

```bash
broker rename jobs.pending jobs.ready
broker rename @legacy jobs.ready --json
```

3. Add an `open_broker` example near the existing `delete_from_queues` docs:

```python
result = broker.rename_queue("jobs.pending", "jobs.ready")
print(result.messages_renamed)
```

4. Explain:
   - rename includes claimed rows
   - target queue must not exist
   - aliases targeting the old queue are retargeted by default
   - Python API uses literal queue names
   - CLI may resolve `@alias` operands

CHANGELOG:

- Add an "Added" entry for `BrokerConnection.rename_queue(...)`,
  `QueueRenameResult`, and CLI `broker rename`.
- Mention SQLite, Postgres, and Redis parity.
- Mention that no schema migration is required.

Run:

```bash
uv run --extra dev pytest tests/test_cli_rename.py tests/test_queue_rename.py -q
```

Docs do not usually need a separate renderer here.

### Task 12: Run the full verification gates

Run fast local gates first:

```bash
uv run --extra dev pytest tests/test_queue_rename.py tests/test_cli_rename.py -q
uv run --extra dev pytest tests/test_aliases_db.py tests/test_queue_metadata.py tests/test_delete_from_queues.py -q
uv run --extra dev ruff check simplebroker tests extensions/simplebroker_pg extensions/simplebroker_redis
uv run --extra dev mypy simplebroker
```

Run backend gates:

```bash
bin/pytest-pg tests/test_queue_rename.py tests/test_cli_rename.py extensions/simplebroker_pg/tests/test_pg_queue_rename.py -q
bin/pytest-redis tests/test_queue_rename.py tests/test_cli_rename.py extensions/simplebroker_redis/tests/test_redis_queue_rename.py -q
```

Run broader regression gates before merging:

```bash
uv run --extra dev pytest -q
bin/pytest-pg -q
bin/pytest-redis -q
```

If Docker or external services are unavailable, record exactly which backend
gate was not run and why. Do not claim backend support without running the
backend-specific tests somewhere.

## Test Design Guide

Do not over-mock.

Good tests:

- Exercise `BrokerCore.rename_queue(...)` through real broker fixtures.
- Exercise CLI through subprocess helpers.
- Exercise Postgres through a real schema.
- Exercise Redis through real Valkey/Redis and the Lua script.
- Assert persisted state after reopening a second broker handle.
- Assert invariants, not private call order.

Bad tests:

- Mocking SQLite, psycopg, or redis-py command methods for behavior coverage.
- Asserting that a helper was called instead of asserting storage changed.
- Testing the Lua script by checking that the string contains `ZCARD`.
- Adding a fake in-memory Redis.

Required invariants:

- Total message count is preserved by successful rename.
- Message IDs are preserved.
- Message bodies are preserved.
- FIFO order for pending messages is preserved.
- Claimed rows remain claimed after SQL rename.
- Redis claimed set members remain claimed after rename.
- Source queue no longer exists after a successful rename.
- Target queue exists with the expected counts after successful rename.
- Target collision leaves source and target unchanged.
- Invalid input leaves all queues unchanged.
- Missing source leaves aliases unchanged.
- Alias retarget default updates every alias whose target equals old.
- Alias retarget default updates no alias whose target differs from old.
- Alias version changes only when aliases are retargeted.
- Redis source reserved state blocks rename.
- Redis target reserved state blocks rename as a collision.
- Postgres and Redis wake old and new queue activity waiters after successful
  rename.

Coverage diagram:

```text
CODE PATHS                                      TESTS
BrokerCore.rename_queue
  +-- invalid old/new name                      shared invalid-name tests
  +-- same old/new                              shared same-name test
  +-- target exists                             shared collision test
  +-- source missing                            shared missing-source test
  +-- messages renamed                          shared pending+claimed test
  +-- aliases retargeted                        shared alias default test
  +-- aliases left behind                       shared no-retarget test
  +-- alias version bump                        shared alias version test

SQLite plugin hooks
  +-- message update row count                  shared tests on sqlite
  +-- alias update row count                    shared alias result tests

Postgres plugin hooks
  +-- table lock for rename                     pg concurrency or lock test
  +-- message update row count                  shared tests under bin/pytest-pg
  +-- alias update row count                    shared tests under bin/pytest-pg
  +-- old/new notifications                     pg activity waiter test

RedisBrokerCore.rename_queue
  +-- Lua success                               shared tests under bin/pytest-redis
  +-- source reserved rejection                 redis-specific test
  +-- target reserved collision                 redis-specific test
  +-- key cleanup                               redis-specific test
  +-- alias hash retarget                       redis-specific test
  +-- old/new publish                           redis-specific activity waiter test

CLI rename
  +-- plain success                             CLI subprocess test
  +-- JSON success                              CLI subprocess test
  +-- missing source exit 2                     CLI subprocess test
  +-- collision error                           CLI subprocess test
  +-- @alias operands                           CLI subprocess test
  +-- global -d/-f placement                    CLI subprocess test
```

## Failure Modes

| Failure mode | Prevention in plan | Test |
|---|---|---|
| Target queue already has messages and rename silently merges queues | Collision check before mutation | Shared collision test |
| Claimed rows stay under old queue and old queue still appears in metadata | Rename includes claimed rows | Shared pending+claimed test |
| Alias points at old queue after default rename | Default alias retarget | Shared alias default test |
| Alias version does not bump and open handles use stale alias cache | Bump alias version only when retarget count is nonzero | Shared alias version/cache test |
| Postgres write races with rename and creates split state | Table lock in rename transaction | PG concurrency or lock test |
| Redis active batch cannot commit after rename | Reject source reserved state | Redis reserved-source test |
| Redis target has only reserved state and rename overwrites it | Reserved target collision check | Redis reserved-target test |
| Watcher waits forever after rename | Notify old and new queues after success | PG and Redis activity tests |
| CLI JSON success is hard to script | Stable one-object JSON payload | CLI JSON test |

No failure mode above should be deferred. They are all in core scope.

## What Already Exists

- Queue names are already validated by `_validate_queue_name_cached(...)` and
  `BrokerCore._validate_queue_name(...)`. Reuse them.
- `QueueStats` already defines queue existence as total rows including claimed.
  Match that model.
- `delete_from_queues` already shows how to add a cross-queue backend hook with
  consistent SQLite, Postgres, and Redis behavior.
- Alias versioning and cache refresh already exist in `BrokerCore`. Reuse
  `_increment_alias_version_locked()` and `_load_aliases_locked()`.
- CLI alias resolution already exists in `_resolve_alias_name(...)`. Reuse it.
- CLI error formatting already exists in `_emit_error(...)`. Reuse it.
- Postgres activity notification infrastructure already exists. Use the
  existing channel and payload model.
- Redis activity waiter infrastructure already exists. Publish old and new
  queue channels.
- Backend-specific test runners already exist: `bin/pytest-pg` and
  `bin/pytest-redis`.

Do not rebuild these.

## NOT in Scope

- Durable queue declarations or queue registry tables.
- Cross-process prevention of future writes to the old name after rename
  commits.
- Queue merge or overwrite behavior.
- A `Queue.rename(...)` instance method.
- Partial rename by timestamp or message ID.
- Rename dry-run.
- Bulk rename by prefix or pattern.
- Alias-only target migration when no messages exist.
- Redis Cluster support.
- New backend capability negotiation.
- Any user-facing UI beyond the CLI.

## Worktree Parallelization Strategy

This feature has parallel work, but only after the shared API contract lands.

| Step | Modules touched | Depends on |
|---|---|---|
| Shared contract and BrokerCore | `simplebroker/` | none |
| SQLite hooks | `simplebroker/_backends/sqlite/`, `simplebroker/_sql/` | shared contract |
| Postgres hooks and tests | `extensions/simplebroker_pg/` | shared contract |
| Redis hooks and tests | `extensions/simplebroker_redis/` | shared contract |
| CLI | `simplebroker/cli.py`, `simplebroker/commands.py`, `tests/` | shared contract |
| Docs | `README.md`, `CHANGELOG.md` | API shape finalized |

Parallel lanes:

- Lane A: shared contract -> BrokerCore -> SQLite hooks. This should land first.
- Lane B: Postgres hooks/tests. Can start after Lane A defines hooks.
- Lane C: Redis hooks/tests. Can start after Lane A defines result type and
  protocol method.
- Lane D: CLI/tests. Can start after Lane A exposes `rename_queue`.
- Lane E: docs/changelog. Do last after exact behavior and names settle.

Execution order:

1. Land Lane A first in one worktree.
2. Launch Lane B, Lane C, and Lane D in parallel worktrees after Lane A is
   merged or cherry-picked.
3. Land docs after the behavior tests pass.

Conflict flags:

- Lane A and Lane D both touch `simplebroker/`; coordinate carefully.
- Lane B and Lane C are independent.
- Docs may conflict with changelog edits from nearby releases.

## Implementation Checklist

- [ ] Red shared API tests exist and fail for the missing method.
- [ ] `QueueRenameResult` exists and is exported.
- [ ] `BrokerConnection` protocol includes `rename_queue`.
- [ ] SQL backend hooks exist for message rename and alias retargeting.
- [ ] SQLite hooks use `changes()` immediately after each update.
- [ ] `BrokerCore.rename_queue` owns validation and transaction boundaries.
- [ ] Postgres rename takes a table lock that blocks concurrent writes.
- [ ] Postgres successful rename notifies old and new queue names.
- [ ] Redis Lua script performs rename atomically.
- [ ] Redis rejects active source reservations.
- [ ] Redis treats target reserved state as collision.
- [ ] Redis successful rename publishes old and new queue channels.
- [ ] CLI command exists and has JSON output.
- [ ] README and CHANGELOG are updated.
- [ ] SQLite, Postgres, Redis, CLI, lint, and type gates have run or are
  explicitly documented as not run.

## Fresh-Eyes Self Review

I reviewed this plan against the feature discussion and the current codebase.

Architecture findings:

- The plan stays aligned with the original direction: rename is a bulk update of
  queue labels, not a new queue registry.
- The biggest risk is backend divergence. The plan reduces that by putting
  shared validation and SQL transaction boundaries in `BrokerCore`, while
  keeping Redis direct because Redis is not SQL-backed.
- The Postgres locking choice is intentionally coarse. It is the right tradeoff
  because writes do not take queue advisory locks today, and rename should be an
  uncommon admin operation.
- Redis reserved batches are the sharp edge. The plan now rejects source
  reservations instead of trying to rewrite active batch metadata.

Code-quality findings:

- A single `QueueRenameResult` dataclass is justified because CLI JSON needs
  both message and alias counts. More result types or option objects would be
  over-engineering.
- Two backend plugin hooks are enough for SQL backends:
  `rename_queue_messages` and `retarget_aliases`. A broad "admin operation"
  abstraction would be premature.
- The plan avoids a `Queue.rename` method because mutating the meaning of an
  existing queue handle is an avoidable state trap.

Test findings:

- The plan uses real backends for behavior. That is the correct posture for
  this project.
- The most important regression tests are collision no-mutation, claimed-row
  inclusion, alias-version bump, and Redis reserved-batch rejection.
- The Postgres concurrency test could be flaky if written with sleeps. The plan
  explicitly requires deterministic coordination or a lock-level test instead.

Performance findings:

- SQLite and Postgres use set-based updates.
- Redis should use key rename or set-level operations, not per-message Python
  loops.
- Alias retarget uses full alias-table scan in Redis. That is acceptable because
  aliases are expected to be small and no alias reverse index exists.

Corrections made during review:

- Clarified missing source semantics: no alias retargeting when no messages were
  renamed.
- Clarified API vs CLI alias syntax.
- Added Postgres table locking because advisory queue locks do not block writes.
- Added Redis reserved-batch rejection because batch metadata stores source
  queue names.
- Changed notification requirement from wildcard-only to old and new
  queue-specific notifications, which matches current Redis listener behavior.

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope and strategy | 0 | not run | Not needed for backend/admin API plan |
| Codex Review | `/codex review` | Independent 2nd opinion | 0 | not run | Not run for this doc-only planning pass |
| Eng Review | `/plan-eng-review` | Architecture and tests | 1 | clear | 5 issues found and folded into the plan; 0 critical gaps |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | not applicable | Backend and CLI only |
| DX Review | `/plan-devex-review` | Developer experience gaps | 0 | not run | CLI/API behavior specified in plan |

**VERDICT:** ENG CLEARED - ready to implement after red shared tests are added.

NO UNRESOLVED DECISIONS
