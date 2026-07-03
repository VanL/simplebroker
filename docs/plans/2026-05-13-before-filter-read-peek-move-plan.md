# `--before` Filter Plan for Read, Peek, and Move

Date: 2026-05-13

Status: proposed

Owner: SimpleBroker

## Purpose

Add a `--before TIMESTAMP` filter to the SimpleBroker CLI for `read`, `peek`,
and `move`.

`--before` must accept the same timestamp formats as `--since`, but apply the
opposite time predicate:

```text
--since  X  means ts > X
--before X  means ts < X
```

This plan is written for an engineer who is skilled but new to this codebase.
Follow it in order. Use red-green TDD. Keep the implementation small. Do not
solve adjacent problems while touching this path.

## Locked Decisions

Do not reopen these decisions unless a red test proves the plan cannot work.

### CLI Scope

Add `--before` only to:

- `broker read QUEUE`
- `broker peek QUEUE`
- `broker move SOURCE_QUEUE DEST_QUEUE`

Do not add `--before` to `watch` in this change.

Reason: `read`, `peek`, and `move` are finite queue operations. `watch --since`
is a moving checkpoint. A static upper bound like `watch --before 123` has
surprising behavior: after matching backlog drains, the process may wait
forever for future messages that will never have `ts < 123`. That needs a
separate design.

### Boundary Semantics

Use strict comparison:

```text
--since  X  includes messages with ts > X
--before X  includes messages with ts < X
```

Do not use `>=` or `<=`.

When both filters are provided, combine them as an open interval:

```text
since_timestamp < ts < before_timestamp
```

This is useful, easy to explain, and maps directly to SQL predicates. It also
keeps the filters orthogonal. Do not make `--since` and `--before` mutually
exclusive.

If the interval is empty or inverted, for example `--since 50 --before 10`, do
not add special validation. Let the query return no rows and exit with the same
empty-queue code used for other no-match filters.

### Message ID Compatibility

`-m/--message` remains incompatible with range filters.

Current behavior rejects:

```bash
broker read q --message ID --since TS
broker peek q --message ID --since TS
broker move src dst --message ID --since TS
```

New behavior must also reject:

```bash
broker read q --message ID --before TS
broker peek q --message ID --before TS
broker move src dst --message ID --before TS
```

When both filters are present with `--message`, error text should mention both
range filters, for example:

```text
--message cannot be used with --all, --since, or --before
```

For `move`, `--all` is already mutually exclusive with `--message` in argparse,
so its error can be:

```text
--message cannot be used with --since or --before
```

### Public Python API

Add `before_timestamp` to the public queue API, not only to the CLI.

Reason: the CLI is built on top of `Queue`, and users already have a public
`since_timestamp` filter in `Queue.read()`, `Queue.peek()`, `Queue.move()`,
`read_many()`, `read_generator()`, `peek_many()`, `peek_generator()`,
`move_many()`, `move_generator()`, and `stream_messages()`. A CLI-only flag
would create an unnecessary split between the CLI and Python API.

Do not add `before_timestamp` to watcher APIs in this change.

### Ordering

Do not reverse result order.

`--before` is an upper time bound, not a request for reverse chronological
output. Preserve existing FIFO order. The first matching message is still the
oldest matching message.

### SQL and Backend Scope

Both built-in SQLite and the PostgreSQL extension must support the feature.

Do not add a backend capability flag. This filter belongs in the shared retrieve
query spec and both SQL builders should implement it.

Do not add a database migration. The existing queue/timestamp indexes are enough
for `queue = ? AND claimed = ? AND ts < ?` predicates.

## Repository Primer

Runtime package:

- `simplebroker/`

Core files:

- `simplebroker/cli.py`
  - Owns argparse setup, global option handling, and command dispatch.
  - `add_read_peek_args()` defines shared `read` and `peek` flags.
  - `create_parser()` defines `move` flags.
  - `main()` dispatches parsed args into `simplebroker.commands`.
- `simplebroker/commands.py`
  - Owns command implementation functions.
  - `_resolve_timestamp_filters()` parses CLI timestamp filters.
  - `_process_queue_fetch()` contains shared `read` and `peek` execution.
  - `cmd_move()` has separate single-message and all-message paths.
- `simplebroker/sbqueue.py`
  - Owns the public queue-first `Queue` API.
  - This is the API users import in Python.
- `simplebroker/db.py`
  - Owns `BrokerCore`/`BrokerDB`, retry handling, validation, and execution.
  - Retrieval is centralized through `_build_retrieve_spec()` and `_retrieve()`.
- `simplebroker/_sql/_query_spec.py`
  - Backend-neutral retrieve-query dataclass.
- `simplebroker/_sql/sqlite.py`
  - SQLite SQL constants and retrieve query builder.
- `simplebroker/_sql/_contract.py`
  - Protocol describing SQL namespace attributes required from backends.
- `simplebroker/_sql/__init__.py`
  - Compatibility exports for SQLite SQL constants.
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
  - PostgreSQL SQL constants and retrieve query builder.

Tests:

- `tests/test_since_flag.py`
  - CLI coverage for `read` and `peek` with `--since`.
  - Use this as the model for CLI `--before` tests.
- `tests/test_cli_move.py`
  - CLI coverage for `move --since`.
  - Use this as the model for CLI `move --before` tests.
- `tests/test_queue_api_comprehensive.py`
  - Public `Queue` API tests for `since_timestamp`.
- `tests/test_batch_operations.py`
  - Batch API coverage.
- `tests/test_commands_helpers.py`
  - Unit tests for command helpers.
- `tests/test_sql_internals.py`
  - Good place for focused SQL builder assertions if existing style fits.
- `extensions/simplebroker_pg/tests/`
  - PostgreSQL extension tests. Add only targeted backend tests if core shared
    tests do not exercise the Postgres builder in CI.

Developer tools:

- Use `rg` for search.
- Use `pytest` through the project environment. If this repo uses `uv`, prefer
  `uv run pytest ...`.
- Do not rely on mocks for database behavior. Use real temp databases through
  existing fixtures such as `workdir`, `queue_factory`, and `run_cli`.

## Engineering Rules

Keep these constraints front of mind:

- DRY: add one shared `before_timestamp` path beside `since_timestamp`; do not
  duplicate command logic per command when a helper already exists.
- YAGNI: do not add `watch --before`, pagination options, closed intervals,
  aliases, config flags, or new query objects beyond the needed field.
- Red-green TDD: write failing tests first for each behavior slice, then
  implement only enough code to pass.
- Real integration tests over mocks: command/helper unit tests are fine for
  argument plumbing, but behavior must be proven through CLI and `Queue` tests
  against real broker storage.
- Preserve public compatibility: existing `since_timestamp` behavior, exit
  codes, output order, and timestamp parsing must not change.
- Keep names boring and symmetric: use `before_str` for CLI strings and
  `before_timestamp` for parsed integer timestamps.

## Bite-Sized Tasks

### Task 1: Add Red Tests for `read` and `peek` CLI Behavior

Files to touch:

- `tests/test_before_flag.py` (new file)

Create a focused new test file instead of adding hundreds of lines to
`tests/test_since_flag.py`.

Test cases:

1. `peek --all --before TS` returns only messages older than `TS`.
   - Write `msg1`, capture `ts1`.
   - Sleep briefly.
   - Write `msg2`, capture `ts2`.
   - `broker peek q --all --before "$ts2"` returns `msg1`.
   - `broker peek q --all --before "$ts1"` returns empty with exit code `2`.

2. `read --before TS` consumes the oldest matching message only.
   - Write `old`, then `new`.
   - Use the timestamp of `new` as the `--before` cutoff.
   - `broker read q --before "$new_ts"` returns `old`.
   - A later `broker read q --all` returns `new`, proving the filter did not
     consume non-matching messages.

3. Boundary is strict `<`, not `<=`.
   - Write one message and capture `ts`.
   - `broker peek q --before "$ts"` returns exit code `2` and no output.
   - `broker peek q --before "$((ts + 1))"` returns the message.

4. `--since` and `--before` combine as an open interval.
   - Write `m1`, `m2`, `m3`, capture all timestamps.
   - `broker peek q --all --since "$ts1" --before "$ts3"` returns exactly
     `m2`.

5. Invalid timestamp errors use the same parser as `--since`.
   - `broker peek q --before invalid` exits with error code `1`.
   - stderr contains `Invalid timestamp`.

6. `--message` cannot be combined with `--before`.
   - Use a syntactically valid message id string.
   - Assert the command exits with error code `1`.
   - Assert stderr mentions `--before`.

Test style:

- Use `run_cli` from `tests/conftest.py`.
- Capture timestamps with `broker peek q --all --json` or `--timestamps`.
- Prefer JSON when indexing multiple messages; it is less brittle than splitting
  tab-delimited text.
- Use `time.sleep(0.001)` only where the existing tests already use it to make
  timestamp order obvious. Do not make long sleeps.

Expected first run:

```bash
uv run pytest tests/test_before_flag.py
```

It should fail because the CLI does not know `--before` yet.

### Task 2: Add Red Tests for `move --before`

Files to touch:

- `tests/test_cli_move.py`

Add a small section near the existing `--since` move tests.

Test cases:

1. Single `move --before TS` moves the oldest matching message.
   - Write `old1`, `old2`, then write `new1`, capture `new1_ts`, then write
     `new2`.
   - `broker move source dest --before "$new1_ts"` returns `old1`.
   - Source still contains `old2`, `new1`, `new2`.
   - Destination contains `old1`.

2. `move --all --before TS` moves all matching older messages in FIFO order.
   - Reuse a fresh queue.
   - Write `old1`, `old2`, then write `new1` and capture `new1_ts`.
   - `broker move source dest --all --before "$new1_ts"` returns
     `old1\nold2`.
   - Source still contains `new1`.

3. Boundary is strict `<`.
   - Write one message and capture `ts`.
   - `broker move source dest --before "$ts"` exits `2`.
   - Source still contains the message.
   - `broker move source dest --before "$((ts + 1))"` moves it.

4. `--since` and `--before` combine for move.
   - Write `m1`, `m2`, `m3`, capture timestamps.
   - `broker move source dest --all --since "$ts1" --before "$ts3"` moves
     exactly `m2`.
   - Source contains `m1\nm3`.

5. `--message` cannot be combined with `--before`.
   - Mirror the existing `--since` validation test.

Expected first run:

```bash
uv run pytest tests/test_cli_move.py -k before
```

It should fail before implementation.

### Task 3: Add Red Tests for Public `Queue` API

Files to touch:

- `tests/test_queue_api_comprehensive.py`
- `tests/test_batch_operations.py` if there is a better existing nearby batch
  test

Add tests that use real `Queue` objects via existing fixtures. Do not mock
`BrokerDB`.

Coverage to add:

1. `Queue.peek_many(limit, before_timestamp=...)`
   - Returns only rows with `ts < before_timestamp`.
   - Preserves FIFO order.

2. `Queue.read_many(limit, before_timestamp=...)`
   - Consumes only matching rows.
   - Leaves newer rows untouched.

3. `Queue.move_many(dest, limit, before_timestamp=...)`
   - Moves only matching rows.
   - Leaves newer rows in source.

4. `Queue.peek_generator(before_timestamp=...)`
   - Yields only matching rows.
   - Works for more than one item.

5. `Queue.read(all_messages=True, before_timestamp=...)`
   - Returns/generates only matching rows.

6. Conflict validation:
   - `Queue.read(message_id=123, before_timestamp=456)` raises `ValueError`.
   - `Queue.peek(message_id=123, before_timestamp=456)` raises `ValueError`.
   - `Queue.move("dest", message_id=123, before_timestamp=456)` raises
     `ValueError`.

Do not add tests for watcher behavior. `before_timestamp` is not being added to
watchers in this plan.

Expected first run:

```bash
uv run pytest tests/test_queue_api_comprehensive.py tests/test_batch_operations.py -k before
```

It should fail before implementation.

### Task 4: Extend the Backend-Neutral Query Spec

Files to touch:

- `simplebroker/_sql/_query_spec.py`

Add:

```python
before_timestamp: int | None = None
```

Place it next to `since_timestamp`:

```python
exact_timestamp: int | None = None
since_timestamp: int | None = None
before_timestamp: int | None = None
```

Reason: this dataclass is the contract between `db.py` and backend SQL builders.
The filter belongs here, not as ad hoc SQL in `db.py`.

Do not add a generic operator enum. That would be overbuilt for two simple
bounds.

### Task 5: Add SQL Predicate Support in SQLite and Postgres

Files to touch:

- `simplebroker/_sql/sqlite.py`
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`

In each `_build_where_clause(spec)`:

```python
if spec.since_timestamp is not None:
    where_conditions.append("ts > ?")
    params.append(spec.since_timestamp)
if spec.before_timestamp is not None:
    where_conditions.append("ts < ?")
    params.append(spec.before_timestamp)
```

Important details:

- Keep exact timestamp lookup separate. If `exact_timestamp is not None`, return
  the exact-timestamp conditions as the code does today. Do not combine exact
  lookup with range filters.
- Keep parameter order identical to condition order.
- Preserve existing `ORDER BY id` behavior in retrieve query templates.
- Do not add backend-specific behavior.

Optional targeted tests:

- If `tests/test_sql_internals.py` already tests retrieve query specs, add
  assertions that `before_timestamp` produces `ts < ?`.
- If not, rely on CLI and `Queue` integration tests unless a reviewer asks for
  focused SQL builder tests.

### Task 6: Thread `before_timestamp` Through `BrokerDB`

Files to touch:

- `simplebroker/db.py`

Add `before_timestamp` next to `since_timestamp` in:

- `_build_retrieve_spec()`
- `_yield_transactional_batches()`
- `_retrieve()`
- `claim_many()`
- `claim_generator()`
- `peek_many()`
- `peek_generator()`
- `move_many()`
- `move_generator()`

Implementation pattern:

```python
def _retrieve(
    ...,
    since_timestamp: int | None = None,
    before_timestamp: int | None = None,
    ...
) -> list[tuple[str, int]]:
```

Then pass it into `_build_retrieve_spec(...)`, and from there into
`RetrieveQuerySpec`.

Do not add `before_timestamp` to `claim_one()`, `peek_one()`, or `move_one()`.
Those methods only support exact timestamp lookup today. Filtered single-message
CLI/API calls already use generators or `many` methods when a range filter is
present.

Do not change delivery semantics. Materialized `many` methods remain
exactly-once. Generator batch behavior remains as-is.

### Task 7: Thread `before_timestamp` Through Public `Queue`

Files to touch:

- `simplebroker/sbqueue.py`

Add `before_timestamp` next to `since_timestamp` in:

- `Queue.read()`
- `Queue.read_many()`
- `Queue.read_generator()`
- `Queue.peek()`
- `Queue.peek_many()`
- `Queue.peek_generator()`
- `Queue.move()`
- `Queue.move_many()`
- `Queue.move_generator()`
- `Queue.stream_messages()`

Update docstrings where they describe `since_timestamp`.

Conflict checks:

Current pattern:

```python
if message_id is not None and (all_messages or since_timestamp):
    raise ValueError(...)
```

Change to:

```python
has_range_filter = since_timestamp is not None or before_timestamp is not None
if message_id is not None and (all_messages or has_range_filter):
    raise ValueError(
        "message_id cannot be used with all_messages, since_timestamp, or before_timestamp"
    )
```

Use `is not None` for both timestamp filters. Do not rely on truthiness because
`0` is a valid timestamp.

Single-message high-level methods:

- `Queue.read(before_timestamp=...)` should use `read_generator(...)` and return
  the first matching item, same as the current `since_timestamp` path.
- `Queue.peek(before_timestamp=...)` should use `peek_generator(...)`.
- `Queue.move(destination, before_timestamp=...)` should use
  `move_generator(...)`.

`stream_messages()`:

- Add `before_timestamp` to the signature and pass it to the underlying
  `claim_generator()`/`peek_generator()` calls.
- Do not add watcher tests. This method can support `before_timestamp` without
  exposing it through `QueueWatcher`.

### Task 8: Update Command Helpers

Files to touch:

- `simplebroker/commands.py`
- `tests/test_commands_helpers.py`

Refactor `_resolve_timestamp_filters()` to parse both `since_str` and
`before_str`.

Suggested signature:

```python
def _resolve_timestamp_filters(
    since_str: str | None,
    before_str: str | None,
    message_id_str: str | None,
) -> tuple[int | None, int | None, int | None, int | None]:
    ...
```

Return shape:

```text
(error_code, since_timestamp, before_timestamp, exact_timestamp)
```

Alternative acceptable style: introduce a small frozen dataclass such as
`ResolvedTimestampFilters`. Only do this if it makes call sites clearer. Do not
introduce a larger abstraction.

Update `_process_queue_fetch()`:

- Add `before_timestamp`.
- Pass both filters to `fetch_generator(...)`.
- The path that currently checks `if since_timestamp is not None:` should check
  `if since_timestamp is not None or before_timestamp is not None:`.

Update helper unit tests:

- Existing tests in `tests/test_commands_helpers.py` use simple local functions
  to assert helper plumbing. Extend them to assert `before_timestamp` is passed.
- Keep these as helper plumbing tests only. Do not use mocks to replace CLI or
  DB integration tests.

### Task 9: Update CLI Parser and Dispatch

Files to touch:

- `simplebroker/cli.py`

Parser changes:

1. In `add_read_peek_args(parser)`, add:

```python
parser.add_argument(
    "--before",
    type=str,
    metavar="TIMESTAMP",
    help="return messages before timestamp (supports same formats as --since)",
)
```

2. In the `move` parser, add `--before` near `--since`:

```python
move_parser.add_argument(
    "--before",
    type=str,
    metavar="TIMESTAMP",
    help="only move messages older than timestamp",
)
```

Do not add `--before` to `watch_parser`.

Dispatch changes:

- In read dispatch, get `before_str = getattr(args, "before", None)`.
- In peek dispatch, same.
- In move dispatch, same.
- Update mutual exclusion checks with `--message`.
- Pass `before_str` into `commands.cmd_read()`, `commands.cmd_peek()`, and
  `commands.cmd_move()` by keyword. Do not rely on positional argument order
  when adding the new optional argument.

Early validation:

- `_validate_early_command_args()` currently validates `--since` before backend
  inspection for `read` and `peek`.
- Extend it to validate both `--since` and `--before`.
- Use the same `commands._validate_timestamp()` function.

Keep `allow_abbrev=False` unchanged. Do not rewrite parser structure.

### Task 10: Update Command Implementations

Files to touch:

- `simplebroker/commands.py`

Update signatures:

```python
def cmd_read(
    ...,
    since_str: str | None = None,
    message_id_str: str | None = None,
    before_str: str | None = None,
)
def cmd_peek(
    ...,
    since_str: str | None = None,
    message_id_str: str | None = None,
    before_str: str | None = None,
)
def cmd_move(
    ...,
    message_id_str: str | None = None,
    since_str: str | None = None,
    before_str: str | None = None,
)
```

Append `before_str` after existing optional arguments to minimize compatibility
churn for internal call sites and tests. Update CLI dispatch to call these
functions with keywords so future argument additions are less fragile.

Parsing:

- Use the refactored `_resolve_timestamp_filters()` for `read` and `peek`.
- For `move`, either use the same helper or keep local parsing for readability.
  Prefer the helper if it does not make the `move` exact-message flow harder to
  read. DRY matters here because timestamp parsing errors should stay identical.

Execution:

- Pass both filters into `Queue` methods.
- For `move --all`, pass `before_timestamp` into `queue.move_many(...)`.
- For single `move` with either filter, use `queue.move_generator(...)` and
  return the first matching message.

Do not special-case inverted intervals.

### Task 11: Update SQL Namespace Contract and Exports Only If Needed

Files to inspect:

- `simplebroker/_sql/_contract.py`
- `simplebroker/_sql/__init__.py`

If you add new standalone SQL constants, update both files. The recommended
implementation does not require new constants for read/peek/move because the
retrieve query builder handles the predicate.

Do not add `CHECK_PENDING_MESSAGES_BEFORE` in this change.

Do not add `before_timestamp` to `Queue.has_pending()`. Keep `has_pending()`
unchanged.

This avoids pulling watcher-adjacent behavior into the first implementation.

### Task 12: Documentation Updates

Files to touch:

- `README.md`
- `CHANGELOG.md` if this repo tracks unreleased changes there
- Any example only if it naturally mentions timestamp filtering

README locations to update:

- The command option list around `--since`.
- The timestamp format section titled for `--since`; rename or broaden it to
  timestamp filters.
- The resilient processing examples only if needed. Do not rewrite them.

Document:

- `--before <timestamp>` returns or moves messages older than timestamp.
- Timestamp formats are identical to `--since`.
- `--since` and `--before` can be combined to select an open interval.
- `--before` is not available on `watch`.

Do not add large tutorials.

### Task 13: Run Focused Test Gates

Run these after implementation:

```bash
uv run pytest tests/test_before_flag.py
uv run pytest tests/test_cli_move.py -k "before or since"
uv run pytest tests/test_commands_helpers.py
uv run pytest tests/test_queue_api_comprehensive.py -k "before or since"
uv run pytest tests/test_batch_operations.py -k "before or since"
```

Why include `since` in some gates: the implementation touches shared filter
plumbing. Existing `--since` behavior is the easiest regression to introduce.

If `uv` is unavailable, use:

```bash
python -m pytest ...
```

### Task 14: Run Broader Regression Gates

Run:

```bash
uv run pytest tests/test_since_flag.py tests/test_cli_move.py tests/test_queue_api_comprehensive.py tests/test_batch_operations.py tests/test_commands_helpers.py
```

If time permits or before landing:

```bash
uv run pytest
```

PostgreSQL extension gate depends on local setup. If configured, run:

```bash
uv run pytest extensions/simplebroker_pg/tests
```

If Postgres tests are not configured locally, say that explicitly in the PR or
handoff. Do not pretend they passed.

### Task 15: Manual Smoke Tests

Use a temporary directory so you do not touch a real broker database:

```bash
tmpdir=$(mktemp -d)
cd "$tmpdir"
broker write q old
broker peek q --all --json
broker write q new
broker peek q --all --json
```

Capture the timestamp of `new`, then:

```bash
broker peek q --all --before "$new_ts"
broker read q --before "$new_ts"
broker read q --all
```

Expected:

- First command shows only `old`.
- Second command consumes `old`.
- Third command shows only `new`.

Move smoke:

```bash
broker write src old1
broker write src old2
broker peek src --all --json
broker write src new1
broker peek src --all --json
broker move src dst --all --before "$new1_ts"
broker peek src --all
broker peek dst --all
```

Expected:

- Move output is `old1` then `old2`.
- Source contains `new1`.
- Destination contains `old1` and `old2`.

## Invariants

These must hold after implementation:

- Existing `--since` behavior is unchanged.
- `--before` accepts exactly the same timestamp formats as `--since`.
- `--before` uses strict `<`.
- `--since` uses strict `>`.
- Combining filters means `since_timestamp < ts < before_timestamp`.
- Matching messages are returned in FIFO order.
- Non-matching messages are not consumed or moved.
- Empty/no-match filtered operations return exit code `2`, matching existing
  queue-empty behavior.
- Invalid timestamp input returns exit code `1` with the existing invalid
  timestamp message shape.
- `--message` cannot be combined with `--before`.
- No `watch --before` flag exists.
- SQLite and Postgres SQL builders both support the new predicate.
- No database migration is added.
- No new runtime dependency is added.

## Common Failure Modes

- Using truthiness for timestamps. `0` is valid. Always use `is not None`.
- Accidentally making `--before` inclusive. The boundary test must fail if this
  happens.
- Reversing output order. Do not change `ORDER BY id`.
- Adding `watch --before` because `watch` has `--since`. That is explicitly out
  of scope.
- Adding a new general filter framework. Two integer bounds do not justify it.
- Mocking `Queue` or `BrokerDB` in behavior tests. Use real temp databases.
- Updating only SQLite and forgetting the Postgres SQL builder.
- Updating CLI but not public `Queue` APIs, creating a split-brain feature.
- Updating public `Queue` APIs but not lower-level `BrokerDB` methods, causing
  some code paths to ignore the filter.

## Self-Review Checklist for the Implementer

Before opening a PR, read your diff with these questions:

1. Did I touch only the files needed for timestamp filtering, tests, and docs?
2. Can I explain why `watch` is untouched?
3. Is every new `before_timestamp` parameter passed through to the next layer?
4. Did I use `is not None` for timestamp checks?
5. Do read, peek, and move all share the same parsing rules?
6. Do SQLite and Postgres use the same predicate semantics?
7. Do tests prove non-matching messages are preserved?
8. Do tests prove strict boundary behavior?
9. Did I run at least the focused gates?
10. Did I document any skipped Postgres gate honestly?

## Plan Review Notes

This plan was reviewed against the current code shape on 2026-05-13.

Review decisions:

- Kept `watch --before` out of scope because the behavior is not a simple
  reversal of `watch --since`.
- Chose an open interval when both filters are present because it is simpler
  than mutual exclusion and more useful for users.
- Chose `before_timestamp` as the public Python API name to match
  `since_timestamp`.
- Avoided new pending-message SQL constants because `has_pending()` and watcher
  behavior stay out of scope.
- Required real CLI and `Queue` tests because helper-only tests would miss
  destructive-read and move preservation bugs.
