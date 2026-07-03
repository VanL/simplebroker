# Message ID Validation and Diagnostics Plan

Date: 2026-07-02

Status: proposed

Owner: SimpleBroker

## Purpose

Make exact message ID handling consistent, diagnosable, and DRY across the
CLI and Python API.

Today malformed `-m/--message` values are not handled consistently:

- Text CLI `read`, `peek`, `delete`, and `move` return exit code `1` for
  some malformed values without writing any stderr diagnostic.
- JSON mode for `read`, `peek`, and `move` already emits a useful
  `INVALID_MESSAGE_ID` diagnostic.
- A valid but absent message ID returns exit code `2` silently. That behavior
  is correct and should remain unchanged.
- `cmd_delete()` treats malformed IDs as "not found" internally, returning
  `EXIT_QUEUE_EMPTY`. The executable CLI masks some of that with its own
  early validation, but out-of-range 19-digit IDs still expose the drift.
- The Python API accepts exact ID parameters typed as `int`, but passing bad
  strings or floats often behaves like a miss instead of raising. This is now
  part of a release that can include minor API hardening, so fix that too.

This plan assumes the implementer is a skilled Python developer but has no
context for this codebase. Follow it in order. Use red-green TDD. Keep the
change focused: validation, diagnostics, and docs only.

## Current Behavior To Preserve Or Change

### CLI Behavior

Preserve:

- Existing valid message ID:
  - `read q -m <id>` returns `0` and prints the message.
  - `peek q -m <id>` returns `0` and prints the message.
  - `delete q -m <id>` returns `0` and prints nothing.
  - `move q dest -m <id>` returns `0` and prints the moved message.
- Valid but absent 19-digit message ID:
  - returns `2`
  - stdout is empty
  - stderr is empty

Change:

- Malformed message ID, including wrong length, non-digits, empty strings,
  floats, ISO dates, Unix timestamp strings, and out-of-range 19-digit values:
  - returns `1`
  - stdout is empty
  - stderr contains a diagnostic
- JSON mode for commands that support `--json` must continue to emit JSON
  diagnostics.

Do not add `--json` to `delete` as part of this work. That is a separate CLI
surface decision.

### Python API Behavior

Preserve:

- Valid but absent exact ID returns the existing miss value:
  - `None` for single read/peek/move paths
  - `False` for `Queue.delete(message_id=...)`
  - `0` for `delete_message_ids(...)`
- Existing valid integer message IDs still work.

Change:

- Exact-ID API parameters should validate type and range before reaching SQL.
- Malformed exact IDs should raise:
  - `TypeError` for unusable types such as `None` where an ID is required,
    `bool`, floats, lists, objects, etc.
  - `ValueError` for malformed or out-of-range values.
- Exact 19-digit strings may be accepted and normalized to `int`. This keeps
  the CLI and API on one validation path and is useful for IDs loaded from
  JSON or shell output. Non-exact strings raise `ValueError`.

### Similar Places Found During Review

The same silent-miss pattern exists for Python API timestamp bounds in core
retrieve paths. For example, `Queue.peek(after_timestamp="bad")` currently
returns `None`, and `Queue.read(after_timestamp=1.5)` can read a message
instead of raising. CLI `--after` and `--before` already diagnose malformed
values. Since this release already includes API hardening, centralize API
validation for retrieval `after_timestamp` and `before_timestamp` too.

Do not broaden the CLI work beyond message ID diagnostics. Other CLI error
paths found in `cli.py` and `commands.py` already print through argparse,
`_emit_error()`, or explicit `stderr` messages, except quiet-mode behavior
which is intentionally quiet and out of scope.

## Locked Decisions

Do not reopen these unless a failing test proves the plan cannot work.

- Use one canonical validator module for exact message IDs.
- Do not keep separate length/digit/range checks in `cli.py`, `commands.py`,
  `sbqueue.py`, `db.py`, or tests.
- Use one diagnostic text for malformed message IDs:

  ```text
  invalid message ID: expected exactly 19 digits within range
  ```

- Text mode diagnostics should go through `commands._emit_error()` so they use
  the standard `simplebroker: error: ...` shape.
- JSON diagnostics should use code `INVALID_MESSAGE_ID` and `retryable: false`.
- Do not introduce a new exit code. Malformed input remains exit code `1`.
- Do not treat malformed IDs as nonexistent messages. Nonexistent applies only
  after an ID has parsed successfully.
- Do not use `DataError` for local API argument validation. `DataError` is
  database-flavored in this project. Use `TypeError` and `ValueError`, matching
  existing helpers such as `validate_timestamp_bound()`.
- Keep `parse_exact_message_id()` as a compatibility wrapper because it is
  exported from `commands.py`, but it must delegate to the canonical validator.
  It must not contain its own validation rules.
- Do not change `--after` / `--before` CLI behavior in this patch except as
  required by shared helper refactoring. They already emit diagnostics.

## Repository Primer

### Runtime Files

- `simplebroker/_timestamp.py`
  - Contains `TimestampGenerator.validate(..., exact=True)`.
  - Contains `validate_timestamp_bound(name, value)`.
  - Use its constants and range rules rather than duplicating max-int checks.

- `simplebroker/_message_insert.py`
  - Has `normalize_message_id()` for exact-ID inserts today.
  - Replace or redirect this helper to the new canonical message-ID validator.

- `simplebroker/commands.py`
  - CLI command implementation layer.
  - Has `parse_exact_message_id()` and `_resolve_timestamp_filters()`.
  - Has `cmd_read()`, `cmd_peek()`, `cmd_delete()`, and `cmd_move()`.
  - `cmd_delete()` currently diverges: malformed ID returns `EXIT_QUEUE_EMPTY`.

- `simplebroker/cli.py`
  - Argument parser and command dispatch.
  - Currently repeats ad hoc `len(...) != 19 or not .isdigit()` validation in
    the `read`, `peek`, `delete`, and `move` dispatch branches.
  - Remove that validation. Leave mutual-exclusion checks here.

- `simplebroker/sbqueue.py`
  - Public `Queue` API.
  - High-level exact-ID args: `read(message_id=...)`, `peek(message_id=...)`,
    `move(message_id=...)`, `delete(message_id=...)`, `delete_many(...)`.
  - Granular exact-ID args: `read_one(exact_timestamp=...)`,
    `read_generator(exact_timestamp=...)`, `peek_one(...)`,
    `peek_generator(...)`, `move_one(...)`, `move_generator(...)`.

- `simplebroker/db.py`
  - Core backend-neutral implementation.
  - Central retrieve seam: `_build_retrieve_spec()` and `_retrieve()`.
  - Batch delete seam: `delete_message_ids()`.
  - Put most core validation here so `Queue` does not duplicate it.

- `simplebroker/_backend_plugins.py`
  - Protocol types used by `Queue`.
  - Update exact-ID type hints if the API accepts exact ID strings.

- `simplebroker/_sql/_query_spec.py`
  - Backend-neutral query spec. Keep this typed as normalized `int | None`.
    Validation should happen before constructing the spec.

- `CHANGELOG.md`
  - Add one Unreleased entry covering CLI diagnostics and API hardening.

- `README.md`
  - Check whether the documented exit-code contract mentions malformed `-m`.
  - Update the command options, exit-code section, and Python API examples for
    the new validation contract.

### Test Files

- `tests/test_message_by_timestamp.py`
  - Existing CLI-by-message-ID behavior tests live here.
  - Add black-box CLI tests here for malformed, absent, and existing IDs.

- `tests/test_json_output.py`
  - Existing JSON diagnostics tests live here.
  - Keep JSON diagnostic assertions here.

- `tests/test_cli_edge_cases.py`
  - Current mocked CLI edge tests include invalid message ID formats.
  - Keep these narrow. Do not make mocked tests the main proof of behavior.

- `tests/test_commands_helpers.py`
  - Tests `_resolve_timestamp_filters()`.
  - Update the invalid-message-ID expectation from silent stderr to diagnostic.

- `tests/test_parse_exact_message_id.py`
  - Existing pure parser tests.
  - Keep these as compatibility wrapper tests.

- `tests/test_message_id_validation.py`
  - New focused validator tests for `simplebroker._message_id`.

- `tests/test_queue_api_comprehensive.py`
  - Existing `Queue` read/peek/move/delete API tests.
  - Add `Queue` API exact-ID validation tests here.
  - Add API timestamp-bound validation tests here.

- `tests/test_queue_api_additions.py`
  - Existing delete/message-ID convenience coverage.
  - Add `Queue.delete()` and `Queue.delete_many()` validation tests here.

- `tests/test_batch_delete.py`
  - Existing `BrokerCore.delete_message_ids()` coverage.
  - Add invalid-ID batch delete tests here.

- `tests/test_move_by_id.py`
  - Existing `BrokerCore.move_one(... exact_timestamp=...)` coverage.
  - Add direct core move validation tests here.

## Tooling And Style

- Run commands from the repository root.
- Use `uv run pytest ...` for normal tests.
- Use `uv run ./bin/pytest-pg ...` for Postgres-backed tests.
- Use `-n0 -v` while developing one failing test. The default suite uses
  xdist and is noisier for red-green work.
- Use `rg` for code search.
- Keep edits ASCII.
- Use `apply_patch` for manual edits.
- Do not mock `Queue`, `BrokerCore`, sqlite3, or backend plugins in the main
  behavior tests. Use the real CLI and real `Queue`/`BrokerCore` APIs.
- A small helper test for `_resolve_timestamp_filters()` is fine because that
  helper is the intended parsing seam. Do not make helper tests substitute for
  black-box CLI tests.
- DRY: every exact-ID call path must use the canonical validator.
- YAGNI: no new exit codes, no new command flags, no delete JSON support, no
  exception hierarchy redesign.

## Canonical Validator Design

Add a new module:

- `simplebroker/_message_id.py`

Define:

```python
INVALID_MESSAGE_ID_MESSAGE = (
    "invalid message ID: expected exactly 19 digits within range"
)

MessageIdInput = int | str

def normalize_message_id(value: object, *, name: str = "message_id") -> int:
    ...
```

Required behavior:

- `int` values:
  - reject `bool` with `TypeError`
  - reject negative values with `ValueError`
  - reject values `>= SQLITE_MAX_INT64` with `ValueError`
  - otherwise return the integer unchanged
- `str` values:
  - use `TimestampGenerator.validate(value, exact=True)`
  - return the normalized integer
  - convert `TimestampError` to `ValueError(INVALID_MESSAGE_ID_MESSAGE)`
- all other types:
  - raise `TypeError(f"{name} must be an int message ID or exact 19-digit string")`

Important: call `TimestampGenerator.validate(..., exact=True)` for strings.
Do not duplicate `TIMESTAMP_EXACT_NUM_DIGITS`, `.isdigit()`, or max-int checks
outside the validator.

Compatibility wrapper:

```python
def parse_exact_message_id(message_id_str: str) -> int | None:
    try:
        return normalize_message_id(message_id_str)
    except (TypeError, ValueError):
        return None
```

That wrapper may remain in `commands.py` for compatibility with existing tests
and imports, but production code should prefer `normalize_message_id()`.

## Task 0: Required Reading

Before editing, read these exact locations:

- `simplebroker/commands.py`
  - `_emit_error`
  - `parse_exact_message_id`
  - `_resolve_timestamp_filters`
  - `cmd_read`
  - `cmd_peek`
  - `cmd_delete`
  - `cmd_move`
- `simplebroker/cli.py`
  - `add_read_peek_args`
  - the `read`, `peek`, `delete`, and `move` dispatch branches in `main`
  - the final `except (ValueError, DatabaseError)` handler
- `simplebroker/_timestamp.py`
  - `validate_timestamp_bound`
  - `TimestampGenerator.validate`
  - `TimestampGenerator._validate_exact_timestamp`
- `simplebroker/sbqueue.py`
  - all methods that accept `message_id` or `exact_timestamp`
- `simplebroker/db.py`
  - `_build_retrieve_spec`
  - `_retrieve`
  - `_yield_transactional_batches`
  - `delete_message_ids`
- `simplebroker/_message_insert.py`
  - `normalize_message_id`
- tests listed in the Test Files section.

## Task 1: Red Test The CLI Contract

Write black-box CLI tests before touching implementation.

Preferred file:

- `tests/test_message_by_timestamp.py`

Use the existing `run_cli` fixture and `workdir` fixture. Create a message,
read its timestamp through an existing timestamp-producing path, then derive a
valid absent ID by adding `1`.

Test cases:

- `read q -m not-an-id`
- `peek q -m not-an-id`
- `delete q -m not-an-id`
- `move q dest -m not-an-id`
- `read q -m 9999999999999999999`
- `peek q -m 9999999999999999999`
- `delete q -m 9999999999999999999`
- `move q dest -m 9999999999999999999`

Expected malformed behavior:

- return code is `1`
- stdout is `""`
- stderr contains `invalid message ID`
- stderr contains `simplebroker: error`

Also test absent valid behavior for the four commands:

- return code is `2`
- stdout is `""`
- stderr is `""`

Keep these tests behavior-focused. Do not assert the exact function that parsed
the ID. Do not mock `commands._emit_error()`.

Red gate:

```bash
uv run pytest tests/test_message_by_timestamp.py -n0 -v -k "message_id"
```

Expected failures on current code:

- malformed text mode has empty stderr for `read`, `peek`, `delete`, `move`
- out-of-range delete returns `2` instead of `1`

## Task 2: Red Test JSON Diagnostics

Update or add JSON tests for supported commands only.

File:

- `tests/test_json_output.py`

Commands:

- `read q -m not-an-id --json`
- `peek q -m not-an-id --json`
- `move q dest -m not-an-id --json`
- `read q -m 9999999999999999999 --json`

Expected behavior:

- return code is `1`
- stdout is `""`
- stderr parses as JSON
- payload has:
  - `error == "INVALID_MESSAGE_ID"`
  - `retryable is False`
  - `message` contains `invalid message ID`

Do not add JSON tests for `delete`; `delete --json` is not supported today.

Red gate:

```bash
uv run pytest tests/test_json_output.py -n0 -v -k "message_id"
```

## Task 3: Red Test The Canonical Validator

Add tests for `normalize_message_id()`.

Preferred file:

- `tests/test_message_id_validation.py`

Use literal expected values. Do not recompute expected values with the same
logic as the code.

Valid cases:

- `1234567890123456789` as `int`
- `"1234567890123456789"` as `str`
- `"0000000000000000001"` normalizes to `1`
- `0` as `int`

Invalid cases:

- `""`
- `"123"`
- `"12345678901234567890"`
- `"123456789012345678a"`
- `"2024-01-15"`
- `"1705329000s"`
- `"9999999999999999999"`
- `-1`
- `9223372036854775808`
- `True`
- `1.25`
- `None`
- `object()`

Expected exceptions:

- malformed strings and out-of-range integers: `ValueError`
- non-ID types and `bool`: `TypeError`

Also update compatibility wrapper tests:

- `parse_exact_message_id(valid_str)` returns `int`
- `parse_exact_message_id(malformed_str)` returns `None`

Red gate:

```bash
uv run pytest tests/test_message_id_validation.py -n0 -v
uv run pytest tests/test_parse_exact_message_id.py -n0 -v
```

## Task 4: Implement The Canonical Validator

Touch:

- add `simplebroker/_message_id.py`
- update `simplebroker/commands.py`
- update `simplebroker/_message_insert.py`

Implementation steps:

1. Add `INVALID_MESSAGE_ID_MESSAGE`, `MessageIdInput`, and
   `normalize_message_id()` to `simplebroker/_message_id.py`.
2. Update `commands.parse_exact_message_id()` to delegate to
   `normalize_message_id()`.
3. Update `_message_insert.normalize_message_id()`:
   - either remove it and import the canonical function under the old name
   - or keep a wrapper for insert-specific error wording
   - do not duplicate timestamp-bound checks
4. Re-run the validator tests.

Green gate:

```bash
uv run pytest tests/test_parse_exact_message_id.py -n0 -v
```

## Task 5: Centralize CLI Message ID Parsing

Touch:

- `simplebroker/cli.py`
- `simplebroker/commands.py`
- `tests/test_commands_helpers.py`
- `tests/test_cli_edge_cases.py`

Implementation steps:

1. In `cli.py`, remove ad hoc message-ID shape checks from `read`, `peek`,
   `delete`, and `move`.
2. Leave mutual-exclusion checks in `cli.py`:
   - `read` / `peek`: `--message` cannot combine with `--all`, `--after`, or
     `--before`
   - `delete`: `--message` requires a queue name
   - `move`: `--message` cannot combine with `--after` or `--before`
3. Remove `TIMESTAMP_EXACT_NUM_DIGITS` from `cli.py` imports if it becomes
   unused.
4. In `commands._resolve_timestamp_filters()`, call `normalize_message_id()`
   for `message_id_str`.
5. On `TypeError` or `ValueError`, call `_emit_error()` for both text and JSON:

   ```python
   _emit_error(
       INVALID_MESSAGE_ID_MESSAGE,
       json_output=json_output,
       code="INVALID_MESSAGE_ID",
   )
   return EXIT_ERROR, None, None, None
   ```

6. In `cmd_delete()`, replace direct `parse_exact_message_id()` usage with the
   same `_resolve_timestamp_filters(None, None, message_id_str, ...)` path.
   Because delete has no `--json`, pass `json_output=False`.
7. Update `tests/test_commands_helpers.py` so invalid message IDs now expect a
   stderr diagnostic instead of empty stderr.
8. Keep `tests/test_cli_edge_cases.py` focused on return code, or update it to
   assert stderr if it captures stderr already. Do not use those mocked tests as
   the only proof.

Green gates:

```bash
uv run pytest tests/test_commands_helpers.py tests/test_cli_edge_cases.py -n0 -v
uv run pytest tests/test_message_by_timestamp.py -n0 -v -k "message_id"
uv run pytest tests/test_json_output.py -n0 -v -k "message_id"
```

## Task 6: Harden Python API Exact ID Parameters

Touch:

- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `simplebroker/_backend_plugins.py`

Core implementation:

1. In `simplebroker/db.py`, import `normalize_message_id` and `MessageIdInput`.
2. In `_build_retrieve_spec()`, normalize `exact_timestamp` if it is not
   `None`, before constructing `RetrieveQuerySpec`.
3. Keep `RetrieveQuerySpec.exact_timestamp` as `int | None`.
4. In `delete_message_ids()`, normalize every supplied ID before deduping:

   ```python
   normalized_ids = tuple(normalize_message_id(mid) for mid in message_ids)
   deduped = tuple(dict.fromkeys(normalized_ids))
   ```

5. Keep `if not message_ids: return 0` before normalization.
6. Do not add per-method duplicate validation in `claim_one()`, `peek_one()`,
   `move_one()`, generators, or `Queue` methods. The central seams are
   `_build_retrieve_spec()` and `delete_message_ids()`.

Type and docs implementation:

1. In `simplebroker/sbqueue.py`, update public type hints and docstrings for
   exact ID inputs to accept `int | str | None` where appropriate.
2. In `simplebroker/db.py`, update type hints for `exact_timestamp` parameters
   to `MessageIdInput | None` where they are public/core API parameters.
3. In `simplebroker/_backend_plugins.py`, update `BrokerConnection` protocol
   exact-ID signatures to match.
4. Do not update SQL query spec types to accept strings. SQL sees only
   normalized integers.

Red tests:

Add API tests before implementation in these files:

- `tests/test_queue_api_comprehensive.py`
- `tests/test_queue_api_additions.py`
- `tests/test_batch_delete.py`
- `tests/test_move_by_id.py`

Required API invariants:

- `Queue.read(message_id="not-an-id")` raises `ValueError`.
- `Queue.peek(message_id="not-an-id")` raises `ValueError`.
- `Queue.move("dest", message_id="not-an-id")` raises `ValueError`.
- `Queue.delete(message_id="not-an-id")` raises `ValueError`.
- `Queue.delete_many(["not-an-id"])` raises `ValueError`.
- `BrokerCore.peek_one("q", exact_timestamp="not-an-id")` raises `ValueError`.
- `BrokerCore.claim_one("q", exact_timestamp="not-an-id")` raises `ValueError`.
- `BrokerCore.move_one("q", "dest", exact_timestamp="not-an-id")` raises
  `ValueError`.
- `BrokerCore.delete_message_ids("q", ["not-an-id"])` raises `ValueError`.
- `True`, `1.25`, `None` where an ID is required raise `TypeError`.
- `9999999999999999999` raises `ValueError`.
- A valid exact string ID works and behaves like the equivalent integer.
- A valid but absent integer ID still returns the miss value.
- Generator methods such as `Queue.read_generator(exact_timestamp=...)`,
  `Queue.peek_generator(exact_timestamp=...)`, and
  `Queue.move_generator(exact_timestamp=...)` raise on iteration. Tests must
  call `next(generator)` or `list(generator)` inside `pytest.raises(...)`;
  constructing the generator object alone is not enough.

Do not write one test per method per bad value. Use a small parametrized matrix
that proves each public surface uses the central path, plus a separate focused
validator test that exhaustively covers value classes.

Green gate:

```bash
uv run pytest tests/test_queue_api_comprehensive.py tests/test_queue_api_additions.py tests/test_batch_delete.py tests/test_move_by_id.py -n0 -v -k "message_id or exact_timestamp"
```

## Task 7: Harden API Timestamp Bounds At The Retrieve Seam

This task is required. It fixes the same class of API bug as malformed
message IDs: caller-supplied values typed as integer timestamps currently
reach SQL without consistent validation and can behave like misses or produce
backend-specific comparisons. Keep the implementation small by validating only
at the central retrieve seam.

Touch:

- `simplebroker/db.py`
- `simplebroker/sbqueue.py` docstrings for `after_timestamp` and
  `before_timestamp`
- API tests in `tests/test_queue_api_comprehensive.py`

Implementation:

1. In `_build_retrieve_spec()`, normalize:
   - `after_timestamp = validate_timestamp_bound("after_timestamp", after_timestamp)`
   - `before_timestamp = validate_timestamp_bound("before_timestamp", before_timestamp)`
2. Keep `find_message_ids()` and `delete_from_queues()` validation as-is; they
   already call `validate_timestamp_bound()`.
3. Do not change CLI timestamp parsing. CLI accepts rich string timestamp
   formats through `_validate_timestamp()`; API timestamp bounds are typed as
   integer hybrid timestamps.

Required API invariants:

- `Queue.peek(after_timestamp="bad")` raises `TypeError`.
- `Queue.read(after_timestamp=1.25)` raises `TypeError`.
- `Queue.move("dest", after_timestamp="bad")` raises `TypeError`.
- `BrokerCore.peek_many("q", after_timestamp="bad")` raises `TypeError`.
- negative integer bounds raise `ValueError`.
- out-of-range integer bounds raise `ValueError`.
- valid integer bounds still filter strictly.
- Generator methods with invalid timestamp bounds raise on iteration. Tests
  must force iteration to verify the exception.

Gate:

```bash
uv run pytest tests/test_queue_api_comprehensive.py -n0 -v -k "timestamp or stream_messages or exact_timestamp"
```

## Task 8: Documentation And Changelog

Touch:

- `CHANGELOG.md`
- `README.md`
- docstrings in `simplebroker/sbqueue.py` and `simplebroker/db.py`

Changelog entry under `[Unreleased]`:

- CLI: malformed `-m/--message` IDs now emit a diagnostic on stderr while
  preserving exit code `1`; valid but absent IDs still exit `2` silently.
- API: exact message ID parameters now validate type/range instead of treating
  malformed values as absent messages.
- API: retrieval timestamp bounds now validate type/range consistently.

Docs should not overpromise new behavior:

- Do not say `delete --json` exists.
- Do not say malformed input has a unique exit code.
- Do say scripts can distinguish malformed from absent by exit code plus
  stderr:
  - malformed: `1` and stderr diagnostic
  - absent: `2` and no stderr
- In README command options, state that `-m <id>` must be an exact 19-digit
  SimpleBroker message ID within range.
- In README Python API sections, state that exact message ID APIs accept an
  integer ID or an exact 19-digit ID string, and that malformed IDs raise
  `TypeError` or `ValueError`.
- In README timestamp-bound API sections, state that Python API
  `after_timestamp` and `before_timestamp` are integer hybrid timestamps, not
  the rich CLI timestamp strings accepted by `--after` and `--before`.

## Task 9: Full Validation Gates

Run narrow gates first:

```bash
uv run pytest tests/test_message_id_validation.py -n0 -v
uv run pytest tests/test_parse_exact_message_id.py -n0 -v
uv run pytest tests/test_commands_helpers.py tests/test_cli_edge_cases.py -n0 -v
uv run pytest tests/test_message_by_timestamp.py -n0 -v -k "message_id"
uv run pytest tests/test_json_output.py -n0 -v -k "message_id"
uv run pytest tests/test_queue_api_comprehensive.py tests/test_queue_api_additions.py tests/test_batch_delete.py tests/test_move_by_id.py -n0 -v -k "message_id or exact_timestamp or timestamp"
```

Then lint and type:

```bash
uv run ruff format --check simplebroker tests
uv run ruff check simplebroker tests
uv run mypy simplebroker bin/release.py extensions/simplebroker_pg/simplebroker_pg
```

Then broad gates:

```bash
uv run pytest
uv run ./bin/pytest-pg
git diff --check
```

If Docker is unavailable, report that `uv run ./bin/pytest-pg` could not be
run and include the exact reason.

## Final Review Checklist

Before finalizing, verify these by reading the diff:

- No `len(message_id)`, `.isdigit()`, or `TIMESTAMP_EXACT_NUM_DIGITS` checks
  remain outside the canonical validator and timestamp internals.
- `cli.py` no longer performs message ID shape validation.
- `cmd_delete()` uses the same message-ID parsing path as `read`, `peek`, and
  `move`.
- Text CLI malformed IDs always emit stderr.
- Valid but absent IDs still emit no stderr.
- JSON diagnostics still parse as JSON.
- `RetrieveQuerySpec` receives normalized integers, not strings.
- API invalid IDs raise before SQL is built.
- API timestamp-bound invalid values raise before SQL is built.
- Generator validation tests actually iterate the generator.
- No new public flags, exit codes, or exception classes were added.

## Pitfalls To Avoid

- Do not "fix" absent IDs by printing diagnostics. Absence is not an error.
- Do not classify malformed IDs as exit code `2`.
- Do not add per-command custom diagnostic strings. Use one message.
- Do not add backend-specific validation in SQL builders or plugins. Validate
  before constructing backend-neutral query specs.
- Do not rely on mocked command functions to prove CLI behavior. Use `run_cli`.
- Do not catch broad `Exception` around API validation tests. Assert the
  specific `TypeError` or `ValueError`.
- Do not make `DataError` the API validation exception in this patch. That
  would mix caller argument validation with database data errors and make the
  API harder to reason about.
