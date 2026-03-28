# Review Remediation Plan

## Purpose

This document is the implementation plan for the three concrete review findings plus the README/API contract drift they exposed:

1. `Queue.stream_messages()` ignores `all_messages` and `batch_processing`.
2. Re-entrant mutation during `at_least_once` generators is not safely supported, despite code comments implying broader re-entry safety.
3. `Queue.delete()` returns `True` for whole-queue deletes even when nothing was deleted.
4. README and code comments are out of sync with actual delete/vacuum behavior.

This plan assumes the implementer is new to this repository. Follow it in order. Keep the changes small, explicit, and well-tested.

## Repository Primer

### Project shape

- Runtime code lives in `simplebroker/`.
- Tests live in `tests/`.
- Package/tooling config lives in `pyproject.toml`.
- Public user docs live in `README.md`.

### Files you must read before changing anything

- `pyproject.toml`
- `simplebroker/sbqueue.py`
- `simplebroker/db.py`
- `simplebroker/watcher.py`
- `README.md`
- `tests/test_queue_api_comprehensive.py`
- `tests/test_queue_api_additions.py`
- `tests/test_generator_methods.py`

### Tooling and commands

Use these commands from the repo root.

- Targeted tests while iterating:
  - `pytest -q -n 0 tests/test_queue_api_comprehensive.py -k "stream_messages or delete"`
  - `pytest -q -n 0 tests/test_generator_methods.py -k "reentrant or at_least_once"`
- Full tests:
  - `pytest -q`
- Lint:
  - `ruff check .`
- Type-check:
  - `mypy simplebroker`

Notes:

- `pyproject.toml` enables `pytest-xdist` by default via `-n auto`. Use `-n 0` for local debugging of DB/transaction tests so failures are easier to reason about.
- Do not add dependencies.

## Engineering Rules For This Work

### TDD: red, green, refactor

For each task:

1. Add or strengthen a real test that fails on current `main`.
2. Run that targeted test and confirm it is red for the right reason.
3. Make the smallest code change that turns it green.
4. Refactor only after behavior is pinned by tests.

Do not “fix code and then add tests.” This repo already has enough behavior drift.

### Prefer real integration-style tests over mocks

Use real `Queue`/`BrokerDB` instances with temporary SQLite files.

- Good: `tempfile.TemporaryDirectory()`, `NamedTemporaryFile()`, `Queue`, `BrokerDB`, real reads/writes/moves.
- Bad: mocking `_runner`, monkeypatching SQLite cursors, asserting internal call counts, testing implementation details instead of queue state.

Mock only at process or OS boundaries when there is no practical real alternative. These tasks do not need that.

### DRY

- Reuse existing high-level methods before inventing new ones.
- If multiple mutating entry points need the same guard, create one small private helper in `BrokerCore` instead of open-coding checks in many methods.
- Keep test helpers local unless you reuse them 3 or more times.

### YAGNI

Do not redesign the transaction model, watcher architecture, or public API shape beyond what is required to fix the documented findings.

Especially:

- Do not build a generic nested transaction framework.
- Do not add savepoint orchestration unless you first prove it is necessary and safe. It probably is not for this scope.
- Do not rewrite watchers just because they consume `stream_messages()`. Only touch `watcher.py` if tests prove it is necessary.

## Non-Goals

- No new features.
- No style-only refactor.
- No new abstractions unless they eliminate duplication introduced by these fixes.
- No change to CLI exit-code behavior unless a failing test proves the current CLI behavior is wrong.

## Task 0: Establish A Safe Baseline

### Goal

Start from a clean understanding of current behavior and create a checkpoint before modifying runtime code.

### Files to touch

- None

### Steps

1. Read the files listed in the primer.
2. Run the full validation once:
   - `pytest -q`
   - `ruff check .`
   - `mypy simplebroker`
3. Do not begin implementation until you understand:
   - `Queue.stream_messages()` in `simplebroker/sbqueue.py`
   - transactional retrieval in `simplebroker/db.py`
   - existing tests around queue API and generators

### Gate

- You can explain, in one paragraph, the difference between:
  - `Queue.read_one()` / `BrokerDB.claim_one()`
  - `Queue.stream_messages()`
  - `BrokerDB.claim_generator(..., delivery_guarantee="at_least_once")`

## Task 1: Fix `Queue.stream_messages()` Contract Drift

### Problem statement

`Queue.stream_messages()` advertises control via `all_messages`, `batch_processing`, and `commit_interval`, but the current implementation ignores key flags.

This is a public API contract problem. Preserve the existing public method, but make its behavior match its documented controls.

### Primary files to touch

- `simplebroker/sbqueue.py`
- `tests/test_queue_api_comprehensive.py`

### Files to read, but avoid touching unless necessary

- `simplebroker/watcher.py`
- `tests/test_streaming.py`

### Red tests to add first

Add real-state tests in `tests/test_queue_api_comprehensive.py` next to the existing `stream_messages` coverage.

Required new tests:

1. `stream_messages(peek=True, all_messages=False)` yields at most one tuple and does not remove that message.
2. `stream_messages(peek=False, all_messages=False)` yields at most one tuple and removes exactly one message.
3. `stream_messages(peek=False, all_messages=True, batch_processing=False, commit_interval=3)` still behaves one-message-at-a-time.
   - Break after the first yielded message.
   - Assert only the first message is gone.
   - Assert the rest remain.
   - This proves `batch_processing=False` is honored even when `commit_interval > 1`.
4. `stream_messages(peek=False, all_messages=True, batch_processing=True, commit_interval=3)` uses batch semantics intentionally.
   - Yield one message, stop early, close the generator.
   - Assert the unfinished batch is rolled back according to the chosen contract.
   - If you map `commit_interval` to batch size, assert rollback/redelivery covers that batch, not some unrelated default batch size.

### Implementation guidance

Do not add new SQL. This is an adapter-layer fix.

`Queue.stream_messages()` should be a thin router over existing primitives:

- Peek + single-message path should use `peek_one()` semantics.
- Peek + all-messages path should use `peek_generator()`.
- Consume + single-message path should use `read_one()` / `claim_one()` semantics.
- Consume + streaming path should use `claim_generator()`.

Recommended behavior:

- `all_messages=False` means “yield at most one item”, regardless of `commit_interval`.
- `batch_processing=False` means “consume one at a time”, even when streaming all messages.
- `batch_processing=True` enables batch behavior for consume mode.
- `commit_interval` should matter only when `batch_processing=True` and `peek=False`.

If `commit_interval` is used for batching, thread it through explicitly rather than silently relying on a global default.

### Invariants to assert in tests

- No hidden over-consumption.
- No hidden rollback when `batch_processing=False`.
- Early termination in single-message mode never affects more than one message.
- Peek mode never removes messages.
- Consume mode removes exactly the messages actually processed in exactly-once mode.

### What not to do

- Do not move queue semantics into `watcher.py`.
- Do not add new branching in multiple call sites if one branch in `Queue.stream_messages()` is enough.
- Do not “fix” this by deleting parameters from the public method. That is a separate API-breaking decision and out of scope.

### Gate

Run:

- `pytest -q -n 0 tests/test_queue_api_comprehensive.py -k "stream_messages"`

Then confirm the old failing tests are green for the right behavioral reason, not because assertions were weakened.

## Task 2: Make Re-Entrant Mutation During Transactional Generators Explicit And Safe

### Problem statement

`BrokerCore` now uses `RLock`, and there is a test proving that read-only re-entry no longer deadlocks. But mutating re-entry during `at_least_once` generator iteration still explodes with SQLite’s raw `cannot start a transaction within a transaction`.

This is not a good public contract.

### Recommended scope decision

Do not attempt to make same-instance nested mutation “work” inside an open transactional batch. That is a trap:

- Same-connection nested writes collide with the outer transaction.
- Savepoint-based semantics would be subtle and easy to get wrong.
- Separate-connection semantics inside the same thread would still fight the write lock.

Recommended fix:

- Keep read-only re-entry working.
- Explicitly reject re-entrant mutating operations on the same `BrokerDB` while an `at_least_once` batch transaction is open.
- Raise a clear, actionable exception from our code before SQLite raises its opaque transaction error.

### Primary files to touch

- `simplebroker/db.py`
- `tests/test_generator_methods.py`

### Red tests to add first

Add real integration tests in `tests/test_generator_methods.py`.

Required tests:

1. Read-only re-entry still works.
   - Keep the existing no-deadlock test.
   - It should continue proving that `peek_one()` during `claim_generator(... at_least_once ...)` does not deadlock.
2. Re-entrant mutation during `claim_generator(... at_least_once ...)` fails with a clear library-level error.
   - Inside the loop, call `broker.write("other", "nested")`.
   - Assert the exception type and message are explicit.
   - Assert the outer batch rolls back cleanly.
   - Assert source queue contents are unchanged.
   - Assert the nested write did not land.
3. Same as above for `move_generator(... at_least_once ...)`.
   - Assert source and destination queues are unchanged after the rejected nested mutation.

If the two mutation tests share nearly identical setup, use one small parametrized test. Do not duplicate 40 lines twice.

### Implementation guidance

Add one small private mechanism in `BrokerCore` to track “transactional batch currently yielding”.

Requirements for that mechanism:

- It must be set in `_yield_transactional_batches()`.
- It must be cleared in `finally`, even on error or generator close.
- It must only guard unsafe mutating operations, not read-only peek paths.

Preferred shape:

- A private context flag plus one helper such as `_assert_no_reentrant_mutation_during_batch()`.
- Call that helper from the minimal set of mutating entry points that can open a new transaction.

Likely mutating paths to audit:

- `write()`
- claim/move entry points that begin transactions
- `delete()`
- any other direct mutation path that starts `BEGIN IMMEDIATE`

Do not over-apply the guard to read-only operations.

### Error-contract guidance

Prefer a library-level exception with a message like:

- operation is not supported during `at_least_once` generator callback re-entry
- use `exactly_once` mode or a separate broker/queue instance

Do not surface raw SQLite transaction text as the primary failure.

### Comment/doc cleanup required with this task

Update any misleading comments/docstrings in `simplebroker/db.py` that imply broad re-entry safety. Be precise:

- Read-only re-entry can be supported.
- Mutating re-entry during an open transactional batch is intentionally rejected.

### Invariants to assert in tests

- No deadlock for read-only re-entry.
- No partial state change when unsafe re-entrant mutation is attempted.
- Rejected nested mutation produces a stable, actionable error.
- Generator cleanup leaves the broker usable after the exception.

### What not to do

- Do not invent nested savepoint semantics.
- Do not add a generic transaction manager abstraction.
- Do not “fix” the problem by swallowing the error and continuing silently.

### Gate

Run:

- `pytest -q -n 0 tests/test_generator_methods.py -k "reentrant or at_least_once"`

Then run those tests twice. Transactional regressions often pass once and fail on rerun if cleanup is wrong.

## Task 3: Fix `Queue.delete()` Boolean Contract

### Problem statement

`Queue.delete()` claims to return whether anything was deleted, but whole-queue deletion always returns `True`.

That is a contract bug in the public queue API.

### Primary files to touch

- `simplebroker/sbqueue.py`
- `simplebroker/db.py`
- `tests/test_queue_api_comprehensive.py`
- `tests/test_queue_api_additions.py`

### Strong recommendation for test cleanup

`tests/test_queue_api_additions.py` currently contains weak comments like “we can’t easily test this.” That is not true here. Use real timestamps from `peek_generator(with_timestamps=True)` and strengthen those tests instead of preserving vague ones.

### Red tests to add first

Add or strengthen tests so they cover real behavior:

1. `Queue.delete()` on a populated queue returns `True`.
2. `Queue.delete()` on an empty queue returns `False`.
3. `Queue.delete()` on a queue that never existed returns `False`.
4. `Queue.delete(message_id=existing_ts)` returns `True` and removes only that message.
5. `Queue.delete(message_id=missing_ts)` returns `False`.

Use a real temp DB and real timestamps retrieved from `peek_generator(with_timestamps=True)`.

### Implementation guidance

Do not add a preflight “does this queue exist?” query in `Queue.delete()`. That duplicates work and is race-prone.

Preferred approach:

- Make the DB layer return an affected-row count or equivalent authoritative result.
- Have `Queue.delete()` convert that to `bool`.

This keeps the source of truth in the DB layer and avoids duplicating delete semantics in two places.

Good options:

- execute delete, then query SQLite for affected rows
- or equivalent existing mechanism in the runner layer

Avoid:

- fetching all rows just to count them
- running a separate existence query before delete
- making the queue wrapper guess

### Compatibility guidance

Keep CLI behavior stable unless a targeted CLI test proves it is wrong.

This task is about the Python `Queue` API contract. If `commands.py` does not need to change, do not touch it.

### Invariants to assert in tests

- Whole-queue delete returns `False` when nothing changed.
- Message-ID delete remains precise.
- No false positives.
- No extra queue state changes beyond the requested deletion.

### Gate

Run:

- `pytest -q -n 0 tests/test_queue_api_comprehensive.py -k "delete"`
- `pytest -q -n 0 tests/test_queue_api_additions.py -k "delete"`

## Task 4: Align Docs And Inline Comments With Actual Supported Behavior

### Goal

Once runtime behavior is correct, make the docs say exactly what the code actually supports.

### Primary files to touch

- `README.md`
- `simplebroker/sbqueue.py`
- `simplebroker/db.py`

### Topics to fix

1. Delete semantics
   - Queue-level delete currently hard-deletes rows immediately.
   - Message-ID delete goes through claim semantics and interacts with vacuum differently.
   - README must stop claiming that all `delete` paths merely “mark for removal”.
2. Vacuum wording
   - Vacuum reclaims claimed rows.
   - It is not the mechanism by which whole-queue delete takes effect.
3. Re-entry wording
   - Comments in `db.py` must not imply all callback re-entry is safe.
4. `stream_messages()` docstring
   - Document the actual meaning of `all_messages`, `batch_processing`, and `commit_interval`.

### Documentation sources to check

- `README.md` command reference
- `README.md` delete/vacuum discussion
- inline docstrings in `simplebroker/sbqueue.py`
- comments around the `RLock` and transactional batch helper in `simplebroker/db.py`

### What not to do

- Do not rewrite the README broadly.
- Do not add a new design document.
- Do not document speculative future behavior.

### Gate

Manual review checklist:

- A new user reading the README should not come away believing whole-queue delete waits for vacuum.
- An API user reading `stream_messages()` should understand single-message vs batch behavior.
- A maintainer reading `db.py` comments should not assume nested mutation is supported inside an open batch transaction.

## Task 5: Final Verification And Merge Checklist

### Required automated gates

Run all of these before considering the work done:

- `pytest -q -n 0 tests/test_queue_api_comprehensive.py -k "stream_messages or delete"`
- `pytest -q -n 0 tests/test_generator_methods.py -k "reentrant or at_least_once"`
- `pytest -q`
- `ruff check .`
- `mypy simplebroker`

### Required behavioral spot-checks

These are worth doing manually if any test was rewritten substantially:

1. `Queue.stream_messages(all_messages=False)` yields one item max.
2. Breaking early from non-batch `stream_messages()` does not roll back already-yielded work.
3. Re-entrant `peek_one()` during `at_least_once` iteration still works.
4. Re-entrant `write()` during `at_least_once` iteration fails clearly and leaves no partial side effects.
5. `Queue.delete()` returns `False` on empty/nonexistent queues.

## Definition Of Done

This work is done only when all of the following are true:

- The new tests fail on the old code and pass on the new code.
- No fix relies on mocks for SQLite state behavior.
- `Queue.stream_messages()` behavior matches its public controls.
- Re-entrant mutation during open `at_least_once` batch iteration is either safely supported or, preferably for this scope, explicitly rejected with a clear error.
- `Queue.delete()` truthiness reflects actual deletion.
- README and inline docs match actual behavior.
- `pytest -q`, `ruff check .`, and `mypy simplebroker` all pass.

## Final Advice To The Implementer

- Keep changes surgical.
- Prefer one good test over three shallow mocked tests.
- If you need to choose between a clever abstraction and a boring explicit helper, choose the boring helper.
- If a test description says “we can’t easily test this,” that is usually a sign to improve the test, not lower the bar.
