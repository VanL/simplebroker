# Runner And Batch Contract Remediation Plan

## Purpose

This document is the implementation plan for two concrete review findings:

1. Custom `SQLRunner` injection is not actually used for queue operations.
2. `delivery_guarantee` on list-returning batch APIs (`claim_many()` / `move_many()` and their `Queue` wrappers) does not provide distinct behavior.

This plan assumes the implementer is strong technically but new to this repository and prone to over-engineering or over-mocking. Follow the tasks in order. Keep the changes small, explicit, and test-driven.

The goal is not to redesign SimpleBroker. The goal is to make the documented/public contracts true with the minimum necessary code change.

## Contract Decisions

These are the intended outcomes. Do not start coding until these are clear.

### P1: Injected runner contract

- If no `runner` is supplied, preserve current behavior:
  - `Queue(..., persistent=False)` uses the built-in ephemeral SQLite path.
  - `Queue(..., persistent=True)` uses the built-in cached/thread-local SQLite path.
- If a `runner` instance **is** supplied:
  - queue operations must execute through that supplied runner
  - the queue must not silently create a hidden default `BrokerDB` for actual work
  - the queue may reuse one runner-backed `BrokerCore` for its lifetime
  - `persistent` no longer means “create a new backend wrapper every call” for this path
- The injected runner remains **caller-owned**
  - `Queue.close()`, `DBConnection.cleanup()`, and finalizers must not close it
  - docs/examples must say this explicitly

### P2: Batch list API contract

- `claim_many()` and `move_many()` are materialized list APIs; they should be treated as **exactly-once only**.
- Real `at_least_once` semantics remain available only on generator-based APIs:
  - `claim_generator()`
  - `move_generator()`
  - higher-level streaming helpers that route to those generators
- For compatibility, keep the `delivery_guarantee` parameter on list-returning APIs in this change set, but:
  - treat all list-returning paths as `exactly_once`
  - emit `DeprecationWarning` when the caller passes anything other than `"exactly_once"`
  - update docs/docstrings to steer users to generator APIs for retryable batch semantics

Do **not** invent a “true at-least-once list return” mechanism by holding transactions open across arbitrary user code. That is the wrong design.

## Repository Primer

### Project shape

- Runtime code: `simplebroker/`
- Tests: `tests/`
- Public docs: `README.md`
- Example docs/code: `examples/`
- Tooling config: `pyproject.toml`
- Existing review plan style reference: `docs/plans/review-remediation-plan.md`

### Files you must read before changing anything

- `pyproject.toml`
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `simplebroker/_runner.py`
- `tests/test_batch_operations.py`
- `tests/test_queue_api_comprehensive.py`
- `tests/test_queue_connection_manager.py`
- `tests/test_queue_coverage.py`
- `examples/README.md`
- `examples/logging_runner.py`
- `examples/example_extension_implementation.md`

### Tooling and commands

Use these commands from the repo root.

- First-time setup for this repo:
  - `uv sync --extra dev`
- Targeted tests while iterating:
  - `uv run pytest -q -n 0 tests/test_batch_operations.py tests/test_queue_api_comprehensive.py -k "many or delivery_guarantee"`
  - `uv run pytest -q -n 0 tests/test_queue_connection_manager.py tests/test_queue_coverage.py -k "runner or custom"`
- Full tests:
  - `uv run pytest -q -n 0`
- Lint:
  - `uv run ruff check .`
- Type-check:
  - `uv run mypy simplebroker`

Notes:

- `pyproject.toml` enables `pytest-xdist` by default. Use `-n 0` while debugging DB/transaction behavior so failures are easier to understand.
- Do not add dependencies.
- Do not use plain `pytest`, `ruff`, or `mypy` unless you have already verified they are installed outside `uv`.

## Engineering Rules For This Work

### TDD: red, green, refactor

For each task:

1. Add or strengthen a real test that fails on current `main`.
2. Run the targeted test and confirm it is red for the right reason.
3. Make the smallest code change that turns it green.
4. Refactor only after the contract is pinned by tests.

Do not “fix code and then add tests.” This repository already has contract drift; do not add more.

### Prefer real integration-style tests over mocks

Use real temporary SQLite databases and real `Queue` / `BrokerDB` behavior.

- Good:
  - `tempfile.TemporaryDirectory()`
  - `NamedTemporaryFile()`
  - real `Queue`
  - real `BrokerDB`
  - a small local `RecordingRunner(SQLiteRunner)` subclass when testing the runner extension boundary itself
- Bad:
  - mocking `BrokerDB` or `DBConnection` to “prove” routing
  - monkeypatching SQLite cursors
  - asserting internal call counts instead of queue state
  - adding fake transaction behavior in tests

The only acceptable fake in this work is a thin test-local runner subclass that wraps `SQLiteRunner` and records how it is used. That is the public extension boundary under test.

### DRY

- If you need the same deprecation warning in more than one method, add one small private helper.
- If the same runner-path branch appears in more than one place, centralize it once.
- Keep test helpers local unless you reuse them at least three times.

### YAGNI

Do not redesign the queue architecture.

Especially:

- no new connection-factory API
- no generic ownership framework
- no savepoints or nested transaction system
- no changes to watcher behavior unless a failing test proves they are necessary
- no new env vars or config flags

## Non-Goals

- No rewrite of `BrokerCore` / `BrokerDB`.
- No change to CLI surface.
- No new public API for “owned runner” vs “borrowed runner.”
- No true at-least-once semantics for list-returning batch APIs.
- No changes to SQL query shape unless a failing test proves they are required. They probably are not.

## Task 0: Establish A Safe Baseline

### Goal

Start from a clean understanding of the current contracts and create a verification baseline.

### Files to touch

- None

### Steps

1. Read all files listed in the primer.
2. Run:
   - `uv sync --extra dev`
   - `uv run pytest -q -n 0`
   - `uv run ruff check .`
   - `uv run mypy simplebroker`
3. Confirm you can explain:
   - the difference between `DBConnection`, `BrokerCore`, and `BrokerDB`
   - why `Queue(..., runner=runner)` cannot be treated exactly like the built-in ephemeral SQLite path
   - why a list-returning API cannot safely provide generator-style `at_least_once` semantics

### Gate

- You can explain, in one paragraph each:
  - how a plain `Queue` operation reaches SQLite today
  - where the injected-runner path is lost
  - where `claim_many()` / `move_many()` commit relative to return

## Task 1: Freeze The P1 Contract With Red Tests

### Problem statement

The public extension surface claims that `Queue(..., runner=runner)` uses the supplied runner. Current behavior does not guarantee that.

### Primary files to touch

- `tests/test_queue_connection_manager.py`
- `tests/test_queue_coverage.py`

### Optional new test file

If the existing files become noisy, create:

- `tests/test_custom_runner_integration.py`

That is preferred over stuffing unrelated runner-specific behavior into generic connection-lifetime tests.

### Required red tests

Add real tests using a local `RecordingRunner(SQLiteRunner)` helper that:

- records SQL statements executed through `run()`
- records whether `close()` was called
- otherwise delegates to a real `SQLiteRunner`

Required tests:

1. **Injected runner is used for actual queue writes**
   - Parameterize over `persistent=True` and `persistent=False`.
   - Create `runner = RecordingRunner(db_path)`.
   - Create `Queue("tasks", db_path=db_path, runner=runner, persistent=...)`.
   - Clear the runner log after queue construction.
   - Call `queue.write("hello")`.
   - Assert the runner log contains the real message insert SQL, not just setup/schema SQL.
   - This should fail on current code in **both** modes. If it does not, your test is probably only observing setup SQL rather than the real write path.

2. **Injected runner is used for actual queue reads**
   - Parameterize over `persistent=True` and `persistent=False`.
   - Same setup.
   - Preload one message through the queue.
   - Clear the runner log.
   - Call `queue.read_one(with_timestamps=False)`.
   - Assert the runner log contains the claim/delete SQL used for the read path.
   - Assert the queue state changes correctly.

3. **Injected runner path is reused for queue lifetime even when `persistent=False`**
   - Create `Queue(..., runner=runner, persistent=False)`.
   - Enter `queue.get_connection()` twice.
   - Assert the returned object identity is the same both times.
   - Assert it is a `BrokerCore`.
   - Assert it is **not** a `BrokerDB`.
   - This pins the contract that custom-runner queues do not recreate hidden default SQLite backends per operation.

4. **Queue close and finalizer do not close injected runner**
   - Create queue with `runner=runner`.
   - Call `queue.close()`.
   - Assert `runner.close()` was **not** called.
   - Recreate the queue, invoke the finalizer path used in `tests/test_queue_coverage.py`, and assert the injected runner still was not closed.
   - Then call `runner.close()` explicitly and assert it closes exactly once.

5. **Built-in behavior remains unchanged without injected runner**
   - Preserve or strengthen one existing test showing:
     - no-runner ephemeral queues still create fresh `BrokerDB` instances
     - no-runner persistent queues still reuse cached/thread-local `BrokerDB`
   - Do **not** “fix” this by weakening existing no-runner assertions from `BrokerDB` to `BrokerCore`. The built-in path should stay specific.

### Test design guidance

- Do not use `Mock(spec=SQLRunner)` for this work.
- Do not patch `BrokerDB` construction just to “prove” it was called or not called.
- Use actual queue state and actual runner logs.
- Reset the recording runner’s log **after queue construction** so setup SQL does not mask whether real queue operations route through it.
- Match stable SQL markers only:
  - good: `"INSERT INTO messages"` for writes
  - acceptable: `"DELETE FROM messages"` and/or `"RETURNING"` for claim paths
  - bad: asserting full query strings character-for-character

### Gate

Run:

- `uv run pytest -q -n 0 tests/test_queue_connection_manager.py tests/test_queue_coverage.py -k "runner or custom"`

If you created a new test file, include it explicitly.

Do not proceed until the runner-routing tests are red for the right reason.

## Task 2: Implement P1 With The Smallest Possible Change

### Problem statement

The current code loses the supplied runner inside `DBConnection.get_connection()` and therefore breaks the extension contract.

### Primary files to touch

- `simplebroker/db.py`
- `simplebroker/sbqueue.py`

### Files to read carefully before editing

- `simplebroker/_runner.py`
- `tests/test_queue_connection_manager.py`
- `tests/test_queue_coverage.py`

### Implementation guidance

#### `Queue` behavior

- Preserve current no-runner behavior.
- When `runner is not None`, create and keep one `DBConnection` wrapper for the queue lifetime, regardless of the `persistent` flag.
- In practice, that means the “ephemeral queue” optimization remains only for the built-in backend path.

#### `DBConnection` behavior

- If `DBConnection` was constructed with an external runner:
  - return the runner-backed `BrokerCore`
  - do not construct `BrokerDB(self.db_path)`
  - do not register a hidden default connection for cleanup
- Preserve existing default SQLite behavior when no runner was supplied.

#### Ownership and cleanup

- Keep injected runners caller-owned.
- `DBConnection.cleanup()` must not close externally supplied runners.
- `Queue.close()` and finalizers must remain safe when a runner is injected.
- Add comments/docstrings clarifying the ownership rule.

#### Stop-event propagation

- Preserve `set_stop_event()` behavior for runner-backed connections.
- Do not silently drop stop-event propagation when returning `BrokerCore` instead of `BrokerDB`.

### Invariants to preserve

- No-runner:
  - ephemeral queues still “get in, get out”
  - persistent queues still cache by thread
  - existing no-runner tests that expect `BrokerDB` should continue to pass unchanged
- With runner:
  - actual operations use the supplied runner
  - no hidden default `BrokerDB` does the real work
  - repeated `get_connection()` calls reuse the same runner-backed core
  - queue cleanup does not close the runner

### What not to do

- Do not introduce `runner_factory`.
- Do not add a new `owns_runner` flag.
- Do not rewrite `DBConnection` into a generic pool.
- Do not make tests depend on exact SQL string formatting beyond matching operation type reliably.

### Gate

Run:

- `uv run pytest -q -n 0 tests/test_queue_connection_manager.py tests/test_queue_coverage.py -k "runner or custom"`
- `uv run pytest -q -n 0 tests/test_queue_connection_manager.py`

Only continue when the new contract is green and the existing connection-manager tests still pass.

## Task 3: Freeze The P2 Contract With Red Tests

### Problem statement

The list-returning batch APIs expose a `delivery_guarantee` choice that does not create distinct behavior. That is misleading API surface.

### Primary files to touch

- `tests/test_batch_operations.py`
- `tests/test_queue_api_comprehensive.py`

### Required red tests

Add tests that pin the compatibility/deprecation behavior.

Required tests:

1. **`BrokerDB.claim_many(..., delivery_guarantee="at_least_once")` warns and behaves exactly like default**
   - Use `pytest.warns(DeprecationWarning, match=...)`.
   - Assert returned bodies/timestamps are still correct.
   - Assert claimed messages are gone immediately after the call returns.

2. **`BrokerDB.move_many(..., delivery_guarantee="at_least_once")` warns and behaves exactly like default**
   - Assert the source queue loses the moved messages.
   - Assert the destination queue gains them.
   - Assert warning text points users at generator APIs.

3. **Queue wrapper: `Queue.read_many(..., delivery_guarantee="at_least_once")` warns**
   - This confirms the public Queue API surfaces the same compatibility behavior.

4. **Queue wrapper: `Queue.move_many(..., delivery_guarantee="at_least_once")` warns**
   - Same reasoning as above.

5. **Generator APIs remain the real at-least-once path**
   - Keep an existing generator test or add a focused one proving that `claim_generator(..., delivery_guarantee="at_least_once")` still has meaningful batch semantics.
   - Do not let the P2 cleanup accidentally flatten generator behavior.

### Tests to update, not just add

Existing tests currently normalize the misleading behavior. Update them.

At minimum, revisit:

- `tests/test_batch_operations.py::test_claim_many_at_least_once`
- `tests/test_batch_operations.py::test_move_many_at_least_once`
- `tests/test_queue_api_comprehensive.py::test_read_many_delivery_guarantees`
- `tests/test_queue_api_comprehensive.py::test_move_many_delivery_guarantees`

Turn them into compatibility/deprecation tests rather than “two valid semantics exist” tests.

### Test design guidance

- Use real broker state, not warning-only assertions.
- Assert both:
  - the warning
  - the queue state after the call
- Keep generator tests separate from list-returning tests so the semantic line is obvious.

### Gate

Run:

- `uv run pytest -q -n 0 tests/test_batch_operations.py tests/test_queue_api_comprehensive.py -k "many or delivery_guarantee"`

Do not proceed until those tests are red for the right reason.

## Task 4: Implement P2 With Compatibility, Not Reinvention

### Problem statement

The fix is to reduce misleading semantics, not to build a new transaction model.

### Primary files to touch

- `simplebroker/db.py`
- `simplebroker/sbqueue.py`

### Implementation guidance

#### Behavior

- `claim_many()`:
  - always execute as exactly-once
  - if `delivery_guarantee != "exactly_once"`, emit `DeprecationWarning`
- `move_many()`:
  - same behavior
- `Queue.read_many()` / `Queue.move_many()`:
  - keep parameter for compatibility
  - continue delegating
  - update docstrings so callers are told the truth

#### Warning text

The warning should clearly say:

- the parameter is deprecated on list-returning APIs
- the call is being treated as `"exactly_once"`
- users should switch to generator APIs for `at_least_once`

Use one small helper rather than copy-pasting warning logic in many places.

#### Docstrings/comments

Audit and correct all misleading text in:

- `simplebroker/db.py`
- `simplebroker/sbqueue.py`

Specifically remove or replace phrases like:

- “commit after retrieval and before return” on list-returning APIs
- wording that implies list APIs expose meaningful at-least-once semantics

### Invariants to preserve

- Generator APIs still provide real at-least-once semantics.
- CLI behavior remains unchanged.
- No SQL/query changes unless a failing test proves they are needed.

### What not to do

- Do not hold a write transaction open across user code for list returns.
- Do not add a new batch wrapper object.
- Do not remove the parameter in this change set unless you intentionally choose a breaking release and update every caller. That is not the plan here.

### Gate

Run:

- `uv run pytest -q -n 0 tests/test_batch_operations.py tests/test_queue_api_comprehensive.py -k "many or delivery_guarantee"`
- `uv run pytest -q -n 0 tests/test_generator_methods.py -k "at_least_once"`

Only continue when the list-returning APIs warn and the generator APIs still behave correctly.

## Task 5: Update Docs And Examples To Match Reality

### Goal

End with docs that reflect the new contracts. Do not leave stale examples behind.

### Primary files to touch

- `examples/README.md`
- `examples/logging_runner.py`
- `examples/example_extension_implementation.md`
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`

### Optional file to touch if wording is stale there too

- `README.md`

### Required documentation changes

1. **Injected runner ownership**
   - State clearly that the caller owns the injected runner.
   - Show explicit runner cleanup in examples that create a concrete runner instance locally.

2. **Injected runner lifecycle**
   - State that a supplied runner is reused for queue operations.
   - Do not describe it as the same lifecycle as the built-in ephemeral SQLite path.
   - Where custom-runner docs sketch the protocol, make sure they reflect the actual required `SQLRunner` surface, including `setup()` and `is_setup_complete()`.

3. **Batch list APIs**
   - Document that `claim_many()` / `move_many()` and `Queue.read_many()` / `Queue.move_many()` are exactly-once materialized batch APIs.
   - Mention that passing `delivery_guarantee="at_least_once"` is deprecated and treated as exactly-once.
   - Point users to generator APIs for retryable batch processing.

### Documentation audit commands

Run these searches and clean up stale text:

- `rg -n "Commit after retrieval and before return|delivery_guarantee" simplebroker`
- `rg -n "runner=runner|custom runner|SQLRunner" README.md examples`

Do not mechanically replace text. Read each hit and update only what is actually stale.

### Gate

- The examples no longer imply that `with Queue(..., runner=runner)` will close the runner for the caller.
- No docstring still claims list-returning APIs have meaningful at-least-once semantics.

## Task 6: Final Validation And Review

### Goal

Prove the change is complete, coherent, and minimal.

### Steps

1. Run focused suites again:
   - `uv run pytest -q -n 0 tests/test_queue_connection_manager.py tests/test_queue_coverage.py -k "runner or custom"`
   - `uv run pytest -q -n 0 tests/test_batch_operations.py tests/test_queue_api_comprehensive.py -k "many or delivery_guarantee"`
   - `uv run pytest -q -n 0 tests/test_generator_methods.py -k "at_least_once"`
2. Run full verification:
   - `uv run pytest -q -n 0`
   - `uv run ruff check .`
   - `uv run mypy simplebroker`
3. Re-read the edited docstrings and examples once after tests are green.

### Final review checklist

- P1:
  - supplied runner is actually used for queue operations
  - no-runner behavior is unchanged
  - injected runner remains caller-owned
  - custom-runner examples are honest about cleanup
- P2:
  - list-returning APIs no longer pretend to expose meaningful at-least-once semantics
  - non-default `delivery_guarantee` warns and is treated as exactly-once
  - generator APIs still carry the real at-least-once behavior
- General:
  - no new abstractions without clear need
  - no stale comments or docs
  - no tests that only prove mocks interacted with each other

## If You Get Stuck

Stop and reassess if you find yourself moving toward any of these:

- inventing a new runner ownership system
- teaching list-returning APIs to hold transactions open across caller code
- changing watcher behavior without a failing test
- broad refactors to “clean things up”

If that happens, you are leaving the agreed direction. Return to the contract decisions near the top of this document and reduce scope.
