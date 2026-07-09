# Remaining Review Findings Remediation Plan

**Date:** 2026-07-09

**Status:** Proposed; implementation has not started

**Scope:** The eight review findings that remain after two findings were withdrawn

**Primary implementation files:** 8

**New runtime abstractions:** 0

## Purpose

Fix the remaining review findings without changing SimpleBroker's message identity
contract or moving example execution into CI. The work has three independent parts:

1. Preserve accumulated coverage data in the GitHub Actions coverage job.
2. Make `examples/reference_reactor.py` enforce its fixed-topology and replay
   contracts, reject malformed control shapes safely, and bound replay result
   materialization.
3. Remove two timing assumptions from tests so they prove the intended behavior.

This is an implementation plan, not a request to redesign the reactor into a
framework. The reference reactor remains an example with a small, explicit policy.
The production `simplebroker/` API and storage backends do not change.

## Locked Decisions

These decisions are settled. Do not reopen them during implementation.

1. **Message ID is the deduplication identity.** On an exact-ID collision, do not
   compare message bodies. Payloads may be byte-identical by design, and body
   equality is not an identity rule.
2. **Example tests remain a local release gate.** Do not add `examples/tests` to a
   GitHub Actions test job. The release helper runs them locally to detect decay.
3. **Reactor topology is fixed for the lifetime of an instance.** Inputs, output,
   control input, and control output are roles, not aliases. Every configured role
   name must be distinct.
4. **A durable pending output is pinned to its recorded output queue.** If a restart
   uses a different configured output queue, fail before publication. Do not reroute
   the row to the new queue and do not open arbitrary queues from sidecar data.
5. **Pending output replay begins on the first driven reactor turn.** Construction
   may create durable schema and start idle workers, but it must not publish pending
   output through an overridable method before subclass initialization finishes.
6. **Plain, unquoted control text remains supported.** `PING` is a plain-text
   command. Valid JSON that is not an object, including `"PING"`, is an invalid
   control payload and receives an error reply. This asymmetry is intentional: the
   JSON form has one schema (an object), while the non-JSON fallback preserves terse
   legacy commands. Document it because quoted commands may surprise callers.
7. **Replay budgeting is a result-materialization bound.** A budget of `N` may return
   and materialize at most `N + 1` pending rows in Python. The extra row is only a
   backlog sentinel. This does not claim that SQLite scans only `N + 1` rows.
8. **Tests should exercise real SQLite files and real `Queue` objects.** Use small
   subclasses only to force lifecycle or failure branches. Do not mock the queue,
   sidecar session, SQL runner, `ThreadPoolExecutor`, or coverage command.

## NOT in Scope

- Comparing the body of an existing exact-ID message with the replay body. This
  conflicts with the project's ID-only identity contract.
- Running example tests in CI. Their intended gate is the local release workflow.
- Making `Reactor` dynamically add or remove lanes. `BaseReactor` deliberately
  seals inherited dynamic queue mutators.
- Automatically migrating pending rows to a newly configured output queue. That is
  a data migration and operator policy, not safe replay behavior.
- A new exception hierarchy for the example. A clear `RuntimeError` is sufficient
  for a stored-route/configuration mismatch.
- Worker deadlines, leases, dead-letter queues, compaction, or sidecar schema
  migration machinery. Those are production-framework concerns already called out
  as outside this example.
- Adding a `(status, input_ts)` sidecar index. The identified defect is unbounded
  result materialization. Query-scan optimization needs separate schema and migration
  analysis.
- Changing the coverage percentage policy. This plan repairs data combination; it
  does not add a new `fail-under` threshold or change `.codecov.yml`.
- Unrelated review ideas that were not in the final issue list. If new defects are
  found while implementing, record them separately unless they block a listed task.

## Repository Primer for the Implementer

### Tooling

- The project supports Python 3.11 through 3.14.
- `uv` owns dependency resolution and command execution. Do not use a second virtual
  environment or add a dependency for this work.
- `pytest` is the test runner. The root configuration uses xdist by default, so use
  `-n0` for focused tests whose purpose is thread or process timing.
- Ruff is both the linter and formatter.
- Mypy checks shipped code and the release helper. The root config excludes
  `examples/` from directory discovery, so pass touched example files explicitly.
- Coverage is configured in `pyproject.toml` with `parallel = true`. Worker and
  subprocess data can therefore appear as `<COVERAGE_FILE>.*` shards.
- PostgreSQL and Redis/Valkey parity is exercised through `bin/pytest-pg` and
  `bin/pytest-redis`. The reactor itself is a SQLite example, but the final branch
  gate still includes both extension suites.
- Do not edit `uv.lock`; no dependency change is needed.

### Code style

- Target Python 3.11 syntax. Keep full type annotations on new functions.
- Prefer small functions with explicit names and early returns.
- Use `Path`, argument-list subprocess calls, and `check=False` plus an explicit
  return code when a helper is meant to forward a child process result.
- Never use `shell=True` for the coverage helper.
- Keep SQLite transactions short. Never hold a sidecar transaction while publishing
  to an output queue.
- Keep dataclasses immutable with `frozen=True, slots=True`, matching `WorkItem`,
  `WorkerResult`, and `PendingOutput`.
- Do not introduce a helper or class used once unless it makes a contract directly
  testable. The one new script is justified because the coverage operation needs a
  stable, real subprocess test surface.
- Keep error text deterministic. Tests may assert the useful parts of a message, but
  should not freeze incidental punctuation.

### Domain model

SimpleBroker messages have a body and a timestamp-like message ID. In this example,
the ID is the only deduplication identity. A source message is observed with `peek`,
processed by a broker-free worker, recorded in a SQLite sidecar table, and published
to the output queue with an exact message ID.

The output state machine is:

```text
source row observed
        |
        v
reactor_seen = inflight
        |
        v
worker result received
        |
        v
reactor_results = output_pending
  stores: source queue, input ID, output queue, output ID, payload
        |
        | exact-ID queue insert
        v
reactor_results = output_written
```

The `output_pending` row is a durable routing fact. Its `output_queue` column must
not become dead metadata. A restart with different topology is an operator error:

```text
stored output_queue == configured output_queue
                  |
             yes  |  no
                  |
          publish | raise RuntimeError
                  | leave row pending
```

Input and control rows are checkpointed rather than consumed. This gives at-least-
once behavior across a crash. Control replies are also at-least-once because the
reply write and checkpoint are separate durable commits.

### Ownership and lifecycle

`BaseReactor.process_once()` is the public single-turn seam. It claims one drive
thread, drains durable backlog, applies completed worker results, processes queue
activity, and drains again. `Reactor.__init__()` configures queues, creates schema,
loads checkpoints, and starts idle worker threads.

After this work, pending publication must follow this sequence:

```text
construct Reactor
  -> validate all role names
  -> open fixed queue handles
  -> ensure sidecar schema
  -> load checkpoints
  -> start idle workers
  -> return to subclass constructor

first process_once() / start() turn
  -> drain pending output with a bounded query
  -> validate stored route against configured route
  -> publish or raise clearly
  -> continue ordinary dispatch only when backlog permits
```

Do not add a second lifecycle flag. `process_once()` already calls
`_drain_reactor_backlog()` before dispatch, so deferring replay uses an existing
deep interface rather than creating a parallel startup path.

## What Already Exists

| Need | Existing code to reuse | Decision |
|---|---|---|
| Single-turn startup seam | `BaseReactor.process_once()` calls `_drain_reactor_backlog()` | Reuse it; remove eager constructor replay |
| Durable route | `reactor_results.output_queue` already exists and fresh inserts populate it | Thread it through `PendingOutput`; do not change schema |
| Retry classification | `_try_publish_output()` catches only `OperationalError` | Keep it; route mismatch must propagate |
| Backpressure | `_output_backlog_blocked` plus `_drain_reactor_backlog()` | Preserve behavior |
| Exact-ID replay | `_publish_output()` handles `IntegrityError` by checking that the ID exists | Keep body-agnostic behavior unchanged |
| Control reply/checkpoint flow | `_process_control_message()` builds a response, writes it, then calls `_record_control_processed()` | Route invalid shapes through the same tail |
| Test helpers | `_make_reactor`, `_seed_pending_result`, `_sidecar_rows`, control send/wait helpers | Extend parameters; do not build a new harness |
| Coverage engine | Coverage.py already provides `coverage combine --append` | Wrap it only to make the no-shard case safe and testable |
| Local example gate | `bin/release.py` includes `EXAMPLE_TEST_COMMAND` and explicit example mypy paths | Keep this local-only gate |

## File Map

Beyond this plan document, the implementation should touch only these eight files:

| File | Why it changes |
|---|---|
| `.github/scripts/combine_coverage.py` | New small command that safely combines only when parallel shards exist |
| `.github/workflows/test.yml` | Invoke the helper instead of destructive plain combine; lint the helper |
| `tests/test_dev_scripts.py` | Real Coverage.py integration tests for the helper |
| `examples/reference_reactor.py` | Topology, lifecycle, control shape, stored route, and bounded replay fixes |
| `examples/tests/test_reference_reactor.py` | Behavior-focused reactor regressions and deterministic checkpoint test |
| `tests/test_process_broker_session.py` | Barrier for the three-worker ownership test |
| `README.md` | Public reactor summary and lifecycle/routing contract |
| `examples/MULTI_QUEUE_README.md` | Detailed example operating contract |

Do not touch `pyproject.toml`, `.codecov.yml`, sidecar schema versions, extension
packages, or `simplebroker/` runtime modules.

## TDD Rules for This Work

Follow red-green-refactor in vertical slices.

1. Add one failing behavior test for one defect.
2. Run only that test with `-n0` and record why it fails.
3. Make the smallest production change that turns it green.
4. Run the focused file.
5. Refactor only while all focused tests are green.
6. Move to the next defect.

Tests must assert externally meaningful state: queue contents, durable sidecar state,
raised errors, checkpoint progress, and return values. A narrow call to
`_pending_output_rows(limit=...)` is allowed because the result-materialization bound
is an internal performance invariant with no public equivalent. It proves the result
contract; code review proves `LIMIT` is in SQL. Do not mock the sidecar runner to
count calls.

Small subclasses are acceptable only when they create the real failure:

- a subclass whose publication method requires subclass initialization to finish;
- a subclass that raises the real `OperationalError` retry class.

Those are controlled seams, not mocks. All durable state and queue behavior must still
use a real temporary SQLite database.

## Dependency and Execution Plan

| Step | Modules touched | Depends on |
|---|---|---|
| Coverage combination | `.github/`, `tests/` | Baseline |
| Reactor lifecycle and routing | `examples/` | Baseline |
| Reactor topology and control | `examples/` | Baseline; sequence after lifecycle to reduce test churn |
| Reactor replay budget | `examples/` | Stored-route field has landed |
| Thread ownership test hardening | `tests/` | Baseline |
| Documentation | root docs, `examples/` | All reactor behavior is final |
| Full verification | whole repository | All prior steps |

The work has three technically independent lanes:

```text
Coverage: helper -> coverage workflow
Reactor: lifecycle -> stored route -> topology -> control -> replay budget -> docs
Thread test: ownership barrier
```

Implement them sequentially in the order above, then run the thread-test slice and
documentation. Three worktrees would save little time and add merge risk to a small
change whose reactor lane is already sequential. If a team deliberately parallelizes,
the coverage and thread-test lanes do not share files; one engineer must still own all
reactor code and reactor tests. Documentation waits until reactor behavior and names
are final.

## Task 0: Establish the Baseline

**Read before editing:**

- `examples/reference_reactor.py`: module contract, `PendingOutput`,
  `BaseReactor.process_once`, `Reactor.__init__`, `_record_pending_result`,
  `_process_control_message`, `_publish_output`, `_pending_output_rows`, and
  `_drain_pending_outputs`.
- `examples/tests/test_reference_reactor.py`: all helpers and every existing replay,
  control, backlog, crash, and restart test.
- `.github/workflows/test.yml`: lint job and coverage job.
- `pyproject.toml`: pytest, Ruff, mypy, and coverage configuration. Read only.
- `.codecov.yml`: existing policy. Read only.
- `bin/release.py`: `EXAMPLE_TEST_COMMAND`, `_examples_mypy_command`, and prechecks.
  Read only.
- `README.md` reactor section and `examples/MULTI_QUEUE_README.md` reactor contract.
- `tests/test_process_broker_session.py` complete thread-owner test, including its
  cleanup fixture behavior.

Run:

```bash
git status --short
uv sync --extra dev
uv run pytest -n0 tests/test_dev_scripts.py
uv run pytest -n0 examples/tests/test_reference_reactor.py
uv run pytest -n0 tests/test_process_broker_session.py -k thread_owners
```

Expected baseline: all focused tests pass before adding regressions. If they do not,
stop and distinguish a pre-existing local failure from a plan error. Do not weaken a
test to make the baseline green.

Keep the worktree's existing user changes intact. Commit by vertical slice if the
repository's current workflow permits it; never mix a behavior change with unrelated
formatting.

## Task 1: Preserve Base Coverage When Combining Parallel Shards

**Finding:** Without `--append`, plain `coverage combine` starts a new aggregate. When
a base `.coverage` file coexists with parallel shards, it can discard the base data
accumulated by `--cov-append` and replace it with sparse or empty shard data. The exact
artifact layout depends on pytest-cov, xdist, and Coverage.py interaction, but this
repository's workflow sequence reproduced the loss. Independently, plain combine
exits with an error when no shard files exist. The helper fixes both cases.

**Files:**

- Create `.github/scripts/combine_coverage.py`.
- Modify `tests/test_dev_scripts.py`.
- Modify `.github/workflows/test.yml`.

### Task 1.1: Add red integration tests

Add a test-local helper in `tests/test_dev_scripts.py` that invokes the new script as
a subprocess. It should:

- use `sys.executable`, not a bare `python`;
- set `COVERAGE_FILE` to an absolute path below `tmp_path`;
- pass an argument list with `shell=False` (the subprocess default);
- capture stdout and stderr for useful assertion failures; and
- use the repository-root path to `.github/scripts/combine_coverage.py`.

Use `coverage.CoverageData` to create real SQLite coverage data files. Do not fake the
CLI or patch `subprocess.run`.

Add these tests:

1. `test_combine_coverage_keeps_base_data_when_no_shards_exist`
   - Create `.coverage` with one measured file and one measured line.
   - Create no `.coverage.*` files.
   - Run the helper.
   - Assert exit code zero.
   - Re-read `.coverage` and assert the base file and line remain.
2. `test_combine_coverage_appends_parallel_shards_to_base_data`
   - Create `.coverage` containing `base_source.py:1`.
   - Create `.coverage.worker` containing `worker_source.py:2`.
   - Run the helper.
   - Assert exit code zero.
   - Re-read `.coverage` and assert both measured files and lines exist.
   - Assert the successfully combined shard was removed, matching Coverage.py's
     default combine cleanup.
3. `test_combine_coverage_creates_base_from_shard_only_data`
   - Create no base `.coverage` file.
   - Create one valid `.coverage.worker` shard with a measured file and line.
   - Run the helper.
   - Assert exit code zero, the base file now exists, its data contains the shard's
     file and line, and the shard was removed.
4. `test_combine_coverage_propagates_corrupt_shard_failure`
   - Create valid base data.
   - Create `.coverage.bad` containing invalid non-database bytes.
   - Run the helper.
   - Assert a nonzero exit code and a useful Coverage.py diagnostic.
   - Assert the valid base data still exists.

Run the four tests before creating the script:

```bash
uv run pytest -n0 tests/test_dev_scripts.py -k combine_coverage -vv
```

Expected red: the subprocess cannot open `.github/scripts/combine_coverage.py`.

### Task 1.2: Implement the smallest helper

The helper is a repository command, not a package API. Give it a short module docstring
and two typed functions at most:

```text
main()
  -> resolve COVERAGE_FILE, defaulting to .coverage
  -> find regular files named <base-name>.* in the base file's parent
  -> if none exist: print a short no-op message and return 0
  -> otherwise run:
       sys.executable -m coverage combine --append
  -> return the child process return code unchanged
```

Requirements:

- Resolve relative `COVERAGE_FILE` from the current working directory.
- Sort shard paths before deciding whether the set is empty, so logs and tests are
  deterministic.
- Do not pass shard paths to the combine command. Coverage.py already discovers the
  `<COVERAGE_FILE>.*` files using its configured data file. The pre-scan exists only
  to make the zero-shard case a no-op.
- Use `--append`. This is the key invariant that retains existing base data.
- Do not catch or rewrite Coverage.py failures. Return the child code.
- Do not inspect coverage percentage or add a threshold.
- End with `raise SystemExit(main())`.

Run:

```bash
uv run pytest -n0 tests/test_dev_scripts.py -k combine_coverage -vv
uv run ruff check .github/scripts/combine_coverage.py tests/test_dev_scripts.py
uv run ruff format --check .github/scripts/combine_coverage.py tests/test_dev_scripts.py
```

Expected green: all four real-data cases pass.

### Task 1.3: Wire the helper into CI

In `.github/workflows/test.yml`:

1. Replace only `uv run coverage combine` in the coverage job with:

   ```bash
   uv run python .github/scripts/combine_coverage.py
   ```

2. Add `.github/scripts` to the existing Ruff check and Ruff format path lists. Do
   not add example paths to CI and do not alter test selection.
3. Leave `coverage report` and `coverage xml` after the helper.
4. Leave the three existing coverage-producing pytest commands and their
   `--cov-append` arguments unchanged.
5. Do not use `|| true` anywhere in this path.

Manual workflow invariant:

```text
base only                  -> helper no-op -> base retained
base + one or more shards  -> combine --append -> union retained
one or more shards only    -> combine --append -> base aggregate created
corrupt shard              -> nonzero exit -> workflow fails
```

Gate:

```bash
uv run pytest -n0 tests/test_dev_scripts.py
uv run ruff check .github/scripts tests/test_dev_scripts.py
uv run ruff format --check .github/scripts tests/test_dev_scripts.py
```

## Task 2: Defer Pending Replay Until the First Driven Turn

**Finding:** `Reactor.__init__()` calls `_replay_pending_outputs()`. That method and the
methods below it are overridable, so a subclass can receive a publication callback
before its own constructor has finished.

**Files:**

- Modify `examples/reference_reactor.py`.
- Modify `examples/tests/test_reference_reactor.py`.

### Task 2.1: Add a red lifecycle test

Replace the construction-timing assumption in
`test_pending_output_rows_replay_on_reactor_construction` with a behavior test named
`test_pending_output_replay_waits_for_first_driven_turn`.

Test setup:

1. Bootstrap the schema and stop the bootstrap reactor.
2. Seed one real `output_pending` row.
3. Define a small `RequiresInitializedPublishReactor` subclass:
   - set `self.subclass_ready = False` before `super().__init__()`;
   - wrap `super().__init__()` in `try/except BaseException`; on the intentional red
     failure, call `self.stop()` and re-raise so opened persistent handles are closed;
   - set it to `True` after `super().__init__()` returns;
   - override `_publish_output()` only to assert `self.subclass_ready`, then call
     `super()._publish_output(...)`.
4. Construct the subclass.
5. Assert construction succeeds and the output queue is still empty.
6. Call `reactor.process_once()` from the test thread.
7. Assert the output is present exactly once and the sidecar status is
   `output_written`.
8. Stop the reactor in `finally`.

The cleanup wrapper is required even in the red phase. The current eager replay fails
after durable handles have opened but before the test can assign the constructed
object to a normal `try/finally` variable.

This uses a subclass seam to expose the lifecycle violation while keeping real queue
and sidecar behavior.

Run before changing production code:

```bash
uv run pytest -n0 examples/tests/test_reference_reactor.py \
  -k pending_output_replay_waits_for_first_driven_turn -vv
```

Expected red: construction reaches `_publish_output()` while
`subclass_ready is False`.

### Task 2.2: Remove eager replay, not replay itself

In `Reactor.__init__()`:

- remove the call to `_replay_pending_outputs()`;
- keep schema creation, checkpoint loading, and worker startup;
- do not add a replacement startup flag or direct call to `_drain_pending_outputs()`.

`BaseReactor.process_once()` already calls `_drain_reactor_backlog()` before normal
queue dispatch. That path calls `_drain_pending_outputs()`, updates
`_output_backlog_blocked`, and preserves output backpressure. This is the only startup
replay path after the change.

Delete `_replay_pending_outputs()` if no call sites remain. Do not retain dead wrapper
code for possible future use.

Update existing tests that construct a reactor and immediately inspect replayed
output. Call one real `process_once()` first. This includes exact-ID replay and crash
recovery tests. Their body-agnostic exact-ID assertions must remain unchanged.

Gate:

```bash
uv run pytest -n0 examples/tests/test_reference_reactor.py \
  -k "pending_output or exact_id or crash_after_result" -vv
```

## Task 3: Enforce the Durable Output Route

**Finding:** The sidecar stores `reactor_results.output_queue`, but pending-row reads
drop it. A restart configured with a different output queue silently publishes the
old row to the new route.

This is a narrow operator-error case in an example, so its main payoff is didactic.
It remains in scope because a durable-reference example should not teach that stored
routing metadata can be ignored, and the complete fix is one field plus one guard.
Do not expand it into dynamic routing or migration support.

**Files:**

- Modify `examples/reference_reactor.py`.
- Modify `examples/tests/test_reference_reactor.py`.

### Task 3.1: Add a red route-drift recovery test

Extend existing test helpers instead of making new ones:

- `_make_reactor(..., output_queue: str = OUTBOX)` should allow a configured route.
- `_seed_pending_result(..., output_queue: str = OUTBOX)` should store the requested
  route.

Add `test_pending_output_rejects_configured_route_drift`:

1. Bootstrap schema and stop the bootstrap reactor.
2. Seed a pending row whose stored route is `OUTBOX`.
3. Construct a reactor configured with a distinct `NEW_OUTBOX` name.
4. Put all work with that reactor in `try/finally` so it is stopped even when a red
   assertion fails.
5. Call `process_once()` and assert `RuntimeError` with both `OUTBOX` and
   `NEW_OUTBOX` in the message. Use substring or regex matching, not full-message
   equality.
6. Assert the old and new output queues are both empty.
7. Assert the sidecar row remains `output_pending`.
8. Stop that reactor in `finally`.
9. Construct a new reactor configured with the original `OUTBOX`.
10. Put it in its own `try/finally`, call `process_once()`, and stop it in `finally`.
11. Assert exactly one output appears in `OUTBOX` and the row becomes
    `output_written`.

The recovery half is required. It proves fail-fast does not destroy or strand valid
durable work.

Run before implementation:

```bash
uv run pytest -n0 examples/tests/test_reference_reactor.py -k route_drift -vv
```

Expected red: the row is written to `NEW_OUTBOX` instead of raising.

### Task 3.2: Thread route data through the existing model

In `PendingOutput`, add `output_queue: str`. Keep the class frozen and slotted.

Populate that field in every construction path:

- fresh worker result: `self.output_queue_name`;
- existing `reactor_results` row in `_record_pending_result()`: select and use the
  stored `output_queue` value;
- `_pending_output_rows()`: select and use the stored `output_queue` value.

At the top of `_publish_output()` and before any queue insert, compare
`pending.output_queue` with `self.output_queue_name`. On mismatch, raise
`RuntimeError` whose text names both routes and states that the pending row was not
published.

When `start()` drives the reactor in a background thread, this non-operational error
propagates out of `run_until_stopped()`, kills that drive thread, and its `finally`
block calls `stop(join=False)` to close resources. This is loud failure by design.
`process_once()` tests catch the same error directly; the docs must state the
background-thread behavior so operators do not expect an automatic retry.

Do not:

- open `Queue(pending.output_queue)` dynamically;
- update the sidecar route to the configured route;
- mark the row written;
- catch this error in `_try_publish_output()`; or
- add payload comparison to the `IntegrityError` path.

`_try_publish_output()` must continue catching only `OperationalError`, so a topology
error is visible instead of becoming an endless backpressure retry.

Gate:

```bash
uv run pytest -n0 examples/tests/test_reference_reactor.py \
  -k "route_drift or pending_output or exact_id" -vv
```

## Task 4: Reject Every Queue-Role Collision

**Finding:** The constructor rejects only `control_in_queue` overlapping an input.
Other overlaps can self-feed outputs into inputs, mix commands with replies, or cause
the parent queue map to collapse duplicate inputs.

**Files:**

- Modify `examples/reference_reactor.py`.
- Modify `examples/tests/test_reference_reactor.py`.

### Task 4.1: Add a complete collision matrix

Add one parameterized test named `test_reactor_rejects_overlapping_queue_roles`. Each
case should pass complete constructor arguments and identify the duplicated name.

Cover all conflict families:

1. duplicate entries in `input_queues`;
2. output equals an input;
3. control input equals an input;
4. control output equals an input;
5. output equals control input;
6. output equals control output;
7. control input equals control output.

For each case, assert `ValueError`, `distinct` in the message, and the duplicated queue
name in the message. Use `tmp_path` and the real constructor. The validation must run
before any database file or worker thread is created; assert the expected database
path does not exist after rejection.

Guard the intentional red phase against leaks:

```text
reactor = None
try:
    with pytest.raises(ValueError):
        reactor = Reactor(...)
finally:
    if reactor is not None:
        reactor.stop()
```

When validation is missing, `pytest.raises` fails after assigning the reactor, and
the `finally` block still closes its workers and persistent queue handles.

Keep the separate empty-input test if one exists. If it does not, add it next to this
matrix because emptiness is a different invariant from collision.

Run before implementation:

```bash
uv run pytest -n0 examples/tests/test_reference_reactor.py \
  -k "overlapping_queue_roles or input_queues_cannot_be_empty" -vv
```

Expected red: only the already-covered control-input/input case raises.

### Task 4.2: Validate once, before side effects

At the start of `Reactor.__init__()`:

1. Materialize `input_queues` once as the existing tuple.
2. Reject an empty tuple with the existing error.
3. Build one list containing every input name plus output, control input, and control
   output.
4. Use `collections.Counter` to find names whose count is greater than one.
5. If any exist, raise one deterministic `ValueError` with sorted duplicate names.

Replace the narrower `control_in_queue` check. Do not stack seven pairwise `if`
statements and do not add a general topology class.

Invariant after constructor validation:

```text
len(set([*inputs, output, control_in, control_out]))
    == len(inputs) + 3
```

Gate:

```bash
uv run pytest -n0 examples/tests/test_reference_reactor.py -k queue_roles -vv
```

## Task 5: Handle Valid Non-Object JSON on the Control Lane

**Finding:** `json.loads()` may return a list, scalar, string, or `None`. The current
code immediately calls `.get()`, so a valid non-object JSON body raises on the reactor
thread and can poison the lane because its checkpoint does not advance.

**Files:**

- Modify `examples/reference_reactor.py`.
- Modify `examples/tests/test_reference_reactor.py`.

### Task 5.1: Add real control-lane shape tests

Write raw bodies with a short-lived real `Queue(CONTROL_IN, ...)`. Do not use
`_write_json` for this test because it would hide which JSON text reached the lane.

Add a parameterized test for these bodies and decoded types:

| Body | Decoded type |
|---|---|
| `[]` | list |
| `null` | null / `None` |
| `1` | integer |
| `true` | boolean |
| `"PING"` | string |

For each case:

1. Start a real reactor thread.
2. Open a short-lived `Queue(CONTROL_IN, ...)` as a context manager, call
   `generate_timestamp()`, then write the raw body with
   `insert_messages([(body, timestamp)])`. `Queue.write()` returns `None`, so it
   cannot supply the timestamp this test needs.
3. Wait for one new reply using a deterministic poll with a two-second deadline.
   Match the reply by its `input_timestamp`; invalid-shape replies deliberately have
   no request ID.
4. Assert the reply has `ok is False` and an error that says a control payload must
   be a JSON object or plain-text command. Assert a stable substring rather than the
   entire error string.
5. Assert the reply's `input_timestamp` matches the input timestamp.
6. Send a normal object `PING` with a request ID as a progress barrier.
7. Await `PONG` and assert success.
8. Assert the checkpoint is at least the malformed row timestamp.
9. Stop externally in `finally`.

Also retain or add focused cases proving:

- unquoted `PING` still returns `PONG`;
- `{}` returns the existing unknown-empty-command error;
- an object with an unknown command returns its existing error.

Run before implementation:

```bash
uv run pytest -n0 examples/tests/test_reference_reactor.py \
  -k "non_object_control or plain_text_control or unknown_control" -vv
```

Expected red: the reactor thread raises `AttributeError` for the first non-object case
and no error reply is written.

### Task 5.2: Route invalid shapes through the common durable tail

Refactor only the parsing and response construction in `_process_control_message()`:

1. Try `json.loads(body)`.
2. On `json.JSONDecodeError`, preserve the current fallback:
   `payload = {"command": body}`.
3. Initialize `stop_after_response = False` before branching, so the malformed-shape
   path reaches the common tail with a defined value.
4. If decoding succeeds but `payload` is not a `Mapping`, build an error response with:
   - `request_id: None`;
   - `command: "<invalid>"`;
   - the input timestamp;
   - `ok: False`; and
   - the stable shape error.
5. Otherwise use the existing object command handling.
6. In both branches, use the existing single response-write,
   `_control_messages_handled`, `_record_control_processed`, and stop-request tail.

Do not return before the common tail. Do not add one checkpoint path for malformed
payloads and another for normal commands. That duplication would make crash behavior
and audit state diverge.

The lane invariant is:

```text
every observed control row
  -> exactly one reply attempt
  -> checkpoint and audit after successful reply write
  -> later valid control can make progress
```

Gate:

```bash
uv run pytest -n0 examples/tests/test_reference_reactor.py -k control -vv
```

## Task 6: Apply the Replay Result Budget in SQL

**Finding:** `_pending_output_rows()` materializes every pending row, then
`_drain_pending_outputs()` stops iterating at `max_outputs`. The configured fairness
budget therefore does not bound rows transferred from SQLite or materialized in
Python.

**Files:**

- Modify `examples/reference_reactor.py`.
- Modify `examples/tests/test_reference_reactor.py`.

### Task 6.1: Add bounded-drain behavior tests

Add `test_pending_output_drain_budget_fetches_one_backlog_sentinel` using a real
sidecar and output queue:

1. Bootstrap a reactor.
2. Seed three pending rows with ordered input IDs and distinct output IDs.
3. Call `_pending_output_rows(limit=2)` and assert exactly the first two rows are
   returned in existing order. This is the one sanctioned internal seam test.
4. Call `_drain_pending_outputs(max_outputs=1)`.
5. Assert it returns `False`, exactly one output was published, and two rows remain
   pending.
6. Call it again with budget one. Assert `False`, two outputs total, one pending.
7. Call it a third time. Assert `True`, three outputs total, no pending rows.
8. Assert every row is `output_written` and every output ID appears exactly once.

Wrap the constructed reactor in `try/finally` before the first assertion. Intentional
red failures must not leave its persistent queues or idle worker threads alive.

Add a small zero-budget assertion in the same test or a separate focused test:

- with pending work, `max_outputs=0` publishes nothing and returns `False`;
- with no pending work, `max_outputs=0` returns `True`.

This prevents an off-by-one implementation from publishing the sentinel row. The
`_pending_output_rows(limit=2)` assertion proves the internal interface returns a
bounded result. It cannot by itself distinguish SQL `LIMIT` from an unbounded query
followed by Python slicing.

Run before implementation:

```bash
uv run pytest -n0 examples/tests/test_reference_reactor.py -k drain_budget -vv
```

Expected red: `_pending_output_rows()` does not accept `limit`, and the existing
method has no query-level bound.

### Task 6.2: Use `LIMIT max_outputs + 1`

Change `_pending_output_rows()` to require a keyword-only `limit: int`. Add `LIMIT ?`
to the existing ordered SQL query and bind the limit as a parameter. Do not interpolate
it into SQL.

Change `_drain_pending_outputs()` to:

1. fetch `max_outputs + 1` rows;
2. remember whether the extra sentinel row exists;
3. publish only the first `max_outputs` rows;
4. return `False` immediately on the existing transient publication failure; and
5. otherwise return `False` when a sentinel proves more backlog remains, or `True`
   when the fetched set was fully drained.

Do not run a separate `COUNT(*)` query. The sentinel makes the complete decision with
one query whose returned result is bounded. Do not slice after an unbounded query.

**Required code-review invariant:** inspect the final implementation and confirm that
`LIMIT ?` is inside the `SELECT`, that `limit` is a bound parameter, and that the full
cursor result is never sliced afterward. Do not add a mocked sidecar or a brittle
full-SQL-string assertion merely to observe this private implementation detail.

The existing table has no `(status, input_ts)` index. `LIMIT` bounds rows returned and
materialized, but SQLite may still scan or sort more of the backlog. Adding an index
would require a schema and migration decision outside this finding. Do not claim that
this task makes the underlying scan O(`max_outputs`).

Preserve the existing ordering and the control-lane budget choice (`1` when control
is ready, otherwise `100`).

Gate:

```bash
uv run pytest -n0 examples/tests/test_reference_reactor.py \
  -k "drain_budget or output_backlog" -vv
```

## Task 7: Make the Restart Checkpoint Test Prove the Right Checkpoint

**Finding:** The current restart test sends `PING`, then sends `STOP`. The later STOP
checkpoint can hide a missing PING checkpoint. After restart it also uses a fixed
sleep as a negative assertion.

**File:** Modify `examples/tests/test_reference_reactor.py` only.

This is a false-positive test repair, not a production behavior change, so there is no
ordinary red-green production slice. Use a temporary mutation to prove sensitivity:

1. Before rewriting the test, temporarily add this branch at the top of
   `_record_control_processed()`, before `now = time.time_ns()`:

   ```python
   if command == "PING":
       self._checkpoints[self.control_in_queue] = max(
           self._checkpoints.get(self.control_in_queue, 0),
           timestamp,
       )
       return
   ```

   This advances only live memory and skips the durable sidecar checkpoint. Leave
   STOP unchanged.
2. Run the old test and confirm it still passes because STOP writes a later durable
   checkpoint that masks the missing PING checkpoint.
3. Rewrite the test as specified below and rerun it with the same temporary mutation.
   It must fail by observing more than one `ping-once` reply after restart.
4. Revert the temporary production mutation, run the rewritten test, and confirm it
   passes.
5. Use `git diff` to confirm the mutation is gone. Never commit it.

This is mutation testing of the test's sensitivity. It is not a mock and it does not
change the shipped design.

Rewrite `test_checkpointed_control_is_not_reprocessed_after_restart`:

1. Start the first reactor.
2. Send `PING` with request ID `ping-once` and await its reply.
3. Stop the reactor by calling `reactor.stop()` from the test, then join its thread.
   Do not send a STOP control message.
4. Start the restarted reactor.
5. Send a new object `PING` with request ID `restart-barrier` and await its reply.
   This proves the restarted reactor advanced through all earlier control rows.
6. Read control replies and assert exactly one reply exists for `ping-once` and one
   for `restart-barrier`.
7. Stop externally in `finally`.

Remove `time.sleep(0.1)` from this test. Polling helpers with deadlines are allowed;
fixed sleeps used to prove absence are not.

Gate:

```bash
uv run pytest -n0 examples/tests/test_reference_reactor.py \
  -k checkpointed_control_is_not_reprocessed_after_restart -vv
```

## Task 8: Make the Three-Thread Ownership Assertion Deterministic

**Finding:** `ThreadPoolExecutor(max_workers=3)` may reuse one worker when submitted
functions do not overlap. The test later requires three distinct thread IDs, but it
does not force all three tasks to be live together.

**File:** Modify `tests/test_process_broker_session.py` only.

Inside
`test_persistent_sqlite_thread_owners_do_not_reapply_connection_pragmas`:

1. Create `start_barrier = threading.Barrier(3)` before the worker function.
2. Make `start_barrier.wait(timeout=5.0)` the first statement in `worker()`, before
   constructing its persistent `Queue`.
3. Keep `ThreadPoolExecutor(max_workers=3)` and all existing assertions.
4. Do not catch `BrokenBarrierError`; a broken barrier is a useful test failure.
5. Do not replace the exact three-thread assertion with `>= 1`. The behavior under
   test is one setup per actual thread, and the barrier makes three threads real.

Why the barrier is placed first:

```text
task 1 blocks at barrier -> executor must start worker 2
task 2 blocks at barrier -> executor must start worker 3
task 3 arrives           -> all three proceed with Queue construction
```

Focused gate:

```bash
uv run pytest -n0 tests/test_process_broker_session.py \
  -k persistent_sqlite_thread_owners_do_not_reapply_connection_pragmas -vv
```

Stress gate, run locally after the focused gate:

```bash
run=1
while [ "$run" -le 20 ]
do
  uv run pytest -n0 -q tests/test_process_broker_session.py \
    -k persistent_sqlite_thread_owners_do_not_reapply_connection_pragmas || exit 1
  run=$((run + 1))
done
```

Do not add `pytest-repeat` for one stress check.

## Task 9: Update the Reactor Contract Documentation

**Files:**

- Modify the module docstring in `examples/reference_reactor.py`.
- Modify the reactor section in `README.md`.
- Modify the reactor contract in `examples/MULTI_QUEUE_README.md`.

Update all three locations in the same slice, after behavior is green. State these
contracts consistently:

1. All input, output, control-input, and control-output queue names are pairwise
   distinct. Construction rejects overlaps before opening resources.
2. Construction creates durable schema, loads checkpoints, and starts idle workers.
   It does not publish pending output.
3. The first `process_once()` or background drive turn replays pending output before
   dispatching new input.
4. Each pending row is pinned to its recorded `output_queue`. A configured route
   mismatch raises and leaves the row pending; recovery requires restoring the old
   topology or performing an explicit external migration. In background mode, the
   error terminates the drive thread and the reactor's `finally` path closes owned
   resources; it is not treated as a retryable output outage.
5. Exact-ID replay remains body-agnostic. Downstream deduplication uses message ID,
   not payload equality.
6. Plain-text commands remain supported; JSON control payloads must otherwise be
   objects.
7. Replay budgets bound both publication count and rows returned/materialized, using
   one extra row only to detect remaining backlog. They do not promise a bounded
   SQLite scan without a supporting index.

Do not claim the constructor is side-effect-free. It still opens queues, creates
schema, loads state, and starts workers. The precise statement is that it no longer
publishes pending output.

No large inline ASCII comment is needed in production code. The state transitions are
already explained by names and the example docs. Adding a large diagram beside the
short methods would hurt locality.

Documentation checks:

```bash
rg -n "construction|Constructing|pending-output replay|output_queue|distinct" \
  README.md examples/MULTI_QUEUE_README.md examples/reference_reactor.py
```

Manually confirm there is no remaining sentence that says construction replays
pending output.

## Task 10: Run Focused and Full Verification

Run gates in this order. Stop at the first failure and fix the implementation or test;
do not rerun blindly.

### 10.1 Focused reactor gate

```bash
uv run pytest -n0 examples/tests/test_reference_reactor.py -vv
uv run ruff check examples/reference_reactor.py examples/tests/test_reference_reactor.py
uv run ruff format --check examples/reference_reactor.py examples/tests/test_reference_reactor.py
uv run mypy examples/reference_reactor.py examples/tests/test_reference_reactor.py \
  --config-file pyproject.toml
```

### 10.2 Focused CI-helper and thread gate

```bash
uv run pytest -n0 tests/test_dev_scripts.py tests/test_process_broker_session.py
uv run ruff check .github/scripts tests/test_dev_scripts.py \
  tests/test_process_broker_session.py
uv run ruff format --check .github/scripts tests/test_dev_scripts.py \
  tests/test_process_broker_session.py
```

### 10.3 Core and extension gates

```bash
uv run pytest -m "not benchmark"
uv run ./bin/pytest-pg --fast
uv run ./bin/pytest-redis --fast
```

The PostgreSQL and Redis services must be available for the last two commands. If the
environment cannot provide them, report the exact skipped gates. Do not imply they
passed.

### 10.4 Local-only example decay gate

This is deliberately not a CI workflow step:

```bash
uv run pytest -n0 examples
```

### 10.5 Repository quality gates

Use the same path sets as the release helper, plus the new GitHub script:

```bash
uv run ruff check simplebroker tests bin examples .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run ruff format --check simplebroker tests bin examples .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run mypy simplebroker bin/release.py \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
```

Then run the release helper's explicit example mypy coverage, or equivalently pass
every Python file below `examples/` explicitly. `bin/release.py --dry-run` does not
execute prechecks, so use this non-mutating command instead:

```bash
find examples -type f -name '*.py' ! -path '*/__pycache__/*' -print0 \
  | xargs -0 uv run mypy --config-file pyproject.toml
find extensions/simplebroker_pg/tests -type f -name '*.py' \
  ! -path '*/__pycache__/*' -print0 \
  | xargs -0 uv run mypy --config-file pyproject.toml
find extensions/simplebroker_redis/tests -type f -name '*.py' \
  ! -path '*/__pycache__/*' -print0 \
  | xargs -0 uv run mypy --config-file pyproject.toml
uv run mypy .github/scripts/combine_coverage.py --config-file pyproject.toml
```

Do not run a version-bumping release command just to type-check examples.

### 10.6 Diff and scope gate

```bash
git diff --check
git status --short
git diff --stat
```

Confirm:

- apart from this plan document, only the eight files in the File Map changed;
- no lock file or package version changed;
- examples were not added to GitHub Actions;
- exact-ID collision code still does not compare payloads;
- no new runtime class or generalized topology layer was added; and
- all temporary databases and coverage data used by tests live below `tmp_path`.

## Acceptance Invariants

The branch is complete only when every invariant below is true.

### Coverage

- Base coverage data survives when no parallel shards exist.
- Base and shard data are unioned when shards exist.
- Shard-only data produces a valid base aggregate.
- A corrupt shard fails the helper and therefore the workflow.
- The workflow invokes `combine --append` through the tested helper.
- The plan does not change coverage thresholds or test selection.

### Reactor topology and lifecycle

- Queue role names are pairwise distinct before any resource opens.
- Construction never publishes pending output through an overridable path.
- The first driven turn replays pending rows before ordinary input dispatch.
- A stored-route mismatch writes to neither route, raises clearly, and leaves the row
  pending.
- Restoring the recorded route permits normal replay and `output_written` transition.
- Exact-ID replay remains ID-only and does not duplicate an extant ID.

### Control lane

- Every JSON non-object shape receives a clear error reply.
- The malformed row is checkpointed after the reply write.
- A later valid command proves the lane is not poisoned.
- Plain unquoted commands and object commands keep existing behavior.

### Replay budget

- A budget of `N` returns and materializes no more than `N + 1` rows.
- No more than `N` rows are published.
- The extra row only determines whether backlog remains.
- Transient output failure still returns `False` and preserves pending state.
- SQLite may scan or sort more than `N + 1` rows because the current schema has no
  supporting `(status, input_ts)` index; changing that schema is outside this plan.

### Tests

- Restart checkpoint coverage is not masked by a later STOP checkpoint.
- Negative restart behavior uses a later control message as a barrier, not a sleep.
- The thread-owner test forces three simultaneous executor workers before asserting
  three thread IDs.
- Behavior tests use real SQLite and real queue handles.

## Failure-Mode Matrix

| Code path | Realistic failure | Test coverage | Handling | User-visible result |
|---|---|---|---|---|
| Coverage helper, no shards | A platform produces only base data | Base-only integration test | No-op, exit 0 | Clear one-line log; data retained |
| Coverage helper, valid shards | Append flag is omitted and base is erased | Base-plus-shard union test | `--append` required | CI test fails before reporting wrong data |
| Coverage helper, shard only | A platform emits parallel files without a base file | Shard-only integration test | Combine creates the base aggregate | Report and XML steps receive valid data |
| Coverage helper, corrupt shard | Partial or corrupt coverage database | Corrupt-shard integration test | Child return code propagates | Coverage job fails with Coverage.py diagnostic |
| Constructor topology validation | Output aliases input and self-feeds | Seven-family parameter matrix | `ValueError` before resources | Clear duplicate-name error |
| First-turn replay | Subclass is not initialized during publish | Lifecycle subclass test | Replay deferred to `process_once()` | Construction completes; replay occurs when driven |
| Stored route | Deployment changes output queue with pending rows | Route-drift and recovery test | `RuntimeError`, row unchanged | Direct drive raises; background drive thread exits and closes resources |
| Exact-ID replay | Output ID already exists | Existing exact-ID test | Existing ID accepted | One logical output; no body comparison |
| Control JSON shape | `json.loads()` returns list/scalar/null | Parameterized raw-body tests | Error reply then checkpoint | Caller sees stable shape error; lane advances |
| Control reply write | Broker write raises | Existing reactor retry/ownership behavior | Checkpoint is not advanced first | Error propagates or retry policy acts; no silent ack |
| Replay budget | Millions of pending rows | Limit seam plus three-row drain test | SQL `LIMIT N+1` bounds returned/materialized rows | Python memory is bounded; SQLite scan cost may remain backlog-sized |
| Replay publication | Output database is transiently locked | Existing `OperationalError` backlog tests | Return `False`, backpressure inputs | STATUS/STOP remain live |
| Restart checkpoint | Old PING was not checkpointed | New later-PING barrier test | Reprocessing becomes observable | Duplicate old reply fails test |
| Thread executor | Executor reuses an idle worker | Barrier stress test | Three tasks overlap | Exact thread assertion is deterministic |

There are no accepted silent failures in the new paths. The route mismatch is loud by
design. Coverage corruption is loud. Malformed control input produces a durable reply
and checkpoint.

## Common Wrong Turns

- **Do not compare payloads on exact-ID collision.** That revives a withdrawn finding
  and contradicts the identity contract.
- **Do not add example tests to CI.** Run them locally through the release path.
- **Do not reroute old pending rows to the new configured output.** That silently
  changes durable intent.
- **Do not dynamically open the stored queue name.** The reactor is fixed topology,
  and sidecar data must not expand its resource set.
- **Do not catch `RuntimeError` as backlog.** A configuration mismatch needs operator
  action, not an infinite retry loop.
- **Do not add a `startup_replayed` boolean.** The existing backlog drain is already
  the correct lifecycle seam.
- **Do not fix query cost with Python slicing.** The `LIMIT` belongs in SQL.
- **Do not run `COUNT(*)` before every bounded replay.** The `N + 1` sentinel answers
  whether more work exists.
- **Do not mock Coverage.py, Queue, SQLite, or the executor.** Those mocks would prove
  the test's setup, not the failing behavior.
- **Do not use fixed sleeps to prove absence.** Use a later observable event as a
  progress barrier.
- **Do not add a dependency for stress repetition or YAML parsing.** Existing tools
  are enough.

## Implementation Tasks

Synthesized from this review. Check each item only after its focused gate is green.

- [ ] **T1 (P1, human: ~2h / Codex: ~20m)** - CI coverage - Preserve base data with a tested conditional `combine --append` helper.
  - Surfaced by: correctness and CI review; `.github/workflows/test.yml` plain combine.
  - Files: `.github/scripts/combine_coverage.py`, `tests/test_dev_scripts.py`, `.github/workflows/test.yml`.
  - Verify: `uv run pytest -n0 tests/test_dev_scripts.py -k combine_coverage -vv`.
- [ ] **T2 (P2, human: ~1h / Codex: ~10m)** - Reactor lifecycle - Move pending replay from construction to the first driven turn.
  - Surfaced by: architecture review; overridable replay from `Reactor.__init__`.
  - Files: `examples/reference_reactor.py`, `examples/tests/test_reference_reactor.py`.
  - Verify: focused lifecycle, pending-output, exact-ID, and crash-replay tests.
- [ ] **T3 (P1, human: ~2h / Codex: ~20m)** - Reactor routing - Enforce the durable stored output route and prove recovery.
  - Surfaced by: correctness review; stored `output_queue` was ignored on replay.
  - Files: `examples/reference_reactor.py`, `examples/tests/test_reference_reactor.py`.
  - Verify: `uv run pytest -n0 examples/tests/test_reference_reactor.py -k route_drift -vv`.
- [ ] **T4 (P1, human: ~1h / Codex: ~10m)** - Reactor topology - Reject every queue-role collision before side effects.
  - Surfaced by: architecture review; role aliasing permits self-feeding or lane collapse.
  - Files: `examples/reference_reactor.py`, `examples/tests/test_reference_reactor.py`.
  - Verify: the seven-family parameterized collision test.
- [ ] **T5 (P1, human: ~2h / Codex: ~20m)** - Control lane - Reply to and checkpoint valid JSON non-object payloads.
  - Surfaced by: correctness review; `.get()` on list/scalar/null poisons the lane.
  - Files: `examples/reference_reactor.py`, `examples/tests/test_reference_reactor.py`.
  - Verify: all raw JSON shapes plus a later PING progress barrier.
- [ ] **T6 (P2, human: ~1.5h / Codex: ~15m)** - Replay performance - Return and materialize at most `budget + 1` pending rows.
  - Surfaced by: performance review; budget was applied after full materialization.
  - Files: `examples/reference_reactor.py`, `examples/tests/test_reference_reactor.py`.
  - Verify: zero/one budget tests and existing output-backlog tests.
- [ ] **T7 (P2, human: ~45m / Codex: ~10m)** - Restart test - Replace STOP masking and fixed sleep with an external stop and later-PING barrier.
  - Surfaced by: test review; later checkpoint hid the behavior under test.
  - Files: `examples/tests/test_reference_reactor.py`.
  - Verify: focused restart test under `-n0`.
- [ ] **T8 (P2, human: ~30m / Codex: ~5m)** - Thread test - Add a three-party start barrier before queue construction.
  - Surfaced by: test review; executor reuse made the thread-count assertion flaky.
  - Files: `tests/test_process_broker_session.py`.
  - Verify: focused test plus 20 local repetitions.
- [ ] **T9 (P2, human: ~1h / Codex: ~15m)** - Documentation - Align all reactor contract text with final lifecycle, routing, topology, and control behavior.
  - Surfaced by: documentation review; current text promises constructor replay.
  - Files: `examples/reference_reactor.py`, `README.md`, `examples/MULTI_QUEUE_README.md`.
  - Verify: stale-phrase search plus local example tests and explicit example mypy.
- [ ] **T10 (P1 gate, human: ~2h / Codex: ~20m plus runtime)** - Repository verification - Run focused, core, backend, local-example, lint, format, type, and diff gates.
  - Surfaced by: release readiness review.
  - Files: no additional files.
  - Verify: every command in Task 10, with unavailable external services reported exactly.

## GSTACK REVIEW REPORT

| Runs | Status | Findings |
|---|---|---|
| Initial scope and architecture review | Clean after revision | Kept eight implementation files, reused the existing backlog seam, rejected dynamic rerouting and new runtime abstractions |
| Code quality review | Clean after revision | Centralized topology validation and malformed-control durable tail; no duplicated checkpoint path |
| Test design review | Clean after revision | Required real Coverage.py, Queue, SQLite, deterministic barriers, and red-green slices; prohibited over-mocking |
| Performance review | Clean after revision | Required SQL `LIMIT budget + 1`; separated the result-materialization guarantee from unindexed SQLite scan cost |
| Outside-voice review | Clean after revision | Codex CLI failed before review, so a fresh read-only subagent checked the full plan; cleanup, timestamp, SQL-proof, mutation-test, and command gaps were repaired |
| User-provided external review | Clean after revision | Made the quoted-command choice explicit, qualified the coverage causal claim, documented background route-failure semantics, and removed worktree ceremony |
| Final self-audit | Clean | Re-read the full plan, verified file references and exact gates, balanced code fences, confirmed current target files are clean, and retained both withdrawn findings as non-goals |

**VERDICT:** PASS. The plan is implementable as written, with no unresolved product,
test, sequencing, or scope decisions.

NO UNRESOLVED DECISIONS
