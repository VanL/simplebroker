# Runner Transaction Ownership and Reactor Correctness Plan

Status: status-review — implementation shipped, but the plan records no
evidence for its required five post-commit coverage-diagnostics runs.

Class: 4 — risky. The change repairs a cross-thread transaction ownership
failure in the public `SQLRunner` compatibility surface, changes the
`SQLiteRunner` state machine, and must preserve behavior across SQLite,
PostgreSQL, injected runners, process sessions, sidecars, generators, setup,
fork recovery, and shutdown.

Plan type: implementation against an existing contract. This is not a new
product behavior or program-theory change. `SQLRunner` already promises
concurrency-safe connections and the transactional boundaries expected by
`BrokerCore`, and the README already tells embedders that one injected runner
may be shared by several queues. The implementation and documentation changes
make that existing promise precise and enforce it.

## Goal

Eliminate the SQLite runner lock inversion that can make concurrent writes on
one shared runner wait through repeated SQLite busy timeouts, while preserving
the public runner method surface and the current backend/session architecture.
Also remove the reference reactor's undocumented dependence on a queue
timestamp allocation silently joining an open sidecar transaction.

The completed change should make transaction ownership explicit: after a
runner successfully begins a transaction, another thread must not be able to
hold the runner's operation lock while waiting on a database lock owned by that
transaction. The transaction owner must retain a path to `commit()` or
`rollback()`.

## Source Documents and Baseline

Source contracts and rationale:

- [`README.md`, “Advanced: First-Party Backend Extensions”](../../README.md#advanced-first-party-backend-extensions):
  `simplebroker.ext` is public for custom runners; SQL-runner-shaped backends
  reuse `BrokerCore`; an explicitly injected runner remains caller-owned and
  may be reused across several queues.
- [`simplebroker/_runner.py`](../../simplebroker/_runner.py), `SQLRunner`
  protocol: implementations must provide concurrency-safe connections and the
  transactional boundaries `BrokerCore` expects.
- [`docs/program-theory.md`](../program-theory.md) [THEORY-2], [THEORY-3],
  [THEORY-4]: the backend runner owns the storage-specific atomic realization;
  explicit safety takes priority over magical recovery.
- [`docs/implementation/06-process-session-core-ownership.md`](../implementation/06-process-session-core-ownership.md):
  process sessions own sharing policy; backend runners own connection,
  transaction, and backend cleanup mechanics.
- [`docs/implementation/07-complexity-and-state-machine-map.md`](../implementation/07-complexity-and-state-machine-map.md),
  `SM-SQLITE-RUNNER`, `SM-REACTOR`, and `SM-REACTOR-OUTPUT`.
- [`examples/MULTI_QUEUE_README.md`](../../examples/MULTI_QUEUE_README.md),
  “Reactor Reference”: one reactor thread owns persistent broker handles and
  sidecar effects; workers remain broker-free.
- [`docs/agent-context/runbooks/testing-patterns.md`](../agent-context/runbooks/testing-patterns.md):
  use the narrowest controlled real-boundary regression and do not mock away
  the lock or SQLite interaction.
- [`docs/agent-context/runbooks/hardening-plans.md`](../agent-context/runbooks/hardening-plans.md):
  required because the work crosses a public compatibility and concurrency
  boundary.

Contract baseline:

- `fd10bbbe6270bdfabe6e9b3db9f3c1a374cae144` — README extension contract,
  `SQLRunner` protocol, SQLite and PostgreSQL runners, process-session
  rationale, reactor example, state-machine map, and current tests at plan
  authoring time.

Source spec:

- No canonical `[SB-*]` spec currently owns the public extension-runner
  transaction protocol. The product-section registry classifies the embedding
  and backend-extension concern as `readme-only`. This plan clarifies that
  winning README contract and the public protocol docstring without creating a
  competing spec.

## Current Structure and Confirmed Failure

`BrokerCore` owns queue-operation transactions. For example,
`_do_write_transaction()` holds the core lock, calls
`runner.begin_immediate()`, allocates a timestamp and inserts the row through
`runner.run()`, then calls `runner.commit()`.

`SQLiteRunner` has one thread-local SQLite connection per calling thread and
one process-wide reentrant `_operation_lock`. Today each public runner method
takes and releases that lock independently:

1. thread A enters `begin_immediate()`, obtains SQLite's write lock, and
   releases `_operation_lock`;
2. thread B obtains `_operation_lock` and blocks inside its own
   `BEGIN IMMEDIATE` on A's SQLite write lock;
3. thread A cannot call `run()` or `commit()` because B holds
   `_operation_lock`;
4. B eventually reaches its SQLite busy timeout, allowing A to resume.

With multiple contenders, repeated busy-timeout cycles can exceed the
caller's deadline. The original CI symptom is
`test_concurrent_first_use_publishes_one_shared_runner` timing out after ten
seconds. A local forced-interleaving probe reproduced the exact stacks:
the contender was inside SQLite `BEGIN IMMEDIATE`, while the owner was waiting
to re-enter `SQLiteRunner.run()`.

The process-session factory intentionally publishes one runner for a
non-SQLite SQL backend and gives each thread a separate `BrokerCore`. The
counting test backend uses `SQLiteRunner` to exercise that supported shared
runner topology. Rejecting shared runner instances would contradict both this
architecture and the README guidance.

PostgreSQL already retains its leased-operation lock from `BEGIN` through
transaction completion when several cores use one leased runner connection.
Non-leased PostgreSQL transactions retain their own pool checkout. Redis is a
direct backend and does not use the `SQLRunner` transaction path.

The reference reactor avoids the inversion in normal operation because
`BaseReactor` admits one drive thread and worker threads exchange only plain
dataclasses. However, `_record_pending_result()` currently calls
`self._output_queue.generate_timestamp()` inside
`self._metadata_queue.sidecar(transaction=True)`. Same-target persistent
queues resolve to the same thread-local core, so this queue operation silently
joins the sidecar transaction. That conflicts with the sidecar contract,
which tells callers not to invoke queue operations on that core inside a
transactional sidecar block.

### Required-reading comprehension checks

Before editing, the implementer must be able to answer:

1. Why does holding only `BrokerCore._lock` not serialize transactions when
   several thread-local cores share one runner?
2. Which runner state must be reset before acquiring any inherited lock after
   `fork()`, and why must inherited SQLite connections remain abandoned rather
   than closed in the child?
3. On a failed `commit()`, why must the transaction owner still be able to
   call `rollback()` before another thread enters the runner?
4. Why must the reactor re-check for an existing pending-output row after
   allocating a candidate message ID outside the sidecar transaction?
5. Which PostgreSQL paths already retain either a leased-operation lock or a
   pool checkout for the whole transaction, and therefore must not be
   mechanically rewritten to match SQLite internals?

If any answer is uncertain, stop and trace the current code and tests before
editing.

## Invariants and Constraints

### Transaction and runner invariants

1. **Owner progress:** once `begin_immediate()` succeeds, that thread must
   retain the in-process synchronization authority needed to reach
   `commit()` or `rollback()`. A contender may wait, but must not hold a runner
   lock needed by the owner while it waits on the owner's database lock.
2. **One connection per transaction:** every `run()`, `commit()`, and
   `rollback()` in one transaction uses the connection on which `BEGIN`
   succeeded.
3. **No protocol expansion:** keep the required public `SQLRunner` method
   shape unchanged. Do not add a mandatory `transaction()` method or a new
   extension capability for this fix.
4. **Normal autocommit remains normal:** `run()` outside an explicit
   transaction keeps current autocommit and exception-translation behavior.
5. **Retry ownership stays in `BrokerCore`:** runner contention still raises
   `OperationalError` as required. Do not add an independent retry loop to
   `SQLiteRunner`.
6. **Commit failure remains unsettled:** if `commit()` fails, do not publish
   the runner as idle before the owner has attempted rollback or terminal
   connection invalidation.
7. **Rollback settles ownership:** successful rollback releases transaction
   ownership. If rollback itself fails, invalidate the owning connection
   through the existing close/cleanup mechanics, preserve the repository's
   primary-versus-cleanup exception rules, and ensure transaction admission
   does not remain permanently closed after the connection is invalidated.
8. **Fork recovery precedes locks:** every runner entry point continues to call
   `_recover_after_fork_if_needed()` before acquiring a runner lock. Child
   recovery replaces inherited synchronization state, clears inherited
   transaction-owner metadata, and abandons rather than closes inherited
   connections.
9. **Bounded foreign waits:** a non-owner waits for transaction admission
   without holding `_operation_lock`, against one monotonic budget derived
   from the configured SQLite busy timeout. If the owner never settles, the
   waiter raises a diagnostic `OperationalError`; it does not wait forever.
   Existing cross-thread generator/sidecar poisoning still requires process
   restart and must not trigger foreign-thread rollback or ownership release.
10. **Terminal calls are owner-checked:** an owner with an active transaction
    may commit or roll back. A foreign thread must not terminate that
    transaction. A terminal call made with no active transaction preserves
    the current SQLite no-op/error behavior and must not over-release any lock
    or mutate owner state.
11. **Close cannot create a second owner:** normal close observes the same
    bounded admission rule. Existing process-session drain and cross-thread
    poison behavior remain the higher-level lifecycle controls; this plan must
    not invent an unsafe foreign-thread rollback path.
12. **State machine is explicit:** add transaction ownership to
    `SM-SQLITE-RUNNER` and give every new state/transition a firing test.

### Backend and compatibility invariants

13. PostgreSQL's leased-lock and checkout ownership model remains intact.
    Audit it against the clarified protocol, but do not copy SQLite's lock
    fields into PostgreSQL.
14. Redis/Valkey remains outside the SQL transaction path.
15. Injected runners remain caller-owned, reusable across queue handles, and
    structurally compatible with the existing protocol.
16. Wrapping runners such as `LoggingRunner`, borrowed runners, and test
    runners must preserve transaction terminal-call behavior. Audit every
    override of `begin_immediate()`, `commit()`, and `rollback()` before
    landing.
17. Existing exception types, retryability classification, cursor cleanup,
    setup behavior, busy-timeout configuration, and fork bounded-failure
    contract do not change.

### Reactor invariants

18. The reactor thread remains the only owner of reactor queue handles and
    sidecar writes. Worker functions stay broker-free.
19. No queue operation may be invoked through another persistent handle while
    a transactional sidecar session on the same core is open.
20. Pending-result insertion, source checkpoint advancement, seen-state
    update, and audit insertion remain one sidecar transaction.
21. Output message IDs remain unique and replay-stable. Allocating an unused
    candidate ID is acceptable; reusing an ID for a different logical result
    is not.
22. Multiple reactor processes remain an at-least-once topology. If another
    reactor inserts the pending row after the first preflight read, the current
    reactor must re-read inside the write transaction and adopt the stored
    route, ID, payload, and status rather than overwrite it.
23. Output publication and the later `output_written` sidecar mark remain
    separate commits with the documented replay window. This plan does not
    claim exactly-once application delivery.

### Failure priority

- A transaction-integrity or ownership failure is fatal to the operation.
- A failed rollback or failed connection invalidation must be surfaced using
  the existing cleanup-precedence rules; it must not be logged and ignored
  while the runner is reported usable.
- Diagnostic logging remains best-effort and must not replace the primary
  database exception.
- An unused reactor timestamp caused by a crash or a lost insert race is
  benign and needs no cleanup.

### Scope and implementation constraints

- No new dependency.
- No second transaction abstraction beside the current
  `begin_immediate()` / `run()` / `commit()` / `rollback()` path.
- Do not move runner ownership into `_ProcessBrokerSession` or add a
  runner-keyed global lock registry in `BrokerCore`; the backend runner owns
  its own concurrency semantics.
- Do not use sleeps as the causal synchronization in the regression test.
  Bounded waits may only serve as watchdogs and diagnostics.
- Do not broaden the fix into connection pooling, a general backend SDK,
  reactor leader election, or sidecar API redesign.

## Proposed Winning-Contract Clarification

The implementation slice should add the following paragraph beside the
injected shared-runner guidance near `README.md` line 2314:

> A runner that can be shared across threads must preserve transaction-owner
> progress: after `begin_immediate()` succeeds, another thread must not hold a
> runner resource needed by the owner to reach `commit()` or `rollback()` while
> waiting on storage state owned by that transaction. Implementations may
> satisfy this with a transaction-scoped lock, a retained connection checkout,
> or an equivalent backend mechanism. The required `SQLRunner` method set does
> not change.

Apply equivalent concise wording to the `SQLRunner` protocol docstring. The
README is the winning product contract; the docstring is the public
code-adjacent extension contract. If review finds that this wording promises
more than PostgreSQL or an existing first-party wrapper provides, stop and
revise the wording or implementation plan before changing code.

## Rollback, Rollout, and One-Way Doors

### Rollback

Keep the runner fix, reactor correction, docs, and tests in one revertible
change. No storage schema, on-disk data, backend API version, or message format
changes. Rolling back restores the old synchronization behavior and reactor
transaction join; it does not require data migration.

If transaction-scoped locking causes an unexpected first-party regression,
revert the change rather than weakening ownership with an environment flag.
The controlled inversion regression must remain available to show what risk
the rollback restores.

### Rollout

1. Land the controlled failing runner test first in the implementation
   change and demonstrate it fails against the baseline behavior.
2. Implement and verify SQLite transaction ownership.
3. Audit first-party wrappers and PostgreSQL before changing the README
   clarification from planned text into the winning contract.
4. Correct and verify the reactor's timestamp allocation boundary.
5. Run targeted and full local suites, then ordinary CI and the explicit
   repeatable coverage workflow.

No staged compatibility period is required because no callable method,
database format, or configuration key changes.

### One-way doors

None. There is no data migration or irreversible external action. Treat any
proposal to bump `backend_api_version`, require a new runner method, or alter a
database schema as a new one-way compatibility decision: stop and reclassify
the work before proceeding.

### Post-merge signals

- The repeatable coverage workflow completes without
  `test_concurrent_first_use_publishes_one_shared_runner` timeouts.
- No SQLite write path spends one or more full busy-timeout periods while its
  transaction owner is waiting only for `_operation_lock`.
- PostgreSQL, Redis, Windows, and coverage jobs retain their current pass
  rates and durations.
- Any new ownership diagnostic identifies runner instance, owner thread, and
  attempted terminal action without exposing database credentials or payloads.

## Dependency-Ordered Tasks

### 1. Convert the forced interleaving into a controlled red regression

Files:

- modify `tests/test_runner_error_handling.py` or add a narrowly named runner
  transaction-ownership test module if keeping the concurrency fixture local
  would make that file materially clearer;
- modify `tests/test_core_persistence_transition_tables.py`;
- update `tests/state_machine_manifest.py` only if the existing
  `SM-SQLITE-RUNNER` declaration needs a new enumerated transition name, not a
  second machine.

Actions:

- Use a real `SQLiteRunner`, real SQLite file, and real threads.
- Pre-create/setup the database so setup contention is not part of the test.
- Coordinate events or barriers so thread A has completed `BEGIN IMMEDIATE`,
  thread B has acquired the current `_operation_lock` and is about to execute
  its competing real `BEGIN IMMEDIATE`, and A then performs a real statement
  and commit.
- Add a probe seam that observes entry at the runner/connection boundary
  without replacing SQLite's locking behavior. Do not mock
  `begin_immediate()`, `run()`, `commit()`, or the SQLite connections.
- Use a deliberately large SQLite busy timeout and a much smaller bounded
  owner-progress assertion. The red proof is the controlled real interleaving
  plus A failing to complete far below the busy timeout; do not claim the test
  can observe SQLite's internal busy-handler state exactly. B must subsequently
  complete. On failure, report thread liveness, captured stacks or phase names,
  and runner instance identity.
- Add explicit `SM-SQLITE-RUNNER` transitions for at least:
  `IDLE -> ACTIVE_OWNER`, `ACTIVE_OWNER -> COMMITTED -> IDLE`,
  `ACTIVE_OWNER -> ROLLED_BACK -> IDLE`, failed begin back to idle,
  failed commit retaining owner authority until rollback, foreign contender
  bounded timeout without `_operation_lock`, terminal call with no active
  transaction, foreign terminal rejection, and fork reset discarding inherited
  transaction ownership.
- Run the test against baseline behavior and record the red evidence in this
  plan's execution log before implementing the fix.

Stop and re-evaluate if the test only fails because of an arbitrary sleep
duration or an instrumented fake connection. A small repeated stress count is
acceptable to make the final handoff between the observable “about to execute”
phase and SQLite's internal busy wait high-confidence; the real SQLite lock
must remain load-bearing.

Done signal:

- one targeted command fails on the baseline with the owner/contender
  inversion at a high, recorded reproduction rate and completes in seconds.

### 2. Add transaction-scoped ownership to `SQLiteRunner`

Files:

- modify `simplebroker/_runner.py`;
- modify the targeted runner and transition-table tests from task 1;
- inspect `tests/test_fork_safety.py`, `tests/test_connection_transition_tables.py`,
  and `tests/test_db_connection_lifecycle.py` for affected assertions.

Actions:

- Add one internal transaction-admission state beside the existing
  `_operation_lock`; do not add a public dispatcher or a second transaction
  API. Guard the state with a condition whose wait releases its own mutex.
- Before executing `BEGIN IMMEDIATE`, reserve the transaction owner identity.
  Contending `begin_immediate()`, `run()`, and `close()` calls wait on that
  condition without holding `_operation_lock`. The owner bypasses the wait and
  uses the existing operation lock normally for each SQLite call.
- Bound a foreign wait with one monotonic deadline derived from the configured
  SQLite busy timeout. Timeout raises a diagnostic, retryable
  `OperationalError` containing runner and owner identity, without exposing
  target credentials or payloads.
- Record the minimum state required for owner checks, diagnostics, and correct
  terminal transitions. Keep state mutation next to admission and settlement.
- Return admission to idle on failed begin, successful commit, and successful
  rollback, then notify all waiters.
- On commit failure, preserve the active owner state for the caller's rollback
  path.
- On rollback failure, invalidate the owning thread's connection using the
  existing connection tracking/close mechanics, attach cleanup evidence
  without replacing the required primary exception, and reopen admission only
  after the connection can no longer silently continue the unsettled
  transaction. If invalidation cannot settle the connection, enter an explicit
  unusable state whose callers fail fast rather than wait forever.
- Owner-check terminal calls. Preserve current behavior for `commit()` or
  `rollback()` when no transaction is active; do not decrement or release a
  retained hold because this design does not retain `_operation_lock`.
- Reset the transaction condition and owner state beside `_operation_lock`
  during fork recovery, before any child-side acquisition.
- Preserve `_recover_after_fork_if_needed()` as the first action in every
  lock-taking entry point.
- Keep cross-thread finalization behavior unchanged: a foreign generator or
  sidecar finalizer publishes core poison but does not release runner admission
  or roll back the owner's connection. Sibling callers receive the new bounded
  owner-wait diagnostic until the required process restart.
- Add an edit-point comment naming the inversion/admission invariant and
  pointing to the `SM-SQLITE-RUNNER` test table.

Stop and re-evaluate if:

- the implementation needs a new public protocol method;
- any contender waits while holding `_operation_lock`;
- rollback failure can leave both an open SQLite transaction and a runner
  advertised as usable;
- the cross-thread poison path gains foreign-thread transaction cleanup;
- fork recovery touches or closes a parent connection;
- ordinary non-shared SQLite use gains a second retry layer.

Done signal:

- the task-1 regression turns green;
- all new owner-state transitions fire;
- existing runner error, lifecycle, and fork tests pass.

### 3. Audit all first-party consumers and wrappers of transaction methods

Files to inspect and change only where required:

- `simplebroker/db.py` (`_BorrowedRunner`, `BrokerCore` transaction call sites);
- `simplebroker/_backends/sqlite/schema.py`;
- `simplebroker/_backends/sqlite/maintenance.py`;
- `examples/logging_runner.py`;
- `tests/test_process_broker_session.py` (`CountingSQLiteRunner`);
- `tests/test_custom_runner_integration.py`;
- `tests/test_write_visibility.py`;
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`;
- PostgreSQL runner lifecycle and integration tests.

Actions:

- Enumerate every override/delegation of `begin_immediate()`, `commit()`, and
  `rollback()` with `rg`.
- Confirm wrappers do not mark a transaction finished, release a leased
  connection, or close caller-owned state before failed-commit rollback has
  settled it.
- Keep `_BorrowedRunner` a transparent ownership-neutral delegate.
- Confirm PostgreSQL leased and non-leased transactions satisfy the clarified
  owner-progress invariant. Add a focused PostgreSQL regression only if the
  audit exposes an untested transition; do not rewrite correct PostgreSQL
  internals for symmetry.
- Confirm Redis has no affected path.
- Rerun `test_concurrent_first_use_publishes_one_shared_runner` repeatedly
  with the real counting backend and retain its one-run assertion in the
  normal suite.

Stop and re-evaluate if any first-party runner cannot satisfy the clarification
without a callable protocol or backend API version change.

Done signal:

- every first-party transaction-method producer and consumer has an explicit
  disposition in the execution log;
- affected tests pass repeatedly without a ten-second future timeout.

### 4. Remove the reactor's cross-handle transaction join

Files:

- modify `examples/reference_reactor.py`;
- modify `examples/tests/test_reference_reactor.py`;
- update `examples/MULTI_QUEUE_README.md`;
- update the `SM-REACTOR-OUTPUT` transition table in the existing example
  tests if the transition inventory changes.

Actions:

- Refactor `_record_pending_result()` so no
  `_output_queue.generate_timestamp()` call occurs inside
  `_metadata_queue.sidecar(transaction=True)`.
- Preserve the efficient existing-row path: preflight-read the pending result;
  allocate a candidate ID outside the transaction only when a new row appears
  necessary; then re-read inside the write transaction before inserting.
- If an existing row wins the race, adopt its stored output queue, message ID,
  payload, and status. Treat the unused candidate ID as a harmless monotonic
  gap.
- Keep pending-row insertion, checkpoint advancement, seen state, and audit
  state in the same sidecar transaction.
- Add a firing test that instruments the public queue boundary only enough to
  assert timestamp generation occurs with no sidecar transaction open. Keep
  the SQLite sidecar and persistent process-session path real.
- Retain all current replay, route-drift, crash-window, backlog, control-lane,
  and multiple-worker behavior.
- Tighten the example docs: the single-writer reactor avoids the shared-runner
  inversion only while broker effects stay on the reactor thread, and queue
  operations must not be nested inside a same-core sidecar transaction.

Stop and re-evaluate if the refactor weakens the atomic sidecar updates, assigns
a new ID to an already-recorded logical result, or needs a new public sidecar
allocator API.

Done signal:

- the new boundary test fires;
- the complete reference-reactor suite passes;
- existing replay tests prove that stored IDs remain stable.

### 5. Clarify the public contract and durable rationale

Files:

- modify `README.md` under “Advanced: First-Party Backend Extensions”;
- modify the `SQLRunner` protocol docstring in `simplebroker/_runner.py`;
- modify `docs/implementation/06-process-session-core-ownership.md`;
- modify `docs/implementation/07-complexity-and-state-machine-map.md`;
- add `CHANGELOG.md` release-note text when the implementation lands;
- update this plan's execution, deviation, and review logs.

Actions:

- Apply the proposed winning-contract clarification after verifying every
  first-party SQL runner against it.
- Document transaction-scoped ownership as runner policy, not process-session
  policy.
- State that a deliberately shared `SQLiteRunner` serializes both reads and
  writes behind an active transaction owner. Do not imply lock-free read
  progress for that topology.
- Update `SM-SQLITE-RUNNER` state and proof references with the new
  transaction-owner transitions.
- Add the plan backlink to the implementation documents. There is no
  canonical spec backlink for this readme-only concern.
- Record the user-visible effect accurately: bounded forward progress for
  shared SQLite runners and removal of an undocumented reactor transaction
  join. Do not claim a new exactly-once guarantee.

Stop and re-evaluate if documentation starts promising fairness, lock-free
reads, bounded application callback time, or cross-process ownership beyond
what the tests prove.

Done signal:

- README, protocol, implementation rationale, state-machine inventory,
  CHANGELOG, code, and tests describe one ownership model.

### 6. Verification, independent implementation review, and closeout

Actions:

- Run the verification matrix below.
- Run ordinary CI, including Windows and service-backed extensions.
- Run the explicit coverage diagnostics workflow five times, using the
  45-minute per-job timeout already present in that workflow.
- Inspect failures as evidence. Do not classify a timeout or flake as noise
  without a causal explanation.
- Request independent review of the completed implementation with this plan,
  the winning README contract, both implementation docs, runner/reactor
  changes, and concrete test output in scope.
- Reproduce reviewer findings before changing code. Record every disposition.
- Update the Status Index row to `completed` only after implementation is
  committed and all required evidence is current.

Stop and re-evaluate if:

- a coverage run again stalls in the concurrent first-use test;
- any runner ownership test requires relaxing a timeout instead of correcting
  state;
- Windows or fork recovery reveals an unbounded wait;
- a first-party backend fails the clarified contract.

Done signal:

- all final gates pass from the committed implementation state;
- independent review returns PASS after dispositions;
- the plan index row is `completed`.

## Testing Plan

### Proof that must stay real

- Real `SQLiteRunner` objects and real SQLite files/connections for the
  inversion regression.
- Real threads and the production `BrokerCore`/process-session path for the
  concurrent first-use regression.
- Real `Queue.sidecar(transaction=True)` and persistent same-target session
  behavior for the reactor boundary.
- Real PostgreSQL and Redis services for their integration gates where CI or
  local service configuration is available.
- Real `fork()` for POSIX fork recovery.

Permitted test seams:

- events, barriers, thread-safe phase recording, bounded watchdogs, and stack
  capture;
- fault injection at an existing runner connection terminal call for
  commit/rollback cleanup precedence;
- spies at the public reactor queue-operation boundary to prove call ordering,
  provided the sidecar transaction and SQLite storage remain real.

Do not mock:

- SQLite's `BEGIN IMMEDIATE` lock acquisition in the inversion proof;
- `SQLiteRunner._operation_lock`;
- process-session core/runner publication;
- sidecar transaction begin/commit/rollback;
- the pending-output database rows or replay reads.

### Required scenarios

1. Owner commits while one and several contenders wait without holding
   `_operation_lock`.
2. Owner rolls back while a contender waits.
3. Failed begin releases ownership.
4. Failed commit retains owner progress through rollback.
5. Failed rollback invalidates the connection and either reopens admission or
   makes the runner fail fast.
6. Fork from a parent with active transaction ownership resets child locks
   and owner metadata before use.
7. Foreign terminal calls are rejected; terminal calls with no active
   transaction preserve current behavior.
8. Cross-thread generator/sidecar poison does not perform foreign cleanup;
   sibling runner callers fail within the bounded admission budget.
9. Close/shutdown does not overtake a live owner or hang after settled
   ownership.
10. Shared process-session first use publishes one runner and all concurrent
   writes complete.
11. Non-shared and ordinary single-thread SQLite behavior remains unchanged.
12. PostgreSQL leased and non-leased transaction paths still complete and
    release their resources.
13. Reactor new result, existing pending result, already-written result,
    competing stored row, replay, and route mismatch preserve the stored ID.
14. Reactor workers remain broker-free and all broker effects happen on the
    drive thread.

### Targeted commands

```bash
uv run pytest -q \
  tests/test_runner_error_handling.py \
  tests/test_core_persistence_transition_tables.py \
  tests/test_process_broker_session.py \
  tests/test_fork_safety.py \
  tests/test_db_connection_lifecycle.py

uv run pytest -q examples/tests/test_reference_reactor.py

uv run pytest -q extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py
uv run pytest -q extensions/simplebroker_pg/tests/test_pg_integration.py
uv run pytest -q extensions/simplebroker_redis/tests/test_redis_integration.py
```

Service-backed commands may require the repository's documented service
environment. If unavailable locally, they remain mandatory CI gates and the
handoff must say they are pending rather than silently omitting them.

## Verification and Gates

Per-task gates:

- Task 1: controlled, high-reproduction red proof against baseline behavior.
- Task 2: targeted regression green plus complete `SM-SQLITE-RUNNER`
  transitions.
- Task 3: transaction-method audit complete; process-session regression passes
  repeatedly; first-party SQL runner contract verified.
- Task 4: complete reference-reactor suite and explicit no-nested-queue-call
  assertion.
- Task 5: documentation and implementation ownership agree.

Final local gates:

```bash
uv run pytest
uv run ruff check .
uv run mypy
python3 bin/check-dom15-fixtures
uv run pytest -q \
  tests/test_state_machine_policy.py \
  tests/test_core_persistence_transition_tables.py
bin/coalesce-check
```

If the repository's mypy entry point is partitioned rather than the single
command above at implementation time, use the commands defined by current CI
and record them in the execution log. Do not weaken or skip the type gate
because the plan's command aged.

Final CI gates:

- ordinary CI on Linux, macOS, and Windows;
- PostgreSQL and Redis extension suites;
- coverage-linux and coverage-redis;
- five manual runs of the explicit coverage diagnostics workflow, each
  bounded by 45 minutes per job.

Success means:

- no owner/contender test reaches the SQLite busy timeout;
- no thread, process, or coverage job remains live past its declared deadline;
- every new state-machine transition is enumerated and fires;
- no backend compatibility or public API change beyond the clarified existing
  semantics;
- docs and traceability checks pass with no new warning debt.

## Independent Review Loop

Plan review:

- Reviewer: Claude, the current preferred live family distinct from the
  Codex author, invoked read-only through `skills/call-agent/SKILL.md`.
- Inputs: this plan verbatim; README extension contract; `SQLRunner`,
  `SQLiteRunner`, and PostgreSQL runner transaction methods; process-session
  ownership rationale; reactor example/docs; runner and reactor state-machine
  registrations; closest tests.
- Stance: find errors, bad ideas, latent ambiguity, missing transitions,
  compatibility breaks, weak test seams, and performative overengineering.
  Prefer deleting unnecessary machinery. Answer `PASS` or `BLOCKED` based on
  whether a zero-context engineer can implement confidently and whether the
  plan would degrade correctness or compatibility.

Implementation review:

- Use the same family if still live and not involved in implementation.
- Include the committed diff and current verification evidence.
- Require findings first and an explicit PASS/BLOCKED verdict.

Disposition:

- Reproduce each factual claim.
- Append every finding to the review log below as accepted, rejected with
  reason, or out of scope with reason.
- A `BLOCKED` plan review prevents implementation until the ambiguity is
  resolved and a bounded round-2 review returns PASS.

## Out of Scope

- Fixing or changing the previously observed PostgreSQL/Windows hang that the
  owner explicitly chose to monitor for recurrence.
- Rewriting the public `SQLRunner` API around a context manager.
- Rejecting all shared runner instances.
- Adding fairness guarantees or reader/writer locks.
- Changing SQLite busy-timeout defaults.
- Replacing thread-local connections with a pool.
- Changing process-session registry identity, refcounts, or shutdown deadlines.
- Redesigning sidecar sessions or adding a public transaction-bound message-ID
  allocator.
- Reactor leadership, leases, exactly-once application processing, or worker
  deadlines.
- Unrelated CI harness or coverage collection changes.

## Deviation Log

| Contract or plan ref | Planned behavior | Actual behavior | Rationale | Contract proposal |
|----------------------|------------------|-----------------|-----------|-------------------|

## Plan Review Log

| Date | Reviewer | Verdict | Finding | Disposition |
|------|----------|---------|---------|-------------|
| 2026-07-30 | Claude | PASS with corrections | P2-1: retaining `_operation_lock` would make cross-thread orphan poison a runner-wide permanent Python lock, conflicting with the no-foreign-cleanup rule. | Accepted and strengthened: replaced retained-lock design with bounded condition-based transaction admission; foreign poison still never cleans up owner state, while sibling callers fail within a bounded diagnostic budget. Material architecture revision requires round-2 plan review. |
| 2026-07-30 | Claude | PASS with corrections | P2-2: retained-hold accounting was under-specified, especially terminal calls with no active transaction. | Accepted: removed retained-hold accounting, added explicit owner checks, no-active-transaction and foreign-terminal transitions, and firing tests. |
| 2026-07-30 | Claude | PASS with corrections | P2-3: a real SQLite test cannot deterministically observe the exact instant a contender is inside SQLite's busy handler. | Accepted: renamed the proof controlled/high-reproduction, specified a large busy timeout and short owner-progress bound, and prohibited claiming exact internal observation. |
| 2026-07-30 | Claude | PASS with correction | P3-1: place the README clarification beside injected shared-runner guidance, not the general backend-shape list. | Accepted. |
| 2026-07-30 | Claude, round 2 | PASS | Verified the condition-based admission revision resolves P2-1/P2-2/P2-3/P3-1 without a new correctness defect. | Accepted; no further synchronization-design change. |
| 2026-07-30 | Claude, round 2 | Non-blocking observation | The plan named nonexistent `bin/check-state-machine-contracts`; the real state-machine gate is pytest policy plus firing tables. | Accepted: replaced it with `tests/test_state_machine_policy.py` and `tests/test_core_persistence_transition_tables.py`. |
| 2026-07-30 | Claude, round 2 | Non-blocking observation | `run()` admission serializes reads behind an active transaction on a deliberately shared SQLite runner. | Accepted and already within the stated risk; Task 5 now requires explicit README/CHANGELOG wording and prohibits implying lock-free reads. |
| 2026-07-30 | Claude, implementation review | no blocker, F1 P3 | Explicit `close()` behind a foreign live owner now waits to the admission budget and raises before closing tracked connections; context-manager exit can therefore propagate the bounded error. | Accepted: documented the close behavior and the distinction between first-party best-effort shutdown and explicit callers in the process-session implementation note. |
| 2026-07-30 | Claude, implementation review | no blocker, F2 nit | Rollback catches `BaseException` and invalidates on interrupts, unlike commit; the safety intent was non-obvious. | Accepted: added a local comment tying the broad catch to settlement of unknown transaction state. |
| 2026-07-30 | Claude, implementation review | no blocker, F3 nit | The reactor preflight adds a read on the new-result path; always allocating a candidate would be simpler because unused IDs are benign. | Rejected optional simplification: Task 4 deliberately requires the efficient existing-row path so replay churn does not allocate IDs. The transactional re-read remains necessary for competing inserts. |
| 2026-07-30 | Claude, implementation review round 2 | PASS | Rechecked only accepted F1/F2 fixes against current close admission, shutdown suppression, rollback invalidation, unusable-state, and exception-precedence code. | Both fixes verified; no new defect introduced. |

## Execution Log

Append evidence during implementation. Do not record transient worktree state.

| Date | Task | Evidence | Result or residual risk |
|------|------|----------|-------------------------|
| 2026-07-30 | T1 controlled red regression | `uv run pytest -q tests/test_runner_error_handling.py::TestSQLiteRunnerErrorHandling::test_shared_runner_transaction_owner_reaches_commit_before_busy_timeout` | RED as required: owner failed the 0.5-second progress bound while the contender held `_operation_lock` through its real SQLite busy wait; command exited 1 in 2.65 seconds. |
| 2026-07-30 | T2 SQLite transaction admission | The T1 regression turned green; `SM-SQLITE-RUNNER` now fires begin/commit, begin/rollback, failed begin, failed commit then rollback, no-transaction terminal calls, foreign terminal rejection, foreign admission timeout, owner close, and active-transaction fork reset. Focused runner, poison, lifecycle, fork, process-session, and reactor command passed. | Condition waiters never hold `_operation_lock`; rollback-close failure enters fail-fast unusable state until explicit `close()` succeeds. |
| 2026-07-30 | T2 orphan-poison and multi-contender boundaries | Added real-thread tests for three contenders held above the operation lock until owner rollback, and for a sibling shared-runner call after real foreign generator finalization. | All contenders completed after rollback; the poisoned sibling failed retryably within the configured admission budget rather than hanging. |
| 2026-07-30 | T3 transaction-method audit | Enumerated overrides with `rg`. `_BorrowedRunner`, `LoggingRunner`, recording wrappers, schema/maintenance callers, and custom-runner fixtures are transparent. `CountingSQLiteRunner.commit()` was corrected to retain local transaction/lease state on failed commit. PostgreSQL retains a leased-operation lock for shared checkout transactions and a pool checkout for non-leased transactions; its terminal failure path returns/resets the backend connection rather than advertising an open transaction. Redis is a direct core. The async pooled example uses a separate async protocol and is outside this SQLRunner change. | No public method or backend API change. PostgreSQL unit/state-machine cases passed; 15 service-backed cases remain skipped locally without `SIMPLEBROKER_PG_TEST_DSN`. |
| 2026-07-30 | T3 original CI regression repetition | Ran `test_concurrent_first_use_publishes_one_shared_runner` in 10 consecutive isolated pytest invocations. | 10/10 passed; no future timeout and no timeout increase. |
| 2026-07-30 | T4 reactor boundary red/green | `test_pending_output_id_is_allocated_outside_sidecar_transaction` first failed with observed state `[True]`, then passed with `[False]`; complete reference-reactor scenario and transition suites passed. | New-result allocation is outside the sidecar transaction; the transactional re-read adopts a competing stored row and preserves replay-stable IDs. |
| 2026-07-30 | T5 contract and rationale | Updated README, `SQLRunner` docstring, process-session ownership rationale, state-machine map, reactor guidance, CHANGELOG, and suppression-registry line references. | One documented model: transaction progress is runner policy; deliberately shared SQLite reads and writes serialize behind the active owner. |
| 2026-07-30 | Local final gates | `uv run pytest` -> `2308 passed, 17 skipped`; `uv run ruff check .`; production mypy partition -> 60 files clean; reference-reactor mypy -> 42 files clean; DOM-15 fixture, Ruff policy, state-machine policy/table, coalescing, and `git diff --check` all passed. | PostgreSQL/Redis service integration, ordinary CI, and five post-commit coverage workflow runs remain landing gates because this implementation is not yet committed or pushed. |
| 2026-07-30 | Independent implementation review | Claude returned `no blocker`; F1/F2 were accepted and fixed, F3's optional preflight removal was declined against Task 4, and focused round-2 verification returned `PASS`. | No unresolved implementation-review finding. |

## Fresh-Eyes Review

Before marking the plan active:

- verify every task names files, current owner, real proof, stop gate, and done
  signal;
- verify the runner algorithm settles failed begin, failed commit, failed
  rollback, fork, and close paths without abandoned ownership;
- verify the plan does not mistake the reactor workaround for a runner fix;
- verify the reactor correction preserves the multi-process re-check and
  replay-stable ID;
- verify no task silently changes PostgreSQL or Redis for symmetry;
- verify rollback precedes tasks and no one-way compatibility decision is
  hidden;
- verify every README/protocol claim has a firing first-party test;
- remove any gate or abstraction that does not address a demonstrated risk;
- rerun the plan-index and DOM-15 documentation checks.

The author must repeat this pass after independent review dispositions. If the
review changes invariants, ownership, compatibility, or blast radius, record a
revision rationale and repeat class-4 review before implementation.
