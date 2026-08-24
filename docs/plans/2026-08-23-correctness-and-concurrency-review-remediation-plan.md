# Correctness and Concurrency Review Remediation Plan

Class: 5 - revises published delivery and Python lifecycle contracts, changes
watcher startup/cleanup concurrency, and hardens generator resource ownership.
The public-contract and concurrency triggers make the hardening checklist
mandatory.

Plan type: implementation with spec revision.

Status: completed - implemented, fully verified, independently reviewed, and
closed in the targeted commit containing this plan.

## Goal

Resolve the actionable parts of the Factor 2 correctness and concurrency
review without replacing SimpleBroker's small queue model with stronger
snapshot, checkpoint, or terminal-lifecycle abstractions that the product does
not promise.

The implementation will close internally consumed Queue generators
deterministically, serialize `BaseWatcher` startup and cleanup ownership, and
pin the existing full-transition lock around timestamp generation. The
contract work will state why live offset paging cannot be repaired by changing
to an ID or current row-sequence cursor, and why `SQLiteRunner.close()` releases
currently owned resources without making the reusable runner terminal. Stale
or unsafe review remedies remain explicit no-action dispositions.

## Investigation Disposition Matrix

| Review item | Disposition | Required action |
|-------------|-------------|-----------------|
| 2.1: `peek_generator()` offset paging can skip rows under concurrent mutation; replace the offset with a keyset cursor | The mechanism is real, but the proposed repair is not correct. Exact insert can put an older public ID behind a `(timestamp, id)` cursor. Move re-homes the row in place while preserving both its public ID and current internal sequence, so a moved-in row can fall behind either cursor. The current live offset stream remains deliberately weak. | Clarify `[SB-DELIVERY-4]`; keep code unchanged; direct callers needing a bounded observation to `peek_many()` or one-message peek/process/delete-by-id. Any stronger traversal must first choose fixed-start, live-rescan, or snapshot semantics. |
| 2.2: range-filtered high-level `Queue.read()` and `Queue.peek()` take one item from a generator without closing it | Accepted. CPython usually finalizes the local generator promptly, but deterministic resource release must not depend on reference-count timing or interpreter implementation. | Add one private typed next-and-close helper, use it for the one-item range branches of `read`, `peek`, and existing `move`, and prove immediate closure against real SQLite generators. |
| 2.3: move timestamp compare-and-swap outside the local lock | Rejected. The CAS is intentionally inside the lock. CAS protects durable uniqueness, but the same-generator lock also prevents an earlier thread from publishing a stale local cache after a later thread has advanced durable state. | Do not move or narrow the lock. Document the full transition, add a deterministic shared-instance serialization firing test, improve the method rationale, and record the historical race in `docs/lessons.md`. |
| 2.4: setup admission errors are not marked retryable | Stale. `SQLiteRunner` explicitly sets `OperationalError.retryable = True` on contention and retry policy consults that marker first. | No product or documentation change. Re-run the existing admission and retry-policy firing gates. |
| 2.5: parent processes retain one abandoned SQLite connection list per forked child, so cap the list | Incorrect for siblings. Fork recovery appends only in the child's copy-on-write address space; sibling forks do not grow the parent list. A nested fork lineage can accumulate references, but dropping them can finalize inherited SQLite connections in the wrong process and recreate the cross-fork close hazard. | No cap, warning, or cleanup change. Preserve abandon-all behavior and re-run fork-safety tests. Reopen only with measured harmful growth in a nested-fork workload and a close-free disposal proof. |
| 2.6: `BaseWatcher.stop()` can race `run_forever()` startup and both paths can clean runtime resources | Accepted. The current stop lock does not cover run publication. A stop caller can sample idle, then a run can publish and later clean while stop also cleans. The review's narrower suggestion to lock only `_running_event.set()` is insufficient because cleanup ownership, thread publication, stop-before-start, join timeout, failure, and finalizer retry form one transition. | Add one lock-protected lifecycle/cleanup-owner state; choose one cleanup owner before blocking work; keep join and cleanup outside the lock; prove the race with event-controlled real watcher tests. Do not add `PollingStrategy` locks without new evidence. |
| 2.7: `SQLiteRunner.close()` can be followed by a concurrent or later connection registration | Clarification, not a defect. `close()` releases connections owned at its linearization point; the runner remains reusable. Terminal admission belongs to the owning process session/factory. | Update `[SB-API-11]`, implementation rationale, state-machine wording, and the `close()` docstring. Add a sequential close/reopen characterization row. Do not add a runner `_closed` latch or generation rejection. |

## Consulted Surfaces

- Product judgment: `docs/program-theory.md`, the winning product specs, and
  the implementation rationale named below.
- Process and hardening: `docs/agent-context/context.index.yaml`,
  `docs/specs/01-development-documentation-operating-model.md`,
  `docs/agent-context/runbooks/writing-plans.md`,
  `docs/agent-context/runbooks/hardening-plans.md`,
  `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md`,
  `docs/agent-context/runbooks/testing-patterns.md`, and
  `docs/agent-context/runbooks/adversarial-acceptance-probes.md`.
- Code and tests: `simplebroker/sbqueue.py`, `simplebroker/_timestamp.py`,
  `simplebroker/_runner.py`, `simplebroker/_retry_policy.py`,
  `simplebroker/watcher.py`, their core transition tables, focused resource,
  fork, timestamp, and watcher suites, and Weft's watcher subclass usage.
- Historical evidence: commit `6f9bd065` and the timestamp thread-safety
  regression it introduced. The review text itself is investigation input,
  not a durable contract owner.

## Source Documents

- `docs/program-theory.md` [THEORY-1], [THEORY-3], [THEORY-4], and
  [REV-THEORY-005]
- `docs/specs/11-delivery.md` [SB-DELIVERY-4] and [SB-DELIVERY-6]
- `docs/specs/13-message-identity.md` [SB-ID-1], [SB-ID-2], [SB-ID-3],
  [SB-ID-4], and [SB-ID-5]
- `docs/specs/16-python-library-api.md` [SB-API-4], [SB-API-5], [SB-API-6],
  and [SB-API-11]
- `docs/specs/product-section-registry.md` (winning delivery, identity, and
  Python API owners)
- `docs/implementation/04-cross-thread-finalization-poisoning.md`
- `docs/implementation/06-process-session-core-ownership.md`
- `docs/implementation/07-complexity-and-state-machine-map.md`
- `docs/implementation/08-message-identity-and-write-visibility.md`
- `docs/lessons.md`
- `CHANGELOG.md`

## Program-Theory Fit

- [THEORY-1] favors a small, predictable interface. This plan repairs hidden
  resource and cleanup ownership inside existing methods instead of adding a
  second pagination or runner lifecycle API.
- [THEORY-3] assigns resources to concrete owners. The process session owns
  terminal admission; a runner owns currently tracked connections; a running
  watcher or a stopped-before-start path owns cleanup; a Queue convenience
  call owns the generator it creates and consumes internally.
- [THEORY-4] requires explicit safety without speculative machinery. The plan
  adds state only where a reproduced race crosses owners. It rejects a cursor,
  retention cap, and terminal runner latch that do not satisfy the real
  contracts.
- [REV-THEORY-005] explains why a suspended generator retains transaction and
  cleanup context. Even the nontransactional peek generator should be closed
  by the convenience method that created it; a consuming generator may own
  stronger settlement state.

No program-theory revision is proposed. If implementation requires a new
public traversal consistency mode, terminal runner state, or concurrent
multi-run watcher semantics, stop and start a new owner-reviewed design plan.

## Spec Baseline

- Repository baseline: `d63e65523103229066de7531cb3b1183cd0f45c4`.
- Authoring hashes:
  - `docs/specs/11-delivery.md`:
    `05d97e2960dd5d480b84bb5866062a282549f9500dfc02f2764991b711fb94f4`
  - `docs/specs/13-message-identity.md`:
    `ac233eb5c0accc285ebb94ebac835707e20fcbecfe912aaf68c29c02439a38b3`
  - `docs/specs/16-python-library-api.md`:
    `9bf9db0e3cdfceeab4dd99631478be10cb6b9ccfa81e9a39775348630161c584`
  - `docs/program-theory.md`:
    `6a830707ff45ef6826d5e2353b1522ed7044a68d050940ecb353f5c1ab824b14`
- The Python API authoring hash includes the uncommitted, separately owned
  `[SB-API-2]` and Related Plans edits from
  `2026-08-23-relative-sqlite-containment-and-config-mode-warning-removal-plan.md`.
  Preserve or rebase those edits. This plan owns only `[SB-API-6]`,
  `[SB-API-11]`, their verification mappings, and its own backlink.
- Before Task 1, record the new HEAD and hashes in the Execution Log. If any
  governing clause changed semantically, update the delta and obtain scoped
  re-review before editing code.

## Context and Key Files

| Area | Current owner and behavior | Planned files |
|------|----------------------------|---------------|
| Live peek pagination | `[SB-DELIVERY-4]`; `Queue.peek_generator()` is an offset-paged live stream. `[SB-ID-1]`, `[SB-ID-4]`, and `[SB-ID-5]` permit older exact IDs and ID-preserving moves; storage order is a separate internal sequence. | `docs/specs/11-delivery.md`, `tests/test_delivery_contract_sb_delivery.py`, `docs/implementation/08-message-identity-and-write-visibility.md` |
| One-item convenience iteration | `Queue.read()` and `Queue.peek()` call `next()` on internally created range-filter generators without `finally`; `Queue.move()` already closes its generator. | `simplebroker/sbqueue.py`, `tests/test_queue_api_comprehensive.py` or the closest existing range-filter suite |
| Timestamp transition | `TimestampGenerator.generate()` holds one `RLock` across PID recovery, local candidate selection, durable compare-and-advance, conflict refresh, and local cache publication. Commit `6f9bd065` moved this whole transition under the lock after a shared-instance race. | `simplebroker/_timestamp.py`, `tests/test_timestamp_edge_cases.py`, `tests/test_core_persistence_transition_tables.py`, implementation docs, lessons |
| Retry marker | `SQLiteRunner` marks contention errors retryable and `_retry_policy.py` uses explicit markers before heuristics. | Verification only: `simplebroker/_runner.py`, `simplebroker/_retry_policy.py`, `tests/test_core_persistence_transition_tables.py`, `tests/test_retry_policy_coverage.py` |
| Fork retention | Child recovery retains inherited connections rather than closing or finalizing them in the child. | Verification only: `simplebroker/_runner.py`, `tests/test_fork_safety.py` |
| Watcher lifecycle | `_stop_lock` serializes stop callers, but `run_forever()` publishes `_run_thread` and `_running_event` outside that lock. Stop decides cleanup from two unlocked event samples. | `simplebroker/watcher.py`, `tests/test_watcher_stop_contract.py`, `tests/test_watcher_transition_tables.py`, `[SB-API-6]`, state-machine rationale |
| Runner close scope | `SQLiteRunner.close()` advances connection generation and closes its current tracked set, but a later call can create a new generation. Process-session/factory close is the terminal owner. | `simplebroker/_runner.py`, `tests/test_core_persistence_transition_tables.py`, `[SB-API-11]`, implementation docs |
| Downstream | Weft subclasses `BaseWatcher` and uses SimpleBroker watcher stop/start behavior. | `../weft/weft/core/tasks/multiqueue_watcher.py`, `../weft/tests/tasks/test_multiqueue_watcher.py` |

## Implementer Comprehension Gates

Before production edits, the implementer records answers in the Execution Log.
Any answer that differs from the expected answer is a stop-and-replan gate.

1. **Why must timestamp CAS remain inside the same-generator lock?**
   Expected: durable CAS prevents duplicate/backward durable writes, but does
   not prevent thread A from publishing an older local cache after thread B
   publishes a later value. One lock must cover candidate calculation,
   durable attempt or refresh, and cache publication.
2. **Why is `(timestamp, id)` not a correct replacement for the live peek
   offset?** Expected: exact insert can add an older ID and move preserves the
   ID. The current internal sequence also changes placement on move. Either
   key can receive a new row behind an already emitted cursor.
3. **Who cleans when stop races watcher startup?** Expected: exactly one owner
   is chosen under the lifecycle lock. A run that wins ownership cleans in its
   `finally`; a stop-before-run path cleans. A join timeout never transfers
   ownership from a live run to stop.
4. **Is `SQLiteRunner.close()` terminal?** Expected: no. It closes the tracked
   resources at its linearization point. A later operation may create a new
   connection generation. Terminal admission belongs to the process
   session/factory.
5. **Can abandoned inherited SQLite connection references be capped safely?**
   Expected: not without a proven disposal operation that cannot call the
   inherited connection finalizer. Ordinary reference dropping is unsafe in
   the forked child.

## Invariants and Constraints

1. Timestamp generation retains one lock across PID/init state, candidate
   calculation, durable compare-and-advance, conflict refresh, and cache
   publication. The CAS does not move outside or before that lock.
2. Every high-level Queue method that creates a generator solely to take one
   item closes that generator immediately on value, exhaustion, and error.
   Close failures are not silently suppressed.
3. Public return shapes, delivery guarantees, selection bounds, and generator
   first-iteration validation timing do not change.
4. Live `peek_generator()` remains offset-paged and weak under concurrent
   mutation. No cursor is described as a completeness checkpoint.
5. A watcher lifecycle permits at most one runtime cleanup owner. The
   lifecycle lock is never held during thread join, strategy close, Queue
   cleanup, callbacks, database work, or signal-context exit.
6. Stop remains idempotent and thread-safe. Stop-before-start prevents later
   run resource acquisition; stop-during-run wakes and optionally joins; join
   timeout does not cause concurrent cleanup.
7. Cleanup success detaches the finalizer. Cleanup failure preserves a safe
   retry/finalizer path and does not falsely publish resources as released.
8. This slice does not define concurrent calls to `run_forever()` on the same
   watcher. Existing behavior must not be changed intentionally; discovered
   downstream reliance is a stop-and-replan condition.
9. `PollingStrategy.notify_activity()` and counter behavior remain unchanged.
   The accepted 2.6 defect is lifecycle ownership, not proof of a strategy
   counter data race.
10. `SQLiteRunner.close()` remains reusable and closes all resources in the
    tracked snapshot it owns. Process-session/factory close remains terminal.
11. Fork recovery retains all unsafe inherited SQLite connections in the
    child. No cap, explicit close, warning, or finalizer-triggering release is
    introduced.
12. Retry classification remains explicit-marker first. No string heuristic,
    retry budget, or exception public shape changes.
13. No storage schema, persisted representation, backend API version, CLI
    flag, exit code, or public callable signature changes.

## Rollback, Rollout, and One-Way Doors

- There is no migration or one-way stored-state change. The generator cleanup,
  watcher lifecycle, documentation, and characterization-test slices can be
  reverted with their matching contract text.
- `[SB-API-6]` and the watcher implementation use promotion Strategy B: land
  the normative wording, state transition, code, and firing tests atomically.
  Do not land a stronger watcher promise before the implementation.
- `[SB-DELIVERY-4]` and `[SB-API-11]` use Strategy D clarification: existing
  behavior wins today. Land the wording with its contract/characterization
  tests and implementation rationale; there is no runtime behavior promotion.
- The Queue generator cleanup is an internal correctness fix under existing
  `[SB-API-4]`, `[SB-API-5]`, and `[SB-DELIVERY-6]`. It may land as its own
  reviewable commit before the watcher slice.
- Normal patch-release rollout is sufficient. PostgreSQL and Redis packages do
  not need a backend API bump, but their suites remain compatibility gates.
- Weft must be tested with an explicit editable overlay of this SimpleBroker
  worktree. A plain Weft `uv run` can silently test its installed pin and is
  invalid evidence.
- Post-release signals: duplicate watcher waiter/connection cleanup errors,
  watcher stop timeouts, watcher threads left alive after ordinary stop,
  SQLite connection/file-handle leaks, timestamp cache regressions, and new
  retry exhaustion. The project has no dedicated telemetry for these private
  transitions, so deterministic firing tests and downstream tests are the
  primary release evidence.
- Roll back the watcher slice if stop latency, finalizer detachment, or Weft
  subclass lifecycle regresses. Do not retain the spec promise while
  reverting its implementation.

## Proposed Spec Delta

### `[SB-DELIVERY-4]`: clarify why a cursor change is not a completeness fix

Append after the current live offset warning:

> Replacing the offset with the public message ID or the current storage
> sequence would not by itself make this traversal complete under concurrent
> mutation. Exact insertion may put an older public ID behind an advanced
> `(timestamp, id)` cursor. Move re-homes a row in place while preserving both
> its public ID and current internal sequence, so a moved-in row may also land
> behind a cursor on either ordering. Callers needing one bounded observation
> should use a materialized peek; a future exhaustive concurrent traversal
> would first need to choose and specify fixed-start, live-rescan, or snapshot
> semantics.

Add the exact contract test node to `[SB-DELIVERY-4]`'s Verification row and a
live Related Plans backlink. Do not describe `after_timestamp` or any cursor as
a durable checkpoint.

### `[SB-API-6]`: serialize watcher startup and cleanup ownership

Insert before the watch-mode delegation paragraph:

> `BaseWatcher.stop()` is thread-safe against watcher startup and active run
> cleanup. Startup and stop choose one cleanup owner at one serialized
> lifecycle transition. A stop that wins before startup prevents that later
> run from acquiring runtime resources and owns cleanup. A run that has won
> startup owns cleanup through its `finally`, including when a joining stop
> call times out. Cleanup is performed at most once for a successful lifecycle
> release; repeated stop calls remain safe. This guarantee does not make two
> concurrent `run_forever()` calls on one watcher supported.

Update `[SB-API-6]`'s Verification row with the deterministic startup/stop race
and `SM-WATCHER-LIFECYCLE` transition node. Add the live plan backlink.

### `[SB-API-11]`: state reusable runner-close scope

Append to the lifecycle-verb paragraphs:

> `SQLiteRunner.close()` closes the connections owned by the runner at that
> operation's linearization point. The runner remains reusable, and a later or
> concurrently linearized operation may acquire a new connection. Callers
> requiring terminal operation admission must close the owning process session
> or factory rather than treating runner `close()` as a permanent latch.

Update `[SB-API-11]`'s Verification row with the close/reopen transition node
and add the live plan backlink.

### Implementation and lesson alignment

- `docs/implementation/08-message-identity-and-write-visibility.md` will own
  the timestamp transition rationale and the negative checkpoint consequence.
- `docs/implementation/06-process-session-core-ownership.md` will own the
  runner/process-session close distinction.
- `docs/implementation/07-complexity-and-state-machine-map.md` will describe
  watcher cleanup ownership, timestamp full-transition locking, and reusable
  runner close. Its existing count of 74 applies to a separate seven-module
  example and reusable test-protocol slice; the three core/watcher rows in
  this plan do not change that count.
- `docs/lessons.md` will add one dated, concise correction: durable CAS and a
  local cache require the local lock to cover post-CAS cache publication. It
  will point to the implementation owner and historical fix rather than
  duplicating the full protocol.
- `CHANGELOG.md` will record the user-visible watcher cleanup race fix and the
  deterministic release of internally consumed Queue generators. Runner and
  pagination clarifications may share that entry but must not imply new
  behavior.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| Verification gate | Full Weft suite through `uv run --with-editable ../simplebroker` | Keep the focused editable-overlay proof; run the full process-spawning suite with the Weft root first and the SimpleBroker source root second on `PYTHONPATH` | The editable overlay imports local SimpleBroker but leaves Weft's namespace-package test helpers unavailable to a spawned script. The ordered source overlay imports the same local SimpleBroker while preserving the child harness. The exact failing node passed against the pin and the ordered source overlay; the complete ordered-overlay suite passed. | None; verification transport only. |

Any implementation discovery that changes a normative statement above is
recorded here before continuing. Silent drift is not permitted.

## Dependency-Ordered Tasks

### Task 0: Rebase evidence and answer the comprehension gates

1. Record HEAD, relevant spec hashes, working-tree overlaps, and the five
   comprehension answers in the Execution Log.
2. Re-run the focused existing gates before edits:

   ```bash
   uv run pytest -q \
     tests/test_timestamp_edge_cases.py::TestTimestampEdgeCases::test_shared_timestamp_generator_serializes_threads \
     tests/test_core_persistence_transition_tables.py \
     tests/test_retry_policy_coverage.py \
     tests/test_fork_safety.py \
     tests/test_watcher_stop_contract.py \
     tests/test_watcher_transition_tables.py
   ```

3. Preserve unrelated work in `docs/specs/10-cli.md`,
   `docs/specs/16-python-library-api.md`, `docs/plans/README.md`, and the
   relative-SQLite plan. If a touched hunk overlaps, rebase rather than
   overwrite.

Done when baseline behavior and ownership answers are recorded. Any failing
existing gate is diagnosed before the planned red tests are added.

### Task 1: Promote the clarification-only contract slices

1. Apply the exact `[SB-DELIVERY-4]` and `[SB-API-11]` text above.
2. Add prose-presence/semantic contract tests in the existing contract files.
   The delivery test must contain the three facts that defeat a naive cursor:
   exact older IDs, move-preserved identity/new placement, and explicit
   semantic choice before stronger traversal.
3. Add `CLOSE_REOPEN` to `SQLITE_RUNNER_TRANSITIONS`: open a real connection,
   close it, assert the old connection rejects use, acquire a new distinct
   connection from the same runner, and assert the new one works.
4. Update `SQLiteRunner.close()`'s docstring and
   `docs/implementation/06-process-session-core-ownership.md`. Do not change
   runner code, admission, generation, or fork handling.
5. Update the implementation-map machine wording for `CLOSE_REOPEN`. Preserve
   the separate 74-row example/reusable-protocol count.

Done when both clarification contracts and the real close/reopen
characterization pass with no runtime behavior change.

### Task 2: Close internally consumed Queue generators deterministically

1. Add failing tests for range-filtered high-level `Queue.read()` and
   `Queue.peek()` that wrap the instance's real generator factory only to
   retain the actual generator object. Use a real SQLite Queue; after the
   public call returns, assert `generator.gi_frame is None` immediately.
   Cover one yielded item and exhaustion. Do not replace database execution.
2. Add a private typed helper in `simplebroker/sbqueue.py` that takes one item,
   returns `None` on `StopIteration`, and always calls `_close_iterator()` in a
   `finally`. Close exceptions propagate.
3. Route the range-filtered one-item branches of `read`, `peek`, and `move`
   through the helper. Preserve every public overload and record shape.
4. Re-run consuming-generator settlement and cross-thread-finalization suites
   because helper reuse must not change transactional ownership timing.
5. Leave `Queue.stream_messages()` unchanged: its one-item branches already
   close their internally consumed generators in `finally` blocks.

Done when immediate closure is observed on success and exhaustion, all three
high-level methods share one cleanup rule, and no delivery behavior changes.

### Task 3: Fix watcher startup/stop cleanup ownership atomically

1. Before implementation, add `STOP_RACES_START` to
   `WATCHER_LIFECYCLE_TRANSITIONS` and an event-controlled regression in
   `tests/test_watcher_stop_contract.py`. The test must force stop to reach the
   old idle-cleanup decision while a run begins, block the first cleanup, and
   prove only one cleanup owner. Use events/barriers, not sleeps.
2. Add one private lifecycle state protected by `_stop_lock`. The design must
   distinguish at least: idle/admissible, run-owned, stop-owned, and released.
   Equivalent names are acceptable; separate booleans that admit impossible
   combinations are not.
3. Under the lock, startup either claims run ownership and publishes the run
   thread/running state atomically, or observes prior stop ownership and
   returns without runtime acquisition. Under the same lock, stop sets the
   stop request, snapshots the join target, and claims cleanup only if no run
   owns it.
4. Release the lock before strategy notification, join, signal-context exit,
   or `_cleanup_runtime_resources()`. After cleanup, publish released state
   and detach the finalizer only on success. On ordinary cleanup failure,
   preserve a legal retry/finalizer path consistent with current behavior.
5. Prove at minimum: stop-before-run, run-before-stop, the forced race,
   repeated concurrent stops, join timeout without ownership transfer,
   cleanup failure, synchronous `run()`, `run_in_thread()`, context manager,
   and existing activity-waiter handoff/close-once behavior.
6. Promote `[SB-API-6]` atomically with code and firing tests. Update
   `SM-WATCHER-LIFECYCLE` rationale. Do not change PollingStrategy counter
   synchronization or support concurrent double-run.

Done when the red race is green, successful runtime cleanup occurs exactly
once in every ordering, no blocking operation holds `_stop_lock`, and all
watcher lifecycle suites pass.

### Task 4: Pin timestamp full-transition serialization

1. Add `SHARED_INSTANCE_SERIALIZATION` to
   `TIMESTAMP_GENERATOR_TRANSITIONS`. Instrument the real SQLite plugin's
   `advance_last_ts` by wrapping and delegating to it. Block thread A inside
   its first durable advance, start thread B, and assert B cannot enter the
   backend advance until A is released. Then assert distinct ordered results,
   durable high-water, and local cache all equal the maximum.
2. Keep the test event-controlled. Do not patch `time.sleep`, replace the
   database, or assert a scheduler-dependent result order beyond the forced
   barrier.
3. Improve `TimestampGenerator.generate()`'s stale "lock-free" docstring and
   nearby rationale. Do not move, split, or narrow the existing `RLock`.
4. Update `docs/implementation/08-message-identity-and-write-visibility.md`,
   the `SM-TIMESTAMP-GENERATOR` row, and the dated lesson. State the historical
   stale-cache ordering explicitly.
5. Confirm state-machine policy and all three firing tables pass. Do not change
   the 74-row example/reusable-protocol count for these unrelated rows.

Done when the existing behavior is pinned by a deterministic same-instance
test and every document teaches the same whole-transition invariant.

### Task 5: Close no-action findings and cross-surface traceability

1. Run the exact retry-marker tests for 2.4. Record why no prose/code change is
   needed, including the explicit-marker-first policy.
2. Run fork-safety tests for 2.5. Record that sibling forks do not mutate the
   parent retained list and that nested-lineage growth remains an accepted
   safety tradeoff pending measured harm.
3. Update Verification rows and Related Plans links for every touched spec.
   Update implementation indexes/maps only if their ownership summaries need
   a new link; do not add redundant narration.
4. Update `CHANGELOG.md` with the actual user-visible fixes and no stronger
   claim. Update this plan's Deviation, Execution, Revision, and Review logs.
5. Run the explicit Weft overlay import proof and focused watcher suite:

   ```bash
   (cd ../weft && uv run --with-editable ../simplebroker python -c \
     'from pathlib import Path; import simplebroker; assert Path(simplebroker.__file__).resolve().is_relative_to(Path("../simplebroker").resolve())')
   (cd ../weft && uv run --with-editable ../simplebroker pytest -q \
     tests/tasks/test_multiqueue_watcher.py)
   ```

Done when all seven review items have a durable disposition, the downstream
gate proves it imported this worktree, and no rejected remedy entered code.

For the full Weft suite, preserve Weft's root before the SimpleBroker worktree
on the child-process import path:

```bash
(cd ../weft && PYTHONPATH="$PWD:$PWD/../simplebroker" uv run python -c \
  'from pathlib import Path; import simplebroker; assert Path(simplebroker.__file__).resolve().is_relative_to(Path("../simplebroker").resolve())')
(cd ../weft && PYTHONPATH="$PWD:$PWD/../simplebroker" uv run pytest -q)
```

### Task 6: Independent completed-work review and closure

1. Give a different-family reviewer the full plan, governing theory/specs,
   final diff, test evidence, and these adversarial questions:
   - Can any stop/start ordering still produce zero or two cleanup owners?
   - Does cleanup failure falsely publish release or disable finalizer retry?
   - Did generator cleanup alter first-iteration validation or settlement?
   - Does any new text imply cursor completeness or terminal runner close?
   - Is the timestamp CAS and cache publication still one locked transition?
2. Resolve each finding in the Review Log. Re-review changed high-risk hunks.
3. Run final full gates from the closing tree. Close the Status Index row only
   when implementation evidence, docs, review, and required commit evidence
   satisfy the repository Definition of Done.

## Testing Plan

### Test design

- Concurrency tests use `threading.Event`, `Barrier`, or an equivalent exact
  handoff. Bare sleeps are not synchronization and are forbidden in the new
  race proofs.
- SQLite remains real. Tests may wrap a production method to pause and observe
  it, but the wrapper delegates to the real plugin, generator, connection, or
  cleanup path.
- Generator cleanup tests capture the real generator object. They do not mock
  `next()`, `close()`, database execution, transaction settlement, or the
  public Queue result.
- Watcher tests retain a real Queue and watcher lifecycle. A counted strategy
  or cleanup wrapper may observe ownership, but it may not replace the
  stop/run transition being proved.
- Timestamp tests use one actual `TimestampGenerator` and real SQLite durable
  high-water. The seam wrapper observes backend entry and delegates.
- Runner close/reopen uses actual `sqlite3.Connection` instances and proves the
  old generation is closed while a later generation is usable.
- Every new transition row must fire through the executable-table decorator;
  no descriptive-only row is accepted.

### Focused gates during implementation

```bash
uv run pytest -q \
  tests/test_queue_api_comprehensive.py \
  tests/test_generator_methods.py \
  tests/test_delivery_contract_sb_delivery.py \
  tests/test_timestamp_edge_cases.py \
  tests/test_core_persistence_transition_tables.py \
  tests/test_retry_policy_coverage.py \
  tests/test_fork_safety.py \
  tests/test_watcher_stop_contract.py \
  tests/test_watcher_transition_tables.py

uv run pytest -q \
  tests/test_cross_thread_finalization_poisoning.py \
  tests/test_cross_thread_generator_probe.py \
  tests/test_watcher.py \
  tests/test_watcher_edge_cases.py \
  tests/test_watcher_race_conditions.py
```

If the range-filter tests live in a more specific existing module after the
initial file inventory, replace `tests/test_queue_api_comprehensive.py` with
that named owner and record the choice in the Execution Log.

## Verification and Gates

### Changed-surface gates

```bash
uv run ruff check simplebroker tests
uv run ruff format --check simplebroker tests
uv run mypy simplebroker bin/release.py bin/ruff_suppression_index.py \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
python3 bin/check-dom15-fixtures
bin/check-plan-context
python3 bin/check-doc-paths
git diff --check
```

### Full product and backend gates

```bash
uv run pytest -q
uv run ./bin/pytest-pg -q extensions/simplebroker_pg/tests
uv run ./bin/pytest-redis -q extensions/simplebroker_redis/tests
(cd ../weft && uv run --with-editable ../simplebroker pytest -q \
  tests/tasks/test_multiqueue_watcher.py)
```

Run the full Weft suite with the ordered source overlay if the focused watcher
test exposes a downstream assumption, if any production watcher method
signature changes despite the plan, or before claiming a release ready to
land. The Weft root must remain first so its spawned test scripts can import
the repository's namespace-package helpers:

```bash
(cd ../weft && PYTHONPATH="$PWD:$PWD/../simplebroker" uv run pytest -q)
```

### Acceptance matrix

| Concern | Required evidence |
|---------|-------------------|
| 2.1 cursor rejection | Exact `[SB-DELIVERY-4]` contract test; no pagination code diff; materialized alternative remains documented |
| 2.2 generator ownership | Real generator `gi_frame is None` immediately after range-filtered read/peek success and exhaustion; move remains green |
| 2.3 timestamp race | Event-controlled shared-instance transition proves B cannot enter durable advance while A owns the transition; cache and durable max agree |
| 2.4 retry marker | Existing admission error carries `retryable=True`; explicit marker wins policy classification |
| 2.5 fork retention | Real fork suite green; no retention-cap code diff |
| 2.6 watcher cleanup | Forced start/stop race and transition table prove one cleanup owner; join timeout, failure, finalizer, and waiter close-once tests green |
| 2.7 runner close | Exact spec/docstring text plus real `CLOSE_REOPEN` row; no `_closed` latch or admission rejection |
| Public compatibility | Full core, PostgreSQL, Redis, and explicit-overlay Weft gates |

## Stop-and-Replan Gates

Stop implementation and return to design review if any of these occur:

- A correct 2.1 repair requires a new traversal mode, persisted placement ID,
  snapshot transaction, unbounded dedup set, or public checkpoint guarantee.
- The watcher state cannot preserve finalizer retry after cleanup failure
  without changing public failure semantics.
- A downstream caller relies on concurrent `run_forever()` calls on one
  watcher or on `stop()` cleaning while a live run remains active.
- Deterministic proof shows the PollingStrategy counter itself causes a lost
  wakeup or unsafe cleanup. That is a separate accepted defect only after such
  evidence.
- `SQLiteRunner.close()` is used as terminal admission by first-party or Weft
  code. Ownership must then be reconciled before clarifying the opposite.
- Fork testing proves parent-side retained-list growth across sibling children,
  or safe bounded disposal requires a new cross-fork lifecycle primitive.
- A touched public signature, backend API version, persisted schema, or
  delivery guarantee changes.

## Independent Review Loop

The plan receives a different-family read-only review before owner approval.
The reviewer must inspect cited files and return `PASS` or `BLOCKED`, with
findings ordered by severity and exact file/section references. Required
questions:

1. Does the plan steelman and correctly reject the keyset-cursor, CAS move,
   fork cap, and terminal runner remedies?
2. Is the watcher lifecycle state sufficient for every startup/stop/failure
   ordering without holding the lock across blocking cleanup?
3. Do the tests observe real owners rather than mocks?
4. Are the exact spec deltas compatible with current behavior and program
   theory?
5. Are rollback, downstream, transition-count, and no-action gates complete?

Implementation begins only after blocking plan findings are resolved and the
owner approves promotion. A separate completed-work review is required by
Task 6.

## Out of Scope

- Replacing offset pagination or adding a snapshot/exhaustive peek API.
- Treating public message ID, `after_timestamp`, high-water, or current row
  sequence as a durable consumption cursor.
- Moving timestamp CAS outside the generator lock, shortening its critical
  section, changing retry timing, or changing timestamp encoding.
- Adding a retained-fork-reference cap, warning, explicit inherited close, or
  weak-reference disposal.
- Locking PollingStrategy counters or redesigning watcher notification without
  a deterministic lost-wakeup defect.
- Supporting concurrent runs of one watcher instance.
- Making `SQLiteRunner.close()` terminal or adding a public runner reopen API.
- Changing backend API version, public method signatures, delivery modes,
  schema, CLI behavior, or exit codes.
- Refactoring unrelated watcher, Queue, timestamp, runner, or retry code.

## Fresh-Eyes Review Checklist

- Can a reader explain why both a timestamp cursor and current row sequence
  can miss a moved or exact-inserted message behind the cursor?
- Does every internally created one-item generator have an explicit owner and
  immediate close path?
- Is the timestamp stale-cache race understandable without reading commit
  history?
- Is exactly one watcher cleanup owner chosen before any join or cleanup?
- Does cleanup failure retain truthful state and a safe retry/finalizer path?
- Is runner close clearly resource-scoped rather than terminal?
- Are 2.4 and 2.5 closed with evidence rather than silently omitted?
- Do all new tests cross a real persistence or lifecycle boundary?
- Is every normative text change mapped to a firing gate and Related Plans
  link?
- Did the implementation preserve unrelated dirty work and explicit Weft
  overlay evidence?

## Execution Log

| Date | Stage | Evidence / result |
|------|-------|-------------------|
| 2026-08-23 | Investigation | Reproduced the timestamp stale-cache ordering that existed before `6f9bd065`: one thread durably advanced and paused before local publication; a second thread advanced later; the first then overwrote the shared cache with the older value. Current full-transition locking prevents it. |
| 2026-08-23 | Investigation | Reproduced the watcher startup/stop cleanup race with controlled thread ordering: stop sampled not-running, run published afterward, and `_cleanup_runtime_resources()` was reached twice. |
| 2026-08-23 | Review disposition | Confirmed retryable admission marking is already explicit; confirmed sibling fork recovery cannot grow the parent's copy-on-write retained list; confirmed `SQLiteRunner.close()` is reusable by current generation-based code. |
| 2026-08-23 | Authoring baseline | Plan written against `d63e65523103229066de7531cb3b1183cd0f45c4`; separately owned relative-SQLite spec/index edits were present and excluded from this plan's ownership. |
| 2026-08-23 | Owner approval | User requested implementation of the independently reviewed plan. Status moved to active; no contract or scope deviation requested. |
| 2026-08-23 | Comprehension gates | (1) CAS alone protects durable monotonicity, while the same-instance lock must also cover candidate selection, conflict refresh, and cache publication to prevent stale local publication. (2) Exact insert defeats a timestamp cursor; in-place ID-preserving move defeats both public-ID and current internal-sequence cursors. (3) Run or stop-before-run must claim the sole cleanup right under the lifecycle lock; join timeout never transfers it. (4) `SQLiteRunner.close()` releases the current tracked generation and remains reusable; the process session/factory owns terminal admission. (5) Dropping inherited SQLite references can run unsafe child-side finalization, so the fork-retention list cannot be capped without a proven close-free disposal mechanism. All answers match the plan's expected invariants. |
| 2026-08-23 | Implementation rebase | Implementation continued on `00fb9f77baa85c82887d6d3bea9b5526ce7c3951` after the separately owned relative-SQLite plan landed. Rebased source hashes: delivery `05d97e2960dd5d480b84bb5866062a282549f9500dfc02f2764991b711fb94f4`; identity `ac233eb5c0accc285ebb94ebac835707e20fcbecfe912aaf68c29c02439a38b3`; Python API `dd0c074ef844c837bbfecb3e1c8bff22b81e0ac82deabcc0e21921352e6a1b02`; program theory `6a830707ff45ef6826d5e2353b1522ed7044a68d050940ecb353f5c1ab824b14`. The only governing-spec delta from the authoring baseline was `[SB-API-2]` and its verification/backlink; `[SB-API-6]`, `[SB-API-11]`, delivery, identity, and program theory were semantically unchanged. |
| 2026-08-23 | Tasks 1 and 2 | Promoted the delivery and runner-close clarifications without pagination or runner admission code changes. Added a real `CLOSE_REOPEN` row. Added failing then passing real-SQLite generator-frame tests for range-filtered read/peek success and exhaustion; read, peek, and move now share one typed next-and-close helper, with close errors left visible. |
| 2026-08-23 | Tasks 3 and 4 | The event-controlled watcher regression failed on the old code with two cleanup calls, then passed with one lock-protected idle/run/stop/released owner transition. Join-timeout and cleanup-retry cases pass. The timestamp mutation that narrowed the shared-instance lock failed the new `SHARED_INSTANCE_SERIALIZATION` row; restored production locking passes with durable and cached maxima equal. Production timestamp lock placement did not change. |
| 2026-08-23 | No-action findings | `tests/test_retry_policy_coverage.py`, the core persistence transition table, and `tests/test_fork_safety.py` passed. No retry-classification or fork-retention production/doc changes were made for 2.4 or 2.5. Explicit marker-first retry and close-free inherited-reference retention remain the owners. |
| 2026-08-23 | Downstream compatibility correction | An initial watcher implementation added `_lifecycle_state`; Weft's fail-closed worker snapshot correctly rejected the new shared instance field. The same four-state owner is now encoded in the existing private `_run_thread` slot, with its historical `None` treated as idle for pinned clones. Focused failed nodes, the watcher suite, and the complete Weft suite pass. No Weft source was changed. |
| 2026-08-23 | Verification transport | `uv run --with-editable ../simplebroker` proved the focused Weft watcher suite imported this worktree. In the full process-spawning suite it made Weft's namespace-package `tests.helpers` unavailable to one child script; the same node passed against the installed pin. Ordering the Weft root before the SimpleBroker source root on `PYTHONPATH` made the child import and local SimpleBroker import both explicit; the exact node and complete suite then passed. |
| 2026-08-23 | Final gates before completed-work review | `uv run pytest -q` passed with 17 expected platform/opt-in skips. Ruff check and format, mypy over production and extensions, DOM-15 fixtures, plan context, doc paths, and `git diff --check` passed. Live PostgreSQL and Redis extension suites passed with five and one opt-in diagnostic skips. The complete ordered-source-overlay Weft suite passed with two expected backend-specific skips. |
| 2026-08-23 | Completed-work review | Different-family read-only review returned PASS with no blocking finding. It traced every watcher ordering to one cleanup owner, confirmed failure returns to retryable idle without detaching the finalizer, confirmed `_run_thread` slot reuse and historical `None` compatibility, and found the generator, timestamp, runner, pagination, no-action, and documentation slices aligned. No post-review production change was required. |
| 2026-08-23 | Closure | Owner authorized a targeted commit. The plan and Status Index close atomically with the reviewed implementation and evidence; post-commit verification uses `git log` rather than embedding a self-referential hash in this commit. |

## Review Log

| Date | Reviewer | Scope | Finding | Disposition |
|------|----------|-------|---------|-------------|
| 2026-08-23 | Claude, different-family plan review | Full draft, cited theory/specs/code/tests, exact deltas | PASS; no blocker. F1: the plan incorrectly assumed the three core/watcher rows belonged to the separately scoped 74-row example/protocol count. F2: cursor prose did not say which mutation defeats which ordering. F3: "serialization gate" could imply production code. F4: `stream_messages()` already closes its one-item generators. | Accepted all four: preserved the separate count at 74; attributed older timestamp ordering to exact insert and internal-sequence failure to in-place move; renamed the timestamp action a firing test; excluded already-compliant `stream_messages()` explicitly. |
| 2026-08-23 | Claude, different-family completed-work review | Full plan, governing theory/specs/implementation docs, final diff, production/tests, and verification evidence | PASS; no blocking finding. Residual notes: the worktree and Status Index remain uncommitted/active; the reviewer relied on the recorded full-Weft run; concurrent double-run remains unsupported and now returns early; close errors can mask a retrieved value by design; finalizer thread-local cleanup remains the pre-existing idempotent sweep. | Accepted. The primary agent directly observed the complete ordered-overlay Weft pass. Double-run is explicitly out of scope, close-error propagation is invariant 2, and finalizer thread-local behavior did not change. Per repository policy, no agent-authored commit is created merely to close the plan; the active row remains truthful pending owner direction. |

## Revision Log

| Date | Revision | Reason |
|------|----------|--------|
| 2026-08-23 | Initial draft | Convert Factor 2 discussion into a Class 5 hardening plan with explicit accepted, clarification-only, and no-action outcomes. |
| 2026-08-23 | Independent-review revision | Apply the review's four precision improvements; verdict PASS with no blocking findings. |
| 2026-08-23 | Implementation evidence | Record red/green ownership probes, no-action gates, downstream field-shape correction, and full verification. |
| 2026-08-23 | Full-suite overlay correction | Preserve Weft's repository root before the SimpleBroker source root for spawned test scripts; retain the focused editable-overlay import proof. |
| 2026-08-23 | Completed-work review | Record different-family PASS and dispose of every residual note without changing production code. |
| 2026-08-23 | Closure | Mark the Class 5 plan and Status Index completed for the owner-authorized targeted commit. |
