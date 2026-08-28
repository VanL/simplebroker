# All Examples Correctness and Contract Alignment Plan

Status: active
Class: 4. This changes published runnable examples and their failure semantics,
including async startup, concurrency, destructive streaming, restart behavior,
and shell mutation paths. It does not revise the SimpleBroker product contract.
`[DOM-5]`, `[DOM-11]`, and `[DOM-15]` require the hardening, behavioral proof,
documentation alignment, and independent review below.
Plan type: implementation against existing specifications; no spec revision

## Goal and Boundary

Bring every file under `examples/` into one honest, tested teaching set for the
SimpleBroker 8 contract. Recommended examples must use public APIs, advanced
examples must state and enforce their narrower support boundary, and no example
may promise processing, retry, restart, ordering, or backup guarantees that its
code does not provide.

This plan covers all 24 files currently under `examples/`, plus the repository
entry points and tests that advertise or execute them. A file may be rewritten,
repaired, or explicitly revalidated. “All examples” does not mean making every
example demonstrate every feature. In particular, generators, watchers, and the
advanced pooled async stream remain ascending-only because `[SB-SELECT-5]` does
not give live traversal a newest-order control.

The plan is a release-readiness companion to
`2026-08-27-message-id-order-and-newest-selection-plan.md`. It depends on that
plan's green, committed SimpleBroker 8 behavior baseline. It must be complete
before the repository is described as ready to publish SimpleBroker 8, but it
does not own package publication or downstream Weft/Taut adoption.

Proportionality guardrails (owner-directed, 2026-08-27): examples are teaching
material, and some remain intentionally non-optimal where the simplification
is honest and labeled. Repairs prefer deletions and clearer forms over added
machinery. Existing example test infrastructure — the reactor and pattern
transition suites, the pooled-async tests, and the stream-transition, shell,
and worker suites — is updated and kept; net-new test infrastructure is added
only where it is a genuine win over those suites, and no harness may outweigh
the lesson its example teaches.

## Product Fit

`docs/program-theory.md` [THEORY-1] treats predictability as part of the small
broker product. Examples that silently consume work, miss valid late lower IDs,
or use a second schema migrator make the product harder to reason about even if
the core is correct. [THEORY-3] keeps storage setup and migration in the
canonical backend implementation. [THEORY-4] requires examples to expose
failure boundaries rather than turn them into implied guarantees.

The repair therefore favors fewer concepts:

- one canonical synchronous setup path for the advanced async SQLite example;
- one public message-ID order contract, including one bounded newest example;
- one explicit distinction between broker delivery and application processing;
- one supported dump/load path for portable pending-message export;
- one durable reactor completion ledger (the existing `reactor_seen` table)
  instead of treating a high-water ID as a complete stream offset.

## Decisions Locked by This Plan

1. `examples/example_extension_implementation.md` is rewritten at the same path
   as a short extension guide and tested-source map. It contains no copied
   implementation and no custom DDL. The tested source examples remain the
   authority.
2. `examples/async_pooled_broker.py` owns no schema DDL, migration ladder, or
   independent startup lock. Before opening its async pool, it invokes the
   public `open_broker(...)` setup path in `asyncio.to_thread`; canonical setup
   therefore owns PhaseLock, admission, migration, and caller-sidecar
   preservation. It takes one `snapshot_config(...)` result at async context
   entry and uses that exact immutable snapshot for both canonical setup and
   async runtime. The async implementation may perform read-only compatibility
   checks after setup but may not repair schema.
3. Pooled async ID allocation and insertion occur in one `BEGIN IMMEDIATE`
   transaction. Batched claim results are normalized by integer public message
   ID before exposure, independent of SQLite `RETURNING` order. The advanced
   pooled API remains an oldest-only subset and does not gain `order="newest"`.
4. `AsyncBroker.pop()` and `AsyncBroker.peek()` in `async_wrapper.py` gain the
   keyword-only `order: str = "oldest"` pass-through. Its async stream consumes
   at most one message for each active `__anext__` attempt. The hidden
   destructive prefetch batch and `batch_size` control are removed. Cancellation
   while that one synchronous read is running can still leave that row claimed
   before it is yielded, as permitted by [SB-DELIVERY-1]; it cannot claim a
   hidden remainder of a batch.
5. `python_api.py` removes the false resumable-checkpoint example. It adds a
   bounded public API demonstration for default oldest and explicit newest
   selection, using out-of-order exact IDs so the example proves ID order
   rather than coincidental insertion order. It does not add newest to a
   generator or watcher.
6. `reference_reactor.py` stops using its maximum checkpoint as a permanent
   `after_timestamp` filter. Each discovery pass starts at the lowest pending
   public ID and uses a pass-local bound only to page. It skips live in-process
   work and durable terminal `reactor_seen` rows. A stale persisted `inflight`
   row is eligible for at-least-once redispatch after restart.
7. Reactor input terminal states are `result_recorded` and `output_written`;
   the durable output backlog, not input redispatch, owns completion after a
   result is recorded. Control completion is recorded as `control_processed`
   in `reactor_seen`. Terminal states are absorbing: dispatch admission uses a
   conditional sidecar write and enqueues work only when that write proves it
   did not race with terminal completion. The numeric checkpoint remains an
   informational high-water value for status and audit only.
8. Exact-ID output replay treats an existing row as idempotent only when its
   body equals the pending payload. A different body at the reserved ID is a
   terminal collision for that drive turn and leaves the sidecar result
   pending for diagnosis or explicit repair.
9. `MultiQueueWatcher` accepts public target types only:
   `BrokerTarget | str | Path | None`. It does not accept or import private
   `BrokerDB`, does not stringify a `BrokerTarget`, and obtains the shared
   target from the initial Queue's public `db_target` property.
10. Recommended examples import supported extension symbols from
    `simplebroker.ext` and define ordinary application handlers locally. They
    do not teach imports from `simplebroker.watcher` or `simplebroker.db` as
    public extension points.
11. Multi-queue and async documentation says plainly that consuming read/watch
    claims before handler or coroutine processing. Handler failure does not
    restore the message. Examples that need retry point to move-to-inflight or
    an application-owned durable retry design.
12. Shell examples distinguish CLI exit `2` (empty/no match) from operational
    failure, obtain counts from validated JSON stats, and never report a
    copy/transform as successful when source deletion failed. Delete-after-write
    failure is a fatal duplicate-risk outcome.
13. The migration menu uses `broker rename` for rename semantics. Its portable
    export uses `broker dump --include ...` and `broker load`, preserves message
    IDs, and is labeled pending-only export/import rather than a full live
    backup. Documented bound strings are passed through unchanged; native
    19-digit IDs are never rewritten as Unix seconds.
14. Tests prove behavior through public outputs and durable state. They do not
    prove correctness by matching source text, assuming native engine order, or
    encoding raw tuple field positions. A row-order perturbation adapter treats
    returned rows as opaque records and reverses only their sequence. Cheap
    documentation-drift text checks on README and guide prose remain
    acceptable; the prohibition is on proving code behavior from source or
    prose text.
15. Existing sidecars remain application-owned. In particular,
    `reference_reactor.py` keeps its `reactor_*` tables and its audit
    `event_id`; no example creates a foreign key to the removed private broker
    surrogate.

## Alternatives Considered

- **Patch every code block in the extension guide.** Rejected. It duplicates
  more than one thousand lines of implementation, has already drifted into a
  retired schema, and cannot be exercised as one coherent program. A short map
  to tested source files has a stronger maintenance boundary.
- **Keep async example migrations and add the canonical lock.** Rejected. That
  would still leave a second migration implementation and a second proof
  burden. Serialization alone does not make duplicated DDL correct.
- **Repair checkpoints by adding `after_timestamp=checkpoint`.** Rejected.
  `[SB-SELECT-2]` and `[SB-SELECT-3]` state that a bound is a filter, not a
  complete offset. Exact insert, load, and ID-preserving move can add a lower ID
  later.
- **Keep async prefetch and document possible loss.** Rejected for the
  recommended wrapper. A destructive hidden batch is surprising and provides
  no correctness gain. The advanced pooled example remains the place to discuss
  materialized batch tradeoffs.
- **Make every example demonstrate newest selection.** Rejected. One primary
  Python example and the wrapper pass-through establish discoverability. Adding
  reverse controls to live or deliberately narrow examples would contradict
  their contract boundary.
- **Treat the shell queue copy as a full backup.** Rejected. The supported dump
  format is pending-only and cannot promise live claimed-state recovery.

## Source Documents

Theory and process:

- `docs/program-theory.md` [THEORY-1], [THEORY-3], [THEORY-4]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-10], [DOM-11], [DOM-15]
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`

Winning product contracts:

- `docs/specs/10-cli.md` [SB-CLI-1], [SB-CLI-4], [SB-CLI-5], [SB-CLI-6]
- `docs/specs/11-delivery.md` [SB-DELIVERY-1] through [SB-DELIVERY-4]
- `docs/specs/13-message-identity.md` [SB-ID-1], [SB-ID-4], [SB-ID-5]
- `docs/specs/14-timestamp-selection.md` [SB-SELECT-1] through [SB-SELECT-5]
- `docs/specs/15-persistence-io.md` [SB-IO-1] through [SB-IO-4]
- `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-4], [SB-API-6],
  [SB-API-7], [SB-API-11]
- `docs/specs/17-ops.md` [SB-OPS-1] through [SB-OPS-4]
- `docs/specs/product-section-registry.md`

Implementation and entry points:

- `docs/implementation/07-complexity-and-state-machine-map.md`
- `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
- `README.md`, `docs/agent-kernel.md`, `docs/guides/python.md`
- `CHANGELOG.md`
- `docs/plans/2026-08-27-message-id-order-and-newest-selection-plan.md`

## Spec Baseline and Dependency Gate

The authoring baseline is commit
`8d109ad48625c03630a766f5ecbafd8eaa2c951e`. The active message-ID/newest
plan has additional review corrections in progress at authoring time. Do not
start example implementation against a mixed worktree.

Task 0 must record a later green committed baseline that contains the final
SimpleBroker 8 public signatures, schema-v6 setup, and [SB-SELECT-5] behavior.
That commit, not the authoring worktree, becomes this plan's implementation
baseline. If the active plan changes any cited contract after Task 0, pause and
reconcile this plan before continuing.

## Proposed Spec and README Delta

**Canonical spec change: none.** The defects are in examples and their prose,
not in the winning product contract. The implementation adds related-plan
backlinks where required for traceability but does not change normative text or
claim new product behavior.

README and guide work is required:

- `examples/README.md`: replace “production-ready” and broad robustness claims
  with a per-example support level; remove the checkpoint claim; describe
  newest selection, consume-before-processing, pending-only export, and the
  advanced async oldest-only boundary; retain the security guidance.
- `examples/ASYNC_README.md`: remove the “asyncio is single-process” /
  no-fork-safety claim; add one sentence to the error-handling snippets making
  explicit that the original message is already consumed and the DLQ/RETRY
  writes are new messages (verification found those snippets honestly labeled
  application code, not false automatic-retry claims); explain canonical
  synchronous setup, oldest-only live traversal, claim-before-processing,
  cancellation, and the advanced/internal compatibility boundary.
- `examples/MULTI_QUEUE_README.md`: remove the false failed-message retry,
  O(1)-memory, and “no locks/races/deadlocks” claims; document public target
  types, round-robin scheduling, consume failure state, retained-message scan
  cost in the reactor, and restart redispatch.
- `example_extension_implementation.md`: become a short guide that classifies
  `logging_runner.py`, `async_wrapper.py`, and `async_pooled_broker.py`, with
  links to their executable tests and no custom DDL.
- `docs/guides/python.md` and `docs/agent-kernel.md`: revalidate every example
  link and update only stale async, watcher, or reactor claims. Root `README.md`
  keeps its examples catalog and worker links unless inspection finds a stale
  restatement.
- `CHANGELOG.md`: add one release-readiness bullet for repaired examples. It
  must distinguish documentation/example guarantees from core product changes.

## Complete Example Inventory and Disposition

| File | Disposition | Required outcome |
|------|-------------|------------------|
| `examples/README.md` | Rewrite catalog claims | Honest support level and behavior for every example family. |
| `examples/ASYNC_README.md` | Repair | Canonical setup, oldest-only pooled boundary, and claim/cancellation semantics. |
| `examples/MULTI_QUEUE_README.md` | Repair | Public targets, honest consume failure semantics, reactor scan/retention costs. |
| `examples/example_extension_implementation.md` | Replace in place | Short tested-source guide; no copied code or DDL. |
| `examples/python_api.py` | Repair and extend | Remove false checkpoint; add bounded oldest/newest public example. |
| `examples/async_wrapper.py` | Repair and extend | `order` pass-through on pop/peek; one destructive read per active iteration with no batch prefetch; local handler. |
| `examples/async_pooled_broker.py` | Architectural repair | Canonical setup, atomic allocate+insert, sorted returned batches, no DDL. |
| `examples/async_simple_example.py` | Align and smoke-test | Match the pooled subset and state consume-before-processing. |
| `examples/logging_runner.py` | Revalidate | Keep focused SQLRunner wrapping example; execute a real smoke scenario. |
| `examples/simple_watcher_example.py` | Rewrite handlers/imports | Local print/JSON/log handlers plus public root/ext imports and explicit consume semantics. |
| `examples/multi_queue_watcher.py` | Repair | Public targets, shared target identity, no private imports, honest handler failure. |
| `examples/multi_queue_patterns.py` | Align and revalidate | Patterns state claim-before-handler and use the repaired watcher surface. |
| `examples/reference_reactor.py` | Correctness repair | Ledger-based complete discovery, control seen state, exact replay body check. |
| `examples/sqlite_connect.py` | Revalidate | Preserve standalone scope; run its existing full tests and link it honestly. |
| `examples/safe_worker.sh` | Revalidate | No redesign unless black-box regression fails. |
| `examples/resilient_worker.sh` | Revalidate | Preserve the current informational-checkpoint design. |
| `examples/dead_letter_queue.sh` | Repair | Empty status handling, validated counts, fatal delete-after-write failure. |
| `examples/queue_migration.sh` | Repair | Real rename, correct bounds, JSON stats, fail-closed transforms, dump/load export. |
| `examples/work_stealing.sh` | Repair | JSON stats, empty status handling, operational failures never become zero load. |
| `examples/test_async_pooled_broker.py` | Extend behaviorally | Startup, concurrency, sidecar, and order perturbation proof. |
| `examples/test_sqlite_connect.py` | Revalidate | Keep comprehensive utility coverage green. |
| `examples/tests/test_multi_queue_pattern_transitions.py` | Extend behaviorally | Public targets and consume failure state; no private-layout assertions. |
| `examples/tests/test_reference_reactor.py` | Extend behaviorally | Late-lower input/control, restart, matching replay, mismatch collision. |
| `examples/tests/test_reference_reactor_transitions.py` | Extend transition table | Terminal/stale seen states and output-pending ownership. |

External example gates to update or extend:

- `tests/test_example_async_stream_transitions.py`
- `tests/test_multi_queue_watcher_example.py`
- `tests/test_sql_builder_validity.py`
- `tests/test_sqlite_connect_example.py`
- `tests/test_worker_examples.py`
- `tests/test_shell_examples.py`
- new focused behavior tests under `examples/tests/` for `python_api.py` and
  `async_wrapper.py`; add another file only when it owns a distinct executable
  scenario rather than source-shape assertions.

## Invariants and Hidden Couplings

1. Examples never create, migrate, validate, address, or order by the retired
   private `messages.id` / `order_id` surrogate.
2. The advanced async example's first open and concurrent opens use production
   setup. No example-local `asyncio.Lock` is represented as cross-process
   startup serialization.
3. Generated IDs remain monotone in commit order because generation and insert
   share one write transaction. Tests must force the old interleaving rather
   than rely on timing.
4. Public result order is derived from integer message IDs after engine output.
   Tests perturb row sequence without assuming or asserting tuple field order.
5. The recommended async stream starts no more than one destructive read for
   each active `__anext__` attempt. Breaking after a yielded item cannot consume
   later rows. Cancelling an in-flight executor read may leave that one current
   row claimed before yield, but cannot consume a hidden batch.
6. A reactor high-water checkpoint cannot hide a still-pending lower ID. Local
   page bounds expire at the end of each scan. Durable terminal seen state,
   not numeric order, prevents duplicate completed work.
7. A crashed process can leave `reactor_seen.status='inflight'`; restart treats
   it as redispatchable. `result_recorded` is terminal for input work even while
   its output remains pending. A stale dispatcher cannot overwrite a terminal
   state with `inflight`.
8. Reactor output replay compares identity and content. An ID collision with a
   different payload never advances `output_written`.
9. Caller sidecars are left alone by canonical setup and by every example
   repair. The reactor may evolve only its own `reactor_*` sidecars.
10. Queue targets retain structured `BrokerTarget` identity and config. No
    `str(target)` conversion may redirect a queue to a display string.
11. Handler continuation means only that the watcher continues. It does not
    mean the failed consumed message becomes pending or is retried.
12. Shell exit `2` is handled only where the invoked command documents
    empty/no-match. Exit `1`, parse failure, malformed JSON, or a missing field
    is fatal.
13. A replacement write followed by failed source delete is ambiguous and may
    duplicate. The script reports that state and stops; it does not increment a
    success counter or continue over the same source row.
14. Dump/load examples preserve pending message IDs and disclose that claimed
    rows and live application sidecars are outside the portable dump.
15. Examples use temporary targets by default. Verification must not create an
    untracked `.broker.db`, `broker.db`, or display-string-named database in the
    repository.

Hidden couplings to preserve during implementation:

- `open_broker(...)` performs setup while constructing its owned connection;
  entering and exiting it in one worker call is the async example's setup seam.
  Cancelling the awaiting coroutine does not stop that worker thread, so the
  wrapper must await setup cleanup before propagating cancellation and must not
  open the async pool afterward.
- Configuration is sampled once. Canonical setup and the async pool must not
  resample ambient configuration or use different targets/options.
- Queue setup can migrate schema v5 to v6. The async/sync startup race must use
  real setup and the canonical PhaseLock path, not a mocked lock assertion.
- `Queue.read_many()` materializes claims before return; the recommended async
  stream therefore cannot regain per-yield safety by wrapping that batch API.
- MultiQueueWatcher subclasses `BaseWatcher`, whose handler error continuation
  and lifecycle rules remain [SB-API-6]. The example may not redefine those
  words casually.
- The reactor retains source rows by peeking. Complete discovery will revisit
  them, so bounded paging, one set-oriented durable terminal lookup per page,
  and documented retention or compaction are required to control scan cost.
- The reactor preserves one live in-flight item per input queue. Complete
  discovery changes which row is eligible, not that queue-level concurrency
  limit.
- Shell menus run under `set -e`; command substitutions need explicit status
  capture or normal empty results exit before their branch executes.

## Comprehension Gates

Before implementation, the implementer and first reviewer record answers in
Execution Evidence. A materially different answer blocks work until this plan
or the cited owner is corrected.

1. **Why is a saved maximum message ID not a complete resume offset?**
   Expected: exact insertion, load, and ID-preserving move can add a lower ID
   later. `after_timestamp` would filter that pending row forever.
2. **Who owns SQLite schema setup for the advanced async example?**
   Expected: the production public open path and its SQLite runner own PhaseLock,
   admission, migration, and proof. The async example waits for it in a worker
   thread and owns no DDL.
3. **What does a consume handler failure guarantee?**
   Expected: the claim or move committed before dispatch. Handler failure does
   not restore pending state; an error handler can continue the watcher only.
4. **Why is reversing opaque returned records a valid order test?**
   Expected: it proves the example normalizes engine output before exposing
   behavior, without asserting a database's native order or the positional
   layout of a raw row.

## Rollout, Rollback, and Success Signals

Examples ship as one repository documentation/code slice after the green
SimpleBroker 8 baseline. There is no new product schema version and no example
is authorized to migrate a target except by invoking canonical setup. Run every
demonstration only against disposable targets during verification.

Implementation slices may be reverted independently before release, except
that docs describing a repaired behavior must revert with its code and tests.
If canonical async setup exposes an incompatibility that cannot be resolved
without product changes, stop this plan and return the issue to the active
message-ID/newest plan. Do not restore local DDL as a fallback.

Post-change success signals:

- a clean examples test run creates no repository-local broker database;
- an async/sync concurrent first open performs one canonical migration and
  preserves an unrelated caller sidecar;
- forced write and `RETURNING` interleavings cannot violate public ID order;
- early break from the recommended async stream leaves later rows pending;
  cancellation during the one in-flight read can claim only that current row,
  with the claim-before-yield window documented;
- a lower exact ID inserted after a reactor high-water value is eventually
  processed on both input and control lanes;
- mismatched exact output replay remains pending and visible as an error;
- shell empty results follow their idle/completion branch, while broker errors
  remain nonzero and delete-after-write failure stops with duplicate-risk text;
- every documented command and import runs from a clean source checkout with
  only its stated optional dependencies.

## Work Plan

### Task 0: Freeze the v8 baseline and add red behavioral probes

1. Finish or set aside unrelated work, record a clean committed SimpleBroker 8
   baseline, and run the current example suites. Record counts, skips, optional
   dependency availability, and all files created by the run.
2. Answer the four comprehension gates.
3. Add minimal red behavioral tests for each verified defect before changing
   the owning example. Red probes stay uncommitted until their implementation
   slice turns them green.
4. Inventory every example link from root README, agent kernel, Python guide,
   and the three example documents. Record broken or stale claims.
   Verification (2026-08-27) already found root `README.md` and
   `docs/guides/python.md` free of stale example claims; treat that as the
   expected result, not a gap.
5. Credit already-conforming code before writing probes:
   `process_retry_queue_once` in `dead_letter_queue.sh`, both hardened worker
   scripts (whose suites already pin the checkpoint-as-filter regression and
   delete-failure behavior), and the behavior-first shell/worker test suites
   already satisfy Decisions 12 and 14. Red probes target only the unrepaired
   functions — `simple_dlq_pattern`, `dlq_with_retry_count`,
   `process_with_delays`, the migration menu's rename/bound/backup paths, and
   the work-stealing load reads — rather than churning green code.

Gate: no example implementation starts until the v8 baseline is committed and
all red tests fail for the intended behavioral reason.

### Task 1: Remove async schema ownership and prove atomic order

Files:

- `examples/async_pooled_broker.py`
- `examples/test_async_pooled_broker.py`
- `tests/test_example_async_stream_transitions.py`

Actions:

1. Delete the example-local create/migration DDL, version ladder, migration
   helper imports, and setup lock. Add a small synchronous setup function using
   `with open_broker(target, config=resolved_config): pass`, called through
   `asyncio.to_thread` before pool construction.
   Obtain `resolved_config` from one `snapshot_config(...)` call at async context
   entry and pass the same object to every setup and runtime owner.
2. Keep async runtime SQL narrowly focused on queue operations. Any read-only
   schema assertion reports incompatibility and never repairs it.
3. Move timestamp generation into the same `BEGIN IMMEDIATE` transaction as
   insertion and durable high-water update.
4. Normalize materialized claim batches by integer public ID before yielding.
5. Enter and exit the synchronous `open_broker` context inside one worker call.
   If async initialization is cancelled, wait for that setup worker to finish
   and close before propagating cancellation; never construct the pool after
   cancellation.
6. Update `tests/test_sql_builder_validity.py`: retain coverage for runtime
   query builders still consumed by the async example and remove ownership
   claims or assertions for deleted migration/DDL imports.

Behavioral proof:

- fresh setup, existing v5 migration, and repeated setup use real SQLite and
  preserve a seeded unrelated sidecar definition and row. Cross-process
  first-open contention is owned by canonical setup's own suite; this
  triple-labeled advanced example does not rebuild that harness
  (owner-directed proportionality trim, 2026-08-27);
- deterministic barriers force writer A to pause at the former allocation
  boundary while writer B runs; the final commit/ID order and a bounded reader
  remain correct;
- a runner adapter reverses opaque multirow `RETURNING` records and the public
  stream still yields ascending IDs;
- failure during canonical setup opens no async pool and leaves the target in
  the canonical setup contract's recoverable state.
- cancellation during setup constructs no pool; the one-worker-call setup seam
  and its claim window are documented, not proven with a dedicated barrier
  harness (owner-directed proportionality trim, 2026-08-27).

Gate: no custom DDL remains in the async source; startup, two-writer
allocation-order, and row-order perturbation tests pass without sleeps as
synchronization.

### Task 2: Repair the recommended Python and async examples

Files:

- `examples/python_api.py`
- `examples/async_wrapper.py`
- `examples/async_simple_example.py`
- new focused tests under `examples/tests/`

Actions:

1. Remove `checkpoint_processing()` and its catalog claim. Do not replace it
   with a monotone bound example.
2. Add a self-contained bounded order demonstration using exact IDs inserted
   in non-sorted order. Cover default oldest and `order="newest"` on one or
   two bounded verbs (read and peek suffice; move optional) without using
   all/generator/watch surfaces — a lesson, not an API matrix.
3. Add keyword-only order to `AsyncBroker.pop` and `peek`; pass it unchanged to
   Queue so core validation remains authoritative.
4. Replace batch-prefetch streaming with one executor-backed timestamped read
   per yield. Remove `batch_size` and `_fetch_batch` rather than retain a
   misleading no-op optimization control.
5. Define the wrapper's logging handler locally and update the simple pooled
   usage program to state its claim-before-processing and oldest-only behavior.

Behavioral proof:

- public newest examples return expected bodies and IDs from out-of-order exact
  insertions (invalid-order rejection remains proven by the core suite; the
  example does not re-prove it);
- breaking after one stream item leaves all later rows pending; with the
  prefetch API removed there is structurally no hidden remainder, and the
  one-read claim-before-yield cancellation window is documented rather than
  barrier-proven;
- smoke runs use temporary targets and close executor/watcher resources.

Gate: recommended examples use supported imports only; no test asserts a raw
tuple's field order or passes merely because IDs happened to be inserted in
sorted order.

### Task 3: Make reactor discovery complete and replay collision-safe

Files:

- `examples/reference_reactor.py`
- `examples/tests/test_reference_reactor.py`
- `examples/tests/test_reference_reactor_transitions.py`

Actions:

1. Add one bounded discovery helper. Each call starts unfiltered, pages in
   ascending public ID using a pass-local lower bound, skips live `_inflight`
   keys and terminal `reactor_seen` states, and returns the first eligible row.
   Fetch terminal state for a whole bounded page in one sidecar query rather
   than one query per retained message.
2. Use that helper for input readiness/dispatch and control readiness/dispatch.
   Stop passing persisted checkpoints to `has_pending()` or `peek_many()`.
3. Record `control_processed` durably in the same sidecar transaction as the
   control checkpoint/audit update. Treat stale `inflight` as redispatchable
   after restart and `result_recorded`/`output_written` as terminal for input.
4. Make dispatch admission a conditional sidecar operation under the existing
   transaction boundary. Terminal states are absorbing; enqueue only when the
   operation returns an admitted `inflight` state. Preserve the current limit
   of one live in-flight item per input queue.
5. Keep checkpoints as maximum informational values for status and audit.
6. On exact output insertion collision, retrieve the existing body including
   claimed state and compare it to the pending payload. Mark written only on an
   exact match; otherwise raise with the pending row unchanged.
7. Document retained-source scan cost and the need for application retention or
   compaction. Reuse the existing `reactor_seen` table and its primary key; do
   not add a sidecar table, index, or migration mechanism for this repair.
8. Proportionality (verification finding, 2026-08-27): `reactor_seen` already
   records `inflight`, `result_recorded`, and `output_written`, and ordinary
   crash-restart redispatch already behaves correctly because the checkpoint
   advances only in the transaction that records the result. This task wires
   the existing ledger into discovery; it does not build new state. Complete
   late-lower-ID discovery remains required. If implementation would require a
   new sidecar mechanism or a disproportionate rewrite, stop and revise this
   plan with owner review rather than silently falling back to a monotone-writer
   precondition.

Behavioral proof:

- insert a lower exact ID after a higher input has completed; it is dispatched;
- repeat for a control request and verify one reply plus durable terminal seen
  state;
- restart with stale `inflight`; work is redispatched at least once;
- restart with `result_recorded` and pending output; input is not redispatched,
  while output replay continues;
- a barrier race terminalizes a discovered row before dispatch admission; the
  stale dispatcher cannot downgrade it or enqueue duplicate work;
- matching exact output replay is idempotent; mismatched body is not marked
  written and remains diagnosable;
- bounded paging is exercised with more completed retained rows than one page.

Gate: no durable numeric bound participates in completeness selection; the
late-lower and restart tests fail if that regression returns.

### Task 4: Repair watcher surfaces and multi-queue documentation

Files:

- `examples/simple_watcher_example.py`
- `examples/multi_queue_watcher.py`
- `examples/multi_queue_patterns.py`
- `examples/MULTI_QUEUE_README.md`
- `examples/tests/test_multi_queue_pattern_transitions.py`
- `tests/test_multi_queue_watcher_example.py`

Actions:

1. Replace package-internal handler imports with small local print, JSON, and
   logging handlers. Import extension types and default error behavior from
   `simplebroker.ext`; keep `Queue` at the package root.
2. Remove `BrokerDB` acceptance. Preserve `BrokerTarget` structurally through
   initial Queue creation and use `initial_queue.db_target` for every managed
   Queue.
3. Correct prose and source comments that imply handler retry, no races, no
   locks, O(1) memory, or transaction sharing beyond what code proves.
   Verification located all four false claims in `MULTI_QUEUE_README.md`
   (retry :400, no-locks :275, O(1) memory :286, transaction scope :280);
   `multi_queue_patterns.py` itself contains none of them.
4. State that weighted/round-robin patterns select which queue to consume next;
   they do not change claim-before-handler delivery.
5. Remove the hardcoded `"broker.db"` fallback in `MultiQueueWatcher`'s
   initial-queue construction (it disagrees with the `DEFAULT_DB_NAME`
   fallback later in the same file); let `Queue(db_path=None)` resolve
   configuration instead.

Behavioral proof:

- a structured `BrokerTarget` and a filesystem `Path` both route every managed
  Queue to the intended disposable target and create no display-string file;
- round-robin and per-queue handler selection remain correct;
- a failing handler leaves the message claimed and continuation moves to a
  later queue without claiming it was retried;
- runnable watcher examples terminate and release resources deterministically.

Gate: no recommended example imports `simplebroker.db` or
`simplebroker.watcher`; docs describe observed broker state after failure.

### Task 5: Repair shell control flow and migration semantics

Files:

- `examples/dead_letter_queue.sh`
- `examples/queue_migration.sh`
- `examples/work_stealing.sh`
- `tests/test_shell_examples.py`
- `tests/test_worker_examples.py`

Actions:

1. Wrap each `broker peek` command substitution so status `0` parses data,
   status `2` follows the documented idle/no-match branch, and every other
   status stops the operation.
2. Replace plain `broker list` text parsing with `broker stats QUEUE --json` or
   `broker list --stats --json`. Each script owns one small
   `queue_depth`-style helper that captures status, validates the numeric
   field with `jq -e`, and fails nonzero on anything unexpected; call sites
   stay one line so the teaching flow remains readable. Never coerce an error
   or missing field to zero, and do not inline full envelope-shape validation
   at every read site.
3. Make every write-then-delete and transform-then-delete path fail closed on
   source deletion error, with explicit duplicate-risk text and no success
   increment.
4. Implement the rename choice with `broker rename` and its documented missing
   source/destination behavior.
5. Accept documented `--before` bound syntax verbatim. Optional date conversion
   must produce one documented suffixed form; an exact 19-digit ID remains
   unchanged.
6. Replace the restore-script body copy with pending-only `broker dump
   --include` output and a documented `broker load` command. Never label it a
   full live backup.

Behavioral proof:

- black-box process tests use a controllable fake `broker` executable for exit
  transitions, then real CLI integration tests prove stats, rename, bounds, and
  dump/load state;
- every affected empty loop reaches its idle/completion branch;
- malformed JSON, broker exit `1`, and missing `pending` fail nonzero;
- write success plus delete failure stops after one replacement and reports
  ambiguity;
- exact-ID and suffixed time bounds reach the CLI unchanged;
- dump/load preserves pending message IDs and excludes claimed rows as
  documented.

Gate: ShellCheck passes; control-flow tests inspect process results and broker
state, not shell source strings.

For the partial-mutation proof, use a shim that delegates every command to the
real CLI and one real disposable database, but injects failure for the selected
source delete after the replacement write. Assert the original remains, exactly
one replacement exists, no later source item was processed, and the script exits
nonzero. A fully fake broker does not satisfy this proof.

### Task 6: Rewrite the example catalog and revalidate unchanged examples

Files:

- `examples/README.md`
- `examples/ASYNC_README.md`
- `examples/example_extension_implementation.md`
- `examples/logging_runner.py`
- `examples/sqlite_connect.py`
- `examples/test_sqlite_connect.py`
- `examples/safe_worker.sh`
- `examples/resilient_worker.sh`
- `docs/guides/python.md`
- `docs/agent-kernel.md`
- root `README.md` only for stale example restatements
- `CHANGELOG.md`

Actions:

1. Apply the proposed README delta and replace the extension guide in place.
2. Run real smoke scenarios for the logging runner and standalone SQLite
   utility; preserve their current designs unless a behavior test exposes a
   defect. The logging runner's `main()` currently writes `example.db` into
   the working directory; point it at a temporary target so demonstration
   runs satisfy the no-stray-databases signal.
3. Re-run the hardened worker black-box suites. Preserve the safe worker and
   informational-checkpoint resilient worker if green.
4. Check every relative link and every command from its documented working
   directory. State optional dependencies at the point of use.
5. Add concise related-plan backlinks to the cited canonical specs and a
   CHANGELOG bullet only after all behavior claims are green.

Gate: every inventory row has recorded green evidence or an explicit approved
deviation; the catalog contains no “production-ready” claim paired with a
demonstration-only disclaimer.

### Task 7: Full verification, fresh-eyes review, and closure

1. Run all focused and full repository gates below from a clean target
   directory and inspect the repository for stray databases afterward.
2. Run an independent review of async/schema ownership and ordering, another
   of reactor/restart state, and a final whole-diff review of delivery claims
   and shell failure behavior. Reviewers must read the relevant specs before
   judging examples.
3. Resolve every finding or record an evidence-backed rebuttal. Add a durable
   lesson only if the work reveals a reusable correction not already present.
4. Reconcile docs, spec related-plan links, the CHANGELOG, execution evidence,
   and deviations. Close this plan and its index row only in the same committed
   change that contains final evidence.

## Dependency Graph

```text
Task 0 green v8 baseline + red probes
  -> Task 1 pooled async setup/order ----\
  -> Task 2 recommended Python/async ----+-> Task 6 catalog/revalidation
  -> Task 3 reactor completeness --------+            |
  -> Task 4 watcher/multi-queue ----------+            v
  -> Task 5 shell behavior --------------/       Task 7 final review
```

Tasks 1 through 5 may proceed in parallel after Task 0 because their primary
source and focused test files are distinct. One consolidation owner serializes
edits to `examples/README.md`, `docs/guides/python.md`, `CHANGELOG.md`, and any
shared test helper. Do not split work by arbitrary line ranges.

## Verification Commands and Evidence Floors

Use exact focused node IDs during implementation and record observed counts.
Minimum final commands:

```bash
uv run pytest -n0 examples
uv run pytest -n0 \
  tests/test_example_async_stream_transitions.py \
  tests/test_multi_queue_watcher_example.py \
  tests/test_sql_builder_validity.py \
  tests/test_sqlite_connect_example.py \
  tests/test_worker_examples.py \
  tests/test_shell_examples.py
uv run pytest
uv run ruff check examples tests/test_example_async_stream_transitions.py \
  tests/test_multi_queue_watcher_example.py tests/test_worker_examples.py \
  tests/test_shell_examples.py
uv run ruff format --check examples tests/test_example_async_stream_transitions.py \
  tests/test_multi_queue_watcher_example.py tests/test_worker_examples.py \
  tests/test_shell_examples.py
uv run python bin/release.py --check-example-types
shellcheck examples/*.sh
python3 bin/check-dom15-fixtures
bin/check-plan-context
```

Also run the repository's current documentation/link and suppression gates.
Optional async dependencies must be installed for the pooled example tests;
record their versions and do not count import-time skips as proof.

Required scenario evidence, independent of suite names:

| Risk | Required firing proof |
|------|-----------------------|
| Async setup duplication | Real fresh, v5, and repeat startup with sidecar preservation; cross-process contention coverage remains owned by canonical setup's own suite. |
| ID commit order | Deterministic two-writer interleaving that would expose allocation outside the transaction. |
| Engine return order | Opaque row-sequence reversal with ascending public output. |
| Async stream loss | Early break leaves later rows pending; the removed prefetch API makes a hidden batch structurally impossible, and the one-read claim window is documented. |
| Newest discoverability | Out-of-order exact IDs demonstrated on public read/peek and wrapper pop/peek; the core contract suite continues to own move parity. |
| Reactor completeness | Late lower input and control IDs plus stale-inflight restart. |
| Reactor replay collision | Matching body succeeds; mismatched body remains pending. |
| Watcher failure language | Real handler failure shows claimed state and continuation behavior. |
| Shell status handling | Empty, operational failure, malformed JSON, and partial mutation transitions. |
| Portable export | Real dump/load preserves pending IDs and discloses claimed exclusion. |

Mocks and source scans may support diagnostics but cannot satisfy these floors.
No test gains value from pinning internal field order, SQL spelling, or private
class names when public behavior and durable state can prove the requirement.

## Acceptance Criteria

1. Every inventory row has an implemented or revalidated disposition with
   recorded evidence.
2. No example owns SimpleBroker schema DDL or refers to the retired surrogate.
3. The async pooled example routes setup through the canonical public open path
   and preserves sidecars for fresh, v5, and repeat startup. Cross-process
   serialization remains owned by
   `tests/test_sqlite_setup_contention.py::test_concurrent_first_writes_serialize_setup`.
4. Async writes cannot commit generated public IDs out of order, and batched
   output does not depend on engine `RETURNING` order.
5. Recommended bounded examples expose `order="newest"`; live/generator examples
   remain oldest-only and say so.
6. Early stream exit cannot hide a destructively prefetched batch. Cancellation
   may expose only the documented one-read claim-before-yield window.
7. Reactor discovery eventually sees valid late lower IDs and uses durable
   terminal state for deduplication.
8. Exact replay collision compares payload and cannot falsely mark success.
9. Watcher examples use public imports and make claim-before-handler state
   explicit.
10. Shell examples distinguish empty from failure, validate JSON counts, and
    stop on ambiguous partial mutation.
11. Rename and pending-only export use their supported CLI operations and
    preserve public IDs where promised.
12. Documentation, links, CHANGELOG, and related-plan traceability agree with
    executable behavior.
13. Full tests, lint, type checks, ShellCheck, docs gates, and independent
    reviews pass with no unexamined skip or new unexplained suppression.
14. The final committed change closes this plan and its index row; no release or
    publication is performed by this plan.

## Out of Scope

- adding newest order to generators, watchers, `--all`, or the advanced pooled
  async API;
- turning the pooled async example into a supported backend plugin;
- changing SimpleBroker delivery, schema, dump, or selection specifications;
- providing exactly-once application processing;
- making retained peek-based reactor history free to scan indefinitely;
- preserving claimed state in portable dump/load;
- changing Weft or Taut code, dependencies, or release coordination;
- migrating databases created by an earlier revision of the reference-reactor
  example;
- publishing a release.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| None | None | None | Empty at authoring. | None |

Record any changed task, skipped firing probe, new public surface, retained
private import, or different reactor state rule here before implementation
proceeds. No `pending` row may remain at completion.

## Execution Evidence

### 2026-08-27 Task 0 baseline and comprehension gate

- Implementation baseline: `be767f24` (`Finalize v8 storage and release
  preparation`). The examples plan and its index row were deliberately excluded
  from that targeted prerequisite commit.
- `uv run pytest -n0 examples` passed 120 tests in 7.87 seconds with no skips.
  The optional advanced-example dependencies resolve as `aiosqlite 0.22.1` and
  `aiosqlitepool 1.0.0` from this checkout's virtual environment.
- The focused v8 API/schema baseline passed 84 tests. Ruff over the examples,
  core, and PostgreSQL extension, `git diff --check`, DOM-15 fixtures, and
  plan-context validation all passed.
- The baseline example run created the ignored repository-local `.broker.db`;
  that artifact is removed before implementation and becomes a final no-stray
  regression signal. The ignored `loop.db` has an older 2026-08-23 modification
  time and is treated as pre-existing owner state.

Comprehension answers:

1. A saved maximum public ID is only a high-water observation. Exact insertion,
   load, and ID-preserving move can later add a lower pending ID, so a persisted
   `after_timestamp` filter would hide valid work permanently.
2. The public production `open_broker(...)` path owns SQLite PhaseLock,
   admission, migration, and proof. The async example waits for that path in one
   worker-thread call and owns no schema DDL.
3. Consumption commits the claim before handler dispatch. Handler failure does
   not restore pending state; an error handler may choose whether the watcher
   continues, but application retry needs a new durable application action.
4. Reversing only the sequence of opaque returned records proves normalization
   is independent of engine output order without pinning tuple layout or claiming
   a native database order guarantee.

### 2026-08-27 Tasks 1-2 async and recommended-Python slice

Red probes fired before their owning repairs:

- `examples/tests/test_recommended_python_examples.py::test_async_wrapper_exposes_oldest_and_newest_bounded_selection`
  failed because `AsyncBroker.peek()` rejected `order`;
- `examples/tests/test_recommended_python_examples.py::test_async_wrapper_early_stream_exit_leaves_later_rows_pending`
  failed with no pending rows after one yield because the wrapper had claimed a
  hidden batch;
- `examples/tests/test_recommended_python_examples.py::test_python_api_demonstrates_public_id_order_not_insertion_order`
  failed because the public-ID order demonstration did not exist;
- `examples/test_async_pooled_broker.py::test_async_context_uses_canonical_setup_and_preserves_sidecar`
  observed zero canonical setup calls;
- `examples/test_async_pooled_broker.py::test_async_setup_failure_does_not_construct_pool`
  observed that context entry admitted a future schema without running setup;
- `examples/test_async_pooled_broker.py::test_generated_ids_follow_commit_order_under_forced_interleaving`
  produced public order `first-started, second-started` after commit completion
  `second-started, first-started`;
- `examples/test_async_pooled_broker.py::test_claim_batch_normalizes_reversed_opaque_runner_records`
  exposed the reversed engine-record sequence unchanged.

The repaired slice routes context entry through one worker-thread
`open_broker(...)` call before constructing the async runner, passes one exact
`ResolvedConfig` snapshot to setup and runtime, performs only read-only async
compatibility checks, allocates IDs inside the insertion transaction, and
normalizes materialized claim records by public ID. The sidecar proof snapshots
table/index definitions, indexed columns, and rows across literal-v5 migration
and repeat setup. The recommended wrapper exposes bounded `order`, starts one
destructive read per active iteration, and removes the batch control. The
primary Python example uses exact IDs `300, 100, 200` to demonstrate both
directions and removes the false resumable checkpoint lesson.

The focused async/recommended set passed 16 tests. The expanded examples plus
async-stream and runtime-builder set passed 140 tests. Ruff, format, the
16-file example mypy gate, and `git diff --check` passed. A DDL/import inventory
found no `CREATE`, `ALTER`, `DROP`, or migration builder in
`async_pooled_broker.py`.

Independent review found one missing oldest-only/claim-before-processing note
in `async_simple_example.py` and asked for stronger sidecar-index evidence.
Both were corrected. Focused re-review reported no remaining actionable
blocker. The slice is committed as `6a4d21f1` (`Repair async and Python
examples`).

### 2026-08-27 Task 3 reactor discovery and replay slice

Five new probes failed against the prior reactor for their intended reasons:
late lower input and control IDs were hidden by the persisted checkpoint;
dispatch admission overwrote a terminal seen state; discovery could not page
past a retained terminal first page; and an exact-ID output occupant with a
different body was falsely accepted as the pending result. The pre-existing
restart behavior already redispatched a stale `inflight` row, so that required
proof was green before the repair and was retained as a regression test rather
than misreported as a red defect.

The repaired discovery pass starts without a durable bound, pages with a local
ascending-ID bound, fetches seen state once per page, skips live work and
terminal ledger rows, and admits input only through a conditional sidecar write.
`control_processed` is recorded with the control checkpoint and audit record.
The checkpoint remains visible status information but no longer participates in
selection. Exact-ID replay now compares the occupant body for both pending and
already-claimed output rows; a mismatch raises while `output_pending` remains
unchanged.

The direct and transition reactor suites pass 70 tests. Ruff, format, the full
16-file examples mypy gate, and `git diff --check` pass. The transition table
now fires stale-inflight redispatch, terminal-state skip, and durable
output-backlog ownership after `result_recorded`. The source documents the
linear retained-history scan cost, application retention/compaction need, and
one-live-item-per-input-queue boundary.

The independent reactor/restart review read Task 3 plus `[SB-DELIVERY]` and
`[SB-SELECT]`, reran both suites and the example type gate, and reported no
actionable findings. It specifically confirmed complete pass-local discovery,
terminal admission, stale-inflight replay, result-recorded output ownership,
pending and claimed exact-ID collision handling, and the one-inflight-per-queue
boundary.

The slice is committed as `af1f062` (`Repair reactor discovery and replay`).

### 2026-08-27 Task 4 watcher and multi-queue slice

The structured-target probe failed against the prior example because
`BrokerTarget` was stringified into a repr-shaped filesystem target. The Path
case and the existing round-robin/error-handler suite were green. The repaired
watcher accepts only `BrokerTarget | str | Path | None`, lets `Queue` normalize
the initial target, then reuses the public `db_target` for every configured and
dynamically added Queue. Real writes through all three managed handles land in
the intended disposable target and no display-string path is created.

Recommended watcher examples now import root watcher types and the public
`simplebroker.ext` subclass/error surface. Their print, JSON, and logging
handlers are ordinary local application code. Source and multi-queue prose state
that consume claims before handler dispatch: continuation reaches later queues
but does not restore or retry the failed row. The transition suite proves the
failure changes pending and claimed counts by one while later queues run.

Initial independent review found that the example's broad per-queue exception
catch swallowed inherited terminal `StopWatching` and error-handler failure
signals. Both new firing probes failed against that behavior: a handler stop
claimed work from the next queue, while an error-handler exception was logged
and suppressed. The catch was removed so the BaseWatcher owner receives those
terminal signals; both probes now leave the later queue pending, and the
error-handler failure remains observable.

The watcher and pattern suites pass 18 tests. Ruff, format, the 16-file examples
mypy gate, and `git diff --check` pass. Both runnable watcher programs completed
against temporary targets; the run created no repository-local database.
The expanded `examples` suite passes 137 tests.

Focused re-review reported no remaining actionable findings. It verified that
terminal signals now reach BaseWatcher, public synchronous `run()` exposes the
exact error-handler `ValueError` with the original handler `RuntimeError` as its
cause, later work remains pending, and target/activity prose is backend-neutral.

### 2026-08-27 Task 5 shell control-flow and migration slice

The initial shell probes failed for the intended contract gaps: none of the
three menus exposed a validated JSON depth helper; `set -e` aborted the DLQ
loops on an ordinary empty `peek`; operational peek failures had no explicit
branch; rename still called `move --all`; a suffixed seconds bound went through
platform date parsing while a native 19-digit ID gained an invalid `s`; and the
queue export still copied peek output into an ad hoc restore script.

Each menu now owns a small `queue_depth` helper over `broker stats QUEUE
--json`. It rejects command failure, missing or nonnumeric `pending`, negative
counts, and fractional counts. Each one-message peek is status-aware and
validates the public JSON envelope before mutation. Exit `2` reaches the
operation's idle or no-match branch; other statuses stop. The repaired
write/delete paths report the duplicate or retry risk and return before any
success count. Load balancing never converts a stats error into zero work.

The migration menu now delegates rename to `broker rename`, passes every
documented bound unchanged, and exports a pending-only native dump with an
explicit `broker load` instruction. Its output states that claimed rows and
application sidecars are excluded and describes the pre-export count as an
observation rather than a transactionally exact dump count.

The black-box fake-broker matrix fires empty, operational-error, malformed
JSON, missing-field, and replacement-delete transitions. Real CLI tests prove
that rename preserves pending and claimed state, existing destinations remain
unchanged, both seconds and native-ID bounds select correctly, JSON stats
reports pending depth independently of claimed rows, and dump/load preserves
the pending public ID while excluding a claimed row. The required partial
mutation shim delegates to a real disposable broker, injects only the selected
source delete failure after the first transformed write, and proves that the
original plus later source row remain while exactly one replacement exists.
These tests assert public bodies, IDs, counts, process status, and CLI argv;
none depends on a raw tuple field layout or shell source text.

The focused shell and unchanged worker suites pass 142 tests. Bash syntax,
ShellCheck for all three repaired menus, Ruff for both test modules, and
`git diff --check` pass. The expanded examples, async-stream, watcher, shell,
and worker regression set passes 294 tests.

Independent shell review found four initial gaps: dash-prefixed and invalid
grep patterns were conflated with ordinary no-match; the “selective” retry ran
after the same function had already drained the DLQ; merge output called a
racy pre-move observation an exact moved count; and several new direct-move
and dump-failure branches lacked firing probes. The implementation now uses
`grep --`, maps invalid pattern syntax to ordinary failure rather than broker
exit `2`, exposes mutually exclusive `all` and `recent` retry modes, labels
merge counts as observations, and exercises those branches. Re-review found
only the invalid-pattern status and stale evidence counts; both were corrected.

### 2026-08-27 Task 6 catalog and unchanged-example revalidation

The example catalog now assigns each family a support level and states the
actual boundary at the point of use: bounded newest selection versus
oldest-only live traversal, claim-before-processing delivery, pending-only
dump/load export, and the advanced pooled implementation's private/internal
status. `ASYNC_README.md` now distinguishes the supported executor wrapper
from the SQLite-only pooled core, identifies canonical synchronous setup as the
schema owner, and describes setup cancellation, one-read stream cancellation,
and `commit_interval > 1` transaction/replay mechanics without promising an
application delivery guarantee.

The former 1,300-line copied extension implementation is now a short map to the
three executable source examples and their tests. It contains no code copy or
DDL. The Python guide's async and reactor sections were brought into line with
the repaired examples, and the agent kernel now routes readers through the
support-level catalog. Inspection found no stale example restatement in the
root README, so it remains unchanged. The CHANGELOG records this as an example
and documentation guarantee rather than new core behavior.

`logging_runner.py` retains its public `SQLRunner` wrapper and caller-owned
lifecycle, but its runnable demo now uses a temporary target. The advanced
pooled and simple async entry points were also found to violate the plan-wide
temporary-target invariant; their demo functions now receive one entry-point
target and `__main__` supplies a temporary database. This does not change the
async core. The batch demo's old blanket at-least-once comment was replaced
with the actual open-transaction and rollback/replay boundary.

Real smoke runs of `logging_runner.py`, `python_api.py`, `async_wrapper.py`,
`simple_watcher_example.py`, `multi_queue_patterns.py`, and both documented
`async_simple_example.py` modes completed from the repository root without
changing the repository database-artifact set. Relative-link validation over
the root, guide, kernel, and example Markdown files passed, as did
`bin/check-doc-paths`. The focused nested-test command states the `examples`
import path explicitly so it works from the repository root without changing
the release type-check boundary.

The standalone SQLite utility plus its external smoke gate and the unchanged
worker/shell suites pass 186 tests. The full `examples` tree passes 136 tests;
the combined Task 6 behavior set passes 218 tests. The example type gate passes
all 16 concrete files. Ruff, format, and `git diff --check` pass. Three tests
that matched README prose or an obsolete literal string were removed: they did
not establish runtime correctness and conflicted with the plan's behavior-first
test rule. Their owning runtime paths remain covered by the behavior suites.

Inventory reconciliation: Tasks 1-2 cover the pooled, recommended Python, and
async source/test rows; Task 3 covers the reactor source and two test rows;
Task 4 covers the watcher/pattern source, documentation, and transition row;
Task 5 covers all five shell source rows and both external shell gates; this
task covers the remaining catalog and guide rows, the logging runner,
standalone SQLite source/test pair, and final smoke revalidation. No inventory
row is left without a recorded disposition or green gate.

Independent Task 6 review found three initial gaps. A package-qualified nested
test import passed pytest but failed the release mypy gate; the source retains
its top-level example imports and the documented focused command now supplies
`PYTHONPATH=examples`, so both gates pass without a suppression. The shell
catalog now runs fixed-name mutations only in a `mktemp` working-directory
target and warns that worker/monitor choices may be long-running. Finally, all
seven cited canonical specs now backlink this plan. Re-review found one stale
selector description: the catalog now uses the finite DLQ setup selector and
accurately identifies which menu prompts. The exact isolated DLQ and
work-stealing setup commands were smoke-tested and created no repository-local
database.

## Independent Plan Review

Independent review completed 2026-08-27 by the repository's `plan_review`
reviewer. Findings and dispositions:

- accepted: executor cancellation cannot promise zero unseen claims. The plan
  now promises and tests no hidden batch plus at most one in-flight claim;
- accepted: reactor terminal state must be absorbing under a stale-dispatch
  race, and one-inflight-per-input-queue remains explicit;
- resolved by owner scope: migration of databases created by older revisions
  of this example is not infrastructure this plan will create. It is expressly
  out of scope; fresh runs and restarts under the revised example are tested;
- accepted: configuration is snapshotted once; setup cancellation waits for the
  worker to close; first-open contention uses separate processes and a literal
  v5-plus-sidecar target;
- accepted: the release helper's concrete examples type-check gate replaces a
  directory-level mypy command excluded by root configuration;
- accepted: partial shell mutation proof delegates to a real CLI/database and
  injects only the selected delete failure;
- accepted: `tests/test_sql_builder_validity.py` is owned by Task 1 and retains
  only live runtime-builder rationale;
- accepted: the Deviation Log uses the required five-column format.

The reviewer also verified the exact 24-file inventory and found no remaining
scope, rollback, generator/newest, or non-brittle-test defect after these
corrections. During implementation, independent reviewers must still check at
minimum:

- whether canonical `open_broker` setup fully replaces example DDL without a
  hidden lifecycle or thread-affinity problem;
- whether the reactor terminal-state and paging rules guarantee eventual
  discovery without duplicate completed input work;
- whether shell partial-failure tests observe real state rather than mocks only;
- whether any test encodes tuple layout, native engine order, or source spelling
  instead of the claimed behavior;
- whether every file in the inventory has a concrete disposition and gate.

### 2026-08-27 defect-verification pass (pre-Task 0)

A five-family verification checked the claimed defects against the code and
corrected this plan as follows, under two owner directives: examples are
teaching material that may stay intentionally non-optimal where the
simplification is honest and labeled, and no new test infrastructure is built
around examples unless it is a genuine win — existing example suites are
updated and kept.

- Reactor: `reactor_seen` already holds the terminal-state ledger and
  ordinary crash-restart redispatch already works (checkpoint advances only
  in the result transaction); Task 3 wires existing state into discovery
  instead of building new state. Complete late-lower-ID discovery remains a
  required outcome rather than an implementation-time fallback choice. The
  checkpoint-as-permanent-filter defect is confirmed on both lanes, and the
  replay collision check confirms occupant presence without comparing bodies.
- Watcher: the four false robustness claims live in `MULTI_QUEUE_README.md`,
  not `multi_queue_patterns.py`. The target-stringification bug is worse than
  claimed — it also misroutes the documented `BrokerDB` type into a
  repr-named database for the first queue. Three of the four taught handlers
  have no public export, so local handlers are the only public-surface
  remedy. The existing watcher tests are behavior-first.
- Shell: every silent-zero, false-success, errexit-empty-abort, and
  ID-to-seconds bound-mangling claim is confirmed at line level.
  `process_retry_queue_once`, both worker scripts, and their suites already
  conform; probes target only unrepaired functions, and count validation is
  hoisted into one helper per script.
- Async: all five code defects confirmed, including the out-of-transaction
  allocation comment and the destructive prefetch in the recommended
  wrapper. The ASYNC_README retry/move snippets are honestly-labeled
  application code needing one clarifying sentence, not removal as false
  claims; the cross-process startup matrix and the barrier'd cancellation
  proof are trimmed because canonical setup's own suite owns that contention
  coverage.
- Python/docs: the checkpoint example's actual mechanism is a loop-index
  checkpoint never read as a filter over a destructive read-all (removal
  disposition unchanged); the extension guide carries about 1,225 copied
  lines including the retired surrogate DDL twice; root `README.md` and
  `docs/guides/python.md` contain no stale example claims.

Items previously accepted by the plan_review reviewer that this pass trims —
the separate-process contention harness and the barrier'd cancellation proof
— are owner-directed proportionality corrections, not reopened review
findings.
