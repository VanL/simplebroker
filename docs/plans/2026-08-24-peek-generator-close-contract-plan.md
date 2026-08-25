# Peek Generator Close Contract Plan

Status: completed
Class: 5 — this plan adds a public Python return type and a synchronous
resource-lifecycle promise to the published `Queue.peek_generator()` contract.
It changes `[SB-DELIVERY-4]`, `[SB-API-1]`, `[SB-API-4]`, and `[SB-API-5]`, so
the public-contract and cleanup-lifecycle hardening requirements apply.
Plan type: implementation with spec revision

## Goal

Make the behavior Taut and existing SimpleBroker consumers already need a
public, typed contract: a caller can identify `Queue.peek_generator()` as a
closeable iterator, and same-thread exhaustion, an exception raised by
advancing the iterator, or explicit early `close()` synchronously ends the
iterator-owned Queue operation before
the caller closes its Queue or client. Preserve live offset-paged peek
semantics, backend ownership, and the existing same-thread limitation.

## Source Documents

Source specs:

- `docs/specs/11-delivery.md` [SB-DELIVERY-4], [SB-DELIVERY-6]
- `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-3], [SB-API-4],
  [SB-API-5], [SB-API-11]
- `docs/specs/product-section-registry.md` (delivery and Python library rows)

Theory:

- `docs/program-theory.md` [THEORY-4] requires explicit safety, a small concept
  count, and growth backed by concrete pressure.
- `docs/program-theory.md` [THEORY-6] treats repeated consumer reliance on
  private behavior as evidence of a missing public Queue primitive or
  ownership rule.
- `docs/program-theory.md` [REV-THEORY-004] places reusable backend-resource
  ownership at the process-session/backend seam rather than on each Queue.
- `docs/program-theory.md` [REV-THEORY-005] requires a suspended operation to
  retain its ownership context rather than transferring cleanup across an
  incompatible thread.

Implementation and guidance:

- `docs/implementation/06-process-session-core-ownership.md`
- `docs/implementation/04-cross-thread-finalization-poisoning.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`
- `docs/agent-context/runbooks/maintaining-traceability.md`
- `skills/interface-review/SKILL.md`

Consumer pressure and rollout owner:

- `../taut/docs/plans/2026-08-24-command-runtime-findings-remediation-plan.md`
  Slice 0 requires a released close contract before Taut may expose
  `iter_log()`.
- `../weft/weft/helpers/__init__.py::iter_queue_json_entries()` and
  `closing_queue_iterator()` already discover and call `close()` defensively
  around production `peek_generator()` use.

No existing SimpleBroker plan owns this contract. The active
`2026-08-24-cli-output-and-error-contract-remediation-plan.md` owns unrelated
CLI output/error work and explicitly excludes release; preserve its changed
surfaces and serialize any shared release preparation.

## Decisions

1. Add one package-root public structural protocol,
   `CloseableIterator[T]`, with `__iter__`, `__next__`, and `close() -> None`.
   Do not use `Generator[...]`: that would also promise `send()` and `throw()`,
   which callers do not need and future implementations should not have to
   retain.
2. Apply the closeable type to `Queue.peek_generator()` and the
   `Queue.peek(all_messages=True)` convenience view. Do not widen this plan to
   read, move, or `stream_messages()` return types.
3. Keep the external seam at `Queue`. The internal/backend-facing
   `BrokerConnection.peek_generator()` may continue to return `Iterator[...]`;
   the outer Queue generator owns the Queue operation scope. No backend API
   version bump is planned.
4. Iterator construction stays lazy. The first advancement attempt establishes
   the owner thread and enters the Queue operation. An active iterator must be
   advanced, exhausted, and explicitly closed on that thread.
5. Cross-thread close remains unsupported. This plan does not add a
   transferable session lease, foreign-thread cleanup, poisoning, or recovery
   path for observational peek iterators.
6. Promise synchronous **operation-scope exit and owned cleanup invocation**,
   not unconditional connection destruction. Persistent Queues retain their
   cached process-session/core resources. Ephemeral Queues exit and clean up
   their operation-owned `DBConnection`/core handle. A Queue constructed with
   a caller-injected runner retains its Queue-owned `DBConnection`/core handle
   until `Queue.close()` and never closes or shuts down the runner; iterator
   termination ends only the lexical iterator operation in that mode.
7. Prove the public rule on all released adapters: real SQLite, PostgreSQL, and
   Redis/Valkey. The original two-backend proposal is insufficient for a
   backend-neutral Queue promise.
8. Extend the existing release packaging smoke instead of inventing a second
   release path. The root wheel and root sdist must each be installed in a
   separate clean environment and pass the same runtime root-import and SQLite
   early-close probe. Source-tree mypy remains the static type proof; the real
   PostgreSQL and Redis harnesses remain the service-parity proof.

## Context and Key Files

### Current public and implementation seams

- `simplebroker/sbqueue.py::Queue.peek()` dispatches `all_messages=True` to
  `Queue.peek_generator()` and currently types that result as `Iterator[...]`.
  Its literal overloads and unknown-`bool` union are a public typing contract.
- `simplebroker/sbqueue.py::Queue.peek_generator()` is a Python generator. It
  enters `with self.get_connection() as connection:` and delegates with
  `yield from connection.peek_generator(...)`. Closing the outer generator on
  its owner thread unwinds that context today.
- `Queue.get_connection()` owns the public Queue operation scope. In persistent
  mode its `finally` calls `DBConnection.release_connection_after_use()`; in
  ephemeral mode it exits an owned `DBConnection` context, which drains and
  closes the Queue operation's registered handle/core according to existing
  backend ownership.
- Supplying `runner=` forces `Queue.conn` even when `persistent=False`.
  `release_connection_after_use()` is then a no-op because the connection is
  not process-shared. The Queue retains that `DBConnection`/core handle until
  `Queue.close()`, which releases the handle but deliberately does not close or
  shut down the caller-owned runner.
- `simplebroker/db.py::DBConnection.release_connection_after_use()` pops the
  current thread's process-session operation and calls
  `release_current_thread_connection()`. That ends the active operation but
  intentionally keeps a persistent backend checkout cached.
- `simplebroker/_broker_session.py::_ProcessBrokerSession.release_current_thread_connection()`
  owns that persistent release rule. Its operation stack is thread-local; a
  foreign thread cannot release the owner's entry.
- `simplebroker/_backend_plugins.py::BrokerConnection.peek_generator()` is the
  backend-facing iterator seam. Do not tighten it unless a real-backend test
  proves the Queue wrapper cannot meet the public contract without doing so.
- `simplebroker/__init__.py` owns root exports. Define `CloseableIterator` next
  to the other Queue result type in `simplebroker/sbqueue.py` and re-export it
  from the package root; do not create a one-type module.

### Existing test and documentation owners

- `tests/test_queue_typing_contract.py` pins every literal and runtime-boolean
  Queue overload through mypy. It is the failing-first owner for the new
  closeable return type.
- `tests/test_python_library_api_contract_sb_api.py` binds package-root exports,
  `[SB-API-*]` prose, implementation maps, and public importability.
- `tests/test_delivery_contract_sb_delivery.py` binds `[SB-DELIVERY-*]` prose,
  verification rows, and the existing live offset-paging behavior.
- `tests/conftest.py::queue_factory` creates real Queue handles for the selected
  SQLite, PostgreSQL, or Redis backend and closes them at teardown. A new
  shared lifecycle test module should use this path rather than creating a
  second backend matrix.
- `bin/pytest-pg` and `bin/pytest-redis` provision and select the existing real
  service-backed harnesses. CI already runs both.
- `simplebroker/_scripts.py::_smoke_install_artifacts()` and
  `tests/test_dev_scripts.py` own pre-publication clean-install smoke. At the
  baseline they install only the three wheels together and probe extension
  discovery; they do not install the root sdist or exercise this lifecycle.
- `docs/agent-kernel.md`, `docs/guides/python.md`, and the root `README.md`
  teach the current generator/thread rules. They must distinguish closeable
  observational peek from transactional delivery semantics.
- `docs/implementation/06-process-session-core-ownership.md` owns the reason
  operation release does not mean persistent connection destruction.
- `CHANGELOG.md` records the new public type and lifecycle promise when the
  implementation is release-ready.

### Downstream compatibility context

- Weft repeatedly wraps `peek_generator()` in
  `closing_queue_iterator()`. The stronger return type should remove an
  undocumented assumption without changing runtime behavior. Its test doubles
  and any Queue subclasses must still be checked because an override returning
  only `Iterator[...]` no longer satisfies the tightened base contract.
- Taut currently declares `simplebroker>=7.4.1`. Its own active plan owns the
  post-release minimum-version and lock update. This SimpleBroker plan provides
  the released contract and immutable artifact evidence; it does not edit Taut.

### Comprehension gates

Before editing runtime code or tests, the implementer records answers in this
plan's execution log. A wrong answer blocks implementation until the cited
owners are reread.

1. **What is synchronously released in each ownership mode?** Expected answer:
   a persistent Queue ends the iterator-owned process-session operation on the
   owner thread while retaining the cached session/core/backend checkout; an
   ephemeral Queue exits its operation-owned `DBConnection` and releases its
   registered handle/core; a Queue with an injected runner has no
   iterator-owned handle to release, so iterator termination ends its lexical
   operation while retaining the Queue-owned `DBConnection`/core until
   `Queue.close()`, and the runner itself is never closed or shut down.
2. **When does a peek iterator become thread-affine?** Expected answer: object
   creation acquires no operation; the first advancement attempt enters the
   operation and establishes the thread that must perform later advancement,
   exhaustion, or close. Closing an unstarted iterator acquires no operation
   and makes that single-use iterator terminal; a later `next()` raises
   `StopIteration`.
3. **Why does `BrokerConnection.peek_generator()` remain `Iterator[...]`?**
   Expected answer: the public guarantee belongs to the outer Queue generator,
   whose context owns the Queue operation. Tightening the backend protocol
   would impose an unnecessary extension contract and could require a backend
   API/version change.
4. **What counts as exhaustion?** Expected answer: the caller has advanced the
   iterator until `StopIteration`; merely receiving the last yielded row leaves
   the generator suspended until another advance or explicit close.
5. **What must stay real in the acceptance proof?** Expected answer: Queue,
   process-session behavior, and each released backend adapter/service. A
   close observer must call the real `DBConnection.close()`; two narrow
   injected delegates may be used only to force an otherwise unavailable
   post-yield advancement failure and delegated-close failure.
6. **Which artifact checks prove which claim?** Expected answer: source-tree
   mypy proves `.close()` is statically available; separate clean installs of
   the root wheel and root sdist prove the public import and SQLite runtime
   lifecycle; hosted source gates prove PostgreSQL and Redis parity.

## Invariants and Constraints

- Peeking remains observation. It does not claim, delete, move, lock, snapshot,
  or promise exhaustive traversal under concurrent mutation.
- `[SB-DELIVERY-4]` live offset paging, result order, filters,
  `include_claimed`, and record shapes remain unchanged.
- `CloseableIterator[T]` remains usable everywhere an `Iterator[T]` is
  accepted. It promises no `send()`, `throw()`, context-manager protocol, async
  iteration, restartability, or concurrent use.
- `Queue.peek_generator()` remains lazy and single-use. No Queue operation or
  backend resource is acquired at iterator construction.
- Once active, iteration and explicit close are same-thread operations.
  Cross-thread use is unsupported, and garbage-collection finalization is not
  a substitute for explicit close.
- Advancing through `StopIteration`, an exception raised by an advancement
  (`next()`) attempt (including validation or backend failure), or explicit
  same-thread `close()` exits the iterator-owned Queue operation and invokes
  owned cleanup before that action returns or raises. An exception raised by
  the caller's loop body does not advance or terminate the iterator; the caller
  must close it in `finally`.
- `close()` before first advancement acquires no operation and makes the
  single-use iterator terminal. Close after terminal exhaustion/failure and
  repeated `close()` are safe. A caller that may exit early must close before
  closing its Queue or higher-level client.
- Synchronous cleanup does not transfer ownership. It must not close a
  persistent Queue's cached process session/core, shut down a caller-injected
  runner, or add a permanent latch to a reusable runner. Ephemeral cleanup does
  release its Queue-owned `DBConnection`/core handle; injected-runner iterator
  cleanup retains the Queue-owned cached handle until `Queue.close()`.
- Existing cleanup failure policy remains in force. This plan does not promise
  that every substrate close failure is recoverable or surfaced; it promises
  that the owned cleanup path has run synchronously and that the Queue
  operation is no longer active.
- Do not introduce new error arbitration. An advancement or delegated-close
  error remains primary; ordinary ephemeral substrate-close `Exception`s stay
  best-effort under `DBConnection._close_best_effort()` and do not replace it.
  A delegated-close error with no earlier active error propagates after the
  Queue scope unwinds. Cleanup-only substrate surfacing remains the existing
  non-public policy rather than a new public exception guarantee.
- Preserve CLI `peek --all` output, exit codes, and closed-pipe behavior. Its
  existing explicit iterator cleanup should continue to work through the new
  type without a second CLI path.
- Do not add a runtime wrapper merely to expose the type. The existing Python
  generator structurally satisfies the protocol. If tests show that a wrapper,
  transferable lease, backend protocol change, backend API bump, or new
  dependency is required, stop and revise this plan before proceeding.
- No storage format, migration, queue state, backend command, or public error
  type changes under this plan.
- Update Taut and Weft evidence, not their source trees. Their own repositories
  retain authority over dependency floors and consumer code.

## Rollback, Rollout, and Observation

There is no storage migration or destructive one-way door. Before release, the
spec, type/export, implementation annotations, tests, docs, and changelog can be
reverted as one contract slice. The implementation already has the same-thread
unwind shape, so strategy-A spec promotion does not authorize a new runtime
path while typing work is pending.

After a release advertises `CloseableIterator`, removing the root export or
weakening close semantics is a breaking public change and is not a patch-level
rollback. Correct a defect forward or issue a new compatible release; do not
move or overwrite a published tag.

Rollout order is strict:

1. land and verify the SimpleBroker contract without changing the backend API;
2. serialize release preparation with other active SimpleBroker work, select
   the next owner-approved compatible core version, extend the release driver's
   existing pre-publication packaging smoke, and independently clean-install
   the candidate root wheel and root sdist;
3. rerun required hosted gates at the exact release SHA and verify the public
   import, source-tree type contract, artifact SQLite probe, and real-service
   early-close behavior before tag push;
4. publish only after explicit owner authorization and those pre-publication
   gates are green;
5. install the immutable published artifact as post-release acceptance, record
   its tag/version/hash evidence, and treat any failure as a release incident
   requiring a corrective version rather than pretending publication can be
   rolled back;
6. hand that evidence to Taut;
7. only then may Taut raise its minimum and claim deterministic `iter_log()`
   close propagation.

No extension-package release is needed if their code and backend API version do
not change. If PostgreSQL or Redis needs an adapter edit, stop and decide
whether a coordinated extension release and dependency-floor change is
required.

Post-release success is observable without new telemetry: separate clean
installs of the published root wheel and root sdist import
`CloseableIterator` and pass the SQLite early-close probe; source-tree mypy
accepts `peek_generator().close()` without a cast; real same-thread early close
permits immediate Queue reuse/close on all adapters; Taut records the released
minimum; and Weft's existing close wrappers remain green. Before publication,
a five-second Queue-close drain, a retained persistent active-operation count,
or an ephemeral real-close observer that has not completed when a terminal
action returns is a release blocker. The same signal found only after
publication is a release incident and corrective-release trigger.

## Spec Baseline

- `36bc6d4d0c079928ef051ea7129c78245c2ee058` —
  `docs/specs/11-delivery.md` and
  `docs/specs/16-python-library-api.md` at plan authoring time.
- Plan type: implementation with spec revision.
- Promotion baseline: uncommitted Strategy-A spec delta against
  `36bc6d4d0c079928ef051ea7129c78245c2ee058`, SHA-256
  `0dd4d54b67c21d5e73331f00c51f4d04100a6c8a62a1401e085158390f1b6b6c`.
  Replace this diff identifier with the landing commit when committed.

## Proposed Spec Delta

Promotion strategy: **A — in-file text before link claims**. Promote the exact
contract text first without adding implementation-link claims. Add the new
verification rows/backlinks together with code and tests in the later
traceability slice. The existing spec files remain active; do not reclassify
them.

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/11-delivery.md` | A — in-file text before link claims | [SB-DELIVERY-4], verification, Related Plans |
| `docs/specs/16-python-library-api.md` | A — in-file text before link claims | [SB-API-1], [SB-API-4], [SB-API-5], verification, Related Plans |

### [SB-DELIVERY-4] — insert after the live offset-paged paragraph

> `Queue.peek_generator()` returns a single-use closeable iterator. Creating
> the iterator is lazy and starts no Queue operation. Its first advancement
> attempt enters one iterator-owned Queue operation and establishes the owner
> thread. While active, the iterator must be advanced, exhausted, and closed
> on that same thread; cross-thread use is unsupported, and callers must not
> rely on garbage collection for cleanup.
>
> Advancing the iterator through `StopIteration`, an exception raised by an
> advancement attempt (including validation or backend failure), or an explicit
> same-thread `close()` synchronously exits that Queue operation and invokes its
> owned cleanup before the action returns or raises. An exception raised by the
> caller's loop body does not advance or terminate the iterator; the caller must
> close it in `finally`.
>
> Closing before first advancement acquires no Queue operation and makes the
> single-use iterator terminal, so a later `next()` raises `StopIteration`.
> Closing after a terminal outcome or more than once is safe. A caller that may
> stop early must close the iterator before closing its Queue or higher-level
> client.
>
> Operation exit does not close resources owned by another lifecycle: a
> persistent Queue may retain its cached process session, core, or backend
> checkout; an ephemeral Queue releases its operation-owned connection/core
> handle; and a Queue with a caller-supplied runner retains its cached
> connection/core handle until `Queue.close()` without closing or shutting down
> the runner. These lifecycle rules do not change the live, offset-paged
> traversal or strengthen peek into a snapshot, claim, or exhaustive concurrent
> traversal.

Update [SB-DELIVERY-4] verification to name
`tests/test_peek_generator_lifecycle.py` and retain the existing live-pagination
gates.

### [SB-API-1] — insert after `MovedMessage`

> `CloseableIterator[T]` is a package-root public structural protocol for a
> single-use iterator with `__iter__`, `__next__`, and `close() -> None`. It is
> compatible with ordinary `Iterator[T]` use and deliberately does not promise
> generator-only `send()` or `throw()` operations. It describes the returned
> object; it does not require a runtime wrapper.

Add `CloseableIterator` to the package-root role row and implementation mapping.

### [SB-API-4] — insert after the overload paragraph

> The `all_messages=True` view of `Queue.peek()` returns
> `CloseableIterator[...]`; an unknown runtime `bool` includes that closeable
> iterator in its existing scalar/tuple/iterator union. The read and move
> families retain their existing return types.

### [SB-API-5] — insert after the opening paragraph

> `Queue.peek_generator()` and the high-level
> `Queue.peek(all_messages=True)` view return `CloseableIterator[...]`. Their
> thread ownership, terminal outcomes, synchronous Queue-operation exit, and
> early-close duty are [SB-DELIVERY-4]. The backend-facing
> `BrokerConnection.peek_generator()` remains an ordinary iterator seam; the
> public close contract is owned by the outer Queue operation.

Update [SB-API-1], [SB-API-4], and [SB-API-5] verification to name the public
export and static typing gates below.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Dependency-Ordered Tasks

### 1. Review and promote the contract before implementation

- Independently review this plan, its exact Proposed Spec Delta, the current
  Queue/session code, and both downstream pressure points.
- Before promotion, verify the named pressure still exists: inspect the current
  Weft helper that defensively closes `peek_generator()` and the current Taut
  plan/API pressure for `iter_log()`. If neither current consumer actually
  depends on early close, stop and re-evaluate whether a new public primitive
  is justified.
- Retain the single `[THEORY-6]` possession probe recorded in the Execution Log
  as this class-5 plan's closure gate. If later evidence overturns its placement
  or predicted bug class, rerun one replacement probe and record why; do not
  create a battery.
- Apply the strategy-A text to `docs/specs/11-delivery.md` and
  `docs/specs/16-python-library-api.md`; add live Related Plans backlinks but
  do not yet claim implementation files/tests that have not landed.
- Run the document and plan gates. Record the promotion baseline identifier in
  `## Spec Baseline`.
- Stop if review cannot state the persistent, ephemeral, and caller-owned
  cleanup scopes without the word "connection" standing in for all three.

Done signal: reviewed text is promoted, the exact baseline is recorded, and no
runtime code cites plan-only requirements.

### 2. Add the failing public type and export contract

- In `tests/test_queue_typing_contract.py`, import
  `CloseableIterator` from `simplebroker`; change only the
  `peek_generator()` and `peek(all_messages=True)` expectations, including
  timestamp and runtime-boolean overloads, and type-check direct `.close()`.
- In `tests/test_python_library_api_contract_sb_api.py`, require the root
  export and exact `[SB-API-1]/[SB-API-4]/[SB-API-5]` ownership language.
- Run the focused mypy/public-surface tests before implementation and record
  the expected failure: the root name and closeable annotations do not yet
  exist. A green pre-change run means the regression does not fire and blocks
  implementation until corrected.
- Define covariant `CloseableIterator[T]` in `simplebroker/sbqueue.py` as the
  minimal structural protocol, export it in `simplebroker/__init__.py`, and
  update the `Queue.peek()` and `Queue.peek_generator()` overloads and concrete
  return annotations. Update both method docstrings to teach lazy acquisition,
  same-thread ownership, terminal `close()`, and the caller's early-exit duty.
  Preserve the current generator body.
- Do not annotate `BrokerConnection.peek_generator()` with the new public
  protocol. Stop if mypy requires a runtime wrapper, a change to read/move
  annotations, or a backend protocol change.

Done signal: the red static/public-surface tests pass, ordinary Iterator
assignment remains valid, and no runtime path changed.

### 3. Bind lifecycle behavior on the real Queue seam

- Add `tests/test_peek_generator_lifecycle.py` as a `pytest.mark.shared`
  contract module using `queue_factory` and real broker targets.
- Parameterize persistent and ephemeral Queue modes. Cover:
  1. construction and close before first advancement acquire no operation;
     the iterator is terminal afterward and a later `next()` raises
     `StopIteration`;
  2. first yield leaves one operation active while suspended;
  3. explicit early close ends it before returning;
  4. natural exhaustion means advancing through `StopIteration` and ends it;
  5. a real first-advancement validation/iteration failure unwinds before its
     error escapes;
  6. close after exhaustion/failure and repeated close are safe;
  7. the Queue can be reused and closed immediately after iterator termination.
- Run lazy construction, early close, and exhaustion through the high-level
  `Queue.peek(all_messages=True)` path as well as the granular method. These
  rows use the same shared real-backend module; a typing assertion alone does
  not prove the convenience path returns the closeable runtime object.
- Use public reuse/close behavior plus mutation-sensitive ownership evidence.
  For persistent rows, assert the existing process-session active-operation
  count returns to baseline before the terminal action returns or raises. For
  each ephemeral terminal row, temporarily wrap the real
  `DBConnection.close()` method: call the original method, record completion
  only after it returns, and assert that record is present before early
  `close()`, `StopIteration`, or the advancement error returns to the caller.
  Assert unstarted close never calls it. This observer must retain the real
  Queue, `DBConnection`, adapter, and service; it may observe the exact instance
  identity but must not replace cleanup. Queue reuse remains supporting
  evidence because ephemeral `Queue.close()` is otherwise a no-op and a later
  operation would allocate a fresh manager.
- Keep Queue, session, backend adapter, and backend service real. Add two
  focused injected-delegate transition tests only for failures a healthy
  service cannot produce deterministically: one delegate yields once and then
  raises on the next `next()` attempt; the other raises from delegated
  `close()`. Run these two focused transitions once on real SQLite, each in
  persistent and ephemeral modes, by substituting only the delegated iterator
  beneath the otherwise real Queue/`DBConnection` path. Assert operation release
  or real-close-observer completion before the error escapes, and verify the
  original advancement/delegated-close error remains primary. Do not multiply
  the synthetic delegate across the PostgreSQL and Redis harnesses: those
  adapters are not exercised once their delegate is replaced. These tests
  supplement rather than replace the real-backend matrix. Real
  first-advancement failure remains part of every backend/mode row.
- Add one focused injected-runner ownership test through the real Queue path:
  terminating the iterator ends the lexical iterator operation while retaining
  the Queue-owned `DBConnection`/core until `Queue.close()`, and neither
  iterator termination nor Queue close calls runner `close()` or `shutdown()`.
  This is an ownership proof, not a fourth backend matrix.
- Run SQLite first, then the existing PostgreSQL and Redis/Valkey harnesses.
  Record whether the lifecycle tests start green as proof of current behavior;
  do not mislabel an already-correct runtime test as red-green TDD.
- Stop and re-plan if same-thread release fails on any adapter, if a service
  test needs sleeps or pool-size guesses to pass, or if an adapter change is
  required.

Done signal: every real lifecycle row fires in SQLite, PostgreSQL, and
Redis/Valkey for both Queue modes; the post-yield advancement and
delegated-close failures unwind in both ownership modes; every ephemeral
terminal row proves real `DBConnection.close()` completion before control
returns; the injected-runner handle/runner ownership remains unchanged; and no
process-session Queue operation is retained.

### 4. Align documentation and traceability

- Update `docs/implementation/06-process-session-core-ownership.md` with the
  outer Queue iterator ownership seam, persistent operation release, and the
  explicit non-transferable thread rule. Do not turn the implementation note
  into a second public contract.
- Update `docs/guides/python.md` with a `contextlib.closing()` early-exit
  example for `peek_generator()` and distinguish it from transactional batch
  rollback semantics.
- Update `docs/agent-kernel.md` and the root `README.md` so agents are told to
  exhaust or close an active peek iterator on its owner thread before closing
  the Queue/client. Preserve the existing warning against deleting while live
  offset iteration is active.
- Update `CHANGELOG.md` with the new root type and contract. Do not select or
  claim a release version in prose before the release owner does so.
- Add reciprocal spec implementation mappings, verification rows, and Related
  Plans backlinks only when their code/tests exist. Reconcile the plan against
  the promoted spec and leave the Deviation Log truthful.
- Run the eleven-principle agent-facing interface review. Any departure must be
  recorded in the governing spec, not only in review notes.

Done signal: theory, specs, plan, implementation rationale, teaching docs,
public export, code annotations, and tests form one navigable chain.

### 5. Prove consumer compatibility and obtain implementation review

- Search SimpleBroker, Weft, and Taut for `peek_generator()` overrides,
  adapters, casts, and test doubles. Confirm each real consumer can use the
  narrower return type and identify any override that returns a non-closeable
  iterator.
- Run focused Taut and Weft tests and their static checks against the local
  editable SimpleBroker. The Weft runtime selection must include
  `tests/system/test_helpers.py::test_iter_queue_entries_closes_underlying_generator_on_early_close`,
  the direct firing test for the named consumer pressure. Do not edit those
  repositories under this plan.
- Run the full SimpleBroker core, PostgreSQL, Redis, lint, formatting, and
  static-type gates after targeted checks pass.
- Obtain an independent completed-work review after the type/lifecycle slice
  and a final review before release readiness. Reviewers must inspect the
  proposed/promoted spec, Queue/session implementation, real-backend evidence,
  downstream scan, and any deviation rows.
- Stop if a supported consumer requires `send()`/`throw()`, a Queue subclass
  cannot satisfy the new contract without a breaking rewrite, or Weft/Taut
  evidence contradicts the assumed additive compatibility.

Done signal: all first-party and downstream gates pass, and every review
finding is accepted and fixed, rejected with evidence, or explicitly deferred
with a reopen condition.

### 6. Prepare release evidence and hand off to Taut

- Keep actual publication separately authorized. From a clean main-branch
  release candidate, use `bin/release.py` as the executable release owner;
  never hand-author a tag or bypass its exact-SHA gates.
- Select the next compatible core version with the owner. A public root type
  and lifecycle promise normally warrant a minor release, but the release
  driver and any co-released active work determine the exact version.
- Run the release driver dry run, required prechecks, exact-SHA hosted gates,
  and pre-publication `bin/packaging-smoke` before tag push. First add
  failing-first cases in `tests/test_dev_scripts.py`, then extend
  `simplebroker/_scripts.py::_smoke_install_artifacts()` so the root wheel and
  root sdist are installed in separate clean virtual environments and run the
  same root import plus real-SQLite early-close/reuse probe. Preserve the
  existing combined-wheel extension-discovery smoke. Run every artifact probe
  with its working directory outside the source checkout, remove inherited
  `PYTHONPATH`, and assert `simplebroker.__file__` resolves under that probe's
  temporary virtual environment. This prevents the checkout from shadowing the
  installed artifact because the helper's `_run()` otherwise defaults to the
  repository root.
- Add a mutually exclusive, version-parameterized published-artifact mode to
  the same `bin/packaging-smoke` owner. It must fetch the PyPI release metadata
  for exactly `simplebroker==VERSION`, require and download its root wheel and
  sdist, verify and print each SHA-256 against the index metadata, then run the
  same two clean-install probes from outside the checkout. Use the standard
  library network/hash facilities or an existing repository helper; do not add
  a release-only dependency. Pin parsing,
  download selection, digest mismatch, source-shadow rejection, and positive
  wheel/sdist signals in `tests/test_dev_scripts.py`. The exact post-publish
  command is:

  ```bash
  uv run ./bin/packaging-smoke \
    --python 3.11 --published-version VERSION
  ```

  Do not claim this runtime smoke proves static typing or real
  PostgreSQL/Redis behavior; those remain owned by mypy and the hosted service
  gates. Record the immutable commit, tag, version, printed artifact hashes,
  and positive wheel/sdist probe signals after publication.
- Verify no backend API or extension floor changed. If one did, revise the
  rollout and release the affected extension package before claiming the core
  contract usable across adapters.
- Give Taut the immutable release evidence and exact `[SB-DELIVERY-4]` /
  `[SB-API-1]/[SB-API-5]` references. Taut then owns its minimum dependency,
  lock, clean-install, and `iter_log()` lifetime verification.
- Close this plan only after the immutable released artifact and Taut handoff
  are recorded. Update the Status Index in the same change as the closure
  claim. If publication is not authorized, leave the plan active rather than
  weakening its endpoint to release readiness.

Done signal: Taut can depend on an immutable public contract rather than the
7.4.1 implementation detail.

## Testing Plan

| Contract element | Primary proof | Supporting proof |
|------------------|---------------|------------------|
| Root `CloseableIterator` export and minimal shape | `tests/test_python_library_api_contract_sb_api.py` | clean-wheel import probe |
| `peek_generator()` literal overloads | `tests/test_queue_typing_contract.py` under mypy | downstream mypy scans |
| `peek(all_messages=True)` and runtime-`bool` overloads | `tests/test_queue_typing_contract.py` under mypy | shared high-level lazy/early-close/exhaustion runtime rows |
| Lazy unstarted close | shared real-backend lifecycle test | operation count remains unchanged |
| Suspended active operation | shared real-backend lifecycle test | persistent active-operation count |
| Early same-thread close | shared real-backend lifecycle test | immediate Queue reuse/close |
| Exhaustion through `StopIteration` | shared real-backend lifecycle test | terminal repeated close |
| Real first-advancement failure unwind | shared real-backend lifecycle test in each ownership mode | persistent active-operation count or completed real-close observer; primary error retained |
| Post-yield advancement failure unwind | one focused injected delegate in each ownership mode | outer scope released before the original advancement error escapes |
| Delegated-close failure unwind | one focused injected delegate in each ownership mode | outer scope released before the close-only delegated error escapes |
| Injected-runner ownership | one focused Queue test with a caller-owned runner | cached handle retained through iterator close; Queue close releases its handle but never the runner |
| Live offset paging and non-claiming | existing [SB-DELIVERY-4] tests | unchanged CLI/kernel contract tests |
| SQLite, PostgreSQL, Redis/Valkey parity | `pytest`, `bin/pytest-pg`, `bin/pytest-redis` with the shared module | hosted release gates |
| Weft/Taut compatibility | local-editable downstream type and focused runtime tests | source scan for overrides/casts |
| Built root wheel and sdist | separate outside-checkout clean installs through `bin/packaging-smoke` | root import origin plus real SQLite early-close/reuse signal |
| Published root wheel and sdist | `bin/packaging-smoke --published-version VERSION` | PyPI digest match, printed SHA-256, and the same outside-checkout runtime signals |

Anti-mocking rule: do not replace Queue, process sessions, backend adapters, or
PostgreSQL/Redis services in the backend parity proof. The ephemeral observer
must call the real `DBConnection.close()` and record only after it returns.
Limited delegate fault injection is allowed only for post-yield advancement and
delegated-close failures that healthy real services cannot produce on demand;
it supplements rather than substitutes for real lifecycle tests.

Relevant adversarial floors are public importability, no private cast needed,
every documented lifecycle state firing, and clean behavior when the caller
stops after one row. CLI encoding, grammar, output-path, and exit-code probes
are not affected; run their existing regression owners in the full suite rather
than inventing new CLI cases.

## Verification and Gates

### Plan and documentation gates

```bash
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

### Targeted core gates

```bash
uv run --extra dev pytest -q \
  tests/test_peek_generator_lifecycle.py \
  tests/test_queue_typing_contract.py \
  tests/test_python_library_api_contract_sb_api.py \
  tests/test_delivery_contract_sb_delivery.py \
  tests/test_agent_kernel_contract.py \
  tests/test_dev_scripts.py \
  tests/test_release_script.py \
  tests/test_release_workflow.py
uv run --extra dev mypy simplebroker \
  tests/test_queue_typing_contract.py \
  tests/test_python_library_api_contract_sb_api.py \
  tests/test_peek_generator_lifecycle.py \
  --config-file pyproject.toml
```

### Real service-backed gates

```bash
uv run --extra dev ./bin/pytest-pg --fast -n0 -q tests/test_peek_generator_lifecycle.py
uv run --extra dev ./bin/pytest-redis --fast -n0 -q tests/test_peek_generator_lifecycle.py
```

### Downstream gates

From `../taut`:

```bash
uv run --extra dev --with-editable ../simplebroker pytest -q \
  tests/test_client.py tests/test_search_client.py
uv run --extra dev --with-editable ../simplebroker mypy taut --config-file pyproject.toml
```

From `../weft`:

```bash
uv run --extra dev --with-editable ../simplebroker pytest -q \
  tests/commands/test_queue.py \
  tests/commands/test_result.py \
  tests/system/test_helpers.py::test_iter_queue_entries_closes_underlying_generator_on_early_close
uv run --extra dev --with-editable ../simplebroker mypy weft --config-file pyproject.toml
```

### Final local gates

```bash
uv run --extra dev pytest
uv run --extra dev ./bin/pytest-pg
uv run --extra dev ./bin/pytest-redis
uv run --extra dev ruff check simplebroker tests extensions/simplebroker_pg extensions/simplebroker_redis
uv run --extra dev ruff format --check simplebroker tests extensions/simplebroker_pg extensions/simplebroker_redis
uv run --extra dev mypy simplebroker \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis \
  --config-file pyproject.toml
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

### Artifact and release gates

From the exact release candidate SHA, before tag push:

```bash
uv run ./bin/packaging-smoke --python 3.11
```

After the owner-authorized publication, replacing `VERSION` with the immutable
published core version:

```bash
uv run ./bin/packaging-smoke \
  --python 3.11 --published-version VERSION
```

Success means every command exits zero with non-empty collection where
applicable, all three real adapters execute the shared lifecycle module, the
downstream checks use the local changed package rather than their locked old
version, both clean artifact probes resolve `simplebroker.__file__` inside their
temporary environments, published digests match the index, and no
skipped-everything run is counted as evidence.

Before publication, rerun the release driver's exact command set from the
chosen release identifier. The driver and release-gate workflows outrank the
command snapshot above if they have evolved. A dry run proves command shape,
not release readiness.

## Agent-Facing Interface Review

Surface and baseline: public Python library API plus its agent-facing structured
documentation, reviewed against baseline `36bc6d4d0c079928ef051ea7129c78245c2ee058`
and reconciled against the implemented delta. The implemented overload and
outer-generator surfaces begin at `simplebroker/sbqueue.py:770` and
`simplebroker/sbqueue.py:1050`; the package export list begins at
`simplebroker/__init__.py:33`.

| Principle | Disposition and evidence |
|-----------|--------------------------|
| 1. Context is the scarcest resource | Met: one three-method structural type carries the lifecycle affordance without a wrapper or second API (`simplebroker/sbqueue.py:52`; `docs/specs/16-python-library-api.md:29`). |
| 2. Progressive disclosure | Met: the method docs teach the immediate duty (`simplebroker/sbqueue.py:1091`), the Python guide gives the early-exit pattern (`docs/guides/python.md:71`), and the ownership note explains the lower seam (`docs/implementation/06-process-session-core-ownership.md:62`). |
| 3. Self-explanatory names; no lookup tables | Met: `CloseableIterator` names the one added capability (`simplebroker/sbqueue.py:55`), while the public contract uses Queue-operation rather than false universal connection language (`docs/specs/11-delivery.md:95`). |
| 4. One identity per thing | Met: the existing `peek_generator()` and `peek(all_messages=True)` surfaces remain the only identities; no parallel iterator API was added (`simplebroker/sbqueue.py:770`; `simplebroker/sbqueue.py:1082`). |
| 5. Derive what is derivable | Met: both return annotations expose `.close()` directly and retain ordinary `Iterator` assignment, so callers need no cast or `getattr` probe (`simplebroker/sbqueue.py:794`; `simplebroker/sbqueue.py:1050`; `tests/test_queue_typing_contract.py:90`). |
| 6. No hidden session setup | Met: the outer Python generator enters `get_connection()` only when advanced (`simplebroker/sbqueue.py:1119`), and the contract makes that lazy boundary and owner thread explicit (`docs/specs/11-delivery.md:95`). |
| 7. Teach, don't reject | Not applicable to input normalization: this read-only return-type/lifecycle delta adds no new input or rejection. The planned method docs instead teach early-close use (`docs/plans/2026-08-24-peek-generator-close-contract-plan.md:447`). |
| 8. Every message carries its action | Met: the contract and kernel tell early-exit callers to close on the owner thread before Queue/client close (`docs/specs/11-delivery.md:109`; `docs/agent-kernel.md:125`). |
| 9. Atomic writes with a recovery path on conflict | Not applicable: peek remains observation and the delta adds no write (`docs/specs/11-delivery.md:85`; `docs/plans/2026-08-24-peek-generator-close-contract-plan.md:217`). |
| 10. Draw the trust boundary in the interface | Met: Queue owns operation exit, while persistent and caller-injected resource owners retain their scopes (`simplebroker/sbqueue.py:287`; `docs/specs/11-delivery.md:115`; `docs/implementation/06-process-session-core-ownership.md:62`). Publication remains an explicit later owner action under Task 6. |
| 11. Wire format matches the agent's mental model, not storage | Met: the public delta speaks in Queue-operation and owner-thread terms (`docs/specs/11-delivery.md:95`); storage-specific ownership remains in the implementation note (`docs/implementation/06-process-session-core-ownership.md:70`). |

Enumerable delta: one root type, two Queue return surfaces, three terminal
actions, and three ownership modes. Their firing owners are enumerated in the
Testing Plan and implemented in `tests/test_queue_typing_contract.py:31` and
`tests/test_peek_generator_lifecycle.py:88`.
No error-code, flag-value, record-shape, or taxonomy set changes
(`docs/plans/2026-08-24-peek-generator-close-contract-plan.md:217`).

| ID | Severity | Location (file:line) | Finding | Suggested disposition |
|----|----------|----------------------|---------|-----------------------|
| IR-01 | P1 | `tests/test_peek_generator_lifecycle.py:68` | Public Queue reuse could false-green an ephemeral leak because later operations allocate a fresh manager. | Resolved: the mutation-sensitive observer calls real `DBConnection.close()` and records only after return. |
| IR-02 | P1 | `tests/test_peek_generator_lifecycle.py:248` | First-advance failure did not fire the suspended-active to later-`next()`-failure transition. | Resolved: the focused post-yield delegate transition fires in both ownership modes. |
| IR-03 | P2 | `simplebroker/_scripts.py:1207` | The baseline packaging smoke neither installed the sdist nor prevented checkout shadowing, so it could not prove the advertised artifact. | Resolved: separate outside-checkout wheel/sdist probes scrub `PYTHONPATH`, assert import origin, and support versioned published-artifact hashes; firing tests start at `tests/test_dev_scripts.py:2768`. |
| IR-04 | P2 | `tests/test_peek_generator_lifecycle.py:287` | Cleanup and primary-error precedence was under-specified for combined failures. | Resolved: delegated/advancement errors remain primary while the outer Queue scope unwinds; the public contract adds no cleanup-only error promise. |

Ratified judgment calls (challenged, upheld): use a minimal structural Protocol,
not `Generator` (`simplebroker/sbqueue.py:52`); keep the contract at the outer
Queue seam and the backend Protocol at `Iterator`
(`docs/specs/16-python-library-api.md:229`); keep cross-thread use unsupported
(`docs/specs/11-delivery.md:95`); prove the public rule on all three released
adapters; use two narrow SQLite-only injected delegates only for otherwise
unreachable transitions (`tests/test_peek_generator_lifecycle.py:248`).

Verdict: **no blocker in the implemented interface**. The implementation keeps
the ratified shape and has passed its enumerable type, lifecycle,
real-backend, downstream, and artifact gates. Final completed-work review and
owner-authorized publication remain separate gates.

Runbook feedback: no new reusable candidate. The existing ownership-boundary,
mental-model, enumerable-contract, and black-box evidence rules exposed every
interface issue found in this review.

## Independent Review Loop

Plan review must use a reviewer that did not author the plan. The reviewer
reads this file, its Proposed Spec Delta, the baseline specs, `Queue` and
process-session lifecycle code, the three backend harnesses, and the Taut/Weft
consumer evidence. Required stance:

> Existence-check every named path, seam, command, and return-type claim. Could
> you implement this plan confidently and correctly after strategy-A promotion,
> and would it avoid degrading lifecycle safety or backend compatibility? Look
> especially for an accidental cross-thread promise, connection-ownership
> overclaim, missing adapter, static type break, weak mocked proof, or process
> ceremony that can be removed. Return PASS or BLOCKED and tie any blocker to
> implementability or robustness.

The author records each finding below and either updates the plan, rejects it
with evidence, or marks it out of scope with a reopen condition. A BLOCKED
verdict prevents implementation. Repeat review after any revision that changes
ownership, invariants, blast radius, or promotion strategy.

### Review Log

| Date | Reviewer | Verdict / finding | Disposition |
|------|----------|-------------------|-------------|
| 2026-08-24 | Hegel, fresh-eyes reader | Ambiguity: ephemeral and injected-runner cleanup scopes, injected failure coverage, and later synthetic-adapter scope needed exact answers. Final reread: **PASS**. | Accepted. The plan now separates all three ownership modes, names two injected transitions, requires real-close observation, and limits synthetic delegates to SQLite in both ownership modes. |
| 2026-08-24 | Meitner, ambiguity audit | Initial findings: release ordering, loop-body exceptions, unstarted-close terminality, high-level runtime coverage, docstrings, cleanup precedence, direct Weft coverage, artifact smoke gaps/source shadowing, and optional-dev command assumptions. Final verdict: **PASS**. | Accepted and incorporated in Decisions, Invariants, Tasks 2–6, Testing Plan, and executable gates. No remaining ambiguity on final reread. |
| 2026-08-24 | Kepler, independent plan review | **BLOCKED** on PK-01 ephemeral mutation-sensitive proof and PK-02 missing post-yield advancement failure; P2 corrections for `_ProcessBrokerSession` and the `[THEORY-6]` possession probe. | Accepted. The plan now calls real `DBConnection.close()`, fires the post-yield transition, names the real seam, and records one possession probe. Final rereview required below. |
| 2026-08-24 | Kepler, independent plan rereview | **PASS** (9/10 confidence); no findings. Residual risk remains unsupported cross-thread close, best-effort substrate destruction, source-owned static typing proof, and possible third-party override annotation updates. | Required independent plan review is complete. Retain these residuals as implementation and downstream review targets. |
| 2026-08-24 | Independent Strategy-A promotion review | **PASS**; the promoted text exactly matches the reviewed delta, preserves Queue/backend ownership, adds no premature implementation or verification claims, and retains the recorded diff hash. | Runtime and failing-test work may begin from the recorded promotion baseline. |
| 2026-08-24 | Hegel, independent final implementation review | **PASS**; no actionable P0-P2 findings. The type delta is limited to the two planned Queue surfaces, the backend seam and generator body are unchanged, real lifecycle evidence is baseline-relative and ownership-sensitive, artifact origin/hash checks are enforced, and docs/traceability align. | Accepted without code changes. Retain the explicit residuals: cross-thread use is unsupported; unknown third-party Queue subclasses may need annotation updates; immutable PyPI hashes and the Taut handoff remain post-publication gates. |

## Execution Log

Record comprehension answers, red/green evidence, promotion baseline,
per-backend results, downstream commands, review dispositions, release
identifier, and residual risk here as implementation proceeds. Do not record
transient staging/worktree claims as durable completion evidence.

- **2026-08-24 `[THEORY-6]` possession probe:** Prompt: “A downstream log API
  needs to stop a live Queue traversal early. Which project owns deterministic
  iterator cleanup, which project owns log decoding/version adoption, and what
  bug class should an audit find if the boundary is implicit?” Outcome:
  SimpleBroker owns the closeable outer Queue-operation contract because it
  creates and releases that operation across all adapters; Taut owns
  `iter_log()`, decoding, and its dependency floor. A page/snapshot or daemon
  API is not justified by this pressure. Predicted bug class: an early-stopped
  iterator retains an active operation, causing delayed Queue close or a
  cross-thread ownership mismatch. The review did find that public reuse alone
  could hide an ephemeral retained core, so the plan now requires a
  mutation-sensitive real-close observer. Result: **possession demonstrated**;
  rerun only if implementation evidence changes the ownership boundary.
- **2026-08-24 type-feasibility probe:** a minimal covariant structural
  `CloseableIterator` with `__iter__`, `__next__`, and `close()` type-checked as
  an `Iterator`, and an existing Python generator body satisfied the annotated
  return without requiring `send()`, `throw()`, a wrapper, or a backend protocol
  change. This is authoring evidence, not an implementation gate result.
- **2026-08-24 comprehension gates:** (1) persistent termination ends the
  owner-thread process-session operation but retains cached resources;
  ephemeral termination exits and closes its operation-owned `DBConnection`;
  injected-runner termination ends only the lexical iterator operation and
  leaves the Queue-owned handle until `Queue.close()` without closing the
  runner. (2) The first advancement attempt, not construction, establishes the
  owner thread; unstarted close is terminal without acquisition. (3) The outer
  Queue generator owns the operation, so the backend iterator remains an
  ordinary `Iterator`. (4) Exhaustion requires the `next()` call that observes
  `StopIteration`; receiving the last row is still suspended. (5) Queue,
  process-session behavior, adapters, services, and the observed
  `DBConnection.close()` stay real; only two unreachable delegate failures are
  injected. (6) Source mypy proves typing, separate wheel/sdist installs prove
  artifact import and SQLite lifetime behavior, and hosted service gates prove
  PostgreSQL/Redis parity. Result: all answers match the named owners; runtime
  and test edits may begin after independent promotion review.
- **2026-08-24 Strategy-A promotion:** promoted the reviewed text in
  `docs/specs/11-delivery.md` and `docs/specs/16-python-library-api.md` without
  adding not-yet-existing verification claims. Baseline is commit
  `36bc6d4d0c079928ef051ea7129c78245c2ee058` plus spec-diff SHA-256
  `0dd4d54b67c21d5e73331f00c51f4d04100a6c8a62a1401e085158390f1b6b6c`;
  DOM-15, plan-context, doc-path, and diff-check gates passed.
- **2026-08-24 public-type red/green:** the first mypy run failed with nine
  errors: the root export was absent, both peek surfaces still inferred
  `Iterator`, and `.close()` was unavailable. After adding the minimal
  structural Protocol, root export, and peek-only annotations, the focused
  mypy gate passed. Read, move, stream, backend annotations, and the
  `peek_generator()` body remain unchanged. A second public-contract tracer
  failed on missing method lifecycle teaching, then passed after both Queue
  docstrings named lazy acquisition, same-thread use, and early close.
- **2026-08-24 lifecycle evidence:** the new shared module started green
  against the existing runtime implementation. SQLite passed 19 cases;
  PostgreSQL and Redis/Valkey each collected and passed 14 shared non-SQLite
  cases. The matrix covers both Queue ownership modes, real-close completion,
  active-operation release, high-level `peek(all_messages=True)`, first-advance
  failure, post-yield failure, delegated-close failure, and injected-runner
  ownership. Focused mypy and Ruff checks passed after formatting. No runtime
  wrapper, backend edit, or adapter-specific behavior change was required.
- **2026-08-24 artifact red/green:** six failing-first packaging cases exposed
  the prior single-wheel-only path and the absent exact-version, hash,
  published-selection, and source-origin gates. The implementation preserves
  the combined extension-wheel discovery smoke, then installs the root wheel
  and root sdist in separate environments outside the checkout with inherited
  `PYTHONPATH` removed. Each probe asserts `simplebroker.__file__` is under its
  temporary virtual environment, imports `CloseableIterator`, and exercises
  early close plus Queue reuse on real SQLite in both ephemeral and persistent
  modes. The focused packaging tests and full `tests/test_dev_scripts.py`
  passed. `uv run ./bin/packaging-smoke --python 3.11` then built all six
  release artifacts and produced positive root-wheel and root-sdist signals
  for the current pre-publication `7.4.1` checkout artifact. The new
  `--published-version X.Y.Z` path verifies indexed SHA-256 values and runs the
  same two probes; it cannot produce immutable post-publication evidence until
  an owner authorizes and publishes the new contract release. A black-box
  malformed-pin probe exits `1` with one diagnostic and no traceback.
- **2026-08-24 downstream compatibility:** Taut's focused client/search tests
  and `mypy taut` passed with `--with-editable ../simplebroker`. Weft's queue,
  result, and direct early-close helper tests plus `mypy weft` passed through
  the same editable override. The source scan found no production Queue
  subclass override; several Weft test doubles and existing defensive
  `cast`/`getattr(close)` paths remain, but none required a widening fix and
  full downstream typing passed. No downstream repository was edited, and the
  narrower public type remained additive for both consumers.
- **2026-08-24 broad verification:** the targeted core contract set passed;
  focused mypy passed over 46 source files. The full SQLite/core suite passed
  3,050 tests with 17 platform/opt-in skips. The PostgreSQL harness passed
  1,337 shared core tests and 183 extension tests; the Redis/Valkey harness
  passed 1,330 shared core tests and 263 extension tests. Full scoped Ruff
  check, Ruff format, and mypy passed. DOM-15, plan-context, doc-path, and
  `git diff --check` gates passed. The public interface walk was reconciled
  against the implemented code with no blocker and no new runbook candidate.
- **2026-08-24 closure:** the owner confirmed the implementation plan is
  complete. The public type, Queue annotations, lifecycle proofs, artifact
  smoke, documentation, downstream compatibility evidence, and independent
  review are all present. Version selection, publication, immutable PyPI
  artifact verification, and downstream adoption are release operations, not
  open implementation work under this plan.

## Out of Scope

- Cross-thread close, transferable operation leases, foreign-thread recovery,
  or extending transactional-generator poisoning to peek.
- A page-oriented, snapshot, fixed-start, live-rescan, or otherwise exhaustive
  traversal interface.
- Return-type changes for `read_generator()`, `move_generator()`,
  `stream_messages()`, or backend-facing generator methods.
- A context-manager wrapper or a second `iter_peek()`/page API.
- Closing persistent process-session resources or caller-injected runners when
  a peek iterator terminates.
- Backend API version changes, extension packaging changes, storage migrations,
  or new dependencies unless a stop gate triggers replanning.
- Editing Taut or Weft, choosing Taut's upper-bound policy, or implementing
  Taut `iter_log()`.
- Unrelated generator, watcher, CLI, or documentation cleanup.

## Fresh-Eyes Review

Before implementation starts and again before closure, a fresh reader must be
able to answer all of these from the plan without prior conversation:

- Which public type and two Queue return surfaces change?
- Exactly when is the iterator lazy, active, and terminal?
- Which thread owns close, and what does cross-thread use promise?
- What is released for ephemeral, persistent, and injected-runner modes?
- Why does the backend protocol not change?
- Which lifecycle rows run against which real adapters?
- What evidence blocks release and what evidence is handed to Taut?
- Which changes force replanning rather than local improvisation?

Cut any gate or abstraction that does not help answer one of those questions or
protect a named invariant. Add no new requirements during fresh-eyes review
unless they close a concrete correctness gap.
