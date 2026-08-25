# Closeable Queue Iterator Contract Plan

Status: completed
Class: 5 - normative delivery and Python API text changes, and the public
return-type compatibility surface changes. The public-contract risk trigger
also fires, so the hardening requirements apply even though the runtime delta
is deliberately small.
Plan type: implementation with spec revision

## Goal

Make the existing cleanup capability of Queue-owned read, move, and stream
iterators official. `Queue.read_generator()`, `Queue.move_generator()`,
`Queue.stream_messages()`, and the high-level `all_messages=True` read/move
views will return the existing package-root `CloseableIterator` protocol.
Same-thread `close()` continues to unwind the existing outer Queue generator;
this plan adds no recovery mechanism, transferable lease, backend protocol, or
runtime wrapper. The only runtime correction is deterministic close forwarding
through the high-level move result-shaping generator.

## Source Documents

Source specs:

- `docs/specs/11-delivery.md` [SB-DELIVERY-5], [SB-DELIVERY-6]
- `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-4], [SB-API-5]
- `docs/specs/product-section-registry.md` (delivery and Python library rows)

Theory and rationale:

- `docs/program-theory.md` [REV-THEORY-005]: suspended operations retain
  their ownership context; cleanup does not transfer ownership or claim
  recovery across threads.
- `docs/implementation/04-cross-thread-finalization-poisoning.md`
- `docs/implementation/06-process-session-core-ownership.md`

Predecessor and guidance:

- `docs/plans/2026-08-24-peek-generator-close-contract-plan.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`

Consulted implementation and test surfaces:

- `simplebroker/sbqueue.py`
- `simplebroker/db.py`
- `simplebroker/_backend_plugins.py`
- `tests/test_queue_typing_contract.py`
- `tests/test_delivery_contract_sb_delivery.py`
- `tests/test_queue_api_additions.py`
- `tests/test_watcher_thundering_herd.py`
- `tests/test_cross_thread_finalization_poisoning.py`
- `tests/test_generator_methods.py`

## Decisions

1. Reuse the existing `CloseableIterator[T]` structural protocol. Do not add a
   second protocol, context-manager wrapper, or `Generator[...]` return type.
2. Apply it at the public Queue seam to:
   - `read_generator()`;
   - `move_generator()`;
   - `stream_messages()`;
   - `read(all_messages=True)` and its unknown-`bool` union;
   - `move(all_messages=True)` and its unknown-`bool` union.
3. Keep `BrokerConnection.claim_generator()`, `peek_generator()`, and
   `move_generator()` typed as ordinary `Iterator[...]` methods. The outer
   Queue generator owns the public Queue-operation lifecycle guarantee.
4. Preserve every generator body except the high-level
   `move(all_messages=True)` result-shaping generator. That wrapper must retain
   its delegated `move_generator()` in a local variable and close it in
   `finally`, because an ordinary `for` loop does not forward `close()` by
   protocol. Do not rely on CPython reference-count finalization.
5. Make the same-thread rule explicit for all affected Queue iterator modes.
   Exactly-once modes still commit each item before yield and hold no open
   transaction across the yield, but their outer Queue operation context still
   remains suspended. Persistent shared Queues additionally retain a
   thread-local process-session lease; ephemeral and injected-runner Queues
   retain their existing, distinct operation scopes. At-least-once SQL modes
   additionally hold the current batch transaction and retain the existing
   poison safety net.
6. Keep `stream_messages()` as one mixed-mode helper. Its `peek`, one-message,
   exactly-once, and at-least-once branches already close delegated iterators
   in `finally`; this plan changes their public return type and documentation,
   not their dispatch or settlement semantics.

## Context and Key Files

### Current public seam

`simplebroker/sbqueue.py` already defines and exports
`CloseableIterator[T]` with only `__iter__`, `__next__`, and `close()`. It is a
structural subtype of ordinary iterator use and promises neither `send()` nor
`throw()`.

`Queue.read_generator()` and `Queue.move_generator()` are outer Python
generators. Each validates its arguments, enters `Queue.get_connection()`, and
delegates with `yield from`. Same-thread `close()` therefore already closes the
delegated generator and exits the Queue context synchronously. Their overloads
and concrete return annotations still say `Iterator[...]`.

`Queue.stream_messages()` is also an outer Python generator under one
`Queue.get_connection()` context. Every branch already closes its delegated
iterator in `finally`. Its annotation still says
`Iterator[tuple[str, int]]`. In the `all_messages=False` branches, receiving
the one yielded row does not exhaust the outer generator; a later advance or
explicit close is still required to exit the operation.

`Queue.read(all_messages=True)` returns `read_generator()` directly. The
high-level `Queue.move(all_messages=True)` path instead returns a nested
`dict_generator()` that converts tuples to `MovedMessage` dictionaries. That
nested generator currently iterates over `move_generator()` without an
explicit close-forwarding `finally`.

### Ownership and poison

`Queue.get_connection()` is the operation scope. For persistent Queue handles,
its `finally` invokes `DBConnection.release_connection_after_use()`, which pops
the process-session operation from the current thread's thread-local stack.
For no-runner ephemeral handles, context exit closes the operation-owned
`DBConnection`. A Queue with a caller-injected runner retains its Queue-owned
core handle until `Queue.close()` and never transfers runner ownership.

`BrokerCore._yield_transactional_batches()` owns SQL at-least-once rollback,
lock release, and foreign-thread poison publication. Exactly-once generators do
not use that lock-across-yield path. This plan exposes the supported
same-thread close action; it does not change the unsupported foreign-thread
path or make poison a general iterator mechanism.

### Existing proof owners

- `tests/test_queue_typing_contract.py` currently pins read and move results as
  ordinary iterators and has no stream return assertion. It is the
  failing-first owner for correcting those expectations, adding the stream
  shape, and proving `contextlib.closing()` compatibility.
- `tests/test_delivery_contract_sb_delivery.py` is already shared across real
  SQLite, PostgreSQL, and Redis/Valkey harnesses. It owns the smallest
  parameterized public Queue proof that an active iterator releases its Queue
  operation on same-thread close.
- `tests/test_queue_api_additions.py` already uses a retained delegated
  generator to prove high-level one-item move closes its helper. Extend that
  exact local pattern to prove the `all_messages=True` shaping wrapper forwards
  close without depending on garbage collection.
- `tests/test_watcher_thundering_herd.py::RecordingQueue` is the one known
  first-party subclass override affected by the tightened
  `stream_messages()` return type. Its annotation must change with the base
  method and must be included in focused mypy.
- `tests/test_generator_methods.py` already proves exactly-once settlement and
  at-least-once rollback behavior. `tests/test_cross_thread_finalization_poisoning.py`
  owns foreign-thread SQL poison and the persistent operation-lease residual.
  Preserve and rerun them; do not duplicate those state machines.

### Comprehension gates

Before editing, record answers in the Execution Log. A wrong answer blocks
implementation until the cited owner is reread.

1. **Why is `CloseableIterator` valid without a runtime wrapper?** Expected
   answer: the affected public methods return outer Python generators, which
   already implement `close()`; `yield from` or existing `finally` blocks
   unwind their delegated iterators and the enclosing `Queue.get_connection()`
   scope on the same thread.
2. **Why does same-thread ownership apply to exactly-once as well as
   at-least-once?** Expected answer: both modes retain the outer Queue operation
   context while suspended. A persistent shared Queue additionally retains its
   thread-local process-session lease; an ephemeral Queue retains its
   operation-scoped `DBConnection`; an injected-runner Queue retains its
   borrowed-core operation scope without a shared-session lease. Only
   at-least-once SQL mode additionally suspends a transaction and held core
   lock, so only that mode uses the poison latch.
3. **Why does high-level move need the sole runtime edit?** Expected answer:
   its tuple-to-dictionary adapter uses an ordinary `for` loop. Closing that
   outer generator does not, by iterator protocol, call `close()` on the inner
   iterator. CPython may finalize an otherwise unreferenced inner generator,
   but the public contract must not depend on garbage collection.

## Invariants and Constraints

- Delivery semantics do not change. Exactly-once still commits before each
  yield. At-least-once still commits after a complete batch and rolls back an
  unfinished batch on graceful same-thread close.
- Move remains an atomic reservation/routing operation. Message identity,
  claimed state, destination selection, and record shapes do not change.
- Iterator construction remains lazy with respect to Queue-operation
  acquisition. Validation that already occurs before operation entry remains
  where it is.
- Once an affected iterator has entered its Queue operation, advance,
  exhaustion, and explicit close remain same-thread actions. A caller-side
  exception does not close a suspended iterator; callers still close in
  `finally` or with `contextlib.closing()`.
- Close before first advancement starts no Queue operation and leaves the
  single-use Python generator terminal. Repeated close after a terminal state
  remains safe.
- Synchronous operation exit does not mean unconditional physical connection
  destruction. Persistent process-session resources and caller-owned runners
  retain their existing owners.
- Foreign-thread close remains undefined behavior. Do not transfer session
  leases, roll back from a foreign thread, clear poison, or add recovery.
- Backend-facing Protocol annotations remain `Iterator[...]`; no backend API
  version or extension release is required.
- `CloseableIterator` remains assignable to `Iterator`; no `send()`, `throw()`,
  async iteration, context-manager method, or concurrency promise is added.
- No new module, dependency, helper abstraction, or parallel implementation
  path is permitted. Reuse direct `.close()` and the existing
  `_close_iterator()` helper only where each already fits.
- Do not add custom cleanup-error arbitration. The high-level move delegate
  closes under ordinary Python `finally` semantics, so a delegated close
  failure follows normal Python exception precedence. Do not promise that an
  error previously left to implicit finalization remains unobservable after
  close forwarding becomes deterministic.

## Compatibility, Rollback, and Rollout

The return-type change is additive for callers because every
`CloseableIterator[T]` remains usable as `Iterator[T]`. A third-party `Queue`
subclass that annotates an override with only `Iterator[...]` may need to
tighten that annotation. Search first-party and known downstream subclasses
and record the result; do not add a compatibility shim.

There is no storage migration, backend command change, or one-way data door.
Before release, the spec, annotations, close-forwarding `finally`, tests, and
docs can be reverted together. After a release advertises the stronger return
type and cleanup contract, removing it is a breaking public change; defects are
fixed forward in a later compatible release.

Implementation rollout is one atomic source release slice after verification.
No separate backend package release is needed because backend protocols and
adapter code do not change. This plan does not authorize publication. The
existing release driver owns later version selection, exact-SHA gates, and
publication.

Success after release is observable without new telemetry: installed-package
mypy accepts `.close()` and `contextlib.closing()` on every named Queue
surface; same-thread early close permits immediate Queue reuse/close; existing
SQL foreign-finalization probes still poison only the at-least-once path. A
downstream override-type failure or retained Queue operation after supported
same-thread close is a release blocker.

## Spec Baseline

- `0694c769972689482f72287563fa9ad08832889c` -
  `docs/specs/11-delivery.md` and `docs/specs/16-python-library-api.md` at plan
  authoring time.
- Plan type: implementation with spec revision.
- Promotion baseline: `81b80403b963c4332297b6946085d806be535b8e` - reviewed
  strategy-B atomic contract/implementation slice.

## Proposed Spec Delta

Promotion strategy: **B - atomic**. After independent review, land the exact
normative text, Queue annotations, local high-level move `finally`, firing
tests, implementation-note updates, reciprocal links, and verification rows as
one coherent source change. Failing-first tests may be observed during the
working sequence, but no partial spec-only or code-only slice is complete or
landable. This keeps the small public-contract correction together and avoids
temporary reciprocity debt.

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/11-delivery.md` | B - atomic | Replace [SB-DELIVERY-6] heading and body |
| `docs/specs/16-python-library-api.md` | B - atomic | Replace the closeable-return paragraphs in [SB-API-4] and [SB-API-5] |

### [SB-DELIVERY-6] - replace the current section heading and body

> ## Queue iterator ownership [SB-DELIVERY-6]
>
> `Queue.read_generator()`, `Queue.move_generator()`,
> `Queue.stream_messages()`, and the high-level
> `Queue.read(all_messages=True)` and `Queue.move(all_messages=True)` views
> return single-use closeable iterators. Create, advance, exhaust, and close an
> affected iterator on the same thread. Creating a returned generator starts no
> Queue operation. Its first advancement performs any lazy validation and, if
> operation entry succeeds, retains that Queue operation until a terminal
> advance or explicit close.
>
> An active iterator must be advanced, exhausted, and explicitly closed on its
> creation thread in every delivery mode. Advancing through `StopIteration`, an
> advancement failure after operation entry, or explicit same-thread `close()`
> synchronously exits the iterator-owned Queue operation before that action
> returns or raises. An exception in the caller's loop body does not terminate
> the iterator; a caller that may stop early must close it in `finally` or with
> `contextlib.closing()`. Closing on the creation thread before first
> advancement acquires no Queue operation and leaves the single-use iterator
> terminal; repeated close after a terminal outcome is safe.
>
> The same-thread rule applies to `"exactly_once"` even though its item is
> committed before yield: the suspended outer Queue iterator still owns its
> Queue operation context. A persistent shared Queue additionally retains a
> thread-local process-session operation lease; an ephemeral Queue owns its
> operation-scoped connection/core handle; and a Queue with a caller-supplied
> runner retains its Queue-owned borrowed core without a shared-session lease.
> In `"at_least_once"` mode, graceful same-thread close of an unfinished batch
> additionally rolls back the open batch as specified by [SB-DELIVERY-5].
> Operation exit does not transfer or destroy resources owned by another
> lifecycle.
>
> Crossing threads remains undefined behavior. SQL-backed
> `"at_least_once"` generators may publish permanent poison and require process
> restart rather than touching owner-thread lock or transaction state from a
> foreign thread. Exactly-once and non-SQL modes need not share that diagnostic
> mechanism. Poison is a safety net, not recovery and not a supported
> multi-thread API.
>
> _Implementation mapping_:
> - `simplebroker/db.py`
> - `simplebroker/sbqueue.py`
> - `docs/implementation/04-cross-thread-finalization-poisoning.md`
> - `docs/implementation/06-process-session-core-ownership.md`
> - `extensions/simplebroker_redis/simplebroker_redis/core.py`

### [SB-API-4] - replace the current closeable-return paragraph

> The `all_messages=True` views of `Queue.read()`, `Queue.peek()`, and
> `Queue.move()` return `CloseableIterator[...]`; an unknown runtime `bool`
> includes that closeable iterator in the existing scalar/tuple/iterator union.

### [SB-API-5] - replace the current peek-only closeable paragraph

> `Queue.read_generator()`, `Queue.peek_generator()`,
> `Queue.move_generator()`, and `Queue.stream_messages()` return
> `CloseableIterator[...]`. The high-level `all_messages=True` read, peek, and
> move views return the corresponding closeable iterator shapes. Peek lifecycle
> and live traversal remain governed by [SB-DELIVERY-4]; delivery settlement
> remains governed by [SB-DELIVERY-5]; read, move, and stream iterator ownership
> is [SB-DELIVERY-6].
>
> Backend-facing `BrokerConnection` generator methods remain ordinary
> `Iterator[...]` seams. The public close contract belongs to the outer Queue
> generator that owns `Queue.get_connection()`; it does not require backend
> API changes, a runtime wrapper, or generator-only `send()` and `throw()`
> operations.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Dependency-Ordered Tasks

### 1. Review and lock the exact atomic contract

- Independently review this plan, the Proposed Spec Delta, the current Queue
  generators, process-session operation stack, and poison implementation.
- Confirm the comprehension-gate answers in the Execution Log.
- Lock strategy B: Tasks 2-5 form one atomic contract/implementation change.
  The implementer may run their failing-first steps in order but must not land
  spec text, code, or link claims separately.
- Stop if review finds that any named Queue surface is not an outer closeable
  Python generator or that supported same-thread close cannot exit its Queue
  operation without backend changes.

Done signal: independent review passes, the exact delta is locked, and the
atomic implementation slice may begin.

### 2. Make the existing public capability type-checkable

- In `tests/test_queue_typing_contract.py`, change the affected literal and
  runtime-boolean expectations to `CloseableIterator[...]`. Add direct
  `.close()` calls, ordinary `Iterator` assignments, and
  `contextlib.closing()` use for read, move, and stream surfaces.
- In `tests/test_watcher_thundering_herd.py`, change `RecordingQueue`'s
  `stream_messages()` override from `Iterator[Any]` to
  `CloseableIterator[Any]` (or the exact tuple specialization) and import the
  public protocol. This is the one known first-party override made
  type-incompatible by the base return-type tightening.
- Run focused mypy before implementation. Expected failure: the current public
  annotations expose only `Iterator`, so `.close()` and `closing()` are not
  accepted. If it starts green, correct the fixture before proceeding.
- In `simplebroker/sbqueue.py`, change only the affected overload and concrete
  return annotations, including the nested high-level move
  `dict_generator()` annotation. Update their docstrings with lazy operation
  acquisition, same-thread early-close duty, and the exactly-once lease versus
  at-least-once poison distinction.
- Do not edit `CloseableIterator`, package exports, backend Protocols, or
  backend implementations. Stop if mypy requires any of those changes.

Done signal: focused mypy passes, each result remains assignable to ordinary
`Iterator`, and no runtime generator body changed in this task.

### 3. Forward close through high-level move

- Extend `tests/test_queue_api_additions.py` using its existing retained
  delegated-generator pattern. Obtain
  `move(destination, all_messages=True)`, advance it once, close the returned
  iterator, and assert the retained inner iterator's `finally` ran. Retaining
  the inner object is essential so CPython garbage collection cannot make the
  pre-change test pass accidentally.
- Run the focused test before implementation. It must fail because the current
  tuple-to-dictionary `for` wrapper does not explicitly close its delegate.
- In the nested `dict_generator()`, bind the delegated `move_generator()` once,
  iterate it under `try`, and call its now-typed `.close()` in `finally`.
- Preserve dictionary shape, iteration errors, filters, destination
  validation, and default exactly-once settlement. Do not add a general
  transforming-iterator abstraction.

Done signal: the focused test turns green, and the production delta is one
local `try/finally` around the existing adapter loop.

### 4. Bind the promoted lifecycle without rebuilding the peek suite

- Extend `tests/test_delivery_contract_sb_delivery.py` with a compact shared
  parameterized public-Queue proof covering:
  - read generator in exactly-once and at-least-once modes;
  - move generator in exactly-once and at-least-once modes;
  - stream peek, single-message exactly-once, full exactly-once, and
    at-least-once batch modes;
  - the high-level read/move `all_messages=True` views.
- For each row, prove construction acquires no Queue operation, first yield
  retains one persistent operation, and same-thread `close()` returns the
  operation count to baseline before returning. Repeated close must be safe and
  the Queue must be immediately reusable.
- Reuse `queue_factory` and real selected backends. The active-operation count
  is supporting ownership evidence; public Queue reuse is the public behavior.
  Do not mock Queue, `DBConnection`, process sessions, or backend services in
  this matrix.
- Retain existing [SB-DELIVERY-5] rollback and cross-thread poison tests as the
  owners of message settlement and unsupported-use diagnostics. Do not copy
  the 400-line peek lifecycle matrix, inject cleanup failures, or add new
  cross-thread scenarios.
- Add the new function names to the [SB-DELIVERY-6], [SB-API-4], and [SB-API-5]
  verification mappings only after the tests exist.

Done signal: the compact shared matrix passes against SQLite, PostgreSQL, and
Redis/Valkey, while the existing rollback and poison owners remain green.

### 5. Align rationale, teaching, compatibility evidence, and changelog

- Apply the exact [SB-DELIVERY-6], [SB-API-4], and [SB-API-5] text from this
  plan in the same atomic source change as Tasks 2-4. Add reciprocal Related
  Plans links, implementation mappings, and verification rows only now that
  their code and tests exist. Record the resulting promotion baseline as the
  implementation commit SHA, or the pre-change SHA plus exact combined diff
  while the work is under uncommitted review.
- In `docs/implementation/06-process-session-core-ownership.md`, generalize the
  suspended Queue-operation rationale from peek alone to the public closeable
  Queue iterator family. Keep persistent, ephemeral, and injected-runner
  ownership distinct.
- In `docs/implementation/04-cross-thread-finalization-poisoning.md`, state that
  the public closeable type exposes supported owner-thread cleanup but adds no
  recovery; retain poison's SQL at-least-once scope.
- Update `docs/guides/python.md`, `docs/agent-kernel.md`, and `README.md` so
  read, move, stream, and peek examples consistently use the public type and
  same-thread close duty. Preserve the existing `contextlib.closing()` example.
- Add one `CHANGELOG.md` entry naming the widened return annotations and
  deterministic high-level move close forwarding. Do not select a version or
  claim publication.
- Search this repository and known local downstreams for `Queue` subclasses or
  overrides of the affected methods. Record findings and run the relevant
  downstream static check against the local package when available; do not edit
  downstream repositories under this plan. The known first-party
  `RecordingQueue.stream_messages()` override is owned by Task 2 and must not
  be rediscovered during closeout.
- Run the agent-facing interface checklist. The expected design is one public
  identity (`CloseableIterator`), an explicit cleanup action, and no new setup
  or wrapper surface.
- Reconcile spec mappings, Related Plans, implementation docs, code, tests, and
  the Deviation Log. Obtain an independent completed-work review before
  closure.

Done signal: the contract chain is bidirectional, known consumers still type
check or any required override annotation is recorded, all review findings are
disposed, the atomic promotion baseline is recorded, and the Status Index row
can close with evidence.

## Testing Plan

| Contract element | Primary proof | Supporting proof |
|------------------|---------------|------------------|
| Read generator return type and `.close()` | `tests/test_queue_typing_contract.py` under mypy | ordinary `Iterator` assignment and `contextlib.closing()` |
| Move generator return type and `.close()` | `tests/test_queue_typing_contract.py` under mypy | ordinary `Iterator` assignment and `contextlib.closing()` |
| Stream return type and `.close()` | `tests/test_queue_typing_contract.py` under mypy | ordinary `Iterator` assignment and `contextlib.closing()` |
| High-level read/move closeable unions | literal and runtime-boolean typing fixtures | shared public lifecycle rows |
| High-level move delegate close | retained inner iterator in `tests/test_queue_api_additions.py` | real shared move lifecycle row |
| Lazy operation acquisition and synchronous same-thread release | compact shared matrix in `tests/test_delivery_contract_sb_delivery.py` | immediate Queue reuse and process-session count |
| Exactly-once commit-before-yield | existing exactly-once and generator tests | unchanged source/destination state checks |
| At-least-once rollback on early close | existing [SB-DELIVERY-5] tests | generator-method batch tests across adapters |
| Foreign-thread poison remains unchanged | existing cross-thread poisoning and process probes | implementation-rationale inspection |
| Backend Protocol remains `Iterator` | mypy plus source inspection | extension suites |
| First-party subclass compatibility | focused mypy on `tests/test_watcher_thundering_herd.py` | source scan for other affected overrides |

Anti-mocking rule: the lifecycle matrix uses real Queue objects, process
sessions, adapters, and selected services. The one permitted retained fake is
under the high-level move dictionary-shaping wrapper, where the behavior under
test is direct close forwarding rather than broker settlement. It may not
replace the real-backend matrix.

The failing-first rule has two direct red phases: static type fixtures fail on
the current `Iterator` annotations, and the retained high-level move delegate
does not receive `close()`. The real lifecycle matrix is expected to start
green because this plan promotes existing runtime behavior; record that as
baseline confirmation rather than mislabeling it red-green TDD.

## Verification and Gates

### Plan and atomic-contract gates

```bash
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

### Focused implementation gates

```bash
uv run --extra dev pytest -q \
  tests/test_queue_typing_contract.py \
  tests/test_queue_api_additions.py \
  tests/test_delivery_contract_sb_delivery.py \
  tests/test_generator_methods.py \
  tests/test_cross_thread_finalization_poisoning.py
uv run --extra dev mypy simplebroker \
  tests/test_queue_typing_contract.py \
  tests/test_delivery_contract_sb_delivery.py \
  tests/test_queue_api_additions.py \
  tests/test_watcher_thundering_herd.py \
  --config-file pyproject.toml
```

### Real service-backed lifecycle gates

```bash
uv run --extra dev ./bin/pytest-pg --fast -n0 -q \
  tests/test_delivery_contract_sb_delivery.py \
  tests/test_queue_api_additions.py
uv run --extra dev ./bin/pytest-redis --fast -n0 -q \
  tests/test_delivery_contract_sb_delivery.py \
  tests/test_queue_api_additions.py
```

### Final local gates

```bash
uv run --extra dev pytest
uv run --extra dev ./bin/pytest-pg
uv run --extra dev ./bin/pytest-redis
uv run --extra dev ruff check simplebroker tests \
  extensions/simplebroker_pg extensions/simplebroker_redis
uv run --extra dev ruff format --check simplebroker tests \
  extensions/simplebroker_pg extensions/simplebroker_redis
uv run --extra dev mypy simplebroker \
  bin/release.py bin/ruff_suppression_index.py \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis \
  --config-file pyproject.toml
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

Success means the focused red tests fail for the expected reason before their
implementation slices, all named post-change gates exit zero with non-empty
collection, every shared lifecycle row executes under each real backend, and
the final diff contains no backend Protocol, poison, recovery, or new
abstraction changes. If a service harness cannot run locally, record that
residual and require its hosted equivalent before release readiness.

## Independent Review Loop

Before the atomic contract/implementation slice, a reviewer who did not author
the plan reads this plan,
its Proposed Spec Delta, [SB-DELIVERY-5/6], [SB-API-1/4/5], both implementation
notes, `simplebroker/sbqueue.py`, `simplebroker/db.py`, and the named tests. The
review stance is:

> Existence-check every named path, method, mode, annotation, and command.
> Decide whether this plan merely promotes an existing owner-thread close
> capability, whether the high-level move `finally` is the only justified
> runtime change, and whether the spec distinguishes the exactly-once
> operation lease from at-least-once SQL transaction/poison. Look especially
> for a hidden backend API change, recovery promise, missing close-forwarding
> wrapper, weak GC-dependent test, or ceremony that can be removed. Return
> PASS or BLOCKED and tie any blocker to implementability or robustness.

The author records each finding below and either updates the plan, rejects it
with evidence, or marks it out of scope with a reopen condition. A BLOCKED
verdict prevents promotion and implementation. Repeat independent review after
implementation before closing the plan.

### Review Log

| Date | Reviewer | Verdict / finding | Disposition |
|------|----------|-------------------|-------------|
| 2026-08-25 | Independent plan review | **BLOCKED**: the first draft overgeneralized the persistent shared-session lease, mixed strategy-A promotion with new link claims, omitted the known `RecordingQueue.stream_messages()` override, misstated close-error precedence, and silently relaxed the existing creation-thread rule. | Accepted. Ownership now distinguishes persistent, ephemeral, and injected-runner scopes; strategy B keeps the small contract/code delta atomic; the override and focused mypy owner are explicit; ordinary `finally` precedence is stated; and create/use/close remains same-thread. Round-2 review required. |
| 2026-08-25 | Independent plan round 2 | **PASS**: all five accepted corrections are present, strategy B is internally consistent, and no new defect was introduced. | Plan review complete. Implementation remains separately authorized. |
| 2026-08-25 | Independent completed-work review | **BLOCKED**: the guide and README flattened peek's first-advancement owner rule into read/move/stream creation-thread affinity, and the guide overclaimed SQL poison for Redis/Valkey. Code, tests, specs, and all other documentation passed. | Accepted. Teaching text now states the two thread-owner rules separately and limits permanent poison to SQL-backed at-least-once generators and SQL sidecars. Round-2 completed-work review required. |
| 2026-08-25 | Independent completed-work review round 2 | **PASS**: both teaching-text blockers are resolved; the guide and README now match [SB-DELIVERY-4/6], and no adjacent contradiction was introduced. | Completed-work review passed. |

## Out of Scope

- Cross-thread close support, transferable leases, owner-thread callbacks,
  in-process healing, poison clearing, or any new recovery operation.
- Changes to delivery settlement, move reservation semantics, claimed state,
  message identity, filters, record shapes, or stream dispatch.
- A new iterator wrapper, context-manager API, generator base class, helper
  module, backend Protocol, backend API version, or dependency.
- Reworking `peek_generator()` or its completed lifecycle suite.
- Changing `sidecar()`, watchers, async APIs, or CLI output/error behavior.
- Release version selection, tag creation, package publication, or downstream
  source edits.
- Broad test-suite cleanup owned by
  `docs/plans/2026-08-25-test-suite-audit-remediation-plan.md`.

## Execution Log

Record comprehension answers, pre-change red evidence, promotion baseline,
focused and real-backend results, downstream scan, interface review,
independent-review dispositions, and final commit evidence here. Do not record
transient worktree or staging claims.

- **2026-08-25 authoring type baseline at
  `0694c769972689482f72287563fa9ad08832889c`:** a no-incremental mypy probe
  passed each of the five affected results to `contextlib.closing()`. It
  produced exactly five `[type-var]` errors because the public results are
  currently typed as `Iterator[str]`, `Iterator[tuple[str, int]]`, or
  `Iterator[MovedMessage]`. This proves the static mismatch without changing
  the source tree; Task 2 still owns the committed firing fixture.
- **2026-08-25 comprehension gates:** (1) The public methods return outer
  Python generators, so their existing `yield from` or `finally` paths already
  close delegates and unwind `Queue.get_connection()` on supported same-thread
  close; no wrapper is needed. (2) Every mode retains the suspended outer Queue
  operation context. A persistent shared Queue additionally retains a
  thread-local process-session lease, an ephemeral Queue retains its
  operation-scoped `DBConnection`, and an injected-runner Queue retains its
  borrowed-core operation scope; only SQL at-least-once additionally suspends
  a transaction and held core lock and therefore uses poison. (3) High-level
  move is the sole runtime edit because its tuple-to-dictionary adapter uses an
  ordinary `for` loop, which does not forward `close()` to the delegated
  iterator by protocol. Result: all answers match the named owners; the atomic
  implementation slice may begin.
- **2026-08-25 failing-first evidence:** focused mypy rejected all five
  affected Queue return surfaces because the published annotations were plain
  `Iterator`; direct `.close()` and `contextlib.closing()` therefore failed.
  The retained-delegate test for high-level move also failed with no observed
  inner close. After the minimal implementation, both gates passed: mypy found
  no issues in 47 source files and the delegate test passed.
- **2026-08-25 promotion baseline:** atomic strategy B landed as
  `81b80403b963c4332297b6946085d806be535b8e` from pre-change commit
  `0694c769972689482f72287563fa9ad08832889c`.
  No backend Protocol, backend implementation, poison, recovery, settlement,
  or message-shape code changed.
- **2026-08-25 lifecycle evidence:** the ten-row public Queue matrix passed on
  SQLite, PostgreSQL, and Valkey/Redis. Each row proved lazy construction,
  one retained persistent operation after first yield, synchronous same-thread
  release, idempotent repeated close, and immediate Queue reuse. The focused
  contract/code suite passed 126 tests; each fast service gate passed 47.
- **2026-08-25 compatibility scan:** the repository-wide Queue subclass and
  affected-method scan found one relevant override,
  `tests/test_watcher_thundering_herd.py::RecordingQueue.stream_messages`, and
  its annotation now matches the base. `ProcessRecordingQueue` does not
  override an affected method. The owner previously excluded Weft from this
  change, so no downstream source edit or check was performed.

### Agent-facing interface review

Scope: public Python return-type and cleanup contract at baseline
`0694c769972689482f72287563fa9ad08832889c` plus the atomic working diff.
The repository's interface-review skill was applied because Queue is an
agent-facing embedding surface.

| Principle | Disposition and evidence |
|-----------|--------------------------|
| 1. Context is the scarcest resource | Met: the existing three-method `CloseableIterator` remains the entire added type information (`simplebroker/sbqueue.py:55`); no wrapper payload or setup response was added. |
| 2. Progressive disclosure | Met: the kernel gives the compact rule, the Python guide teaches `contextlib.closing()`, and [SB-DELIVERY-6] owns the full lifecycle. |
| 3. Self-explanatory names | Met: existing Queue method names and the existing `CloseableIterator` identity are unchanged (`simplebroker/sbqueue.py:55`, `:696`, `:1440`, `:1813`). |
| 4. One identity per thing | Met: read, peek, move, stream, and high-level all-message views use the one package-root protocol; no second closeable type exists ([SB-API-5], `docs/specs/16-python-library-api.md:233`). |
| 5. Derive what is derivable | Not applicable: the change adds no caller-supplied field or option. |
| 6. No hidden session setup | Met: construction is lazy and operation entry occurs on first advancement ([SB-DELIVERY-6], `docs/specs/11-delivery.md:174`). |
| 7. Teach, don't reject | Not applicable: input acceptance and validation are unchanged. |
| 8. Every message carries its action | Not applicable: the change adds no success, error, or guidance message. |
| 9. Atomic writes with recovery | Not applicable to this type/cleanup surface; settlement remains owned by [SB-DELIVERY-5], and operation exit does not transfer other ownership (`docs/specs/11-delivery.md:193`). |
| 10. Draw the trust boundary | Met: the outer Queue generator owns public close while backend generator Protocols remain ordinary `Iterator` seams (`docs/specs/16-python-library-api.md:241`). |
| 11. Wire format matches the mental model | Met: all record shapes remain unchanged (`docs/specs/16-python-library-api.md:202`); only the existing cleanup capability becomes visible. |

| ID | Severity | Location | Finding | Suggested disposition |
|----|----------|----------|---------|-----------------------|
| — | — | — | No interface finding. | Keep the one public protocol and backend seam boundary. |

Ratified judgment: expose existing `.close()` through one structural protocol
at the outer Queue seam; do not add a wrapper, backend protocol, or hidden
setup step. Verdict: **no blocker**. Runbook feedback: no new reusable
interface-review candidate.

- **2026-08-25 final verification:** root suite: 3,111 passed, 17
  platform/opt-in skips. PostgreSQL: 1,358 root tests passed with 5 skips and
  207 extension tests passed with 5 opt-in skips. Valkey/Redis: 1,351 root
  tests passed with 12 skips and 270 extension tests passed with 1 opt-in
  skip. Full ruff check, ruff format check (316 files), and mypy (63 source
  files) passed. DOM-15 fixtures, plan context, documentation paths, and diff
  whitespace gates passed after the accepted review corrections. The
  post-correction contract/documentation subset passed 58 tests.
- **2026-08-25 commit evidence:** `git log -1 --oneline` and `git rev-parse
  HEAD` verified atomic implementation commit
  `81b80403b963c4332297b6946085d806be535b8e` (`Expose closeable Queue
  iterators`) before plan closure.

## Fresh-Eyes Review

Before promotion and again before closure, verify that a zero-context reader
can answer:

- Which five Queue return surfaces become closeable, and which backend surfaces
  remain ordinary iterators?
- Why does exactly-once require same-thread close despite having no transaction
  open across yield?
- Which mode uses poison, and why does the public type add no recovery?
- Why is high-level move the only runtime body edit?
- Which two checks fail before implementation, and which existing tests retain
  ownership of settlement and poison?
- What evidence proves same-thread operation release across real backends?
- What would force replanning?

Cut any gate, abstraction, or test expansion that does not answer one of those
questions or protect a named invariant.
