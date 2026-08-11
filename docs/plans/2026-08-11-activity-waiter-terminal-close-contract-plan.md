# Activity Waiter Terminal Close and Lifecycle Contract Plan

Date: 2026-08-11
Status: completed
Class: 5. This plan strengthens the normative public Python contract in
`[SB-API-6]` and `[SB-API-11]`, changes cleanup-failure behavior in first-party
backend waiters, and advances the backend-plugin handshake. Public contract,
cleanup lifecycle, and cross-package rollout triggers make the hardening-plan
checklist mandatory.
Plan type: implementation with spec revision.
Promotion strategy: **A, spec first.** Promote the exact `[SB-API-6]` and
`[SB-API-11]` text plus reciprocal plan backlink as the first reviewable slice.
Do not add implementation-evidence claims until code and firing tests land.
Release target: `simplebroker` 7.1.0, `simplebroker-pg` 3.6.0, and
`simplebroker-redis` 3.6.0. Publication and tags remain a separate operation.

## Goal

Make `ActivityWaiter.close()` a precise terminal lifecycle contract that
callers can trust without retaining an identity ledger:

- the first call marks the waiter closed before backend cleanup begins;
- that call attempts every cleanup action that is safe to attempt
  independently, even after an ordinary cleanup exception;
- it raises the first ordinary cleanup exception after those attempts and
  retains later failures as ordered PEP 678 exception notes;
- every later call is a no-op, including when the first call raised.

Keep `ActivityWaiter` a close-only leaf resource. Do not add
`ActivityWaiter.shutdown()`. Formalize the existing broader lifecycle
vocabulary separately: `close()` releases resources in the receiver's
ownership scope; optional `shutdown()` tears down shared or process-wide
substrate when that stronger scope exists; SimpleBroker-owned runner teardown
prefers `shutdown()` and falls back to `close()`.

Ship conforming PostgreSQL and Redis/Valkey waiters, including the Redis
multi-queue composite that currently lacks its own terminal guard. Advance the
backend API handshake so an older plugin cannot be accepted while providing a
waiter with weaker close semantics.

The downstream Weft identity set and its use of Python `id()` are evidence for
the contract, not SimpleBroker implementation work. Weft removes that state in
a separate downstream change after it floors a release containing this
contract.

## Evaluation of the Proposed Feedback

| Feedback element | Evaluation | Evidence / action |
|------------------|------------|-------------------|
| Put idempotence in the `ActivityWaiter` protocol | **Already present, but incomplete** | `simplebroker/_backend_plugins.py:486-495` already says `close()` must be idempotent and tolerate repeated defensive calls. Promote exact terminal and post-error meaning into `[SB-API-6]`; align the docstring. |
| Add a guard to PostgreSQL waiters | **Already present** | Both PostgreSQL waiter classes set `_closed` before cleanup (`extensions/simplebroker_pg/simplebroker_pg/runner.py:438-443,507-512`). Retain the guard; harden multi-step cleanup so an early ordinary failure does not skip a later independent release. |
| Delete a tracking set from SimpleBroker | **Not needed here** | SimpleBroker has no persistent waiter-ID tracking set. Its handoffs compare live objects with `is` in `simplebroker/watcher.py:1286-1356`. |
| Delete Weft's `_closed_activity_waiter_ids` | **Good downstream follow-up, out of scope here** | The set and `id(waiter)` use are in `../weft/weft/core/tasks/multiqueue_watcher.py:283,494-505`. Persistent `id()` values are unsafe because CPython may recycle an address after the old object dies. |
| Delete Weft's `close_candidates` identity-dedup loop | **Only after the upstream guarantee ships** | The local loop uses live `is` comparisons, so it does not itself have the recycling bug. It may become unnecessary once direct repeated close calls are safe after success and failure, but that deletion belongs to Weft. |
| Add `shutdown()` to `ActivityWaiter` | **Not needed and conceptually wrong** | A waiter owns one registration or composite registration. It does not own the runner or process-wide listener substrate. `close()` is the correct leaf lifecycle verb. |
| Clarify `close()` versus `shutdown()` | **Good idea, with scope-based wording** | Existing runner/core behavior already distinguishes handle-local release from owned shared-substrate teardown. `[SB-API-11]` should make that ownership rule public without requiring every type to expose both verbs. |
| Fix SimpleBroker examples that use `id()` | **Not needed** | `examples/` has no activity-waiter identity ledger. The replacement example in `docs/guides/python.md:616-628` retains live references and closes them directly. It needs contract prose, not an identity rewrite. |

## Source Documents

- Product theory: `docs/program-theory.md` `[THEORY-3]` assigns watcher/waiter
  adaptation to SimpleBroker and backend-substrate ownership to the backend
  runner; `[THEORY-4]` favors explicit safety and a small concept count.
- Development process:
  `docs/specs/01-development-documentation-operating-model.md` `[DOM-5]`,
  `[DOM-6]`, `[DOM-10]`, `[DOM-11]`, and `[DOM-15]`.
- Winning product contract: `docs/specs/16-python-library-api.md`
  `[SB-API-6]` and `[SB-API-11]`.
- Ownership registry: `docs/specs/product-section-registry.md`, row
  “Python library / embedding API surfaces.”
- Implementation rationale:
  `docs/implementation/06-process-session-core-ownership.md` and
  `docs/implementation/07-complexity-and-state-machine-map.md`.
- Public guidance: `docs/guides/python.md`, `docs/guides/backends.md`,
  `docs/agent-kernel.md`, and `simplebroker/ext.py`.
- Planning and review guidance:
  `docs/agent-context/runbooks/writing-plans.md`,
  `docs/agent-context/runbooks/hardening-plans.md`,
  `docs/agent-context/runbooks/testing-patterns.md`,
  `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`,
  `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md`,
  `skills/interface-review/SKILL.md`, and `skills/call-agent/SKILL.md`.
- Current code and tests named under Context and Key Files below.
- Downstream evidence only (not an authority and not an edit target):
  `../weft/weft/core/tasks/multiqueue_watcher.py` and
  `../weft/docs/plans/2026-07-10-postgresql-dynamic-native-waiter-rebind-plan.md`.

## Spec Baseline

- Authoring baseline: `7610c73b1a3ee5b72389273e5235a28fc69a3bb3`.
- `[SB-API-6]` lists `ActivityWaiter`, the multi-queue factory, and watcher
  types, but does not define waiter ownership, close/shutdown vocabulary,
  idempotence, terminal-before-cleanup ordering, or failure precedence.
- `[SB-API-11]` lists `SQLRunner`, backend plugin types, and
  `BACKEND_API_VERSION`, but does not define `close()` versus `shutdown()`.
- The protocol docstring already requires idempotent close. PostgreSQL single
  and multi waiters and the Redis single waiter already set `_closed` before
  cleanup. The Redis multi waiter delegates to children without its own guard.
- `PollingStrategy.start()` and `.close()` call waiter `close()` directly after
  detaching ownership; they do not and should not keep a second closed-object
  registry.
- `close_owned_runner()` already prefers callable `shutdown()` and otherwise
  calls `close()`. `BrokerCore` and Redis core already distinguish handle-local
  `close()` from runner `shutdown()`; PostgreSQL and Redis runners may alias
  the two where their ownership scopes coincide.
- The worktree had unrelated in-progress plan files at authoring time:
  `docs/plans/2026-08-11-drive-until-test-helper-adoption-plan.md`, its Status
  Index row, task-owned temporary review prompts, and timing-helper changes.
  They are excluded from this plan and must not be edited, deleted, staged, or
  attributed to it by this work. Their owning task may continue changing them.

Before implementation, record the current SHA and diff. If `[SB-API-6]`,
`[SB-API-11]`, any waiter close method, the backend API version, or package
versions changed after this baseline, stop and rebase this plan's exact delta
and compatibility sequence before editing.

## Context and Key Files

| Surface | Current owner and behavior | Planned edit |
|---------|----------------------------|--------------|
| Canonical Python contract | `docs/specs/16-python-library-api.md:139-155,240-260` | Add exact lifecycle rules under `[SB-API-6]` and `[SB-API-11]`; add firing evidence and plan backlink. |
| Public waiter protocol | `simplebroker/_backend_plugins.py:486-495` | Align the docstring with terminal-before-cleanup, post-error no-op, owner serialization, and no `shutdown()`. |
| Watcher ownership | `simplebroker/watcher.py:1286-1356` | No behavior change. Preserve live-identity handoff and direct close calls. Update docstrings only if needed to cross-reference the stronger waiter contract. |
| PostgreSQL waiters | `extensions/simplebroker_pg/simplebroker_pg/runner.py:388-512` | Preserve existing guards; attempt unregister and registry release under first-error precedence. |
| Redis single waiter | `extensions/simplebroker_redis/simplebroker_redis/plugin.py:251-275` | Preserve guard; attempt unregister and registry release under first-error precedence. |
| Redis composite waiter | `extensions/simplebroker_redis/simplebroker_redis/plugin.py:278-297` | Add its own `_closed` terminal state; close every child once on the first call; preserve first error and note later ones. |
| Runner lifecycle | `simplebroker/_runner.py:75-185`, `simplebroker/db.py:3488-3514`, Redis core `core.py:1923-1927`, PostgreSQL runner `runner.py:835-851` | Preserve behavior; document the ownership-scope rule and keep `close_owned_runner()` tests. |
| Backend handshake | `simplebroker/_backend_plugins.py:23`, first-party plugin literals, `bin/release.py:110-116` | Advance v5 to v6, map v6 to core 7.1.0, and keep all first-party literals/floors synchronized. |
| Contract gates | `tests/test_python_library_api_contract_sb_api.py`, `tests/test_backend_plugin_resolution.py`, `tests/test_release_script.py` | Add exact semantic and version binds. |
| First-party waiter tests | PostgreSQL notify/state-machine suites and Redis plugin/state-machine/integration suites | Add success, ordinary-failure, secondary-failure, and repeated-close proofs at real waiter seams. |
| Public guidance | `docs/guides/python.md:586-642`, `docs/guides/backends.md:35-100`, `docs/agent-kernel.md:326-337` | Teach direct close, no identity ledger, leaf close versus runner shutdown, and backend API v6. |
| Downstream pressure | `../weft/weft/core/tasks/multiqueue_watcher.py:283,494-505,619-628` | Read-only verification. No SimpleBroker code should copy its `id()` ledger. |

Planning inspection confirms the unregister/release boundary is safe to
attempt in order. PostgreSQL listener registration refcounts are local to the
listener (`runner.py:185-240`), while `_SharedActivityRegistry.release()`
balances the separate waiter acquisition refcount (`runner.py:350-382`). Redis
registry release does not blindly close the listener: it checks the live
registration state after unregister and closes only when none remain
(`plugin.py:202-214,300-321`). If implementation discovers that these seams
changed, the Stop Gate still applies.

## Invariants and Constraints

1. **Leaf ownership:** an `ActivityWaiter` releases only the backend activity
   registration(s) it owns. It never shuts down an injected runner, shared
   process session, pool, or service.
2. **Terminal transition first:** first close marks the waiter closed before
   any external cleanup call. Re-entry or a later call cannot repeat cleanup.
3. **Post-error idempotence:** a first close that raises is still the one
   terminal close attempt. Later closes return without effect and do not retry
   partially completed cleanup.
4. **Best available first attempt:** after an ordinary `Exception`, continue
   with cleanup actions that are safe to attempt independently. Raise the
   first error; attach later errors in cleanup order with
   `BaseException.add_note()`, following `simplebroker/db.py:1295`. Tests inspect
   `__notes__` for ordered error type/message evidence without freezing
   cosmetic wording. Do not replace the first cause.
5. **Interrupt priority:** `BaseException` subclasses outside `Exception`
   propagate immediately. The terminal flag remains set, but no promise is
   made that later cleanup actions run after such an interruption.
6. **Owner serialization:** the waiter contract does not make `wait()` and
   `close()` concurrent. The strategy or caller serializes wait, replacement,
   ownership transfer, and close.
7. **No post-close wait promise:** this plan does not standardize the result of
   `wait()` after close. Callers must stop waiting before close.
8. **No identity registry:** do not add a set of `id()` values, weak IDs,
   object hashes, or closed-waiter references to SimpleBroker. Terminal state
   belongs on the resource.
9. **No new lifecycle verb:** do not add `shutdown()` to `ActivityWaiter` or a
   generic lifecycle base class. Existing runner/core `shutdown()` remains
   scope-specific and optional where documented.
10. **Injected ownership is unchanged:** an explicitly injected runner remains
    caller-owned. SimpleBroker uses `shutdown()` preference only for a runner
    it owns.
11. **Wake semantics are unchanged:** waiters remain hints, queue-set
    replacement remains owner-serialized, and delivery/claim behavior does not
    change.
12. **No storage or wire change:** schemas, queue data, target serialization,
    CLI output, and persistence formats are unchanged.
13. **Handshake truth:** once `[SB-API-6]` requires the stronger waiter
    behavior, backend API v5 plugins may not be accepted as if they implement
    it. Core, SQLite, PostgreSQL, Redis, release mapping, package floors, and
    docs move to v6 together.
14. **First-party package lockstep:** core 7.1.0 and extension 3.6.0 metadata,
    root extras, extension core floors, and lockfiles change atomically. Tags
    and publication do not occur in this implementation plan.
15. **No unrelated cleanup:** do not modify or stage the pre-existing
    drive-until plan, its task-owned prompts, or timing-helper changes.

## Comprehension Questions

The implementing agent records these answers in the Execution Log before code
edits:

1. Why does `ActivityWaiter` have `close()` but not `shutdown()`?
   **Expected:** it owns a leaf registration or composite registration, not the
   runner or process-wide substrate.
2. What happens if unregister raises `RuntimeError` and registry release then
   raises `ValueError`?
   **Expected:** both are attempted on the first call; `RuntimeError` is raised
   with the `ValueError` retained as a note; a second close is a no-op.
3. Who serializes `wait()` against `close()`?
   **Expected:** the strategy/caller owner. Idempotence does not imply
   cross-thread safety.
4. When does SimpleBroker call `shutdown()` on a runner?
   **Expected:** when tearing down a runner SimpleBroker owns and a callable
   hook exists; otherwise it calls `close()`. Injected runners stay
   caller-owned.
5. Why is a Python `id()` set not a valid replacement for resource-local
   terminal state?
   **Expected:** `id()` is unique only during an object's lifetime and may be
   recycled; a retained integer can falsely classify a different later
   object.
6. Why does this plan advance `BACKEND_API_VERSION`?
   **Expected:** post-error terminal close is a new semantic obligation on
   waiters returned by backend hooks; accepting v5 would make the guarantee
   unenforceable across plugins.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Proposed Spec Delta

### `docs/specs/16-python-library-api.md` `[SB-API-6]`

After the current public-surface table and before the watch-mode delegation,
insert this exact normative text:

> An `ActivityWaiter` is a close-only leaf resource. It owns one backend
> activity registration or one composite set of registrations; it does not own
> the runner or shared process substrate and does not expose `shutdown()`.
>
> The waiter owner must serialize `wait()`, replacement or ownership transfer,
> and `close()`. This contract does not make `wait()` and `close()` safe to run
> concurrently, and it does not define `wait()` behavior after close.
>
> `ActivityWaiter.close()` is terminal and idempotent. The first invocation
> marks the waiter closed before backend cleanup begins. During that invocation
> it attempts every owned cleanup action that remains safe to attempt
> independently after an ordinary `Exception`. It then raises the first such
> exception and retains later cleanup exceptions, in cleanup order, as PEP 678
> exception notes added with `BaseException.add_note()`. Every
> later invocation returns without effect, including when the first invocation
> raised; it does not retry partial cleanup. A `BaseException` outside
> `Exception` propagates immediately, while the waiter remains terminal.

Keep the existing delivery-mode delegation and implementation mapping. Extend
the `[SB-API-6]` Verification row with named PostgreSQL and Redis waiter tests
added by this plan.

### `docs/specs/16-python-library-api.md` `[SB-API-11]`

After the current advanced-export/SDK boundary and before the timestamp
validator paragraph, insert this exact normative text:

> Lifecycle verbs follow ownership scope. `close()` releases resources owned
> by the receiving handle or runner. `shutdown()` is the optional stronger
> operation when that receiver owns shared or process-wide substrate beyond an
> ordinary handle release. An implementation may make one delegate to the
> other when those scopes coincide.
>
> SimpleBroker-owned runner teardown calls callable `shutdown()` when present
> and otherwise calls `close()`. This preference does not transfer ownership
> of an explicitly injected runner from its caller to SimpleBroker.
>
> Backend API v6 requires every waiter returned by a backend activity-waiter
> hook to satisfy `[SB-API-6]` terminal close semantics. Core rejects older or
> newer backend API versions through the existing exact-version handshake.

Extend the `[SB-API-11]` Verification row with the backend v6 handshake and
owned-runner lifecycle tests. Add this plan to Related Plans.

### Public protocol and guidance alignment

- Replace the current `ActivityWaiter` protocol docstring with wording that
  mirrors `[SB-API-6]` without making the private module a second authority.
- In `simplebroker/ext.py`, add a backend API v6 note after the v5 note:
  waiter hooks now return close-only resources whose first close is terminal
  before cleanup and whose later closes are no-ops even after ordinary cleanup
  failure.
- `docs/guides/python.md` teaches callers to close live waiter references
  directly, says no identity ledger is needed, says first-close failure is not
  retried by calling close again, and retains the existing owner-serialization
  warning.
- `docs/guides/backends.md` distinguishes waiter close from runner shutdown,
  documents backend API v6, and keeps explicit injected runners caller-owned.
- `docs/agent-kernel.md` gets one compact ActivityWaiter lifecycle sentence and
  links to `[SB-API-6]`; it does not duplicate failure mechanics.
- `docs/implementation/06-process-session-core-ownership.md` records the
  ownership rationale for `close()` versus `shutdown()`.
- `docs/implementation/07-complexity-and-state-machine-map.md` adds a confirmed
  `SM-ACTIVITY-WAITER` row with owner, primary transition table, neighboring
  evidence, and terminal/failure-order rationale.

### Version and changelog delta

- Advance `BACKEND_API_VERSION` and the SQLite/PostgreSQL/Redis plugin literals
  from 5 to 6.
- Add `6: "7.1.0"` to `BACKEND_API_MIN_CORE_VERSION` without changing prior
  historical mappings.
- Set versions to core 7.1.0 and both extensions 3.6.0.
- Set both extension core floors to `simplebroker>=7.1.0`; set root `pg` and
  `redis` extra floors to `>=3.6.0`; regenerate all owned lockfiles through the
  repository's release/lock workflow.
- Add an `Unreleased` changelog section describing the terminal close contract,
  first-error cleanup behavior, Redis composite fix, backend API v6, and the
  coordinated package targets. Do not date it or publish artifacts in this
  plan.

## Failure and Error-Priority Matrix

| Case | Required first-call behavior | Required later-call behavior | Primary evidence |
|------|------------------------------|------------------------------|------------------|
| All cleanup succeeds | Mark closed; run each owned action once; return | No-op | Per-class repeated-close tests; real service replacement tests |
| First ordinary action fails; later action succeeds | Mark closed; retain first error; run later independent action; raise first | No-op | PostgreSQL and Redis single-waiter fault tests |
| First and later ordinary actions fail | Mark closed; run both; raise first; add ordered PEP 678 note for later failure | No-op | One PostgreSQL and one Redis representative test |
| First Redis composite child fails | Mark composite closed; close every remaining child; raise first | No child is called again | Redis composite fault test |
| Multiple Redis children fail | Close every child; raise first child error; note later failures in order | No-op | Redis composite ordered-failure test |
| `KeyboardInterrupt`/`SystemExit` from cleanup | Mark closed first; propagate immediately; remaining actions need not run | No-op | Focused representative unit test; no broad BaseException swallowing |
| Concurrent wait and close | Unsupported by contract; owner must serialize | N/A | Existing PollingStrategy ownership/transition tests and docs |

## Hidden Couplings

| Coupling | Risk | Required control |
|----------|------|------------------|
| `PollingStrategy.start()` and `.close()` call waiter close directly | A non-terminal backend waiter makes ordinary strategy cleanup unsafe | Keep direct calls; enforce the obligation at the waiter protocol and backend handshake instead of adding strategy tracking. |
| Queue caches and strategy ownership transfer | Closing before detach or closing both live aliases can unregister the installed waiter | Preserve `detach_activity_waiter(expected=...)`, live `is` checks, and exception-atomic replacement tests. |
| Shared listener registries | Unregister and registry release are separate obligations; an early exception can leak a reference | Preserve the inspected separation above; attempt both on the first close where safe; pin first-error precedence. |
| Redis multi waiter wraps individually guarded children | Child guards make success look idempotent but do not make composite failure terminal | Add composite `_closed` before iterating; attempt all children in one first call. |
| Runner `close()` meanings vary by receiver | A blanket “close is local” rule would contradict PostgreSQL runner behavior | Define both verbs by receiver ownership scope; allow aliasing when scopes coincide. |
| Explicitly injected runners | A broad shutdown rule could close caller-owned pools | Preserve caller ownership and the existing `close_owned_runner()` boundary. |
| Backend API exact handshake | Semantic change without a bump silently admits weaker plugins | Advance core and all first-party literals to v6; retain stale/future diagnostic tests. |
| Separately published extension packages | Core could ship before compliant extension artifacts and reject users' installed plugins | Coordinate version metadata and release floors now; block any later publication unless all three distributions are ready from one exact SHA. |
| Downstream Weft identity ledger | Deleting it before the release floor would expose old/custom waiters | Keep Weft edits out of this plan; record release/version prerequisite for its follow-up. |
| Python `id()` recycling | A dead waiter's integer ID can collide with a later live waiter | Do not persist IDs; state lives on each waiter. No SimpleBroker example introduces such tracking. |

## Tasks

### T0. Activation and baseline gate

1. Owner changes this plan's index row from `draft` to `active` before
   implementation begins.
2. Record SHA, `git status --short`, versions, backend API version, and the
   comprehension answers in the Execution Log.
3. Re-run the current-structure inventory. Stop on any conflicting change
   named in Spec Baseline.

### T1. Promote the contract (strategy A)

1. Apply the exact `[SB-API-6]` and `[SB-API-11]` text and Related Plans
   backlink.
2. Do not yet add the new test names to Verification or claim first-party
   conformance.
3. Run `tests/test_python_library_api_contract_sb_api.py`, doc gates, and a
   focused diff review. Commit this spec-only slice before product code.

### T2. Add red firing tests

1. Extend `tests/test_python_library_api_contract_sb_api.py` with a focused
   `[SB-API-6]` lifecycle-language bind and an `[SB-API-11]` ownership/v6 bind.
   Do not pin whole paragraphs; assert the required concepts and links.
2. Add PostgreSQL real-waiter tests at the listener/registry substrate boundary
   for single and multi waiters: success twice, unregister failure plus release,
   two ordinary failures with first-error priority, and representative
   BaseException behavior.
3. Add Redis real-waiter tests for single and composite waiters: success twice,
   unregister/release failure ordering, every child attempted, first error plus
   ordered `__notes__` evidence, second close no-op, and representative
   BaseException behavior.
4. Update the real PostgreSQL and Redis replacement integration tests to assert
   terminal state and harmless repeated close after strategy ownership cleanup.
5. Update backend handshake/release tests to expect v6 and the 7.1.0 minimum.
   Capture red output before implementation. Failures must be behavioral, not
   missing-import scaffolding.

### T3. Implement terminal cleanup and backend API v6

1. Align the `ActivityWaiter` protocol docstring with the promoted contract.
2. Re-verify the inspected registry separation in current code before wrapping
   cleanup. Stop if release is no longer safe to attempt after unregister
   raises.
3. In PostgreSQL single and multi waiters, keep `_closed = True` before cleanup;
   attempt unregister and registry release under first-error precedence.
4. In Redis single waiter, do the same. In Redis multi waiter, add its own
   `_closed` state before child cleanup and close every child under ordered
   first-error precedence.
5. Use `BaseException.add_note()` for secondary ordinary failures, following
   the existing `db.py:1295` cleanup-note pattern. A small module-local helper
   is allowed only if it makes exception order clearer in that extension. Do
   not add a public utility, cross-extension framework, or lifecycle base
   class.
6. Advance core, SQLite, PostgreSQL, and Redis backend API literals to v6 in the
   same slice. Update `ext.py`'s version note.
7. Run targeted tests, static checks, and both extension fast suites. Commit the
   coherent code/test/handshake slice.

### T4. Align rationale, public guidance, and version metadata

1. Apply the guidance and implementation-doc delta above. Update the
   `[SB-API-6]` and `[SB-API-11]` Verification rows only after the named tests
   pass.
2. Update the state-machine map with `SM-ACTIVITY-WAITER` and verify its owner
   and test paths resolve.
3. Apply core 7.1.0 / extension 3.6.0 metadata, dependency floors, backend API
   minimum mapping, extension README compatibility notes, and regenerated
   lockfiles.
4. Add the `Unreleased` changelog entry. No tag, GitHub release, or package
   upload is part of this task.
5. Run metadata/release guards, doc gates, package builds, and clean-archive
   metadata inspection. Commit this alignment slice.

### T5. Downstream compatibility check (read-only)

1. Reinspect Weft's current SimpleBroker floors and waiter lifecycle call sites.
2. Run its focused multi-queue watcher tests against the built local
   distributions if the sibling worktree and environment are usable without
   mutation. Record any inability as residual evidence, not a reason to edit
   Weft here.
3. Confirm the downstream follow-up prerequisite explicitly: Weft may delete
   `_closed_activity_waiter_ids` only after flooring core 7.1.0 and compatible
   first-party extensions. Its live `is` dedup loop is a separate simplification
   decision, not the Python `id()` defect itself.

### T6. Completion gates and review

1. Run all gates below from the exact candidate SHA and record outputs/counts.
2. Run an independent implementation review against this plan, the spec delta,
   code, tests, version metadata, and downstream boundary. Disposition every
   finding in Review Log.
3. Close the Status Index row only when code, tests, docs, metadata, reviews,
   and commits are complete. Do not claim publication.

## Testing Plan and Verification Gates

### Anti-mocking rule

Tests instantiate the real first-party waiter class and fake only the external
listener/registry or service boundary needed to force cleanup errors. Do not
mock `ActivityWaiter.close()` when testing its contract, replace the production
class with a counting fake as the primary proof, or infer correctness only from
`_closed`. Service-backed integration tests remain required for successful
registration and teardown.

### Targeted red/green gates

```text
uv run pytest -q tests/test_python_library_api_contract_sb_api.py tests/test_runner_lifecycle.py tests/test_backend_plugin_resolution.py
uv run ./bin/pytest-pg --fast -q -k 'activity_waiter or backend_api'
uv run ./bin/pytest-redis --fast -q -k 'activity_waiter or backend_api'
uv run pytest -q tests/test_release_script.py -k 'backend_api or extension_core_floor'
```

The implementer may narrow to exact node IDs during red/green iteration, but
the commands above must pass before the slice closes.

### Neighbor and service-backed gates

```text
uv run pytest -q tests/test_watcher.py tests/test_watcher_transition_tables.py tests/test_activity_waiter_api.py tests/test_activity_waiter_replacement.py
uv run ./bin/pytest-pg --fast
uv run ./bin/pytest-redis --fast
```

Run the normal service-backed PostgreSQL/Redis integration lanes used by CI.
If a service is unavailable, do not replace that proof with more mocks; record
the missing lane and leave integration readiness open.

### Static, docs, metadata, and full gates

```text
uv run ruff check .
uv run ruff format --check simplebroker tests bin extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run mypy simplebroker bin/release.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
uv run pytest -v --tb=short -m "not benchmark"
```

At execution time, mirror the current CI partitions from
`.github/workflows/test.yml` and use the repository release tooling for builds,
lock regeneration, exact-SHA checks, and artifact metadata inspection. Do not
copy stale command lists when the workflow has changed.

### Structural inspection gates

- `rg -n "id\\(.*waiter|waiter.*id\\(" simplebroker examples docs/guides`
  finds no persistent waiter identity ledger.
- `rg -n "backend_api_version = 6|BACKEND_API_VERSION.*= 6" simplebroker extensions`
  shows the core and all three first-party plugin literals aligned.
- `git diff --check` is clean.
- Every changed canonical promise has a named firing test in the spec's
  Verification table.
- `git status --short` shows this plan did not add changes to the unrelated
  drive-until plan, its prompts, or timing-helper files.

## Interface Review

Authoring review of the public Python/backend surface against
`docs/agent-context/runbooks/designing-agent-facing-interfaces.md` at baseline
`7610c73`:

| Principle | Status | Evidence / planned disposition |
|-----------|--------|--------------------------------|
| 1. Context is the scarcest resource | Met | One lifecycle rule replaces caller-side identity ledgers; the kernel carries only a link and compact statement. |
| 2. Progressive disclosure | Met when T4 closes | `[SB-API-6]` owns the rule; Python/backend guides teach use and implementation; the kernel points to the owner. |
| 3. Self-explanatory names; no lookup tables | Met | Keep conventional `close()` for a leaf and `shutdown()` for stronger owned substrate; do not add lifecycle enums or ID tables. |
| 4. One identity per thing | Met | Resource-local `_closed` is authoritative. Persistent Python `id()` values are forbidden. |
| 5. Derive what is derivable | Met | The waiter tracks its own terminal state; callers do not infer or remember it. |
| 6. No hidden session setup | Met | No session handle, registration token, or prior setup call is added to close. |
| 7. Teach, don't reject | Not applicable: lifecycle call | No input normalization surface changes. Exceptions retain their existing types. |
| 8. Every message carries its action | Not applicable: Python resource method | No new CLI/MCP message or error catalog is introduced; docs state that a failed first close is terminal and must not be retried. |
| 9. Atomic writes with a recovery path on conflict | Not applicable: cleanup lifecycle | No write/merge surface changes. The plan instead specifies first-error cleanup order. |
| 10. Draw the trust boundary in the interface | Met | `[SB-API-6]` draws waiter registration ownership; `[SB-API-11]` draws runner/shared-substrate ownership. |
| 11. Wire format matches the agent's mental model | Not applicable: no wire format | The Python mental model is direct resource close, not an integer identity ledger. |

Findings:

| ID | Severity | Location | Finding | Suggested disposition |
|----|----------|----------|---------|-----------------------|
| IR-F1 | P1 | `docs/specs/16-python-library-api.md:139-155` | The canonical waiter surface omits lifecycle semantics already relied on by core and downstream consumers. | Apply the exact `[SB-API-6]` delta and firing tests. |
| IR-F2 | P1 | `extensions/simplebroker_redis/simplebroker_redis/plugin.py:278-297` | Redis composite close aborts the first cleanup pass when one child raises, leaving later children unclosed; it also has no composite terminal state to make the failed first call final. | Add composite state and all-child first-attempt cleanup. |
| IR-F3 | P1 | `simplebroker/_backend_plugins.py:23` and first-party plugin literals | Keeping backend API v5 would accept plugins that were never checked against the stronger semantic obligation. | Advance the exact handshake to v6 and coordinate floors. |
| IR-F4 | P2 | `docs/guides/backends.md:35-65` | Current guidance permits both runner verbs but does not explain their ownership relationship. | Add scope-based close/shutdown guidance; do not add waiter shutdown. |

Ratified judgments: no `ActivityWaiter.shutdown()`; no caller-side identity
registry; terminal state before cleanup; ordinary errors do not stop later
independent cleanup; first error remains primary; BaseException interrupts
immediately; backend API v6; no post-close wait guarantee.

Authoring verdict: no unresolved interface-design blocker. Independent plan
review still gates activation.

Runbook feedback: no new reusable principle proposed. The existing one-identity
and trust-boundary principles exposed the relevant defects.

## Rollout, Compatibility, and Rollback

### Rollout

1. Land spec promotion first, then code/tests/handshake, then docs/metadata.
2. Build core 7.1.0 and both 3.6.0 extension artifacts from one exact SHA.
3. A later release plan must publish compatible extension artifacts and core
   in coordinated order and prove clean-index installs through the root extras
   and direct extension dependencies. This plan performs no publication.
4. Downstreams may simplify close tracking only after their dependency floors
   exclude core/backend API v5 combinations.

Success signals after a later publication:

- clean installs through `simplebroker[pg]` and `simplebroker[redis]` resolve
  the coordinated versions;
- stale v5 plugins fail at resolution with upgrade-or-pin guidance;
- repeated waiter close, including after injected ordinary cleanup failure,
  has no second effect in first-party conformance tests;
- Weft can remove the integer ID ledger without changing topology behavior.

### Compatibility statement

Ordinary waiter consumers gain a stronger backward-compatible guarantee. Full
backend implementations gain a new semantic obligation and therefore require
the v6 handshake. This repository does not promise a stable complete
third-party backend SDK, but it does promise exact rejection instead of running
an unverified plugin. No stored data or CLI output changes.

### Rollback

Before publication, revert spec, protocol, waiter code, v6 literals, version
metadata, floors, docs, tests, and lockfiles together. There is no data
migration. Do not roll back only the Redis guard or only the API constant; both
would make the accepted handshake lie about behavior.

After publication, do not republish or move immutable tags. A corrective
release may restore v5 acceptance only by explicitly reverting the public
post-error guarantee and coordinating extension floors; the safer path is a
forward v6 fix. Downstream deletion of its tracking set is separately
reversible in Weft and is not part of this rollback.

## Stop Gates

Stop and return to owner review if any of these occur:

- a first-party waiter requires concurrent `wait()`/`close()` safety to meet
  its backend API;
- unregister and registry release cannot safely be attempted independently;
- a cleanup error type must be translated or suppressed rather than re-raised;
- backend API v6 cannot be coordinated across core, SQLite, PostgreSQL, Redis,
  release mapping, and dependency floors in one candidate;
- package release targets conflict with another active release plan;
- service-backed tests show a registration leak, duplicate unregister, lost
  wakeup, or changed queue-set handoff;
- the implementation needs a new public lifecycle type, retry policy, or
  persistent identity mechanism;
- the sibling Weft checkout would need mutation to complete a SimpleBroker
  gate.

## Independent Review Loop

- **Plan review before activation:** use a verified different-family reviewer
  via `skills/call-agent/SKILL.md`, with read-only containment and a bounded
  attempt. The reviewer existence-checks every named path and seam first, then
  answers: (1) can the plan be implemented confidently as written, and (2)
  would it avoid regressions in ownership, error priority, plugin
  compatibility, and release sequencing? Verdict: PASS or BLOCKED.
- **Review stance:** find errors, bad ideas, latent ambiguity, missing tests,
  and performative overengineering. Prefer removing unnecessary work.
  Pre-existing issues are observations unless this delta worsens them.
  Severity is a claim, not an automatic disposition. Expansion requests are
  owner decisions, not blockers by default.
- **Meaningful-slice reviews:** spec promotion; code/test/handshake; and
  docs/metadata each receive a scoped diff review before the next slice.
- **Completed-work review:** different-family preferred, against exact
  candidate SHA, with no blocker required before the Status Index closes.
- Every finding is reproduced and entered below as accepted-and-addressed,
  rejected-with-reasoning, deferred-with-owner, or out-of-scope observation.

## Review Log

| Date | Stage | Reviewer / result | Findings and disposition |
|------|-------|-------------------|--------------------------|
| 2026-08-11 | Authoring interface review | Primary agent / no unresolved interface blocker | IR-F1 through IR-F4 are incorporated into the exact delta and tasks; ratified judgments recorded above. |
| 2026-08-11 | Independent plan review | Claude CLI 2.1.207, different family, read-only / **PASS** | Both gates passed with no blocker. Accepted P3-1: the exact spec and T3 now name PEP 678 `BaseException.add_note()` and tests bind ordered `__notes__` evidence. Accepted P3-2: Context records why PostgreSQL acquisition release and Redis conditional registry release remain safe after an unregister error, with T3 re-verification and the existing Stop Gate retained. Accepted P3-3: IR-F2 now leads with the Redis composite's intra-call leak rather than only its later-call behavior. Out-of-scope observations about the real v6 rejection cost, synthetic release fixtures, and undefined post-close PG/Redis wait asymmetry require no change. Reviewer found no removal opportunity. |
| 2026-08-11 | Ruff suppression adoption | Independent subagent, read-only / **PASS** | Approved new `[RUFF-SUP-035]`: two module-local `BLE001` directives are necessary to catch arbitrary ordinary cleanup errors while preserving first-error/ordered-note semantics and immediate `BaseException` propagation. A shared helper would violate the backend ownership boundary. Registry, counts, markers, and derived index move atomically. |
| 2026-08-11 | Code/test/handshake slice review | Independent subagent, read-only / initially **BLOCKED**, bounded re-review **PASS** | Accepted P1 F1: the Redis composite initially noted a later child's primary error but dropped that child's existing secondary-cleanup note. A three-failure red test reproduced it. Both module-local helpers now snapshot and copy nested notes after the later primary note, preserving full cleanup order. PostgreSQL 5 and Redis 7 lifecycle tests, Ruff, mypy, and the bounded re-review passed. |
| 2026-08-11 | Docs/metadata/state-machine slice review | Independent subagent, read-only / initially **BLOCKED**, bounded re-review **PASS** | Accepted two P2 findings. Extension READMEs no longer describe open-ended dependency floors as proof of compatibility; they identify 7.1.0/3.6.0 as the first coordinated v6 set and keep the exact runtime handshake authoritative. Both changed implementation documents now link this active plan. Focused release and document gates passed after correction. |
| 2026-08-11 | Final completed-work review | Independent subagent, read-only / initial findings addressed, final **PASS** at `11284c21` | Accepted P1: CI-shaped mypy found missing/ambiguous annotations in the two new lifecycle test files; `d0523f3` fixed them and the 31-file PostgreSQL and 28-file Redis partitions passed. Accepted P2: the generic cleanup-retry lesson contradicted one-shot terminal resources; `11284c2` now distinguishes retry-capable bookkept resources from `[SB-API-6]` terminal waiters. Reviewer answered yes to confident plan/spec implementation and regression avoidance in ownership, error priority, plugin compatibility, and release sequencing. No remaining P1/P2 finding. |

## Out of Scope

- Editing Weft, deleting its `_closed_activity_waiter_ids`, or simplifying its
  `close_candidates` loop.
- Defining post-close `wait()` behavior or adding concurrent wait/close safety.
- Adding `shutdown()` to `ActivityWaiter` or requiring every runner/core to
  expose both lifecycle verbs.
- Changing queue delivery, watcher retry, topology replacement, wake-hint,
  service durability, storage schema, CLI, or persistence semantics.
- Creating a general cleanup framework or complete third-party backend SDK.
- Tags, package upload, GitHub release, downstream dependency updates, or
  post-publication communication.
- Coalescing plans or touching the unrelated drive-until plan worktree changes.

## Fresh-Eyes Review Checklist

Before activation and again before completion, a fresh reviewer should be able
to answer yes to all of these:

- Does the plan distinguish what already exists from what actually changes?
- Is the Python `id()` bug correctly located downstream rather than in
  SimpleBroker or its examples?
- Is the live `is` dedup loop distinguished from the persistent `id()` set?
- Can a waiter fail during its first close without leaking independently
  releasable resources or being retried later?
- Is first-error and secondary-error behavior exact for single and composite
  waiters?
- Is BaseException behavior explicit and narrow?
- Is `ActivityWaiter` still a close-only leaf?
- Does close/shutdown wording follow ownership scope across cores and runners?
- Are injected runners protected from SimpleBroker shutdown?
- Does the backend API handshake reject unverified old semantics?
- Are version floors, release mapping, package versions, and lockfiles
  coordinated?
- Do tests exercise real waiter classes and preserve service-backed proof?
- Is Weft read-only evidence rather than a hidden completion dependency?
- Are rollout, rollback, and publication boundaries executable and honest?

## Execution Log

| Date | Slice / SHA | Commands and observed result | Residual risk / next action |
|------|-------------|------------------------------|-----------------------------|
| 2026-08-11 | T0 activation / `77bac20972aaef6bd5e2646490878da0d0f24baa` | Owner authorized implementation. `git diff 7610c73..HEAD` over every contract, waiter, handshake, and version seam was empty; current versions remain core 7.0.1, extensions 3.5.2, backend API v5. Comprehension answers: waiter owns registrations, not substrate; first `RuntimeError` stays primary and later `ValueError` becomes a note; owner serializes wait/close; owned-runner teardown prefers shutdown then close; Python `id()` may be recycled; v6 makes the stronger returned-waiter obligation enforceable. | Proceed with strategy-A spec promotion. Publication remains out of scope. |
| 2026-08-11 | T1 spec promotion | Promoted the exact terminal waiter and ownership-scoped lifecycle contracts in `[SB-API-6]` and `[SB-API-11]`; added the reciprocal plan link. `uv run pytest -q tests/test_python_library_api_contract_sb_api.py` passed 13 tests. `python3 bin/check-dom15-fixtures`, `bin/check-plan-context`, `bin/check-doc-paths`, and `git diff --check` passed. | Implementation evidence remains intentionally absent from the spec Verification rows until the firing tests and first-party code pass. |
| 2026-08-11 | T2/T3 red-green and handshake | Red proofs: Redis composite aborted after its first child error; PostgreSQL unregister failure skipped registry release; the protocol lacked terminal/post-error language; backend literals and core remained v5. Green implementation sets terminal state before cleanup, attempts independently safe ordinary cleanup, keeps the first error, flattens later errors/notes in order, and leaves `BaseException` immediate. Redis adds a composite guard. Backend API is v6; versions/floors/locks align at core 7.1.0 and extensions 3.6.0. Core contract/runner/resolution gate passed 42 tests; release backend/floor gate passed 12; Docker-backed PostgreSQL filtered lanes passed 24 core + 20 extension; Redis passed 24 core + 15 extension; neighbor watcher gate passed 91. Focused Ruff, format, mypy, suppression-index, and lock regeneration checks passed. | Independent review F1 was reproduced and fixed with a three-failure Redis test; bounded re-review passed. Full fast backend and repository gates remain for completion. |
| 2026-08-11 | T4 public alignment and package proof | Updated canonical Verification rows, Python/backend guides, agent kernel, ownership rationale, state-machine map, extension compatibility notes, and Unreleased changelog. Registered `SM-ACTIVITY-WAITER`; its initial structural gate failed on the intentionally absent table, then the five-row real-Redis transition contract passed. Full manifest policy passed 13 tests and the Redis lifecycle file passed 12. Ruff passed repository-wide; format checked 315 files; mypy checked 62 source files; suppression index and doc gates passed. Packaging smoke built core 7.1.0 plus both 3.6.0 sdists/wheels, installed all three wheels together on Python 3.11, and resolved both plugins. | Docs review findings were addressed and re-review passed. Root full suite passed 2,562 with 17 platform/opt-in skips. Full backend fast lanes and downstream read-only check remain. |
| 2026-08-11 | T5 downstream read-only compatibility | Weft remains at `simplebroker>=7.0.0` and `simplebroker-pg>=3.5.2`; its `_closed_activity_waiter_ids` and persistent `id(waiter)` checks remain in `weft/core/tasks/multiqueue_watcher.py`, while the local `close_candidates` loop deduplicates live references with `is`. Without mutating its pre-existing dirty tree, its focused multi-queue watcher file ran against the built core 7.1.0 and PostgreSQL 3.6.0 wheels: 62 passed, one PostgreSQL-only test skipped. Its status before and after was unchanged. | Downstream deletion is correctly deferred. Weft must first floor core 7.1.0 and compatible v6 extensions; removal of the live-`is` loop remains a separate simplification decision. |
| 2026-08-11 | T6 completion candidate / `11284c21` | Core full suite: 2,562 passed / 17 skipped. Examples: 119 passed; example mypy: 15 files. PostgreSQL full fast lane: 1,091 passed / 5 skipped core and 180 passed / 5 skipped extension. Redis: 1,084 passed / 12 skipped core and 258 passed / 1 skipped extension. Ruff, 315-file format check, production mypy (62 files), CI-shaped test mypy (200 core, 31 PostgreSQL, 28 Redis), suppression index, all three lock checks, DOM-15 fixtures, plan context, doc paths, diff hygiene, structural identity/version searches, state-machine manifest, and Python 3.11 packaging smoke passed. Final independent review passed after both findings were accepted and fixed. | Implementation is complete and committed. Versions/artifacts are prepared but no tag, upload, GitHub release, or downstream dependency edit was performed. |
