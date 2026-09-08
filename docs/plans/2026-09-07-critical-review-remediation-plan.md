# Critical Review Remediation

Class: 5+P. Queue target identity and post-fork resource ownership are risky
public boundaries; this plan clarifies normative delivery text and changes
release verification. [DOM-5], [DOM-6], [DOM-11], and [DOM-15] apply.
Plan type: implementation with spec revision.
Owner: SimpleBroker product owner; execution by the assigned implementer.
Status: see the Status Index in `docs/plans/README.md`.

## Goal

Resolve the five findings from the 2026-09-07 critical review: prevent mutable
target metadata from misrouting a move, repair the published worker's failure
paths, recover inherited global locks before use in a forked child, describe
delivery timing honestly, and make the affected verification tests prove their
stated properties. This is a bounded correction of existing owners, not an API
expansion or another general hardening framework. The owner authorized
implementation per this reviewed plan on 2026-09-07.
Release/publication remains outside this implementation task.

## Source Documents

- `docs/program-theory.md`: [THEORY-1], [THEORY-3], [THEORY-4],
  [REV-THEORY-003], [REV-THEORY-004], [REV-THEORY-005]. Queue semantics and
  resource ownership belong here; application processing/retry policy does not.
- `docs/specs/11-delivery.md`: [SB-DELIVERY-1], [SB-DELIVERY-3],
  [SB-DELIVERY-5], [SB-DELIVERY-6]. Cross-target moves stay prohibited.
- `docs/specs/16-python-library-api.md`: [SB-API-2], [SB-API-6], [SB-API-11].
  Mutable descriptors, effective handle identity, waiters, and fork recovery.
- `docs/specs/10-cli.md`: [SB-CLI-1], [SB-CLI-2], [SB-CLI-3]. Worker commands
  retain existing exit codes, exact JSON IDs, and stdout/stderr roles.
- `docs/specs/17-ops.md`: [SB-OPS-3]. Acknowledgement remains exact-ID delete.
- `docs/specs/01-development-documentation-operating-model.md`: [DOM-5],
  [DOM-6], [DOM-10], [DOM-11], [DOM-15], [DOM-16].
- Consulted shared read order: `docs/agent-context/context.index.yaml`,
  `docs/agent-context/README.md`, `docs/agent-context/decision-hierarchy.md`,
  `docs/agent-context/principles.md`,
  `docs/agent-context/engineering-principles.md`,
  `docs/agent-context/lessons.md`, and the Golden Rules plus post-watermark
  ledger in `docs/lessons.md`.
- Consulted runbooks: `docs/agent-context/runbooks/writing-plans.md`,
  `docs/agent-context/runbooks/hardening-plans.md`,
  `docs/agent-context/runbooks/testing-patterns.md`,
  `docs/agent-context/runbooks/adversarial-acceptance-probes.md`,
  `docs/agent-context/runbooks/maintaining-traceability.md`,
  `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md`, and
  `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`.
- `docs/implementation/06-process-session-core-ownership.md` owns snapshot
  and fork rationale; `docs/implementation/07-complexity-and-state-machine-map.md`
  owns state-machine navigation; `docs/implementation/03-agent-inventory.md`
  and `skills/call-agent/SKILL.md` own reviewer selection/invocation.
- `skills/interface-review/SKILL.md`, `docs/README.md`,
  `docs/agent-kernel.md`, and `docs/guides/python.md` own the interface walk
  and placement of teaching material.

Historical evidence is retrieved through the Retired Plans ledger, not dead
live-path links. At source `813dd7ce`, inspect the named plans:
`2026-08-24-comprehensive-review-findings-remediation-plan.md` (mutable
descriptor compatibility and fork F3),
`2026-08-25-schema-and-representation-assumption-remediation-plan.md`
(session key/factory snapshots),
`2026-08-25-test-suite-audit-remediation-plan.md` Task 6.2 (identifier-only
documentation gates), and
`2026-08-27-all-examples-correctness-and-contract-alignment-plan.md`
(kernel review limited to links and selected claims). At `197629e2`, inspect
`2026-05-04-process-local-broker-session-plan.md` (fresh child handles),
`2026-07-05-vendored-retry-consolidation-plan.md` (global log-only warning),
and `2026-07-28-delivery-contract-spec-promotion-plan.md` (delivery vocabulary).
Retrieval form: `git show <source>:docs/plans/<name>`.

## Spec Baseline

Baseline: `feece6a13c27d54a6664d75d20665f4bf0fc852d` for every source,
contract, workflow, and code reference in this plan. The review reproduced
the findings at that revision. Recheck changed owners if execution starts
from a later revision; record a revision rather than transplanting assumptions.

Promotion baseline: the reviewed diff against the baseline above, carried in
the commit closing this plan. Both
reviewed deltas are promoted in the working spec tree; that tree is now
authoritative. Exact spec-diff hashes are recorded in the Execution Log.
No theory revision was required.

## Findings and Dispositions

| ID | Priority and kind | Evidence at baseline | Disposition and owner |
|----|-------------------|----------------------|-----------------------|
| F1 | P2 runtime, conditional on target mutation | Real Valkey: persistent source opened in namespace A; descriptor changed to B; destination opened in B; `source.move(destination)` returned success while B was empty and payload appeared in A/destination. | Task 2: one effective target per Queue. The user's cross-backend restriction is preserved, not relaxed. |
| F2 | P1 executable documentation | Kernel loop with a real DLQ write rejection (`BROKER_MAX_MESSAGE_SIZE=1`) returned 0 and left tasks, inflight, and dlq empty. | Task 3: atomic exact-ID dead-letter move; truthful errors; test the canonical recipe itself. |
| F3 | P2 runtime | Hold the session registry lock in another parent thread, fork, create a fresh persistent Queue in child: hangs; unlocked control completes. Retry hot-loop guard has the same inherited-lock defect. | Task 4: PID-before-lock recovery in the two existing owners. |
| F4 | P2 contract wording | [SB-DELIVERY-5] and Python guide call commit-before-return a stricter realization of `at_least_once`, although no rollback opportunity exists after materialization. | Task 1: clarify timing and failure windows without changing selector spellings or execution. |
| F5a | P2 verification | Release-order test passes when PyPI's staging dependency exists only in YAML comments. Current workflows have the correct edges. | Task 5: parse jobs and test dependency relationships; do not change publication behavior. |
| F5b | P3 verification | Two kernel tests pass prose asserting the opposite of their semantic names, because they check tokens only. | Task 3: accurately name structural checks; executable recipe proof owns safety. |

The larger architecture, optional backends, cohesive source files, and live
peek limitations were investigated and are not findings. The original P1
rating of F1 was narrowed to P2 after user discussion of its precondition.
The optional weak concurrency smoke test mentioned by a reviewer was not in
the final five findings and is outside this plan.

## Context and Key Files

| Owner / files to edit | Current behavior and required reuse |
|-----------------------|-------------------------------------|
| `simplebroker/sbqueue.py` | `_canonicalize_queue_target` retains non-SQLite descriptors; `db_target` exposes that object; `_move_destination_name` and `_activity_waiter_identity` reread its options. Extend the existing canonicalization owner and return a detached reporting value. |
| `simplebroker/_key_material.py` (read, normally no edit), `simplebroker/_targets.py` (read) | Reuse `snapshot_key_material` and `dataclasses.replace`; preserve opaque values by identity and the standalone descriptor's mutable, shallow-copy, picklable contract. No `deepcopy`, JSON round trip, or mapping proxy. |
| `simplebroker/db.py` (docstrings only), `simplebroker/_broker_session.py` | Session key and factory already derive from one acquisition snapshot. Keep that protection for direct DBConnection consumers. Registry acquire/release/close_all currently take its global inherited lock. |
| `simplebroker/_retry.py`, `simplebroker/_runner.py` (read) | `_check_hot_loop` uses global data and a lock. Reuse the existing pre-lock PID recovery and abandon-without-close rationale; retain the warning's process-wide meaning. |
| `tests/test_queue_move_cross_target.py`, `tests/test_process_broker_session.py`, `tests/test_activity_waiter_api.py`, `tests/test_queue_api_additions.py` | Extend existing identity, nested snapshot, recorded waiter-argument, and reporting proofs. Preserve existing runner and redaction tests. |
| `extensions/simplebroker_redis/tests/test_redis_core_behaviors.py`, its `conftest.py` (read) | Real `redis_url` and unique namespace fixtures support the decisive wrong-namespace test. Each newly owned namespace must be cleaned after handles close. |
| `tests/test_fork_safety.py`, `tests/test_retry.py` | Existing real-fork pipes/watchdogs and retry controls are the proof homes. No spawn or mocked PID substitute for the failing case. |
| `docs/agent-kernel.md`, `tests/test_worker_examples.py`, `tests/test_agent_kernel_contract.py` | Keep one canonical move-reserve Bash recipe in the kernel, allowed by `docs/README.md`. Existing worker tests supply real CLI shims, subprocess isolation, controlled handlers, and failure cases. |
| `examples/safe_worker.sh`, `examples/resilient_worker.sh`, `README.md` (read; edit only stale links) | Existing peek workers remain their own documented examples. The README already uses a JSON-envelope handler; reuse that interface concept rather than placing a raw body in a shell variable. |
| `tests/test_release_workflow.py`, three `.github/workflows/release-gate*.yml` files (read) | Replace only the affected text-order gate with a parser-backed dependency assertion. Keep the shared publication helper and three workflow identities. |
| `docs/specs/11-delivery.md`, `docs/specs/16-python-library-api.md`, `docs/guides/python.md`, `CHANGELOG.md`, implementation docs 06/07, `docs/lessons.md` | Align exact behavior, rationale, teaching, and observed corrections with their implementation slices. |

The three exact release workflow inputs are
`.github/workflows/release-gate.yml`,
`.github/workflows/release-gate-pg.yml`, and
`.github/workflows/release-gate-redis.yml`.

### Comprehension gate

Before the relevant code slice, record these answers in the Execution Log.
Wrong answers require rereading the owner before editing.

- Why copy both constructor input and `db_target` output? The getter otherwise
  leaks the private target, recreating the same storage/identity split.
- Why is a PID in the session key insufficient? The inherited lock is taken
  before lookup; replace it before use and retain inherited entries without
  finalizing them.

## Invariants, Rollback, and Scope Controls

- Cross-target moves stay prohibited. One Queue target governs storage, move
  checks, and waiters. Standalone descriptors stay mutable/picklable; opaque
  objects retain existing identity semantics. No arbitrary deep freezing.
- Preserve the existing SQLite path, injected-runner, resource-sharing, and
  redacted-error behavior. Reuse current snapshot and fork-recovery helpers.
- Child recovery must not acquire inherited locks or finalize parent resources.
  Preserve parent state, retry policy, and the process-wide warning meaning.
- Worker failure preserves the original at its last successful atomic location.
  Handler success permits delete; handler failure permits atomic DLQ move.
  No business retries, leases, inflight reaper, or worker framework.
- Keep delivery selector names, signatures, and commit timing unchanged. The
  wording correction does not add aliases or a new API.
- Keep release workflow identities, permissions, artifacts, and actual job graph.
  Fix the affected test, not the whole workflow suite.
- Owner correction (2026-09-07): prove the reproduced use paths, not a Cartesian
  matrix of hypothetical mutations, hostile objects, or lifecycle combinations.
  Add further tests only when implementation exposes another actual coupling.

There is no storage migration, backend protocol bump, publication, or one-way
format change. Revert a code slice with its tests/docs; no data conversion is
needed. A revert restores the bug risk and cannot recover work lost by the old
recipe. Release remains a separate authorized task. Existing release/platform
CI owns broad compatibility; installed-artifact acceptance should rerun the
namespace rejection and fresh-child probes, plus the worker failure case.

Stop and revise if a fix requires changing any boundary above, downstream
production edits, or a new framework. The runtime fixes should be local.

## Dependency Decision

No YAML parser is declared in `pyproject.toml` or `uv.lock` at the baseline.
Proposed: PyYAML as a development-only dependency, installed through the
existing `dev` extra. A real parser is needed to interpret comments and job
structure; Python's standard library has none. A custom YAML parser would
create a new grammar implementation solely to test this small property.

Owner decision: the 2026-09-07 instruction “Please implement per plan”
authorizes the explicitly proposed development-only parser addition as part
of implementation. Record the resolved package and any typing-only companion
needed by the existing mypy gate in the execution evidence. No runtime
dependency is added.

## Proposed Spec Delta

| Spec | Strategy | Scope |
|------|----------|-------|
| `docs/specs/11-delivery.md` | D, clarification-only spec-promotion slice (Task 1) | Replace the materialized-method sentences in [SB-DELIVERY-5]. Behavior and existing mappings already exist. |
| `docs/specs/16-python-library-api.md` | B, atomic with Task 2 code/tests | Insert after the `resolve_config` / `snapshot_config` bullet in [SB-API-2], immediately before “Project configuration is a trusted developer input.” It makes effective Queue ownership explicit. |

### [SB-DELIVERY-5] replacement

Replace from “Materialized `read_many()` / `move_many()` commit before” through
“stricter commit-before-return behavior.” with:

> Materialized `read_many()` / `move_many()` commit before returning their
> result lists. They accept `"at_least_once"` for compatibility, but that value
> does not defer their commit or provide rollback when caller processing
> fails. A failure after a consume claim commits and before the result reaches
> the caller can leave a message claimed without a handoff, as specified in
> [SB-DELIVERY-1]. A committed move retains the message at its destination
> under [SB-DELIVERY-3]. Use a transactional generator when rollback of an
> incomplete yielded batch is required; neither form promises successful
> application processing.

### [SB-API-2] insertion

> A `Queue` binds its effective target at construction. Supported mutable
> containers in a supplied target's backend options are recursively detached
> using the same value and opaque-identity rules as process-session identity.
> Later mutation of the supplied descriptor does not retarget that Queue,
> including its ephemeral operations, move compatibility checks, or activity
> waiters. A newly constructed Queue may use the descriptor's edited values.
> `Queue.db_target` reports a value-equivalent detached descriptor when the
> effective target is a `BrokerTarget`; changing supported containers in that
> returned value does not alter the Queue. String targets remain strings.
> Caller-supplied runners retain their existing resource and identity ownership.

No normative fork delta is needed: [SB-API-11] already requires recovery
before inherited lock acquisition. The fix completes that contract. Update
its implementation mapping/rationale with Task 4, not its guarantee.

## Tasks

### 0. Review and establish evidence

- [x] Independently review the plan and exact deltas before promotion. Resolve
  the parser decision before Task 5; other tasks do not depend on it.
- [x] For F1/F2/F3, turn the existing reproduction into a failing regression in
  its owning test module, then fix it. For F5a, demonstrate the old gate accepts
  a comment-only edge. Prose-only F4 uses the baseline quote, exact replacement,
  semantic review, and existing behavior tests as testing Rule 5's proof; do
  not manufacture a sentence-pinning test.

### 1. Clarify delivery, spec-promotion slice

- [x] Apply [SB-DELIVERY-5] using strategy D; record the promotion baseline.
  Correct the matching paragraph in `docs/guides/python.md` and the four
  materialized-method docstrings in `simplebroker/sbqueue.py` (read_many,
  move_many) and `simplebroker/db.py` (claim_many, move_many). Inspect
  kernel and README restatements and edit only contradictions.
- [x] Reuse existing materialized commit-before-return and incomplete-generator
  rollback tests. No change to execution or selector vocabulary.
- [x] Done: exact wording reviewed, relevant existing delivery tests green.
  Stop if the correction starts requiring different runtime semantics.

### 2. Bind effective Queue identity, atomic spec-promotion slice

- [x] Extend `_canonicalize_queue_target` with `replace` and
  `snapshot_key_material`. `db_target` returns a detached value-equivalent
  descriptor; internal consumers keep using the private target. Preserve
  `_broker_session.py` acquisition snapshots for direct DBConnection callers.
- [x] Apply [SB-API-2] atomically with the fix/tests (strategy B); record its
  promotion baseline. Explain the descriptor/handle distinction in impl 06.
- [x] Real Redis regression in `test_redis_core_behaviors.py`: persistent
  source A, caller edits descriptor to B, destination B, `move(destination)`
  rejects and leaves source plus both destinations unchanged. Same-target
  control moves successfully. Close handles before cleaning owned namespaces.
- [x] In existing Queue/session tests, cover the other exposed alias
  (`queue.db_target.backend_options`) and nested options once. Include an
  ephemeral handle to prove it stays bound across its ordinary operations.
  Existing tests retain pickle/replace, opaque identity, runner, redaction,
  and the other move-entry-point coverage; do not multiply the regression
  across every combination unless the methods use different identity paths.
- [x] In `test_activity_waiter_api.py`, use RecordingPlugin to show edited
  descriptor metadata cannot change the existing Queue's waiter arguments.
  Keep real storage for the misroute proof. Run the focused Weft checks below.
- [x] Done: the reproduced failure is red then green, existing handle stays A,
  deliberately edited descriptor configures new handle B, and downstream
  construction still works. Stop if new plugin or public immutability rules
  are needed.

### 3. Correct the canonical worker recipe

- [x] Keep one move-reserve Bash recipe in the kernel. For the baseline red
  proof, extract just its existing move loop (as in the recorded reproduction),
  excluding the separate write/read and peek demonstrations. Give the corrected
  worker its own fenced block and have tests execute that exact block. Supply
  DB and handlers from the test environment; never keep a test-owned worker copy.
  Require the caller's DB and `process_task_json` handler, which receives the
  JSON envelope on stdin. Remove the duplicate unsafe peek loop and link
  `examples/safe_worker.sh`. Preserve the Python recipes. No new worker script
  or configuration surface.
- [x] Reserve with `broker -f "$DB" move tasks inflight --json`. Status 2 ends
  the finite drain successfully; operational errors stop nonzero with stderr
  visible. Obtain the exact string ID with jq. Keep the body encoded in the
  envelope; use printf, not a decoded shell variable. Match the existing
  README JSON-handler pattern and worker PIPESTATUS handling for early close.
- [x] On handler success, delete the exact inflight ID. On handler failure,
  move that exact ID from inflight to dlq. A failed delete or DLQ move stops
  nonzero. No write-copy, unconditional delete, or hidden retry follows it.
- [x] Execute the exact published block with the existing real CLI shim and
  disposable SQLite target. Prove successful acknowledgement, failed handler
  routed with original ID/body, failed DLQ move retaining inflight, failed
  acknowledgement stopping, and reserve empty versus operational error. Use
  a newline-containing body in the real path to prove envelope preservation.
  One injected CLI failure is allowed; successful storage stays real.
  No second concurrency suite or generic malformed-output matrix: the CLI's
  existing JSON and atomic-move contracts own those properties.
- [x] Rename only the two misleading kernel token-test names to describe their
  actual inventories; their bodies already say this in comments. No new tests
  or replacement sentence assertions. Keep export/exit-code inventories.
  Explicit semantic review owns prose; the executable block owns recipe safety.
- [x] Done: the original one-byte-size-limit reproduction loses work before
  the fix and successfully routes the original ID/body afterward (move does
  not rewrite the body). A separately failed DLQ move retains inflight and
  exits nonzero. Existing worker tests pass. Stop if this grows into worker
  policy or framework work.

### 4. Repair the two inherited global guards

- [x] Add PID-before-lock recovery to `_ProcessBrokerSessionRegistry` acquire,
  release, and close_all. Child gets a fresh lock/entry map and retains the old
  entry graph without closing it. A stale parent key cannot release a child
  entry. Reuse the established abandonment pattern; no cleanup cap or reaper.
- [x] Before `_check_hot_loop` acquires its guard, reset its lock/data on PID
  change. Use a module-owned PID seam. Preserve warning scope, retry timing,
  exception handling, and stop behavior.
- [x] In the existing fork tests, hold the registry lock in another parent
  thread, fork, and construct/write/close a fresh Queue in the child. Observe
  completion with a pipe and bounded watchdog; always reap the child. Prove
  the parent's live Queue still works afterward and inherited entries were
  not closed. Add the analogous real retry probe with one retryable failure
  and the held hot-loop guard. Include ordinary unlocked controls.
- [x] Existing non-fork lifecycle tests own normal release/close behavior.
  Do not add nested-fork, arbitrary finalizer, or every-entry-point matrices.
  Unsupported platforms keep the existing fork skips. Update impl 06 and its
  relevant state-machine edit-point note; claim only these corrected guards.
- [x] Done: both reproduced hangs fail red then complete after the fix;
  existing fork/session/retry tests pass. Stop if inherited locks or resource
  finalization are required during recovery.

### 5. Correct the release dependency proof

- [x] After owner authorization of a dev YAML parser, replace the affected
  test's file-order/substrings proof with parsed job dependencies. Use one
  local helper in `tests/test_release_workflow.py`, not a new gate executable.
- [x] Required edges: verify-tag-current needs require-tests; build needs
  require-tests and verify-tag-current; stage-github-release needs build;
  publish-to-pypi needs stage-github-release; publish-github-release needs
  stage-github-release and publish-to-pypi. Both extension workflows also need
  extract-version from verify-tag-current, build, stage-github-release, and
  publish-github-release. Permit additional edges and scalar/list spelling.
- [x] Preserve existing draft/action/artifact assertions, using parsed steps
  for those migrated from the replaced test. Keep all three real workflows
  unchanged. At authoring, remove each required edge and verify rejection;
  retain focused regression cases for the observed comment-only bypass and
  equivalent declaration reorder. Do not build a gate-over-gate framework.
- [x] Done: current workflows pass, comment-only edge fails, reorder passes.
  Stop if publication behavior must change to satisfy the test.

### 6. Reconcile and close implementation

- [x] Align CHANGELOG, spec mappings/backlinks, impl 06/07, and affected links.
  Add a dated lesson only where this work contributes evidence missing from
  existing rules; evaluate the used skills without automatically expanding them.
- [x] Review each coherent runtime/worker/verification slice independently,
  then the combined change. Disposition findings; rerun only accepted fixes
  in round 2. Run final gates below and reconcile tasks against evidence.
- [x] Land only with owner authorization and explicit file-list staging;
  verify git log before an implementation-complete claim. Flip the index row
  to completed at implementation closeout. A reviewed plan stays draft until
  implementation begins.

## Verification and Anti-Mocking

Per-task checks (SimpleBroker root):

```bash
uv run --locked pytest -n 0 tests/test_delivery_contract_sb_delivery.py tests/test_exactly_once_delivery.py tests/test_generator_methods.py
uv run --locked pytest -n 0 tests/test_queue_move_cross_target.py tests/test_process_broker_session.py tests/test_activity_waiter_api.py tests/test_queue_api_additions.py tests/test_project_config.py
uv run --locked ./bin/pytest-redis -n 0 extensions/simplebroker_redis/tests/test_redis_core_behaviors.py
uv run --locked pytest -n 0 tests/test_worker_examples.py tests/test_agent_kernel_contract.py
uv run --locked pytest -n 0 tests/test_fork_safety.py tests/test_retry.py tests/test_process_broker_session.py
uv run --locked pytest -n 0 tests/test_release_workflow.py tests/test_release_workflow_gate.py tests/test_release_publication_script.py
```

Ensure the Redis wrapper executes the added node, not a skipped-all success.
Storage, fork, Queue identity, and recipe code stay real. Doubles may inject
one CLI failure, observe waiter arguments, or control lock entry.

Final local gates: `python3 bin/check-dom15-fixtures`, `bin/check-plan-context`,
`bin/check-doc-paths`, `uv run --locked pytest`, `git diff --check`, and the
existing Ruff/type-check selections for changed Python files. Use
`bin/release.py::_core_test_mypy_command` for ordinary tests, preserving its
negative-fixture exclusions and argument order; do not enter a release action.
Existing backend/platform CI owns broader parity. No new benchmark or repeated
full-suite runs absent new failures.

Adversarial acceptance is scoped to the changed paths: real default worker
invocation, reserve empty versus failure, failed routing/acknowledgement, and
literal payload preservation; release comment/grammar mimicry. Check truthful
exit class and retained state. No new broker parser, flags, or structured input
format exists here, so do not duplicate their already-owned hostile-input suites.

### Downstream proof

Weft was inspected at `bcea628ee2ea7322988de9ef688113820611a032` in
`/Users/van/Developer/weft`. `weft/context.py` and
`weft/core/tasks/multiqueue_watcher.py` pass the shared target to Queues and
waiters; no production descriptor mutation was found. Source inspection also
found no `is`/`id()` reliance on `Queue.db_target`: the sole production getter
read feeds later Queue construction, and the target initialization test asserts
value equality (`task._db_path == target`). Recheck these actual consumers if
Weft changes before implementation; do not add an identity test for a behavior
they do not use. Preserve its unrelated
work. Do not change downstream code, dependencies, or lockfile.

From that checkout, set PYTHONPATH with Weft first, then candidate SimpleBroker
and its PostgreSQL extension; verify imported paths before the four focused
checks. The exact inspected local command is:

```bash
PYTHONPATH=/Users/van/Developer/weft:/Users/van/Developer/simplebroker:/Users/van/Developer/simplebroker/extensions/simplebroker_pg .venv/bin/python -m pytest -n 0 tests/tasks/test_tasks_simple.py::TestTaskSimple::test_task_initialization_with_broker_target tests/context/test_context.py::test_context_queue_ignores_invalid_ambient_broker_config tests/tasks/test_multiqueue_watcher.py::test_watcher_retains_broker_snapshot_separate_from_weft_policy tests/core/test_queue_wait.py::test_queue_change_monitor_does_not_consume_messages
```

On another host resolve the checkouts explicitly. A run against the installed
old SimpleBroker is not candidate compatibility evidence.

## Independent Review Loop

Preferred reviewer: Claude via the verified read-only invocation in
`skills/call-agent/SKILL.md`; separate Codex reviewer is the bounded fallback.
Give the reviewer this entire plan including exact deltas, baseline SHA, the
code/test owner table, source theory, and implementation rationale. Bound
different-family attempts to two, with a 540-second review budget each; capture
stdout/stderr and record command, timeout, and outcome. A timeout is no verdict.

Review brief: review only this remediation plan, do not implement or modify
files. Existence-check every named file, seam, flag, and driver order. Accepted
boundaries are mutable standalone descriptors, prohibited cross-target moves,
unchanged delivery names/timing, application-owned recovery, and three release
identities. Prior unrelated concerns are observations unless this plan worsens
them. Prefer removing unnecessary work. Return findings with suggested
dispositions and a separate observations section. Answer PASS or BLOCKED based
on confident implementability and whether the plan would degrade robustness.

Record findings verbatim, then the author's disposition and evidence. Re-review
only accepted fixes and defects introduced by them. Dependency authorization,
plan review, and implementation verification are separate gates.

## Interface Review Record

Surface: Python target reporting plus the structured-doc worker and delivery
guidance. This is a plan review, not an integration-ready implementation claim.

| Principle | Baseline assessment and planned disposition |
|-----------|---------------------------------------------|
| 1 Context | Met by one canonical kernel recipe; remove its duplicate peek loop (Task 3). |
| 2 Progressive disclosure | Preserve kernel-to-existing-example links and guide depth. |
| 3 Names | F4 departure: legacy selector names remain for compatibility; exact timing replaces the misleading stronger/weaker claim. |
| 4 Identity | F1 departure at sbqueue.py:286; Task 2 binds all consumers to one effective target. |
| 5 Derivation | Existing move/waiter identity is derived internally; no new caller identity fields. |
| 6 Setup | Worker pins DB in each invocation; Queue binds its existing explicit construction target. |
| 7 Rejection | Cross-target rejection is an unsafe-write boundary, not a recoverable spelling mismatch. Preserve redacted ValueError. |
| 8 Action | F2 departure at kernel reserve/error branch; Task 3 tells the operator which transition failed and retains work. |
| 9 Atomicity | Use existing exact-ID move/delete; application processing stays outside the transaction. No concurrent configuration merge surface. |
| 10 Trust | Input descriptor mutability cannot change an existing handle; handler owns application success. Publication authority remains unchanged. |
| 11 Wire shape | Preserve JSON envelope and string ID; no decoded body in a shell variable. |

Interface findings are F1/F2/F4 above. Ratified boundary from user discussion:
cross-backend moves remain unsupported. Plan design judgment: preserve legacy
delivery selectors and standalone descriptor mutability while correcting the
teaching and handle boundary. Independent plan reviews and final implementation
review passed. Parser authorization was resolved at implementation entry.
Runbook feedback: no new rule proposed; existing identity, atomicity, and
truthful-interface principles already describe these failures.

## Review Log

| Date / reviewer | Scope, command, bound, and result |
|-----------------|----------------------------------|
| 2026-09-07 / Claude Code 2.1.207 | Full plan and exact delta; `claude -p <verbatim brief+plan> --permission-mode plan --allowedTools Read,Grep,Glob`, stdin closed, stdout/stderr captured; timeout 540s; exit 0, PASS, stderr empty. Reviewed plan SHA-256 `132abc9c2ddb8a884ab9c5f638436860d6fa7b01ca16b738142055c6aab4e5e0`. |
| 2026-09-07 / separate Codex core reviewer | Tasks 2–4 only, actual-use/over-armoring check: no blocker in Tasks 2/4; identified F-R4 postcondition error below. |
| 2026-09-07 / Claude round 2 | Accepted corrections only; same read-only invocation and 540s bound; exit 0, PASS, stderr empty. Reviewed plan SHA-256 `57e0190ff5da37e6a62c9252e0bc3a5a14f6e196ae04600f0e0fc1d7b3331eee`; verified exact anchors, downstream value identity, worker failure distinction, and all four docstring owners. No new findings. |
| 2026-09-07 / Claude Code 2.1.207 final implementation review | Combined implementation, contracts, worker and tests; same read-only CLI invocation and 540s bound; exit 0, PASS, stderr empty. Submitted diff SHA-256 `ebd153860bf63542ec2083314869b1e4d0e3648d05bf08a79a5a1b2fdacbf8b9`; reviewer also inspected current files, including final docstring wording. Static review, no test-execution claim. |

### Claude findings, verbatim

**F-R1 (P3, spec-delta ambiguity) — `docs/specs/16-python-library-api.md` [SB-API-2] insertion point.** The plan says "after the BrokerTarget bullet list … before project-configuration prose," but the bullet list (lines 69–88) *contains* the project-config-helpers bullet, so "after the bullet list" and "before project-configuration prose" could be read as different anchors. Disposition: pin it — "after the `resolve_config`/`snapshot_config` bullet (line 88), before the 'Project configuration is a trusted developer input' paragraph (line 90)." Author decides.

**F-R2 (P2, downstream-proof completeness) — `db_target` return-identity change.** Current `db_target` returns the *same* private object (`return self._db_path`); the fix returns a detached value-equivalent descriptor. That is the intended F1 tightening, but it is an observable identity change, not only a mutation-safety change. The plan's weft proof (`bcea628e`) checks for *descriptor mutation* and passes the shared target to Queues/waiters, but the brief does not state it checks for reliance on `queue.db_target is original_target`. Disposition: add one explicit identity-reliance check to the weft focused set (or confirm `test_task_initialization_with_broker_target` already exercises it) before landing Task 2. Low residual risk given the additive `is`-not-guaranteed contract, but worth pinning since weft is the pinned downstream.

**F-R3 (P3, possibly unnecessary work — candidate to REMOVE) — F5b test renaming (Task 3).** The two "semantic-looking token tests" already carry corrective comments ("Identifier tokens only; sentence pins removed (audit Task 6.2)", `test_agent_kernel_contract.py:73,82`) — the misleading *sentence pins* were already excised in prior work; the bodies are already honest identifier inventories. The remaining rename is near-cosmetic once the executable recipe (Task 3) owns safety. The plan already hedges ("Rename … *or remove redundancy*"), so this is not a blocker; but the author should confirm the current *test names* actually mislead before spending effort, and prefer deletion of redundancy over renaming. Removing this sub-item entirely is defensible.

## Review Dispositions

| ID | Reviewer finding (verbatim) | Author disposition and evidence |
|----|-----------------------------|---------------------------------|
| F-R1 | Verbatim finding above. | Accepted: [SB-API-2] insertion now names the exact final bullet and following paragraph. |
| F-R2 | Verbatim finding above. | Accepted as source verification, not a new hypothetical test: searched Weft production and the focused tests for db_target, target `is`, and target `id()` use. The sole getter read at multiqueue_watcher.py:202 configures later handles; test_tasks_simple.py uses value equality. Recorded evidence and recheck condition above. |
| F-R3 | Verbatim finding above. | Keep only the two-line rename: current names say “forbids_delete_while_peek_stream” and “does_not_claim_identical_cli_python_packaging”, neither proven by token presence. Their corrective comments do not make the names accurate. No extra gate, test matrix, or sentence pin is added. |
| F-R4 | “The original size-rejection reproduction loses work before the fix and successfully routes the original ID/body afterward. A separately failed DLQ move retains inflight and exits nonzero.” | Accepted: Task 3 now separates those already-planned cases. The reviewer verified exact-ID move succeeds under a one-byte message limit using real SQLite. |
| Author fresh-eyes | The same “stricter” claim remains in four materialized-method docstrings; extracting the whole original Bash fence would execute unrelated demonstrations before the worker. | Added the four actual docstring owners to Task 1 and specified baseline loop extraction and direct execution of the corrected standalone fenced block. No execution or test scope added. |

## Revision Log

- 2026-09-07: Applied the owner's actual-use constraint before independent
  review: removed Cartesian mutation/lifecycle matrices, a second worker
  concurrency suite, generic malformed-output probes, and redundant broad
  local backend runs. Existing owning suites and CI remain.
- 2026-09-07: Applied F-R1/F-R2/F-R4, retained the minimal F-R3 naming
  correction with rationale, and completed the author's docstring/extraction
  checks. No proposed guarantee or implementation boundary widened.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Execution Log

### 2026-09-07 implementation entry

- Baseline remains `feece6a`; the only starting changes are this reviewed plan,
  its index row, and its three spec backlinks.
- Comprehension: constructor input and `db_target` output both need detachment
  because the getter otherwise exposes the retained options. Existing handles
  stay on their captured target; edited descriptors configure future handles.
- Comprehension: PID-bearing keys do not protect an inherited registry lock;
  child reset must happen before acquisition, preserving the old resource graph
  without invoking cleanup.
- Work split: Queue identity and delivery text owned by the root implementer;
  fork guards, canonical worker, and release-test parser are disjoint delegated
  slices. All use the reviewed actual-use scope and failing-first proof.

Planning evidence: all five retained findings were checked against current
theory and archived plans. The root reviewer independently reproduced F1,
F2, F3 registry hang, and both F5 mutation probes at the baseline. The 37
kernel/delivery tests and documentation gates passed despite those gaps;
the core reviewer additionally ran the existing 90-case fork/retry/session
selection successfully. These are baseline evidence, not implementation proof.
Independent review: full-plan PASS and accepted-corrections round-2 PASS;
all planning findings dispositioned above. Implementation evidence follows.
Planning gates rerun: DOM-15 fixtures, plan context (2 in-flight plans), doc
paths, and diff check passed; plan-context/doc/program-theory tests: 28 passed.
Skill evaluation: the existing planning and interface rules suffice. No skill
change proposed. The actual-use correction is recorded above and governs scope.

### 2026-09-07 implementation evidence

At the verification handoff, implementation was uncommitted and HEAD was
`feece6a`. There is no release, workflow edit, backend protocol change, or
downstream edit.

- F1 failing-first: the new ephemeral/reporting and waiter tests both failed
  before the fix (nested edits leaked; waiter received edited options). The
  real Redis namespace test failed because the cross-target move did not raise.
  After construction/getter detachment, the combined session, waiter, Queue,
  project-config, and delivery selection passed 211 cases; the real Valkey
  core-behavior module passed all 17, including the new test with no skips.
  The separate move/release-gate/publication selection passed 34.
- F2 failing-first: running only the original published move loop with a real
  one-byte write limit returned 0 and lost the original from all three queues.
  The corrected exact fence preserves ID/body via move under that same limit.
  Six cases cover routing (including NUL/trailing newlines), successful ack,
  failed route/ack, empty reserve, and operational reserve error. The owning
  worker/kernel selection passed 59. Exact-fence ShellCheck passed.
- F3 failing-first: both held-lock fork cases timed out while both unlocked
  controls passed (2 failed, 2 passed). Local pre-lock PID recovery made all
  four pass. The combined fork/retry/session selection passed 95, including
  child shutdown without inherited cleanup and continued parent use.
- F4: promoted the reviewed clarification and aligned the guide plus all four
  docstrings. Existing delivery behavior tests passed in the 211-case selection.
  README and kernel teaching were inspected; no selector or timing changed.
- F5: old gate accepted comment-only PyPI staging dependencies in all three
  workflows. The new regressions initially failed for that bypass and for a
  valid reordered declaration. Parsed dependency checks now pass the 51-case
  release-workflow module; all 29 individual required-edge removals fail.
  The two token-test renames retain their existing inventory-only bodies.
- Dependencies: added dev-only `pyyaml>=6.0.3` and
  `types-pyyaml>=6.0.12.20260906`; mypy's observed `import-untyped` failure
  required the stubs. Runtime dependencies and existing package versions are
  unchanged. `uv lock --check` passed in independent review.
- Downstream: the four planned Weft tests passed against this source tree.
  Import preflight printed this checkout's `simplebroker/__init__.py` and
  `extensions/simplebroker_pg/simplebroker_pg/__init__.py`. Weft source remains
  at `bcea628e`; the getter consumer uses its value to configure later handles.
- Combined static checks: Ruff lint/format passed for all 12 changed Python
  files; the official ordinary-test mypy builder passed 212 files, runtime
  mypy passed 44, and the changed Redis test passed its existing mypy command.
  DOM-15 fixtures, plan context (2 in-flight plans), doc paths, and diff checks
  passed. The full `uv run --locked pytest` run passed 3,408 tests with
  18 expected platform/backend/opt-in skips in 71.91s. Final independent
  review passed, as recorded below.
- Documentation: aligned CHANGELOG and implementation 06/07, retained the
  three plan backlinks, and added one evidence-based target-snapshot lesson.
  Used planning, interface-review, and call-agent guidance remains sufficient;
  no new skill, runbook, lifecycle layer, or generic proof framework is needed.

Promotion diff evidence (SHA-256 of `git diff feece6a -- <spec>` at promotion,
including the already-planned backlink):

- `docs/specs/11-delivery.md`: `842909e28b22c8b833acca3329544148b9a36d4d46451110be3f98795a778139`.
- `docs/specs/16-python-library-api.md`: `23caa54f942197b2616c54904a3c86be5cf9a90a2f628ca45b0bb315727732a0`.

Independent slice reviews (read-only, distinct from each slice's author):

- F1 core reviewer: PASS; confirmed private target consistency across storage,
  move and waiter paths, detached reporting, and retained opaque/runner rules.
  Independently ran 157 focused tests and the real Redis regression (1 passed).
- F3 API reviewer: PASS; confirmed reset-before-lock, retained inherited graph,
  stale-key separation, and unchanged retry meaning. Independently ran 55 cases.
  Existing single-threaded first child access remains the boundary.
- F5 worker reviewer: PASS; independently ran 51 tests, rejected all 29 removed
  edges, and verified unchanged workflows and dev-only dependencies. This does
  not claim full hosted GitHub Actions or publication validation.
- F2 root reviewer: inspected the exact fence and test invocation. Handler
  failure routes by original ID; failed storage transitions stop without delete;
  empty and operational reserve outcomes differ. Successful storage uses the
  real CLI, and the injected failures affect only the named inflight operation.
  No actionable defect. Combined external review also covers this slice.

### Final independent review and handoff

Final reviewer verdict, verbatim:

> **Verdict: PASS.** Read-only review of the combined diff against current owners
> and tests. All five findings (F1–F5) are fixed correctly, coherently, match
> the approved plan's deltas, and are proved by tests that exercise real paths.
> No actionable defects found. Details below; out-of-scope observations separated.

Reviewer limits, verbatim:

> Static review only (plan mode: no test execution). Behavioral assumptions the
> tests encode but I did not run — broker exit code 1 for a directory-as-DB, and
> `--json` timestamp emitted as a JSON string — are exercised by the real-broker
> worker tests; recommend running the plan's Verification block (esp.
> `bin/pytest-redis … test_redis_core_behaviors.py`, `test_worker_examples.py`,
> `test_fork_safety.py`, `test_retry.py`, `test_release_workflow.py`) to convert
> those into observed evidence before landing.

Disposition: the requested execution evidence already exists above, including
real CLI/SQLite and Valkey runs. No corrective round 2 is needed because there
are no findings. The review's observations about single-threaded child first
access, detached getter allocation, and retained inherited graphs remain the
approved boundaries. Retention is child-local; sibling forks do not grow the
parent's graph, as implementation 06 explains. No extra guard or memory policy
was added.

All planned implementation and verification changes passed the handoff gates.
The owner then authorized closeout with “Close with a targeted commit.” This
plan and its index row close in the same commit as the reviewed implementation,
using an explicit list of the 25 remediation files. The final commit is
verified with `git log` after creation; publication remains outside scope.
Only closeout records changed after verification, so documentation gates and
the staged diff check are rerun; the passing runtime evidence above stands.

## Out of Scope

New queue primitives, cross-target transfer, descriptor immutability, delivery
aliases, visibility timeouts, daemon/worker management, broad YAML linting,
general test cleanup, coverage infrastructure redesign, file-size refactors,
coalescing, releases, and edits in downstream repositories. Retained-child
resource growth keeps its existing rationale and reconsideration conditions.
