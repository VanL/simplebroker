# Test Proof Quality

Class: 4. Test rewrites cross connection, thread, subprocess, and documentation
boundaries; concurrency coordination and cleanup require [DOM-5] hardening.
Plan type: implementation, with no intended product-contract revision.
Owner: repository owner; execution by the assigned implementer.
Status: see the Status Index in `docs/plans/README.md`.

## Goal

Make the six identified tests or gates prove their stated properties while
allowing harmless changes. Replace shared-transaction observation, scheduling
heuristics, recipe substring checks, and incorrect structural parsing. Preserve
real integration coverage and the existing scope of the product. This is a
bounded correction, not a test-count target or a general test-suite rewrite.

## Source Documents

- `docs/program-theory.md`: [THEORY-3], [THEORY-4], [REV-THEORY-003],
  [REV-THEORY-004]. Claims differ from application completion; Queue handles
  can share process-owned storage resources.
- `docs/specs/01-development-documentation-operating-model.md`: [DOM-5],
  [DOM-10], [DOM-11], [DOM-15], [DOM-16]. Planning, verification, review, and
  structural theory records. No normative change is proposed.
- `docs/specs/11-delivery.md`: [SB-DELIVERY-1], [SB-DELIVERY-2],
  [SB-DELIVERY-3], [SB-DELIVERY-5]. Commit visibility and exact-ID routing.
- `docs/specs/16-python-library-api.md`: [SB-API-2], [SB-API-11]. Effective
  targets, process-session sharing, and ephemeral resource ownership.
- `docs/implementation/06-process-session-core-ownership.md`: same-thread
  persistent handles can share one core; independent observation needs a
  separate connection. `docs/implementation/07-complexity-and-state-machine-map.md`
  supplies test ownership context where relevant.
- `docs/agent-context/runbooks/testing-patterns.md`,
  `docs/agent-context/runbooks/writing-plans.md`,
  `docs/agent-context/runbooks/hardening-plans.md`,
  `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md`, and
  `docs/lessons.md` (2026-08-25 entries) govern proof selection, causal
  concurrency, meaningful deletion, and structural rather than prose gates.
- `README.md` Dead Letter Queue Pattern; `tests/test_worker_examples.py`
  owns executable shell-recipe verification.

Historical decisions, retrieved through `docs/plans/README.md`:

- `813dd7ce:docs/plans/2026-08-25-test-suite-audit-remediation-plan.md`,
  especially invariants and Tasks 1, 6, 8: preserve equivalent behavioral
  ownership, control ordering, and discover every relevant pytest step.
- `813dd7ce:docs/plans/2026-08-25-shared-backend-proof-remediation-plan.md`:
  keep evaluated markers and the existing backend topology; wholesale marker
  migration is explicitly outside this change.
- `197629e2:docs/plans/2026-07-28-delivery-contract-spec-promotion-plan.md`:
  materialized commit-before-return is intended; same-session observation is
  not a deliberate substitute for that proof.

## Spec Baseline

`b25db7f10bc5ea87cb24b030ff24e8aaf57916fe` is the source, test, and spec
baseline. Recheck changed owners if execution begins from another revision.
No spec promotion, storage migration, dependency addition, or release is needed.
README fence separation in Task 3 preserves its commands and distinguishes
worker execution from the existing manual retry demonstration.

## Findings and Evidence

All mutations below were confined to memory or disposable test targets during
the review. They are evidence about tests, not changes to production behavior.

| ID | Observed problem | Required correction |
|----|------------------|---------------------|
| T1 | Removing the SQL operation's commit left both materialized read/move variants and `test_single_message_immediate_commit` passing. Their reads share the transaction. Another test detects this mutation indirectly; it is not a suite-wide escape. | Independent observers in the existing commit tests. |
| T2 | Delaying only the first writer call by 0.8s made `test_concurrent_read_write` fail while all ten correct messages remained intact. | Writer completion and bounded draining replace three-empty-poll termination. |
| T3 | A no-op watcher drain still passed `test_pre_check_drain_race`; the competitor consumed all 50 messages. The claimed post-check delay is actually before the real check. | Establish the specific race and prove watcher progress afterward. |
| T4 | README DLQ gate rejects an extra space in `python3 -c` but accepts deleting the source before the DLQ move; executing that mutation loses the message. | Execute the published worker block and inspect real state. |
| T5 | Adding a valid unnamed, unbounded pytest step passes the CI bounds gate; its splitter treats it as part of a preceding named step. | Read each YAML step's own run command and metadata. |
| T6 | Theory-record fence stripping treats a shorter fence as a closer and changes delimiter type inside a fence; valid examples fail and malformed live records disappear. | Apply the established fence-state rules and extend the existing parser case. |

The August 25 audit's exceptions do not cover T2/T3. Its explicitly retained
join/liveness smoke test is `test_queue_move_watcher::test_stop_event_handling`.
The policies remain sound; these corrections implement them more faithfully.

## Context and Key Files

| Edit owner | Existing structure and reuse |
|------------|------------------------------|
| `tests/test_delivery_contract_sb_delivery.py` | `test_materialized_batches_commit_before_return` has two variants and shared-session observers; also contains the README DLQ substring gate to replace. Keep all other structural and delivery tests. |
| `tests/test_exactly_once_delivery.py` | `test_single_message_immediate_commit` reads its own pending view. Strengthen this existing test rather than rename or delete it. |
| `tests/conftest.py`, `tests/helper_scripts/broker_factory.py` (read) | `queue_factory(name, persistent=False)` and `make_queue` create a separately owned operation connection; default persistent handles share sessions. Keep the fixture default and backend target routing. |
| `tests/test_concurrency.py` | `test_concurrent_read_write` owns one writer and reader and already uses real CLI subprocesses; use its existing timing helper import. |
| `tests/test_watcher_race_conditions.py` | `ConcurrencyTestWatcher` supplies instrumentation; change only the named race's local instrumentation, avoiding changes to other subclass users. |
| `tests/helper_scripts/timing.py` (read) | Reuse `scale_timeout_for_ci` and existing bounded completion patterns. Product durations and injected schedules are not CI-scaled liveness valves. |
| `README.md`, `tests/test_worker_examples.py` | The DLQ fence currently includes a trailing manual retry command. Separate that demo into its own fence; reuse `_write_executable`, `_run_worker`, and real-CLI shim patterns for the exact first fence. |
| `tests/test_release_workflow.py` | `_job_pytest_steps` and `test_every_matrix_pytest_path_bounds_hangs_and_worker_loss` split source text. PyYAML is already a dev dependency. Preserve direct/wrapper timeout and topology rules. |
| `tests/test_program_theory_contract.py` | `_without_fenced_blocks`, `_parse_records`, and `test_record_parser_ignores_fenced_examples` own the faulty parsing and its existing case. |
| `bin/check-plan-context` (read) | `strip_fenced_blocks` already tracks fence character and opening length and closes only on a valid matching delimiter. Use its rules; do not refactor the executable or create a shared Markdown framework. |
| `docs/plans/README.md`, governing specs' Related Plans | Track this plan and keep verification mappings accurate if proof ownership moves. |

### Comprehension before editing

Record answers in the Execution Log. Wrong answers require rereading the owner.

1. Why does another Queue not prove a commit? Expected: same-thread persistent
   handles for the same target share a core/transaction. Observe from an
   independent ephemeral handle before closing or cleaning up the producer.
2. Where must the race pause? Expected: after a real positive pre-check but
   before the drain, without holding a storage or instrumentation lock. A
   delayed thread start or aggregate consumer count does not establish it.

## Invariants, Scope Controls, and Rollback

- No production Python, backend, public API, delivery selector, or workflow
  behavior changes. The three test workflows and existing release graphs stay
  unchanged. New production seams require stopping and revising this plan.
- Preserve real storage, real CLI subprocesses, and real competing consumers.
  Inject faults only at the test-owned seam needed for the specific proof.
- Keep current backend markers and fixture defaults. Independent observation
  is local to commit tests; do not make all test Queues ephemeral.
- Retain tests at equivalent topology, lifecycle stage, and public boundary.
  The only removed assertion body is T4's substring gate, replaced in the
  owning executable-worker module in the same slice.
- No general mutation runner, timing framework, shell parser, Markdown library,
  coverage threshold, blanket mock ban, or broad duplicate-test deletion.
  Mutations and harmless-change probes are author-time checks recorded here.
- Test-owned events, threads, Queues, processes, and temporary scripts must be
  released on failure. Release coordination events in `finally` before stop or
  join; never let cleanup conceal the primary assertion failure. All waits
  and joins have bounded liveness; report the missing phase on failure.
- There are no one-way doors or rollout compatibility dependencies. Revert a
  failing test slice with its replacement/deletion and doc mapping together.
  Restoring an old test restores its known proof limitation, not a broker fix.
- Existing CI remains the deployment observation: the corrected selections
  pass in their current platform/backend jobs and failures name the actual
  missing condition. Do not claim a flake-rate reduction from one green run.

## Tasks

### 1. Independent commit observations (T1)

- [x] In both materialized variants, make the observer and destination
  `persistent=False`; keep the source persistent. Inspect source and
  destination through independent connections immediately after return.
- [x] Give `test_single_message_immediate_commit` an independent ephemeral
  observer and retain its single-message result and remaining-message checks.
  Keep its existing name and shared marker; no deletion or extra test is needed.
- [x] Author-time red proof on SQLite: remove only
  `BrokerCore._execute_transactional_operation`'s `self._runner.commit()` in
  an in-memory function replacement. Confirm each corrected case fails on
  external visibility before producer cleanup; restore the real callable and
  pass the same cases. No permanent mutation test or call-count assertion.
- [x] Run the three cases on the existing PostgreSQL and Redis wrappers too.
  Confirm selection executes, not all-skipped success. Stop if independent
  observation requires changing production sharing or fixture defaults.

### 2. Causal concurrency (T2 and T3)

- [x] In `test_concurrent_read_write`, set a writer-done event in the writer's
  `finally`. Drain until that event is set and a subsequent read returns empty,
  using an overall scaled deadline. Keep nonempty reads, errors, and completion
  distinct. Check writer/reader results and the exact multiset of the ten
  bodies; remove the three-empty counter and pacing sleeps used as ordering.
- [x] Preserve actual reader/writer overlap and the existing CLI boundary.
  Release/finish both futures on all outcomes with bounded waits. Reapply the
  0.8s first-write delay once at authoring: corrected test passes and observes
  all messages. Do not retain a suite of scheduler-delay parameters.
- [x] In the named pre-check/drain test, start with an empty queue and wait
  for the existing `main_loop_entered` event, after startup drain has finished.
  Then seed the initial messages. A local subclass/hook pauses after a real
  first positive pre-check, outside locks. Once seeding is complete and that
  phase is observed, a separate real consumer drains those messages, then
  releases the watcher.
- [x] Observe the now-empty drain specifically after the gated pre-check;
  startup drain cannot satisfy this event. Write one additional
  message and prove the watcher processes it. Retain exact accounting and
  uniqueness across both consumers. Replace the one-second sleep and misplaced
  delay with these events and bounded waits. Release gates before stopping
  the watcher or shutting down brokers, including failure paths.
- [x] Author-time mutation: replace the watcher's drain with a no-op. The
  corrected case must fail because watcher progress is missing. The real
  case must pass on SQLite and the existing PostgreSQL/Redis shared selections.
  Stop if this needs production hooks or changes other race tests.

### 3. Executable README DLQ proof (T4)

- [x] Split the Dead Letter Queue Pattern's Bash fence immediately before
  `# Retry failed messages`; keep the retry commands in a separate, explicitly
  manual example. Preserve the worker commands and single-consumer boundary.
- [x] In `tests/test_worker_examples.py`, extract the first Bash fence under
  that existing summary and execute it unchanged. Reuse the worker harness;
  provide `process_task_json` and a broker shim invoking the real CLI against
  a disposable explicit SQLite path. Use the current interpreter for the
  recipe's `python3` command so environment lookup cannot select another install.
- [x] Use one seeded message with an exact ID and newline-containing body.
  Parameterize the existing scenario pattern for handler success, handler
  failure with successful DLQ move, and handler failure with an injected
  move error. Inspect source/DLQ, including claimed rows: exact deletion on
  success, original ID/body in DLQ after routing, original pending in source
  after failed routing with nonzero exit. Handler receives the exact envelope.
- [x] Remove `test_readme_dlq_recipe_preserves_pending_work_on_failure`'s
  substring proof when its executable replacement is present. Other README
  and kernel tests keep their existing scope. Do not add malformed-message,
  concurrency, or worker-policy matrices.
- [x] Author-time checks: the delete-before-move mutation must fail the new
  test; harmless whitespace in `python3 -c` must still pass. ShellCheck the
  extracted fence. Stop and revise if executing the unchanged worker reveals
  a new product/example defect; do not silently widen this test-only scope.

### 4. Correct structural interpretation (T5 and T6)

- [x] Replace `_job_pytest_steps`' text splitting with parsed YAML job/step
  iteration in the existing module. Read each step's own `run`; use its name
  or ordinal for diagnostics, without using names as discovery keys. Preserve
  direct/wrapper classification, per-test timeout rules, explicit serial or
  bounded xdist rules, and wrapper job timeouts. No shell-command grammar is
  introduced; existing command spellings and wrappers remain the scope.
- [x] Adapt the gate's current discovery anchors so they prove nonempty real
  discovery without requiring presentational step names. Keep current helper
  negative cases. Add one focused adjacent named/unnamed-step regression so
  the latter cannot borrow bounds; an equivalent bounded unnamed step passes.
  Keep actual workflow files unchanged and reuse the installed dev PyYAML.
- [x] Correct `_without_fenced_blocks` using the fence-state rules from
  `bin/check-plan-context::strip_fenced_blocks`: opening character/length,
  valid indentation, same-character closer at least as long, no trailing
  nonspace closer content, other delimiter types inert inside a block.
  Keep this local and small; do not change the corpus gate's field vocabulary.
- [x] Extend `test_record_parser_ignores_fenced_examples` for the two reviewed
  inputs: a four-backtick block containing triple backticks and a sample REV
  heading yields no records; a triple-backtick block containing tildes must
  not hide the malformed live REV record after its real closer.
  Demonstrate both failures before repair, then pass with unchanged real corpus.
  Stop if correction requires new record syntax or normative changes.

### 5. Reconcile evidence and review

- [x] Record mutations, benign-change probes, executed selections, and any
  deletion/replacement owner. Keep evidence precise: a selected test's blind
  spot does not imply the whole suite misses the regression.
- [x] Independent review after each coherent slice and again on the combined
  result. Prefer a different family for final review; disposition findings and
  re-review accepted corrections only.
- [x] Align governing spec verification mappings and Related Plans, plus
  implementation test navigation only if named ownership changed. Evaluate
  used runbooks/skills; the existing rules already cover these findings, so do
  not add another policy layer or repeat established lessons without new evidence.
- [x] Run final gates below.
- [x] Close the plan/index in the same owner-authorized targeted commit,
  using explicit file-list staging and verifying `git log` at closeout.
  Publication and downstream edits are not part of this plan.

## Verification and Anti-Mocking

Focused SQLite/structural selection:

```bash
uv run --locked pytest -n 0 tests/test_delivery_contract_sb_delivery.py tests/test_exactly_once_delivery.py
uv run --locked pytest -n 0 tests/test_concurrency.py::test_concurrent_read_write tests/test_watcher_race_conditions.py::test_pre_check_drain_race
uv run --locked pytest -n 0 tests/test_worker_examples.py tests/test_agent_kernel_contract.py
uv run --locked pytest -n 0 tests/test_release_workflow.py tests/test_program_theory_contract.py tests/test_plan_context_gate.py
```

For each of `./bin/pytest-pg` and `./bin/pytest-redis`, run through
`uv run --locked` with `-n 0` and these exact existing shared nodes:

- `tests/test_delivery_contract_sb_delivery.py::test_materialized_batches_commit_before_return`
- `tests/test_exactly_once_delivery.py::test_single_message_immediate_commit`
- `tests/test_watcher_race_conditions.py::test_pre_check_drain_race`

Storage/CLI/competing consumers remain real. Only the targeted fault or ordering
is controlled; root SQLite owns the SQL omitted-commit mutation. The recipe's
failure shim may reject the named move; successful storage operations remain real.
Backend service absence is a disclosed verification gap, not a skipped pass.

Final: `uv run --locked pytest` once; `python3 bin/check-dom15-fixtures`,
`bin/check-plan-context`, `bin/check-doc-paths`, `git diff --check`; Ruff lint and
format for changed Python; ordinary-test mypy through
`bin/release.py::_core_test_mypy_command` (command construction only, never a
release action). No production changes mean no new Weft compatibility exercise,
artifact builds, broad coverage campaign, or repeat full-suite runs unless a
failure or accepted correction warrants them.

## Review Log

| Review | Evidence and disposition |
|--------|--------------------------|
| Separate implementation slice reviews | T1 PASS with three independently executed cases; T2/T3 PASS with two independently executed cases; T4 PASS with three scenarios and repeated destructive/benign probes. Root reviewed T5/T6 and independently verified the corrected rename-resistant regression. Details and findings are in the Execution Log. |
| Claude Code 2.1.207, combined implementation | PASS, no actionable code defects. Read-only source review against `b25db7f`; original input SHA-256 `9fcf853faccc2182def6f87a4dcd4540ea45a8b19bd9c875c80d3edf5aacf9d1`. First invocation reached its 540s review-tool timeout after inspecting all six corrections; resumed the same session for the final consistency check and verdict, exit 0, stderr empty. Reviewer did not execute tests. Precision corrections below. |
| Separate core reviewer, Tasks 1–3 | PASS with startup-order clarification R1 below; existing files, seams, fixture behavior, and selected nodes checked. Scoped R1 re-review PASS: actual startup order and exclusion of startup drain verified; no new ordering issue. |
| Claude Code 2.1.207, full plan | Read-only invocation with embedded plan, `--permission-mode plan --allowedTools Read,Grep,Glob`, closed stdin, 540s timeout, stdout/stderr captured. Reviewed input SHA-256 `e5e631241a48cc940d0a57a450eef3edd15aff903fdb9d3549b5d64cfd602299`. Exit 0, PASS, stderr empty; static source review, no reviewer execution claim. Three non-blocking observations dispositioned below. |

R1, verbatim: “seeding before watcher startup means the watcher performs an
initial drain before its first pre-check. Explicitly require the ‘empty drain
completed’ event to identify the drain after the gated positive pre-check,
excluding that startup drain.”

Disposition: accepted. Task 2 now starts empty, waits for the existing
`main_loop_entered` event, then seeds and gates the first positive pre-check.
The drain-completion event explicitly excludes startup. Source verification:
`BaseWatcher._run_with_retries` drains before `_process_messages`; the test
subclass publishes `main_loop_entered` from `_process_messages`. No new seam.


Full-plan verdict, verbatim: “The plan is implementable as written without
degrading correctness.”

| Observation (verbatim) | Disposition |
|------------------------|-------------|
| “Whether `destination` is the move actor or purely an observer should be confirmed at author-time” | Confirmed from `Queue.move_many`: `_move_destination_name` checks identity and extracts the name, then `self.get_connection()` performs the move. The destination's later peek is the independent observation. No task change required. |
| “Sibling same-session observers left in place” | Accepted boundary: those tests concern peeking/traversal, not external commit visibility. No scope expansion. |
| “`uv run --locked` in verification” | Retain the existing locked commands; dependency drift should be visible. |

Review precision: the review's T2 description used “dropped” for messages the
reader missed. The executed probe retained all ten correct messages; the
finding is a false test failure under benign scheduling, not product data loss.
R1's revised startup ordering received a separate scoped PASS after source
inspection. No other corrective round was needed.

## Revision Log

- 2026-09-07: Applied R1 after verifying actual watcher startup order. Other
  task boundaries and production behavior remain unchanged.

## Execution Log

Planning evidence: review at the baseline reproduced T1, T2, T5, T6 in the root
reviewer process; independent reviewers also executed the T3 no-op and T4
spacing/destructive mutations. The original focused commit, workflow, and
theory selection passed 24 tests despite those proof gaps. No implementation
or backend-matrix result is implied by this planning evidence.
Planning gates: 22 literal file references and three archived plan sources
resolve; DOM-15 fixtures, plan context, doc paths, and diff checks passed.
The plan-context, doc-gate, and theory selection passed 28 tests.


### Implementation entry, 2026-09-07

The owner authorized implementation per plan and requested causal explanations
for failures. Baseline remains `b25db7f`; the starting changes comprise only
this plan, its index row, and its two spec backlinks.

Comprehension: persistent same-thread handles may share one transaction, so
commit visibility must be observed through an independent ephemeral handle
before producer cleanup. A move uses the source connection; the destination
handle supplies its name and later performs the independent peek.

Comprehension: the race pauses after a real positive pre-check and outside
locks. Start empty, wait for `main_loop_entered` after initial drain, then seed;
startup drain cannot satisfy the post-check completion event.

Failure diagnosis: record planned injected-red evidence separately from
unexpected failures. For unexpected failures, trace the unchanged product
path and determine whether the cause is a product defect, a test/harness
assumption, or an environment limitation. Do not weaken the intended contract
or silently change production to obtain green results. Report concrete state,
trigger, and causal evidence to the owner; continue independent slices while
investigating.

Work split: root owns T1, deletion of the replaced T4 gate, and reconciliation;
separate implementers own causal concurrency, executable README proof, and the
two structural parsers in disjoint files.


### T1 verification and first failure diagnosis

- Changed the two materialized variants' source observer and destination to
  ephemeral handles; the single-message test now uses an independent observer.
  The producer stays persistent and open during every visibility assertion.
- Author-time omitted-commit mutation: all three cases failed on expected
  external state. Read still exposed both original rows; move's destination
  remained empty; the single-message observer still saw both messages. This is
  intentionally broken in-memory product code, not a bug in the unchanged broker.
- Unchanged production: the owning delivery/exactly-once modules passed 48
  tests. All three corrected cases passed on real PostgreSQL and Valkey through
  the existing wrappers, with no skips. Changed-file Ruff checks passed.
- Replaced the T4 substring gate after its executable three-case replacement
  was green; moved its delivery verification link to that actual firing owner.

| Failure | Diagnosis and evidence | Required action |
|---------|------------------------|-----------------|
| Initial parsed CI gate rejected the unchanged coverage-linux job | New test-gate implementation overlooked the step's own `env.PYTEST_ADDOPTS`; `.github/workflows/test.yml` supplies all required timeout and xdist controls there. No broker or workflow defect. | Corrected: each discovered step supplies its own command plus its own pytest options; no adjacent environment borrowing. |
| New adjacency regression failed after a harmless workflow step rename | Test setup used the real display name as a text insertion anchor. Root reproduced `DID NOT RAISE` while the valid renamed workflow still passed the gate. No product or workflow defect. | Corrected: append parsed step dictionaries. Root independently repeated the same harmless rename; both the unbounded rejection and bounded acceptance passed. |


### T4 execution and slice reviews

The executable README replacement passed its three scenarios and the combined
worker/kernel selection passed 62. The extracted fence passed ShellCheck,
Ruff, and mypy. The delete-before-move mutation failed on missing original
source state; harmless `python3  -c` spacing passed. These are author-time
mutations, not changes to the published worker; no product/example defect was
found. The retry demo was separated without changing worker commands.

Independent T1 review: PASS. Reviewer confirmed ephemeral connection ownership,
producer lifetime, source-owned move execution, retained assertions, and the
updated verification mapping; independently ran the three cases successfully.


### T2/T3 execution and review

The old CLI test failed after only a 0.8s first-write delay with an empty
observed list; the corrected case passes and accounts for all ten bodies.
The old watcher case passed with a process-local no-op drain; the corrected
case fails specifically because the watcher makes no progress after the rival
drain. Real production passes both cases. Owning concurrency/race modules:
15 passed, one benchmark deselected. The corrected race also passed once on
PostgreSQL and once on Valkey, with no skips, through the existing wrappers.

Independent review PASS: verified real CLI overlap, completion-before-empty
ordering, startup exclusion, rival drain before release, later watcher progress,
and bounded cooperative cleanup. Reviewer independently ran both cases: 2 passed.
A preliminary author-time delay probe imported the test before collection and
triggered pytest's assert-rewrite warning; moving injection to a collection hook
corrected that probe-harness mistake. It was not a product failure.

### T5/T6 execution and review

The original gates rejected neither the unnamed unbounded step nor the two
incorrect fence cases: the author-time red selection produced three expected
failures while two controls passed. The corrected workflow/theory modules
passed 75 tests. Workflow files and theory corpus remain unchanged. Root read
the final parser changes and independently verified both adjacency cases still
pass after a harmless workflow display-name change. The two unexpected gate
implementation failures are diagnosed in the table above; both are corrected.

Independent T4 review PASS: reviewer ran all three exact-fence scenarios,
repeated the harmless spacing probe successfully, and confirmed that an
injected delete before failed routing is rejected by original-message state.
This checks the documented single-consumer recipe; existing backend move tests
continue to own backend atomicity.

### Final verification

- `uv run --locked pytest`: 3414 passed, 18 skipped in 74.05s. Skips were
  existing platform, artifact, opt-in, backend-environment, and separate-probe
  selections; there were no failures. The four changed shared cases executed
  separately on each real PostgreSQL and Valkey backend: 4 passed per backend,
  no skips (three commit cases plus one race case).
- Changed-Python Ruff lint and format: all checks passed; seven files formatted.
  Ordinary-test mypy through the release command builder: no issues in 212 files.
- DOM-15 fixtures, plan context, doc paths, and diff checks passed. Final plan
  evidence edits receive the same documentation gates before handoff.
- No production Python, dependency manifests, workflow files, or public behavior
  changed. No unchanged-product failure was found. Planned fault injections
  correctly fail stronger tests; unexpected failures are causally separated above.
- Reviewed the testing, planning, and review guidance used for this work: existing
  rules already require causal coordination, independent visibility, behavioral
  recipe proof, and structural gates. No new policy, runbook, or lesson is needed.

### Final review disposition and handoff

Final verdict, verbatim: “Overall: PASS. All six corrections faithfully prove
their stated properties.” No actionable code findings or corrective code round.
The review's intermediate inspection and final verdict covered all six changes;
resumption completed that same review rather than starting another broad pass.

Two report inaccuracies need explicit correction. The reviewer called backend
execution an accepted gap: it is not a gap, because root ran all four targeted
shared cases on both real services, with no skips. Only the reviewer personally
did not run them. Its checkbox note used the older embedded plan: Tasks 2 and 4
were already marked checked in the current file. No source edit was required.
The isolated review-tool timeout was not a broker, test, or workflow failure.

The owner authorized closure and commit on 2026-09-07. This targeted commit
closes the plan and index together after the recorded implementation review
and verification. Closeout changes only plan status and this execution record;
documentation gates are rerun, with no reason to repeat the unchanged full suite.
Changed files are the seven test modules listed in Context and Key Files,
README fence separation, this plan and its index row, and the DOM/delivery
spec backlinks and verification mapping. No implementation navigation owner
was renamed, so no map change is needed.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Out of Scope

Production fixes, new public guarantees, new dependencies, wholesale backend
marker migration, general test deletion, new proof registries, benchmarks,
coverage-percentage targets, broad shell/Markdown parsing, worker runtime or
retry policy, coalescing, releases, and downstream repository changes.
