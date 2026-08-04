# Complexity and State-Machine Hardening Plan

Class: 5+P — this plan materially changes the repository verification policy,
adds a normative state-machine testing requirement, and coordinates risky
behavior-preserving refactors across public CLI, transaction, concurrency,
backend, release, subprocess, and example surfaces.

Plan type: implementation with spec revision.

Hardening: required. The work touches suspended transactions, cross-thread
poisoning, polling, background listeners, subprocess termination, release
publication, and a Python/Lua atomic protocol.

## Goal

Reduce accidental cyclomatic complexity without breaking cohesive algorithms
or scattering load-bearing state, and make every genuine state machine an
explicitly named contract with a table-driven test that fires every declared
transition. Fix the concrete correctness and false-confidence defects exposed
by the audit, refactor only the functions where a clearer seam improves
debugging or ownership, retain cohesive high-complexity functions deliberately,
and make every remaining high-complexity function visible through Ruff and
auditable through a narrow, reasoned suppression registered in the governing
spec.

## Requested Outcomes

- [x] Preserve the distinction between accidental complexity and justified
  complexity.
- [x] Give every genuine state machine a stable name, explicit states, events,
  guarded transitions, actions, terminal/error outcomes, and one table-driven
  firing suite.
- [x] Make scenario and integration tests supplement transition tables rather
  than substitute for them.
- [x] Fix the weak or tautological tests found during the C901 audit.
- [x] Fix the `MultiQueueWatcher` handler carry-over defect and align the
  copyable SQLite validation example with production lock-safety.
- [x] Refactor the P1 and P2 complexity findings without changing public
  SimpleBroker behavior, diagnostics, storage, CLI shape, backend semantics, or
  release ordering.
- [x] Keep the 25 cohesive P3 findings local, explicit, and individually
  justified in the [DOM-10.1.1] suppression registry.
- [x] Enable repository-wide C901 at Ruff's normal threshold
  (`lint.mccabe.max-complexity = 10`) early enough that later refactors operate
  under the audit gate.
- [x] Require exact agreement among raw `--ignore-noqa` C901 findings, narrow
  source suppressions, and the durable suppression registry.
- [x] Add no per-file ignore, global ignore, unexplained suppression, or
  baseline snapshot.
- [x] Verify SimpleBroker's primary downstream, Weft, against the refactored
  public and embedder surfaces.

## Premises and Decisions

The user has already confirmed the governing premises in this thread:

1. Cyclomatic complexity is a candidate signal, not a refactor mandate.
2. Coupling, debugging locality, and semantic risk outrank score reduction.
3. A readable known-item chain can be the best implementation.
4. Any genuine state machine must have an explicit table-driven suite covering
   all transitions, following the Weft standard.
5. The value of C901 is visibility and auditability. High complexity is not
   itself a defect; high complexity without a reviewed reason is.
6. A justified high-complexity function should remain cohesive and carry a
   narrow local suppression whose reason and proof are registered under
   [DOM-10.1.1]. Broad or unregistered suppression remains prohibited.

The useful state-machine boundary is narrower than “code with branches.”
Behavior qualifies when state persists across calls, yields, threads,
callbacks, processes, or invocations and changes which next events are legal or
which actions must occur. A one-pass checklist, parser-precedence chain,
ordinary scan, fixed-mode dispatcher, or retry loop with only local loop
control does not qualify solely because it can be drawn as boxes and arrows.

## Source Documents

Source specs and process rules:

- `docs/specs/01-development-documentation-operating-model.md`
  [DOM-5], [DOM-6], [DOM-10], [DOM-10.1], [DOM-11], [DOM-15]
- `docs/agent-context/engineering-principles.md`, especially principles 4,
  8, 9, 10, 12, and 14
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`

Product and implementation contracts:

- `docs/specs/10-cli.md` [SB-CLI-*]
- `docs/specs/11-delivery.md` [SB-DELIVERY-1..7]
- `docs/implementation/04-cross-thread-finalization-poisoning.md`
- `docs/implementation/06-process-session-core-ownership.md`
- retired plan `2026-07-29-code-quality-cleanup-plan.md` at `197629e2`
- `docs/plans/2026-07-29-ruff-lint-expansion-plan.md`

Source audit:

```bash
uv run --frozen --no-sync ruff check --select C901 .
```

At the authoring baseline this reports 76 functions: 32 production, 13
repository tooling/examples, and 31 tests. Disposition: 25 keep, 28 extract
coupled helpers, 9 simplify locally, and 14 redesign an internal seam.

## Spec Baseline

- `1324a1f647b7c8fc9b14a6aed6bae696aefe62ea` —
  `docs/specs/01-development-documentation-operating-model.md`,
  `docs/specs/10-cli.md`, and
  `docs/specs/11-delivery.md` at plan authoring time.
- This plan revises the verification policy in
  `docs/specs/01-development-documentation-operating-model.md`; product
  behavior remains governed by the two product specs above.
- Promotion baseline identifier: not preserved before implementation. The
  implementation checkpoint contains both the promoted policy and its
  realization, so it is not misrepresented as a promotion-only baseline. See
  the deviation log for the process failure and closure disposition.
- Implementation and promoted-policy realization:
  `86f73f5e8d4e05bb768b780af4f4183458fe56eb`. This is the first immutable
  checkpoint containing the completed implementation; it does not retroactively
  satisfy the missing promotion-only baseline.

## Proposed Spec Delta

Promotion strategies:

| Spec file | Strategy | Sections |
|-----------|----------|----------|
| `docs/specs/01-development-documentation-operating-model.md` | A — in-file policy text before implementation-link claims | New [DOM-10.2] |
| `docs/specs/01-development-documentation-operating-model.md` | B — atomic with Ruff configuration and firing policy tests | [DOM-10.1] C901 paragraph |

### Add after [DOM-10.1.1]

> ### State-Machine Transition Gate [DOM-10.2]
>
> Every genuine state machine in production code, repository tooling, examples,
> or test infrastructure must be a named unit with an executable transition
> contract.
>
> A state machine exists when state persists across calls, yields, threads,
> callbacks, processes, or invocations and changes which next events are legal
> or which actions and outcomes must occur. A one-pass decision tree, validation
> checklist, parser-precedence chain, fixed-mode dispatcher, scan, or ordinary
> retry loop does not qualify solely because its control flow can be drawn as a
> graph.
>
> Owner: the module that owns the persistent state owns the machine; its closest
> contract test owns the executable transition table. Boundary: all first-party
> Python code, including extensions, repository tooling, examples represented
> as supported or copyable, and reusable test-process helpers. Verification:
> each machine has a table-driven test whose rows name a stable transition ID,
> start state, event, guard or precondition, next state, required actions or
> durable effects, and expected error or terminal result. Required action: a
> machine may not be refactored, extended, or declared verified until every
> legal, rejected, terminal, and failure transition in its declared contract
> has a firing row.
>
> Requirements:
>
> - transition rows are the executable contract; scenario, property, race, and
>   integration tests supplement them but do not replace them
> - every declared transition ID fires exactly once in the table owner, while a
>   case may execute against more than one backend or operating mode
> - forbidden events and failure precedence are transitions when they alter
>   state, preserve state, or select a different terminal outcome
> - concurrency and persistence transitions use real threads, processes,
>   filesystems, SQLite databases, and backend integration fixtures where those
>   interactions are the behavior under test; mocks may replace only external
>   nondeterministic transports, clocks, or fault sources, never the state owner
> - production enums or a generic state-machine framework are not required;
>   introduce them only when they make the owned state or transition seam
>   clearer
> - the implementation inventory maps every named machine to its owner,
>   transition-table test, integration proof, and governing product or process
>   contract
> - when discovery changes the machine boundary or transition set, update the
>   transition table and implementation inventory in the same change

### Add to [DOM-10.1] in the atomic C901 activation slice

> Ruff's `C901` rule is enabled repository-wide with
> `lint.mccabe.max-complexity = 10`. The score is a visibility signal, not a
> design verdict. Each finding must either be simplified around a real
> ownership seam or carry a narrow local `C901` suppression registered in
> [DOM-10.1.1]. The registry must explain why coupling, debugging locality, or
> semantic risk justifies retaining the function; name the real behavioral
> proof; record rejected decompositions and approval; and identify the exact
> source location and count.
>
> The policy test runs normal Ruff and a raw `C901` audit with `--ignore-noqa`.
> Raw findings (using Ruff's reported `noqa_row`), source directives, and
> [DOM-10.1.1] registry entries must match exactly by repository-relative path
> and line. A new unsuppressed finding, an unregistered directive, a stale
> directive, a stale registry entry, or moved code fails verification.
> Per-file ignores, global ignores, and baseline allowlists are not permitted.
> A cohesive parser, checklist, or state machine must not be fragmented merely
> to lower its score.

### Amend [DOM-10.1.1] with the C901 activation slice

> For an approved registered exception, the local
> `approved [DOM-10.1.1] exception` text is the required nearby reason pointer;
> the registry row is the single durable full reason. Do not duplicate the full
> rationale in source comments. A temporary C901 row must also name the plan
> task that removes or re-evaluates it.

The spec-promotion slice must also add this plan under `## Related Plans`.
Product specs need no normative delta because all runtime refactors are
contract-preserving. If implementation discovers that a public result,
diagnostic, timing guarantee, storage format, or delivery semantic must change,
stop and add an explicit product-spec deviation before continuing.

## Current Structure and Key Files

### Complexity and policy ownership

- `pyproject.toml` owns Ruff rule selection. C901 is currently an explicit
  audit command, not part of the normal lint gate.
- `tests/test_ruff_policy.py` proves repository discovery, effective rule
  selection, and the approved suppression registry.
- `docs/specs/01-development-documentation-operating-model.md` [DOM-10.1]
  owns the durable static-analysis contract.
- Add `docs/implementation/07-complexity-and-state-machine-map.md` as the
  durable implementation inventory. It records rationale and ownership, not a
  second normative product contract.

### Core production ownership

- `simplebroker/cli.py::main` owns argument normalization, global actions,
  target resolution, SQLite preparation, command dispatch, and error
  translation in one function.
- `simplebroker/_constants.py::load_config` separately declares environment
  parsing while `_CONFIG_NORMALIZERS` declares override parsing.
- `simplebroker/db.py` owns concrete connection ownership, cleanup, sidecar
  transactions, transactional generator delivery, and SQLite broadcast.
- `simplebroker/watcher.py::PollingStrategy` owns native-waiter versus polling
  mode, burst/backoff, activity hints, waiter replacement, and stop behavior.
- `simplebroker/_phaselock.py` owns process and OS lock acquisition plus durable
  per-phase completion.

### Extension and tool ownership

- `extensions/simplebroker_redis/simplebroker_redis/core.py::broadcast` owns
  selector normalization, a separate pattern path, timestamp reservation,
  capacity growth, conflict handling, and the Lua result-code protocol.
- `extensions/simplebroker_pg/simplebroker_pg/runner.py::_SharedActivityListener`
  owns listener startup, notification routing, failure publication, and close.
- `.github/scripts/combine_coverage.py::_wait_for_stable_sources` owns the
  temporal coverage-shard settlement and repair policy.
- `bin/release.py` owns single and synchronized release planning, safety gates,
  generated-file decisions, commit creation/reuse, CI observation, and tag
  publication.

### Test and example ownership

- `tests/helper_scripts/managed_subprocess.py` currently contains two overlapping
  subprocess termination implementations.
- `tests/helper_scripts/cross_thread_generator_probe.py`,
  `tests/test_watcher_multiprocess.py`, and
  `tests/helper_scripts/watcher_sigint_script_improved.py` encode reusable actor
  protocols without explicit transition tables.
- `examples/async_pooled_broker.py::stream_read` spans several async-generator
  transaction modes without direct runtime tests.
- `examples/reference_reactor.py` has a real behavior suite but no compact
  scheduling transition table.
- `examples/multi_queue_patterns.py` contains nested priority and monitoring
  scheduler machines with no direct tests.

### Required reading comprehension gates

Before editing the delivery machines, the implementer must be able to answer:

1. Which thread owns the SQLite lock and transaction while a generator is
   suspended, and why must a foreign thread publish poison without rollback or
   lock release?
2. Which cleanup failure wins for ordinary owner-thread teardown, and which
   diagnostic wins after poison?

Before editing Redis broadcast, the implementer must be able to answer:

1. Which target set is selected inside Lua at the atomic insertion point?
2. What do result codes `1`, `-1`, `-2`, `-3`, `-4`, `-5`, and `-6` mean, and
   which retry counter or terminal action does each select?

Before editing polling or listener code, the implementer must be able to answer:

1. Which activity hints are local, native, or data-version-derived?
2. Which replacement and close operations own the waiter, and which merely
   detach it?

Before editing release or coverage tooling, the implementer must be able to
answer:

1. Which state is durable outside the process and must be safe on rerun?
2. Which corruption or publication failures are fatal, and which narrowly
   recognized interrupted states may be repaired or excluded?

## State-Machine Seed Inventory

The spec-promotion and characterization slices must validate this inventory
against code. A changed classification is a deviation requiring evidence, not
an unlogged implementation choice.

| ID | Named machine | Current owner | Required table owner |
|----|---------------|---------------|----------------------|
| SM-SQLITE-SCHEMA | SQLite v3 schema reconciliation | `simplebroker/_backends/sqlite/schema.py` | `tests/test_sqlite_schema.py` |
| SM-DUMP-LOAD | Dump-stream parser and batch mutation | `simplebroker/_dump.py` | `tests/test_dump_load.py` |
| SM-TIMESTAMP-GENERATOR | Initialization, physical/logical clock, CAS retry, reservation, and fork reset | `simplebroker/_timestamp.py` | timestamp edge, resilience, and backend integration tests |
| SM-DARWIN-XATTR | Darwin xattr-provider discovery and cache | `simplebroker/_phaselock.py` | `tests/test_phaselock.py` |
| SM-PHASE-LOCK | Advisory acquisition and durable phase completion | `simplebroker/_phaselock.py` | `tests/test_phaselock.py` |
| SM-CONNECTION | `DBConnection` ownership and cleanup | `simplebroker/db.py` | `tests/test_db_connection_lifecycle.py` plus process-session integration |
| SM-PROCESS-SESSION | Process broker session creation/close arbitration | `simplebroker/_broker_session.py` | `tests/test_process_broker_session.py` |
| SM-SETUP-BUDGET | Setup progress budget | `simplebroker/helpers.py` | `tests/test_helpers_coverage.py` |
| SM-DELIVERY-POISON | Sidecar and transactional-generator ownership/poison | `simplebroker/db.py` | `tests/test_cross_thread_finalization_poisoning.py` |
| SM-POLLING | Native/fallback polling and burst lifecycle | `simplebroker/watcher.py` | `tests/test_watcher.py` |
| SM-WATCHER-LIFECYCLE | Base watcher start, run, retry, stop, and cleanup across threads | `simplebroker/watcher.py` | watcher lifecycle, edge-case, concurrency, and race tests |
| SM-CLI-WATCH | Watch callback, output, interrupt, and shutdown lifecycle | `simplebroker/commands.py` | `tests/test_cli_watch.py` |
| SM-PG-LISTENER | PostgreSQL shared activity listener | PostgreSQL `runner.py` | PostgreSQL notify/lifecycle tests |
| SM-PG-VACUUM | PostgreSQL maintenance lease, advisory lock, delete batches, maintenance, and release | PostgreSQL `plugin.py` | PostgreSQL maintenance and contract-edge tests |
| SM-REDIS-BROADCAST | Redis Python/Lua broadcast protocol | Redis `core.py` and Lua script | Redis integration/atomicity tests |
| SM-REDIS-ACTIVITY-LISTENER | Redis listener readiness, registration/refcount, notification, failure, and close | Redis `plugin.py` | Redis listener lifecycle and contract-edge tests |
| SM-SQLITE-RUNNER | Per-thread SQLite connection, fork recovery, setup-marker, and close lifecycle | `simplebroker/_runner.py` | runner lifecycle, fork-safety, and setup tests |
| SM-REDIS-RUNNER | Lazy Redis client/pool creation, fork reset, and close lifecycle | Redis `runner.py` | Redis runner lifecycle and fork tests |
| SM-COVERAGE-SETTLEMENT | Coverage shard settlement/repair | `.github/scripts/combine_coverage.py` | `tests/test_dev_scripts.py` |
| SM-CLI-COVERAGE | CLI coverage staging/publication | `tests/conftest.py` | `tests/test_dev_scripts.py` |
| SM-RELEASE | Single and synchronized release workflow | `bin/release.py` | `tests/test_release_script.py` |
| SM-ASYNC-STREAM | Async example delivery generator | `examples/async_pooled_broker.py` | new example runtime tests |
| SM-REACTOR | Reference reactor scheduling | `examples/reference_reactor.py` | `examples/tests/test_reference_reactor.py` |
| SM-REACTOR-OUTPUT | Pending-output publication, retry, success, and terminal error | `examples/reference_reactor.py` | `examples/tests/test_reference_reactor.py` |
| SM-PRIORITY-WATCHER | Priority multi-queue scheduling | nested example watcher | new multi-queue example tests |
| SM-MONITORING-WATCHER | Monitoring dispatch lifecycle | nested example watcher | new multi-queue example tests |
| SM-SUBPROCESS | Managed subprocess escalation and reader cleanup | `tests/helper_scripts/managed_subprocess.py` | direct helper tests |
| SM-CROSS-THREAD-PROBE | Cross-thread probe actor protocol | cross-thread probe helper | direct protocol tests plus backend probes |
| SM-MULTIPROCESS-WATCHER | Multiprocess watcher worker protocol | `tests/test_watcher_multiprocess.py` | same file |
| SM-SIGINT-PROBE | Watcher SIGINT helper lifecycle | SIGINT helper script | watcher subprocess tests |

The Redis body scan, configuration and path validators, timestamp parsers,
retry helper, CLI dispatcher, and repository-settings checklist are not state
machines under this definition. They still receive normal behavior tests and
complexity dispositions. T2 must explicitly resolve borderline candidates
rather than treating this seed list as closed; in particular, an omitted
persistent cache, callback, lease, or actor protocol is a missing machine even
when the enclosing C901 finding looks like an ordinary parser or loop.

## Complexity Disposition Ledger

### P1: address in near-term slices

| Function | Required outcome |
|----------|------------------|
| `_validate_safe_path_components` | Extract same-file component and dangerous-character helpers; preserve validation order and messages. |
| `load_config` | Use one private field schema for environment and override coercion; preserve every key/default/conversion. |
| `cli.main` | Separate parsing, global actions, legacy SQLite preparation, and dispatch; retain one public entry point and explicit command dispatch. |
| `DBConnection.cleanup` | Name ownership snapshot/drain and best-effort close operations inside `DBConnection`. |
| Redis `broadcast` | Name selector and Lua result states; separate pattern and atomic strategies while retaining one retry owner. |
| `_wait_for_stable_sources` | Introduce a named inspection result; retain one polling/deadline owner. |
| `release.main` | Extract the single-target workflow and share only genuine phase planning with batch release. |
| async `stream_read` | Split fixed delivery modes into named private generators after transition characterization. |
| `MultiQueueWatcher.__init__` | Fix handler carry-over and extract validation/entry construction. |
| example `validate_database_path` | Use production-equivalent SQLite-managed validation and add live-WAL lock proof. |
| `managed_subprocess` | Give `ManagedProcess` one idempotent escalation/close owner. |
| improved SIGINT helper `main` | Replace implicit readiness with a deterministic protocol. |
| `test_database_creation_timing` | Rewrite as an exact protocol assertion or delete when stronger coverage subsumes it. |
| `test_concurrent_database_access` | Require all intended workers and clean shutdown or delete the diagnostic test. |
| `test_graceful_shutdown_sigint` | Fail when SIGTERM/SIGKILL fallback is used. |
| `test_multiple_peek_watchers` | Require every watcher to observe the exact expected sequence. |

### P2: improve within the owning slice

Core:

- `_AdvisoryLock.acquire`
- `packaging_smoke_main`
- `_parse_with_unit_suffix`
- `cmd_move`
- `cmd_watch`
- `cmd_init`
- `DBConnection.get_connection`
- `BrokerCore.broadcast`
- `Queue.stream_messages`

Extensions, tooling, and examples:

- `_SharedActivityListener._run`
- Redis `cleanup_target`
- `coalesce-check.main`
- `_run_batch_release`
- `PooledAsyncSQLiteRunner.run`
- `pattern_2_priority_simulation`
- `pattern_5_monitoring`

Tests and harness:

- `run_cli`
- cross-thread probe `owner`, `_execute_probe`, and `_queue_close_probe_child`
- failed-core-creation and post-close process-session tests
- schema idle-budget test setup
- dependency AST scan
- polling jitter
- concurrent watcher writes
- multi-queue concurrent activity and pre-check contention
- both watcher multiprocess worker functions and all five C901 multiprocess
  scenarios

### P3: retain; add transition coverage where applicable

The following 25 functions remain cohesive. Do not split them to improve a
number:

- `ensure_schema_v3`
- `validate_database`
- `load_lines`
- `_darwin_xattr_provider`
- `PhaseLockService.run_phases`
- `execute_retry`
- `_extract_pytest_runner_overrides`
- `TimestampGenerator.validate`
- `_parse_numeric_timestamp`
- `_process_queue_fetch`
- `BrokerCore.sidecar`
- `BrokerCore._yield_transactional_batches`
- `Queue.move`
- `PollingStrategy.wait_for_activity`
- PostgreSQL `vacuum`
- Redis `find_message_ids`
- `repository_settings_issues`
- `BaseReactor._drain_queue`
- standalone `validate_safe_path_components`
- PostgreSQL vacuum contract-edge test
- `BenchmarkSettings.validate`
- `_execute_sidecar_probe`
- backend-plugin target-resolution test
- burst-mode state-transition scenario
- multi-active-queue thundering-herd test

P3 means no structural refactor. It does not waive missing transition tables,
weak assertions, documentation drift, or specific correctness defects. Each P3
finding receives a narrow `C901` directive and an exact [DOM-10.1.1] registry
entry whose reason addresses coupling, debugging locality, and rejected
decompositions rather than merely restating its score. P1 and P2 findings that
remain above 10 after a sound refactor use the same reviewed path; they must not
be split further just to avoid registration.

## Invariants and Constraints

### Public and downstream contracts

- No public function, class, import path, option, environment variable, exit
  code, stdout/stderr format, backend protocol, or return shape changes.
- `simplebroker.ext.PollingStrategy`, `Queue`, `BrokerDB`, `BrokerTarget`, and
  backend discovery remain compatible with Weft.
- The dump format remains `simplebroker-dump` v1 with current streaming and
  partial-mutation behavior.
- Release commands retain exact safety ordering and dry-run truthfulness.
- This is patch-level, behavior-preserving work unless a discovered defect
  requires a separately approved contract change.

### Transaction and concurrency invariants

- Foreign-thread finalization never rolls back, commits, releases the owner's
  lock, clears owner batch state, or transfers a session lease.
- Poison is one-way and first-cause preserving.
- Owner-thread exception identity and cleanup-failure precedence remain exact.
- At-least-once batches commit only after the complete batch is yielded and
  replay after early close or failure.
- Redis broadcast retains atomic target selection, monotonic timestamp fencing,
  zero-target no-op behavior, capacity growth, wakeup scope, and error
  translation.
- Polling retains waiter ownership, immediate activity response, bounded
  fallback sleeps, and stop responsiveness.

### Complexity and layering invariants

- Extract helpers only around named ownership seams. Do not create pass-through
  wrappers, generic command frameworks, generic state-machine engines, or new
  modules merely to lower C901.
- Keep coupled helpers in the same file or owning class unless there are two
  real adapters at a seam.
- The final command dispatch, known-record parser chains, audit checklists, and
  tightly coupled suspended transaction frames may remain explicit.
- Normal Ruff fails every new, unsuppressed function above complexity 10.
- Raw Ruff with `--ignore-noqa` keeps every approved high-complexity function
  visible. Its locations must exactly equal both the local `C901` directives
  and the durable [DOM-10.1.1] registry.
- Every retained finding has a reason tied to coupling, debugging locality, or
  semantic risk, plus real behavioral proof and rejected alternatives.
- `RUF100` and the exact-match policy make obsolete suppressions and registry
  entries fail when a function is simplified or moved.
- No new dependency.
- No circular import may be introduced to house a helper or test contract.

### Error priorities

- Data-integrity, malformed-protocol, corruption, publication, and impossible
  state transitions remain fatal.
- Logging, warning emission, reader cleanup, and best-effort owned-resource
  cleanup must not replace a primary failure unless the existing contract says
  cleanup failure wins.
- Test cleanup may force-terminate a leaked subprocess, but the behavioral
  assertion must record that fallback as failure when graceful termination was
  the contract under test.

## Anti-Mocking Rules

- Use real SQLite files, locks, transactions, generators, threads, and spawned
  processes for SQLite ownership and poison transitions.
- Run PostgreSQL listener and vacuum transitions through the extension harness;
  do not replace lease, advisory-lock, or notification behavior with call-count
  mocks.
- Run Redis broadcast transitions against the real Redis/Valkey integration
  harness and real Lua script. Focused fault injection may control candidate
  timestamps or script return values only when the real server cannot
  deterministically produce the transition.
- Coverage settlement tests use real coverage SQLite files and real atomic
  replacement.
- Release tests may fake GitHub, PyPI, CI, and subprocess transports, but the
  release planner and phase ordering remain real and results must be asserted,
  not only mock call counts.
- Polling tests may inject clocks, RNG, activity waiters, and version providers;
  the `PollingStrategy` state owner remains real.
- A transition-table row is not complete when all state changes were performed
  by the test double.

## Rollout and Rollback

There are no data migrations or intentional public-contract changes. The safe
sequence is:

1. promote [DOM-10.2] and add the implementation inventory
2. atomically enable C901 at 10 with the reviewed suppression registry and
   exact-match audit test
3. land characterization/transition tables before structural refactors
4. fix concrete correctness and false-confidence defects
5. refactor one coherent ownership slice at a time, updating the registry when
   an approved finding is removed, moved, or remains justified
6. run full SimpleBroker and downstream Weft verification

Each refactor slice is independently revertible. If a behavioral regression is
found, revert that structural slice while retaining the spec, transition
inventory, characterization tests, and C901 audit policy. If an exception is
disputed, keep the finding visible and re-open its registry rationale; do not
raise the threshold, add a broad ignore, or fragment the function before its
ownership and debugging tradeoffs are reviewed. The C901 configuration, source
directives, registry entries, and policy test form one atomic policy slice for
rollback.

One-way doors: none expected. Stop and re-plan if implementation requires a
storage-format change, public deprecation, backend protocol version change,
new dependency, new process/thread lifecycle, or weaker failure behavior.

## Dependency-Ordered Tasks

### T1 — Independent plan and proposed-delta review

- Review this plan, [DOM-10.2], the C901 audit policy, the 76-finding
  inventory, the proposed exception rationales, and the seed machine inventory
  before implementation.
- Reviewer must challenge over-classification, missing machines, transition
  completeness, weak exception reasons, hidden findings, slice size,
  public-contract risk, and performative abstraction.
- Resolve every finding in the revision/review log.
- Done signal: reviewer says a zero-context engineer can execute the plan and
  every risky boundary is explicit.

Stop and re-plan if the reviewer cannot distinguish the state owner from its
adapters, or if complete transition enumeration would require changing product
behavior.

### T2 — Spec-promotion slice and implementation inventory

Files:

- `docs/specs/01-development-documentation-operating-model.md`
- `docs/implementation/07-complexity-and-state-machine-map.md`
- `docs/implementation/00-implementation-index.md`
- `docs/plans/2026-07-29-complexity-and-state-machine-hardening-plan.md`

Actions:

1. Promote [DOM-10.2] using strategy A and add the related-plan backlink.
2. Record the promotion baseline identifier.
3. Create the implementation map with all 76 C901 dispositions and every
   validated state-machine owner/table/integration proof.
4. For every seed machine, record `confirmed`, `merged with <ID>`, or
   `not-machine` plus evidence. Do not silently change the list.
5. Draft exact [DOM-10.1.1] rows for all 76 findings present when T3 activates
   C901. P1/P2 rows are explicitly temporary and name the T9-T12 removal or
   re-evaluation slice; P3 rows state why the cohesive structure is retained.
   Group locations only when they share one protected invariant, proof,
   rejected alternative, approval, and planned disposition; counts and
   locations must remain exact.
6. Obtain independent review and user approval of all initial rows before T3
   enables them. Do not add source suppressions while their rationale is
   unresolved.

Verification:

```bash
python3 bin/check-dom15-fixtures
bin/check-doc-paths
git diff --check
```

Done signal: one durable inventory exists, no transition owner is ambiguous,
the promoted spec is the only normative verification contract, and every
proposed retained finding has an approved reason rather than a score-only
waiver.

### T3 — Atomic C901 visibility and auditability gate

Files:

- `pyproject.toml`
- `tests/test_ruff_policy.py`
- `tests/fixtures/ruff-enabled-rules.txt`
- `docs/specs/01-development-documentation-operating-model.md`
- all 76 approved source locations from the implementation inventory
- implementation inventory

Actions:

1. Add `C901` to the normal Ruff selection and set
   `lint.mccabe.max-complexity = 10`.
2. Update the configured-family expectation and effective enabled-rule fixture,
   and add a normal-config firing probe that fails on complexity 11 while
   accepting complexity 10.
3. Add only narrow local directives in the approved form to every one of the
   76 initial findings:
   `# noqa: C901 approved [DOM-10.1.1] exception`. Keep the full reason in the
   spec registry so there is one durable rationale to audit. Temporary P1/P2
   entries remain suppressions, not a baseline: each is individually reviewed,
   exact, visible to raw Ruff, and tied to its named removal slice.
4. Add the approved rows to [DOM-10.1.1], amend its nearby-reason rule, and
   promote the [DOM-10.1] paragraph in the same change.
5. Extend `tests/test_ruff_policy.py` to run normal Ruff and to parse
   `ruff check --select C901 --ignore-noqa --output-format json .`. Require
   exact equality by repository-relative path and Ruff's `noqa_row` among raw
   findings, source directives, and registry locations and counts.
6. Add firing fixtures proving that normal Ruff rejects a new unsuppressed
   function above 10; a registered local exception remains visible in the raw
   audit; an unregistered, stale, moved, or overly broad exception fails; and
   `RUF100` removes obsolete directives from the legal state.
7. Assert that no C901 per-file ignore, global ignore, blanket file directive,
   or baseline allowlist exists.

Done signal: normal `ruff check .` is clean, the raw audit reports every
approved finding despite `noqa`, all three inventories match exactly, and each
policy mutation above causes a targeted test failure.

### T4 — Transition-table test foundation

Files:

- `tests/helpers/state_machine_contracts.py`
- `tests/state_machine_manifest.py`
- closest existing test file for each machine
- `tests/test_state_machine_policy.py`
- implementation inventory

Actions:

1. Add a test-only immutable `TransitionCase` metadata type with the common
   required fields: stable transition ID, start state, event, guard or
   precondition, expected next state, actions/effects, and terminal/error
   result. Add a small `fires_transition_table(machine_id, table)` decorator
   that attaches explicit manifest metadata and delegates to
   `pytest.mark.parametrize`. Allow a typed machine-local payload; do not force
   unrelated machines into one execution API.
2. Keep each machine's table and executor beside its closest behavior tests.
   Each firing test uses `fires_transition_table` with its named table constant,
   and each case ID includes the machine and transition IDs.
3. Add a small manifest mapping every machine ID to its production owner, test
   module, table constant, and firing-test function. This manifest contains
   metadata and import references only, not unrelated transition
   implementations.
4. Add a structural policy test that imports each manifest entry and detects
   duplicate machine IDs, missing table owners, duplicate transition IDs,
   empty required fields, inventory rows without a manifest entry, manifest
   entries without inventory rows, and firing tests that do not parameterize
   from the declared table constant.
5. Require each transition table to report exactly which row failed.

The policy test must not claim semantic completeness from source parsing.
Completeness is established by the machine-specific table, independent review,
and the real integration proof. The helper and manifest are test metadata, not
a production state-machine framework.

Done signal: a deliberately incomplete fixture fails the policy test for each
structural deficiency, and one pilot machine demonstrates the complete pattern.

### T5 — Core persistent-state transition contracts

Machines:

- SM-SQLITE-SCHEMA
- SM-DUMP-LOAD
- SM-TIMESTAMP-GENERATOR
- SM-PHASE-LOCK
- SM-CONNECTION
- SM-PROCESS-SESSION
- SM-SETUP-BUDGET
- SM-DELIVERY-POISON
- SM-POLLING
- SM-WATCHER-LIFECYCLE
- SM-DARWIN-XATTR
- SM-CLI-WATCH
- SM-SQLITE-RUNNER

Files:

- current production owners
- `tests/test_sqlite_schema.py`
- `tests/test_dump_load.py`
- `tests/test_property_dump_load.py`
- `tests/test_phaselock.py`
- `tests/test_db_connection_lifecycle.py`
- `tests/test_process_broker_session.py`
- `tests/test_helpers_coverage.py`
- `tests/test_cross_thread_finalization_poisoning.py`
- `tests/test_generator_methods.py`
- `tests/test_watcher.py`
- `tests/test_watcher_edge_cases.py`
- `tests/test_watcher_burst_mode.py`

Actions:

1. Write transition inventories before changing production structure.
2. Cover legal, rejected, empty, terminal, timeout, cancellation, poison,
   rollback/commit failure, idempotent cleanup, replacement, and stop
   transitions.
3. Retain existing race, property, and scenario tests as independent
   integration evidence.
4. Explicitly cover dump duplicate-header, blank-only EOF, batch
   `size-1/size/size+1`, and partial-mutation-before-later-error behavior.
5. Explicitly cover owner/foreign × `next`/`throw`/`close` × rollback outcome
   for the delivery machine without manufacturing an unsafe foreign cleanup.
6. Cover Darwin provider discovery success/failure caching, the ERANGE
   reprobe/read cycle, and concurrent first initialization.
7. Cover watch output, one-time newline warning, clean stop, interrupt,
   callback error, broken pipe, flush failure, and final watcher cleanup.
8. Cover base-watcher start, retry, terminal error propagation, stop-before/during wait,
   thread exit, waiter detachment, and idempotent cleanup using real lifecycle,
   concurrency, and race proofs.
9. Cover SQLite runner per-thread connection create/reuse/close, fork reset,
   setup-marker success/failure, owned/borrowed connection handling, and
   repeated close.
10. Cover timestamp lazy initialization, physical-clock advance/regression,
    logical-counter increment/overflow wait/failure, CAS loss/reload/retry,
    local reservation/refresh, and fork reset with real concurrent CAS and
    backend integration proof where those interactions are the behavior.

Done signal: every table passes against the unrefactored behavior and the
implementation map names no uncovered transition.

### T6 — Extension and tooling transition contracts

Machines:

- SM-PG-LISTENER
- SM-PG-VACUUM
- SM-REDIS-BROADCAST
- SM-REDIS-ACTIVITY-LISTENER
- SM-REDIS-RUNNER
- SM-COVERAGE-SETTLEMENT
- SM-CLI-COVERAGE
- SM-RELEASE

Files:

- PostgreSQL runner and notify/lifecycle tests
- Redis core, Lua broadcast script, integration and atomicity tests
- `.github/scripts/combine_coverage.py`
- `tests/conftest.py`
- `tests/test_dev_scripts.py`
- `bin/release.py`
- `tests/test_release_script.py`
- `tests/test_release_publication_script.py`

Actions:

1. Enumerate PostgreSQL startup, routing, unknown-notification, failure, stop,
   and close transitions.
2. Enumerate PostgreSQL vacuum lease acquisition/release, advisory-lock
   refusal/acquisition, delete commit/rollback/failure, compact/analyze/no-op,
   unlock warning/failure, and final lease release.
3. Enumerate every Redis selector guard and Python-visible Lua status code at
   each allowed retry count, including impossible-code handling.
4. Enumerate Redis activity-listener readiness, registration/refcount,
   notification routing, unknown queues, read failure, unregister, stop, and
   idempotent close.
5. Enumerate Redis runner lazy client/pool creation, reuse, fork reset,
   borrowed/owned resource handling, failure, and repeated close.
6. Enumerate coverage snapshot changes from every settling state, mixed
   empty/corrupt inputs, repair success/failure, exclusion, and deadline edges.
7. Enumerate release planning, dry-run, dirty state, version/file changes,
   commit reuse/create, CI success/failure, tag action, and safe rerun.
8. Prefer pure release planning inputs for table cases; retain black-box
   command tests for destructive ordering.

Done signal: every machine has a table and a separate real integration or
black-box proof.

### T7 — Example and test-protocol transition contracts

Machines:

- SM-ASYNC-STREAM
- SM-REACTOR
- SM-REACTOR-OUTPUT
- SM-PRIORITY-WATCHER
- SM-MONITORING-WATCHER
- SM-SUBPROCESS
- SM-CROSS-THREAD-PROBE
- SM-MULTIPROCESS-WATCHER
- SM-SIGINT-PROBE

Files:

- `examples/async_pooled_broker.py` and new runtime tests
- `examples/reference_reactor.py` and its existing tests
- `examples/multi_queue_patterns.py` and new tests
- `tests/helper_scripts/managed_subprocess.py`
- `tests/helper_scripts/cross_thread_generator_probe.py`
- `tests/test_cross_thread_generator_probe.py`
- released-backend probe tests
- `tests/test_watcher_multiprocess.py`
- `tests/helper_scripts/watcher_sigint_script_improved.py`
- `tests/test_watcher.py`

Actions:

1. Characterize async generator empty, commit, rollback, early close, consumer
   exception, commit failure, and delivery-mode transitions.
2. Add compact scheduling tables for reactor, priority, and monitoring
   examples without replacing their end-to-end scenarios. Give the reactor's
   pending-output publication, success, retry, and terminal-error flow its own
   table rather than hiding it inside scheduling cases.
3. Enumerate subprocess normal exit, body exception, already-exited cleanup,
   interrupt, terminate, kill, terminal failure, stdin failure, and reader
   cleanup.
4. Make probe actor phases and timeout/error publication explicit.
5. Ensure process probes run in normal CI where platform/backend support
   exists; opt-in-only tests cannot be the sole firing proof.

Done signal: test infrastructure can identify exactly which lifecycle
transition failed rather than accepting a set of unrelated exit codes.

### T8 — Repair concrete defects and false-confidence tests

Files:

- `examples/multi_queue_watcher.py` and new direct tests
- `examples/sqlite_connect.py` and validation/WAL safety tests
- `simplebroker/commands.py`
- `simplebroker/sbqueue.py` plus public typing/behavior tests
- race, SIGINT, peek-watcher, and process tests named in P1

Actions:

1. Preserve an immutable default queue error handler and construct each queue
   entry from it; prove a missing override cannot inherit the previous queue's
   override.
2. Replace direct SQLite-header descriptor reads in the copyable example with
   SQLite-managed read-only validation; prove a live WAL connection keeps its
   lock behavior.
3. Explicitly close the bounded move generator.
4. Reconcile `Queue.move` return annotations/docs with the actual return paths
   without changing runtime behavior.
5. Rewrite or delete the two diagnostic race tests according to the stronger
   transition owners.
6. Make graceful SIGINT fallback observable as test failure.
7. Require every peek watcher to see the exact expected message sequence.

Done signal: each old weak assertion is demonstrated to pass an injected bad
implementation before rewrite, then the corrected test fails that bad
implementation and passes production.

### T9 — P1 core refactors

Files:

- `simplebroker/_constants.py`
- `simplebroker/cli.py`
- `simplebroker/db.py`
- closest tests and implementation docs

Actions:

1. Extract path-component validation helpers without changing validation order
   or diagnostics.
2. Introduce one private configuration-field schema used by environment and
   override parsing; fire all 31 `BROKER_*` fields and their invalid cases.
3. Reduce `cli.main` to named phases while retaining explicit dispatch and one
   error boundary.
4. Give `DBConnection.cleanup` one ownership snapshot/drain operation and one
   best-effort close operation, both inside the owning class.

Run an independent review after configuration/CLI and again after connection
cleanup. Stop if a helper needs public request context, changes target
precedence, changes cleanup ownership, or creates a new import cycle.

### T10 — P1 extension and tooling refactors

Files:

- Redis broadcast core/script/tests
- `.github/scripts/combine_coverage.py`
- `bin/release.py`
- transition and integration tests from T6

Actions:

1. Introduce named Redis selector/result representations and split pattern from
   atomic broadcast. Keep retry/capacity state in one owner.
2. Introduce a named coverage-source inspection result; keep the polling
   deadline loop intact.
3. Extract `_run_single_release`; make single and batch workflows consume
   shared phase planning only where ordering and semantics match.

Stop if Redis atomic selection moves out of Lua, coverage corruption becomes
best-effort, or release safety order becomes dependent on a generic command
framework.

### T11 — P1 example and subprocess refactors

Files:

- `examples/async_pooled_broker.py`
- `examples/multi_queue_watcher.py`
- `examples/sqlite_connect.py`
- `tests/helper_scripts/managed_subprocess.py`
- SIGINT helper and direct tests

Actions:

1. Split async streaming into named mode-specific private generators.
2. Extract multi-queue validation and entry construction after the handler bug
   is pinned.
3. Centralize managed-process escalation and make context cleanup delegate to
   it.
4. Use an explicit SIGINT readiness/stopping/stopped protocol.

Done signal: transition tables remain unchanged and pass through the new
structure.

### T12 — P2 local and coupled-helper improvements

Implement the P2 ledger in ownership-coherent commits:

1. phase lock, packaging smoke, timestamp suffix, and command helpers
2. `DBConnection.get_connection`, SQLite broadcast, and stream closure
3. PostgreSQL notification routing and Redis cleanup
4. coalescing/release batch tooling
5. pooled async and multi-queue example dispatch hooks
6. CLI/process/concurrency test harness consolidation

For every function:

- record before/after C901 score
- retain or improve failure localization
- delete actual duplication rather than wrap it
- keep helpers in the owning file/class unless a real seam has two adapters
- run the closest transition and integration tests

Stop if the proposed helper has more state parameters than the code it replaces
or if extraction makes a maintainer reconstruct one transaction/lifecycle
across files.

### T13 — C901 registry reconciliation

Files:

- `tests/test_ruff_policy.py`
- [DOM-10.1.1] suppression registry
- approved source locations
- implementation inventory

Actions:

1. Run the raw `--ignore-noqa` audit after all refactor slices.
2. Remove registry rows and local directives for findings now at or below 10.
3. Update exact locations and before/after scores for moved functions.
4. Independently review every remaining reason against the final code. Reject
   reasons that merely restate complexity, cite only unit mocks, or ignore a
   clearer ownership seam.
5. Re-run the exact-match policy fixtures and verify P3 functions were not
   fragmented to reduce registry size.

Done signal: normal Ruff is clean; raw C901 findings, local directives, and
[DOM-10.1.1] match exactly; every remaining finding has current behavioral
proof and a defensible coupling, debugging-locality, or semantic-risk reason.

### T14 — Downstream, full verification, docs, and closure

Files:

- implementation inventory/index
- relevant implementation docs
- this plan and Status Index
- `docs/lessons.md` only if implementation exposes a reusable correction

Actions:

1. Run full SimpleBroker and extension verification.
2. Run Weft's SimpleBroker-facing architecture, queue, context, polling,
   signal, dump/load, and task suites against the local SimpleBroker checkout.
3. Inspect public annotations, exported names, CLI help, exit codes, and
   package metadata for drift.
4. Obtain independent completed-work review with every transition table,
   before/after complexity score, full diff, and verification evidence.
5. Reconcile spec, plan, implementation map, code, and tests.
6. Change the plan index row to `completed` only after current evidence passes.

Done signal: the transition inventory has no missing/placeholder table, the
C901 audit is fully accounted for by reviewed [DOM-10.1.1] entries, all
affected real behavior suites pass, Weft remains compatible, and the
independent reviewer reports no unresolved blocker.

## Testing Plan

### Transition-contract proof

Each named machine gets one local `pytest.mark.parametrize` table or equivalent
data-driven loop. Every row reports its stable transition ID. Tables cover:

- legal forward transitions
- self-transitions and idempotent no-ops
- rejected or impossible events
- timeout, cancellation, and stop
- fatal versus best-effort auxiliary failure
- cleanup and error precedence
- terminal states and safe rerun/re-entry

For multi-backend behavior, one logical transition row may be parameterized over
SQLite, PostgreSQL, and Redis, but every released backend must fire where the
contract applies.

### Targeted commands

Commands must be refined per slice, but the following owners are mandatory:

```bash
uv run --frozen --no-sync pytest \
  tests/test_state_machine_policy.py \
  tests/test_sqlite_schema.py \
  tests/test_dump_load.py \
  tests/test_property_dump_load.py \
  tests/test_phaselock.py \
  tests/test_db_connection_lifecycle.py \
  tests/test_process_broker_session.py \
  tests/test_helpers_coverage.py \
  tests/test_cross_thread_finalization_poisoning.py \
  tests/test_generator_methods.py \
  tests/test_watcher.py \
  tests/test_watcher_edge_cases.py \
  tests/test_watcher_burst_mode.py \
  tests/test_dev_scripts.py \
  tests/test_release_script.py \
  tests/test_cli_watch.py \
  tests/test_watcher_multiprocess.py

uv run --frozen --no-sync ./bin/pytest-pg --fast
uv run --frozen --no-sync ./bin/pytest-redis --fast
```

Backend transition slices should invoke narrower extension files during
iteration before the extension-wide gates.

### Final repository gates

```bash
uv run --frozen --no-sync pytest
uv run --frozen --no-sync ./bin/pytest-pg --fast
uv run --frozen --no-sync ./bin/pytest-redis --fast

uv run --frozen --no-sync ruff check .
uv run --frozen --no-sync pytest tests/test_ruff_policy.py
uv run --frozen --no-sync ruff format --check \
  simplebroker tests bin .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests

uv run --frozen --no-sync mypy simplebroker bin/release.py \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
uv run --frozen --no-sync python bin/release.py --check-example-types

uv lock --check
uv lock --check --directory extensions/simplebroker_pg
uv lock --check --directory extensions/simplebroker_redis
python3 bin/check-dom15-fixtures
bin/check-doc-paths
git diff --check
```

For human-readable audit evidence, also run:

```bash
uv run --frozen --no-sync ruff check --select C901 --ignore-noqa \
  --output-format concise .
```

That audit command intentionally exits 1 because approved findings remain
visible. Its exact output is evidence, not a success gate;
`tests/test_ruff_policy.py` is the pass/fail comparison against the registry.

Use the repository's extension-test mypy partition command from the active Ruff
plan when extension test files change.

### Weft downstream proof

Weft currently imports public `Queue`, `BrokerDB`, `BrokerTarget`,
`PollingStrategy`, `BrokerError`, and backend discovery, and exercises queue,
polling, signal, dump/load, and task workflows. Before implementation starts,
record the exact Weft commands available at its then-current baseline. At
minimum run:

- its private-SimpleBroker-import architecture gate
- its SimpleBroker dependency-floor test
- context/backend target tests
- queue command and dump/load tests
- queue wait, multi-queue watcher, signal, terminal-event, and task execution
  tests

Use the local SimpleBroker checkout without editing Weft's committed dependency
floor or lock solely to make the test pass.

The reproducible closure command is:

```bash
cd ../weft
uv run --frozen --with-editable ../simplebroker pytest -n auto --dist loadgroup \
  tests/architecture/test_import_boundaries.py \
  tests/system/test_optional_extras.py \
  tests/context \
  tests/commands/test_queue.py \
  tests/commands/test_dump_load.py \
  tests/core/test_queue_wait.py \
  tests/tasks/test_multiqueue_watcher.py \
  tests/tasks/test_signal_deferral.py \
  tests/tasks/test_consumer_terminal_events.py \
  tests/tasks/test_terminal_event_retry.py \
  tests/tasks/test_task_execution.py
```

## Verification Evidence Record

At implementation time, record each meaningful slice:

| Slice | Changed files | Targeted command | Observed result | Independent review | Residual risk |
|-------|---------------|------------------|-----------------|--------------------|---------------|
| Spec promotion and transition foundation | [DOM-10.1], [DOM-10.1.1], [DOM-10.2], implementation map/index, transition helpers/manifest/policy, setup-budget pilot | `pytest tests/test_ruff_policy.py tests/test_state_machine_policy.py tests/test_helpers_coverage.py`; docs gates; Ruff; mypy | Initial 60-test foundation passed; final manifest is `COMPLETE`, all 30 inventory IDs equal the 30 registered table owners, and the temporary staged-adoption clause is removed | Initial P1/P2 findings incorporated; foundation and final policy re-reviews PASS | None; partial adoption is no longer permitted by the policy. |
| Core transition tables | Four core transition modules, manifest, Darwin discovery single-flight fix | Core transition modules plus state-machine policy; Ruff; format; mypy | 105/105 transition rows passed; core plus policy 112 passed; lint, format, typing passed | Semantic re-review and final repository suppression reconciliation PASS | Real `os.fork` rows are skipped only on platforms without fork; Windows CI remains required. |
| Extension/tool transition tables | PG/Redis transition modules, importable coverage tool plus thin CI entry, dev-script and release tables, Redis broadcast/runner fixes, manifest | PG transitions; real Valkey transitions/atomicity/integration; dev/release suites; state policy; Ruff; format; mypy | PG 28 rows passed; Redis 57 rows and focused real-Valkey suite 133 passed; coverage/CLI 20 rows; release 18 rows; full PG shared 1039 passed/3 skipped and extension 174 passed/5 opt-in skips; full Redis shared 1032 passed/10 skipped and extension 220 passed/1 opt-in skip; dev/release/policy 223 passed | First and second reviews blocked false owner/mocked-state and missing-transition gaps; all remediated; final re-review PASS after exact batch dry-run tuple fix | External nondeterministic transports remain fault-injected only at the declared boundary; PostgreSQL/Valkey owners and filesystem planners are real. |
| Example/test transition tables | Seven transition modules covering nine machines, manifest, managed-stdin cleanup | Seven-module targeted pytest; state policy; actual multiprocess scenarios; Ruff; format; mypy | 74/74 transition rows passed; actual multiprocess plus table 13 passed; targeted scenarios/protocols 116 passed; lint, format, and 14-file example typing passed | Two review rounds closed missing reactor control/backlog transitions, raw multiprocess timing, and a false-owner checkpoint fault; final re-review PASS | Async early-close and injected-error rollback occur at the generator boundary; checkpoint rollback now executes the real sidecar transaction. |
| P1 defects | Multi-queue and SQLite example direct tests, command/Queue move cleanup tests, watcher SIGINT/peek assertions; deleted diagnostic-only race module; Redis and Darwin fixes | Targeted defect tests; real WAL lock probe; real Redis suite; core transition suite | Handler carry-over, WAL lock loss, bounded-generator cleanup, Redis zero-target timestamp mutation, Darwin provisional cache, forced SIGINT fallback, partial-peeker acceptance, and monitoring failure accounting are pinned and fixed | Core, extension, and protocol reviews PASS after recorded corrections | None beyond supported-platform skips and external backend availability. |
| P1 refactors | CLI/config/DB ownership seams; multi-queue construction; coverage inspection/deadline phases; Redis broadcast strategy/result seams; single release owner; async stream modes; one managed-process close owner | Characterization and transition suites by owner; broad CLI/DB/queue/release/dev/example/subprocess suites; Ruff; mypy | Targeted suites pass; notable scores: CLI 69→10, Redis broadcast 36→28 retained, coverage 19→9, config 19→2, async stream 19→7, managed subprocess 24→7; implementation map records all 23 removed findings | Independent core/tool review PASS; extension and protocol owners reviewed separately | Retained release and Redis workflow findings remain registered because their safety/atomic state is genuinely coupled. |
| P2 refactors | Core command/lock/parser/broadcast/stream seams; coalescing evidence phases; dependency import-AST traversal; setup-budget fixtures; async SQL execution; transition-driven re-evaluation of cohesive process/concurrency owners | Owner-targeted tests; live `bin/coalesce-check`; dependency/setup tests; Ruff; raw C901; mypy | Every scoped core P2 is ≤10; coalescing, dependency, setup-budget, and async runner findings removed; remaining process/concurrency candidates reclassified with explicit transition evidence and reasons | Independent core/tool review PASS | No temporary P1/P2 suppression remains; retained findings are P3 in [DOM-10.1.1]. |
| C901 activation and initial registry | `pyproject.toml`, Ruff fixture/policy, 76 source directives, 35 [DOM-10.1.1] rows | Normal Ruff; raw C901 audit; policy suite | Normal Ruff passed; raw audit reported exactly 76; raw `noqa_row`, directives, registry locations/counts matched | Final foundation re-review PASS | P1/P2 entries are temporary and must shrink or be re-approved in T9-T13. |
| C901 final registry reconciliation | Updated source directives, [DOM-10.1.1], policy counts/scanner, and implementation map | Full Ruff policy; normal Ruff; raw Ruff; docs gates | Initial 76 findings reduced to 53; raw findings, 53 directives, and 53 registry locations match exactly; raw BLE001 100 and 98 approved directives/locations match; policy 10 passed | Independent core/tool review PASS | Ruff-discovered files, including untracked additions, now participate in the exact registry gate. |
| Full/downstream closure | Full root, PostgreSQL, Redis, lint, policy, format, all mypy partitions, locks, docs, raw C901, and the exact Weft public-surface selection above | Final repository gates plus the recorded Weft command | Root 2266 passed/17 skipped; PostgreSQL 1038 passed/3 skipped plus 174 passed/5 skipped; Redis 1031 passed/10 skipped plus 220 passed/1 skipped; Ruff/policy/format, 60-file production mypy, 29-file PostgreSQL and 27-file Redis mypy, 14-file example mypy, locks, DOM-15, doc paths, and diff passed; raw audit reported exactly 53; the reproducible Weft selection collected 345 tests and passed 344 with one PostgreSQL-only skip | Meaningful-slice reviews and Grok 4.5 completed-work review PASS after recorded corrections | The earlier 307-test Weft selector was not preserved. Its count is historical evidence only; the stronger explicit 345-test replacement is the reproducible closure proof. |

## Independent Review Loop

Plan review:

> Read this plan, its exact [DOM-10.2] and [DOM-10.1] proposed deltas, the
> current C901 findings, the seed state-machine inventory, and the relevant
> production/tests. Look for incorrect state-machine classifications, missing
> transitions, harmful score-driven decomposition, public-contract drift,
> unsafe mocking, oversized slices, hidden or stale findings, weak suppression
> reasons, broad ignores, and performative process. Challenge each retained
> finding: does its reason explain a real coupling, debugging-locality, or
> semantic-risk benefit, does its proof exercise the real owner, and would the
> proposed decomposition make the code worse? Do not implement. Could a
> zero-context engineer execute every slice confidently and preserve all
> transaction, concurrency, backend, release, and consumer invariants?

Use a different model family when available. The author must reproduce each
finding and either revise the plan, reject it with evidence, or record it as an
explicitly reasoned exclusion.

Meaningful-slice review is mandatory after:

1. transition policy and inventory
2. core transition tables
3. extension/tool transition tables
4. concrete P1 defect fixes
5. P1 production refactors
6. managed subprocess and test-harness refactors
7. C901 activation and initial exception registry
8. final C901 registry reconciliation

Completed-work review receives the promoted spec baseline, final plan, transition
inventory, all transition tables, before/after C901 output, SimpleBroker and
Weft evidence, and the full diff.

## Stop and Re-Plan Gates

Stop rather than improvising when:

- a seed machine has no defensible owner
- “all transitions” cannot be stated without changing behavior
- a required transition can only be tested by mocking away its state owner
- a refactor changes a public diagnostic, timing guarantee, exit code, return
  shape, or backend behavior
- a helper introduces a second execution path or circular import
- a C901 reason cannot identify a protected coupling, debugging-locality, or
  semantic-risk benefit and real behavioral proof
- passing normal Ruff would require splitting a cohesive P3
  machine/parser/checklist or adding a broad ignore
- release dry-run and real execution can no longer be compared phase by phase
- a process test still treats SIGTERM/SIGKILL cleanup as graceful success
- Weft requires a private compatibility shim
- a new dependency or generic state-machine framework appears necessary

## Out of Scope

- Public feature work, new CLI commands, or new backend behavior.
- Storage-format, message-ID, dump-format, delivery-guarantee, or backend API
  version changes.
- Rewriting SQL, Lua, polling, release, or process behavior beyond the
  dispositioned seams.
- Splitting files because they are long.
- Refactoring the 25 P3 functions solely for C901.
- Enabling Ruff preview rules or `select = ["ALL"]`.
- Raising the C901 threshold above 10 to reduce the reviewed registry.
- Adding C901 per-file ignores, global ignores, blanket file directives,
  unexplained local suppressions, or baseline snapshots.
- Adding a generic state-machine runtime or dependency.
- Making every ordinary loop, transaction block, parser branch, or cleanup
  sequence into a named machine.
- Changing Weft source, pins, or tests to accommodate a SimpleBroker
  regression.
- Publishing a release. Release classification is decided only after the final
  diff establishes whether any consumer-visible behavior changed.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| Spec promotion process | Record a promotion baseline after applying [DOM-10.2] and the C901 policy, before implementation begins. | No separate commit or worktree snapshot preserved the promotion-only state. The implementation checkpoint contains the promoted spec and its realization together. | The missing historical state cannot be reconstructed honestly. Current traceability is closed and the final policy is fully gated, but no later SHA can prove the required sequencing. | None. [DOM-5] already requires the missing record; this row records nonconformance rather than weakening that rule. |
| [DOM-10.2] | The seven T7 table modules would cover the nine example and reusable test-protocol machines. | Independent review expanded `SM-REACTOR` from 7 to 15 rows, raising the T7 total from 66 to 74, and required checkpoint failure below the real SQLite transaction owner. | The initial rows omitted durable-backlog and control-lane transitions; the first checkpoint-failure revision mocked away the state owner. The added rows and lower-boundary injection are required by the existing all-transition and anti-mocking contract. | None; implementation was corrected to conform to [DOM-10.2]. |
| Downstream verification | Preserve and run the exact selected Weft public-surface command against the local checkout. | The first xdist run exposed a race in Weft's mocked `QueueChangeMonitor` close-count test; its exact case passed against both locked and local SimpleBroker. The earlier 307-test selector was not preserved, so closure reconstructed and recorded a stronger explicit selection: 345 collected, 344 passed, one PostgreSQL-only skip. | The fake waiter race does not exercise changed SimpleBroker behavior. A test count cannot reproduce a selection, so only the newly recorded command is closure evidence; the 307-test result remains historical context. | None for SimpleBroker; Weft should separately synchronize ownership before asserting the mock call count. Future downstream records must preserve the literal command, not only its count. |

## Revision Log

| Date | Reviewed baseline | Revision | Reason | Re-review |
|------|-------------------|----------|--------|-----------|
| 2026-07-29 | Initial draft at `1324a1f647b7c8fc9b14a6aed6bae696aefe62ea` | Replaced a threshold-23/zero-exception gate with C901 at 10, exact raw-audit visibility, and reasoned [DOM-10.1.1] exceptions. | User clarified that the value is visibility and auditability; complexity without a reason is the defect. | Independent review found sequencing, manifest, rule-fixture, nearby-reason, and machine-inventory gaps; all were revised. |
| 2026-07-29 | First revised draft | Made all 76 initial findings registered, tied temporary P1/P2 entries to removal slices, specified the test-only table manifest/decorator, added missing lifecycle machines, and separated the expected-nonzero raw audit from success gates. | Resolve independent review blockers. | Final independent re-review: PASS, no remaining blocker. |

## Review Log

| Review | Date | Verdict | Disposition |
|--------|------|---------|-------------|
| Independent engineering review (`gpt-5.6-terra`) | 2026-07-29 | PASS after two revision rounds | Incorporated all findings: executable early C901 activation, enabled-rule inventory, explicit table/test metadata, durable nearby-reason ownership, missing state machines, and unambiguous audit command semantics. |
| Claude outside-voice attempt | 2026-07-29 | Unavailable | Read-only review hit its 300-second execution cap and returned no findings; no inference was drawn from the timeout. |
| Foundation implementation review (`gpt-5.6-terra`) | 2026-07-29 | PASS after revision | Added staged DOM-10.2 adoption and completion enforcement, per-row C901 count/duplicate checks, normal-Ruff policy proof, and a current activation record. |
| Core transition semantic review | 2026-07-29 | PASS after correction | Required real Darwin single-flight ownership and exact table-to-owner evidence; confirmed 105 core transition rows and no remaining behavioral defect. |
| Extension/tool transition review | 2026-07-29 | PASS after two correction rounds | Rejected mocked PostgreSQL/Redis state owners and incomplete release evidence; confirmed real backend owners, exact batch dry-run tuple evidence, and complete extension/tool tables. |
| Core/tool refactor and final C901 review | 2026-07-29 | PASS | Confirmed public signatures except the intentional `Queue.move` annotation correction, preserved release and subprocess order, and exact agreement among 53 raw findings, directives, and registry locations. |
| Protocol/example transition review, round 1 | 2026-07-29 | BLOCKED | Found missing reactor backlog/control transitions, unscaled deadlines in the actual multiprocess protocol, and a dishonest monitoring return annotation; all were corrected. |
| Protocol/example transition review, round 2 | 2026-07-29 | BLOCKED | Found that checkpoint failure replaced the state owner and could not prove transaction rollback; fault injection moved below the owner to the SQLite runner audit write. |
| Protocol/example transition review, final | 2026-07-29 | PASS | Confirmed the real sidecar transaction rolls back checkpoint and audit together, retry visibly replays, multiprocess cleanup is bounded and scaled, and all prior blockers are resolved. |
| Grok 4.5 completed-work review | 2026-07-29 | PASS after focused follow-up | Reproduced the exact 53-finding C901 inventory and sampled real transition owners. Permanent approval language replaced three stale temporary registry labels; the implementation map now labels historical table candidates honestly; the duplicated iterator-close helper now has one owner; the Redis zero-target behavior is recorded in `CHANGELOG.md`. A focused second pass confirmed all four corrections with no immediate regression. Commit and index closure were left to the owner-directed targeted-commit step. |

## Fresh-Eyes Checklist

- [x] Every one of the 76 findings appears in exactly one disposition.
- [x] Every genuine machine has one owner and one table owner.
- [x] “All transitions” includes rejection, cleanup, failure precedence, and
  terminal behavior rather than only happy paths.
- [x] Existing real scenario/race/property tests remain as independent proof.
- [x] The plan does not demand production enums or generic machinery where a
  local table is enough.
- [x] No P3 function is scheduled for score-driven decomposition.
- [x] Each P1/P2 task names files, proof, and a stop condition.
- [x] C901 activation is early and atomic with policy/spec/registry proof.
- [x] Every retained C901 finding has one narrow local directive and one exact
  [DOM-10.1.1] entry with a reason, real proof, rejected alternatives, and
  approval.
- [x] Raw `--ignore-noqa` findings, source directives, and registry entries
  match exactly; broad suppression paths do not exist.
- [x] Rollback preserves characterization tests and reverts structural slices
  independently.
- [x] Weft verification uses public surfaces and does not edit Weft to hide
  incompatibility.
- [x] Status Index closure and independent completed-work review are explicit.
