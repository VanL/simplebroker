# Development Documentation Operating Model

Status: Active

This spec defines the documentation operating model for this repository. It is
the source of truth for how agent context, specs, plans, implementation docs,
skills, agent reviews, bootstrap inventory, and lessons are expected to work
together.

## 1. Overview [DOM-1]

This repository uses a docs-first operating model for development.

Requirements:

- shared agent context is repository-owned and loaded at session start
- specs define intended behavior
- plans define execution for concrete changes
- independent review agents validate plans and completed work
- implementation docs explain current rationale and important boundaries
- skills capture reusable recurring workflows
- lessons capture durable corrections
- documentation should optimize for agent usability, not only human readability

Agent-usable documentation should make these explicit whenever they matter:

- owner: who acts or which surface owns the behavior
- boundary: when the rule applies and when it does not
- verification: how correctness is checked
- required action: what the reader should do next

## 2. Documentation Taxonomy [DOM-2]

The repository documentation surface is split by role:

- `docs/agent-context/`: canonical shared context and reusable runbooks
- `docs/specs/`: intended behavior, invariants, and verification expectations
- `docs/plans/`: dated execution documents for concrete work
- `docs/implementation/`: rationale, boundaries, repository maps, and current
  architecture notes
- `skills/`: reusable task-scoped workflow instructions
- `docs/lessons.md`: canonical lessons ledger

The roles should remain distinct. A document may link to another role, but it
should not collapse multiple roles into one file without a strong reason.

## 3. Agent Startup Context [DOM-3]

At the start of a session, agents should load:

1. the root agent entry point
2. the read order defined in `docs/agent-context/README.md`
3. the current agent availability inventory, if one exists
4. the relevant spec, active plan, implementation note, and skill for the task

The shared agent context should stay repository-owned so multiple agent tools
can consume the same durable guidance.

Tool-specific root aliases should symlink to the canonical root entry point
when the environment supports symlinks. If symlinks are not practical, keep
those files as thin pointers back to the canonical entry point.

## 4. Traceability Requirements [DOM-4]

For material behavior changes, as defined in [DOM-6], the repository should
preserve the chain:

`spec section <-> plan <-> implementation doc <-> code`

Requirements:

- plans cite exact spec files and reference codes when they exist
- specs maintain backlinks to related plans
- implementation docs cite governing spec sections and key files or modules
- code should point back to the governing spec where ownership would otherwise
  be ambiguous

_Implementation snapshot_: the current repository setup models this chain with
the documentation system itself because product code has not been added yet.

## 5. Planning Standard [DOM-5]

Classify the task first ([DOM-15]). Classes 3 and above begin with a
dated plan in `docs/plans/`; classes 1–2 keep their planning record in
the commit history or handoff report instead. The lists below remain
the canonical trigger definitions [DOM-15] cites.

For this operating model, treat a change as non-trivial when any of these are
true:

- it changes intended behavior
- it crosses more than one major documentation surface or code boundary
- it introduces or revises a reusable workflow
- it would leave a zero-context implementer guessing without a plan

The plan must be executable by a zero-context engineer and include:

- goal
- source documents
- context and key files
- invariants and constraints
- dependency-ordered tasks
- testing plan
- verification and gates
- independent review loop
- out-of-scope statement
- fresh-eyes review

Plans should state invariants before or alongside tasks.

For this operating model, treat a change as risky when any of these are true:

- it introduces async, deferred, queued, or background work
- the same core behavior must run in more than one execution context
- it changes a public contract, compatibility surface, CLI shape, or storage
  format
- rollback depends on backward compatibility or rollout order
- it introduces a one-way door, destructive edge, new persistence, temp-file,
  cleanup, or deferred-input lifecycle

Risky plans are not review-ready until they also make explicit:

- hidden couplings and boundary-crossing state
- stop-and-re-evaluate gates for risky tasks
- what should not be mocked
- current owner or current-structure context for the main edit points
- which auxiliary failures are best-effort versus fatal
- rollback path and rollout sequencing
- rollback written early enough to shape the task decomposition
- one-way doors
- post-deploy success signals
- required reading with comprehension questions for complex areas

This spec names the planning contract. The operational checklist, rewrite
criteria, and examples live in `docs/agent-context/runbooks/writing-plans.md`
and `docs/agent-context/runbooks/hardening-plans.md`.

## 6. Spec Standard [DOM-6]

Specs must define intended behavior and not merely document current file layout.

Requirements:

- use stable reference codes for requirements that need to be cited
- document invariants, interfaces, failure modes, and verification
- keep `## Related Plans` current
- update the spec before or with any material behavior change
- if wording is human-clear but agent-ambiguous, tighten it and suggest a more
  agent-usable formulation

For this operating model, treat a change as material when it changes intended
behavior, changes a governing boundary or invariant, or would alter how future
work should be planned, implemented, reviewed, or verified.

## 7. Implementation Docs Standard [DOM-7]

Implementation docs must explain why the current design exists.

Requirements:

- capture rationale, boundaries, tradeoffs, and key edit points
- cite governing spec sections
- remain concise and durable
- avoid turning into line-by-line code tours
- update when the rationale or ownership changes materially, meaning the current
  explanation of why the design exists or who owns the decision would no longer
  be reliable after the change
- prefer structures and wording that help agents locate decisions, boundaries,
  and edit points reliably

Helpful structures include:

- a dedicated governing-spec section
- explicit key-file or key-module lists
- change-guidance checklists
- named invariants rather than prose-only rationale

## 8. Documentation Maintenance Gate [DOM-8]

Documentation maintenance is part of the definition of done.

Requirements:

- plans, specs, implementation docs, and code must stay aligned within the same
  change
- if no governing spec exists, the plan must say so explicitly
- if a skill or runbook was central to the work, evaluate whether it should be
  improved while context is still fresh
- if a correction reveals a reusable rule, add it to `docs/lessons.md`
- if an external note, review comment, or one-off plan fix produces a durable
  planning rule, promote it into the relevant runbook instead of leaving it
  buried in a single plan
- if something remains human-readable but agent-confusing, notify the user and
  suggest a concrete improvement

## 9. Lessons Learned [DOM-9]

Durable lessons live in `docs/lessons.md`.

Lessons should be:

- short
- dated
- written as reusable rules
- added when they would prevent future rework

Durable means the lesson should still help on future sessions or future changes,
not just the task that happened to reveal it.

When recurring lessons describe a stable workflow rather than a one-off rule,
promote them into a skill or runbook.

## 10. Verification and Completion [DOM-10]

Each completed task should leave behind explicit evidence.

At minimum, completion should name:

- the file(s) changed
- the verification command or inspection gate
- the observed result or residual risk

Docs-only changes may be verified by inspection, link checks, formatting checks,
and targeted grep-based assertions when runtime behavior is not involved.

For runtime behavior changes, completion should also name the intended rollout
observation or rollback path when those materially affect operational safety.

For risky changes, completion should also say whether the rollout or rollback
assumptions still hold and whether post-deploy observation is pending or
complete.

### Repository Static-Analysis Gate [DOM-10.1]

SimpleBroker's Python lint gate uses the stable default rule set of the Ruff
version locked in `uv.lock`, extended with the repository's existing `E`, `W`,
`F`, `I`, `B`, `C4`, and `UP` rule families. The configuration must extend
Ruff's defaults rather than replace them.

Owner: `pyproject.toml` owns rule selection; the root CI lint job enforces it.
Boundary: every tracked first-party `.py`/`.pyi` file and Python-shebang script
in the repository. Verification: `tests/test_ruff_policy.py` invokes the real
locked Ruff binary, compares effective discovery and rule selection with
reviewed inventories, and proves that a stable-default rule outside the legacy
families and a retained legacy-family rule both fire. Required action: a Ruff
version refresh reviews and intentionally updates the enabled-rule inventory
before regenerating the lock.

Requirements:

- the root lint job uses repository discovery (`ruff check .`), and Ruff's
  include configuration plus a tracked-file discovery test covers extensionless
  Python-shebang tools that default discovery omits
- Ruff preview rules remain opt-in and are not part of the default gate
- global ignores are limited to explicitly documented repository-wide
  conflicts; per-file ignores remain empty, and other suppressions are local,
  narrow, carry a reason, protect a tested invariant, and require explicit
  review before adoption
- intentionally broad exception or best-effort cleanup boundaries retain their
  runtime behavior through an explicit code structure where practical; a
  suppression is the reviewed last resort, not the default alternative to a
  behavior-changing rewrite
- formatter paths stay explicit so widening lint discovery does not implicitly
  widen Markdown formatting ownership

Ruff's `C901` rule is enabled repository-wide with
`lint.mccabe.max-complexity = 10`. The score is a visibility signal, not a
design verdict. Each finding must either be simplified around a real ownership
seam or carry a narrow local `C901` suppression registered in [DOM-10.1.1].
The registry must explain why coupling, debugging locality, or semantic risk
justifies retaining the function; name the real behavioral proof; record
rejected decompositions and approval; and identify the exact source location
and count.

The policy test runs normal Ruff and a raw `C901` audit with `--ignore-noqa`.
Raw findings (using Ruff's reported `noqa_row`), source directives, and
[DOM-10.1.1] registry entries must match exactly by repository-relative path
and line. A new unsuppressed finding, an unregistered directive, a stale
directive, a stale registry entry, or moved code fails verification. Per-file
ignores, global ignores, and baseline allowlists are not permitted. A cohesive
parser, checklist, or state machine must not be fragmented merely to lower its
score.

#### Approved Ruff Suppression Registry [DOM-10.1.1]

The following local suppressions are approved exceptions to [DOM-10.1]. This
spec owns the durable registry. A plan may propose or review a candidate, but
must not become the lasting source of truth for an adopted exception.

Owner: this section owns the approved groups; the suppression remains local to
the listed code boundary. Boundary: only the rule, location group, and invariant
listed below. Verification: the named real proof plus `ruff check .` and
`RUF100`. Required action: preserve the nearby reason and update this registry
in the same change when an approved suppression is added, removed, regrouped,
or materially changes its invariant.

For an approved registered exception, the local
`approved [DOM-10.1.1] exception` text is the required nearby reason pointer;
the registry row is the single durable full reason. Do not duplicate the full
rationale in source comments. A temporary C901 row must also name the plan task
that removes or re-evaluates it.

| Location group | Rule and count | Protected invariant | Real proof | Rejected alternatives | Approval |
|----------------|----------------|---------------------|------------|-----------------------|----------|
| `simplebroker/_phaselock.py:523`; `simplebroker/db.py:894,898,3490,3494,3601`; `simplebroker/sbqueue.py:1089,1093`; `simplebroker/_runner.py:744`; `simplebroker/_backend_plugins.py:257`; `simplebroker/watcher.py:1045`; `extensions/simplebroker_redis/simplebroker_redis/core.py:1820,1823` | `PYI034`, `PYI036` (25) | Existing concrete `__enter__` returns and permissive `Any`-typed `__exit__` parameters remain valid for downstream subclasses, manual calls, and runtime annotation inspection. | `tests/test_ruff_policy.py::test_public_context_manager_annotations_remain_override_compatible`; `tests/test_ruff_policy.py::test_public_exit_annotations_keep_any_typed_parameters`; full mypy partitions. | `Self` broke a downstream override that copied the former concrete return. Precise exception/traceback types broke formerly valid calls. `object` parameters still break formerly valid narrower overrides. Removing annotations weakens the public contract. | Independent T8 PASS; user approved 2026-07-29. |
| `simplebroker/commands.py:69` | `SIM115` (1) | The replacement `sys.stdout` stream remains open through later and interpreter-shutdown flushes after a downstream pipe closes. | `tests/test_commands_helpers.py` closed-pipe cases on descriptor and wrapper fallback paths. | A local context manager closes the installed stream. A retained-handle registry adds lifecycle complexity solely for lint. Lower-level open spelling only evades the diagnostic. | Independent T8 PASS; user approved 2026-07-29. |
| `bin/check-dom15-fixtures:215`; `bin/pytest-redis:211`; `simplebroker/_scripts.py:596,888`; `simplebroker/cli.py:1020,1055,1567`; `simplebroker/commands.py:804,942,1389,1460,1482` | `BLE001` (12) | Agent-facing CLI and development-tool entry points convert every ordinary internal, backend, plugin, filesystem, and configuration failure into the documented no-traceback exit/result boundary while leaving `KeyboardInterrupt` handling distinct. | CLI, command, development-script, release-workflow, Redis harness, DOM-15 fixture, and documented exit-code suites. | Enumerating current exception subclasses is incomplete across third-party backends and plugins. A generic wrapper hides boundary ownership. Propagation changes the CLI contract. | Independent T8 PASS; user approved 2026-07-29. |
| `examples/async_simple_example.py:74`; `examples/reference_reactor.py:775`; `extensions/simplebroker_pg/simplebroker_pg/runner.py:170`; `extensions/simplebroker_redis/simplebroker_redis/plugin.py:176`; `simplebroker/watcher.py:877,1428` | `BLE001` (6) | Long-lived workers contain arbitrary user callback, backend listener, and injected polling-provider failures at their declared isolation or retry boundary. | Reference-reactor tests; watcher retry, handler, edge-case, and backend notification/listener suites. | Narrow backend types miss user code and adapter failures. Letting a background thread die loses stored-error/wakeup behavior. A generic helper obscures distinct retry and notification semantics. | Independent T8 PASS; user approved 2026-07-29. |
| `simplebroker/db.py:246,801,852`; `simplebroker/sbqueue.py:1442`; `simplebroker/watcher.py:1053` | `BLE001` (5) | Checkout release, owned backend shutdown, weakref finalization, and context-exit cleanup remain best-effort and never replace the primary failure or make teardown fatal. | Connection lifecycle, queue connection-manager, cleanup, runner error-handling, and watcher edge-case suites. | Narrowing is unsound for arbitrary protocol/plugin implementations. Propagation changes cleanup semantics. `suppress(Exception)` would erase logging or failure-note behavior. | Independent T8 PASS; user approved 2026-07-29. |
| `simplebroker/db.py:1410,1438,1914` | `BLE001` (3) | Generator and sidecar transaction arbitration observes `GeneratorExit`, cancellation, and ordinary failures; rollback failure never silently commits partial caller-owned work and primary exception/poison precedence is retained. | Cross-thread finalization poisoning, generator-method, sidecar, and database lifecycle suites. | `Exception` misses required `BaseException` paths. Suppression contexts lose cleanup-failure notes and precedence. Built-in subclass enumeration is incomplete. | Independent T8 PASS; user approved 2026-07-29. |
| `extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py:101,159`; `extensions/simplebroker_pg/tests/test_pg_notify.py:448`; `extensions/simplebroker_pg/tests/test_pg_queue_rename.py:143,194`; `extensions/simplebroker_redis/tests/test_redis_atomicity.py:118`; `tests/helper_scripts/cross_thread_generator_probe.py:134,156,161,168,188,205,237,247,281,325,340,364,376,402,572,579,588`; `tests/test_cross_thread_finalization_poisoning.py:100,188,199,470,617,843,876,943`; `tests/test_edge_cases.py:135`; `tests/test_fork_safety.py:44,87,188,226,275,374,441`; `tests/test_generator_methods.py:309`; `tests/test_phaselock.py:1299,1351,1405,1515,1523,1627,1752`; `tests/test_process_broker_session.py:703,709,985,1070,1155`; `tests/test_queue_move_watcher.py:352`; `tests/test_runner_error_handling.py:790`; `tests/test_watcher_concurrency.py:598`; `tests/test_watcher_multiprocess.py:137,192,274`; `tests/test_watcher_race_conditions.py:797`; `tests/test_connection_transition_tables.py:474` | `BLE001` (60) | Thread, process, fork, deadlock-timeout, and generator-resumption probes capture every child outcome, including `BaseException`, so the parent can assert identity, traceback, cleanup, poison, and liveness. | The named real concurrency, backend, phase-lock, fork-safety, watcher, process-session, transition-table, and poisoning suites. | Futures replaced catches where timeout and lifecycle behavior stayed intact. They are unsafe for intentional daemon-thread deadlock, child-process serialization, and exact-`BaseException` assertions. Narrowing loses the behavior under test; generic capture helpers are lint evasion. | Independent T8 and transition-contract review PASS; user approved 2026-07-29. |
| `tests/backend_benchmark.py:839`; `tests/helper_scripts/cleanup.py:52`; `tests/helper_scripts/managed_subprocess.py:56`; `tests/helper_scripts/timing.py:151`; `tests/helper_scripts/watcher_sigint_script_improved.py:53,86,111,120`; `tests/helper_scripts/watcher_sigint_script_instrumented.py:35,57,73,92` | `BLE001` (12) | Test harness entry points, diagnostic readers, arbitrary condition callbacks, watcher cleanup, and signal scripts report or contain unknown support failures without masking the primary test result or hanging a subprocess. | Benchmark argument tests; helper-script subprocess tests; watcher SIGINT and cleanup suites; timing-helper users throughout watcher/process tests. | Exception enumeration is not closed over arbitrary callbacks, streams, and watcher implementations. Propagation loses structured diagnostics or cleanup. `suppress` loses intentional reporting. | Independent T8 PASS; user approved 2026-07-29. |
| `simplebroker/_backends/sqlite/schema.py:179`; `simplebroker/_backends/sqlite/validation.py:14`; `simplebroker/_dump.py:155` | `C901` (3) | Schema reconciliation, database validation, and dump loading keep ordered format checks, transaction effects, and failure precedence in one debugging unit. | SQLite schema, validation, dump/load, and property suites; SM-SQLITE-SCHEMA and SM-DUMP-LOAD transition tables. | Raising the threshold hides new findings. Splitting branch-only helpers separates error order from mutation order without reducing caller knowledge. | P3 retained; initial C901 activation; user approved 2026-07-29. |
| `simplebroker/_phaselock.py:162,631` | `C901` (2) | Darwin provider discovery and durable phase completion keep cache publication, fallback, cancellation, lock ownership, and marker state local. | `tests/test_phaselock.py`; SM-DARWIN-XATTR and SM-PHASE-LOCK transition tables. | Moving lock stages across modules obscures unwind ownership. A generic state-machine layer adds indirection without a second adapter. Advisory acquisition was simplified at a real retry seam; further splitting these two owners would separate state publication from its failure order. | P3 retained after T9/T12 refactor and transition-contract review; user approved 2026-07-29. |
| `simplebroker/_retry.py:258` | `C901` (1) | Retry attempt, elapsed-budget, stop, notification, and wait-generator decisions remain readable as one bounded retry algorithm. | `tests/test_retry.py`, watcher/setup retry integration, and interruption tests. | Callback wrappers or a generic retry DSL would scatter stop and error precedence while leaving the same decisions. | P3 retained; initial C901 activation; user approved 2026-07-29. |
| `simplebroker/_scripts.py:412` | `C901` (1) | Pytest override parsing keeps ordered argument-shape, worker, timeout, and compatibility precedence in one parser. | Script argument, development-tool, and subprocess suites. | A generic command framework enlarges the seam. Tiny branch wrappers hide rather than remove parser precedence. Packaging smoke orchestration was separately simplified into build, inspection, and install phases. | P3 retained after T12 refactor review; user approved 2026-07-29. |
| `simplebroker/_timestamp.py:298,520` | `C901` (2) | Timestamp validation and numeric precedence stay in one parser family with exact accepted forms, error order, and ordering semantics. | Timestamp edge, resilience, property, and backend integration suites; SM-TIMESTAMP-GENERATOR transition table. | Parser-combinator machinery would be larger than the grammar. Score-only predicates would duplicate precedence and diagnostics. Unit-suffix conversion was extracted at a genuine seam. | P3 retained after T12 refactor and transition-contract review; user approved 2026-07-29. |
| `simplebroker/commands.py:414` | `C901` (1) | Queue fetch keeps selection, delivery, output, and empty-result precedence beside the public command result. | Command helper, fetch, CLI subprocess, and delivery suites. | A generic command runner obscures command-specific exit and cleanup contracts. Move, watch, and init were simplified through owner-local mode and lifecycle seams. | P3 retained after T12 refactor review; user approved 2026-07-29. |
| `simplebroker/db.py:1339,1795` | `C901` (2) | Suspended sidecar and transactional-batch frames retain lock, transaction, owner-thread, poison, and cleanup-failure precedence in one frame. | Sidecar, generator-method, exactly-once, cross-thread poisoning, and SM-DELIVERY-POISON transition suites. | Extraction would pass live transaction state across helpers or create a second unsafe cleanup path. A generic state-machine runtime weakens locality. | P3 retained after transition-contract review; user approved 2026-07-29. |
| `simplebroker/sbqueue.py:771` | `C901` (1) | Queue move retains delivery-mode selection, return semantics, generator closure, and public error translation at the queue interface. | Queue move, generator, delivery, watcher, typing, and public behavior suites. | Moving public-mode logic into detached helpers enlarges the interface. Stream cleanup now uses one shared iterator-close path and no longer needs an exception. | P3 retained after T12 refactor review; user approved 2026-07-29. |
| `simplebroker/watcher.py:1179` | `C901` (1) | Polling mode, burst/backoff, activity hints, waiter replacement, data-version checks, and stop behavior remain one debuggable state owner. | Watcher, activity-replacement, burst, edge, race, and SM-POLLING transition suites. | Fragmenting the decision loop would spread live counters and waiter ownership. A generic state-machine framework adds no adapter. | P3 retained; initial C901 activation; user approved 2026-07-29. |
| `extensions/simplebroker_pg/simplebroker_pg/plugin.py:741` | `C901` (1) | PostgreSQL vacuum keeps lease, advisory lock, delete batches, maintenance choice, unlock, and release precedence in one protocol owner. | PostgreSQL maintenance and contract-edge suites; SM-PG-VACUUM transition table. | Phase helpers would pass live lease/transaction state and hide unwind order. A generic maintenance framework has no second adapter. | P3 retained; initial C901 activation; user approved 2026-07-29. |
| `extensions/simplebroker_pg/simplebroker_pg/runner.py:142` | `C901` (1) | Listener startup, notification routing, error publication, wakeup, stop, and close stay with the shared-listener owner. | PostgreSQL notify and runner-lifecycle suites; SM-PG-LISTENER transition table including startup timeout, fan-in, stored failure, and idempotent close. | Splitting routing from failure publication would duplicate listener state or require a wider internal interface. The completed transition table shows these branches share one thread-owned listener lifecycle. | P3 retained after T12 transition-contract review; user approved 2026-07-29. |
| `extensions/simplebroker_redis/simplebroker_redis/core.py:1333`; `extensions/simplebroker_redis/simplebroker_redis/plugin.py:447` | `C901` (2) | Redis bounded scans and target cleanup keep cursor/key topology, reservation visibility, and cleanup ordering with their Redis owners. | Redis integration, batch, cleanup, pool, and plugin contract suites. | Generic scan/cleanup helpers would expose Redis key-layout state and make debugging cross-file. Cleanup is an ordered key-topology checklist with one owner, not a persistent state machine. | P3 retained after T12 review; user approved 2026-07-29. |
| `extensions/simplebroker_redis/simplebroker_redis/core.py:1409` | `C901` (1) | Redis broadcast keeps the atomic Lua result protocol, capacity growth, stale-fence retry, and terminal error translation together after selector normalization and patterned broadcast were extracted. | Redis atomicity, broadcast, integration, and SM-REDIS-BROADCAST transition suites using a real server and Lua script, including every result code and retry stage. | Moving atomic selection out of Lua breaks the protocol. Splitting retry state from result decoding would make the atomic script and Python owner harder to debug together; a generic framework hides result-code meaning. | P3 retained after T10 refactor and independent real-Valkey review; user approved 2026-07-29. |
| `extensions/simplebroker_pg/tests/test_pg_plugin_contract_edges.py:189` | `C901` (1) | The real vacuum scenario keeps causal setup, batch commit, zero-row rollback, compact, unlock, and lease-release assertions in one readable proof. | The named test plus PostgreSQL maintenance integration and SM-PG-VACUUM table. | Test helpers would hide ordering or merely relocate branches; mocks would replace the behavior under proof. | P3 retained; initial C901 activation; user approved 2026-07-29. |
| `bin/release.py:1403,2182,2308` | `C901` (3) | Repository settings, synchronized release, and single-target release keep safety order, rerun state, exact post-commit SHA, CI truth, and tag publication in their respective workflow owners. | Release script, publication, workflow, and SM-RELEASE transition suites covering dirty/dry-run, generated-file commits, reuse, batch commits, and exact publication SHA. | A generic command framework hides destructive ordering. Sharing more phases between batch and single workflows would make subtly different commit and baseline semantics appear identical. The public dispatcher is now below the gate. | P3 retained after T10/T12 extraction and independent transition review; user approved 2026-07-29. |
| `examples/multi_queue_patterns.py:86,309` | `C901` (2) | Priority and monitoring examples keep their distinct scheduling loops, handler dispatch, metrics, and activity notifications within each copyable pattern. | Multi-queue example runtime tests; SM-PRIORITY-WATCHER and SM-MONITORING-WATCHER transition tables. | A shared generic watcher would obscure distinct scheduling policies. Validation and entry construction were extracted from `MultiQueueWatcher`; further score-only helpers would separate each nested policy from its visible example flow. | P3 retained after T11/T12 refactor and transition-contract review; user approved 2026-07-29. |
| `examples/reference_reactor.py:889` | `C901` (1) | Reactor draining keeps queue role, scheduling, lease, backlog, active-set, and notification decisions beside the persistent reactor state. | Reference-reactor scenarios; SM-REACTOR and SM-REACTOR-OUTPUT transition tables. | Extracting by branch would pass the same mutable reactor state through a shallow interface. | P3 retained; initial C901 activation; user approved 2026-07-29. |
| `examples/sqlite_connect.py:314,418` | `C901` (2) | The copyable SQLite example preserves explicit path checks and database validation order while production-equivalent SQLite-managed validation replaces unsafe header access. | Example validation tests, live-WAL lock proof, and production SQLite validation suites. | A shared import defeats the standalone example. Direct descriptor reads are unsafe; score-only predicates hide validation order. | P3 retained after T8/T11 validation rewrite and independent review; user approved 2026-07-29. |
| `tests/backend_benchmark.py:78`; `tests/test_backend_plugin_resolution.py:344` | `C901` (2) | Benchmark validation and backend-resolution setup retain complete contract assertions as readable repository gates. | The named suites across supported backends and repository self-application. | Generic test DSLs would hide the contract being proved. The duplicate dependency AST scans were consolidated and no longer need an exception. | P3 retained after T12 review; user approved 2026-07-29. |
| `tests/conftest.py:880` | `C901` (1) | CLI subprocess coverage keeps staging, child result, validation, atomic promotion, and cleanup in one harness owner. | `tests/test_dev_scripts.py`, CLI subprocess suites, and SM-CLI-COVERAGE transition table with a real child-to-promotion row. | Splitting publication from process ownership risks publishing incomplete coverage. Call-count mocks would not prove file semantics. The completed transition contract confirms this is one persistent file-lifecycle owner. | P3 retained after T12 transition-contract review; user approved 2026-07-29. |
| `tests/helper_scripts/cross_thread_generator_probe.py:84,109,293,506` | `C901` (4) | Cross-thread probes keep owner/foreign actor phases, timeouts, result publication, poison inspection, and cleanup in causal order. | Direct probe tests plus SQLite, PostgreSQL, and Redis backend process probes; SM-CROSS-THREAD-PROBE table. | Generic actor helpers can hide which thread owns cleanup. Narrow exception mocks would replace the failure identities under test. Deterministic synchronization and bounded retry seams were added without moving actor ownership. | P3 retained after T12 transition-contract review; user approved 2026-07-29. |
| `tests/test_process_broker_session.py:811,1016` | `C901` (2) | Process-session failure and post-close races keep real thread/process coordination, ownership assertions, and cleanup outcomes together. | Process-session lifecycle and SM-PROCESS-SESSION transition suites. | Mock sessions cannot prove lease cleanup. Test extraction that passes all actors/state would reduce locality without reducing complexity. | P3 retained after T12 transition-contract and refactor review; user approved 2026-07-29. |
| `tests/helper_scripts/watcher_sigint_script_improved.py:16`; `tests/test_watcher.py:454` | `C901` (2) | The helper and integration proof keep readiness, interrupt delivery, watcher shutdown, diagnostics, and terminal exit distinguishable. | Direct SM-SIGINT-PROBE table plus watcher SIGINT subprocess tests. | Accepting all exit codes hides failed graceful shutdown. The managed-subprocess duplicate escalation paths were consolidated and no longer need an exception; these two POSIX/Windows protocol owners remain intentionally explicit. | P3 retained after T8/T11 transition review; user approved 2026-07-29. |
| `tests/test_watcher_burst_mode.py:494,708` | `C901` (2) | Jitter and burst/backoff scenarios keep deterministic clock/event sequences and exact polling-state outcomes readable. | Watcher burst, edge, and SM-POLLING transition suites. | A test DSL would hide the timeline. Fragmented assertions make the state progression harder to debug. | P3 retained after T12 polling transition-contract review; user approved 2026-07-29. |
| `tests/test_watcher_concurrency.py:471,554`; `tests/test_watcher_race_conditions.py:472,745`; `tests/test_watcher_thundering_herd.py:246` | `C901` (5) | Real watcher concurrency tests keep actor startup, exact expected observations, liveness, contention, results, and cleanup in one causal proof. | Watcher concurrency, race, thundering-herd, and SM-WATCHER-LIFECYCLE integration suites. | Mocks cannot prove scheduling or locks. Over-shared helpers would conceal which actor or queue failed and can recreate silent passes. Every peek observer now must see the complete sequence. | P3 retained after T8/T12 transition review; user approved 2026-07-29. |
| `tests/test_watcher_multiprocess.py:64,196,313,452,581,712,821` | `C901` (7) | Multiprocess watcher workers and scenarios keep spawn readiness, aggregate deadlines, queue activity, lock state, results, diagnostics, and cleanup together. | The five real multiprocess scenarios and SM-MULTIPROCESS-WATCHER transition table on supported platforms. | Thread mocks cannot prove spawn behavior. Generic worker/test DSLs hide PID, exit, and liveness diagnostics. Aggregate scaled deadlines, watcher-thread liveness, joins, and late-result inspection were strengthened in place. | P3 retained after T12 transition-contract review; user approved 2026-07-29. |

### State-Machine Transition Gate [DOM-10.2]

Every genuine state machine in production code, repository tooling, examples,
or test infrastructure must be a named unit with an executable transition
contract.

A state machine exists when state persists across calls, yields, threads,
callbacks, processes, or invocations and changes which next events are legal or
which actions and outcomes must occur. A one-pass decision tree, validation
checklist, parser-precedence chain, fixed-mode dispatcher, scan, or ordinary
retry loop does not qualify solely because its control flow can be drawn as a
graph.

Owner: the module that owns the persistent state owns the machine; its closest
contract test owns the executable transition table. Boundary: all first-party
Python code, including extensions, repository tooling, examples represented as
supported or copyable, and reusable test-process helpers. Verification: each
machine has a table-driven test whose rows name a stable transition ID, start
state, event, guard or precondition, next state, required actions or durable
effects, and expected error or terminal result. Required action: a machine may
not be refactored, extended, or declared verified until every legal, rejected,
terminal, and failure transition in its declared contract has a firing row.

Requirements:

- transition rows are the executable contract; scenario, property, race, and
  integration tests supplement them but do not replace them
- every declared transition ID fires exactly once in the table owner, while a
  case may execute against more than one backend or operating mode
- forbidden events and failure precedence are transitions when they alter
  state, preserve state, or select a different terminal outcome
- concurrency and persistence transitions use real threads, processes,
  filesystems, SQLite databases, and backend integration fixtures where those
  interactions are the behavior under test; mocks may replace only external
  nondeterministic transports, clocks, or fault sources, never the state owner
- production enums or a generic state-machine framework are not required;
  introduce them only when they make the owned state or transition seam clearer
- the implementation inventory maps every named machine to its owner,
  transition-table test, integration proof, and governing product or process
  contract
- when discovery changes the machine boundary or transition set, update the
  transition table and implementation inventory in the same change

## 11. Independent Review Workflow [DOM-11]

Non-trivial plans and completed work should receive an independent review.

Requirements:

- the reviewer receives the governing spec, active plan, relevant
  implementation note, and touched files
- the review focuses on errors, bad ideas, latent ambiguities, performative
  overengineering — process, abstraction, or ceremony that does not address
  a real risk or improve correctness — and whether a different engineer
  could implement the plan confidently and correctly
- the authoring agent considers each review point explicitly
- the authoring agent either updates the work or records why the current path
  remains the best choice
- prefer a different agent family or model than the original author when one is
  available

## 12. Skills Lifecycle [DOM-12]

Reusable skills live in `skills/`.

Requirements:

- create a skill when repeated work in a stable area would benefit from shared
  instructions
- common candidates include running, adding, testing, debugging, release, or
  domain-specific workflows
- skills should complement runbooks: skills are task-scoped instructions,
  runbooks are repository process guidance
- after using a skill, evaluate whether it should be updated

Useful evaluation questions:

- did the skill omit a required command, check, or failure mode?
- did it leave the owner, boundary, verification, or required action unclear?
- did the work require repeated clarification that should become part of the
  skill?

## 13. Agent Availability Bootstrap [DOM-13]

At session start and periodically over time, record which agent families are
available in the current environment.

Requirements:

- note which agents are available for independent review work
- distinguish between present, verified usable, and blocked states when
  recording availability
- refresh the inventory when tooling changes materially, meaning agent
  availability, credentials, invocation path, or review preference has changed
  enough to alter how review work should be assigned
- prefer a different agent, not just a same-family subagent, for plan review
  when one is available

## 14. Coalescing and Memory Maintenance [DOM-14]

The documentation surface is a tiered memory. Raw, dated records (lesson
entries; completed plans) are the moment tier. Distilled rules (golden
rules, runbook amendments), the plans ledger, and promoted skills are
summary tiers. The working tree holds only the current, assembled state;
git history is the archive. Docs change in place to match reality — going
back in time is git's job, not the working tree's.

Requirements:

- each repository keeps coalescing state in `docs/coalescing.md`: declared
  per-tier thresholds, per-tier watermarks, and a one-line-per-run log
- coalescing triggers are event-derived, not calendar-based: counts are
  computed from the watermark and the current tree, never stored, and are
  denominated in the repository's fold unit — a domain-grouped ledger
  counts per section, not repo-wide — counting only fold-eligible (cold,
  unfolded) material; the fold unit and its matching progress model are
  declared in the repository's `docs/coalescing.md` (per-section
  watermarks for domain-grouped ledgers; a fold-records index, not a date
  cursor, for ledgers folded by theme-cluster across dates, since a date
  cursor falsely claims older unfolded material behind it was folded)
- the session-start trigger check is read-only: a tripped threshold is
  reported to the user, never acted on mid-task. All coalescing writes —
  including checked-deferred records — happen only inside an authorized
  maintenance task (user request, or agreed completion-boundary work).
  Silently ignoring a trip is the only invalid response; reporting costs
  one sentence
- coalescing is additive-first across commit boundaries: distillation
  drafts and retirement candidates may exist uncommitted; deleting raw
  material, advancing watermarks, and retiring plans require a
  landing-authorized phase with a durable checkpoint
- deferrals have real state: a checked-deferred record carries
  `checked_through` (date and SHA), the derived counts, the reason, and a
  reconsideration condition — so an unchanged count does not re-nag every
  session, and a changed count does
- coalescing is two-phase and additive-first: distill, verify links and
  cues, then retire; every fold leaves a retrieval cue — the date range
  plus a `source_sha`, a pre-fold commit that verifiably contains the raw
  material — in the surviving summary or ledger line. The fold commit may
  be recorded in the run log after it exists, but it is never the cue
- recent or still-cited raw material stays verbatim; golden rules and
  safety invariants carry an importance floor — exempt from automated
  decay, changed only by explicit revision, supersession, or deprecation
  with a `(revised YYYY-MM-DD; was: <gist>)` marker
- active plans keep instructions mutable and logs append-only, and become
  immutable at closure; retirement is two-step — the sweep soft-retires
  (status `retired-pending`, backlinks converted, ledger line written)
  only after the harvest gate in `runbooks/writing-plans.md` passes, and
  physical deletion happens in a dedicated follow-up change after the
  gate is independently verified; plans marked `exemplar` in the status
  index are exempt until their exemplar role is superseded
- run-log entries are claims: each fold line must be spot-checkable
  against the diff of the fold commit

Owner: whoever the sweep check nags — any agent that observes a tripped
threshold at session start. Boundary: applies to lessons, plans, runbook
and skill promotion, and (for the guidance repo) cross-repo fold-up; specs
and implementation docs are living documents maintained per [DOM-6] and
[DOM-7], not coalesced. Verification: the run log plus the repository's
traceability gate. Required action: when a threshold is tripped, report
the trip state; respond with a sweep or a checked-deferred line per the
trigger rules above.

## 15. Task Classification [DOM-15]

Every unit of work is classified before the repository preflight or
first edit. The unit of work is the whole requested outcome; slices
inherit the unit's minimum class. Classification scales planning
artifacts and review machinery; it never scales the verification floor —
evidence lines, completion claims backed by reruns from current state,
firing tests for touched enumerable contracts, failing-test-first with
its named exit (engineering principle §10), declared deviations,
formatter ownership, no agent self-attribution, and dirty-tree
discipline apply identically at every class.

The class is the **highest trigger that fires**, judged by what the
change requires — not by what the author chooses to produce:

| Class | Fires when | Planning artifact | Review |
|-------|-----------|-------------------|--------|
| 0 — Read-only | Nothing in the repository changes | None | None; claims cite evidence and distinguish verified from inferred |
| 1 — Trivial | A change with no observable behavior change and no normative doc force (typos, comments, link repairs, formatting) | Classification line plus what/why/verification, recorded in the commit message — or in the handoff report when the work is left uncommitted for review | None |
| 2 — Small | Observable behavior changes but **conforms to existing intended behavior**, evidenced by something independently inspectable — a governing spec section, an explicit user requirement in the session, or an existing contract test. Author inference is not intent evidence; without it, the class is 3. Also requires: reversible, and **no [DOM-5] non-trivial or risky trigger fires** | The abbreviated preflight, pre-edit: (1) outcome checklist, (2) the intent evidence or `Source spec: None — <reason>`, (3) invariants that must not move, (4) the planned verification command. The observed result is appended at completion. Recorded in the commit/PR description or handoff report | Author fresh-eyes |
| 3 — Standard | Any **[DOM-5] non-trivial trigger** | Full dated plan per `runbooks/writing-plans.md`, status-index row, deviation log | Independent review of the plan **and** of the completed work ([DOM-11]) |
| 4 — Risky | Any **[DOM-5] risky trigger** | Class 3 plus the hardening-plans checklist | Class 3 plus review before implementation begins |
| 5 — Spec-changing | **[DOM-6] requires a spec change** (whether or not one has been drafted), or any normative spec text is edited — including clarification-only edits, which use promotion strategy D per `writing-plans.md` §4c | Class 3 plus spec baseline, exact proposed delta, named promotion strategy; the hardening-plans checklist **only if a [DOM-5] risky trigger also fires** — otherwise declare `hardening: N/A — no risky trigger` | Class 3 reviews plus independent review of the delta before the spec-promotion slice; review-before-implementation when hardening applies |
| +P — Process-changing (modifier, not a class) | The change is [DOM-6]-material to how future work is **planned, implemented, reviewed, or verified** — regardless of which surface hosts it. A non-material edit to a skill or runbook (a typo, a link fix) is not +P; a material process change hiding in an "implementation" doc is | Declared as `Class N+P`; effective requirements are `max(N, 5)`'s | Effective class's review plus pre-landing review, different agent family preferred |

Rules:

- the review and verification floors accumulate; planning artifacts
  **subsume**: a higher-class plan replaces the lower-class records, it
  does not add to them (a class-3 plan is the planning record — no
  separate class-2 preflight note is owed). The hardening-plans
  checklist is required by the class-4 trigger, never by inheritance:
  class-5 work with no [DOM-5] risky trigger declares `hardening: N/A —
  no risky trigger` instead of writing empty rollback sections. [DOM-5]
  risk and [DOM-6] materiality are different axes; they combine when
  both fire
- class-3 independent review may return a short structured brief —
  goal, class claim, invariants, verification, top risks. The brief is
  an **output** form only: the reviewer still receives the canonical
  inputs (governing spec, plan, touched files) and the disposition loop
  still runs in full. Classes 4 and 5 keep the full output bar. Author
  fresh-eyes substitutes for independent review only when no second
  agent is available, with the limitation disclosed — at every class
- classification is a one-line declared claim citing its trigger
  reasoning ("Class 2: restores spec section XYZ-3 intent, reversible, no DOM-5
  trigger"); an undeclared class on non-read-only work fails the
  completion gate
- escalators are one-way and declared mid-flight: when any [DOM-5]
  trigger or [DOM-6]-material discovery fires during work, the class
  rises to that trigger's class at that moment. The engineering
  warning signs (a second path appearing, rollback becoming
  undescribable) are not triggers of their own — they force
  re-classification against the same [DOM-5]/[DOM-6] lists. Silent
  continuation at the old class is the violation, not the escalation
- `+P` is a modifier: it combines with the base class as
  `max(base, 5)` plus the pre-landing different-family review; there
  is exactly one declaration format, `Class N+P`
- classes 1–2 keep their record in the commit history (or the handoff
  report when uncommitted) — git is the ledger for small work, which
  also keeps `docs/plans/` free of [DOM-14] harvest debt
- when classification is genuinely uncertain after reading the [DOM-5]
  lists, ask once, narrowly

Classification fixtures. This table is [DOM-15]'s enumerable contract
(engineering principle §12) and carries an executable gate: a
repository adopting this section ships a structural checker that fails
when a fixture names an unknown class, a class or the `+P` modifier
has no fixture, a class-1/2 fixture omits its negative-trigger facts,
or the cumulative-requirements rule is absent (this repository:
`bin/check-dom15-fixtures`, exit nonzero on violation). Semantic
classification of real tasks remains judgment, verified by the
declared-claim line and by review; repositories with test harnesses
additionally encode these fixtures as firing tests over their own
tooling. Fixture rows state their trigger facts explicitly — class
follows from the stated facts, never from file topology. Edits to
[DOM-5]'s trigger lists update these fixtures in the same change: the
checker enforces presence, review enforces meaning.

| Fixture (trigger facts stated) | Class |
|---------|-------|
| Answer an architecture question; survey a repo — nothing changes | 0 |
| Fix a spelling error; repair a broken doc link — no behavior change, no normative force, no [DOM-5] trigger fires | 1 |
| Behavior-preserving refactor, one module, following the established pattern — given: no [DOM-5] non-trivial or risky trigger fires (in particular, no zero-context ambiguity) | 1 |
| Behavior-preserving refactor across two modules with unclear ownership — zero-context ambiguity, a [DOM-5] non-trivial trigger, fires | 3 |
| Bug fix restoring validation that a cited spec section requires — the cited section is the intent evidence; reversible; given: no [DOM-5] trigger fires | 2 |
| Same fix, but no spec, no stated user requirement, no contract test — intent evidence absent | 3 |
| Fix spanning a producer and a consumer — given: the two sides are distinct major surfaces, so a [DOM-5] non-trivial trigger fires | 3 |
| Same shape, but both sides live inside one module — reversible, spec-cited intent, and no other [DOM-5] trigger fires | 2 |
| Implement an already-specified CLI flag — CLI shape changes ([DOM-5] risky) | 4 |
| Introduce background or deferred processing whose intended behavior an existing spec already governs — a [DOM-5] risky trigger fires; no [DOM-6] spec change is required | 4 |
| Clarify normative spec wording, behavior unchanged — normative spec text edited; no risky trigger, so `hardening: N/A` | 5 (strategy D) |
| New feature whose intended behavior is undocumented and [DOM-6]-material — a spec is required first | 5 |
| Materially change a skill, runbook, or gate — [DOM-6]-material to future process; base class 3 | Class 3+P (effective 5) |
| Typo fix inside a skill file — not [DOM-6]-material | 1 |
| Class-2 fix discovers a storage-format edit is needed — a [DOM-5] risky trigger fires mid-flight | Escalate to 4 at that moment, declared |

Owner: the agent starting the work declares the class; any reviewer
may challenge it. Boundary: every unit of work from promotion of this
section forward; explicit user instructions and safety constraints
still rank above classification in the decision hierarchy.
Verification: the declared class line plus the class-required
artifacts existing; new classification guidance checked against the
fixture table. Required action: declare the class before the first
edit; escalate loudly the moment a trigger fires.

## Related Plans

This spec was authored and evolved in the agent-guidance repository;
the plans below live there, not in this repository (quoted by name so
no local path dangles).

Local adoption record (soft-retired; not a live path claim):

- retired: 2026-07-16-agent-guidance-bootstrap-plan — source f133ce7; see docs/plans/README.md
- retired: 2026-07-17-propagate-guidance-delta-wave-plan — source f133ce7; see docs/plans/README.md
- retired: 2026-07-27-agent-docs-coalescing-and-status-hygiene-plan — source f133ce7; see docs/plans/README.md

Local plans:

- `docs/plans/2026-07-29-ruff-lint-expansion-plan.md`
- `docs/plans/2026-07-29-complexity-and-state-machine-hardening-plan.md`

Hub plans (names only; live in agent-guidance):

- "2026-04-07-development-documentation-foundation-plan"
- "2026-04-07-plan-hardening-guidance-plan"
- "2026-04-07-review-skills-bootstrap-plan"
- "2026-04-07-specs-index-renumbering-plan"
- "2026-07-14-coalescing-layer-plan"
- "2026-07-14-task-class-matrix-plan"
