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

Two startup paths serve different jobs.

For repository work, load in this relative order:

1. the root agent entry point for safety, repository constraints, and routing
2. the `docs/agent-context/context.index.yaml` `read_order`, whose first item
   is `docs/program-theory.md`, before product-scope or design judgment
3. the current agent inventory, when one exists
4. the relevant winning product contract, active plan, implementation
   rationale, code, tests, and task-specific skill or runbook

For product use or embedding, load:

1. `docs/agent-kernel.md`
2. the winning root-README or canonical-spec section linked by the kernel and
   product-section registry

`docs/program-theory.md` owns the current conceptual account of purpose,
mental model, concepts, desired feel, durable principles, non-goals, tensions,
falsifiers, and revisions. It does not independently own exact product
behavior. Entry points may shorten or route these paths; they may not define a
competing order.

The shared agent context should stay repository-owned so multiple agent tools
can consume the same durable guidance. Tool-specific root aliases should
symlink to the canonical root entry point when the environment supports
symlinks. If symlinks are not practical, keep those files as thin pointers
back to the canonical entry point.

## 4. Traceability Requirements [DOM-4]

For changes that affect product identity, concept meaning, a durable principle
or non-goal, or a recorded decision case, preserve:

`program theory <-> winning product contract <-> plan/decision <-> implementation rationale <-> code/test evidence`

For other material behavior changes, the chain begins at the winning product
contract.

Requirements:

- theory-changing plans cite exact `[THEORY-*]`, `[REV-*]`, or `[ALT-*]`
  references when they exist
- plans cite exact winning product-contract files and reference codes when
  they exist
- theory and specs maintain backlinks to related plans
- implementation docs cite governing theory or contract sections and key files
  when ownership would otherwise be ambiguous
- code points to the governing contract where ownership would otherwise be
  ambiguous
- theory may summarize a stable behavioral consequence only non-normatively
  and with a precise link to the winning contract

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

Git-backed coalescing is not a destructive edge for classification purposes
when every removed item has a verified pre-fold source SHA reachable from a
retained Git ref and the repository's traceability gate passes. An ordinary
authorized sweep does not require a task plan merely because it soft-retires or
physically removes plans, removes already-distilled or expired raw ledger
entries, advances watermarks, or updates the run log. A plan is required
when the sweep promotes or materially changes durable guidance (for example a
golden rule, principle, runbook, skill, or cross-repository rule), or when some
other [DOM-5] trigger independently fires. The routine sweep is Class 2:
explicit authorization supplies intent, Git makes it reversible, and this
paragraph excludes the coalescing removals themselves from [DOM-5]'s triggers.

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
rejected decompositions and approval; and assign a stable suppression-group
ID.

The policy gate runs normal Ruff and a raw audit with `--ignore-noqa`. Source
directives, human-owned [DOM-10.1.1] groups, the generated location index, and
raw findings at tagged locations using Ruff's reported `noqa_row` must
reconcile exactly, including each group's human-approved directive and
raw-diagnostic cardinalities. A new unsuppressed finding, an unregistered
tagged directive, an unknown or empty group, a cardinality change, a stale
directive, a stale generated index, or a mismatched raw finding at a tagged
location fails verification.

A separate movement-stable global raw-diagnostic inventory continues to cover
every local `noqa`, including reasoned suppressions outside this registry. It
is an exact aggregate by rule code: aggregate changes fail verification, while
a same-code remove/add swap remains visible to source review rather than
receiving false identity semantics. Per-file ignores, global ignores, and
baseline allowlists are not permitted. A cohesive parser, checklist, or state
machine must not be fragmented merely to lower its score.

#### Approved Ruff Suppression Registry [DOM-10.1.1]

The following local suppressions are approved exceptions to [DOM-10.1]. This
spec owns the durable registry. A plan may propose or review a candidate, but
must not become the lasting source of truth for an adopted exception.

Owner: this section owns each stable suppression group, approved
cardinalities, and human-reviewed rationale. The local directive owns the rule
codes and stable group pointer. The generated location index owns only derived
paths, symbols, and actual cardinalities. A symbol is the outermost enclosing
`def`, class-qualified, or `<module>`; keying on symbols rather than lines
keeps the index stable when code moves and makes a suppression migrating
between functions visible even when the group cardinality is unchanged. Boundary: only the rule family,
cardinality, and invariant approved in the human row. Verification: the named
real proof, `ruff check .`, `RUF100`, and
`uv run --frozen --no-sync python bin/ruff_suppression_index.py --check`.
Required action: obtain explicit review before adding or regrouping a
suppression; update the human row, approved cardinalities, and source pointer
in the same change; then regenerate the derived index with
`uv run --frozen --no-sync python bin/ruff_suppression_index.py --write`.

For an approved exception, the local form is
`# noqa: <codes> approved [DOM-10.1.1] [RUFF-SUP-NNN] exception`. The stable
group points to the single durable full reason. Do not duplicate the full
rationale in source comments. The generator may update only the delimited
derived index; it must never create or edit an approval, invariant, proof, or
rejected alternative. A temporary C901 group must also name the plan task that
removes or re-evaluates it.

The human registry columns are `Group`, `Rules`, `Approved cardinality`,
`Protected invariant`, `Real proof`, `Rejected alternatives`, and `Approval`.
Group IDs are unique and match `RUFF-SUP-[0-9]{3}`. Rules list the only codes
the group may own. Approved cardinality records the permitted directive count
and raw-diagnostic count by code. Every human group must have at least one
live source directive.

This section also owns one complete, lexically sorted
`Global raw-\`noqa\` inventory:` line using backticked `CODE=count` entries. It
records every raw Ruff diagnostic exposed by `--ignore-noqa`, including
locally reasoned directives outside the grouped registry. It is an aggregate
count tripwire, not a second group registry.

| Group | Rules | Approved cardinality | Protected invariant | Real proof | Rejected alternatives | Approval |
|-------|-------|----------------------|---------------------|------------|-----------------------|----------|
| `[RUFF-SUP-001]` | `PYI034`, `PYI036` | `13` directives; raw: `PYI034=6`, `PYI036=19` | Existing concrete `__enter__` returns and permissive `Any`-typed `__exit__` parameters remain valid for downstream subclasses, manual calls, and runtime annotation inspection. | `tests/test_ruff_policy.py::test_public_context_manager_annotations_remain_override_compatible`; `tests/test_ruff_policy.py::test_public_exit_annotations_keep_any_typed_parameters`; full mypy partitions. | `Self` broke a downstream override that copied the former concrete return. Precise exception/traceback types broke formerly valid calls. `object` parameters still break formerly valid narrower overrides. Removing annotations weakens the public contract. | Independent T8 PASS; user approved 2026-07-29. |
| `[RUFF-SUP-002]` | `SIM115` | `1` directive; raw: `SIM115=1` | The replacement `sys.stdout` stream remains open through later and interpreter-shutdown flushes after a downstream pipe closes. | `tests/test_commands_helpers.py` closed-pipe cases on descriptor and wrapper fallback paths. | A local context manager closes the installed stream. A retained-handle registry adds lifecycle complexity solely for lint. Lower-level open spelling only evades the diagnostic. | Independent T8 PASS; user approved 2026-07-29. |
| `[RUFF-SUP-003]` | `BLE001` | `14` directives; raw: `BLE001=14` | Agent-facing CLI and development-tool entry points convert every ordinary internal, backend, plugin, filesystem, and configuration failure into the documented no-traceback exit/result boundary while leaving `KeyboardInterrupt` handling distinct. | CLI, command, development-script, release-workflow, Redis harness, DOM-15 fixture, and documented exit-code suites. | Enumerating current exception subclasses is incomplete across third-party backends and plugins. A generic wrapper hides boundary ownership. Propagation changes the CLI contract. | Independent T8 PASS; user approved 2026-07-29; benchmark additions independently approved 2026-08-06. |
| `[RUFF-SUP-004]` | `BLE001` | `6` directives; raw: `BLE001=6` | Long-lived workers contain arbitrary user callback, backend listener, and injected polling-provider failures at their declared isolation or retry boundary. | Reference-reactor tests; watcher retry, handler, edge-case, and backend notification/listener suites. | Narrow backend types miss user code and adapter failures. Letting a background thread die loses stored-error/wakeup behavior. A generic helper obscures distinct retry and notification semantics. | Independent T8 PASS; user approved 2026-07-29. |
| `[RUFF-SUP-005]` | `BLE001` | `5` directives; raw: `BLE001=5` | Checkout release, owned backend shutdown, weakref finalization, and context-exit cleanup remain best-effort and never replace the primary failure or make teardown fatal. | Connection lifecycle, queue connection-manager, cleanup, runner error-handling, and watcher edge-case suites. | Narrowing is unsound for arbitrary protocol/plugin implementations. Propagation changes cleanup semantics. `suppress(Exception)` would erase logging or failure-note behavior. | Independent T8 PASS; user approved 2026-07-29. |
| `[RUFF-SUP-006]` | `BLE001` | `3` directives; raw: `BLE001=3` | Generator and sidecar transaction arbitration observes `GeneratorExit`, cancellation, and ordinary failures; rollback failure never silently commits partial caller-owned work and primary exception/poison precedence is retained. | Cross-thread finalization poisoning, generator-method, sidecar, and database lifecycle suites. | `Exception` misses required `BaseException` paths. Suppression contexts lose cleanup-failure notes and precedence. Built-in subclass enumeration is incomplete. | Independent T8 PASS; user approved 2026-07-29. |
| `[RUFF-SUP-007]` | `BLE001` | `60` directives; raw: `BLE001=60` | Thread, process, fork, deadlock-timeout, and generator-resumption probes capture every child outcome, including `BaseException`, so the parent can assert identity, traceback, cleanup, poison, and liveness. | The named real concurrency, backend, phase-lock, fork-safety, watcher, process-session, transition-table, and poisoning suites. | Futures replaced catches where timeout and lifecycle behavior stayed intact. They are unsafe for intentional daemon-thread deadlock, child-process serialization, and exact-`BaseException` assertions. Narrowing loses the behavior under test; generic capture helpers are lint evasion. | Independent T8 and transition-contract review PASS; user approved 2026-07-29. |
| `[RUFF-SUP-008]` | `BLE001` | `12` directives; raw: `BLE001=12` | Test harness entry points, diagnostic readers, arbitrary condition callbacks, watcher cleanup, and signal scripts report or contain unknown support failures without masking the primary test result or hanging a subprocess. | Benchmark argument tests; helper-script subprocess tests; watcher SIGINT and cleanup suites; timing-helper users throughout watcher/process tests. | Exception enumeration is not closed over arbitrary callbacks, streams, and watcher implementations. Propagation loses structured diagnostics or cleanup. `suppress` loses intentional reporting. | Independent T8 PASS; user approved 2026-07-29. |
| `[RUFF-SUP-009]` | `C901` | `3` directives; raw: `C901=3` | Schema reconciliation, database validation, and dump loading keep ordered format checks, transaction effects, and failure precedence in one debugging unit. | SQLite schema, validation, dump/load, and property suites; SM-SQLITE-SCHEMA and SM-DUMP-LOAD transition tables. | Raising the threshold hides new findings. Splitting branch-only helpers separates error order from mutation order without reducing caller knowledge. | P3 retained; initial C901 activation; user approved 2026-07-29. |
| `[RUFF-SUP-010]` | `C901` | `2` directives; raw: `C901=2` | Darwin provider discovery and durable phase completion keep cache publication, fallback, cancellation, lock ownership, and marker state local. | `tests/test_phaselock.py`; SM-DARWIN-XATTR and SM-PHASE-LOCK transition tables. | Moving lock stages across modules obscures unwind ownership. A generic state-machine layer adds indirection without a second adapter. Advisory acquisition was simplified at a real retry seam; further splitting these two owners would separate state publication from its failure order. | P3 retained after T9/T12 refactor and transition-contract review; user approved 2026-07-29. |
| `[RUFF-SUP-011]` | `C901` | `1` directive; raw: `C901=1` | Retry attempt, elapsed-budget, stop, notification, and wait-generator decisions remain readable as one bounded retry algorithm. | `tests/test_retry.py`, watcher/setup retry integration, and interruption tests. | Callback wrappers or a generic retry DSL would scatter stop and error precedence while leaving the same decisions. | P3 retained; initial C901 activation; user approved 2026-07-29. |
| `[RUFF-SUP-012]` | `C901` | `1` directive; raw: `C901=1` | Pytest override parsing keeps ordered argument-shape, worker, timeout, and compatibility precedence in one parser. | Script argument, development-tool, and subprocess suites. | A generic command framework enlarges the seam. Tiny branch wrappers hide rather than remove parser precedence. Packaging smoke orchestration was separately simplified into build, inspection, and install phases. | P3 retained after T12 refactor review; user approved 2026-07-29. |
| `[RUFF-SUP-013]` | `C901` | `2` directives; raw: `C901=2` | Timestamp validation and numeric precedence stay in one parser family with exact accepted forms, error order, and ordering semantics. | Timestamp edge, resilience, property, and backend integration suites; SM-TIMESTAMP-GENERATOR transition table. | Parser-combinator machinery would be larger than the grammar. Score-only predicates would duplicate precedence and diagnostics. Unit-suffix conversion was extracted at a genuine seam. | P3 retained after T12 refactor and transition-contract review; user approved 2026-07-29. |
| `[RUFF-SUP-014]` | `C901` | `1` directive; raw: `C901=1` | Queue fetch keeps selection, delivery, output, and empty-result precedence beside the public command result. | Command helper, fetch, CLI subprocess, and delivery suites. | A generic command runner obscures command-specific exit and cleanup contracts. Move, watch, and init were simplified through owner-local mode and lifecycle seams. | P3 retained after T12 refactor review; user approved 2026-07-29. |
| `[RUFF-SUP-015]` | `C901` | `2` directives; raw: `C901=2` | Suspended sidecar and transactional-batch frames retain lock, transaction, owner-thread, poison, and cleanup-failure precedence in one frame. | Sidecar, generator-method, exactly-once, cross-thread poisoning, and SM-DELIVERY-POISON transition suites. | Extraction would pass live transaction state across helpers or create a second unsafe cleanup path. A generic state-machine runtime weakens locality. | P3 retained after transition-contract review; user approved 2026-07-29. |
| `[RUFF-SUP-016]` | `C901` | `1` directive; raw: `C901=1` | Queue move retains delivery-mode selection, return semantics, generator closure, and public error translation at the queue interface. | Queue move, generator, delivery, watcher, typing, and public behavior suites. | Moving public-mode logic into detached helpers enlarges the interface. Stream cleanup now uses one shared iterator-close path and no longer needs an exception. | P3 retained after T12 refactor review; user approved 2026-07-29. |
| `[RUFF-SUP-017]` | `C901` | `1` directive; raw: `C901=1` | Polling mode, burst/backoff, activity hints, waiter replacement, data-version checks, and stop behavior remain one debuggable state owner. | Watcher, activity-replacement, burst, edge, race, and SM-POLLING transition suites. | Fragmenting the decision loop would spread live counters and waiter ownership. A generic state-machine framework adds no adapter. | P3 retained; initial C901 activation; user approved 2026-07-29. |
| `[RUFF-SUP-018]` | `C901` | `1` directive; raw: `C901=1` | PostgreSQL vacuum keeps lease, advisory lock, delete batches, maintenance choice, unlock, and release precedence in one protocol owner. | PostgreSQL maintenance and contract-edge suites; SM-PG-VACUUM transition table. | Phase helpers would pass live lease/transaction state and hide unwind order. A generic maintenance framework has no second adapter. | P3 retained; initial C901 activation; user approved 2026-07-29. |
| `[RUFF-SUP-019]` | `C901` | `1` directive; raw: `C901=1` | Listener startup, notification routing, error publication, wakeup, stop, and close stay with the shared-listener owner. | PostgreSQL notify and runner-lifecycle suites; SM-PG-LISTENER transition table including startup timeout, fan-in, stored failure, and idempotent close. | Splitting routing from failure publication would duplicate listener state or require a wider internal interface. The completed transition table shows these branches share one thread-owned listener lifecycle. | P3 retained after T12 transition-contract review; user approved 2026-07-29. |
| `[RUFF-SUP-020]` | `C901` | `2` directives; raw: `C901=2` | Redis bounded scans and target cleanup keep cursor/key topology, reservation visibility, and cleanup ordering with their Redis owners. | Redis integration, batch, cleanup, pool, and plugin contract suites. | Generic scan/cleanup helpers would expose Redis key-layout state and make debugging cross-file. Cleanup is an ordered key-topology checklist with one owner, not a persistent state machine. | P3 retained after T12 review; user approved 2026-07-29. |
| `[RUFF-SUP-021]` | `C901` | `1` directive; raw: `C901=1` | Redis broadcast keeps the atomic Lua result protocol, capacity growth, stale-fence retry, and terminal error translation together after selector normalization and patterned broadcast were extracted. | Redis atomicity, broadcast, integration, and SM-REDIS-BROADCAST transition suites using a real server and Lua script, including every result code and retry stage. | Moving atomic selection out of Lua breaks the protocol. Splitting retry state from result decoding would make the atomic script and Python owner harder to debug together; a generic framework hides result-code meaning. | P3 retained after T10 refactor and independent real-Valkey review; user approved 2026-07-29. |
| `[RUFF-SUP-022]` | `C901` | `1` directive; raw: `C901=1` | The real vacuum scenario keeps causal setup, batch commit, zero-row rollback, compact, unlock, and lease-release assertions in one readable proof. | The named test plus PostgreSQL maintenance integration and SM-PG-VACUUM table. | Test helpers would hide ordering or merely relocate branches; mocks would replace the behavior under proof. | P3 retained; initial C901 activation; user approved 2026-07-29. |
| `[RUFF-SUP-023]` | `C901` | `3` directives; raw: `C901=3` | Repository settings, synchronized release, and single-target release keep safety order, rerun state, exact post-commit SHA, CI truth, and tag publication in their respective workflow owners. | Release script, publication, workflow, and SM-RELEASE transition suites covering dirty/dry-run, generated-file commits, reuse, batch commits, and exact publication SHA. | A generic command framework hides destructive ordering. Sharing more phases between batch and single workflows would make subtly different commit and baseline semantics appear identical. The public dispatcher is now below the gate. | P3 retained after T10/T12 extraction and independent transition review; user approved 2026-07-29. |
| `[RUFF-SUP-024]` | `C901` | `2` directives; raw: `C901=2` | Priority and monitoring examples keep their distinct scheduling loops, handler dispatch, metrics, and activity notifications within each copyable pattern. | Multi-queue example runtime tests; SM-PRIORITY-WATCHER and SM-MONITORING-WATCHER transition tables. | A shared generic watcher would obscure distinct scheduling policies. Validation and entry construction were extracted from `MultiQueueWatcher`; further score-only helpers would separate each nested policy from its visible example flow. | P3 retained after T11/T12 refactor and transition-contract review; user approved 2026-07-29. |
| `[RUFF-SUP-025]` | `C901` | `1` directive; raw: `C901=1` | Reactor draining keeps queue role, scheduling, lease, backlog, active-set, and notification decisions beside the persistent reactor state. | Reference-reactor scenarios; SM-REACTOR and SM-REACTOR-OUTPUT transition tables. | Extracting by branch would pass the same mutable reactor state through a shallow interface. | P3 retained; initial C901 activation; user approved 2026-07-29. |
| `[RUFF-SUP-026]` | `C901` | `2` directives; raw: `C901=2` | The copyable SQLite example preserves explicit path checks and database validation order while production-equivalent SQLite-managed validation replaces unsafe header access. | Example validation tests, live-WAL lock proof, and production SQLite validation suites. | A shared import defeats the standalone example. Direct descriptor reads are unsafe; score-only predicates hide validation order. | P3 retained after T8/T11 validation rewrite and independent review; user approved 2026-07-29. |
| `[RUFF-SUP-027]` | `C901` | `2` directives; raw: `C901=2` | Benchmark validation and backend-resolution setup retain complete contract assertions as readable repository gates. | The named suites across supported backends and repository self-application. | Generic test DSLs would hide the contract being proved. The duplicate dependency AST scans were consolidated and no longer need an exception. | P3 retained after T12 review; user approved 2026-07-29. |
| `[RUFF-SUP-028]` | `C901` | `1` directive; raw: `C901=1` | CLI subprocess coverage keeps staging, child result, validation, atomic promotion, and cleanup in one harness owner. | `tests/test_dev_scripts.py`, CLI subprocess suites, and SM-CLI-COVERAGE transition table with a real child-to-promotion row. | Splitting publication from process ownership risks publishing incomplete coverage. Call-count mocks would not prove file semantics. The completed transition contract confirms this is one persistent file-lifecycle owner. | P3 retained after T12 transition-contract review; user approved 2026-07-29. |
| `[RUFF-SUP-029]` | `C901` | `4` directives; raw: `C901=4` | Cross-thread probes keep owner/foreign actor phases, timeouts, result publication, poison inspection, and cleanup in causal order. | Direct probe tests plus SQLite, PostgreSQL, and Redis backend process probes; SM-CROSS-THREAD-PROBE table. | Generic actor helpers can hide which thread owns cleanup. Narrow exception mocks would replace the failure identities under test. Deterministic synchronization and bounded retry seams were added without moving actor ownership. | P3 retained after T12 transition-contract review; user approved 2026-07-29. |
| `[RUFF-SUP-030]` | `C901` | `2` directives; raw: `C901=2` | Process-session failure and post-close races keep real thread/process coordination, ownership assertions, and cleanup outcomes together. | Process-session lifecycle and SM-PROCESS-SESSION transition suites. | Mock sessions cannot prove lease cleanup. Test extraction that passes all actors/state would reduce locality without reducing complexity. | P3 retained after T12 transition-contract and refactor review; user approved 2026-07-29. |
| `[RUFF-SUP-031]` | `C901` | `2` directives; raw: `C901=2` | The helper and integration proof keep readiness, interrupt delivery, watcher shutdown, diagnostics, and terminal exit distinguishable. | Direct SM-SIGINT-PROBE table plus watcher SIGINT subprocess tests. | Accepting all exit codes hides failed graceful shutdown. The managed-subprocess duplicate escalation paths were consolidated and no longer need an exception; these two POSIX/Windows protocol owners remain intentionally explicit. | P3 retained after T8/T11 transition review; user approved 2026-07-29. |
| `[RUFF-SUP-032]` | `C901` | `2` directives; raw: `C901=2` | Jitter and burst/backoff scenarios keep deterministic clock/event sequences and exact polling-state outcomes readable. | Watcher burst, edge, and SM-POLLING transition suites. | A test DSL would hide the timeline. Fragmented assertions make the state progression harder to debug. | P3 retained after T12 polling transition-contract review; user approved 2026-07-29. |
| `[RUFF-SUP-033]` | `C901` | `5` directives; raw: `C901=5` | Real watcher concurrency tests keep actor startup, exact expected observations, liveness, contention, results, and cleanup in one causal proof. | Watcher concurrency, race, thundering-herd, and SM-WATCHER-LIFECYCLE integration suites. | Mocks cannot prove scheduling or locks. Over-shared helpers would conceal which actor or queue failed and can recreate silent passes. Every peek observer now must see the complete sequence. | P3 retained after T8/T12 transition review; user approved 2026-07-29. |
| `[RUFF-SUP-034]` | `C901` | `7` directives; raw: `C901=7` | Multiprocess watcher workers and scenarios keep spawn readiness, aggregate deadlines, queue activity, lock state, results, diagnostics, and cleanup together. | The five real multiprocess scenarios and SM-MULTIPROCESS-WATCHER transition table on supported platforms. | Thread mocks cannot prove spawn behavior. Generic worker/test DSLs hide PID, exit, and liveness diagnostics. Aggregate scaled deadlines, watcher-thread liveness, joins, and late-result inspection were strengthened in place. | P3 retained after T12 transition-contract review; user approved 2026-07-29. |

Global raw-`noqa` inventory: `BLE001=102`, `C901=53`, `E402=2`, `F401=4`, `PYI034=6`, `PYI036=19`, `SIM115=1`

The generated location index is enclosed by the unique markers below. Its
columns are `Group`, `Locations`, `Directives`, and `Raw diagnostics`.
Generated rows are sorted by group ID; paths use repository-relative POSIX
spelling; lines are ascending; and codes are lexical. Content outside the
markers is human-owned and must remain byte-for-byte unchanged during
regeneration.

<!-- BEGIN GENERATED RUFF SUPPRESSION INDEX -->
| Group | Locations | Directives | Raw diagnostics |
|-------|-----------|-----------:|-----------------|
| `[RUFF-SUP-001]` | `extensions/simplebroker_redis/simplebroker_redis/core.py::RedisBrokerCore.__enter__`; `extensions/simplebroker_redis/simplebroker_redis/core.py::RedisBrokerCore.__exit__`; `simplebroker/_backend_plugins.py::BrokerConnection.__exit__`; `simplebroker/_phaselock.py::AdvisoryFileLock.__enter__`; `simplebroker/_runner.py::SQLiteRunner.__exit__`; `simplebroker/db.py::BrokerCore.__enter__`; `simplebroker/db.py::BrokerCore.__exit__`; `simplebroker/db.py::BrokerDB.__enter__`; `simplebroker/db.py::DBConnection.__enter__`; `simplebroker/db.py::DBConnection.__exit__`; `simplebroker/sbqueue.py::Queue.__enter__`; `simplebroker/sbqueue.py::Queue.__exit__`; `simplebroker/watcher.py::BaseWatcher.__exit__` | 13 | `PYI034=6`, `PYI036=19` |
| `[RUFF-SUP-002]` | `simplebroker/commands.py::_replace_stdout_with_devnull` | 1 | `SIM115=1` |
| `[RUFF-SUP-003]` | `bin/benchmark.py::_provision_backend`; `bin/benchmark.py::main`; `bin/check-dom15-fixtures::<module>`; `simplebroker/_scripts.py::packaging_smoke_main`; `simplebroker/_scripts.py::pytest_pg_main`; `simplebroker/_scripts.py::pytest_redis_main`; `simplebroker/cli.py::_run_cleanup`; `simplebroker/cli.py::_run_vacuum`; `simplebroker/cli.py::main`; `simplebroker/commands.py::_init_broker_target`; `simplebroker/commands.py::_init_sqlite_path`; `simplebroker/commands.py::_move_all_messages`; `simplebroker/commands.py::cmd_status`; `simplebroker/commands.py::cmd_watch` | 14 | `BLE001=14` |
| `[RUFF-SUP-004]` | `examples/async_simple_example.py::worker`; `examples/reference_reactor.py::Reactor._worker_loop`; `extensions/simplebroker_pg/simplebroker_pg/runner.py::_SharedActivityListener._run`; `extensions/simplebroker_redis/simplebroker_redis/plugin.py::_SharedRedisActivityListener._run`; `simplebroker/watcher.py::BaseWatcher._safe_call_handler`; `simplebroker/watcher.py::PollingStrategy._check_data_version` | 6 | `BLE001=6` |
| `[RUFF-SUP-005]` | `simplebroker/db.py::DBConnection._close_best_effort`; `simplebroker/db.py::DBConnection.cleanup`; `simplebroker/db.py::_ProcessSessionCoreFactory.create`; `simplebroker/sbqueue.py::Queue._install_finalizer`; `simplebroker/watcher.py::BaseWatcher.__exit__` | 5 | `BLE001=5` |
| `[RUFF-SUP-006]` | `simplebroker/db.py::BrokerCore._yield_transactional_batches`; `simplebroker/db.py::BrokerCore.sidecar` | 3 | `BLE001=3` |
| `[RUFF-SUP-007]` | `extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py::test_exact_broadcast_create_missing_resurrects_queue_deleted_before_atomic_point`; `extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py::test_exact_broadcast_does_not_resurrect_queue_deleted_before_selection`; `extensions/simplebroker_pg/tests/test_pg_notify.py::test_multi_queue_activity_waiter_listener_close_wakes_waiters`; `extensions/simplebroker_pg/tests/test_pg_queue_rename.py::test_postgres_prepare_rename_waits_for_meta_before_messages_lock`; `extensions/simplebroker_pg/tests/test_pg_queue_rename.py::test_postgres_rename_waits_for_write_like_table_lock`; `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_delete_write_race_never_orphans_redis_storage`; `tests/helper_scripts/cross_thread_generator_probe.py::_execute_probe`; `tests/helper_scripts/cross_thread_generator_probe.py::_execute_sidecar_probe`; `tests/helper_scripts/cross_thread_generator_probe.py::_probe_child`; `tests/helper_scripts/cross_thread_generator_probe.py::_queue_close_probe_child`; `tests/helper_scripts/cross_thread_generator_probe.py::_sidecar_probe_child`; `tests/test_connection_transition_tables.py::_foreign_call`; `tests/test_cross_thread_finalization_poisoning.py::_call_in_thread`; `tests/test_cross_thread_finalization_poisoning.py::_queue_close_mode_child`; `tests/test_cross_thread_finalization_poisoning.py::test_concurrent_publication_preserves_exactly_one_first_cause`; `tests/test_cross_thread_finalization_poisoning.py::test_healthy_lock_contention_stress_never_publishes_poison`; `tests/test_cross_thread_finalization_poisoning.py::test_poison_aware_lock_preserves_explicit_rlock_compatibility`; `tests/test_cross_thread_finalization_poisoning.py::test_poison_published_during_query_blocks_first_yield`; `tests/test_cross_thread_finalization_poisoning.py::test_preblocked_waiter_observes_poison_without_hanging`; `tests/test_edge_cases.py::_schema_migration_worker`; `tests/test_fork_safety.py::_abandon_fork_child`; `tests/test_fork_safety.py::test_fork_recovery_does_not_block_on_inherited_locks`; `tests/test_fork_safety.py::test_fork_recovery_runs_before_operation_lock`; `tests/test_fork_safety.py::test_fork_safety_protection`; `tests/test_fork_safety.py::test_forked_child_guarded_methods_raise`; `tests/test_fork_safety.py::test_forked_child_queue_generate_timestamp_raises`; `tests/test_fork_safety.py::test_new_instance_after_fork_works`; `tests/test_generator_methods.py::TestGeneratorMethods.test_at_least_once_generator_reentrant_call_no_deadlock`; `tests/test_phaselock.py::test_advisory_file_lock_rejects_same_instance_reentrant_context`; `tests/test_phaselock.py::test_no_xattr_existing_status_marker_does_not_bypass_held_lock`; `tests/test_phaselock.py::test_no_xattr_waiter_does_not_skip_when_phase_marked_while_lock_is_held`; `tests/test_phaselock.py::test_process_local_lock_serializes_threads`; `tests/test_phaselock.py::test_process_local_lock_timeout_includes_diagnostics`; `tests/test_phaselock.py::test_strict_lock_wait_can_be_cancelled`; `tests/test_process_broker_session.py::test_factory_close_does_not_cancel_checkout_rollback`; `tests/test_process_broker_session.py::test_non_sqlite_core_creation_after_close_does_not_retain_runner`; `tests/test_process_broker_session.py::test_persistent_sqlite_queue_close_waits_for_in_flight_operation`; `tests/test_process_broker_session.py::test_session_close_wins_race_with_core_creation`; `tests/test_queue_move_watcher.py::TestQueueMoveWatcher.test_concurrent_operations`; `tests/test_runner_error_handling.py::TestSQLiteRunnerErrorHandling.test_run_exclusive_setup_marker_does_not_bypass_held_lock`; `tests/test_watcher_concurrency.py::TestMixedMode.test_concurrent_writes_during_watch`; `tests/test_watcher_multiprocess.py::lock_test_process`; `tests/test_watcher_multiprocess.py::shutdown_test_process`; `tests/test_watcher_multiprocess.py::watcher_process`; `tests/test_watcher_race_conditions.py::test_pre_check_database_contention` | 60 | `BLE001=60` |
| `[RUFF-SUP-008]` | `tests/backend_benchmark.py::main`; `tests/helper_scripts/cleanup.py::WatcherTracker.stop_all`; `tests/helper_scripts/managed_subprocess.py::OutputReader.run`; `tests/helper_scripts/timing.py::_machine_performance_ratio`; `tests/helper_scripts/watcher_sigint_script_improved.py::main`; `tests/helper_scripts/watcher_sigint_script_instrumented.py::main` | 12 | `BLE001=12` |
| `[RUFF-SUP-009]` | `simplebroker/_backends/sqlite/schema.py::ensure_schema_v3`; `simplebroker/_backends/sqlite/validation.py::validate_database`; `simplebroker/_dump.py::load_lines` | 3 | `C901=3` |
| `[RUFF-SUP-010]` | `simplebroker/_phaselock.py::PhaseLockService.run_phases`; `simplebroker/_phaselock.py::_discover_darwin_xattr_provider` | 2 | `C901=2` |
| `[RUFF-SUP-011]` | `simplebroker/_retry.py::execute_retry` | 1 | `C901=1` |
| `[RUFF-SUP-012]` | `simplebroker/_scripts.py::_extract_pytest_runner_overrides` | 1 | `C901=1` |
| `[RUFF-SUP-013]` | `simplebroker/_timestamp.py::TimestampGenerator._parse_numeric_timestamp`; `simplebroker/_timestamp.py::TimestampGenerator.validate` | 2 | `C901=2` |
| `[RUFF-SUP-014]` | `simplebroker/commands.py::_process_queue_fetch` | 1 | `C901=1` |
| `[RUFF-SUP-015]` | `simplebroker/db.py::BrokerCore._yield_transactional_batches`; `simplebroker/db.py::BrokerCore.sidecar` | 2 | `C901=2` |
| `[RUFF-SUP-016]` | `simplebroker/sbqueue.py::Queue.move` | 1 | `C901=1` |
| `[RUFF-SUP-017]` | `simplebroker/watcher.py::PollingStrategy.wait_for_activity` | 1 | `C901=1` |
| `[RUFF-SUP-018]` | `extensions/simplebroker_pg/simplebroker_pg/plugin.py::PostgresBackendPlugin.vacuum` | 1 | `C901=1` |
| `[RUFF-SUP-019]` | `extensions/simplebroker_pg/simplebroker_pg/runner.py::_SharedActivityListener._run` | 1 | `C901=1` |
| `[RUFF-SUP-020]` | `extensions/simplebroker_redis/simplebroker_redis/core.py::RedisBrokerCore.find_message_ids`; `extensions/simplebroker_redis/simplebroker_redis/plugin.py::RedisBackendPlugin.cleanup_target` | 2 | `C901=2` |
| `[RUFF-SUP-021]` | `extensions/simplebroker_redis/simplebroker_redis/core.py::RedisBrokerCore.broadcast` | 1 | `C901=1` |
| `[RUFF-SUP-022]` | `extensions/simplebroker_pg/tests/test_pg_plugin_contract_edges.py::test_vacuum_compacts_after_deleting_claimed_batches` | 1 | `C901=1` |
| `[RUFF-SUP-023]` | `bin/release.py::_run_batch_release`; `bin/release.py::_run_single_release`; `bin/release.py::repository_settings_issues` | 3 | `C901=3` |
| `[RUFF-SUP-024]` | `examples/multi_queue_patterns.py::pattern_2_priority_simulation`; `examples/multi_queue_patterns.py::pattern_5_monitoring` | 2 | `C901=2` |
| `[RUFF-SUP-025]` | `examples/reference_reactor.py::Reactor._drain_queue` | 1 | `C901=1` |
| `[RUFF-SUP-026]` | `examples/sqlite_connect.py::validate_database_path`; `examples/sqlite_connect.py::validate_safe_path_components` | 2 | `C901=2` |
| `[RUFF-SUP-027]` | `tests/backend_benchmark.py::BenchmarkSettings.validate`; `tests/test_backend_plugin_resolution.py::test_non_aware_runner_with_resolved_target_uses_target_plugin` | 2 | `C901=2` |
| `[RUFF-SUP-028]` | `tests/conftest.py::run_cli` | 1 | `C901=1` |
| `[RUFF-SUP-029]` | `tests/helper_scripts/cross_thread_generator_probe.py::_execute_probe`; `tests/helper_scripts/cross_thread_generator_probe.py::_execute_sidecar_probe`; `tests/helper_scripts/cross_thread_generator_probe.py::_queue_close_probe_child` | 4 | `C901=4` |
| `[RUFF-SUP-030]` | `tests/test_process_broker_session.py::test_failed_core_creation_releases_any_runner_lease`; `tests/test_process_broker_session.py::test_non_sqlite_core_creation_after_close_does_not_retain_runner` | 2 | `C901=2` |
| `[RUFF-SUP-031]` | `tests/helper_scripts/watcher_sigint_script_improved.py::main`; `tests/test_watcher.py::TestQueueWatcher.test_graceful_shutdown_sigint` | 2 | `C901=2` |
| `[RUFF-SUP-032]` | `tests/test_watcher_burst_mode.py::test_burst_mode_state_transitions`; `tests/test_watcher_burst_mode.py::test_polling_jitter` | 2 | `C901=2` |
| `[RUFF-SUP-033]` | `tests/test_watcher_concurrency.py::TestMixedMode.test_concurrent_writes_during_watch`; `tests/test_watcher_concurrency.py::TestMixedMode.test_multiple_peek_watchers`; `tests/test_watcher_race_conditions.py::test_multiple_queues_concurrent_activity`; `tests/test_watcher_race_conditions.py::test_pre_check_database_contention`; `tests/test_watcher_thundering_herd.py::test_thundering_herd_with_multiple_active_queues` | 5 | `C901=5` |
| `[RUFF-SUP-034]` | `tests/test_watcher_multiprocess.py::lock_test_process`; `tests/test_watcher_multiprocess.py::test_multiprocess_database_locking`; `tests/test_watcher_multiprocess.py::test_multiprocess_graceful_shutdown`; `tests/test_watcher_multiprocess.py::test_multiprocess_separate_queues`; `tests/test_watcher_multiprocess.py::test_multiprocess_single_queue`; `tests/test_watcher_multiprocess.py::test_multiprocess_thundering_herd`; `tests/test_watcher_multiprocess.py::watcher_process` | 7 | `C901=7` |
<!-- END GENERATED RUFF SUPPRESSION INDEX -->

An unreadable or syntactically malformed discovered Python file makes the
complete index unverifiable. The tool must abort before writing, identify the
file in a one-line diagnostic, emit no traceback, and leave the existing spec
unchanged; partial indexes are prohibited. Other anticipated invocation,
decoding, and replacement failures follow the same clean exit-2 boundary.
Unexpected programming errors retain their traceback as bug evidence.

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
- coalescing removals are Git-backed archive maintenance, not permanently
  destructive, when a verified pre-fold source SHA reachable from a retained
  ref contains every removed item. The authorized sweep may delete
  already-distilled, expired, or otherwise nonnormative raw material, advance
  watermarks, and retire plans without a separate task plan or
  coalescing-specific commit authorization; an item that exists only in the
  worktree remains ineligible because it has no archive cue
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
- routine coalescing maintenance is plan-exempt. Promotion or material revision
  of durable guidance (golden rules, principles, runbooks, skills, or
  cross-repository rules) follows the ordinary [DOM-5]/[DOM-15] planning and
  review requirements before that promotion is written
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
| 5 — Theory/spec-changing | **[DOM-16] requires a material program-theory change**; a normative theory claim is added, removed, or reworded; **[DOM-6] requires a spec change**; or normative spec text is edited, including clarification-only spec edits, which use promotion strategy D per `writing-plans.md` §4c | Class 3 plus theory/spec baseline, exact proposed delta, named promotion strategy; the hardening-plans checklist **only if a [DOM-5] risky trigger also fires** — otherwise declare `hardening: N/A — no risky trigger` and state that no risky trigger fires | Class 3 reviews plus independent review of the delta before promotion; review-before-implementation when hardening applies |
| +P — Process-changing (modifier, not a class) | The change is [DOM-6]-material to how future work is **planned, implemented, reviewed, or verified** — regardless of which surface hosts it. A non-material edit to a skill or runbook (a typo, a link fix) is not +P; a material process change hiding in an "implementation" doc is | Declared as `Class N+P`; effective requirements are `max(N, 5)`'s | Effective class's review plus pre-landing review, different agent family preferred |

Rules:

- a material theory change adds, removes, or changes product purpose or desired
  feel, a core concept or its owner, a durable principle or non-goal, or a
  revision that changes current design judgment. Link repairs, source
  corrections, metadata edits, and other changes with no behavior change and
  no normative-force change do not trigger class 5 by file location alone
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
| Materially revise a product non-goal or core-concept owner, with no runtime behavior change; [DOM-16] requires a material theory change | 5 |
| Repair a broken program-theory evidence link; no behavior change, no normative-force change, and no [DOM-5] trigger fires | 1 |
| Materially change a skill, runbook, or gate — [DOM-6]-material to future process; base class 3 | Class 3+P (effective 5) |
| Authorized coalescing run that only removes already-distilled, expired, or nonnormative source-pinned raw entries, retires or deletes source-pinned plans, advances watermarks, and updates its run log — explicit user intent, reversible through a retained Git ref, and no [DOM-5] trigger fires because this section excludes those archive removals | 2 |
| Coalescing run that promotes a lesson into a golden rule or materially changes a runbook/skill — durable guidance changes | Class 3+P (effective 5) |
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

## 16. Program Theory and Negative Knowledge [DOM-16]

A program theory is the working explanatory model used to understand and
change a program coherently. It includes what the program is and is not, which
concepts exist, what they mean, which component owns each concern, why the main
boundaries exist, and what evidence would show the model is wrong.
`docs/program-theory.md` is this repository's current best externalized account
of that model.

The term follows Peter Naur's “Programming as Theory Building.” It does not
mean a formal theory, a requirements catalog, an architecture inventory, or a
design document renamed. It means the working explanatory model that lets a
maintainer connect the problem world to the program: explain why the system
has this shape, predict how a change will propagate, diagnose a surprise,
distinguish an extension from a category error, and revise the solution
without losing its coherence.

Theory owns the problem-world model, concept meanings, ownership boundaries,
and conceptual constraints that guide realization. Implementation documents
own concrete architectural and mechanical choices and why the current
realization chose them. A theory principle may constrain an architecture
without becoming the architecture record.

Code, specifications, tests, plans, and implementation documents are
expressions of and evidence about that model. None is the whole theory. A
program-theory document is therefore the current best externalized account and
a transfer surface, not a claim that tacit working understanding has been
completely serialized.

In this repository, agent-theory is the wager that a human and agents can
iteratively reconstruct, challenge, and refine a sufficiently shared theory by
keeping intent, code, tests, alternatives, and implementation surprises in
contact. Reading the theory document is a starting condition for judgment, not
proof that the reader possesses the theory.

No method can mechanically derive a coherent program from an arbitrary
problem. Executable gates nevertheless do more than educate or prompt: they
bind selected consequences of the theory, reject known-invalid states, and
force discrepancies into view. A green gate is binding evidence for the claim
it covers, not proof that the whole theory is correct or that a reader
possesses it. Judgment remains necessary to decide what a gate should express,
whether its premise still holds, and how evidence should revise the theory.

The model is provisional:

```text
concept → provisional theory → specification → implementation
        → evidence or surprise → revised theory
```

Owner: the human product owner approves product identity, concept meaning,
durable principles, and non-goals. Agents recover evidence, draft language,
challenge inconsistencies, and propose revisions. They do not infer intent
from current code or feature absence.

Boundary: theory owns conceptual identity and judgment. Winning product
contracts own exact behavior. Implementation docs own realization rationale.
Plans own work in flight.

Verification: structural gates check required metadata, sections, recognized
record syntax, stable references, links, and read order. Dogfood and owner
review judge meaning. Concrete consequences belong in the winning product
contract and receive firing tests there.

Required action: before materially changing a concept, boundary, principle, or
non-goal, read the theory and either conform or propose a class-5 revision.
Record the current account first, then the superseded account in summary,
pressure, and evidence.

The program-theory account must cover:

- what “program theory” means and what the document can and cannot transfer
- purpose and desired feel
- whole-program mental model
- core concepts and ownership
- durable principles and design consequences
- durable product non-goals
- live tensions and falsifiers
- founding continuity and evolution
- material revisions and decision cases

Exact current limitations live in the winning product contract. Theory may
link a limitation when it creates a live tension, but must not duplicate the
capability claim.

Negative statements have four types:

| Type | Meaning | Lifecycle |
|------|---------|-----------|
| Product non-goal | Durable identity boundary | Explicit theory revision and owner approval |
| Current limitation | Capability not currently provided | Owned and changed by the winning product contract |
| Rejected alternative | Plausible candidate declined under stated premises | Reopen only when its condition fires |
| Plan out-of-scope | Boundary on one work unit | Expires with the plan; implies no product judgment |

Do not record every local choice. A durable alternative is warranted when a
competent future editor is likely to propose it again, material investigation
cost was paid, it exposed a hidden constraint, or blind retry could cause harm.

Every durable alternative uses this exact shape:

```markdown
### [ALT-<SCOPE>-<NNN>] Short title

Disposition: adopted | rejected | deferred | superseded | invalidated
Owner: <decision owner>
Governs: <stable theory, spec, or implementation reference>
Source record: none | [ALT-...] in <live plan path> | <plan filename> at <source SHA> [ALT-...]
Candidate: <candidate>
Why plausible: <steelman>
Evidence:
- contemporaneous | owner-recalled | inferred | unknown: <direct source>
Reason: <reason for disposition>
Current consequence: <what current work must do>
Reconsider when: <observable condition>
Promoted to: none | [ALT-...]
```

Every theory revision uses this exact shape:

```markdown
### [REV-<SCOPE>-<NNN>] Short title

Current account: <revised theory>
Supersedes: <short description of the prior account; do not make it compete with current theory>
Pressure: <what made the prior account inadequate>
Evidence:
- contemporaneous | owner-recalled | inferred | unknown: <direct source>
```

`SCOPE` matches `[A-Z][A-Z0-9]*` and identifies the defining artifact, such as
`PT20260729`, `THEORY`, `DOM16`, or `IMPL01`. `NNN` is three digits allocated
by scanning existing definitions in that scope. Full IDs are unique across
definitions in root `README.md`, `docs/**/*.md`, and `skills/**/*.md`.
References may repeat; headings that define the record may not.

The structural parser scans only those three corpora. Its malformed fixture
strings remain inside the owning test module and are passed directly to parser
helpers, not discovered as repository records. The provenance token is one of
the four closed values shown above. Mixed provenance uses separate evidence
rows, never a compound token.

Revision records are current-account-first to reduce anchoring. Historical
sources are evidence, not startup assignments. A dedicated lineage section may
quote a few short founding phrases only when each is paired with explicit
`Maintained` and `Evolved` analysis and a statement that the current theory
governs. Do not reproduce the original README or obsolete theory at length.

When work touches a recorded boundary, the proposer and reviewer must search
the governing theory, spec, or implementation artifact for relevant
`[ALT-*]` records. A fired `Reconsider when` condition reopens review; it does
not adopt the old candidate. The proposal cites the old ID, presents new
evidence, gains owner approval, and updates the old disposition or records its
successor.

Active plans keep genuine alternatives append-only. Before closure, durable
content is copied to its steady-state owner under a new owner-scoped ID. The
plan record adds `Promoted to`; the steady-state record adds `Source record`.
The two records link reciprocally. The closed plan remains immutable
historical evidence; the steady-state record alone is current authority:

- identity, principle, or non-goal → program theory
- exact behavior → winning product contract
- architecture constraint → implementation doc
- reusable process correction → lesson, runbook, or skill
- temporary choice → immutable plan and git history

Do not create an unowned alternatives graveyard.

The reciprocal live-plan form is not permanent. Before physically deleting a
retired plan, coalescing rewrites each steady-state `Source record` from:

```text
[ALT-...] in docs/plans/<plan>.md
```

to:

```text
<plan>.md at <retired source SHA> [ALT-...]
```

The source-pinned form is a one-way retrieval cue because the plan definition
no longer exists in the worktree. Its plan name and SHA must match the Retired
Plans ledger. The structural gate requires a reciprocal `Promoted to` only for
the live-plan form. Before deleting the plan, the physical-deletion gate must
retrieve the ledger source and prove that the exact `### [ALT-ID]` heading
exists. A missing conversion, ledger mismatch, failed retrieval, or missing
heading blocks deletion.

## Related Plans

This spec was authored and evolved in the agent-guidance repository;
the plans below live there, not in this repository (quoted by name so
no local path dangles).

Local adoption record (soft-retired; not a live path claim):

- retired: 2026-07-16-agent-guidance-bootstrap-plan — source 197629e2; see docs/plans/README.md
- retired: 2026-07-17-propagate-guidance-delta-wave-plan — source 197629e2; see docs/plans/README.md
- retired: 2026-07-27-agent-docs-coalescing-and-status-hygiene-plan — source 197629e2; see docs/plans/README.md

Local plans:

- `docs/plans/2026-08-04-coalescing-git-archive-policy-plan.md`
- `docs/plans/2026-07-29-ruff-lint-expansion-plan.md`
- `docs/plans/2026-07-29-complexity-and-state-machine-hardening-plan.md`
- `docs/plans/2026-07-29-program-theory-and-negative-knowledge-plan.md`
- `docs/plans/2026-07-30-ruff-suppression-index-generator-plan.md`

Hub plans (names only; live in agent-guidance):

- "2026-04-07-development-documentation-foundation-plan"
- "2026-04-07-plan-hardening-guidance-plan"
- "2026-04-07-review-skills-bootstrap-plan"
- "2026-04-07-specs-index-renumbering-plan"
- "2026-07-14-coalescing-layer-plan"
- "2026-07-14-task-class-matrix-plan"
