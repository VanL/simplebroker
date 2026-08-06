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

#### Ruff Suppression Exceptions [DOM-10.1.1]

Refactor the code by default. Adopt a local suppression only when the smallest
behavior-preserving refactor would be a net negative for understandability,
locality, and readability, or would change a protected invariant. A lower lint
score or smaller function is not sufficient reason.

Owner: this section owns suppression-adoption policy and source-marker grammar.
The task-scoped `docs/implementation/10-ruff-suppression-registry.md` owns
approved groups, cardinalities, rationales, the global raw-`noqa` inventory,
and the generated location index. Boundary: local Ruff suppressions in
first-party Python files. Verification: `ruff check .`, `RUF100`, and
`uv run --frozen --no-sync python bin/ruff_suppression_index.py --check`.
Required action: obtain explicit review before adding or regrouping a
suppression; update the source pointer and task-scoped registry in the same
change; then regenerate with
`uv run --frozen --no-sync python bin/ruff_suppression_index.py --write`.

An approved exception uses exactly `# noqa: <codes> approved [DOM-10.1.1]
[RUFF-SUP-NNN] exception`. Its registry row records the stable group ID,
allowed rules and cardinalities, protected invariant, real proof, rejected
alternatives, and approval. The checker reconciles those human approvals,
every tagged source directive, raw Ruff diagnostics, the complete global
aggregate, and the generated location index. The generator may replace only
its delimited generated block; it must never create or edit human approval
evidence. Unreadable or malformed discovered Python input aborts without a
partial write and uses the clean exit-2 tool-failure boundary.

The registry is operational evidence, not required startup or spec reading.
Consult it only when proposing, reviewing, regrouping, regenerating, or
auditing a suppression.

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
| 3 — Standard | Any **[DOM-5] non-trivial trigger** | Full dated plan per `runbooks/writing-plans.md`, status-index row, deviation log, consulted-surfaces declaration (gated for in-flight plans by `bin/check-plan-context`) | Independent review of the plan **and** of the completed work ([DOM-11]) |
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

- `docs/plans/2026-08-06-ruff-suppression-registry-extraction-plan.md`
- `docs/plans/2026-08-06-plan-context-gate-plan.md`
- retired: 2026-08-04-coalescing-git-archive-policy-plan — source `5023710`;
  see the ledger in `docs/plans/README.md`
- `docs/plans/2026-07-29-ruff-lint-expansion-plan.md`
- retired: 2026-07-29-complexity-and-state-machine-hardening-plan — source
  `5023710`; see the ledger in `docs/plans/README.md`
- retired: 2026-07-29-program-theory-and-negative-knowledge-plan — source
  `5023710`; see the ledger in `docs/plans/README.md`
- `docs/plans/2026-07-30-ruff-suppression-index-generator-plan.md`

Hub plans (names only; live in agent-guidance):

- "2026-04-07-development-documentation-foundation-plan"
- "2026-04-07-plan-hardening-guidance-plan"
- "2026-04-07-review-skills-bootstrap-plan"
- "2026-04-07-specs-index-renumbering-plan"
- "2026-07-14-coalescing-layer-plan"
- "2026-07-14-task-class-matrix-plan"
