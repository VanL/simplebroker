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
no local path dangles). This copy: adopted from agent-guidance @
`fc23eae` via `docs/plans/2026-07-16-agent-guidance-bootstrap-plan.md`.

- "2026-04-07-development-documentation-foundation-plan"
- "2026-04-07-plan-hardening-guidance-plan"
- "2026-04-07-review-skills-bootstrap-plan"
- "2026-04-07-specs-index-renumbering-plan"
- "2026-07-14-coalescing-layer-plan"
- "2026-07-14-task-class-matrix-plan"
