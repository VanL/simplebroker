# Program Theory and Negative Knowledge Documentation Plan

Status: active

Class: 5+P. This plan changes the governing documentation model, the class
definition for theory-changing work, and the required repository-agent read
path. It changes no runtime behavior.

Plan type: implementation with spec revision.

Hardening: N/A. No [DOM-5] risky-change trigger fires. The plan still
states authority invariants, sequencing gates, and rollback because a bad
documentation boundary could misdirect later work.

## Goal

Make SimpleBroker's current program theory explicit and easy for a zero-context
agent to acquire. Preserve both positive theory (what the program is, how its
concepts fit, and how it should feel) and negative theory (what it is not,
which plausible alternatives were rejected, and under which premises).

The theory is provisional. Use, bugs, implementation pressure, and review can
revise it. Exact current behavior remains owned by the root README or a
canonical product spec according to the product-section registry.

SimpleBroker is the first dogfood site. A later, separate plan may transplant
the proven process into `../agent-theory`; this plan does not edit that
repository.

## Requested Outcomes

- [x] Add a concise canonical `docs/program-theory.md`.
- [x] Explain what “program theory” means in the Naur-derived sense, how it
  differs from specifications and architecture documentation, and why the
  repository can externalize only a current account of the theory rather than
  the whole working understanding.
- [x] Preserve the original README as source-pinned historical evidence. Quote
  a few founding statements inside a clearly subordinate lineage section that
  explains what was maintained and what evolved. Do not freeze or copy the
  document as a parallel current theory.
- [x] Define the whole-program mental model and the ownership boundary between
  queue semantics, storage infrastructure, and application execution.
- [x] Replace the broad “not for distributed systems” claim with a precise
  boundary: SimpleBroker handles distributed-systems problems among
  cooperating processes, but does not operate a broker cluster, distributed
  storage system, application runtime, or control plane.
- [x] Give durable rejected alternatives a fixed record shape, provenance,
  owner, consequence, and explicit reconsideration action.
- [x] Recover only evidence-backed history before promoting it.
- [x] Dogfood theory transfer and proposal judgment before standardizing the
  workflow in runbooks and skills.
- [x] Add structural gates for syntax, references, and read order. Leave
  semantic judgment to owner review and dogfood evidence.
- [x] Correct stale documentation-system scaffold language.
- [x] Record an upstream export brief after the SimpleBroker adoption is
  complete.

## Settled Decisions From Exploration

1. “Program theory” means the working explanatory model that connects the
   problem being solved to the chosen concepts, boundaries, and realization.
   It is shown by the ability to explain, diagnose, predict, and change the
   program coherently, not by memorizing documentation.
2. A program-theory document is a transfer and challenge surface for that
   working model. Code, specs, tests, decisions, and implementation experience
   remain evidence needed to reconstruct and revise it; no document contains
   the whole theory.
3. A program theory precedes and informs specifications. It does not replace
   them.
4. Theory includes what the program is and what it is not.
5. Theory changes under concrete pressure. The program-theory document is the
   current best externalized conceptual account; product contracts state exact
   intended behavior. Neither is an advance decree made before discovery.
6. The original README was a theory artifact. It established the desired
   experience and an explicit “is / is not” boundary before formal specs or
   agent-theory existed. Short quotes can show continuity and evolution, but
   the original document is evidence, not current authority or a required read.
7. SimpleBroker owns queue semantics. It does not own a broker cluster,
   replicated or partitioned storage, membership, leader election, application
   task execution, or a distributed control plane.
8. Optional shared backends may let clients on multiple hosts coordinate queue
   operations. The backend owns its service topology, replication,
   consistency/availability mechanisms, and recovery. The application owns the
   work being coordinated.
9. Rejected proposals that failed a standard of “simplebrokerness” are evidence
   about the theory, not disposable discussion.
10. Missing history remains unknown. Feature absence is not evidence that the
   feature was considered and rejected.
11. `../agent-theory` currently expresses process more clearly than theory.
   Generalization follows concrete adoption.

## Source Documents and Evidence

Conceptual source:

- Peter Naur, “Programming as Theory Building” (1985; reprinted in
  *Computing: A Human Activity*, 1992), reviewed from the owner-supplied
  markdown copy during implementation. It supplies the distinction between
  code and the working theory used to explain and change it, the practical
  criteria for possessing that theory, and the challenge that no documentary
  or procedural expression is the whole theory.
- Donald E. Knuth,
  [“Literate Programming”](https://doi.org/10.1093/comjnl/27.2.97)
  (1984), together with
  [the author's overview](https://cs.stanford.edu/~knuth/lp.html). It supplies
  the discipline of addressing program explanation to human understanding and
  mechanically keeping explanatory and executable views in contact.
- The owner's agent-theory account in this session: humans may delegate
  implementation, but not understanding; intent, code, tests, decisions, and
  surprises are used iteratively to reconstruct and refine a shared theory.

Governing process:

- `AGENTS.md`
- `docs/specs/01-development-documentation-operating-model.md`
  [DOM-3], [DOM-4], [DOM-5], [DOM-6], [DOM-7], [DOM-8], [DOM-14], [DOM-15]
- `docs/agent-context/decision-hierarchy.md`
- `docs/agent-context/principles.md`
- `docs/agent-context/engineering-principles.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/writing-specs.md`
- `docs/agent-context/runbooks/writing-implementation-docs.md`
- `docs/agent-context/runbooks/maintaining-traceability.md`
- `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md`
- `skills/brainstorming-to-plan/SKILL.md`
- `skills/coalescing/SKILL.md`

Product-theory evidence:

- root `README.md`, current and initial version at
  `f1bd821640d2f51006eec321b21d5341b0175cdc`
- `docs/agent-kernel.md` and `llms.txt`
- `docs/specs/product-section-registry.md`
- `docs/specs/10-cli.md` [SB-CLI-*]
- `docs/specs/11-delivery.md` [SB-DELIVERY-*]
- `docs/implementation/04-cross-thread-finalization-poisoning.md`
- `docs/implementation/05-product-invariant-inventory.md`
- `docs/implementation/06-process-session-core-ownership.md`
- `CHANGELOG.md`
- source-pinned retired-plan history in `docs/plans/README.md`

Upstream comparison, read-only:

- `../agent-theory/README.md`
- `../agent-theory/docs/specs/01-development-documentation-operating-model.md`
- `../agent-theory/docs/implementation/01-documentation-system.md`
- `../agent-theory/skills/brainstorming-to-plan/SKILL.md`
- `../agent-theory/bin/bootstrap-agent-theory`

## Spec Baseline and Promotion

Baseline:
`2daa2fb48dd478fee5c01bec86add53793d55940`.

This is strategy A under `writing-plans.md` §4d: promote the reviewed process
contract early, then implement docs, workflow, and tests against it. Before
dependent edits, record a promotion baseline identifier: a commit SHA when
committed, or the diff base plus exact worktree-state identifier when held
uncommitted for owner review.

Promotion baseline after T4: diff base
`2daa2fb48dd478fee5c01bec86add53793d55940`; SHA-256 of
`git diff -- docs/specs/01-development-documentation-operating-model.md`:
`55336fe23437c540b62fd51d0e4d0ddf763ec291f8f6d3785dfb776fe707b836`.
The worktree also contains this active plan and its Status Index row. This is
an uncommitted owner-review baseline, not a landing claim.

| Owner | Planned change |
|-------|----------------|
| [DOM-3] | Add program theory to the artifact graph and define the two persona-specific read orders. |
| [DOM-4] | Extend the trace chain upstream to theory and define when that edge is required. |
| [DOM-15] | Make material theory changes explicitly class 5 and add classification fixtures. |
| new [DOM-16] | Define program theory and negative-knowledge ownership, records, and lifecycle. |
| Related Plans | Link this plan while active. |

The root README wording correction is a user-visible documentation
clarification. Add a top-level `## [Unreleased]` / `### Documented` entry. Do
not rewrite the dated `5.6.2` section or change a package version. The release
process may move the entry into its eventual version heading.

## Authority Model

```text
program theory
  purpose, concepts, principles, non-goals, tensions, falsifiers
        ↕
winning product contract
  exact observable behavior and enumerable obligations
        ↕
plan/decision ↔ implementation rationale ↔ code/test evidence
```

| Artifact | Owns | Does not own |
|----------|------|--------------|
| `docs/program-theory.md` | Product identity, conceptual model, concept meaning, durable principles and non-goals, theory revisions | Independent definition of exact current behavior, capability inventory, file layout, task checklists |
| Root `README.md` | Human product expression; exact behavior for registry rows marked `readme-only`; restatements and links for promoted concerns | A conceptual override of program theory; a competing copy of canonical-spec behavior |
| Canonical product specs | Exact intended behavior, invariants, interfaces, and verification obligations for promoted concerns | Product manifesto or temporary implementation choices |
| `docs/agent-kernel.md` | Compact agent-facing view of winning product contracts | Independent normative behavior or conceptual authority |
| Plans | Approved execution path, in-flight alternatives, assumptions, deviations, evidence | Steady-state truth after closure |
| Implementation docs | Why the current realization has its shape and the architectural alternatives that constrain edits | Product behavior authority |
| Tests and gates | Evidence for concrete and enumerable obligations; document syntax and navigation | Proof that qualitative judgment is wise |

A spec may refine a principle into observable obligations. It may not silently
contradict the theory. Theory may summarize a stable behavioral consequence
non-normatively when it links the precise winning contract. It may not define
that consequence independently; the linked contract controls.

## Proposed Process-Spec Delta

### [DOM-3] exact replacement

Replace the startup list with:

> Two startup paths serve different jobs.
>
> For repository work, load in this relative order:
>
> 1. the root agent entry point for safety, repository constraints, and routing
> 2. the `docs/agent-context/context.index.yaml` `read_order`, whose first item
>    is `docs/program-theory.md`, before product-scope or design judgment
> 3. the current agent inventory, when one exists
> 4. the relevant winning product contract, active plan, implementation
>    rationale, code, tests, and task-specific skill or runbook
>
> For product use or embedding, load:
>
> 1. `docs/agent-kernel.md`
> 2. the winning root-README or canonical-spec section linked by the kernel and
>    product-section registry
>
> `docs/program-theory.md` owns the current conceptual account of purpose,
> mental model, concepts, desired feel, durable principles, non-goals,
> tensions, falsifiers, and revisions. It does not independently own exact
> product behavior. Entry points may shorten or route these paths; they may not
> define a competing order.

Add `docs/program-theory.md` as the first
`docs/agent-context/context.index.yaml` `read_order` entry and as a `documents`
entry with role `program_theory`. The root `AGENTS.md` routes into that one
machine-readable order; it does not separately tell agents to read theory and
then repeat it through the index.

### [DOM-4] exact replacement

Replace the current trace-chain paragraph and requirements with:

> For changes that affect product identity, concept meaning, a durable
> principle or non-goal, or a recorded decision case, preserve:
>
> `program theory <-> winning product contract <-> plan/decision <->
> implementation rationale <-> code/test evidence`
>
> For other material behavior changes, the chain begins at the winning product
> contract.
>
> Requirements:
>
> - theory-changing plans cite exact `[THEORY-*]`, `[REV-*]`, or `[ALT-*]`
>   references when they exist
> - plans cite exact winning product-contract files and reference codes when
>   they exist
> - theory and specs maintain backlinks to related plans
> - implementation docs cite governing theory or contract sections and key
>   files when ownership would otherwise be ambiguous
> - code points to the governing contract where ownership would otherwise be
>   ambiguous
> - theory may summarize a stable behavioral consequence only
>   non-normatively and with a precise link to the winning contract

Delete the stale implementation snapshot claiming that product code has not
been added.

### [DOM-15] exact replacement row, rule, and fixtures

Replace the class-5 table row with:

> | 5 - Theory/spec-changing | [DOM-16] requires a material program-theory
> change; a normative theory claim is added, removed, or reworded; [DOM-6]
> requires a spec change; or normative spec text is edited, including
> clarification-only spec edits | Class 3 plus theory/spec baseline, exact
> proposed delta, named promotion strategy; the hardening-plans checklist only
> if a [DOM-5] risky trigger also fires, otherwise declare hardening N/A and
> state that no risky trigger fires | Class 3 reviews plus independent review
> of the delta before promotion; review-before-implementation when hardening
> applies |

Add this rule after the class table:

> A material theory change adds, removes, or changes product purpose or desired
> feel, a core concept or its owner, a durable principle or non-goal, or a
> revision that changes current design judgment. Link repairs, source
> corrections, metadata edits, and other changes with no behavior change and no
> normative-force change do not trigger class 5 by file location alone.

Add these exact fixture rows:

> | Materially revise a product non-goal or core-concept owner, with no runtime
> behavior change; [DOM-16] requires a material theory change | 5 |
> | Repair a broken program-theory evidence link; no behavior change, no
> normative-force change, and no [DOM-5] trigger fires | 1 |

Update `bin/check-dom15-fixtures` only if its current grammar cannot express
the new class label or rows.

### New [DOM-16] Program Theory and Negative Knowledge

The promoted section will state:

> A program theory is the working explanatory model used to understand and
> change a program coherently. It includes what the program is and is not,
> which concepts exist, what they mean, which component owns each concern, why
> the main boundaries exist, and what evidence would show the model is wrong.
> `docs/program-theory.md` is the repository's current best externalized account
> of that model.
>
> The term follows Peter Naur's “Programming as Theory Building.” It does not
> mean a formal theory, a requirements catalog, an architecture inventory, or a
> design document renamed. It means the working explanatory model that lets a
> maintainer connect the problem world to the program: explain why the system
> has this shape, predict how a change will propagate, diagnose a surprise,
> distinguish an extension from a category error, and revise the solution
> without losing its coherence.
>
> Theory owns the problem-world model, concept meanings, ownership boundaries,
> and conceptual constraints that guide realization. Implementation documents
> own concrete architectural and mechanical choices and why the current
> realization chose them. A theory principle may constrain an architecture
> without becoming the architecture record.
>
> Code, specifications, tests, plans, and implementation documents are
> expressions of and evidence about that model. None is the whole theory. A
> program-theory document is therefore the current best externalized account
> and a transfer surface, not a claim that tacit working understanding has been
> completely serialized.
>
> In this repository, agent-theory is the wager that a human and agents can
> iteratively reconstruct, challenge, and refine a sufficiently shared theory
> by keeping intent, code, tests, alternatives, and implementation surprises in
> contact. Reading the theory document is a starting condition for judgment,
> not proof that the reader possesses the theory.
>
> It is provisional:
>
> ```text
> concept → provisional theory → specification → implementation
>         → evidence or surprise → revised theory
> ```
>
> Owner: the human product owner approves product identity, concept meaning,
> durable principles, and non-goals. Agents recover evidence, draft language,
> challenge inconsistencies, and propose revisions. They do not infer intent
> from current code or feature absence.
>
> Boundary: theory owns conceptual identity and judgment. Winning product
> contracts own exact behavior. Implementation docs own realization rationale.
> Plans own work in flight.
>
> Verification: structural gates check required metadata, sections, recognized
> record syntax, stable references, links, and read order. Dogfood and owner
> review judge meaning. Concrete consequences belong in the winning product
> contract and receive firing tests there.
>
> Required action: before materially changing a concept, boundary, principle,
> or non-goal, read the theory and either conform or propose a class-5 revision.
> Record the current account first, then the superseded account in summary,
> pressure, and evidence.

The required theory sections are:

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
strings remain inside `tests/test_program_theory_contract.py` and are passed
directly to parser helpers, not discovered as repository records. The
provenance token is one of the four closed values shown above. Mixed provenance
uses separate evidence rows, never a compound token.

Revision records are current-account-first to reduce anchoring. Historical
sources are evidence, not startup assignments. A dedicated lineage section may
quote a few short founding phrases only when each is paired with explicit
`Maintained` and `Evolved` analysis and a statement that the current theory
governs. Do not reproduce the original README or obsolete theory at length.

When work touches a recorded boundary, the proposer and reviewer must search
the governing theory/spec/implementation artifact for relevant `[ALT-*]`
records. A fired `Reconsider when` condition reopens review; it does not adopt
the old candidate. The proposal cites the old ID, presents new evidence, gains
owner approval, and updates the old disposition or records its successor.

Active plans keep genuine alternatives append-only. Before closure, durable
content is copied to its steady-state owner under a new owner-scoped ID. The
plan record adds `Promoted to`; the steady-state record adds `Source record`.
The two records link reciprocally. The closed plan remains immutable historical
evidence; the steady-state record alone is current authority:

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
the live-plan form. The physical-deletion gate must update and verify these
cues before removing the plan; `bin/coalesce-check` remains the retrieval
evidence when the cited history is available locally.

## Proposed `docs/program-theory.md`

The document begins with an operational preface:

```text
Status: Active
Owner: SimpleBroker product owner
Boundary: Conceptual identity and design judgment, not exact current behavior.
Verification: Structural contract plus owner and independent semantic review.
Required action: Read before product-scope or design decisions; conform or
propose an explicit class-5 theory revision.
Governing process: [DOM-16]
```

It uses `[THEORY-*]`, not `[SB-*]`. `[SB-*]` remains reserved for canonical
product-contract sections.

### What “program theory” means [THEORY-0]

> “Theory” here is adapted from Peter Naur's “Programming as Theory Building.”
> It is not a formal theory or another name for requirements. It is the working
> explanatory model that connects the problem SimpleBroker addresses to the
> concepts, ownership boundaries, and conceptual constraints used to address
> it. Concrete architectural and mechanical choices, and the rationale for the
> current realization, belong to implementation documentation.
>
> Someone can use the theory when they can explain why the system has this
> shape, predict where a proposed change belongs and what it affects, diagnose
> evidence that does not fit, and revise the program without losing coherence.
> Memorizing this document is not enough.
>
> This file is the current best externalized account and a transfer surface.
> The fuller working theory is reconstructed and challenged through this
> account together with product contracts, implementation rationale, code,
> tests, recorded alternatives, and concrete surprises.

The section includes this concern split:

| Surface | Question it answers |
|---------|---------------------|
| Program theory | What problem and model make the program coherent; why do the concepts and boundaries exist? |
| Winning product contract | What exact behavior is intended now? |
| Implementation rationale | Which concrete architectural and mechanical choices realize the model, and why were they chosen? |
| Code and tests | How is the behavior realized, and what evidence fires against concrete claims? |
| Plans and alternative records | What change or competing account was considered, under which evidence and premises? |

### Purpose and desired feel [THEORY-1]

> SimpleBroker is a durable queue primitive for the space between shell pipes
> and broker platforms. It should feel like a small Unix tool and a matching
> Python capability: easy to start, explicit under failure, composable, and
> usable without operating queue infrastructure.
>
> “Simple” describes the use surface and operational model. Internal
> complexity is justified when it protects a smaller, predictable external
> model and remains locally debuggable.

### Whole-program mental model [THEORY-2]

> Cooperating processes exchange durable messages through named queues on one
> resolved broker target. SimpleBroker owns the semantics and coordination of
> queue operations.
>
> A backend supplies persistence and atomic storage operations. SQLite is the
> local default. Optional services can supply shared storage while owning their
> service topology, replication, consistency/availability mechanisms, and
> recovery. The registered embedding concern and
> README Embedding section (`../README.md#embedding-simplebroker-in-your-project`
> from the target theory file) own those current capability statements.
>
> The application owns message meaning, task execution, business retries,
> worker topology, orchestration, and business-level completion. SimpleBroker
> supplies queue primitives; it does not become the application runtime.

### Core concepts and ownership [THEORY-3]

The theory defines concepts without freezing exact behavior. T6 adds this exact
residual registry row:

| Concern | State | Spec section | README anchor / locus | Gate |
|---------|-------|--------------|-----------------------|------|
| Base queue/broker operation catalog residual | `readme-only` | `-` | README Command Reference / Python API | `(future)` |

The row's required boundary note says it owns only the remaining command/API
catalog and base operation meanings. It explicitly excludes CLI I/O and exits;
delivery, claim, peek, and watch safety; message identity and move/checkpoint
rules; dump/load; and embedding, backends, and sidecar. Every exclusion remains
with its existing registry row. No contract is migrated or changed. Link paths
below are relative to the target `docs/program-theory.md`; T5 exempts this
embedded draft from current path resolution and T6 verifies the same paths in
the destination file.

| Concept | Conceptual meaning | Owner | Exact current contract owner |
|---------|--------------------|-------|------------------------------|
| Broker target | One resolved queue namespace and backend configuration | SimpleBroker for resolution; backend for substrate | Registry `Embedding targets, backends, sidecar` → README Embedding (`../README.md#embedding-simplebroker-in-your-project`) |
| Queue | Named durable message collection and operation surface | SimpleBroker | Registry `Base queue/broker operation catalog residual` → README Python API (`../README.md#python-api`) and Command Reference (`../README.md#command-reference`) |
| Message identity | Identity used to preserve and select queue messages | SimpleBroker | Registry `Message identity (hybrid ts, last_ts, move+checkpoint)` → README Core Concepts (`../README.md#core-concepts`) |
| Claim | Delivery-state transition distinct from proof of application completion | SimpleBroker | Registry `Delivery guarantees, claim/peek/watch safety` → `[SB-DELIVERY-*]` (`specs/11-delivery.md`) |
| Move | Queue-level reservation or routing primitive | SimpleBroker | Registry `Base queue/broker operation catalog residual` → README Command Reference (`../README.md#command-reference`); identity consequences remain with the registered identity row |
| Watcher/waiter | Adapter from queue activity to bounded waiting or consumption | SimpleBroker | Registry `Delivery guarantees, claim/peek/watch safety` → `[SB-DELIVERY-*]` (`specs/11-delivery.md`); modes remain README Real-time Queue Watching (`../README.md#real-time-queue-watching`) |
| Process session | Process-local owner of reusable backend resources | SimpleBroker | Registry `Embedding targets, backends, sidecar` → README Embedding (`../README.md#embedding-simplebroker-in-your-project`); rationale in `implementation/06-process-session-core-ownership.md` |
| Broker core | Queue-operation protocol and shared semantics over one resolved target | SimpleBroker | Registry `Base queue/broker operation catalog residual` → README Python API (`../README.md#python-api`); specialized identity and delivery contracts remain with their registered rows |
| Backend adapter/runner | Storage-specific atomic realization and substrate-resource ownership | Backend implementation | Registry `Embedding targets, backends, sidecar` → README Advanced Extensions (`../README.md#advanced-custom-extensions`) |

### Design principles [THEORY-4]

Each principle states a conceptual consequence and links exact consequences to
their winning contract:

| Principle | Design consequence | Current contract |
|-----------|--------------------|------------------|
| Local-first, infrastructure-optional | The default remains operationally small; optional substrates may widen topology without redefining the core product. | Registry `Embedding targets, backends, sidecar` → README Embedding (`../README.md#embedding-simplebroker-in-your-project`) |
| Unix composability | CLI decisions protect composition and truthful machine use. | Registry `CLI exit codes and CLI I/O contract` → `[SB-CLI-*]` (`specs/10-cli.md`) |
| Matching queue semantics across surfaces | CLI and Python express one queue model even when packaging differs. | Registry `Base queue/broker operation catalog residual` → README Command Reference (`../README.md#command-reference`) and Python API (`../README.md#python-api`) |
| Queue semantics, not application execution | Reusable queue primitives belong here; business workflows and task interpretation belong to consumers such as Weft. | Registry `Base queue/broker operation catalog residual` → README Python API (`../README.md#python-api`); consumer boundary remains conceptual theory |
| Explicit safety over magical recovery | Guarantees are named narrowly enough that convenience cannot imply stronger recovery than exists. | Registry `Delivery guarantees, claim/peek/watch safety` → `[SB-DELIVERY-*]` (`specs/11-delivery.md`) |
| Small concept count over small source count | Cohesive code may be large when splitting would obscure ownership or failure order. New frameworks and parallel paths need stronger cause. | `implementation/00-implementation-index.md` and the owning implementation document |
| Concrete pressure justifies growth | A use case, bug, or invariant supports new concepts; speculative platform growth does not. | Proposed [DOM-16], active plan evidence, and `[REV-*]` records |

### What SimpleBroker is not [THEORY-5]

> SimpleBroker is not a broker fleet, managed queue service, replicated event
> stream, pub/sub platform, distributed task framework, application
> orchestration system, or distributed control plane. It does not own cluster
> membership, leader election, storage partitioning/replication, execution
> routing, or application task semantics.
>
> This is an ownership boundary, not a claim that SimpleBroker avoids
> distributed-systems problems or can only be used on one host. Cooperating
> processes are a distributed system. Optional shared backends may support
> clients on multiple hosts. SimpleBroker still owns queue-operation semantics;
> the backend owns its distributed substrate; the application owns work
> execution.

### Tensions and falsifiers [THEORY-6]

- “Simple at the use surface” has replaced source-line count as the useful
  simplicity test. Whether the advanced surface remains progressive is a live
  tension.
- Optional networked backends widen operational reach without changing the
  local-first identity. Documentation that collapses those modes would
  challenge the account.
- Repeated consumer use of private internals is evidence that a public queue
  primitive or ownership boundary may be missing.
- Repeated proposals requiring daemon ownership, membership, replication,
  application task models, or hidden workflow state challenge either the
  proposal's placement or the theory itself.
- Failures that cannot be diagnosed from the owning queue operation are
  evidence that decomposition or public error semantics need revision.

### Founding continuity and evolution [THEORY-7]

This section begins:

> `[THEORY-1]` through `[THEORY-6]` are the current account and govern design
> judgment. The quotations below are historical evidence from the initial
> README at `f1bd821640d2f51006eec321b21d5341b0175cdc`. They are not independent
> requirements. Each is included only to show what the current theory
> maintained and how it evolved.

Use at most four short quotations, no more than 20 words each or 60 quoted words
in total. Present the current principle before the historical phrase:

| Current principle | Founding evidence | Maintained | Evolved |
|-------------------|-------------------|------------|---------|
| Simple at the use surface; internal complexity must remain coherent and debuggable. | “simple enough to understand in an afternoon, yet powerful enough for real work” | Low cognitive and operational burden remain design goals. | The useful unit is now the public model and local ownership, not comprehension of every source line. |
| Unix composability and queue semantics remain central. | “do one thing well” | Pipes, scripts, explicit failure, and a focused queue role remain. | The product gained a Python embedding surface and optional backends without taking ownership of application execution. |
| SimpleBroker is a queue primitive, not a broker platform. | “It's not trying to replace RabbitMQ or Redis” | It still does not own broker-cluster infrastructure, pub/sub, or an application control plane. | Redis can now serve as an optional backend, and SimpleBroker explicitly handles distributed-systems problems and cross-host queue coordination without becoming that platform. |
| Small concept count matters more than small source count. | “the entire codebase should stay under 1000 lines” | Simplicity remains a hard design constraint. | A line ceiling was rejected because cohesive concurrency, lifecycle, and backend code can protect a simpler external model. |

The implementation may tighten the wording after T2 source verification and
owner review, but it may not add a quote without both `Maintained` and `Evolved`
analysis.

### Revisions and decision cases [THEORY-8]

The initial ledger is seeded only after T2 evidence recovery. Candidate
revisions are:

- `[REV-THEORY-001]` simplicity: small source count to simple use surface
- `[REV-THEORY-002]` topology: local default plus optional shared substrates
- `[REV-THEORY-003]` delivery: queue delivery state versus application
  completion
- `[REV-THEORY-004]` embedding: queue objects plus process-local resource
  ownership
- `[REV-THEORY-005]` cross-thread lifecycle: thread-affine suspended
  transactions

Each becomes a full `[REV-*]` record with one direct source per provenance row.
Owner-recalled and contemporaneous evidence remain separate. An inferred item
is not promoted as current theory until the owner confirms it. The origin story
is handled in the bounded `[THEORY-7]` lineage section, not as a competing
origin revision.

T2 recovered these exact candidate records. They remain fenced here so this
plan does not define IDs that the final theory document will own:

```markdown
### [REV-THEORY-001] Simplicity moved from line count to coherent use surface

Current account: “Simple” means a small, predictable use surface and concept
set. Cohesive internal complexity is justified when it protects that external
model and remains locally debuggable; source-line count is not the governing
measure.
Supersedes: The founding account tied simplicity partly to afternoon
comprehension of the source and an explicit repository-wide limit of 1,000
lines.
Pressure: The implementation grew through real use, optional backends, and
safety work. Later quality work chose cohesion, debugging locality, and
auditable complexity over mechanical size limits.
Evidence:
- contemporaneous: `README.md` at
  `f1bd821640d2f51006eec321b21d5341b0175cdc`, lines 12 and 239
- contemporaneous: commit
  `8b36b81200d9a09aa7b4710fbca15f389b0ce005`, whose `README.md` diff replaces
  the 1,000-line ceiling with afternoon understandability
- contemporaneous: `docs/agent-context/engineering-principles.md` §14 and
  `docs/plans/2026-07-29-complexity-and-state-machine-hardening-plan.md`,
  “Premises and Decisions,” at
  `2daa2fb48dd478fee5c01bec86add53793d55940`
- owner-recalled: the approved current account in this plan, proposed
  `[THEORY-1]` and `[THEORY-4]`

### [REV-THEORY-002] Topology widened without becoming a broker platform

Current account: SimpleBroker remains SQLite-first and
infrastructure-optional. Optional PostgreSQL and Redis/Valkey substrates may
coordinate queue operations for clients on multiple hosts. SimpleBroker owns
queue-operation semantics; the backend owns service topology, replication,
consistency, availability, and recovery; the application owns work execution.
Supersedes: The founding account described a zero-configuration SQLite queue
and excluded being a distributed message broker. A later README summary
compressed that boundary into the overbroad phrase “Not for: Distributed
systems.”
Pressure: PostgreSQL and Redis backends shipped, and current guidance directs
multi-host users to them. Host count can no longer express the product
boundary; ownership does.
Evidence:
- contemporaneous: `README.md` at
  `f1bd821640d2f51006eec321b21d5341b0175cdc`, lines 3, 16–20, and 154–177
- contemporaneous: `CHANGELOG.md` sections `3.0.0` and `3.7.0` at
  `2daa2fb48dd478fee5c01bec86add53793d55940`; PostgreSQL entered in
  `17d15f4213c74c32e500443f902a674f33728538` and Redis 1.0 in
  `f95594a18910e4d9a52e9c6d1cd6ac4889130c7e`
- contemporaneous: current `README.md`, “Shared storage warning” and
  “Advanced: First-Party Backend Extensions,” at
  `2daa2fb48dd478fee5c01bec86add53793d55940`
- contemporaneous: source-pinned
  `197629e2c46edd755c66b272d387c08e984bf32b:docs/plans/2026-05-14-simplebroker-redis-second-backend-plan.md`,
  “Non-Negotiables” and “Target Semantics”
- owner-recalled: the product-owner boundary recorded in this plan, Settled
  Decisions 7–8

### [REV-THEORY-003] Delivery state is not application completion

Current account: SimpleBroker owns queue-level delivery transitions and names
their failure windows. A committed claim prevents another normal delivery of
that claim, but does not prove successful application processing. Retryable
work requires an explicit reservation or transactional-generator pattern.
Supersedes: The founding account said messages were “delivered exactly once
using atomic DELETE operations” without distinguishing broker delivery from
successful application work.
Pressure: Claim-based deletion, watchers, concurrent consumers, move
reservation, and retry-on-stop generators exposed distinct loss and duplicate
windows. The unqualified wording could be read as crash-safe exactly-once
processing.
Evidence:
- contemporaneous: `README.md` at
  `f1bd821640d2f51006eec321b21d5341b0175cdc`, line 197
- contemporaneous: source-pinned
  `197629e2c46edd755c66b272d387c08e984bf32b:docs/plans/2026-07-28-delivery-contract-spec-promotion-plan.md`,
  §§1, 3.2, and 6
- contemporaneous: `docs/specs/11-delivery.md`, “Delivery risk
  model” and `[SB-DELIVERY-1]` through `[SB-DELIVERY-5]`, at
  `2daa2fb48dd478fee5c01bec86add53793d55940`
- contemporaneous: `CHANGELOG.md` `5.6.1` “Documented” entry at
  `2daa2fb48dd478fee5c01bec86add53793d55940`

### [REV-THEORY-004] Queue handles do not each own a backend stack

Current account: A `Queue` is a named handle over one resolved broker target,
not ownership of an independent backend stack. Embedded applications should
resolve that target behind a small client or context. Persistent same-target
handles share process-local session resources while preserving thread,
process, target, and caller-injected-runner isolation.
Supersedes: Each persistent `Queue` previously owned an independent
`DBConnection`, allowing queue count to scale backend runner and pool
allocation.
Pressure: Weft’s multi-queue watcher naturally created one persistent handle
per queue. With PostgreSQL, queue fan-out became pool fan-out and could exhaust
server connection limits.
Evidence:
- contemporaneous: source-pinned
  `197629e2c46edd755c66b272d387c08e984bf32b:docs/plans/2026-05-04-process-local-broker-session-plan.md`,
  “Purpose,” “Ownership lock,” “Thread lock,” and “Current runtime map”
- contemporaneous: `CHANGELOG.md` section `3.3.0` at
  `2daa2fb48dd478fee5c01bec86add53793d55940`; implementation commit
  `9d455e7830eb77a985cbbf0b5ae7dd50811431ff`
- contemporaneous: current `README.md`, “Embedding SimpleBroker in Your
  Project” and the process-session paragraph under backend extensions, at
  `2daa2fb48dd478fee5c01bec86add53793d55940`
- contemporaneous:
  `docs/implementation/06-process-session-core-ownership.md` at
  `2daa2fb48dd478fee5c01bec86add53793d55940`

### [REV-THEORY-005] Suspended transactions retain thread ownership

Current account: Transactional generators and sidecar sessions retain
thread-affine lock and transaction ownership while suspended. Foreign-thread
finalization cannot safely transfer or heal that ownership in process. The
violation is made visible and bounded by permanently poisoning the affected
SQL core; recovery is process restart.
Supersedes: Earlier recovery designs assumed foreign finalization could
self-heal or that `close()` could safely perform in-process teardown. Before
the fix, the violation could instead leave a silent permanent wedge.
Pressure: A concrete generator-finalization bug showed that the owner’s
`RLock`, thread-local transaction, runner resources, and process-session lease
could not be released coherently from another thread. Repeated review rejected
in-process recovery.
Evidence:
- contemporaneous: source-pinned
  `197629e2c46edd755c66b272d387c08e984bf32b:docs/plans/2026-07-27-cross-thread-generator-orphan-healing-plan.md`,
  “Goal,” “The two wedge surfaces,” and round-4 recovery findings
- contemporaneous:
  `docs/implementation/04-cross-thread-finalization-poisoning.md`, “A permanent
  latch, not in-process recovery,” at
  `2daa2fb48dd478fee5c01bec86add53793d55940`
- contemporaneous: `docs/specs/11-delivery.md` `[SB-DELIVERY-6]` at
  `2daa2fb48dd478fee5c01bec86add53793d55940`
- contemporaneous: `CHANGELOG.md` section `5.5.0` at
  `2daa2fb48dd478fee5c01bec86add53793d55940`; implementation commit
  `9d03e77d258127acfff4352435251e892daa8493`
```

T2 deliberately leaves two absent claims unstated: the original author's full
intent behind “distributed” is not recoverable from the text, and the absence
of a Python API in the first README is not evidence that embedding was
considered and rejected.

#### T2 quote manifest

Source:
`f1bd821640d2f51006eec321b21d5341b0175cdc:README.md`
(initial commit, 2025-07-06).

| Exact contiguous quote | Heading/locus | Line | Words |
|------------------------|---------------|-----:|------:|
| `simple enough to understand in an afternoon, yet powerful enough for real work` | `# SimpleBroker`, introductory paragraph | 12 | 13 |
| `do one thing well` | `## Design Philosophy` | 166 | 4 |
| `It's not trying to replace RabbitMQ or Redis` | `## Design Philosophy` | 166 | 8 |
| `the entire codebase should stay under 1000 lines` | `## Contributing`, item 1 | 239 | 8 |

All four are exact contiguous substrings: 33 words total, within the 20-word
per-quote and 60-word aggregate bounds.

## Initial Durable Alternatives

These records are part of the plan's negative knowledge. Their planned
steady-state owners are explicit:

| Plan record | Steady-state record | Current authority after closure |
|-------------|---------------------|---------------------------------|
| `[ALT-PT20260729-001]` | `[ALT-IMPL01-001]` | `docs/implementation/01-documentation-system.md` |
| `[ALT-PT20260729-002]` | `[ALT-IMPL01-002]` | `docs/implementation/01-documentation-system.md` |
| `[ALT-PT20260729-003]` | `[ALT-IMPL01-003]` | `docs/implementation/01-documentation-system.md` |
| `[ALT-PT20260729-004]` | none | This immutable plan; historical-integrity choice only |
| `[ALT-PT20260729-005]` | `[ALT-THEORY-001]` | `docs/program-theory.md` |
| `[ALT-PT20260729-006]` | none | This immutable plan; sequencing choice only |

### [ALT-PT20260729-001] Leave theory only in the root README

Disposition: rejected
Owner: SimpleBroker product owner
Governs: proposed [DOM-3] and [THEORY-1]
Source record: none
Candidate: Keep the root README as the only program-theory artifact.
Why plausible: It was the founding theory artifact and remains the main human
product entry.
Evidence:
- contemporaneous: root `README.md` at baseline `2daa2fb48dd478fee5c01bec86add53793d55940`
Reason: It now mixes theory, catalog, examples, and exact `readme-only`
behavior, and is not guaranteed early repository-agent context.
Current consequence: Keep the README as human expression and behavior owner
where registered, but give conceptual theory one concise owner.
Reconsider when: The README becomes concise, stable, and mandatory startup
context without losing its product-document role.
Promoted to: [ALT-IMPL01-001]

### [ALT-PT20260729-002] Put theory in an ordinary behavior spec

Disposition: rejected
Owner: Process-spec owner
Governs: proposed [DOM-16]
Source record: none
Candidate: Store program theory as an ordinary canonical product spec.
Why plausible: Specs already have stable references and own intended behavior.
Evidence:
- contemporaneous: `docs/specs/01-development-documentation-operating-model.md` at baseline `2daa2fb48dd478fee5c01bec86add53793d55940`
Reason: Identity principles and non-goals require semantic review, while
enumerable behavior requires firing tests. Combining them encourages fake
prose-pinning gates.
Current consequence: Store theory outside `docs/specs/`; link consequences to
the winning behavior owner.
Reconsider when: The spec system gains a conceptual-contract type with honest
non-behavior verification semantics.
Promoted to: [ALT-IMPL01-002]

### [ALT-PT20260729-003] Add a detached alternatives directory

Disposition: rejected
Owner: Process-spec owner
Governs: proposed [DOM-16]
Source record: none
Candidate: Add `docs/alternatives/` or a general ADR graveyard.
Why plausible: Dedicated records are easy to append and preserve.
Evidence:
- contemporaneous: `docs/plans/2026-07-29-program-theory-and-negative-knowledge-plan.md`, Authority Model and proposed [DOM-16]
Reason: Detached records become stale tombstones and compete with current
rationale.
Current consequence: Durable alternatives live beside the theory, contract, or
implementation boundary they constrain.
Reconsider when: A cross-cutting decision cannot be owned or found from any
current governing artifact.
Promoted to: [ALT-IMPL01-003]

### [ALT-PT20260729-004] Freeze the original README as permanent theory

Disposition: rejected
Owner: SimpleBroker product owner
Governs: this plan's historical-integrity boundary
Source record: none
Candidate: Treat the initial README as the permanent account.
Why plausible: It contains the founding concept and desired feel.
Evidence:
- contemporaneous: root `README.md` at commit `f1bd821640d2f51006eec321b21d5341b0175cdc`
- owner-recalled: Settled Decisions 3 and 4 in this plan
Reason: Use cases and bugs materially improved the theory.
Current consequence: Do not add the original README as a parallel document or
origin revision. `[THEORY-7]` may quote only the bounded founding phrases that
are paired with `Maintained` and `Evolved` analysis; other revision records
state the current account first.
Reconsider when: The product owner proposes immutable founding text and supplies
evidence that revisable theory has harmed product judgment.
Promoted to: none

### [ALT-PT20260729-005] Say SimpleBroker is not for distributed systems

Disposition: rejected
Owner: SimpleBroker product owner
Governs: proposed [THEORY-2] and [THEORY-5]
Source record: none
Candidate: Exclude distributed systems or all cross-host coordination.
Why plausible: It sharply distinguishes the project from SQS and large broker
platforms.
Evidence:
- contemporaneous: root `README.md` Embedding SimpleBroker in Your Project and
  `docs/specs/product-section-registry.md` row `Embedding targets, backends,
  sidecar` at `2daa2fb48dd478fee5c01bec86add53793d55940`
- owner-recalled: Settled Decisions 5 and 6 in this plan
Reason: Cooperating processes already present distributed-systems problems, and
shared backends can support cross-host queue coordination. The missing
ownership is broker-platform infrastructure and application execution.
Current consequence: State the boundary in terms of ownership, not host count.
Reconsider when: Product scope intentionally expands to own membership,
replication, routing, or execution orchestration.
Promoted to: [ALT-THEORY-001]

### [ALT-PT20260729-006] Generalize in `../agent-theory` first

Disposition: deferred
Owner: Agent-theory product owner
Governs: this plan's upstream boundary
Source record: none
Candidate: Design and implement the reusable model upstream before adopting it
in SimpleBroker.
Why plausible: The upstream repository distributes the process.
Evidence:
- owner-recalled: Settled Decision 9 in this plan
- contemporaneous: `../agent-theory/README.md` and
  `../agent-theory/docs/specs/01-development-documentation-operating-model.md`
  at `2cfbe8f15d99b94180e3f316baee4d6e03d2880d`
Reason: SimpleBroker supplies the concrete corpus needed to falsify a weak
abstraction.
Current consequence: Dogfood and close this adoption first; produce only an
export brief here.
Reconsider when: SimpleBroker adoption survives review and at least one real or
representative theory-affecting proposal.
Promoted to: none

## Current Structure and Key Files

- `AGENTS.md` sends repository agents into process docs before a concise
  product theory.
- `docs/agent-kernel.md` is a strong product-use view, not a theory owner.
- Root `README.md` owns `readme-only` behavior and carries the evolved public
  product expression, but it is too mixed to be mandatory concise theory.
- `docs/agent-context/decision-hierarchy.md`, `principles.md`,
  `engineering-principles.md`, and `runbooks/writing-specs.md` currently begin
  authority or mental-model traceability at specs. All four must acknowledge
  the new upstream conceptual layer.
- `docs/agent-context/context.index.yaml` is the machine-readable shared-context
  order and currently begins at the decision hierarchy. It must put program
  theory first for repository work.
- The plan, implementation-doc, brainstorming, and coalescing guidance records
  alternatives inconsistently and lacks a reconsideration action.
- `docs/implementation/01-documentation-system.md` still describes a scaffold
  awaiting real product code.

## Invariants and Constraints

- Theory may summarize stable behavior with a precise contract link, but never
  becomes an independent source of that behavior.
- `docs/program-theory.md` is an externalized current account and transfer
  surface. It must not imply that the document, code, or tests alone contain
  the whole working theory.
- Root README remains normative for `readme-only` concerns.
- `[SB-*]` remains the canonical product-contract namespace; theory uses
  `[THEORY-*]`.
- Historical claims have direct evidence or an explicit owner-recalled,
  inferred, or unknown label.
- The original README stays source-pinned in git. It is not copied into a
  parallel artifact or assigned as a separate startup read. The current theory
  may contain only the bounded `[THEORY-7]` quotations, each subordinate to
  current `Maintained` and `Evolved` analysis.
- Revision records put the current account before a short superseded-account
  summary. They do not reproduce obsolete prose at length or require readers
  to reconcile two governing accounts.
- The initial theory stays concise enough for startup reading, with a target of
  at most roughly 250 lines.
- Entry points link; they do not copy theory prose.
- Ordinary class-1/2 work does not gain an empty alternatives form.
- A fired reconsideration condition reopens review. It does not auto-adopt.
- No runtime, API, CLI, packaging, backend, delivery, or consumer behavior
  changes.
- The SimpleBroker and upstream changes remain separate.

## Dependency-Ordered Tasks

### T1: Approve the authority model and draft

Run independent theory-authority, implementation-readiness, and zero-context
reviews. Resolve every P1/P2 finding. Obtain owner approval for purpose,
concepts, principles, non-goals, exact process delta, record grammar, and the
proposed read order.

After approval, change this plan and the Status Index row from `draft` to
`active`. Do not implement while either remains draft.

Done: no unresolved P1/P2 finding and explicit owner approval is recorded.

### T2: Recover bounded provenance

Inspect the initial/current README, CHANGELOG, source-pinned retired plans,
implementation docs, locally available issues/reviews, and owner statements.

For each candidate revision:

1. cite a direct source
2. separate provenance rows
3. mark missing history `unknown`
4. ask the owner to confirm any inference before treating it as current theory
5. omit entries supported only by feature absence
6. verify each proposed `[THEORY-7]` quote as a contiguous verbatim substring
   of the pinned README; record the quote, commit, heading/locus, and source
   line in the Verification Evidence Record as the reviewed quote manifest
7. write each quote's `Maintained` and `Evolved` analysis before admission
8. extract only the delta needed to explain the current account; do not copy
   the original README into a parallel repository artifact

Any change to purpose, concept ownership, principle, non-goal, authority, or
record grammar updates the Revision Log and reruns the affected independent
review plus owner approval. Evidence-link or provenance-label refinements that
do not change meaning need only a fresh-eyes inspection.

Done: every historical claim has a source or honest uncertainty; all recalled
or formerly inferred current-theory claims are owner-confirmed.

### T3: Dogfood the proposed theory before promotion

Create an isolated temporary review bundle with `mktemp -d`; do not commit it.
It contains:

- a candidate `AGENTS.md` routing excerpt with the proposed repository-work
  order and no plan content
- a candidate `docs/agent-context/context.index.yaml` with theory first
- a candidate `docs/program-theory.md` rendered from the T2-reviewed draft
- only the exact winning-contract excerpts needed for the decision exercise

Reconstruction reviewer input is limited to the candidate routing excerpt,
candidate context index, and candidate theory. Decision reviewer input is
limited to the candidate theory and the named winning-contract excerpts. This
plan, the expected answer, prior reviews, and other repository docs are
excluded. The initial README and historical commit are also excluded; the
reviewer sees only the curated `[THEORY-7]` excerpts with their current
analysis. The exercise tests acquisition of the current theory, including
whether the lineage section causes regression to an old boundary.

The owner or an independent facilitator prepares withheld proposals after this
plan is approved. Their exact text is not enumerated here. Collectively they
must test an obvious fit, an obvious ownership violation, and a genuinely
borderline case. Reviewers receive the proposals only in the exercise prompt.

The reconstruction prompt also asks the reviewer to explain what was maintained
and what evolved for two founding quotations, then state whether “not trying to
replace RabbitMQ or Redis” forbids a Redis backend or current cross-host use. A
response that repeats the old platform boundary as a backend or host-count ban
without the current ownership distinction fails the dogfood gate.

The reviewer must also explain, without quoting a definition, what “program
theory” adds beyond a product spec or implementation rationale and why reading
`docs/program-theory.md` is not by itself proof of understanding. The
facilitator supplies one withheld mixed example containing a conceptual
boundary, an observable behavior claim, and a concrete architectural choice;
the reviewer must route them respectively to theory, the winning product
contract, and implementation rationale while explaining their links. A
response that treats theory as a requirements catalog, file inventory,
architecture decision record, or complete serialized artifact fails.

For every exercise, append to the Verification Evidence Record:

- exact prompt
- exact allowed files and excerpts
- unedited output or durable artifact reference
- reviewer identity
- owner evaluation
- disposition and any theory correction

Delete the temporary bundle after its content hash and required evidence have
been recorded. The owner judges semantics; model agreement is transfer
evidence, not product authority.

Stop if reviewers cannot recover the boundary or apply it to the withheld
cases. Revise, reapprove material changes, and repeat before T4.

### T4: Promote the process spec only

Files:

- `docs/specs/01-development-documentation-operating-model.md`
- this plan and `docs/plans/README.md`
- DOM-15 fixtures and `bin/check-dom15-fixtures` only if required

Apply the exact [DOM-3], [DOM-4], [DOM-15], and new [DOM-16] delta with strategy
A. Add the Related Plans backlink. Do not create `docs/program-theory.md`,
entry links, policy-owner edits, or tests in this slice.

Record a promotion baseline identifier before dependent implementation. Use a
commit SHA when the slice is committed; for uncommitted owner review, record
the diff base plus exact worktree-state identifier allowed by the writing-plans
runbook.

Done: process-spec review is reconciled, DOM fixtures and diff checks pass,
and one promoted process contract governs all later work. The docs-path gate
has one intentional three-occurrence red condition until T6 creates
`docs/program-theory.md`; the exact failure is recorded in the Verification
Evidence Record and Deviation Log rather than hidden with a placeholder.

### T5: Add the failing structural contract

Create `tests/test_program_theory_contract.py` with a small private Markdown
parser. Do not add a general documentation framework.

The repository corpus is root `README.md`, `docs/**/*.md`, and
`skills/**/*.md`. Definitions are headings only; malformed fixture strings are
passed directly to helpers and excluded from the corpus scan. The parser
ignores fenced code examples, including the grammar examples in this plan.
Tests check:

1. required theory metadata and section codes, including `[THEORY-0]`
2. program theory first in `docs/agent-context/context.index.yaml` `read_order`
3. persona-specific entry links in the [DOM-3] relative order
4. exact `[ALT-*]` and `[REV-*]` field order, ID grammar, definition
   uniqueness, and closed vocabularies
5. live-plan `Source record` / `Promoted to` reciprocity; source-pinned retired
   records instead match a Retired Plans ledger name and SHA. Direct helper
   fixtures use an actual Retired Plans row to prove that the exact filename
   and SHA pass, a mutated filename fails, a mutated SHA fails, and live and
   retired source forms cannot be substituted for one another. These fixtures
   establish the release-archive-safe ledger contract; the pre-deletion
   historical-heading check belongs to T7 because it requires git history.
6. each core-concept current-contract cell names a registry concern and
   resolves to its precise current owner
7. local links and stable references resolve
8. revision records put `Current account` before `Supersedes`, and no startup
   entry assigns the initial README as a separate read
9. `[THEORY-7]` contains at most four quotations, each at most 20 words and no
   more than 60 quoted words total; every row has non-empty
   `Current principle`, `Maintained`, and `Evolved` cells; normalized quote
   cells exactly equal the private reviewed allowlist copied from T2's quote
   manifest, and the section's source SHA equals the manifest SHA
10. the initial commit hash occurs in startup theory only inside `[THEORY-7]`
    provenance; no origin-revision record, alternative sourced from
    `[ALT-PT20260729-004]`, or language presenting the initial README as
    governing theory is allowed

Tests do not judge concepts or prose. Observe malformed fixtures fail for the
intended field and observe the live repository test fail because the theory,
registry row, and ordered links do not yet exist.

The quote allowlist pins an enumerable historical-source claim, not a
qualitative principle. It contains each exact quote plus its source heading and
line. The test does not call git, because release archives and shallow CI
checkouts may not contain the initial commit; T2's recorded `git show`
inspection establishes provenance, while the allowlist prevents later drift.

Done: the red command, exit code, and relevant assertion are recorded.

### T6: Create theory and align all authority and entry owners

Canonical theory sub-slice:

- `docs/program-theory.md`
- `docs/specs/product-section-registry.md`

Create the reviewed theory using `[THEORY-*]` and `[REV-THEORY-*]`. Add only
`[ALT-THEORY-001]`, sourced from `[ALT-PT20260729-005]`, then update plan
record 005 from `Promoted to: none` to the created ID. Record 004 stays only in
this plan. Add the exact narrow residual `readme-only` registry row defined in
the proposed theory. Every current-contract cell names an exact registry
concern and owner link. Add `[THEORY-0]` before the product-specific account,
and add the reviewed `[THEORY-7]` comparison with its source pin, word bounds,
and current-first analysis. Do not add an origin revision or a theory
alternative sourced from record 004.

Process-authority sub-slice:

- `docs/agent-context/decision-hierarchy.md`
- `docs/agent-context/principles.md`
- `docs/agent-context/engineering-principles.md`
- `docs/agent-context/runbooks/writing-specs.md`

Done: none says conceptual authority or mental-model traceability starts at
specs; all preserve winning contracts as exact behavior owners.

Repository-work entry sub-slice:

- `AGENTS.md`
- `docs/README.md`
- `docs/specs/00-specs-index.md`
- `docs/agent-context/context.index.yaml`
- `docs/agent-context/README.md`

Done: root safety remains first; theory is the machine-readable first
shared-context item; the context hub's hard-coded order is aligned; all entry
prose matches [DOM-3].

Product-use entry sub-slice:

- `docs/agent-kernel.md`
- `llms.txt`
- root `README.md`
- `CHANGELOG.md`

Done: kernel remains first for product use and routes to winning contracts;
README retains `readme-only` authority; the ownership-based
distributed-systems wording replaces the broad exclusion; a top-level
`Unreleased` `Documented` entry records the clarification.

### T7: Standardize the dogfooded negative-knowledge workflow

Files:

- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/writing-implementation-docs.md`
- `docs/agent-context/runbooks/maintaining-traceability.md`
- `docs/agent-context/README.md`
- `skills/brainstorming-to-plan/SKILL.md`
- `skills/coalescing/SKILL.md`

Add the taxonomy, admission threshold, exact record grammar, corpus, ID
allocation rule, reconsideration action, and closure-copy routing.
Product- or architecture-specific rejections do not automatically become
generic lessons. Retirement/coalescing requires explicit disposition without
forcing empty records.

Add the physical-retirement transition: before deleting a plan with promoted
alternatives, coalescing converts every live reciprocal source to the
plan-name + retired-source-SHA + original-ALT-ID form, checks the Retired Plans
ledger, retrieves the recorded source with
`git show <ledger-sha>:docs/plans/<plan-name>.md`, and proves that it contains
the exact `### [ALT-ID]` heading before permitting deletion. A missing
conversion, ledger mismatch, failed retrieval, or missing exact heading blocks
physical deletion.

Add table-driven
`test_negative_knowledge_examples_route_to_one_owner` in
`tests/test_program_theory_contract.py`:

| Fixture | Expected steady-state owner |
|---------|-----------------------------|
| Durable non-goal: SimpleBroker does not own broker-cluster membership | `docs/program-theory.md` |
| Current limitation: a named operation is not currently offered | the product-section registry's winning README/spec owner |
| Rejected backend resource abstraction with a governing implementation reference | that implementation document |
| One plan excludes an adjacent cleanup with no product judgment | that plan and git history only |

The fixture validates the documented routing table and rejects zero or multiple
owners. Run:

```bash
uv run --frozen --no-sync pytest \
  tests/test_program_theory_contract.py::test_negative_knowledge_examples_route_to_one_owner
```

Done: record the command, exit code, four expected/observed owners, and a
read-only reviewer disposition in the Verification Evidence Record. Actual
closure-copy evidence is deferred to T9.

### T8: Align implementation maps and make gates green

Files:

- `docs/implementation/01-documentation-system.md`
- `docs/implementation/02-repository-map.md`
- `docs/implementation/05-product-invariant-inventory.md`
- `docs/implementation/00-implementation-index.md`
- `tests/test_program_theory_contract.py`

Remove stale scaffold language. Explain the theory/spec/implementation/evidence
split. Add the artifact and edit path to maps. Keep the invariant inventory
non-normative.

Create `[ALT-IMPL01-001]` through `[ALT-IMPL01-003]` in
`implementation/01`, each pointing back to its plan record, then update plan
records 001 through 003 from `Promoted to: none` to their created IDs. Do not
copy `[ALT-PT20260729-006]`; it remains a temporary sequencing decision here.

Done: structural, docs-path, DOM-fixture, format, lint, and type gates pass.

### T9: Final review, closure routing, and upstream export brief

Run independent completed-work reviews over authority, provenance,
implementation readiness, zero-context transfer, and the full diff.

Verify every live `Promoted to` / `Source record` pair, every source-pinned
retired cue, and that the steady-state copy alone claims current authority.
Append an upstream export brief:

- project-neutral principles that survived dogfood
- SimpleBroker-specific parts
- rejected generalizations
- dogfood surprises
- open questions for `../agent-theory`

Close the Status Index row only when evidence is complete and the requested
landing commit exists. Open no upstream implementation in this slice.

## Testing Plan

The process contract is promoted in T4 before the failing structural proof is
added in T5. The structural gate can check syntax and navigation only:

- metadata, required sections, and namespaces
- exact `[ALT-*]` and `[REV-*]` shapes
- ID-definition uniqueness, live reciprocal or retired source-pinned promotion
  cues, and closed vocabularies
- local links and stable references
- declared relative read order in prose and `context.index.yaml`
- current-contract cells resolving through the product registry
- DOM-15 classification fixtures

Semantic evidence comes from T2 provenance review, T3 isolated dogfood, owner
approval, and final independent review.

Targeted commands:

```bash
uv run --frozen --no-sync pytest \
  tests/test_program_theory_contract.py \
  tests/test_agent_kernel_contract.py \
  tests/test_delivery_contract_sb_delivery.py \
  tests/test_documented_exit_codes.py

python3 bin/check-dom15-fixtures
bin/check-doc-paths
git diff --check
```

Final code-quality gates for the added test:

```bash
uv run --frozen --no-sync ruff check tests/test_program_theory_contract.py
uv run --frozen --no-sync ruff format --check \
  tests/test_program_theory_contract.py
uv run --frozen --no-sync mypy \
  tests/test_program_theory_contract.py --config-file pyproject.toml
```

Discover and run every existing test that owns an edited entry or policy file:

```bash
rg -n \
  "README|agent-kernel|product-section-registry|llms.txt|decision-hierarchy|writing-specs" \
  tests
```

A full runtime, PostgreSQL, Redis, or Weft suite is unnecessary unless the diff
unexpectedly touches runtime or consumer contracts.

## Rollout and Rollback

Sequence:

1. approve plan
2. recover provenance
3. dogfood the proposed theory in an isolated bundle
4. promote the process spec only
5. add the red structural contract
6. create theory and align authority and entry owners
7. standardize workflow
8. align implementation maps and gates
9. review and close

If the work lands as one squashed commit, rollback is one revert. If it lands as
ordered commits, revert the dependent set in reverse order. Remove entry links
and workflow requirements before or in the same revert that removes the theory
file; revert the process-spec promotion last. No data migration or one-way door
exists.

## Verification Evidence Record

| Slice | Changed files | Command or inspection | Observed result | Review | Residual risk |
|-------|---------------|-----------------------|-----------------|--------|---------------|
| Initial plan authoring | this plan; Status Index | `python3 bin/check-dom15-fixtures`; `bin/check-doc-paths`; `git diff --check` | passed before review revision | three independent reviewers returned blocking/revision findings | Draft required authority, schema, provenance, and task-order correction. |
| Plan activation | this plan; Status Index | Owner approval in the 2026-07-29 session | Approved: “Please implement the plan” | Prior theory/authority, zero-context, and implementation-readiness reviews passed | Plan is active; implementation evidence remains pending. |
| Plan revision | this plan; Status Index | `python3 bin/check-dom15-fixtures`; `bin/check-doc-paths`; `git diff --check`; `bin/coalesce-check` | all gates passed; 0 retrieval cues; 3 foreign and 4 local-only SHA notices are pre-existing evidence-location notes | theory/authority, zero-context, and implementation-readiness reviewers passed the final revision with no P1/P2 findings | Owner approval required before activation. |
| Historical-anchoring revision | this plan | `python3 bin/check-dom15-fixtures`; `bin/check-doc-paths`; `git diff --check` | passed before lineage rebalance | theory/authority and zero-context reviewers passed the exclusion model | Owner then chose a bounded current-first lineage comparison instead of complete exclusion. |
| Bounded-lineage revision | this plan | `git show f1bd821640d2f51006eec321b21d5341b0175cdc:README.md` (lines 12, 166, 239); `python3 bin/check-dom15-fixtures`; `bin/check-doc-paths`; `git diff --check` | four exact quotes verified at 13/4/8/8 words, 33 total; repository gates passed | theory/authority, zero-context, and implementation-readiness reviewers passed with no P1/P2 findings | T2 must record the formal quote manifest before promotion. |
| Program-theory background revision | this plan | `python3 bin/check-dom15-fixtures`; `bin/check-doc-paths`; `git diff --check` | passed | theory/authority, zero-context, and implementation-readiness reviewers passed with no P1/P2 findings | Applied dogfood remains necessary to show that readers acquire the distinction rather than recite it. |
| T3 reconstruction dogfood | isolated bundle files and hashes in T3 evidence below | Fresh zero-context reviewer, exact prompt and unedited output below | passed: reviewer reconstructed the causal model, authority split, current topology boundary, both Maintained/Evolved pairs, and two valid falsifiers | `/root/theory_dogfood_reconstruction`; owner evaluation: pass | The review demonstrates transfer from one model and one prompt, not universal comprehension. |
| T3 decision dogfood | candidate theory plus winning-contract excerpts; hashes below | Fresh zero-context reviewer, exact prompt and unedited output below | partial pass: obvious fit, violation, and mixed routing were correct; the planned Redis/waiter “borderline” case was correctly identified as an obvious fit | `/root/theory_dogfood_decisions`; owner evaluation: pass on theory, revise exercise | The exercise, not the theory, failed to supply a genuinely ambiguous proposal. |
| T3 replacement borderline dogfood | same candidate theory and winning-contract excerpts | Fresh zero-context reviewer, exact prompt and unedited output below | passed: reviewer treated lease/requeue as borderline, separated conceptual, behavioral, and mechanical owners, and named evidence that would resolve it | `/root/theory_dogfood_borderline`; owner evaluation: pass | Semantics still require owner judgment; model agreement is transfer evidence only. |
| T4 process-spec promotion | `docs/specs/01-development-documentation-operating-model.md`; this plan | `python3 bin/check-dom15-fixtures`; `bin/check-doc-paths`; `git diff --check`; spec-diff SHA-256 | DOM fixtures and diff check pass; path gate exits 1 on exactly three references to the intentionally absent `docs/program-theory.md`; promotion baseline recorded as base `2daa2fb...` plus spec-diff `55336fe...` | Reviewed process delta plus focused retirement-lifecycle re-review passed | T6 must create the target and turn the exact path failures green before completion. |
| T5 structural contract red | `tests/test_program_theory_contract.py` | `uv run --frozen --no-sync ruff format tests/test_program_theory_contract.py`; `uv run --frozen --no-sync ruff check tests/test_program_theory_contract.py`; `uv run --frozen --no-sync pytest tests/test_program_theory_contract.py -q` | format and lint pass; 3 helper tests pass; repository contract fails at `assert THEORY.exists()` with `docs/program-theory.md must be created after DOM-16 promotion` | author observed exact red after correcting an initial unsanctioned PyYAML dependency/import error | T6 must make the repository contract green without weakening parser fixtures. |
| T6–T8 implementation and final local gates | theory, registry, entry paths, policy/runbooks/skills, implementation maps, plan links, structural test | full `uv run --frozen --no-sync pytest -q`; exact CI Ruff check/format; exact CI mypy core plus PG/Redis extension commands; DOM fixtures; docs paths; coalesce check; diff check | full suite passed with 17 documented skips; Ruff check passed and 442 files formatted; mypy passed 60 core, 29 PG, and 27 Redis source files; DOM/path/diff gates passed; coalesce-check reported 12 SHA claims, 3 foreign, 4 pre-existing local-only pins, 0 retrieval cues, all cues locally resolved | completed-work authority and structural mutation reviews passed after corrections; Claude other-family fallback returned PASS | Grok provider was unavailable before execution; no runtime/backend service suites were required because runtime behavior did not change. |

### T3 isolated dogfood evidence

The temporary bundle was `.dogfood-theory.3kXvYr/`. It excluded this plan,
repository history, the initial README, prior reviews, and all repository files
except the copied candidate material. Input hashes:

| Input | SHA-256 |
|-------|---------|
| `AGENTS-routing.md` | `87cd0c77c9ef65aaeba7705555a948323bc6068342c190413b55e592bba160b8` |
| `context.index.yaml` | `a2f1c551102b0257138c39678a9fb9d02107f3685baa2df19d29eeb7af89e00d` |
| `program-theory.md` | `7a214833f167f7e4d2fe31b7103ed2b2a9e380c82e7bae670fba30bc003fd7f7` |
| `winning-contract-excerpts.md` | `cede876966df1a91078b7b79110e0a126e99862bd9de280e625bdf4401987188` |

#### Reconstruction exercise

Exact prompt:

```text
You are an isolated zero-context reviewer. Work read-only. You may inspect ONLY
these three files under the temporary bundle: AGENTS-routing.md,
context.index.yaml, program-theory.md. Do not inspect the repository, git
history, sibling files, or infer an answer from filenames beyond those inputs.

From those inputs, produce a concise but substantive reconstruction:
1. Explain in your own words what “program theory” adds beyond a product
   specification or implementation rationale, and why reading program-theory.md
   is not proof that you possess the theory.
2. State SimpleBroker’s purpose, desired feel, whole-program model, and the
   ownership split among SimpleBroker, a backend, and an embedding application.
3. Explain what was maintained and what evolved for these two founding phrases:
   “It's not trying to replace RabbitMQ or Redis” and “the entire codebase should
   stay under 1000 lines.”
4. Answer directly: does the first phrase forbid a Redis backend or current
   cross-host use? Explain the current boundary.
5. Name two concrete observations that would count as evidence against the
   current theory.
Do not quote a definition as your answer. Identify ambiguities or tensions
rather than smoothing them over. Return only your review response.
```

Unedited output:

```text
Program theory adds the causal model behind the product: what problem it
solves, why its concepts exist, where ownership boundaries fall, and how to
judge changes without losing coherence. A product specification owns exact
current behavior; implementation rationale owns concrete mechanisms and their
tradeoffs. Reading the theory is not proof of possessing it. Possession is
demonstrated by using it to place a proposed change, predict its effects,
diagnose conflicting evidence, and revise the system coherently. The document
is only a transfer surface; the fuller theory must be tested against contracts,
code, tests, alternatives, and surprises.

SimpleBroker’s purpose is to provide a durable queue primitive between shell
pipes and full broker platforms. It should feel like a small Unix tool with a
matching Python capability: quick to start, composable, explicit on failure,
and usable without queue infrastructure. The whole-program model is
cooperating processes exchanging durable messages through named queues on one
resolved broker target. SimpleBroker owns queue semantics, target resolution,
and operation coordination. The backend owns persistence, atomic storage
operations, resources, and, for a shared service, topology, replication,
consistency/availability, and recovery. The embedding application owns message
meaning, execution, business retries, worker topology, orchestration, and
business completion.

For “It’s not trying to replace RabbitMQ or Redis,” the maintained claim is
about product role: SimpleBroker still does not own a broker fleet, pub/sub
platform, replicated stream, application runtime, or distributed control
plane. What evolved is substrate support and reach. Redis may now be an
optional backend, and clients may coordinate across hosts, while SimpleBroker
still stops at queue-operation semantics.

For “the entire codebase should stay under 1000 lines,” the maintained
constraint is simplicity and low cognitive and operational burden. What
evolved is the metric. A source-line ceiling was rejected in favor of a small
public concept count, clear local ownership, and debuggable failure order.
More cohesive lifecycle, concurrency, or backend code is acceptable when it
protects a simpler external model.

No, the first phrase does not forbid either a Redis backend or current
cross-host use. The boundary is ownership, not process location or backend
brand. One resolved broker target can be a shared network service used from
multiple hosts. SimpleBroker owns the queue contract over that target; Redis
or another backend owns the distributed storage substrate; the application
owns execution. There is still a real tension: “SimpleBroker handles
distributed-systems problems” can blur into backend responsibilities. The
proposed dividing line is queue coordination versus substrate topology and
recovery, but edge cases will require the winning product contract and
implementation evidence. This theory alone does not establish every exact
supported mode or guarantee.

Two observations that would count against the current theory are:

- Multiple consumers repeatedly importing private claim, move, or watcher
  internals because no public queue primitive can express a common need. That
  would suggest the claimed public concept set or ownership boundary is
  incomplete.
- Cross-host queue correctness repeatedly requiring SimpleBroker itself to
  manage membership, leader election, replication, or hidden workflow state.
  That would contradict the claim that those concerns belong to the backend or
  application rather than the queue primitive.
```

#### Decision and mixed-routing exercise

Exact prompt:

```text
You are an isolated zero-context decision reviewer. Work read-only. You may
inspect ONLY program-theory.md and winning-contract-excerpts.md in the
temporary bundle. Do not inspect the repository, git history, sibling files,
or any plan.

Evaluate these withheld proposals using the current theory. For each, say FIT,
OWNERSHIP VIOLATION, or BORDERLINE/REVISE; identify which owner and
documentary surface should govern the next decision; and explain the smallest
coherent version if one exists.

A. Add an atomic purge(queue) queue primitive to both the Python and CLI
surfaces, with explicit count/exit behavior.
B. Add a SimpleBroker daemon that registers Python task types, elects a leader,
routes tasks to worker hosts, retries failed business tasks, and reports
workflow completion.
C. Permit cross-host clients through an optional Redis backend and add a
bounded queue-activity waiter, while leaving Redis replication/failover and
worker execution to other systems.

Then route this mixed proposal into exactly three concerns, with links between
them but one primary owner each:
“After user code rejects a message three times, automatically move it to a
dead-letter queue, implemented by a new WorkflowCoordinator that stores retry
policy in SQLite.”
- conceptual boundary
- observable behavior claim
- concrete architectural choice

Finally state what evidence or unresolved question makes C genuinely
borderline, if any. Do not merely restate document labels. Identify
counterarguments and do not assume every useful feature belongs in
SimpleBroker. Return only your review response.
```

Unedited output:

```text
A. Atomic purge(queue): FIT

This is a queue-level operation on one named queue and one resolved broker
target. It does not interpret message meaning or execute application work.
Atomicity is especially plausible as a SimpleBroker responsibility because a
client-side drain loop cannot provide the same race-free behavior.

Primary ownership belongs to the base queue/broker operation catalog in the
root README. The Python return value and command availability belong there.
Exact CLI count encoding, stdout/stderr use, and exit statuses belong to
docs/specs/10-cli.md. Backend-specific atomic mechanics belong in
implementation rationale, not theory.

The smallest coherent version is queue-local and backend-neutral: atomically
remove the messages covered by a precisely defined queue state, return the
removed count in Python, and expose the same result through machine-readable
CLI output with explicit failure exits. It should not grow into target-wide
cleanup, retention policy, or a background janitor. The contract must settle
whether claimed or reserved messages are included and what concurrent enqueue
sees.

Counterargument: purge is destructive and could enlarge the public surface for
a convenience operation. That is real. Concrete use pressure is still needed.
But if callers need atomic emptying, composing existing primitives is
semantically inadequate.

B. Integrated task daemon: OWNERSHIP VIOLATION

The proposal combines nearly every responsibility the theory assigns
elsewhere: Python task types and business retries belong to the application;
worker-host routing and workflow completion belong to the application runtime;
leader election and host membership belong to a distributed control plane.
Calling the result a daemon does not turn these into queue semantics.

The next decision belongs first to the program-theory owner on the
program-theory surface. Under the current theory, the decision is to keep this
outside SimpleBroker. Admitting it would require an explicit theory revision,
not a routine product-contract addition.

The smallest coherent version is an external task-runtime or workflow package
built on SimpleBroker. SimpleBroker may add independently useful queue
primitives that this runtime proves are missing, such as reservation, move, or
bounded waiting, but it should not register task meanings, elect runtime
leaders, route execution, retry business work, or declare workflow completion.

The strongest counterargument is that an integrated daemon could make the
user-facing system much simpler, and internal complexity is allowed when it
protects a small surface. That does not overcome the ownership problem: this
daemon introduces a new application-runtime model and distributed control
plane, not merely hidden machinery supporting an existing queue concept.

C. Optional Redis plus bounded queue waiter: FIT

Both halves preserve the current model. Redis widens the storage topology
while SimpleBroker still owns queue-operation semantics; Redis deployment,
replication, failover, and recovery remain outside it. A bounded waiter adapts
queue activity into waiting without claiming that application work completed.

The Redis decision belongs to the owner of “Embedding targets, backends,
sidecar” in the root README. The waiter’s exact timeout, wakeup, error, and
delivery-safety behavior belongs to docs/specs/11-delivery.md.
Backend mechanics belong in implementation rationale.

The smallest coherent version is one resolved Redis-backed queue namespace
plus a waiter that terminates on a documented queue event, timeout, or explicit
backend error. It must not imply worker discovery, execution routing,
end-to-end workflow completion, or liveness across Redis failover.

The counterargument is that cross-host access and blocking waits can become
the thin end of a managed broker platform. The stated boundaries answer that
objection. Redis topology remains someone else’s problem, and the waiter
remains bounded and queue-scoped.

C is not genuinely borderline at the conceptual level as written. The current
theory expressly allows optional shared backends and names waiter behavior as
a SimpleBroker concept. The unresolved question is the waiter’s exact promise.
Evidence that it only observes bounded queue activity on one target, with
explicit timeout and backend-failure windows, confirms fit. A contract or test
implying worker completion, durable notification across outages, automatic
failover, or hidden coordination would change the classification. Lack of
concrete use pressure could also justify declining the feature, but that is a
prioritization and concept-growth issue, not an ownership ambiguity.

Mixed dead-letter proposal: exactly three concerns

1. Conceptual boundary: who owns “user code rejected this three times.”
   Primary owner: the SimpleBroker product owner. Documentary surface: program
   theory. This is business retry policy because it interprets an application
   outcome and counts application-level failures. Current theory assigns it to
   the application. That decision constrains the behavior contract below;
   neither an automatic move nor a SQLite implementation can silently reverse
   it.

2. Observable behavior claim: after the third rejection, move the message to
   a dead-letter queue. Primary owner: the delivery-contract owner.
   Documentary surface: docs/specs/11-delivery.md. If proposed as
   SimpleBroker behavior, the contract would need to define “rejection,”
   attempt identity and scope, atomicity of the move, crash windows, duplicate
   effects, and what happens under concurrent consumers. But the conceptual
   ruling currently prevents this from becoming an automatic SimpleBroker
   business-retry guarantee. The coherent product-level remainder is a generic
   reservation/move primitive with exact delivery effects; the application
   invokes it according to its own three-attempt policy.

3. Concrete architectural choice: WorkflowCoordinator stores retry policy in
   SQLite. Primary owner: implementation-rationale owner. Documentary surface:
   implementation documentation. This choice is subordinate to both earlier
   decisions. It cannot justify the behavior or change the ownership boundary.
   It also creates a backend mismatch: retry policy hidden in local SQLite
   would not naturally follow a Redis or PostgreSQL broker target. The smallest
   coherent architecture is either an application-owned coordinator and policy
   store, or no coordinator at all inside SimpleBroker, with SimpleBroker
   supplying only backend-neutral queue primitives.
```

Disposition: the current theory passed. The exercise was corrected rather
than forcing an expected label onto C.

#### Replacement borderline exercise

Exact prompt:

```text
You are an isolated zero-context decision reviewer. Work read-only. You may
inspect ONLY program-theory.md and winning-contract-excerpts.md in the
temporary bundle. Do not inspect the repository, git history, sibling files,
or any plan.

Evaluate this proposal using the current theory:

“Add a queue-local lease operation. Claiming a message makes it invisible for
a caller-selected duration. The message becomes visible again when the lease
expires unless the caller acknowledges it. There are no task types, worker
registration, business retry counters, or workflow-completion claims. SQLite
performs expiration cleanup opportunistically during queue operations;
optional service backends use their atomic time/expiry facilities.”

Classify it as FIT, OWNERSHIP VIOLATION, or BORDERLINE/REVISE. Steelman both
admission and rejection. Identify which concern belongs to program theory,
which exact behavior belongs to a winning product contract, and which
mechanics belong to implementation rationale. State the smallest coherent
version and the evidence that would resolve the ambiguity. Do not assume that
usefulness is sufficient reason to add a concept. Return only your review
response.
```

Unedited output:

```text
Classification: BORDERLINE/REVISE

Admission, steelmanned: A time-bounded reservation is a queue-level delivery
primitive. It coordinates competing consumers without defining task types,
worker topology, business retry policy, or workflow success. That fits
[THEORY-2] through [THEORY-5]: SimpleBroker owns queue-operation semantics;
applications still own execution and the meaning of success. Lease expiry can
also satisfy “explicit safety over magical recovery” if its failure windows
are named precisely. SQLite-local and optional shared-backend realizations
need not change the core model.

Rejection, steelmanned: As written, the proposal silently changes the meaning
of claim. The winning delivery excerpt says a committed claim prevents a
second normal delivery of that claim. Automatic visibility after a duration
is different behavior. Adding ack also risks presenting application success
as a broker-owned concept, despite the theory’s explicit separation between
delivery state and application completion. Caller-selected time, expiry,
redelivery, stale acknowledgements, and crash races introduce a substantial
new state machine. The absence of task types or workers does not by itself
make that state machine conceptually cheap. The theory also says new concepts
require concrete pressure; usefulness alone is insufficient. Existing
“explicit reservation or transactional-generator” patterns may already own
this need.

Surface ownership:

- Program theory: Whether a temporary reservation and settlement token are
  coherent queue primitives rather than application-runtime concepts; whether
  the added concept count is justified by concrete pressure; and whether ack
  can remain a delivery-state transition with no claim about business
  completion.
- Winning product contract: The exact operation name and state machine. It
  must specify whether this is distinct from claim; selection and invisibility
  rules; lease duration bounds; the expiry boundary and clock model; whether
  physical cleanup is required for semantic visibility; reservation ownership
  or receipt tokens; acknowledgement before, at, and after expiry; stale-token
  behavior; competing acknowledgements; crash and commit windows; redelivery
  guarantees; backend parity; CLI/Python availability; payloads, diagnostics,
  and exit statuses.
- Implementation rationale: Opportunistic SQLite cleanup, indexes or
  tombstones, transaction shape, and the use of PostgreSQL or Redis/Valkey
  atomic time/expiry facilities. If cleanup timing affects externally
  observable visibility, that portion must move into the product contract;
  only the physical reclamation mechanism remains implementation rationale.

Smallest coherent version: Add a distinct queue-local reserve operation, not a
changed claim. It atomically returns one message plus an opaque reservation
token and hides that message from normal delivery until a bounded deadline.
settle(token) removes the still-current reservation and message; it means only
“finish this delivery transition,” not “business work succeeded.” If the
deadline passes without settlement, the message is eligible for normal
delivery again. No renewal, worker identity, task metadata, broker-managed
retry count, dead-letter policy, or workflow-completion claim should be
included initially. Semantic expiry must be enforced when selecting messages;
physical deletion of expired reservation metadata may remain opportunistic.

Evidence needed to resolve the ambiguity: Show concrete recurring consumer
failures or repeated private-internal use that existing claim, move,
reservation, and transactional-generator patterns cannot solve cleanly. Then
provide a cross-backend state-transition table and adversarial probes for
crash-before/after commit, simultaneous reserve, expiry-boundary races, stale
settlement, clock skew or server-time choice, and redelivery. If that evidence
shows one narrow, backend-independent queue contract and a materially simpler
consumer model, the revised proposal is a fit. If it requires worker
ownership, heartbeat/renewal policy, business retry accounting, or
application-success semantics, reject it as an ownership violation.
```

## Independent Review Loop

Required plan and completed-work stances:

1. Theory/authority: challenge concern ownership, historical honesty, negative
   knowledge lifecycle, and whether this remains iterative theory building.
2. Zero-context usability: follow only the declared paths, reconstruct the
   theory, apply an alternative, and report navigation or authority traps.
3. Implementation readiness: verify exact edit owners, task order, red/green
   gates, record grammar, and done signals.

The author must reproduce each finding and either incorporate it or record a
reasoned rejection.

## Stop and Re-Plan Gates

Stop if:

- the owner does not approve purpose, concept, principle, or non-goal text
- theory contradicts released behavior
- theory and a winning contract both claim exact behavior
- provenance recovery starts inferring decisions from feature absence
- the theory cannot remain concise startup context
- dogfood cannot transfer the ownership boundary
- the record schema creates ordinary-work boilerplate
- a test claims to prove qualitative judgment
- runtime, public API, packaging, backend, delivery, or consumer behavior must
  change
- this slice begins editing `../agent-theory`

## Assumptions and Open Questions

| Question | Current assumption | Owner | Resolve or reopen when |
|----------|--------------------|-------|------------------------|
| Artifact name | “Program Theory” best covers mental model, negative boundaries, falsifiers, and revisions. | Product owner | Owner prefers another term before activation. |
| Authority location | `docs/program-theory.md` stays outside `docs/specs/` to keep conceptual and behavioral verification distinct. | Product owner and process reviewer | Review finds the concern split ambiguous. |
| Historical depth | Seed representative evidence-backed revisions, not a complete archaeology. | Product owner | A local decision corpus can be recovered cheaply with provenance. |
| “Simplebrokerness” | Use it only as shorthand for the stated principles and consequences. | Product owner | Independent reviewers cannot apply it consistently. |
| Upstream timing | Open a separate plan after SimpleBroker dogfood and closure. | Product owner | Owner explicitly requests a different sequence. |

## Upstream Export Brief

This is a brief for a later, separate `../agent-theory` plan. It is not an
upstream change.

### Project-neutral principles that survived dogfood

- “Program theory” is a working explanatory model, not a formal theory,
  requirements catalog, architecture inventory, or claim that tacit knowledge
  has been fully serialized.
- Theory, winning behavior contracts, implementation rationale, code/tests,
  and plans answer different questions. The links among them matter more than
  collapsing them into one document.
- Current accounts should precede superseded accounts. Historical evidence can
  show continuity and evolution without becoming a second startup authority.
- Negative statements need distinct types: durable non-goal, current
  limitation, rejected alternative, and plan-local scope.
- Durable alternatives need an admission threshold, an observable
  reconsideration condition, a current owner, and a closure route. They do not
  belong in a detached graveyard.
- No method can mechanically derive a coherent program from an arbitrary
  problem. Executable gates have more than educational or heuristic value:
  they bind selected consequences of the theory, reject known-invalid states,
  and force discrepancies into view.
- Semantic transfer still needs applied dogfood. A green gate is binding
  evidence for the claim it covers, but it cannot prove the whole theory
  correct or establish that a reader possesses it.

### Naur passages to carry into upstream framing

- “The Theory to Be Built by the Programmer” defines the object as “a theory
  of how certain affairs of the world will be handled by, or supported by, a
  computer program.” Agent-theory should retain that problem-world-to-program
  relation.
- The same section gives three practical criteria: connect affairs of the
  world to program structure, justify why the program has its shape, and
  respond constructively to modification. These are better possession tests
  than definition recall.
- “Program Life, Death, and Revival” includes discussion of “the limits set on
  the real world matters dealt with by the program.” Agent-theory should use
  this to explain why non-goals and relevance boundaries are part of the
  theory, not ancillary product positioning.
- “Problems and Costs of Program Modifications” explains why two changes can
  produce acceptable external behavior while only one fits or naturally
  extends the program theory. This is the central risk when an agent produces
  plausible, passing code without shared understanding.
- Naur calls revival of a theory from documentation alone “strictly
  impossible.” Agent-theory should present its departure precisely: it does
  not claim complete serialization. It wagers that humans and agents can
  iteratively reconstruct and maintain a sufficiently shared theory by keeping
  intent, code, tests, decisions, alternatives, and surprises in contact.
- “Method and Theory Building” rejects procedures that mechanically produce
  good solutions. Agent-theory should agree about derivation but reject the
  weaker conclusion that executable gates are merely educational or
  heuristic. Gates mechanically preserve selected consequences and expose
  contradictions; judgment chooses and revises those consequences.

### Knuth's contribution and agent-theory's departure

- Naur supplies the object: the working theory that makes coherent judgment
  possible. Knuth supplies a representational discipline: explanation and
  executable realization should be developed in contact and organized for
  human understanding. Agent-theory combines both commitments.
- Knuth describes the central idea as treating a program as “a piece of
  literature, addressed to human beings rather than to a computer.”
  Agent-theory retains the human-oriented explanatory order even when agents
  are major readers and implementers: the human must remain able to possess
  and challenge the theory.
- Literate programming combines a programming language and documentation
  language so that machine-ready code and human-readable explanation can be
  produced from one source. Agent-theory does not require one master artifact.
  Theory, winning contracts, plans, implementation rationale, decisions,
  tests, and code remain separate because they answer different questions and
  have different owners and lifecycles.
- Agent-theory does not require one structured source from which both
  executable and explanatory views are produced. It assigns different
  questions to artifacts with distinct authority and lifecycles, then uses
  traceability and executable gates to keep their claims in contact. Prose and
  executable evidence constrain and challenge each other; neither is complete
  authority over every concern.
- The difference is not that literate programming cannot discuss product
  identity, non-goals, alternatives, revisions, ownership, or falsifying
  evidence. Agent-theory makes those concerns explicit nodes in a
  repository-wide theory-and-evidence graph, with declared owners and update
  rules. It is therefore not merely “literate programming with agents.”

### SimpleBroker-specific parts

- The queue primitive, backend substrate, and embedding application's work
  execution have separate owners.
- The topology boundary is ownership, not host count. SQLite remains local;
  optional service backends can serve cross-host queue clients.
- Delivery state is distinct from application completion.
- Cohesive concurrency and lifecycle code may protect a simpler external
  model; line count is not the simplicity metric.

### Rejected generalizations

- Do not freeze a project's founding README as permanent theory.
- Do not require an alternative record for every local choice or an
  alternatives section in every plan.
- Do not turn qualitative theory into an ordinary behavior spec merely to
  obtain mechanical gates.
- Do not assume all documentation work is meaningfully TDD-able. Use red/green
  for enumerable structural claims and semantic review for explanatory models.

### Dogfood surprises

- The planned Redis-plus-waiter “borderline” proposal was an obvious fit under
  the current model. The exercise was wrong; a lease/requeue proposal supplied
  the real ambiguity.
- Live reciprocal source links cannot survive physical plan deletion.
  Retirement needs a one-way plan-name, source-SHA, and exact-ALT-ID cue plus a
  historical heading check.
- A spec-promotion slice that names a not-yet-created artifact necessarily
  makes the path gate red until the implementation slice. The plan must name
  that transient state rather than weaken the claim or create a placeholder.
- The first test attempt imported an undeclared YAML library. A narrow
  structural gate did not need that dependency.

### Open questions for upstream work

- Should the exact `[ALT-*]` and `[REV-*]` grammar be universal, a supplied
  default, or a repository profile?
- Which repositories benefit from theory as first shared context, and which
  need a smaller persona-specific theory kernel?
- How should source-pinned alternative retrieval work in shallow clones,
  release archives, or repositories without locally available history?
- What repeatable semantic dogfood patterns transfer beyond product software
  without turning model agreement into authority?

## Out of Scope

- Runtime, API, CLI, storage, delivery, backend, packaging, or consumer changes.
- Rewriting the full README or product catalog.
- Promoting all `readme-only` concerns into specs.
- Recovering every historical chat.
- Treating absent features as rejected features.
- A general ADR system or alternatives directory.
- Mandatory alternatives sections in every plan.
- Backstitch support for theory records.
- Editing or committing `../agent-theory`.
- Publishing a release or changing release versions.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| [DOM-3], [DOM-16] / T4 gate order | T4 would promote the spec without creating `docs/program-theory.md` and still pass `bin/check-doc-paths`. | During T4, `bin/check-doc-paths` reported the three promoted references to the intentionally not-yet-created path. T6 created it; the final gate exits 0. | A normative path claim cannot resolve before its target exists. Creating a placeholder would blur the spec-promotion and implementation slices; weakening the claim would game the gate. | Reconciled by the recorded T4 red and T6/final green evidence. |

## Revision Log

| Date | Reviewed baseline | Revision | Reason | Re-review |
|------|-------------------|----------|--------|-----------|
| 2026-07-29 | initial draft | Recast as strategy-A implementation, separated conceptual and behavioral authority, added exact read order and record grammar, moved provenance and dogfood before promotion and the red proof immediately after it, and enumerated all policy owners. | Three independent reviews converged on authority, provenance, and sequencing defects. | required |
| 2026-07-29 | first revision | Removed dogfood answer-key contamination; defined ID scope, destination links, registry boundaries, and reciprocal promotion; made the exact DOM delta and machine-readable read path complete; added concrete routing fixtures and multi-commit rollback. | Re-review found remaining implementation and zero-context ambiguities. | passed |
| 2026-07-29 | reviewed draft | Made revision records current-account-first and confined the original README to source-pinned git/plan evidence. It is neither copied, promoted as an alternative, nor assigned as startup context. | Owner identified the risk that preserving an old theory artifact would anchor agents on a superseded account. | passed |
| 2026-07-29 | anchoring-safe exclusion model | Added a bounded `[THEORY-7]` lineage table: current principle first, short founding quote, then explicit `Maintained` and `Evolved` analysis. The full original remains unassigned and non-normative. | Owner judged complete exclusion too aggressive because founding language can make continuity and evolution legible. | passed |
| 2026-07-29 | bounded-lineage model | Added the Naur-derived meaning of program theory to [DOM-16] and a startup `[THEORY-0]` section that distinguishes the working model from its documentary, contractual, architectural, and executable evidence. | Owner identified that agents need background on what “theory of the program” means before they can use the SimpleBroker account correctly. | passed |
| 2026-07-29 | owner-supplied Naur source review | Added one bounded Naur quotation and a practical possession test to `[THEORY-0]`; clarified in `[DOM-16]` that executable gates bind selected consequences without deriving or proving the whole theory; recorded the article's modification, limits, revival, and method passages for later upstream work. | The source text sharpened the theory/program relation, while the owner correctly rejected “educational and heuristic” as an adequate account of executable gates in agent-theory. | passed |
| 2026-07-29 | active-plan lifecycle review | Split promoted-alternative provenance into reciprocal live-plan and source-pinned retired-plan forms; added exact ledger fixtures and a pre-deletion `git show` check for the cited `ALT` heading. | Physical coalescing deletes plan files, so permanent reciprocal links were impossible; name and SHA alone also could not prove that the cited alternative existed in the pinned source. | passed |
| 2026-07-29 | completed-work authority review | Reduced revision records 003–005 to conceptual ownership lessons with live contract/rationale links, made the product kernel local-first rather than host-bound, and split SimpleBroker's shared broker core from backend-owned adapters/runners. | Independent review reproduced competing-authority defects and two topology/ownership ambiguities in the implemented draft. | passed |
| 2026-07-29 | completed-work structural-gate review | Added firing malformed-record and provenance mutations, live/retired form distinction and exact live-path reciprocity, a complete concept-to-registry-owner table reconciled to registry loci, exact repository/product persona orders, anchor and stable-reference resolution, and complete quotation/analysis checks. Split the end-to-end test by concern when C901 fired. | Independent review reproduced nine false-green classes across two rounds in the first structural test. | passed |
| 2026-07-30 | Knuth source review | Added a compact orientation reference to `[THEORY-0]`, literate programming as a distinct upstream source, and the synthesis: Naur supplies the theory to be possessed; Knuth supplies human-oriented explanatory discipline; agent-theory links separately owned artifacts through traceability and executable gates instead of requiring a single literate source. | The owner identified literate programming as a second intellectual precursor and judged that its explanatory discipline should orient the SimpleBroker agent reader as well as later upstream work. The stated departure prevents “literate programming with agents” from obscuring theory revision, negative knowledge, ownership, and executable evidence. | passed after narrowing the claimed departure |

## Review Log

| Review | Date | Verdict | Disposition |
|--------|------|---------|-------------|
| Product-owner approval | 2026-07-29 | approved | Plan and Status Index activated for implementation. |
| Zero-context agent usability | 2026-07-29 | blocked | Findings incorporated into the revised read order, authority split, record grammar, reconsideration action, namespace, and task order. |
| Theory and authority | 2026-07-29 | needs revision | Findings incorporated into provenance-first sequencing, README/kernel split, durable non-goal boundary, and backend ownership wording. |
| Implementation readiness | 2026-07-29 | not execution-ready | Findings incorporated into strategy A, exact DOM-3/4/15 deltas, post-promotion red proof, machine-readable order, owner map, dogfood evidence schema, and unreleased CHANGELOG treatment. |
| Theory and authority final | 2026-07-29 | pass | No remaining P1/P2 findings. |
| Implementation readiness final | 2026-07-29 | pass | No remaining P1/P2 findings; ready after owner activation. |
| Zero-context agent usability final | 2026-07-29 | pass | No remaining P1/P2 findings after aligning the context-hub read order in T6. |
| Historical-anchoring theory review | 2026-07-29 | pass | Current-account-first revisions preserve lineage without making the original README current authority. |
| Historical-anchoring zero-context review | 2026-07-29 | pass | Original README and its freeze alternative remain outside startup theory and dogfood inputs. |
| Promoted-alternative retirement lifecycle | 2026-07-29 | pass | Source-form fixtures cover exact and mutated ledger cues; physical deletion requires exact historical `ALT` heading retrieval. No remaining P1/P2 findings. |
| Bounded-lineage theory review | 2026-07-29 | pass | Passed after replacing one paraphrase with an exact pinned-source quotation. |
| Bounded-lineage zero-context review | 2026-07-29 | pass | Current-first comparison and dogfood bound the remaining anchoring risk. |
| Bounded-lineage implementation review | 2026-07-29 | pass | Quote manifest, allowlist enforcement, bounds, and shallow-checkout behavior are executable. |
| Program-theory background theory review | 2026-07-29 | pass | Working theory, externalized account, exact contract, and implementation-rationale authority are distinct. |
| Program-theory background zero-context review | 2026-07-29 | pass | Mixed-example dogfood tests applied routing rather than definition recall. |
| Program-theory background implementation review | 2026-07-29 | pass | `[THEORY-0]` is structurally required, placed first, and covered by a semantic dogfood gate. |
| Completed-work theory/authority review | 2026-07-29 | pass after revision | Reproduced and corrected four findings across two rounds: REV records duplicated exact behavior/mechanics, kernel summaries implied a host boundary, and the core table assigned shared broker semantics to backend implementations. Re-review returned PASS. |
| Completed-work structural-gate review | 2026-07-29 | pass after revision | Reproduced and corrected nine false-green classes across two rounds covering record/evidence grammar, source lifecycle, contract-owner routing and registry loci, persona read order, links/anchors/stable references, and all lineage quotations/analysis. All mutations then failed at the intended assertions; re-review returned PASS. |
| Requested Grok other-model review | 2026-07-29 | unavailable | `grok-4.1-fast`, `grok-4`, and `grok-code-fast-1` each failed before repository execution with the same provider-side server error. No Grok verdict was inferred. |
| Claude other-family fallback review | 2026-07-29 | pass | No P1/P2 findings. The one non-blocking stale concept-table illustration was aligned while the plan remained active. |
| Naur source and executable-gate delta | 2026-07-29 | pass | A fresh narrow review checked the quotation against the owner-supplied article, the mechanical-derivation versus executable-constraint distinction, attribution, redundancy, and authority boundaries; no P1/P2 findings remained. |
| Knuth source and orientation delta | 2026-07-30 | pass after revision | Review rejected an overbroad contrast that reduced WEB to physical colocation or prose-to-code generation and implied that literate programming could not carry broader rationale. The final text limits the departure to agent-theory's separately authoritative, lifecycle-owned artifact graph and passed re-review. |

## Fresh-Eyes Checklist

- [x] A repository agent reads safety first, then theory, then process.
- [x] A product user reads the kernel first, then the winning contract.
- [x] `[THEORY-0]` explains the working explanatory model, distinguishes it
  from a spec or architecture inventory, and states that the document is only a
  transfer surface.
- [x] Theory and product contracts have non-overlapping authority.
- [x] README retains `readme-only` authority.
- [x] `[THEORY-*]` does not collide with `[SB-*]`.
- [x] The distributed-systems boundary is about ownership, not host count.
- [x] Exact capability limits remain with their winning contract.
- [x] Durable alternatives have the fixed shape and reconsideration action.
- [x] Historical gaps remain unknown.
- [x] The original README is not frozen or assigned as a parallel read; only
  bounded, source-pinned quotations appear in the current-first lineage table.
- [x] Every founding quotation says both what was maintained and what evolved,
  and no quoted statement claims current authority.
- [x] Revision records foreground the current account and summarize superseded
  theory only as needed to explain the delta.
- [x] Dogfood precedes workflow standardization.
- [x] Structural tests make no semantic claims.
- [x] Ordinary work gains no empty ceremony.
- [x] The stale scaffold note is corrected.
- [ ] SimpleBroker closes before upstream generalization begins.
