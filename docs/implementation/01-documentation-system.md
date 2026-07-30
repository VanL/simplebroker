# Documentation System

## Purpose and Scope

This document explains why the repository is organized around shared agent
context, specs, dated plans, implementation docs, reusable skills,
independent reviews, and a lessons ledger.

This is the project-owned rationale for SimpleBroker's documentation system.
It began from the agent-guidance scaffold and now records the repository's
actual theory, product-contract, implementation, and evidence boundaries.

## Governing Spec References

- `docs/specs/01-development-documentation-operating-model.md` [DOM-2]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-3]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-4]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-7]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-8]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-16]
- `docs/program-theory.md` [THEORY-0] through [THEORY-8]

## Design Rationale

### Shared Agent Context

The repository keeps durable guidance in `docs/agent-context/` so multiple
agent tools can consume one source of truth. Root files such as `AGENTS.md`
and tool-specific aliases are intentionally thin entry points rather than
separate policy documents.

### Separate Theory, Contracts, Plans, and Implementation Docs

The split exists because each document answers a different question:

- program theory explains the problem-world model, concept meanings, product
  identity, and the judgment boundary for proposed changes
- the product-section registry's winning README/spec owner answers what exact
  behavior should be true
- plans answer how a specific change will be executed without breaking
  load-bearing boundaries
- implementation docs answer why the current design exists and where it lives
- code and tests realize behavior and supply firing evidence for concrete
  claims

Combining those roles makes documents harder to trust and easier to let drift.

### Documentation As a Delivery Gate

The repository treats documentation maintenance as part of completion because
the main failure mode in agentic development is silent drift between intent,
execution, and implementation.

### Current Theory Before Historical Evidence

Agents read the current theory before process and implementation detail.
Historical sources remain evidence, not competing startup assignments.
Revision records state the current account first so a useful founding phrase
does not silently restore an obsolete boundary.

### [ALT-IMPL01-001] Leave theory only in the root README

Disposition: rejected
Owner: SimpleBroker product owner
Governs: [DOM-3] and [THEORY-1]
Source record: [ALT-PT20260729-001] in docs/plans/2026-07-29-program-theory-and-negative-knowledge-plan.md
Candidate: Keep the root README as the only program-theory artifact.
Why plausible: The README was the founding theory artifact and remains the main human product entry.
Evidence:
- contemporaneous: current README combines product identity, catalog, examples, and exact readme-only behavior
- owner-recalled: the source plan's approved authority model
Reason: The README is not guaranteed early repository-agent context and must retain its product-contract role.
Current consequence: Keep README product expression and registered behavior ownership while giving conceptual theory one concise owner.
Reconsider when: The README becomes concise, stable, and mandatory startup context without losing its product-document role.
Promoted to: none

### [ALT-IMPL01-002] Put theory in an ordinary behavior spec

Disposition: rejected
Owner: Process-spec owner
Governs: [DOM-16]
Source record: [ALT-PT20260729-002] in docs/plans/2026-07-29-program-theory-and-negative-knowledge-plan.md
Candidate: Store program theory as an ordinary canonical product spec.
Why plausible: Specs already have stable references and own intended behavior.
Evidence:
- contemporaneous: the repository's product-section registry and firing-test rule for canonical product obligations
- owner-recalled: the source plan's approved conceptual and behavioral authority split
Reason: Identity principles and non-goals require semantic review, while enumerable behavior requires firing tests. Combining them encourages fake prose-pinning gates.
Current consequence: Store theory outside docs/specs and link concrete consequences to the winning behavior owner.
Reconsider when: The spec system gains a conceptual-contract type with honest non-behavior verification semantics.
Promoted to: none

### [ALT-IMPL01-003] Add a detached alternatives directory

Disposition: rejected
Owner: Process-spec owner
Governs: [DOM-16]
Source record: [ALT-PT20260729-003] in docs/plans/2026-07-29-program-theory-and-negative-knowledge-plan.md
Candidate: Add docs/alternatives or a general ADR graveyard.
Why plausible: Dedicated records are easy to append and preserve.
Evidence:
- contemporaneous: [DOM-16] negative-knowledge ownership and closure routing
- owner-recalled: the source plan's rejection of unowned alternative storage
Reason: Detached records become stale tombstones and compete with current rationale.
Current consequence: Durable alternatives live beside the theory, contract, implementation, or process boundary they constrain.
Reconsider when: A cross-cutting decision cannot be owned or found from any current governing artifact.
Promoted to: none

## Boundaries and Invariants

- `docs/agent-context/` is the canonical shared context surface.
- `docs/program-theory.md` owns the current conceptual account and does not
  define exact behavior.
- `docs/specs/` holds process specs (`[DOM-*]`) and product specs
  (`[SB-*]`) when present. Product authority is mechanical via
  `docs/specs/product-section-registry.md` (`readme-only` → README;
  `canonical-spec` → listed section). See `docs/README.md` and
  `docs/specs/README.md`.
- `docs/plans/` contains dated execution records.
- `docs/implementation/` explains rationale and important edit points.
- `skills/` stores reusable task-scoped workflow instructions.
- `docs/lessons.md` is the one canonical lessons ledger.

These roles should stay distinct even as simplebroker grows.

## Key Files

| Path | Purpose |
|------|---------|
| `AGENTS.md` | Primary agent entry point |
| `docs/program-theory.md` | Current conceptual account for design judgment |
| `CLAUDE.md` | Alias for tools that expect Claude-style root guidance |
| `docs/agent-context/README.md` | Shared context hub |
| `docs/specs/00-specs-index.md` | Numbered entry point for specs |
| `docs/specs/01-development-documentation-operating-model.md` | Governing operating-model spec |
| `docs/implementation/00-implementation-index.md` | Numbered entry point for implementation docs |
| `docs/implementation/02-repository-map.md` | Quick pointer map for important docs |
| `docs/implementation/03-agent-inventory.md` | Current observed agent availability and review preference |
| `skills/README.md` | Skill directory conventions and promotion criteria |
| `docs/coalescing.md` | Coalescing state per [DOM-14]: thresholds, watermarks, deferrals, run log |

## Change Guidance

When future work changes product code or its governing model:

1. classify the task per [DOM-15] — planning artifacts and review scale
   with the class; the verification floor never does. Classes 1-2 keep
   their record in the commit history
2. read the program theory for scope and concept ownership, then add or update
   the winning product contract first when exact behavior changes
3. create a dated plan for classes 3 and above
4. for risky work, harden the plan before implementation by making invariants,
   hidden couplings, anti-mocking guidance, rollback or rollout, and one-way
   doors explicit
5. run independent plan review and feed the results back into the plan
6. add or update the relevant implementation note for the touched area
7. update the repository map when new entry points become important
8. decide whether repeated workflow knowledge should become or update a skill
9. capture reusable corrections in `docs/lessons.md`
