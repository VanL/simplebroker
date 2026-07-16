# Documentation System

## Purpose and Scope

This document explains why the repository is organized around shared agent
context, specs, dated plans, implementation docs, reusable skills,
independent reviews, and a lessons ledger.

This file is part of the starter scaffold installed from the agent-guidance
repository. Keep it project-owned and adapt it as real product code and
repo-specific boundaries appear.

## Governing Spec References

- `docs/specs/01-development-documentation-operating-model.md` [DOM-2]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-3]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-4]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-7]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-8]

## Design Rationale

### Shared Agent Context

The repository keeps durable guidance in `docs/agent-context/` so multiple
agent tools can consume one source of truth. Root files such as `AGENTS.md`
and tool-specific aliases are intentionally thin entry points rather than
separate policy documents.

### Separate Specs, Plans, and Implementation Docs

The split exists because each document answers a different question:

- specs answer what should be true
- plans answer how a specific change will be executed without breaking
  load-bearing boundaries
- implementation docs answer why the current design exists and where it lives

Combining those roles makes documents harder to trust and easier to let drift.

### Documentation As a Delivery Gate

The repository treats documentation maintenance as part of completion because
the main failure mode in agentic development is silent drift between intent,
execution, and implementation.

### Scaffold Boundary

The bootstrap script installs only the neutral starter surface. It does not
infer repo-specific engineering principles, merge with existing docs, or decide
what your product architecture should be.

## Boundaries and Invariants

- `docs/agent-context/` is the canonical shared context surface.
- `docs/specs/` is the source of truth for the development process
  ([DOM-*]); in this repository, **product behavior remains specified by
  the root `README.md`** (scope note: `docs/specs/README.md`).
- `docs/plans/` contains dated execution records.
- `docs/implementation/` explains rationale and important edit points.
- `skills/` stores reusable task-scoped workflow instructions.
- `docs/lessons.md` is the one canonical lessons ledger.

These roles should stay distinct even as simplebroker grows.

## Key Files

| Path | Purpose |
|------|---------|
| `AGENTS.md` | Primary agent entry point |
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

When future work adds product code:

1. classify the task per [DOM-15] — planning artifacts and review scale
   with the class; the verification floor never does. Classes 1-2 keep
   their record in the commit history
2. add or update the governing spec first
3. create a dated plan for classes 3 and above
3. for risky work, harden the plan before implementation by making invariants,
   hidden couplings, anti-mocking guidance, rollback or rollout, and one-way
   doors explicit
4. run independent plan review and feed the results back into the plan
5. add or update the relevant implementation note for the touched area
6. update the repository map when new entry points become important
7. decide whether repeated workflow knowledge should become or update a skill
8. capture reusable corrections in `docs/lessons.md`
