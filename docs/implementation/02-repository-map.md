# Repository Map

Quick pointers to the key guidance documents in this repository.

## Root Entry Points

| Path | Purpose |
|------|---------|
| `AGENTS.md` | Canonical agent entry point for *repository* work |
| `docs/program-theory.md` | Current conceptual account: purpose, mental model, concept ownership, durable principles/non-goals, tensions, and revisions |
| `docs/agent-kernel.md` | Agent **use** kernel: CLI + matching client API, delivery hazards, safe recipes |
| `llms.txt` | llmstxt.org-style link index (points at agent-kernel and key docs) |
| `CLAUDE.md` | Alias for tools that expect Claude-style root guidance |
| `README.md` | Human product entry; catalogs/examples; restates + links canonical `[SB-*]` specs |
| `docs/specs/product-section-registry.md` | Mechanical product-doc ownership (all current product families `canonical-spec`) |
| `docs/specs/10-cli.md` … `17-ops.md` | Canonical product specs `[SB-CLI-*]` … `[SB-OPS-*]` |
| `CHANGELOG.md` | Behavior deltas for the published package |
| `SECURITY.md` | Security policy for external reporters |

## Product Code

| Path | Purpose |
|------|---------|
| `simplebroker/` | The package: CLI (`__main__.py`), broker session, delivery, backends (`_backends/`), constants, maintenance |
| `simplebroker/_broker_session.py` | Process-session identity, registry/refcount, admission and drain, per-thread core cache |
| `simplebroker/db.py` | Concrete process-session core factory, runner publication and checkout rollback, plus database operations |
| `extensions/` | Optional extensions (Postgres support etc.) |
| `examples/` | Runnable usage examples (async wrappers, multi-queue) — examples are claims; keep them working |
| `tests/` | Pytest suite (~148 files), `uv run pytest` |
| `fuzz/` | Fuzzing harness |
| `bin/` | Repo tooling (incl. `check-dom15-fixtures`, `check-doc-paths`, `coalesce-check`) |

## Shared Agent Context

| Path | Purpose |
|------|---------|
| `bin/check-doc-paths` | Gate: every backticked repo-relative path claim in the guidance surfaces resolves |
| `bin/coalesce-check` | Evidence trail for the coalescing layer: verifies every run-log SHA and retrieval cue (local, sibling, and published-remote), reports local-only pins, derives the lessons count |
| `bin/ruff_suppression_index.py` | Gate and generator: reconciles human-approved Ruff suppression groups, local source pointers, raw diagnostics, and the derived location index |
| `docs/agent-context/README.md` | Context hub and read order |
| `docs/agent-context/context.index.yaml` | Machine-readable context index |
| `docs/agent-context/decision-hierarchy.md` | Conflict-resolution order |
| `docs/agent-context/principles.md` | Shared execution principles |
| `docs/agent-context/engineering-principles.md` | Engineering rules and warning signs |

## Runbooks

| Path | Purpose |
|------|---------|
| `docs/agent-context/runbooks/writing-plans.md` | Plan-writing standard |
| `docs/agent-context/runbooks/hardening-plans.md` | Required hardening checklist for risky or boundary-crossing plans |
| `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md` | Independent review workflow and agent bootstrap |
| `docs/agent-context/runbooks/writing-specs.md` | Spec-writing standard |
| `docs/agent-context/runbooks/writing-implementation-docs.md` | Implementation-doc standard |
| `docs/agent-context/runbooks/testing-patterns.md` | Testing and verification guidance |
| `docs/agent-context/runbooks/maintaining-traceability.md` | Documentation-maintenance gate |
| `docs/agent-context/runbooks/skills-lifecycle.md` | Skill promotion and maintenance guidance |
| `docs/agent-context/runbooks/external-skill-suites.md` | Precedence and crosswalk for external skill suites (superpowers, gstack, compound engineering) |
| `docs/agent-context/runbooks/designing-agent-facing-interfaces.md` | Principles for designing APIs, CLIs, and docs that agents consume |

## Core Documentation Corpus

| Path | Purpose |
|------|---------|
| `docs/specs/00-specs-index.md` | Numbered entry point for specs |
| `docs/specs/01-development-documentation-operating-model.md` | Governing spec for the documentation workflow |
| `docs/plans/README.md` | Plan directory rules |
| `docs/implementation/00-implementation-index.md` | Numbered entry point for implementation docs |
| `docs/implementation/01-documentation-system.md` | Why the documentation system is shaped this way |
| `docs/implementation/03-agent-inventory.md` | Current observed agent availability and review preference |
| `docs/implementation/06-process-session-core-ownership.md` | Internal ownership and concurrency boundary between process sessions and concrete backend construction |
| `docs/implementation/08-message-identity-and-write-visibility.md` | Reserved-zero insertion boundary and SQL/Redis ordinary-write visibility rationale |
| `docs/lessons.md` | Canonical lessons ledger |
| `docs/coalescing.md` | Coalescing state: thresholds, watermarks, deferrals, run log |

## Skills

| Path | Purpose |
|------|---------|
| `skills/README.md` | Skill directory purpose and conventions |
| `skills/_template/SKILL.md` | Starter template for new reusable skills |
| `skills/coalescing/SKILL.md` | Coalescing sweep: distill lessons, harvest and retire plans, promote skills |
| `skills/debugging/SKILL.md` | Root-cause-first debugging with proof and replan gates |
| `skills/brainstorming-to-plan/SKILL.md` | Bridge from exploration to a dated plan or spec delta |
| `skills/call-agent/SKILL.md` | Invoke an independent reviewer agent (read-only postures, probes) |
| `skills/interface-review/SKILL.md` | Review an agent-facing surface (the CLI here) against `designing-agent-facing-interfaces.md` |

## Update Guidance

When the repository grows:

- add new important entry points here
- keep descriptions short and navigational
- prefer linking to the document that explains a concept, not every file that
  happens to mention it
- route product-scope judgment through program theory, exact behavior through
  the registered winning contract, and concrete design rationale through the
  owning implementation document
