# Decision Hierarchy

Use this order whenever instructions or context seem inconsistent:

1. Explicit user instruction in the current thread.
2. Safety and repository constraints:
   dirty-tree discipline, no destructive commands, do-not-revert-others.
3. Task source-of-truth documents:
   relevant specs, invariants, active plan, and user-facing behavior docs.
4. Canonical repo context in `docs/agent-context/`.
5. Root agent files such as `AGENTS.md` and `CLAUDE.md`.
6. Existing code and test patterns.
7. Agent inference.

## Classify Before the Preflight

Before the repository preflight or first edit — after explicit user
instructions and safety constraints, which always rank higher —
declare the task class per [DOM-15] in
`docs/specs/01-development-documentation-operating-model.md`. The
class decides whether the full preflight below applies (classes 3+)
or the abbreviated four-item record defined in [DOM-15]'s class-2 row
suffices (classes 1–2 use their commit message or handoff report).
The class is a claim; escalators are one-way and declared.

## Required Preflight Before Edits

- List the requested outcomes as a checklist.
- Identify the governing spec, or record plainly that no spec exists.
- Record a baseline identifier for the governing spec in the plan, so
  "compliant" always means compliant with a fixed, named object: the commit
  SHA when the spec is committed; otherwise the file path plus worktree
  state and diff base; or an explicit "this change revises the spec itself".
- When the plan includes a `## Proposed Spec Delta`, record both the **spec
  baseline** (at plan start) and, after the **spec-promotion slice**, the
  **promotion baseline identifier** (after the delta is applied to the spec
  tree). Use a commit SHA when committed; otherwise diff base plus worktree
  state. Mid-implementation claims are against the promotion baseline — not
  plan appendix text and not the pre-promotion identifier.
- Identify the active plan or create one if the change is non-trivial.
- Identify the review agent or review path for non-trivial work.
- Call out invariants that must not move.
- Record assumptions that could change correctness.
- Decide which commands can run in parallel and which must run in sequence.

## Conflict Handling

- If user correction conflicts with your inference, stop and re-derive.
- If specs and code disagree, follow the hierarchy above and call out the
  mismatch.
- Never edit the governing spec silently while claiming compliance with its
  previous version. If implementation reveals the spec must change: pause,
  record the deviation in the plan's deviation log (see
  `runbooks/writing-plans.md`), update the spec as an explicit spec-revision
  slice, then implement against the revised spec — this is how "update the
  spec before or with the code" ([DOM-*], `runbooks/writing-specs.md`) and
  "deviation is declared" coexist. Spec-authoring tasks (merges, harvests,
  revisions) edit specs as their primary work product and are governed by
  `runbooks/writing-specs.md`; they still record the baseline they started
  from for downstream implementers.
- **Spec-changing implementation** (see `runbooks/writing-plans.md`
  §4b–4d): the plan carries exact proposed sections for review; the
  **spec-promotion slice** (early, not last) applies them per a named
  strategy (A/B/C/D). Do not implement code that cites spec paths against
  plan-only text while the spec tree still reflects the baseline.
  **Exploration** (behavior undecided) is not implementation against a
  governing spec — when behavior is decided, promote to the spec tree first,
  then build. Prose `Status: Proposed` and machine classification are
  different mechanisms — see §4d.
- **Single governing contract:** after promotion, the spec tree is
  canonical. The plan's delta is historical review material, not a parallel
  source of truth.
- If uncertainty remains on a high-impact change, ask once and narrowly.

## Completion Gate

Every requested item should have at least one evidence line:

- changed file and what changed
- verification command and result
- observed behavior or explicit residual risk

A status document — a ledger, an execution log, a prior "passing" note, your
own earlier summary — is a claim, not evidence. Every "done", "passing", or
"ship-ready" assertion must cite a command rerun from the current state, not
a document that says so.

For spec-changing work, the final completion gate includes **traceability
reconciliation**: the promoted spec, plan, implementation doc, and code form
a closed chain, and any traceability or self-check command named in the plan
has been rerun from the current state. Where the repo declares that gate
mandatory (for example a zero-error, zero-warning self-corpus check), it is
not waivable. Clearing reciprocal backlink debt and completing classification
graduation (when strategy C was used) are part of done, not optional cleanup.
