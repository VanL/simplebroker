# Writing Implementation Plans

Plans must be detailed enough that a skilled developer with little or no
repository context can implement them correctly without guesswork.

Write every plan as if the implementer is technically strong but:

- has zero context for the codebase
- knows almost nothing about the local tooling or domain
- will make poor local design choices if the plan leaves room for them
- and tends to choose mock-heavy or shallow verification unless the real proof
  is named

If the plan is ambiguous, assume the implementer will choose the wrong file,
the wrong abstraction, and the wrong test seam.

For risky or boundary-crossing work, the plan is not review-ready or
implementation-ready until it also satisfies the companion runbook:

- `docs/agent-context/runbooks/hardening-plans.md`

Role split:

- this runbook defines the required plan shape, mandatory sections, and minimum
  blockers before implementation
- `hardening-plans.md` defines the rewrite criteria, rationale, and generic
  examples for risky work

## Audience Assumptions

- Strong engineer, limited or zero project context.
- Unfamiliar with repo-specific helper paths unless you point to them.
- Prone to over-abstracting or future-proofing if reuse paths are not explicit.
- Prone to adding weak tests unless the production path is spelled out.
- Will follow the plan literally, including ambiguities.

## Planning Standard

Plans are executable documents, not rough notes.

- Document everything needed to succeed on the first pass:
  source specs, files to touch, files to read first, helpers to reuse,
  invariants to preserve, tests to write, and commands to run.
- State what must not change, not just what should be added.
- Break work into bite-sized, dependency-ordered tasks. Each task should be
  small enough to implement and verify independently.
- Plan for independent review, not just author self-checking.
- Prefer over-prescribing boundaries and load-bearing behavior to leaving room
  for implementer inference.
- Prefer explicit local reuse over invention.
- Apply YAGNI aggressively.
- For risky changes, write rollback and rollout notes early enough to shape the
  task breakdown instead of appending them at the end.
- Required reading should describe the current structure and load-bearing
  behavior, not only name files.
- Prefer red-green TDD when the behavior can be expressed cleanly as a failing
  test first. If not, say why and name the smallest concrete proof that should
  replace it.

If a first draft is structurally complete but still feels easy to implement
wrong, or if the change is risky, use the companion runbook:

- `docs/agent-context/runbooks/hardening-plans.md`

## When Hardening Is Mandatory

Treat `hardening-plans.md` as required input when any of these are true:

- the change introduces async, deferred, queued, or background work
- the same core logic must run in more than one execution context
- a public contract, storage format, CLI shape, or compatibility surface is
  changing
- rollback depends on backward compatibility or rollout order
- the work introduces new persistence, temp-file, or cleanup lifecycle
- the change contains a one-way door or destructive edge

## File Placement

- Put plans in `docs/plans/`.
- Prefer descriptive filenames.
- Use a date prefix for new plans when possible:
  `YYYY-MM-DD-short-name-plan.md`.
- Classes 0–2 per [DOM-15] do not produce plan files — their record
  lives in the commit history or handoff report. This runbook governs
  classes 3 and above. Class-3+ plans carry a mandatory `Class:`
  metadata line stating the class and trigger reasoning; a post-hoc
  class claim with no mid-flight escalator history is a review smell.

## Required Plan Sections

### 1. Goal

One short paragraph on what is changing and why.

### 2. Source Documents

Link the source spec(s) and any existing plan, README, or implementation note
that defines the desired outcome.

Use exact spec files and section identifiers when they exist. Prefer:

```text
Source specs:
- docs/specs/00-some-spec.md [ABC-2], [ABC-4]
- docs/specs/01-another-spec.md [XYZ-1.2]
```

If no spec exists, say so plainly:

```text
Source spec: None — bug fix / refactor / tooling change
```

### 3. Context and Key Files

For the change, list:

- files to modify
- files to read first
- style or guidance docs to consult
- shared helpers or patterns that must be reused
- what the important existing files, entry points, or contracts currently do
- which current class, function, command, route, or module owns the behavior
- which registrations, imports, auth rules, cleanup jobs, or lock semantics are
  load-bearing today when they matter

For complex or risky changes, required reading should not stop at file paths.
Add one or two comprehension questions so the implementer can verify they
understood the load-bearing behavior before editing.

Do not make the implementer infer the file list from later prose.
If the reader could still open the file cold and guess wrong about where or how
to edit, this section is incomplete.

### 4. Invariants and Constraints

Call out what must stay true. At minimum, consider:

- behavior or contract invariants
- boundaries that must not split into parallel paths
- compatibility constraints
- hidden couplings or state that crosses boundaries
- which failures are fatal versus best-effort
- which auxiliary failures must not downgrade a successful core operation
- lifecycle constraints for deferred work, temp files, or queued inputs
- rollback compatibility that must hold during rollout
- one-way doors that need a higher verification or rollout bar
- review gates such as no drive-by refactor, no silent CLI change, or no new
  dependency

State invariants before or alongside the task breakdown, not after it. If the
plan only says what to build and not what must not move, it is not ready.

### 4a. Deviation Log

Every plan that implements against a spec carries a `## Deviation Log`
section, empty at the start, appended to whenever implementation departs
from the recorded baseline (see the decision hierarchy: deviation is
legitimate; undeclared deviation is not). One row per departure:

```markdown
## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
```

The `Spec proposal` column holds the pointer to the spec-revision slice or
proposal that reconciles the deviation, or `pending` — it must not stay
`pending` past the plan's completion gate. An empty deviation log at the end
of a plan is a claim ("we built exactly what the baseline says"), and like
any claim it should survive a spot-check against the diff.

### 4b. Spec Baseline

Every plan that implements against a committed spec records where
implementation started:

```markdown
## Spec Baseline

- `abc123def` — docs/specs/02-core.md, docs/specs/03-configuration.md
  at plan authoring time
```

Rules:

- use the commit SHA when the spec is committed
- if the plan **revises** the spec, say so explicitly (`Plan type:
  implementation with spec revision`)
- after the **spec-promotion slice** (see §4d), record a **promotion
  baseline identifier** — where the proposed delta was applied to the spec
  tree. Use a commit SHA when that slice is committed; otherwise use diff
  base plus worktree state and the spec file diff (same spirit as the spec
  baseline). Mid-implementation compliance claims are against the promotion
  baseline, not the pre-promotion identifier. Do not require an intermediate
  commit before continuing when the user wants uncommitted review — require
  a recorded identifier and a rerunnable verification gate instead
- spec-authoring-only plans record the baseline they started from and the
  identifier after the spec lands

### 4c. Proposed Spec Delta

When a plan changes intended behavior, include exact proposed spec text for
review — not a summary. The active spec at the baseline identifier remains
the governing contract until promotion; the delta is the **review target**
and, after promotion, the **implementation target**.

```markdown
## Proposed Spec Delta

Promotion strategy (see §4d — pick one):

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| docs/specs/02-core.md | A — in-file, text before link claims | [REF-4] paragraph after … |

### [REF-4] — insert after "…" paragraph

> (exact proposed markdown — replacement or insertion text)
```

Rules:

- inline exact sections when the delta is small; link
  `docs/plans/YYYY-MM-DD-<name>-spec-draft.md` when it is large
- every touched requirement must cite stable `[REF-*]` codes
- name the **promotion strategy** (§4d) — not merely "add to spec"
- clarification-only deltas (behavior already matches code) still belong
  here so reviewers see the exact wording
- when the delta codifies existing behavior as general rules, verify each
  rule against the implementation before review — rules drafted from
  memory overclaim, and the reviewer should check rule-vs-code, not just
  rule-vs-intent
- do not treat plan-only text as a second governing contract after
  promotion — once promoted, the spec tree is canonical

### 4d. Spec-Changing Work — Slice Order

**Owner:** plan author defines slice order; implementer follows it literally.
**Boundary:** applies when intended behavior in the spec tree changes or new
sections are added under it. **Verification:** each slice names commands; the
final gate includes traceability reconciliation (below). **Required action:**
pick a plan type; never implement against plan appendix text while the spec
tree still reflects the baseline.

#### Plan types

| Type | When | First slice |
|------|------|-------------|
| **Implementation** (default) | Behavior decided; code will cite spec paths | Spec-promotion slice (strategy below), then code |
| **Spec-authoring** | Harvest, clarification, merge — spec is the primary deliverable | Apply delta to the spec tree; no separate "promotion" task |
| **Exploration** | Intended behavior not yet decided | No implementation against a governing spec; spike only. When decided, open a new plan and promote first |

Exploration is not "park the spec in the plan." Once behavior is cited from
shipped code, the text must live in the spec tree using a named promotion
strategy so traceability tooling (and reviewers) can see it.

#### Default slice order (implementation with spec revision)

1. **Plan** — baseline, proposed delta (with promotion strategy), invariants,
   tasks, deviation log (empty)
2. **Independent review** — critiques plan **and** proposed delta
3. **Spec-promotion slice** — apply delta to the spec tree per chosen
   strategy; update `## Related Plans`; record promotion baseline identifier
4. **Slices 1…N** — code, tests, implementation docs against the
   **promoted** spec
5. **Deviation handling** — if reality disagrees with promoted text:
   deviation log row, explicit spec edit slice, continue against revised spec
6. **Final slice: traceability reconciliation** — backlinks, implementation
   doc, lessons/runbooks; close link debt (below)

Do **not** make "copy appendix into spec" the **last** slice by default.
Promotion belongs early — before code cites new spec sections — so later
tasks are judged against one governing spec. Name the slice
**spec-promotion slice**; if the repo already assigns a meaning to
"slice 0" (for example an acceptance-probe suite), do not reuse that name.

#### Two status systems (do not conflate)

Repos may use **both** of these, and they are different mechanisms:

- **Prose `Status:` header** on a spec file (e.g. `Status: Proposed`,
  `Status: Active`) — governs human/agent **adoption**: whether the spec may
  be implemented at all. Traceability tooling typically does **not** read
  this header.
- **Machine classification** (e.g. per-file glob rungs in a traceability
  checker such as backstitch: `planned_spec_globs`,
  `exploratory_spec_globs`) — governs what the **scanner** reports when
  shipped code cites the file. Classification is usually per-**file**: you
  cannot stage one paragraph of an active file by reclassifying the whole
  file without downgrading every section in it.

Plans must say which mechanism they use, and must not assume a
planned/exploratory classification exists without naming the configuration
change that creates it.

#### Promotion strategies (pick one in the plan)

**A — In-file edit, text before link claims (default for paragraph edits):**
In the spec-promotion slice, land requirement text in the existing spec file
**without** claiming implementation links (mapping annotations, "implemented
by" pointers). Unlinked new text should be at worst info-class debt to a
traceability checker. In a later slice, add the link claims, the code, and
the reciprocal backlink **together**. Slices between promotion and that
linking slice must not cite the new sections from code — a code backlink to
a section that claims no implementation yet is warning-class debt in
checkers that enforce reciprocity.

**B — Atomic:** Land requirement text, link claims, code, and reciprocal
backlinks in **one** change. Promotion and implementation are the same
slice; use when the delta is small or the team prefers a single landing.

**C — New file under an in-flight classification:** Add a **new** spec file
classified planned/exploratory (requires a configuration change if the repo
has no such classification). Shipped code may cite it with warning-class
debt until graduation. Use for substantial new behavior, not a paragraph
inside an existing active file.

**D — Spec-authoring / clarification only:** No code cites new behavior;
land text as active (or a prose `Status:` update for whole-spec adoption).
No link claims required unless reciprocity is already claimed.

Do **not** reclassify an existing active spec file as planned/exploratory
just to stage a single paragraph.

#### Two-PR / stacked-commit trap

If an active spec section **with** implementation-link claims lands before
the reciprocal code backlink, the repo carries reciprocity debt between the
two landings. Repositories with a zero-warning traceability gate cannot
leave that debt on the main branch. Mitigate with strategy A (no link claims
until the code slice) or strategy B (atomic), and say which in the plan.

#### Graduating an in-flight classification (heavy — not routine)

Moving a spec file from planned/exploratory to active is not a one-line
edit: narrowing a classification pattern affects every file matching it, and
filename-convention patterns often require renaming the file, which breaks
path-qualified citations until backlinks are updated. Name graduation steps,
citation updates, and verification in the plan.

#### Final slice: traceability reconciliation

The last implementation slice is not "tidy prose." It closes the graph:

- complete link claims and reciprocal backlinks; clear warning-class debt
- graduate in-flight classifications only when strategy C was used and
  graduation steps are named
- rerun the project's traceability or self-check gate named in the plan,
  from the current state, and record the result. Where the repo declares
  that gate mandatory (for example a zero-error, zero-warning self-corpus
  check), it is not waivable via a "residual-risk budget" — residual risk
  documents blockers, it does not redefine done
- update the promotion baseline identifier in plan closeout if the spec
  moved again

### 5. Tasks

Use a numbered, dependency-ordered checklist. Each task should be small enough
to implement and verify independently.

For each task, include:

- outcome
- exact files to touch
- what to read before editing
- helpers or patterns to reuse
- tests to add or update
- stop-and-re-evaluate gates when the implementation starts drifting
- per-task done signal

When relevant, tasks should also say:

- whether the task is introducing a wrapper or the core work
- whether rollback depends on the task remaining backward-compatible
- whether the task touches a one-way door that needs narrower sequencing
- what new evidence would force replanning instead of continuing implementation

Prefer:

```text
2. Update the existing serializer path to emit the new field.
   - Files to touch: src/serializer.py, tests/test_serializer.py
   - Read first: docs/specs/00-api.md [API-3]
   - Reuse the current response builder; do not add a second formatter
   - Verify with the targeted test file
```

Not:

```text
2. Update serialization
```

### 6. Testing Plan

Every plan must say what to test and how.

Include:

- which harness or layer to use
- which test file(s) to update or add
- which commands to run
- what observable behavior should prove the change
- which invariants the tests protect
- what should not be mocked

Bias the testing plan toward contract and behavior:

- public request/response shapes
- durable side effects
- externally visible state transitions
- compatibility behavior

Do not leave the implementer to infer the anti-mocking posture. If a real
dependency must stay real, say so explicitly.

If the plan says only “write tests” without naming what must stay real, what
may be mocked, and which contract the proof protects, the testing plan is
incomplete.

For docs-only changes, say that verification is by inspection and document
quality gates instead of runtime behavior.

### 7. Verification and Gates

List the exact commands to run and what success looks like.

Every plan should distinguish:

- per-task verification
- final gates before claiming completion

For changes that affect runtime behavior, also say:

- how success will be observed after deploy
- what rollout sequencing matters
- what rollback path exists
- what operational signal should confirm the change worked

For risky changes, write the rollback notes before implementation starts. If
you cannot describe rollback or safe rollout cleanly, stop and revise the plan
before coding.

### 8. Independent Review Loop

Every non-trivial plan should say how independent review will happen.

At minimum, include:

- which other agent or agent family should review the plan
- which files and docs the reviewer should read — including
  **`## Proposed Spec Delta`** when the plan changes intended behavior
- the review prompt or review stance
- how feedback will be handed back to the plan author

Recommended prompt:

> Read the plan at [path] and its `## Proposed Spec Delta` (if present),
> including the named promotion strategy. Carefully examine the plan, the
> proposed spec text, and the associated code. Look for errors, bad ideas,
> latent ambiguities, and performative overengineering — process,
> abstraction, or ceremony that does not address a real risk or improve
> correctness; recommending removal is as valuable as recommending
> additions. Don't do any implementation, but answer carefully: Could
> you implement this confidently and correctly against the delta as promoted,
> if asked?

The authoring agent must then consider each review point explicitly and either:

- update the plan
- explain why the current path is still the best choice
- or mark the point out of scope with reasoning

If the reviewer says they could not implement the plan confidently and
correctly, treat that as a blocker until the ambiguity is resolved or the
limitation is recorded explicitly.

### 9. Out of Scope

State what is explicitly not changing. This reduces scope creep.

### 10. Fresh-Eyes Review

Before considering the plan complete, re-read it as if you are a new engineer.

Check for:

- missing file paths
- ambiguous phrases like “update the logic”
- unstated invariants
- missing test harness or verification commands
- tasks that are too large to review safely
- hidden assumptions about local style or tooling
- accidental drift away from the requested direction
- performative overengineering: sections, gates, or abstractions the plan
  carries that do not address a real risk or improve correctness — cut
  them; a fresh-eyes pass that only adds is a warning sign

Fix those gaps and re-read the plan again.

If tightening the plan would require materially changing scope, architecture, or
direction, stop and report that instead of quietly rewriting the task.

## Plan Hardening Checklist

Before treating a plan as review-ready, confirm that it covers these when
relevant:

- invariants named before tasks
- hidden couplings and boundary-crossing state called out
- wrapper logic separated from core work when the same logic spans contexts
- stop-and-re-evaluate gates included for risky tasks
- explicit out-of-scope notes
- anti-mocking guidance
- contract-focused tests
- fatal versus best-effort error-path priorities
- post-deploy success signals
- current-file or current-contract context
- rollout sequencing and rollback
- rollback written early enough to shape the design
- one-way doors
- required reading with comprehension questions

## Blockers Before Implementation

Do not start implementation on risky work if the plan is missing any of these:

- invariants that say what must not change
- enough current-structure context to find the right edit point
- anti-mocking guidance for the important proof
- rollback or rollout notes when order or compatibility matters
- an independent review loop
- deferred-processing lifecycle constraints
- required reading with comprehension questions

For **spec-changing implementation** plans, also do not start **code**
slices until:

- `## Spec Baseline` and `## Proposed Spec Delta` exist
- independent review of the delta has completed
- the **spec-promotion slice** has landed in the worktree (or the plan is
  typed **spec-authoring**, where spec landing is the work)
- the promotion baseline **identifier** is recorded (commit SHA or diff base
  + worktree state — not necessarily a commit)
- the promotion **strategy** (A/B/C/D) and gate-preservation plan are
  explicit

For the deeper rationale and examples behind this checklist, see:

- `docs/agent-context/runbooks/hardening-plans.md`

## Backlink Rule

When a plan implements a spec in `docs/specs/`, add a backlink in that spec
under `## Related Plans` or `## Plans`.

When the touched spec already contains nearby implementation notes such as
`_Implementation snapshot_`, `_Implementation status_`, or
`_Implementation mapping_`, update those notes in the same change.

## Plan Lifecycle and Retirement

Plans move through: `draft` → `active` → `completed` or `superseded` →
`retired`. Status lives in the plan index (`docs/plans/README.md`), not in
ceremony inside the plan file.

- **Active plans have a mutability boundary**: task instructions and
  checklists stay current and mutable — stale instructions are worse than
  edited ones, and git preserves prior versions; decision, deviation, and
  review logs are append-only. At closure the whole plan becomes
  immutable.
- **Completed and superseded plans are harvest candidates.** They stay in
  the tree until the coalescing sweep retires them.
- **The harvest gate — all four before deletion, no exceptions:**
  1. deviation log closed (no `pending` spec proposals)
  2. durable rationale absorbed into the governing spec or implementation
     doc (or explicitly judged not durable)
  3. lessons extracted to `docs/lessons.md` where applicable
  4. every spec `## Related Plans` backlink converted to the retired
     citation form (see `maintaining-traceability.md`)
- **Superseded plans additionally require** the superseding plan to name
  what it inherits (open deviation rows, decided-but-unbuilt behavior)
  before the predecessor retires.
- **Retirement is two-step: soft-retire, then delete.** The sweep performs
  the soft retirement — status flips to `retired-pending` in the index,
  backlinks convert to the retired citation form, and the ledger line is
  written (name, date range, one-sentence outcome, what absorbed it,
  source SHA). Physical deletion happens in a dedicated follow-up change only
  after a second agent or the user verifies the harvest gate. Never
  soft-retire and delete in the same change, and never create a
  retired/archived plans directory — git is the archive.
- **Exemplar plans are exempt.** The status index may mark a plan
  `exemplar` (bootstrap or operating-model foundation plans that serve as
  onboarding examples). Exemplars are not retirement candidates until the
  index note says their exemplar role has been superseded.
- Record the source SHA as a mainline commit that actually contains the
  plan's final state; with squash merges, the squashed mainline commit is
  the one to cite.

## Anti-Patterns

- “Update the system” without naming the file, path, or invariant involved
- citing only a whole spec document when section codes exist
- assuming the implementer knows local helpers or style rules
- tasks that bundle several unrelated edits into one step
- “add tests” without naming the layer, harness, or regression
- plans that lean on mocks for core behavior
- plans that require independent review but never say who reviews them or how
  feedback is handled
- plans that describe what to build but not what must not change
- plans that omit rollback, rollout, or one-way doors on risky changes
- plans that introduce async or deferred processing without input-lifecycle
  answers
- future-proofing or abstraction decisions left to the implementer
- over-scoping with unrelated cleanup
- spec-changing work without `## Proposed Spec Delta` or a promotion
  baseline identifier
- implementing code that cites spec paths before the spec-promotion slice
  lands
- treating plan appendix text as the governing contract after promotion
- reclassifying an active spec file as planned/exploratory to stage one
  section (classification is per-file)
- landing actively-linked spec sections before reciprocal code when the repo
  enforces a zero-warning traceability gate (use strategy A or B instead)
- waiving a mandatory traceability gate via a "residual-risk budget"
