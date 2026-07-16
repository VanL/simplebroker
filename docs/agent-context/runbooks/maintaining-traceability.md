# Maintaining Traceability

Documentation maintenance is part of delivery. A change is not complete if the
code moved but the plan, spec, or implementation notes did not.

## Preflight

Before editing:

1. identify the requested outcome
2. identify the governing spec section, or record `Source spec: None`
3. identify the active plan, or create one for non-trivial work
4. identify the relevant implementation doc or repository map
5. identify the review agent or review path for non-trivial work
6. identify the verification evidence that will prove the change
7. if the plan changes intended spec behavior, confirm it has `## Spec
   Baseline`, `## Proposed Spec Delta`, and an explicit promotion slice (see
   `runbooks/writing-plans.md` §4b–4d)

## During Execution

### For Each Material Step

- keep the plan current enough that another engineer can see what changed
- if intended behavior changed, update the spec in the same change — for
  implementation plans, that is the **spec-promotion slice** before code
  cites new spec paths, unless the plan is typed exploration (no governing
  spec yet)
- if rationale, boundaries, or ownership changed, update the implementation doc
  in the same change
- if new important modules or directories were introduced, update the relevant
  repository or code map
- if the work depends on repeated task-shaped guidance, decide whether a skill
  should be added or updated

### When the Direction Changes

- stop and revise the plan or spec instead of silently drifting
- if the change becomes materially different from the request, report that

### When You Learn Something Durable

- add a short lesson to `docs/lessons.md`
- strengthen the appropriate runbook if the lesson should become process
- create or update a skill in `skills/` if the lesson is really a recurring
  workflow

### After Using a Skill or Runbook

- ask whether it missed a step, command, or failure mode
- update it while context is fresh if the improvement is reusable

## Completion Gate

Before calling the work done, check:

- the spec points to the right plan
- the plan cites the right spec sections
- the implementation doc still explains the current rationale
- independent review findings were answered explicitly for non-trivial work
- verification evidence exists and is named explicitly
- any central skill or runbook used during the work was evaluated for possible
  improvement
- any residual risk or skipped verification is called out
- for spec-changing work: promotion baseline identifier recorded; promotion
  strategy executed; classification graduation (if any) completed with
  citation updates; the repo's traceability or self-check gate rerun (named
  command and result)

Retired-plan citation form: when a plan is retired, spec backlinks change
from a live path to a non-path citation:
`- retired: 2026-05-02-example-plan — source <source_sha>; see the ledger
in docs/plans/README.md`. The source SHA is a commit verifiably
containing the plan file. This keeps the traceability gate clean (no dead
path claims) while preserving the retrieval cue. Do not leave live-path
backlinks to deleted plans, and do not delete the backlink itself — the
spec's plan history remains part of its record.

## Minimum Traceability Chain

For any material feature or behavior change, maintain this chain:

`spec section <-> plan <-> implementation doc <-> code`

For docs-only or tooling-only changes where no spec exists, the minimum chain
is:

`plan <-> implementation note or README <-> changed files`

Do not fake spec references. Be explicit when no spec exists.
