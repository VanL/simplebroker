# Crystallize Program Theory

Status: active — adopted from `agent-theory` @ `8f41a9d` per `docs/plans/2026-10-14-agent-theory-delta-wave-plan.md`.

## Purpose

Interview the product owner relentlessly until a theory file can be written:

- **Product theory** — `docs/program-theory.md` (apex shared language), or
- **Module theory** — the **concrete extension** of that model for one owning
  unit (typical path: `<module>/MODULE-THEORY.md`).

Module theory is not a lesser document. It is how the model grows when product
theory would become too long for general entry. See the primer
`docs/specs/02-agent-theory-and-program-theory.md` `[AT-REF-4]` (module
theory).

The result is a transfer surface for agents and humans—not a requirements dump
and not a copy of the agent-theory hub's own theory.

Inspired by interview-style “grill” workflows: one decision at a time, owner
owns the answers, environment facts are looked up rather than asked.

## When To Use

- A consumer repo still has the bootstrap **program-theory stub** (product).
- Product identity is only in chat (“X-ness”) or a long README residual.
- Agents repeatedly propose category errors or re-litigate non-goals.
- **Extending the model:** a subsystem has its own generative rule, ownership
  boundaries, or non-goals; putting them in product theory would bloat general
  entry—write **module theory** instead.
- Before class-5 identity work without a written conceptual account.

## When Not To Use

- Editing the **agent-theory hub's** own meta-theory — that is a foreign
  repository's file; edit it there under its review. This repository's
  `docs/program-theory.md` is SimpleBroker's product theory (`Status: Active`,
  `[PT-*]`); revise it through this skill's revision path with owner
  ratification.
- Pure process changes with no product-identity impact.
- Writing exact behavioral contracts (use specs / writing-specs runbook).

## Governing References

- Definitional primer: `docs/specs/02-agent-theory-and-program-theory.md`
- This repository's product theory: `docs/program-theory.md` (`Status: Active`,
  `[PT-*]`). The hub's meta-theory (`[AT-THEORY-0]`–`[AT-THEORY-8]`) lives in
  the `agent-theory` repository (foreign; cited by name, not path)
- Operating model: `docs/specs/01-development-documentation-operating-model.md`
- Spec writing: `docs/agent-context/runbooks/writing-specs.md`

## Read First

1. Existing `docs/program-theory.md` (SimpleBroker product theory, stub, or draft).
2. Product entry (README, agent kernel if any, product-context if any).
3. Any module path the user named for subsystem theory.
4. Recent plans or lessons that show identity friction.

## Blast Radius

- Creates or replaces `docs/program-theory.md` (product) or a module
  `MODULE-THEORY.md` / agreed path.
- May require AGENTS.md / `context.index.yaml` read-order updates so theory
  loads at the right scope.
- Does **not** silently change product specs; identity changes may imply
  later class-5 contract work.

## Workflow

### 1. Frame

State that:

- Program theory is Naur-style working understanding externalized as a
  **current account**.
- Possession stays practical; the file is a transfer surface.
- Hub agent-theory text must not stand as product identity.
- **Module theory is how we extend the product model** when depth is local
  ([AT-THEORY-2.1])—not optional color commentary.
- You will ask **one question at a time**, give a **recommended answer**
  when useful, and wait for the owner.

Decide **scope up front** with the owner: product apex, module extension, or
both (thin product + module). Do not draft the full document until shared
understanding on the spine—or they explicitly ask for a draft from answers so far.

### 2. Interview (one question at a time)

This skill owns the interview spine ([AT-THEORY-8] by reference from hub
program theory). For each item:

1. Ask the question.
2. Offer a short recommended direction grounded in repo evidence when possible.
3. Wait for the owner's decision.
4. Record the answer; note open dependencies.
5. Only then proceed to the next question.

If a **fact** is in the repo, look it up. **Decisions** stay with the owner.

Walk branches: if non-goals depend on topology ownership, resolve topology
before finalizing non-goals.

#### Spine questions

1. **Problem world** — What affairs of the world does this program handle?
   For whom? What fails if it does not exist?
2. **Desired feel** — What should using it feel like in one paragraph?
3. **Whole-program metaphor** — In three to seven bullets, what is the
   mental model (not the file tree)?
4. **Core concepts** — Name each concept, its meaning, and its **owner**
   (this product / a module / the user / another system).
5. **Matching surface** — How do CLI, API, UI, or docs express the same
   model without inventing a second product?
6. **Non-goals** — What will competent people propose that you will refuse?
   Why? When would you reconsider?
7. **Generative rule** — Is there one question that organizes a large part
   of the design (as control locality might organize a taxonomy)?
8. **Delivery / safety honesty** — Where can work be lost, duplicated, or
   weakly authenticated? Name the guarantee narrowly.
9. **Tensions** — What live tensions would falsify the account if they
   worsened?
10. **Extension by module theory** — What depth should become module theory
    (ownership table, local generative rule, local non-goals) so product theory
    stays short? Which path owns that file?
11. **Possession test** — What surprises (bugs or bad agent paths) would
    force a theory revision rather than a local patch?
12. **Replacement check** — If this were only a README restatement, what
    judgment would still be missing?

Stop when the owner can **place a feature**, **refuse a category error**,
and **predict a failure mode** without re-deriving the product from scratch.

### 3. Choose extension shape

| Situation | Action |
|-----------|--------|
| No product theory yet | Crystallize **product** `docs/program-theory.md` first |
| Product theory exists; one module needs deep identity | **Extend** with module theory; do not bulk product theory |
| Answers mix product-wide and local depth | Split: thin product apex + module file(s) |
| Content is exact rules only | Stop; write or update a **contract** (spec/fixture), not theory |

If extending via module theory, require: ownership table, generative model or
equivalent, local non-goals, required action on entry, “no sibling module
theory required,” and promotion path for any product-scope ALT parked there.

### 4. Draft

**Product** — write `docs/program-theory.md` with at least:

- Status / Owner / Boundary / Verification / Required action
- What program theory means (short; link Naur)
- Purpose and desired feel; whole-program mental model
- Core concepts and ownership; principles; non-goals; falsifiers
- How the model is extended (pointer to module theory convention)
- Revisions/ALT if decided; related contracts

Choose references on two tests: **accurately represented** and
**load-bearing**. A load-bearing reference contributes a concept,
distinction, refusal, or vocabulary the theory actually uses. Make
that contribution explicit where it would otherwise be unclear; omit
ornamental citations. Treat any register effect as a drafting
hypothesis, never as evidence that the theory or its outputs are
correct ([REV-AT-003]).

**Module** — write `<module>/MODULE-THEORY.md` (or agreed path) with at least:

- Status / Owner / Boundary / Verification / Required action (“read on entry
  to … before changing …”)
- Specializes: product program theory (link)
- What this document is / is not (contracts vs theory)
- Ownership table (owner / consumer / propagator)
- What the module owns; what it is not
- Generative model or design consequences
- Tensions and falsifiers; REV/ALT records
- Related contracts; promotion notes for product-scope items

Use stable section codes if the repo already uses them (`[THEORY-n]`,
`[MODULE-n]`, etc.).

### 5. Wire entry (if missing)

- **Product:** theory early in `context.index.yaml` `read_order` for
  product-scope repository work.
- **Module:** document entry in product theory and/or AGENTS (“when touching
  `path/…`, read `…/MODULE-THEORY.md`”). **Do not** put every module theory
  on global session start.

Do not force full process read order onto product-use-only paths.

### 6. On refuse or revise — write a decision record

If the interview **refuses or defers** a competent proposal, or **changes** the
working model, append a filled ALT or REV using the field templates in this
skill. Do this **when it happens**, not as a quota of empty records.

Do not create empty `[ALT-001]` placeholders or decision-case directories.

### 7. Confirm

Ask the owner to test possession:

- Place one hypothetical feature.
- Refuse one category error.
- Name one surprise that would revise the theory.

For a **product apex**, also walk the fuller owner's checklist — the nine
understandings the founding dialogue named (origin: the hub's lore record
of the 2026-07-29 backstitch README dialogue). The owner should be able to
state, without reading the draft back:

1. what concepts exist;
2. what each concept means;
3. which component owns each behavior;
4. why the boundaries are where they are;
5. which alternatives were rejected and why;
6. what must remain invariant;
7. what the system explicitly does not do;
8. how a proposed change propagates through the rest of the model;
9. what evidence would show the current understanding is wrong.

Any item the owner cannot answer marks a section that is not yet theory:
interview further there, or keep the account's Status at `Draft` and record
the gap. Items 4 and 6 are the ones the spine reaches least directly — probe
them explicitly rather than assuming Q3/Q8 covered them.

Revise the draft until they confirm.

## Decision record templates

### ALT (rejected / deferred / …)

```markdown
### [ALT-<SCOPE>-<NNN>] Short title

Disposition: rejected | deferred | adopted | superseded | invalidated
Owner: <decision owner>
Governs: <theory section, module theory, or contract reference>
Source record: none | <plan path> | <commit or issue>
Candidate: <what was proposed>
Why plausible: <steelman>
Evidence:
- contemporaneous | owner-recalled | inferred | unknown: <source>
Reason: <why this disposition>
Current consequence: <what work must do now>
Reconsider when: <observable condition — not vibes>
Promoted to: none | <id if moved upstairs or into a contract>
```

### REV (theory change)

```markdown
### [REV-<SCOPE>-<NNN>] Short title

Current account: <revised theory — put current first>
Supersedes: <short prior account; do not let it compete with current>
Pressure: <what made the prior account inadequate>
Evidence:
- contemporaneous | owner-recalled | inferred | unknown: <source>
```

## Output Standard

- Written theory file at the agreed path.
- Explicit non-goals and ownership table.
- Entry-order note or patch so the file is loadable by agents.
- Any **real** ALT/REV from the interview filled in full; zero empty slots.
- List of follow-ups (spec promotions, parked ALTs) without implementing them
  unless asked.

## Anti-patterns

- Copying the hub's agent-theory program-theory into a product repo as-is.
- Stuffing module depth into product theory “so agents see it,” instead of
  **extending** with module theory and entry wiring.
- Writing module theory that requires three sibling theories to place work
  (broken ownership table).
- Turning the interview into a full architecture dump or API catalog.
- Asking multiple independent questions in one turn.
- Acting on implementation before shared understanding is confirmed.
- Treating the written file as proof of possession without the placement/refusal
  test.
- Pre-creating empty ALT/REV records or decision directories “for later.”

## Module-theory drafting checklist

When extending via module theory, require:

1. File next to owning code (conventional `MODULE-THEORY.md`) or an agreed path
   documented from product theory / AGENTS.
2. Metadata: Status, Owner, Boundary, Verification, Required action
   (“read on entry to … before changing …”).
3. Ownership table (owner / consumer / propagator). Two modules claiming
   owner of the same concept is a gate failure.
4. Generative model or design consequences; local non-goals.
5. Point exact rules at contracts/fixtures; do not duplicate them as theory.
6. Local ALT/REV when real; park product-scope ALTs for promotion.
7. Wire entry: agents working in that tree read module theory **after** product
   theory (or overview) and **before** changing the module — not on every
   global session start.
8. Actionable without reading sibling modules.

## Maintenance Notes

- This skill is the sole home for the interview spine, ALT/REV field templates,
  module drafting checklist, and entry-wiring procedure ([AT-THEORY-8] by
  reference). Do not re-expand those into hub `docs/program-theory.md`.
- If interviews always need the same extra domain questions, promote them into
  this skill.
- If consumers keep shipping without replacing the stub, strengthen bootstrap
  messaging and AGENTS required-action language.
