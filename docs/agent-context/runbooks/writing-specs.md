# Writing Specs

Specs define intended behavior. They are the source of truth for what the
system should do, not a narration of how the current code happens to work.

## Purpose

Use specs to document:

- system behavior and user-visible outcomes
- invariants and boundaries
- interfaces, contracts, and data shapes
- failure modes and edge cases
- verification expectations

Do not use specs for temporary implementation notes or task checklists.

Write specs so a strong zero-context agent can use them reliably, not just so a
human reader finds them reasonable.

Agent-usable spec writing should make these explicit whenever they matter:

- owner
- boundary
- verification
- required action

## File Placement

- Put specs in `docs/specs/`.
- Prefer stable filenames. Numbered prefixes are useful when the directory is a
  long-lived corpus.
- Add a `README.md` in the directory to explain the reading order and naming
  scheme.

## Reference Codes

Specs should use stable reference codes such as `[DOM-1]`, `[API-4]`, or
`[AUTH-2.3]`.

Rules:

- use codes on the requirements people will need to cite later
- prefer stable codes over prose-only references
- extend the existing code family instead of inventing a second style
- update backlinks when a section is split or replaced

## Recommended Spec Sections

### 1. Purpose and Scope

Explain what the spec governs and what it does not.

### 2. Mental Model

Describe the core concepts needed to reason about the system correctly.

### 3. Requirements

State the intended behavior using stable section or requirement codes.

### 4. Invariants and Constraints

Call out what must remain true even as the implementation evolves.

### 5. Interfaces and Data Contracts

Describe public behavior, payloads, state transitions, or file formats as
needed.

### 6. Failure Modes and Edge Cases

State what should happen under conflict, error, unsupported input, timing race,
or partial failure.

### 7. Verification Expectations

Name the evidence required to prove the behavior.

### 8. Related Plans

Link dated plans in `docs/plans/` that implement or materially revise the spec.

### 9. Status: Two Mechanisms (Prose Header vs Machine Classification)

Spec status can live in **two** places. Do not conflate them.

**Prose `Status:` header (adoption / authoring).** The `Status:` line at the
top of a spec file (e.g. `Status: Proposed`, `Status: Active`) expresses
whether the spec is adopted for implementation. Traceability tooling
typically does **not** read this header: a `Status: Proposed` file can still
be scanned as active.

**Machine classification (scanner behavior, usually per-file).** Checkers
such as backstitch classify whole files via configuration globs
(`planned_spec_globs`, `exploratory_spec_globs`); shipped code citing a
classified file gets warning-class findings instead of full graph rules.
There is usually **no per-section classification**: you cannot mark one
paragraph of an active file as planned without downgrading the whole file.
Many repos ship with these patterns empty — every scanned spec file is
active until configuration adds them.

Choosing a mechanism:

| Situation | Use |
|-----------|-----|
| Whole spec not yet adopted | Prose `Status: Proposed` + explicit out-of-scope for implementation; accept info-class unmapped debt or a meta classification if links must not be required |
| Substantial in-flight behavior in a **new** file | New file path under a planned/exploratory classification (name the config change) |
| Paragraph edit inside an existing active file | Promotion strategy A or B in `runbooks/writing-plans.md` §4d — not a reclassification of the parent file |

**Owner:** spec/plan author names mechanism and promotion strategy.
**Boundary:** classification and scanning require the spec tree; plan
directories are not a substitute. **Verification:** the repo's traceability
gate, when one exists. **Required action:** promote draft plan text into the
spec tree in the **spec-promotion slice** (see `writing-plans.md` §4d), not
only in the plan appendix.

### 10. State Contracts as Rules, Not Only Examples

An example (a JSON shape, a sample record, a fenced transcript) shows one
valid instance; a rule states the property every instance must satisfy
("all identifiers non-blank", "every enumerated vocabulary closed",
"validation covers the producer's full contract, not the consumer's
projection"). Examples invite implementers and reviewers to check only the
fields they can see being consumed; rules invite a sweep.

The failure mode is measurable: contracts stated only by example get
enforced one field at a time, and reviews rediscover the same missing rule
per field. When a contract has a general property, write the property as a
sentence and keep the example as illustration — the example never replaces
the rule.

Corollary for promoted deltas: when spec text codifies behavior that
already exists, verify each stated rule against the actual implementation
before promotion — a rule drafted from memory overclaims. The independent
reviewer's job includes checking rule-vs-code, not just rule-vs-intent.

## Spec Maintenance Rules

- Update the spec before or with the code change when intended behavior shifts.
  For implementation plans, that means **before code that cites the new
  behavior** — usually as the first implementation slice after review,
  using the plan's `## Proposed Spec Delta` and promotion strategy (see
  `runbooks/writing-plans.md` §4c–4d).
- Keep `## Related Plans` current.
- If an implementation note exists for the touched area, update it in the same
  change.
- If no spec exists for material new behavior, add one instead of burying the
  decision in a plan or commit message.
- If a change is intentionally spec-free, make that explicit in the plan.
- If a section is understandable to a human but likely ambiguous to an agent,
  rewrite it with clearer structure, references, examples, or explicit
  boundaries.
- When you notice that kind of ambiguity during work, notify the user and
  suggest a concrete improvement.

## Anti-Patterns

- specs that only describe current file layout
- vague requirements with no stable references
- prose that sounds clear in discussion but leaves an agent guessing about the
  boundary, owner, or required action
- missing verification guidance for an otherwise clear requirement
- mixing active task checklists into the spec body
- documenting speculative future behavior as if it were required now
- letting the spec drift while code and plans move underneath it
- leaving proposed spec text only in a plan while shipped code cites spec
  paths — that yields missing-section errors or invisible debt
- parking speculative behavior only in plan directories when it should be
  an exploratory spec file — traceability tooling cannot report what it
  does not scan
- treating prose `Status: Proposed` as if it were a machine classification —
  the scanner may still treat the file as active
- reclassifying an existing active spec file as planned/exploratory just to
  stage one section — classification is per-file
- stating a contract only by example (a JSON shape with fields) when it has
  a general property — by-example contracts get enforced and reviewed one
  field at a time
- promoting rule-form spec text without verifying each rule against what
  the implementation actually enforces — memory-drafted rules overclaim
