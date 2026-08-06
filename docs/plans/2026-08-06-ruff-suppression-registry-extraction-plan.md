# Ruff Suppression Registry Extraction Plan

Date: 2026-08-06
Status: completed
Class: 5+P — changes normative [DOM-10.1.1] ownership and the repository's
suppression gate interface. The implementation is a behavior-neutral
documentation-architecture change. No public package, runtime, storage,
async, persistence, rollout, or one-way-door trigger fires.
`hardening: N/A — no risky trigger`.
Plan type: implementation with spec revision.
Promotion strategy: B — promote the exact [DOM-10.1.1] replacement atomically
with the registry, parser, and tests because no intermediate mixed-owner state
is valid.

## Spec Baseline

`b6c5c4602c36f3cb2d63f84a8638c35a33e6cbdd` is the reviewed baseline for
`docs/specs/01-development-documentation-operating-model.md`. The current
[DOM-10.1.1] registry table, global inventory, and generated block at that SHA
are the conservation baseline for this move.

## Goal

Keep Ruff suppression policy in the required development operating-model spec,
but move the human approval registry and generated location inventory to a
task-scoped implementation document. A contributor should read the registry
only when proposing, reviewing, regrouping, regenerating, or auditing a
suppression.

## Source Documents

- `docs/specs/01-development-documentation-operating-model.md` [DOM-10.1],
  [DOM-10.1.1], [DOM-15]
- `docs/implementation/00-implementation-index.md`
- `docs/implementation/02-repository-map.md`
- `docs/plans/2026-07-30-ruff-suppression-index-generator-plan.md`
- `bin/ruff_suppression_index.py`
- `tests/test_ruff_policy.py`
- `tests/test_ruff_suppression_index.py`
- Theory: [THEORY-4] — simplicity is measured through coherent ownership and
  use surfaces; required reading should expose the policy interface without
  forcing readers through operational state they do not need.

## Context and Key Files

Today [DOM-10.1.1] owns both the normative policy and a large operational
ledger. `bin/ruff_suppression_index.py` defaults to parsing that spec, validates
the human rows and global raw-`noqa` aggregate, and owns only the delimited
generated location block. `tests/test_ruff_policy.py` invokes its Python
interface; `tests/test_ruff_suppression_index.py` exercises the CLI and writer.
The implementation index's numbered starting path is recommended reading, so
the extracted registry must be listed separately as task-scoped material.

Files to modify or add:

- `docs/specs/01-development-documentation-operating-model.md`
- `docs/implementation/10-ruff-suppression-registry.md` (new)
- `docs/implementation/00-implementation-index.md`
- `docs/implementation/02-repository-map.md`
- `bin/ruff_suppression_index.py`
- `tests/test_ruff_policy.py`
- `tests/test_ruff_suppression_index.py`
- this plan and `docs/plans/README.md`

Comprehension checks before editing:

1. Can the checker still distinguish human approvals from its generated block
   without writing any rationale or cardinality?
2. Can a contributor learn the default-refactor policy and the exact required
   source marker without opening the registry?

## Ownership Delta

- [DOM-10.1.1] remains the normative owner of the default-refactor rule,
  exception standard, exact source marker, review requirement, and the pointer
  to the task-scoped registry and its verification commands.
- `docs/implementation/10-ruff-suppression-registry.md` becomes the durable
  owner of group approvals, approved cardinalities, rationales, the global raw
  inventory, and the generated location index.
- `bin/ruff_suppression_index.py` exposes this boundary as a registry, not a
  spec: its default path and CLI/Python parameter names use `registry`.
- The implementation index and repository map identify the document as
  task-scoped operational state, outside the required/recommended reading
  sequence.

## Proposed Spec Delta

Replace the complete current `#### Approved Ruff Suppression Registry
[DOM-10.1.1]` section, through the paragraph ending "Unexpected programming
errors retain their traceback as bug evidence.", with exactly:

> #### Ruff Suppression Exceptions [DOM-10.1.1]
>
> Refactor the code by default. Adopt a local suppression only when the
> smallest behavior-preserving refactor would be a net negative for
> understandability, locality, and readability, or would change a protected
> invariant. A lower lint score or smaller function is not sufficient reason.
>
> Owner: this section owns suppression-adoption policy and source-marker
> grammar. The task-scoped
> `docs/implementation/10-ruff-suppression-registry.md` owns approved groups,
> cardinalities, rationales, the global raw-`noqa` inventory, and the generated
> location index. Boundary: local Ruff suppressions in first-party Python
> files. Verification: `ruff check .`, `RUF100`, and
> `uv run --frozen --no-sync python bin/ruff_suppression_index.py --check`.
> Required action: obtain explicit review before adding or regrouping a
> suppression; update the source pointer and task-scoped registry in the same
> change; then regenerate with
> `uv run --frozen --no-sync python bin/ruff_suppression_index.py --write`.
>
> An approved exception uses exactly `# noqa: <codes> approved [DOM-10.1.1]
> [RUFF-SUP-NNN] exception`. Its registry row records the stable group ID,
> allowed rules and cardinalities, protected invariant, real proof, rejected
> alternatives, and approval. The checker reconciles those human approvals,
> every tagged source directive, raw Ruff diagnostics, the complete global
> aggregate, and the generated location index. The generator may replace only
> its delimited generated block; it must never create or edit human approval
> evidence. Unreadable or malformed discovered Python input aborts without a
> partial write and uses the clean exit-2 tool-failure boundary.
>
> The registry is operational evidence, not required startup or spec reading.
> Consult it only when proposing, reviewing, regrouping, regenerating, or
> auditing a suppression.

Under the spec's `Local plans:` list, add:

> - `docs/plans/2026-08-06-ruff-suppression-registry-extraction-plan.md`

## Invariants and Constraints

- Preserve every existing group ID, rule set, approval, rationale,
  cardinality, global raw count, and generated location row. This change does
  not approve, reject, add, or remove any suppression.
- Preserve source markers in the exact form documented by [DOM-10.1.1].
- Preserve no-argument `--check` and `--write` commands. Rename the explicit
  path option and Python parameter from `spec` to `registry` so the interface
  states the new ownership correctly.
- The writer may replace only the delimited generated block. It must not edit
  human approval rows or the global aggregate.
- The required spec must state the default clearly: refactor unless doing so
  is a net negative for understandability, locality, and readability or would
  change protected behavior. The registry is consulted only for suppression
  work.
- Do not introduce a new general documentation tier or alter Ruff rule
  selection, lint discovery, CI behavior, package behavior, or source
  suppressions.
- Stop and re-evaluate if the move requires a second parser, changes approved
  inventory, or makes the registry part of a startup/read-order list.

## Deviation Log

None.

## Tasks

1. Add ownership tests that require the default registry path to be outside
   `docs/specs/`, require [DOM-10.1.1] to point to it without embedding the
   human table or generated markers, and require both direct commands in the
   task-scoped registry. Assert that the registry appears only in a
   task-scoped section of `docs/implementation/00-implementation-index.md`, is
   absent from its numbered Recommended Starting Points, and is absent from
   `docs/agent-context/context.index.yaml`'s `read_order`.
2. Move the complete human registry, raw inventory, and generated block to
   `docs/implementation/10-ruff-suppression-registry.md`. Replace the large
   spec section with concise normative policy and an explicit task-scoped
   pointer.
3. Rename the generator's default/path interface from `spec` to `registry`,
   update tests and errors, and retain the no-argument commands. Assert that
   `--help` exposes `--registry` and that the removed `--spec` option is
   rejected.
4. Add the registry to a task-scoped section of the implementation index and
   repository map. Do not add it to the recommended starting sequence. Add
   this plan to the touched spec's `## Related Plans` list.
5. Compare the moved human table, global inventory, and generated block with
   `git show b6c5c4602c36f3cb2d63f84a8638c35a33e6cbdd:docs/specs/01-development-documentation-operating-model.md`;
   all three must match exactly before running the writer.
6. Run targeted and repository gates; obtain an independent completion
   review; incorporate or disposition findings; close the plan row in the
   completion change.

## Testing Plan

- Real parser and writer only; do not mock filesystem parsing or Ruff output.
- Red/green ownership proof in `tests/test_ruff_suppression_index.py`.
- Baseline conservation proof: extract the human table, global inventory, and
  generated block from the baseline spec and new registry with the same
  delimiters, then compare them byte-for-byte before `--write`.
- `uv run --frozen --no-sync python bin/ruff_suppression_index.py --check`
- `uv run pytest tests/test_ruff_policy.py tests/test_ruff_suppression_index.py -q`
- `uv run ruff check bin/ruff_suppression_index.py tests/test_ruff_policy.py tests/test_ruff_suppression_index.py`
- `uv run ruff format --check bin/ruff_suppression_index.py tests/test_ruff_policy.py tests/test_ruff_suppression_index.py`
- `python3 bin/check-dom15-fixtures`
- `bin/check-doc-paths`
- `bin/check-plan-context`
- `bin/coalesce-check`

## Verification and Gates

Acceptance requires exact inventory reconciliation from the moved file; a
byte-stable `--write` rerun; green targeted tests, Ruff, and document gates;
and diff inspection proving that the spec retains policy but not registry
state. Any changed inventory is a blocker, not an expected regeneration.

## Rollback and Rollout

This is an atomic repository-local move with no runtime rollout. Rollback is
the inverse move of the registry contents and default parser path in one
commit. Partial rollout is invalid because the parser, tests, and owner
document must agree.

## Out of Scope

- Re-evaluating existing suppressions or their rationales.
- Changing Ruff configuration, source directives, or approved counts.
- General reorganization of specs or implementation documentation.
- Compatibility aliases for an undocumented explicit `--spec` tool option.

## Review Log

- Independent plan review initially blocked implementation on three gaps: no
  frozen spec delta/baseline, no structural required-reading proof, and no
  reproducible inventory-conservation check. The plan added all three plus the
  reciprocal Related Plans and CLI-removal checks; re-review passed.
- Independent completion review confirmed exact inventory conservation and
  plan conformance. Its sole LOW finding found stale "spec registry" wording
  in the moved failure contract; the text now says "registry document."

## Verification Results

- Frozen-baseline comparison: the human table, global raw inventory, and
  generated block matched
  `b6c5c4602c36f3cb2d63f84a8638c35a33e6cbdd` byte-for-byte.
- Generator: `--check` passed; `--write` was byte-stable at SHA-256
  `aaa212a08366348c26112b321b2226566edd98dcb531a93a8af07678e38a93d8`.
- Targeted policy/index suite: 40 tests passed.
- Ruff check and format check: three touched Python files passed.
- Documentation gates: DOM-15 fixtures, doc paths, plan context, and
  coalescing passed; `tests/test_doc_gates.py` passed 3 tests.
- Coalescing reported 16 SHA claims, 0 retrieval cues, and 29 dated lessons;
  its three foreign claims and two local-only pins remain pre-existing
  informational state, not failures from this change.

## Completion Gate

Before claiming completion: record concrete command results and independent
review disposition; set this plan to `completed`; update its Status Index row
in the same change; commit the exact reviewed file set; and verify the commit
with `git log` and a clean worktree.
