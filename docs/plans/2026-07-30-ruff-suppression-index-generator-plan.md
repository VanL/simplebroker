# Ruff Suppression Index Generator Plan

Date: 2026-07-30
Status: completed — the original stable-group generator and Revision R1's
symbol-keyed location index landed.
Class: 5+P. The base class is 5 because the implementation revises normative
[DOM-10.1] and [DOM-10.1.1] verification policy. The `+P` modifier applies
because the change materially alters how future suppressions are registered,
reviewed, and verified. The new repository-tool CLI also fires the [DOM-5]
risky CLI-shape trigger, so the hardening checklist applies.
Plan type: implementation with spec revision
Hardening: required

## Goal

Replace the manually maintained, line-sensitive Ruff suppression location
inventory with an in-repository scanner and deterministic generator. Keep
suppression reasons, proofs, rejected alternatives, and approvals human-owned
in [DOM-10.1.1]. Give each approved group a stable ID, carry that ID in every
local source directive, and generate only the derived path, line, directive,
and raw-diagnostic index.

The tool must make code movement cheap without making suppression adoption
automatic. It may refresh evidence for an already approved group; it must
never create a group, reason, proof, rejected alternative, or approval.

## Requested Outcomes

- [x] Replace location-based group identity with 34 stable suppression-group
  IDs for the current registry.
- [x] Extend every approved source directive with exactly one stable group ID.
- [x] Add an importable repository tool with deterministic `--check` and
  `--write` modes.
- [x] Generate a machine-owned location index inside explicit markers in
  [DOM-10.1.1], while preserving every byte outside those markers.
- [x] Replace the global approved-directive count with human-owned, per-group
  approved cardinalities. Derive actual group cardinalities and fail when an
  existing group grows or shrinks without an explicit human-table edit.
- [x] Retain a separate movement-stable global raw-diagnostic inventory for
  every local `noqa`, including reasoned suppressions outside the approved
  registry. This is an aggregate tripwire by rule code, not an identity
  registry: a same-code remove/add swap cannot be distinguished by the
  aggregate alone.
- [x] Keep exact agreement among Ruff discovery, Python comment tokens,
  human-owned groups, the generated index, and raw Ruff diagnostics: tagged
  findings reconcile against the registry, while all findings reconcile
  against the global raw-diagnostic inventory.
- [x] Refuse to write when a suppression is malformed, unapproved, orphaned,
  stale, or not backed by a raw diagnostic.
- [x] Put the check mode in the repository lint gate and retain the focused
  policy test in the normal pytest suite.
- [x] Document the tool owner, regeneration command, edit boundary, and
  rollback path.
- [x] Change no lint selection, complexity threshold, product behavior, public
  package API, or approved suppression rationale.

## Revision R1 — Symbol-Keyed Location Index (2026-07-31)

Same class (5+P) and same hardening posture as the original scope: this
revises normative [DOM-10.1.1] text and changes what the verification gate can
detect.

### Trigger

The completed implementation keys the derived index on `path:line`. Two
failure modes surfaced in use on the same day, during the 5.7.0 release work.

**Churn.** Any edit above a directive moves its line, so the index goes stale
and `--check` fails on changes that alter no suppression. Observed six times in
one session, never once from a real lint failure — `ruff check .` passed every
time. The round-1 review accepted "deterministic generated churn" as the price
of offline auditability. In practice the churn is frequent enough that a
reviewer learns to skim the index diff, which erodes the auditability it was
bought for.

**Blindness — the decisive one.** Line keying does not detect the failure it
appears to guard. Simplify function A so it no longer trips a rule (one
directive removed), then add function B with a `noqa` copied from the
surrounding convention (one directive added). Per-group cardinality is
unchanged, so that tripwire does not fire. The index diff shows only line
movement, indistinguishable from the unrelated churn above. A suppression has
migrated from a reviewed site to an unreviewed one, silently. This is the
expected failure mode in an agent-populated codebase, where copying an
adjacent `noqa` is the path of least resistance.

Symbol keying fixes both: stable under edits that move code without changing
suppressions, and it renders a *set of sites*, so A disappearing and B
appearing is a visible `-`/`+` pair in review.

### Rejected alternatives

- **`(path, rule, count)`.** Kills churn but is strictly blinder than lines: it
  cannot see the A→B migration at all, because the count is what stays constant.
- **Auto-regenerating the index on every pytest run.** Considered on the ground
  that the substantive gates — unclean `ruff check`, malformed directive,
  unknown group, group does not approve rule, cardinality mismatch — are all
  upstream of the index, so auto-writing would not weaken them. That reasoning
  is correct, but it addresses only churn and leaves blindness untouched. With
  symbol keying the remaining regeneration events are precisely the ones worth
  seeing in review, so silent regeneration is no longer desirable.
- **Bare function names.** Insufficient: `simplebroker/db.py` has 12 duplicate
  bare names (`__init__` ×6, `close` ×4, `__enter__` ×4) and
  `simplebroker/watcher.py` has 6 (`_drain_queue` ×3). Unqualified names would
  merge distinct sites into one entry and reintroduce the blindness inside it.

### Requested outcomes

- [x] Key the derived location index on `path::qualified_symbol` instead of
  `path:line`, rendering one entry per distinct site.
- [x] Attribute a directive to its outermost enclosing `def`, qualified by
  enclosing class names; module-level directives render `<module>`. Decorator
  lines count as inside the function they decorate.
- [x] Retain `line` as internal identity for raw-diagnostic reconciliation,
  duplicate detection, and error messages. Only the rendered index changes.
- [x] Revise the single [DOM-10.1.1] sentence granting the generated index its
  scope.
- [x] Preserve every existing gate: unclean-ruff, malformed directive, unknown
  group, rule-not-approved, per-group cardinality, and the global raw
  inventory are all unchanged.

### R1 execution evidence

Both halves of the claim were measured against the live repository rather than
argued:

| Scenario | `--check` exit | Meaning |
|----------|---------------:|---------|
| Comment inserted mid-file, `ruff check` still clean (shifts every directive below it) | 0 | Stable under line movement — the churn R1 removes |
| A suppressed function renamed, cardinality unchanged | 1 | A suppression at a new symbol is detected — the blindness R1 closes |

An earlier attempt inserted blank lines into the import block and failed;
diagnosis showed the failure was the unclean-`ruff` gate, not index staleness,
which is itself evidence the upstream gates still fire independently.

`tests/test_ruff_suppression_index.py` fixture expectations moved from
`probe.py:4` to `probe.py::contain_failure`, and the `PYI036` case now asserts
`probe.py::Context.__exit__`, which pins class qualification. One assertion in
`test_syntactically_invalid_source_is_an_unverifiable_exit_two` was changed
from CPython's `invalid syntax` phrasing to the tool's own
`could not read Python source`: the parse path moved from `tokenize` to `ast`,
which raises the same `SyntaxError` type with different wording. Exit code,
named path, no-traceback, and no-partial-write behavior are unchanged.

### Proposed spec delta

Exactly one sentence in [DOM-10.1.1]:

- Before: "The generated location index owns only derived paths, **lines**, and
  actual cardinalities."
- After: "The generated location index owns only derived paths, **symbols**,
  and actual cardinalities."

### Known residual

Two directives for the same rule inside the *same* qualified symbol, one
removed and one added, remain invisible: the site set and the cardinality both
hold constant. This is strictly narrower than the A→B case R1 closes, and it
does not match the copied-convention failure mode. Accepted rather than paying
for a per-symbol count, which would make the rendered table a count list again
rather than a readable set of sites.

## Source Documents

- User instruction in the originating session: create a plan for an in-repo
  tool that scans and regenerates the brittle suppression index.
- `docs/specs/01-development-documentation-operating-model.md`
  [DOM-5], [DOM-6], [DOM-10.1], [DOM-10.1.1], [DOM-11], and [DOM-15].
- `docs/agent-context/engineering-principles.md`, especially principles 4,
  8, 9, 10, 12, and 13.
- `docs/agent-context/runbooks/writing-plans.md`.
- `docs/agent-context/runbooks/hardening-plans.md`.
- `docs/agent-context/runbooks/testing-patterns.md`.
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`.
- `docs/agent-context/runbooks/maintaining-traceability.md`.
- `docs/plans/2026-07-29-ruff-lint-expansion-plan.md`, which introduced the
  general suppression registry and policy test.
- `docs/plans/2026-07-29-complexity-and-state-machine-hardening-plan.md`,
  which activated C901 and expanded the registry to the current 34 groups.
- `docs/implementation/07-complexity-and-state-machine-map.md`, which explains
  why the semantic registry is not a baseline allowlist.

## Spec Baseline

- Committed baseline: `84fcc5706834fd85115ab404c1beae47ab9f4e08`.
- Plan-authoring worktree baseline for
  `docs/specs/01-development-documentation-operating-model.md`:
  blob `46cbb23ef16bd6eec04caeb1617acfea4c2f01dd`.
- The worktree baseline already contains unrelated, uncommitted line-number
  refreshes in [DOM-10.1.1] caused by active Redis and write-path work. Those
  edits belong to the existing work and must not be overwritten. Before
  implementation, re-read and re-hash the spec, source directives, and policy
  test. Record that identifier as the implementation baseline.
- Promotion baseline: pending. Record the commit SHA, or diff base plus
  worktree blobs, after the atomic spec/migration/tool slice.

## Current Evidence

The existing registry fails when semantics stay fixed but nearby code moves.
At plan authoring time:

```text
uv run --frozen --no-sync pytest -q \
  tests/test_ruff_policy.py::test_approved_suppressions_match_the_spec_registry
```

fails because source directives moved while the manually maintained locations
did not all move with them. The mismatch includes current `db.py` and
`commands.py` locations. The same worktree already contains manual registry
edits for other moved Redis and dump/load locations. This is the concrete
failure the new tool must reproduce in an isolated red test and eliminate by
regeneration.

Current structure:

- [DOM-10.1.1] has 34 human-authored rows. Each row combines derived locations
  and counts with durable rationale, proof, rejected alternatives, and
  approval.
- Approved comments use the generic pointer
  `approved [DOM-10.1.1] exception`; they do not identify which rationale row
  owns them.
- `tests/test_ruff_policy.py` independently parses the Markdown table, scans
  Python `COMMENT` tokens, invokes real Ruff, and hard-codes total diagnostic
  and directive counts.
- Ruff's JSON `noqa_row` is already the correct physical-line source for raw
  diagnostics. One source directive can correspond to several raw diagnostics,
  so set equality alone is insufficient; multiplicity must be retained.
- Raw Ruff currently also reports two reasoned `BLE001` suppressions outside
  the approved registry:
  `examples/tests/test_multi_queue_pattern_transitions.py:383` and
  `examples/tests/test_reference_reactor_transitions.py:244`. They remain
  governed by [DOM-10.1]'s local-reason and review rule, not by a
  `[RUFF-SUP-NNN]` group. The complete global raw-diagnostic inventory catches
  aggregate count changes across both registered and non-registered local
  suppressions and must remain a separate policy tripwire. It includes the
  existing non-registry `E402` and `F401` suppressions, which the old
  five-family test constant omitted.

The location index remains committed rather than existing only as live test
output because the user explicitly finds the index useful. It provides an
offline, reviewable footprint of every approved exception without requiring a
Ruff run, and a location diff makes suppression movement visible in the same
change. The generator removes manual line maintenance; it does not remove the
audit surface.

## Context and Key Files

### Files to create

- `bin/ruff_suppression_index.py`: one importable owner for discovery, source
  comment parsing, spec parsing, raw-Ruff reconciliation, deterministic
  rendering, CLI exit behavior, and atomic replacement.
- `tests/test_ruff_suppression_index.py`: focused unit and black-box tests for
  scan, check, write, failure classification, and hostile inputs.

### Files to modify

- `docs/specs/01-development-documentation-operating-model.md`: promote the
  [DOM-10.1]/[DOM-10.1.1] contract, add stable IDs to the human registry,
  replace derived location cells with the generated index, document the
  command, and backlink this plan.
- Every Python source file containing
  `approved [DOM-10.1.1] exception`: add the owning `[RUFF-SUP-NNN]` ID to
  the existing comment. Change no code or selected rule while doing so.
- `tests/test_ruff_policy.py`: reuse the tool's parser/reconciler, remove the
  duplicate Markdown/source scanner and hard-coded suppression counts, and
  assert the committed index is current.
- `.github/workflows/test.yml`: run check mode in the root lint job after
  normal Ruff.
- `docs/implementation/02-repository-map.md`: list the new tool and its
  purpose.
- `docs/implementation/07-complexity-and-state-machine-map.md`: replace the
  line-sensitive-source wording with stable-group and generated-index
  ownership.
- `docs/plans/README.md`: keep this plan's state current.

### Existing paths to reuse

- Use `ruff check --show-files .` as the discovery owner. Do not create a
  competing tracked-file scanner.
- Use Python's `tokenize` module so marker-like string literals do not become
  directives.
- Use Ruff's real JSON output from
  `ruff check --ignore-noqa --output-format json .`; do not reproduce Ruff
  rule logic.
- Follow `bin/coverage_combine.py` for an importable `bin` module and the
  existing repository tools for truthful exit classes and no-traceback CLI
  handling.
- Use `PurePath.as_posix()` for committed paths so generated output is stable
  on Windows.

### Comprehension gates

Before editing, the implementer must be able to answer:

1. Why can neither a Ruff rule code nor a source path reliably identify a
   suppression's human rationale group?
2. Which registry fields are human judgment, and which fields can be derived
   without changing policy?
3. Why must raw diagnostics be compared as a multiset keyed by
   `(path, noqa_row, code)` rather than as a set of locations?
4. Why must `--write` validate the complete semantic graph before replacing
   the generated block?

## Suppression Group Migration

Assign IDs in current registry order. The ID, label, rules, and approved
cardinalities are human-owned. Locations and actual cardinalities are derived.
Preserve the current invariant, proof, rejected-alternative, and approval
cells verbatim during migration.

| ID | Stable label | Rules | Approved directives | Approved raw diagnostics |
|----|--------------|-------|--------------------:|--------------------------|
| `[RUFF-SUP-001]` | public context-manager typing | `PYI034`, `PYI036` | 13 | `PYI034=6`, `PYI036=19` |
| `[RUFF-SUP-002]` | closed-pipe stdout replacement lifetime | `SIM115` | 1 | `SIM115=1` |
| `[RUFF-SUP-003]` | agent-facing CLI and tool boundaries | `BLE001` | 12 | `BLE001=12` |
| `[RUFF-SUP-004]` | long-lived worker containment | `BLE001` | 6 | `BLE001=6` |
| `[RUFF-SUP-005]` | best-effort resource cleanup | `BLE001` | 5 | `BLE001=5` |
| `[RUFF-SUP-006]` | transaction arbitration and cleanup precedence | `BLE001` | 3 | `BLE001=3` |
| `[RUFF-SUP-007]` | concurrency probe outcome capture | `BLE001` | 60 | `BLE001=60` |
| `[RUFF-SUP-008]` | test-harness diagnostic and cleanup boundaries | `BLE001` | 12 | `BLE001=12` |
| `[RUFF-SUP-009]` | SQLite schema, validation, and dump/load order | `C901` | 3 | `C901=3` |
| `[RUFF-SUP-010]` | Darwin discovery and phase completion | `C901` | 2 | `C901=2` |
| `[RUFF-SUP-011]` | retry algorithm | `C901` | 1 | `C901=1` |
| `[RUFF-SUP-012]` | pytest override parser | `C901` | 1 | `C901=1` |
| `[RUFF-SUP-013]` | timestamp parser family | `C901` | 2 | `C901=2` |
| `[RUFF-SUP-014]` | queue fetch | `C901` | 1 | `C901=1` |
| `[RUFF-SUP-015]` | sidecar and transactional generators | `C901` | 2 | `C901=2` |
| `[RUFF-SUP-016]` | queue move | `C901` | 1 | `C901=1` |
| `[RUFF-SUP-017]` | watcher polling | `C901` | 1 | `C901=1` |
| `[RUFF-SUP-018]` | PostgreSQL vacuum | `C901` | 1 | `C901=1` |
| `[RUFF-SUP-019]` | PostgreSQL listener | `C901` | 1 | `C901=1` |
| `[RUFF-SUP-020]` | Redis bounded scan and cleanup | `C901` | 2 | `C901=2` |
| `[RUFF-SUP-021]` | Redis broadcast | `C901` | 1 | `C901=1` |
| `[RUFF-SUP-022]` | PostgreSQL vacuum integration proof | `C901` | 1 | `C901=1` |
| `[RUFF-SUP-023]` | release workflows | `C901` | 3 | `C901=3` |
| `[RUFF-SUP-024]` | multi-queue example patterns | `C901` | 2 | `C901=2` |
| `[RUFF-SUP-025]` | reference-reactor draining | `C901` | 1 | `C901=1` |
| `[RUFF-SUP-026]` | copyable SQLite example validation | `C901` | 2 | `C901=2` |
| `[RUFF-SUP-027]` | benchmark and backend-resolution proofs | `C901` | 2 | `C901=2` |
| `[RUFF-SUP-028]` | CLI coverage harness | `C901` | 1 | `C901=1` |
| `[RUFF-SUP-029]` | cross-thread generator probes | `C901` | 4 | `C901=4` |
| `[RUFF-SUP-030]` | process-session race proofs | `C901` | 2 | `C901=2` |
| `[RUFF-SUP-031]` | watcher SIGINT probe | `C901` | 2 | `C901=2` |
| `[RUFF-SUP-032]` | watcher burst and jitter proofs | `C901` | 2 | `C901=2` |
| `[RUFF-SUP-033]` | watcher concurrency proofs | `C901` | 5 | `C901=5` |
| `[RUFF-SUP-034]` | watcher multiprocess proofs | `C901` | 7 | `C901=7` |

## Invariants and Constraints

1. Human approval remains the authority. The tool may not add, delete, or edit
   a human registry row.
2. The human table owns stable group ID, allowed rule codes, approved
   directive cardinality, approved raw-diagnostic cardinality by code,
   protected invariant, real proof, rejected alternatives, and approval. The
   generated table owns group ID, locations, and actual cardinalities.
3. Every source directive has exactly one `[RUFF-SUP-NNN]` ID; every ID exists
   exactly once in the human table; every human group has at least one live
   directive; every generated group has the same ID; and actual directive and
   raw-diagnostic cardinalities equal the human-approved cardinalities.
4. Copying an existing ID onto a new suppression is not self-approving. It
   changes actual cardinality, so check mode fails until the human row's
   approved cardinality is deliberately edited and reviewed. Review also owns
   whether the invariant and proof genuinely place the directive in that
   group; the generator verifies structure and cardinality.
5. A source directive's listed codes must equal the raw Ruff codes at that
   physical line after multiplicity is collapsed for the directive. Each raw
   diagnostic remains counted so `PYI036`-style multiple findings cannot
   disappear.
6. Normal Ruff must remain clean. Raw Ruff diagnostics at a tagged registry
   location must map to that approved directive, and each tagged code must have
   at least one raw diagnostic. Raw diagnostics at other locally reasoned
   `noqa` comments are not generated into [DOM-10.1.1]; a separately reviewed
   global raw-diagnostic inventory remains exact by rule code so aggregate
   count changes cannot pass silently. A same-code remove/add swap remains a
   review concern rather than something aggregate counts can identify.
   `RUF100` remains part of normal policy.
7. Ruff discovery is canonical. Do not maintain a second glob, Git inventory,
   or baseline allowlist in the generator.
8. Source scanning uses comment tokens only. Marker text in strings, docstrings,
   test fixtures, Markdown fences, or this plan is inert.
9. The complete human table must precede the begin marker. The parser scopes
   human rows before that marker and generated rows inside the marker pair; a
   generated row can never be parsed as a human approval row.
10. Rendering is deterministic: group ID, then POSIX path, then ascending line;
   codes sorted lexically; path lines compressed only within one path.
11. `--check` is the default and never writes. It exits 0 only when semantic
    reconciliation passes and the generated block is byte-for-byte current.
12. `--write` validates first, changes only the uniquely marked generated
    block, writes a sibling temporary file, flushes it, then uses `os.replace`.
    It removes an abandoned temporary file best-effort after failure.
13. Missing or duplicate generated markers, malformed human rows, unknown IDs,
    orphan groups, malformed source markers, unreadable Python, Ruff invocation
    failure, and invalid JSON are fatal before replacement.
14. Because this tool rewrites one canonical index, an unreadable or malformed
    discovered source aborts the run instead of producing a partial index.
    This intentionally overrides the generic per-file continuation default in
    the adversarial-probe runbook. It must still produce one clean diagnostic,
    no traceback, and no partial write.
15. Exit 0 means current; exit 1 means repository policy mismatch or stale
    generated content; exit 2 means an anticipated invocation, input,
    decoding, or replacement failure. Unexpected programming errors retain
    their traceback and normal nonzero Python exit so defects are not hidden
    behind a new broad-exception suppression.
16. Do not add a dependency. Use the standard library and the already locked
    Ruff executable.
17. Do not change `pyproject.toml` rule selection, Ruff versions, any `noqa`
    rule list, any rationale, or any product/runtime code in the migration.
18. Do not add a second registry file. [DOM-10.1.1] remains the durable source
    of truth for both the human registry and its generated evidence.
19. Stop and re-plan if the implementation needs to infer group membership,
    rewrite human prose, parse Python without `tokenize`, or maintain an
    independent list of scanned files or expected counts.
20. The implementation slice must start from a reconciled worktree. If active
    work still overlaps the spec or any source directive, sequence after that
    work or obtain an explicit rebaseline; never overwrite its line refreshes
    or code changes.

## Error Priorities, Rollout, and Rollback

The command is a repository policy tool, not a product surface.

- A stale generated block or structural policy mismatch is an expected
  repository finding and exits 1.
- An unusable invocation, missing Ruff binary, unreadable spec, invalid Ruff
  JSON, or replacement failure exits 2.
- Anticipated policy, invocation, unverifiable-input, and replacement failures
  print no traceback. Diagnostics name the file, line, group, or command when
  known. Unexpected programming errors retain tracebacks as bug evidence.
- `--write` is all-or-nothing. Validation failure leaves the spec untouched.
- Migration lands atomically with the spec schema, all source group tags, the
  generated block, policy-test adoption, and CI check. There is no supported
  mixed old/new format.
- Rollback reverts that single migration slice. No storage, published package,
  or external compatibility state exists.
- There is no destructive one-way door. The only write target is the marked
  generated block, and Git retains the previous form.
- Post-landing success is observable when ordinary source movement produces a
  stale-index exit 1, `--write` changes only the generated block, and the next
  `--check` plus CI lint gate pass.

## Proposed Spec Delta

Promotion strategy: **B, atomic**. The new source marker grammar and generated
registry format cannot satisfy the old exact-location policy independently.
Promote the following normative changes in the same slice as the scanner,
source-tag migration, generated block, policy-test migration, and CI check.

### [DOM-10.1] replacement for the exact-match paragraphs

> Ruff's `C901` rule is enabled repository-wide with
> `lint.mccabe.max-complexity = 10`. The score is a visibility signal, not a
> design verdict. Each finding must either be simplified around a real
> ownership seam or carry a narrow local `C901` suppression registered in
> [DOM-10.1.1]. The registry must explain why coupling, debugging locality, or
> semantic risk justifies retaining the function; name the real behavioral
> proof; record rejected decompositions and approval; and assign a stable
> suppression-group ID.
>
> The policy gate runs normal Ruff and a raw audit with `--ignore-noqa`.
> Source directives, human-owned [DOM-10.1.1] groups, the generated location
> index, and raw findings at tagged locations using Ruff's reported
> `noqa_row` must reconcile exactly, including each group's human-approved
> directive and raw-diagnostic cardinalities. A new unsuppressed finding, an
> unregistered tagged directive, an unknown or empty group, a cardinality
> change, a stale directive, a stale generated index, or a mismatched raw
> finding at a tagged location fails verification. A separate movement-stable
> global raw-diagnostic inventory continues to cover every local `noqa`,
> including reasoned suppressions outside this registry. It is an exact
> aggregate by rule code: aggregate changes fail verification, while a
> same-code remove/add swap remains visible to source review rather than
> receiving false identity semantics. Per-file ignores, global ignores, and
> baseline allowlists are not permitted. A cohesive parser, checklist, or
> state machine must not be fragmented merely to lower its score.

### [DOM-10.1.1] replacement for the owner and source-pointer paragraphs

> Owner: this section owns each stable suppression group, approved
> cardinalities, and human-reviewed rationale. The local directive owns the
> rule codes and stable group pointer. The generated location index owns only
> derived paths, lines, and actual cardinalities. Boundary: only the rule
> family, cardinality, and invariant approved in the human row. Verification:
> the named real proof, `ruff check .`, `RUF100`, and
> `uv run --frozen --no-sync python bin/ruff_suppression_index.py --check`.
> Required action: obtain explicit review before adding or regrouping a
> suppression; update the human row, approved cardinalities, and source pointer
> in the same change; then regenerate the derived index with
> `uv run --frozen --no-sync python bin/ruff_suppression_index.py --write`.
>
> For an approved exception, the local form is
> `# noqa: <codes> approved [DOM-10.1.1] [RUFF-SUP-NNN] exception`.
> The stable group points to the single durable full reason. Do not duplicate
> the full rationale in source comments. The generator may update only the
> delimited derived index; it must never create or edit an approval,
> invariant, proof, or rejected alternative. A temporary C901 group must also
> name the plan task that removes or re-evaluates it.
>
> The human registry columns are `Group`, `Rules`, `Approved cardinality`,
> `Protected invariant`, `Real proof`, `Rejected alternatives`, and
> `Approval`. Group IDs are unique and match `RUFF-SUP-[0-9]{3}`. Rules list
> the only codes the group may own. Approved cardinality records the permitted
> directive count and raw-diagnostic count by code. Every human group must
> have at least one live source directive.
>
> This section also owns one complete, lexically sorted
> `Global raw-\`noqa\` inventory:` line using backticked `CODE=count` entries.
> It records every raw Ruff diagnostic exposed by `--ignore-noqa`, including
> locally reasoned directives outside the grouped registry. It is an aggregate
> count tripwire, not a second group registry.
>
> The generated location index is enclosed by the unique markers
> `<!-- BEGIN GENERATED RUFF SUPPRESSION INDEX -->` and
> `<!-- END GENERATED RUFF SUPPRESSION INDEX -->`. Its columns are `Group`,
> `Locations`, `Directives`, and `Raw diagnostics`. Generated rows are sorted
> by group ID; paths use repository-relative POSIX spelling; lines are
> ascending; and codes are lexical. Content outside the markers is
> human-owned and must remain byte-for-byte unchanged during regeneration.
>
> An unreadable or syntactically malformed discovered Python file makes the
> complete index unverifiable. The tool must abort before writing, identify
> the file in a one-line diagnostic, emit no traceback, and leave the existing
> spec unchanged; partial indexes are prohibited.

### Human table transformation

Change the current table header from:

```text
| Location group | Rule and count | Protected invariant | Real proof | Rejected alternatives | Approval |
```

to:

```text
| Group | Rules | Approved cardinality | Protected invariant | Real proof | Rejected alternatives | Approval |
```

For each row, replace only the location cell with the exact group ID in
`## Suppression Group Migration`; remove the ambiguous parenthesized count from
the rule cell; insert the exact approved cardinality from the migration table;
and preserve the other four cells verbatim. Insert the generated index and
markers immediately after the human table.

Add this plan under the spec's `## Related Plans`.

## Tasks

### T1. Add the failing black-box proof

- Create `tests/test_ruff_suppression_index.py`.
- Build isolated temporary repositories containing a minimal Ruff
  configuration, a miniature human registry, and Python files with real Ruff
  violations and approved source markers.
- Invoke the real Ruff binary through the future module CLI. Do not mock Ruff,
  subprocess execution, tokenization, or filesystem replacement.
- First prove that a line move makes check mode fail while write mode is absent
  or cannot repair the index. Record the red result in this plan.
- Add table-driven failures for: unknown group, duplicate human ID, empty human
  group, malformed marker, duplicate generated markers, source code outside
  the group's allowed rules, unused directive, a reasoned non-registry raw
  finding that changes the global inventory, several raw diagnostics at one
  `noqa_row`, and marker-like string literals.
- Done signal: the focused test fails for the missing production module and
  expresses the exact intended CLI and output contract.
- Stop if the fixture must reproduce Ruff logic. The production tool must use
  real Ruff output instead.

### T2. Implement one scanner, reconciler, renderer, and CLI

- Create `bin/ruff_suppression_index.py`.
- Define small immutable records for human groups, source directives, and raw
  diagnostics. Keep parsing, reconciliation, rendering, and replacement as
  named functions in this one cohesive module.
- Parse only the named [DOM-10.1.1] section and unique generated markers.
  Ignore fenced examples and rows outside the live human table.
- Discover files through Ruff, tokenize source comments, invoke raw Ruff, and
  reconcile with counters keyed by `(path, noqa_row, code)`.
- Implement default `--check`, explicit `--write`, optional `--repo-root` for
  black-box fixtures, and optional `--spec` resolved under that root unless
  absolute.
- Implement the anticipated exit classes and one-line diagnostics from the
  invariants above. Do not catch unexpected programming errors merely to
  suppress their traceback.
- Replace the generated block atomically and preserve the rest of the spec
  exactly.
- Done signal: T1 passes, including mutation and no-traceback cases; running
  the tool against the current repository fails cleanly because migration has
  not happened.
- Stop if implementation adds a dependency, guesses a group, writes human
  rows, or needs more than one index owner.

### T3. Review the proposed delta and migration before promotion

- Give an independent different-family reviewer this plan, [DOM-10.1],
  [DOM-10.1.1], the current policy test, the tool, T1 tests, and the 34-group
  migration table.
- Ask whether group identity is stable, human judgment is protected from the
  generator, raw-diagnostic multiplicity is preserved, and write failure
  cannot partially rewrite the spec.
- Resolve every finding before promotion. Record dispositions in the Review
  Log.
- Done signal: reviewer returns PASS on implementability and no degradation.

### T4. Promote the spec and migrate atomically

- Rebaseline all overlapping files first. Do not perform this task while
  current Redis, write-path, database, dump/load, or policy-table edits remain
  unowned in the worktree.
- First make the old exact-location policy test pass and record that reconciled
  pre-migration commit SHA or worktree baseline. Parse its 34 location rows in
  order and mechanically derive the complete `(path, line) -> group ID`
  mapping from `## Suppression Group Migration`; do not assign shared-rule
  groups by judgment while editing comments.
- Apply the Proposed Spec Delta.
- Convert the 34 human rows to stable IDs, rule sets, and approved
  cardinalities.
- Add the spec-owned complete global raw-`noqa` inventory from the tool's
  current all-code raw output:
  `BLE001=100`, `C901=53`, `E402=2`, `F401=4`, `PYI034=6`, `PYI036=19`,
  and `SIM115=1`. Do not copy the incomplete old five-family constant.
- Add the matching group ID to every current approved source comment without
  changing its `noqa` codes or executable line.
- Run `--write` once to create the generated index.
- Run a read-only migration verifier against the recorded pre-migration
  baseline: every old location must carry its row's exact new group ID, with
  no missing, duplicate, or additional assignment. Give that mapping diff to
  both implementation reviewers.
- Update `tests/test_ruff_policy.py` to call the shared reconciler/check path.
  Remove `APPROVED_DIRECTIVE_COUNTS` and the duplicate registry/source parser;
  approved directive counts now live per group in the human table. Relocate
  the global raw-`noqa` inventory from its incomplete test constant to the
  spec so the production CLI enforces all seven current raw rule codes. Retain
  the real behavior proofs for public context-manager compatibility and the
  configured C901 boundary.
- Record the promotion baseline identifier.
- Done signal: the tool's `--check`, both focused test modules, normal Ruff,
  and raw reconciliation pass with 34 nonempty groups whose actual
  cardinalities equal the human-approved values.
- Stop if any migrated rule, rationale, proof, rejected alternative, or
  approval differs from the baseline for reasons other than the planned schema
  transformation.

### T5. Put the check on the normal repository path

- Add
  `uv run --frozen --no-sync python bin/ruff_suppression_index.py --check`
  to the root CI lint job after normal Ruff.
- Update `docs/implementation/02-repository-map.md` and
  `docs/implementation/07-complexity-and-state-machine-map.md`.
- Keep formatter scope explicit. Do not change the Ruff rule fixture,
  selected families, ignored rules, complexity threshold, or extension lint
  jobs.
- Done signal: workflow structure tests and the direct command prove the same
  check path CI will run.
- Stop if CI requires a second configuration or generated registry copy.

### T6. Run adversarial acceptance and full verification

- Exercise the production CLI against isolated fixtures for hostile source
  encoding, grammar mimicry, malformed Markdown, missing target, unwritable
  output, stale index, and self-application.
- For the deliberate unreadable-source fail-fast policy, assert exit 2, one
  diagnostic naming the file, no traceback, unchanged spec, and continued
  availability of `--check` after the fixture is repaired.
- Run the exact commands in `## Verification and Gates`.
- Inspect the final diff to prove that migration changed only source comments,
  table schema/IDs, the generated block, and named tool/policy/docs files.
- Done signal: all gates pass from the current state.

### T7. Independent completed-work review and closeout

- Give a different-family reviewer the promoted spec, plan, implementation
  docs, tool, focused tests, policy test, workflow change, generated block, and
  verification evidence.
- Ask specifically whether the tool can approve new suppressions implicitly,
  whether check and write can disagree, whether line movement is now repaired
  mechanically, and whether any generated data remains hand-maintained.
- Resolve or disposition every finding, rerun affected gates, update this
  plan's evidence, and flip the Status Index row to `completed` only when
  implementation is committed and verified.
- Evaluate whether the writing-plans, testing, or adversarial-probe runbooks
  missed a reusable rule. Add a lesson only if implementation reveals a new
  durable correction.

### T8. Revision R1 — symbol resolution and rendering

Add an AST symbol index resolving a directive line to its outermost enclosing
`def`, class-qualified, with `<module>` for module-level directives and
decorator lines attributed to the function they decorate. Carry the result on
`SourceDirective` alongside `line`. Change `_render_locations` to emit
`path::symbol` entries, deduplicated and sorted. Leave reconciliation,
cardinality, and the global inventory untouched.

### T9. Revision R1 — promote the delta and regenerate

Apply the one-sentence [DOM-10.1.1] change, regenerate the index with
`--write`, and confirm the derived block now carries symbols. Verify the gate
still fails for an unclean `ruff check`, an unknown group, a rule a group does
not approve, and a cardinality change.

### T10. Revision R1 — verification and closeout

Full suite, `ruff check .`, `bin/check-dom15-fixtures`, and the generator's own
`--check`. Confirm the index is stable across an edit that moves lines without
changing suppressions — the churn case R1 exists to remove.

## Testing Plan

Use the real Ruff executable, real temporary files, and the production CLI.
Do not mock Ruff output, source discovery, `tokenize`, the spec parser, or the
normal replacement path.

Focused behavior:

- clean check is a no-op and exits 0
- stale line numbers exit 1
- write regenerates only the marked block and exits 0
- second write is byte-for-byte idempotent
- moving a tagged source line changes only generated locations
- multiple diagnostics at one line retain their raw count
- all 34 human groups have live directives and exact allowed rules
- adding a directive to an existing valid group fails until the human-owned
  approved cardinality changes
- changing the aggregate count of a reasoned non-registry `noqa` fails the
  global raw inventory without adding it to the generated registry
- new, unknown, duplicate, empty, or malformed groups fail closed
- missing or duplicate markers fail without writing
- string/docstring marker mimicry is inert
- POSIX path output is identical from `PureWindowsPath` input
- replacement failure leaves the original spec unchanged
- every failure class prints no traceback

The existing `tests/test_ruff_policy.py` remains responsible for Ruff
configuration, enabled-rule inventory, real rule firing, public annotation
compatibility, tracked discovery, and workflow shape. It delegates suppression
reconciliation to the production scanner instead of carrying another parser.

## Verification and Gates

Per-task:

```bash
uv run --frozen --no-sync pytest -q \
  tests/test_ruff_suppression_index.py tests/test_ruff_policy.py
uv run --frozen --no-sync python bin/ruff_suppression_index.py --check
```

Static and documentation gates:

```bash
uv run --frozen --no-sync ruff check .
uv run --frozen --no-sync ruff format --check \
  simplebroker tests bin .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis \
  extensions/simplebroker_redis/tests
uv run --frozen --no-sync mypy bin/ruff_suppression_index.py
python3 bin/check-dom15-fixtures
bin/check-doc-paths
git diff --check
```

Final:

```bash
uv run --frozen --no-sync pytest
```

Success means the committed generated block is current; normal Ruff is clean;
raw findings, source directives, human groups, and generated rows reconcile
with multiplicity; the generator is idempotent; the workflow invokes check
mode; the full suite passes; and no approved suppression meaning changed.

## Independent Review Loop

Plan review should use Claude if available because the authoring family is
Codex and the current agent inventory records Claude as live. Give the reviewer:

- this plan and its Proposed Spec Delta
- the worktree-baseline [DOM-10.1]/[DOM-10.1.1] section
- `tests/test_ruff_policy.py`
- `docs/implementation/07-complexity-and-state-machine-map.md`
- the current source-marker inventory

Required verdict:

1. Could the reviewer implement the plan confidently and correctly?
2. Would implementation weaken explicit suppression review, repository
   robustness, or failure truthfulness?

A BLOCKED verdict must identify which question fails. The author must
reproduce each factual claim and record every disposition below.

Repeat review after T2 and before the atomic T4 promotion. Run completed-work
review after T6.

## Out of Scope

- Changing any currently approved suppression, rationale, proof, rejected
  alternative, or approval.
- Adding a new suppression.
- Reclassifying the two existing reasoned non-registry `BLE001` suppressions as
  approved registry exceptions.
- Lowering or raising C901 complexity 10.
- Changing Ruff version, selected rules, global ignores, or format scope.
- Refactoring high-complexity functions.
- Generating human prose from source comments or model output.
- Moving the durable registry to TOML, YAML, JSON, a plan, or a separate
  database.
- Publishing the tool as part of the SimpleBroker package or public CLI.
- Generalizing this into a Markdown-table generator framework.
- Repairing unrelated dirty-worktree changes.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| [DOM-10.1] global raw inventory | Retain and rename the old five-family test constant | The canonical aggregate moves into [DOM-10.1.1] and includes all seven raw rule codes; the production CLI enforces it | The old constant omitted six `E402`/`F401` findings and could not govern the CLI | Promote the complete, spec-owned aggregate grammar in the atomic delta |
| Invariants 14–15 | Every failure, including unexpected internal errors, exits 2 without a traceback | Enumerated operational and unverifiable-input failures exit 2 cleanly; unexpected programming errors retain tracebacks | A catch-all would require a new `BLE001` suppression and would hide bug evidence | Narrow the guarantee in the atomic delta; add no suppression |
| Canonical invocation | Use the importable module form in CI and documentation | Use the direct `bin/ruff_suppression_index.py` path everywhere users run the tool; retain imports only for test seams | The repository script is easier to find, copy, and connect to the repository map | Make both full direct commands part of [DOM-10.1.1] and test their presence |

## Review Log

| Date | Reviewer | Verdict | Findings and disposition |
|------|----------|---------|--------------------------|
| 2026-07-30 | Claude 2.1.207, independent plan review | PASS with one pre-promotion P1 | P1 accepted: restored movement-stable human-approved per-group cardinalities so reusing an ID cannot auto-approve growth. P2 committed-index removal declined: the user explicitly values the index, and its offline auditability justifies deterministic generated churn. P2 migration ambiguity accepted: T4 now derives and verifies every source assignment mechanically from a reconciled old-format baseline. P3 fail-fast durability accepted in the exact spec delta. P3 table-scope ambiguity accepted as an explicit parser invariant. Round-2 verification passed below. |
| 2026-07-30 | Claude 2.1.207, round-2 verification | PASS | Verified F1 per-group growth and shrinkage tripwire, F2 mechanical old-row-to-group migration and read-only verifier, F3 durable fail-fast/no-partial-write spec text, and F4 strict human/generated parser scopes. Confirmed the committed-index rationale is coherent and found no new defect. |
| 2026-07-30 | Claude 2.1.207, scoped round-3 verification | PASS | Verified the correction for two pre-existing, reasoned non-registry `BLE001` suppressions. Tagged findings remain subject to exact group reconciliation and human-approved cardinalities; all local `noqa` findings remain subject to the exact global raw-diagnostic inventory. Incorporated two wording clarifications so the plan cannot be read as requiring untagged local suppressions to enter the registry. |
| 2026-07-30 | Claude 2.1.207, T1/T2 pre-promotion implementation review | PASS | No P1. Accepted P2: narrowed exit-2/no-traceback to enumerated failures and declined a new catch-all `BLE001` suppression. Accepted P3: ordinary prose references are inert; added empty-group and missing-marker tests; made the global aggregate complete and spec-owned. Second-write idempotence and Windows path spelling are explicit focused proofs. |
| 2026-07-30 | Claude 2.1.207, completed-work review | PASS | No P1/P2. Confirmed no implicit approval path, shared check/write validation, mechanical movement repair, fully derived generated fields, truthful global multiplicity, and rationale-preserving migration. Accepted the sole cosmetic P3: syntax/tool exit classification now keys only on Ruff's diagnostic code, never a message substring. |

## Execution Evidence

| Slice | Baseline | Evidence | Result | Review | Residual risk |
|-------|----------|----------|--------|--------|---------------|
| T4 pre-migration rebaseline | HEAD `84fcc5706834fd85115ab404c1beae47ab9f4e08`; reconciled spec blob `1f8c89e349ea3a71e87a0e2bfc44851f434670c6`; policy-test blob `38d7778d2a325e48113732c1402171a8f6599a56` | Existing exact-location policy test; source/spec/raw inventory audit | PASS: 165 directives, 34 rows, six derived locations refreshed, exact ordered row-to-group migration is bijective | Read-only subagent audit | Active worktree remains dirty; migration must preserve all unrelated source and spec edits. |
| T1/T2 scanner tracer and boundary slice | Reconciled worktree above | `uv run --frozen --no-sync pytest -q tests/test_ruff_suppression_index.py`; focused Ruff and mypy | PASS after reviewer dispositions; repository self-check failed closed on the intentionally absent pre-migration marker | Claude 2.1.207 PASS | None after T6 hostile-input completion. |
| T4 atomic promotion and migration | Spec blob `5d695446a356a20b6323d754dff9e39554f7a338`; final reviewed tool blob `b9a9a06d69a205384e362ec57699356f34e3f577`; policy-test blob `3652b07ea43ebe8a78486809f618ca84ed153c1e` | Pre-write bijection assertion in the mechanical migrator; post-write read-only comparison with the committed registry; production `--check` | PASS: 34 groups, 165 directives, 12 moved lines, zero group reassignments; all human rationale cells preserved | Claude completed-work PASS | The worktree remains uncommitted and contains unrelated active work. |
| T5/T6 CI, docs, adversarial, and full verification | Workflow blob `6340b8e1bf5d2bae6d4e4c9a538a8f1717ca80b8`; focused-test blob `4f25e471ff6cceb26f847e023284b1fd6fe37b74` | 38 focused tests; normal Ruff; direct production `bin/ruff_suppression_index.py --check`; tool mypy; DOM-15 fixtures; doc paths; diff check; full pytest | PASS: 2,361 tests passed, 17 skipped; hostile encoding/syntax/markers/paths/replacement and byte-preservation cases pass; registry and script expose both complete direct commands | Claude completed-work PASS | Full repository format check remains red in four pre-existing dirty files outside this plan: `_runner.py`, `test_broadcast_contract_sb_bcast.py`, `test_core_persistence_transition_tables.py`, and `test_runner_error_handling.py`. |
| T7 final rebase and targeted-commit audit | HEAD `e155d5f9`; final tool blob `8fc905f3e90d236c599b52f5cff7f3a276424ca7`; spec blob `5bd591a96c080569eb0f941521f24a51270a4aa5`; policy-test blob `66f5d3b074ba667063182a05a37be2023101e774` | Recovered the reviewed stable-ID mapping from the preserved worktree snapshot without applying unrelated snapshot content; reconciled it against the T4 blobs; normalized marker-only audit of every tracked source edit; 38 focused tests; full pytest; Ruff; direct production check; tool mypy; DOM-15 fixtures; doc paths; diff check | PASS: 165 directives in 34 approved groups; 54 tracked source files differ from HEAD only by stable-ID insertions; 38 focused tests and the full suite pass | Prior completed-work review plus deterministic recovery audit | Current HEAD has two unrelated `F401` findings in `test_timestamp_selection_contract_sb_select.py` and seven unrelated formatter findings. Ruff, self-check, focused tests, and the full suite passed with only those two imports temporarily removed; the imports were restored before staging and are absent from this change. |

## Fresh-Eyes Review

Before implementation approval, confirm:

- a zero-context implementer can distinguish human-owned and generated fields
- every migration group has one stable ID
- no task permits the generator to infer approval
- exact commands and exit classes are stated
- source discovery and raw rule behavior have one owner each
- write failure cannot leave a partial spec
- dirty-tree overlap requires rebaseline rather than overwrite
- tests prove the production path and hostile boundaries
- rollback is one atomic revert
- no abstraction extends beyond this concrete registry problem
