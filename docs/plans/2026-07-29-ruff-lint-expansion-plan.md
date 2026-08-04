# Ruff Lint Expansion Plan

Date: 2026-07-29
Status: active
Class: 3+P — [DOM-5] non-trivial triggers fire because the change revises a
repository-wide verification workflow and drives cleanup across core, extension,
test, example, fuzz, and maintenance-script surfaces. The `+P` modifier fires
because changing the lint gate is [DOM-6]-material to how future work is
implemented and verified. Effective class-5 planning and review requirements
apply. No [DOM-5] risky trigger fires.
Plan type: implementation with spec revision
Hardening: N/A — no [DOM-5] risky trigger

## Goal

Adopt Ruff 0.16's expanded stable default lint policy without losing
SimpleBroker's existing lint coverage. Preserve the current `E`, `W`, `F`, `I`,
`B`, `C4`, and `UP` families by extending Ruff's defaults, resolve the resulting
findings without changing product behavior, and make the expanded policy a
firing repository gate rather than an undocumented configuration accident.

## Requested Outcomes

- [x] Ruff's stable default rules are enabled instead of being shadowed by an
  explicit `lint.select`.
- [x] The existing `E`, `W`, `F`, `I`, `B`, `C4`, and `UP` families remain
  enabled, including rules Ruff 0.16 removed from its defaults.
- [x] All tracked first-party `.py`/`.pyi` files and Python-shebang scripts,
  including extensionless `bin/*` tools, are covered by the root lint job.
- [x] Every current expanded-policy diagnostic is resolved through a
  behavior-preserving code change where practical. The target is zero new
  suppressions; any irreducible candidate is separately inventoried, proven
  against a tested invariant, independently reviewed, and presented for user
  approval before policy activation.
- [x] A real-Ruff policy test proves both a new-default rule and a retained
  legacy-family rule fire.
- [x] Ruff check/format, mypy, core, PostgreSQL, Redis, examples, process-doc,
  and diff gates pass.

## Source Documents

- User instruction in the originating session: create a lint-expansion plan
  after evaluating Ruff 0.16's broader lint policy.
- Ruff 0.16.0 release notes and migration guide, especially the change from 59
  to 413 default rules and the 18 `E`/`F` rules removed from the new defaults:
  - <https://github.com/astral-sh/ruff/releases/tag/0.16.0>
  - <https://astral.sh/blog/ruff-v0.16.0>
- Ruff rule-selection guidance:
  <https://docs.astral.sh/ruff/linter/#rule-selection>.
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-6], [DOM-10], [DOM-11], [DOM-15].
- `docs/agent-context/engineering-principles.md`, especially real behavior over
  mock-heavy proof, failing-test-first, enumerable gates, and declared
  variation.
- `docs/agent-context/runbooks/writing-plans.md` and
  `docs/agent-context/runbooks/testing-patterns.md`.
- retired plan `2026-07-29-development-toolchain-refresh-plan.md` at
  `197629e2`, which moves the repository to Ruff 0.16.0 but deliberately leaves
  lint policy unchanged.

## Spec Baseline

- `b21bbcd87af7ea868ad7ea02d0d8ce4c83dea8a8` —
  `docs/specs/01-development-documentation-operating-model.md` at plan
  authoring time.
- Implementation prerequisite: the Ruff 0.16.0 manifest and lock updates from
  retired plan `2026-07-29-development-toolchain-refresh-plan.md` at
  `197629e2` must be present in the implementation baseline. Land this
  lint-policy change separately from that dependency-refresh unit.
- Promotion baseline: `b21bbcd87af7ea868ad7ea02d0d8ce4c83dea8a8`
  plus worktree blob `7527756c69c0dcdd8dbe4353083d5a8d3efa8d7f`
  for `docs/specs/01-development-documentation-operating-model.md`.
  `python3 bin/check-dom15-fixtures`, `bin/check-doc-paths`, and
  `git diff --check` passed after promotion.

## Current Baseline

Ruff 0.16.0 resolves 170 enabled rules from the current explicit
`[tool.ruff.lint].select`. A read-only probe using Ruff's stable defaults plus
the existing seven families resolves 452 rules. It adds 282 rules without
removing any current rule.

The exact baseline probe was:

```bash
uv run --frozen --no-sync ruff check \
  --isolated \
  --target-version py311 \
  --extend-select E,W,F,I,B,C4,UP \
  --ignore E501,B008 \
  --output-format json \
  .
```

The first probe reported 660 diagnostics in 136 files; 240 diagnostics offered
a Ruff fix. A discovery review then found that Ruff's default include patterns
omit six tracked extensionless Python scripts:
`bin/check-doc-paths`, `bin/check-dom15-fixtures`, `bin/coalesce-check`,
`bin/packaging-smoke`, `bin/pytest-pg`, and `bin/pytest-redis`. Including those
scripts raises the full baseline to 668 diagnostics in 141 files, with 245
offered fixes. The probe used `--isolated` so the current `select` could not
hide the new defaults. Counts are planning evidence for Ruff 0.16.0, not the
release-refresh gate; the accepted enabled-rule inventory is.

| Rule group | Count | Main risk |
|------------|------:|-----------|
| `BLE001` | 204 | Broad catches are often intentional at worker, plugin, CLI, and cleanup boundaries; narrowing blindly can change failure containment. |
| `RUF059` | 123 | Mostly unused tuple members in tests; mechanical renames can obscure assertions if applied without review. |
| `S110`, `S112` | 82 | Silent best-effort cleanup and retry behavior must not be converted into noisy or fatal behavior merely to satisfy lint. |
| `SIM117` | 67 | Context-manager flattening can change enter/exit ordering if the rewrite is not equivalent. |
| `PYI034`, `PYI036` | 40 | Context-manager annotations affect mypy-visible contracts across core and extensions. |
| `EXE001` | 15 | A shebang/file-mode mismatch needs an ownership decision, not a blanket chmod. |
| `PIE790`, `RUF022`, `SIM102` | 36 | Generally local cleanup, but import/export order and branch rewrites still require diff review. |
| Remaining 26 rule codes | 93 | Includes subprocess checks, exception types, resource ownership, mutable class defaults, import placement, and one hidden bare-reraise coupling. |

## Context and Key Files

### Files to modify

- `docs/specs/01-development-documentation-operating-model.md`:
  promote the repository lint-gate contract under [DOM-10.1] and backlink this
  plan.
- `pyproject.toml`: add `extend-include = ["bin/*"]`, replace `lint.select`
  with `lint.extend-select`, and retain the seven existing families and two
  existing global ignores.
- `tests/test_ruff_policy.py` (new): own the configuration and real-Ruff firing
  tests for rule selection, tracked-file discovery, preview posture,
  suppression structure, and CI/formatter commands.
- `tests/fixtures/ruff-enabled-rules.txt` (new): record the accepted effective
  rule-code inventory for the locked Ruff version. A tool refresh must review
  and intentionally update this inventory rather than silently inheriting a
  changed default.
- `.github/workflows/test.yml`: make the root lint command discover all
  first-party Python paths with `ruff check .`; keep formatter scope explicit.
- `README.md`: align the contributor lint command and explain the expanded
  stable-default-plus-local-family policy.
- Python files reported by the expanded lint probe under `simplebroker/`,
  `tests/`, `bin/`, `examples/`, `fuzz/`, `.github/scripts/`, and both extension
  source/test trees. The probe output, not this plan's current path list, is the
  authoritative edit inventory during implementation.
- `docs/plans/README.md`: keep this plan's status row aligned.

### Read before editing

- `pyproject.toml` `[tool.ruff]` and `[tool.ruff.lint]`: this is the single
  rule-selection owner.
- `.github/workflows/test.yml` `lint` job and both extension lint jobs: the
  root job is the comprehensive gate; extension jobs retain fast,
  backend-specific feedback.
- `tests/test_release_workflow.py`: reuse its workflow-reading conventions but
  keep Ruff-policy assertions in the new cohesive test module.
- `simplebroker/watcher.py::_handle_retry` and its callers: its bare `raise`
  depends on being called from an active exception handler. Do not replace it
  with `raise e` and silently alter traceback semantics.
- Broad exception catches in `simplebroker/db.py`, `simplebroker/watcher.py`,
  backend plugins/runners, cleanup helpers, and multiprocess tests: classify
  whether each catch is a public error boundary, a retry boundary, best-effort
  cleanup, test observation, or an accidentally broad catch before editing it.

### Comprehension gates

Before changing exception-related findings, the implementer must be able to
answer:

1. Which broad catches are responsible for containing backend/plugin failures,
   and which merely compensate for an overly broad implementation?
2. Which `except` blocks intentionally make cleanup best-effort, and what
   observable behavior would change if logging or propagation were added?
3. Why does `_handle_retry` use a bare re-raise, and which targeted test proves
   its traceback/retry contract if that code is changed?

## Invariants and Constraints

1. Preserve all runtime, CLI, storage, backend-protocol, exception-shape, and
   public API behavior. This is a verification-policy change, not a product
   change.
2. Preserve the existing seven lint families while enabling Ruff's stable
   defaults. Do not replace one policy with the other.
3. Keep preview rules disabled. This plan expands to Ruff 0.16's stable policy,
   not unstable `--preview` behavior.
4. Keep global ignores limited to the existing `E501` (formatter-owned line
   length) and `B008` (intentional call-in-default patterns).
5. Target zero new suppressions. Do not add `per-file-ignores`. A new local
   suppression is a last resort, not an ordinary resolution: it must protect a
   named, tested invariant; be narrower and clearer than every attempted
   behavior-preserving rewrite; carry a nearby reason; appear in the governing
   spec's [DOM-10.1.1] registry; pass independent review; and receive user
   approval before T9 activates the policy.
6. Do not run `--unsafe-fixes`. Apply safe fixes by coherent rule group, inspect
   every diff, and manually handle fixes whose semantics are not obvious.
7. Preserve context-manager enter/exit order, subprocess exit handling,
   cleanup best-effort semantics, and retry boundaries. For re-raised
   exceptions, preserve the same exception object, type, message, cause,
   context, and original failure frame. A private retry-orchestration frame may
   change only when the targeted test and independent review confirm that no
   consumer-visible traceback information is lost.
8. Do not add logging solely to silence a security-style rule when logging
   would create noise, leak user data, or alter a hot path. Prefer an explicit
   code structure that expresses best-effort cleanup. If no clear structure
   satisfies both the rule and the runtime contract, stop and register a
   suppression candidate rather than adding one silently.
9. Do not remove shebangs or change executable bits without deciding how the
   file is invoked. Directly executable scripts keep both; module-only scripts
   lose the shebang.
10. Keep Ruff formatting on explicit Python paths. Ruff 0.16 can format
    Markdown code fences; this lint expansion must not silently widen formatter
    ownership to all repository Markdown or add new directories to the CI
    formatter gate.
11. Use the real installed Ruff binary in the policy test. Do not mock Ruff,
    synthesize its enabled-rule set, or assert only a numeric rule count. Compare
    the effective code set from `ruff check --show-settings` with the reviewed
    fixture so additions and removals both fail closed during a tool refresh.
12. Add no dependency, package version, CHANGELOG entry, or product
    documentation claim.
13. Stop and re-plan if satisfying a rule requires changing a public exception,
    changing backend behavior, introducing a helper used by more than one
    subsystem, or globally ignoring a newly enabled family.
14. Treat annotations on exported classes and methods as public compatibility
    surfaces. A `PYI034`/`PYI036` rewrite is permitted only for a private
    surface or after proving the annotation change is consumer-compatible.
    Otherwise leave the finding as a suppression candidate and stop for review;
    stop and re-plan if a public typing contract must change.

## Proposed Spec Delta

Promotion strategy: **A — in-file edit, text before link claims**. Promote the
following text into
`docs/specs/01-development-documentation-operating-model.md` after the existing
[DOM-10] completion-evidence paragraphs. Add the plan backlink under that
spec's `## Related Plans`. Do not claim implementation completion until the
configuration, firing test, CI gate, and full verification slices have landed.

### [DOM-10.1] — insert after [DOM-10]

> ### Repository Static-Analysis Gate [DOM-10.1]
>
> SimpleBroker's Python lint gate uses the stable default rule set of the Ruff
> version locked in `uv.lock`, extended with the repository's existing `E`,
> `W`, `F`, `I`, `B`, `C4`, and `UP` rule families. The configuration must
> extend Ruff's defaults rather than replace them.
>
> Owner: `pyproject.toml` owns rule selection; the root CI lint job enforces it.
> Boundary: every tracked first-party `.py`/`.pyi` file and Python-shebang
> script in the repository. Verification: `tests/test_ruff_policy.py` invokes
> the real locked Ruff binary, compares effective discovery and rule selection
> with reviewed inventories, and proves that a stable-default rule outside the
> legacy families and a retained legacy-family rule both fire. Required action:
> a Ruff version refresh reviews and intentionally updates the enabled-rule
> inventory before regenerating the lock.
>
> Requirements:
>
> - the root lint job uses repository discovery (`ruff check .`), and Ruff's
>   include configuration plus a tracked-file discovery test covers
>   extensionless Python-shebang tools that default discovery omits;
> - Ruff preview rules remain opt-in and are not part of the default gate;
> - global ignores are limited to explicitly documented repository-wide
>   conflicts; per-file ignores remain empty, and other suppressions are local,
>   narrow, carry a reason, protect a tested invariant, and require explicit
>   review before adoption;
> - intentionally broad exception or best-effort cleanup boundaries retain
>   their runtime behavior through an explicit code structure where practical;
>   a suppression is the reviewed last resort, not the default alternative to
>   a behavior-changing rewrite;
> - formatter paths stay explicit so widening lint discovery does not
>   implicitly widen Markdown formatting ownership.

## Dependency-Ordered Tasks

### T1 — Independently review the plan and spec delta

- Use a different agent family when available; otherwise use an independent
  agent with a separate review role and record that limitation.
- Files to read: this plan, the proposed [DOM-10.1] delta, current Ruff
  configuration, representative exception-boundary findings, the root lint
  workflow, and the Ruff 0.16 migration notes.
- Review stance: look for missing behavior invariants, a rule-adoption strategy
  that loses existing coverage, overbroad exemptions, weak self-testing, and
  cleanup work that changes runtime behavior merely to satisfy lint.
- Done signal: reviewer returns `PASS`, or every blocking finding is resolved
  and a scoped round confirms the correction.
- Stop if the reviewer cannot implement the plan confidently or believes it
  would weaken runtime robustness.

### T2 — Promote the lint-gate spec

- Files to touch:
  `docs/specs/01-development-documentation-operating-model.md` and this plan.
- Apply the exact [DOM-10.1] text using promotion strategy A.
- Add the plan backlink under `## Related Plans`.
- Record the promotion baseline as a commit SHA, or as
  `b21bbcd87af7ea868ad7ea02d0d8ce4c83dea8a8` plus the exact spec diff when the
  work remains uncommitted.
- Verify with `python3 bin/check-dom15-fixtures`, `bin/check-doc-paths`, and
  `git diff --check`.
- Done signal: the promoted spec is the single governing contract before
  configuration or cleanup implementation begins.

### T3 — Add the failing Ruff-policy proof

- File to add: `tests/test_ruff_policy.py`.
- Parse `pyproject.toml` with `tomllib` and assert:
  - top-level `extend-include` is exactly `["bin/*"]`;
  - `lint.select` is absent;
  - `lint.extend-select` contains exactly `E`, `W`, `F`, `I`, `B`, `C4`, and
    `UP`;
  - the repository-wide ignores remain exactly `E501` and `B008`.
  - preview is absent or false and the per-file-ignore map is empty.
- Invoke the real locked Ruff binary against a small stdin probe using the
  repository config. Assert that:
  - `BLE001` fires as a representative Ruff 0.16 stable-default rule outside
    the prior configured families;
  - `B904` fires as a representative legacy-family rule not present in Ruff
    0.16's curated defaults.
- Obtain Ruff's effective file inventory with `ruff check --show-files .`.
  Compare it with tracked `.py`/`.pyi` paths plus tracked files whose first line
  is a Python shebang. The expected tracked set must be a subset of Ruff's
  discovered set; the test must fail if a new extensionless Python tool escapes.
- Obtain Ruff's effective enabled codes from `ruff check --show-settings` and
  compare the whole set with `tests/fixtures/ruff-enabled-rules.txt`. Generate
  the initial fixture from the reviewed 452-rule union; do not reduce the gate
  to a count or two sentinels.
- Parse `.github/workflows/test.yml` and assert the root lint invocation is
  `ruff check .`, preview is not enabled, and the formatter invocation remains
  the current explicit path list rather than `ruff format --check .`.
- First run the test against the current `lint.select` configuration and record
  the expected failure. Do not mock Ruff or assert only subprocess call
  arguments.
- Done signal: the test fails for the current shadowing configuration for the
  intended reason.

### T4 — Apply reviewable mechanical fixes

- Run the expanded policy through explicit CLI overrides while the checked-in
  configuration still selects the old clean set.
- Group changes by rule and behavior, not by whichever order `--fix` emits:
  - unused unpacked members and expression cleanup;
  - import, `__all__`, and regular-expression spelling;
  - redundant returns, passes, branches, and collection construction;
  - context-manager flattening only where enter/exit order is unchanged.
- Use `ruff check --fix --diff --select <coherent-codes>` before applying each
  group. Never use `--unsafe-fixes`.
- Run targeted tests for each touched production module and inspect the diff
  before proceeding to the next group.
- Done signal: the mechanical groups are clean under the override and no diff
  changes observable behavior.

### T5 — Resolve correctness, resource, private typing, and file-mode findings

- Audit `PLE0704`, `TC004`, `PLW1510`, `SIM115`, `TRY004`, `RUF012`,
  `PYI034`, `PYI036`, and `EXE001` individually.
- For `_handle_retry`, first add a red-green test that pins the same exception
  object, type, message, cause, context, original failure frame, retry count,
  terminal log behavior, and terminal invocation of `_handle_retry`.
- Prefer this structural resolution: keep calling `_handle_retry` for the
  terminal attempt; let it log and return `False` without sleeping or cleaning
  up; then perform the terminal bare re-raise in `_run_with_retries`' active
  `except` block. This preserves the protected hook invocation and its `bool`
  return shape while moving the re-raise to syntax Ruff can prove safe. Do not
  replace the bare raise with `raise e`.
- The private helper's terminal path currently does not return. Accepting a
  `False` return there is allowed only after the real watcher test proves the
  public exception and retry contract above. If the structural split cannot
  meet that proof, stop and register `PLE0704` as a suppression candidate.
- Add explicit subprocess `check=` values that match the caller's existing
  treatment of nonzero exits.
- Use context managers only when the current object lifecycle allows the same
  close timing.
- For `PYI034`/`PYI036`, identify whether the annotation is exported before
  editing. Correct private surfaces when compatible. Preserve public
  annotations unless a consumer-facing typing change is separately authorized
  and planned. Treat any remaining public finding as a suppression candidate
  subject to the registry and approval gate. Run all mypy partitions.
- For each shebang mismatch, choose either executable mode or shebang removal
  based on the real invocation path.
- Done signal: these codes are clean or recorded as unresolved registry
  candidates with concrete proof; no new suppression is applied before T8
  approval. Targeted tests and mypy pass.

### T6 — Independent review checkpoint: mechanical and resource slice

- Review the T4/T5 diff, focused on context-manager ordering, resource lifetime,
  subprocess exit handling, exported annotations, file-mode decisions, and
  whether any mechanical fix changed behavior.
- Resolve every finding before beginning the exception-boundary audit.
- Done signal: a scoped reviewer returns `PASS` over the accepted corrections.

### T7 — Audit exception and best-effort boundaries

- Review every `BLE001`, `S110`, `S112`, `TRY002`, and `TRY401` finding in its
  surrounding control flow.
- Narrow exception types when the called operations define a stable, complete
  exception set.
- Prefer an explicit behavior-preserving structure when the boundary
  deliberately contains unknown backend/plugin/handler failures, aggregates
  child failures, or performs best-effort cleanup. Use
  `contextlib.suppress(...)` only when it communicates the actual cleanup
  contract; do not replace `except Exception` with
  `contextlib.suppress(Exception)` merely to evade a diagnostic.
- If a broad catch remains necessary after concrete alternatives are tried,
  add it to the suppression registry with the protected invariant, real test,
  and rejected alternatives. Do not add the suppression yet.
- Do not introduce logging into silent cleanup or polling loops unless logging
  is already part of that boundary's contract.
- Prefer an existing helper when several findings share the same real
  operation; do not invent a generic suppression wrapper to make diagnostics
  disappear.
- Run the nearest real tests after each production subsystem, including retry,
  cleanup, plugin, CLI, multiprocess, and watcher tests. Do not replace those
  paths with mocks.
- Done signal: every exception finding is fixed, or the finite unresolved set
  is present in the registry with proof. The preferred outcome is an empty
  registry.

### T8 — Independent review checkpoint: exception-boundary slice

- Review every production exception change from T7, every proposed new
  suppression, and a representative sample of test-only code changes.
- Ask whether exception containment, retry, cleanup silence, log exposure, or
  child-process error aggregation changed.
- Present any surviving registry rows to the user. Only explicitly approved
  rows may become local suppressions; rejected rows return to T7 for a code
  solution or a declared scope decision.
- Apply only the approved suppressions, then run each row's targeted real test
  and the expanded Ruff override. Give the scoped reviewer the exact
  suppression diff for confirmation.
- Any rejected row that remains unresolved blocks T9. There is no
  "activate now, inspect later" path.
- Resolve every finding before activating the repository policy.
- Done signal: a scoped reviewer returns `PASS` over the final code and exact
  approved-suppression diff; every registry row has an explicit user
  disposition; no rejected unresolved row remains.

### T9 — Activate the policy and synchronize its consumers

- Files to touch: `pyproject.toml`, `.github/workflows/test.yml`, `README.md`,
  `tests/test_ruff_policy.py`, and any workflow-policy assertion it owns.
- Rename `lint.select` to `lint.extend-select` without changing the seven
  listed families.
- Add `extend-include = ["bin/*"]` and confirm all eight current `bin/*` files
  appear in Ruff's discovered inventory.
- Keep global ignores at `E501` and `B008`.
- Change the root CI lint command to `ruff check .`.
- Keep the root CI formatter command's current explicit path list unchanged;
  do not add `examples/`, `fuzz/`, or `.` in this policy change.
- Align the README commands and explain that stable defaults are extended by
  local families.
- Run the red policy test and observe it pass.
- Done signal: normal `ruff check .` passes without isolated/selector
  overrides, while the real-Ruff firing test proves both policy halves.

### T10 — Run final repository verification

- Run the exact commands in `## Testing and Verification`.
- Inspect every newly added suppression and match it one-for-one to an approved
  registry row. Confirm it is narrow, reasoned, tested, and used (`RUF100`
  clean).
- Inspect the final diff for public behavior, package metadata, CHANGELOG, and
  formatter-scope drift.
- Stop if any runtime test requires weakening to accommodate a lint rewrite.
- Done signal: all required suites and static/process gates pass from the
  current state.

### T11 — Independent completed-work review and close

- Give the reviewer the promoted [DOM-10.1] baseline, this plan, Ruff config,
  policy test, CI/README consumers, all cleanup diffs, suppression inventory,
  and verification evidence.
- Ask specifically whether any lint fix changed exception containment,
  resource lifetime, subprocess semantics, or public behavior; also ask whether
  any suppression is broader than necessary.
- Resolve or disposition every finding and run a scoped confirmation for
  accepted corrections.
- Reconcile spec, plan, README, test, and configuration traceability.
- Mark this plan and its Status Index row `completed` only after every gate
  passes.

## Testing and Verification

### Policy proof

```bash
uv run --frozen --no-sync pytest tests/test_ruff_policy.py
uv run --frozen --no-sync ruff check .
uv run --frozen --no-sync ruff format --check \
  simplebroker tests bin .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
```

Success means the real-Ruff sentinel test passes, no expanded-policy diagnostic
remains, no unused suppression remains, and formatting ownership stays limited
to explicit Python paths.

### Type and targeted behavior proof

```bash
uv run --frozen --no-sync mypy simplebroker bin/release.py \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
python3 - <<'PY'
from pathlib import Path
import subprocess

for package, tests in (
    ("extensions/simplebroker_pg/simplebroker_pg", "extensions/simplebroker_pg/tests"),
    (
        "extensions/simplebroker_redis/simplebroker_redis",
        "extensions/simplebroker_redis/tests",
    ),
):
    test_files = sorted(
        str(path)
        for path in Path(tests).rglob("*.py")
        if "__pycache__" not in path.parts
    )
    subprocess.run(
        [
            "uv", "run", "--frozen", "--no-sync", "mypy",
            package, *test_files, "--config-file", "pyproject.toml",
        ],
        check=True,
    )
PY
uv run --frozen --no-sync python bin/release.py --check-example-types
```

During T4–T7, run the closest tests named by each touched subsystem. The final
gate is:

```bash
uv run --frozen --no-sync pytest
uv run --frozen --no-sync ./bin/pytest-pg --fast
uv run --frozen --no-sync ./bin/pytest-redis --fast
```

No core behavior should be mocked to make a lint-driven rewrite pass. Test the
real retry, cleanup, plugin, subprocess, and watcher paths already owned by
their suites.

### Process and diff gates

```bash
uv lock --check
uv lock --check --directory extensions/simplebroker_pg
uv lock --check --directory extensions/simplebroker_redis
python3 bin/check-dom15-fixtures
bin/check-doc-paths
git diff --check
```

## Rollout and Rollback

The rollout boundary is the merge gate: the expanded policy must be clean in
the same change that activates `extend-select`. Do not land the configuration
before the cleanup or land cleanup that depends on a disabled policy test.

If an adopted rule proves incompatible with an intentional repository
contract, stop at the registry gate. Prefer a clearer code boundary; add the
narrowest local suppression only after the invariant, proof, rejected
alternatives, independent review, and user approval are recorded. Reverting
the entire expanded policy is the last resort. Behavior-preserving cleanup can
remain even if the config activation is reverted. No data, protocol, release,
or deployment rollback is involved.

There are no one-way doors and no post-deploy product signal. Success is
observable in pull requests: the root Ruff gate discovers all Python paths,
the firing test proves both rule sources, and normal CI remains green.

## Out of Scope

- Ruff preview rules or `select = ["ALL"]`.
- Enabling Markdown formatting or changing formatter style.
- Adding a separate linter, formatter, type checker, or dependency.
- Refactoring cohesive modules, renaming public functions, or performing
  unrelated code-quality cleanup.
- Changing runtime exception, logging, retry, cleanup, storage, CLI, or backend
  behavior to satisfy a style rule.
- Package version, release metadata, or CHANGELOG changes.
- Changing extension-job topology beyond keeping their existing subset lint
  feedback aligned with the root gate.
- Accepting a changed effective rule inventory during a Ruff refresh without
  reviewing and updating the fixture.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| [DOM-10.1.1] | The eight reviewed groups covered every required local suppression. | Full core verification initially exposed `RUF022` after `simplebroker.commands.__all__` was sorted. The exact-order test was then found to encode presentation order without a documented or consumer-backed contract. The list remains sorted, the test now pins exact membership, and no suppression was added. | Ruff owns deterministic ordering; the public-surface test owns names. Treating list order as API added a false constraint and would have created an avoidable exception. | No spec exception required. |
| [DOM-10.1.1] | The suppression-registry test would distinguish approved suppression comments from other source text. | Post-commit verification included the newly tracked policy-test file in `git ls-files`; the raw-line scanner then mistook its own `SUPPRESSION_REASON` string constant for a malformed suppression. The pre-commit run had excluded that untracked file. | The scanner now examines Python `COMMENT` tokens. String literals cannot self-match, while malformed approved `# noqa` comments still fail with an exact path and line. | No spec change required. Verification must reproduce tracked-file inventory when a test derives scope from Git. |
| [DOM-10.1.1] | Exact registry locations would compare portably on every supported CI platform. | All four Windows jobs rendered `Path.relative_to()` with `\`, while the repository-owned spec correctly records portable `/` paths. Linux and macOS therefore passed the same assertion. | The scanner now serializes repository-relative paths with `Path.as_posix()`. The registry stays platform-neutral and Windows observes the same location keys as other platforms. | No spec change required. Repository path inventories must use repository syntax, not host-native display syntax. |

## Approved Suppression Disposition

The durable approved registry is
`docs/specs/01-development-documentation-operating-model.md` [DOM-10.1.1].
Independent T8 review passed all eight groups, and the user approved every
group on 2026-07-29. This plan records the implementation decision and evidence,
not the lasting exception inventory.

## Revision Log

| Date | Reviewed baseline | Revision | Reason | Re-review |
|------|-------------------|----------|--------|-----------|
| 2026-07-29 | `22be1ca11c089d890a68bc346d2d18e9bbd56c47` | Added extensionless-script discovery/config, full effective-rule inventory, firing gates for every structural [DOM-10.1] requirement, public-annotation compatibility, two intermediate reviews, and unchanged CI formatter scope. | Independent review found the original discovery claim false and several policy elements ungated. | Round 2 passed on `90d971a23252290cb2cbc781b9f140603552590d`. |
| 2026-07-29 | `1684f6ecb1ea7fb555bc7828ad9821d30364589c` | Made zero new suppressions the target; added a suppression exception registry and user approval gate; replaced the planned `_handle_retry` suppression with a tested structural split. | User explicitly disfavored suppressions. | Scoped re-review required. |
| 2026-07-29 | `7546185b569135433021b66231a7b23ed17596ee` | Closed the early-suppression leak; added approved-suppression application, testing, and exact-diff review before activation; defined the permitted traceback contract and preserved terminal `_handle_retry` invocation. | Scoped review found three blockers in the first suppression-strategy revision. | Scoped confirmation passed on `1c93a3c63168921f51d2cbce77af3f983004b4c8`. |

## Review Log

| Review | Date | Verdict | Disposition |
|--------|------|---------|-------------|
| Independent agent plan review, round 1 | 2026-07-29 | BLOCKED | Accepted all five findings. Added scoped `bin/*` inclusion and tracked discovery proof; gated preview, rule inventory, ignores, workflow and formatter structure; protected exported annotations; added meaningful-slice reviews; removed formatter-scope expansion. |
| Independent agent plan review, round 2 | 2026-07-29 | PASS | Verified all five accepted corrections. A real override probe found all 293 expected tracked Python files within Ruff's 296 discovered files; no new defect was introduced. |
| Scoped suppression-strategy review, round 1 | 2026-07-29 | BLOCKED | Accepted all three findings. Removed T5's early-suppression permission, made approved-suppression application and exact-diff review precede activation, and replaced ambiguous traceback identity with an explicit exception/traceback/hook contract. |
| Scoped suppression-strategy review, round 2 | 2026-07-29 | PASS | Verified the zero-suppression target, pre-activation approval/application gate, exact-diff review, and `_handle_retry` exception/hook contract; no new defect was introduced. |
| Mechanical/resource implementation review | 2026-07-29 | PASS after correction | Restored the exported `__enter__` and `__exit__` typing contracts after concrete downstream mypy probes exposed incompatibilities; fixed two branch-precedence regressions and one stale import. Reviewer then confirmed context ordering, resource ownership, subprocess behavior, watcher traceback behavior, shebang decisions, existing-policy Ruff, targeted tests, mypy, and diff checks. |
| Exception-boundary implementation review | 2026-07-29 | PASS before user disposition | Restored six exact logging contracts and checkpoint failure containment, added firing public-typing and stdout-lifetime proofs, and corrected registry accounting. Reviewer confirmed all 124 surviving diagnostics match the eight registry groups and protect real public, cleanup, containment, transaction, concurrency, or process-boundary invariants without a clearer behavior-preserving rewrite. |
| Grok 4.5 completed-work review | 2026-07-29 | PASS | Reviewed the final lint-policy, cleanup, suppression, test, spec, and consumer changes. It found no production blocker. Its principal actionable residual was that the policy test proved registry counts and reasons but not exact file-and-line fidelity; the test now compares every approved local directive with the durable [DOM-10.1.1] location inventory. Other residuals were either pre-existing, separately owned by the toolchain-refresh landing unit, or covered by the full verification below. |

## Completion Evidence

The implementation and required verification pass in the working tree. The
plan remains active, rather than claiming a completed landing unit, because
these changes are not yet committed.

T4-T8 reduced the expanded-policy baseline from 668 diagnostics to 124 approved
exceptions: 98 `BLE001`, 25 public-contract `PYI034`/`PYI036`, and one
`SIM115`. Those local suppressions are applied and trace exactly, by file and
line, to the durable [DOM-10.1.1] registry. `RUF100` reports no unused
suppression. Full core verification initially exposed an ordered-`__all__`
test conflict; the test was corrected to pin exact public membership rather
than unsupported presentation order, so no `RUF022` exception remains.

Observed verification on 2026-07-29:

- `ruff check .` and `ruff check --extend-select RUF100 .`: pass.
- The exact CI formatter path set: 280 files already formatted.
- `tests/test_ruff_policy.py`: 9 passed, including real rule firing, discovery,
  effective-rule inventory, workflow shape, public typing compatibility,
  stdout lifetime, portable repository-path serialization, and exact
  suppression-registry fidelity.
- Post-commit CI exposed a policy-scanner self-match that pre-commit
  `git ls-files` omitted. Token-aware comment scanning now passes with the
  policy test tracked and retains malformed-comment detection.
- GitHub Actions run `30493235231` exposed host-native path serialization on
  all four Windows jobs. POSIX serialization now matches the portable spec
  inventory on every platform.
- Core: 2018 passed, 17 skipped.
- PostgreSQL fast partitions: 989 shared-core passed, 3 skipped; 146 extension
  passed, 5 skipped.
- Redis fast partitions: 982 shared-core passed, 10 skipped; 158 extension
  passed, 1 skipped.
- Core, PostgreSQL, Redis, and example mypy partitions: pass.
- Root, PostgreSQL, and Redis `uv lock --check`: pass.
- DOM-15 fixtures, document paths, and `git diff --check`: pass.
- Independent mechanical/resource, exception-boundary, and Grok 4.5 reviews:
  pass after the recorded corrections.
