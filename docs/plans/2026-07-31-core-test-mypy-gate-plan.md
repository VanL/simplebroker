# Core-Test Mypy Gate Plan

Status: completed
Class: 5+P — materially changes the repository-wide verification process by
making the core test tree a required strict-mypy gate; no product contract,
CLI behavior, storage format, or runtime behavior changes.

## Goal

Add `tests/` to the same explicit mypy verification surface used by CI and the
local release helper, then repair the resulting annotation errors without
changing the behaviors and invariants that the tests exercise.

## Source Documents

- Source spec: None — internal verification-tooling change requested directly
  by the product owner.
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-10], [DOM-11], [DOM-15].
- `docs/agent-context/runbooks/writing-plans.md` and
  `docs/agent-context/runbooks/testing-patterns.md`.

## Context and Key Files

- `pyproject.toml` owns mypy discovery exclusions. It keeps the root `tests/`
  tree excluded from ambient discovery; CI and the release helper must instead
  pass a concrete sorted file list, just as they do for extension tests.
- `.github/workflows/test.yml` owns the normal CI mypy invocation. It currently
  checks production modules and explicitly enumerates extension test files.
- `bin/release.py` owns the corresponding local release precheck commands and
  already has reusable concrete-Python-file discovery for examples and
  extension tests.
- `tests/test_release_script.py`, `tests/test_release_workflow.py`, and
  `tests/test_ruff_policy.py` pin the release-helper and CI command shapes.
- `README.md` documents the developer and release verification commands.

Comprehension checks before editing:

1. Confirm that mypy exclusions affect directory discovery, and that explicit
   file lists are the established way to type-check an otherwise excluded tree.
2. Confirm that the release helper scopes root, PostgreSQL, and Redis prechecks
   separately, so core tests are added only where the root package is checked.

## Invariants and Constraints

- Do not change production queue behavior, test assertions, fixtures, test
  data, or their asserted runtime invariants merely to satisfy typing.
- Reuse `bin/release.py`'s concrete file-discovery pattern. Do not introduce a
  second traversal or make mypy rely on ambient discovery.
- CI and root-release prechecks must both cover every `tests/**/*.py` file,
  excluding only `__pycache__` artifacts; extension-release prechecks retain
  their current target scope.
- Keep examples and extension-test handling intact. No new dependencies,
  global strictness reductions, or `--follow-imports=skip`; the core-test gate
  must type-check the imported local example rather than hiding its boundary.
- The core-test command may use the narrow namespace-package mapping required
  for the existing `examples.multi_queue_watcher` import. It must retain the
  root configuration's strict checks and must not change how production or
  extension source partitions resolve modules.
- Existing pytest helper and test-function signatures may remain untyped in
  this partition (`--allow-untyped-defs --allow-incomplete-defs`), but
  `check_untyped_defs = true` remains active. All test bodies and typed
  interfaces are therefore checked; this avoids 1,354 mechanical signature
  annotations that do not increase the gate's behavioral coverage.
- A typing fix may refine local annotations, casts, or test-only helper types;
  it must preserve the existing test's observable setup, action, assertion,
  and cleanup sequence.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Tasks

1. Add a failing structural proof for core-test mypy coverage.
   - Files: `tests/test_release_script.py`, `tests/test_release_workflow.py`,
     `tests/test_ruff_policy.py` as appropriate.
   - Prove that CI and the root release precheck invoke mypy over every core
     Python test file exactly once, with the local namespace mapping that lets
     mypy analyze the imported example; extension-only release prechecks must
     not inherit root-test scope.
   - Run the focused structural tests and record the red result before
     implementation.

2. Resolve the test import graph, then extend the canonical command builders
   and CI invocation.
   - Files: `bin/release.py`, `.github/workflows/test.yml`, and `pyproject.toml`
     only if needed to make the selected invocation discover the intended
     explicit files.
   - Replace the source-tree Redis import in `tests/test_ruff_policy.py` with
     its installed canonical module path, so mypy never sees the same source
     as both `simplebroker_redis.core` and an `extensions.*` module.
   - Reuse `_required_python_file_paths()` and the extension-test command
     pattern for a dedicated `_core_test_mypy_paths()` helper. CI uses `mapfile`
     with the same sorted `find` predicate. Keep `^tests/` excluded in the
     root configuration, and keep the test command separate from the existing
     production-source partition.
   - Use `MYPYPATH=.` plus `--namespace-packages --explicit-package-bases` only
     on this test partition, so the existing local `examples.*` import is
     analyzed as source. Use `--allow-untyped-defs --allow-incomplete-defs`
     only for existing test signatures while retaining `check_untyped_defs`.
     Do not add an import ignore or `--follow-imports=skip`.
   - Verify the structural tests turn green and inspect the generated command
     strings for root versus extension target scoping.

3. Repair mypy findings in `tests/` with behavior-preserving annotations.
   - Files: only failing files under `tests/`, unless a test-only helper's
     annotation needs a compatible adjustment.
   - Run the exact core-test file-list mypy command after each coherent group.
   - For every changed test, run its focused pytest selection. Do not mock
     production behavior or weaken assertions; the production code stays real.
   - Stop and re-plan if a required fix changes a fixture's behavior, test
     oracle, public annotation, or production code path.

4. Align developer documentation and close the traceability chain.
   - Files: `README.md`, this plan, `docs/plans/README.md`.
   - Document the complete local mypy command and state that core tests are in
     both CI and root-release type checking.

## Testing Plan

- TDD structural proof: first make the command-shape tests demand core-test
  mypy coverage, observe failure against the current exclusions/commands, then
  implement the command changes.
- Type coverage: invoke mypy with a concrete, sorted list of every
  `tests/**/*.py` file so the result proves actual test-file coverage rather
  than a possibly excluded directory traversal.
- Runtime invariants: run focused pytest files for each typed test edit, then
  the full core pytest suite. Tests execute against real code; no mocks are
  added to conceal type-only changes.

## Verification and Gates

Per-task:

```bash
uv run --frozen --no-sync pytest -q tests/test_release_script.py tests/test_release_workflow.py tests/test_ruff_policy.py
MYPYPATH=. uv run --frozen --no-sync mypy --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs <concrete sorted core-test file list> --config-file pyproject.toml
```

Final:

```bash
uv run --frozen --no-sync pytest -q
uv run --frozen --no-sync ruff check .
uv run --frozen --no-sync ruff format --check simplebroker tests bin .github/scripts extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
python3 bin/check-dom15-fixtures
git diff --check
```

The exact CI mypy partitions, including core tests, extension sources/tests,
and examples, must also pass. Rollback is a normal revert of the tooling and
annotation-only commits; no deployed runtime state or compatibility sequence
is involved.

## Independent Review Loop

Before implementation, an independent agent reviews this plan and the current
`pyproject.toml`, workflow, release helper, and command-shape tests for scope,
coverage holes, and accidental changes to test semantics. After implementation,
the reviewer receives the diff and final command evidence, verifies that
coverage is complete and target scoping remains correct, and reports findings
with dispositions recorded below.

## Review Dispositions

| Review | Finding | Disposition |
|--------|---------|-------------|
| Independent plan review | P1: explicit test files initially stop at an untyped local example import and the same Redis source is discovered under two module names. | Accepted: keep strict imported-source analysis, use the canonical Redis package import, and scope namespace-package mapping to the core-test partition. |
| Independent plan review | P2: shell command substitution is less robust than the established release-helper traversal and does not itself prove target scoping. | Accepted: add a named core-test path helper, use `mapfile` in CI, and assert exact root versus extension target scope. |
| Implementation evidence | Strict test signatures produce 1,354 missing-annotation findings, obscuring 286 body/interface findings across 56 files. | Accepted scoped variation: retain body checking and strict production partitions, but allow existing untyped test signatures; repair every remaining test-body/interface finding. |
| Implementation evidence | Mypy applies a later `--config-file` after CLI overrides, restoring `disallow_untyped_defs` and obscuring the intended test-only policy. | Accepted: place `--config-file pyproject.toml` before test-partition overrides in CI, release helper, and documented invocation. |

## Completion Evidence

- CI and the root release helper enumerate every concrete `tests/**/*.py` file
  for a dedicated mypy partition. The partition retains body checking while
  allowing legacy untyped pytest signatures, and resolves local namespace
  imports with the config loaded before its command-line overrides.
- Every test file changed by this work was individually checked with the
  partition's mypy flags, its focused pytest module (or an owning module for a
  helper script), `ruff check`, and `ruff format --check`, then committed as a
  single explicit-path commit.
- Final gates passed: the 197-file core-test mypy partition, `ruff check .`,
  `ruff format --check tests`, `tests/test_release_script.py`,
  `tests/test_release_workflow.py`, `tests/test_ruff_policy.py`, and the full
  `uv run --frozen --no-sync pytest -q` suite (exit code 0).
- Residual repository gate: the broader formatter command also reports one
  unrelated pre-existing format delta in `simplebroker/_runner.py`; that file
  is outside this plan's test and CI-scope changes and was left untouched.
