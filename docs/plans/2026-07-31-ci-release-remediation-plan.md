# CI and 6.0.0 Release Remediation Plan

Class: 4 — the work spans multiple OS execution contexts and ends in the
one-way publication of immutable Git tags and PyPI artifacts.

## Goal

Repair every independent failure in the current `main` CI run without changing
SimpleBroker behavior or weakening the contracts the tests protect. Land each
root-cause fix in its own commit, restore green required workflows, then run
`bin/release.py all` and monitor the exact release workflows through terminal
success or a precisely reported blocker.

## Source Documents

Source spec: None — CI portability and test-lifecycle bug fixes plus an
explicitly requested release retry. Product behavior is not intended to
change.

- `AGENTS.md` Definition of Done and release/public-package constraints
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/plans/2026-07-31-core-test-mypy-gate-plan.md`
- `docs/plans/2026-07-30-ruff-suppression-index-generator-plan.md`
- `docs/plans/2026-07-30-runner-transaction-ownership-and-reactor-correctness-plan.md`
- `bin/release.py` and `.github/workflows/release-*.yml`

## Context and Key Files

- GitHub Actions run `30651950369` is the red-capable system proof. Its job
  logs reduce the broad matrix failure to four independent causes:
  1. `tests/test_ruff_policy.py` imports optional `simplebroker_redis` from the
     root-only test environment.
  2. `tests/test_ruff_suppression_index.py` constructs a CRLF fixture through
     a platform-translating text write.
  3. The same file tries to create `probe|unsafe.py`, which Windows rejects
     before the production validator runs.
  4. On Windows, `_skip_unavailable_fork_transition()` recognizes
     `FORK_RESET` but not `FORK_ACTIVE_RESET`. The latter opens a real
     transaction, then `_assert_runner_fork_reset()` skips because `os.fork`
     is absent; pytest unwinds before rollback and `runner.close()`, so fixture
     GC reports the unclosed SQLite connection.
- `tests/test_ruff_policy.py` owns root Ruff and annotation-policy contracts.
  Redis-specific annotation proof must stay in the Redis extension suite or
  use an explicit isolated source-path setup; it must not make Redis a root
  runtime dependency.
- `bin/ruff_suppression_index.py` owns byte-preserving Markdown index updates
  and rejects paths that cannot be rendered safely. Tests must feed the same
  bytes and reach that production validation on every supported OS.
- `simplebroker/_runner.py` owns runner connection reset and close semantics;
  `tests/test_core_persistence_transition_tables.py` fires every named
  transition. Its platform gate must skip both real-fork cases before runner
  construction where `os.fork` is unavailable. Production fork recovery must
  continue abandoning inherited connections rather than closing them.
- `bin/release.py all` selects unpublished core and extension versions, runs
  local gates, pushes `main`, waits for exact-SHA required workflows, and only
  then creates immutable tags whose workflows publish to PyPI and GitHub.

Comprehension gates before editing:

1. Can the root suite prove core annotation compatibility without importing an
   optional extension while the Redis suite keeps the Redis-specific proof?
2. Does each Windows portability fix make the original production path run,
   or merely skip the assertion that CI exposed?
3. Does the platform gate run before any runner connection is constructed for
   both transitions whose proof requires a real `os.fork`?

## Invariants and Constraints

- Do not change queue, runner, delivery, transaction, or release semantics to
  make tests pass.
- Preserve the public `__enter__`/`__exit__` annotation compatibility checks
  for both core and Redis exports; moving a proof to its owning suite is
  allowed, deleting it is not.
- Preserve byte-for-byte content outside generated suppression-index markers,
  including CRLF and non-ASCII bytes.
- Preserve fail-closed rejection of Markdown-unsafe source paths. The test
  must reach the production validator on Windows rather than fail during
  fixture setup.
- No fork-only transition may construct a runner on a platform without
  `os.fork`. On POSIX, preserve the deliberate rule that child recovery
  abandons inherited parent connections and never closes them.
- Each independent root cause gets one commit after targeted pytest, mypy when
  typing is touched, `ruff check`, and `ruff format --check` pass for the
  affected files.
- Stage explicit file lists only. Before every commit, the staged path set must
  exactly match that fix.
- Never move, delete, or reuse a remote release tag. Stop if any selected tag
  exists at a different SHA or any selected version is already published.
- No new dependency, no broad ignore, no platform-wide skip, and no unrelated
  cleanup.

## Rollback, Rollout, and One-Way Doors

Each CI fix is independently revertible before publication. Push fixes in
dependency order and require the exact `main` SHA's root, PostgreSQL, and Redis
workflows to succeed before tagging. `bin/release.py all` owns that sequencing.

Remote tag creation and PyPI publication are one-way doors. Before invoking
the release command, prove a clean `main`, exact parity with `origin/main`,
green required workflows, absent selected remote tags, and absent selected
PyPI versions. If a tag exists, do not recreate or move it: inspect the tag
SHA and workflow state. If publication succeeds but a later release step
fails, repair forward with the release driver's documented retry path or a
new patch version rather than rewriting history.

Post-release success is: all selected Release Gate workflows conclude
`success`; GitHub releases are published at their immutable tag SHAs; PyPI
reports the selected core and extension versions.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Tasks

1. Restore the optional-extension boundary in annotation policy tests.
   - Files: `tests/test_ruff_policy.py` and
     `extensions/simplebroker_redis/tests/test_redis_plugin_contract_edges.py`.
   - First prove the failure in a root-only import environment.
   - Keep core proof in the root suite and Redis proof in the Redis suite; do
     not install the extension into every root matrix job.
   - Root red/green command:
     `uv run --isolated --frozen --extra dev pytest -q tests/test_ruff_policy.py::test_public_context_manager_annotations_remain_override_compatible tests/test_ruff_policy.py::test_public_exit_annotations_keep_any_typed_parameters`.
   - Redis proof runs the owning extension test module and its release-helper
     mypy partition from the extension directory.
   - Done: focused root and Redis tests, their mypy partitions, Ruff check, and
     Ruff format check pass; one root-cause commit exists.

2. Make the CRLF fixture byte-exact on every OS.
   - File: `tests/test_ruff_suppression_index.py`; production code only if a
     byte-exact fixture proves the bug is in production rather than setup.
   - Write the intended bytes without newline translation, run the production
     CLI, and assert bytes outside generated markers remain unchanged.
   - Stop and re-plan if fixing the test would narrow the production byte
     preservation contract.
   - Done: targeted test plus full affected module, Ruff check, and Ruff format
     check pass; one root-cause commit exists.

3. Use a cross-platform construct for the Markdown-unsafe path test.
   - File: `tests/test_ruff_suppression_index.py`; production code only if a
     legal Windows path reaches and defeats the validator.
   - Rename the fixture to ``probe`unsafe.py``. Backtick is legal in a Windows
     filename but `_index_path()` rejects it because an unescaped backtick
     cannot be represented safely in the Markdown table. Do not skip Windows
     and do not mock `Path`.
   - Done: the targeted production CLI assertion and affected module pass with
     Ruff gates; one root-cause commit exists.

4. Pre-skip every real-fork transition on platforms without `os.fork`.
   - File: `tests/test_core_persistence_transition_tables.py` only.
   - Extend `_skip_unavailable_fork_transition()` to recognize both
     `FORK_RESET` and `FORK_ACTIVE_RESET`, so pytest skips before constructing
     `SQLiteRunner`. Add a direct helper regression that simulates missing
     `os.fork` and proves both cases skip while a non-fork case does not.
   - Do not change `_assert_runner_fork_reset()` or production runner recovery;
     child abandonment and parent ownership are intentional.
   - Stop and re-plan if any production runner lifecycle behavior must change.
   - Done: the helper regression, exact transition module, warning-strict run,
     mypy, and Ruff gates pass; one root-cause commit exists.

5. Reconcile all current CI evidence and run final local gates.
   - Inspect every failed job in run `30651950369`; distinguish fixed causes
     from cascades and independently reproduce any newly discovered failure.
   - Run the full non-benchmark root suite and release preflight checks, plus
     the PostgreSQL and Redis suites required by the release driver.
   - Obtain an independent completed-work review and disposition every finding.

6. Release and monitor.
   - Confirm clean `main == origin/main`, selected versions/tags remain free,
     and exact-SHA required CI is green.
   - Run `bin/release.py all` once. Do not retry blindly after a nonzero result;
     first inspect local commits, remote tags, PyPI, GitHub releases, and exact
     workflow state.
   - Poll release workflows with `gh` at a bounded interval until all selected
     packages succeed or a concrete failure requires a new remediation cycle.

7. Close the plan.
   - Record commit SHAs and current-state verification evidence.
   - Update the Status Index row to `completed` only after release monitoring
     reaches a terminal successful state.

## Testing Plan

Use the real root and extension environments, real subprocess invocations of
mypy and the suppression-index CLI, real filesystem bytes, and real SQLite
connections. Do not mock package imports, `Path`, SQLite connections, runner
reset, or the release workflow state. GitHub and PyPI are observed through
their real CLIs/APIs; no publication action occurs before the release task.

Per-fix commands will start with the exact failing test, then the full affected
module. Each commit also runs:

```bash
uv run --frozen --no-sync ruff check <affected-files>
uv run --frozen --no-sync ruff format --check <affected-files>
```

Typing-sensitive commits run the owning root or extension mypy partition from
`bin/release.py`. Final verification uses `bin/release.py all`'s preflight as
the authoritative release gate rather than a hand-maintained approximation.

## Verification and Gates

- Red-capable feedback: exact failed tests and job logs from run `30651950369`.
- Per cause: targeted red/green proof, full affected module, Ruff check and
  format check, plus owning mypy partition where relevant.
- Before release: clean worktree; `main == origin/main`; current root,
  PostgreSQL, and Redis CI success at the exact SHA; selected tags and package
  versions absent.
- Release: `python3 bin/release.py all` (or the repository-documented
  interpreter invocation if the script requires the managed environment).
- Monitor: `gh run list` and `gh run view` matched by exact tag and SHA until
  terminal; verify GitHub releases and PyPI versions afterward.

Fatal failures: any test or static gate, version/tag collision, required CI
failure, release workflow failure, or publication mismatch. Best-effort only:
dependency-graph workflows and local reporting artifacts that the release
driver does not classify as required.

## Independent Review Loop

Before implementation, a separate agent reviews this plan, run `30651950369`
evidence, the four affected test/production areas, and `bin/release.py`. It
must answer PASS or BLOCKED on implementability and whether the plan would
degrade the system. Every finding is accepted, rejected with evidence, or
marked out of scope before editing.

After fixes and before publication, a separate completed-work review examines
the four per-cause commits and current verification evidence. Publication does
not begin with an unresolved blocker.

## Review Log

| Date | Reviewer | Verdict | Findings and disposition |
|------|----------|---------|--------------------------|
| 2026-07-31 | Independent agent plan review | BLOCKED | Accepted the safety finding: Task 4 must not close an inherited connection or change production fork recovery. Corrected the factual attribution with completed Windows job `91227066890`: `FORK_ACTIVE_RESET` is not recognized by the outer pre-skip, opens a transaction, and then skips inside `_assert_runner_fork_reset`, bypassing cleanup. Narrowed the fix to the test's pre-construction platform gate. Accepted the precision findings by naming the Redis owner file/commands and the Windows-legal backtick fixture. Scoped round-2 review required before implementation. |
| 2026-07-31 | Independent agent plan review, round 2 | PASS | Verified the accepted corrections against the exact transition control flow and Windows job evidence. The pre-construction skip preserves production fork abandonment, the Redis proof has an owning extension target, and backtick reaches the cross-platform Markdown rejection path. No new defect found. |

## Out of Scope

- Product behavior, public API, storage format, and delivery semantics.
- Refactoring the release driver or CI matrix unless a reproduced release
  blocker requires it.
- Repairing the older, non-current Python 3.14 concurrency timeout unless it
  reproduces against the final exact SHA.
- Coalescing plans or lessons.

## Fresh-Eyes Review

The plan is incomplete if any root cause lacks a direct red-capable proof, if a
Windows fix skips the affected platform rather than reaching production code,
if a Redis annotation contract disappears, or if publication can occur before
exact-SHA CI success. Reclassify and re-review if any fix crosses into product
behavior or release-driver semantics.
