# Ruff Default Drift During Release

Class: 3+P (effective 5): revises the static-analysis process contract to stop freezing an upstream tool's complete default rule inventory. Escalated from Class 2 release preparation after the real release gate exposed Ruff 0.17 default drift.
Plan type: implementation with spec revision, strategy B (atomic).
Owner: repository maintainer; tests own proof of repository rule selection.
Boundary: Ruff policy test, its obsolete fixture and [DOM-10.1]; no broker runtime or release-gate bypass.
Verification: real Ruff 0.16.10 and 0.17.0, controlled bad selection, full release driver and exact-SHA CI.
Required action: independently review the exact delta, apply it atomically, verify and close the index row after release acceptance.

## Goal

Ordinary Ruff updates must not fail merely because upstream stable defaults change. Prove the repository extends the running Ruff's defaults with its explicit families, while keeping real lint findings, file discovery, rule-firing and suppression checks fatal. Owner authority: the current thread's instruction that the CI policy check must not enforce versions or break ordinary updates, and the instruction to clean source for new Ruff checks.

## Source Documents

Consulted startup surfaces: `docs/program-theory.md`, `docs/agent-context/README.md`, `decision-hierarchy.md`, `principles.md`, `engineering-principles.md`, the runbooks `writing-plans.md`, `hardening-plans.md`, `review-loops-and-agent-bootstrap.md`, and `docs/lessons.md`. The test-audit and diagnosing-bugs skills govern test value and causal proof.

- `docs/specs/01-development-documentation-operating-model.md` [DOM-10.1], [DOM-15], [DOM-16].
- Theory: N/A — lint-tool policy does not alter product concepts; [THEORY-4] still requires a small use surface and no runtime changes for process trivia.
- `tests/test_ruff_policy.py`, `tests/fixtures/ruff-enabled-rules.txt`, `pyproject.toml`, `bin/release.py`, `CONTRIBUTING.md`.

## Spec Baseline and Proposed Spec Delta

Baseline: `97737df`, [DOM-10.1]. Strategy B: replace its introductory paragraph and Verification/Required action paragraph in the same change as the test repair.

Exact introductory text:

> SimpleBroker's Python lint gate extends Ruff's stable defaults with the repository's explicit `E`, `W`, `F`, `I`, `B`, `C901`, `C4`, `UP`, and `PLR2004` selections. The configuration must extend Ruff's defaults rather than replace them. CI uses the version resolved in `uv.lock`; local tool updates within the declared dependency range may use newer defaults without maintaining a copied upstream rule list.

Exact Verification/Required action text (retain Owner and Boundary):

> Verification: `tests/test_ruff_policy.py` invokes the real Ruff binary, compares effective discovery with tracked files and effective rule selection with that binary's isolated defaults plus the reviewed explicit rule families, compares real diagnostics with the binary's isolated default diagnostics, and proves a retained legacy-family rule fires. Required action: review and fix real diagnostics when refreshing Ruff; do not freeze tool versions or copy the complete upstream default inventory into a fixture.

## Context, Invariants and Rollback

`_enabled_rules` currently parses real `ruff check --show-settings`; reuse it with CLI arguments for isolated defaults and explicit family selection. The failing snapshot has seven DTZ rules removed by Ruff 0.17; upstream release notes and a two-version real-binary comparison agree. Source, formatting and the suppression checker already pass Ruff 0.17.

Do not change lint selection, ignores, suppressions, runtime code, dependency ceilings, worker counts or test budgets. Expected selection is `(isolated defaults union isolated explicit families) minus GLOBAL_IGNORES`. Run real binaries, not mocks. Derive the live-fire probe's default diagnostic expectation from an isolated real Ruff invocation of the same probe; keep B904 mandatory because the repository explicitly selects B. No candidate-rule list or replacement upstream snapshot is needed. Keep complexity, file-discovery, scope and suppression probes. A mismatch stays fatal. Rollback is an ordinary targeted revert before publication; immutable tags and published packages cannot be rolled back. No new lifecycle or persistence is introduced.

Comprehension: why not update the snapshot? Expected answer: it repeats the same maintenance failure on the next upstream default change. Why keep live-fire probes? Expected answer: equality of configuration inventories alone does not prove lint diagnostics fire. Both answers were confirmed before implementation.

## Tasks and Testing Plan

1. [x] Independent plan/delta review; stop for lost lint coverage or ambiguous expected selection.
2. [x] Atomic spec/test/fixture change: parameterize `_enabled_rules`, derive expected rules from real isolated Ruff invocations, delete only the obsolete rule-list fixture, add a Related Plans backlink and note the correction in this release's Development entry. Stop if a real diagnostic requires runtime changes.
3. [x] Red/green proof: original snapshot fails under real Ruff 0.17; repaired test passes 0.16.10 and 0.17. Put a temporary root `ruff.toml` with only explicit legacy selection in the discovery path: structural pyproject test must pass, derived effective-selection test must fail. Then use a temporary root config extending pyproject but adding ASYNC to prove unexpected surplus fails too. Remove each control exactly; no control is committed. This protects the effective-discovery boundary, not just the already-tested pyproject structure.
4. [x] Independent completed-change review, lint/format/suppression and document gates, targeted commit, pull, then rerun `uv run --locked python bin/release.py all` without skipping checks. Exact-SHA CI precedes tags; validate immutable GitHub and PyPI artifact hashes/metadata and clean installed artifacts after publication.

## Independent Review Loop

Give a separate reviewer this plan, exact proposed delta, current test, fixture consumers and baseline. Ask whether the derived oracle independently detects lost defaults, whether any deleted contract loses its only proof, and whether the scope adds needless machinery. Resolve findings before implementation and repeat review after proof.

## Deviation Log

No deviations recorded. Promotion and release baseline:
`6923276bb7aa72f53000bc2f1d9266c42f1d2965`, containing the atomic [DOM-10.1]
spec/test repair reviewed against `97737df`.

## Execution Evidence

- Original release gate: one snapshot failure, 3,201 passes and 18 skips before fail-fast stopped collection; no tags created.
- Minimal original test failed deterministically in 0.81 seconds with Ruff 0.17.0. Missing codes: DTZ001, DTZ005, DTZ006, DTZ007, DTZ011, DTZ012, DTZ901. Explicit repository config and isolated upstream defaults both confirm the change; no parsing failure or wrong config.
- Ruff 0.17 source check and formatting: passed, 350 files formatted; suppression registry check passed.
- Claude plan review completed with two required corrections. B1 accepted with the smaller same-probe isolated diagnostic oracle instead of a new candidate-rule inventory; B2 accepted with real root config shadowing and missing/surplus controls. N1/N4 accepted (complete selections and declared range). N2 is a pre-existing [DOM-10.1] per-file-ignore prose contradiction, not changed here. N3 consumer search found only the owning test and historical plan evidence. N5 binary provenance remains PATH under the canonical uv runner; no locked-binary claim is made.
- Separate native Codex scoped confirmation passed the revised plan/delta without blockers. Promotion baseline: [DOM-10.1] atomic worktree delta against `97737df`.
- Real Ruff 0.16.10: all 11 policy tests passed via `uv run --extra dev --with ruff==0.16.10 pytest tests/test_ruff_policy.py -q`. Real Ruff 0.17.0: all 176 policy/release-workflow/release-script tests passed via the canonical parallel runner. Lint, 350-file formatting, suppression registry, all three docs gates and diff whitespace passed.
- Missing-default control: temporary root `ruff.toml` selected only explicit families; the structural test passed and effective-selection test failed on missing defaults. Surplus control: temporary root config retained all selections/ignores and added ASYNC; missing set was empty and added set exactly ASYNC109, ASYNC110, ASYNC212, ASYNC240, ASYNC250; structural test passed and effective-selection test failed. Both root controls were removed.
- Completed-change independent review identified one genuine lost guard: a shared parser returning three empty sets could falsely satisfy equality. Added a nonempty parse assertion. A process-local empty-parser control was rejected by that guard; restored real parsing passed. Scoped independent confirmation returned PASS with no remaining blockers. No runtime, lint selection, suppression, ceiling or timing change was needed.

## Release Acceptance — 2026-10-13

`uv run --locked python bin/release.py all` completed with exit 0 against
`6923276bb7aa72f53000bc2f1d9266c42f1d2965`, after a fresh `git pull --ff-only`.
No checks were skipped. Local acceptance: core 4,016 passed / 18 skipped;
nested coverage 1 passed; benchmarks 20 passed; PostgreSQL shared 1,767 passed /
11 skipped and extension 410 passed / 7 skipped; Redis shared 1,759 passed /
19 skipped and extension 379 passed / 1 skipped; examples 143 passed. Shell,
lint, formatting, all type-check tiers, three lockfiles and clean Python 3.11
artifact installs passed.

An earlier full local attempt had one Redis benchmark timing miss (20 ms
average against its approximately 13 ms calibrated limit), with zero operation
errors. Its code and the Redis/watcher runtime were unchanged from released
`f578fc5`. The exact unchanged isolated benchmark passed, followed by the full
unchanged release gate above. No threshold, worker count, serialization policy
or runtime change was made. Residual wall-clock benchmark variability remains;
the passing rerun does not establish that every timing fluctuation is eliminated.

All exact-SHA CI passed before tag creation:

- [Test 38458241452](https://github.com/VanL/simplebroker/actions/runs/38458241452): all 22 final jobs successful, including Python 3.15 on Linux, macOS and Windows and the final coverage aggregation job.
- [PostgreSQL 38458241729](https://github.com/VanL/simplebroker/actions/runs/38458241729) and [Redis 38458241711](https://github.com/VanL/simplebroker/actions/runs/38458241711): successful, including native Linux Python 3.15.
- [CodeQL 38458240988](https://github.com/VanL/simplebroker/actions/runs/38458240988) and [Scorecard 38458241224](https://github.com/VanL/simplebroker/actions/runs/38458241224): successful.

The helper created all three immutable tags at that exact SHA. Publication and
immutable GitHub Release completion passed in [core gate 38461259577](https://github.com/VanL/simplebroker/actions/runs/38461259577),
[PostgreSQL gate 38461246478](https://github.com/VanL/simplebroker/actions/runs/38461246478)
and [Redis gate 38461254352](https://github.com/VanL/simplebroker/actions/runs/38461254352).
Published versions: [SimpleBroker 8.7.0](https://pypi.org/project/simplebroker/8.7.0/),
[simplebroker-pg 4.7.0](https://pypi.org/project/simplebroker-pg/4.7.0/) and
[simplebroker-redis 4.7.0](https://pypi.org/project/simplebroker-redis/4.7.0/).
Remote tag reads and GitHub release APIs verified `v8.7.0`,
`simplebroker_pg/v4.7.0` and `simplebroker_redis/v4.7.0` at the release SHA,
with final, non-prerelease, immutable releases and exactly one wheel, one
sdist and one combined Sigstore bundle each.

Downloaded all six PyPI distributions and checked their bytes against PyPI
SHA-256 and GitHub asset digests. Wheel METADATA and sdist PKG-INFO confirm
the exact versions, unchanged `Requires-Python: >=3.11`, Python 3.15
classifiers, core extras requiring extension 4.7.0 or newer and both
extensions requiring core 8.7.0 or newer:

| Distribution | Verified SHA-256 (PyPI and GitHub) |
|---|---|
| simplebroker-8.7.0-py3-none-any.whl | `c8bc4e9297c8aed6f7c981122182f9b5cc71d60cb5deaa8558859a5f22c3cb50` |
| simplebroker-8.7.0.tar.gz | `3b9c93a9340b38f4acde8759604543d55d3af943978447bae91854cbf7e99f56` |
| simplebroker_pg-4.7.0-py3-none-any.whl | `b3db95824f199270db4d4687bd2f9b859a6e2d3f508cf94e1d8b33d720ffe2c6` |
| simplebroker_pg-4.7.0.tar.gz | `3110f698c80b7ab04f14a729a3fa275a3d191735664f832b2e48ec06efe37f17` |
| simplebroker_redis-4.7.0-py3-none-any.whl | `663c206dc5fff3796379c4aaf433cc37f3b8fcdf9204605a8d4a26f9878ba8c1` |
| simplebroker_redis-4.7.0.tar.gz | `7906b6af89c7debbdcf58357f0400529e63ca2aa3989350dba8851cc07bbe94f` |

`uv run --locked ./bin/packaging-smoke --python 3.15 --published-version 8.7.0`
passed for the downloaded core wheel and sdist outside the checkout, including
real queue reuse after early iterator close. A fresh isolated Python 3.15
environment installed all three exact published versions together and passed
version, import and backend-plugin discovery checks. Refreshing uv's cached
index was needed to see the just-published extension version; no package or
dependency metadata correction was required. Standard GIL-enabled support,
not free-threaded or JIT-specific qualification, is the released scope.

Documentation and traceability are reconciled in the promoted [DOM-10.1],
test, changelog and this plan. The existing test-audit/diagnosis guidance
covered the causal proof and independent-review correction; no additional
skill or runbook change was needed.

## Out of Scope and Fresh-Eyes Review

No new style rules, blanket skips, tool ceiling, native CI expansion, runtime refactor, test-budget change, downstream edits or release authentication changes. Existence-check every named path and flag, verify both real versions and the negative control, and close this plan and its Status Index row with concrete evidence. A full default snapshot is not a product invariant.
