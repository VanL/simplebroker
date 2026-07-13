# Release Reproducibility and Publication Hardening Plan

## Purpose

This document is the implementation plan for two linked improvements:

1. make CI and release builds consume checked-in, current dependency locks
   instead of resolving live environments
2. make release tags and GitHub Releases write-once while preserving the
   existing PyPI Trusted Publishing flow

It also includes three small, high-value follow-ups discovered during the same
review:

- retain Codecov repository-secret authentication, make upload failures visible,
  and enforce a local coverage floor
- enforce the repository's existing full-SHA Actions convention in GitHub
  settings
- run the already-maintained Python examples in normal CI

This plan is intentionally scoped for a solo-maintained public library. It does
**not** add mandatory pull requests, independent approval, release reviewers,
or an OpenSSF badge. It hardens machine-verifiable publication boundaries.

Assume the implementer has no prior context. Follow the tasks in order. Do not
apply the irreversible GitHub settings until the compatible workflow changes
are merged to `main`.

Revised 2026-07-13 after solo-maintainer burden review: final release tags are
created only after exact-SHA CI is green, uv version changes use a checked-in
maintenance command, publication-state logic is shared outside the three
trusted-publisher workflow files, rollout is split into three reviewable PRs,
and Codecov keeps its existing `CODECOV_TOKEN` authentication while upload
failures remain visible without making a reporting-service outage block
otherwise-green CI. The earlier bounded (not exact) Hatchling decision,
delete-and-recreate draft idempotency, PyPI lag budget, closed-list
`--frozen --no-sync` invariant, example-mypy helper flag, and 85% local
coverage floor remain. Two SQLite correctness prerequisites found by the
Windows rollout gates landed separately from the three release slices: atomic
first-time bootstrap, and a PhaseLock-style idle budget that refreshes normal
lock retries only while other connections are observably committing.

## Verified Current State

Verified on 2026-07-12 (America/Chicago) at `main` SHA `7311c278`.

| Area | Current state | Evidence |
|---|---|---|
| PyPI authentication | Correct: all three packages use Trusted Publishing with `id-token: write`; no long-lived PyPI token is passed | `.github/workflows/release-gate*.yml` |
| Artifact provenance | Correct: all three release workflows generate Sigstore attestations and attach the bundle to the GitHub Release | `.github/workflows/release-gate*.yml` |
| Action pinning in YAML | Correct: all 15 `uses:` references are pinned to 40-character SHAs | `.github/workflows/*.yml` |
| Repository Actions policy | Permits all actions; full-SHA pinning is not enforced by GitHub | `GET /repos/VanL/simplebroker/actions/permissions` returns `allowed_actions: all`, `sha_pinning_required: false` |
| Root lock | Current | `uv lock --check` exits 0 |
| Postgres extension lock | Stale | `uv lock --check --directory extensions/simplebroker_pg` exits 1 |
| Redis extension lock | Stale | `uv lock --check --directory extensions/simplebroker_redis` exits 1 |
| Normal test install | Resolves live via `uv pip install --system -e ".[dev]"` | `.github/workflows/test.yml` |
| Extension test helpers | Add editable packages with `uv run --with-editable`, which creates a live overlay instead of proving the root lock | `simplebroker/_scripts.py`, `bin/pytest-redis` |
| Release build frontend | `uv build` resolves the unpinned `hatchling` build requirement in an isolated environment | `pyproject.toml`, both extension `pyproject.toml` files, release workflows |
| Release cache | Enabled in all three release build jobs | `.github/workflows/release-gate*.yml` |
| Release tag movement | The local release helper supports `--retag`, including deleting and recreating remote tags | `bin/release.py` |
| Recent tag correction frequency | 9 of 34 release tag names in the available 2026-06-11 through 2026-07-11 workflow history ran against more than one SHA, grouped into five correction events; several followed Windows-only CI failures | GitHub Actions runs for the three `release-gate*.yml` workflows |
| Release tag rules | No tag ruleset exists | `GET /repos/VanL/simplebroker/rulesets` returns only the default-branch destructive-update ruleset |
| GitHub Release immutability | Disabled; current releases report `immutable: false` | `GET /repos/VanL/simplebroker/immutable-releases` returns `enabled: false` |
| PyPI environment policy | Environment exists, but any branch or tag may target it; administrators may bypass | `GET /repos/VanL/simplebroker/environments/pypi` |
| Codecov | Coverage is calculated locally, but an upload failed with `Token required because branch is protected`; `fail_ci_if_error: false` kept the job green | `.github/workflows/test.yml` and Actions run `29214380088` |
| Example verification | 80 Python example tests, mypy, and ShellCheck pass locally; Python example checks are run by the release helper but not normal CI | `examples/`, `bin/release.py` |

## What Already Works (Do Not Replace)

Preserve these controls:

- PyPI Trusted Publishing through the `pypi` environment
- the three existing top-level release workflow filenames:
  - `.github/workflows/release-gate.yml`
  - `.github/workflows/release-gate-pg.yml`
  - `.github/workflows/release-gate-redis.yml`
- the PyPI trusted-publisher identities bound to those workflow files
- top-level workflow gates that require the relevant test workflows to pass on
  the exact release commit
- package-version-to-tag validation
- checkout by `${{ github.sha }}`
- least-privilege job permissions
- Sigstore artifact attestations
- separate root, Postgres, and Redis distributions
- existing Dependabot cooldowns and fail-closed auto-merge gate
- current default-branch rules: block deletion and non-fast-forward updates,
  without adding PR-only or approval requirements
- all timing-sensitive and contention-heavy tests exactly as tests; do not
  serialize them or relax their thresholds in this work

Do **not** consolidate publication into a reusable workflow. PyPI Trusted
Publishing identifies the calling workflow file. A cleanup that changes that
identity can break publication or require new trusted-publisher entries.

This restriction applies to the top-level workflow identity, not to checked-in
implementation helpers. Keep the three workflow filenames and publication
jobs, but put GitHub Release discovery, draft replacement, exact-SHA checks,
PyPI lag polling, and final publication state handling in one shared Python
script. Do not maintain three copies of the same API state machine in YAML.

## Locked Decisions

### Trusted Publishing stays

- Do not add `PYPI_TOKEN`, `TWINE_PASSWORD`, API tokens, or repository secrets.
- Keep `pypa/gh-action-pypi-publish` and job-scoped `id-token: write`.
- Keep the environment name exactly `pypi` for all three packages.

### Remote release tags become write-once

Once a release tag reaches `origin`, it may not move or be deleted.

- Remove the `--retag` CLI option.
- Remove the `replace_remote` tag action and remote-tag deletion path.
- A remote tag at the wrong commit is a hard error.
- Do not use the final release tag as a candidate marker. The release helper
  must push the release commit to `main`, wait for the target's required
  workflows to pass on that exact SHA, and only then create and push the tag.
- A CI failure before the remote tag exists does **not** burn the version. Fix
  the commit and rerun the helper with the same unpublished version.
- After a remote tag exists, a transient publication failure may be rerun only
  against that same tag and SHA. If recovery requires a code change, the tag
  and version are consumed; publish the fix as a new patch version.
- The pre-tag wait is resumable. If the helper is interrupted after pushing
  `main` but before pushing the tag, rerunning it at the same unpublished
  version reuses the existing commit, observes or resumes exact-SHA CI, and
  creates the tag only after success.
- Local-only tags may still be replaced before they are pushed.

This is deliberately stricter than the branch policy. Release tags are public
supply-chain identifiers, not collaboration branches. Delaying the tag until
cross-platform CI is green puts the irreversible boundary after reversible
validation instead of making routine CI discovery consume public versions.

### Immutable Releases apply only after workflow compatibility

GitHub recommends attaching all assets to a draft before publishing an
immutable release. Therefore:

1. merge the draft-first workflow changes
2. apply tag and environment policies
3. enable immutable releases last

Do not enable release immutability against the current upload-after-publish
workflow shape.

### One root lock drives repository CI

- `uv.lock` at the repository root is the execution lock for normal tests,
  lint, coverage, examples, backend test helpers, packaging smoke, and release
  builds.
- Extension lockfiles remain supported for developers working directly inside
  an extension and for Dependabot.
- All three lockfiles must pass `uv lock --check` in CI.
- A lockfile that is kept but never checked is not an acceptable artifact.

### CI uses a fixed uv version

- Pin GitHub Actions to uv `0.11.28`, routed through a per-workflow
  `env: UV_VERSION` value (as `release-gate.yml` already does) so a version
  bump is one line per workflow file.
- Every `astral-sh/setup-uv` step must reference `${{ env.UV_VERSION }}`; no
  step may carry its own literal version.
- Add `required-version = ">=0.11.11,<0.12"` under `[tool.uv]` so local
  development remains compatible across the supported uv minor line. The
  ceiling is deliberate: a too-new local uv fails with an explicit refusal
  instead of regenerating locks that the pinned CI resolver may reject.
- Add a workflow-structure test that every uv-installing workflow defines the
  same `UV_VERSION` value and that every setup-uv step references it.
- Add a stdlib-only `bin/bump_uv.py` maintenance command. It accepts the exact CI
  version and the supported local range, updates `[tool.uv].required-version`
  plus every workflow `UV_VERSION`, and regenerates all three lockfiles. It
  supports `--check` and `--dry-run`; check mode validates the range/pin
  invariant and runs all three lock freshness checks. The command validates
  that every expected workflow was updated exactly once and fails rather than
  partially updating an unknown workflow shape.
- Updating the pinned CI uv version is a normal dependency-maintenance change,
  with all lock and packaging gates required. Dependabot updates the setup-uv
  action SHA but never `version:` inputs or `required-version`. The expected
  trigger is local breakage after a Homebrew uv upgrade past the ceiling. Run
  the maintenance command with system Python (not `uv run`, because the old
  range may reject the installed uv), review its workflow and lock diffs, then
  run the normal gates. Document the exact command in the README release
  section.

### Release builds use a locked, non-isolated frontend

Add an opt-in `release` dependency group containing exact versions:

```toml
[dependency-groups]
release = [
    "build==1.5.1",
    "hatchling==1.31.0",
]
```

The exact versions must be present in `uv.lock`. Release and packaging jobs must
invoke `python -m build --no-isolation` from the root locked environment. Do not
let PEP 517 create a second environment and resolve a newer backend.

Bound the three `[build-system].requires` entries to `hatchling>=1.31,<2`. Do
**not** pin an exact backend version in published metadata: repository builds
already get the exact version from the lock plus `--no-isolation`, while an
exact pin in a published sdist constrains every downstream rebuilder (distro
packagers, future `pip install` from sdist) forever and turns each Hatchling
bump into a mandatory release of all three packages.

### Release jobs do not consume Actions caches

- Set `enable-cache: false` explicitly on the setup-uv step in all three
  release workflows.
- Do not add `actions/cache` to a release workflow.
- Normal test and fuzz workflows may continue using caches.

### Codecov keeps repository-secret authentication

- Keep the existing `token: ${{ secrets.CODECOV_TOKEN }}` input. Do not add
  `id-token: write` or `use_oidc`; changing Codecov authentication is out of
  scope.
- Set `fail_ci_if_error: true` on the Codecov action so the upload step records
  an accurate failure, but set `continue-on-error: true` on that step and emit
  an explicit workflow warning when `steps.codecov.outcome == 'failure'`.
  Codecov is external reporting, not the coverage gate; an outage must be
  visible without blocking a green local coverage result or Dependabot merge.
- Add `fail_under = 85` to the local coverage report configuration so the
  coverage gate remains meaningful even if Codecov is unavailable. Measured
  combined coverage on 2026-07-12 is 92.4%; the floor is set to catch collapse
  (an untested module landing, broken coverage wiring), not to fight refactors
  that delete well-covered code.

### Solo-maintainer operational budget

- A normal release remains one helper invocation plus unattended CI wait. It
  must not require manual tag creation, settings inspection, asset upload, or
  version recovery on the green path.
- Interrupting the helper during its pre-tag wait is safe and resumable.
- A uv compatibility bump is one checked-in command plus diff review and normal
  gates, not a hand-edited multi-file runbook.
- The completion-evidence table and first immutable release record are rollout
  artifacts, not a checklist to repeat for every later release.
- Codecov availability never overrides the local coverage gate.
- New services, bots, reviewers, or approval queues remain out of scope.

## Non-Goals

- no runtime behavior changes beyond the separately identified SQLite
  bootstrap atomicity and progress-aware lock-retry correctness fixes
- no test serialization or wider timing budgets
- no PR approval requirement
- no release reviewer requirement
- no change from Trusted Publishing to token-based publishing
- no uv workspace conversion
- no consolidation of the three release workflows
- no new release orchestration service or GitHub App
- no SBOM project in this slice
- no attempt to make old GitHub Releases retroactively immutable
- no claim of bit-for-bit reproducibility across arbitrary operating systems;
  the target is a fixed resolver, fixed build frontend, fixed source SHA, and
  cache-independent hosted build

## Repository Primer and File Map

| File or setting | Role | Planned change |
|---|---|---|
| `pyproject.toml` | Root dependencies, uv, build backend, coverage | Add release group, uv version range, bounded Hatchling requirement, and 85% local coverage floor |
| `uv.lock` | Root execution lock | Add exact release tools and refresh metadata |
| `extensions/simplebroker_pg/pyproject.toml` | PG build metadata | Bound Hatchling build requirement to `>=1.31,<2` |
| `extensions/simplebroker_pg/uv.lock` | PG-local lock | Regenerate and prove current |
| `extensions/simplebroker_redis/pyproject.toml` | Redis build metadata | Bound Hatchling build requirement to `>=1.31,<2` |
| `extensions/simplebroker_redis/uv.lock` | Redis-local lock | Regenerate and prove current |
| `.github/workflows/test.yml` | Main matrix, lint, packaging, coverage | Frozen sync/run, lock job, examples, local coverage floor, and visible secret-auth Codecov failures |
| `.github/workflows/test-pg-extension.yml` | PG service and extension gates | Frozen root environment and explicit uv version |
| `.github/workflows/test-redis-extension.yml` | Redis service and extension gates | Frozen root environment and explicit uv version |
| `.github/workflows/release-gate.yml` | Core publication | Cache-free locked build and draft-first immutable release flow |
| `.github/workflows/release-gate-pg.yml` | PG publication | Same release mechanics for PG tag pattern |
| `.github/workflows/release-gate-redis.yml` | Redis publication | Same release mechanics for Redis tag pattern |
| other `.github/workflows/*.yml` | CodeQL, Scorecard, fuzz, Dependabot | Explicit uv version where applicable; otherwise preserve behavior |
| `bin/bump_uv.py` | uv maintenance automation | Update the local uv range and every workflow pin atomically, regenerate all three locks, and support check/dry-run modes |
| `simplebroker/_scripts.py` | PG test and packaging developer helpers | Replace live editable/build overlays with locked root-project invocations; no broker runtime semantics change |
| `bin/pytest-redis` | Redis service-test helper | Replace live editable overlay with locked root-project invocation |
| `bin/release.py` | Maintainer preflight, release commit, CI wait, tag creation, release checks | Remove remote retagging, require exact-SHA CI on `main` before the tag push, add fail-closed repository-settings verification, and expose `--check-example-types` |
| `.github/scripts/require_green_workflows.py` | Existing exact-SHA workflow poller | Reuse from the local release helper before tag creation, preserve the post-tag workflow check as defense in depth, and keep JSON integer narrowing type-clean under the locked mypy version |
| `.github/scripts/release_publication.py` | Shared GitHub Release state machine | Discover/delete matching drafts, verify expected tag/SHA/assets, publish drafts, and handle exact already-published reruns plus PyPI lag |
| `tests/test_release_workflow.py` | Workflow structure regressions | Add lock, build, draft, coverage, Codecov secret-auth, and pin-policy assertions |
| `tests/test_release_script.py` | Release helper regressions | Add immutable-tag and repository-setting tests |
| `tests/test_release_publication_script.py` | Shared publication-state regressions | Cover draft replacement, mismatched state, publication, and exact rerun behavior without duplicating API logic in YAML |
| `tests/test_bump_uv.py` | uv maintenance regressions | Cover atomic updates, check/dry-run behavior, unknown workflow shapes, and command failures |
| `tests/test_dev_scripts.py` | Developer helper regressions | Assert PG helper uses the root lock without ephemeral editable overlays |
| `tests/test_redis_dev_script.py` or existing Redis helper tests | Redis helper regressions | Assert Redis helper uses the root lock without ephemeral editable overlays |
| `README.md` | Current release instructions | Document write-once tags, immutable releases, frozen build contract, and recovery behavior |
| GitHub Actions permissions | Repository setting | Selected actions plus full-SHA enforcement |
| `pypi` environment | Repository setting | Allow only the three release-tag families |
| release-tag ruleset | Repository setting | Block tag updates/deletions without blocking creation |
| immutable releases | Repository setting | Enable only after draft-first jobs are merged |

## External References

- GitHub immutable releases and draft-first guidance:
  <https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases>
- Enable repository release immutability:
  <https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/establish-provenance-and-integrity/prevent-release-changes>
- GitHub tag rulesets:
  <https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/creating-rulesets-for-a-repository>
- Environment branch and tag policies:
  <https://docs.github.com/en/actions/reference/workflows-and-actions/deployments-and-environments>
- Repository Actions permissions and SHA enforcement:
  <https://docs.github.com/en/rest/actions/permissions>
- uv lock checking and frozen sync:
  <https://docs.astral.sh/uv/concepts/projects/sync/>
- Codecov action configuration:
  <https://github.com/codecov/codecov-action>

## Engineering Rules

### Red-green TDD

For each repository task:

1. add or strengthen the structural/unit test first
2. prove it fails for the intended reason
3. make the smallest production or workflow change
4. rerun the focused test
5. rerun the relevant local gate before moving on

GitHub settings cannot be made red locally. For those tasks, the red evidence is
the captured API baseline. The green evidence is an authenticated API readback
after the setting changes.

### No text-only workflow tests

Workflow structure tests may parse YAML/text for invariants, but every command
shape must also be exercised by at least one real local invocation:

- all three lock checks
- root frozen sync
- PG and Redis locked helper invocation
- packaging smoke using the locked, non-isolated build frontend
- `python bin/bump_uv.py --check` and `--dry-run`
- `bin/release.py --dry-run`

Do not claim reproducibility solely because a YAML line contains `--frozen`.

### Fail closed at mutation boundaries

The release helper must verify GitHub release settings before it creates a
release commit or pushes a tag. After pushing the release commit to `main`, it
must require the target's normal workflows to pass on that exact SHA before it
creates the local tag or pushes any tag. `--skip-checks` may skip expensive
local tests; it must not skip repository-setting verification, exact-SHA CI,
publication-state checks, tag-state checks, branch checks, or version checks.

### Preserve local developer ergonomics

`bin/pytest-pg` and `bin/pytest-redis` should continue to provision their
containers automatically. Their nested Python test command should use the root
project lock, but callers should not need to pre-create a virtual environment.

Use `uv run --locked` for these local bootstrap helpers. In already-synced CI
steps, use `uv run --frozen --no-sync` where no nested helper needs to change the
environment.

Run local pytest release gates with one worker more than the available logical
CPU count. `bin/release.py` calculates `(os.cpu_count() or 1) + 1`; ad hoc plan
gates use the same calculation. This preserves the prior small worker
oversubscription. Do not serialize local gates to avoid exposing contention;
hosted CI remains the stricter timing environment.

## Dependency and Execution Order

```text
Task 0: baseline
  |
  v
Task 1: current locks + fixed uv + frozen CI
  |
  v
Task 2: locked build frontend + cache-free release builds
  |
  v
PR A: merge Tasks 1-2 and prove reproducibility CI
  |
  v
Task 3: pre-tag CI gate + write-once tag helper + shared draft-first flow
  |
  v
PR B: merge Task 3 and prove publication-flow structure
  |
  v
Task 4: quick wins
  |
  v
PR C: merge Task 4 and prove independent CI changes
  |
  v
Task 5: verify all three merged slices together on main
  |
  v
Task 6: apply GitHub settings
  |
  v
Task 7: readback + next-release proof
```

Why this order:

- frozen CI cannot work until locks are current
- non-isolated builds cannot work until the release frontend is in the lock
- a final tag must not exist until the release commit is on `main` and its
  target-specific required workflows are green on that exact SHA
- immutable releases should not be enabled until assets are staged on a draft
- tag-update restrictions should not be enabled while the helper still offers
  `--retag`
- GitHub settings must be applied only after the matching code is on `main`
- three smaller PRs isolate resolver/build changes, the publication state
  machine, and unrelated CI quick wins so one failure has a narrow cause

## Task 0: Capture the Baseline

### Goal

Record enough state to distinguish an intentional setting change from drift.

### Steps

1. Record the starting SHA and clean worktree:

   ```bash
   git rev-parse HEAD
   git status --short --branch
   ```

2. Record all lock results:

   ```bash
   uv lock --check
   uv lock --check --directory extensions/simplebroker_pg
   uv lock --check --directory extensions/simplebroker_redis
   ```

   Expected baseline: root passes; PG and Redis fail.

3. Record the unique Actions inventory and verify every reference is currently
   a full SHA.

4. Save authenticated JSON readbacks for:

   ```bash
   gh api -H 'X-GitHub-Api-Version: 2026-03-10' \
     /repos/VanL/simplebroker/immutable-releases
   gh api -H 'X-GitHub-Api-Version: 2026-03-10' \
     /repos/VanL/simplebroker/actions/permissions
   gh api -H 'X-GitHub-Api-Version: 2026-03-10' \
     /repos/VanL/simplebroker/environments/pypi
   gh api /repos/VanL/simplebroker/rulesets
   ```

5. Record the last successful core, PG, Redis, packaging, CodeQL, Scorecard,
   and fuzz run URLs.

### Gate

PR A contains the starting SHA, three lock outcomes, current action inventory,
current settings JSON summary, and current green run URLs. PRs B and C link
back to that baseline instead of copying it; refresh only evidence that changed
between slices.

## Task 1: Make Locks Authoritative in CI

### Primary files

- `pyproject.toml`
- `uv.lock`
- `extensions/simplebroker_pg/uv.lock`
- `extensions/simplebroker_redis/uv.lock`
- `.github/workflows/test.yml`
- `.github/workflows/test-pg-extension.yml`
- `.github/workflows/test-redis-extension.yml`
- other workflows containing `astral-sh/setup-uv`
- `bin/bump_uv.py`
- `simplebroker/_scripts.py`
- `bin/pytest-redis`
- focused tests listed in the file map

### Red tests first

Add tests that require:

1. every uv-installing workflow to define an exact `env: UV_VERSION`, every
   setup-uv step to reference `${{ env.UV_VERSION }}`, all workflows to agree
   on one value, and the initial implementation to set that value to `0.11.28`
2. the main `Test` workflow to run all three `uv lock --check` commands
3. no normal test, lint, coverage, PG, or Redis workflow to contain
   `uv pip install`, `pip install`, or a live `--with-editable` overlay
4. each job to sync the exact extras it needs with `uv sync --frozen`
5. in `test.yml`, `test-pg-extension.yml`, and `test-redis-extension.yml`,
   every post-sync step that invokes a Python-ecosystem tool to go through
   `uv run --frozen --no-sync`; any exception must be enumerated inside the
   test itself (expected: none)
6. PG helper commands to use the root lock plus `--extra pg`
7. Redis helper commands to use the root lock plus `--extra redis`
8. both helpers to reject a stale root lock instead of rewriting it
9. `bin/bump_uv.py --check` to prove the root range and every workflow pin agree
10. `bin/bump_uv.py --dry-run` to report every intended file without changing it,
    and mutation tests to prove unknown/missing/duplicate workflow pins fail
    before any file is written

### Implementation

1. Add the stdlib-only `bin/bump_uv.py` command described in Locked Decisions.
   Its write path must stage all text changes in memory, validate the complete
   file set, save original bytes, apply the edits, then regenerate all three
   locks. On a caught write or lock-generation failure, restore every original
   file and exit nonzero. Successful changes remain in the worktree for normal
   diff review.
2. Use it for the initial version/range update:

   ```bash
   python bin/bump_uv.py \
     --ci-version 0.11.28 \
     --required-version '>=0.11.11,<0.12'
   ```

   The command updates `[tool.uv].required-version`, sets the same
   `UV_VERSION` in every uv-installing workflow, and runs the three `uv lock`
   commands. Review all generated lock changes.
3. Add a small `locks` job to `Test` that runs
   `python bin/bump_uv.py --check`; the command performs all three lock checks
   as well as the range/pin consistency check.
4. Replace the main test/lint/coverage system installs with explicit syncs:

   - core matrix: `uv sync --frozen --extra dev`
   - root lint/type check: `uv sync --frozen --extra dev --extra pg --extra redis`
   - coverage: `uv sync --frozen --extra dev --extra pg --extra redis`
   - PG workflow: `uv sync --frozen --extra dev --extra pg`
   - Redis workflow: `uv sync --frozen --extra dev --extra redis`

5. Run post-sync tools with `uv run --frozen --no-sync`.
6. Change CLI installation checks to:

   ```bash
   uv run --frozen --no-sync broker --version
   uv run --frozen --no-sync simplebroker --version
   ```

7. Replace PG/Redis helper `--with-editable` overlays with locked root-project
   extras. Preserve automatic container management and the existing test
   selection behavior.
8. Set the shared `UV_VERSION` env and setup-uv reference on CodeQL-adjacent,
   fuzz, test, and release workflows that install uv.
9. Document the stdlib command in README. Do not tell maintainers to perform
   seven workflow edits and three lock regenerations by hand.

### Gates

```bash
python bin/bump_uv.py --check
PYTEST_WORKERS="$(python -c 'import os; print((os.cpu_count() or 1) + 1)')"
uv run pytest -q -n "$PYTEST_WORKERS" \
  tests/test_bump_uv.py \
  tests/test_release_workflow.py \
  tests/test_dev_scripts.py \
  tests/test_release_script.py
uv run ./bin/pytest-pg --fast
uv run ./bin/pytest-redis --fast
```

All commands pass, and rerunning them leaves every lockfile byte-for-byte
unchanged.

## Task 2: Lock the Build Frontend and Remove Release Caches

### Primary files

- all three `pyproject.toml` files
- `uv.lock`
- `simplebroker/_scripts.py`
- all three release workflows
- packaging/release tests

### Red tests first

Add tests that fail until:

- the root release group contains exact `build` and `hatchling` versions
- every build-system requirement declares `hatchling>=1.31,<2` and none pins
  an exact backend version
- release workflows contain no bare `uv build`
- release workflows build with `python -m build --no-isolation`
- release setup-uv steps explicitly disable cache
- packaging smoke uses the root locked release group for all three projects

### Implementation

1. Add the locked release group from the Locked Decisions section.
2. Bound Hatchling to `>=1.31,<2` in the root, PG, and Redis build-system
   tables.
3. Refresh `uv.lock` and prove Hatchling is present with hashes.
4. Change packaging smoke's build helper to invoke the root project explicitly:

   ```text
   uv run --project <repo-root> --locked --group release \
     python -m build --no-isolation <project-dir>
   ```

   Use argument lists, not a shell string. Preserve Windows support.

5. In each release workflow:

   - explicitly disable setup-uv caching
   - sync the root `release` group from `uv.lock`
   - build the selected package directory with the locked frontend and
     `--no-isolation`
   - keep checkout pinned to `${{ github.sha }}`

6. Do not remove the existing packaging metadata, artifact-install,
   entry-point, license, or excluded-file checks.

### Gates

```bash
uv lock --check
uv run ./bin/packaging-smoke --python 3.11
PYTEST_WORKERS="$(python -c 'import os; print((os.cpu_count() or 1) + 1)')"
uv run pytest -q -n "$PYTEST_WORKERS" \
  tests/test_release_workflow.py \
  tests/test_release_script.py \
  tests/test_dev_scripts.py
```

Capture the exact `build` and `hatchling` versions printed by packaging smoke.
No release workflow restores or saves an Actions cache.

### PR A gate

Open PR A with Tasks 1 and 2 only. Require Test, Test Postgres Extension, Test
Redis Extension, and CodeQL on the PR head; manually run both fuzz jobs because
resolver and build behavior changed. Merge PR A and require the same normal
workflows to pass on `main` before starting Task 3. A failure here is a
resolver/build problem, not mixed publication-state or Codecov work.

## Task 3: Validate Before Tagging and Publish Draft-First

### Primary files

- `bin/release.py`
- `tests/test_release_script.py`
- `.github/scripts/require_green_workflows.py`
- `.github/scripts/release_publication.py`
- `tests/test_release_publication_script.py`
- `.github/workflows/release-gate.yml`
- `.github/workflows/release-gate-pg.yml`
- `.github/workflows/release-gate-redis.yml`
- `tests/test_release_workflow.py`
- `README.md`

### Red tests first

#### Release helper

Add tests proving:

- `--retag` is no longer accepted
- a wrong remote tag always raises an error that says to choose a new version
- no command path emits `git push --delete origin <tag>`
- local-only wrong tags may still be replaced before push
- a real release must run from `main`; after the release commit is pushed, the
  stored release SHA must remain reachable from a freshly fetched
  `origin/main`
- the helper invokes the existing exact-SHA workflow poller with the correct
  target-specific workflows before it creates a local tag or pushes a remote
  tag: core requires Test + PG + Redis, PG requires Test + PG, Redis requires
  Test + Redis, and a batch uses the union
- command-order tests prove `git push origin main` happens before the CI wait
  and all tag creation/push commands happen only after the wait succeeds
- failed, cancelled, missing, or timed-out pre-tag CI leaves no new local or
  remote tag and allows the same unpublished version to be retried after a fix
- an interrupted helper rerun at the same release commit resumes the pre-tag
  check without creating another release commit or requiring a version bump
- a normal fast-forward on `main` during the CI wait does not invalidate an
  already-green stored release SHA; the final tag still names that explicit
  SHA, while removal of the SHA from `main` fails closed
- repository-settings verification cannot be bypassed with `--skip-checks`
- branch, exact-SHA CI, publication-state, tag-state, and version checks cannot
  be bypassed with `--skip-checks`
- missing immutable-release, tag-ruleset, environment-policy, or SHA-pinning
  state fails with one targeted message per missing control

#### Shared publication-state script

Add unit tests proving `.github/scripts/release_publication.py`:

- lists releases to find drafts by `tag_name` instead of assuming the normal
  tag lookup endpoint can discover drafts
- deletes only a matching draft and never deletes a published release
- resolves the remote tag ref independently of the GitHub Release object's
  `target_commitish` field and rejects any SHA other than the expected release
  SHA
- verifies the complete expected wheel, sdist, and Sigstore asset set before
  final publication
- publishes the matching draft and fails closed on missing or duplicate state
- treats an already-published immutable release as rerun success only when the
  tag, SHA, package version, and PyPI state all match exactly
- keeps PyPI lag polling bounded to about five attempts over a few minutes and
  reports the last observed state on failure

#### Workflow structure

For each release workflow, require this order:

```text
require-tests
  -> verify-tag-current
  -> build + attest
  -> stage-github-release (draft + all assets)
  -> publish-to-pypi (Trusted Publishing)
  -> publish-github-release (draft -> published)
```

Tests must also prove:

- every release workflow uses the shared publication-state script for draft
  discovery/deletion and final publication; no workflow carries a copied
  `gh api`, release-list parser, PyPI polling loop, or `gh release edit` state
  machine
- all wheel, sdist, and Sigstore files are attached before final publication
- no file upload occurs after the release leaves draft state
- the draft-staging job invokes the shared script to delete any pre-existing
  draft for the tag before staging, so reruns replace drafts instead of
  duplicating them
- the PyPI job still uses only `id-token: write`
- no PyPI secret appears anywhere
- a failed PyPI job leaves a draft release instead of a partially populated
  published release
- final publication depends on successful PyPI publication

### Implementation

1. Remove remote retagging from the release helper and its type definitions.
2. Add authenticated GitHub settings readback to the release helper. Use the
   `2026-03-10` API version and never print the token.
3. Add a standalone read-only command for maintainers:

   ```bash
   uv run python bin/release.py --check-repository-settings
   ```

4. Run the same settings check automatically before any non-dry-run release
   mutates version files, creates a release commit, pushes `main`, or pushes a
   tag. During the short interval after PR B merges and before Task 6 applies
   the settings, this intentionally blocks real releases.
5. Move the final-tag commit point behind exact-SHA CI:

   1. require the current branch to be `main`
   2. run the existing local prechecks unless `--skip-checks` was passed
   3. update version files and create the release commit as today
   4. store the release SHA and push it explicitly with `git push origin main`
   5. invoke `.github/scripts/require_green_workflows.py` with the authenticated
      GitHub token, repository slug, stored SHA, and the target-specific normal
      workflow names
   6. after the poller succeeds, fetch `origin/main`, require
      `git merge-base --is-ancestor <release-sha> origin/main`, and re-read
      remote tag state; a normal later fast-forward does not force a retry
   7. only then create the local final tag with the stored SHA as an explicit
      target and push that tag

   Reuse the existing poller rather than building a second GitHub Actions
   polling implementation. Resolve the token through the release helper's
   existing environment/`gh auth` path and pass it to the subprocess only as
   `GITHUB_TOKEN`, never as an argument or printed value. Preserve the release
   workflow's post-tag
   `require-tests` job as defense in depth against manual tag creation and
   state changes between the local wait and hosted build.
6. Add `.github/scripts/release_publication.py` as the single implementation of
   GitHub Release discovery, matching-draft deletion, expected-state
   verification, bounded PyPI polling, and final draft publication. Keep it
   stdlib-only so workflows do not need a mutable install step. Give it two
   explicit workflow-facing subcommands:

   - `replace-draft`: accepts repository, tag, and expected-SHA arguments;
     resolves the remote tag ref, rejects mismatched or published state, and
     deletes only a matching draft
   - `publish-draft`: accepts repository, tag, expected SHA, package, version,
     and repeated expected-asset arguments; verifies the exact draft and asset
     set, publishes it, and implements the bounded already-published/PyPI
     rerun check

   Read authentication from `GITHUB_TOKEN`; never accept a token argument or
   print request headers.
7. Split the current GitHub Release job into:

   - a draft-staging job that downloads both artifact sets and invokes the
     shared script to remove a stale matching draft, then invokes the existing
     pinned `softprops/action-gh-release` with `draft: true`
   - a final job after PyPI publication that invokes the shared script to
     verify the release, asset set, tag, and SHA before publishing the draft

8. Make reruns idempotent by replacement, not in-place update:

   - before invoking the release action, the shared script deletes any
     existing draft release whose `tag_name` matches, then creates a fresh
     draft with all assets. Do not rely on the release action finding and
     updating an existing draft; drafts are not reliably discoverable by tag
     lookup. Draft deletion is unaffected by immutable releases (which bind at
     publish time) and by the tag ruleset (which governs git refs only)
   - artifact names remain stable
   - on the rerun path only, the shared script treats an already-published
     immutable release at the exact expected tag/SHA as success once PyPI
     reports that version; it polls the PyPI JSON API with backoff (about five
     attempts over a few minutes) before declaring failure, because fresh
     uploads can lag. A first run needs no PyPI query; success is implied by
     the job dependency on the publish job
   - any mismatched tag/SHA is a hard failure

9. Update release documentation:

   - remote tags are permanent
   - a failed pre-tag CI run leaves the version reusable; fix `main` and rerun
     the helper
   - retry a transient publication failure when the tag and SHA are correct
   - if a post-tag recovery needs a new commit, bump to the next patch version
   - do not delete or reuse a published version
   - the uv version-bump runbook from the Locked Decisions section

### Gates

```bash
PYTEST_WORKERS="$(python -c 'import os; print((os.cpu_count() or 1) + 1)')"
uv run pytest -q -n "$PYTEST_WORKERS" \
  tests/test_release_script.py \
  tests/test_release_publication_script.py \
  tests/test_release_workflow.py
uv run python bin/release.py --dry-run
```

The dry run proposes no remote tag deletion or external mutation and shows this
order without waiting for live CI: repository settings -> local checks ->
release commit -> push `main` -> wait for exact-SHA CI -> create/push final tag
-> hosted draft -> PyPI -> publish immutable release. Existing read-only remote
publication-state lookups may still run.

### PR B gate

Open PR B with Task 3 only. Require normal PR CI and the focused tests above.
Merge it after PR A is green on `main`. Before Task 6 applies the repository
settings, verify the merged helper's dry-run order and the three workflow job
graphs; do not create a real release tag merely to test this slice.

## Task 4: Apply the Quick Wins

### 4.1 Codecov visibility and local coverage floor

Files:

- `.github/workflows/test.yml`
- `pyproject.toml`
- `tests/test_release_workflow.py`

Tests first:

- Codecov keeps `token: ${{ secrets.CODECOV_TOKEN }}`
- `use_oidc` and job-scoped `id-token: write` are absent
- the Codecov step has an `id`, uses `fail_ci_if_error: true`, and is marked
  `continue-on-error: true`
- a following step runs only when the Codecov step's `outcome` is `failure`
  and emits an explicit GitHub workflow warning
- coverage configuration contains `fail_under = 85`
- each of the three partial pytest-cov collection commands uses
  `--cov-fail-under=0`; only the final combined `coverage report` applies the
  85% floor

Gate:

```bash
PYTEST_WORKERS="$(python -c 'import os; print((os.cpu_count() or 1) + 1)')"
uv run pytest -q -n "$PYTEST_WORKERS" tests/test_release_workflow.py \
  -k 'codecov or coverage'
```

This focused gate proves the workflow and configuration shape without assuming
that a `.coverage` data file already exists. Task 5 must run the real coverage
job, including `coverage report --show-missing`, to prove the current combined
coverage remains at least 85% (measured 92.4% on 2026-07-12). The interim zero
overrides prevent incomplete core/extension datasets from being judged before
they are combined; they do not weaken the final report. Codecov keeps
the existing repository-secret authentication. An external Codecov outage does
not make otherwise-green code unmergeable.

### 4.2 Python examples in normal CI

Add a `--check-example-types` flag to `bin/release.py` that runs the existing
`_examples_mypy_command()` file-list logic. The root mypy config excludes
`examples/` from directory discovery, and that helper is already the single
source of truth for example file discovery; do not duplicate the glob in a
workflow. Cover the flag in `tests/test_release_script.py`.

Then add these to the existing lint/quality job after its frozen sync:

```bash
uv run --frozen --no-sync pytest -n auto examples
uv run --frozen --no-sync python bin/release.py --check-example-types
```

Do not add a live package installation. Keep ShellCheck in the release helper
unless CI can consume a versioned ShellCheck binary without adding another
mutable installer.

### 4.3 Repository Action allowlist

This is a GitHub setting applied in Task 6, but add local tests now that produce
the expected inventory.

Desired repository policy:

- `allowed_actions: selected`
- `sha_pinning_required: true`
- GitHub-owned actions allowed
- all verified publishers **not** allowed as a blanket category
- explicit third-party repository patterns:
  - `astral-sh/setup-uv@*`
  - `codecov/codecov-action@*`
  - `dependabot/fetch-metadata@*`
  - `ossf/scorecard-action@*`
  - `pypa/gh-action-pypi-publish@*`
  - `softprops/action-gh-release@*`

The allowlist identifies approved action repositories; the separate SHA policy
ensures every actual reference remains immutable.

### 4.4 Keep the exact-SHA poller type-clean

The documented final mypy command includes
`.github/scripts/require_green_workflows.py`. Narrow GitHub JSON integer values
to the scalar types accepted by `int()` before conversion instead of carrying a
stale `type: ignore`. Preserve the existing default behavior for missing or
invalid fields and cover string IDs plus invalid run attempts through
`WorkflowRun.from_api`. This is a type-gate cleanup only; it does not change
valid GitHub API handling or workflow selection.

### PR C gate

Open PR C with Task 4 only. Require Test and CodeQL on the PR head. Confirm the
local 85% coverage command is a hard gate, the repository-secret Codecov upload
attempt is visible, and the example pytest/mypy commands run from the frozen
environment. If the Codecov service itself is unavailable, the PR may merge
after the explicit warning path is observed and local coverage passes; keep the
Codecov evidence row pending until a later authenticated upload succeeds.

## Task 5: Verify the Three Merged Slices Together

### Steps

1. Confirm PR A contains only Tasks 1-2, PR B only Task 3, and PR C only Task 4.
   Do not squash all three concerns into a replacement mega-PR if one slice
   needs revision.
2. Require these workflows to finish on the final merged `main` SHA:

   - Test
   - Test Postgres Extension
   - Test Redis Extension
   - CodeQL

3. Manually run the fuzz workflow on the PR A head and again on the final
   merged `main` SHA because lock and uv behavior changed.
4. Confirm both 15-minute fuzz harnesses pass.
5. Confirm packaging smoke prints exact locked build-tool versions.
6. Confirm at least one PR or `main` Codecov upload succeeds with the existing
   repository secret. If Codecov is externally unavailable, do not block
   merging otherwise-green slices; leave evidence pending and capture the
   warning-bearing run.
7. Confirm all three `uv lock --check` commands pass in CI.
8. Review the final release-hardening diff for runtime behavior changes.
   Changes to `simplebroker/_scripts.py` must be limited to
   developer/test/packaging command construction. Review the separately landed
   SQLite bootstrap and lock-progress fixes as explicit correctness
   prerequisites, not incidental release-script changes.
9. If a required gate is red, stop the settings rollout and fix the problem in
   a focused follow-up assigned to the responsible PR slice. A warning caused
   solely by external Codecov availability may leave the Codecov rollout row
   pending, but does not redefine the repository gates.

### Gate

The final merged `main` contains reproducible builds, the pre-tag exact-SHA
gate, shared immutable-compatible publication workflows, and independent CI
quick wins before any immutable release or tag setting is enabled. Each PR has
its own green evidence, and the combined `main` SHA is also green.

## Task 6: Apply GitHub Settings in Safe Order

These are external state changes. Capture the before JSON and apply one setting
at a time. Read it back before proceeding. Do not start until PRs A, B, and C
are merged, the combined `main` SHA is green, and the merged release-helper dry
run shows `push main -> exact-SHA CI -> create tag`. No real release is allowed
in the transition window because the merged settings preflight intentionally
fails until this task is complete.

### 6.1 Enforce selected, SHA-pinned Actions

1. Set repository Actions permissions to:

   ```json
   {
     "enabled": true,
     "allowed_actions": "selected",
     "sha_pinning_required": true
   }
   ```

2. Set selected actions to GitHub-owned plus the six explicit third-party
   patterns from Task 4.3.
3. Dispatch `Test`, CodeQL, Scorecard, and Fuzz far enough to prove every action
   is admitted. Do not wait for fuzz completion before checking that both jobs
   start successfully; still require final fuzz success before completion.
4. If an action is blocked, add only that exact repository pattern. Do not
   enable all verified actions as a shortcut.

### 6.2 Restrict the `pypi` environment to release tags

First update the existing `pypi` environment to enable custom deployment
policies:

```json
{
  "deployment_branch_policy": {
    "protected_branches": false,
    "custom_branch_policies": true
  }
}
```

Then create these custom deployment tag policies, with no branch policies:

- type `tag`, name `v*`
- type `tag`, name `simplebroker_pg/v*`
- type `tag`, name `simplebroker_redis/v*`

Do not add required reviewers. The environment restriction is an automated ref
check, not a team approval process.

Read back:

```bash
gh api -H 'X-GitHub-Api-Version: 2026-03-10' \
  /repos/VanL/simplebroker/environments/pypi
gh api -H 'X-GitHub-Api-Version: 2026-03-10' \
  /repos/VanL/simplebroker/environments/pypi/deployment-branch-policies
```

### 6.3 Add a write-once release-tag ruleset

Create one active tag ruleset named `Protect release tags` covering:

- `refs/tags/v*`
- `refs/tags/simplebroker_pg/v*`
- `refs/tags/simplebroker_redis/v*`

Rules:

- restrict updates
- restrict deletions
- do **not** restrict creation
- no bypass actors

Do not test this with a destructive force-push. Verify through the ruleset API
readback and the release helper's unit tests. The no-bypass policy is acceptable
only because Task 3 moved candidate validation before final tag creation.

### 6.4 Enable immutable releases last

Use repository Settings -> General -> Releases -> Enable release immutability,
or the corresponding repository API.

Read back:

```bash
gh api -H 'X-GitHub-Api-Version: 2026-03-10' \
  /repos/VanL/simplebroker/immutable-releases
```

Expected: `enabled: true`.

Then run:

```bash
uv run python bin/release.py --check-repository-settings
```

Expected: one success line for immutable releases, tag rules, environment tag
policies, and Actions SHA enforcement.

## Task 7: Final Verification and First-Release Evidence

### Local gates

```bash
python bin/bump_uv.py --check
PYTEST_WORKERS="$(python -c 'import os; print((os.cpu_count() or 1) + 1)')"
uv run pytest -q -n "$PYTEST_WORKERS" \
  tests/test_bump_uv.py \
  tests/test_release_publication_script.py \
  tests/test_release_workflow.py \
  tests/test_release_script.py \
  tests/test_dev_scripts.py
uv run pytest -n "$PYTEST_WORKERS"
uv run ruff check simplebroker tests bin .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis \
  extensions/simplebroker_redis/tests
uv run ruff format --check simplebroker tests bin .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis \
  extensions/simplebroker_redis/tests
uv run mypy simplebroker bin/release.py bin/bump_uv.py \
  .github/scripts/require_green_workflows.py \
  .github/scripts/release_publication.py \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis \
  --config-file pyproject.toml
uv run ./bin/packaging-smoke --python 3.11
uv run python bin/release.py --dry-run
uv run python bin/release.py --check-repository-settings
git diff --check
git status --short
```

### GitHub gates

- Test passes on `main`
- Postgres extension passes on `main`
- Redis extension passes on `main`
- CodeQL passes with zero new alerts
- Scorecard uploads successfully
- both fuzz harnesses pass
- at least one Codecov upload succeeds with repository-secret authentication;
  any later upload failure is an explicit warning while the local 85% gate
  remains hard
- Actions settings show selected actions plus required SHA pinning
- `pypi` environment shows only the three tag policies
- tag ruleset shows update/deletion restrictions and no bypass
- immutable releases endpoint reports enabled

### First real release proof

Do not create a throwaway PyPI version solely to test this plan. The next normal
package release is the live publication proof.

For that release, record:

1. release commit SHA pushed to `main`
2. required Test, PG, and/or Redis workflow URLs for that exact SHA, with
   completion times before the tag-push event
3. helper output showing the pre-tag gate succeeded
4. tag name and confirmation that it points to the same immutable commit SHA
5. release workflow URL and its defense-in-depth required-test readback
6. build log showing exact uv, build, and Hatchling versions
7. draft release created with wheel, sdist, and Sigstore bundle before PyPI
8. successful Trusted Publishing job with no token
9. final GitHub Release marked immutable
10. GitHub release attestation plus repository-generated Sigstore bundle
11. PyPI version and files matching the workflow artifacts

If PyPI succeeds but final GitHub publication fails, rerun the final job against
the existing draft. Do not move the tag or rebuild different artifacts.

## Rollback Strategy

### Before a real release

- Repository code: revert the affected PR slice or slices; do not revert all
  three if the failure is isolated to one.
- Actions allowlist: restore the captured `allowed_actions: all` policy only if
  the selected policy blocks required workflows and the exact missing action
  cannot be identified promptly.
- Environment policy: restore `deployment_branch_policy: null` only if the tag
  patterns are proven incorrect; fix patterns and re-enable immediately.
- Tag ruleset: disable only before a new protected release tag is used and only
  to repair configuration, not to move a release tag.
- Immutable releases: disable for future releases only if the draft-first flow
  is incompatible in practice. Do not assume already-immutable releases become
  mutable.

### After tag push, before PyPI publication

The tag is already a public, immutable identity even though PyPI has no version
yet.

- retry transient hosted-workflow, draft, attestation, or Trusted Publishing
  configuration failures against the same tag and SHA
- delete and recreate only a draft release through the shared script; never
  change the tag or rebuild from a different commit
- if recovery requires a source change, leave the old tag untouched and use a
  new patch version for the corrected commit

### After PyPI publication

There is no rollback that reuses the version or tag.

- never overwrite the tag
- never replace release assets
- never republish different bits under the same version
- fix forward with a new patch release
- use PyPI yank only when users should avoid the bad release; yanking is not a
  substitute for publishing corrected artifacts

## Failure-Mode Matrix

| Failure | Expected behavior | Recovery |
|---|---|---|
| Root or extension lock is stale | CI fails before tests | Regenerate the intended lock, review diff, rerun |
| Locked dependency unavailable | Sync/build fails; no fallback resolution | Investigate index/outage; do not remove `--frozen` |
| Release cache absent | Build downloads locked artifacts and succeeds | No action; this is expected |
| Repository-settings preflight fails | Release stops before version files or git state change | Correct the setting and rerun the same version |
| Pre-tag required CI fails, is cancelled, is missing, or times out | Release commit remains on `main`; no final tag is created or pushed | Fix `main` and rerun the helper with the same unpublished version |
| `main` fast-forwards during the pre-tag wait but still contains the stored release SHA | Helper tags the stored, already-green release SHA | No action; unrelated later commits do not invalidate the tested release commit |
| The stored release SHA is no longer reachable from `origin/main` after the pre-tag wait | Helper fails the ancestry recheck and creates no tag | Put the intended release commit back on `main` through a normal forward change, let exact-SHA CI pass, and rerun |
| Draft creation fails | PyPI job does not run | Fix GitHub release job and rerun |
| Artifact upload to draft fails | PyPI job does not run | Rerun draft-staging job |
| PyPI Trusted Publishing fails | Draft remains unpublished | Fix publisher/environment configuration and rerun same workflow/tag |
| Final release publication fails after PyPI succeeds | PyPI version exists; draft remains | Rerun final job; do not rebuild or retag |
| Remote tag exists at the wrong SHA | Release helper and workflow fail closed | Choose a new version; never retag |
| Post-tag failure requires a code change | Existing tag and version remain bound to their original SHA | Commit the fix and publish a new patch version |
| Wrong tag pattern targets `pypi` | Deployment is rejected before PyPI Trusted Publishing | Correct environment tag pattern; rerun |
| Action allowlist blocks a workflow | Workflow fails before the action starts | Add the exact action repository pattern, retain SHA enforcement |
| Codecov authentication or service upload fails | Upload step records failure and emits a warning; local 85% coverage gate still controls the job | Check the existing repository secret and Codecov configuration; rerun opportunistically for an outage; do not change authentication or block unrelated green work |
| GitHub immutable release is published with wrong assets | Assets cannot be replaced | Publish a corrected patch release; do not weaken immutability |

## Common Wrong Turns

- Do not add a PyPI token because Trusted Publishing already works.
- Do not enable immutable releases before the draft-first workflow lands.
- Do not protect tag creation; the release helper must still create new tags.
- Do not leave `--retag` as an undocumented escape hatch.
- Do not push the final release tag to discover whether cross-platform CI is
  green; the exact-SHA gate belongs before tag creation.
- Do not use `--frozen` without a separate lock-freshness gate and then claim a
  stale lock is valid.
- Do not update the uv range, seven workflow pins, and three locks by hand; use
  `bin/bump_uv.py`, review its diff, and run its check mode.
- Do not keep extension locks if CI never checks them.
- Do not replace live `uv pip install` with a different live resolver command.
- Do not use `uv run --with build` in packaging or release paths.
- Do not enable all verified Marketplace actions to avoid maintaining six
  explicit patterns.
- Do not add human release approval to solve a machine-verifiable ref problem.
- Do not pin an exact build backend in published `[build-system].requires`;
  exactness belongs in the release group and lock.
- Do not rely on the release action to find and update an existing draft by
  tag; delete any stale draft and recreate it.
- Do not copy release discovery, draft deletion, PyPI polling, or final
  publication API logic into three YAML files; call the shared checked-in
  script while preserving the three top-level workflow identities.
- Do not touch Windows timing factors or contention tests in this work.
- Do not turn Codecov reporting availability into the hard coverage gate; the
  local 85% report is the gate and Codecov failures must be visible warnings.
- Do not replace Codecov's repository-secret authentication with OIDC; it does
  not improve coverage correctness or a release security boundary in this plan.
- Do not move publication into a reusable workflow without first changing the
  PyPI trusted-publisher identities; that is explicitly out of scope.

## Acceptance Criteria

1. `uv lock --check` passes for root, PG, and Redis without modifying files.
2. `python bin/bump_uv.py --check` proves the supported local uv range and every
   workflow `UV_VERSION` agree; dry-run and failure tests prove the updater is
   atomic and fail-closed.
3. Every setup-uv step references its workflow's shared `UV_VERSION`.
4. Normal CI contains no `uv pip install`, `pip install`, or ephemeral editable
   dependency overlay.
5. Test, lint, coverage, PG, and Redis jobs consume the checked-in root lock.
6. Packaging smoke and all release builds use exact locked `build` and
   `hatchling` versions with build isolation disabled.
7. Release workflows explicitly disable setup-uv caching.
8. Root and all extension artifacts still build, install, expose licenses, and
   register backend entry points.
9. Trusted Publishing remains tokenless and bound to the existing three
   workflow files plus the `pypi` environment.
10. A real release runs only from `main`, pushes the release commit first, and
    requires the target-specific normal workflows to pass on that exact SHA
    before any final tag is created or pushed; immediately before tagging, the
    stored SHA must still be reachable from freshly fetched `origin/main`.
11. Failed or missing pre-tag CI creates no tag and the same unpublished
    version can be retried after fixing `main`.
12. Remote release tags cannot be moved or deleted and the helper has no
    `--retag` path.
13. A transient post-tag publication failure can rerun only at the same tag and
    SHA; a recovery requiring new code uses a new patch version.
14. All three release workflows call one shared publication-state script; no
    workflow duplicates draft discovery/deletion, PyPI lag polling, or final
    publication API logic.
15. Every release stages all assets on a draft before PyPI publication.
16. A GitHub Release becomes published only after PyPI succeeds.
17. The `pypi` environment accepts only the three documented tag families.
18. GitHub enforces full-SHA Actions references and admits only GitHub-owned
    actions plus the six explicit third-party action repositories.
19. Future GitHub Releases are immutable.
20. Codecov keeps repository-secret authentication and has at least one
    successful upload; later upload errors produce an explicit warning without
    overriding the local coverage result.
21. Local combined coverage below 85% fails the coverage job independently of
    Codecov.
22. Python examples run under normal CI.
23. PRs A, B, and C each have focused green evidence, and full core, PG, Redis,
    packaging, CodeQL, Scorecard, and fuzz gates pass on the combined `main` SHA.
24. The release-hardening slices change no public queue, watcher, or backend
    API. The separate SQLite runtime prerequisites are limited to atomic
    first-time bootstrap and progress-aware lock retries, with the Windows
    concurrency detectors left intact.

## Completion Evidence

Fill this table during implementation. A GitHub-setting row is not complete
from YAML or local tests alone.

| Slice | Status | Evidence |
|---|---|---|
| Baseline captured | Complete | `7311c278`; stale extension locks, live CI resolution, movable tags, open Actions policy, and mutable releases recorded above |
| SQLite rollout prerequisites | Complete | Atomic bootstrap in PR #54; elapsed retry baseline in PR #55; forward-progress refresh in `bac886c3`; unchanged Windows detectors pass in run `29284078540` |
| PR A: reproducible builds | Complete | PR #53, merged as `91ad0eae`; combined Test run `29284078540` and Fuzz run `29284100824` |
| uv bump automation | Complete | `bin/bump_uv.py --check`, updater unit tests, and portable dry-run coverage pass |
| Locks current | Complete | Root, Postgres, and Redis `uv lock --check` gates pass in run `29284078540` |
| Frozen CI | Complete | PR #53 structural tests and merged workflow runs use frozen installs with no sync |
| Locked build frontend | Complete | Packaging smoke passes with the locked uv/build/Hatchling set in run `29284078540` |
| Cache-free release builds | Complete | PR #53 workflow diff and release-workflow structural tests |
| PR B: publication flow | Complete | PR #56, merged as `4c131f8f`; release-helper/shared-script tests and merged 5.3.2 dry-run order pass |
| Pre-tag exact-SHA gate | Complete | Command-order tests and 5.3.2 dry run show `push main -> exact-SHA CI -> create tag` |
| Write-once release helper | Complete | No `--retag` or remote tag deletion path; immutable-tag and ancestry tests pass |
| Shared draft-first release flow | Complete | Shared publication-state tests and all three release job graphs pass structural checks |
| PR C: CI quick wins | Complete | PR #57; Test run `29281924077` passed with 92% combined coverage and all example gates |
| Codecov secret-auth upload | Complete | Secret-authenticated upload for `ed7c031b`; Codecov accepted and queued the 92% report |
| Example CI | Complete | Frozen example pytest and explicit example-mypy steps pass in run `29281924077` |
| Actions policy | Complete | API readback: selected actions, full-SHA required, GitHub-owned plus exactly six third-party patterns |
| PyPI environment policy | Complete | API readback: exactly three tag policies and no branch policy |
| Release tag ruleset | Complete | Active ruleset `18894182`: update/deletion only, three tag families, no bypass |
| Immutable releases | Complete | Repository API returns `enabled: true` |
| Repository-settings preflight | Complete | `bin/release.py --check-repository-settings` reports all four controls OK |
| First immutable release | Pending: 5.3.2 | Record workflow, PyPI, immutable GitHub Release, and attestation URLs after publication |

## Fresh-Eyes Review Checklist

- Does any normal CI path still resolve dependencies outside a checked lock?
- Can a stale extension lock pass every workflow?
- Does any packaging or release build create an isolated environment that
  resolves Hatchling live?
- Can a release build restore an Actions cache?
- Can the release helper create or push a final tag before the target-specific
  workflows are green on the exact release SHA contained in `main`?
- Does a pre-tag CI failure leave any local or remote tag behind, or prevent
  reuse of the still-unpublished version?
- Can any remote release tag be deleted or moved by the helper or GitHub rules?
- Are all assets present before an immutable release is published?
- Can a rerun of draft staging leave duplicate drafts for one tag?
- Is any release discovery, draft deletion, PyPI polling, or final-publication
  state machine duplicated across the three workflow YAML files?
- Can PyPI run before draft asset staging succeeds?
- Can final GitHub publication run before PyPI succeeds?
- Can a branch, pull request ref, or unrelated tag deploy to `pypi`?
- Can a non-SHA action reference start in Actions?
- Can Codecov upload fail without an explicit warning?
- Can Codecov availability override the local 85% coverage result?
- Did any change weaken PyPI Trusted Publishing, add a PyPI token, or replace
  Codecov's existing repository-secret authentication?
- Did any change alter contention, timing, queue, watcher, or backend behavior?
