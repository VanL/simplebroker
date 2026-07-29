# Development Toolchain Refresh Plan

Date: 2026-07-29
Status: completed
Class: 3 — [DOM-5] non-trivial triggers fire because the refresh crosses the
root manifest, both extension manifests, three lock environments, and the
repository-wide uv policy consumed by seven workflows. No [DOM-5] risky
trigger or product-spec change fires.

## Goal

Update every declared development, test, fuzz, build, and release tool to the
latest non-yanked release compatible with each supported Python environment on
2026-07-29. Move the repository-wide uv policy to the current minor line,
regenerate all three lockfiles with that exact uv version, and prove the
upgraded Ruff and mypy versions pass every CI and release-check target.

## Requested Outcomes

- [x] Root and extension development-tool minimums name current releases.
- [x] All three pytest `minversion` settings match the current pytest release.
- [x] Fuzz, build-system, and exactly pinned release tools use current
  non-yanked, environment-compatible releases.
- [x] Every uv-installing workflow pins the current uv release and the local
  policy accepts only its current minor line.
- [x] Root, PostgreSQL, and Redis lockfiles are regenerated and current.
- [x] Ruff check, Ruff format check, and all mypy partitions pass with the
  upgraded environment.
- [x] Core tests, tool-policy tests, example type checks, and packaging smoke
  pass.

## Source Documents

- User instruction in the originating session: update all development tools to
  their most recent versions.
- `docs/plans/2026-07-12-release-reproducibility-and-publication-hardening-plan.md`,
  especially “CI uses a fixed uv version,” the three-lock invariant, and local
  release gates.
- `README.md`, “Lint and format” and release-helper verification paragraphs.
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-10], [DOM-11], [DOM-15].
- Current release sources:
  - PyPI JSON/project metadata for Python packages.
  - `astral-sh/uv` GitHub releases for uv.

Source spec: None — dependency-maintenance/tooling change. Product behavior is
not being revised.

## Baseline

- `b21bbcd87af7ea868ad7ea02d0d8ce4c83dea8a8` — repository state before
  this plan.
- Latest-release inventory queried on 2026-07-29:

| Tool | Current declaration or lock | Target |
|------|-----------------------------|--------|
| uv | CI `0.11.28`; local `>=0.11.11,<0.12` | `0.12.0`; `>=0.12.0,<0.13` |
| build | dev `>=1.2`; release `==1.5.1` (yanked) | dev `>=1.5.0`; release `==1.5.0` |
| hatchling | `>=1.31,<2`; release `==1.31.0` | unchanged; already current |
| hypothesis | `>=6.100`; lock `6.161.7` | `>=6.163.0` |
| pytest | `>=7.0`; lock `9.1.1` | `>=9.1.1` |
| pytest-cov | `>=4.0`; lock `7.1.0` | `>=7.1.0` |
| pytest-xdist | `>=3.0`; lock `3.8.0` | `>=3.8.0` |
| pytest-timeout | `>=2.4.0`; lock `2.4.0` | unchanged; already current |
| mypy | `>=1.0`; lock `2.3.0` | `>=2.3.0` |
| Ruff | `>=0.1.0`; lock `0.16.0` | `>=0.16.0` |
| aiosqlite | `>=0.22.1`; lock `0.22.1` | unchanged; already current |
| aiosqlitepool | `>=1.0.0`; lock `1.0.0` | unchanged; already current |
| Atheris | `>=2.3.0`; lock `3.1.0` | Python 3.11: `>=3.0,<3.1`; Python 3.12+: `>=3.1.0` |

`build==1.5.1` is excluded despite being numerically newer because every PyPI
file for that release is yanked; “latest” means the latest usable non-yanked
release.

Atheris is split by Python version because 3.1.0 publishes only CPython
3.12–3.14 Linux wheels and no source distribution. Version 3.0.0 is the latest
release with a CPython 3.11 wheel and source distribution. The existing
Linux/x86-64 guards remain on both requirements.

## Context and Key Files

- `pyproject.toml` owns root dev/fuzz/release requirements and the local uv
  compatibility range.
- `extensions/simplebroker_pg/pyproject.toml` and
  `extensions/simplebroker_redis/pyproject.toml` own extension-local test
  minimums.
- `uv.lock` and both extension `uv.lock` files are supported execution locks.
- `bin/bump_uv.py` is the canonical atomic updater for the uv range, all seven
  workflow pins, and lock consistency.
- `README.md` publishes the supported uv minor line and exact maintenance
  command; it must move with the policy.
- `.github/workflows/{fuzz,release-gate,release-gate-pg,release-gate-redis,test,test-pg-extension,test-redis-extension}.yml`
  consume one shared exact uv version.
- `tests/test_bump_uv.py` and release-tool tests enforce the maintenance path.
- `tests/test_release_workflow.py` enumerates the fuzz requirement, uv workflow
  pin, and exact release-tool group.

## Invariants and Constraints

1. Do not change SimpleBroker, extension, Python-support, backend-protocol, or
   storage versions.
2. Preserve Python 3.11 support. Select the latest compatible Atheris on 3.11
   rather than making the fuzz group unresolvable there.
3. Do not raise public runtime dependency minimums for psycopg, psycopg-pool,
   or redis merely because root development uses them. Their resolved lock
   versions may refresh.
4. Keep uv exact in CI and minor-bounded locally; every setup-uv workflow must
   remain managed by `bin/bump_uv.py`.
5. Keep release tools exactly pinned and select non-yanked releases.
6. Preserve `--frozen --no-sync` CI and release execution.
7. Regenerate all three locks with uv 0.12.0. Record `uv --version` before
   mutation; do not infer generator provenance from lock checks. Do not
   hand-edit lockfiles.
8. Keep the fuzz workflow on its intentional Linux/Python-3.12 execution
   environment while retaining a resolvable Python-3.11 fuzz group.
9. No product code, public product-documentation contract, CHANGELOG release
   entry, or package version changes belong in this unit. The README
   development command is in scope.

## Dependency-Ordered Tasks

### T1 — Review the plan and inventory

- Independently review scope, latest-version evidence, invariants, and gates.
- Stop if a named target is pre-release, yanked, Python-3.11-incompatible, or
  absent from a supported platform without an explicit compatible marker.

### T2 — Update direct declarations

- Raise stale dev and fuzz minimums in the root manifest.
- Raise pytest minimums in both extension dev extras.
- Raise all three pytest `minversion` settings to 9.1.1.
- Split Atheris markers so Python 3.11 resolves 3.0.x and Python 3.12+ resolves
  3.1.0 or newer on Linux/x86-64.
- Replace the yanked release `build` pin with the latest non-yanked release.
- Leave already-current declarations unchanged.
- Update the exact dependency inventory in `tests/test_release_workflow.py`.

### T3 — Move the uv policy

- Assert `uv --version` is exactly `uv 0.12.0`.
- Run `python3 bin/bump_uv.py --ci-version 0.12.0
  --required-version '>=0.12.0,<0.13'`.
- Refresh `bin/bump_uv.py`'s range-shape diagnostic example.
- Update the maintenance command and supported uv line in `README.md`.
- Update uv pin assertions in `tests/test_release_workflow.py`.
- Inspect all workflow and lock diffs. Stop if any workflow loses its shared
  pin.

### T4 — Upgrade and synchronize locks

- Run `uv lock --upgrade` at the root and in both extension directories.
- Run all three `uv lock --check` gates.
- Inspect package deltas; explain any non-tool transitive movement.

### T5 — Verify the upgraded toolchain

- Sync the root dev/PG/Redis environment with `--frozen`.
- Run exact CI Ruff check and format-check paths, plus `examples/`.
- Run the core/release, PostgreSQL-test, Redis-test, and example mypy
  partitions.
- Run `tests/test_bump_uv.py` and `tests/test_release_workflow.py`.
- Run both extension suites through `./bin/pytest-pg --fast` and
  `./bin/pytest-redis --fast`.
- In a Linux/x86-64 Python 3.12 environment, sync the frozen fuzz group and
  run bounded smoke executions of both Atheris harnesses. A local container is
  acceptable; otherwise a manually dispatched Fuzz workflow must supply the
  evidence.
- Run the core pytest suite and packaging smoke.
- Run DOM-15, doc-path, and diff checks.

### T6 — Review and close

- Obtain an independent completed-work review over manifests, workflow pins,
  lock deltas, and verification evidence.
- Resolve or disposition every finding.
- Mark this plan and its Status Index row `completed` only after all gates pass.

## Testing and Verification

```bash
test "$(uv --version | awk '{print $1, $2}')" = "uv 0.12.0"
python3 bin/bump_uv.py --check
uv lock --check
uv lock --check --directory extensions/simplebroker_pg
uv lock --check --directory extensions/simplebroker_redis
uv sync --frozen --extra dev --extra pg --extra redis
uv run --frozen --no-sync ruff check \
  simplebroker tests bin examples .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run --frozen --no-sync ruff format --check \
  simplebroker tests bin examples .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
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
uv run --frozen --no-sync pytest \
  tests/test_bump_uv.py tests/test_release_workflow.py
uv run --frozen --no-sync pytest
uv run --frozen --no-sync ./bin/pytest-pg --fast
uv run --frozen --no-sync ./bin/pytest-redis --fast
uv run --frozen --no-sync ./bin/packaging-smoke --python 3.11
docker run --rm --platform linux/amd64 \
  -e UV_PROJECT_ENVIRONMENT=/tmp/simplebroker-fuzz \
  -v "$PWD:/work:ro" -w /work \
  ghcr.io/astral-sh/uv:0.12.0-python3.11-trixie-slim \
  uv sync --frozen --extra dev --group fuzz --python 3.11
docker run --rm --platform linux/amd64 \
  -e UV_PROJECT_ENVIRONMENT=/tmp/simplebroker-fuzz \
  -v "$PWD:/work:ro" -w /work \
  ghcr.io/astral-sh/uv:0.12.0-python3.12-trixie-slim \
  sh -c 'uv sync --frozen --extra dev --group fuzz --python 3.12 &&
    mkdir -p /tmp/corpus-timestamp /tmp/corpus-dump-load &&
    uv run --frozen --no-sync --python 3.12 \
      python fuzz/fuzz_timestamp_validate.py \
      /tmp/corpus-timestamp -runs=100 &&
    uv run --frozen --no-sync --python 3.12 \
      python fuzz/fuzz_dump_load.py \
      /tmp/corpus-dump-load -runs=100'
python3 bin/check-dom15-fixtures
bin/check-doc-paths
git diff --check
```

## Out of Scope

- Product or backend behavior changes.
- Public runtime dependency-floor changes for psycopg, psycopg-pool, or redis.
- Package release/version changes.
- Adding or removing tools.
- Rewriting lint, type-check, test, or release policy.
- Updating GitHub Actions other than the existing shared uv version value.

## Deviation Log

| Source | Planned behavior | Actual behavior | Rationale | Follow-up |
|--------|------------------|-----------------|-----------|-----------|

## Review Log

| Review | Date | Verdict | Disposition |
|--------|------|---------|-------------|
| Claude plan review | 2026-07-29 | tool failure | Read-only invocation stalled and returned no findings; not counted as review evidence. |
| Independent agent plan review, round 1 | 2026-07-29 | BLOCKED | Corrected the Atheris/Python-3.11 target, added README and release-workflow consumers, all pytest floors, exact uv provenance, extension/fuzz execution, and executable mypy partitions. Round-2 review required. |
| Independent agent plan review, round 2 | 2026-07-29 | BLOCKED | Replaced Linux-only `mapfile` gates with a portable stdlib subprocess driver. Exact Docker-based Python 3.11/3.12 fuzz sync and bounded two-harness smoke commands were added while the review was in flight. Round-3 confirmation required. |
| Independent agent plan review, round 3 | 2026-07-29 | PASS after named correction | Added the missing POSIX continuations to both Dockerized Atheris commands. Reviewer confirmed no other implementation blocker. |
| Independent agent completed-work review | 2026-07-29 | PASS | Found no correctness, compatibility, scope, or verification blocker. Confirmed public runtime floors and package versions remain unchanged and all lock movement is expected. |

## Completion Evidence

- uv policy and locks: `python3 bin/bump_uv.py --check` passed with CI
  `0.12.0`, local `>=0.12.0,<0.13`, and 38/14/11-package root/PG/Redis
  resolutions.
- Static gates: Ruff 0.16.0 reported all checks passed and 289 files already
  formatted. Mypy 2.3.0 reported no issues in the 60-file core/release,
  28-file PostgreSQL, 26-file Redis, and 12-file example partitions.
- Policy tests: `tests/test_bump_uv.py` and `tests/test_release_workflow.py`
  passed, 40 tests total.
- Core suite: 2,010 passed and 17 platform/opt-in tests skipped.
- PostgreSQL: 989 shared tests passed with 3 skips; 146 extension tests passed
  with 5 opt-in skips.
- Redis: 982 shared tests passed with 10 backend/platform skips; 158 extension
  tests passed with 1 opt-in skip.
- Packaging: all three distributions built; the Python 3.11 wheel install and
  backend-plugin import smoke passed.
- Fuzz compatibility: Linux/x86-64 Python 3.11 frozen sync installed Atheris
  3.0.0. Python 3.12 frozen sync installed Atheris 3.1.0; both
  `timestamp_validate` and `dump_load` completed 100 libFuzzer runs.
- Non-tool transitive lock movement from `--upgrade` was limited to
  `typing-extensions` 4.15.0 → 4.16.0 and `tzdata` 2026.2 → 2026.3.
  Psycopg, psycopg-pool, Redis, SimpleBroker, and extension versions did not
  move.
- Documentation/process gates: DOM-15 fixtures, doc paths, and
  `git diff --check` passed.
