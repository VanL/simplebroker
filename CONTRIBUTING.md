# Contributing to SimpleBroker

This guide covers development setup, the test harness, lint and type
checks, and the release procedure. For product behavior, read the
canonical specs in [`docs/specs/`](docs/specs/00-specs-index.md). For
repository process (plans, reviews, documentation rules), start at
[`AGENTS.md`](AGENTS.md).

## Development setup

SimpleBroker uses [`uv`](https://github.com/astral-sh/uv) for package management and [`ruff`](https://github.com/astral-sh/ruff) for linting.

```bash
# Clone the repository
git clone git@github.com:VanL/simplebroker.git
cd simplebroker

# Install development environment
uv sync --all-extras

# Run tests
uv run pytest              # Fast tests only
uv run pytest -m ""        # All tests including benchmarks
PHASELOCK_ENABLE_XATTRS=0 uv run pytest tests/test_phaselock.py tests/test_runner_validation.py tests/test_runner_error_handling.py tests/test_queue_config_defaults.py tests/test_sqlite_setup_contention.py
uv run ./bin/pytest-pg     # All PG-backed tests with automatic Docker setup/teardown
uv run ./bin/pytest-redis  # All Redis-backed tests with automatic Docker setup/teardown (Valkey)
HYPOTHESIS_PROFILE=ci uv run pytest tests/test_property_*.py  # deeper property-test run (50 -> 200 examples per property)
python fuzz/fuzz_timestamp_validate.py  # coverage-guided fuzzing via Atheris (Linux only; see fuzz/*.py)
uv run ./bin/pytest-pg -q tests/test_watcher_metrics.py -k basic
uv run ./bin/packaging-smoke --python 3.11

# Lint and format
uv run ruff check .
uv run ruff format simplebroker tests bin .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run mypy simplebroker bin/release.py
MYPYPATH=. uv run mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases \
  --allow-untyped-defs --allow-incomplete-defs \
  $(find tests -type f -name '*.py' -not -path '*/__pycache__/*' | sort)
```

The Ruff lint gate extends the locked release's stable defaults with the
repository's existing `E`, `W`, `F`, `I`, `B`, `C4`, and `UP` families. Lint
discovery covers all tracked Python files and Python-shebang tools. Formatting
keeps the explicit path boundary shown above and does not format Markdown.

CI uses one pinned uv release while local development accepts the compatible
uv 0.12 line. Update both policies and all three lockfiles with one command:

```bash
python bin/bump_uv.py \
  --ci-version 0.12.0 \
  --required-version '>=0.12.0,<0.13'
python bin/bump_uv.py --check
```

Run the update with system Python. This still works when a newly installed uv
falls outside the old repository range. Review the workflow and lockfile diffs
before running the normal tests.

Property-based tests (`tests/test_property_*.py`, powered by Hypothesis)
check parser totality/round-trips and run a stateful model of queue
semantics against every backend; failures print a `@reproduce_failure`
blob that replays the exact case. The `fuzz/` harnesses drive the same
properties coverage-guided under Atheris (weekly via the Fuzz workflow);
a fuzz crash is a real property violation, replayable with plain pytest.

**Contributing guidelines:**
1. Keep it simple - the entire codebase should stay understandable
2. Maintain backward compatibility
3. Add tests for new features
4. Update documentation
5. Run linting and tests before submitting PRs

## Releases

Use the repo-local release helper instead of pushing release tags by hand:

```bash
# Release simplebroker
python bin/release.py --version X.Y.Z

# Release simplebroker-pg
python bin/release.py pg --version X.Y.Z

# Release every current unpublished package version with one local check run
python bin/release.py all

# Preview the checks, version files, commit, and tag action
python bin/release.py --dry-run

# Read back the release-related GitHub settings without changing anything
uv run python bin/release.py --check-repository-settings
```

Replace `X.Y.Z` with the next unpublished version for the package being
released.

Real releases must run from `main`. The helper checks the target version against
GitHub Releases and PyPI, verifies the repository's immutable-release, tag,
environment, and Actions SHA-pinning settings, runs the local release checks,
updates and commits release files, and pushes the release commit to `main`. It
then waits for the target's normal workflows to pass on that exact commit and
checks that the commit is still reachable from a freshly fetched `origin/main`.
Only then does it create the final tag at the tested SHA and push it.

Remote release tags are permanent. They are never moved or deleted. A wrong
remote tag requires a new version. A local-only tag may be replaced before it
is pushed.

The tag workflow rechecks the normal workflows and tag SHA, builds and attests
the distributions, and stages every distribution and Sigstore bundle on a
draft GitHub Release. It publishes to PyPI with trusted publishing only after
that complete draft exists, then verifies the exact draft asset set and
publishes the immutable GitHub Release. Keeping the build, attestation, and
publish steps in the top-level gate workflow makes PyPI's trusted-publisher
identity match the artifact attestation build-config URI.
The local release helper also ruff-checks `examples/`, runs all
pytest-discovered example tests under `examples/`, mypy-checks every Python
example file, and mypy-checks the selected extension test tree. Core and batch
releases also mypy-check every root-test Python file; extension releases retain
their selected extension-test scope. Those extra local checks are not part of
the CI release workflows.
Core releases wait for `Test`, `Test Postgres Extension`, and
`Test Redis Extension`; extension releases wait for `Test` plus their matching
backend workflow.

If pre-tag CI fails, is cancelled, is missing, or times out, no final tag has
been created. Fix `main` and rerun the helper with the same unpublished
version. An interrupted helper can also be rerun at the same release commit;
it resumes the exact-SHA check without creating another release commit. If a
transient publication step fails after the tag exists, retry the workflow only
when the tag still points at the same SHA. If recovery needs a code change, use
the next patch version. Never delete, move, or reuse a published tag or version.

PyPI trusted publisher entries should use repository `VanL/simplebroker`, the
`pypi` environment, and these GitHub Actions workflows:

- `release-gate.yml` for `simplebroker`
- `release-gate-pg.yml` for `simplebroker-pg`
- `release-gate-redis.yml` for `simplebroker-redis`

Use `python bin/release.py all` after version files have already been bumped
across packages. It scans `simplebroker`, `simplebroker-pg`, and
`simplebroker-redis`, skips versions already published on GitHub Releases or
PyPI, runs the combined release checks once, syncs root extension extras when
the core package is part of the batch, creates one release commit if needed, and
pushes all selected tags. Extension tags are prepared before the core tag so a
batch release can carry new extension baselines and the matching core package
together.

When releasing only `simplebroker` with updated extension extras, the extension
versions must already be available on PyPI first. The `all` target is the path
for releasing unpublished extension baselines and the core package together.
