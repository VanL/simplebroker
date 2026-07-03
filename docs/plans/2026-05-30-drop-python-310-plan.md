# Drop Python 3.10 Support Plan

Date: 2026-05-30

Status: planned

Owner: SimpleBroker

## Purpose

Raise SimpleBroker's supported Python floor from Python 3.10 to Python 3.11.
Use that change to replace the custom `.broker.toml` parser with the standard
library `tomllib` module while keeping the core package dependency-free.

The important motivation is not syntax polish. The important motivation is
removing hand-written TOML parsing code from core runtime paths.

Assume the implementing engineer is skilled, but new to this repository and
likely to over-mock tests or over-design the migration. Follow this plan in
order. Keep each task small. Use red-green TDD. Prefer real config files, real
wheel metadata, and real subprocess smoke tests over mocks.

## Locked Decisions

These decisions are locked for this plan. Do not reopen them unless a red test
proves the plan cannot work.

### Drop 3.10 Everywhere

Use one support floor across all publishable packages:

- `simplebroker`
- `simplebroker-pg`
- `simplebroker-redis`

Do not keep the extensions at Python 3.10 after core moves to 3.11. A split
floor would create confusing metadata and weakens packaging smoke coverage.

### Do Not Add `tomli`

Do not add `tomli` or any other TOML dependency.

Reason: the root package's no-runtime-dependencies promise is a product
constraint. Python 3.11 gives us `tomllib` in the standard library. Use it.

### Do Not Build A New Config System

This plan is a parser replacement and support-floor change.

Do not add:

- a new config file name
- config includes
- TOML writing
- config migration commands
- a schema language
- backend-option allow lists in core
- `pydantic`, `attrs`, `dataclasses-json`, or any other validation framework

Keep the shape of `.broker.toml` stable.

### Keep Validation Where It Protects The Contract

`tomllib` parses TOML. It does not decide SimpleBroker's config contract.

Keep validation that protects existing behavior:

- `version` must be exactly `1`
- `backend` must be a non-empty string
- `target` must be a non-empty string
- `backend_options`, when present, must be a TOML table
- `backend_options` values must stay scalar values supported by the previous
  parser: string, integer, float, and boolean

Do not validate backend-specific option names in core. Postgres, Redis, and
future backends own their own option semantics.

Do not reject unknown top-level keys or unknown top-level tables in this plan.
The current loader ignores unused top-level data. Tightening that is a
separate behavior change and does not help the Python 3.11 migration.

### Keep The Code Boring

Favor deletion over abstraction.

Do not add a parser adapter class, compatibility module, config object model, or
new helper package. A small private validation helper in
`simplebroker/_project_config.py` is enough.

## Non-Goals

- Do not change queue behavior, watcher behavior, database schema, or backend
  APIs.
- Do not convert the repo to a uv workspace.
- Do not refactor release automation beyond the Python floor changes listed in
  this plan.
- Do not change the public `.broker.toml` examples except for wording tied to
  Python requirements.
- Do not add tests that merely prove `tomllib` works. Test SimpleBroker's
  config contract.

## Repository Primer

Runtime package:

- `simplebroker/`

Extension packages:

- `extensions/simplebroker_pg/`
- `extensions/simplebroker_redis/`

Important runtime files:

- `simplebroker/_project_config.py`
  - Owns `.broker.toml` discovery-adjacent loading and validation.
  - Contains the custom TOML subset parser to remove.
- `simplebroker/_runner.py`
  - Contains the Python 3.10-compatible `typing.Self` shim.
- `simplebroker/_backend_plugins.py`
  - Contains an entry-point compatibility fallback that can be removed on
    Python 3.11+.
- `simplebroker/watcher.py`
  - Contains a stale Python 3.8 compatibility comment and `__enter__`
    annotations that can use `Self`.
- `simplebroker/_scripts.py`
  - Owns `packaging-smoke`.
  - Hard-codes the default smoke Python and expected extension
    `Requires-Python`.
- `bin/release.py`
  - Owns release precheck commands.
  - Hard-codes `uv run ./bin/packaging-smoke --python 3.10`.

Packaging metadata:

- `pyproject.toml`
- `extensions/simplebroker_pg/pyproject.toml`
- `extensions/simplebroker_redis/pyproject.toml`
- `uv.lock`
- `extensions/simplebroker_pg/uv.lock`
- `extensions/simplebroker_redis/uv.lock`

Docs:

- `README.md`
- `extensions/simplebroker_pg/README.md`
- `extensions/simplebroker_redis/README.md`
- `CHANGELOG.md`

CI:

- `.github/workflows/test.yml`
- `.github/workflows/test-pg-extension.yml`
- `.github/workflows/test-redis-extension.yml`
- `.github/workflows/release-simplebroker.yml`
- `.github/workflows/release-simplebroker-pg.yml`
- `.github/workflows/release-simplebroker-redis.yml`

Primary tests:

- `tests/test_project_config.py`
- `tests/test_backend_plugin_resolution.py`
- `tests/test_no_dependencies.py`
- `tests/test_dev_scripts.py`
- `tests/test_release_script.py`
- `tests/test_performance.py`

External docs to skim before coding:

- Python `tomllib` docs:
  <https://docs.python.org/3.11/library/tomllib.html>
- PyPA core metadata, especially `Requires-Python` and `Provides-Extra`:
  <https://packaging.python.org/specifications/core-metadata/>
- uv dependency fields and `tool.uv.sources`:
  <https://docs.astral.sh/uv/concepts/projects/dependencies/>
- Python version status:
  <https://devguide.python.org/versions/>

## Current Behavior To Preserve

The current `.broker.toml` contract is small.

Required fields:

```toml
version = 1
backend = "sqlite"
target = "data/queue.db"
```

Optional backend options:

```toml
[backend_options]
schema = "simplebroker_pg_v1"
max_connections = 50
pool_timeout = 5.0
```

Rules to preserve:

- SQLite project targets are resolved relative to the config file directory.
- Non-SQLite backends receive the project target through `toml_target`.
- Backend options are passed through as a mapping through `toml_options`.
- Project config wins over ambient `BROKER_BACKEND_TARGET`.
- Ambient config can still supply supplemental secrets, such as
  `BROKER_BACKEND_PASSWORD`.
- Missing `backend_options` behaves like `{}`.
- A non-table `backend_options` value is invalid.

Rules not to invent:

- No config inheritance.
- No nested backend options.
- No arrays in backend options.
- No date, time, or datetime backend options.
- No core allow list for `schema`, `namespace`, `max_connections`, or
  `pool_timeout`. Backends validate their own names and values.

## Engineering Rules

### Red-Green TDD

For each behavior change:

1. Add or update the smallest test that fails for the current code.
2. Make the smallest implementation change.
3. Run the smallest relevant test slice.
4. Only then broaden the test run.

Do not batch ten file edits before seeing a red test. That is how compatibility
migrations grow bugs.

### DRY

Do not duplicate the target Python floor in ad hoc helper constants unless there
is already a single local place that owns that decision. Update existing
hard-coded occurrences where they already exist. Do not add a new global config
module just for this migration.

### YAGNI

Do not add optional abstractions for future TOML features. If a config shape is
not used today, do not support it today.

### Test Real Behavior

Prefer:

- temp `.broker.toml` files
- `load_project_config()`
- `resolve_project_target()`
- real wheel builds
- real packaging smoke installs

Avoid:

- mocking `tomllib`
- mocking file reads
- asserting implementation-private helper calls
- testing Python's TOML parser instead of SimpleBroker's config contract

Existing tests may use small dummy backend plugins to observe `toml_target` and
`toml_options`. That is acceptable when the behavior under test is core-to-plugin
handoff. Do not add more mocks unless there is no practical public seam.

## Task 0: Confirm The Worktree And Baseline

Files to inspect:

- `git status --short --branch`
- `pyproject.toml`
- `simplebroker/_project_config.py`
- `tests/test_project_config.py`
- `.github/workflows/test.yml`

Commands:

```bash
git status --short --branch
uv run pytest tests/test_project_config.py tests/test_no_dependencies.py tests/test_backend_plugin_resolution.py tests/test_dev_scripts.py tests/test_release_script.py
```

Expected baseline:

- Tests pass before you start, or failures are understood and unrelated.
- If lockfiles are already modified, do not revert them. Work with the current
  tree and keep your final diff clear.

Stop if:

- The repo has unrelated changes in files you need to edit and you cannot tell
  whether they are user work.
- The test baseline is failing in the exact area you are about to change and
  the failure is not understood.

## Task 1: Update Project-Config Tests First

Goal: describe the desired `tomllib` behavior before replacing the parser.

File to edit:

- `tests/test_project_config.py`

Tests to keep:

- `test_load_project_config_and_resolve_relative_sqlite_target`
- `test_load_project_config_preserves_unicode_sqlite_target`
- `test_load_project_config_decodes_toml_basic_string_escapes`
- `test_load_project_config_accepts_toml_literal_strings`
- project config precedence and discovery tests later in the file

Test update:

- Change `test_load_project_config_rejects_invalid_toml_basic_string_escape`
  so it asserts a `ValueError`, not the old custom message
  `Invalid TOML string escape: \q`.
- It is fine to assert a broad message fragment such as `Invalid` if the
  standard exception message is stable enough locally. Do not assert the full
  `tomllib` message.

Add contract tests:

1. Duplicate TOML keys are rejected.

   Example config:

   ```toml
   version = 1
   backend = "sqlite"
   target = "first.db"
   target = "second.db"
   ```

   Expected: `load_project_config()` raises `ValueError`.

   Reason: the old parser silently overwrites duplicate keys. `tomllib`
   rejects invalid TOML. This test should fail before the parser replacement
   and pass after it.

2. `backend_options` rejects arrays.

   Example config:

   ```toml
   version = 1
   backend = "redis"
   target = "redis://127.0.0.1:6379/0"

   [backend_options]
   namespace = ["one", "two"]
   ```

   Expected: `load_project_config()` raises `ValueError`.

3. `backend_options` rejects nested tables.

   Example config:

   ```toml
   version = 1
   backend = "redis"
   target = "redis://127.0.0.1:6379/0"

   [backend_options.pool]
   timeout = 5
   ```

   Expected: `load_project_config()` raises `ValueError`.

4. Unknown top-level fields remain harmless.

   Example config:

   ```toml
   version = 1
   backend = "sqlite"
   target = "queue.db"
   description = "ignored"
   ```

   Expected: `load_project_config()` returns the normal four keys:
   `version`, `backend`, `target`, and `backend_options`.

Do not add:

- parameterized tests covering every TOML type
- tests that import private validation helpers
- tests that mock `tomllib.load`

Small test gate:

```bash
uv run pytest tests/test_project_config.py -q
```

Expected before implementation:

- The invalid-escape update may still pass.
- The duplicate-key test should fail until `tomllib` is used.
- The array and nested-table tests are regression guardrails. They may already
  pass under the old parser, but they must keep passing after the replacement.

## Task 2: Replace The Custom TOML Parser With `tomllib`

Goal: delete parser code and keep only SimpleBroker schema validation.

File to edit:

- `simplebroker/_project_config.py`

Remove:

- `import re`
- `_SECTION_RE`
- `_BASIC_STRING_ESCAPES`
- `_HEX_RE`
- `_strip_comments()`
- `_parse_value()`
- `_parse_basic_string()`
- `_is_disallowed_basic_string_char()`
- `_parse_literal_string()`
- `_parse_project_config_text()`

Add:

```python
import tomllib
```

Implementation shape:

```python
def load_project_config(config_path: Path) -> dict[str, Any]:
    """Load and validate a .broker.toml file."""
    with config_path.open("rb") as config_file:
        data = tomllib.load(config_file)

    version = data.get("version")
    backend = data.get("backend")
    target = data.get("target")
    backend_options = _validated_backend_options(data.get("backend_options", {}))
    ...
```

Validation helper guidance:

- Keep it private.
- Keep it in `simplebroker/_project_config.py`.
- Keep it tiny.
- Accept only scalar backend option values:
  `str`, `int`, `float`, and `bool`.
- Reject `dict`, `list`, `datetime.date`, `datetime.time`,
  `datetime.datetime`, and other objects by rejecting anything outside the
  scalar allow list.
- Return a shallow `dict[str, Any]`.

Suggested helper:

```python
def _validated_backend_options(raw_options: object) -> dict[str, Any]:
    if not isinstance(raw_options, dict):
        raise ValueError("'backend_options' must be a table in .broker.toml")

    options: dict[str, Any] = {}
    for key, value in raw_options.items():
        if not isinstance(value, str | int | float | bool):
            raise ValueError(
                "'backend_options' values must be strings, integers, floats, "
                "or booleans in .broker.toml"
            )
        options[str(key)] = value
    return options
```

Important Python detail:

- `bool` is a subclass of `int`, but the union above is still clear and fine.
- Do not use `collections.abc.Mapping` for the parsed table. `tomllib` returns
  normal dictionaries, and this loader does not need a broader abstraction.

Small test gate:

```bash
uv run pytest tests/test_project_config.py -q
```

Invariants:

- The custom parser code is gone.
- Valid existing project configs still load.
- Invalid TOML raises `ValueError` through `tomllib.TOMLDecodeError`, which is
  a `ValueError` subclass.
- Backend option arrays and nested tables are rejected by SimpleBroker
  validation, not passed down to plugins.

## Task 3: Remove The `Self` Shim And Stale Compatibility Paths

Goal: remove Python <3.11 runtime compatibility code.

Files to edit:

- `simplebroker/_runner.py`
- `simplebroker/_backend_plugins.py`
- `simplebroker/watcher.py`
- `tests/test_performance.py`

In `simplebroker/_runner.py`:

- Remove `import sys` if it is only used for the shim.
- Replace the conditional import with a direct import:

  ```python
  from typing import Any, Literal, Protocol, Self, cast
  ```

- Keep `SQLiteRunner.__enter__(self) -> Self`.

In `simplebroker/_backend_plugins.py`:

- Simplify `_load_entry_point_plugin()` to use selectable entry points
  directly.
- Remove the fallback for old `metadata.entry_points()` dictionaries.

Target shape:

```python
def _load_entry_point_plugin(name: str) -> BackendPlugin:
    """Load an external backend plugin by entry point name."""
    matches = metadata.entry_points().select(
        group=BACKEND_ENTRY_POINT_GROUP,
        name=name,
    )
    ...
```

In `simplebroker/watcher.py`:

- Remove the stale comment:

  ```python
  # For Python 3.8 compatibility, we avoid using Self type
  # and use string forward references instead
  ```

- Import `Self` from `typing`.
- Change `BaseWatcher.__enter__()` to return `Self`.
- Change `SignalHandlerContext.__enter__()` to return `Self`.
- Do not refactor watcher lifecycle code.

In `tests/test_performance.py`:

- Remove the three stale `skipif` decorators that skip Windows performance
  tests only for Python 3.8 and 3.9.
- Keep the Windows-specific timeout values. Those are runtime/platform tuning,
  not Python-floor compatibility shims.

Tests:

```bash
uv run pytest tests/test_backend_plugin_resolution.py tests/test_process_broker_session.py tests/test_watcher.py tests/test_performance.py -q
```

Invariants:

- Built-in and external backend plugin resolution still works.
- Watcher context managers still return `self`.
- No `typing_extensions` import is introduced.

## Task 4: Update No-Dependency Tests For Python 3.11

Goal: remove test compatibility branches that only existed for Python 3.10.

File to edit:

- `tests/test_no_dependencies.py`

Changes:

- Replace the guarded `tomllib` import with direct `import tomllib`.
- Remove `pytest.mark.skipif(tomllib is None, ...)`.
- Keep the shared pytest marker.
- Update the `typing_extensions` failure guidance so it recommends direct
  standard library imports. Do not show a Python 3.10 fallback example.

Suggested guidance text:

```text
Use stdlib typing imports directly. This project requires Python 3.11+.
```

Tests:

```bash
uv run pytest tests/test_no_dependencies.py -q
```

Invariants:

- Root `project.dependencies` remains exactly `[]`.
- `typing_extensions` remains forbidden.

## Task 5: Update Packaging Metadata And Ruff Target

Goal: publish a coherent Python 3.11+ support policy.

Files to edit:

- `pyproject.toml`
- `extensions/simplebroker_pg/pyproject.toml`
- `extensions/simplebroker_redis/pyproject.toml`

Root `pyproject.toml` changes:

- Change:

  ```toml
  requires-python = ">=3.10"
  ```

  to:

  ```toml
  requires-python = ">=3.11"
  ```

- Remove classifier:

  ```toml
  "Programming Language :: Python :: 3.10",
  ```

- Keep classifiers for 3.11, 3.12, 3.13, and 3.14.
- Change Ruff target:

  ```toml
  target-version = "py311"
  ```

- Remove `UP045` and `UP006` from the Ruff ignore list.
- If removing them creates Ruff failures, fix the code. Do not re-ignore those
  rules without maintainer approval.

Extension `pyproject.toml` changes:

- Change both extension packages to:

  ```toml
  requires-python = ">=3.11"
  ```

Do not add runtime dependencies.

Small gates:

```bash
uv run ruff check pyproject.toml simplebroker tests bin extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run ruff format --check simplebroker tests bin extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
```

Invariants:

- `dependencies = []` remains unchanged in root metadata.
- Extension dependency lists remain backend-specific only.
- No Python 3.10 classifier remains in publishable metadata.

## Task 6: Update Packaging Smoke And Release Tooling

Goal: make development and release gates test the new floor.

Files to edit:

- `simplebroker/_scripts.py`
- `bin/release.py`
- `tests/test_dev_scripts.py`
- `tests/test_release_script.py`

In `simplebroker/_scripts.py`:

- Change the `packaging-smoke --python` default from `3.10` to `3.11`.
- Change expected extension wheel metadata from `>=3.10` to `>=3.11`.
- Keep the existing smoke behavior:
  - build root, PG, and Redis artifacts
  - verify distribution cleanliness
  - verify licenses
  - install `simplebroker[pg,redis]` into a fresh venv
  - resolve both external backend plugins

In `bin/release.py`:

- Change `ROOT_PACKAGING_SMOKE_COMMAND` from:

  ```python
  "3.10",
  ```

  to:

  ```python
  "3.11",
  ```

In `tests/test_dev_scripts.py`:

- Change fake extension metadata to `Requires-Python: >=3.11`.
- Add or update an assertion that default `packaging-smoke` uses `3.11` when
  no `--python` is supplied. Prefer observing the command passed to `_run()`
  over adding a parser-only unit test.

In `tests/test_release_script.py`:

- Update any expected packaging-smoke command text to use `3.11`.
- Do not weaken release tests just to make string updates easier.

Tests:

```bash
uv run pytest tests/test_dev_scripts.py tests/test_release_script.py -q
uv run ./bin/packaging-smoke --python 3.11
```

Invariants:

- Built wheel metadata for both extensions says `Requires-Python: >=3.11`.
- Fresh install smoke still proves both extras and entry points.
- Release prechecks no longer mention Python 3.10.

## Task 7: Update CI Workflows

Goal: CI should no longer test or advertise Python 3.10.

Files to edit:

- `.github/workflows/test.yml`
- `.github/workflows/test-pg-extension.yml`
- `.github/workflows/test-redis-extension.yml`
- release workflows only if their comments or setup versions contradict the new
  floor

In `.github/workflows/test.yml`:

- Change the main matrix from:

  ```yaml
  python-version: ["3.10", "3.11", "3.12", "3.13", "3.14"]
  ```

  to:

  ```yaml
  python-version: ["3.11", "3.12", "3.13", "3.14"]
  ```

- Remove the macOS exclusion for Python 3.10.
- Keep the existing macOS exclusions for older versions if the intended policy
  remains "macOS runners are expensive, so only test latest Python."
- Change the packaging job setup Python from `3.10` to `3.11`.
- Change packaging smoke from `--python 3.10` to `--python 3.11`.

In extension test workflows:

- Current extension matrices test 3.13 and 3.14 only. That does not prove the
  floor.
- Add Python 3.11 to both extension matrices.

Recommended matrices:

```yaml
python-version: ["3.11", "3.13", "3.14"]
```

Reason: 3.11 proves the floor, 3.13/3.14 preserve current newer-runtime
coverage. Do not add every version unless there is a concrete risk.

Release workflows:

- They currently use Python 3.13. That is fine. Do not change release workflow
  Python versions unless a test or comment depends on the floor.

Review gate:

```bash
rg -n "3\\.10|py310|>=3\\.10|Python 3\\.10" .github pyproject.toml extensions README.md simplebroker tests bin
```

Expected:

- No matches tied to Python support policy.
- No CI job should set up Python 3.10.

## Task 8: Update Docs And Changelog

Goal: user-facing docs match install metadata.

Files to edit:

- `README.md`
- `extensions/simplebroker_pg/README.md`
- `extensions/simplebroker_redis/README.md`
- `CHANGELOG.md`

In `README.md`:

- Requirements section: change `Python 3.10+` to `Python 3.11+`.
- Development command examples: change
  `uv run ./bin/packaging-smoke --python 3.10` to `--python 3.11`.
- Do not rewrite unrelated install sections.

In `extensions/simplebroker_pg/README.md`:

- Requirements section: change `Python 3.10+` to `Python 3.11+`.

In `extensions/simplebroker_redis/README.md`:

- Add a Requirements section if none exists, or update the existing wording if
  it already mentions Python.
- The extension should state `Python 3.11+`.

In `CHANGELOG.md`:

- Add an unreleased entry if the file has an unreleased section.
- If there is no unreleased section, add a clear top entry following existing
  style.
- Mention:
  - Python 3.10 support was dropped.
  - Python 3.11 is now the minimum supported version.
  - `.broker.toml` parsing now uses standard library `tomllib`.

Do not over-explain the internal parser deletion in user-facing docs. The
changelog is enough.

Docs gate:

```bash
rg -n "Python 3\\.10\\+|--python 3\\.10|requires Python 3\\.10|3\\.10\\+" README.md extensions CHANGELOG.md
```

Expected:

- Only historical changelog entries should remain.

## Task 9: Regenerate Lockfiles

Goal: lockfiles reflect the new Python floor and metadata.

Files expected to change:

- `uv.lock`
- `extensions/simplebroker_pg/uv.lock`
- `extensions/simplebroker_redis/uv.lock`

Commands:

```bash
uv lock
(cd extensions/simplebroker_pg && uv lock)
(cd extensions/simplebroker_redis && uv lock)
```

Review:

```bash
git diff -- uv.lock extensions/simplebroker_pg/uv.lock extensions/simplebroker_redis/uv.lock
```

Expected:

- `requires-python` entries move to `>=3.11`.
- Lockfile changes are explainable from metadata and dependency resolution.
- Do not manually edit lockfiles unless `uv lock` is unavailable and the
  maintainer explicitly accepts hand edits.

Note:

- If existing lockfile changes were present before this work, preserve them
  unless they directly conflict with this migration.

## Task 10: Run Focused Gates

Run these before broad tests. Fix failures in the task that caused them.

```bash
uv run pytest tests/test_project_config.py -q
uv run pytest tests/test_backend_plugin_resolution.py tests/test_no_dependencies.py -q
uv run pytest tests/test_dev_scripts.py tests/test_release_script.py -q
uv run ruff check simplebroker tests bin extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run ruff format --check simplebroker tests bin extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run mypy simplebroker bin/release.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests --config-file pyproject.toml
uv run ./bin/packaging-smoke --python 3.11
```

Expected:

- Config tests prove TOML parsing and validation.
- No-dependency test proves root runtime dependencies are still empty.
- Dev script tests prove packaging metadata expectations changed to 3.11.
- Mypy accepts direct `Self` imports.
- Packaging smoke builds and installs real artifacts.

## Task 11: Run Broad Gates

Run the normal suite after focused gates pass.

```bash
uv run pytest
PHASELOCK_ENABLE_XATTRS=0 uv run pytest tests/test_phaselock.py tests/test_runner_validation.py tests/test_runner_error_handling.py tests/test_queue_config_defaults.py tests/test_sqlite_setup_contention.py
```

If local services are available:

```bash
uv run ./bin/pytest-pg
uv run ./bin/pytest-redis
```

If Python 3.11 is not the current local interpreter:

```bash
uv run --python 3.11 pytest tests/test_project_config.py tests/test_no_dependencies.py tests/test_backend_plugin_resolution.py
uv run --python 3.11 ./bin/packaging-smoke --python 3.11
```

Do not mark the work complete until at least one 3.11 runtime gate passes
locally or in CI.

## Task 12: Final Diff Audit

Run:

```bash
git diff --stat
git diff -- pyproject.toml extensions/simplebroker_pg/pyproject.toml extensions/simplebroker_redis/pyproject.toml
git diff -- simplebroker/_project_config.py simplebroker/_runner.py simplebroker/_backend_plugins.py simplebroker/watcher.py
rg -n "3\\.10|>=3\\.10|py310|Python < 3\\.11|fallback for Python 3\\.10|Self was added|tomli|typing_extensions" \
  --glob '!.agents/**' \
  --glob '!uv.lock' \
  --glob '!extensions/*/uv.lock' \
  --glob '!venv/**' \
  --glob '!docs/plans/2026-05-30-drop-python-310-plan.md' \
  --glob '!CHANGELOG.md' \
  .
rg -n "Python 3\\.10 support was dropped|Python 3\\.11|tomllib|3\\.10" CHANGELOG.md docs/plans/2026-05-30-drop-python-310-plan.md
```

Expected:

- The first search has no support-policy matches. It may find irrelevant
  version numbers not tied to Python support; review them instead of ignoring
  them.
- The second search shows only the intended changelog note and this plan.
- No `tomli` dependency exists.
- No `typing_extensions` import exists.
- No custom TOML parser helper remains.
- No Python-version branch remains for `Self`.
- No CI workflow sets up Python 3.10.
- Docs say Python 3.11+.
- Packaging smoke defaults to Python 3.11.

If either grep finds a real support-policy reference outside the intended
changelog note, fix it. Do not silence the grep by adding broad excludes.

## Expected Final File Changes

Runtime:

- `simplebroker/_project_config.py`
- `simplebroker/_runner.py`
- `simplebroker/_backend_plugins.py`
- `simplebroker/watcher.py`
- `simplebroker/_scripts.py`

Tests:

- `tests/test_project_config.py`
- `tests/test_no_dependencies.py`
- `tests/test_dev_scripts.py`
- `tests/test_release_script.py`
- `tests/test_performance.py`

Packaging:

- `pyproject.toml`
- `extensions/simplebroker_pg/pyproject.toml`
- `extensions/simplebroker_redis/pyproject.toml`
- `uv.lock`
- `extensions/simplebroker_pg/uv.lock`
- `extensions/simplebroker_redis/uv.lock`

CI:

- `.github/workflows/test.yml`
- `.github/workflows/test-pg-extension.yml`
- `.github/workflows/test-redis-extension.yml`

Docs:

- `README.md`
- `extensions/simplebroker_pg/README.md`
- `extensions/simplebroker_redis/README.md`
- `CHANGELOG.md`

Plan:

- `docs/plans/2026-05-30-drop-python-310-plan.md`

## Common Mistakes To Avoid

- Adding `tomli` because it feels conservative. That defeats the purpose.
- Keeping the old parser "just in case." Delete it.
- Accepting every TOML type in `backend_options`. That silently expands the
  config API.
- Rejecting unknown top-level fields. That is a separate compatibility change.
- Adding core validation for Redis or Postgres option names. Plugins already
  own backend-specific validation.
- Testing parser implementation details instead of config behavior.
- Using mocks for packaging smoke. Build and install real artifacts.
- Updating root metadata but forgetting extension metadata.
- Updating `pyproject.toml` but forgetting `uv.lock`.
- Dropping 3.10 from CI but leaving packaging smoke on 3.10.
- Changing release workflow Python versions for no reason. Release workflows can
  run on a newer Python than the support floor.

## Completion Criteria

The migration is complete only when all of these are true:

1. Root and extension metadata require Python 3.11+.
2. No publishable package advertises Python 3.10 support.
3. Core runtime dependencies remain empty.
4. `.broker.toml` loads through `tomllib`.
5. Backend options remain a shallow scalar table.
6. The `Self` shim is removed.
7. The entry-point compatibility fallback is removed.
8. CI no longer runs Python 3.10.
9. Packaging smoke defaults to and passes on Python 3.11.
10. Docs and changelog describe Python 3.11+.
11. Focused and broad test gates pass, or any unavailable integration gate is
    explicitly reported.
