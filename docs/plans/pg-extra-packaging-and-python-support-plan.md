# PG Extra Packaging And Python Support Plan

## Purpose

This document is the implementation plan for two linked packaging changes:

1. make `simplebroker[pg]` a real convenience install path that pulls in the
   external `simplebroker-pg` package
2. lower `simplebroker-pg` to Python 3.10+ so it matches SimpleBroker core's
   supported floor

The important product boundary does **not** change:

- `simplebroker` remains SQLite-first
- `simplebroker-pg` remains a separate distribution
- Postgres remains an external plugin discovered via entry points
- the new `pg` extra is metadata convenience, not a built-in backend

Assume the implementing engineer is technically strong, but:

- has zero context for this repository
- will over-abstract if not constrained
- tends to over-mock tests instead of exercising the real system

Follow this plan in order. Keep each slice small. Keep the suite green after
each task.

## Scope Locks

These decisions are locked for this plan. Do not reopen them while
implementing.

### Product boundary lock

Keep Postgres separate.

- Do **not** move Postgres code under
  [`simplebroker/`](/Users/van/Developer/simplebroker/simplebroker).
- Do **not** add direct imports of `simplebroker_pg` inside core runtime code.
- Do **not** replace entry-point plugin discovery with direct imports.
- Do **not** reframe Postgres as "part of core." It remains a sibling package.

### Python-version lock

Do **not** raise SimpleBroker core to Python 3.11 for this work.

- Core stays at Python 3.10+.
- The extension should be lowered to Python 3.10+ unless a real 3.10 runtime
  incompatibility is discovered.
- A speculative reason is not enough. Only a reproduced, real incompatibility
  can block the floor change.

### TOML-boundary lock

TOML parsing stays in core.

- Do **not** add `tomllib` or TOML parsing logic to
  [`extensions/simplebroker_pg/simplebroker_pg/`](/Users/van/Developer/simplebroker/extensions/simplebroker_pg/simplebroker_pg).
- Core already parses `.broker.toml` and passes normalized values to the
  plugin.
- The plugin should continue to consume `toml_target` and `toml_options`
  inputs, not read project config files itself.

### Packaging lock

Use published extras plus uv-only local source mapping.

- Add a `pg` extra to the root project in
  [`pyproject.toml`](/Users/van/Developer/simplebroker/pyproject.toml).
- Use `[tool.uv.sources]` for local development so `uv sync --extra pg` resolves
  the sibling extension from this repo.
- Use an **editable** path source for the local uv source mapping.
- Do **not** convert the repo into a uv workspace in this plan.
- Do **not** try to make one wheel contain both projects.

### Version-coupling lock

Keep compatibility bounds explicit and simple.

- The root `pg` extra should depend on a bounded major range of
  `simplebroker-pg`, not an exact pin.
- The extension should depend on a bounded major range of `simplebroker`, not
  an unbounded lower-only range.
- Do **not** invent lockstep same-version coupling between the two packages.
- Do **not** leave cross-major compatibility entirely unbounded.

### Testing lock

Prefer real packaging and runtime tests over mocks.

- Do **not** "test" the extra only by reading `pyproject.toml`.
- Do **not** mock package installation, wheel metadata, or entry-point loading
  for the packaging smoke gate.
- Build real artifacts.
- Install real artifacts into a fresh environment.
- Resolve the real `postgres` plugin through the public plugin loader.
- For Python support, run the real PG-backed test flow on Python 3.10.

### Scope-control lock

Keep this plan about packaging and support alignment only.

- Do **not** refactor backend architecture.
- Do **not** revisit uv workspace adoption.
- Do **not** rewrite the dev dependency model unless a concrete packaging issue
  forces it.
- Do **not** opportunistically rework docs unrelated to installation,
  requirements, release notes, or install hints.

## Why This Is The Right Shape

The current repo already has the hard runtime pieces in place:

- core resolves external backends through the `simplebroker.backends` entry
  point group in
  [`simplebroker/_backend_plugins.py`](/Users/van/Developer/simplebroker/simplebroker/_backend_plugins.py)
- `simplebroker-pg` already publishes the `postgres` backend entry point in
  [`extensions/simplebroker_pg/pyproject.toml`](/Users/van/Developer/simplebroker/extensions/simplebroker_pg/pyproject.toml)
- core already gives a targeted install hint for missing Postgres support in
  [`simplebroker/project.py`](/Users/van/Developer/simplebroker/simplebroker/project.py)

That means the remaining work is packaging, support policy, docs, and CI:

- published metadata must advertise a `pg` extra
- local uv development must resolve the sibling package from the repo
- Python 3.10 support for the extension must be proven instead of assumed
- docs must describe the convenience extra without breaking the "SQLite-first,
  no-dependency by default" story

## Success Criteria

The work is done only when **all** of these are true:

1. The built `simplebroker` wheel metadata contains:
   - `Provides-Extra: pg`
   - a `Requires-Dist` entry for `simplebroker-pg ... ; extra == 'pg'`
2. A fresh Python 3.10 environment can install `simplebroker[pg]` from built
   local artifacts and then successfully:
   - import `simplebroker_pg`
   - resolve `get_backend_plugin("postgres")`
3. From the repo root, `uv sync --extra pg` uses the sibling extension through
   `tool.uv.sources` and makes `simplebroker_pg` importable without publishing
   anything first.
4. The extension PG test workflow runs on Python 3.10 and at least one
   representative newer Python version.
5. The docs still make it clear that:
   - plain `simplebroker` remains SQLite-first and zero-dependency by default
   - `simplebroker[pg]` is only a convenience install path for the separate
     plugin package
6. Release sequencing is documented so core is never published with a `pg`
   extra pointing at a not-yet-published extension version.

## Repository Primer

### Project layout

- Runtime code:
  [`simplebroker/`](/Users/van/Developer/simplebroker/simplebroker)
- Root packaging metadata:
  [`pyproject.toml`](/Users/van/Developer/simplebroker/pyproject.toml)
- Root lockfile:
  [`uv.lock`](/Users/van/Developer/simplebroker/uv.lock)
- Postgres extension subproject:
  [`extensions/simplebroker_pg/`](/Users/van/Developer/simplebroker/extensions/simplebroker_pg)
- Extension packaging metadata:
  [`extensions/simplebroker_pg/pyproject.toml`](/Users/van/Developer/simplebroker/extensions/simplebroker_pg/pyproject.toml)
- Extension lockfile:
  [`extensions/simplebroker_pg/uv.lock`](/Users/van/Developer/simplebroker/extensions/simplebroker_pg/uv.lock)
- Root tests:
  [`tests/`](/Users/van/Developer/simplebroker/tests)
- Extension tests:
  [`extensions/simplebroker_pg/tests/`](/Users/van/Developer/simplebroker/extensions/simplebroker_pg/tests)
- Test helper for PG-backed flows:
  [`bin/pytest-pg`](/Users/van/Developer/simplebroker/bin/pytest-pg)
- CI workflows:
  [`.github/workflows/test.yml`](/Users/van/Developer/simplebroker/.github/workflows/test.yml)
  [`.github/workflows/test-pg-extension.yml`](/Users/van/Developer/simplebroker/.github/workflows/test-pg-extension.yml)
  [`.github/workflows/release-simplebroker.yml`](/Users/van/Developer/simplebroker/.github/workflows/release-simplebroker.yml)
  [`.github/workflows/release-simplebroker-pg.yml`](/Users/van/Developer/simplebroker/.github/workflows/release-simplebroker-pg.yml)

### Files you must read before changing anything

Read these first. Do not skip them.

- [`pyproject.toml`](/Users/van/Developer/simplebroker/pyproject.toml)
- [`extensions/simplebroker_pg/pyproject.toml`](/Users/van/Developer/simplebroker/extensions/simplebroker_pg/pyproject.toml)
- [`README.md`](/Users/van/Developer/simplebroker/README.md)
- [`extensions/simplebroker_pg/README.md`](/Users/van/Developer/simplebroker/extensions/simplebroker_pg/README.md)
- [`simplebroker/project.py`](/Users/van/Developer/simplebroker/simplebroker/project.py)
- [`simplebroker/_backend_plugins.py`](/Users/van/Developer/simplebroker/simplebroker/_backend_plugins.py)
- [`tests/test_project_config.py`](/Users/van/Developer/simplebroker/tests/test_project_config.py)
- [`tests/test_backend_plugin_resolution.py`](/Users/van/Developer/simplebroker/tests/test_backend_plugin_resolution.py)
- [`tests/test_constants.py`](/Users/van/Developer/simplebroker/tests/test_constants.py)
- [`bin/pytest-pg`](/Users/van/Developer/simplebroker/bin/pytest-pg)

### External docs worth skimming

If you are unfamiliar with extras, uv source overrides, or packaging metadata,
read these before you edit packaging files:

- uv dependency fields and `tool.uv.sources`:
  <https://docs.astral.sh/uv/concepts/projects/dependencies/>
- PyPA core metadata, especially `Requires-Dist`, `Requires-Python`, and
  `Provides-Extra`:
  <https://packaging.python.org/en/latest/specifications/core-metadata/>

## Engineering Rules

### Red-green TDD

For every task:

1. make the expected failure visible first
2. make the smallest change that fixes it
3. rerun the smallest relevant test slice
4. only then move on

If you cannot make the failure visible locally because the needed Python
interpreter is unavailable, wire the automated CI check in the same task and
use that as the proof gate. Do not claim support based on static reading alone.

### DRY

Do not duplicate complex packaging smoke logic in multiple workflows.

If you need a non-trivial artifact smoke test, put it in a reusable script under
[`bin/`](/Users/van/Developer/simplebroker/bin) and call that script from CI and
from the local verification instructions.

### YAGNI

Do not use this packaging change to invent a larger monorepo system.

- no uv workspace migration
- no release orchestration framework
- no bespoke metadata parser
- no generic plugin-management abstraction

### Avoid bad tests

This repo already has some acceptable monkeypatch-based tests around error
paths. Keep those where they already exist.

For the new work:

- good: build artifacts, install artifacts, real import, real entry-point
  resolution, real PG-backed tests
- bad: asserting only on TOML text, mocking wheel metadata, mocking the
  installer, mocking importlib metadata in the new packaging smoke tests

## Test Strategy

There are **four** different things to prove. Do not collapse them into one
test and then miss coverage.

### 1. Local-development uv source proof

Goal:

- `uv sync --extra pg` from the repo root should install the sibling extension
  from this checkout

Why this matters:

- `project.optional-dependencies` is published metadata
- `tool.uv.sources` is uv-only development metadata
- both must be right for the local developer experience to work

What to assert:

- `simplebroker_pg` is importable after `uv sync --extra pg`
- `get_backend_plugin("postgres")` resolves successfully

### 2. Built-artifact metadata proof

Goal:

- the published wheel metadata for `simplebroker` declares the `pg` extra
  correctly

What to assert:

- `Provides-Extra: pg` exists
- `Requires-Dist: simplebroker-pg ... ; extra == 'pg'` exists
- the extra name is exactly `pg`

### 3. Built-artifact install proof

Goal:

- a clean Python 3.10 environment can install the built artifacts and load the
  Postgres plugin

What to assert:

- install succeeds from local `dist/` artifacts without reaching out to PyPI
- `import simplebroker_pg` succeeds
- `from simplebroker.ext import get_backend_plugin` followed by
  `get_backend_plugin("postgres")` succeeds

### 4. Real runtime support proof

Goal:

- the extension truly works on Python 3.10, not just installs on 3.10

What to assert:

- the PG-backed release-gate suite passes on Python 3.10
- the same workflow still passes on one newer Python version

## Implementation Tasks

Each task is a complete red-green slice. Keep the scope inside the task.

### Task 1: Align The Extension's Supported Python Floor

#### Goal

Lower `simplebroker-pg` from Python 3.11+ to Python 3.10+ and prove that claim
with real tests.

#### Files to touch

- [`extensions/simplebroker_pg/pyproject.toml`](/Users/van/Developer/simplebroker/extensions/simplebroker_pg/pyproject.toml)
- [`extensions/simplebroker_pg/uv.lock`](/Users/van/Developer/simplebroker/extensions/simplebroker_pg/uv.lock)
- [`.github/workflows/test-pg-extension.yml`](/Users/van/Developer/simplebroker/.github/workflows/test-pg-extension.yml)

Possible doc touch in the same task only if you add a Python version statement:

- [`extensions/simplebroker_pg/README.md`](/Users/van/Developer/simplebroker/extensions/simplebroker_pg/README.md)

#### What to change

1. Change the extension `requires-python` field from `>=3.11` to `>=3.10`.
2. Tighten the extension's dependency on core to a bounded major range:
   - preferred shape: `simplebroker>=3.0.0,<4`
3. Regenerate the extension lockfile. Do **not** hand-edit it.
4. Expand the PG extension CI workflow to run on:
   - Python 3.10
   - one representative newer Python version

Use a small matrix. Do **not** explode it to every Python version and every OS.
This workflow runs Docker-backed tests and will get expensive quickly.

Recommended matrix:

- `["3.10", "3.12"]`

That is enough to prove the new floor and keep one modern interpreter lane.

#### Red step

Before the metadata change, make the unsupported floor visible.

If Python 3.10 is available locally:

```bash
cd /Users/van/Developer/simplebroker
uv run --python 3.10 --with-editable ./extensions/simplebroker_pg python -c "import simplebroker_pg"
```

Expected current behavior before the change:

- installation or environment resolution should fail because the extension says
  it needs Python 3.11+

If Python 3.10 is not available locally:

- add the 3.10 CI lane first
- use the failing CI result as your red proof

#### Green step

After editing metadata:

```bash
cd /Users/van/Developer/simplebroker/extensions/simplebroker_pg
uv lock
```

Then rerun the local smoke if Python 3.10 is available:

```bash
cd /Users/van/Developer/simplebroker
uv run --python 3.10 --with-editable ./extensions/simplebroker_pg python -c "import simplebroker_pg"
uv run --python 3.10 bin/pytest-pg --fast
```

#### Invariants

- Do **not** add `tomllib` or any 3.11-only fallback package here.
- Do **not** change core's Python floor as a shortcut.
- Do **not** declare success based only on importability. The PG-backed test
  flow must pass on 3.10.

#### Gate to leave this task

You may leave this task only when:

- the extension metadata is 3.10+
- the extension lockfile is regenerated
- the PG workflow has a 3.10 lane
- the 3.10 lane is passing, locally or in CI

### Task 2: Add The Root `pg` Extra And Local uv Source Mapping

#### Goal

Make the root package advertise an installable `pg` extra and make local repo
development resolve the sibling extension automatically when using uv.

#### Files to touch

- [`pyproject.toml`](/Users/van/Developer/simplebroker/pyproject.toml)
- [`uv.lock`](/Users/van/Developer/simplebroker/uv.lock)

#### What to change

1. Add a new published optional dependency:

   - section:
     `[project.optional-dependencies]`
   - key:
     `pg`
   - value shape:
     `["simplebroker-pg>=<new-extension-version>,<2"]`

Do **not** guess `<new-extension-version>`.

- It must be the first extension release that includes the 3.10 support change
  from Task 1.
- If you have not chosen that version yet, do the version-selection part of
  Task 5 before you finalize this edit.

2. Add a uv-only local source mapping:

   - section:
     `[tool.uv.sources]`
   - entry:
     `simplebroker-pg = { path = "./extensions/simplebroker_pg", editable = true }`

Why `editable = true`:

- this is a sibling package under active development
- a non-editable local path would make extension edits invisible until a resync
- editable behavior is the least surprising choice for a monorepo-like checkout

Why `tool.uv.sources` instead of a workspace:

- it solves exactly the local-dev problem we have
- it does not require collapsing the repo into a shared workspace model
- it does not force a broader lockfile/layout decision

#### Red step

Before the metadata change, prove that the repo root cannot currently resolve
the extension through an extra:

```bash
cd /Users/van/Developer/simplebroker
uv sync --extra pg
```

Expected current behavior before the change:

- uv should fail because the root project does not define a `pg` extra

#### Green step

After editing [`pyproject.toml`](/Users/van/Developer/simplebroker/pyproject.toml):

```bash
cd /Users/van/Developer/simplebroker
uv lock
uv sync --extra pg
uv run --extra pg python -c "import simplebroker_pg; from simplebroker.ext import get_backend_plugin; assert get_backend_plugin('postgres').name == 'postgres'"
```

#### Invariants

- Do **not** add `simplebroker-pg` to root `project.dependencies`.
- Do **not** make Postgres part of the default install.
- Do **not** use a direct path dependency in published metadata.
- Do **not** hand-edit [`uv.lock`](/Users/van/Developer/simplebroker/uv.lock).

#### Gate to leave this task

You may leave this task only when:

- `uv sync --extra pg` succeeds from the repo root
- the installed root environment can import `simplebroker_pg`
- `get_backend_plugin("postgres")` resolves successfully

### Task 3: Add A Reusable Packaging Smoke Gate For Built Artifacts

#### Goal

Prove that published artifacts, not just the checkout, support
`simplebroker[pg]` correctly on Python 3.10.

#### Files to touch

- add one reusable script under
  [`bin/`](/Users/van/Developer/simplebroker/bin)
- [`.github/workflows/test.yml`](/Users/van/Developer/simplebroker/.github/workflows/test.yml)

Recommended script name:

- [`bin/packaging-smoke`](/Users/van/Developer/simplebroker/bin/packaging-smoke)

Use Python for the script, not shell glue. The repo already uses Python helper
scripts in `bin/`.

#### What the script must do

1. Build the root distributions:

   First remove stale build output so the script cannot accidentally inspect an
   older wheel:

   ```bash
   rm -rf dist extensions/simplebroker_pg/dist
   ```

   Then build:

   ```bash
   python -m build
   ```

2. Build the extension distributions:

   ```bash
   python -m build extensions/simplebroker_pg
   ```

3. Locate the built root wheel **and** the built extension wheel, then inspect
   their `METADATA` files using stdlib:
   - `zipfile`
   - `email.parser`

4. Assert on metadata:
   - root wheel:
     - `Provides-Extra: pg`
     - a `Requires-Dist` entry for `simplebroker-pg` with `extra == 'pg'`
   - extension wheel:
     - `Requires-Python: >=3.10`

5. Create a fresh temporary environment for a target Python version.

6. Install from local artifacts only, using both `dist/` directories as
   `--find-links` sources.

   Recommended shape:

   ```bash
   uv pip install \
     --python <temp-python> \
     --no-index \
     --find-links dist \
     --find-links extensions/simplebroker_pg/dist \
     "simplebroker[pg]==<root-version>"
   ```

7. Run a real import and plugin-resolution smoke:

   ```python
   import simplebroker_pg
   from simplebroker.ext import get_backend_plugin

   plugin = get_backend_plugin("postgres")
   assert plugin.name == "postgres"
   ```

The script should accept a Python version argument, so CI can explicitly run it
on 3.10.

Recommended CLI:

```bash
uv run --python 3.10 bin/packaging-smoke --python 3.10
```

If you prefer a single source of truth, the script may detect the active
interpreter itself and only take `--python` for the install target. Keep it
simple.

#### Why this needs a script

Without a script, the same long, easy-to-break packaging logic will end up:

- in CI YAML
- in local verification notes
- in engineer memory

That is bad DRY.

#### CI work in this task

Add a small `packaging` job to
[`.github/workflows/test.yml`](/Users/van/Developer/simplebroker/.github/workflows/test.yml).

Keep it narrow:

- one OS: `ubuntu-latest`
- one Python version: `3.10`

The job should:

1. check out the repo
2. set up Python 3.10
3. install uv
4. install build tooling
5. run the packaging smoke script

Do **not** add this smoke to every OS or every Python version. That would be
wasteful and slower than the value gained.

#### Red step

Before the script is green, make it fail for the right reason:

- missing `Provides-Extra: pg`
- missing `Requires-Dist` linkage
- install failure from local artifacts
- plugin resolution failure after install

Any one of those is acceptable as the initial red proof. What matters is that
the script proves the actual packaging contract, not a proxy for it.

#### Green step

Run the script locally if possible:

```bash
cd /Users/van/Developer/simplebroker
uv run --python 3.10 bin/packaging-smoke --python 3.10
```

Then make sure the new CI job passes.

#### Invariants

- Do **not** parse `pyproject.toml` as the primary proof. Inspect built wheel
  metadata instead.
- Do **not** hit PyPI in this smoke test.
- Do **not** mock entry-point discovery.
- Do **not** skip the Python 3.10 artifact install. That is the whole point of
  this gate.

#### Gate to leave this task

You may leave this task only when:

- the packaging smoke script passes locally or in CI
- the `packaging` CI job is green

### Task 4: Update User-Facing Install Hints And Docs Without Breaking Expectations

#### Goal

Explain the convenience extra clearly while preserving the current "SQLite-first
by default" and "separate Postgres package" story.

#### Files to touch

- [`README.md`](/Users/van/Developer/simplebroker/README.md)
- [`extensions/simplebroker_pg/README.md`](/Users/van/Developer/simplebroker/extensions/simplebroker_pg/README.md)
- [`simplebroker/project.py`](/Users/van/Developer/simplebroker/simplebroker/project.py)
- [`tests/test_project_config.py`](/Users/van/Developer/simplebroker/tests/test_project_config.py)

Optional maintainer-facing doc touch if you decide to encode release order in
the README release section:

- [`README.md`](/Users/van/Developer/simplebroker/README.md)

#### What to change

1. Root README install section:
   - keep plain `simplebroker` install instructions
   - add a short, clearly optional Postgres install path such as:
     - `uv add "simplebroker[pg]"`
     - `pip install "simplebroker[pg]"`
   - for `pipx`, either:
     - document fresh install with `pipx install "simplebroker[pg]"`
     - or keep `pipx inject simplebroker simplebroker-pg` as the recommended
       path for existing installs

2. Root README advanced plugin section:
   - explicitly say `simplebroker[pg]` is a convenience way to install the
     external `simplebroker-pg` package
   - keep the wording that core remains SQLite-first and dependency-free by
     default

3. Extension README install section:
   - keep the statement that this package is intentionally separate
   - add a note that users may also install it via `simplebroker[pg]`
   - do **not** imply that the extension is built into core

4. Missing-plugin install hint in
   [`simplebroker/project.py`](/Users/van/Developer/simplebroker/simplebroker/project.py):
   - update the message so it mentions both the direct package and the
     convenience extra
   - keep the wording package-manager-agnostic

Recommended message shape:

```text
Requested backend 'postgres' is not available. Install simplebroker-pg or simplebroker[pg].
```

That is intentionally generic and stable. Do **not** bake `uv add`, `pip`, or
`pipx` into the runtime exception text.

5. Update the existing test in
   [`tests/test_project_config.py`](/Users/van/Developer/simplebroker/tests/test_project_config.py)
   to match the new install hint.

#### Red step

Run the targeted existing test before changing the message:

```bash
cd /Users/van/Developer/simplebroker
uv run pytest tests/test_project_config.py -k missing_postgres_plugin_has_install_hint
```

It should fail once you change the expected string and before you update the
runtime message.

#### Green step

After the doc and message updates:

```bash
cd /Users/van/Developer/simplebroker
uv run pytest tests/test_project_config.py -k missing_postgres_plugin_has_install_hint
```

Also do a quick doc truthfulness pass:

- root README still says plain `simplebroker` is the default install
- extension README still says it is a separate package
- no README sentence implies Postgres is built into the default core install

#### Invariants

- Do **not** weaken or remove the "zero-dependency by default" story for plain
  `simplebroker`.
- Do **not** describe the extra as if it were a built-in backend.
- Do **not** add package-manager-specific commands to runtime exceptions.

#### Gate to leave this task

You may leave this task only when:

- the install-hint test passes
- the docs are truthful and internally consistent

### Task 5: Record The Release Sequence And Version-Bound Contract

#### Goal

Prevent the most likely packaging footgun: publishing core with a `pg` extra
that points at an extension version that does not exist yet on PyPI.

#### Files to touch

If release prep happens in the same PR:

- [`extensions/simplebroker_pg/pyproject.toml`](/Users/van/Developer/simplebroker/extensions/simplebroker_pg/pyproject.toml)
- [`pyproject.toml`](/Users/van/Developer/simplebroker/pyproject.toml)
- [`simplebroker/_constants.py`](/Users/van/Developer/simplebroker/simplebroker/_constants.py)
- [`CHANGELOG.md`](/Users/van/Developer/simplebroker/CHANGELOG.md)

If release prep happens in a separate PR:

- document the exact sequence in that release PR and do **not** lose the steps
  below

#### What to do

1. Pick the next extension version that will be the first 3.10-compatible
   release.

2. Make the root `pg` extra's lower bound point at **that** extension version.

3. If the repo's release process includes checked-in version bumps, update:
   - extension version in
     [`extensions/simplebroker_pg/pyproject.toml`](/Users/van/Developer/simplebroker/extensions/simplebroker_pg/pyproject.toml)
   - root version in
     [`pyproject.toml`](/Users/van/Developer/simplebroker/pyproject.toml)
   - root `__version__` in
     [`simplebroker/_constants.py`](/Users/van/Developer/simplebroker/simplebroker/_constants.py)
   - changelog entry in
     [`CHANGELOG.md`](/Users/van/Developer/simplebroker/CHANGELOG.md)

4. Release order must be:

   1. publish `simplebroker-pg`
   2. verify the new extension version is available on PyPI
   3. publish `simplebroker` with the `pg` extra lower bound that references
      that published extension version

5. If you update maintainer-facing docs, put that release-order note where
   maintainers will actually see it. The current release snippets in
   [`README.md`](/Users/van/Developer/simplebroker/README.md) are a plausible
   place because that file already documents release tags.

#### Invariants

- Do **not** publish the root extra first.
- Do **not** leave the root extra lower bound pointing at an older extension
  version if the point of this change is 3.10 compatibility.
- Do **not** forget to keep root version declarations synchronized between
  [`pyproject.toml`](/Users/van/Developer/simplebroker/pyproject.toml) and
  [`simplebroker/_constants.py`](/Users/van/Developer/simplebroker/simplebroker/_constants.py).

#### Gate to leave this task

You may leave this task only when:

- the release order is written down somewhere durable
- the version bound in the root `pg` extra matches the intended extension
  release

## Verification Checklist

Run these before declaring the implementation done.

### Targeted tests

```bash
cd /Users/van/Developer/simplebroker
uv run pytest tests/test_constants.py
uv run pytest tests/test_backend_plugin_resolution.py
uv run pytest tests/test_project_config.py
```

### Local uv-development smoke

```bash
cd /Users/van/Developer/simplebroker
uv lock
uv sync --extra pg
uv run --extra pg python -c "import simplebroker_pg; from simplebroker.ext import get_backend_plugin; assert get_backend_plugin('postgres').name == 'postgres'"
```

### Extension 3.10 smoke

If Python 3.10 is installed locally:

```bash
cd /Users/van/Developer/simplebroker
uv run --python 3.10 bin/pytest-pg --fast
```

### Artifact packaging smoke

```bash
cd /Users/van/Developer/simplebroker
uv run --python 3.10 bin/packaging-smoke --python 3.10
```

### CI gates

All of these must be green:

- root test workflow
- new packaging job in the root workflow
- PG extension workflow on Python 3.10
- PG extension workflow on the chosen newer Python lane

## Stop Conditions

Stop and escalate instead of improvising if any of these happen:

1. Python 3.10 PG-backed tests fail because of a real dependency or runtime
   incompatibility in `psycopg`, `psycopg_pool`, or the extension code.

   Acceptable next action:

   - debug and fix the real incompatibility if it is in our code

   Not acceptable:

   - silently raising core to Python 3.11
   - adding TOML parsing or unrelated compatibility hacks to the extension

2. The packaging smoke requires a much larger toolchain redesign than expected.

   Acceptable next action:

   - keep the reusable smoke script small and focused

   Not acceptable:

   - turning this into a workspace migration or release-system rewrite

3. The documentation starts implying a different product direction than what was
   agreed.

   If that happens, reset to the locked story:

   - `simplebroker` remains SQLite-first by default
   - `simplebroker-pg` remains separate
   - `simplebroker[pg]` is only a convenience install path

## Final Definition Of Done

The plan is complete when:

- `simplebroker-pg` truthfully supports Python 3.10+
- `simplebroker[pg]` works both in local uv development and from built artifacts
- the packaging contract is enforced by CI
- the docs are accurate
- the release sequence prevents a broken publish order
