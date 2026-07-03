# Backend API Version Handshake Plan

Date: 2026-07-03

Status: proposed

Owner: SimpleBroker

Review status: drafted from the 2026-07-03 handoff and then re-reviewed in
this file. See "Self-review log" at the bottom before implementation.

## Purpose

Add a mechanical compatibility guard between the `simplebroker` core package
and the first-party backend extension packages:

- `simplebroker-pg`
- `simplebroker-redis`

These extensions are first-party components shipped as separate distributions
so the core package can stay dependency-free. They are not a promised
third-party backend SDK. The current artifacts make that easy to misread,
because the repo exposes a backend entry-point group and documents backend
plugins without a clear "first-party seam, exact-pin if external" warning.

The concrete bug is packaging compatibility:

```text
simplebroker-pg 2.5.0 declares:    simplebroker>=4.10.0
simplebroker-redis 2.7.0 declares: simplebroker>=4.10.0
```

Those floor dependencies let `pip` legally install an old extension with a
future core release. That can produce combinations that never existed in
development or CI, while the extensions import private core modules such as
`simplebroker._sql`, `simplebroker._backend_plugins`,
`simplebroker._message_insert`, `simplebroker._message_search`, and private
validators in `simplebroker.db`.

This plan adds one integer handshake:

```text
simplebroker core declares BACKEND_API_VERSION = 1
each backend plugin declares backend_api_version = 1
core refuses to load a backend plugin unless the values are exactly equal
```

It also fixes the other direction, "new extension + old core", by requiring
extension dependency floors and release-tooling checks to move together with
backend API bumps.

## Non-goals

- No backend API ranges, `min_version`, `max_version`, or semver carve-out.
- No deprecation policy for the backend seam.
- No third-party SDK promise.
- No narrowing of the wide backend protocol.
- No generalized test harness for arbitrary third-party backends.
- No new runtime dependency.
- No change to stored data schema versions.
- No version-number release bump in this implementation PR. Release version
  bumps happen through the existing release process.

YAGNI rule: if a change starts looking like a plugin framework, stop. This PR
is a compatibility tripwire and documentation correction.

## Repository primer

SimpleBroker has one dependency-free core package plus two sibling extension
packages.

Important files:

| File | Why it matters |
|---|---|
| `simplebroker/_backend_plugins.py` | Backend protocol, entry-point loading, capability validation. This is the main implementation file. |
| `simplebroker/_backends/sqlite/plugin.py` | Built-in SQLite plugin. It must also declare `backend_api_version`; do not special-case it away. |
| `extensions/simplebroker_pg/simplebroker_pg/plugin.py` | Postgres plugin object, loaded via the `simplebroker.backends` entry point. |
| `extensions/simplebroker_redis/simplebroker_redis/plugin.py` | Redis/Valkey plugin object, loaded via the `simplebroker.backends` entry point. |
| `extensions/simplebroker_pg/pyproject.toml` | PG extension dependency floor on core. |
| `extensions/simplebroker_redis/pyproject.toml` | Redis extension dependency floor on core. |
| `bin/release.py` | Release helper. It already synchronizes root extras; extend that pattern for backend API version checks. |
| `tests/test_backend_plugin_resolution.py` | Existing tests for entry-point resolution and plugin capability validation. Most new core tests belong here. |
| `tests/test_release_script.py` | Existing tests for release helper mechanics. New release guard tests belong here. |
| `README.md` | Current "Advanced: External Backend Plugins" section oversells the seam. Revise it. |
| `simplebroker/ext.py` | Public extension surface docstring. Add one intent line and backend API warning. |
| `CHANGELOG.md` | Add `[Unreleased]` entries. |

Current backend shapes:

```text
SQL-runner-shaped backend
  plugin.sql is a BackendSQLNamespace
  plugin.create_runner(...) returns an SQLRunner
  core BrokerCore owns queue behavior
  reference: simplebroker-pg

Direct-core backend
  plugin.sql is None
  plugin.is_direct_backend is True
  plugin.create_core(...) returns a BrokerConnection implementation
  backend owns queue behavior
  reference: simplebroker-redis
```

Capability validation is currently in `simplebroker/_backend_plugins.py`:

```text
_ensure_backend_plugin_capabilities(plugin)
  if plugin.sql is not None:
      ensure_backend_sql_namespace(plugin.sql)
      return
  if not plugin.is_direct_backend:
      raise
  if plugin.create_core is not callable:
      raise
```

The new backend API validation must run before that shape validation.

## Engineering rules

- Use red-green TDD for behavior changes. Write the failing tests first.
- Prefer real plugin resolution tests over mocks of internals. It is fine to
  monkeypatch `importlib.metadata.entry_points`; that is the external discovery
  boundary.
- Do not mock databases, queues, or backend plugins when a tiny real plugin
  double is clearer. A simple class with the required attributes is better than
  a `Mock`.
- Keep the diff boring and local. This is a compatibility guard, not an
  architecture rewrite.
- Keep runtime dependencies at zero for `simplebroker`.
- Python floor is 3.11. Do not use 3.12-only syntax.
- Production code in `simplebroker/` and `bin/` must be fully typed.
- Use `Final[int]` for constants.
- Keep plugin test doubles DRY. If several tests need a valid dummy plugin,
  make one local helper or base class in the test file.
- Do not import `BACKEND_API_VERSION` into extension plugin modules to assign
  the plugin value. The extension value must be a literal or extension-local
  constant, otherwise old extensions silently match any installed core.

Commands use `uv`:

```bash
uv run pytest tests/test_backend_plugin_resolution.py -n0 -v
uv run pytest tests/test_release_script.py -n0 -v
uv run pytest tests/test_backend_plugin_resolution.py tests/test_release_script.py
uv run ruff check simplebroker tests bin extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis
uv run ruff format --check simplebroker tests bin extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis
uv run mypy simplebroker bin/release.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
```

Docker-dependent gates, if Docker is available:

```bash
uv run ./bin/pytest-pg --fast
uv run ./bin/pytest-redis --fast
```

If Docker is unavailable, report that honestly. Do not claim those gates passed.

## Target behavior

Successful load:

```text
core BACKEND_API_VERSION = 1
plugin backend_api_version = 1
plugin shape is valid
get_backend_plugin(name) returns plugin
```

Mismatch:

```text
simplebroker-pg was built against backend API v1 but simplebroker 4.11.0
provides backend API v2. Upgrade simplebroker-pg, or pin simplebroker to a
compatible release.
```

The exact wording can differ, but it must include:

- backend/plugin name
- plugin package name when known or inferable
- plugin backend API version
- core backend API version
- installed core version
- remedy: upgrade extension or pin core

Missing or non-integer plugin field:

```text
Backend plugin 'dummy' must declare integer backend_api_version.
```

Entry-point import failure:

```text
Backend plugin 'postgres' could not be loaded under simplebroker 4.11.0
backend API v2: <original error>. Upgrade the backend extension, or pin
simplebroker to a compatible release.
```

This wrapper matters because `entry_point.load()` imports the extension module
before core can inspect `backend_api_version`. If an old extension imports a
private core module that no longer exists, the friendly mismatch error cannot
fire unless load failures get actionable context.

## Data flow

```text
get_backend_plugin(name)
  |
  +-- name == "sqlite"
  |     |
  |     +-- import sqlite_backend_plugin
  |     +-- _ensure_backend_plugin_capabilities(sqlite_backend_plugin)
  |           |
  |           +-- _ensure_backend_api_version(plugin)
  |           +-- validate SQL namespace or direct-core shape
  |
  +-- name != "sqlite"
        |
        +-- metadata.entry_points().select(group="simplebroker.backends", name=name)
        +-- entry_point.load()
        +-- loaded() if callable else loaded
        +-- plugin.name must equal name
        +-- _ensure_backend_plugin_capabilities(plugin)
              |
              +-- _ensure_backend_api_version(plugin)
              +-- validate SQL namespace or direct-core shape
```

Do not add a second validation path. The handshake belongs in the existing
capability validation path so every resolver route gets it.

## Bite-sized tasks

### Task 0: Confirm current anchors and make a tiny implementation branch

Files to inspect only:

- `simplebroker/_backend_plugins.py`
- `simplebroker/_backends/sqlite/plugin.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `bin/release.py`
- `tests/test_backend_plugin_resolution.py`
- `tests/test_release_script.py`
- `README.md`
- `CHANGELOG.md`

Checklist:

- [ ] Confirm the code is rooted at `simplebroker/`, not `src/simplebroker/`.
- [ ] Confirm `BACKEND_ENTRY_POINT_GROUP = "simplebroker.backends"` still exists.
- [ ] Confirm `_ensure_backend_plugin_capabilities()` is still the shared validation path.
- [ ] Confirm PG and Redis plugin classes still sit in `plugin.py`.
- [ ] Confirm extension pyprojects still declare `simplebroker>=...`.
- [ ] Confirm the README still has "Advanced: External Backend Plugins".
- [ ] Run `git status --short` and protect unrelated work.

Expected current dirty state at plan-writing time:

```text
 M tests/test_sqlite_setup_contention.py
```

That file is unrelated. Do not edit or revert it unless a later user explicitly
asks.

### Task 1: Add red tests for backend API validation

Primary file:

- `tests/test_backend_plugin_resolution.py`

Test-design note: do not over-mock. Follow the existing entry-point tests in
this file. Use small classes and a tiny fake entry-point collection. It is
acceptable to monkeypatch `simplebroker._backend_plugins.metadata.entry_points`,
because entry-point discovery is the boundary under test.

Add or refactor a helper in the test file:

```python
class EntryPointsMock(list[EntryPoint]):
    def select(self, *, group: str, name: str):
        if group == "simplebroker.backends" and name == "dummy":
            return self
        return EntryPointsMock()
```

If the file already has repeated local copies of this helper, DRY them up as
part of this task. Keep the refactor mechanical and in the same test file only.

Write these failing tests before production code:

- [ ] `test_builtin_sqlite_backend_plugin_declares_backend_api_version`
  - Calls `get_backend_plugin("sqlite")`.
  - Asserts `plugin.backend_api_version == BACKEND_API_VERSION`.
  - Imports `BACKEND_API_VERSION` from `simplebroker._backend_plugins`.

- [ ] `test_external_backend_plugin_with_matching_backend_api_version_loads`
  - Existing dummy plugin test should be updated to declare
    `backend_api_version = BACKEND_API_VERSION`.
  - It should still load and assert `plugin.name == "dummy"`.

- [ ] `test_external_backend_plugin_missing_backend_api_version_is_rejected`
  - Dummy plugin has `name`, valid `sql`, and `schema_version`, but no
    `backend_api_version`.
  - `get_backend_plugin("dummy")` raises `RuntimeError`.
  - Assert message includes `dummy` and `backend_api_version`.

- [ ] `test_external_backend_plugin_with_stale_backend_api_version_is_rejected`
  - Dummy plugin sets `backend_api_version = BACKEND_API_VERSION - 1`.
  - Only run this if `BACKEND_API_VERSION > 1`; otherwise set the plugin value
    to `BACKEND_API_VERSION + 1` and name the test "mismatched".
  - Assert message includes both versions and the upgrade/pin remedy.

- [ ] `test_external_backend_plugin_with_future_backend_api_version_is_rejected`
  - Dummy plugin sets `backend_api_version = BACKEND_API_VERSION + 1`.
  - Assert message includes both versions and the upgrade/pin remedy.

- [ ] `test_external_backend_plugin_non_integer_backend_api_version_is_rejected`
  - Dummy plugin sets `backend_api_version = "1"`.
  - Assert message says it must be an integer.

- [ ] `test_entry_point_load_failure_gets_actionable_context`
  - Use a tiny fake entry point object whose `load()` raises
    `ImportError("cannot import name 'BackendPlugin'")`.
  - `get_backend_plugin("dummy")` raises `RuntimeError`.
  - Assert message includes `dummy`, `simplebroker`, `backend API`, original
    error text, and remedy.

Red run:

```bash
uv run pytest tests/test_backend_plugin_resolution.py -n0 -v
```

Expected failure: missing `BACKEND_API_VERSION`, missing plugin attributes, or
no actionable load wrapper. If these tests pass before implementation, the tests
are not proving the new behavior. Stop and fix them.

### Task 2: Implement core backend API validation

Primary file:

- `simplebroker/_backend_plugins.py`

Secondary file:

- `simplebroker/_backends/sqlite/plugin.py`

Implementation steps:

- [ ] Import `Final` from `typing` if not already imported.
- [ ] Add `BACKEND_API_VERSION: Final[int] = 1` near
  `BACKEND_ENTRY_POINT_GROUP`.
- [ ] Add `backend_api_version: int` to `BackendPlugin` directly beside
  `schema_version`.
- [ ] Update the `BackendPlugin` docstring with the distinction:
  - `schema_version` versions stored data.
  - `backend_api_version` versions the Python seam between core and backend.
- [ ] Add helper `_plugin_display_name(plugin: object) -> str` if it keeps error
  messages DRY.
- [ ] Add helper `_ensure_backend_api_version(plugin: object) -> None`.
- [ ] Call `_ensure_backend_api_version(plugin)` as the first line of
  `_ensure_backend_plugin_capabilities()`.
- [ ] Add `backend_api_version = 1` to `SQLiteBackendPlugin`.
- [ ] Add `"BACKEND_API_VERSION"` to `__all__` in `_backend_plugins.py`.

Do not:

- Do not put version ranges in the protocol.
- Do not make `backend_api_version` optional.
- Do not catch broad exceptions around shape validation. Only entry-point
  loading needs wrapping.
- Do not special-case SQLite out of the handshake.

Suggested helper shape:

```python
def _ensure_backend_api_version(plugin: object) -> None:
    plugin_name = getattr(plugin, "name", "<unknown>")
    plugin_version = getattr(plugin, "backend_api_version", None)
    if not isinstance(plugin_version, int):
        raise RuntimeError(
            f"Backend plugin '{plugin_name}' must declare integer "
            "backend_api_version"
        )
    if plugin_version != BACKEND_API_VERSION:
        raise RuntimeError(
            f"Backend plugin '{plugin_name}' was built against backend API "
            f"v{plugin_version} but simplebroker {__version__} provides "
            f"backend API v{BACKEND_API_VERSION}; upgrade the backend "
            "extension, or pin simplebroker to a compatible release"
        )
```

Need `__version__`? Import from `simplebroker._constants`. If importing it
creates a cycle, avoid the import and omit the core package version from this
helper, then include it in the entry-point wrapper where practical. Prefer no
cycle over a prettier message.

Run:

```bash
uv run pytest tests/test_backend_plugin_resolution.py -n0 -v
```

Expected: Task 1 tests pass except tests that require PG/Redis literals, which
come later if they were added in the same file.

### Task 3: Wrap entry-point load failures with actionable context

Primary file:

- `simplebroker/_backend_plugins.py`

Current risk:

```python
loaded = entry_point.load()
plugin = loaded() if callable(loaded) else loaded
```

This imports extension module code before the plugin object exists. PG currently
imports private core modules at top level. If a future core removes one, an old
extension can fail with a raw `ImportError` before the handshake can run.

Implementation steps:

- [ ] Wrap `entry_point.load()` and plugin construction in `try/except Exception`.
- [ ] Re-raise `RuntimeError` with actionable context.
- [ ] Chain the original exception with `from exc`.
- [ ] Include the backend entry-point name and original exception text.
- [ ] Do not wrap the deliberate validation errors after plugin construction.
  Missing/mismatched `backend_api_version` should keep their specific messages.

Suggested structure:

```python
try:
    loaded = entry_point.load()
    plugin = loaded() if callable(loaded) else loaded
except Exception as exc:
    raise RuntimeError(
        f"Backend plugin '{name}' could not be loaded under simplebroker "
        f"backend API v{BACKEND_API_VERSION}: {exc}. Upgrade the backend "
        "extension, or pin simplebroker to a compatible release."
    ) from exc
```

Counterargument: catching `Exception` is usually too broad. Here it is
defensible because this is a package-boundary load path and the user cannot fix
an extension import traceback. The original exception is still chained for
debugging. Do not catch `BaseException`.

Run:

```bash
uv run pytest tests/test_backend_plugin_resolution.py -n0 -v
```

### Task 4: Add literal backend API declarations to first-party extensions

Primary files:

- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`

Implementation steps:

- [ ] Add `backend_api_version = 1` beside `schema_version`.
- [ ] Keep it a literal integer or extension-local constant.
- [ ] Do not write `from simplebroker._backend_plugins import BACKEND_API_VERSION`
  and do not assign `backend_api_version = BACKEND_API_VERSION`.

Why literal matters:

```text
old extension loaded with future core
  extension imports current core BACKEND_API_VERSION
  extension assigns backend_api_version to that current value
  check always matches
  guard is useless
```

Tests:

- [ ] Add tests in `tests/test_backend_plugin_resolution.py` or extension test
  files that instantiate `PostgresBackendPlugin` and `RedisBackendPlugin` and
  assert their values equal core `BACKEND_API_VERSION`.
- [ ] Add a source-text guard only if it stays simple:
  - Read the plugin file.
  - Assert it contains `backend_api_version = 1`.
  - Assert it does not contain `backend_api_version = BACKEND_API_VERSION`.

The source-text guard is a bit blunt, but it protects the important invariant.
Do not build an AST framework for this.

Run:

```bash
uv run pytest tests/test_backend_plugin_resolution.py -n0 -v
uv run pytest extensions/simplebroker_pg/tests/test_pg_init_backend.py extensions/simplebroker_redis/tests/test_redis_integration.py -n0 -v
```

If Redis integration tests need a live Redis server, skip that second command
and rely on the full `bin/pytest-redis` gate later. Do not fake Redis just to
check a class attribute.

### Task 5: Update test plugin doubles across the repo

Primary search:

```bash
rg -n "schema_version\\s*=" tests simplebroker extensions -g '*.py'
```

Expected current locations include:

- `tests/test_backend_plugin_resolution.py`
- `tests/test_process_broker_session.py`
- `tests/test_activity_waiter_api.py`
- `tests/test_timestamp_edge_cases.py`
- `tests/test_timestamp_resilience.py`
- fake plugin classes inside `tests/test_runner_error_handling.py`
- extension tests that define local plugin doubles

Implementation steps:

- [ ] For each fake plugin that flows through `_ensure_backend_plugin_capabilities()`,
  add `backend_api_version = BACKEND_API_VERSION`.
- [ ] Prefer importing `BACKEND_API_VERSION` in tests over hard-coding `1`, except
  for the deliberate source-text guard in extension plugin files.
- [ ] Do not add the attribute to objects that are not backend plugins and never
  hit the validation path.
- [ ] If a fake plugin deliberately tests missing attributes, leave it missing
  only when the test is specifically about missing `backend_api_version`.

Run:

```bash
uv run pytest tests/test_backend_plugin_resolution.py tests/test_process_broker_session.py tests/test_activity_waiter_api.py tests/test_timestamp_edge_cases.py tests/test_timestamp_resilience.py tests/test_runner_error_handling.py
```

The purpose is to catch local test doubles broken by the new required protocol.

### Task 6: Add release-helper backend API guards

Primary files:

- `bin/release.py`
- `tests/test_release_script.py`

Why this task exists:

The load-time handshake catches "future core + old extension" at runtime. It
does not protect "new extension + old core" because old core does not know about
`backend_api_version`. Extension dependency floors must move with backend API
requirements.

Implement the smallest mechanical release guard. Do not create a general package
metadata framework.

Implementation plan:

- [ ] Add paths:
  - `BACKEND_PLUGINS_PATH = PROJECT_ROOT / "simplebroker" / "_backend_plugins.py"`
  - `PG_PLUGIN_PATH = PG_EXTENSION_DIR / "simplebroker_pg" / "plugin.py"`
  - `REDIS_PLUGIN_PATH = REDIS_EXTENSION_DIR / "simplebroker_redis" / "plugin.py"`
- [ ] Add regex:
  - `BACKEND_API_VERSION_PATTERN` matching `BACKEND_API_VERSION: Final[int] = N`
    and `backend_api_version = N`.
  - `PG_CORE_DEPENDENCY_PATTERN` matching extension pyproject
    `"simplebroker>=X.Y.Z"`.
  - `REDIS_CORE_DEPENDENCY_PATTERN` matching extension pyproject
    `"simplebroker>=X.Y.Z"`.
- [ ] Add small readers:
  - `read_core_backend_api_version(...) -> int`
  - `read_plugin_backend_api_version(path: Path, label: str) -> int`
  - `read_extension_core_floor(path: Path, pattern: re.Pattern[str], label: str) -> str`
- [ ] Add `require_backend_api_versions_match() -> None`.
- [ ] Add `require_extension_core_floors_for_backend_api() -> None`.
- [ ] Call both from release precheck paths for any release involving core, PG,
  Redis, or `all`.

Keep floor policy explicit and boring:

```text
For the initial API v1 handshake, PG and Redis extension floors must be at
least the first core version that contains the handshake.
```

Because this plan does not bump versions, implementation needs a stable way to
know that floor. Use the current root package version from `pyproject.toml`
only if this PR is going out in the next release with version files bumped
before release. Better: define a tiny mapping in `bin/release.py`:

```python
BACKEND_API_MIN_CORE_VERSION: Final[dict[int, str]] = {1: "4.11.0"}
```

Replace `4.11.0` with the actual next core version if the maintainer has
already chosen it. If not chosen, use a clear placeholder in the plan review
before implementation starts. Do not guess silently in code.

If the maintainer does not want a hard-coded version mapping yet, use this
fallback:

```text
Release helper checks only that extension backend_api_version equals core.
The actual extension floor bump is done in the release PR and verified by
tests added at that time.
```

Recommendation: use the mapping. It is explicit, cheap, and prevents social
coordination from being the release guard.

Tests in `tests/test_release_script.py`:

- [ ] `test_read_core_backend_api_version_reads_final_int`
- [ ] `test_read_plugin_backend_api_version_reads_literal_assignment`
- [ ] `test_backend_api_version_guard_accepts_matching_versions`
- [ ] `test_backend_api_version_guard_rejects_pg_mismatch`
- [ ] `test_backend_api_version_guard_rejects_redis_mismatch`
- [ ] `test_extension_core_floor_guard_accepts_required_floor`
- [ ] `test_extension_core_floor_guard_rejects_too_low_floor`

Use `tmp_path` files for reader tests. Do not patch the real repo files in
tests. Existing release tests already follow this pattern.

Run red first:

```bash
uv run pytest tests/test_release_script.py -n0 -v
```

Then implement and rerun.

### Task 7: Update extension dependency floors if needed

Primary files:

- `extensions/simplebroker_pg/pyproject.toml`
- `extensions/simplebroker_redis/pyproject.toml`

Decision:

- If the next release version is known, update both floors to that version.
- If not known, leave floors unchanged in this PR but make the release-helper
  mapping and tests fail until the version is chosen. Do not ship a passing
  release guard that lets stale floors through.

Example if next core is `4.11.0`:

```toml
dependencies = [
    "simplebroker>=4.11.0",
    "psycopg[binary]>=3",
    "psycopg-pool>=3.1",
]
```

and:

```toml
dependencies = [
    "simplebroker>=4.11.0",
    "redis>=5",
]
```

Do not update package `version = ...` fields here unless the release process
requires it in the same branch. This plan is not a release commit.

### Task 8: Revise README to stop implying a third-party SDK

Primary file:

- `README.md`

Current section:

- `<summary>Advanced: External Backend Plugins</summary>`

Problem:

The current README says "If you need a different backend, use an external plugin
package through `simplebroker.ext`." That reads like a supported third-party SDK.
The actual intent is different: first-party extensions are shipped separately for
dependency isolation; external backends are possible but unsupported and must
exact-pin.

Implementation steps:

- [ ] Rename the section summary to something like:
  - `Advanced: First-Party Backend Extensions`
- [ ] Replace the first paragraphs with the intent:

```text
The extension packages (`simplebroker-pg`, `simplebroker-redis`) are
first-party components shipped separately so the core package stays
dependency-free. The backend seam is an internal architecture boundary guarded
by `BACKEND_API_VERSION`, not a stable third-party SDK.
```

- [ ] Keep useful end-user install examples.
- [ ] Keep the two backend-shape explanation.
- [ ] Add a "Backend authors" subsection that says:
  - Backend authors currently need private modules listed in `simplebroker.ext`.
  - Private modules may change in any release.
  - `backend_api_version` is bumped on incompatible seam changes.
  - External authors must pin exact `simplebroker` versions and re-run tests.
  - The shared test suite is a behavioral reference, not a turnkey certification
    kit for arbitrary backends.
- [ ] Remove or rewrite any sentence that suggests `simplebroker.ext` alone is
  sufficient for backend authors.

Conformance wording:

```text
The shared suite defines expected behavior for first-party backends. The
repo-local wrappers `bin/pytest-pg` and `bin/pytest-redis` run that suite
against the sibling extension packages. Experimental external backend authors
can reuse the shared tests, but must provide their own target setup, cleanup,
and pytest fixture wiring.
```

Do not add a long SDK tutorial. One explicit section is enough.

### Task 9: Update `simplebroker.ext` intent documentation

Primary file:

- `simplebroker/ext.py`

Current docstring already warns that backend authors need private modules. Keep
that. Add the missing intent sentence and handshake note.

Add near the "Authoring a full alternative backend" paragraph:

```text
The first-party extension packages are shipped separately so the core package
can stay dependency-free. Their backend seam is guarded by
`BACKEND_API_VERSION`; it is not a stable third-party SDK.
```

Do not re-export `BACKEND_API_VERSION` from `simplebroker.ext` unless a test or
maintainer explicitly requires it. The backend API version is an internal
release guard, not an embedding API.

Update `tests/test_ext_imports.py` only if you intentionally change `ext.__all__`.
The recommended plan does not.

### Task 10: Update CHANGELOG

Primary file:

- `CHANGELOG.md`

Add under `[Unreleased]`.

Suggested entries:

```markdown
### Added
- Added a backend API version handshake between `simplebroker` core and
  first-party backend extension packages. Backend plugins must now declare
  `backend_api_version`, and core rejects mismatches at load time with an
  upgrade-or-pin diagnostic.

### Changed
- Release tooling now verifies first-party backend extension API versions match
  the core backend API version before release.

### Documented
- Clarified that `simplebroker-pg` and `simplebroker-redis` are first-party
  extension packages shipped separately for dependency isolation; the backend
  seam is guarded by `BACKEND_API_VERSION`, not promised as a stable third-party
  SDK.
```

If matched-version installs behave the same, do not claim a user-visible
behavior change for normal installs. The behavior change is for incompatible
package combinations.

### Task 11: Full test and quality gates

Run targeted gates first:

```bash
uv run pytest tests/test_backend_plugin_resolution.py -n0 -v
uv run pytest tests/test_release_script.py -n0 -v
uv run pytest tests/test_backend_plugin_resolution.py tests/test_release_script.py
uv run pytest tests/test_process_broker_session.py tests/test_activity_waiter_api.py tests/test_timestamp_edge_cases.py tests/test_timestamp_resilience.py tests/test_runner_error_handling.py
```

Run static gates:

```bash
uv run ruff check simplebroker tests bin extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis
uv run ruff format --check simplebroker tests bin extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis
uv run mypy simplebroker bin/release.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
```

Run package/backends if available:

```bash
uv run pytest
uv run ./bin/pytest-pg --fast
uv run ./bin/pytest-redis --fast
```

If `uv run pytest` is too slow locally, at minimum run:

```bash
uv run pytest tests/test_backend_plugin_resolution.py tests/test_release_script.py tests/test_ext_imports.py tests/test_dev_scripts.py
```

But do not mark the PR ready until the full default suite and Docker backend
fast suites have passed somewhere.

## Invariants checklist

The implementing engineer must be able to answer "yes" to every item:

- [ ] `BACKEND_API_VERSION` exists in `simplebroker/_backend_plugins.py`.
- [ ] `BackendPlugin` requires `backend_api_version: int`.
- [ ] SQLite plugin declares `backend_api_version`.
- [ ] PG plugin declares a literal `backend_api_version`.
- [ ] Redis plugin declares a literal `backend_api_version`.
- [ ] Missing backend API version fails before SQL namespace validation.
- [ ] Mismatched backend API version fails before any backend connection opens.
- [ ] Entry-point import failure is wrapped with upgrade/pin context.
- [ ] Release helper rejects first-party backend API mismatches.
- [ ] Release helper rejects extension core floors below the API minimum, unless
  the maintainer explicitly chooses the weaker release-only guard and documents
  why in this plan.
- [ ] README no longer presents backend plugins as a supported external SDK.
- [ ] `ext.py` states first-party dependency-isolation intent.
- [ ] CHANGELOG documents the handshake and docs correction.
- [ ] No new runtime dependencies.
- [ ] No version ranges.
- [ ] No generic third-party backend harness.

## Rollback plan

If the handshake breaks normal matched installs:

1. Revert only the validation call inside `_ensure_backend_plugin_capabilities()`.
2. Keep docs corrections if they are accurate.
3. Keep plugin `backend_api_version` declarations if harmless.
4. Open a follow-up with the failing package combination and traceback.

Do not replace strict equality with ranges as a quick rollback. That changes the
design, not just the failure mode.

## Self-review log

Round 1 findings while drafting:

- The original handoff implied the shared conformance suite could become a
  near-zero-code certification kit for arbitrary backends. That is too broad for
  this feature. Current fixtures special-case `sqlite`, `postgres`, and `redis`.
  This plan documents the shared suite as a behavioral reference and keeps a
  generic harness out of scope.
- The first version of the release-tooling task only checked matching plugin
  values. That missed "new extension + old core". This plan adds dependency
  floor checks or a deliberate blocking decision if the next core version is not
  known.
- The initial error-message requirement wanted package names for all plugins.
  Core only reliably knows the entry-point name at load time. This plan requires
  backend name always and package name when known or inferable, avoiding fragile
  metadata lookup.

Round 2 findings to check after implementation:

- If importing `__version__` into `_backend_plugins.py` creates a cycle, do not
  force it. Keep the validation simple and put version context in the
  entry-point wrapper where possible.
- If release-helper floor checks need the next core version and it is not chosen,
  do not guess silently. Either set the explicit mapping before merge or leave
  the guard failing until release planning supplies the version.
- If source-text tests for literal plugin assignment become brittle, prefer a
  smaller assertion over deleting the invariant. The invariant is real: importing
  the core constant in extension plugin modules defeats the handshake.
