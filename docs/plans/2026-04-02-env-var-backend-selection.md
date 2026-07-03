# Environment Variable Backend Selection for SimpleBroker

**Status**: Draft specification
**Date**: 2026-04-02

---

## 1. Context

SimpleBroker supports pluggable backends (SQLite, Postgres). Today, backend selection
requires a `.broker.toml` project config file. For containerized deployments (Docker,
systemd services), environment variables are the canonical control surface. Operators need
to select and configure the Postgres backend without writing config files, and without
putting passwords in files that might be checked into version control.

This plan adds `BROKER_BACKEND` and `BROKER_BACKEND_*` environment variables to SimpleBroker's
existing configuration system, following the same patterns used by all other `BROKER_*` vars.

---

## 2. Design

### 2.1 New environment variables

| Variable | Default | Purpose |
|---|---|---|
| `BROKER_BACKEND` | `"sqlite"` | Backend name. `"sqlite"` or `"postgres"`. |
| `BROKER_BACKEND_HOST` | `"localhost"` | Postgres host |
| `BROKER_BACKEND_PORT` | `5432` | Postgres port |
| `BROKER_BACKEND_USER` | `"postgres"` | Postgres user |
| `BROKER_BACKEND_PASSWORD` | *(none)* | Postgres password. **Never written to toml.** |
| `BROKER_BACKEND_DATABASE` | `"simplebroker"` | Postgres database name |
| `BROKER_BACKEND_SCHEMA` | `"simplebroker_pg_v1"` | Postgres schema name |
| `BROKER_BACKEND_TARGET` | *(none)* | Full DSN. If set, overrides HOST/PORT/USER/PASSWORD/DATABASE. |

### 2.2 Resolution precedence

There are **two separate precedence decisions** in this plan:

**Backend selection**

From lowest to highest priority:

1. **Hard-coded default** — `"sqlite"`
2. **`BROKER_BACKEND` environment variable**
3. **`.broker.toml` `backend` field** (if a project config is found)

Project config remains authoritative when present. This plan does **not** add a
`--backend` CLI flag, so there is no backend-selecting CLI override yet.

**Postgres connection parameter resolution**

Once the backend is `"postgres"`:

1. **`BROKER_BACKEND_TARGET`** — if set, overrides the entire target string
2. **Toml `target`** — if a `.broker.toml` exists and `BROKER_BACKEND_TARGET` is unset
3. **Construct from parts** — only when there is no target from env or toml:
   `BROKER_BACKEND_HOST/PORT/USER/PASSWORD/DATABASE`
4. **Schema resolution**:
   - explicit `BROKER_BACKEND_SCHEMA` in `os.environ`
   - else toml `[backend_options].schema`
   - else default `"simplebroker_pg_v1"`

Individual `BROKER_BACKEND_HOST/PORT/USER/PASSWORD/DATABASE` vars do **not** rewrite an
existing toml `target` in this plan. Supporting that would require parsing and merging an
arbitrary DSN/conninfo string, which is unnecessary scope expansion for the initial feature.

SimpleBroker will never **generate** a toml target containing a password. Existing manually
authored toml files that already contain a password remain supported, but are discouraged and
are not rewritten or scrubbed by this change.

### 2.3 Backend validation

When `BROKER_BACKEND` selects a non-sqlite backend:
- `"sqlite"` — accepted unconditionally (built-in)
- `"postgres"` — resolve via `get_backend_plugin("postgres")`
  - if the failure is specifically `RuntimeError("Unknown backend plugin: postgres")`,
    raise `"Requested backend 'postgres' is not available. Install simplebroker-pg."`
  - if the plugin exists but is broken (bad entry point, invalid SQL namespace, mismatched
    plugin name), let the real exception bubble so the operator sees the actual defect
- Anything else — raise:
  `"Requested backend '<name>' is not available."`

SimpleBroker core must not directly import `simplebroker_pg`; all validation continues to go
through the entry-point resolver.

### 2.4 DSN construction

When `BROKER_BACKEND=postgres` and there is no target from env or toml, construct the DSN:

```
postgresql://{user}:{password}@{host}:{port}/{database}
```

The constructed URL must percent-encode `user`, `password`, and `database`
(for example with `urllib.parse.quote(..., safe="")`) so reserved characters in credentials
do not produce an invalid DSN.

If `BROKER_BACKEND_PASSWORD` is empty/unset, construct without the password portion:

```
postgresql://{user}@{host}:{port}/{database}
```

This remains compatible with `PGPASSWORD`, `.pgpass`, or other libpq auth mechanisms because
psycopg can still source credentials outside the DSN.

When `BROKER_BACKEND_TARGET` **is** set, use it verbatim as the target string. When a toml
`target` is used, also pass it through verbatim. In both cases the target may be a URL or any
other libpq conninfo string that psycopg accepts; this plan does **not** parse or normalize it.

The Postgres **database** itself must already exist. This plan only selects a database
connection target and, during `broker init`, initializes the managed **schema** inside that
database. It does **not** attempt to create the database.

### 2.5 Where this lives in the code

The `init_backend()` function is **exported by each backend plugin**, not by SimpleBroker
core. SimpleBroker core is responsible for:

1. Reading `BROKER_BACKEND` in `load_config()` (in `_constants.py`)
2. Validating the backend name and that the plugin is importable
3. Calling `plugin.init_backend(config)` to let the plugin resolve its own connection
   parameters from the config dict

The Postgres plugin's `init_backend()`:
1. Reads `BROKER_BACKEND_*` vars from the config dict
2. Merges schema selection with toml `backend_options`
3. Uses `BROKER_BACKEND_TARGET` if present, else toml `target` if present, else constructs
   a DSN from individual env vars
4. Returns `{"target": dsn, "backend_options": {"schema": schema}}`

This keeps backend-specific env var knowledge in the backend plugin, not in core.

**Important code-path note**: the public helpers in [`simplebroker/project.py`](/Users/van/Developer/simplebroker/simplebroker/project.py)
and the real CLI resolver in [`simplebroker/cli.py`](/Users/van/Developer/simplebroker/simplebroker/cli.py)
both need updates. Changing only `project.py` would not make the CLI honor `BROKER_BACKEND`.

### 2.6 What the toml file stores

For Postgres, the toml stores non-secret parameters:

```toml
version = 1
backend = "postgres"
target = "postgresql://postgres@localhost:5432/simplebroker"

[backend_options]
schema = "simplebroker_pg_v1"
```

The recommended `target` in the toml is the DSN **without the password**. If
`BROKER_BACKEND_TARGET` is set, it overrides the toml's `target` entirely. If the operator
manually authored a toml target that already includes a password, this plan leaves it alone;
it does not attempt to parse and scrub existing config files.

The toml is **optional**. If `BROKER_BACKEND=postgres` is set via env and no toml exists,
the system constructs everything from env vars and defaults. No toml is required.

Existing SQLite path knobs stay SQLite-specific:
- `-f/--file`
- `BROKER_DEFAULT_DB_NAME`
- `BROKER_DEFAULT_DB_LOCATION`

For non-sqlite backends selected via env, those settings do not participate in connection
construction. `--dir` still matters as the starting directory for project-config lookup and
as the logical project root recorded on the returned `ResolvedTarget`.

---

## 3. Implementation Tasks

Each task follows **red-green TDD**: write the failing test first, then write the minimum
code to make it pass.

### Task 1: Add `BROKER_BACKEND` to `load_config()`

**File**: `simplebroker/_constants.py`

**What to do**:

Add `BROKER_BACKEND` to the config dict in `load_config()`, after the project scoping
section (around line 603). Follow the exact pattern of the existing env var loading:

```python
# Backend selection
"BROKER_BACKEND": os.environ.get("BROKER_BACKEND", "sqlite"),
```

Also add all `BROKER_BACKEND_*` vars with their defaults:

```python
"BROKER_BACKEND_HOST": os.environ.get("BROKER_BACKEND_HOST", "localhost"),
"BROKER_BACKEND_PORT": int(os.environ.get("BROKER_BACKEND_PORT", "5432")),
"BROKER_BACKEND_USER": os.environ.get("BROKER_BACKEND_USER", "postgres"),
"BROKER_BACKEND_PASSWORD": os.environ.get("BROKER_BACKEND_PASSWORD", ""),
"BROKER_BACKEND_DATABASE": os.environ.get("BROKER_BACKEND_DATABASE", "simplebroker"),
"BROKER_BACKEND_SCHEMA": os.environ.get("BROKER_BACKEND_SCHEMA", "simplebroker_pg_v1"),
"BROKER_BACKEND_TARGET": os.environ.get("BROKER_BACKEND_TARGET", ""),
```

No validation logic here — `load_config()` just reads values. Validation happens in the
plugin's `init_backend()`.

**Test file**: `tests/test_constants.py`

**Tests to write first (RED)**:

```python
def test_load_config_includes_broker_backend_default():
    """BROKER_BACKEND defaults to sqlite."""
    config = load_config()
    assert config["BROKER_BACKEND"] == "sqlite"

def test_load_config_reads_broker_backend_from_env(monkeypatch):
    """BROKER_BACKEND reads from environment."""
    monkeypatch.setenv("BROKER_BACKEND", "postgres")
    config = load_config()
    assert config["BROKER_BACKEND"] == "postgres"

def test_load_config_includes_all_backend_vars():
    """All BROKER_BACKEND_* vars have defaults."""
    config = load_config()
    assert config["BROKER_BACKEND_HOST"] == "localhost"
    assert config["BROKER_BACKEND_PORT"] == 5432
    assert config["BROKER_BACKEND_USER"] == "postgres"
    assert config["BROKER_BACKEND_PASSWORD"] == ""
    assert config["BROKER_BACKEND_DATABASE"] == "simplebroker"
    assert config["BROKER_BACKEND_SCHEMA"] == "simplebroker_pg_v1"
    assert config["BROKER_BACKEND_TARGET"] == ""
```

**Gate**: `uv run pytest tests/test_constants.py -k backend -v` — all pass.

---

### Task 2: Add `init_backend()` to the `BackendPlugin` protocol

**File**: `simplebroker/_backend_plugins.py`

**What to do**:

Add `init_backend` to the `BackendPlugin` Protocol class (around line 20). This method
receives the merged config dict and returns a dict with `target` and `backend_options`:

```python
def init_backend(
    self,
    config: Mapping[str, Any],
    *,
    toml_target: str = "",
    toml_options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve backend connection parameters from config/env.

    Args:
        config: The config dict from load_config() (contains BROKER_BACKEND_* values)
        toml_target: Target string from .broker.toml (if present)
        toml_options: backend_options dict from .broker.toml (if present)

    Returns a dict with at minimum:
        "target": str — the connection target (file path or DSN)
        "backend_options": dict — backend-specific options
    """
    ...
```

**File**: `simplebroker/_backends/sqlite/plugin.py`

Add the SQLite implementation. For SQLite, `init_backend()` is trivial — the target is
resolved later by path logic in `project.py`:

```python
def init_backend(
    self,
    config: Mapping[str, Any],
    *,
    toml_target: str = "",
    toml_options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "target": toml_target,  # resolved to absolute path by caller
        "backend_options": dict(toml_options) if toml_options else {},
    }
```

**Test file**: `tests/test_backend_plugin_resolution.py`

**Tests to write first (RED)**:

```python
def test_sqlite_plugin_has_init_backend():
    plugin = get_backend_plugin("sqlite")
    result = plugin.init_backend(load_config())
    assert "target" in result
    assert "backend_options" in result
```

**Gate**: `uv run pytest tests/test_backend_plugin_resolution.py -k init_backend -v` — passes.

---

### Task 3: Add `init_backend()` to the Postgres plugin

**Files**:
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `extensions/simplebroker_pg/simplebroker_pg/validation.py`

**What to do**:

Add two backend-specific helpers to the Postgres extension:

1. `verify_env(config, *, toml_target="", toml_options=None)`:
   - validates and normalizes the env/toml inputs before they are used
   - decides whether configuration is coming from env target, toml target, or constructed parts
   - validates the schema source
   - returns structured, validated values used by `init_backend()`
2. `init_backend(...)`:
   - calls `verify_env(...)`
   - builds the final target string from the validated values
   - returns `{"target": ..., "backend_options": {"schema": ...}}`

`verify_env()` should live in the Postgres extension, not core, because these rules are
backend-specific.

`verify_env()` is for deterministic cleanup/validation only. It should normalize env/toml
inputs, validate schema/port/precedence, and decide which target mode is in effect. It should
**not** open a network connection or try to authenticate; connectivity, wrong-password, and
"database does not exist" failures still belong to backend connection/validation code.

Clarified precedence for this method:

- `BROKER_BACKEND_TARGET` from the config dict wins over everything
- else `toml_target` wins
- else build a DSN from `BROKER_BACKEND_HOST/PORT/USER/PASSWORD/DATABASE`
- `BROKER_BACKEND_SCHEMA` overrides toml schema **only when explicitly present in**
  `os.environ`
- individual `HOST/PORT/USER/PASSWORD/DATABASE` env vars do **not** rewrite a pre-existing
  toml target

`verify_env()` must also distinguish between two error classes:

1. **Configuration unavailable / missing**
   - examples: unknown backend, missing plugin, no schema source when one is required
   - these are setup/availability errors
2. **Configuration present but invalid / unusable**
   - examples: invalid schema name, malformed DSN, bad password, authentication failure,
     database does not exist
   - these are validation/connection errors and should not reuse the "not available" wording

That distinction matters operationally. "Install simplebroker-pg" is appropriate for a missing
plugin; it is wrong when the plugin is present and the password is incorrect.

```python
@dataclass(frozen=True, slots=True)
class VerifiedPostgresEnv:
    target_mode: Literal["env_target", "toml_target", "parts"]
    host: str | None
    port: int | None
    user: str | None
    password: str | None
    database: str | None
    target: str | None
    schema: str

def verify_env(
    config: Mapping[str, Any],
    *,
    toml_target: str = "",
    toml_options: Mapping[str, Any] | None = None,
) -> VerifiedPostgresEnv:
    ...

def init_backend(
    self,
    config: Mapping[str, Any],
    *,
    toml_target: str = "",
    toml_options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    verified = verify_env(
        config,
        toml_target=toml_target,
        toml_options=toml_options,
    )
    if verified.target is not None:
        target = verified.target
    else:
        from urllib.parse import quote

        encoded_user = quote(str(verified.user), safe="")
        encoded_database = quote(str(verified.database), safe="")
        if verified.password:
            encoded_password = quote(str(verified.password), safe="")
            target = (
                f"postgresql://{encoded_user}:{encoded_password}"
                f"@{verified.host}:{verified.port}/{encoded_database}"
            )
        else:
            target = (
                f"postgresql://{encoded_user}"
                f"@{verified.host}:{verified.port}/{encoded_database}"
            )
    return {"target": target, "backend_options": {"schema": verified.schema}}
```

Note: checking `os.environ` directly (not just the config dict) is how we distinguish
"explicitly set by operator" from "loaded with default value". This is the standard
pattern for env-overrides-file precedence.

Also update `require_schema_name()` in `validation.py`. Its current error text says the
schema must come from `.broker.toml`, which becomes incorrect once env-based schema
selection exists. The message should mention both configuration paths, for example:

```python
"Postgres backend requires backend_options.schema or BROKER_BACKEND_SCHEMA"
```

**Test file**: `extensions/simplebroker_pg/tests/test_pg_init_backend.py` (new file)

**Tests to write first (RED)**:

```python
"""Tests for Postgres verify_env()/init_backend() env var resolution."""
import pytest
from simplebroker._exceptions import DatabaseError
from simplebroker_pg.plugin import PostgresBackendPlugin, verify_env

def test_init_backend_constructs_dsn_from_individual_vars():
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_HOST": "db.example.com",
        "BROKER_BACKEND_PORT": 5433,
        "BROKER_BACKEND_USER": "myuser",
        "BROKER_BACKEND_PASSWORD": "secret",
        "BROKER_BACKEND_DATABASE": "mydb",
        "BROKER_BACKEND_SCHEMA": "app_v1",
        "BROKER_BACKEND_TARGET": "",
    }
    result = plugin.init_backend(config)
    assert result["target"] == "postgresql://myuser:secret@db.example.com:5433/mydb"
    assert result["backend_options"] == {"schema": "app_v1"}

def test_init_backend_percent_encodes_reserved_characters():
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_HOST": "db.example.com",
        "BROKER_BACKEND_PORT": 5432,
        "BROKER_BACKEND_USER": "user:name",
        "BROKER_BACKEND_PASSWORD": "p@ss/w:rd",
        "BROKER_BACKEND_DATABASE": "db/name",
        "BROKER_BACKEND_SCHEMA": "app_v1",
        "BROKER_BACKEND_TARGET": "",
    }
    result = plugin.init_backend(config)
    assert result["target"] == (
        "postgresql://user%3Aname:p%40ss%2Fw%3Ard@db.example.com:5432/db%2Fname"
    )

def test_init_backend_omits_password_when_empty():
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_HOST": "localhost",
        "BROKER_BACKEND_PORT": 5432,
        "BROKER_BACKEND_USER": "postgres",
        "BROKER_BACKEND_PASSWORD": "",
        "BROKER_BACKEND_DATABASE": "simplebroker",
        "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
        "BROKER_BACKEND_TARGET": "",
    }
    result = plugin.init_backend(config)
    assert ":" not in result["target"].split("@")[0].split("//")[1]  # no password in userinfo

def test_verify_env_rejects_invalid_schema(monkeypatch):
    monkeypatch.setenv("BROKER_BACKEND_SCHEMA", "not-valid!")
    with pytest.raises(DatabaseError, match="schema"):
        verify_env(
            {
                "BROKER_BACKEND_TARGET": "postgresql://x@y/z",
                "BROKER_BACKEND_SCHEMA": "not-valid!",
            }
        )

def test_init_backend_target_overrides_individual_vars():
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_HOST": "ignored",
        "BROKER_BACKEND_PORT": 9999,
        "BROKER_BACKEND_USER": "ignored",
        "BROKER_BACKEND_PASSWORD": "ignored",
        "BROKER_BACKEND_DATABASE": "ignored",
        "BROKER_BACKEND_SCHEMA": "my_schema",
        "BROKER_BACKEND_TARGET": "postgresql://real@realhost:5432/realdb",
    }
    result = plugin.init_backend(config)
    assert result["target"] == "postgresql://real@realhost:5432/realdb"
    assert result["backend_options"] == {"schema": "my_schema"}

def test_init_backend_uses_defaults():
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_TARGET": "",
        "BROKER_BACKEND_HOST": "localhost",
        "BROKER_BACKEND_PORT": 5432,
        "BROKER_BACKEND_USER": "postgres",
        "BROKER_BACKEND_PASSWORD": "",
        "BROKER_BACKEND_DATABASE": "simplebroker",
        "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
    }
    result = plugin.init_backend(config)
    assert result["target"] == "postgresql://postgres@localhost:5432/simplebroker"
    assert result["backend_options"] == {"schema": "simplebroker_pg_v1"}
```

Also add tests for the toml merge behavior:

```python
def test_init_backend_toml_target_used_as_fallback():
    """toml_target is used when BROKER_BACKEND_TARGET env is empty."""
    plugin = PostgresBackendPlugin()
    config = {"BROKER_BACKEND_TARGET": "", "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1"}
    result = plugin.init_backend(
        config,
        toml_target="postgresql://toml@tomlhost/tomldb",
    )
    assert result["target"] == "postgresql://toml@tomlhost/tomldb"

def test_init_backend_env_target_overrides_toml_target():
    """BROKER_BACKEND_TARGET env overrides toml_target."""
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_TARGET": "postgresql://env@envhost/envdb",
        "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
    }
    result = plugin.init_backend(
        config,
        toml_target="postgresql://toml@tomlhost/tomldb",
    )
    assert result["target"] == "postgresql://env@envhost/envdb"

def test_init_backend_individual_env_parts_do_not_rewrite_toml_target(monkeypatch):
    """Granular env vars do not rewrite a pre-existing toml target."""
    monkeypatch.setenv("BROKER_BACKEND_HOST", "envhost")
    plugin = PostgresBackendPlugin()
    config = {
        "BROKER_BACKEND_TARGET": "",
        "BROKER_BACKEND_HOST": "envhost",
        "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
    }
    result = plugin.init_backend(
        config,
        toml_target="postgresql://toml@tomlhost/tomldb",
    )
    assert result["target"] == "postgresql://toml@tomlhost/tomldb"

def test_init_backend_toml_schema_preserved_when_env_not_set(monkeypatch):
    """toml schema wins when BROKER_BACKEND_SCHEMA is not in os.environ."""
    monkeypatch.delenv("BROKER_BACKEND_SCHEMA", raising=False)
    plugin = PostgresBackendPlugin()
    config = {"BROKER_BACKEND_TARGET": "postgresql://x@y/z", "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1"}
    result = plugin.init_backend(
        config,
        toml_options={"schema": "from_toml"},
    )
    assert result["backend_options"]["schema"] == "from_toml"

def test_init_backend_env_schema_overrides_toml(monkeypatch):
    """Explicit BROKER_BACKEND_SCHEMA in os.environ overrides toml."""
    monkeypatch.setenv("BROKER_BACKEND_SCHEMA", "from_env")
    plugin = PostgresBackendPlugin()
    config = {"BROKER_BACKEND_TARGET": "postgresql://x@y/z", "BROKER_BACKEND_SCHEMA": "from_env"}
    result = plugin.init_backend(
        config,
        toml_options={"schema": "from_toml"},
    )
    assert result["backend_options"]["schema"] == "from_env"
```

**Gate**: `uv run pytest extensions/simplebroker_pg/tests/test_pg_init_backend.py -v` — all pass.

---

### Task 4: Public helper resolution without project config

**File**: `simplebroker/project.py`

**What to do**:

Modify `resolve_broker_target()` and `target_for_directory()` to consult `BROKER_BACKEND`
from the config when no `.broker.toml` is found. Today these functions fall back to
SQLite unconditionally. After this change:

1. If `.broker.toml` exists, use it (no change to current behavior).
2. If no toml exists and `config["BROKER_BACKEND"]` is `"sqlite"`, use the existing SQLite
   path resolution (no change).
3. If no toml exists and `config["BROKER_BACKEND"]` is something else:
   a. Load the backend plugin via `get_backend_plugin(name)`.
   b. If the exact failure is `Unknown backend plugin: postgres`, raise:
      `"Requested backend 'postgres' is not available. Install simplebroker-pg."`.
   c. If the exact failure is `Unknown backend plugin: <other>`, raise:
      `"Requested backend '<other>' is not available."`.
   d. For any other plugin-loading failure, preserve the real exception.
   e. Call `plugin.init_backend(config)` to get `{"target": ..., "backend_options": ...}`.
   f. Return a `ResolvedTarget` with the backend name, target, and options.

The validation (is the backend importable?) happens at this point, not in `load_config()`.
`load_config()` just reads the string; `resolve_broker_target()` acts on it.

Because `resolve_broker_target()` currently means "discover an existing broker target", update
its docstring to reflect the new behavior for non-sqlite env selection: it may synthesize a
logical target from env even before initialization has happened. Existence and credentials are
validated later by backend-specific `validate_target()` / `initialize_target()`.

**Test file**: `tests/test_project_config.py` (add new tests)

**Tests to write first (RED)**:

```python
def test_resolve_target_defaults_to_sqlite_without_toml(tmp_path, monkeypatch):
    """Without toml and without BROKER_BACKEND, defaults to sqlite."""
    monkeypatch.chdir(tmp_path)
    config = load_config()
    target = resolve_broker_target(tmp_path, config=config)
    # No toml, no existing db — returns None (sqlite discovery finds nothing)
    assert target is None

def test_resolve_target_unknown_backend_raises(tmp_path, monkeypatch):
    """Unknown backend name produces clear error."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BROKER_BACKEND", "mysql")
    config = load_config()
    with pytest.raises(RuntimeError, match="Requested backend 'mysql' is not available"):
        target_for_directory(tmp_path, config=config)

def test_resolve_target_missing_postgres_plugin_has_install_hint(tmp_path, monkeypatch):
    """Missing postgres plugin gives an actionable install hint."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BROKER_BACKEND", "postgres")
    config = load_config()

    def raise_unknown(name: str):
        raise RuntimeError(f"Unknown backend plugin: {name}")

    monkeypatch.setattr("simplebroker.project.get_backend_plugin", raise_unknown)
    with pytest.raises(
        RuntimeError,
        match="Requested backend 'postgres' is not available. Install simplebroker-pg.",
    ):
        target_for_directory(tmp_path, config=config)

def test_toml_overrides_env_backend_in_public_helpers(tmp_path, monkeypatch):
    """A .broker.toml takes precedence over BROKER_BACKEND env var."""
    monkeypatch.setenv("BROKER_BACKEND", "postgres")
    toml_path = tmp_path / ".broker.toml"
    toml_path.write_text('version = 1\nbackend = "sqlite"\ntarget = "test.db"\n')
    target = target_for_directory(tmp_path, config=load_config())
    assert target.backend_name == "sqlite"
```

These stay in the core test suite because they do not require the Postgres extension to be
installed. Positive Postgres resolution tests move to the extension test suite in Tasks 5-6.

**Gate**: `uv run pytest tests/test_project_config.py -v -k backend` — all pass.

---

### Task 5: The real CLI path must honor `BROKER_BACKEND`

**File**: `simplebroker/cli.py`

**What to do**:

Update `_resolve_target()` in the CLI. This is the actual operator-facing code path; changing
only `simplebroker/project.py` would leave `python -m simplebroker.cli ...` stuck on SQLite.

Required behavior:

1. Existing project-config behavior remains unchanged:
   - only search upward for `.broker.toml` when `BROKER_PROJECT_SCOPE=1`
   - if found, use `resolve_project_target(config_path)`
2. If no project config was found and `BROKER_BACKEND == "sqlite"`, keep the existing SQLite
   resolution path exactly as-is
3. If no project config was found and `BROKER_BACKEND != "sqlite"`:
   - resolve the backend plugin through the same helper/error path used in `project.py`
   - call `plugin.init_backend(config)`
   - return a non-sqlite `ResolvedTarget`
   - record `project_root` as `Path(args.dir).expanduser().resolve()`
   - set `legacy_sqlite_path_mode=False`

No new CLI backend flags are added in this plan. Existing SQLite path knobs
(`-f/--file`, `BROKER_DEFAULT_DB_NAME`, `BROKER_DEFAULT_DB_LOCATION`) remain SQLite-only and
are not part of non-sqlite target construction.

Distinct error semantics matter here too:

- "backend not available" errors are only for missing backends/plugins
- malformed targets, invalid schemas, auth failures, bad passwords, and other connection
  problems must surface as validation/connection errors from the backend plugin
- a missing Postgres database is a connection/target error, not a plugin-availability error

**Test file**: `extensions/simplebroker_pg/tests/test_pg_integration.py`

**Tests to write first (RED)**:

```python
def test_cli_uses_env_selected_postgres_without_toml(tmp_path: Path) -> None:
    """The CLI should operate against Postgres using env vars only."""
    schema = _schema_name()
    project_root = tmp_path / "project"
    project_root.mkdir()

    env = {
        "BROKER_BACKEND": "postgres",
        "BROKER_BACKEND_TARGET": TEST_DSN,
        "BROKER_BACKEND_SCHEMA": schema,
    }

    code, stdout, stderr = _run_cli("init", cwd=project_root, env=env)
    assert code == 0, stderr

    code, stdout, stderr = _run_cli("write", "jobs", "hello", cwd=project_root, env=env)
    assert code == 0, stderr

    code, stdout, stderr = _run_cli("read", "jobs", cwd=project_root, env=env)
    assert code == 0, stderr
    assert stdout == "hello"

    code, stdout, stderr = _run_cli("--cleanup", cwd=project_root, env=env)
    assert code == 0, stderr

def test_cli_bad_password_is_not_reported_as_backend_unavailable(tmp_path: Path) -> None:
    """Auth failures should stay auth failures, not availability errors."""
    schema = _schema_name()
    project_root = tmp_path / "project"
    project_root.mkdir()

    env = {
        "BROKER_BACKEND": "postgres",
        "BROKER_BACKEND_TARGET": TEST_DSN,
        "BROKER_BACKEND_SCHEMA": schema,
        "BROKER_BACKEND_PASSWORD": "definitely-wrong",
    }

    code, stdout, stderr = _run_cli("init", cwd=project_root, env=env)
    assert code != 0
    assert "not available" not in stderr
```

Add a second integration test showing that an upward `.broker.toml` still wins when
`BROKER_PROJECT_SCOPE=1`, even if `BROKER_BACKEND=postgres` is set in the environment.

**Gate**: `uv run pytest extensions/simplebroker_pg/tests/test_pg_integration.py -v -k env_selected_postgres`
— passes.

---

### Task 6: Pass toml values through `init_backend()` in `resolve_project_target()`

**File**: `simplebroker/_project_config.py`

**What to do**:

Modify `resolve_project_target()` so that for non-sqlite backends, it delegates to
`plugin.init_backend()` with the toml values as context. The merge logic lives in the
plugin (Task 3), not here. This function just passes the toml data through.

```python
def resolve_project_target(config_path: Path) -> ResolvedTarget:
    config_data = load_project_config(config_path)
    backend_name = config_data["backend"]
    plugin = get_backend_plugin(backend_name)
    toml_target = config_data["target"]
    toml_options = dict(config_data["backend_options"])

    if backend_name == "sqlite":
        target = str((config_path.parent / toml_target).expanduser().resolve())
        return ResolvedTarget(
            backend_name=backend_name, target=target, backend_options=toml_options,
            project_root=config_path.parent, config_path=config_path,
            used_project_scope=True, legacy_sqlite_path_mode=False,
        )

    # Non-sqlite: plugin merges env vars with toml values
    from ._constants import load_config
    resolved = plugin.init_backend(
        load_config(),
        toml_target=toml_target,
        toml_options=toml_options,
    )
    return ResolvedTarget(
        backend_name=backend_name,
        target=resolved["target"],
        backend_options=resolved["backend_options"],
        project_root=config_path.parent,
        config_path=config_path,
        used_project_scope=True,
        legacy_sqlite_path_mode=False,
    )
```

This is cleaner than the original plan because the merge logic is entirely in `init_backend()`
(DRY — one place for all precedence decisions).

**Test files**:
- `extensions/simplebroker_pg/tests/test_pg_init_backend.py`
- `extensions/simplebroker_pg/tests/test_pg_integration.py`

**Tests to write first (RED)**:

```python
def test_toml_schema_preserved_when_env_not_explicitly_set(tmp_path, monkeypatch):
    """Toml schema is used when BROKER_BACKEND_SCHEMA is not in os.environ."""
    monkeypatch.delenv("BROKER_BACKEND_SCHEMA", raising=False)
    toml_path = tmp_path / ".broker.toml"
    toml_path.write_text(
        'version = 1\nbackend = "postgres"\n'
        'target = "postgresql://postgres@localhost/mydb"\n\n'
        '[backend_options]\nschema = "custom_schema"\n'
    )
    target = resolve_project_target(toml_path)
    assert target.backend_options["schema"] == "custom_schema"

def test_env_schema_overrides_toml_when_explicitly_set(tmp_path, monkeypatch):
    """BROKER_BACKEND_SCHEMA overrides toml when explicitly set."""
    monkeypatch.setenv("BROKER_BACKEND_SCHEMA", "env_schema")
    toml_path = tmp_path / ".broker.toml"
    toml_path.write_text(
        'version = 1\nbackend = "postgres"\n'
        'target = "postgresql://postgres@localhost/mydb"\n\n'
        '[backend_options]\nschema = "toml_schema"\n'
    )
    target = resolve_project_target(toml_path)
    assert target.backend_options["schema"] == "env_schema"

def test_toml_target_not_rewritten_by_individual_env_parts(tmp_path, monkeypatch):
    """Granular env vars do not rewrite the toml target."""
    monkeypatch.setenv("BROKER_BACKEND_HOST", "envhost")
    toml_path = tmp_path / ".broker.toml"
    toml_path.write_text(
        'version = 1\nbackend = "postgres"\n'
        'target = "postgresql://postgres@tomlhost/mydb"\n\n'
        '[backend_options]\nschema = "toml_schema"\n'
    )
    target = resolve_project_target(toml_path)
    assert target.target == "postgresql://postgres@tomlhost/mydb"

def test_env_target_overrides_toml_target(tmp_path, monkeypatch):
    """BROKER_BACKEND_TARGET overrides the toml target wholesale."""
    monkeypatch.setenv("BROKER_BACKEND_TARGET", "postgresql://env@envhost/envdb")
    toml_path = tmp_path / ".broker.toml"
    toml_path.write_text(
        'version = 1\nbackend = "postgres"\n'
        'target = "postgresql://postgres@tomlhost/mydb"\n\n'
        '[backend_options]\nschema = "toml_schema"\n'
    )
    target = resolve_project_target(toml_path)
    assert target.target == "postgresql://env@envhost/envdb"
```

These tests belong in the Postgres extension suite because the positive path requires the
plugin to be importable.

**Gate**:
- `uv run pytest extensions/simplebroker_pg/tests/test_pg_init_backend.py -v`
- `uv run pytest extensions/simplebroker_pg/tests/test_pg_integration.py -v -k toml`

---

### Task 7: Update Postgres docs and examples

**File**: `extensions/simplebroker_pg/README.md`

**What to do**:

Update the operator-facing examples so the documentation matches the new behavior:

1. Replace the password-in-toml example with a passwordless target
2. Add an env-only example showing `BROKER_BACKEND=postgres`
3. Document the scope boundary:
   - no `--backend` CLI flag in this plan
   - `BROKER_BACKEND_TARGET` overrides the whole target
   - granular `HOST/PORT/USER/PASSWORD/DATABASE` env vars are only used when there is no
     target from env or toml
4. Document the error distinction:
   - missing backend/plugin => availability error
   - invalid schema / bad password / auth failure / malformed target / missing database =>
     validation or connection error

Suggested README example:

```toml
version = 1
backend = "postgres"
target = "postgresql://postgres@127.0.0.1:54329/simplebroker_test"

[backend_options]
schema = "simplebroker_app"
```

```bash
BROKER_BACKEND=postgres \
BROKER_BACKEND_TARGET='postgresql://postgres@127.0.0.1:54329/simplebroker_test' \
BROKER_BACKEND_SCHEMA='simplebroker_app' \
BROKER_BACKEND_PASSWORD='postgres' \
python -m simplebroker.cli init
```

**Gate**: README examples are updated as part of the same change.

---

### Deferred: `broker init --backend postgres`

Still **not** in this plan. Today, SimpleBroker does not auto-generate `.broker.toml`
files. The toml is created manually by the operator or by tooling (weft, test fixtures).

If a future plan adds `broker init --backend postgres` or any automatic toml writer, that
future work must:

1. Strip passwords from any DSN written to disk
2. Add a comment pointing operators to `BROKER_BACKEND_PASSWORD`
3. Preserve existing env-over-file precedence rules
4. Keep database creation out of scope unless a separate admin-connection design is approved

---

## 4. Files Modified (Summary)

| File | Change |
|---|---|
| `simplebroker/_constants.py` | Add `BROKER_BACKEND` and `BROKER_BACKEND_*` to `load_config()` |
| `simplebroker/_backend_plugins.py` | Add `init_backend()` to `BackendPlugin` protocol |
| `simplebroker/_backends/sqlite/plugin.py` | Implement `init_backend()` (trivial) |
| `simplebroker/project.py` | Consult `BROKER_BACKEND` when no toml found; wrap unknown-backend errors precisely |
| `simplebroker/cli.py` | Honor env-selected non-sqlite backends in the real CLI resolution path |
| `simplebroker/_project_config.py` | Pass toml values through `init_backend()` for non-sqlite backends |
| `extensions/simplebroker_pg/simplebroker_pg/plugin.py` | Add `verify_env()` and implement `init_backend()` with clarified precedence and DSN encoding |
| `extensions/simplebroker_pg/simplebroker_pg/validation.py` | Update schema error text to mention env-based config |
| `extensions/simplebroker_pg/README.md` | Remove password-bearing examples and document env-only usage |

| Test File | Change |
|---|---|
| `tests/test_constants.py` | Test `BROKER_BACKEND` defaults and env reading |
| `tests/test_backend_plugin_resolution.py` | Test `init_backend()` exists on plugins |
| `tests/test_project_config.py` | Test sqlite fallback and backend-validation failure paths in core |
| `extensions/simplebroker_pg/tests/test_pg_init_backend.py` | Test `verify_env()`, DSN construction, encoding, target precedence, schema precedence |
| `extensions/simplebroker_pg/tests/test_pg_integration.py` | Test env-only CLI usage and toml/env precedence end-to-end |

---

## 5. Verification

After all tasks are complete:

```bash
# All existing tests still pass
uv run pytest tests/ -x -q

# Backend env var tests pass
uv run pytest tests/test_constants.py tests/test_project_config.py tests/test_backend_plugin_resolution.py -v -k backend

# Postgres verify_env/init_backend tests pass
uv run pytest extensions/simplebroker_pg/tests/test_pg_init_backend.py -v

# Env-only and toml/env CLI integration tests pass
uv run pytest extensions/simplebroker_pg/tests/test_pg_integration.py -v -k 'env or toml'

# Full pg suite passes
source .envrc && uv run bin/pytest-pg --fast
```

---

## 6. What NOT to Do

- Do NOT put backend-specific env var parsing in `_constants.py`. Core reads the raw values;
  the plugin interprets them.
- Do NOT import `simplebroker_pg` from SimpleBroker core. Always go through `get_backend_plugin()`.
- Do NOT write the password to the toml file under any circumstances.
- Do NOT mock `load_config()` or `os.environ` in tests when you can use `monkeypatch.setenv()`.
  Use real config loading with real env var overrides. The tests should exercise the actual
  config path, not a mocked version of it.
- Do NOT create a separate config file format for Postgres. The `.broker.toml` format
  is sufficient.
- Do NOT add `BROKER_BACKEND` validation to `load_config()`. Validation happens when the
  backend is actually used (in `resolve_broker_target` / `target_for_directory`), not when
  config is loaded.
- Do NOT auto-create Postgres databases in this plan. `broker init` owns schema/table
  initialization inside an existing database, not database creation.
- Do NOT label malformed DSNs, invalid schemas, missing databases, or authentication failures
  as "backend not available". That wording is only for missing/unknown backend plugins.
- Do NOT concatenate raw Postgres credentials into a URL without percent-encoding them.

---

## 7. Engineering Principles

- **Red-green TDD**: Write the failing test, then the code. No code without a test that demanded it.
- **DRY**: Postgres env normalization lives in `verify_env()`, and final DSN construction lives
  in `PostgresBackendPlugin.init_backend()`. No duplicate target-building logic in core, CLI,
  or tests.
- **YAGNI**: No `BROKER_BACKEND_SSL_MODE`, no `BROKER_BACKEND_CONNECT_TIMEOUT`, no
  `BROKER_BACKEND_OPTIONS` bag. Add them when someone needs them.
- **Real tests**: Use `monkeypatch.setenv()` and real `load_config()` calls. Do not mock
  the config system. Do not mock the plugin loading. The only acceptable mock is for
  testing that an uninstalled backend produces a clear error (mock the entry-point loader).
