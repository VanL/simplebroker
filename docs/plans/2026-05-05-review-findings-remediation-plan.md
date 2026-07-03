# Review Findings Remediation Plan

## Purpose

This document is the implementation plan for four review findings in
SimpleBroker and `simplebroker-pg`:

1. Postgres `search_path` is not quoted, so mixed-case schemas can route work to
   the wrong schema.
2. Alias cycle prevention uses stale per-connection cache state and can race
   across connections.
3. `PostgresRunner.close()` does not close the connection pool, despite the
   public runner contract saying `close()` releases resources.
4. `BROKER_MAX_MESSAGE_SIZE` is documented and parsed from config, but runtime
   validation still uses the module constant in several paths.

Assume the implementer is a skilled developer with no SimpleBroker context. Also
assume they tend to over-mock tests. Follow this plan in order. Use
red-green-refactor. Keep fixes boring, small, and local.

## Repository Primer

### Project shape

- Core runtime code: `simplebroker/`
- Built-in SQLite backend: `simplebroker/_backends/sqlite/`
- Postgres extension runtime: `extensions/simplebroker_pg/simplebroker_pg/`
- Core tests: `tests/`
- Postgres extension tests: `extensions/simplebroker_pg/tests/`
- Public docs: `README.md`
- Plans: `docs/plans/`

### Tooling

Use `uv` from the repository root unless you have already verified the local
virtualenv is active.

Initial setup:

```bash
uv sync --extra dev
```

Core gates:

```bash
uv run pytest -q -n 0
uv run ruff check .
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg
```

Postgres integration gates require a real test DSN:

```bash
export SIMPLEBROKER_PG_TEST_DSN="postgresql://..."
uv run pytest -q -n 0 extensions/simplebroker_pg/tests
```

Use `-n 0` while developing database, lifecycle, and concurrency fixes. The repo
enables xdist by default in `pyproject.toml`; parallel output hides ordering and
lifecycle bugs.

### Files to read before coding

Read these first:

- `pyproject.toml`
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `simplebroker/watcher.py`
- `simplebroker/_backend_plugins.py`
- `simplebroker/_runner.py`
- `simplebroker/_constants.py`
- `simplebroker/commands.py`
- `simplebroker/_targets.py`
- `simplebroker/_backends/sqlite/plugin.py`
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `extensions/simplebroker_pg/simplebroker_pg/schema.py`
- `extensions/simplebroker_pg/simplebroker_pg/validation.py`
- `extensions/simplebroker_pg/simplebroker_pg/_identifiers.py`
- `tests/test_aliases_db.py`
- `tests/test_security_fixes.py`
- `tests/test_queue_api_comprehensive.py`
- `extensions/simplebroker_pg/tests/test_pg_integration.py`
- `extensions/simplebroker_pg/tests/test_pg_ownership.py`

Why these files matter:

- `BrokerCore` owns queue semantics and most transaction boundaries.
- `DBConnection` and process-local sessions own runner lifetime.
- `Queue` and CLI commands are public entry points that pass config into core
  code.
- The Postgres extension implements backend-specific locking, pooling, and
  schema rules.

## Engineering Rules

### TDD order

For each task:

1. Add or strengthen a real test that fails on current code for the expected
   reason.
2. Run only the targeted test and confirm it is red.
3. Make the smallest code change that turns it green.
4. Refactor only after the behavior is pinned.
5. Run the task gate before moving on.

Do not implement first and backfill tests. These findings are contract drift and
race bugs; tests are the only way to keep the fixes honest.

### Prefer real tests over mocks

Good tests:

- Use real `Queue`, `BrokerDB`, `BrokerCore`, and temporary databases.
- Use two real connections when the bug is cross-connection stale state.
- Use live Postgres tests when validating Postgres schema behavior, if a DSN is
  available.
- Use a small fake only at a hard external boundary, such as a pool object whose
  only purpose is to prove `close()` calls `pool.close()`.

Bad tests:

- Mocking `BrokerCore` or `DBConnection`.
- Patching SQLite cursors.
- Asserting full SQL strings character-for-character.
- Counting private calls when queue state would prove the behavior.

### DRY and YAGNI

- Add one small helper when the same validation or lock pattern appears in more
  than one place.
- Do not add a generic transaction framework.
- Do not add new dependencies.
- Do not redesign the backend plugin system.
- Do not change public queue semantics beyond the four findings.
- Do not remove accepted schema-name syntax unless this plan explicitly says to.

## Locked Decisions

These are implementation decisions for this remediation. Do not reopen them
unless a red test proves the plan is wrong.

### Finding 1: preserve schema-name compatibility

Do not fix mixed-case schema routing by banning uppercase schema names. That
would be a public compatibility change and it still would not teach the runner to
quote identifiers correctly.

Fix the runner so each pooled connection sets the search path using the existing
validated and quoted identifier helper.

### Finding 2: lock alias mutation at the database boundary

The invariant is global to one broker database or one Postgres schema:

```text
The alias graph must remain acyclic across all live connections.
```

Local alias cache is only an optimization. It must not be trusted for mutation
validation.

For SQLite, a write transaction is enough once the live alias state is reloaded
inside that transaction.

For Postgres, add a backend-specific advisory transaction lock for alias
mutation, then reload live alias state inside that transaction before checking
for duplicates, alias targets, and cycles.

### Finding 3: separate public runner close from per-thread release

`PostgresRunner.close()` should satisfy the public contract and close the pool.
The internal "return only this thread's checked-out connection" behavior should
move behind a clearly named internal method.

Do not let shared process-local sessions accidentally shut down a shared pool
when a watcher recycles one thread's handle.

### Finding 4: use resolved config at every validation boundary

`BROKER_MAX_MESSAGE_SIZE` from resolved config should control:

- CLI stdin reads
- CLI direct argument validation
- `Queue(..., config=...)` writes through `BrokerCore.write()`
- watcher dispatch validation

The module constant remains the default value, not the runtime source of truth
once config has been resolved.

## Task 0: Baseline

### Goal

Start from known behavior before editing runtime code.

### Files to touch

None.

### Steps

1. Read all files listed above.
2. Run:

   ```bash
   uv run pytest -q -n 0 tests/test_aliases_db.py tests/test_security_fixes.py
   uv run pytest -q -n 0 extensions/simplebroker_pg/tests
   uv run ruff check .
   uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg
   ```

3. If `SIMPLEBROKER_PG_TEST_DSN` is not set, note that Postgres integration
   tests are skipped. Do not treat skipped DSN tests as validation of Postgres
   behavior.

### Gate

You can explain:

- how a `Queue.write()` reaches `BrokerCore.write()`
- where `BrokerCore` gets its backend plugin
- why `PostgresRunner` owns a pool rather than one connection
- why alias cache state can be stale across two `BrokerDB` or `BrokerCore`
  instances

## Task 1: Quote Postgres Search Path

### Problem

`PostgresRunner._create_pool()` builds a DSN with:

```python
options=f"-csearch_path={self._schema},public"
```

Postgres folds unquoted identifiers. A schema named `SimpleBroker` can be
created as `"SimpleBroker"` while unqualified queries run against `simplebroker`
or `public`.

### Primary files to touch

- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `extensions/simplebroker_pg/tests/test_pg_integration.py`

### Files to read

- `extensions/simplebroker_pg/simplebroker_pg/validation.py`
- `extensions/simplebroker_pg/simplebroker_pg/schema.py`
- `extensions/simplebroker_pg/tests/conftest.py`

### Red tests first

Add a Postgres integration test in
`extensions/simplebroker_pg/tests/test_pg_integration.py`.

Suggested test name:

```python
def test_postgres_mixed_case_schema_uses_configured_schema(...) -> None:
```

Test shape:

1. Require `SIMPLEBROKER_PG_TEST_DSN` using the existing helpers.
2. Use a schema name with uppercase characters, for example
   `SimpleBrokerCase_<unique_suffix>` if validation allows it. If the existing
   schema validator rejects digits after the prefix incorrectly, fix the test
   name, not the validator.
3. Create a `ResolvedTarget` or project `.broker.toml` that selects that schema.
4. Write one message through `Queue("jobs", db_path=target)` or through project
   config.
5. Read the message back through the same target.
6. Inspect Postgres catalogs using `psycopg`:
   - tables `messages`, `meta`, and `aliases` exist in the exact mixed-case
     schema
   - those tables were not created in `public`
   - if a lowercase schema with the folded name exists, it does not contain the
     broker tables from this run
7. Clean up the exact mixed-case schema in `finally`.

Expected red failure on current code:

- write/read fails because tables are not found, or
- tables appear in the wrong schema.

If no DSN is available locally, still write the integration test. It is the real
regression test for this bug.

### Implementation guidance

In `PostgresRunner._create_pool()`:

1. Stop passing `search_path` through the DSN `options` string.
2. Keep `conn.autocommit = True`.
3. In the pool `configure(conn)` callback, execute:

   ```python
   SET search_path TO {quote_ident(self._schema)}, public
   ```

   Use the existing `quote_ident()` helper. Do not hand-roll quoting.

4. Keep the code local to `_create_pool()`. Do not add a general SQL formatting
   helper.

Potential shape:

```python
def configure(conn: psycopg.Connection[Any]) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {quote_ident(self._schema)}, public")

pool = ConnectionPool(
    self._dsn,
    min_size=self._pool_min_size,
    max_size=self._pool_max_size,
    configure=configure,
    open=False,
    timeout=30.0,
)
```

This f-string is acceptable because `quote_ident()` validates the schema name
against `SCHEMA_NAME_RE` before quoting.

### Invariants

- Every unqualified query issued by `PostgresRunner` runs in the configured
  schema.
- `public` is still present as a fallback for built-in functions and extension
  lookup, but broker tables are not created there.
- Existing lowercase schema behavior is unchanged.
- `require_schema_name()` remains the single schema-name validator.

### What not to do

- Do not restrict schema names to lowercase as the first fix.
- Do not duplicate `quote_ident()`.
- Do not interpolate unvalidated schema names.
- Do not add schema qualification to every SQL statement. That is broader,
  noisier, and not needed if connection search path is correct.

### Gate

Run:

```bash
uv run pytest -q -n 0 extensions/simplebroker_pg/tests/test_pg_integration.py -k "mixed_case_schema or runner_roundtrip"
uv run ruff check extensions/simplebroker_pg/simplebroker_pg/runner.py extensions/simplebroker_pg/tests/test_pg_integration.py
uv run mypy extensions/simplebroker_pg/simplebroker_pg
```

With a DSN available, the mixed-case schema test must pass.

## Task 2: Make Alias Mutation Race-Safe

### Problem

`BrokerCore.add_alias()` checks duplicates and cycles against
`self._alias_cache`. That cache can be stale across live connections. A
deterministic stale-cache sequence can create a cycle:

```text
db1 loads empty alias cache
db2 adds b -> a and commits
db1 adds a -> b using stale cache
```

After both commits, the alias graph contains a cycle.

### Primary files to touch

- `simplebroker/db.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `tests/test_aliases_db.py`
- `extensions/simplebroker_pg/tests/test_pg_integration.py`

### Files to read

- `simplebroker/_backend_plugins.py`
- `extensions/simplebroker_pg/simplebroker_pg/_identifiers.py`
- `simplebroker/_backends/sqlite/plugin.py`
- `tests/test_backend_plugin_resolution.py`

### Red tests first

Add a deterministic stale-cache test to `tests/test_aliases_db.py`.

Suggested test name:

```python
def test_alias_cycle_check_uses_live_state_after_stale_cache(...) -> None:
```

Test shape:

1. Create two real broker connections to the same `broker_target`.
2. On `db1`, call `list_aliases()` or `resolve_alias("missing")` to load an
   empty alias cache.
3. On `db2`, call `add_alias("b", "a")`.
4. On `db1`, call `add_alias("a", "b")`.
5. Assert step 4 raises `ValueError`.
6. Assert final alias state contains only `b -> a`.

This should fail on current code without needing threads.

Do not assert the exact error text here. After live state is reloaded, either
"target is already an alias" or "cycle detected" is a valid rejection. The
important invariant is that the stale connection cannot commit `a -> b`.

Add a Postgres integration variant if `SIMPLEBROKER_PG_TEST_DSN` is available:

- Use two `BrokerCore` instances, two `open_broker()` contexts, or the existing
  alias command helpers against the same Postgres target and schema. `Queue`
  itself has no public alias-management API.
- Use the same stale-cache sequence.
- Assert the same invariant.

### Implementation guidance

Refactor `BrokerCore.add_alias()` so mutation validation happens inside the
write transaction against live alias state.

Recommended shape in `simplebroker/db.py`:

1. Keep `should_warn = self.queue_exists_and_has_messages(alias)` before the
   transaction. The warning is advisory; it does not protect an invariant.
2. Enter `with self._lock:`.
3. Validate simple local inputs with `_validate_alias_target(alias, target)`.
4. Begin the transaction.
5. Acquire backend alias mutation coordination if available.
6. Reload aliases from the database inside the transaction.
7. Check:
   - alias does not already exist
   - target is not itself an alias
   - adding the alias does not create a cycle
8. Insert alias.
9. Increment alias version.
10. Reload aliases so this core's cache is current.
11. Commit.
12. Roll back on any exception.

Do not rely on `_alias_cache_version` to skip reloads on the mutation path.

Add a small private helper only if it keeps the code DRY, for example:

```python
def _prepare_alias_mutation_locked(self) -> None:
    preparer = getattr(self._backend_plugin, "prepare_alias_mutation", None)
    if callable(preparer):
        preparer(self._runner)
```

This keeps SQLite unchanged and lets Postgres add a lock without forcing a large
backend protocol migration. If you choose to add the method to
`BackendPlugin`, update all test stubs in `tests/test_backend_plugin_resolution.py`.
The optional `getattr()` approach is smaller and preferred for this fix.

In `extensions/simplebroker_pg/simplebroker_pg/plugin.py`, implement:

```python
def prepare_alias_mutation(self, runner: SQLRunner) -> None:
    schema_name = cast(_SchemaAwareRunner, runner).schema
    runner.run(
        "SELECT pg_advisory_xact_lock(?)",
        (stable_lock_key("aliases", schema_name),),
    )
```

Use a transaction-scoped advisory lock. Do not use session-scoped
`pg_advisory_lock()` for alias mutation.

### Invariants

- The alias graph is acyclic across all connections.
- The mutation path always validates against live database state.
- Local alias cache remains a read optimization only.
- Duplicate alias errors remain clear and deterministic.
- Existing alias read APIs keep using cache refresh by `alias_version`.
- SQLite and Postgres preserve the same public alias semantics.

### What not to do

- Do not solve this by clearing all caches globally. That still does not lock
  concurrent Postgres writers.
- Do not add triggers or recursive SQL unless a simple transactional reload plus
  advisory lock proves insufficient.
- Do not make alias checks depend on queue existence. Aliases point to names,
  not necessarily existing queues.
- Do not add public alias APIs.
- Do not use sleep-based race tests. The stale-cache test above is deterministic.

### Gate

Run:

```bash
uv run pytest -q -n 0 tests/test_aliases_db.py
uv run pytest -q -n 0 tests/test_alias_cli.py
uv run ruff check simplebroker/db.py extensions/simplebroker_pg/simplebroker_pg/plugin.py tests/test_aliases_db.py
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg
```

With a DSN available, also run:

```bash
uv run pytest -q -n 0 extensions/simplebroker_pg/tests/test_pg_integration.py -k "alias or persistent"
```

## Task 3: Fix Postgres Runner Close Semantics Without Breaking Shared Sessions

### Problem

The public `SQLRunner` contract says `close()` releases resources. The Postgres
runner currently uses `close()` to return only the current thread's connection to
the pool. That leaves pool worker threads alive for explicit runner users who
call `runner.close()`, including examples and tests.

At the same time, internal shared sessions need a cheap per-thread release path.
Do not make shared session cleanup close the pool for every queue handle.

### Primary files to touch

- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `simplebroker/db.py`
- `simplebroker/_runner_lifecycle.py`
- `extensions/simplebroker_pg/tests/test_pg_integration.py`
- `tests/test_process_broker_session.py`

### Files to read

- `simplebroker/_broker_session.py`
- `simplebroker/db.py`, especially `BrokerCore.close()` and `BrokerCore.shutdown()`
- `tests/test_queue_coverage.py`
- `extensions/simplebroker_pg/tests/test_pg_notify.py`

### Red tests first

Add a small unit-level test for `PostgresRunner.close()` that does not need a
live Postgres server.

Suggested location:

- If there is already a runner-specific test file in the extension, use it.
- Otherwise create `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py`.

Test shape:

1. Construct a `PostgresRunner` instance without opening a real pool by using
   `object.__new__(PostgresRunner)`.
2. Install minimal attributes:
   - `_pool` as a small object with a `close()` method that records calls
   - `_thread_local`
   - any other attributes `close()` needs after the refactor
3. Call `runner.close()`.
4. Assert `pool.close()` was called exactly once.

This is an acceptable boundary fake. It tests lifecycle behavior without a
database.

Add a second test for the new per-thread release method:

1. Install a fake `_pool` with `putconn()`.
2. Put a fake connection in `_thread_local.conn`.
3. Call the new method, for example `release_thread_connection()`.
4. Assert `putconn()` was called and `_thread_local.conn` was removed.
5. Assert `pool.close()` was not called.

Add or update a shared-session test:

- `Queue.cleanup_connections()` on a persistent Postgres-backed queue should
  recycle the current thread handle without closing the shared runner pool.
- If no live Postgres DSN is available, use the existing counting backend pattern
  in `tests/test_process_broker_session.py`, but make the test runner expose a
  `release_thread_connection()` method. Prove that cleanup calls the release
  hook and does not call public `close()` when public `close()` would mean
  shutdown.

### Implementation guidance

In `extensions/simplebroker_pg/simplebroker_pg/runner.py`:

1. Rename the current per-thread behavior into a clearly named method:

   ```python
   def release_thread_connection(self) -> None:
       self._return_thread_conn()
   ```

2. Change `close()` to close the pool permanently:

   ```python
   def close(self) -> None:
       self.shutdown()
   ```

3. Keep `shutdown()` as the explicit permanent close operation. It can remain
   the implementation behind `close()`.

In `simplebroker/db.py`, update `BrokerCore.close()` so it releases only the
current handle when a runner supports that internal method:

```python
release_thread_connection = getattr(self._runner, "release_thread_connection", None)
if callable(release_thread_connection):
    release_thread_connection()
else:
    self._runner.close()
```

Keep `BrokerCore.shutdown()` using `close_owned_runner(self._runner)`.

Why this split matters:

- `BrokerCore.close()` is handle cleanup.
- `BrokerCore.shutdown()` is ownership cleanup.
- `PostgresRunner.close()` is public runner cleanup and should not mean
  "current thread only."
- The process-local session owns the runner, so final release should still go
  through `close_owned_runner()` and shut down the pool.

Check `simplebroker/_runner_lifecycle.py` after the change. It already prefers
`shutdown()` when available; that remains correct.

### Invariants

- Explicit `PostgresRunner.close()` shuts down the pool.
- `PostgresRunner.shutdown()` remains valid and idempotent.
- `BrokerCore.close()` does not shut down a shared Postgres runner.
- `BrokerCore.shutdown()` does shut down an owned Postgres runner.
- `Queue(..., runner=runner).close()` still does not close caller-owned runners
  because injected runners are wrapped by `_BorrowedRunner`.
- Existing SQLite behavior does not change.

### What not to do

- Do not make `Queue.cleanup_connections()` release a process-local session
  lease.
- Do not make `PostgresRunner.close()` silently keep the pool alive.
- Do not remove `shutdown()`; existing internal ownership code uses it.
- Do not add a public connection-pool management API.

### Gate

Run:

```bash
uv run pytest -q -n 0 extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py
uv run pytest -q -n 0 tests/test_process_broker_session.py
uv run pytest -q -n 0 tests/test_queue_coverage.py -k "runner or cleanup"
uv run ruff check simplebroker/db.py simplebroker/_runner_lifecycle.py extensions/simplebroker_pg/simplebroker_pg/runner.py extensions/simplebroker_pg/tests
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg
```

With a DSN available:

```bash
uv run pytest -q -n 0 extensions/simplebroker_pg/tests/test_pg_integration.py extensions/simplebroker_pg/tests/test_pg_notify.py
```

## Task 4: Honor `BROKER_MAX_MESSAGE_SIZE`

### Problem

`BROKER_MAX_MESSAGE_SIZE` is loaded into resolved config, but several runtime
paths validate against the constant `MAX_MESSAGE_SIZE`. That makes the env/config
knob ineffective.

### Primary files to touch

- `simplebroker/commands.py`
- `simplebroker/db.py`
- `simplebroker/watcher.py`
- `tests/test_security_fixes.py`
- `tests/test_queue_api_comprehensive.py`
- `tests/test_watcher.py` or a focused watcher test file if one already covers
  message size

### Files to read

- `simplebroker/_constants.py`
- `simplebroker/sbqueue.py`
- `tests/conftest.py`

### Red tests first

Add tests for each boundary. Keep sizes tiny by lowering config in tests. Do not
allocate 11 MB strings for new tests.

#### CLI stdin and argument tests

In `tests/test_security_fixes.py`, add subprocess CLI tests that pass env:

1. `BROKER_MAX_MESSAGE_SIZE=3`, direct argument `"abcd"` should fail with a
   max-size error.
2. `BROKER_MAX_MESSAGE_SIZE=3`, stdin `"abcd"` should fail with a max-size
   error.
3. `BROKER_MAX_MESSAGE_SIZE=4`, direct argument `"abcd"` should succeed.
4. `BROKER_MAX_MESSAGE_SIZE=4`, stdin `"abcd"` should succeed.

Use the existing `run_cli()` helper and pass env through the helper if it
supports it. If it does not, extend the helper in the smallest compatible way.

#### Queue/Core tests

In `tests/test_queue_api_comprehensive.py` or a focused config test:

1. `Queue("q", config={"BROKER_MAX_MESSAGE_SIZE": 3}).write("abcd")` raises
   `ValueError`.
2. `Queue("q", config={"BROKER_MAX_MESSAGE_SIZE": 4}).write("abcd")` succeeds
   and the message can be read back.

Use real temporary DBs through the existing `queue_factory` if it can pass
config. If not, instantiate `Queue` directly with `tmp_path`.

#### Watcher dispatch test

Add a watcher test that proves dispatch uses watcher config:

1. Write a message of length 4 using a queue configured with max size 4.
2. Start a `QueueWatcher` with `config={"BROKER_MAX_MESSAGE_SIZE": 3}` and a
   custom `error_handler` that records errors.
3. Assert the handler is not called.
4. Assert the error handler receives a `ValueError` mentioning max size.

Do not mock `_dispatch()`. Use a real queue and watcher.

### Implementation guidance

In `simplebroker/commands.py`:

1. Change `_read_from_stdin()` so the default max comes from resolved config.
   Prefer:

   ```python
   def _read_from_stdin(*, config: dict[str, Any] = _config) -> str:
       max_bytes = int(config["BROKER_MAX_MESSAGE_SIZE"])
       ...
   ```

2. Change `_get_message_content()` to accept `config` and use
   `config["BROKER_MAX_MESSAGE_SIZE"]` for direct argument validation.
3. Change `cmd_write()` and `cmd_broadcast()` to pass config to message-content
   loading if needed. If command functions already use module `_config`, keep
   that pattern. Do not introduce a new config plumbing layer just for this.

In `simplebroker/db.py`:

1. In `BrokerCore.__init__()`, store resolved config on the instance:

   ```python
   self._config = config
   self._max_message_size = int(config["BROKER_MAX_MESSAGE_SIZE"])
   ```

2. In `BrokerCore.write()`, compare message bytes to
   `self._max_message_size`.
3. Use the configured value in the error text.
4. Make sure SQLite construction actually passes config into `BrokerCore`.
   This is easy to miss. Today `BrokerDB` constructs `BrokerCore` without a
   config argument.

   Required shape:

   - update `BrokerDB.__init__()` to accept `config: dict[str, Any] = _config`
   - call `super().__init__(self._runner, config=config)`
   - in `DBConnection._create_managed_connection()`, pass `self._config` when
     constructing `BrokerDB`
   - in `DBConnection.get_core()`, pass `self._config` when it lazily constructs
     a SQLite `BrokerDB`
   - in `_ProcessBrokerSession._create_core()`, pass `self._config` when it
     constructs a SQLite `BrokerDB`

   Without this plumbing, `Queue("q", config={"BROKER_MAX_MESSAGE_SIZE": 3})`
   will still use the module default on the SQLite path.

In `simplebroker/watcher.py`:

1. In `_dispatch()`, use `config["BROKER_MAX_MESSAGE_SIZE"]`.
2. Keep the existing `config` parameter because callers already pass
   `self._config`.
3. Update error text so tests do not depend on the hard-coded 10 MB string.

### Invariants

- The constant `MAX_MESSAGE_SIZE` remains the default only.
- Resolved config controls runtime validation.
- CLI direct argument and stdin behavior match.
- Queue API and watcher API behavior match.
- Existing default behavior remains 10 MB.
- Tests do not allocate huge strings unless they are existing legacy tests.

### What not to do

- Do not remove `MAX_MESSAGE_SIZE`.
- Do not add a second env var.
- Do not parse env directly in `db.py`, `commands.py`, or `watcher.py`. Use
  resolved config.
- Do not weaken the existing large-stdin security tests.

### Gate

Run:

```bash
uv run pytest -q -n 0 tests/test_security_fixes.py -k "message_size or stdin"
uv run pytest -q -n 0 tests/test_queue_api_comprehensive.py -k "message_size or max_message"
uv run pytest -q -n 0 tests/test_watcher.py -k "message_size or error_handler"
uv run ruff check simplebroker/commands.py simplebroker/db.py simplebroker/watcher.py tests/test_security_fixes.py
uv run mypy simplebroker
```

## Task 5: Documentation Updates

### Goal

Update docs only after runtime behavior and tests are green.

### Primary files to touch

- `README.md`
- `CHANGELOG.md`
- `extensions/simplebroker_pg/README.md` only if it currently documents runner
  lifecycle or schema casing

### Required updates

1. Document that Postgres schema names are validated and safely quoted when used
   as `search_path`.
2. Clarify explicit Postgres runner lifecycle:
   - `runner.close()` releases the pool.
   - `runner.shutdown()` remains available as an explicit synonym or owned
     shutdown path if the code keeps both.
   - `Queue(..., runner=runner).close()` does not close a caller-owned runner.
3. Clarify `BROKER_MAX_MESSAGE_SIZE`:
   - default is 10 MB
   - env/config changes apply to CLI, Queue writes, and watcher dispatch
4. Add changelog bullets for the four fixes.

### What not to do

- Do not document internals like process-local registry details unless public
  behavior changed.
- Do not add a long architecture essay to the README.
- Do not claim Postgres mixed-case schema support unless the integration test
  passes with a real DSN.

### Gate

Run:

```bash
uv run ruff check .
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg
```

Docs do not need a separate renderer for this task.

## Task 6: Final Verification

### Required gates

Run from the repo root:

```bash
uv run pytest -q -n 0 tests/test_aliases_db.py tests/test_security_fixes.py tests/test_queue_api_comprehensive.py tests/test_process_broker_session.py
uv run pytest -q -n 0 extensions/simplebroker_pg/tests
uv run ruff check .
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg
```

If a Postgres DSN is available:

```bash
export SIMPLEBROKER_PG_TEST_DSN="postgresql://..."
uv run pytest -q -n 0 extensions/simplebroker_pg/tests
```

Then run the default parallel suite:

```bash
uv run pytest -q
```

If parallel tests fail but `-n 0` passes, do not ignore it. Classify it:

- real concurrency bug
- timeout/performance flake
- test isolation bug

Document the classification in the PR notes with exact failing tests.

### Final invariants checklist

Before declaring done, verify:

- Mixed-case Postgres schemas do not create broker tables in `public`.
- Alias cycles cannot be created through stale cache state.
- Postgres explicit `runner.close()` shuts down its pool.
- Shared session cleanup does not shut down a shared pool prematurely.
- Configured max message size is honored by CLI, Queue, BrokerCore, and watcher.
- Existing default max message size is unchanged.
- No public API was added unnecessarily.
- No test relies on sleeps to prove a race.
- No new dependencies were added.

## Fresh-Eyes Review Checklist For The Implementer

After all tasks are green, step away and reread the diff as if you are reviewing
someone else's work. Look for these specific mistakes:

- Did any fix solve the symptom by weakening validation or tests?
- Did any Postgres SQL interpolate an unvalidated identifier?
- Did alias mutation still read from `_alias_cache` before acquiring the
  transaction or backend lock?
- Did `BrokerCore.close()` accidentally become an owner shutdown path?
- Did `Queue.cleanup_connections()` release a process-local session lease?
- Did a config-size test pass only because it used the default 10 MB path?
- Did you update docs before code behavior was proven?
- Did you add a broad abstraction for one backend-specific need?

If the answer to any question is yes, fix that before final review.

## Non-Goals

- No new public broker/session API.
- No generic graph storage for aliases.
- No schema-name policy change unless the quoted search path approach fails with
  a real Postgres test.
- No attempt to make `Queue.close()` close caller-owned injected runners.
- No change to queue delivery semantics.
- No new test-only sleeps for concurrency.
