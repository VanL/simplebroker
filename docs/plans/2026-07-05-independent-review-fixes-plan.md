# Independent Review Fixes Remediation Plan

## Purpose

This document is the implementation plan for the confirmed, real findings from an
independent code review of SimpleBroker, `simplebroker-pg`, and
`simplebroker-redis`. Each finding below was verified against the actual code
(not the original review's summary), and false or already-designed findings were
dropped. See "Findings explicitly out of scope" near the end before you assume
anything is missing.

Findings to fix, in priority order:

1. **Credential leak in backend-target redaction.** `redact_backend_target()`
   uses a fragile URL regex that leaves passwords containing `/ ? # @`
   partially or fully unredacted. Redacted targets are printed to stdout by
   `init`/`--cleanup` and by `_scripts.py`, so real credentials can leak into
   terminals and logs. (`simplebroker/_targets.py:10`)
2. **`BROKER_READ_COMMIT_INTERVAL` is documented but not wired.** The README
   says it controls commit batching for `read --all`, but the CLI read path is
   hardcoded to exactly-once and never reads the value. Nothing outside
   `_constants.py` consumes it. (`simplebroker/commands.py:386`,
   `simplebroker/sbqueue.py:504`, `README.md:1330`)
3. **Scientific-notation timestamp guard is too broad.** Any input containing the
   letter `e` is rejected with "scientific notation not supported", hijacking the
   honest `Invalid timestamp: <input>` message for ordinary garbage like `hello`
   or `tuesday`. (`simplebroker/_timestamp.py:312`)
4. **Redis `broadcast()` is not atomic.** SQLite `broadcast()` inserts to all
   queues in one transaction; the Redis backend loops per-queue with no wrapping
   atomic operation, so a mid-loop failure fans the message out to only some
   queues. The base-class docstring promises atomic broadcast.
   (`extensions/simplebroker_redis/simplebroker_redis/core.py:1323`,
   `simplebroker/db.py:2586`)
5. **Redis activity waiters are not fork-safe and ignore the watcher stop
   event.** The registry key omits the PID (Postgres includes it), and the
   watcher `stop_event` is discarded with `del stop_event`, so a forked child
   waiter never wakes and shutdown blocks until timeout.
   (`extensions/simplebroker_redis/simplebroker_redis/plugin.py:275`,
   `.../plugin.py:524`)
6. **`move --all` silently truncates at 1,000,000 with no signal, and a `Queue`
   destination silently drops its backend target.** `move --all` materializes at
   most 1M messages and returns `EXIT_SUCCESS` with no indication more remain.
   Separately, `dest_name = destination.name` ignores a destination `Queue`'s
   `db_path`, so a cross-target `Queue` is silently reinterpreted as a same-DB
   queue name. (`simplebroker/commands.py:836`, `simplebroker/sbqueue.py:772`)
7. **Release gates do not assert the tag matches the packaged version.** All three
   release-gate workflows extract the tag version but build whatever
   `pyproject.toml` is in the checkout, without comparing the two.
   (`.github/workflows/release-gate.yml`, `release-gate-pg.yml`,
   `release-gate-redis.yml`)
8. **Two CLI honesty bugs.** `init --force` is advertised but never passed
   through, so it is a dead flag; `--cleanup` help says "delete the database
   file" even though non-SQLite backends drop schema/backend state instead.
   (`simplebroker/cli.py:434`, `.../cli.py:903`, `.../cli.py:188`)

Assume the implementer is a skilled developer with no SimpleBroker context and a
tendency to over-mock tests. Follow this plan in order. Use red-green-refactor.
Keep fixes boring, small, and local. This is a mature project with a very high
bar for change: do not refactor stable code, do not add public API, and do not
"improve" anything this plan does not name.

## Repository Primer

### Project shape

- Core runtime code: `simplebroker/`
- Built-in SQLite backend: `simplebroker/_backends/sqlite/`
- Postgres extension: `extensions/simplebroker_pg/simplebroker_pg/`
- Redis extension: `extensions/simplebroker_redis/simplebroker_redis/`
- Core tests: `tests/`
- Extension tests: `extensions/simplebroker_pg/tests/`,
  `extensions/simplebroker_redis/tests/`
- CI workflows: `.github/workflows/`
- Public docs: `README.md`, `CHANGELOG.md`
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
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis
```

Backend integration gates (these auto-provision Docker; do not hand-roll DSNs or
env vars):

```bash
uv run bin/pytest-pg
uv run bin/pytest-redis
```

Use `-n 0` while developing anything touching databases, lifecycle, or
concurrency. The repo enables xdist by default; parallel output hides ordering
and lifecycle bugs.

### Files to read before coding

Read these first:

- `simplebroker/_targets.py`
- `simplebroker/_timestamp.py`
- `simplebroker/commands.py`
- `simplebroker/sbqueue.py`
- `simplebroker/db.py` (especially `broadcast()` and `claim_generator()`)
- `simplebroker/_constants.py`
- `simplebroker/cli.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py` (especially
  `broadcast()`, `_write_message()`, and the `scripts` Lua module it imports)
- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_pg/simplebroker_pg/runner.py` (the reference
  fork-safe / stop-event listener behavior)
- `.github/workflows/release-gate.yml`,
  `.github/workflows/release-gate-pg.yml`,
  `.github/workflows/release-gate-redis.yml`

Why these matter:

- `_targets.py` is the single redaction chokepoint used by every display path.
- `commands.py` and `sbqueue.py` are the CLI-to-core plumbing for read/move.
- `db.py` holds the authoritative (SQLite) `broadcast()` and the exactly-once /
  at-least-once claim machinery that `read --all` should honor.
- The Redis extension re-implements the same public contract independently, so
  the Redis and Postgres extensions are the two places contract drift hides.

## Engineering Rules

### TDD order

For each task:

1. Add or strengthen a real test that fails on current code for the expected
   reason.
2. Run only the targeted test and confirm it is red.
3. Make the smallest code change that turns it green.
4. Refactor only after behavior is pinned.
5. Run the task gate before moving on.

Do not implement first and backfill tests. Several of these are messaging and
security bugs; tests are the only way to keep the fixes honest.

### Prefer real tests over mocks

Good tests:

- Use real `Queue`, `BrokerDB`, temporary databases, and (for extensions) the
  Dockerized backends via `bin/pytest-pg` / `bin/pytest-redis`.
- Assert observable behavior: redacted output strings, queue contents after a
  simulated mid-broadcast failure, exit codes, error messages.
- Use a small fake only at a hard external boundary (e.g. a fake Redis client
  that raises on the Nth write to prove broadcast atomicity).

Bad tests:

- Mocking `BrokerCore` / `DBConnection`.
- Asserting full SQL or full Lua strings character-for-character.
- Sleep-based race tests.

### DRY and YAGNI

- No new dependencies. `urllib.parse` (stdlib) is available for URL work.
- Do not add a general config-plumbing layer, transaction framework, or
  streaming-move framework.
- Do not change public queue delivery semantics beyond what these findings name.
- Do not touch the SQLite hot paths (`db.py` retrieval, timestamp generation)
  except where a task explicitly says to.

## Locked Decisions

Do not reopen these unless a red test proves the plan is wrong.

### Finding 1: parse structurally, redact the whole password

Replace the URL password regex with structural parsing plus a conservative raw
fallback. For URL-shaped targets (`scheme://...`), first use
`urllib.parse.urlsplit`. If `parsed.password` is non-empty, rebuild the URL by
rewriting the raw `parsed.netloc`: split at the last `@`, split the userinfo at
the first `:`, replace everything after that colon with `***`, and pass the
rebuilt pieces to `urlunsplit`. Do **not** rebuild from `parsed.hostname` /
`parsed.port`; malformed authority text can make `parsed.port` raise, and this
redaction path must never fail open.

Some of the required red tests intentionally use malformed-but-real-world DSNs
where raw `/`, `?`, or `#` appear inside the password. `urlsplit()` will not
expose those as `parsed.password`. For any string that contains `://`, a colon
after that marker, and a later `@`, apply a raw fallback that redacts from the
first colon in the userinfo span through the last `@` before whitespace,
preserving the `@` and trailing text. This may over-redact malformed targets;
that is acceptable. Keep the existing conninfo (`key=value`) regex - the review
confirmed it is sound. The observable contract is: no password substring from
the input appears in the output, and redaction errors must redact more, never
less.

### Finding 2: wire the knob to the existing at-least-once path

Do not invent new machinery. The connection-level `claim_generator()` already
has an `at_least_once` batched path using `batch_size`, and the public
`Queue.stream_messages()` wrapper already exposes it as
`batch_processing=True, commit_interval=<n>`. In the CLI `read --all` path, read
`BROKER_READ_COMMIT_INTERVAL` from resolved config:

- interval `== 1` (default): keep `exactly_once` (unchanged behavior).
- interval `> 1`: use the existing `Queue.stream_messages()` path with
  `batch_processing=True` and `commit_interval=interval`.

This matches the README and the comments in `tests/test_exactly_once_delivery.py`.
`peek --all` is non-destructive and must remain unaffected. Do not wire the knob
into any write/move path. Do not add a new public `batch_size` parameter to
`Queue.read_generator()` unless a red test proves the existing stream path cannot
serve the CLI behavior.

### Finding 3: narrow the guard to actual scientific notation

Keep the guard where it is (it must run before the float parse at
`_parse_numeric_timestamp`, since `float("1e10")` would otherwise succeed). Change
the condition from `"e" in timestamp_str.lower()` to a full-match against a
scientific-notation numeric shape, e.g.
`re.fullmatch(r"[+-]?(\d+\.?\d*|\.\d+)[eE][+-]?\d+", timestamp_str)`. Everything
that does not match falls through to the honest `Invalid timestamp: <input>`
error two lines down. All existing scientific-notation tests use genuinely
exponent-shaped inputs and must stay green.

### Finding 4: one atomic multi-queue write, wakeups after

The Redis `broadcast()` must be all-or-nothing across its target queues, matching
the SQLite contract. Mirror the SQLite approach in `db.py:2622-2637`: compute the
target queue list (apply the `pattern` filter in Python), pre-generate one
timestamp/encoded id per target queue, then perform **one** atomic Redis
operation that writes every message. Before adding new Lua, evaluate the
existing `insert_messages()` / `INSERT_MESSAGES` path: it already performs an
atomic multi-record write and publishes wakeups only after success. Reuse or
adapt that path if it preserves the broadcast contract cleanly; add a dedicated
Lua script only if the existing script cannot express the behavior without
awkward coupling.

Preserve `_write_message()`'s conflict posture: if a generated ID collides, retry
with regenerated IDs and timestamp resync as appropriate rather than turning a
rare timestamp race into a user-visible partial failure. Publish per-queue wakeup
notifications **after** the atomic write succeeds, never mid-loop. Return the
count of queues written (unchanged contract). Do not change single-message
`_write_message`.

### Finding 5: match the Postgres listener's fork/stop behavior

The Postgres extension is the reference implementation. For Redis:

- Add `os.getpid()` to the activity-listener registry key so a forked child
  builds its own listener instead of inheriting the parent's dead one. Mirror
  the pg key shape (pid, target, namespace).
- Thread the watcher `stop_event` through `create_activity_waiter` into the
  single-queue wait loop and gate the blocking wait on it (as pg does with
  `while not stop_event.is_set() and not self._stop_event.is_set()`). Remove the
  `del stop_event`.
- Thread the same stop-event behavior through `create_activity_waiter_for_queues`
  and `RedisMultiQueueActivityWaiter`. The fan-in waiter has its own polling /
  sleep loop today; fixing only `RedisActivityWaiter` still leaves multi-queue
  waits blocked until timeout.

Do not redesign the listener; make Redis behave like Postgres.

### Finding 6: signal truncation, reject cross-target Queue destinations

- `move --all`: keep the atomic materialized move (the 1M cap and single
  transaction are deliberate). When the moved count equals the cap, print a
  clear warning to **stderr** that the move was capped and messages may remain.
  Do not change the success exit code (compatibility), and do not convert the
  move to a streaming generator. Replace the inline `1000000` literal with a
  private named constant in `commands.py` so the behavior has one source of
  truth and the boundary path can be tested without materializing 1M messages.
- `Queue` destination: when `destination` is a `Queue`, reject it if its resolved
  backend target differs from the source's, with a `ValueError` naming both
  targets. Cross-database/cross-backend move is not supported and must fail
  loudly instead of silently reinterpreting the name. Compare resolved target
  identity, not raw input strings, so equivalent SQLite paths or resolved
  backend targets do not false-positive. Apply this in every public move entry
  point that accepts a `Queue`: `move`, `move_one`, `move_many`, `move_generator`.

### Findings 1b / config trust boundary: document, do not restrict

The review also flagged that programmatic config overrides and `.broker.toml`
SQLite targets skip the path validation applied to environment values. **These
are not code-change tasks in this plan.** `.broker.toml` and in-process config
are trusted, developer-authored inputs; the validation on environment values
exists for shared/hostile-env cases. Restricting absolute or parent paths there
could break intended use (e.g. a deliberately absolute DB path). The correct fix
is a documentation clarification of the trust model (see Task 8), not new
enforcement. Do not add path restrictions to `_project_config.py` or
`resolve_config()`.

## Task 0: Baseline

### Goal

Start from known behavior before editing anything.

### Steps

1. Read all files listed above.
2. Run:

   ```bash
   uv run pytest -q -n 0
   uv run ruff check .
   uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis
   ```

3. Confirm the Docker-backed suites run in this environment:

   ```bash
   uv run bin/pytest-redis -q -k broadcast
   uv run bin/pytest-pg -q -k activity
   ```

### Gate

You can explain: how a CLI `read --all` reaches `claim_generator`; where
`broadcast()` lives for SQLite vs Redis; how `redact_backend_target()` is reached
from `init`/`--cleanup`; and why the Postgres activity listener keys on PID.

## Task 1: Fix Credential Redaction

### Primary files to touch

- `simplebroker/_targets.py`
- `tests/test_target_redaction.py`

### Red tests first

Add table-driven tests asserting no input password substring survives:

- `postgres://user:p@ss:word@host/db`
- `postgresql://user:secret/withslash@host/db`
- `postgresql://user:s#cret@host/db`
- `postgresql://user:s?cret@host/db`
- a percent-encoded password (must still redact)
- a `host=... password=secret ...` conninfo string (already worked; keep green)
- a plain sqlite path (must pass through unchanged)

For each, assert the raw password fragment (`ss:word`, `secret/withslash`,
`s#cret`, etc.) is absent from the output and that `***` is present for the
URL/conninfo cases. These fail on current code for the special-character URLs.

### Implementation guidance

Parse URL-shaped targets with `urllib.parse.urlsplit`; if `password` is set,
rebuild from the raw `netloc` as `<username>:***@<hostport>` without touching
`parsed.port`. Then handle malformed URL-shaped targets with the raw fallback
described in the Locked Decision. Leave non-URL strings to the existing conninfo
regex. Keep both passes composable so a target that is neither still returns
safely.

### What not to do

- Do not add a dependency (no `sqlalchemy`, no `dsnparse`).
- Do not try to canonicalize or normalize the DSN; only redact.

### Gate

```bash
uv run pytest -q -n 0 tests/test_target_redaction.py
uv run ruff check simplebroker/_targets.py tests/test_target_redaction.py
uv run mypy simplebroker
```

## Task 2: Wire `BROKER_READ_COMMIT_INTERVAL` Into `read --all`

### Primary files to touch

- `simplebroker/commands.py`
- `simplebroker/cli.py` (to pass resolved config into `cmd_read`)
- `tests/test_after_flag.py` (the existing `test_after_with_commit_interval`
  sets the env var but asserts nothing about batching — strengthen it)

### Red tests first

Add a CLI test proving the interval changes commit behavior, not just output:

1. With `BROKER_READ_COMMIT_INTERVAL=1`, `read --all` on a known queue drains it
   and prints all messages in order (unchanged).
2. With `BROKER_READ_COMMIT_INTERVAL > 1`, prove at-least-once batching: simulate
   consumer failure partway (e.g. the existing broken-pipe / early-exit pattern,
   or a small harness around `Queue.stream_messages(batch_processing=True,
   commit_interval=interval)`) and assert an uncommitted batch survives for
   redelivery. If a pure-CLI failure injection is awkward, add a command/Queue-
   level test that asserts interval > 1 selects the existing stream path via
   config-derived behavior, not by mocking `DBConnection`.

Both must be red before the wiring exists (today the value is ignored).

### Implementation guidance

Add a `config` keyword to `cmd_read`, resolve it with `resolve_config(config)`,
and pass the resolved config from `cli.main()` when dispatching the `read`
command. In the `read --all` branch, use a small local wrapper around
`queue.stream_messages(peek=False, all_messages=True, batch_processing=interval > 1,
commit_interval=interval, after_timestamp=..., before_timestamp=...)` so the
existing at-least-once implementation supplies the batch size. Keep the exact-ID,
single-read, and range-single-read paths on the existing `read_one` /
`read_generator` code. Leave `peek` untouched.

### What not to do

- Do not change the default (interval 1 stays exactly-once).
- Do not thread the interval into write, move, or peek.

### Gate

```bash
uv run pytest -q -n 0 tests/test_after_flag.py tests/test_exactly_once_delivery.py
uv run ruff check simplebroker/commands.py simplebroker/cli.py tests/test_after_flag.py
uv run mypy simplebroker
```

## Task 3: Narrow The Scientific-Notation Timestamp Guard

### Primary files to touch

- `simplebroker/_timestamp.py`
- `tests/test_timestamp_edge_cases.py`, `tests/test_after_flag.py`

### Red tests first

Add cases asserting the honest message for non-numeric input containing `e`:

- `validate("hello")` raises with `Invalid timestamp: hello` (not "scientific
  notation").
- CLI `read q --after tuesday` prints `Invalid timestamp: tuesday`.
- Keep the existing exponent cases (`1e10`, `1.5E+9`, `1e-5`, `1.23E+10`) still
  raising the scientific-notation message.

### Implementation guidance

Replace the `"e" in timestamp_str.lower()` condition with the full-match regex in
the Locked Decision. Keep the raise position (before numeric/float parsing).

### Gate

```bash
uv run pytest -q -n 0 tests/test_timestamp_edge_cases.py tests/test_after_flag.py tests/test_property_timestamp_validate.py
uv run ruff check simplebroker/_timestamp.py
uv run mypy simplebroker
```

## Task 4: Make Redis `broadcast()` Atomic

### Primary files to touch

- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- the Redis `scripts` Lua module (grep for `WRITE_MESSAGE`) if a new script is
  needed
- `extensions/simplebroker_redis/tests/` (the broadcast test module)

### Red tests first

Add an atomicity test using a fake/wrapped client that raises on the Nth
underlying write:

1. Create several queues.
2. Broadcast with an injected failure partway through the write.
3. Assert **no** queue received the message (all-or-nothing), and that the
   exception propagates.
4. Add a success test asserting every target queue (including under a `pattern`)
   received exactly one copy and the returned count is correct.

On current code the partial-failure test is red (some queues get the message).

### Implementation guidance

Follow the Locked Decision: compute targets + `pattern` filter in Python,
pre-generate one timestamp/encoded id per queue, then issue one atomic Redis
operation. First try to reuse or adapt `insert_messages()` / `INSERT_MESSAGES`
because it already has the right all-or-nothing shape and post-success wakeups.
If that path is too coupled to explicit imports, add one focused Lua script (or
one `MULTI/EXEC` pipeline only if it is genuinely equivalent for this key
layout). Reuse the key layout and id encoding from `_write_message`, preserve ID
collision retry behavior, and keep the count semantics.

### What not to do

- Do not change `_write_message`.
- Do not publish wakeups inside the atomic write.
- Do not drop `pattern` support.

### Gate

```bash
uv run bin/pytest-redis -q -k broadcast
uv run ruff check extensions/simplebroker_redis/simplebroker_redis/core.py
uv run mypy extensions/simplebroker_redis/simplebroker_redis
```

## Task 5: Redis Activity Waiter Fork-Safety And Stop-Event

### Primary files to touch

- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- `extensions/simplebroker_redis/tests/` (activity/watcher test module)

### Files to read

- `extensions/simplebroker_pg/simplebroker_pg/runner.py` (PID-in-key and
  stop-event-gated wait are the reference)

### Red tests first

1. **Stop-event:** start a Redis-backed waiter with a long timeout, set the
   watcher `stop_event`, and assert `wait()` returns promptly (well under the
   timeout). Red today because `stop_event` is discarded.
2. **Multi-queue stop-event:** create a Redis-backed multi-queue waiter with a
   long timeout, set the same watcher `stop_event`, and assert its `wait()`
   returns promptly. Red today because the fan-in waiter has its own loop with
   no stop-event awareness.
3. **Fork-safety:** assert the registry key includes the PID (unit-level check of
   the key construction is acceptable here since a real fork test is heavy);
   optionally, if the existing suite already forks, add a child-gets-fresh-
   listener assertion.

### Implementation guidance

Add `os.getpid()` to the registry key (mirror pg's tuple). Thread `stop_event`
into the single-queue waiter and check it in the blocking wait loop; remove
`del stop_event`. Thread the same event into the multi-queue waiter and check it
between child waiter polls and before sleeps so fan-in waits do not block until
timeout during shutdown.

### Gate

```bash
uv run bin/pytest-redis -q -k "activity or watcher or stop"
uv run ruff check extensions/simplebroker_redis/simplebroker_redis/plugin.py
uv run mypy extensions/simplebroker_redis/simplebroker_redis
```

## Task 6: `move --all` Truncation Signal And Cross-Target Destination Guard

### Primary files to touch

- `simplebroker/commands.py` (the `move --all` branch)
- `simplebroker/sbqueue.py` (`move`, `move_one`, `move_many`, `move_generator`)
- `tests/` move test modules (grep for `move_all`, `move_one`, `move_generator`)

### Red tests first

1. **Truncation signal:** with the cap lowered for the test (or by asserting the
   boundary path), prove that when the moved count hits the cap, a warning is
   written to stderr. Keep exit code `EXIT_SUCCESS`.
2. **Cross-target guard:** `Queue("in", db_path=a).move_one(Queue("out",
   db_path=b))` raises `ValueError` naming both targets. Add the analogous case
   for `move`, `move_many`, `move_generator`. A same-target `Queue` destination
   (same resolved backend) still works.

Both are red today (silent truncation; silent same-DB reinterpretation).

### Implementation guidance

- Add a single private helper that, given a `destination`, returns the resolved
  destination queue name and raises if a `Queue` destination's resolved target
  differs from `self`'s. Route all four move entry points through it so the
  `dest_name = destination.name` shortcut is replaced in one place (DRY). The
  existing same-name check stays.
- In the `move --all` command branch, replace the inline `1000000` limit with a
  private module constant, compare the moved count to that constant, and emit
  the stderr warning on equality. Tests may monkeypatch the constant to exercise
  the boundary without building a million-message fixture.

### What not to do

- Do not implement cross-database move.
- Do not change the cap value or make `move --all` stream.

### Gate

```bash
uv run pytest -q -n 0 tests/ -k "move"
uv run ruff check simplebroker/commands.py simplebroker/sbqueue.py
uv run mypy simplebroker
```

## Task 7: Release-Gate Tag/Version Check

### Primary files to touch

- `.github/workflows/release-gate.yml`
- `.github/workflows/release-gate-pg.yml`
- `.github/workflows/release-gate-redis.yml`
- `tests/test_release_workflow.py`

### Red tests first

Add workflow text tests proving each release gate contains a tag/package version
check and normalizes the tag to the bare package version before comparison:

- root: `v5.0.1` -> `5.0.1`
- pg: `simplebroker_pg/v3.0.0` -> `3.0.0`
- redis: `simplebroker_redis/v3.0.0` -> `3.0.0`

These tests are intentionally textual. The workflows cannot be run locally, but
the release footgun is in the shell glue, so asserting the normalization strings
and `tomllib` check exist is useful.

### Implementation guidance

In each workflow, after checkout/commit verification and before build/publish,
add a step that normalizes the tag to the bare package version, reads the
packaged version from the relevant `pyproject.toml` with `tomllib`, and fails
the job if they differ. Keep each workflow's own tag prefix and path:

- root: `TAG_VERSION="${GITHUB_REF_NAME#v}"`, path `pyproject.toml`
- pg: `TAG_VERSION="${GITHUB_REF_NAME#simplebroker_pg/v}"`, path
  `extensions/simplebroker_pg/pyproject.toml`
- redis: `TAG_VERSION="${GITHUB_REF_NAME#simplebroker_redis/v}"`, path
  `extensions/simplebroker_redis/pyproject.toml`

If you also adjust the pg/redis `extract-version` job to expose the bare version,
preserve the existing release-name shape by adding the leading `v` back in the
release name (e.g. `simplebroker-pg v${version}`).

Example step body:

```bash
TAG_VERSION="${GITHUB_REF_NAME#v}"
python -c "
import sys, tomllib
tag = sys.argv[1]
with open(sys.argv[2], 'rb') as f:
    pkg = tomllib.load(f)['project']['version']
if pkg != tag:
    sys.exit(f'tag {tag} != pyproject version {pkg}')
" "$TAG_VERSION" pyproject.toml
```

Adapt the `TAG_VERSION` line and `pyproject.toml` path per workflow. Do not add
third-party actions.

### Gate

Workflows cannot be run locally. Validate syntax with `actionlint` if available;
otherwise re-read each edited workflow carefully and confirm the version variable
names, prefix stripping, and `pyproject.toml` paths are correct per workflow.
Note in the PR that these were validated by inspection.

## Task 8: CLI Honesty And Documentation

### Primary files to touch

- `simplebroker/cli.py` (`init --force` removal, `--cleanup` help text)
- `README.md`, `CHANGELOG.md`
- relevant CLI test modules

### Decisions and steps

1. **`init --force`: remove it.** Add a red CLI/help test proving `broker init
   --help` no longer advertises `--force` and `broker init --force` is rejected
   as an unknown argument. Then remove the parser flag and help text. Do not
   thread `force` into `cmd_init`; `cmd_init` is intentionally non-destructive
   across all backends.
2. **`--cleanup` help text:** change "delete the database file and exit" to
   wording accurate for all backends (it drops the backend's state — a file for
   SQLite, schema/keys for pg/redis).
3. **Config trust model (from Findings 1b):** add a short README note stating that
   environment-derived DB paths are validated for safe components, while
   `.broker.toml` targets and in-process `config=` overrides are trusted
   developer inputs and may point anywhere (including absolute/parent paths).
   This documents the intentional asymmetry; do not add enforcement.
4. **Docs for shipped behavior:** confirm `BROKER_READ_COMMIT_INTERVAL` now
   matches its README description; add a one-line note that Redis `broadcast()`
   is atomic like SQLite. Add `CHANGELOG.md` bullets for every fix in this plan.

### Gate

```bash
uv run pytest -q -n 0 tests/ -k "init or cleanup or cli"
uv run ruff check simplebroker/cli.py
uv run mypy simplebroker
```

## Task 9: Final Verification

### Required gates

```bash
uv run pytest -q -n 0
uv run bin/pytest-pg
uv run bin/pytest-redis
uv run ruff check .
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis
```

Then the default parallel suite:

```bash
uv run pytest -q
```

If parallel fails but `-n 0` passes, classify it (real concurrency bug /
performance flake / test-isolation bug) and record exact failing tests in the PR.

### Final invariants checklist

- No input password substring appears in any redacted target output.
- `BROKER_READ_COMMIT_INTERVAL > 1` changes `read --all` commit batching;
  interval 1 is unchanged.
- Non-numeric input containing `e` yields `Invalid timestamp: <input>`; genuine
  scientific notation still rejected with its specific message.
- Redis `broadcast()` is all-or-nothing across target queues; wakeups fire only
  after the atomic write.
- Redis activity registry keys include PID; the watcher `stop_event` breaks the
  wait promptly.
- `move --all` warns on stderr when capped; a cross-target `Queue` destination
  raises `ValueError`.
- All three release-gate workflows fail on a tag/version mismatch.
- `init --force` is absent/rejected; `--cleanup` help matches actual behavior.
- No new dependencies; no public API added; SQLite hot paths untouched.

## Fresh-Eyes Review Checklist For The Implementer

After all tasks are green, reread the diff as a reviewer:

- Did the redaction fix fail closed on unparseable targets, or could a weird
  input slip a password through?
- Did the commit-interval wiring accidentally change the default or touch
  peek/write/move?
- Did the timestamp regex accidentally reject a valid numeric form, or still
  swallow a plain word?
- Did the Redis broadcast publish any wakeup before the atomic write committed?
- Did the move guard get applied to all four entry points, or just `move_one`?
- Did any workflow edit use the wrong `pyproject.toml` path or tag variable?
- Did you add enforcement to `.broker.toml`/`resolve_config()` when the plan said
  document only?

Fix any "yes" before final review.

## Findings explicitly out of scope

These were reviewed and deliberately excluded. Do not "fix" them.

- **`read --all | head` data loss** — designed and documented at-most-once
  destructive read (default interval 1). Only the unwired knob (Task 2) is a bug.
- **`load_lines()` partial mutation on parse error** — documented: load targets a
  fresh DB and should be retried into a clean one. All-or-nothing is not promised.
- **Loose extension version floors** — mitigated by the exact-equality
  `backend_api_version` runtime handshake, which fails loudly on a mismatch.
- **Watcher `__exit__` returning while the daemon thread lives** — only on the
  bounded 2s join timeout, intentional and documented.
- **`.broker.toml` / programmatic config path "escape"** — trusted developer
  inputs; resolved by documentation (Task 8), not enforcement.
- **SQLite `ORDER BY id` vs message-ID order** — false: `id` and `ts` increase
  together under the write lock, so the orderings are identical.
- **Setup marker race** — false: strict marker locking plus version-stamped
  schema markers route schema setup through the advisory lock.
- **CHANGELOG "5.0 under Unreleased"** — false: it is under a released `[5.0.0]`
  heading.

## Non-Goals

- No cross-database or cross-backend `move`.
- No streaming rewrite of `move --all`; the atomic cap stays.
- No new public broker/queue/session API.
- No new env vars or config-plumbing layer.
- No path-restriction enforcement on `.broker.toml` or programmatic config.
- No refactor of SQLite retrieval, timestamp generation, or the backend plugin
  system.
- No new dependencies.
