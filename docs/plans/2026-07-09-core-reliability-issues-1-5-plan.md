<!-- /autoplan restore point: /Users/van/.gstack/projects/VanL-simplebroker/main-autoplan-restore-20260709-192804.md -->
# Core Reliability Issues 1-5 Remediation Plan

**Date:** 2026-07-09

**Status:** Proposed; implementation has not started

**Scope:** The five numbered findings from the expanded SimpleBroker assessment

**Implementation order:** Sequential. These changes share core modules and public contracts.

## Purpose

Fix five verified boundary defects without changing SimpleBroker's message identity
contract, adding a background service, or broadening the broker into a task framework:

1. Prevent backend credentials from appearing in public representations and
   cross-target error text.
2. Make instance configuration authoritative throughout connection, watcher, SQL-core,
   and Redis-core operations.
3. Keep a failed peek-handler message retryable by the same watcher instead of
   checkpointing it as a success.
4. Make automatic vacuum useful and honest for long-lived cores, while documenting why
   ephemeral and one-command use still requires explicit maintenance.
5. Repair three watcher and delivery-contract defects: one callback invocation per
   failure, truthful `is_running()` state, and fail-fast validation of
   `delivery_guarantee`.

Issue 5 is one review item with three independent sub-fixes. Keep them as separate
red-green slices so a failure in one does not blur the others.

This plan is intentionally limited to the identified blast radius. It does not take on
the separate assessment findings about exactly-once wording, strict unknown config
keys, backend scale limits, documentation restructuring, or protocol splitting.

## Locked Decisions

These decisions are part of the requested direction. Do not reopen them during
implementation unless direct code evidence makes one infeasible.

1. **Display paths are safe by default.** `repr()` and exception messages must never
   expose a non-SQLite target password or any `backend_options` value. Option names may
   remain visible. SQLite file paths remain visible because they are diagnostic paths,
   matching the existing `display_target` contract.
2. **Serialization remains lossless and sensitive.** `serialize_broker_target()` must
   continue to include connection material because its job is process transport. Add a
   warning to its docstring and documentation; do not redact the serialized payload.
3. **Instance config wins.** Constructors normalize config once into `self._config`.
   Instance methods use that snapshot unless an existing public per-call `config=`
   parameter is explicitly supplied. Private instance methods do not reach back to a
   module-level `_config` default.
4. **Peek success means handler success.** A caught handler exception, oversized
   payload, missing handler, or failed error handler is not a successful dispatch and
   cannot advance `_last_seen_ts`. For protected subclass compatibility, an overridden
   `_dispatch()` returning legacy `None` still means success; only explicit `False`
   means failure.
5. **A failed peek item blocks later IDs for that watcher.** Stop the current peek batch
   at the first failed dispatch. On the next driven turn, retry that ID. This preserves
   timestamp checkpoint ordering and matches the README's safe peek-and-ack pattern.
6. **No new retry-action enum.** `error_handler=False` still stops the watcher.
   `True` or `None` still means the watcher may continue. In peek mode, continuing means
   retrying the failed item on a later turn. To skip a poison item, user code must
   explicitly delete or move it in the error handler.
7. **Automatic vacuum remains opportunistic.** It runs only when a long-lived core sees
   enough successful message mutations. It is not a timer, daemon, cross-process
   scheduler, or durability guarantee. CLI and default ephemeral-handle users must run
   `broker --vacuum` periodically when retention matters. Preserve the current
   nonpositive-interval behavior by treating every configured interval less than one as
   one, meaning check after every positive mutation.
8. **Maintenance never changes a committed operation's result.** Automatic maintenance
   runs after the message transaction commits. A maintenance failure is logged with a
   traceback when instance logging is enabled and retried on later activity; it must not
   turn a committed write, claim, or move into a reported failure. Manual `vacuum()`
   continues to raise its storage error.
9. **One error means one callback invocation.** The public error-handler contract is
   exactly `(exception, message, timestamp)`. Bind config into the internal default at
   construction. Never infer callback shape by catching a callback's `TypeError`.
10. **`is_running()` reports execution, not permission to start.** It is false before
    `run_forever()` enters, true while its loop and cleanup own the run (including the
    interval after stop is requested but before cleanup finishes), and false after normal
    stop or fatal exit. Do not add a public lifecycle enum or `last_error` API.
11. **Delivery modes are closed input.** Only `"exactly_once"` and
    `"at_least_once"` are accepted. Every materialized and generator claim/move entry
    point validates before storage mutation. Queue wrappers validate before acquiring a
    connection, so an invalid call cannot create a new SQLite file/schema. SQL and Redis
    cores validate again before message-state mutation for direct callers. Generator
    validation occurs on first iteration, consistent with normal Python generator
    execution.
12. **No runtime dependency or schema change.** Use the standard library and existing
    storage structures. The implementation diff does not edit lockfiles, package
    versions, or backend schema versions. The later release commit must raise the Redis
    package's core floor because Redis imports the two new core-private modules; that
    release-only compatibility step is specified below.

## NOT in Scope

- Comparing payload bodies during exact-ID replay or changing message identity.
- Rewording the broader exactly-once documentation. That was a separate assessment
  finding and needs its own contract review.
- Rejecting unknown `BROKER_*` keys or adding `resolve_config(strict=True)`.
- Persisting an automatic-vacuum counter in SQLite, Postgres, or Redis.
- Adding a maintenance thread, timer, daemon, scheduler, signal, metric service, or
  background process.
- Making the default ephemeral `Queue` retain a process-global core solely to accumulate
  maintenance activity.
- Adding dead-letter policy, retry counts, visibility timeouts, acknowledgements, or a
  four-state handler-result protocol.
- Adding a public watcher lifecycle enum, restart support, or a background exception
  retrieval API.
- Splitting `BrokerConnection` into public and private protocols.
- Centralizing all backend core construction or extracting unrelated validators from
  `db.py`.
- Changing Redis persistence settings, Postgres timestamp allocation, benchmark claims,
  or backend support matrices.
- Moving example execution into CI. Examples and shellcheck remain local release gates.
- Editing the user's shell-example fixes or `bin/release.py` shellcheck work except to
  run the existing local gate during final verification.

If implementation evidence shows that one of the five fixes requires any item above,
stop and report the conflict. Do not silently grow the plan.

## Repository Primer for the Implementer

### Product and domain model

SimpleBroker is a synchronous Python message queue with a zero-dependency SQLite core
and first-party Postgres and Redis/Valkey extensions. The public `Queue` API delegates
to a backend-neutral connection contract. Postgres reuses the SQL `BrokerCore`; Redis
has a direct `RedisBrokerCore` because Redis state transitions are implemented with
Redis data structures and Lua.

The message lifecycle relevant to this plan is:

```text
write/insert/broadcast
        |
        v
pending message
        |
        | claim/read
        v
claimed row or claimed Redis member
        |
        | explicit or opportunistic vacuum
        v
physically removed
```

`peek` does not claim. A peek watcher maintains an in-memory timestamp checkpoint:

```text
read next ID > _last_seen_ts
        |
        v
call handler
   | success                  | failure
   v                          v
advance checkpoint       keep checkpoint
continue batch           stop this batch
                              |
                              v
                       retry on later turn
```

Advancing past a failed ID is unsafe because the query for the next turn asks only for
IDs greater than the checkpoint. A later successful ID would permanently hide the
older failed ID from that watcher even though the row is still pending.

### Configuration model

`resolve_config()` starts with environment-derived defaults, applies caller overrides,
and normalizes known values. Constructors store the resulting dictionary on the
instance. Treat that dictionary as immutable configuration for the instance. Do not
mutate it in tests or production code.

Module `_config = load_config()` values are acceptable as public call defaults where
changing the API is not part of this work. They are not acceptable as defaults on
private instance methods when the object already has `self._config`; that is the source
of issue 2.

For existing public per-call generator `config=` parameters, use this precedence:

```text
explicit per-call config provided? -- yes --> resolve_config(per-call config)
              |
              no
              v
        self._config
```

Do not add new per-call config parameters.

### Delivery semantics touched here

The two accepted strings select transaction timing:

- `exactly_once`: commit each broker claim/move before yielding or returning it.
- `at_least_once`: a generator commits a batch only after the full batch was yielded;
  closing or failing mid-batch rolls it back for replay.

This task validates the selector. It does not rename or reinterpret the modes.

### Tooling

- Python support: 3.11 through 3.14. Write Python 3.11-compatible syntax.
- Dependency and command runner: `uv`.
- Test runner: `pytest`. Root tests use xdist by default. Use `-n0` for focused
  watcher, callback, and lifecycle tests.
- Backend parity: `bin/pytest-pg` and `bin/pytest-redis` run shared tests against live
  Postgres and Redis/Valkey services.
- Lint and format: Ruff.
- Type checking: mypy with `pyproject.toml`.
- Local release checks: `bin/release.py`; example tests and shellcheck are deliberately
  local-only.

No dependency installation or lockfile update is required.

### Code style

- Prefer small named helpers, explicit branches, and early returns.
- Use `Mapping[str, Any] | None` for optional config inputs; normalize only at the
  boundary. Internal stored config remains `dict[str, Any]`.
- Use `repr(value)` or `value!r`; never build quoted repr output by hand.
- Keep target-display logic in one helper. Do not scatter ad hoc password regular
  expressions through `sbqueue.py`.
- Put backend-neutral delivery validation in one small internal module. Do not repeat
  string tuples in four implementations.
- Put automatic-vacuum scheduling state in the small internal maintenance module named
  below. Keep backend I/O and logging out of it; do not create a general scheduler.
- Keep backend I/O in the existing cores. Shared helpers may decide policy but must not
  know SQLite SQL, Postgres SQL, or Redis commands.
- Do not catch `TypeError` to detect callback signatures.
- A broad `except Exception` is allowed only at the automatic-maintenance isolation
  boundary, where the exception is logged with traceback and the already-committed
  user operation must remain successful. Do not catch `BaseException`.
- Error tests assert stable substrings and state invariants, not full punctuation.
- Update docstrings when return types change from `None` to `bool` internally.
- Preserve existing public names and import surfaces. New modules are private and are
  not added to `simplebroker.__init__` or `simplebroker.ext`.

### Test design rules

The implementer is expected to follow red-green-refactor literally:

1. Add one failing behavior test.
2. Run only that test and verify it fails for the intended reason.
3. Implement the smallest production change that makes it pass.
4. Run the focused file.
5. Refactor only while green.
6. Move to the next slice.

Tests should use real `Queue`, `BrokerCore`, SQLite files, watcher threads, and live
backend fixtures. Do not mock `Queue`, `DBConnection`, `BrokerCore`, SQL runners,
Redis clients, `PollingStrategy`, or `threading.Thread` as the main proof.

Allowed narrow seams:

- A patched backend `vacuum` hook that raises once, solely to prove post-commit
  automatic-maintenance failure isolation. The real database must still prove the
  message operation committed.
- One patched Redis client vacuum command/pipeline that raises `redis.RedisError`,
  solely to prove translation to `OperationalError`; construct and close a real Redis
  core around that seam.
- `threading.excepthook` capture to keep a deliberate fatal background-thread test
  deterministic and warning-free.

Do not use `time.sleep()` as synchronization. Use `threading.Event`, `Barrier`, thread
`join()`, and the repository's existing wait helpers. A timeout is a test safety bound,
not the condition being asserted.

## What Already Exists

| Need | Existing code | Reuse decision |
|---|---|---|
| Target password redaction | `simplebroker._targets.redact_backend_target()` and `BrokerTarget.display_target` | Reuse; do not add another URL/conninfo parser |
| Public target type | `BrokerTarget` aliases `BrokerTarget` in `simplebroker.project` | Fix `BrokerTarget.__repr__` because it is public in practice |
| Cross-target protection | `Queue._move_destination_name()` compares activity-waiter compatibility keys before mutation | Keep comparison; make its diagnostics safe |
| Config normalization | `resolve_config()` and `_merge_config()` | Reuse at constructor and explicit per-call boundaries |
| Watcher strategy values | `PollingStrategy` constructor fields | Use them as a narrow structural assertion; do not write timing tests |
| Handler isolation | `_safe_call_handler()` and `_handle_handler_error()` | Return explicit success and invoke the callback once |
| Peek checkpoint | `QueueWatcher._last_seen_ts` and `_after_timestamp_filter()` | Preserve; update only after true success |
| Watcher cleanup | `run_forever()` already owns cleanup in `finally` | Add running-state set/clear around this existing owner |
| SQL vacuum decision | `BrokerCore._should_vacuum()` and backend plugin `vacuum()` | Reuse; feed instance config and trigger after committed activity |
| Redis stats and vacuum | `RedisBrokerCore.get_overall_stats()` and `vacuum()` | Reuse to implement the same opportunistic policy |
| Delivery type annotations | Repeated `Literal["exactly_once", "at_least_once"]` | Replace with one internal alias and validator |
| Cross-backend behavior fixtures | `broker_target`, `broker`, `queue_factory`, and `make_broker()` | Use shared tests for SQLite, Postgres, and Redis |
| Real batch rollback tests | `tests/test_exactly_once_delivery.py` and Redis batch tests | Extend them; do not invent a mock transaction harness |
| Local release gate | `bin/release.py` now includes example tests and shellcheck | Run it locally; do not add examples to CI |

## Target Architecture

```text
                           PUBLIC SURFACE
        +--------------------------------------------------+
        | Queue / QueueWatcher / QueueMoveWatcher          |
        +----------------------+---------------------------+
                               |
             safe target text  |  instance config snapshot
                               v
        +----------------------+---------------------------+
        | DBConnection / BrokerConnection protocol         |
        +----------------------+---------------------------+
                               |
                   +-----------+-----------+
                   |                       |
                   v                       v
        +----------+----------+   +--------+-------------+
        | SQL BrokerCore      |   | RedisBrokerCore      |
        | SQLite + Postgres   |   | direct backend       |
        +----------+----------+   +--------+-------------+
                   |                       |
                   +-----------+-----------+
                               |
                 +-------------+--------------+
                 | validate delivery selector |
                 | record committed activity  |
                 | run opportunistic vacuum   |
                 +----------------------------+

Watcher dispatch path:

message -> _dispatch() -> _safe_call_handler() -> bool success
                         | handler exception
                         v
                 error handler exactly once
                         |
              +----------+----------+
              |                     |
           success                failure
              |                     |
      observe/checkpoint       do not observe
      continue batch          stop peek batch
```

No new public component appears. The two new modules are small private policy modules:

- `simplebroker/_delivery.py`: accepted type, allowed values, validator.
- `simplebroker/_maintenance.py`: only the cross-backend interval/counter policy and
  claimed-ratio/absolute-count predicate.

Do not place backend I/O in either module.

## File Map

The implementer may touch only the files below unless a failing test proves a direct
importer also needs a mechanical annotation update.

| File | Planned change |
|---|---|
| `simplebroker/_targets.py` | Safe custom `BrokerTarget.__repr__`; redact all backend-option values |
| `simplebroker/project.py` | Warn that target serialization contains secrets |
| `simplebroker/sbqueue.py` | Shared safe target display; repr quoting; safe cross-target errors; delivery alias annotations |
| `simplebroker/_constants.py` | Clarify nonpositive automatic-vacuum interval behavior in config docs |
| `simplebroker/db.py` | Instance config use, delivery validation, committed-activity maintenance triggers |
| `simplebroker/watcher.py` | Instance config use, callback binding, dispatch result, peek retry, lifecycle state |
| `simplebroker/_backend_plugins.py` | Use shared delivery type and optional per-call config contract |
| `simplebroker/_delivery.py` | New private delivery validator and type alias |
| `simplebroker/_maintenance.py` | New private maintenance counter and eligibility policy shared by SQL and Redis |
| `extensions/simplebroker_redis/simplebroker_redis/core.py` | Config, validation, maintenance parity, Redis error translation for vacuum |
| `tests/test_target_redaction.py` | Public target repr secret regressions |
| `tests/test_queue_api_additions.py` | Queue repr redaction and correct repr quoting |
| `tests/test_queue_move_cross_target.py` | Safe error text plus no-mutation invariant |
| `tests/test_connection_config.py` | Instance-config behavior and explicit per-call precedence |
| `tests/test_watcher.py` | Peek failure retry/checkpoint and lifecycle behavior |
| `tests/test_watcher_edge_cases.py` | Exactly-once error callback and config-aware default behavior |
| `tests/test_exactly_once_delivery.py` | Shared invalid delivery-mode matrix and generator laziness |
| `tests/test_message_claim.py` | Shared automatic-vacuum behavior tests; replace the misleading SQLite-only test |
| `tests/test_maintenance_policy.py` | New pure counter-boundary and vacuum-eligibility tests |
| `extensions/simplebroker_redis/tests/test_redis_core_behaviors.py` | Redis-only manual-vacuum `RedisError` translation test |
| `README.md` | Safe serialization, config authority, peek retry, watcher health, opportunistic vacuum |
| `CHANGELOG.md` | Add an `[Unreleased]` entry for user-visible fixes |

The list is longer than eight files because the public contract spans one shared SQL
core, one direct Redis core, watcher code, and backend-agnostic tests. Do not split this
into parallel worktrees: `db.py`, `watcher.py`, shared tests, README, and type contracts
create more merge risk than useful concurrency.

## Dependency and Execution Plan

```text
Task 0 baseline
    |
    +--> Task 1 safe display
    |
    +--> Task 2 instance config
             |
             +--> Task 3 peek retry
             |
             +--> Task 4 maintenance
             |
             +--> Task 5A callback once
             +--> Task 5B lifecycle truth
             +--> Task 5C delivery validation
                          |
                          v
                    Task 6 docs
                          |
                          v
                    Task 7 full gates
```

Task 2 lands before the watcher and maintenance behavior changes so every later test
observes the instance config it passed. Within Task 5, A, B, and C are separate commits
or at least separate green checkpoints.

Sequential implementation is preferred. If more than one engineer must work at once,
the only low-conflict split is Task 1 versus Task 2. Merge Task 2 before starting Tasks
3-5 and rebase before touching shared tests.

## Task 0: Establish and Protect the Baseline

### Task 0.1: Record repository state

Run:

```bash
export IMPLEMENTATION_BASE="$(git rev-parse HEAD)"
printf 'Implementation base: %s\n' "$IMPLEMENTATION_BASE"
git status --short --branch
git diff --check
git diff --stat origin/main...HEAD
```

Copy the printed 40-character hash into the implementation work log. If later commands
run in a new shell, re-export that recorded value before using it. Do not recompute it
after making commits; it is the fixed lower bound for the final scope review.

Expected current state:

- branch `main` is ahead of `origin/main` by the user's shellcheck/example commit;
- the worktree is clean before this plan file is added;
- `bin/release.py` and shell examples already contain intentional user work.

Do not reset, rewrite, reformat, or fold that commit into this implementation.

### Task 0.2: Read the contracts before editing

Read these exact areas:

- `README.md`: Real-time Queue Watching, Python API safe peek-and-ack pattern,
  Environment Variables, Message Lifecycle, Embedding SimpleBroker.
- `simplebroker/_targets.py`: `redact_backend_target`, `BrokerTarget`.
- `simplebroker/project.py`: target serialization.
- `simplebroker/sbqueue.py`: constructor ownership, `_move_destination_name`, granular
  read/move APIs, `__repr__`.
- `simplebroker/db.py`: `DBConnection`, `BrokerCore` construction, transactional
  retrieve/generator helpers, maintenance methods.
- `simplebroker/watcher.py`: BaseWatcher construction, dispatch, retry, lifecycle,
  QueueWatcher peek processing, QueueMoveWatcher construction.
- `simplebroker/_backend_plugins.py`: `BrokerConnection` claim/move signatures.
- Redis direct core: construction, writes/inserts, claim/move methods and generators,
  stats, vacuum.

### Task 0.3: Run a focused green baseline

```bash
uv run --extra dev pytest -n0 \
  tests/test_target_redaction.py \
  tests/test_queue_api_additions.py \
  tests/test_queue_move_cross_target.py \
  tests/test_connection_config.py \
  tests/test_watcher.py \
  tests/test_watcher_edge_cases.py \
  tests/test_queue_config_defaults.py \
  tests/test_exactly_once_delivery.py \
  tests/test_message_claim.py
```

If this baseline is red before adding a regression, stop and separate the pre-existing
failure from this work. Do not weaken an assertion to manufacture green.

## Task 1: Make Every Display Surface Credential-Safe

### Task 1.1: Add red `BrokerTarget` repr tests

Edit `tests/test_target_redaction.py`.

Add a parameterized test using sentinel secrets in all of these positions:

- a normal URI password;
- a URL-encoded password;
- conninfo `password='secret value'`;
- at least two `backend_options` values, one named like a secret and one innocuous
  (for example `password` and `schema`).

Assert:

- `repr(target)` contains none of the sentinel values;
- the target backend name and redacted host/path remain useful;
- nonempty backend options expose keys at most, never values;
- `repr(target)` is deterministic regardless of backend-option insertion order;
- SQLite `display_target` still leaves a file path unchanged.

Run the new test alone and confirm it fails because the generated dataclass repr contains
the raw target and option values.

### Task 1.2: Implement one safe target representation

Edit `simplebroker/_targets.py`.

- Add an explicit `__repr__` to `BrokerTarget`.
- Render `target` through `display_target`.
- Render every backend-option value as a fixed redaction marker. Sort keys for stable
  output. Do not maintain a list of secret-looking key names; unknown backend plugins
  can store secret material under arbitrary names.
- It is acceptable for the repr to stop being constructor-evaluable. Security takes
  priority over reconstructability.
- Preserve useful nonsecret fields such as backend name, project root, config path, and
  scope flags.

Do not change `redact_backend_target()` or the SQLite `display_target` behavior unless
the new tests expose a real redaction defect.

### Task 1.3: Add red Queue repr and cross-target error tests

Edit `tests/test_queue_api_additions.py` and
`tests/test_queue_move_cross_target.py`.

Queue repr test:

- Construct a nonpersistent `Queue` with a password-bearing `BrokerTarget`; no backend
  connection is required merely to call `repr()`.
- Assert the raw and encoded sentinels are absent, `***` is present, the queue name is
  present, and ordinary SQLite repr expectations remain unchanged.
- Add a queue/path value containing a quote and assert Python repr escaping is correct.

Cross-target error test:

- Use two real SQLite `BrokerTarget` instances with different `tmp_path` database
  paths and secret sentinel values in `backend_options`.
- Seed the source with a real message.
- Exercise `move`, `move_one`, `move_many`, and lazy `move_generator`.
- Assert the error identifies the safe source and destination filenames, contains no
  backend-option sentinel, leaves the source message pending, and leaves the destination
  empty.

This test must not mock the move method or compatibility-key comparison.

### Task 1.4: Centralize Queue target display

Edit `simplebroker/sbqueue.py`.

- Add one small private helper that returns `BrokerTarget.display_target` for resolved
  targets and the string path unchanged otherwise.
- Use it in `Queue.__repr__` and cross-target error construction.
- Build repr fields with `!r`; remove hand-written quote wrapping.
- Keep `Queue.__str__` unchanged.
- Do not rely only on `BrokerTarget.__repr__` for error safety. The Queue helper is
  defense in depth and keeps all Queue diagnostics consistent.

### Task 1.5: Mark serialization as sensitive

Edit `simplebroker/project.py` and the embedding/target documentation in `README.md`.

State that `serialize_broker_target()` is a transport payload, may contain credentials,
and must not be logged or exposed. Do not redact it and do not change round-trip tests.

### Task 1 gates

```bash
uv run --extra dev pytest -n0 \
  tests/test_target_redaction.py \
  tests/test_queue_api_additions.py \
  tests/test_queue_move_cross_target.py
uv run --extra dev ruff check \
  simplebroker/_targets.py simplebroker/project.py simplebroker/sbqueue.py \
  tests/test_target_redaction.py tests/test_queue_api_additions.py \
  tests/test_queue_move_cross_target.py
uv run --extra dev ruff format --check \
  simplebroker/_targets.py simplebroker/project.py simplebroker/sbqueue.py \
  tests/test_target_redaction.py tests/test_queue_api_additions.py \
  tests/test_queue_move_cross_target.py
```

## Task 2: Make Instance Config Authoritative

### Task 2.1: Add red watcher strategy config coverage

Edit `tests/test_connection_config.py`. Keep the new instance-authority tests together
even though their integration subjects include watchers and generators.

Construct a real `QueueWatcher` with explicit values that differ strongly from defaults:

```text
BROKER_INITIAL_CHECKS = 7
BROKER_MAX_INTERVAL = 2.5
BROKER_BURST_SLEEP = 0.3
BROKER_JITTER_FACTOR = 0.01
```

Assert the default `PollingStrategy` contains those normalized values. This is a
permitted structural assertion because the watcher has no public tuning introspection
and a timing-based test would be slow and flaky.

Run the test first. It must fail with the current module defaults.

### Task 2.2: Add red generator batch-size behavior tests

In `tests/test_connection_config.py`, add shared tests using
`make_broker(broker_target, config=...)`, a real backend, and real messages.

Instance config case:

1. Create a core with `BROKER_GENERATOR_BATCH_SIZE=1` and auto-vacuum disabled.
2. Seed three messages.
3. Start an `at_least_once` claim generator without `batch_size=`.
4. Consume two messages, then close the generator.
5. Assert the first one-message batch committed and the second one-message batch rolled
   back. Exactly two messages remain pending.

Explicit override case:

1. Use instance batch size 1.
2. Seed four messages.
3. Pass an existing explicit per-call config with batch size 2.
4. Consume three messages and close.
5. Assert the first two-message batch committed, the current two-message batch rolled
   back, and exactly two messages remain pending.

Mirror the same instance-default and explicit-override assertions for `move_generator`;
the existing public per-call config remains in its contract. Assert source and
destination pending state after close. Do not test private counters.

Mark the behavior tests `@pytest.mark.shared` so SQL and Redis implementations must
agree.

### Task 2.3: Remove private module-config fallbacks from `DBConnection`

Edit `simplebroker/db.py`.

For `DBConnection.get_connection`, `_get_shared_connection`, and `cleanup`:

- change the internal default to `config: Mapping[str, Any] | None = None`;
- use `self._config` when no explicit override is supplied;
- normalize a supplied override once;
- pass the effective config through retry logging and cleanup logging;
- preserve public call compatibility for callers that already pass `config=`.

Do not change connection ownership, shared-session keys, retry counts, or cleanup order.

### Task 2.4: Remove private module-config fallbacks from `BrokerCore`

Edit `simplebroker/db.py`.

- `_log_ts_conflict`, `_do_write_with_ts_retry`, `_should_vacuum`, and
  `_vacuum_claimed_messages` use `self._config` directly.
- `claim_generator` and `move_generator` use `self._config` when `config is None` and
  `resolve_config(config)` only for an explicit per-call override.
- Keep the explicit generator parameter in the `BrokerConnection` protocol for
  compatibility; update it from an ellipsis/module-default shape to optional config.
- Do not add config parameters to materialized claim/move methods.

### Task 2.5: Remove private module-config fallbacks from watchers

Edit `simplebroker/watcher.py`.

Update `_create_strategy`, `_process_with_retry`, `_handle_handler_error`,
`_safe_call_handler`, `_dispatch`, `_move_all_messages`, and `__exit__`:

- no explicit config means `self._config`;
- an existing explicit protected-call override is normalized and honored;
- `_create_strategy()` continues to be called with no keyword so external subclasses
  that override the existing no-argument call pattern do not break;
- preserve protected method parameters where repository subclasses currently forward
  `config=`; change only their defaults and effective-config calculation.
- replace `QueueMoveWatcher._move_all_messages`'s read from the stray lowercase module
  `config` with `self._config`; remove `config = load_config()` if no valid use remains.

Update the `_create_strategy` override example so it does not teach a module-global
default.

### Task 2.6: Apply the same rule to Redis

Edit `extensions/simplebroker_redis/simplebroker_redis/core.py` and
`simplebroker/_backend_plugins.py`.

- `claim_generator` and `move_generator` use `self._config` by default.
- Explicit per-call config still overrides after normalization.
- Keep protocol and direct-core signatures aligned.
- Do not reload environment config during generator iteration.

### Task 2.7: Audit the touched classes

Run:

```bash
rg -n "config: .* = _config|config=_config" \
  simplebroker/db.py simplebroker/watcher.py \
  extensions/simplebroker_redis/simplebroker_redis/core.py \
  simplebroker/_backend_plugins.py
```

Review every remaining match. Constructor and module-function defaults may remain where
they are outside the verified bypass. The only expected production-code matches after
this task are public construction boundaries: `DBConnection.__init__`, `open_broker`,
`BrokerCore.__init__`, `BrokerDB.__init__`, `BaseWatcher.__init__`,
`QueueWatcher.__init__`, and `QueueMoveWatcher.__init__`. The watcher override example
and every private operational method must have no match. Redis generator and protocol
signatures must also have no `_config` default.

Then run a second audit for the separate lowercase global:

```bash
rg -n "(^|[^._])config\[" simplebroker/watcher.py
```

Every operational read in an instance method must resolve to `self._config` or a local
effective config. A separate `rg -n "^config = load_config" simplebroker/watcher.py`
must return no matches; remove that lowercase module global.

### Task 2 gates

```bash
uv run --extra dev pytest -n0 \
  tests/test_connection_config.py \
  tests/test_queue_config_defaults.py \
  tests/test_generator_methods.py \
  tests/test_exactly_once_delivery.py
uv run --extra dev ./bin/pytest-pg --fast \
  tests/test_connection_config.py tests/test_exactly_once_delivery.py
uv run --extra dev ./bin/pytest-redis --fast \
  tests/test_connection_config.py tests/test_exactly_once_delivery.py
```

The extension commands require their live services. If unavailable, report the exact
commands as not run; do not call them passed.

## Task 3: Preserve Peek Retry State on Handler Failure

### Task 3.1: Add a red single-message checkpoint test

Edit `tests/test_watcher.py`.

Use a real queue and real watcher thread:

1. Seed one message.
2. The message handler records the attempt and raises.
3. The error handler calls `watcher.stop(join=False)` and returns `True`.
4. Join the watcher thread.
5. Assert the handler ran once, the message is still pending, and `_last_seen_ts` did
   not advance.

Inspecting `_last_seen_ts` is acceptable here because the defect is specifically an
internal checkpoint and no public getter exists. The real queue state remains the main
behavioral proof.

The current code should fail by advancing the checkpoint after `_safe_call_handler`
swallows the handler exception.

### Task 3.2: Add a red batch ordering test

Using `peek=True` and `batch_processing=True`:

1. Seed `bad` followed by `later`.
2. Fail `bad`; the error handler requests stop and returns `True`.
3. Assert only `bad` was attempted, `_last_seen_ts` remains at the initial value, and
   both messages remain pending.
4. Start a fresh successful watcher from the same initial checkpoint and prove it can
   process `bad` and then `later` in order.

This proves the failure cannot be masked by a later successful ID. Synchronize with
events and thread joins, not sleeps.

### Task 3.3: Return dispatch success explicitly

Edit `simplebroker/watcher.py` as one vertical slice:

- `_safe_call_handler(...) -> bool`: `True` only when the main handler returns normally;
  `False` after a handled main-handler exception or error-handler failure.
- `_dispatch(...) -> bool | None` at the protected contract boundary. The base
  implementation returns `bool`: `False` for oversized messages, absent handlers, and
  safe handler failure; `True` only for a completed handler. The union preserves
  existing subclass annotations such as `examples/multi_queue_patterns.py`;
- `_try_dispatch_message(...) -> bool`: treat only `dispatch_result is False` as failure.
  Observe the timestamp for `True` and for legacy `None` returned by a protected
  subclass override;
- Remove the broad silent catch in `_try_dispatch_message`. Expected handler failures
  are now values; unexpected dispatch defects should reach the existing watcher retry
  loop and logs.
- Preserve `StopWatching` propagation.

`BaseWatcher` and `QueueWatcher` are subclassed outside this repository, and the public
watcher documentation explicitly shows `_dispatch()` overrides. Do not use ordinary
truthiness here: changing legacy `None` into failure would silently pin those watchers.
Add a small compatibility test subclass whose `_dispatch()` records the call and
returns `None`; prove its successful peek still advances the checkpoint.

### Task 3.4: Stop a peek batch at the first failed item

In `_process_peek_messages`:

- break immediately when `_try_dispatch_message` returns `False`;
- do not set `found_messages` for the failure;
- preserve checkpoint advancement for all successes before the failed item;
- leave consuming-mode behavior unchanged because the message is already claimed before
  dispatch there.

Do not auto-delete, auto-move, or add a retry counter.

### Task 3.5: Document poison-message behavior

Update the watcher docstring and the README safe peek example:

- a failed peek item remains pending and is retried on a later turn;
- later IDs are not processed past it by that watcher;
- `True` means continue watching, not acknowledge or skip;
- an application that wants to skip must explicitly delete or move the item;
- repeated poison failures eventually use watcher polling/backoff behavior, but no
  broker-level retry count is recorded.

### Task 3 gates

```bash
uv run --extra dev pytest -n0 \
  tests/test_watcher.py \
  tests/test_watcher_edge_cases.py \
  tests/test_watcher_burst_mode.py \
  tests/test_watcher_race_conditions.py \
  tests/test_watcher_thundering_herd.py
```

## Task 4: Make Opportunistic Vacuum Useful and Honest

### Task 4.1: Replace the misleading automatic-vacuum test with red behavior

Edit `tests/test_message_claim.py`.

Replace `test_automatic_vacuum_trigger`, which currently calls `vacuum()` manually,
with real public behavior tests. Keep the existing manual-vacuum test separate.

Add shared tests, using `make_broker` with real storage:

1. **Claim-triggered cleanup:** set auto-vacuum to 1, interval 4, threshold 0.5, and
   vacuum batch size 10. Write two messages (logical count 2), claim one (count 3), and
   assert one claimed row remains. Claim the second (count 4); assert claimed count is
   now zero without a manual vacuum call.
2. **Disabled means disabled:** use the same two writes and two claims with
   `BROKER_AUTO_VACUUM=0`; assert both claimed messages remain.
3. **No early cleanup:** set interval 4, threshold 0.1, and batch size 10. Write two
   messages (count 2), claim one (count 3), and assert it remains claimed. Write a third
   message (count 4); assert the due check removes the one claimed row and leaves two
   pending rows.
4. **Bulk-operation remainder:** set interval 4, threshold 0.1, and batch size 10.
   `insert_messages()` ten rows in one committed call; the one due check sees no
   claimed rows and leaves remainder 2. Claim one row (effective count 3) and assert it
   remains claimed. Claim a second row (count 4) and assert the check removes both.
   This proves the observable remainder without reading a private counter. The pure
   schedule test in Task 4.4 proves that one bulk `record()` yields only one check.

Use `get_overall_stats()`, `count_claimed_messages()`, `Queue.stats()`, or real backend
state. Do not read `_write_count`.

### Task 4.2: Prove generator transaction boundaries

Extend `tests/test_exactly_once_delivery.py` with a shared at-least-once generator test:

1. Configure auto-vacuum 1, interval 4, threshold 0.1, vacuum batch size 10, and
   generator batch size 2.
2. Seed two messages with one `insert_messages()` call (schedule count 2).
3. Consume one item from an at-least-once claim generator and close it. Assert the batch
   rolled back, both messages remain pending, and claimed count is zero.
4. Start a new generator and call `next()` twice. Then advance once more and assert
   `StopIteration`; that final advance lets the generator commit its full batch, record
   two committed mutations (count 4), and run maintenance.
5. Assert pending and claimed counts are both zero only after that commit-driving
   advance.

This is the critical invariant: no maintenance occurs while an at-least-once batch is
open.

### Task 4.3: Isolate automatic-maintenance failure

Add one focused SQLite-core fault-injection test:

- use a real SQLite runner/core configured with auto-vacuum 1, interval 1, threshold
  0.1, batch size 10, and logging enabled;
- write one message. Its due check sees no claimed rows and completes normally;
- patch only the backend plugin's `vacuum` hook to raise one `RuntimeError` at the
  automatic-maintenance boundary;
- claim the message. Assert the claim returns normally, the row is committed as claimed,
  and one traceback-bearing maintenance log is emitted;
- restore/allow the real vacuum hook, write a second message, and assert that the still-
  due schedule retries and removes the first claimed row while the second stays pending.

This is the one permitted backend-hook patch. Do not mock the operation or transaction.

### Task 4.4: Introduce the exact shared maintenance policy

Create `simplebroker/_maintenance.py`. It contains one small state object and one pure
predicate. Use names with this meaning; do not add an abstract base class or plugin
hook:

```python
class MaintenanceSchedule:
    def __init__(self, interval: int) -> None: ...
    def record(self, completed: int) -> bool: ...
    def mark_check_succeeded(self) -> None: ...

def vacuum_is_eligible(
    *, claimed_count: int, total_count: int, threshold: float
) -> bool: ...
```

Required state semantics:

- normalize the constructor interval with `max(1, int(interval))`. Existing zero and
  negative config values already cause a check on every write; preserve that behavior
  instead of adding unrelated strict config validation;
- `record(completed)` raises `ValueError("completed must be non-negative")` for a
  negative value, adds positive values, and returns whether the accumulated count is at
  least the interval; zero neither advances nor triggers;
- `mark_check_succeeded()` preserves `count % interval`; call it after a successful
  eligibility check, whether or not vacuum was eligible;
- do not call `mark_check_succeeded()` if stats or vacuum raises. The accumulated count
  remains due, so the next positive committed mutation retries;
- one call to `record()` can produce at most one check, even if a bulk operation crosses
  several intervals;
- `vacuum_is_eligible` returns false when `total_count == 0`; otherwise it returns true
  when the claimed ratio reaches the configured threshold **or** claimed count is
  greater than 10,000.

Add focused tests in `tests/test_maintenance_policy.py` for interval-minus-one, exact
interval, multi-interval modulo, zero/negative intervals normalized to one, zero and
negative completed counts, success reset, failure-by-omission, empty totals, exact
ratio (`claimed=2`, `total=10`, `threshold=0.2`), and the independent absolute rule
(`claimed=10_001`, `total=20_000`, `threshold=1.0`). These are state/pure-policy tests,
not mock-based backend tests.

This module must not import a backend, start a thread, sleep, log, read config, or
perform I/O. Do not expose the raw count; prove remainder behavior through subsequent
`record()` return values.

### Task 4.5: Trigger SQL maintenance after committed message mutations

Edit `simplebroker/db.py`.

Use one `_record_maintenance_activity(completed: int)` path. The authoritative SQL call
sites are:

| Operation | Recording point | Count |
|---|---|---|
| `write` | after `_do_write_transaction` succeeds | 1 |
| `insert_messages` | after insert transaction and timestamp refresh succeed | normalized record count |
| `broadcast` | after `_run_with_retry(_do_broadcast)` succeeds | returned queue-copy count |
| `claim_one`, `claim_many`, `move_one`, `move_many` | once in `_retrieve`, after `_execute_transactional_operation` has returned through its commit `finally` | returned row count |
| exactly-once claim/move generator | same `_retrieve` path, before yielding each committed row | 1 |
| at-least-once claim/move generator | immediately after the full batch `commit()` succeeds | committed batch length |

Do not also record in the public claim/move wrappers. `_retrieve` is the single SQL
owner for materialized and exactly-once retrieval, which prevents double counting.

Rules:

- zero-result operations do not advance the schedule;
- a batch crossing several intervals runs one check, not a loop of vacuums;
- `_record_maintenance_activity` returns immediately when auto-vacuum is disabled or
  `record()` is not due. When due, it reads stats, conditionally vacuums, and then calls
  `mark_check_succeeded()` only if that entire check returned normally;
- automatic cleanup uses `self._config` for enable, interval, threshold, and batch size;
- run stats and vacuum under the core's existing re-entrant lock;
- catch and log `Exception` only inside the optional automatic-maintenance boundary;
  never catch `BaseException`;
- use `logger.exception` only when `self._config["BROKER_LOGGING_ENABLED"]` is true;
- keep the schedule due after failure;
- manual `vacuum()` bypasses the scheduler and still propagates failure.

Rename `_write_count` to a maintenance-accurate name or replace it with the shared
policy. Do not leave a write-only name after claims and moves count.

### Task 4.6: Implement Redis parity

Edit `extensions/simplebroker_redis/simplebroker_redis/core.py`.

- Wire the existing unused interval/counter state into the same committed-activity
  policy.
- Use these authoritative recording layers: public `write` records one after
  `_write_message`; `insert_messages` records its normalized record count after the Lua
  insert succeeds; `_claim_rows` and `_move_rows` record their returned row counts once
  for one/many/exactly-once-generator callers; at-least-once generators record only
  after `_commit_claim_batch` or `_commit_move_batch` succeeds.
- Redis `broadcast` delegates to `insert_messages`, so `insert_messages` owns the count.
  Do not record again in `broadcast`.
- Guard schedule mutation, the due stats read, and automatic vacuum with the existing
  Redis core `_lock`, matching SQL. `MaintenanceSchedule` itself stays lock-free because
  its owning core supplies synchronization.
- Add a module logger in Redis core and use the same instance logging gate and
  traceback-bearing `logger.exception` behavior as SQL.
- Use `get_overall_stats()` for claimed and total counts.
- Apply the same eligibility rule as SQL: configured ratio threshold or more than 10,000
  claimed messages. Test the pure absolute-count predicate directly rather than seeding
  10,001 rows into every live backend suite.
- `vacuum()` removes at most the configured batch per queue, as it already does.
- Translate `redis.RedisError` from manual and automatic vacuum work into
  `OperationalError`; manual calls propagate it, automatic calls log and retry later.
- Place generator maintenance after `_commit_claim_batch` or `_commit_move_batch`, never
  after reservation and never after rollback.

Shared tests prove scheduling and generator boundaries against Redis. In
`extensions/simplebroker_redis/tests/test_redis_core_behaviors.py`, add one narrow live-
core test: write and claim one real message, then patch that core client's `zrange`
method to raise `redis.RedisError` when vacuum reads claimed IDs. Assert `vacuum()`
raises `OperationalError`, preserves the Redis error as `__cause__`, and leaves the
claimed row in place. Do not mock the shared maintenance policy or ordinary message
operations.

### Task 4.7: Correct the public contract

Update `README.md` and the `load_config()` environment-variable documentation in
`simplebroker/_constants.py`:

- rename “Background process” to “explicit or opportunistic maintenance”;
- define `BROKER_AUTO_VACUUM_INTERVAL` as successful message mutations between checks
  on one long-lived core;
- state that interval values less than 1 retain the historical “check every mutation”
  behavior and are normalized internally to 1;
- state that the schedule is per core: same-thread persistent handles may share a core
  and schedule, while separate thread-local cores, ephemeral handles, and processes do
  not;
- state that default ephemeral `Queue` operations and one-command CLI use generally do
  not reach the default interval;
- recommend persistent queues/open brokers for long-lived services and scheduled
  `broker --vacuum` for CLI/ephemeral workflows;
- explain that automatic maintenance is best effort and never changes a committed
  message operation's outcome;
- state that maintenance is synchronous with the triggering operation, not background;
- document backend cleanup size honestly: SQLite and Postgres currently drain the
  eligible claimed backlog in configured batches during one pass, while Redis removes at
  most one configured batch per queue per pass. Scheduling and eligibility are shared;
  per-pass deletion volume is not identical.

Do not call it a background phase anywhere in the touched sections.

### Task 4 gates

```bash
uv run --extra dev pytest -n0 \
  tests/test_maintenance_policy.py \
  tests/test_message_claim.py \
  tests/test_exactly_once_delivery.py \
  tests/test_vacuum_compact.py \
  tests/test_vacuum_lock.py
uv run --extra dev ./bin/pytest-pg --fast \
  tests/test_message_claim.py tests/test_exactly_once_delivery.py
uv run --extra dev ./bin/pytest-redis --fast \
  tests/test_message_claim.py tests/test_exactly_once_delivery.py \
  extensions/simplebroker_redis/tests/test_redis_core_behaviors.py
```

## Task 5A: Invoke Error Handlers Exactly Once

### Task 5A.1: Add the red callback test

Edit `tests/test_watcher_edge_cases.py`.

Use a real queue/watcher. The main handler raises. A custom error handler:

- accepts the documented three arguments plus an optional `config` keyword so the
  current probing path enters its body;
- records each call;
- requests watcher stop without joining;
- raises `TypeError` from inside its body.

Run through the real watcher thread and assert:

- the error handler was invoked exactly once;
- the callback's own error was logged once with the original handler error in context;
- the watcher exits and releases resources.

The current code must fail with two callback invocations.

### Task 5A.2: Bind the internal default once

Edit `simplebroker/watcher.py`.

- Import `functools.partial`.
- Make `config_aware_default_error_handler` require an explicit keyword config; remove
  its module-global fallback.
- Add one private binder used by both `QueueWatcher` and `QueueMoveWatcher`.
- If the selected handler is the internal config-aware default, bind
  `config=self._config` after base construction has normalized config.
- Custom handlers remain untouched and are always called with the documented three
  positional arguments.
- `_handle_handler_error` calls the handler once. Remove the nested TypeError fallback.
- If the error handler itself fails, log it with the original error and treat dispatch
  as failed; do not call it again.

Do not use `inspect.signature`, trial calls, arity heuristics, or a new callback protocol.

### Task 5A.3: Prove default logging config on both watcher types

Add parameterized behavior tests for `QueueWatcher` and `QueueMoveWatcher`:

- with instance `BROKER_LOGGING_ENABLED=0`, the internal default does not emit the
  handler error;
- with instance `BROKER_LOGGING_ENABLED=1`, it emits one handler error;
- the result is independent of the module `_config` captured at import.

Use `caplog`; do not patch the logger object.

## Task 5B: Make `is_running()` Truthful

### Task 5B.1: Add lifecycle state tests

Edit `tests/test_watcher.py`.

Cover:

1. New watcher: `is_running()` is false.
2. Background run: wait on an event or condition until the run loop is active, then
   assert true.
3. Graceful stop and join: assert false after cleanup.
4. Stop requested while a controlled cleanup barrier is still held: assert
   `is_running()` remains true until cleanup completes. Implement the barrier in a tiny
   test subclass overriding `_cleanup_thread_local`; do not change production cleanup
   merely to expose a test hook.
5. Fatal exit: use a small test subclass whose `_run_with_retries` raises a controlled
   `RuntimeError`; capture `threading.excepthook`, join, and assert the thread is dead and
   `is_running()` is false.
6. Synchronous fatal exit: `run_forever()` raises and clears running state in `finally`.
7. Cleanup failure in synchronous main-thread execution: a test subclass returns from
   `_run_with_retries` and raises from `_cleanup_thread_local`. Assert the cleanup error
   propagates, the prior real `SIGINT` handler is restored, and `is_running()` is false.

Do not assert that a thread is running immediately after `thread.start()`; wait for the
state event to avoid a race.

### Task 5B.2: Track active execution in `BaseWatcher`

Edit `simplebroker/watcher.py`.

- Add a private `threading.Event` initialized clear.
- Set it when `run_forever()` takes ownership of the run, before strategy startup.
- Use explicit nested `try/finally` ownership: the inner finalization attempts
  `_cleanup_thread_local()` and restores the signal handler in that cleanup attempt's
  `finally`; the outermost `finally` clears the active-run event. Thus cleanup failure
  cannot strand a process signal handler or a true running state.
- `is_running()` returns the active-run event. A requested stop does not make cleanup
  complete, so do not combine the event with `_stop_event`.
- Do not derive health solely from the weak thread reference; synchronous `run()` must
  report correctly too.
- Do not store or suppress fatal exceptions as part of this task.

One `BaseWatcher` instance remains a one-shot object after stop, matching its existing
stop-event lifecycle. Concurrent double-start policy is not expanded here.

## Task 5C: Reject Invalid Delivery Guarantees

### Task 5C.1: Add a shared red validation matrix

Edit `tests/test_exactly_once_delivery.py`.

For `delivery_guarantee="typo"`, cover all four public families with real queues:

- `read_many`;
- `read_generator` (advance once to execute the generator body);
- `move_many`;
- `move_generator` (advance once or materialize).

For every case assert:

- `ValueError` names the invalid value and both accepted values;
- source messages remain pending and in order;
- move destinations remain empty;
- no claimed/reserved batch is left behind;
- valid accepted values continue to work.

Add separate Queue-level no-side-effect cases against fresh, previously nonexistent
SQLite target paths: one invalid materialized call and first iteration of one invalid
lazy generator. Assert each path remains absent, proving validation occurred before
connection/schema creation. The shared state matrix above continues to use an
initialized real queue.

Mark the matrix shared. Do not assert generator construction raises; Python generators
normally execute on first iteration.

Add a second shared matrix in the same file against the direct connection returned by
`make_broker(broker_target)`. Cover `claim_many`, first iteration of
`claim_generator`, `move_many`, and first iteration of `move_generator`. Seed real
source messages and assert the same pending/claimed/destination invariants. This matrix
must bypass `Queue` so Queue-level validation cannot mask a missing SQL or Redis
backend-boundary validator.

### Task 5C.2: Add one delivery module

Create `simplebroker/_delivery.py` containing only:

- `DeliveryGuarantee = Literal["exactly_once", "at_least_once"]`;
- an immutable tuple of accepted values;
- `validate_delivery_guarantee(value: object) -> DeliveryGuarantee`.

The validator returns the narrowed value or raises `ValueError`. Keep error text stable
and direct. Do not add a public enum, dependency, or compatibility alias.

### Task 5C.3: Validate at the deepest backend boundary

Edit `simplebroker/db.py`, `simplebroker/_backend_plugins.py`, `simplebroker/sbqueue.py`,
and Redis core.

- Replace repeated Literal annotations with `DeliveryGuarantee`.
- Validate in Queue `read_many` and `move_many` before entering `get_connection()`.
  Validate in Queue `read_generator` and `move_generator` inside the generator body but
  before entering `get_connection()`, so normal lazy timing remains intact.
- Validate in SQL and Redis `claim_many`, `claim_generator`, `move_many`, and
  `move_generator` before any storage mutation.
- Branch on the validated result so the final `else` is safe, not a typo fallback.
- Queue validation prevents connection/schema side effects; backend validation remains
  authoritative for `open_broker()` and direct-core callers. This intentional two-level
  check is a cheap invariant guard, not duplicated business logic because both levels
  call the same validator.
- Keep Postgres source unchanged; it inherits SQL `BrokerCore` behavior.

### Task 5 gates

```bash
uv run --extra dev pytest -n0 \
  tests/test_watcher.py \
  tests/test_watcher_edge_cases.py \
  tests/test_queue_config_defaults.py \
  tests/test_exactly_once_delivery.py \
  tests/test_generator_methods.py \
  tests/test_queue_api_comprehensive.py
uv run --extra dev ./bin/pytest-pg --fast \
  tests/test_exactly_once_delivery.py tests/test_watcher.py \
  tests/test_watcher_edge_cases.py
uv run --extra dev ./bin/pytest-redis --fast \
  tests/test_exactly_once_delivery.py tests/test_watcher.py \
  tests/test_watcher_edge_cases.py
```

## Task 6: Update Public Documentation and Change History

### Task 6.1: Update README contracts in place

Edit only the sections directly affected:

- Embedding/targets: repr and error display are redacted; serialization is sensitive.
- Config: passed config is an instance snapshot and operational methods honor it.
- Watchers: error callback signature, exactly-once invocation, peek retry behavior,
  poison-message escape hatch, and `is_running()` meaning.
- Delivery selector: invalid strings raise `ValueError`; generator timing remains lazy.
- Vacuum: opportunistic long-lived-core policy and explicit maintenance for ephemeral
  or CLI use.

Do not restructure the 2,000-line README in this branch.

### Task 6.2: Add `[Unreleased]` changelog entries

At the top of `CHANGELOG.md`, before `5.1.1`, add:

- Fixed: credentials no longer appear in target/queue repr or cross-target errors.
- Fixed: passed instance config now controls watcher, generator, and maintenance paths.
- Fixed: failed peek handlers no longer advance past the message.
- Fixed: error handlers run once and watcher running state clears after fatal exit.
- Fixed: invalid delivery modes fail before mutation.
- Changed: automatic vacuum is claim/mutation-triggered for long-lived cores and is
  documented as opportunistic rather than background.

Do not edit version numbers or rewrite the already released `5.1.1` section.

### Task 6.3: Documentation consistency search

```bash
rg -n "Background process|automatic vacuum|AUTO_VACUUM_INTERVAL|is_running|error_handler|delivery_guarantee|serialize_broker_target" \
  README.md CHANGELOG.md simplebroker extensions/simplebroker_redis
```

Check each match for consistency. Do not broaden this into the separate exactly-once
wording project.

## Task 7: Run Focused, Cross-Backend, and Repository Gates

Run in this order. Stop on the first failure and diagnose it; do not rerun blindly.

### 7.1 Focused core gate

```bash
uv run --extra dev pytest -n0 \
  tests/test_target_redaction.py \
  tests/test_queue_api_additions.py \
  tests/test_queue_move_cross_target.py \
  tests/test_connection_config.py \
  tests/test_watcher.py \
  tests/test_watcher_edge_cases.py \
  tests/test_queue_config_defaults.py \
  tests/test_exactly_once_delivery.py \
  tests/test_generator_methods.py \
  tests/test_maintenance_policy.py \
  tests/test_message_claim.py \
  tests/test_vacuum_compact.py \
  tests/test_vacuum_lock.py
```

### 7.2 Full core correctness gate

```bash
uv run --extra dev pytest -m "not benchmark"
```

Do not add a coverage threshold or change benchmark policy in this work.

### 7.3 First-party backend parity

```bash
uv run --extra dev ./bin/pytest-pg --fast
uv run --extra dev ./bin/pytest-redis --fast
```

These require live services. Report unavailable infrastructure honestly.

### 7.4 Quality gates

```bash
uv run --extra dev ruff check \
  simplebroker tests bin examples .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run --extra dev ruff format --check \
  simplebroker tests bin examples .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run --extra dev mypy \
  simplebroker bin/release.py \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis \
  --config-file pyproject.toml
pg_test_files=()
while IFS= read -r file; do pg_test_files+=("$file"); done < <(
  find extensions/simplebroker_pg/tests \
    -type f -name '*.py' -not -path '*/__pycache__/*' | sort
)
redis_test_files=()
while IFS= read -r file; do redis_test_files+=("$file"); done < <(
  find extensions/simplebroker_redis/tests \
    -type f -name '*.py' -not -path '*/__pycache__/*' | sort
)
uv run --extra dev mypy \
  extensions/simplebroker_pg/simplebroker_pg "${pg_test_files[@]}" \
  --config-file pyproject.toml
uv run --extra dev mypy \
  extensions/simplebroker_redis/simplebroker_redis "${redis_test_files[@]}" \
  --config-file pyproject.toml
```

### 7.5 Existing local-only release decay gates

These stay local and do not become CI steps:

```bash
uv run --extra dev pytest -n0 examples
example_files=()
while IFS= read -r file; do example_files+=("$file"); done < <(
  find examples -type f -name '*.py' -not -path '*/__pycache__/*' | sort
)
uv run --extra dev mypy "${example_files[@]}" --config-file pyproject.toml
uv run --extra dev ./bin/release.py --check-shell-examples
```

The shellcheck helper may skip if shellcheck is not installed. If it skips, state that
explicitly. Do not modify the new shell examples as part of this plan.

### 7.6 Diff and scope gate

```bash
test -n "${IMPLEMENTATION_BASE:-}"
git cat-file -e "${IMPLEMENTATION_BASE}^{commit}"
git diff --check "$IMPLEMENTATION_BASE"
git status --short
git diff --stat "$IMPLEMENTATION_BASE"
git diff --name-only "$IMPLEMENTATION_BASE"
git diff "$IMPLEMENTATION_BASE"
git ls-files --others --exclude-standard
```

These comparisons include committed, staged, and unstaged implementation changes since
the fixed Task 0 base. Untracked files are listed separately because Git does not put
their contents in a normal diff; open every listed file in full. The expected new code
files are only `simplebroker/_delivery.py`, `simplebroker/_maintenance.py`, and
`tests/test_maintenance_policy.py` (plus this plan if it was never committed). Plain
`git diff` is not an adequate scope gate after bite-sized commits. If the shell was
restarted, export the recorded Task 0 hash first.

Confirm:

- no package version or lock file changed;
- no example test moved into CI;
- no exact-ID payload comparison was added;
- no background service or persistent maintenance schema was added;
- no public enum or new export was added;
- the user's shellcheck/example work remains intact;
- all new tests use temporary namespaces/files and close threads, generators, queues,
  cores, and runners in `finally` or context managers.

## Acceptance Invariants

### Secret safety

- No raw non-SQLite target password appears in `repr(BrokerTarget)`, `repr(Queue)`, or
  cross-target move errors.
- No backend-option value appears in those display surfaces, regardless of option key.
- Safe diagnostics still identify backend, host/path, queue, source, and destination.
- Serialized targets remain lossless and are documented as sensitive.

### Configuration

- A config passed to Queue, watcher, DBConnection, SQL core, or Redis core controls all
  operational paths owned by that instance.
- Explicit existing per-call generator config wins over instance config.
- No private instance method in the touched classes accidentally reads module `_config`
  when `self._config` is available.

### Peek watcher

- Handler success is the only event that advances a peek checkpoint.
- A legacy protected `_dispatch()` override returning `None` still advances; only an
  explicit false result blocks it.
- Failure of the main handler or error handler leaves the failed row retryable.
- A failed ID prevents later IDs in the same batch from advancing the checkpoint past it.
- Consuming watcher semantics remain consume-before-handler.

### Maintenance

- Automatic maintenance is counted per long-lived core across committed message
  mutations, including claims after producers stop.
- Eligibility preserves the existing ratio-or-more-than-10,000-claimed policy on SQL
  and Redis.
- Disabled automatic vacuum never deletes claimed rows.
- No automatic vacuum runs before an at-least-once generator batch commits.
- Automatic maintenance failure is logged and does not alter the committed operation's
  return/raise behavior.
- Manual vacuum still propagates failures.
- Redis, Postgres, and SQLite satisfy the shared behavior.
- Shared behavior means scheduling, eligibility, commit timing, and failure isolation;
  SQL drains the eligible backlog in batches while Redis performs one configured batch
  per queue per pass.
- Documentation never calls the mechanism a background process.

### Watcher and delivery contracts

- One main-handler failure invokes its error handler at most once, even when that error
  handler raises `TypeError`.
- `is_running()` is false before start and after graceful or fatal termination.
- Every invalid delivery selector fails before mutation; lazy generators fail on first
  iteration.
- Queue-level invalid selectors do not create a SQLite file/schema merely by opening a
  connection; direct SQL and Redis callers still receive backend-boundary validation.
- Both accepted delivery strings retain current semantics on every backend.

## Error and Rescue Registry

| Method/codepath | Failure | Exception | Rescued? | Action | Caller/operator sees |
|---|---|---|---|---|---|
| `BrokerTarget.__repr__` | Secret-bearing target/options | N/A (data exposure) | Prevented | Render safe target and redacted option values | Useful safe repr |
| `Queue._move_destination_name` | Different broker targets | `ValueError` | No | Raise before mutation with safe displays | Safe source/destination diagnostic |
| `serialize_broker_target` | Caller logs sensitive transport payload | N/A | Not preventable without breaking transport | Document as sensitive | Clear warning in docs/docstring |
| Explicit per-call config | Invalid known value | Existing parse exception | No | `resolve_config` raises before operation | Existing config error |
| Main watcher handler | User callback raises `Exception` | Original callback exception | Yes | Call error handler once; return dispatch failure | Error handler/log policy |
| Error handler | Callback raises `Exception`, including `TypeError` | Callback exception | Yes | Log once with original error; dispatch remains failed | One traceback when logging enabled |
| Unexpected dispatch internals | Non-callback defect | Specific runtime/storage exception | Existing watcher retry | Propagate to retry loop | Retry log, then fatal thread exception if exhausted |
| Peek batch | Dispatch returns false | No exception | Yes | Stop batch without checkpoint | Same ID retried later |
| `run_forever` | Retry exhaustion/fatal error | Original exception | No | Cleanup in finally; clear running state | Thread exception plus false health state |
| Delivery selector | Unknown value | `ValueError` | No | Reject before storage mutation | Invalid value plus accepted values |
| Automatic maintenance stats/vacuum | Backend or programming exception | `Exception` (not `BaseException`) | Yes at isolation boundary | Log traceback; keep schedule due; preserve committed result | Operation succeeds; maintenance warning if enabled |
| Manual vacuum | Backend failure | `OperationalError` or backend database error | No | Propagate | Explicit maintenance command fails |
| Redis vacuum command | Redis command/pipeline failure | `redis.RedisError` translated to `OperationalError` | Only for automatic path | Manual propagates; automatic logs/retries | Consistent backend error contract |

The automatic-maintenance catch-all is deliberate and limited: cleanup is an optional
post-commit side effect, and propagating its failure would falsely report the primary
message operation as failed. Every other new path names and preserves its exception.

## Failure Modes Registry

| Codepath | Failure mode | Error handling | Test | Silent? |
|---|---|---|---|---|
| Target repr | URI or conninfo password leaks | Safe representation | Sentinel parameter matrix | No |
| Target repr | Innocuous option key contains secret value | Redact all values | Option-name/value test | No |
| Queue repr | Quote-containing path corrupts repr | Use `!r` | Quote regression | No |
| Cross-target move | Error leaks options or mutates source | Safe display; validation before move | Four-method matrix | No |
| Watcher construction | Passed tuning ignored | Use instance config | Strategy field assertion | No |
| Claim/move generator | Instance batch size ignored | Effective config precedence | Real rollback state | No |
| Peek handler | Failure treated as success | Boolean dispatch result | Single-message checkpoint test | No |
| Peek batch | Later success hides earlier failure | Break at first failure | Two-message ordering test | No |
| Error callback | Internal TypeError causes second call | Bind default; single three-arg call | Side-effect count test | No |
| Watcher thread | Fatal exit leaves health true | Running event cleared in finally | Background fatal test | No |
| Delivery input | Typo chooses at-least-once branch | Shared validator | Four-entry-point matrix | No |
| Automatic vacuum | Consumer-only service never checks | Count committed claims/moves | Claim-triggered shared test | No |
| Automatic vacuum | Redis counter remains dead state | Wire common scheduling and eligibility policy | Shared Redis test | No |
| Automatic vacuum | Runs during open rollbackable batch | Trigger only after commit | Partial-batch rollback test | No |
| Automatic vacuum | Post-commit cleanup error escapes | Isolation boundary | Real DB plus one hook fault | No, logged |
| Ephemeral use | Counter never reaches interval | Honest docs and explicit command | Documentation review | No, documented limit |

No row is allowed to finish with no test, no error handling, and silent impact.

## Test Coverage Diagram

```text
SECRET DISPLAY
  BrokerTarget repr
    +-- URI password............................ parameter test
    +-- encoded password........................ parameter test
    +-- conninfo password....................... parameter test
    +-- arbitrary backend option values......... parameter test
  Queue repr
    +-- resolved target redaction................ real object test
    +-- SQLite compatibility..................... existing exact assertions
    +-- quote escaping........................... repr regression
  Cross-target move
    +-- move / one / many / generator............ real SQLite state test

CONFIG
  watcher -> default strategy.................... structural behavior test
  SQL generator -> batch commit/rollback......... shared integration test
  Redis generator -> reserved batch rollback..... shared + Redis suite
  explicit per-call config precedence............ shared integration test

WATCHER DISPATCH
  handler success -> checkpoint.................. existing + regression
  handler failure -> no checkpoint............... real thread test
  first batch item fails -> later untouched....... real thread ordering test
  error handler TypeError -> one call............. side-effect count
  default logging enabled/disabled................ caplog, both watcher types
  before/running/stopped/fatal health............. event-synchronized lifecycle

DELIVERY VALIDATION
  read_many invalid............................... shared state invariant
  read_generator invalid on next()............... shared lazy invariant
  move_many invalid............................... shared source/dest invariant
  move_generator invalid on next()............... shared source/dest invariant

MAINTENANCE
  due after committed mutations.................. shared integration test
  disabled........................................ shared integration test
  below interval.................................. shared boundary test
  partial generator rollback...................... shared transaction test
  completed generator batch....................... shared transaction test
  automatic failure isolation..................... real SQLite + hook fault
  Redis stats/vacuum parity........................ full Redis suite
```

The maintenance state/predicate receives the direct boundary tests required in Task
4.4. Delivery validation and every backend effect remain integration-tested through
the public/shared matrix. No helper test replaces shared backend behavior.

## Performance and Concurrency Review

- Target redaction and delivery validation are bounded by small in-memory values.
- Watcher dispatch adds a boolean return only; no additional I/O.
- Lifecycle state uses one event set and clear per run.
- Maintenance adds an integer update per committed message mutation. Backend stats are
  read only when the interval is due.
- SQL continues to serialize maintenance with the existing core lock and backend vacuum
  lock.
- Redis's due check scans queue stats at the configured interval. Do not scan all queues
  on every message.
- Automatic maintenance is synchronous. A due SQL pass can drain the full claimed
  backlog in batches and add latency to the triggering operation; Redis bounds one pass
  to one configured batch per queue. Document this instead of claiming equal pass cost.
- A bulk operation crossing multiple intervals runs one maintenance pass, avoiding an
  unbounded post-operation loop.
- Do not add timing thresholds to correctness tests. Existing benchmark policy remains
  unchanged.

The first scale limit is still backend vacuum/stat work on very large stores. This plan
does not claim to make maintenance free or continuous.

## Implementer Journey and Developer Experience Check

**Persona:** a skilled Python library engineer who has not worked on SimpleBroker,
SQLite claim semantics, the direct Redis backend, or this repository's release tools.
They are prone to testing mocks and call order instead of committed state.

Their intended journey is:

| Stage | Required context/action | Observable exit proof | Likely trap prevented here |
|---|---|---|---|
| Orient | Read Task 0 contracts and the product/config primer | Can identify pending vs claimed and ephemeral vs persistent core ownership | Treating read as physical delete or config as live global state |
| Red | Add one smallest regression and run its exact focused command | Failure points at the named old behavior, not fixture setup | Writing a broad mock test that is green before the fix |
| Green | Change the named owner at the deepest required boundary | Focused test passes with real SQLite state | Fixing only Queue while direct cores remain wrong |
| Parity | Run shared tests through Postgres and Redis helpers | Same state invariant passes on all available backends | Assuming SQL and Redis transaction timing is identical |
| Refactor | Remove duplication only after green | Shared validator/schedule has no backend I/O | Building a framework before the policy is known |
| Document | Update only named README/config/changelog sections | Searches show one consistent contract | Rewriting adjacent docs or exactly-once semantics |
| Gate | Run full, type, local-example, and fixed-base diff checks | All available gates pass; unavailable services are named | Missing protected example overrides or committed scope drift |

Target time to the first meaningful red test is under 30 minutes after Task 0. The plan
keeps every later slice independently diagnosable. A backend service outage can block a
parity gate, but not SQLite red-green work; record the skipped command verbatim rather
than replacing it with a mock.

Developer-experience invariants:

- no new public type, daemon, configuration key, or setup step;
- invalid delivery values fail at the caller boundary with accepted values in the
  error;
- watcher and maintenance errors preserve the original exception/traceback where they
  are reported;
- local Bash commands work with the repository host's Bash 3.2 (no `mapfile`);
- UI/design scope is absent; all affected surfaces are Python APIs, logs, docs, and
  release metadata.

## Rollout and Rollback

This is a library release with no schema migration or feature flag.

Rollout sequence:

```text
merge code + tests
      |
      v
run core and live backend gates
      |
      v
publish coordinated core/extension versions as release policy requires
      |
      v
verify repr redaction, invalid-mode errors, watcher health, and maintenance smoke
```

The implementation branch stays `[Unreleased]` and must not publish mismatched
extension metadata. Backend API v2 adds public delivery and maintenance contracts and
requires a coordinated minor release: `simplebroker==5.2.0`,
`simplebroker-pg==3.1.0`, and `simplebroker-redis==3.1.0`. Both extensions require
`simplebroker>=5.2.0`; the root `pg` and `redis` extras require their matching `3.1.0`
extensions. Let `bin/release.py` synchronize the root and extension lockfiles, and run
the repository's extension-baseline checks before tags. Version preparation is not
authorization to publish from the implementation task.

Backward-compatibility points:

- Repr text changes intentionally and must not be treated as a stable serialization
  format.
- Invalid delivery strings that previously selected at-least-once behavior now raise.
- Failed peek handlers retry instead of being skipped by that watcher.
- Automatic maintenance may delete claimed historical rows sooner on long-lived cores.
- Error handlers that relied on an undocumented `config=` callback argument no longer
  receive it. The documented three-argument contract is enforced.

Rollback is a normal git revert because storage formats do not change. If rollback is
needed after publishing, claimed rows already vacuumed cannot be restored; they were
already logically consumed and deletion-pending by contract. No pending row should be
lost by these changes.

## Common Wrong Turns

- Redacting only keys named `password` or `token`. Unknown option names can still hold
  credentials; redact every option value.
- Fixing `BrokerTarget.__repr__` but leaving Queue errors to format `.target` directly.
- Re-reading `load_config()` inside each operation. The contract is the instance
  snapshot, not live environment mutation.
- Removing protected `config=` parameters that repository subclasses already forward.
  Use optional effective config instead.
- Treating a caught handler exception as success because the error handler returned
  `True`. `True` means continue policy, not successful dispatch.
- Continuing a peek batch after failure. A later checkpoint would hide the failed ID.
- Adding sleep-based poison retry or a retry counter. That is a different policy layer.
- Catching `TypeError` around user callback execution to infer its signature.
- Implementing `is_running()` from only `_thread.is_alive()`. Synchronous run would be
  reported incorrectly.
- Validating delivery only in Queue wrappers. Direct `open_broker()` and backend callers
  must be protected.
- Running automatic vacuum before a deferred-commit generator batch commits.
- Letting optional maintenance errors escape after the primary transaction committed.
- Claiming that default ephemeral handles now have automatic cleanup. They do not share
  the in-memory schedule.
- Persisting maintenance counters or adding a background thread to make that claim true.
  That is out of scope and changes the operational model.
- Rewriting the README, renaming delivery guarantees, or addressing unrelated adoption
  findings while these five fixes are in flight.
- Adding examples to CI or modifying the user's shellcheck/example commit.

## Implementation Tasks

- [ ] **T0 (P1, human: 20m)** Record baseline, read contracts, and run focused green tests.
- [ ] **T1 (P1, human: 1.5h)** Add secret sentinels, implement safe target/Queue repr, and protect cross-target errors.
- [ ] **T2 (P1, human: 2h)** Make instance config authoritative in DBConnection, SQL core, watchers, protocol, and Redis core.
- [ ] **T3 (P1, human: 1.5h)** Return dispatch success and keep failed peek IDs retryable and ordered.
- [ ] **T4 (P1, human: 3h)** Implement committed-activity automatic maintenance for SQL and Redis with failure isolation and honest docs.
- [ ] **T5A (P1, human: 1h)** Bind the default error handler and guarantee one callback call.
- [ ] **T5B (P1, human: 1h)** Track real watcher run state through cleanup and fatal exit.
- [ ] **T5C (P1, human: 1.5h)** Add shared delivery validation across Queue, protocol, SQL, and Redis.
- [ ] **T6 (P2, human: 1h)** Update README and `[Unreleased]` changelog contracts.
- [ ] **T7 (P1, human: 2h plus service startup)** Run focused, full, backend, quality, local example, shellcheck, and diff gates.

## Decision Audit Trail

| # | Phase | Decision | Classification | Principle | Rationale | Rejected |
|---|---|---|---|---|---|---|
| 1 | Scope | Keep issues 1-5 only | Mechanical | YAGNI | User named the issue set and warned against drift | Unrelated assessment fixes |
| 2 | Security | Redact every backend-option value | Mechanical | Safe default | Secret key names are not enumerable | Key-name classifier |
| 3 | Config | Instance snapshot wins unless explicit per-call config exists | Mechanical | Explicit over clever | Matches constructor ownership and current public override seam | Reload environment per operation |
| 4 | Peek | Stop batch and retry failed ID later | Taste | Correctness first | Prevents permanent checkpoint skip and matches safe peek docs | Skip on `True`; new result enum |
| 5 | Maintenance | Improve long-lived opportunistic cleanup and document ephemeral limitation | Taste | YAGNI | Durable scheduling/background work would change architecture | Persistent counter; maintenance thread |
| 6 | Callback | Bind internal default and call public three-arg handler once | Mechanical | Explicit over clever | Removes side-effect duplication without signature introspection | TypeError probing; inspect.signature |
| 7 | Lifecycle | Use a private running event, no public state enum | Mechanical | Smallest complete fix | Covers sync and background execution | Thread-only check; lifecycle API |
| 8 | Delivery | One validator called at Queue and backend boundaries | Mechanical | DRY | Prevents schema side effects and protects direct callers | Wrapper-only validation; public enum |
| 9 | Packaging | Keep implementation unreleased; coordinate core, PG, and Redis minor releases for backend API v2 | Mechanical | Installability | Extensions declaring API v2 must require the first core release that provides it | Patch-only bumps; publish mismatched metadata |

## Review Completion Summary

| Area | Result |
|---|---|
| Scope | Five findings retained; unrelated assessment work explicitly excluded |
| Architecture | No schema, new runtime package, daemon, or public-type expansion |
| Security | All public display surfaces safe; lossless serialization explicitly sensitive |
| Errors | Handler, maintenance, lifecycle, and validation paths named and tested |
| Tests | Real storage and shared backend fixtures; patches limited to two named fault boundaries |
| Performance | O(1) per-mutation bookkeeping; stats only at due interval |
| Rollback | Git revert; no stored-format migration |
| Parallelization | Sequential due shared core files and parity tests |
| Fresh-eye revision | 5 scope, 6 engineering, and 5 DX findings corrected; engineering and DX confirmation passes cleared |
| Unresolved decisions | None |

## GSTACK REVIEW REPORT

| Review | Initial result | Corrections applied | Confirmation |
|---|---|---|---|
| CEO/scope | 5 material findings | Core sharing, 10,000-row rule, backend cleanup volume, lifecycle meaning, stray watcher config global | Cleared after revision |
| Engineering | 6 material findings | Protected dispatch compatibility, interval bounds, exact recording ownership, validation side effects, executable maintenance arithmetic, nested cleanup | `CLEARED` on re-read |
| Developer experience | 5 material findings | Union return type, direct-core red tests, constants file ownership, CI/local mypy parity, fixed-base diff | `CLEARED` on re-read |
| Direct audit | Duplicate/optional instructions and release-floor risk | Exact maintenance policy, Bash 3.2 commands, untracked-file scope check, coordinated Redis floor | Cleared |
| Outside Codex voice | Unavailable | Local Codex CLI/model mismatch and one local MCP connection failure; not reported as a pass | Recorded as unavailable |

**VERDICT:** CEO + ENG + DX CLEARED. Ready to implement sequentially with red-green TDD.

NO UNRESOLVED DECISIONS
