# Changelog

All notable changes to SimpleBroker will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [7.4.0] - 2026-08-24

### Added

- Added public `resolve_isolated_config()` and immutable `ResolvedConfig` for
  embedders that need a complete configuration independent of ambient
  `BROKER_*`. The marker remains ambient-free through Queue, project, watcher,
  runner, broker, and dump/load configuration boundaries. `ResolvedConfig`
  guarantees all canonical keys and can carry opaque extra keys;
  `resolve_isolated_config()` remains strict by default and adds
  `preserve_unknown=True` as an explicit extension-key opt-in.
- Added package-root `snapshot_config()` so applications can capture the
  current ambient configuration once and reuse the same `ResolvedConfig`
  across handles.
- Added package-root `MovedMessage`, a `TypedDict` for the existing ordinary
  dictionaries returned by high-level `Queue.move()`. Literal-sensitive
  overloads now narrow read, peek, and move results without changing runtime
  dispatch or record shapes.

### Changed

- Configuration now snapshots at each public handle or invocation boundary
  instead of package import. New Queue, watcher, broker, discovery, load,
  command, CLI, and direct-runner calls can observe intentional environment
  changes between calls; existing handles and active operations retain their
  original snapshot through lazy session and backend creation. This timing
  change is compatibility-relevant for long-running API users.
- Bare `Queue.delete()` remains the intentional queue-wide physical delete,
  while explicit `Queue.delete(message_id=None)` now raises `TypeError` before
  mutation. An unhandled `KeyboardInterrupt` reaching the outer CLI wrapper
  now returns `130`; normal watch stop and closed-pipe handling remain success.
- Direct `PollingStrategy(stop_event)` construction now uses the canonical
  10-microsecond `BROKER_BURST_SLEEP` default. Explicit constructor values and
  configured watcher values remain unchanged.
- Advanced the coordinated first-party extensions to `simplebroker-pg` 3.9.0
  and `simplebroker-redis` 3.9.0. Both extensions now require
  `simplebroker>=7.4.0`, and the root `pg` and `redis` extras require the new
  extension versions.

### Fixed

- `BaseWatcher.stop()` and watcher startup now serialize cleanup ownership, so
  a stop racing `run_forever()` cannot close strategy and Queue resources in
  both threads. A live run retains cleanup ownership even when a joining stop
  times out; cleanup failure remains retryable by a later stop or finalizer.
- Range-filtered one-item `Queue.read()` and `Queue.peek()` now close the
  generators they create immediately on a value or exhaustion instead of
  relying on interpreter-specific generator finalization timing. The matching
  `Queue.move()` path uses the same deterministic cleanup rule.
- Ordinary relative SQLite CLI targets now fail closed when their physical
  containment cannot be established. Command dispatch, status, and vacuum use
  the same canonical target that passed containment; explicit absolute and
  trusted project-config targets retain their existing authority.
- Recognized JSON mode is now preserved through post-parse global validation,
  including cleanup, vacuum, and compact conflicts. Ordinary alias-removal and
  initialization failures now use the shared `simplebroker: error:` prefix;
  the command-specific `broker load:` dialect is unchanged.

### Removed

- Removed the POSIX group/other-readable `.broker.toml` warning. SimpleBroker
  still warns, without exposing the value, for recognized inline passwords;
  effective file and directory permissions and ACLs remain operator-owned.
- Removed the unused `Message` tuple from `simplebroker.watcher`. Direct
  `simplebroker.watcher` imports are outside [SB-API-1]'s supported import
  surfaces; package-root and `simplebroker.ext` watcher APIs are unchanged.

### simplebroker-pg 3.9.0

- Synchronized with SimpleBroker 7.4.0 and raised the core dependency floor to
  `simplebroker>=7.4.0`. Backend creation now preserves the core-owned
  configuration snapshot instead of re-reading ambient configuration.

### simplebroker-redis 3.9.0

- Synchronized with SimpleBroker 7.4.0 and raised the core dependency floor to
  `simplebroker>=7.4.0`. Backend, pool, and runner creation now preserve the
  core-owned configuration snapshot instead of re-reading ambient
  configuration.

## [7.3.1] - 2026-08-13

### Changed
- Test-only changes

## [7.3.0] - 2026-08-13

### Changed

- Dump v1 load now requires its existing `last_ts` header field and restores
  it as a monotone broker-global allocation floor after message replay. Dump
  now samples that value once and excludes messages above it, making the header
  an inclusive live-export bound. Current dumps emit the canonical 19-digit
  string; load accepts the documented integer and exact-string forms. Dumps
  produced by older versions while writes were active may need to be recreated
  with 7.3.0 before loading if their rows exceed the recorded header bound.
- Load now warns when a dump watermark is physically ahead of local wall time.
  It refuses skew beyond the configurable 300-second default before mutation;
  `load --force` warns and bypasses only that refusal. The matching Python
  surface exposes `DumpClockSkewWarning` and keyword-only `force`/`config`.
- Advanced the exact backend-plugin handshake to API v7 with required
  `BrokerConnection.advance_last_timestamp()`. Coordinated package versions
  are SimpleBroker 7.3.0, `simplebroker-pg` 3.8.0, and
  `simplebroker-redis` 3.8.0.

### Fixed

- Invalid recognized `BROKER_*` values no longer crash package import before
  the CLI can report them. Strict config loading now raises public
  `simplebroker.ext.InvalidConfigError` with safe key/source/expected metadata;
  the CLI emits one redacted plain-text diagnostic and exits `1` before parser
  behavior or broker side effects. It never continues with fallback defaults.
- Timestamp-floor restore now fails loudly when durable metadata is missing or
  remains below the requested floor, avoids a redundant initial read, and
  distinguishes known failure from an outcome-ambiguous attempted write/read.

## [7.1.0] - 2026-08-11

### Changed

- `ActivityWaiter.close()` is now a terminal, idempotent public contract. The
  first call marks the waiter closed before cleanup, attempts every
  independently safe action after an ordinary failure, raises the first
  failure with later failures retained as ordered exception notes, and makes
  every later close a no-op even when the first call raised. Activity waiters
  remain close-only leaf resources; runner `shutdown()` remains the optional
  stronger verb for owned shared or process-wide substrate.
- Advanced the exact backend-plugin handshake to API v6. Published coordinated
  SimpleBroker 7.1.0, `simplebroker-pg` 3.6.0, and `simplebroker-redis` 3.6.0
  artifacts with extension and root-extra floors aligned.

### Fixed

- PostgreSQL and Redis/Valkey waiters now attempt registry release after an
  ordinary unregister failure. Redis multi-queue waiters now own a composite
  terminal guard, attempt every child once on the first close, and retain
  nested child cleanup failures in execution order.

## [7.0.1] - 2026-08-10

### Fixed

- Fresh SQLite databases and SimpleBroker phase-lock/status companion files
  now retain their ordinary creation modes filtered by the process umask
  instead of being rewritten or created as `0600`. This preserves
  operator-owned sharing policy; existing database and stable lock-file modes
  remain unchanged.

## [7.0.0] - 2026-08-10

### Changed

- **Breaking:** SimpleBroker-owned JSON now emits
  message IDs and broker high-water values as exact 19-digit ASCII decimal
  strings instead of JSON numbers, because JavaScript does not ensure integer 
  stability for numbers larger than 2^53-1, which is smaller than the 64-bit 
  numbers used for message IDs. This covers CLI `timestamp` and
  `last_timestamp`, dump-v1 `id` and `last_ts`, and the public watcher JSON
  handler. Python queue/connection APIs, callbacks, status dictionaries, and
  backend/storage protocols remain integer-valued. Dump v1 keeps accepting
  legacy integer IDs as input, while new dumps use strings. The new public
  `simplebroker.format_message_id(...)` helper gives embedders the same wire
  representation without duplicating validation or padding rules.
- Bumped synchronized patch versions `simplebroker-pg` 3.5.2 and
  `simplebroker-redis` 3.5.2. Both extensions now require
  `simplebroker>=7.0.0`; the root `pg` and `redis` extras require those patch
  versions. Their backend APIs and runtime behavior are unchanged.

### simplebroker-pg 3.5.2

- Synchronized with SimpleBroker 7.0.0. Raises the core package
  floor to `simplebroker>=7.0.0`; PostgreSQL backend behavior is unchanged.

### simplebroker-redis 3.5.2

- Synchronized with SimpleBroker 7.0.0. Raises the core package
  floor to `simplebroker>=7.0.0`; Redis/Valkey backend behavior is unchanged.

## [6.0.2] - 2026-08-06

### Fixed
- Timestamp bounds now use integral seconds only. Fractional components in bare
  numeric, suffixed numeric, and ISO-8601 spellings are rejected with guidance
  to use integer `ms`, integer `ns`, or a native hybrid message ID. Numeric
  bounds also require decimal-digit-only spellings; forms such as
  `1_705_329_000` or `+99999999999` can no longer select the wrong unit because
  punctuation was counted during unit classification.
- SQLite `--cleanup` now attempts the configured main database plus its
  `-journal`, `-wal`, `-shm`, `.lock`, `.status`,
  `.status.tmp.<pid>.<time_ns>`, and `.vacuum.lock` state. It remains
  deliberately destructive and non-atomic. Concurrent SQLite storage outcomes
  are undefined per SQLite upstream, while validation, exit, quiet, and
  partial-failure diagnostics remain defined.
- Made shared SQL timestamp resynchronization use guarded high-water advance,
  so a stale PostgreSQL repair cannot overwrite a larger value committed by a
  concurrent allocator. Repair diagnostics and the local generator cache now
  reflect the durable surviving high-water.
- Made closed stdout a clean exit for default-buffered short `read --all`
  batches and exact `read` / `move` output, with active at-least-once batches
  rolled back before commit. Quiet mode no longer hides errors, `init` rejects
  explicit `-d` / `-f` targets instead of ignoring them, and JSON errors now
  report explicit retryability through the documented code/key inventory.
  Closed-pipe truthfulness requires per-record flushing and can reduce bulk CLI
  drain throughput compared with exit-time buffering.
- Enforced flat alias creation in either mutation order across SQL and Redis.
  Alias names and canonical targets now use the ordinary queue-name grammar;
  Redis validates live graph state and publishes its version atomically. Old
  invalid rows remain available for one-hop inspection and removal. The Redis
  delete-all partial-completion floor is now explicit and regression-tested;
  Redis client failures during alias mutation now raise the same
  `OperationalError` family as sibling Redis operations.
- Validated `Queue` names at construction, preserved line context for oversized
  JSON integer tokens during load, honored an explicit watcher
  `after_timestamp=0`, and made every backend reject non-string bodies with
  `MessageError` before mutation.
- Repaired the standalone async SQLite example so fresh and incorrectly
  v5-stamped databases contain the v5 pending-queue index, removed the
  unsupported `EXTRA` sync-mode example, and documented the distinct string
  percentage and numeric fraction forms of `BROKER_VACUUM_THRESHOLD` plus its
  absolute backstop above 10,000 claimed rows.
- Bound delivery clause ranges and affected spec evidence citations to
  executable semantic manifests, including routine versus opt-in cross-backend
  dump/load suites and the real SQLite broadcast lock owner.
- Unified the PostgreSQL and Redis pytest wrappers on one argument-routing and
  owned-container cleanup path, enabled the CI Hypothesis profile in the Linux
  coverage lane, and stopped excluding all `__repr__` and bare `pass` lines
  from coverage.

### Changed
- Bumped the synchronized first-party `simplebroker-pg` and
  `simplebroker-redis` packages to 3.5.1, raised both extension core floors to
  `simplebroker>=6.0.2`, and raised the root optional backend floors to those
  extension versions.

### Documented
- Clarified the SQLite filesystem access boundary: cross-user deployments must
  grant every intended writer access to the main database, all SQLite and
  SimpleBroker companion files, and the containing directory on POSIX and
  Windows. SimpleBroker does not automatically provision or preserve a
  group-sharing policy.
- Recorded the SimpleBroker 6.0.0 removal of the unsupported
  `simplebroker.helpers` compatibility module; import supported helpers from
  their documented public modules instead.

### simplebroker-pg 3.5.1
- Synchronized release for SimpleBroker 6.0.2. Shared SQL timestamp repair now
  preserves a higher high-water committed concurrently by another PostgreSQL
  connection.

### simplebroker-redis 3.5.1
- Synchronized release for SimpleBroker 6.0.2. Alias creation now validates
  and publishes the flat live graph atomically, and delete-all reports truthful
  partial completion if a later queue cannot be removed.


## [6.0.1] - 2026-08-04

### Fixed
- Corrected the published shell workers so processing and acknowledgement
  failures stop before newer messages, broker failures remain visible instead
  of becoming successful pipeline exits or "No new messages," and only CLI
  exit `2` is treated as an empty queue. `safe_worker.sh` now polls one message
  at a time and requires an explicit `PROCESS_TASK`; `resilient_worker.sh` now
  rejects corrupt checkpoints and successful peeks with empty or invalid JSON.
  Both preserve trailing-newline payloads and reject NUL before processing or
  acknowledgement because Bash variables cannot represent NUL. The workers now
  require jq 1.7+ so 64-bit JSON IDs are not rounded, accept and canonically
  target the full stored-ID range, stream bodies to handler stdin instead of
  argv, and do not use the resilient worker's informational checkpoint as a
  durable selection offset. Signal-time checkpoint failures also exit nonzero
  explicitly.
- Made `tests/test_worker_examples.py` collect on Windows by guarding the
  Unix-only effective-user-ID probe.
- Narrowly repair settled coverage shards whose exact installed schema contains
  duplicate identical installed-version rows; conflicting versions and schema
  shapes remain fatal.

### Documented
- Corrected the README newline and dead-letter recipes, made specification links
  work on PyPI, and documented the core-extension API-version handshake on both
  first-party extension package pages.
- Restructured the documentation around three purposes: the README now
  carries the project theory, rapid CLI/API onboarding, and a router to
  owning documents (2,520 → 818 lines). New how-to guides hold the depth:
  `docs/guides/python.md` (advanced API, embedding, sidecar, reactor),
  `docs/guides/configuration.md` (full `BROKER_*` catalog, project
  scoping, security, tuning), and `docs/guides/backends.md`
  (Postgres/Redis, backend authoring). Contributor and release process
  moved to `CONTRIBUTING.md`; storage internals to
  `docs/implementation/09-storage-schema-and-claim-lifecycle.md`; long
  shell recipes to `examples/` (new `safe_worker.sh`;
  `resilient_worker.sh` hardened: atomic trap checkpoint,
  stop-on-delete-failure, validated `BATCH_SIZE` bound). Performance
  numbers replaced with measured values (~1,700 ops/s mixed API use,
  ~30,000 ops/s optimized, ~20 ops/s CLI). Delivery claims now cite the
  exact spec clauses (`[SB-DELIVERY-1]`, `[SB-DELIVERY-5]`) instead of
  bare "exactly-once." No product behavior changed.

## [6.0.0] - 2026-07-31

### Breaking
- `simplebroker.commands`: options on `cmd_read`, `cmd_peek`, `cmd_move`,
  `cmd_watch`, and `cmd_list` are now keyword-only, matching `cmd_write`,
  `cmd_broadcast`, and `cmd_rename`. `cmd_peek` and `cmd_move` previously took
  nine positional parameters, three of them consecutive booleans, so
  `cmd_peek(db, "q", True, False, True, None, None, None, None)` was legal and
  unreadable. No parameter was renamed, removed, reordered, or given a new
  default; only the binding changed, and callers already passing keywords are
  unaffected.

  This lands in the release that first makes the command layer normative
  (`[SB-API-10]`), so the specified shape is the intended one rather than an
  inherited accident. `[SB-API-10]` describes each `cmd_*` as the programmatic
  equivalent of a CLI subcommand, and CLI flags are named rather than
  positional.

### Added
- Re-exported project-config discovery helpers on `simplebroker.ext`:
  `find_project_config`, `project_config_path_for_directory`, and
  `resolve_project_target` (same objects as `simplebroker.project`, now also
  listed in `project.__all__`).

### Fixed
- Exact message-ID validation now gates on `str.isdecimal()` instead of
  `str.isdigit()`. Digit-like characters that `int()` cannot parse (for
  example superscript `²`) previously passed the digit check and surfaced as
  a raw `int()` `ValueError`; they now raise the canonical
  `invalid message ID: expected exactly 19 digits within range`. Accepted
  input is unchanged: integers, and strings of exactly 19 decimal digits
  (including non-ASCII decimal digits) after surrounding whitespace is
  stripped. The same swap was applied to the unit-suffix and ISO-8601
  heuristics so each gate accepts exactly what its conversion accepts.
- Timestamp parsing now folds non-ASCII decimal digits to ASCII before any
  parsing, so a value's script no longer changes how it is interpreted.
  `int()`/`float()` accept Unicode decimal digits but
  `datetime.fromisoformat()` does not, so an 8-digit value previously took the
  compact-`YYYYMMDD` path in ASCII and fell through to the unix-seconds path in
  any other script — `20240115` and `٢٠٢٤٠١١٥` named instants ~54 years apart.
  They now select the same instant.
- Made ordinary Redis `write()` advance generated-ID high-water and insert the
  row in one fenced Lua operation. Stale candidates now retry without mutation,
  and timestamp resynchronization can no longer overwrite high-water backward.

### Changed
- Fixed `BrokerConnection.canonicalize_queue` to apply the alias sigil rule.
  It previously resolved *plain* names as aliases, so `canonicalize_queue("x")`
  returned the alias target when an alias named `x` existed — contradicting the
  documented rule that a plain name always means the literal queue, and able to
  redirect work intended for a literal queue that shares a name with an alias.
  Now a plain name passes through unchanged, `@name` resolves to its target,
  and an empty or undefined alias raises `ValueError`. The CLI was already
  correct and is unaffected. Backend authors implementing this method must
  match the sigil rule; the shared implementation is
  `simplebroker._aliases.resolve_queue_operand`, which the CLI command layer
  and every first-party backend now share.
- Reserved message ID `0` as the lower-bound/checkpoint origin for new exact
  insertion. Exact selectors still accept legacy zero rows for recovery, but a
  dump containing ID `0` must be intentionally re-IDed before restore.
- Bumped the synchronized first-party `simplebroker-pg` and
  `simplebroker-redis` packages to 3.5.0, raised both extension core floors to
  `simplebroker>=6.0.0`, and raised the root optional backend floors to those
  extension versions.

### Documented
- Keyed the generated Ruff suppression index on `path::qualified_symbol`
  instead of `path:line`, and revised the `[DOM-10.1.1]` sentence granting the
  derived index its scope. Line keying churned on any edit above a directive
  while staying blind to a suppression migrating between functions at a
  constant group cardinality; symbol keying inverts both. Developer tooling
  only — no product behavior, API, or CLI change.
- Added `[SB-DELIVERY-8]` (message and queue-name constraints): queue-name
  grammar and length, UTF-8 body requirement, the 10 MB default size limit and
  `BROKER_MAX_MESSAGE_SIZE`, and the per-backend NUL divergence (PostgreSQL
  rejects raw NUL; SQLite and Redis round-trip it). These were README-only.
- Corrected the alias surface in `[SB-API-12]`, `[SB-OPS-5]`, and the agent
  kernel. Aliases resolve in the CLI **and** in `simplebroker.commands`, and
  are managed through the public `BrokerConnection` alias methods; only
  `Queue` is literal-only. The kernel previously said "CLI-only".
- Stated in `[SB-CLI-2]` that a data-bearing read writes the body to stdout,
  never stderr; recorded in `[SB-BCAST-4]` the limits of broadcast atomicity
  (unexpected script-runtime errors, pattern target resolution) and that a
  no-op broadcast leaves the id high-water unchanged; noted in `[SB-ID-4]`
  that an id with no room above it cannot be inserted; added patterned
  broadcast to `[SB-SELECT-3]` as a source of ids arriving behind a bound.
- Reworded the `[SB-ID-1]` uniqueness paragraph. It had restated a uniqueness
  constraint through invented framing ("coexistence rule", "tombstone ledger")
  and left global monotonicity implied. It now scopes monotonic allocation to
  broker-generated ids and states that caller-supplied exact ids are bounded
  only by range and uniqueness, so stored ids are not ordered by write time —
  the premise `[SB-SELECT-3]` and `[SB-ID-5]` both rely on.
- Bound `[SB-BCAST-1]`'s empty-string-body promise to a firing shared test
  across the selector-free, exact, and `create_missing` paths; it was
  previously an unbound obligation.
- Corrected `[SB-ID-4]` (and the README/kernel restatements), which described
  exact-ID strings as "19 ASCII digits". Non-ASCII decimal digits are accepted
  and whitespace is stripped; both are now stated.
- Corrected `[SB-ID-3]` (and the kernel restatement), which said
  `Queue.last_ts` is `None` on a fresh handle. The first read lazily fetches
  backend high-water (`0` on an empty target); `None` means the fetch failed.
- Promoted the Python library / embedding API surface to
  `docs/specs/16-python-library-api.md` (`[SB-API-1]`…`[SB-API-12]`): package
  root, `simplebroker.ext`, command layer, targets, library packaging, and
  cross-links to the CLI and vertical specs.
- Promoted residual queue/broker operations to
  `docs/specs/17-ops.md` (`[SB-OPS-1]`…`[SB-OPS-6]`): implicit queues,
  metadata, physical delete, rename, aliases, and vacuum.
- Completed product-documentation authority cutover: every registered product
  concern family is `canonical-spec` (`docs/specs/10-cli.md` …
  `17-ops.md`). Root README is human entry/catalog with links; ownership
  rules live in `docs/README.md` and `docs/specs/product-section-registry.md`.

### simplebroker-pg 3.5.0
- Synchronized release for SimpleBroker 6.0.0; the core floor rises to
  `simplebroker>=6.0.0`. The extension's own API is unchanged, and runtime
  backend behavior is unchanged beyond shared core exact-ID reserved-zero
  admission.

### simplebroker-redis 3.5.0
- Synchronized release for SimpleBroker 6.0.0; the core floor rises to
  `simplebroker>=6.0.0`. The extension's own API is unchanged. Ordinary
  generated
  `write()` now advances high-water and inserts in one fenced Lua operation.
- `canonicalize_queue` now shares the core sigil-rule implementation, so a
  plain name is literal and only `@name` resolves (matching SQLite and the
  CLI). It previously resolved plain names as aliases.

## [5.6.2] - 2026-07-30

### Documented
- Added the program-theory account used for product-scope and design judgment,
  including concept ownership, durable non-goals, falsifiers, revision
  provenance, and tried alternatives. Exact behavior remains with the
  product-section registry's winning README/spec owner.
- Clarified that SimpleBroker's boundary is ownership, not host count:
  optional shared backends may serve cross-host clients while SimpleBroker
  remains a queue primitive rather than a broker fleet, task framework, or
  application control plane.

### Changed
- Consolidated duplicate internal timestamp decoding, backend key-material
  freezing, and `read` / `peek` selector validation paths without changing
  public APIs, timestamp IDs, queue identity, CLI diagnostics, or exit codes.
- Replaced weak or misleading tests with production-path coverage for watcher
  retry cleanup, SQLite connection failures, and public `Queue.write`
  message-size behavior. The size contract now runs against SQLite,
  PostgreSQL, and Redis at the exact byte limit, above the limit, with
  multibyte UTF-8, and with lone surrogates.
- Preserved transaction-owner progress when one SQLite runner is shared by
  several threads. Foreign reads and writes now wait without blocking the
  owner's commit or rollback path, and fail within the configured busy timeout
  if an orphaned owner cannot settle. The reference reactor also allocates
  output IDs outside its sidecar transaction, removing an undocumented
  same-core transaction join without changing replay or delivery guarantees.
- Separated process-session bookkeeping from backend-specific core
  construction, removing the internal circular import while preserving public
  behavior. Concurrent first use now publishes one shared runner, failed core
  construction releases its checkout, and shutdown drains in-flight core
  creation without retaining late runners.
- Closed bounded move iterators on every early-return path so filtered
  one-message moves release transactional resources immediately.
- Made Redis zero-target all-queue broadcasts state-neutral: they no longer
  advance persisted `last_ts` when no message is inserted.
- Corrected the copyable `MultiQueueWatcher` example so a queue without a
  queue-specific error handler uses the configured default instead of
  inheriting the preceding queue's handler.
- Bumped the synchronized first-party `simplebroker-pg` and
  `simplebroker-redis` packages to 3.3.2, raised both extension core floors to
  `simplebroker>=5.6.2`, and raised the root optional backend floors to those
  extension versions.

### simplebroker-pg 3.3.2
- Synchronized patch release for SimpleBroker 5.6.2. Runtime backend behavior
  is unchanged; shared conformance coverage now verifies the public
  message-size contract through PostgreSQL.

### simplebroker-redis 3.3.2
- Synchronized patch release for SimpleBroker 5.6.2. Zero-target all-queue
  broadcasts now leave persisted `last_ts` unchanged; shared conformance
  coverage verifies the public message-size contract through Redis.

## [5.6.1] - 2026-07-28

### Added
- Added Python exact-target provisioning through
  `broker.broadcast(message, queue_names=..., create_missing=True)`. The
  default remains existing-only. Creation validates and deduplicates every
  literal name before mutation, inserts one ordinary pending message into the
  complete requested set, and returns the unique requested-name count.
- Creation mode intentionally recreates an exact requested queue deleted
  before the backend atomic point. Empty exact input remains a state-neutral
  `0`; selector-free, pattern, and CLI broadcast never create queues.

### Changed
- Bumped the direct backend handshake to API v5 so old direct backends fail at
  resolution rather than rejecting `create_missing` at call time.
- Bumped `simplebroker-pg` and `simplebroker-redis` to 3.3.1, raised both
  extension core floors to `simplebroker>=5.6.1`, and raised root optional
  backend floors to those extension versions.

### Documented
- Promoted delivery, reservation, peek/watch safety, and transactional
  generator ownership into canonical `[SB-DELIVERY-*]` product specs. This is
  a documentation-authority migration and does not change runtime behavior.

### simplebroker-pg 3.3.1
- Added transactional full-requested-set exact broadcast creation while
  preserving default existing-only and no-resurrection behavior.

### simplebroker-redis 3.3.1
- Added exact-create Lua selection. Anticipated validation and timestamp
  conflicts are rejected before mutation; registry and message writes then run
  in one non-interleaved script phase.

## [5.6.0] - 2026-07-28

### Added
- Added exact existing-queue broadcast selection through
  `broker.broadcast(message, queue_names=...)` and repeatable CLI
  `broker broadcast --queue QUEUE ... MESSAGE`. Exact names are snapshotted,
  deduplicated, and validated before mutation; missing names are ignored and
  are not created. The selector is mutually exclusive with `pattern`, including
  the legacy empty pattern value.
- Added backend-agnostic broadcast contract coverage plus PostgreSQL
  concurrent-delete and Redis Lua atomicity probes.

### Changed
- Bumped the direct backend handshake to API v4. The exact-version check rejects
  every v3 backend, including SQL-namespace plugins whose broadcast preparation
  hook is otherwise unchanged, with the existing upgrade-or-pin diagnostic.
- Bumped `simplebroker-pg` and `simplebroker-redis` to 3.3.0, raised both
  extension core floors to `simplebroker>=5.6.0`, and raised the root optional
  backend floors to those extension versions.
- Redis exact-target broadcast intersects the requested names with the queue
  registry and inserts all copies in one Lua invocation. An all-missing request
  leaves persisted `last_ts`, wakeups, and maintenance scheduling unchanged.
- Broadcast long-option abbreviations are rejected before mutation. Use the
  full `--pattern` / `--queue` spelling, or `--` before a literal
  option-looking message.

### simplebroker-pg 3.3.0
- Added transactional exact existing-queue broadcast selection and backend API
  v4 compatibility.

### simplebroker-redis 3.3.0
- Added atomic Lua exact existing-queue broadcast selection, including
  no-resurrection and zero-target persisted-state guarantees, and backend API
  v4 compatibility.

### Documented
- Adopted layered product documentation ownership. Root `README.md` remains
  normative for every concern still `readme-only`; sections marked
  `canonical-spec` in `docs/specs/product-section-registry.md` win on conflict
  and are updated in place. Process specs stay under `[DOM-*]`; product
  sections use `[SB-*]`.
- Promoted the first product vertical, `docs/specs/10-cli.md`
  (`[SB-CLI-1]`…`[SB-CLI-4]`): CLI exit codes `{0,1,2}`, stdout vs stderr,
  global options before the subcommand, and message-line JSON fields for
  `read` / `peek` / `move` / `dump --json` (not other `--json` shapes such as
  `list --json`). README Exit Codes and `docs/agent-kernel.md` link the
  section; contract tests bind each clause. No CLI runtime behavior changes.

## [5.5.0] - 2026-07-28

### Changed
- Foreign-thread close or garbage-collector finalization of an
  `at_least_once` transactional generator or a `sidecar()` session no longer
  corrupts cleanup state and raises `RuntimeError: cannot release un-acquired
  lock`, and no longer wedges the broker instance in a permanent silent
  deadlock. The violation is recorded and a `RuntimeWarning` emitted; the
  instance is permanently poisoned and core operations reaching a poison check
  promptly raise `OperationalError` (`retryable=False`); poisoning never adds a
  hang to closing a `Queue` handle. A persistent shared wrapper may first wait
  the existing five-second session-drain bound. Recovery is restarting the
  process; the interrupted batch's messages remain available for delivery
  afterward. The supported contract is unchanged: these objects are
  thread-affine and must be closed or exhausted, never abandoned.
- Generator and sidecar docstrings now state the thread-affinity contract.
- Bumped the coordinated first-party `simplebroker-pg` and
  `simplebroker-redis` packages to 3.2.3 and 3.2.4, respectively, and raised
  the root optional-backend dependency floors to those versions.

### simplebroker-pg 3.2.3
- Patch compatibility release for SimpleBroker 5.5.0. The extension now
  requires `simplebroker>=5.5.0`; runtime schema behavior is unchanged.
- Added PostgreSQL-backed parity coverage for poisoned owner mutation,
  waiters, core shutdown, public queue close modes, and both sidecar modes and
  foreign-resumption arms.

### simplebroker-redis 3.2.4
- Patch compatibility release for SimpleBroker 5.5.0. The extension now
  requires `simplebroker>=5.5.0`; runtime queue behavior is unchanged.
- Added a regression probe confirming that Redis cross-thread generator
  behavior remains unchanged.

## [5.4.0] - 2026-07-17

### Changed
- Closed stdout pipes are a clean CLI shutdown. `watch`, `read --all`,
  `peek --all`, and `dump` exit `0` without a traceback or interpreter flush
  error, stop claiming further messages, and close transactional iterators.
  With a configured at-least-once read batch, uncommitted messages are rolled
  back instead of being lost. Windows pipe closure is recognized when the C
  runtime reports either a pipe error or a generic `EINVAL`. `watch` also exits
  cleanly on SIGTERM.
- `DatabaseError` is now the common base for SimpleBroker's
  `OperationalError`, `IntegrityError`, and `DataError`, while all remain under
  `BrokerError`. Code that catches `DatabaseError` now catches those subclasses.
  Conversely, `DatabaseError` no longer subclasses `OSError`; catch
  `DatabaseError` and `OSError` separately when supporting both storage and OS
  failures. This hierarchy is new in 5.4.0, so do not rely on it in code that
  must also run against SimpleBroker 5.3.x. Plain `RuntimeError` escape paths
  such as retry exhaustion remain outside `DatabaseError`.
- With project scope disabled, the CLI now honors `.broker.toml` only in the
  selected current directory. With scope enabled, discovery is git-like: it
  walks the physical parent chain but stops at the filesystem root, a mount
  boundary, or 100 levels. An explicit `-f` still wins, and a current-directory
  project config wins over environment defaults.
- Relative SQLite queue targets are bound to their construction directory.
  Plain paths and equivalent resolved SQLite `BrokerTarget` values now compare
  as the same storage target for moves and activity-waiter lookup.
- Watchers now treat `StopWatching` and the internal `StopException` as clean
  stops from either message or error handlers. Stop/join tracks the actual run
  owner, peek batches observe stop requests between messages, and native
  activity-waiter replacement has explicit single-owner cleanup semantics.
- Documented the single-threaded first-touch assumption in SQLite fork
  recovery and its bounded failure mode. A reproducible migration benchmark
  found that 10M- and 50M-row legacy databases exceed the fixed schema-lock
  wait budget; a separate migration-aware waiting proposal is required, with
  no timeout behavior changed in this release.

### Fixed
- POSIX watcher signal handlers now request shutdown without asynchronously
  unwinding an active backend call. PostgreSQL `watch` shutdown no longer
  returns an active libpq connection to the pool or emits pool diagnostics on
  stderr.
- Preserved attached broadcast patterns such as `-pqueue*`, rejected missing
  broadcast messages cleanly, and retained `--` as the escape for literal
  option-looking messages. Write-output flags before an explicit escape are
  canonicalized for consistent parsing on every supported Python version.
  Global status JSON preprocessing no longer consumes a post-command literal
  `--json`.
- Sidecar writes and schema-v3 repair now roll back if commit fails, preserving
  the original commit exception even if rollback also fails.
- `has_pending_messages()` now consistently rejects boolean and string
  timestamp bounds across first-party backends.
- Opportunistic SQLite vacuum distinguishes ordinary lock contention from a
  failure to open the lock sidecar. Contention remains a skip; open failures
  propagate so maintenance records the failed attempt and retries later.
- Backend target redaction now replaces every ASCII control character before
  parsing or display, preventing embedded line and terminal-control injection.

### Security
- Loading `.broker.toml` warns when its target embeds a URL/conninfo password
  and, on POSIX, when the file is group- or other-readable. Warnings never print
  the credential and never block config loading; environment-supplied passwords
  remain silent.

### simplebroker-redis 3.2.3
- Made per-queue deletion atomic across the registry, pending, claimed, body,
  reserved, and global-ID structures. A reservation that begins during a
  multi-queue delete stops later deletions without corrupting the reserved
  queue.
- Made claim, move, and batch reservation scan beyond large reserved prefixes
  through bounded Lua windows with an exclusive continuation cursor.
- Made claimed-message vacuum atomic with queue-registry preservation.
- Made patternless broadcast select queues and write all copies atomically,
  including timestamp-growth retries and waiter/accounting updates. Patterned
  broadcast deliberately retains Python `fnmatchcase` snapshot semantics, now
  documented in the extension README.
- Added explicit queue-name and pending-bound validation at the Redis core
  boundary. Storage layout and backend API version are unchanged.

## [5.3.3] - 2026-07-14

### Fixed
- Removed a mistaken SQLite cleanup heuristic that treated `Mock` in a database
  path as proof that the file was disposable. Runner teardown no longer deletes
  a database or its shared setup sidecars based on its name.
- Initialized timestamp-conflict metrics when a SQL-backed core is created.
  Concurrent first writes can no longer reset conflict or resync counters while
  another write is updating them.

### Changed
- Documented that transactional generators on SQL-backed cores must be created,
  consumed, exhausted, and closed on the same thread. Foreign-thread
  finalization remains unsupported; Redis/Valkey does not share this SQL
  transaction failure mode.
- Enabled branch coverage and made unexpected pytest warnings fail the suite.
  Added behavior-focused tests until all 58 measured runtime files exceeded 90%
  statement and branch coverage. The existing aggregate coverage gate remains
  85%.
- Bumped the coordinated first-party `simplebroker-pg` and
  `simplebroker-redis` packages to 3.2.2 and raised the root optional-backend
  dependency floors to those versions.

### simplebroker-pg 3.2.2
- Coordinated compatibility and verification release for SimpleBroker 5.3.3;
  the extension now requires `simplebroker>=5.3.3`. Runtime schema behavior is
  unchanged. Expanded coverage of schema migration and validation, plugin
  contracts, runner lifecycle failures, and resource cleanup.

### simplebroker-redis 3.2.2
- Coordinated compatibility and verification release for SimpleBroker 5.3.3;
  the extension now requires `simplebroker>=5.3.3`. Runtime queue behavior is
  unchanged. Expanded Valkey-backed coverage of core operations, namespace
  validation, activity-listener lifecycle, plugin contracts, and resource
  cleanup.

## [5.3.2] - 2026-07-13

### Fixed
- Made first-time SQLite schema setup atomic. A failed bootstrap now rolls
  back completely and leaves its PhaseLock phase incomplete so a later opener
  can retry from a clean database.
- Made normal SQLite lock retries use a 30-second no-progress budget instead
  of a fixed total deadline. Commits from other connections refresh the budget,
  so sustained Windows contention cannot starve a writer while a genuinely
  stalled database still fails in bounded time.
- Made watcher startup honor stop requests while waiting for PhaseLock and
  retrying SQLite connection setup. Lock contention during WAL setup can no
  longer keep a stopped watcher alive for the full bootstrap timeout.

### Changed
- Hardened CI and releases around checked locks, frozen and cache-free builds,
  exact-SHA pre-tag gates, write-once tags, draft-first publication, immutable
  GitHub Releases, and an 85% local coverage floor.
- Added Python example tests and type checks to normal CI. Codecov continues to
  use the repository secret; upload failures are visible but do not override
  the local coverage result.

## [5.3.1] - 2026-07-11

### Added
- `Queue.write()`, `BrokerCore.write()`, and every first-party backend
  connection now return the committed message's 64-bit timestamp/message ID
  instead of `None`. Producers can record, correlate, or later target the
  exact message they wrote without a racy peek. Callers that ignore the
  return value are unaffected.
- `broker write` accepts `-t/--timestamps` (print the new message's
  19-digit ID) and `--json` (print `{"timestamp": <id>}`). Default write
  output is unchanged (silent). `broker write q - -t` (stdin marker followed
  by an output flag) now parses; it was previously an argument error.

### Changed
- `simplebroker.ext.BrokerConnection.write` is now typed `-> int` (was
  `-> Any`) and required to return the committed timestamp. Because this
  changes the backend seam contract, `BACKEND_API_VERSION` is bumped to 3.
- Bumped the coordinated `simplebroker-pg` and `simplebroker-redis` packages
  to 3.2.1 and raised the root optional-backend dependency floors to those
  versions. The Redis release returns the committed ID from `write()`; the
  Postgres release is a handshake-only version bump (Postgres writes are
  driven by the shared core). Both extension core floors move to
  `simplebroker>=5.3.1`.

## [5.3.0] - 2026-07-10

### Added
- Added `PollingStrategy.replace_activity_waiter()` for owner-serialized live
  replacement of backend-native queue-set waiters. The API preserves
  data-version and local-activity state, resets backoff and native-generation
  state for each distinct replacement, returns the displaced waiter without
  closing it, accepts `None` to select polling fallback, and treats
  same-object replacement as an exact no-op.

### Changed
- Bumped the coordinated first-party `simplebroker-pg` and
  `simplebroker-redis` packages to 3.2.0 and raised the root optional-backend
  dependency floors to those versions.

## [5.2.1] - 2026-07-10

### Fixed
- Preserved live SQLite WAL locks during database validation by replacing the
  raw header read with stat and a SQLite-managed read-only connection.
- Extended the cross-backend benchmark to Redis/Valkey with optional
  Docker-managed Postgres and Valkey services, including cleanup when a
  container fails its readiness check.

## [5.2.0] - 2026-07-09

### Fixed
- Prevented connection passwords and backend-option values from appearing in target
  or Queue representations and cross-target move errors.
- Made instance configuration authoritative for connection, watcher, generator, and
  maintenance operations.
- Kept failed peek-handler messages retryable without advancing past later message IDs.
- Ensured each handler failure invokes its error callback at most once and made watcher
  running state clear after normal or fatal termination.
- Rejected invalid delivery guarantees before connection or message-state mutation.

### Changed
- Automatic vacuum now counts committed claims, moves, imports, broadcasts, and
  generator batches on long-lived cores. It is documented as synchronous,
  opportunistic maintenance rather than a background process.
- Backend API v2 exposes the shared delivery-guarantee and maintenance contracts
  from `simplebroker.ext`; first-party extensions no longer import their private
  implementation modules.
- `BrokerTarget` is now the single target descriptor name throughout the public
  API and implementation; the duplicate private `ResolvedTarget` name was removed.

## [5.1.1] - 2026-07-09
### Added
- Added `examples/reference_reactor.py`, a copyable sidecar-aware reactor reference
  that layers `BaseReactor` on the `MultiQueueWatcher` example. It demonstrates
  fixed input/output/control lanes, broker-free worker payloads over Python
  `queue.Queue`, sidecar checkpoints, exact-ID output replay, and graceful
  reactor shutdown.
- Added focused reactor example tests under `examples/tests`, covering worker
  wakeups, input activity wakeups, fixed queue topology, stop/close ordering,
  control checkpointing, pending-output replay, crash/restart idempotency,
  output-backlog control responsiveness, poison result handling, and per-queue
  in-flight ordering.
- Added regression coverage proving persistent SQLite queue handles keep
  thread-local connection ownership stable: each worker thread applies
  connection settings once while queue writes and sidecar transactions continue
  to pass SQLite integrity checks.

### Changed
- `examples/multi_queue_watcher.py` now exposes `get_queue(queue_name)` so
  subclasses can retrieve managed `Queue` handles without reaching into private
  watcher state.
- The example reactor now treats stuck output replay as backpressure for new
  input dispatch only; the control lane remains responsive so `STATUS` and
  `STOP` can still run while an output sink is wedged. Pending control traffic
  caps output replay work for the turn rather than starving it entirely.

### Documented
- Documented the reactor threading and multi-process contract in the main README
  and example docs: the reactor owns its persistent handles and sidecar writes
  on one thread; SimpleBroker owns storage-level SQLite contention; input,
  output, and control effects are application-level at-least-once workstreams
  across restart rather than a reason to add a database lease.
- Documented the reactor's stuck-output behavior and STATUS fields
  (`pending_output_backlog`, `output_backlog_blocked`) for operators.
- Documented that constructing the example `Reactor` performs durable setup,
  pending-output replay, and worker startup.

## [5.1.0] - 2026-07-06
### Added
- Added protected `BaseWatcher` lifecycle hooks for embedders that need custom
  activity waiters without cloning the watcher retry loop.
- Added `PollingStrategy.detach_activity_waiter()` so embedder-owned activity
  waiters can be released from the strategy without private attribute access.

### Changed
- Bumped `simplebroker-redis` to 3.0.1 and raised the `simplebroker[redis]`
  extra floor to pull the fixed Redis backend.
- Removed the unused `broker init --force` CLI surface and updated `--cleanup`
  help text to describe backend target cleanup instead of only SQLite files.

### Fixed
- Redis broadcast now inserts the matched queue set atomically, so timestamp
  conflicts cannot leave only part of a broadcast delivered.
- Redis activity waiters now honor stop events promptly and keep their shared
  listener registry scoped by process ID.
- `broker read --all` now honors `BROKER_READ_COMMIT_INTERVAL` from CLI config.
- Timestamp validation now rejects only real scientific notation instead of
  plain words containing `e`.
- Backend target redaction now masks URL passwords containing reserved
  characters.
- Queue move APIs now reject cross-target `Queue` destinations, and
  `broker move --all` warns if its materialized move cap is hit.
- Release gate workflows now verify that the pushed tag version matches the
  package version in `pyproject.toml` before building or publishing.
- `PollingStrategy.start()` no longer closes and reinstalls the same cached
  activity waiter object.

## [5.0.1]
### Changed
- Consolidated internal retry/backoff logic into a vendorable
  `simplebroker._retry` module. Lock-contention retries (helpers + watcher
  operational retries) now use bounded jitter (uniform sleep between 5ms and
  the computed backoff cap) instead of additive 0-25ms jitter. Connection open
  and watcher crash-recovery pacing remain deterministic exponential sleeps.

## [5.0.0]
### Migration notes (5.0)

What breaks or changes behavior when upgrading from 4.x, gathered in one
place; details in the sections below and in the 4.x→5.0 entries:

- **Old backend extension packages are rejected at runtime.** simplebroker
  5.0 requires backend plugins to declare `backend_api_version`;
  `simplebroker-pg` < 3.0.0 and `simplebroker-redis` < 3.0.0 fail backend
  resolution with a `RuntimeError`. Upgrade the extension packages together
  with core (the `[pg]`/`[redis]` extras do this automatically); projects
  depending on the extension packages directly should floor them at 3.0.0
  alongside `simplebroker>=5.0.0`.
- **Phase-lock sidecar files are renamed** (append-style: `mydb.db.lock`,
  not `mydb.lock`). Setup re-runs once per database after upgrade
  (idempotent); old-style `.lock`/`.status` files are orphaned and never
  read again — delete them at leisure.
- **The vacuum lock file is renamed the same way** (`mydb.db.vacuum.lock`,
  not `mydb.vacuum.lock`) and is now a permanent, kernel-released advisory
  flock: it is never unlinked, and its existence no longer means anything.
  Old-style `.vacuum.lock` files are orphaned and can be deleted.
- **`BROKER_VACUUM_LOCK_TIMEOUT` is removed** (no backwards compatibility;
  setting it has no effect): crash recovery is via kernel lock release, not
  mtime staleness. Note for weft users: weft's `WEFT_VACUUM_LOCK_TIMEOUT`
  passthrough maps to this key and becomes a no-op against simplebroker 5.0.
- **The `slow` pytest marker is gone**; timing tests use `benchmark`.
  Scripts using `-m "not slow"` should switch to `-m "not benchmark"`.
- **Fork misuse now fails loudly**: five previously-unguarded entry points
  (`generate_timestamp`, `get_cached_last_timestamp`,
  `refresh_last_timestamp`, `sidecar`, `get_data_version`) raise
  `RuntimeError` when called on a connection inherited across `fork()`,
  and the fork fallback abandons inherited connections instead of closing
  them.

### Changed
- Bumped the next release versions to `simplebroker` 5.0.0,
  `simplebroker-pg` 3.0.0, and `simplebroker-redis` 3.0.0. The first-party
  extension packages now require `simplebroker>=5.0.0`.
- Release tooling now verifies first-party backend extension API versions match
  the core backend API version and that extension core dependency floors cover
  the backend API minimum before release.
- Test-marker split: the `slow` marker is replaced by `benchmark`, and the
  policy is now self-documenting -- the 14 timing/performance tests are marked
  `benchmark` and never gate CI (run locally with `pytest -m benchmark` or the
  full `-m ""` suite); the 3 slow-by-construction correctness tests
  (`test_streaming_read_all`, `test_streaming_peek_all`,
  `test_worker_pool_with_slow_handlers`) are unmarked and now run in every CI
  pass. The streaming pair seeds through the Python API
  (`target_for_directory` + `Queue`) instead of 1,000 per-message CLI
  subprocesses, on every backend. Operators with scripts using
  `-m "not slow"` should switch to `-m "not benchmark"`.

### Added
- Added a code-level backend API version handshake between `simplebroker` core
  and first-party backend extension packages. Backend plugins now declare
  `backend_api_version`, and core rejects mismatches during backend resolution
  with an upgrade-or-pin diagnostic.
- `simplebroker.commands` is now documented public embedding surface: the
  programmatic CLI equivalent (each `cmd_*` prints to stdout and returns an exit
  code). Its `__all__` is completed to all 19 `cmd_*` functions plus
  `parse_exact_message_id` (the previous list omitted `cmd_alias_list`,
  `cmd_alias_add`, `cmd_alias_remove`, `cmd_rename`, `cmd_dump`, `cmd_load`).
- `DatabaseError` is now re-exported from `simplebroker.ext`. It is the base
  exception class embedders and both first-party extensions must catch for
  storage failures; this narrowly overrides the earlier "extension seam is
  documentation only" decision for exception-handling surface.

### Documented
- Clarified that `simplebroker-pg` and `simplebroker-redis` are first-party
  extension packages shipped separately for dependency isolation; the backend
  seam is guarded by `BACKEND_API_VERSION`. Third-party backend extension PRs
  are welcome, but there is not yet a stable standalone backend SDK.

### Fixed
- Fork-safety guards now cover five previously-unguarded core entry points
  (`generate_timestamp`, `get_cached_last_timestamp`, `refresh_last_timestamp`,
  `sidecar`, `get_data_version`), including through the public
  `Queue.generate_timestamp()` path: calling them on a connection inherited
  across `fork()` now raises `RuntimeError` instead of touching the parent's
  SQLite connection. The runner's fork fallback no longer *closes* inherited
  connections (SQLite documents cross-fork close as unsafe — it implies
  rollback and can touch the shared WAL-index while the parent is mid-write);
  it abandons every inherited connection (holding a reference so GC never
  finalizes it in the child) — a deliberate, bounded leak of one reference per
  inherited connection. Post-fork recovery also no longer acquires inherited
  runner locks: it replaces `_setup_lock`, `_connections_lock`, and
  `_operation_lock` with fresh objects, and it now runs at the top of every
  runner entry point that takes a lock (`run`, `begin_immediate`, `commit`,
  `rollback`, `close`, `get_connection`, `run_exclusive_setup`,
  `is_setup_complete`) — so a lock held by a parent thread at `fork()` time can
  no longer hang the child on any path.
- SQLite vacuum now serializes through a kernel-released advisory flock
  (`AdvisoryFileLock`) instead of an `O_CREAT|O_EXCL` PID/mtime lock file. This
  removes the TOCTOU in stale-lock removal and the up-to-`BROKER_VACUUM_LOCK_TIMEOUT`
  vacuum outage after a `SIGKILL`: a crashed holder's flock is released by the
  kernel, so the next vacuum proceeds immediately. The vacuum lock file is now
  never unlinked (the flock is ownership; the file is permanent) — a deliberate
  behavior change. Concurrent vacuums still skip silently when the lock is held.
- Phase-lock sidecar paths are now derived by appending to the full target name
  (`mydb.db` -> `mydb.db.lock`/`mydb.db.status`) instead of `with_suffix`, which
  collapsed same-stem targets (`mydb.db`, `mydb.backup`) onto one
  `mydb.lock`/`mydb.status` pair. On xattr-less filesystems that collision could
  make a sibling database inherit another's completion markers and skip WAL
  enablement or schema creation. Behavior note: existing `mydb.lock`/`mydb.status`
  sidecars from older versions are orphaned (never read, migrated, or cleaned up;
  they are tiny), and setup re-runs exactly once per database after upgrade
  because the status markers move to the new path once (setup phases are
  idempotent).
- The CI "phaselock fallback-path gate" now sets `PHASELOCK_ENABLE_XATTRS` (the
  variable the code reads) instead of the misspelled `_ENABLE_PHASELOCK_XATTRS`,
  so the xattr-less fallback path is actually exercised in CI.
- `watch --after` now applies its strict checkpoint filter in consume mode, not
  only peek mode. Consume-mode watchers no longer claim and deliver messages at
  or before the checkpoint boundary.
- Malformed `-m` / `--message` IDs now use the shared command error path:
  plain mode prints `invalid message ID: expected exactly 19 digits within range`
  on stderr and exits `1`, while `--json` emits the same diagnostic as a
  structured `INVALID_MESSAGE_ID` error. Valid but absent IDs still remain the
  silent "no matching message" case and exit `2`.

### Changed
- Exact message-ID APIs now share one validator. Python APIs that target exact
  IDs accept integer IDs or exact 19-digit string IDs; malformed string IDs
  raise `ValueError`, unsupported types such as `bool` raise `TypeError`, and
  `after_timestamp` / `before_timestamp` remain int-only bounds.

### Documented
- Documented the move-vs-checkpoint skip: `move` preserves a message's original
  timestamp (stable IDs), so any timestamp-checkpoint consumer — `peek --after`,
  `read --after`, a peek-mode watcher, a consume-mode watcher started with
  `after_timestamp`, or a hand-rolled `ts > last_seen` filter — permanently
  skips messages moved into its queue behind its checkpoint. Consume without a
  timestamp filter or rescan from `--after 0`. Added a README caveat, watcher
  docstring caveats, and characterization tests pinning the behavior.

### Removed
- `BROKER_VACUUM_LOCK_TIMEOUT`. The kernel-released vacuum flock made
  mtime-staleness detection unnecessary; the config key is gone and setting
  the environment variable has no effect.

### simplebroker-pg
- `rename_queue()` now takes the singleton meta row before the `messages` table,
  matching writer and broadcast lock order. This avoids a retryable `40P01`
  deadlock when aliases are retargeted during a concurrent write.

## [4.10.0] - 2026-07-02
### Fixed
- `write()` now allocates its timestamp inside the insert transaction, so the
  `last_ts` advance and the message row become visible atomically. Previously
  a concurrent writer could commit a higher timestamp during another writer's
  lock wait, letting checkpoint readers (`peek --after`, peek-mode watchers)
  permanently skip a message. Applies to SQLite and Postgres (see the
  simplebroker-pg entry below); the Redis backend was already atomic.
- `broker write --help` and `broker broadcast --help` now show help.
  Previously `write --help` failed with a queue-name error and
  `broadcast --help` silently broadcast the literal message `--help` to every
  queue. Use `-- --help` to write a literal `--help` message.
- `alias` is registered in the argument rearranger's subcommand set. Previously
  global-looking flags after `alias` were hoisted to global position:
  `broker alias add a b --cleanup` deleted the database. Such flags now fail
  with "unrecognized arguments".
- `--cleanup` combined with a command is now rejected ("--cleanup cannot be
  used with commands"), matching the existing `--status`/`--vacuum` guards.
  Previously the command was silently dropped and the database deleted.
- Retry classification is backend-neutral: runners can set
  `OperationalError.retryable` instead of relying on SQLite lock-message
  matching (`True` forces retry, `False` forbids it, `None` keeps the
  SQLite marker fallback). See the simplebroker-pg entry below for the
  Postgres side.
- `build_move_by_id_query` (private `_sql` module, used by
  `examples/async_pooled_broker.py`) emitted invalid SQL (`ORDER BY` after
  `RETURNING`); the example's move-by-ID path crashed. All four legacy
  builders now have executable-SQL tests.
- README: removed the nonexistent `watch --quiet` flag (use global `-q`),
  documented the `--compact` global option, fixed the `alias` command row,
  and replaced the network-filesystem recommendation with a corruption
  warning (WAL mode is single-host).

### Removed
- Internal: byte-identical duplicated alias methods on `BrokerDB` (behavior
  unchanged; `BrokerCore` remains the single implementation).

### Internal
- Test harness scrubs ambient `BROKER_*` environment variables in
  `pytest_configure`, before any test runs (`BROKER_TEST_BACKEND`
  allowlisted).
- mypy now type-checks against the Python 3.11 floor instead of 3.14.
- `ext.py`, `SQLRunner`, and `BackendPlugin` now document the real
  backend-author contract, including the getattr-probed optional hooks.

### simplebroker-pg 2.5.0
- Postgres contention SQLSTATEs (`55P03` lock_not_available, `40001`
  serialization_failure, `40P01` deadlock_detected) are now marked
  `retryable`, so the core retry machinery retries them instead of failing
  immediately.
- `prepare_broadcast` takes the meta `last_ts` row lock before the messages
  table lock, matching the write path's lock order now that writers allocate
  their timestamp inside the insert transaction. The old order deadlocked
  against in-flight writes. Requires simplebroker>=4.10.0.

## [4.9.0] - 2026-06-18
### Added
- Added `BrokerConnection.rename_queue(...)`, `QueueRenameResult`, and
  `broker rename <old> <new> [--json]` for retagging existing queue state
  without a schema migration. SQLite, Postgres, and Redis support the API;
  claimed messages are included, target queues must be empty, and aliases
  targeting the old queue retarget by default.

### Fixed
- Postgres activity listeners now drain batched notifications from one poll,
  ensuring queue-specific notifications emitted in the same transaction are not
  dropped.

### simplebroker-pg 2.4.0
- Added Postgres backend support for
  `BrokerConnection.rename_queue(...)`, including table-level serialization and
  old/new queue LISTEN/NOTIFY wakeups. Requires simplebroker>=4.9.0.

### simplebroker-redis 2.6.0
- Added Redis backend support for
  `BrokerConnection.rename_queue(...)`, implemented as one atomic Lua script
  that preserves pending/claimed state, rejects active source reservations, and
  publishes old/new queue activity. Requires simplebroker>=4.9.0.

## [4.8.0] - 2026-06-18
### Added
- Added `Queue.latest_pending_timestamp()` and
  `BrokerConnection.latest_pending_timestamp(queue)` to retrieve the newest
  pending message timestamp for one queue without scanning message history.
  SQLite, Postgres, and Redis backends implement the API; SQLite schema v5 adds
  `idx_messages_pending_queue_ts` for the lookup.
- Coverage-guided fuzzing (Atheris) of the timestamp parser and the
  dump/load parser, reusing the existing Hypothesis properties through
  their external-fuzzer hook (`fuzz/`, weekly `Fuzz` workflow with a
  persistent corpus). Verified to rediscover the known YYYYMMDD parsing
  quirk from a cold corpus in seconds.

### Fixed
- Postgres schema migration now idempotently recreates
  `idx_messages_queue_ts_order_unclaimed` even when the schema already reports
  the current version, repairing drifted schemas without a version bump.

### simplebroker-pg 2.3.0
- Added Postgres backend support for
  `BrokerConnection.latest_pending_timestamp(queue)`, using the existing
  pending queue/timestamp index.
- Postgres schema migration now idempotently recreates
  `idx_messages_queue_ts_order_unclaimed` for current-version schemas when the
  index is missing. Requires simplebroker>=4.8.0.

### simplebroker-redis 2.5.0
- Added Redis backend support for
  `BrokerConnection.latest_pending_timestamp(queue)`, returning the newest
  pending timestamp while skipping IDs reserved by active at-least-once
  batches. Requires simplebroker>=4.8.0.

## [4.7.0] - 2026-06-11
### Changed
- Pre-epoch ISO dates passed to timestamp parsing (e.g. `--after 1950-01-01`)
  now clamp to the Unix epoch, uniformly meaning "everything". Previously
  they parsed to negative internal values that the CLI accepted by accident
  while API bound checks rejected them.

### Added
- Added `broker dump` and `broker load`: a versioned ndjson backup/restore and
  backend-migration format on stdout/stdin, with repeatable `--include`/`--exclude`
  queue-name globs on dump (aliases match on either their own name or their
  target; exclude wins; the flags compose). Mirrored Python APIs:
  `dump_lines(broker, *, include, exclude)` and `load_lines(broker, lines)`
  (plus `LoadResult`). Dumps are deterministic (header, sorted aliases, sorted
  queues, ascending message-ID order; pending messages only) and restore with
  exact message IDs on any backend — `broker dump | BROKER_BACKEND=postgres
  broker load` migrates a broker in one pipeline. Built entirely on the public
  connection surface; no backend changes.
- Property-based test suite (Hypothesis): timestamp-parser totality and
  round-trip properties, cross-backend queue-name/body round-trip properties,
  and a stateful reference-model test that runs identical operation sequences
  against the SQLite, Postgres, and Redis backends.

### Fixed
- `TimestampGenerator.validate()` now raises `TimestampError` (as documented)
  instead of leaking `ValueError` for ISO dates beyond the year-2262
  timestamp horizon. CLI behavior is unchanged.
- Invalid queue names now raise `QueueNameError` and invalid message bodies
  (oversize, or not UTF-8 encodable) raise `MessageError`, as the docstrings
  always promised. Both exception types now subclass `ValueError`, so
  existing `except ValueError` handlers keep working unchanged.
- Queue-name validation now uses `fullmatch`, closing a regex quirk where a
  name with a trailing newline (e.g. `"jobs\n"`) was accepted despite the
  documented character set. Such names — previously creatable only through
  the Python API — are now rejected at first use, matching the prefix
  validator's existing strictness.
- `bin/pytest-pg` now applies the default suite paths for flag-only
  invocations (`pytest-pg -q` previously made the extension phase collect
  zero tests and exit 5).

### simplebroker-redis 2.4.1
- Aligned validation errors with simplebroker 4.7.0: invalid queue names
  raise `QueueNameError` and oversize or non-UTF-8-encodable bodies raise
  `MessageError` (both `ValueError` subclasses), matching the SQLite and
  Postgres backends. Caught by the new cross-backend property suite.
  Requires simplebroker>=4.7.0 (where those exception types gained their
  `ValueError` base).

## [4.6.0] - 2026-06-10
### Added
- Added `include_claimed` to the public peek surface — `Queue.peek/peek_one/
  peek_many/peek_generator`, the `BrokerConnection` protocol, and the CLI
  (`broker peek --include-claimed`). When set, peeks return claimed (consumed
  but not yet vacuumed) messages merged with pending ones in message-ID order.
  Claimed rows are deletion-pending; peeking never changes claim state. This
  completes the public claimed-row round trip started by
  `find_message_ids(include_claimed=...)`.

### simplebroker-redis 2.4.0
- Implemented `include_claimed` peeks by merging the per-queue pending and
  claimed ZSETs. Requires simplebroker>=4.6.0.

## [4.5.0] - 2026-06-10
### Added
- Added a public sidecar-table API for embedding applications: `Queue.sidecar()` /
  `BrokerCore.sidecar()` yield a `SidecarSession` for caller-owned tables in the
  broker's database, inheriting the broker's retry loop, locking, and
  ephemeral-vs-persistent connection discipline. Exported `SidecarSession`,
  `SidecarUnavailableError`, and `RESERVED_TABLE_NAMES` from `simplebroker.ext`.
- Promoted the watcher subclassing contract into `simplebroker.ext`: `BaseWatcher`,
  `PollingStrategy`, `default_error_handler`, and the new public `StopWatching`
  exception (the former private `_StopLoop`, which remains as an alias).

### simplebroker-redis 2.3.1
- `RedisBrokerCore.sidecar()` raises `SidecarUnavailableError` (the Redis backend has
  no SQL storage). Requires simplebroker>=4.5.0.

## [4.3.0] - 2026-06-01
### Changed
- Replaced the 4.1/4.2 exact-ID APIs with a single API-only `insert_messages(...)` method on broker handles and `Queue`. It accepts one or more exact-ID records, advances `last_ts` above the largest supplied ID inside the same transaction, and inserts pending messages with their supplied IDs.

### Removed
- Removed `import_message(...)`, `import_messages(...)`, and `write_reserved_message(...)` before they had external compatibility requirements.

### simplebroker-pg 2.2.0
- Bumped the Postgres extension for the SimpleBroker 4.3.0 exact-ID `insert_messages(...)` API.

### simplebroker-redis 2.2.0
- Bumped the Redis extension for the SimpleBroker 4.3.0 exact-ID `insert_messages(...)` API.
- Fixed Redis `last_ts` advancement to compare 64-bit timestamp strings exactly instead of converting them to Lua numbers.

## [4.2.0] - 2026-06-01
### Added
- Added API-only `write_reserved_message(...)` for writing a message with a previously generated broker message ID.
- Added API-only `import_messages(...)` for atomic dump/load restore that advances `last_ts` above the imported message IDs before inserting rows.

## [4.1.0] - 2026-06-01
### Added
- Added API-only `import_message(...)` on broker handles and `Queue` for restoring pending messages with exact historical message IDs. The supplied ID must be lower than the broker's current `last_ts`.

## [4.0.0] - 2026-05-30
### Changed
- Dropped Python 3.10 support. SimpleBroker and its Postgres and Redis extension packages now require Python 3.11+.
- `.broker.toml` parsing now uses the Python standard library `tomllib` module.

### simplebroker-pg 2.0.0
- Dropped Python 3.10 support. The Postgres extension now requires Python 3.11+ and SimpleBroker 4.0.0+.

### simplebroker-redis 2.0.0
- Dropped Python 3.10 support. The Redis extension now requires Python 3.11+ and SimpleBroker 4.0.0+.

## [3.8.0] - 2026-05-20
### Added
- Added `delete_from_queues(...)` for backend-level physical deletion across several queues, with strict `before_timestamp` filtering and consistent SQLite, Postgres, and Redis behavior.
- Added `find_message_ids(...)` on broker handles and queues for API-only, literal, case-sensitive message body substring search that returns message IDs without mutating messages.

### Changed
- `list_queues()` now returns queue names only, including claimed-only queues. Use `list_queue_stats()`, `get_queue_stats()`, or `get_queue_stat()` when counts are needed.
- `broker list` now prints queue names by default. Use `broker list --stats` to include pending, claimed, and total counts.
- Updated README and async examples to use names-only queue listing and explicit stats APIs for counts.

### simplebroker-pg 1.6.0
- Added Postgres implementations for multi-queue physical delete, literal body substring ID search, and names-only queue listing with prefix filtering.

### simplebroker-redis 1.1.0
- Added Redis implementations for multi-queue physical delete, literal body substring ID search, and names-only queue listing from the Redis queue-name set.
- Redis batch deletion now refuses active at-least-once reserved batches, while body search skips active reserved IDs without mutating them.

## [3.7.1] - 2026-05-18
### Fixed
- Make connection phases strict on POSIX

## [3.7.0] - 2026-05-17
### Added
- Added full delete APIs, including batch delete
- Released redis 1.0 backend
### Changed
- Changed all delete APIs to actually delete, not just claim. Read still performs a claim

## [3.6.0] - 2026-05-14
### Added
- Added a shared command-layer error formatter. Commands that already accept `--json` now emit structured JSON errors on stderr when `--json` is present, while non-JSON invocations keep the canonical prose error format.

### Changed
- Invalid `-m` / `--message` IDs now return exit code `1` as invalid input. Valid message IDs that do not match any message still return exit code `2`.
- `init`, `--cleanup`, and `--vacuum` status messages now go to stderr instead of stdout, keeping stdout reserved for command payloads.
- `--quiet` now suppresses `--vacuum` and `--cleanup` status output consistently.
- `broker alias list --target <queue>` now emits no stdout and returns exit code `2` when no aliases match the target.
- Moved owned-runner shutdown into `simplebroker._runner` and removed the internal `_runner_lifecycle.py` module.
- Materialized batch APIs continue to accept `delivery_guarantee="at_least_once"` and satisfy it with stricter exactly-once materialization. Use generator APIs for retry-on-stop batch processing.

### Fixed
- `--cleanup` now validates SQLite targets as SimpleBroker databases before deleting them, including legacy path mode and project-config SQLite targets.
- SQLite `last_ts` advancement now uses a single `UPDATE ... RETURNING` query instead of a separate `SELECT changes()` statement.
- Hot-path batch retrieval and SQLite metadata reads avoid redundant list copies when the runner already returned a list.

### simplebroker-pg 1.4.0
- Compatibility release for SimpleBroker 3.6.0. The Postgres backend inherits the updated core CLI behavior when used with SimpleBroker 3.6.0; there are no Postgres-specific schema or runner changes in this release.

## [3.5.0] - 2026-05-13
### Added
- Added `--before <timestamp>` for `read`, `peek`, and `move`, using the same timestamp formats as `--since` and strict `ts < timestamp` filtering.
- Added `before_timestamp` range filtering to the public `Queue` and `BrokerCore` read, peek, move, and stream APIs.
### Changed
- Moved --since to --after to mirror --before

## [3.4.0] - 2026-05-13
### Added
- Added targeted queue metadata APIs: `Queue.exists()`, `Queue.stats()`, `BrokerCore.queue_exists()`, `BrokerCore.get_queue_stat()`, and `BrokerCore.list_queue_stats(...)`.
- Added CLI metadata commands and filters: `broker exists`, `broker stats`, and `broker list --prefix ... --json`.
- Added required backend SQL namespace entries for exact queue stats and prefix-filtered queue stats. No schema migration is required.

## [3.3.0] - 2026-05-05
### Added
- Added an internal phase-lock coordinator that serializes ordered setup phases with advisory lock files, durable xattr completion hints, and a status-file fallback for filesystems without usable xattrs.
- Added `create_activity_waiter_for_queues(...)` and top-level `ActivityWaiter` exports for integrations that need one backend-native wake hint across several queues, with polling remaining the fallback when a backend has no efficient hook.
- Added PostgreSQL multi-queue activity waiter support using the existing process-local shared LISTEN/NOTIFY listener instead of one listener connection per watched queue.

### Changed
- Persistent queue handles for the same resolved backend target now share process-local backend session state, preventing backend runner or pool allocation from scaling with same-process queue count while preserving thread and process isolation.
- Backend plugins can opt into the multi-queue activity waiter hook without changing the required `BackendPlugin` protocol surface.

## [3.2.0] - 2026-05-01
### Added
- Added the repo-local `bin/release.py` helper for core and `simplebroker-pg` releases, including version validation, release-state checks, preflight commands, and tag planning.

### Fixed
- Serialized SQLite schema bootstrap across processes with a versioned setup marker, preventing high-concurrency xdist/worker runs from entering schema creation or migration for the same fresh database at the same time.

## [3.1.9] - 2026-04-23
### Fixed
- Fixed resolution of the broker.toml file for the pg backend to use the same rules as broker.b

## [3.1.5] - 2026-04-08
### Fixed
- Addressed a race in concurrent lock file handling under high load

## [3.1.4] - 2026-04-06
### Changed
- Add a `pg` packaging extra so `simplebroker[pg]` installs the external `simplebroker-pg` plugin.
- Lower `simplebroker-pg` to Python 3.10+ and align local development with uv source mapping for the sibling extension.

## [3.1.3] - 2026-04-05
### Fixed
- Fix inconsistencies between the pg and sqlite backends

## [3.1.1] - 2026-04-04
### Fixed
- Normalize partial config overrides against the default config snapshot so non-SQLite backends do not fail connection setup with missing shared keys like `BROKER_AUTO_VACUUM_INTERVAL`.

## [3.1.0] - 2026-04-03
### Added
- Add piped input support

## [3.0.0] - 2026-04-01
### Added
- Pluggable backend system with `BackendPlugin` protocol and entry-point registration.
- `BackendSQLNamespace` contract with import-time validation (`ensure_backend_sql_namespace`).
- `ResolvedTarget` and `serialize_broker_target`/`deserialize_broker_target` for backend-agnostic target resolution.
- `.broker.toml` project configuration for multi-backend support.
- Postgres backend extension (`simplebroker-pg` 1.0.0) with advisory lock serialization, LISTEN/NOTIFY watcher wakeups, typed singleton meta table, and connection pooling via `psycopg_pool`.
- Backend-agnostic test infrastructure (`broker_factory`, `broker_target`/`broker`/`queue_factory` fixtures) — 543 of 831 tests now run on both SQLite and Postgres.

### Fixed
- Timestamp conflict detection in `BrokerCore.write()` no longer depends on SQLite-specific error message text.
- `initialize_target` on Postgres rejects any pre-existing schema that isn't ABSENT or OWNED.

### Changed
- Backend plugin runner protocols use public `schema`/`dsn` properties instead of private attribute casts.
- Test container for Postgres runs with `max_connections=300` to support concurrency test workloads.

## [2.9.0] - 2026-03-30
### Changed
- Refactored SQLite-specific internals into dedicated internal backend modules under `simplebroker._backends.sqlite` and moved built-in SQL definitions into `simplebroker._sql.sqlite`.
- Standardized internal backend access through the new internal backend resolver used by helpers, runner setup, and database orchestration.
- Preserved the public API and CLI behavior while making the built-in SQLite implementation easier to isolate from future external backends.

## [2.8.6] - 2026-03-28
### Fixed
- Injected `runner=` queues now execute queue reads and writes through the supplied `SQLRunner` in both persistent and non-persistent modes instead of silently falling back to the built-in SQLite path.
- `stream_messages(..., all_messages=False)` now yields at most one message in both peek and consume modes.

### Changed
- Materialized batch APIs (`claim_many()`, `move_many()`, `Queue.read_many()`, and `Queue.move_many()`) commit before returning their result lists. Passing `delivery_guarantee="at_least_once"` is supported and satisfied by the stricter exactly-once materialization behavior; use the generator APIs for retryable batch processing.
- Injected runners are now explicitly caller-owned in the docs and examples, and are reused for the lifetime of the `Queue` object.
- Updated README wording to emphasize SimpleBroker as simple to install and operate, and refreshed documentation around generator batch semantics and delete behavior.

## [2.8.5] - 2026-02-10
### Changed
- Fixed `move_generator(..., exact_timestamp=...)` in exactly-once mode to honor the filter correctly.
- Transaction start failures now propagate as operational errors instead of being treated as empty queue results.
- Unified alias resolution for CLI queue operations (`peek`, `move`, `watch`, and `delete`) when using `@alias`.
- At-least-once generator semantics now commit only after a full batch is yielded; interrupted batches are rolled back for retry.
- Switched internal DB core lock to `RLock` to avoid re-entrant deadlocks during generator-driven callbacks.
- Replaced command-layer private DB handle access with public DB methods for claimed/overall stats.
- Updated docs to clarify at-least-once rollback/retry and lock contention tradeoffs for larger batch sizes.

## [2.8.4] - 2025-11-06
### Added
- `Queue.last_ts` lazy cache plus `refresh_last_ts()` for on-demand meta reads.
### Changed
- Watcher polling refreshes cached timestamps automatically on `PRAGMA data_version` changes.
- Documentation and regression tests covering timestamp caching behaviour.

## [2.8.3] - 2025-11-05
### Changed
- Fix regression in allowing an external dependency
- Add tests to enforce no external dependencies

## [2.8.2] - 2025-11-05
### Changed
- Internal exception handling, not visible in API

## [2.8.1] - 2025-11-05
### Added
- Added -p/--pattern to list command
### Changed
- Updated tests to be more deterministic on Windows
- Added run_with_retry helper and .set_stop_event so that queue watchers process stop events more quickly

## [2.7.2] - 2025-10-27
### Added
- Added .get_meta method on BrokerCore/BrokerDB to provide a readout of the meta table
- Added additional tests

## [2.7.1] - 2025-10-24
### Changed
- Updated tests on windows to use unbuffered I/O and longer timeouts

## [2.7.0] - 2025-10-24
### Added
- Selective broadcast support via `broker broadcast --pattern`, using fnmatch-style globs.
- Queue alias management commands (`broker alias add/remove/list`) with explicit `@alias` usage.
- Alias cache auto-refresh on version changes and warning when alias names already have messages.
- CLI option `broker alias list --target <queue>` and reverse lookup helper in the Python API.
- Dedicated test suites for alias DB/CLI behaviour.

### Changed
- Alias resolution now only occurs when the queue name is prefixed with `@`, keeping plain queue names untouched.
- Schema migration for alias support now wraps DDL in transactions and gracefully handles existing tables/indexes.

## [2.6.2] - 2025-10-23
### Added
- Set auto vacuum to INCREMENTAL by default 
- Added --compact paramenter to --vacuum (also enables incremental auto vacuum)
- No changes needed from calling code

## [2.6.1] - 2025-10-21
### Added
- Added --json as on option for the --status flag 
- Refactored commands.py to consolidate common logic
- Added tests

## [2.6.0] - 2025-10-21
### Added
- Added --status flag that efficiently reports global information about the database 
- Added .generate_timestamp() (alias: .get_ts()) methods on the Queue and db objects that delegate to the TimestampGenerator

## [2.5.1] - 2025-10-13
### Changed
- Updated Python version support: now requires Python 3.10+ (dropped 3.8, 3.9)
- Added Python 3.14 support
- Updated CI/CD testing matrix and package classifiers

## [2.5.0] - 2025-08-13
### Changed
- Added config keyword argument to functions that use config values
- Allows easier testing, passing configs from API users
- All existing code should work without change

## [2.4.0] - 2025-08-13
### Added
- Added project scoping rules, gated by BROKER_PROJECT_SCOPE=True
- Created new environment options for default database name and default db dir (-f/-d)
- Added tests

## [2.3.2] - 2025-08-11
### Changed
- Moved common functionality in Watchers to BaseWatcher
- Exposed PollingStrategy as an optional parameter to Watchers 
- Added __str__ and __repr__ to Queues for convenience

## [2.3.1] - 2025-08-11
### Added
- Default message handlers: `simple_print_handler`, `json_print_handler`, `logger_handler`
- Default error handler: `default_error_handler` with config-aware internal wrapper
- Example demonstrating all default handlers (`examples/simple_watcher_example.py`)

### Changed
- Error handlers now use function defaults instead of None checks
- Simplified error handling code paths and improved type signatures

## [2.3.0] - 2025-08-10
### Changed
- **BREAKING**: Watcher API reorganization for improved consistency:
  - All Watcher classes now use queue-first parameter order: `(queue, handler, *, db=None, stop_event=None)`
  - `QueueWatcher(queue_name, handler, db=db_path)` instead of `QueueWatcher(db_path, queue_name, handler)`
  - `QueueMoveWatcher(source_queue, dest_queue, handler, db=db_path)` follows same pattern
  - Removed complex dual-convention detection logic (175+ lines eliminated)
  - Simplified BaseWatcher architecture with consistent Queue object delegation
- Updated all examples and tests to use new API
- Improved docstring examples throughout watcher.py

### Technical
- Maintains CLI backward compatibility
- All watcher functionality preserved with cleaner, more intuitive API
- Enhanced test coverage for new API patterns

## [2.2.0] - 2025-08-09
### Added
- Connection tracking for persistent mode database connections
- Instance ID support for testing scenarios

### Changed
- Comprehensive API refactoring and improvements
- Enhanced Queue API with new methods and better error handling
- Improved connection management and resource cleanup
- Better Windows test compatibility and cleanup logic
- Enhanced signal handling responsiveness

### Technical
- Major internal restructuring (7,000+ lines changed)
- New comprehensive test suites for queue operations and SQL internals
- Improved batch operations and generator methods
- Better connection pooling and resource management

## [2.1.0] - 2025-08-06
### Added
- Constants centralization in `_constants.py`
- Thundering herd mitigation for watchers to prevent resource contention
- Comprehensive watcher test suites:
  - Burst mode handling
  - Metrics collection and monitoring
  - Multiprocess coordination
  - Race condition prevention
  - Performance optimization

### Changed
- Improved time handling with `time.monotonic()` for better accuracy
- Enhanced Windows test compatibility
- Better CLI command parsing and BaseWatcher refactoring
- Improved timestamp validation and jitter handling

### Technical
- Extensive test quality improvements
- Better test isolation and determinism
- Enhanced performance calibration system

## [2.0.1] - 2025-07-20
### Fixed
- Windows test compatibility and finalization logic
- Queue resource cleanup for Windows environments
- Minor formatting and documentation improvements

### Added
- Code coverage reporting with Codecov integration
- Additional test coverage and README badges

## [2.0.0] - 2025-07-19
### Added
- **New Python API** for programmatic queue operations:
  - `Queue` class with context manager support for simplified message handling
  - `QueueWatcher` and `QueueMoveWatcher` for real-time monitoring
- **Extension system** for custom backends:
  - `SQLRunner` protocol in `simplebroker.ext` for alternative database implementations
  - `TimestampGenerator` exposed for consistent timestamp handling across extensions
  - Example implementations included in new `examples/` directory
- `move` command to transfer specific messages by ID between queues
- Comprehensive examples directory with Python API usage and shell script patterns

### Changed
- **BREAKING**: Complete internal refactoring (CLI remains unchanged):
  - Core logic split into `BrokerCore` (database-agnostic) and runner implementations

### Technical
- Maintains full backward compatibility for CLI usage

## [1.5.0] - 2025-07-12
### Changed
- **BREAKING**: Renamed `purge` command to `delete` for consistency
- **BREAKING**: Renamed `watch --transfer` to `watch --move` for clarity
- Added UNIQUE constraint on timestamp column for improved data integrity

## [1.4.0] - 2025-07-11
### Added
- `watch` command with three distinct modes:
  - **Consume mode** (default): Process and remove messages
  - **Peek mode** (`--peek`): Monitor without consuming
  - **Move mode** (`--move`): Drain all messages to another queue

## [1.3.0] - 2025-07-09
- Performance optimizations
- New --vacuum flag and list --stats command to manage claimed messages

### Changed
- Improved differentiation between empty queue and filtered messages situations
- Better test determinism with adjusted timing

## [1.2.0] - 2025-07-09
### Added
- New `--since` flag to filter messages by timestamp
### Fixed
- Better application-level retry logic to handle SQLite's immediate return behavior
- Improved database contention handling during setup
- Enhanced concurrency logic in tests

## [1.1.2] - 2025-07-07
### Fixed
- Updated database setup contention logic

## [1.1.1] - 2025-07-07
### Changed
- Python 3.8 and 3.9 compatibility improved

## [1.1.0] - 2025-07-07
### Added
- Application-level retry logic for database operations
- Type checking support
- Windows compatibility improvements

### Fixed
- Type annotations and test errors
- Python 3.8 compatibility in cli.py
- Windows compatibility in parallel writes test
- Cross-platform path handling

### Changed
- Updated tests to use multiprocess for better simulation of expected usage
- Use non-destructive peek for test verification
- Various formatting improvements

## [1.0] - 2025-07-06
### Added
- Initial release of SimpleBroker
- Lightweight message queue backed by SQLite
- FIFO message guarantees
- Simple command-line interface
- Full test suite
