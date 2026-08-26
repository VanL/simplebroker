# Schema and Representation Assumption Remediation Plan

Status: completed
Class: 5+P. The work changes published storage admission, migration and
backend-compatibility behavior, widens accepted path and project-config forms,
corrects built-in SQLite delivery ordering, and changes the durable coverage
repair rule. `[DOM-5]`, `[DOM-6]`, `[DOM-11]`, and `[DOM-15]` therefore require
reviewed contract text before implementation, the risky-plan hardening
checklist, and a different-family pre-landing review.
Plan type: implementation with spec revision

## Goal and Scope

Fix every material issue from the 2026-08-25 schema and representation audit
without replacing cheap open-time checks with full schema validation. The
implementation must separate ownership, compatibility, current-schema proof,
and coordination state; preserve FIFO using logical storage order rather than
engine return order; remove non-semantic path, configuration, JSON, coverage,
session-identity, cleanup-order, and plugin-discovery constraints; and keep
strict checks whose exactness is part of a real compatibility or safety
contract.

This is a bounded remediation program, not authority for a new backend SDK,
general schema framework, generic serializer, phase-lock rewrite, path-policy
rewrite, or unrelated cleanup. Each finding below receives either an
implementation slice or an explicit no-change disposition.

## Resolution Inventory

| ID | Baseline defect | Planned resolution | Priority |
|----|-----------------|--------------------|----------|
| F1 | SQLite bootstrap can mutate a newer database before the newer-version rejection. | Extend the existing early magic probe on the runner's normal connection to read stored schema version and proof facts in the same scalar snapshot; reject a newer owned version before `SetupPhase.CONNECTION`, WAL/auto-vacuum setup, bootstrap DDL, index cleanup, metadata writes, or marker mutation. | P1 |
| F2 | SQLite claim and move return rows in `UPDATE ... RETURNING` order, which SQLite does not guarantee. | Add a private `id` column only to the built-in SQLite result, then sort and strip it through one shared core helper used by list and generator paths. Keep PostgreSQL, Redis, and backend API v7 unchanged. | P1 |
| F3 | `schema-v5` is treated as proof of current schema state. | Keep the marker as a cache hint. Trust it only when database-internal proof-version and proof-cookie rows match the current proof algorithm and `PRAGMA schema_version`; otherwise take the existing schema lock and run the idempotent slow path once. | P1 |
| F4 | A legacy database with no stored version can receive the current version before its migrations finish. | Distinguish a new database from an existing unversioned legacy database; seed the latter at the oldest supported baseline and advance only after each migration transaction commits. | P1 |
| F5 | PostgreSQL current-shape ownership checks and Redis exact-version ownership can block an older owned target before migration. | Separate ownership from version relation and current-shape readiness. Older owned targets reach a migration or explicit no-migration diagnostic; newer targets fail before mutation. | P2, latent today |
| F6 | PostgreSQL preflight accepts an empty pre-created schema for initialization, while `initialize_target` rejects it. | Make absent and empty schemas use the same initialization admission; preserve rejection of foreign and partial schemas. | P2 |
| F7 | SQLite timestamp uniqueness is identified by one exact index name, causing a redundant index on fresh databases and accepting a wrong same-named index. | Inspect unique-index semantics by column and predicate. Accept an equivalent autoindex or differently named index; fail safely on a conflicting established name rather than dropping it. | P2 |
| F8 | POSIX paths are rejected for shell punctuation even though target paths are not executed by a shell; a product-wide 1,024-character ceiling is invented. | Permit punctuation that has no meaning to an actual internal consumer and rely on OS limits. Retain control, traversal/containment, reserved-name, platform-syntax, and live glob-pattern protections. | P2 |
| F9 | `.broker.toml` rejects nested/list backend options before the owning plugin can validate or normalize them. | Validate only that the top-level value is a table, pass recursive TOML-native values to the plugin, and require the plugin's returned options to satisfy lossless `BrokerTarget` transport. | P2 |
| F10 | Session key freezing merges type-distinct or opaque-distinct configuration values. | Recursively detach supported mutable containers once at acquisition, use that same snapshot for the session key and lazy factory, and use process-local identity for opaque values; unsafe sharing and key/spec drift must never be fallbacks. | P1 |
| F11 | Process-session and registry cleanup stop at the first ordinary exception, so set/dict iteration order decides which resources leak. | Attempt every safe ordinary cleanup, preserve one failure as primary using the existing convention, retain the others as diagnostics, and preserve `BaseException` priority. Failure order is not a contract. | P1 |
| F12 | Coverage marker repair requires physical column order and an exact table set. | Validate the installed coverage schema by required names and repair-relevant semantics, independent of ordinal column order and harmless extra tables; retain fail-closed rejection of missing required structure and conflicting version rows. | P3 |
| F13 | CLI tests require JSON object member order although `[SB-CLI-4]` defines fields and types. | Assert key membership and values, not parsed mapping iteration order; deterministic emission may remain an implementation detail. | P3 |
| F14 | Duplicate backend entry points are resolved by discovery order. | Reject duplicate matches before loading either plugin, with a deterministic ambiguity diagnostic. | P2 |

No-change dispositions: exact backend API equality, dump header/version order,
queue and Redis key grammar, positional Lua result protocols, registered CLI
option order, deterministic dump ordering, and foreign-target ownership checks
remain strict because their exactness is correctness, compatibility, or
destructive-ownership policy.

## Source Documents

Source specs:

- `docs/specs/10-cli.md` [SB-CLI-2], [SB-CLI-4]
- `docs/specs/11-delivery.md` [SB-DELIVERY-1], [SB-DELIVERY-3],
  [SB-DELIVERY-5]
- `docs/specs/13-message-identity.md` [SB-ID-1]
- `docs/specs/16-python-library-api.md` [SB-API-2], [SB-API-6],
  [SB-API-11]
- `docs/specs/17-ops.md` [SB-OPS-7]

Theory:

- `docs/program-theory.md` [THEORY-1] requires a small and predictable queue
  tool rather than one whose correctness depends on hidden storage order.
- `docs/program-theory.md` [THEORY-3] keeps broker storage, backend ownership,
  and application execution in separate conceptual owners.
- `docs/program-theory.md` [THEORY-4] requires explicit failure, matching
  CLI/Python meaning, compatibility for published surfaces, and no new concept
  without concrete pressure. The state separation below replaces accidental
  coupling; it does not add public configuration or user workflow.

Implementation and process owners:

- `docs/implementation/06-process-session-core-ownership.md`
- `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`
- `docs/lessons.md` coverage-shard recovery rule

Historical context, not implementation authority:

- `docs/plans/2026-08-24-phaselock-status-temp-ownership-plan.md`
- `docs/plans/2026-08-25-verified-review-findings-remediation-plan.md`
- retired `docs/plans/2026-07-29-process-session-core-factory-plan.md` at
  source SHA `197629e2` in the Status Index ledger

## Baseline and Reproduction Receipts

Plan and spec baseline: `b8cfa509f8eb373b44416dedbc327b0e66530679`.

- A v6 SQLite fixture was rejected by the v5 client only after
  `idx_messages_queue_ts` had been dropped.
- An unversioned v1-style database with duplicate timestamps failed the v3
  migration but durably reported schema version 5, with only part of the
  current schema present.
- A migrated legacy database with a valid `schema-v5` marker accepted duplicate
  timestamps after its unique index was dropped and the database was reopened.
- A physically reordered v1 SQLite `messages` and `meta` schema migrated to v5
  and operated successfully; product migration code is not currently ordinal.
- SQLite documentation states that `RETURNING` rows have arbitrary order. Core
  currently returns those rows without normalization; PostgreSQL adds a final
  order explicitly.
- PostgreSQL connection preflight accepts `EMPTY`, while `initialize_target`
  rejects it. Redis classifies a correct older version as partial/foreign rather
  than owned but unsupported.
- Coverage.py read a database whose `meta` columns were physically reordered,
  while `_repair_schema_version_marker()` rejected the same logical schema.
- `_validate_safe_path_components()` rejected valid POSIX `#` and parentheses;
  nested backend options were rejected before plugin dispatch; type-distinct
  key material froze equal.

If a finding no longer reproduces when its slice begins, inspect the intervening
change and mark the slice superseded with evidence. Do not stack a second fix
on already-correct code.

## Spec Baseline

- `b8cfa509f8eb373b44416dedbc327b0e66530679` —
  `docs/specs/10-cli.md`, `docs/specs/11-delivery.md`, and
  `docs/specs/16-python-library-api.md` at plan authoring time.
- Promotion baseline: uncommitted spec delta against
  `b8cfa509f8eb373b44416dedbc327b0e66530679`, SHA-256
  `d9a6d9d2d737a19bd008d4f5ea4ac1ec665ae11c4aa6a6d13ac2e385572c767a`.

## Proposed Spec Delta

Promotion strategy: **A — in-file text before link claims**. Promote the text
below after plan review and before runtime implementation. Do not add
implementation-link claims until the owning code and firing tests land in the
same slice. Existing active spec files remain active; no file classification
changes.

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/10-cli.md` | A | [SB-CLI-2] semantic path admission |
| `docs/specs/11-delivery.md` | Existing normative text; no new rule | [SB-DELIVERY-3] verification mapping only after implementation |
| `docs/specs/13-message-identity.md` | Existing normative text; no new rule | [SB-ID-1] verification mapping only after implementation |
| `docs/specs/16-python-library-api.md` | A | [SB-API-2] plugin-owned nested options and session identity; [SB-API-11] target states, schema proof, cleanup, and plugin uniqueness |

### [SB-CLI-2] — append after explicit absolute and project target policy

> Path admission is based on hazards in an actual SimpleBroker or operating-
> system consumer, not on characters a shell would interpret if a path were
> later copied into an unquoted command. On POSIX, shell-only punctuation such
> as `#`, `$`, backtick, single/double quotes, parentheses, braces, semicolon,
> ampersand, exclamation, caret, pipe, and angle brackets is accepted when the
> filesystem accepts it.
> NUL and control characters, applicable traversal or containment violations,
> platform-reserved names and syntax, and punctuation still interpreted by an
> internal path-pattern consumer remain rejected. In particular, `*`, `?`,
> `[` and `]` remain rejected until every owned-file enumeration treats them
> literally, and `~` remains rejected while target consumers expand it. POSIX
> target length is governed by the effective filesystem and
> system calls; SimpleBroker does not impose a smaller product-wide total-path
> ceiling.

### [SB-API-2] — append to `BrokerTarget` and project-config target policy

> A project file's `backend_options` value must be a TOML table. Its recursive
> TOML-native values, including nested tables and arrays, are passed unchanged
> to the selected backend plugin; the core project loader does not impose a
> scalar-only option schema before plugin dispatch. The plugin owns option
> validation and normalization and returns an ordinary options dictionary that
> remains lossless through `BrokerTarget` serialization. SQLite follows the
> same ownership rule and rejects or normalizes its options through its plugin
> rather than bypassing plugin validation. TOML date/time values reach the
> plugin as native values; a plugin must normalize them to lossless target
> transport values or reject them explicitly rather than relying on the core to
> coerce them.

> Process-session identity preserves type distinctions within recursively
> supported option and configuration values. It does not merge distinct opaque
> values solely because their `repr()` strings match. When no stable value
> representation exists, process-local object identity is the safe fallback:
> creating an extra session is acceptable; sharing a session across distinct
> backend configuration is not. At session acquisition, supported mutable
> containers are recursively detached once, and the same detached snapshot is
> used for both registry identity and lazy factory construction. Later nested
> mutation of the source target or config cannot make a stored key describe
> different factory inputs.

### [SB-API-11] — append after the exact backend API handshake

> Backend target admission separates four questions: whether the target is
> owned by SimpleBroker, whether its stored version is older, current, or newer
> than this implementation, whether current-version correctness postconditions
> hold, and whether a setup coordination phase previously completed. An older
> owned target reaches its migration path, or an explicit owned-but-unsupported
> diagnostic when that backend has no migration. A newer owned target is
> rejected before any connection-wide setup or SimpleBroker-authored durable
> schema, index, metadata, or marker mutation. Opening the one normal SQLite
> connection, applying connection-local settings, and SQLite's own recovery or
> WAL coordination are outside that durable-state invariant. Foreign,
> malformed, and irrecoverably partial targets remain
> rejected. Absent targets and backend namespaces that are present but empty
> may be initialized; initialization never overwrites a foreign or partial
> target.

> A SQLite `schema-vN` phase marker is a cache hint, not schema proof. The
> marker may skip idempotent migration and repair only when database-internal
> proof metadata names the current proof algorithm and records the current
> SQLite `PRAGMA schema_version` cookie. Missing or stale proof takes the
> existing schema lock, rechecks state, runs the fact-level slow path, and
> republishes proof before phase completion. Matching proof requires only
> scalar metadata reads and does not perform table, index, or message scans on
> ordinary open. Proof metadata is additive and optional to older clients; it
> does not require a stored schema-version bump.

> Proof means only that the existing idempotent setup, migration, and repair
> routine completed successfully for this SQLite schema generation using the
> current proof algorithm. It is not a general schema attestation and does not
> attest mutable metadata or message data. The slow path remains the single
> owner of schema correctness; proof publication must not add a second schema
> validator that can drift from it.

> A backend entry-point name must resolve to exactly one installed entry point.
> Ambiguous duplicate registrations fail before either candidate is loaded.

> Process-session shutdown attempts every ordinary core, factory, and remaining
> registry cleanup that is still safe after an `Exception`. One ordinary
> cleanup exception remains primary using the existing convention; later
> failures remain available as diagnostics, but their order is not public
> behavior. A `BaseException` outside `Exception` retains its
> existing propagation priority and may interrupt later cleanup.

## Context and Key Files

### Storage admission, migration, and proof

- `simplebroker/db.py`: `BrokerCore.__init__`, `_setup_schema()`,
  `_verify_database_magic()`, and `_migrate_schema()` own setup order. The
  current `_read_explicit_sqlite_magic_before_setup()` preflight checks only
  foreign magic through `SQLiteRunner.get_connection()`. Despite its position,
  that is the normal read/write connection and applies connection settings.
- `simplebroker/_runner.py` and
  `simplebroker/_backends/sqlite/runtime.py` show that this first normal open
  applies only connection-local `busy_timeout` and `wal_autocheckpoint` before
  the explicit connection setup phase. Reuse this connection; do not add a
  read-only or raw-file preflight open.
- `simplebroker/_backends/sqlite/schema.py`: `initialize_database()` mixes
  fresh bootstrap and legacy preparation, then `migrate_schema()` owns
  fact-level v2-v5 transitions. Keep one migration owner; do not add a second
  validator that can disagree with it.
- `simplebroker/_sql/sqlite.py`: owns schema DDL, exact-name index probes, and
  claim/move `RETURNING` queries.
- `simplebroker/_runner.py` and `simplebroker/_phaselock.py`: own phase locking,
  xattr/fallback markers, and marker publication after operation success. Keep
  the existing phase name and sidecar set. The proof lives in the database,
  not a new sidecar format.
- `tests/test_sqlite_admission.py`, `tests/test_sqlite_schema.py`,
  `tests/test_runner_error_handling.py`, `tests/test_phaselock.py`, and
  `tests/test_core_persistence_transition_tables.py` own the real SQLite and
  coordination proofs.

The proof consists of two internal `meta` rows: one integer proof-algorithm
version and one integer SQLite schema cookie captured only after the complete
current slow path succeeds. Ordinary data writes do not change the SQLite
schema cookie. A new proof algorithm can invalidate old proof without changing
the product schema version.

The proof is deliberately narrow. It records that the existing idempotent
setup, migration, and repair owner completed for the schema cookie. It does not
turn ordinary open into an inventory of columns, indexes, mutable metadata, or
message data. Task F7 still replaces the one incorrect exact-name index check
inside the existing slow path with the semantic check needed for timestamp
uniqueness. That local repair rule is not a new general validation framework.

After all existing slow-path work commits, a small `BEGIN IMMEDIATE` verifies
that the stored product schema version is still current, reads the SQLite
schema cookie, writes both proof rows, and commits. The phase marker may publish
only after that commit. A crash after repair but before proof publication causes
another idempotent slow pass; it cannot create a false fast path.

### Delivery ordering boundary

- `simplebroker/_sql/_contract.py`, `simplebroker/_sql/sqlite.py`, and
  `extensions/simplebroker_pg/simplebroker_pg/_sql.py` define the SQL query
  seam. Claim and move currently expose two columns to core; PostgreSQL already
  carries its order key internally.
- `simplebroker/db.py::_execute_transactional_operation()` and
  `_yield_transactional_batches()` materialize or yield runner order unchanged.
  For the built-in SQLite namespace only, normalize its keyed rows in one
  shared private helper so list and generator APIs cannot diverge. Existing
  backend-extension rows remain `(body, timestamp)` and backend API v7 remains
  unchanged.
- `tests/test_batch_operations.py`, `tests/test_message_claim.py`,
  `tests/test_move.py`, `tests/test_generator_methods.py`, and PostgreSQL FIFO
  tests own public order. Add a deterministic reversed-raw-row unit proof plus
  real-backend public integration proof.

### Backend target classification and initialization

- `extensions/simplebroker_pg/simplebroker_pg/validation.py` currently requires
  current typed metadata columns and all current tables before reporting
  ownership. `plugin.py::setup_connection_phase()` calls that classifier before
  core migration. `initialize_target()` disagrees with the preflight on EMPTY.
- `extensions/simplebroker_redis/simplebroker_redis/validation.py` currently
  folds exact current version into ownership. Redis has no released migration;
  older owned must therefore receive an owned-but-unsupported diagnostic until
  a migration exists, not be mislabeled foreign.
- Keep ownership anchors minimal and stable: correct magic plus a readable
  stored schema version. Current-version shape checks remain postconditions,
  not ownership identity.

### Paths, project options, sessions, and plugins

- `simplebroker/_constants.py::_validate_safe_path_components()` owns lexical
  path policy. Inventory every caller in `simplebroker/cli.py`,
  `simplebroker/_paths.py`, `_project_config.py`, and SQLite cleanup/phase-lock
  enumeration before relaxing a character.
- `simplebroker/_project_config.py` currently rejects non-scalar option values
  before `get_backend_plugin()`. Route raw recursive values through
  `BackendPlugin.init_backend()` for every backend, including SQLite.
- `simplebroker/_key_material.py` feeds `_SessionKey` and activity-waiter
  identity. Recursively detach standard mappings/lists/tuples/sets once; use
  that same snapshot in `_SessionKey`, `_SessionSpec.backend_options`, and a
  newly constructed `ResolvedConfig` for the lazy factory. Preserve
  deterministic value identity for ordinary immutable structures while
  type-tagging primitives and sequences; use a private identity-key wrapper
  that retains a strong reference, hashes by object identity, and compares
  with `is` for opaque objects. Preserve the same opaque object in the factory
  snapshot. A bare numeric `id()` token is insufficient because the object
  could be collected and that id reused while the session key remains live.
- `simplebroker/_broker_session.py` owns core/factory/registry cleanup. Reuse
  the repository's first-error-plus-ordered-notes convention; do not create a
  general cleanup framework.
- `simplebroker/_backend_plugins.py::_load_entry_point_plugin()` must detect
  duplicate matches before calling either `load()`.

### Developer tooling and contract tests

- `bin/coverage_combine.py::_repair_schema_version_marker()` is a narrow repair
  of third-party coverage state. Preserve its transaction, installed
  `SCHEMA_VERSION`, required tables/columns, conflicting-version rejection, and
  post-repair `CoverageData.read()` proof; remove only ordinal and harmless-
  extra-object assumptions.
- `docs/lessons.md` and the coverage transition table in
  `tests/test_dev_scripts.py` currently say every table and column matches
  exactly. Revise them to say required installed structure matches by semantic
  name/role.
- `simplebroker/commands.py::_JSON_ERROR_KEYS` and CLI tests pin mapping
  iteration order even though JSON object order is not semantic. Keep the
  closed field vocabulary; remove only the order assertion.

## Required Comprehension Gate

Before runtime edits, the implementer records answers in the Execution Log.
An incorrect or missing answer blocks that slice until the cited owners are
reread.

1. **What may happen before an owned newer SQLite version is rejected?**
   Expected: the one normal runner connection may open, apply its connection-
   local busy-timeout/autocheckpoint values, perform SQLite-owned recovery or
   WAL coordination, and issue scalar admission/proof reads. No
   `SetupPhase.CONNECTION`, journal-mode/auto-vacuum setup, DDL, index cleanup,
   metadata write, xattr/status publication, or optimization may occur.
2. **Why is the proof cookie not heavyweight validation?** Expected: normal
   open reads scalar meta values and `PRAGMA schema_version` in one statement;
   only a missing or mismatched proof enters fact-level migration/repair under
   the existing lock.
3. **Why does an unversioned legacy database start at the oldest supported
   baseline rather than current?** Expected: it already contains storage whose
   required transitions have not published; current is valid only for a truly
   fresh bootstrap or a fully completed migration sequence.
4. **Why can claim/move results not be sorted by public timestamp?** Expected:
   FIFO is storage insertion order; exact-ID insertion can make timestamp order
   differ from storage `id`/`order_id`.
5. **Which path characters remain blocked after F8?** Expected: characters
   with an actual consumer or platform hazard remain blocked, including
   control/NUL, currently glob-interpreted `*`, `?`, `[` and `]`, and `~` while
   consumers call `expanduser`; shell-only punctuation is not a filesystem
   safety boundary.
6. **Why does Redis older-version ownership not imply it can be migrated
   today?** Expected: ownership and migration support are separate. It should
   fail as owned-but-unsupported until a migration exists, without being
   mislabeled foreign or overwritten by init.
7. **Why is process-local identity safer than `repr()` for opaque config?**
   Expected: equal repr is not semantic equality. Extra sessions cost
   resources; incorrectly shared sessions cross backend configuration and can
   violate correctness.
8. **Why does coverage repair remain fail-closed after order checks are
   removed?** Expected: required installed tables/columns, repair-relevant
   structure, version-row states, transaction success, and a real coverage.py
   read remain required; only representation order and harmless extras stop
   deciding compatibility.

## Architecture and State Flow

### Target admission state model

```text
                      early ownership anchor
                               |
              +----------------+----------------+
              |                |                |
           absent/empty      foreign        owned + version
              |                |                |
              v                v         +------+------+------+
          initialize       reject,       |             |      |
          under owner      no writes    older        current  newer
                                          |             |      |
                                   migrate or       proof?     reject,
                                   explicit no-     /    \     no writes
                                   migration error yes   no
                                                   |     |
                                                fast   lock, recheck,
                                                open   repair, publish proof
```

Ownership answers “is this ours?” Version answers “which direction is safe?”
Postconditions answer “is current storage correct?” The phase marker answers
only “did coordinated setup previously complete?” No one answer substitutes
for another.

### SQLite marker fast and slow paths

```text
single-connection magic/version/proof admission
        |
        +-- newer/foreign --> fail before mutation
        |
        v
existing schema-v5 marker?
        |
        +-- no --> schema lock --> bootstrap/migrate/repair --> proof rows --> marker
        |
        v
one statement reads proof_version + proof_cookie + PRAGMA schema_version
        |
        +-- exact match --> ordinary open, no schema enumeration or DDL
        |
        `-- missing/mismatch --> schema lock --> recheck --> slow path once
```

The proof rows travel with backups. A restored current database with matching
proof remains fast. An older, partially migrated, replaced, or DDL-modified
database lacks matching proof and cannot borrow a stale path-level marker.

### SQLite delivery order seam

```text
SQLite SELECT chooses ids ORDER BY id
        |
SQLite UPDATE ... RETURNING id, body, timestamp
        |
shared core helper sorts built-in SQLite rows by id inside the transaction
        |
core strips private key, commits, returns/yields existing public shape
```

## Invariants and Constraints

- A newer owned target has the same logical schema, indexes, metadata, durable
  persistence settings, and messages after rejection. The test permits the
  already-documented normal connection's SQLite-owned recovery/WAL
  coordination and connection-local PRAGMAs; it forbids SimpleBroker setup,
  repair, marker, or optimization work.
- Normal SQLite open does not enumerate tables/indexes or scan messages when
  marker and database-internal proof match.
- The normal valid-marker path uses the one retained runner connection and one
  scalar statement that reads the selected `meta` rows and
  `PRAGMA schema_version` from the same SQLite statement snapshot, with no later
  duplicate magic/version verification query. Add an observation test for this
  budget; do not add a read-only, immutable, or raw-file preflight.
- Missing proof is an invalid cache entry, not corruption. It enters the
  existing idempotent slow path and becomes fast after one successful open.
- Marker trust is conditional in both places where phase completion is checked:
  before lock acquisition and again while holding the lock. A marker with stale
  proof must not short-circuit on either the xattr or fallback-status path.
- Proof version identifies the setup/repair algorithm that published it.
  Change it when that algorithm changes in a way that makes an older success
  receipt insufficient for the current fast path.
- Keep `schema-v5`, existing xattr/fallback status files, and the phase lock.
  Add no sidecar, daemon, background validator, timestamp, checksum, or random
  database identifier.
- Proof metadata is additive and optional. Do not bump SQLite's stored schema
  version merely to cache proof; older clients must ignore the extra rows.
- Schema version records only committed migration facts. It never publishes an
  intended final state before the corresponding transition commits.
- FIFO is storage insertion order for each queue across every materialized and
  generator claim/move surface. Public timestamps and engine return order are
  not substitutes.
- Physical column order, harmless extra tables/fields, JSON member order,
  plugin discovery order, cleanup iteration order, and shell-only pathname
  syntax do not decide compatibility or correctness.
- Exact backend API version equality remains at v7. Only the built-in SQLite
  query and its private core normalization change; PostgreSQL and other SQL
  extensions retain their existing ordered `(body, timestamp)` rows, and Redis
  retains its direct-core path.
- Foreign or partial targets are never initialized over. Empty PostgreSQL
  schemas are safe only because inspection found no objects.
- Process cleanup attempts every action still safe after an ordinary failure;
  preserve one failure as primary and retain the others without making their
  order contractual. Do not swallow or flatten exceptions.
- Path relaxation is consumer-driven. If a character is interpreted by an
  internal glob, URI, environment, or platform layer, either keep it rejected
  or make that consumer literal-safe in the same slice.
- Plugin options remain plugin-owned. Core must not recursively reinterpret,
  coerce, redact, or silently drop nested values.
- Session acquisition uses one detached snapshot for both identity and factory
  inputs. Supported nested mutation after acquisition cannot change an already-
  keyed lazy factory; opaque values retain identity rather than being copied or
  represented as text.
- Coverage recovery remains narrow and transactional. Never repair a missing
  required table/column, conflicting schema version, unreadable measurement,
  or failed commit.
- No new runtime dependency, public CLI flag, public configuration key, new
  process, or asynchronous work.

## Rollback, Rollout, and One-Way Doors

Rollback shapes the slice order:

1. Land non-mutating newer-version admission and legacy version publication
   before relying on any new proof behavior.
2. Land database-internal proof with the existing marker format. Before
   publication it is independently revertible; after rollback, extra proof
   rows are harmless and ignored by older clients.
3. Land the SQLite FIFO correction without changing backend API v7. Existing
   PostgreSQL ordered-row and Redis direct-core proofs must stay green.
4. Land acceptance widenings only after all path/config consumers pass. Once a
   release accepts a path spelling or nested plugin option, re-rejecting it is
   a public compatibility break; that widening is a one-way contract door.
5. Developer-tooling/test-only corrections can land independently after their
   contract text and lesson wording agree.

No task rewrites message rows or removes user data. The only storage addition
is optional proof metadata. Index repair may add a missing correctness index;
it must never drop an unknown or conflicting user-visible index automatically.

Rollout success signals:

- exact-SHA core and extension CI passes on Linux, macOS, and Windows;
- SQLite old/current/new, fallback-marker replacement, and concurrent-open
  probes pass without mutation or repeated slow-path work;
- PostgreSQL empty-schema init and owned-old/current/new classifier tests pass;
- Redis owned-old/current/new diagnostics are explicit and non-mutating;
- the Weft pinned/programmatic-use compatibility check passes before release;
- no release is published while any class-5 spec, changelog, package floor, or
  verification mapping is out of sync.

Stop the whole plan and re-review if the proof requires a new sidecar, stored
schema-version bump, data scan on every open, or changed phase-lock failure
ordering; if FIFO correction requires a backend API change; or if path widening
exposes a literal-unsafe enumeration not fixed in the same slice.

## Dependency-Ordered Tasks

### 1. Promote the reviewed contract text

- Files to touch: `docs/specs/10-cli.md`,
  `docs/specs/16-python-library-api.md`, and their `## Related Plans` sections.
- Apply the exact proposed text with strategy A. Do not add implementation-link
  claims yet.
- Record the promotion baseline identifier in `## Spec Baseline`.
- Run spec/plan structural gates. Stop if the proposed text requires a new
  public surface or conflicts with a more specific vertical contract.
- Done signal: promoted text has independent review, exact links resolve, and
  no code cites an unpromoted plan-only rule.

### 2. Make SQLite admission and version publication non-mutating and factual

- Files to touch: `simplebroker/db.py`,
  `simplebroker/_backends/sqlite/schema.py`, targeted SQLite admission/schema
  tests, `docs/implementation/06-process-session-core-ownership.md`, and
  `docs/implementation/09-storage-schema-and-claim-lifecycle.md`.
- Extend `_read_explicit_sqlite_magic_before_setup()` into one early admission
  snapshot on the existing normal runner connection. Read `magic`, stored
  `schema_version`, the optional proof version/cookie rows, and the SQLite
  schema cookie in one statement snapshot on that connection. If `meta` does
  not exist, catch only the expected missing-table outcome and fall through to
  the existing locked fresh-or-legacy path. Under that lock, an empty target is
  fresh while a pre-existing `messages` table follows legacy preparation. Do
  not add a general shape classifier before migration. A matching magic xattr
  may remain positive ownership evidence, but it must not skip the stored-
  version read. Reject newer before `SetupPhase.CONNECTION`. Carry the snapshot
  only through schema admission so a valid marker/proof can reuse it. Inventory
  every `_verify_database_magic()` caller with `rg`; remove only the duplicate
  setup-path query and retain any caller that serves another boundary.
- Split truly fresh bootstrap from existing unversioned legacy preparation. A
  pre-existing `messages` table with no stored version starts at the oldest
  supported baseline; migration callbacks advance versions only inside their
  committing transactions.
- Encode the admission outcomes as a transition table: absent/empty or missing
  legacy metadata continues to bootstrap/legacy classification; explicit
  foreign magic rejects; correct magic plus one parseable version below or at
  current continues; correct magic plus a newer version rejects; and correct
  magic plus duplicate, non-integer, boolean, or below-baseline version data
  rejects as malformed before setup. A correct cached magic xattr can establish
  ownership but cannot replace the version read. Do not introduce a current-
  shape classifier before migration.
- Red tests first: future-version durable-state immutability, proof that only
  the retained normal connection opens and no setup phase runs for that
  refusal, and failed unversioned-v1 migration version receipts.
- Stop if admission adds a second connection or raw-file read, starts a setup
  phase, or requires exact physical order/table equality.
- Done signal: a newer fixture's logical schema/index/meta/message and durable-
  setting snapshots are unchanged after refusal; one normal connection was
  used; the duplicate-timestamp legacy fixture publishes only completed steps.

### 3. Replace name-based schema proof with semantic slow-path proof

- Depends on task 2.
- Files to touch: `simplebroker/_backends/sqlite/schema.py`,
  `simplebroker/_sql/sqlite.py`, `simplebroker/_runner.py`,
  `simplebroker/_phaselock.py`, optionally a narrow SQLite semantic-inspection
  helper, phase-lock/schema tests, and implementation doc 09.
- Replace exact-name timestamp uniqueness detection with semantic inspection of
  `PRAGMA index_list`, `index_info`/`index_xinfo`, uniqueness, indexed columns,
  and partial status. Accept the table's `UNIQUE(ts)` autoindex. If the
  established name exists with the wrong definition and no correct equivalent
  exists, fail with an actionable schema diagnostic; do not drop it.
- After the complete idempotent current-schema slow path succeeds, store the
  internal proof-algorithm version and current SQLite schema cookie in `meta`
  before the phase service publishes completion.
- Add one narrow conditional-completion seam shared by `SQLiteRunner` and
  `PhaseLockService`. A schema marker skips only when its proof predicate is
  true. Evaluate that predicate against the early admission snapshot before
  acquiring the lock, then evaluate fresh database facts again while holding
  the lock. A false predicate must bypass both the xattr and fallback marker
  short-circuits without deleting either marker; the successful callback
  commits proof before ordinary marker publication. Do not expose a general
  force-run flag that could bypass unrelated phase guarantees.
- On marker hit, use the early magic/version/proof/cookie snapshot. Exact proof
  skips migration only when both proof rows are present, integer-valued, and
  match the current algorithm and cookie; missing/mismatch reaches the
  conditional phase-lock seam. The under-lock recheck lets concurrent waiters
  observe the first repair and skip duplicate work. The fast path deliberately
  does not attest mutable `meta` rows.
- Keep the existing idempotent setup/migration/repair routine as the only
  schema-correctness owner. After it succeeds, a small immediate transaction
  rechecks only the stored current product version, captures the cookie, writes
  proof, and commits. Do not add a second column, index, metadata, or data
  inventory merely to publish proof.
- Use real xattr and forced-fallback paths where supported. Test file
  replacement, stale existing xattr, stale fallback status, same-inode DDL
  drift, concurrent open, failure between repair and proof publication,
  `compact` followed by one conservative repair and then a fast open, and a
  second ordinary fast open.
- Stop if the normal fast path needs `sqlite_master`, `PRAGMA table_info`, index
  enumeration, or message data; if proof publication becomes best-effort; or
  if a new marker file/format is proposed.
- Done signal: matching proof performs no repair/DDL; every stale-proof case
  enters one serialized slow path and ends with valid proof.

### 4. Make SQLite FIFO independent of `RETURNING` order

- Can begin after plan review.
- Files to touch: `simplebroker/_sql/sqlite.py`, `simplebroker/db.py`,
  FIFO suites, docs spec 11 delivery verification, docs spec 13 [SB-ID-1]
  storage-order verification, and implementation doc 09. Do not touch backend
  API constants, extension plugin versions, or release floors.
- Make built-in SQLite claim/move queries return `(id, body, timestamp)`. In one
  shared private core helper used by materialized and generator transactions,
  validate that built-in SQLite shape, sort ascending by `id`, and strip `id`
  before commit/return or yield. Existing backend extension rows remain the
  ordered `(body, timestamp)` contract and bypass this normalization. Dispatch
  by the selected built-in SQLite plugin/namespace, not by tuple arity; do not
  widen `BackendSQLNamespace` or teach extension plugins a second row shape.
- Red test the helper with deliberately reversed built-in SQLite rows. Pair it
  with real SQLite public tests using exact IDs whose timestamp order differs
  from insertion order. Keep the existing PostgreSQL ordered-query and Redis
  direct-core FIFO suites as regression gates; neither implementation changes.
- Stop if list and generator paths need separate normalization, if public
  return shapes change, or if the fix requires a backend API version change.
- Done signal: forced reversed raw results still produce FIFO on every public
  SQLite claim/move surface and backend API v7 files are unchanged.

### 5. Separate first-party backend ownership, compatibility, and readiness

- Can run in parallel with tasks 2-4 after task 1.
- Files to touch: PostgreSQL and Redis `validation.py`, `plugin.py`, schema
  tests, initialization tests, and spec 16 verification mappings.
- Introduce private inspection results that preserve ownership separately from
  version relation. Use stable minimal ownership anchors; perform current-shape
  postconditions after admission or through migration/repair.
- PostgreSQL `initialize_target` accepts ABSENT and EMPTY exactly as its
  initialization preflight does. It continues to reject FOREIGN and PARTIAL.
- Redis reports a correct old version as owned-but-unsupported until a real
  migration exists. Add a transition-table or equivalent enumerable fixture
  proving that a future old-version migration cannot be blocked by ownership
  classification. Reuse the existing unsupported-version exception and CLI
  mapping if they fit; enumerate that observable outcome in the verification
  mapping rather than inventing a new exit code without a contract gap.
- Test old/current/new and missing/corrupt magic/version independently. Use
  real PostgreSQL/Redis integration for absent/empty/current paths; inspection
  fakes are acceptable only for otherwise-unconstructable future versions.
  Run the existing PostgreSQL initialization concurrency proof with two EMPTY
  initializers; do not add a new locking scheme unless admitting EMPTY is shown
  to bypass the current serialization.
- Stop if a current required table/field is reintroduced into ownership or if
  `init` could overwrite any non-empty unknown namespace.
- Done signal: every state has one explicit outcome, and old owned state reaches
  migration/support classification before current-shape validation.

### 6. Remove non-semantic path and project-option admission constraints

- Can run in parallel with tasks 2-5 after task 1.
- Files to touch: `simplebroker/_constants.py`, `simplebroker/cli.py`,
  `simplebroker/_paths.py`, `simplebroker/_project_config.py`, SQLite plugin
  initialization, project/path/cleanup/security tests, guides, spec 10 and spec
  16 verification mappings, and `CHANGELOG.md` in final reconciliation.
- Inventory actual path consumers first. Replace the broad Unix shell-character
  set with hazard-specific sets. Permit the exact shell-only punctuation named
  in [SB-CLI-2] on POSIX. Retain glob metacharacters until enumeration is
  literal-safe; do not widen Windows reserved syntax. Remove the 1,024-character
  POSIX product limit and test at a filesystem-supported long path without
  assuming one universal OS maximum.
- Change project loading to validate only the top-level options table. Route
  recursive values to every selected plugin. Require SQLite's plugin to reject
  unsupported options rather than silently discard them. Require returned
  options to round-trip through target serialization. Include TOML date/time
  values: prove plugin normalization or plugin-owned rejection before target
  construction.
- Test explicit `-f`, project discovery, init, cleanup, fallback status paths,
  and error JSON with real directories containing newly accepted punctuation.
  Test a fake plugin that normalizes nested TLS/pool options and a plugin that
  rejects them with its own diagnostic.
- Stop if any newly accepted character remains pattern-interpreted, or if core
  must understand a plugin's nested schema.
- Done signal: actual-hazard negatives remain red/green; valid punctuation and
  nested plugin options succeed through public entry points and serialization.

### 7. Make process identity, cleanup, and plugin discovery order-safe

- Can run in parallel with tasks 2-6 after task 1.
- Files to touch: `simplebroker/_key_material.py`,
  `simplebroker/_broker_session.py`, `simplebroker/_backend_plugins.py`,
  `simplebroker/sbqueue.py` only if waiter identity needs the shared fix,
  session/key/plugin tests, spec 16 verification, and implementation doc 06.
- Type-tag mappings, sequences, sets, primitives, and opaque fallback. Preserve
  order-insensitive mapping/set identity while distinguishing bool/int/float,
  list/tuple, and distinct same-repr opaque objects. Build one recursive
  acquisition snapshot of supported containers and derive both `_SessionKey`
  and `_SessionSpec` from it. Test mutation after acquire but before first lazy
  core creation, then acquire a second target with the original values; it must
  not reuse a factory whose live inputs drifted. Cover both backend options and
  `ResolvedConfig` extras.
- In session and registry `close_all`, detach ownership under the existing
  locks, then attempt every safe close outside the lock. Preserve one ordinary
  error as primary using the repository's existing convention and retain later
  core, factory, and session failures as diagnostics. Do not add creation-order
  tracking or make diagnostic order a contract. Preserve idempotence and no-
  reentry after `_closed`.
- Materialize all matching backend entry points before loading. Zero remains
  unknown; one follows existing load/name/API checks; more than one raises a
  deterministic ambiguity error independent of discovery order.
- Use controlled failure doubles to prove every cleanup is attempted and every
  failure remains observable, plus one real process-session smoke. Do not mock
  registry locks, refcounts, or close-state transitions in the integration
  proof. Tests must not make failure ordering contractual.
- Stop if cleanup requires holding the registry/session lock across backend
  close, if opaque values are serialized or hashed by repr, or if ambiguity is
  resolved by sorting and choosing a winner.
- Done signal: permutations produce the same identity/discovery result, and
  every ordinary cleanup action fires once despite earlier failures.

### 8. Correct coverage and JSON test-only representation contracts

- Can run independently after plan review.
- Files to touch: `bin/coverage_combine.py`, `tests/test_dev_scripts.py`,
  `docs/lessons.md`, and JSON assertions including
  `tests/test_cli_contract_sb_cli.py`, `tests/test_symlink_security.py`, and
  `tests/test_vacuum_compact.py`.
- For coverage repair, compare required tables/columns by name and validate
  only repair-relevant installed semantics. Permit column reordering and
  harmless extra tables. Preserve conflicting-version, missing-required-
  structure, unreadable-data, and commit-failure rejection. Require a real
  `CoverageData.read()` after repair before combination succeeds.
- Rewrite the durable lesson and transition guard from “matches exactly” to
  the new semantic rule. This process-guidance change requires the +P review.
- Replace `tuple(payload)` JSON assertions with exact key-set, type, code, and
  value assertions. This applies the existing JSON object contract; it adds no
  spec text and does not change deterministic emission or dump ordering.
- Stop if the coverage change accepts a shard that installed coverage.py still
  cannot read or if it repairs more than the schema-version marker anomaly.
- Done signal: reordered/extra-object compatible shards repair; every corrupt
  or incomplete fixture remains a hard failure; JSON key permutations satisfy
  contract tests.

### 9. Reconcile contracts, run the release gate, and close the plan

- Depends on tasks 2-8.
- Files to touch: affected spec verification tables and `## Related Plans`,
  implementation docs 06/09, guides, root/package READMEs if their restatements
  are affected, `CHANGELOG.md`, this plan, and the Status Index.
- Each implementation slice adds its landed firing tests to the owning
  verification table. Add narrative plan/implementation links only here, once
  the referenced code exists.
- Run the full core and first-party extension suites, static/docs gates,
  package builds and isolated artifact smoke, parser/property/fuzz probes where
  path/config parsing changed, interface review, Weft compatibility, and an
  independent final diff review from a different agent family.
- Record the owner-supplied Weft compatibility command before release. If no
  durable command exists, record the exact pinned install and programmatic
  import/open smoke used; do not invent a new framework in this slice.
- Publish nothing during implementation. Publication is a separate owner-
  authorized release action after exact-SHA hosted jobs pass.
- Stop if any spec-to-test inventory row lacks a firing test, backend API v7
  changed without a new reviewed need, or the active plan has an unresolved
  deviation/review item.
- Done signal: all gates pass from current state, plan evidence is recorded,
  the index row flips to `completed` in the implementation change, and every
  published-contract delta appears in the changelog.

## Testing Plan

### Execution and failure-path coverage

```text
SQLite open
  +-- foreign magic ---------------------- reject before SB setup mutation
  +-- owned newer ------------------------ reject before SB setup mutation [CRITICAL]
  +-- absent/empty ----------------------- fresh current bootstrap
  +-- unversioned legacy ---------------- seed oldest, migrate stepwise
  |    `-- duplicate ts at v3 ------------ fail with last completed version
  +-- current + marker + valid proof ----- scalar fast path only
  +-- current + marker + missing proof --- one locked repair
  +-- current + marker + changed cookie -- one locked repair
  +-- fallback marker + replaced DB ------ replacement state wins
  `-- concurrent stale proof ------------- one owner repairs, waiters observe

Claim/move
  +-- raw rows already ordered ----------- existing public FIFO
  +-- SQLite keyed rows reversed --------- shared helper sorts by private id
  +-- exact IDs oppose timestamp order --- insertion/storage FIFO wins
  +-- materialized list ------------------ commit then ordered return
  `-- generator batch -------------------- ordered yield, existing settlement

Backend target
  +-- absent / empty --------------------- initialize when safe
  +-- foreign / partial ------------------ reject, no overwrite
  +-- owned older + migration ------------ migration reachable
  +-- owned older + no migration --------- explicit owned-unsupported error
  +-- owned current ---------------------- validate/use
  `-- owned newer ------------------------ reject before mutation

Representation boundaries
  +-- valid POSIX shell punctuation ------ accepted through real filesystem
  +-- actual path hazards ---------------- rejected before side effects
  +-- nested plugin options -------------- plugin receives and normalizes
  +-- type-distinct/opaque config -------- distinct sessions/waiters
  +-- cleanup failures ------------------- every safe cleanup attempted
  +-- duplicate entry points ------------- deterministic ambiguity error
  +-- reordered coverage columns -------- narrow repair succeeds
  +-- incomplete coverage schema -------- remains hard failure
  `-- JSON member permutation ------------ same object contract
```

### Anti-mocking rules

- Keep real SQLite files, single-connection admission, DDL, schema cookies, xattr or
  forced-fallback phase locks, file replacement, transactions, and public
  `BrokerDB`/`Queue` construction in storage tests. Spies may observe that the
  slow path did not run; they may not replace the database proof.
- Keep real SQLite and PostgreSQL in FIFO integration tests. A controlled
  reversed-row unit seam is required because the engine is allowed, not
  required, to reorder `RETURNING` output.
- Keep real PostgreSQL schemas and Redis namespaces for ordinary
  absent/empty/current integration. Mock only impossible future-version rows
  and network-independent classifier branches.
- Keep real filesystem names for path tests and real target serialization for
  nested options. A fake plugin is appropriate because plugin ownership is the
  contract under test.
- Cleanup failure doubles are appropriate to force precise exception order;
  registry locks, refcounts, state flags, and at least one first-party session
  remain real.
- Coverage tests use real coverage.py databases and readers. Do not construct a
  look-alike SQLite schema without confirming `CoverageData.read()` behavior.

### Firing test inventory

| Finding | Required firing proof |
|---------|-----------------------|
| F1 | Future-version public open uses no extra preflight connection, enters no setup phase, and leaves logical schema/index/meta/message state plus durable persistence settings unchanged. |
| F2 | Reversed raw SQL rows and real exact-ID fixtures return storage FIFO for claim/move list and generator APIs. |
| F3 | Valid proof fast path; missing proof; cookie mismatch after DDL; stale xattr and fallback marker; fallback replacement; concurrent repair; proof-publication fault; compact→one conservative repair→fast open. |
| F4 | Unversioned legacy success advances 1→5; v3 failure leaves only the last committed version and no completion marker/proof. |
| F5 | PostgreSQL and Redis owned older/current/new matrices; old ownership reaches migration/support decision. |
| F6 | Real pre-created empty PostgreSQL schema initializes, including the existing two-initializer concurrency proof; non-empty foreign/partial remains unchanged. |
| F7 | Autoindex accepted with no duplicate; equivalent named index accepted; wrong same-named index rejected; duplicate data diagnostic preserved. |
| F8 | POSIX accepted punctuation across explicit/project/init/cleanup; controls, traversal, reserved names, and glob metacharacters remain rejected. |
| F9 | Nested table/array reaches fake plugin, normalizes, serializes; plugin rejection owns diagnostic; SQLite does not silently ignore options. |
| F10 | bool/int/float, list/tuple, same-repr opaque values, mapping/set permutations, mutate-after-acquire-before-lazy-create for options/config extras, second acquirer, and waiter/session consumers. |
| F11 | Multiple core/factory/session failures all attempted once and all remain observable without an order assertion; BaseException priority; repeated close safe. |
| F12 | Reordered columns and harmless extra table repair; missing column/table, conflicting versions, corrupt measurement, and failed commit reject. |
| F13 | Error JSON exact keys/types/values with no member-order assertion; dump order tests unchanged. |
| F14 | Duplicate entry-point permutations reject before load; zero/one and mismatched-name/API cases retain behavior. |

Apply the adversarial parser/CLI floors from
`docs/agent-context/runbooks/adversarial-acceptance-probes.md` to path and
project-option input: valid, omitted/empty, malformed, boundary size, hostile
Unicode/control, wrong type, duplicate/ambiguous source, and no-side-effect
failure cases.

## Verification and Gates

Per-task commands are refined to exact node IDs during implementation. Minimum
targeted gates:

```bash
uv run pytest -q tests/test_sqlite_admission.py tests/test_sqlite_schema.py tests/test_runner_error_handling.py tests/test_phaselock.py tests/test_core_persistence_transition_tables.py
uv run pytest -q tests/test_batch_operations.py tests/test_message_claim.py tests/test_move.py tests/test_generator_methods.py
uv run pytest -q tests/test_path_security.py tests/test_project_config.py tests/test_cleanup.py tests/test_symlink_security.py
uv run pytest -q tests/test_key_material.py tests/test_process_broker_session.py tests/test_backend_plugin_resolution.py
uv run pytest -q tests/test_dev_scripts.py tests/test_cli_contract_sb_cli.py tests/test_json_output.py
uv run pytest -q extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py extensions/simplebroker_pg/tests/test_pg_fifo_semantics.py
uv run pytest -q extensions/simplebroker_redis/tests/test_redis_plugin_validation_paths.py
```

Final local gates:

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run --frozen --no-sync mypy simplebroker bin/release.py bin/ruff_suppression_index.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
mapfile -t core_test_files < <(find tests -type f -name '*.py' -not -path '*/__pycache__/*' -not -path 'tests/typecheck_fixtures/*' | sort)
MYPYPATH=. uv run --frozen --no-sync mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs "${core_test_files[@]}"
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

Also run the extension suites against real configured PostgreSQL and Redis,
their package mypy/lint gates, root and extension build/install smoke tests,
release-policy/API-version synchronization tests, affected fuzz/property
suites, the agent-facing interface review, the Weft compatibility gate, and
exact-SHA hosted Windows/POSIX jobs before any release. Record commands,
counts, skips, and observed results in the Execution Log; do not summarize a
red or skipped required backend as green.

## Parallelization Strategy

| Lane | Tasks | Modules | Depends on |
|------|-------|---------|------------|
| A | 1 → 2 → 3 | specs; core SQLite schema/runner/phaselock | plan review |
| B | 4 | built-in SQLite SQL; core delivery normalization | plan review |
| C | 1 → 5 | PG/Redis validation and init | plan review |
| D | 1 → 6 | core paths/project config; SQLite plugin | plan review |
| E | 1 → 7 | core session/key/plugin resolution | plan review |
| F | 1 → 8 | developer tooling; CLI contract tests; lessons | plan review |
| Final | 9 | specs/docs/release integration | A-F |

After the single spec-promotion slice, C-E may use separate worktrees; B and F
can begin after plan review. Lane A
is sequential because admission and proof share setup owners. Lanes B and C
share only regression gates, not implementation files. Every lane updates only
its verification mappings; final reconciliation resolves shared
spec/CHANGELOG/index edits.

## Independent Review Loop

Before implementation:

1. Give a different-family reviewer this plan, its full `## Proposed Spec
   Delta`, the source specs, implementation docs 06/09, and the cited runtime
   owners.
2. Ask the reviewer to challenge scope, the database-internal proof design,
   pre-migration admission, SQLite-local FIFO seam, rollback order, literal
   path safety, anti-mocking seams, and whether all 14 findings have a firing
   test.
3. The author records every point below and either revises the plan, explains
   why the current design remains best, or marks it out of scope with owner and
   reopen condition. “Could not implement confidently” blocks `active` status.

After each meaningful lane, run an independent slice review against its spec
and plan tasks. Before completion, run a different-family full-diff review plus
the interface review required for CLI/path changes. Any revision that changes an
invariant, authority boundary, or blast radius returns the plan delta to review.

## Independent Review Record

- **2026-08-25, round 1:** independent repository agent
  `boundary_validation_audit`; code/spec baseline
  `b8cfa509f8eb373b44416dedbc327b0e66530679`; pre-revision draft (content hash
  was not captured before revision). Verdict: not implementable. Findings:
  admission used an extra read-only open with an unsound zero-filesystem-
  mutation claim; stale proof could not bypass both phase-marker short-circuits;
  proof facts were not enumerated; recursive keys could drift from lazy factory
  inputs after nested mutation; [SB-ID-1] was missing; cleanup primary-error
  order and several firing probes were underspecified. Disposition: revised to
  one normal-connection snapshot and a durable SimpleBroker-mutation boundary;
  added a pre-lock/under-lock conditional marker seam, one acquisition snapshot
  for key/spec, [SB-ID-1], and missing firing probes. The round also prompted a
  broad proof inventory and ordered cleanup design that later review removed as
  unnecessary coupling.
- **2026-08-25, round 2:** same reviewer; plan SHA-256
  `76564d0157ae54e93a8d7ffc5b566e0c7a51122492b243ecefbd8d753fe2797f`.
  Verdict: one blocker remained in the now-superseded broad attestation: DML can
  change `last_ts` and `alias_version` without changing SQLite's schema cookie.
  Final disposition: narrow proof to a receipt for successful setup/repair of
  one schema generation. Mutable metadata is deliberately outside the fast-
  path receipt, so no extra sentinel reads or metadata attestation remain.
- **2026-08-25, round 3:** same reviewer; blocking-only confirmation of design
  SHA-256
  `9b58d72d99cd2c627387f5145d153c2a5c5fecb04008d2a64dbe924c09165ab3`.
  Verdict: the prior, broader design was implementable; no blocker remained.
- **2026-08-25, round 4:** Claude Code CLI, tool-less full plan/index review.
  Verdict: `PLAN IMPLEMENTABLE`; no blocker. Nine nonblocking notes asked for
  clearer Redis scope, missing-`meta` flow, verification-link timing,
  `_verify_database_magic()` caller inventory, ascending sort, downstream gate
  evidence, concurrent PostgreSQL EMPTY initialization, and review-status
  wording. Those useful points are incorporated above. The user then directed
  a separate simplicity pass rather than accepting the broader design merely
  because it was implementable.
- **2026-08-25, round 5:** Claude Code CLI, tool-less full plan/index review
  focused on brittleness and ceremony. Verdict: `PLAN IMPLEMENTABLE`; no
  blocker and no needed invariant lost. It identified the coordinated backend
  API v8 rollout as the remaining large blast-radius increase for a SQLite-only
  defect. Inspection showed SQLite cannot sort `RETURNING` in SQL, so the plan
  takes the smaller viable variant: built-in SQLite returns a private `id`, one
  shared core helper sorts and strips it, and PostgreSQL, Redis, and backend API
  v7 remain unchanged. It also prompted the explicit mutable-meta exclusion and
  removal of stale broad-proof and reverse-cleanup wording from this record.
- **2026-08-25, round 6:** Claude Code CLI, tool-less focused re-review after
  the F2 blast-radius change. Verdict: `PLAN IMPLEMENTABLE`; no blocker and no
  stale live API-v8 or schema-attestation requirement. It confirmed explicit
  built-in SQLite dispatch, one list/generator helper, unchanged extension row
  contracts and backend API v7, and requested only the F2 wording correction
  applied above.
- **2026-08-25, SQLite/FIFO implementation review:** independent repository
  agent `backend_states` found four concrete gaps: SQLite TEXT-affinity version
  rows failed the second open; separate cookie/proof reads admitted a DDL race;
  FIFO dispatch depended on the plugin singleton rather than the built-in SQL
  namespace; and the xattr stale-proof branch lacked a deterministic probe.
  All four were reproduced, fixed, and returned green. Its focused re-review
  found no remaining issue. Concurrent raw external DDL after the atomic open
  snapshot remains the explicit out-of-scope race.
- **2026-08-25, backend/representation implementation review:** independent
  repository agent `coverage_json` found that a PostgreSQL schema containing
  only non-relation objects could be misclassified as empty, and that a Redis
  namespace with an unknown exact-prefix key could be misclassified as absent.
  Both takeover hazards were reproduced and fixed. Focused re-review found no
  remaining issue.
- **2026-08-25, round 7:** Claude Code CLI, tool-less full implementation-diff
  review focused on correctness and unnecessary complexity. Verdict: no
  hot-path, data-loss, or ordinary-open defect; safe after two conditions. It
  required a mechanical no-shell-sink gate for the one-way POSIX path widening
  and alias/status proof after the bare legacy migration ladder. It also found
  a non-deterministic corrupt-file diagnostic, a dead schema-cookie cardinality
  branch, and two nonblocking design concerns. The two conditions received
  firing tests; deeply corrupt files now normalize to one diagnostic; the dead
  branch was removed. The external FIFO pass-through remains shape-agnostic
  because backend API v7 assigns row shape to the plugin, and `init_backend`
  remains the sole option-validation owner rather than adding a parallel plugin
  protocol.
- **2026-08-25, round 8:** the same Claude session reviewed only those
  dispositions and their incremental diff. Verdict: both landability conditions
  resolved, the test-only AST shell-sink gate is proportionate and has no
  runtime cost, both P3 cleanups are correct, the rejected nonblocker changes
  should remain rejected, no new defect, and **safe to proceed**.

The different-family plan-review gate is satisfied. Any later change to an
invariant, authority boundary, or blast radius still requires re-review, as
does the final implementation diff.

### Agent-facing interface review

Scope: the changed CLI SQLite-path admission and project `backend_options`
boundary in the uncommitted implementation diff against
`b8cfa509f8eb373b44416dedbc327b0e66530679`.

| Principle | Result and evidence |
|-----------|---------------------|
| 1. Context is the scarcest resource | Not applicable to this input-admission delta; no success or mutation response shape changed. |
| 2. Progressive disclosure | Met: the semantic path rule is in `docs/specs/10-cli.md:114`; plugin-owned option normalization is in `docs/specs/16-python-library-api.md:97`. |
| 3. Self-explanatory names; no lookup tables | Met: `backend_options`, `target`, and the SQLite rejection name the public concepts directly (`simplebroker/_project_config.py:181`, `simplebroker/_backends/sqlite/plugin.py:44`). |
| 4. One identity per thing | Met: resolution returns one `BrokerTarget` carrying the selected backend, target, and options (`simplebroker/_project_config.py:209`). |
| 5. Derive what is derivable | Met: the core preserves TOML-native values, while the selected plugin alone validates and normalizes them (`simplebroker/_project_config.py:191`). |
| 6. No hidden session setup | Met: an explicit project path resolves the complete target in one call, and the CLI ambient-path boundary is documented (`simplebroker/_project_config.py:170`, `docs/specs/10-cli.md:107`). |
| 7. Teach, don't reject | Met: POSIX punctuation with no live consumer meaning is accepted; only actual pattern, expansion, platform, control, and containment hazards remain rejected (`simplebroker/_constants.py:229`, `tests/test_path_security.py:171`). |
| 8. Every message carries its action | Met after IF1: the path diagnostic tells the caller what class of character to remove, and SQLite options say to remove them or select a supporting backend (`simplebroker/_constants.py:305`, `simplebroker/_backends/sqlite/plugin.py:49`). |
| 9. Atomic writes with a recovery path on conflict | Met for this single-writer admission boundary: rejection occurs before target creation (`tests/test_project_config.py:490`, `tests/test_path_security.py:557`). The concurrent-merge clause is not applicable. |
| 10. Draw the trust boundary in the interface | Met: project config is trusted developer input, while the plugin owns option meaning and target transport remains core-enforced (`docs/specs/16-python-library-api.md:89`, `simplebroker/_project_config.py:191`). |
| 11. Wire format matches the agent's mental model | Met: nested TOML tables and arrays reach the plugin in their discovered shape and are decomposed only behind that boundary (`simplebroker/_project_config.py:45`, `tests/test_project_config.py:582`). |

| ID | Severity | Location | Finding | Disposition |
|----|----------|----------|---------|-------------|
| IF1 | P3 | `simplebroker/_backends/sqlite/plugin.py:49` | The SQLite option diagnostic stated a limitation but no next action. | Incorporated: one sentence now tells the caller to remove the options or select a supporting backend; no new error type or control flow was added. |

Ratified judgment calls: retain rejection of `*`, `?`, `[`, `]`, and `~`
because live owned-file enumeration or expansion still interprets them; do not
add escaping or canonicalization machinery merely to widen that set. Keep
plugin-specific option schemas out of the core.

Verdict: **no blocker**. The changed enumerable character categories and option
forms have firing tests; no flag, exit-code set, or JSON error-code set changed.
The black-box probes cover valid, empty/wrong-type, hostile control, boundary
length, duplicate TOML, JSON/no-traceback, and no-side-effect rejection cases.

Runbook feedback: no new reusable interface pattern surfaced.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| F6 / Task 5 | Make PostgreSQL empty-schema initialization internally consistent; retain its existing concurrency proof. | The real two-initializer proof exposed a catalog race after both callers independently admitted the same empty schema. Project-scoped PostgreSQL and Redis initialization now shares the existing `PhaseLockService` using `.broker.toml` as the target. Explicit targets remain backend-idempotent and do not acquire a filesystem lock. | Backend-local transactions cannot safely span preflight inspection and bootstrap DDL. The project config is the existing shared local identity. Reusing PhaseLock preserves forward progress and avoids a new distributed-lock concept. | None. This is implementation coordination under [SB-API-11], not a new public option or backend protocol member. |

## Execution Log

- **2026-08-25, implementation start:** promoted the reviewed [SB-CLI-2],
  [SB-API-2], and [SB-API-11] text as the uncommitted spec delta recorded in
  `## Spec Baseline`. Runtime implementation proceeds in vertical red-to-green
  slices. No runtime file had changed at this checkpoint.
- **SQLite admission, proof, migration, and FIFO:** RED probes reproduced
  newer-version mutation, false marker trust, partial legacy version
  publication, physical index-name coupling, and unordered `RETURNING` rows.
  GREEN implementation uses the runner's one normal connection and one scalar
  admission statement for magic, stored version, proof rows, and SQLite schema
  cookie. Missing/malformed metadata alone takes a fallback query. A stale
  marker reruns the existing idempotent slow path under its existing lock.
  Fresh SQLite bootstraps atomically; unversioned legacy databases publish only
  committed migration steps. Built-in SQLite alone returns a private storage
  id, and one shared core helper sorts and strips it for list and generator
  paths. Backend API v7 and extension row shapes did not change.
- **Backend admission:** PostgreSQL and Redis now keep ownership, version
  relation, and current readiness separate. Empty PostgreSQL schemas initialize;
  any schema-owned non-relation object prevents takeover. Redis older-owned
  targets receive the explicit no-migration error, and every physical key under
  the exact namespace prefix participates in foreign/partial admission.
- **Project service coordination:** the retained PostgreSQL empty-schema race
  reproduced as two concurrent catalog creators. Project-scoped PostgreSQL and
  Redis setup now uses one PhaseLock keyed by `.broker.toml`; explicit targets
  remain direct. Completion markers are accepted only after current-schema
  validation. POSIX retains marker-first validation while Windows retains the
  lock-before-marker happens-after rule. Forcing strict marker locking on POSIX
  was rejected after it serialized every CLI start and caused PostgreSQL CLI
  timeouts.
- **Representation and lifecycle boundaries:** POSIX path admission now follows
  actual consumers; nested TOML values reach the plugin and returned options
  must survive target serialization losslessly. Session identity is type-tagged,
  opaque values retain process-local identity, and one recursive snapshot feeds
  both the registry key and lazy factory. Ordinary cleanup attempts all safe
  remaining resources and attaches later failures as notes. Duplicate backend
  entry points fail before load. Coverage repair checks required names rather
  than physical order or exact table set; CLI JSON tests check object content,
  not member iteration order.
- **Aggregate local verification:** `uv run pytest -q` passed with 3,210 tests
  and 17 documented platform/opt-in/service skips (final rerun also exited 0).
  The non-service first-party extension matrix passed with 259 tests and 288
  service/opt-in skips. `uv run ruff check .`, `uv run ruff format --check .`,
  source mypy (65 files), core-test mypy (207 files), Ruff suppression-index
  check, DOM-15 fixtures, plan context, doc paths, and `git diff --check` passed.
  The interface-focused path/config/CLI selection passed with only its five
  expected Windows/filesystem skips.
- **Artifact verification:**
  `uv run --frozen --no-sync ./bin/packaging-smoke` rebuilt the root and both
  first-party extension wheel/sdist pairs, installed them outside the checkout,
  discovered both extensions, and passed the isolated root wheel and root sdist
  SQLite runtime probes on Python 3.11.
- **Service and performance verification:** the final integrated PostgreSQL
  rerun passed 1,504 shared tests plus 291 extension tests; Redis/Valkey passed
  1,497 shared tests plus 283 extension tests. The full core plus Weft gate
  passed with 17 workers. Controlled 100-operation CLI A/B runs comparing the
  project PhaseLock with its setup bypass showed PostgreSQL deltas of +0.9%,
  -1.0%, +0.7%, and -1.5%, and Redis deltas of +0.5%, +0.2%, -0.4%, and +0.2%
  for write, read, peek, and mixed workloads respectively. Those differences
  are within trial noise; the benchmark-only bypass was removed afterward.
- **Final review:** an independent pre-release review found that a PostgreSQL
  supported-old schema could borrow a completed config marker and that a wrong
  reserved SQLite index name was accepted beside a valid equivalent index.
  Both were reproduced with failing tests. `verify_initialized=True` now
  requires current PostgreSQL state while the migration-admission path remains
  permissive, and the reserved index-name conflict now wins over semantic
  equivalence. The real PostgreSQL marker-restoration test and full SQLite
  schema file pass. Re-review found no remaining code P0-P2; this traceability
  update resolves its final documentation finding.
- **Final core rerun:** `PYTEST_XDIST_AUTO_NUM_WORKERS=17 uv run pytest`
  passed 3,227 tests with 17 documented platform, service, and opt-in skips.
- **Unavailable release-only evidence:** `SIMPLEBROKER_PG_TEST_DSN` and
  `SIMPLEBROKER_VALKEY_TEST_URL` were unset, so live-service cases remain
  explicitly skipped. Exact-SHA hosted Windows/POSIX jobs and publication are
  release gates, not local implementation claims. Per the user's stated scope,
  this task did not inspect the Weft repository; its owner-supplied compatibility
  command remains required before release.

Record per-slice red/green evidence, stop-gate decisions, review receipts,
release receipts, and final commit identifiers here. Do not record transient
staging or worktree state.

## Out of Scope

- Full schema validation, table/index enumeration, checksums, or data scans on
  every ordinary open.
- Detecting deliberate corruption that also forges SQLite's schema cookie and
  SimpleBroker proof metadata.
- Supporting concurrent raw external DDL; a later open detects completed DDL,
  but no new linearizability promise is added around an external race.
- A new SQLite stored schema version solely for proof metadata.
- Redesigning phase-lock status syntax, adding sidecars, or changing explicit
  global cleanup's undefined overlap with live use.
- Adding a Redis schema migration without a real Redis schema change.
- Relaxing path characters still interpreted by actual glob/path consumers
  unless those consumers are made literal-safe in the same reviewed slice.
- Turning `BackendPlugin` into a complete third-party SDK or weakening exact
  backend API equality.
- Changing public message shapes, FIFO definition, dump canonical order,
  queue/key grammar, CLI option grammar, or positional Redis Lua protocols.
- Config diagnostic first-error ordering and Redis multi-queue partial-delete
  order, which were low-impact or already explicitly partial and were not in
  the accepted material finding set.
- Publishing packages, tags, or releases as part of implementation without a
  separate owner-authorized release action.

## Fresh-Eyes Review Checklist

- [x] Every named file, function, test seam, spec code, and command exists at
      the implementation baseline.
- [x] Normal SQLite open has a scalar-only fast path and no hidden schema scan.
- [x] Newer-version rejection is before every mutating setup action.
- [x] Proof failure and marker publication order cannot publish a false fast
      path.
- [x] FIFO proof uses storage order and covers both list and generator paths.
- [x] Ownership, version relation, readiness, and coordination are not folded
      back into one classifier.
- [x] Path widening follows actual consumers and does not weaken cleanup
      ownership or containment.
- [x] Nested option validation remains plugin-owned and target transport stays
      lossless.
- [x] Cleanup and opaque identity favor over-separation/continued cleanup over
      unsafe sharing/order-dependent leaks.
- [x] Coverage repair remains narrow and fail-closed after ordinal checks are
      removed.
- [x] Rollback, preserved backend API v7, one-way acceptance widening, downstream
      verification, and post-release signals are explicit.
- [x] No plan section, abstraction, or gate survives merely as ceremony; each
      addresses a named risk or repository requirement.
