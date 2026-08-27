# Message-ID Order and Newest Selection Plan

Status: active
Class: 5+P. This changes the published default selection order, adds a public
Python and CLI selection mode, changes both SQL storage schemas and backend
API compatibility, and creates a one-way schema-version boundary for older
clients. `[DOM-5]`, `[DOM-6]`, `[DOM-11]`, and `[DOM-15]` require the spec
revision, risky-plan hardening, exact cross-backend proof, downstream proof,
and independent review described below.
Plan type: implementation with spec revision

## Goal and Boundary

Make the public message ID (`ts`) the only semantic identity and the uniform
cross-backend ordering key. Default bounded retrieval remains oldest-first,
now defined as ascending integer public message ID. Callers may request
newest-first bounded retrieval with `order: str = "oldest"` and
`order="newest"`; the CLI exposes the same choice as `--newest` sugar.

The ordered selection applies to `read`, `peek`, and `move`, including their
one-message and bounded-many forms. It does not turn a queue into a persistent
stack, change stored queue state, or make message ID equal to insertion time.
It changes which eligible message is selected first for one operation.

Generator, `--all`, stream, and watch surfaces remain oldest-first only and do
not gain an order control. Their default traversal nevertheless becomes
ascending public message ID because that is the new default order. Supporting
reverse live traversal would require a separate cursor and concurrency
contract and is outside this feature.

Fresh SQLite and PostgreSQL databases omit the private `id` / `order_id`
surrogate, its indexes, and its sequence machinery. The v5-to-v6 migration
rebuilds the SimpleBroker-owned `messages` table into that same canonical
layout. The sole supported current layout produced by successful setup or
migration has no surrogate in a broker-owned object. Caller-owned sidecar
tables remain untouched; additions or changes to reserved broker tables and
owned indexes are unsupported and carry no preservation promise. Redis retains
its current physical representation and changes only bounded reverse selection.

## Product Fit

`docs/program-theory.md` [THEORY-1] and [THEORY-4] favor a small, predictable
queue primitive whose CLI and Python meanings match and whose backend choice
does not change behavior. One public identity plus one cross-backend order is
smaller than the present split between public timestamp identity and private
SQL insertion order. The `order` argument adds one bounded selection choice,
not a new durable queue type or task-execution concept.

Weft is the primary downstream and has concrete prospective use for
newest-first bounded selection. That is demand evidence, not authority to put
Weft-specific state or policy into SimpleBroker. Weft must be checked for
existing order assumptions before the contract changes and again against the
release artifacts.

## Decisions Locked by This Plan

1. The public API is `order: str = "oldest"`; the only alternative is the
   exact string `"newest"`. Any other runtime value raises `ValueError` before
   broker-target acquisition or mutation. The public annotation remains
   `str`, matching the settled API, while overloads may use literals to retain
   precise result typing.
2. `--newest` is CLI sugar for `order="newest"`. There are no
   `read_last`, `peek_last`, or `move_last` methods and no second semantic path.
3. Oldest and newest mean the lowest and highest eligible integer public
   message ID, not earliest or latest wall-clock insertion. Bounds are applied
   first; order then controls selection and returned sequence.
4. High-level `Queue.read`, `Queue.peek`, and `Queue.move` accept `order` when
   `all_messages=False`. The corresponding `*_one` and `*_many` methods accept
   it. Generator methods do not. Passing a non-default order with
   `all_messages=True` raises `ValueError` before target acquisition.
5. CLI `--newest` is valid for `read`, `peek`, and `move`; it is invalid with
   `--all`. Exact `-m` selection accepts it but the one possible result is
   unchanged. `--after` and `--before` compose normally with it.
6. Default retrieval order is ascending public message ID on SQLite,
   PostgreSQL, and Redis. Ordinary broker-generated writes still appear FIFO
   because generated IDs are monotone. Exact insertion, load/import, and move
   can put a smaller ID into a queue later; default retrieval then exposes that
   smaller ID first.
7. SQLite and PostgreSQL schema v6 has exactly one supported canonical
   SimpleBroker-owned layout: `ts` is the primary key and no private row-order
   surrogate, surrogate index, or surrogate sequence entry exists for
   `messages`. Fresh databases are created in that layout and the v5-to-v6
   migration rebuilds legacy broker-owned objects into it. Runtime behavior
   never creates, requires, addresses, or orders by a surrogate column.
8. SQLite and PostgreSQL schema versions advance from 5 to 6. Existing v5
   owned databases undergo a rebuilding v6 migration: SQLite rebuilds
   `messages` transactionally into the canonical table; PostgreSQL drops
   `order_id` with `RESTRICT` (its owned sequence goes with it) and promotes
   the existing `ts` unique index to the primary key. PostgreSQL migration
   correctness is owned by a transaction-scoped advisory lock plus a
   cache-bypassing live schema-version recheck on the same connection before
   version-dependent DDL. Fresh databases are created directly in the same
   canonical v6 layout. This single-layout choice is premised on the near-zero
   external installed base recorded by the owner on 2026-08-27.
9. Schema v6 has one supported canonical owned layout. Validation keeps its
   existing magic/version, complete owned-table, and typed-metadata anchors
   unchanged and does not grow a message-column checker. Setup and migration
   prove semantic `ts` uniqueness and required access paths. Fresh and upgraded
   databases have equivalent SimpleBroker-owned object introspection, modulo
   engine-derived internal names and SQLite's persistent engine-owned
   `sqlite_sequence` table. Unsupported extra columns do not become a second
   supported layout merely because projection-based admission tolerates them.
10. Successful and failed SQL migrations leave caller-owned sidecar table
    definitions, rows, indexes, constraints, and sequence state unchanged.
    Migration may rewrite only SimpleBroker-owned objects. Modifications to
    reserved tables (`messages`, `meta`, `queue_aliases` / `aliases`) or
    broker-owned indexes are unsupported and have no preservation contract.
11. Backend API version advances from 7 to 8. All first-party backends must
    implement the order parameter together; mixed v7/v8 packages fail at the
    existing plugin handshake rather than degrading silently.
12. The PostgreSQL and Redis extension releases precede the core release by
    three waited single-target `bin/release.py` invocations; the batch `all`
    target is forbidden because it does not serialize publication. Core is a major release because the
    default public order changes for out-of-order exact IDs: the target core
    version is 8.0.0 and `BACKEND_API_MIN_CORE_VERSION[8]` is `"8.0.0"`.
    Exact extension version numbers are chosen in the release slice, but their
    dependency and backend-handshake floors must exclude the old contract.

## Alternatives Considered

- **Keep SQL schema version 5.** Rejected. A previous client would accept a
  fresh no-surrogate database and reach an `id` / `order_id` query it cannot
  execute. A clean version-gate refusal is part of compatibility, even though
  it makes first v8 open a one-way admission boundary for old clients.
- **Keep creating the private surrogate.** Rejected. It preserves two row
  identities after public behavior has stopped using one and leaves future
  code tempted to recover backend-specific insertion order.
- **Retain the surrogate in upgraded databases as an ignored legacy column.**
  Rejected 2026-08-27 after owner review; it was this plan's original
  conservative choice. It forces a permanent second owned layout: a doubled
  SQL test matrix, layout-keyed fixtures, tolerance rules in validation and
  maintenance paths, and PostgreSQL allocating `order_id` values forever to
  satisfy a retained constraint. With a near-zero external installed base, a
  one-time transactional rebuild is cheaper than carrying two layouts
  indefinitely; queue databases are small in practice, and the release notes
  already require a backup before first open.
- **Stamp v6 on fresh databases only and leave v5 databases unmigrated.**
  Rejected. It preserves old-client coexistence that matters only for an
  external installed base this project does not currently have, at the price
  of the same permanent dual-layout support burden.
- **Add `read_last` / `peek_last` / `move_last` methods or a persistent queue
  LIFO flag.** Rejected. They multiply parallel surfaces or add durable queue
  state for a per-operation choice already expressed by one shared argument.
- **Support reverse generators/watch.** Rejected until a separate live cursor
  and concurrency contract defines how later higher and lower IDs interact
  with descending progress.

## Prerequisite and Historical Supersession

The completed
`docs/plans/2026-08-25-schema-and-representation-assumption-remediation-plan.md`
is a hard prerequisite. Its ownership/readiness separation and semantic
schema validation must be present before this plan changes the canonical SQL
layouts and their migration. Current HEAD
`4ef13bbbfb3888cf88fa31eb1a0d6646da61f373` contains that completed work.

During this implementation, append a dated note to that completed plan. The
note must say, in substance:

> Superseded in part on 2026-08-27 by the message-ID order plan: storage-order
> FIFO is replaced by ascending public-message-ID order, and fresh SQL schemas
> omit the private row-order surrogate. The original invariant that retrieve
> results never depend on engine `RETURNING` order remains fully in force.
> Explicit normalization by `ts` is the stronger logical-order rule.

Do not rewrite the completed plan's historical tasks, evidence, or status.
Only its storage-order invariant and the rationale for sorting by a private ID
are superseded. Its actual deliverable, deterministic logical ordering after
unordered engine output, survives unchanged.

## Source Documents

Theory records:

- `docs/program-theory.md` [THEORY-1]: keep the broker a small, predictable
  queue tool; one public identity and one backend-independent order remove a
  storage-specific concept.
- `docs/program-theory.md` [THEORY-3]: storage-specific atomic realization
  belongs to each backend, while the ordered-selection meaning belongs to the
  shared broker contract.
- `docs/program-theory.md` [THEORY-4]: CLI and Python behavior must match,
  failures must be explicit, and a new concept requires concrete pressure.

Winning product contracts:

- `docs/specs/13-message-identity.md` [SB-ID-1], [SB-ID-4], [SB-ID-5]
- `docs/specs/14-timestamp-selection.md` [SB-SELECT-1] through [SB-SELECT-4]
- `docs/specs/11-delivery.md` [SB-DELIVERY-3] verification and related-plan
  traceability only; delivery semantics do not change
- `docs/specs/10-cli.md` [SB-CLI-1] through [SB-CLI-5]
- `docs/specs/16-python-library-api.md` [SB-API-4], [SB-API-7], [SB-API-10],
  [SB-API-11], [SB-API-12], [SB-API-13]
- `docs/specs/product-section-registry.md`

Human and agent entry points:

- `README.md`
- `docs/agent-kernel.md`
- `docs/guides/python.md`
- `CHANGELOG.md`

Implementation and process owners:

- `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
- `docs/implementation/02-repository-map.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`
- `skills/interface-review/SKILL.md`

## Context and Key Files

| Owner | Current load-bearing behavior | Planned change |
|-------|-------------------------------|----------------|
| `simplebroker/sbqueue.py` | Public overloads split high-level, one, many, and generator results; bounded high-level calls may currently delegate through a generator. | Add keyword-only order only to high-level bounded, one, and many forms; route bounded single selection through a non-generator primitive. |
| `simplebroker/commands.py`, `simplebroker/cli.py` | Direct commands normalize selectors; parser-construction metadata owns registered tokens and rearrangement. | Add one normalized direct-command argument and `--newest`; keep newest+all validation pre-target and update the single grammar inventory. |
| `simplebroker/db.py`, `simplebroker/_sql/_query_spec.py`, `simplebroker/_sql/sqlite.py` | Core retrieval selects by private SQLite order and normalizes unordered DML results by private ID. | Carry the validated direction, select/address by `ts`, and normalize unordered results by `ts`. |
| `simplebroker/_backend_plugins.py` | Exact backend API v7 protocol and plugin handshake. | Advance to v8 for one/many order arguments; do not change generator batch methods. |
| `simplebroker/_selection.py` (new) | No shared order vocabulary exists. | Own the exact `oldest`/`newest` validation used above every backend. |
| `simplebroker/_backends/sqlite/schema.py`, `simplebroker/_backends/sqlite/validation.py` | Schema v5 and semantic admission/migration checks. | Create the canonical schema-v6 layout and the transactional rebuilding v5 migration while preserving caller-owned sidecars. |
| `extensions/simplebroker_pg/simplebroker_pg/schema.py`, `validation.py`, `_sql.py`, `runner.py` | Schema v5, ownership by magic/version, readiness by all current tables and typed metadata, order-ID SQL, and query classification. | Create the canonical v6 layout, migrate v5 under a transaction-scoped advisory lock and live version recheck, keep validator anchors unchanged, preserve sidecars, and make every runtime path use public ID. |
| `extensions/simplebroker_redis/simplebroker_redis/core.py`, `scripts.py`, `runner.py` | Fixed-width ID members and ascending lexicographic bounded/Lua selection. | Add direction only to bounded one/many paths and preserve ascending live batches. |
| `simplebroker/_sidecar.py`, sidecar contract suites | Caller-owned SQL tables share the broker target but reserved broker tables and indexes are excluded. | Keep the public sidecar boundary unchanged and prove successful/failed migrations do not mutate caller-owned schema or state. |
| `tests/conftest.py` and extension `conftest.py` files | Backend fixtures construct one current SQL layout. | Unchanged for behavior suites; migration transition tests use the dedicated v5 helper scripts and seed caller-owned sidecars. |
| `bin/release.py`, package metadata, artifact smoke tests | Exact backend API floors and extension-first coordinated release. | Register API v8 and prove compatible artifacts before core publication. |

Primary contract and regression files to extend rather than replace:

- `tests/test_timestamp_selection_contract_sb_select.py`
- `tests/test_message_identity_contract_sb_id.py`
- `tests/test_python_library_api_contract_sb_api.py`
- `tests/test_queue_typing_contract.py`
- `tests/test_cli_contract_sb_cli.py`, `tests/test_cli_rearrange_args.py`, and
  `fuzz/fuzz_cli_args.py`
- `tests/test_sqlite_fifo_returning.py` (rename to
  `tests/test_sqlite_message_id_returning_order.py`),
  `tests/test_sqlite_schema.py`, and `tests/test_sqlite_admission.py`
- `tests/test_property_queue_model.py`
- `extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py` and
  `extensions/simplebroker_pg/tests/test_pg_ownership.py`
- `extensions/simplebroker_redis/tests/test_redis_integration.py` and
  `extensions/simplebroker_redis/tests/test_redis_batches.py`
- `tests/compatibility/test_weft_artifact_versions.py` (new, executed inside
  Weft's child pytest process to prove exact versions and non-source imports)

Before coding, the implementer and first reviewer must answer these possession
questions in the Execution Evidence. The expected answers below are gates. A
materially different answer blocks implementation until the cited owner text
and code have been reread and the plan is corrected if necessary.

1. **Why does ordered SQLite selection not prove ordered `RETURNING` output?**
   Expected: an inner `ORDER BY` determines the eligible limited ID set, but
   SQLite does not promise that DML `RETURNING` emits that set in the same
   order. `BrokerCore` must therefore sort the returned public IDs by `ts` in
   the requested direction before exposing rows.
2. **What proves PostgreSQL ownership/current shape without `order_id`?**
   Expected: the existing validator proves ownership from broker magic/version
   and current readiness from the complete owned table set plus current typed
   metadata columns. `order_id` is neither required nor forbidden and the
   validator does not grow a second full message-table/index checker. Foreign
   identity, partial owned state, conflicting or wrongly typed metadata, and a
   newer version still fail closed. Setup/migration code and real schema tests,
   not ownership classification, prove `ts` uniqueness and access indexes.
3. **Why is newest bounded but not live?** Expected: a finite one/many
   operation can choose and mutate one eligible set atomically in descending
   ID order. A descending live cursor that moves its upper bound downward can
   miss later higher IDs and has undefined interaction with later lower-ID
   inserts and moves. That requires a separate progress/concurrency contract.
4. **What serializes PostgreSQL migration?** Expected: `.broker.toml.lock`
   remains the outer startup serializer for project-scoped targets, but it is
   not the correctness owner because env/direct targets have no config path and
   two project files may address one schema. Every v5-to-v6 migration therefore
   takes one stable transaction-scoped PostgreSQL advisory lock and performs a
   cache-bypassing live `meta.schema_version` read on that same connection
   before any version-dependent DDL. A waiter that sees v6 refreshes its cache
   and exits without replaying v5 DDL.
5. **What survives a table rebuild?** Expected: every SimpleBroker-owned row
   and public state named by the migration contract, plus every caller-owned
   sidecar object's definition and state. Unsupported modifications inside
   reserved broker tables or indexes are not sidecars and have no preservation
   promise. SQLite may retain the engine-owned `sqlite_sequence` table, but no
   `messages` entry may remain and caller-owned sequence rows must be unchanged.

## Spec Baseline

- `4ef13bbbfb3888cf88fa31eb1a0d6646da61f373`:
  `docs/specs/11-delivery.md`,
  `docs/specs/13-message-identity.md`,
  `docs/specs/14-timestamp-selection.md`, `docs/specs/10-cli.md`,
  `docs/specs/16-python-library-api.md`, and
  `docs/specs/product-section-registry.md` at plan authoring time.
- Plan type: implementation with spec revision. After Task 1 applies the
  proposed text, record the spec-promotion commit SHA, or the diff base plus
  exact spec-tree diff when the owner requests an uncommitted review. Later
  implementation compliance is judged against that promotion baseline.

If another change alters a cited clause, schema version, backend API version,
or public signature before promotion, pause and update the Deviation Log and
baseline before proceeding.

## Proposed Spec Delta

Promotion strategy: **A, in-file edits with requirement text before
implementation-link claims.** The canonical files are already active; no
planned-spec reclassification is needed.

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/11-delivery.md` | A | [SB-DELIVERY-3] verification and related plans only; no normative delivery change |
| `docs/specs/13-message-identity.md` | A | [SB-ID-1] final identity/order paragraph |
| `docs/specs/14-timestamp-selection.md` | A | owner, boundary, new [SB-SELECT-5], mapping and verification |
| `docs/specs/10-cli.md` | A | new [SB-CLI-6], grammar mapping and verification |
| `docs/specs/16-python-library-api.md` | A | [SB-API-4], [SB-API-7], [SB-API-10], [SB-API-11], [SB-API-12], conditionally [SB-API-13] |
| `docs/specs/product-section-registry.md`, `docs/specs/00-specs-index.md` | A | clause ranges, owners, gates, discoverability |

Task 1 is the spec-promotion slice. It applies the exact text and red contract
tests before product implementation. Do not cite plan-only text from shipped
code or postpone promotion until the documentation cleanup slice.

The text below is the intended normative contract. Implementation may adjust
grammar for the surrounding document, but not meaning, without recording a
deviation.

### `docs/specs/13-message-identity.md` [SB-ID-1]

Replace the final sentence of [SB-ID-1] and join it to the preceding paragraph
as follows:

> A generated id's physical component is generation-time (`now()`) within the
> encoding grain. Callers may insert exact ids; those need not equal wall-clock
> now. Move preserves id (see [SB-ID-5]). The public message ID is the sole
> semantic row identity and the cross-backend retrieval order is owned by
> [SB-SELECT-5]. The sole supported canonical current SQL layout contains no
> private storage surrogate in a SimpleBroker-owned object: fresh databases
> are created without one, and the schema-v6 migration rebuilds databases
> created by older releases into that layout. Projection-based admission may
> tolerate unsupported extra columns without making them part of the supported
> layout or giving them a preservation promise.

The earlier [SB-ID-1] statement that a later exact write may carry a smaller ID
remains unchanged. It is the reason the new order is not described as general
write-time FIFO.

In [SB-ID-1]'s verification row, replace the stale SQLite evidence with:

> `tests/test_sqlite_message_id_returning_order.py` (forced reverse raw
> `RETURNING` rows cannot alter ascending/default or descending/newest public
> message-ID order)

Rename `tests/test_sqlite_fifo_returning.py` to that path when its assertions
change. In [SB-ID-1] Related Plans, mark the 2026-08-25 schema plan completed
and replace its stale storage-FIFO gloss with the dated supersession note. Add
this active plan as the owner of public-ID order.

### `docs/specs/11-delivery.md` traceability-only delta

[SB-DELIVERY-3]'s claim/move meaning does not change. Replace only its stale
verification/related-plan text:

> [SB-DELIVERY-3] firing evidence includes
> `tests/test_sqlite_message_id_returning_order.py` (forced reverse raw
> `RETURNING` order across claim and move cannot change the public-ID sequence).
>
> Related plan: completed
> `2026-08-25-schema-and-representation-assumption-remediation-plan.md`
> established engine-order independence; its private storage-FIFO rule is
> superseded by the active
> `2026-08-27-message-id-order-and-newest-selection-plan.md` and
> [SB-SELECT-5].

This is traceability repair, not a transfer of result-order ownership back to
the delivery vertical.

### `docs/specs/14-timestamp-selection.md` owner and boundary

Revise the owner and boundary text to say:

> Owner: SimpleBroker ordered-selection surface: read, peek, and move bounds,
> cross-backend result order, and watcher lower bounds. Exact message-ID
> identity remains with [SB-ID-*]. Delivery claim/peek/move outcomes remain
> with [SB-DELIVERY-*]. CLI string-to-integer bound parsing for non-exact forms
> is [SB-CLI-5].
>
> Boundary: integer lower/upper bounds on message ID, the order of eligible
> bounded results, the filter nature of bounds, and consequences when IDs
> arrive later with values below a bound already used as a filter. Exact
> single-ID targeting (`-m` / `message_id`) is [SB-ID-4]. Generator and live
> traversal lifecycle remain with [SB-API-*] and [SB-DELIVERY-*]; this section
> fixes their default order but does not add reverse live traversal.

Add this clause after [SB-SELECT-4]:

> ## Cross-backend retrieval order [SB-SELECT-5]
>
> After queue, claim-state, exact-ID, and open-bound predicates determine the
> eligible set, `read`, `peek`, and `move` order eligible messages by the
> integer public message ID. `oldest` means ascending ID and is the default;
> `newest` means descending ID. Selection and the sequence returned by a
> bounded-many operation use the same order on SQLite, PostgreSQL, and Redis.
> No operation may depend on database engine row order or SQL `RETURNING`
> order.
>
> Ordinary generated writes retain insertion-order behavior because generated
> IDs are monotone. Exact insertion, load/import, or ID-preserving move may add
> a lower ID later; the lower ID is then selected earlier under `oldest`.
> `oldest` and `newest` therefore describe public-ID order, not general
> insertion time or wall-clock chronology.
>
> Python one-message and bounded-many read/peek/move forms accept the closed
> string choice `order="oldest" | "newest"`; any other value raises
> `ValueError` before target acquisition or mutation. High-level forms accept
> the choice only when `all_messages=False`; a non-default order with
> `all_messages=True` raises the same pre-target error. Exact single-ID
> selection accepts either value but has at most one observable result. Open
> `after_timestamp` and `before_timestamp` bounds filter first and order the
> remaining set.
>
> Generator, all-messages, stream, and watch forms expose only `oldest` and do
> not accept a reverse-order control. Their traversal is ascending public
> message ID. Reverse live traversal requires a separate cursor and
> concurrency contract and is not implied by bounded `newest` selection.

Update the clause verification table with direct shared-backend evidence for
default, newest, bounds, exact selection, unsupported live/all forms, and
engine-return normalization.

### `docs/specs/10-cli.md` [SB-CLI-6]

Add:

> ## Newest-first bounded selection [SB-CLI-6]
>
> `simplebroker read`, `peek`, and `move` accept `--newest` as CLI sugar for the
> Python selection value `order="newest"`. With no flag, they use `oldest`.
> `--after` and `--before` filter the eligible set before descending selection.
> `-m` may be combined with `--newest`, although exact selection has at most
> one result. `--newest` and `--all` are mutually exclusive and fail before
> target acquisition or mutation with the ordinary invalid-argument
> diagnostic: text mode exits 1; JSON mode emits the [SB-CLI-4] error object
> with `error="INVALID_ARGUMENT"` and exits 1.
>
> `--newest` is a registered option token under [SB-CLI-3]. A write or
> broadcast body that begins with that literal must use the existing `--`
> escape boundary. Watch, stream, and generator surfaces do not acquire a
> corresponding flag.

Update the complete grammar/option inventory and firing table. Include both
normal placement and rearranged registered-token cases so parser convenience
does not create a second interpretation. A firing JSON test asserts the exact
`error`, `message`, and `retryable` fields, stderr-only placement, and exit 1.

### `docs/specs/16-python-library-api.md`

Amend [SB-API-4] with:

> `Queue.read`, `Queue.peek`, and `Queue.move` accept keyword-only
> `order: str = "oldest"` when they return one message or a bounded materialized
> result. Their `*_one` and `*_many` forms accept the same keyword. The only
> non-default value is `"newest"`; invalid values raise `ValueError` before a
> broker target is acquired or mutated. A high-level call with
> `all_messages=True` accepts only the default and rejects `"newest"` before
> target acquisition. `*_generator`, `stream_messages`, and watcher forms do
> not accept `order`. `find_message_ids()` remains an ascending-only
> administrative search: it returns matching IDs in ascending integer public
> message-ID order and does not accept `order`.

Amend [SB-API-7] with:

> SQL schema migration may rewrite only SimpleBroker-owned tables, indexes,
> constraints, and sequence state. Successful and failed migration leaves each
> caller-owned sidecar table definition, row, index, constraint, and sequence
> state unchanged. `RESERVED_TABLE_NAMES` and broker-owned `idx_*` indexes are
> not sidecars: caller changes to those owned objects are unsupported and have
> no migration-preservation promise. A sidecar dependency on a removed private
> broker column must make migration fail without mutation; migration never uses
> `CASCADE` to erase caller-owned state.

Amend [SB-API-10] with:

> Direct command functions for bounded read, peek, and move accept the same
> normalized `order` string. The CLI adapter maps `--newest` to `"newest"` and
> otherwise passes `"oldest"`; it does not implement ordering independently.

Amend [SB-API-11] with:

> Backend API v8 adds the validated selection order to claim, peek, and move
> one/many operations. First-party plugins declare v8 exactly. SQL backend
> storage schema v6 has one supported canonical SimpleBroker-owned layout,
> where `ts` is the primary key and no private surrogate exists; the v6
> migration rebuilds older layouts into it without mutating caller-owned
> sidecars. PostgreSQL migration is serialized by a database advisory lock and
> makes its version decision from a live under-lock metadata read. A database
> with a schema version newer than a client supports is rejected during cold
> admission, before any message-table operation. Already-admitted old clients
> are outside that guarantee; operators must quiesce all clients for the
> one-way migration.

Amend [SB-API-12] and the firing table so the CLI, direct-command, Queue,
backend-protocol, typing, and runtime surfaces enumerate the same support and
rejection matrix. Amend [SB-API-13] only if its PostgreSQL inspection output
enumerates storage columns; inspection must reflect the canonical
surrogate-free layout.

### Registry and index changes

Update `docs/specs/product-section-registry.md` so timestamp selection owns
[SB-SELECT-1] through [SB-SELECT-5], and the CLI family owns [SB-CLI-1] through
[SB-CLI-6]. Add the new gates to the registry's verification column. Update
`docs/specs/00-specs-index.md` descriptions so cross-backend ordering and
newest-first bounded selection are discoverable.

All verification-table, firing-table, mapping, and registry evidence links
described above land only with the implementation slice that makes the named
test green. The Strategy A promotion carries requirement text and backlinks,
not premature implementation claims.

## Proposed Root README Delta

The root README remains a human entry and catalog, not a second contract. Add
or replace the following concise restatements, each linked to the canonical
clause.

### Quick Start

Extend the existing shell-only block after its read/peek/move examples:

```bash
broker peek jobs --newest
broker read jobs --newest
broker move jobs archive --newest
```

The CLI's non-`--all` form selects one message, so do not imply that the CLI
has a bounded-many limit flag. Bounded-many newest selection is a Python API
surface. Every final example must execute in the README example gate.

### Read, peek, and move options

Add this prose near the shared option list:

> Read, peek, and move select the lowest eligible public message ID first by
> default. Use `--newest` for highest-ID-first bounded selection. Ordinary
> generated writes still behave like FIFO because their IDs increase, but an
> exact insert, load, or move can add a lower ID later. `--newest` cannot be
> combined with `--all`; generators and watch remain ascending-only. See
> [SB-SELECT-5](https://github.com/VanL/simplebroker/blob/main/docs/specs/14-timestamp-selection.md#cross-backend-retrieval-order-sb-select-5)
> and
> [SB-CLI-6](https://github.com/VanL/simplebroker/blob/main/docs/specs/10-cli.md#newest-first-bounded-selection-sb-cli-6).

Add `--newest` to the read/peek/move option lists and explain its interaction
with `--after`, `--before`, `-m`, and `--all`. Because it is a registered CLI
token, update the payload-escaping example to include a body beginning with
`--newest` if that section lists concrete collisions.

### The SimpleBroker API and Python guide

At the existing `## The SimpleBroker API` locus, add this self-contained,
executable example rather than putting Python into the shell Quick Start:

```python
from simplebroker import Queue

with Queue("jobs") as queue:
    oldest = queue.peek_one()
    newest = queue.peek_one(order="newest")
    recent = queue.peek_many(limit=10, order="newest")
```

In `docs/guides/python.md`, add self-contained `order="newest"` examples for
`read_one`, `peek_many`, and `move_one`, then state explicitly:

> `order` is available on one-message and bounded-many read, peek, and move
> operations. It is not accepted by generator, all-messages, stream, or watch
> forms. The default and the only alternative are `"oldest"` and `"newest"`.

Keep `docs/guides/python.md` and `docs/agent-kernel.md` aligned with those
support boundaries. Do not describe a queue as having a durable FIFO/LIFO
mode; the choice belongs to each bounded operation. The Python guide's
`find_message_ids()` section must state that its limited result is ascending
by public message ID and has no newest control.

At the root README's existing sidecar paragraph and the Python guide's
[SB-API-7] teaching locus, add the migration boundary without turning it into a
second storage contract: SimpleBroker may rebuild only its reserved broker
objects; successful or failed migration leaves caller-owned sidecar schema and
state unchanged. State that additions to `messages`, `meta`, aliases tables, or
broker-owned indexes are modifications to reserved objects, not sidecars, and
are unsupported. Link to [SB-API-7].

## Proposed CHANGELOG Delta

Under the release heading, add a user-visible breaking-change entry with this
meaning:

> Changed: read, peek, and move now use ascending public message ID as their
> uniform default order. Ordinary generated writes remain FIFO-like; exact
> inserts, loads, and moves of lower IDs may now be returned earlier than rows
> inserted first. Added Python `order="newest"` and CLI `--newest` for bounded
> newest-first selection. Fresh SQL databases no longer contain private
> `id`/`order_id` columns; opening an owned v5 target with v8 rebuilds the
> SimpleBroker-owned schema and removes the legacy surrogate. SQL schema
> version 6 makes an older client that opens afterward reject the database
> cleanly before operation. This is a downtime cutover, not a rolling upgrade:
> stop all v7 clients and sidecar transactions, take a whole-target backup,
> install the coherent v8 package set, migrate and verify once, then restart
> only v8 clients. PostgreSQL takes an `ACCESS EXCLUSIVE` migration lock.
> Caller-owned sidecar tables and state are preserved; changes inside reserved
> broker objects are unsupported. Backend API v8 requires matching first-party
> extension releases.

Also call out the invalid `--newest --all` combination and the registered
option-token effect on unescaped write/broadcast bodies.

## Storage Design

### Canonical supported SQLite v6

Fresh schema:

```sql
CREATE TABLE messages (
    queue TEXT NOT NULL,
    body TEXT NOT NULL,
    ts INTEGER PRIMARY KEY,
    claimed INTEGER DEFAULT 0
);
CREATE INDEX idx_messages_queue_ts
    ON messages(queue, ts);
CREATE INDEX idx_messages_pending_queue_ts
    ON messages(queue, ts) WHERE claimed = 0;
```

Retain the other current broker tables, metadata, aliases, constraints, and
indexes whose semantics are unrelated to row order. Leave every caller-owned
sidecar table and object untouched. The exact DDL must be derived from the
current canonical schema rather than copied from this abbreviated block.
SQLite semantic uniqueness validation must recognize `ts INTEGER PRIMARY KEY`
as satisfying message-ID uniqueness even when no separate unique index exists.

The v5-to-v6 migration rebuilds only the SimpleBroker-owned `messages` table
and indexes in one transaction: create the canonical table under a temporary
name, copy `queue, body, ts, claimed` for every row (pending and claimed alike),
verify row-count equality, drop the old table, rename, and create the canonical
indexes. Metadata advances only after all migration DDL succeeds inside the
same transaction. The migration preflights dependencies on the removed private
column. A caller object that depends on `messages.id` causes a clear failure
before mutation; the migration never uses `CASCADE` to remove caller state.
Changes inside reserved broker tables or indexes are unsupported and need not
be preserved as sidecars.

Sidecar references to the supported public `messages.ts` key must remain valid
and textually unchanged. The SQLite rebuild must use a foreign-key-safe
procedure and run `PRAGMA foreign_key_check` before commit. If it temporarily
changes connection-level foreign-key enforcement, it restores the prior
setting on success and failure and never permits invalid state to commit.

After migration no `id` column or order-specific secondary index remains, and
there is no `messages` row in `sqlite_sequence`. SQLite may retain the
engine-owned `sqlite_sequence` table, especially when a caller sidecar uses
`AUTOINCREMENT`; every caller-owned sequence row remains unchanged. A failed
migration rolls back to the intact, still-usable v5 layout. Success and failure
tests snapshot and compare sidecar DDL, rows, indexes, constraints, and sequence
state. The rebuild requires temporary storage proportional to the message
table; disk-full and interruption probes must show transaction rollback without
sidecar damage.

All runtime SQL, including retrieve, staged delete, claimed vacuum, admin find,
and move paths, addresses rows by `ts`. Selection subqueries use explicit
`ORDER BY ts ASC|DESC`. SQLite `RETURNING` order remains unspecified, so core
normalization sorts returned `(body, ts)` rows by `ts` in the requested
direction before exposing them.

### Canonical supported PostgreSQL v6

Fresh schema uses `ts BIGINT PRIMARY KEY` and contains no `order_id` or owned
sequence. It creates general `(queue, ts)` and pending partial `(queue, ts)`
access paths. The v5-to-v6 migration runs in one transaction: drop the
`order_id` column with `RESTRICT` (its primary-key constraint and owned sequence
go with it), promote the existing `ts` unique index to the primary key via
`ADD PRIMARY KEY USING INDEX` (`ts` is already `NOT NULL`), create the canonical
access indexes, retire order-specific secondary indexes, and advance metadata
last. It never uses `CASCADE`. A caller sidecar dependency on the removed
private column therefore fails the transaction without mutation; unrelated
sidecar tables, data, indexes, constraints, and sequences remain untouched.

Project-scoped startup still takes `.broker.toml.lock` through PhaseLock as an
outer process lock. PostgreSQL correctness does not depend on that file lock:
direct and environment-selected targets have no config path, and two project
configs can name the same schema. Every setup/migration transaction therefore
takes a transaction-scoped advisory lock derived from stable database and
schema identity. After acquiring it, the transaction performs a live,
cache-bypassing `meta` version read on the same connection. A waiter that sees
v6 refreshes or invalidates stale setup state and exits without rerunning DDL;
only a live v5 observation may migrate. Failure rolls back and releases the
advisory lock automatically.

The rebuild holds an `ACCESS EXCLUSIVE` table lock for its duration; that is
acceptable for this installed base and must be noted in the release record. A
failed migration rolls back to the intact, still-usable v5 layout.

Retrieve CTEs select `ts` with explicit `ORDER BY ts ASC|DESC`; the outer
result query also orders by selected `ts` in the requested direction. No
correctness claim may rely on PostgreSQL `RETURNING` or CTE row order. Update
the runner's statement classification so it does not recognize a retrieve
query by the obsolete `SELECT order_id` prefix.

PostgreSQL ownership/current-shape validation keeps its existing anchors:
broker magic/version, complete owned tables, and current typed metadata. The
sole supported current SimpleBroker-owned layout has no surrogate. It must
reject foreign, partial, wrongly typed metadata, and newer owned shapes exactly
as before. Validation need not turn every tolerated but unknown addition into a
supported shape, and it does not promise to preserve modifications inside
reserved broker objects. Do not expand ownership validation into a
catalog-wide messages column/constraint/index validator. The v6
setup/migration owner separately checks the semantic `ts` uniqueness and
access paths, with real schema and sidecar-preservation tests.

### Redis

Keep the storage version and member encoding unchanged. Add a private order
argument to bounded claim/peek/move paths. `oldest` uses the current ascending
lexicographic range; `newest` uses the reverse lexicographic command with max
and min in Redis's required argument order. Lua continuation cursors must move
downward with an exclusive upper boundary so filtered windows cannot repeat or
skip eligible members.

Exact-ID lookup is direction-independent. Pending/claimed merge for
`peek(include_claimed=True)` sorts by decoded integer ID in the requested
direction before applying the limit. At-least-once generator batch scripts
remain ascending and do not acquire a reverse-order mode.

## Schema-Version Compatibility Gate

Advance `SCHEMA_VERSION` and `POSTGRES_SCHEMA_VERSION` to 6. Admission must
read and compare the owned `meta` version before any query that names a
message-table column. An old client making a new, cold open of a fresh
no-surrogate database must fail with its existing clear newer-schema
diagnostic. It must never reach `no such column: id`, `column order_id does not
exist`, or an equivalent mid-operation backend error.

This gate requires a real previous-release compatibility probe, not a mock of
the version constant:

1. Build the new artifacts and use them to create fresh SQLite and PostgreSQL
   v6 databases.
2. In isolated environments install the immediately previous published core
   and matching PostgreSQL extension.
3. Attempt a normal read against each new database.
4. Assert rejection occurs during setup/admission, mentions the unsupported
   newer schema version, and contains no missing-column diagnostic.
5. Assert the attempted operation did not mutate metadata, messages, aliases,
   claims, or queues.

The same probe must cover a v5 database after the new client migrates it to
v6. Its SimpleBroker-owned schema is equivalent to a fresh v6 database, modulo
engine-derived names and SQLite's persistent internal `sqlite_sequence` table,
and the old client must produce the same clean newer-schema refusal.

This is a cold-admission guarantee, not a live compatibility guarantee. An
already-open v7 handle has passed its only version check and may fail against a
subsequently migrated layout. Production cutover must stop and drain every v7
process and sidecar transaction before migration; live v7/v8 coexistence is
unsupported. A disposable transition probe keeps an old handle open across a
migration to demonstrate this boundary, but its backend-specific failure text
is not a public contract.

## Public and Internal API Design

Create one private order validator and one private literal vocabulary. All
Queue, direct-command, core, SQL, and extension paths consume its normalized
result. Do not duplicate case folding or boolean flag interpretation in each
backend. Accepted values are exact and case-sensitive; the error names both
allowed strings.

Change `simplebroker/_backend_plugins.py` to backend API v8 and add `order` to
claim, peek, and move one/many protocol methods. Update first-party extension
metadata, dependency floors, package assertions, release maps, fixtures, and
third-party diagnostic text. Do not add order to generator/batch protocol
methods.

High-level single-message methods that currently implement bounds through a
one-item generator must route to a bounded one/many primitive instead. This is
required to keep generator signatures free of `order`; do not add and then
hide a reverse generator option.

For move, the returned `MovedMessage` sequence follows selection order. The
destination queue stores the same IDs and its later default read independently
uses ascending ID. For peek with `include_claimed=True`, claimed state does not
change ordering.

## CLI Interface Review Requirements

Run `skills/interface-review/SKILL.md` against the final command grammar and
record the result in this plan. At minimum, verify:

- discoverability in `--help` for read, peek, and move;
- the name says what the operation does without claiming wall-clock order;
- text and JSON invalid-combination diagnostics identify both conflicting
  flags and the remedy;
- invalid values/combinations fail before target acquisition and mutation;
- registered-token rearrangement and `--` body escaping remain deterministic;
- direct command functions and CLI calls have identical semantics;
- examples are copy/paste executable and do not imply persistent LIFO mode;
- no generator/watch flag is advertised or silently ignored.

Walk all eleven principles in
`docs/agent-context/runbooks/designing-agent-facing-interfaces.md`, not only
the bullets above. Any exception belongs in the Deviation Log.

## Invariants and Hidden Couplings

The implementation and reviews must preserve these invariants:

1. **One identity:** `ts` is public identity, uniqueness key, selection key,
   update/delete key, move-preserved key, and deterministic result-order key.
2. **No engine order:** every materialized result is explicitly ordered or
   normalized after unordered engine output.
3. **Predicate before direction:** queue, claim-state, exact-ID, and open-range
   filters define eligibility before ascending or descending limit selection.
4. **Atomic destructive selection:** read/claim and move select and mutate the
   same ID set transactionally. A second query may order returned rows but may
   not reselect a different set.
5. **No supported surrogate layout:** fresh creation and completed migration
   both yield the sole supported canonical SimpleBroker-owned layout. Runtime
   SQL, validators, cleanup, test resets, query classifiers, and admin tools
   never reference `id` / `order_id`; SQLite has no `messages` sequence row.
6. **Migration atomicity:** a v5-to-v6 migration either completes to the
   canonical layout with every row preserved (including claimed state,
   aliases, metadata, and caller-owned sidecars) or leaves the intact v5
   database and sidecars behind.
7. **Version-before-shape use:** newer schema rejection precedes every message
   operation, including read-only commands.
8. **Generator boundary:** generators, all, stream, and watch are ascending
   only; no hidden reverse cursor exists.
9. **Backend parity:** SQLite, PostgreSQL, and Redis select and return the same
   IDs in the same requested order for an equivalent state.
10. **No timing fiction:** exact IDs can arrive out of insertion order; docs and
    errors use public-ID order, not chronology.
11. **Move identity:** moving never allocates or rewrites an ID.
12. **Dump stability:** persistence format and its deterministic ID order do
    not change unless a firing test proves a required consequence and the plan
    records it as a deviation.
13. **Sidecar noninterference:** setup and migration change only
    SimpleBroker-owned objects. On success and failure, caller-owned sidecar
    definitions, rows, indexes, constraints, and sequence state are unchanged.
14. **Migration serialization:** project PhaseLock is an outer startup lock;
    PostgreSQL's transaction advisory lock plus a live under-lock version read
    is the authoritative migration guard for every target form.
15. **Quiesced cutover:** clean old-client rejection applies to a new cold
    admission. It does not imply that an already-open v7 client can coexist
    with, or survive migration by, v8.

Hidden couplings to inspect explicitly:

- SQLite `RETURNING` normalization and tests that currently carry private IDs.
- PostgreSQL runner/query classification based on SQL prefixes.
- PostgreSQL direct/environment targets with no `config_path`, multiple config
  files naming one schema, stale setup caches, and advisory-lock key identity.
- delete/vacuum staging and any `WHERE id IN (...)` maintenance query.
- index migration and readiness checks that infer uniqueness from an exact
  index name rather than semantics.
- test resets using `RESTART IDENTITY` and factories assuming a sequence.
- SQLite's persistent engine-owned `sqlite_sequence` table, caller-owned
  sequence rows, sidecar dependencies, and rebuild temporary-space failure.
- Redis Lua max/min ordering, exclusive cursors, and fixed-width lexicographic
  ID representation.
- CLI registered option-token inventory, rearrangement normalization, parser
  fuzzing, shell completion/help snapshots, and body escaping.
- overloads, protocols, mocks/fakes, third-party backend error messages,
  package dependency floors, and the release driver backend-API map.
- Weft exact insertion, move, resume-bound, and first-message assumptions.
- Already-admitted v7 handles, in-flight sidecar transactions, and operational
  drains that are outside the schema admission gate.

## Rollout and Rollback

This is a coordinated major release. Do not use `python3 bin/release.py all`:
that target pushes all candidate tags after shared checks and does not serialize
their publication workflows. Record owner-approved extension versions, then
run and wait for these single-target releases in order:

```bash
python3 bin/release.py pg --version <PG_VERSION>
python3 bin/release.py redis --version <REDIS_VERSION>
python3 bin/release.py core --version 8.0.0
```

After each extension command, wait for its tag workflow and immutable PyPI
artifact to succeed. Download the wheel without dependencies and inspect its
name, hash, backend API v8 metadata, and `simplebroker>=8.0.0` requirement
before starting the next release. Those extension artifacts are intentionally
not dependency-resolvable from a clean index until core 8.0.0 is published.
Keep that interval short, announce it in the release record, and do not claim a
clean install as extension-publication evidence during it. Only after both
extension artifacts are visible and correct may the core command run. After
core publication, require a clean index install of core with both extras and a
full import/version/handshake smoke before calling the coordinated release
usable. The release notes must tell operators to back up SQL targets before
first open with the new release.

The operator cutover is a quiesced, one-way transition:

1. Stop and drain every v7 SimpleBroker process and every transaction that
   accesses caller-owned sidecars in the target database or schema.
2. Take and verify a whole-target SQLite file backup or PostgreSQL schema/
   database backup. The backup includes SimpleBroker objects and sidecars.
3. Install a coherent v8 core and backend-extension set.
4. Let exactly one v8 initializer migrate, then inspect schema version,
   SimpleBroker-owned shape, row counts, sidecar snapshots, and representative
   behavior.
5. Start only v8 clients. Do not restart v7 against the migrated target.

Opening an existing v5 SQL target with v8 then rebuilds it to the canonical v6
layout: a transactional table rebuild on SQLite, a column/constraint rewrite
on PostgreSQL. That is a deliberate one-way compatibility boundary. A
previous client making a later cold open rejects the result. Mixed live v7/v8
operation is unsupported, so backup-before-first-open is load-bearing, not
ceremonial. Do not advise users to edit the metadata version backward.
Supported rollback is another quiesced operation: stop and drain v8 clients,
restore the whole pre-v8 backup, reinstall the old package set, verify the
restored sidecars and broker state, then restart v7. A v8 dump may help move
pending portable data into an old fresh target only where the unchanged dump
contract is sufficient; it is not a substitute for a full database backup or
a promise to preserve in-flight claim state.

Before publication, exercise rollback from real SQLite and PostgreSQL backups:
quiesce the fixture, upgrade v5 to v6, verify v8 behavior and unchanged
sidecars, quiesce v8, restore the whole v5 backup, reinstall the previous
artifacts, and verify the original broker data, sidecars, and order contract.
Keep the v8 artifacts available until that proof and downstream rollout
complete.

Post-release success signals:

- fresh SQLite/PG inspection shows no surrogate column;
- a migrated legacy target shows the same supported SimpleBroker-owned layout
  as fresh, modulo engine-derived internal names;
- sidecar schema, data, indexes, constraints, and sequence state match their
  pre-migration snapshots;
- ordinary generated-write workloads retain observed FIFO order;
- exact out-of-order workloads match ascending/default and descending/newest
  expectations on all backends;
- previous clients making a cold open reject v6 at admission with the
  documented diagnostic;
- PostgreSQL contention proves one migration across project, direct, and
  environment-selected startup paths;
- Weft's existing suite remains green and its prospective bounded-newest use
  needs no backend-specific branch.

## Storage Test Matrix

There is one current SQL layout, so the behavior suites run once per backend
with no layout parameter. Migration correctness is owned by dedicated
transition tests, not a doubled matrix:

- Add `tests/helper_scripts/sql_storage_layouts.py` with
  `create_sqlite_v5_layout` and `create_postgres_v5_layout`. These helpers
  embed the last v5 DDL and literal schema version; they do not import current
  schema constants or current create-table templates that change at v6. The
  PostgreSQL helper imports its driver lazily so SQLite-only collection does
  not acquire a PostgreSQL dependency. Root and extension migration tests
  reuse these helpers rather than copying DDL.
- Migration transition tests construct a real v5 layout with those helpers,
  seed pending and claimed rows, exact IDs, aliases, metadata, and caller-owned
  sidecars with rows, indexes, constraints (including a reference to the public
  `messages.ts` key), and sequence state, open it with current code, and assert:
  schema version 6; the canonical broker-owned table shape with no surrogate
  column, `messages` sequence row, or order-specific index; every broker row
  preserved with identical `queue, body, ts, claimed`; every sidecar snapshot
  unchanged; and post-migration behavior identical to a fresh database on a
  sample of the behavior matrix.
- Migration failure-injection tests abort the migration at representative DDL
  steps, including SQLite disk exhaustion, and assert the v5 layout, metadata,
  and sidecar snapshots remain intact and usable.
- One equivalence test proves a migrated database and a fresh database
  produce identical SimpleBroker-owned schema introspection output, modulo
  engine-derived internal names and SQLite's internal `sqlite_sequence` table.
  Caller sidecars are excluded from physical-equivalence comparison because
  preservation, not canonicalization, is their contract.
- PostgreSQL concurrency tests race direct/direct initializers with no config
  path, two project configs naming one schema, and project/direct initializers.
  A stale cached v5 observation must not rerun DDL after the waiter acquires the
  advisory lock and reads live v6. Assert one migration, successful admission
  by both callers, and advisory-lock release after injected failure.

At minimum, the per-backend behavior suite covers:

- setup/admission and repeat setup;
- sidecar preservation on fresh setup, successful migration, and failed
  migration;
- read/peek/move one and many in both orders;
- exact insertion, bounds, include-claimed peek, and ID-preserving move;
- generator/all ascending behavior;
- delete, vacuum, find/admin, aliases, reset/cleanup, and reopen;
- contention/atomicity cases that execute retrieve SQL;
- schema validation, semantic uniqueness, index readiness, and migration
  failure rollback.

Use real SQLite and PostgreSQL. Mocks may test call boundaries but do not count
as evidence for SQL shape, engine return order, locks, migration atomicity,
ownership admission, or old-version rejection.

## Executable Behavior Matrix

Seed equivalent queues with exact IDs in insertion order `300, 100, 200` and
use generated IDs in a separate ordinary-write case. Required results:

| Surface | Order/control | Expected IDs | Notes |
|---------|---------------|--------------|-------|
| read/peek/move one | default / `oldest` | `100` | Destructive forms mutate that same ID. |
| read/peek/move one | `newest` | `300` | Exact-ID filters still return only their match. |
| read/peek/move many, limit 2 | `oldest` | `100, 200` | Returned order equals selection order. |
| read/peek/move many, limit 2 | `newest` | `300, 200` | Returned order equals selection order. |
| bounded after 100, before 300 | `newest` | `200` | Bounds apply before direction. |
| generators / all | no order control | `100, 200, 300` | Signature and CLI reject/omit reverse control. |
| ordinary generated writes | default | insertion order | Consequence of monotone generated IDs. |
| `find_message_ids`, limit 2 | ascending only | `100, 200` | Administrative search has no order control. |

Repeat the behavioral rows on SQLite, PostgreSQL, and Redis; the migration
transition tests additionally run a sample of them on databases migrated
from v5. Force SQLite test
adapters to return `RETURNING` rows in the opposite physical order and prove
normalization in both directions. Do not make a production guarantee from a
test that happens to observe native engine order.

## Work Plan

### Task 0: Freeze baseline and inspect Weft first

Use `../weft` when present and permit an owner-supplied alternate through
`SIMPLEBROKER_WEFT_ROOT`. Run this exact baseline from outside the
SimpleBroker import path:

```bash
simplebroker_weft_root="${SIMPLEBROKER_WEFT_ROOT:-../weft}"
test -f "$simplebroker_weft_root/pyproject.toml"
git -C "$simplebroker_weft_root" rev-parse HEAD
rg -n 'read(_one|_many|_generator)?\(|peek(_one|_many|_generator)?\(|move(_one|_many|_generator)?\(|insert_messages\(|load_lines\(|after_timestamp|before_timestamp|last_ts|latest_pending_timestamp' "$simplebroker_weft_root/weft" "$simplebroker_weft_root/tests" "$simplebroker_weft_root/integrations" "$simplebroker_weft_root/extensions"
(cd "$simplebroker_weft_root" && env -u PYTHONPATH uv sync --frozen --all-extras)
(cd "$simplebroker_weft_root" && env -u PYTHONPATH uv run --frozen --all-extras python -c 'import importlib.metadata as m, pathlib, simplebroker; print(m.version("simplebroker")); print(pathlib.Path(simplebroker.__file__).resolve())')
(cd "$simplebroker_weft_root" && env -u PYTHONPATH uv run --frozen --all-extras pytest)
(cd "$simplebroker_weft_root" && env -u PYTHONPATH uv run --frozen --all-extras python bin/pytest-pg --all)
```

1. Record SimpleBroker and Weft commit SHAs, resolved package versions and
   module paths, test counts, skips, and PostgreSQL service version. The module
   path must not resolve to the sibling SimpleBroker source tree.
2. Classify every inventory hit as bounded one/many, live generator/watch,
   exact insertion/load, move, resume-bound, or unrelated occurrence.
3. Record each first/last or order assumption and its disposition. If Weft
   relies on private SQL insertion order for out-of-order exact IDs, treat it
   as a migration issue, not a reason to hide the SimpleBroker contract.
4. Reconfirm the 2026-08-25 prerequisite is present and both SQL services used
   for acceptance are real supported versions.

Gate: no product-code edit until the Weft report and baseline results are
recorded under **Execution Evidence**.

### Task 1: Green spec promotion, then uncommitted red probes

1. Apply the exact requirement text to the canonical specs, registry, and spec
   index using Strategy A. Add related-plan backlinks and the limited
   historical supersession note, but do not add implementation mappings or
   firing-test claims that are not green yet.
2. Run the product-doc, path, plan-context, and traceability gates. Land this
   spec-promotion slice only while all repository gates are green, then record
   its commit SHA as the promotion baseline.
3. In the worktree, add minimal red probes for [SB-ID-1], [SB-SELECT-5],
   [SB-CLI-6], and amended [SB-API-4/7/10/11/12]. Run and record their exact
   node IDs and expected failures. Do not commit or describe a failing test
   state as landed.
4. Each later implementation task lands its now-green firing tests and
   reciprocal spec mapping with the owning code. README, guides,
   implementation rationale, and CHANGELOG also land with the first green
   slice that makes their claim true, then receive final reconciliation in
   Task 8.
5. Inventory active prose with
   `rg -n -i '\bfifo\b|insertion order|write order|storage order' README.md docs/agent-kernel.md docs/guides docs/specs docs/implementation`.
   Revise every active private-storage-order claim, including [SB-ID-1] and
   [SB-DELIVERY-3] evidence/related-plan text and implementation document 09.
   Historical plans and old CHANGELOG entries remain historical; record rather
   than rewrite them.

Gate: the spec-promotion commit is green and recorded; every planned behavior
has a red probe with the intended cause; no failing state is committed.

### Task 2: Central order vocabulary and backend API v8

Add one internal validator, extend Queue/direct-command/core one/many
signatures, update overloads and protocols, and advance the backend handshake.
Refactor bounded high-level single calls away from generators. Update fake and
third-party-backend contract fixtures. Reject invalid order and newest+all
before target acquisition. Land the green Python/API tests, spec mappings,
root API example, Python guide, and agent-kernel boundary with this slice.

Gate: unit, typing, protocol, plugin mismatch, and pre-target possession tests
pass. Generator signatures contain no `order` parameter.

### Task 3: SQLite v6 canonical layout and rebuild migration

Create the fresh canonical schema, the transactional rebuilding v5 migration,
semantic uniqueness recognition for `ts INTEGER PRIMARY KEY`, ts-addressed
maintenance SQL, direction-aware retrieval SQL, and direction-aware
post-`RETURNING` normalization. Add the v5 helper, migration transition,
failure-injection, sidecar-preservation, `sqlite_sequence`, disk-exhaustion,
and fresh/migrated owned-schema equivalence tests, and the SQLite half of the
storage implementation-rationale update.

Gate: the full SQLite backend-touching matrix passes; migration transition,
failure-injection, sidecar-preservation, and equivalence tests pass; query plan
evidence shows bounded pending selection uses an intended ts index; forced
reverse raw rows cannot change public order.

### Task 4: PostgreSQL v6 canonical layout and rebuild migration

Create the fresh canonical schema and the atomic rebuilding v5 migration
(column and sequence drop, primary-key promotion); convert every runtime/admin
query and query classifier to `ts`; keep ownership/readiness validation
anchors unchanged with their current foreign/partial/type rejections. Do not
add a parallel full message-schema validator. Reuse the Task 3 migration-test
pattern with real PostgreSQL. Add the transaction-scoped advisory lock and a
cache-bypassing, same-connection version recheck after lock acquisition. Keep
project PhaseLock as the outer startup lock, not the database-level correctness
mechanism. Preserve sidecars and use `RESTRICT`, never `CASCADE`, for the
private-column removal.

Land the PostgreSQL half of the storage implementation-rationale update with
this slice.

Gate: the full real-PostgreSQL matrix passes on every supported CI PostgreSQL
version; migration-failure injection leaves v5 metadata and DDL recoverable;
fresh/migrated owned-schema equivalence and sidecar preservation hold;
direct/direct, two-config/one-schema, and project/direct contention each run
one migration despite stale setup state; advisory locks release on failure;
`EXPLAIN` confirms intended bounded selection indexes.

### Task 5: Redis descending bounded selection

Thread the normalized order through only bounded one/many paths, implement
reverse lexicographic commands and Lua cursor direction, and order claimed /
pending merge results consistently. Keep live batch/generator behavior
ascending.

Gate: real Redis/Valkey contract, Lua boundary, filtered-window, contention,
and include-claimed tests pass in both orders. Cross-backend property tests
produce the same ID sequences.

### Task 6: CLI sugar and grammar hardening

Add `--newest` to read/peek/move help and parser metadata, route it to the
shared order value, add newest+all pre-target rejection, and update registered
token escaping, rearrangement, property, and fuzz coverage. Do not add a watch
flag. Land the root README command/options text and CHANGELOG entry when these
tests are green.

Gate: text/JSON/direct-command equivalence, help snapshots, registered-token
collision, shell examples, property tests, and adversarial parser fuzz corpus
pass. Complete and record the agent-facing interface review.

### Task 7: Compatibility, downstream, packaging, and release proof

Run real previous-client rejection probes, backup/restore rollback drills, and
build all artifacts into one recorded manifest. Let `simplebroker_core_wheel`,
`simplebroker_pg_wheel`, and `simplebroker_redis_wheel` be the absolute paths
and verify their hashes before this clean Weft artifact run:

```bash
simplebroker_weft_root="${SIMPLEBROKER_WEFT_ROOT:-../weft}"
(cd "$simplebroker_weft_root" && env -u PYTHONPATH uv run --frozen --all-extras --with "$simplebroker_core_wheel" --with "$simplebroker_pg_wheel" --with "$simplebroker_redis_wheel" python -c 'import importlib.metadata as m, pathlib, simplebroker, simplebroker_pg, simplebroker_redis; print(m.version("simplebroker"), m.version("simplebroker-pg"), m.version("simplebroker-redis")); print(pathlib.Path(simplebroker.__file__).resolve())')
(cd "$simplebroker_weft_root" && env -u PYTHONPATH uv run --frozen --all-extras --with "$simplebroker_core_wheel" --with "$simplebroker_pg_wheel" --with "$simplebroker_redis_wheel" pytest)
```

The module path must be inside the uv environment, not this source checkout.
Before any publication, create a disposable Weft worktree at the recorded Task
0 SHA. Add the three absolute wheel paths to that worktree's optional `dev`
extra, without committing its `pyproject.toml` or lock changes. Export exact
expected-version and source-root values. Using `apply_patch`, add an exact copy
of the checked-in SimpleBroker compatibility probe at the unique Weft-local
path `tests/test_simplebroker_artifact_versions_probe.py`; do not collect a
test by absolute path from the SimpleBroker tree because that would load
SimpleBroker's `tests/conftest.py`. Verify byte equality and record the hash,
then run Weft's real PG wrapper over its own `tests` tree:

```bash
simplebroker_source_root="$(pwd -P)"
export SIMPLEBROKER_EXPECTED_CORE_VERSION=8.0.0
export SIMPLEBROKER_EXPECTED_PG_VERSION=<PG_VERSION>
export SIMPLEBROKER_EXPECTED_REDIS_VERSION=<REDIS_VERSION>
export SIMPLEBROKER_EXPECTED_SOURCE_ROOT="$simplebroker_source_root"
(cd "$simplebroker_weft_artifact_worktree" && uv add --optional dev "$simplebroker_core_wheel" "$simplebroker_pg_wheel" "$simplebroker_redis_wheel")
(cmp -s "$simplebroker_source_root/tests/compatibility/test_weft_artifact_versions.py" "$simplebroker_weft_artifact_worktree/tests/test_simplebroker_artifact_versions_probe.py" && shasum -a 256 "$simplebroker_weft_artifact_worktree/tests/test_simplebroker_artifact_versions_probe.py")
(cd "$simplebroker_weft_artifact_worktree" && env -u PYTHONPATH uv run --all-extras python -c 'import importlib.metadata as m, os; assert m.version("simplebroker") == os.environ["SIMPLEBROKER_EXPECTED_CORE_VERSION"]; assert m.version("simplebroker-pg") == os.environ["SIMPLEBROKER_EXPECTED_PG_VERSION"]; assert m.version("simplebroker-redis") == os.environ["SIMPLEBROKER_EXPECTED_REDIS_VERSION"]')
(cd "$simplebroker_weft_artifact_worktree" && env -u PYTHONPATH uv run --all-extras python bin/pytest-pg --all tests)
```

The compatibility probe runs inside the wrapper's inner pytest process. It
asserts all three metadata versions, imports all three modules, and rejects any
module path under `SIMPLEBROKER_EXPECTED_SOURCE_ROOT`. If the wrapper's
hard-coded `--with simplebroker-pg[dev]` overrides the disposable worktree's
direct wheel source, the probe fails and publication is blocked. Do not patch
the probe or accept an outer-environment version print as a substitute; record
the uv resolution as a plan deviation before changing this test architecture.
In the ordinary SimpleBroker suite the source probe may skip only when all four
`SIMPLEBROKER_EXPECTED_*` variables are absent. A partial variable set is an
error; with all four present every assertion must fire. The copied Weft-local
file imports no SimpleBroker test helper or conftest module.

After the three waited single-target publications in **Rollout and Rollback**,
create a second disposable Weft worktree at the same recorded SHA. Pin the
three expected published versions explicitly, not through an unconstrained
upgrade. Add the same byte-identical probe under that worktree's `tests` tree,
repeat the equality/hash check, then run the in-child possession probe and PG
suite:

```bash
(cd "$simplebroker_weft_release_worktree" && uv add --optional dev "simplebroker==$SIMPLEBROKER_EXPECTED_CORE_VERSION" "simplebroker-pg==$SIMPLEBROKER_EXPECTED_PG_VERSION" "simplebroker-redis==$SIMPLEBROKER_EXPECTED_REDIS_VERSION")
(cmp -s "$simplebroker_source_root/tests/compatibility/test_weft_artifact_versions.py" "$simplebroker_weft_release_worktree/tests/test_simplebroker_artifact_versions_probe.py" && shasum -a 256 "$simplebroker_weft_release_worktree/tests/test_simplebroker_artifact_versions_probe.py")
(cd "$simplebroker_weft_release_worktree" && env -u PYTHONPATH uv run --all-extras python bin/pytest-pg --all tests)
```

Do not commit either disposable Weft dependency/lock change. Update all
SimpleBroker version floors and backend API mappings before artifact
construction.

Gate: no source-tree import leakage; artifact metadata selects compatible
extensions; cold old-client opens reject v6 cleanly; the already-open-handle
probe documents why quiescence is required; whole-target rollback restores
broker and sidecar state before old artifacts restart; Weft's default suite
passes the built artifacts; Weft's PostgreSQL suite passes the exact published
versions; all SimpleBroker backend suites pass the built artifacts before
publication.

### Task 8: Final documentation, review, and closure

Re-read code against every invariant and enumerable contract element. Update
implementation rationale, repository maps, execution evidence, deviations,
and any durable lesson. Run independent different-family reviews after the SQL
slice, after the Redis/CLI slice, and before completion. Resolve each finding
or record an explicit rebuttal with evidence. Close this plan and its index row
only in the same committed change that contains final evidence.

## Dependency Graph

```text
Task 0 Weft/prerequisite baseline
  -> Task 1 green spec promotion + uncommitted red probes
     -> Task 2 shared API/backend v8
        -> Task 3 SQLite v6 ----\
        -> Task 4 PostgreSQL v6 --+-> Task 7 compatibility/artifacts/Weft
        -> Task 5 Redis order ----/                 |
        -> Task 6 CLI grammar ----------------------/
                                                   -> Task 8 final review
```

Tasks 3, 4, 5, and 6 may proceed in parallel only after Task 2 fixes the
shared signature and red contracts. Each is a meaningful independently
reviewable slice.

## Verification Commands and Evidence Floors

Use targeted tests during each task, then run the repository's current exact
commands from its runbooks. The minimum final evidence is:

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run --frozen --no-sync mypy simplebroker bin/release.py bin/ruff_suppression_index.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
mapfile -t core_test_files < <(find tests -type f -name '*.py' -not -path '*/__pycache__/*' -not -path 'tests/typecheck_fixtures/*' | sort)
MYPYPATH=. uv run --frozen --no-sync mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs "${core_test_files[@]}"
uv run --frozen --no-sync ./bin/pytest-pg
uv run --frozen --no-sync ./bin/pytest-redis
uv run --frozen --no-sync ./bin/packaging-smoke
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

Resolve exact extension and live-service commands from the current repository
runbooks rather than assuming the abbreviated examples above remain complete.
Run the supported PostgreSQL matrix and the repository's Redis/Valkey service
matrix. Run parser fuzzing and property tests at their documented time budgets,
not just seed examples.

Every enumerable contract element must have a firing test:

- two order values and invalid runtime values;
- read, peek, move; one, many, high-level bounded form;
- exact ID, after, before, and both bounds;
- include-claimed peek and move destination behavior;
- generators/all/watch exclusion and ascending default;
- CLI default/newest, newest+all, registered-token collision, text/JSON error;
- three backends; fresh and migrated-from-v5 SQL databases;
- fresh schema, v5 migration, repeat setup, migration rollback;
- sidecar DDL, rows, indexes, constraints, and sequence state across successful
  and failed SQLite/PostgreSQL migration;
- SQLite `sqlite_sequence` with both the removed `messages` row and preserved
  caller-owned rows;
- PostgreSQL advisory-lock contention for direct/direct, two project configs
  naming one schema, and project/direct startup, including stale cached state
  and failure-release behavior;
- cold old client against fresh v6 and upgraded v6, plus the already-open
  handle boundary that makes quiescence mandatory;
- forced unordered `RETURNING` result;
- plugin v7/v8 mismatch, artifact installation, and Weft.

Apply the adversarial acceptance probes for empty queues, limit zero/one/max,
IDs at `1` and `2**63 - 1` where valid, exact missing ID, invalid bounds,
concurrent claims/moves, duplicate exact IDs, interruption, and backend
failure. Include SQLite temporary-space exhaustion and a PostgreSQL migration
failure after advisory-lock acquisition. A mock-only proof is unacceptable for
engine, schema, atomicity, locking, ordering, or compatibility claims.

## Acceptance Criteria

This plan is complete only when:

1. [SB-ID-1], [SB-SELECT-5], [SB-CLI-6], [SB-API-4/7/10/11/12], the registry,
   README, guides, implementation docs, and CHANGELOG agree.
2. Default and newest bounded selections produce the specified ID order on
   SQLite, PostgreSQL, and Redis.
3. The sole supported current SimpleBroker-owned SQL layout contains no private
   surrogate: fresh databases are created without it, migrated databases have
   been rebuilt without it, runtime code does not reference it, and SQLite has
   no `messages` sequence row.
4. Migration transition, failure-injection, and fresh/migrated equivalence
   tests pass on real SQLite and PostgreSQL, with all seeded rows, claimed
   state, aliases, and metadata preserved. Caller-owned sidecar definitions,
   rows, indexes, constraints, and sequence state are unchanged on success and
   failure.
5. PostgreSQL validation anchors are unchanged and still reject foreign
   identity, partial owned state, conflicting or wrongly typed metadata, and
   newer schemas. Setup/migration tests prove `ts` uniqueness and access
   paths.
6. PostgreSQL migration serialization works for direct and project target
   topologies through an advisory lock and live under-lock version recheck;
   stale cache state cannot rerun DDL and failure releases the lock.
7. A real old client making a cold open rejects fresh and upgraded v6 databases
   during admission with a clear version diagnostic and no mutation or
   missing-column error. Cutover and rollback tests quiesce clients and prove
   that live v7/v8 coexistence is not promised.
8. Generators, all, stream, and watch have no newest control and remain
   ascending by public ID.
9. Engine `RETURNING` order cannot affect exposed order.
10. Whole-target backup/restore rollback, clean artifact installs, backend
    handshake, full suites, and Weft proof pass. The rollback rehearsal
    includes sidecar state.
11. Independent review findings are resolved, final evidence is recorded, the
    work is committed without agent attribution, and the index row is changed
    to `completed` in that same change.

## Out of Scope

- A persistent queue-level FIFO/LIFO setting.
- `read_last`, `peek_last`, or `move_last` aliases.
- Reverse generator, stream, `--all`, or watch traversal.
- Ordering by message body, claimed time, insertion time, or wall clock.
- Changing dump format or Redis storage version.
- A Weft feature or Weft-specific broker mode.
- Rolling or mixed live v7/v8 operation on a migrated SQL target.
- Preserving or supporting caller modifications inside reserved broker tables,
  constraints, or indexes. Caller-owned sidecar tables remain in scope for
  noninterference.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| [SB-API-11], acceptance 7 | Every previous-client PostgreSQL cold open rejects v6 at the metadata-version gate before legacy column use. | The published v7.5.1/PG 3.10.0 `BrokerTarget` path rejects fresh and upgraded v6 cleanly and without mutation. The documented injected `Queue(..., runner=PostgresRunner(...))` path wraps the runner; v7's extension then misses its concrete-runner metadata fast path and parses legacy bootstrap DDL against `order_id` before core performs its version check. It fails with `column "order_id" does not exist`. | The previous artifacts are immutable. PostgreSQL resolves the missing column even when an index of the legacy name already exists, so a surrogate-free v6 target cannot repair that path with an index-name sentinel. Satisfying the unconditional promise requires retaining an `order_id`-compatible column, patching and releasing the old line first, or narrowing the promise. | No contract change has been selected. The no-surrogate implementation remains intact and plan completion is blocked on this owner choice. |
| Task 0 inventory, Task 7 Weft gate | Weft has no production dependency on insertion order for exact-ID rows and its suites pass the v8 artifacts unchanged. | Pipeline child TIDs are allocated stage-then-edge but exact-ID spawn rows are submitted edge-then-stage. Ascending public-ID retrieval therefore changes the observed internal spawn order. The two pipeline-order tests pass with core 7.5.1 and fail with core 8.0.0 on both SQLite and PostgreSQL artifact runs. | The static Task 0 inventory saw exact insertion but incorrectly assumed the generated IDs were monotone in submission order. SimpleBroker is behaving according to the promoted [SB-SELECT-5] contract. | Keep the SimpleBroker contract. Weft should allocate edge and stage TIDs in dependency/submission order (or otherwise stop relying on exact-ID insertion order) in a separately governed downstream change. |

Empty at authoring time. Any implementation change to supported surfaces,
schema-version strategy, accepted layouts, order vocabulary, generator
boundary, release sequence, rollback method, or spec wording must add a row
before the differing code lands. `pending` is not allowed at plan completion.

## Execution Evidence

### 2026-08-27 Task 0 baseline and possession gate

- SimpleBroker baseline: `4ef13bbbfb3888cf88fa31eb1a0d6646da61f373`.
  Weft baseline: `33e1ab767046e6a7e22904d5198840b174552798`.
  `uv sync --frozen --all-extras` completed. The possession probe resolved
  SimpleBroker 7.5.1 from
  `/Users/van/Developer/weft/.venv/lib/python3.14/site-packages/simplebroker`,
  not this checkout. The environment resolved simplebroker-pg 3.10.0; Redis
  was not installed in the ordinary Weft environment.
- `env -u PYTHONPATH uv run --frozen --all-extras pytest` exercised the dirty
  owner worktree and finished with 4308 passed, 5 skipped, and 3 failed in
  250.41 seconds. All three failures came from the pre-existing untracked
  `tests/benchmarks/test_tid_mapping_capacity_benchmarks.py`: four Ruff
  findings caused the real-Ruff and suppression-index policy gates to fail.
  No SimpleBroker behavioral baseline test failed.
- `env -u PYTHONPATH uv run --frozen --all-extras python bin/pytest-pg --all`
  started a real PostgreSQL 18 container and passed the extension preflight.
  The same dirty-tree Ruff failure appeared, and the untracked capacity suite
  left one worker running successive multi-minute benchmarks. The run was
  interrupted after 16 minutes rather than modify or delete owner work; the
  wrapper removed its container. This is baseline contamination, not green
  PostgreSQL evidence, and must be rerun in the disposable pinned Weft
  worktrees required by Task 7.
- Inventory of Weft production calls found bounded `read`/`peek`/`move`, live
  ascending generators, exact spawn-request insertion, dump/load, and
  timestamp bounds. Spawn-request exact IDs are preallocated through
  `Queue.generate_timestamp()`, so ordinary order remains monotone. Dump/load
  intentionally preserves public IDs. `_drain_outbox_until_timestamp()` relies
  on the default exposing the lowest eligible public ID before it stops above
  a boundary, which the new contract strengthens. No production path was
  found that relies on private SQL insertion order. Manager idle tracking has
  an existing `newest pending` docstring over default `peek_one()`; the new
  bounded newest surface can serve that prospective use later, but changing
  Weft is outside this plan.
- The 2026-08-25 prerequisite is present at the SimpleBroker baseline. The
  baseline service proof used real PostgreSQL 18. Redis/Valkey remains a later
  SimpleBroker backend gate.

Possession answers:

1. SQLite's inner `ORDER BY` chooses the limited ID set but does not order DML
   `RETURNING`; core must normalize exposed rows by `ts` in the requested
   direction.
2. PostgreSQL ownership remains magic/version plus the complete broker table
   set and typed metadata. Setup/migration and real schema tests, not admission
   classification, prove `ts` uniqueness and access paths.
3. Bounded newest chooses one finite atomic set. A reverse live cursor could
   miss later higher IDs and has no defined progress contract, so generators
   and watch stay ascending.
4. Project PhaseLock is only the outer `.broker.toml.lock`. A stable
   transaction-scoped PostgreSQL advisory lock plus a same-connection,
   cache-bypassing live version read serializes every target form.
5. Migration preserves all supported broker state and every caller-owned
   sidecar definition and state. Reserved-object modifications are unsupported;
   SQLite may retain the engine `sqlite_sequence` table but not a `messages`
   row, and caller sequence rows remain unchanged.

### 2026-08-27 Tasks 1–2 contract and shared API slice

- Spec promotion committed at
  `a3afca7140e0b2c43e3cb39c81e6475f6351c742`. Its gates were
  `python3 bin/check-dom15-fixtures`, `bin/check-plan-context`,
  `python3 bin/check-doc-paths`, the affected canonical-contract tests, and
  `git diff --check`; all passed.
- The first public behavior probe inserted IDs `300, 100, 200` and failed
  because default `peek_one()` returned physical-first ID `300` instead of
  lowest public ID `100`. After the shared order/query slice, the complete
  SQLite Queue/direct-command matrix in
  `tests/test_timestamp_selection_contract_sb_select.py` passed, including
  default/newest one and many, open bounds, exact ID, pre-target rejection,
  and generator-signature exclusion.
- `uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg
  extensions/simplebroker_redis/simplebroker_redis --config-file
  pyproject.toml` reported no issues in 64 source files. The focused protocol,
  plugin-resolution, command, SQL-internal, API-contract, and typing suites
  passed. The Redis extension suite passed all locally runnable tests; real
  Redis behavior remained skipped pending Task 6's service gate.
- Backend API v8 is registered with minimum core `8.0.0`. The coordinated
  source versions are core `8.0.0`, PostgreSQL extension `4.0.0`, and Redis
  extension `4.0.0`; publication remains forbidden until the ordered Task 7
  gates and explicit owner authorization.

### 2026-08-27 Task 3 SQLite v6 slice

- Fresh SQLite databases use `ts INTEGER PRIMARY KEY` with no private `id`, no
  message sequence row, and only the canonical general and pending
  `(queue, ts)` indexes. Runtime retrieval, exact delete, claimed cleanup,
  admin lookup, and move SQL address rows by `ts`.
- The literal v5 fixture rebuilds in one transaction and verifies row-count
  equality before publishing schema version 6. Success and injected
  disk-full/rename failures preserve caller tables, rows, indexes, foreign
  keys to `messages.ts`, and unrelated `sqlite_sequence` entries. Both paths
  restore the caller's prior `PRAGMA foreign_keys` setting. Dependencies on
  removed `messages.id` fail before mutation.
- `uv run pytest -m sqlite_only -q --tb=short` passed the full SQLite marker
  matrix with six documented environment/platform skips. Focused schema,
  forced-reverse-`RETURNING`, [SB-ID-1], [SB-SELECT-5], and [SB-DELIVERY-3]
  evidence passed. The query-plan probe covers both `ORDER BY ts ASC` and
  `DESC` and observes `idx_messages_pending_queue_ts`.
- `uv run ruff check .`, the SQLite/core mypy gate, Ruff suppression-registry
  regeneration/check, `python3 bin/check-dom15-fixtures`,
  `bin/check-plan-context`, `python3 bin/check-doc-paths`, and
  `git diff --check` passed. The async pooled SQLite example was migrated to
  the v6 schema and contains no runtime reference to the retired key.

### 2026-08-27 Task 4 PostgreSQL v6 slice

- Fresh PostgreSQL schema v6 uses `ts BIGINT PRIMARY KEY`, has no `order_id`,
  and owns no message-order sequence. Runtime and maintenance SQL, prepared
  query classification, finite selection, mutation joins, and outer result
  order use `ts` only. Both ascending and descending bounded pending plans use
  `idx_messages_pending_queue_ts` under the real planner probe.
- The literal v5 fixture migrates under a transaction-scoped advisory lock and
  `ACCESS EXCLUSIVE` message-table lock. It rereads live metadata after the
  advisory lock, drops `order_id` with `RESTRICT`, promotes the existing `ts`
  unique index to the primary key, creates the canonical access paths, checks
  the resulting semantic shape, and writes version 6 last. Healthy v6 startup
  emits no DDL; a missing owned required index is repaired only after a failed
  semantic shape check.
- Real PostgreSQL 18 tests cover direct/direct, two-project-config/one-schema,
  and project/direct concurrent v5 startup. Both contenders complete while
  only the under-lock v5 observer can execute destructive DDL. Injected
  migration failure restores v5 metadata, column, sequence, rows, and
  independent sidecar state; a fresh runner then acquires the released
  advisory lock and completes migration. A dependency on retired `order_id`
  fails under `RESTRICT` without mutating the v5 target.
- The full shared release subset under PostgreSQL passed with 1509 passed and
  10 platform/backend skips in 62.62 seconds. After the final startup repair,
  the final full extension suite passed with 309 passed and 7 documented
  opt-in skips in 3.36 seconds, including the topology expansion and
  advisory-release recovery probes. Focused Ruff, PostgreSQL mypy,
  contract-manifest, doc-path,
  DOM-15, plan-context, and diff checks passed. Task 8 will record the final
  all-suite rerun and the PostgreSQL slice commit SHA.

### 2026-08-27 Task 5 Redis/Valkey bounded newest slice

- Bounded pending selection uses `ZREVRANGEBYLEX` for newest while default and
  every generator batch remain ascending. Claim and move Lua scripts carry the
  normalized direction, return selected rows in that direction, and resume a
  descending bounded scan with an exclusive upper cursor. Peek with
  `include_claimed=True` merges both state sets before applying the requested
  direction and slice.
- Real Valkey 7.2 tests use out-of-order exact IDs for read/peek/move one and
  many, strict bounds, ascending live traversal, claimed/pending merge, and
  concurrent newest claims. Claim and move each cross more than the Lua
  invocation's 256-candidate limit-one scan budget through 300 reserved higher
  IDs and still select the lower eligible ID on the resumed call.
- `uv run bin/pytest-redis --fast` passed the shared release subset with 1502
  passed and 17 backend/platform skips in 43.08 seconds. After correcting a
  test that had reused Redis's namespace-global IDs across queues, the full
  real extension suite passed with 288 passed and one opt-in diagnostic skip
  in 3.36 seconds. Focused Ruff and the [SB-SELECT-5] executable manifest gate
  passed. Task 8 will record the Redis slice commit SHA and final all-suite
  rerun.

### 2026-08-27 Task 6 CLI and agent-interface slice

- `--newest` is registered only on read, peek, and move and dispatches through
  the direct command layer as `order="newest"`; no CLI-specific retrieval path
  exists. Normal and rearranged placements, exact `-m`, strict bounds, and
  out-of-order exact IDs select the highest eligible public ID. Watch help has
  no reverse flag.
- `--newest --all` is rejected before target resolution. Text and JSON probes
  use a corrupt target to prove the target is unobserved; JSON asserts exactly
  `error="INVALID_ARGUMENT"`, the actionable message, `retryable=false`,
  stderr-only output, and exit 1. Parser grammar conservation automatically
  adds `--newest` to the registered-token set, so unescaped write/broadcast
  bodies fail with the existing `use --` guidance and escaped literals work.
- The focused CLI, main-dispatch, move, property, command-helper, and
  [SB-SELECT-5]/[SB-CLI-6] suites passed. Ruff passed on the touched code and
  tests. README Quick Start/options/escaping, the agent kernel, Python sidecar
  guidance, the canonical CLI mapping/evidence, and the breaking CHANGELOG
  entry were aligned. The Hypothesis/Atheris property harness now has explicit
  `--newest`, conflict, and escaped-literal corpus examples; Atheris itself is
  dependency-gated to Linux x86_64 and cannot execute on this macOS arm64 host,
  so its hosted run remains post-commit evidence rather than a local claim.

Interface-review baseline: `5d4f6df` plus the uncommitted Task 6 delta. Surface
kind: CLI.

| # | Result | Evidence |
|---|--------|----------|
| 1. Context is the scarcest resource | met | The flag changes selection only and preserves existing compact output (`simplebroker/cli.py:1628`, `tests/test_cli_contract_sb_cli.py:663`). |
| 2. Progressive disclosure | met | Command help teaches highest-ID meaning and the `--all` boundary; README adds bounded examples and option interactions (`simplebroker/cli.py:188`, `README.md:182`, `README.md:317`). |
| 3. Self-explanatory names; no lookup tables | met | `--newest` names the requested result directly and help names the public-ID key (`simplebroker/cli.py:190`). |
| 4. One identity per thing | met | The interface exposes only public message ID and cites the single cross-backend owner (`docs/specs/10-cli.md:343`, `README.md:334`). |
| 5. Derive what is derivable | met | The CLI derives the canonical order string from one boolean flag at dispatch (`simplebroker/cli.py:1639`, `simplebroker/cli.py:1718`). |
| 6. No hidden session setup | met | Each invocation carries command, queue operands, bounds, and `--newest`; no cursor or prior mode setup is introduced (`docs/specs/10-cli.md:343`). |
| 7. Teach, don't reject | met | The one true conflict names both valid recovery choices before target access (`simplebroker/cli.py:79`). |
| 8. Every message carries its action | met | The conflict diagnostic says which flag to remove for ascending-all versus newest-bounded behavior, in both text and exact JSON proof (`simplebroker/cli.py:84`, `tests/test_cli_contract_sb_cli.py:748`). |
| 9. Atomic writes with recovery on conflict | met; merge clause not applicable to a single-invocation CLI | Selection conflict fails before mutation; read/move retain their backend atomic operations (`tests/test_cli_contract_sb_cli.py:728`, `simplebroker/commands.py:1260`). |
| 10. Draw the trust boundary | met | Reverse live/all traversal is deliberately absent and the inventory proves watch does not advertise it (`docs/specs/10-cli.md:352`, `tests/test_cli_contract_sb_cli.py:762`). |
| 11. Wire format matches the mental model | met | One `--newest` flag maps to semantic public-ID order; storage keys and backend direction syntax stay internal (`simplebroker/cli.py:1639`). |

Findings:

| ID | Severity | Location | Finding | Disposition |
|----|----------|----------|---------|-------------|
| F1 | P2 | `simplebroker/cli.py:84` | The first conflict message named the invalid pair but gave no recovery action. | Resolved: it now names both valid flag-removal choices; exact text/JSON tests pin it. |

Ratified judgments (challenged, upheld): `--newest` remains sugar over the one
`order` path rather than a persistent LIFO mode; reverse `--all`/generator/watch
stays out pending a live-cursor contract; making `--newest` a registered token
correctly requires the existing explicit `--` boundary for literal message
data.

Verdict: no blocker.

Runbook feedback: no new reusable principle or probe class; the actionable
two-choice conflict is an instance of principles 7 and 8 already covered.

Later tasks append command, date, commit/artifact SHA, backend/service version,
observed result, and residual risk for each gate. Do not replace evidence with
“tests pass.”

### 2026-08-27 Task 7 artifact, rollback, and downstream evidence

- One coherent local wheel set was built from `50f5c858` into
  `/tmp/simplebroker-v8-artifacts.79yZIB`. The recorded manifest is:
  `simplebroker-8.0.0-py3-none-any.whl`
  `24056667381174b435219b27e3edb49cd56e6579c824bb4a13b88d386cbc992d`;
  `simplebroker_pg-4.0.0-py3-none-any.whl`
  `ce76c5ad2c41e8193b3eaa3bdb9e80073a48a588f22188b0b43c16ff99c5d0ce`;
  `simplebroker_redis-4.0.0-py3-none-any.whl`
  `a9ef68e1fbf6509eb67154d70de6d2f7f176362e7626047c446b9c1c51f45d5e`.
  An isolated no-project import resolved all three modules from uv's artifact
  environment, outside this source checkout. `./bin/packaging-smoke` then
  passed wheel/sdist construction, Python 3.11 clean installs, first-party
  backend discovery, and root package behavior.
- `tests/compatibility/test_weft_artifact_versions.py` owns the downstream
  child-process possession check. It skips only when all four expectation
  variables are absent, rejects partial configuration, asserts all three
  metadata versions and imports when active, and rejects module paths below
  this source root. Its byte-identical Weft copy had SHA-256
  `f2a5abb709090e43ef1168e0c4c933eed5995845983459769b37883eb5e22fd1`.
  The probe's three tests passed under Weft's own conftest boundary. The real
  PostgreSQL wrapper's inner `--with simplebroker-pg[dev]` process also passed
  all three, proving it retained core 8.0.0, PG 4.0.0, and Redis 4.0.0 rather
  than resolving a published older extension.
- Real previous-artifact probes used core 7.5.1 and PG 3.10.0. SQLite rejected
  fresh and upgraded v6 on first access with the newer-schema-version
  diagnostic, no missing-column text, and a byte-identical database after the
  upgraded-target attempt. PostgreSQL `BrokerTarget` rejected fresh and
  upgraded v6 with the same diagnostic; a before/after logical snapshot proved
  no metadata, message, alias, claim, queue, or sidecar mutation. The injected
  runner exception described in the Deviation Log is a release blocker under
  the current unconditional acceptance text.
- The SQLite rollback drill quiesced a real v5 target, copied the whole file,
  migrated it with the core wheel, and verified ascending broker rows plus
  unchanged sidecar DDL, data, index, and `sqlite_sequence` state. Restoring
  the v5 file let core 7.5.1 reopen it with version 5, the surrogate column,
  original order, and the same sidecar sequence. The PostgreSQL drill used a
  PostgreSQL 18 custom-format whole-schema dump (SHA-256
  `94ffd78743cb17449d33ea82dd5632436c6bb1f513a27a661f378655b720104a`),
  migrated under v8, verified sidecar DDL/data/index/sequence state, dropped
  only the disposable test schema, restored the dump, and reopened it with the
  old package pair. Already-admitted v7 handles across each migration failed
  respectively on `id` and `order_id`, which is the intended proof that
  quiescence is mandatory rather than a mixed-version compatibility claim.
- A disposable Weft worktree at the recorded Task 0 SHA
  `33e1ab767046e6a7e22904d5198840b174552798` resolved all three absolute wheel
  paths. The ordinary artifact suite finished with 4305 passed, 5 skipped, and
  the two pipeline exact-ID order failures recorded in the Deviation Log.
  Both exact tests pass when the same worktree overlays core 7.5.1. The full
  PostgreSQL wrapper retained the intended artifacts and stopped under its
  `-x` policy on the same pipeline-order assertion; no separate PostgreSQL
  compatibility failure preceded it.
- No package, tag, or repository publication was attempted. The plan's second
  disposable worktree against exact published versions remains unavailable
  until the owner explicitly authorizes and completes the three serial
  publications. Task 7 and the plan remain active because the two deviations
  above must be resolved before publication.

## Independent Plan Review

The 2026-08-27 independent review found ten material issues. All were
accepted into the plan before re-review:

| Finding | Disposition |
|---------|-------------|
| CLI JSON text named a nonexistent `code` field. | Corrected to [SB-CLI-4]'s `error="INVALID_ARGUMENT"` and added an exact object/stream/exit firing test. |
| `bin/release.py all` did not enforce claimed extension-first publication. | Forbid `all`; use three waited single-target releases and document the temporarily non-resolvable extension interval. |
| PostgreSQL validation was widened into an unnecessary full catalog validator. | Retain existing magic/version, required-table, and typed-metadata anchors; leave `ts` indexes/uniqueness with setup and migration. The later single-layout amendment supersedes the original both-layout proof. |
| Strategy A was paired with a committed failing test slice. | Split green spec promotion from uncommitted red probes; land tests, mappings, and docs only with green owner slices. |
| Dual-layout fixture architecture remained open. | The later single-layout amendment removes the dual-layout behavior matrix. Immutable shared v5 DDL helpers now own transition and rollback proof. |
| README proposal named the wrong locus and used undefined Python state. | Use existing Quick Start, Command Options, and The SimpleBroker API loci; provide self-contained examples and real canonical links. |
| Weft gates were not rerunnable or artifact-pinned. | Add exact baseline inventory/suite commands, source-path exclusion, local-wheel run, disposable published-version PG run, and recorded module/version evidence. |
| A locked decision still assigned `ts` constraints/indexes to PostgreSQL admission. | Corrected the decision so admission retains existing anchors and setup/migration owns semantic `ts` repair/proof. |
| Weft PG proof first ran after immutable publication and did not assert exact versions. | Added a pre-publication three-wheel PG run and exact pre/post-publication child-process version/path assertions. |
| Collecting an external probe would load SimpleBroker's test conftest into Weft. | Copy the byte-identical probe into each disposable Weft `tests` tree, record equality/hash, and collect only under Weft's conftest boundary. |

Final focused re-review passed on 2026-08-27 with no material blocker. It
confirmed admission-before-column-use, the narrowed PostgreSQL validation
scope, exclusive descending Redis cursors, generator exclusion, operational
rollback, serial release commands, pre-publication downstream possession, and
the absence of chronology or persistent-LIFO claims.

### 2026-08-27 single-layout amendment (owner-directed)

After that re-review, the owner directed a more aggressive compatibility
posture on the recorded premise of a near-zero external installed base: the
v5-to-v6 migration now rebuilds legacy tables to the canonical surrogate-free
layout instead of retaining an ignored surrogate. This eliminates the
dual-layout fixture matrix, the layout-tolerance rules in validation and
maintenance paths, and PostgreSQL's perpetual `order_id` allocation.

Sections amended: Goal and Boundary; Decisions 7–10; Alternatives Considered;
the [SB-ID-1] and [SB-API-7/11] spec deltas; README and CHANGELOG proposals;
Storage Design; Schema-Version Compatibility Gate; Invariants and Hidden
Couplings; Context and Key Files; Storage Test Matrix; Executable Behavior
Matrix; Tasks 1, 3, 4, and 7; Rollout and Rollback; Verification; Acceptance
Criteria; and Out of Scope.

The focused independent re-review of this amendment completed on 2026-08-27.
Its material findings and dispositions are:

| Finding | Disposition |
|---------|-------------|
| PhaseLock does not cover direct/environment PostgreSQL targets or two configs naming one schema. | Add a transaction-scoped database advisory lock keyed by database/schema identity, followed by a cache-bypassing live version read on the same connection. Keep PhaseLock as the project-scoped outer lock. Add all contention topologies and failure-release tests. |
| A v7 handle admitted before migration will not recheck the version and can fail after the surrogate disappears. | Define only cold old-client admission, require a quiesced downtime cutover and rollback, forbid mixed live v7/v8 operation, and add an already-open-handle boundary probe. |
| The migration contract did not explicitly protect caller-owned sidecars. | Add [SB-API-7], README/Python-guide teaching, sidecar noninterference invariants, success/failure snapshots, whole-target backup drills, and `RESTRICT`/no-`CASCADE` rules. Reserved-object modifications remain unsupported. |
| The proposed CHANGELOG and plan index still described retained surrogates or a doubled layout matrix. | Replace both with the single supported surrogate-free layout, dedicated v5 transition proof, advisory serialization, sidecar preservation, and quiesced cutover. |
| Absolute physical-layout wording ignored tolerated unsupported additions and SQLite's persistent `sqlite_sequence` table. | Scope equivalence to the supported SimpleBroker-owned shape, modulo engine internals; require no `messages` sequence row while preserving caller sequence rows. Tolerated unknown additions do not become supported. |

The prior review's findings on dual-layout fixtures and both-layout validation
are superseded by this amendment; its other findings stand. No other material
gap was found in newest selection, generator exclusion, Redis direction,
`RETURNING` normalization, release sequencing, or cold old-client admission.
Task 1 remains gated on the plan and documentation checks below.
