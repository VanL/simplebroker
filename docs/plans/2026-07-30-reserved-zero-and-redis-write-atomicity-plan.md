# Reserved Message ID Zero and Redis Generated-Write Atomicity Plan

Status: completed — implementation, documentation, released-backend
verification, independent review, and the targeted commit are complete. The
full root gate remains blocked by unrelated concurrent Ruff
suppression-registry line shifts recorded below.

Class: 5 — this changes the active `[SB-ID-*]` public contract and corrects a
Redis concurrency boundary. Hardening is mandatory because the package is
published, exact-ID acceptance narrows, and Redis allocation/publication
ordering changes.

Plan type: implementation with spec revision.

Promotion strategy: A — edit the active identity spec first, without adding
implementation-link claims that do not yet exist; add code/test mappings and
reciprocal implementation links in the later implementation slices.

Program-theory impact: no material theory revision. `[THEORY-3]` already gives
SimpleBroker ownership of message identity and queue-operation semantics, and
`[THEORY-4]` already prefers explicit safety over implied recovery. This plan
narrows an exact product contract and changes one backend realization; it does
not add a new product concept.

## Goal

Reserve message ID `0` as the lower-bound/checkpoint origin for newly created
messages while preserving recovery access to legacy zero-ID rows, and make an
ordinary Redis `write()` publish its generated ID high-water and message row in
one atomic Lua operation. The result should make the watcher's existing
zero-sentinel behavior observationally harmless for conforming positive-only
new data and close the Redis interleaving in which a later ordinary write
becomes visible before an earlier generated write.

This plan does **not** make `after_timestamp` a universally lossless queue
cursor. ID-preserving moves, live exact-ID insertion, legacy zero-ID rows, and
Redis patterned broadcast can still introduce an ID behind a checkpoint. The
Phase 2B selection contract must retain that distinction.

## Source Documents

Source specs and conceptual owners:

- `docs/program-theory.md` `[THEORY-2]`, `[THEORY-3]`, `[THEORY-4]`
- `docs/specs/13-message-identity.md` `[SB-ID-1]`,
  `[SB-ID-2]`, `[SB-ID-3]`, `[SB-ID-4]`
- `docs/specs/product-section-registry.md`, message-identity row
- `docs/specs/12-broadcast.md` `[SB-BCAST-3]`, `[SB-BCAST-4]`
  for the patterned-broadcast exclusion

Related active plan:

- `docs/plans/2026-07-30-product-documentation-cutover-plan.md`, Phase 2B
  readiness stop

Process requirements:

- `docs/specs/01-development-documentation-operating-model.md` `[DOM-5]`,
  `[DOM-10]`, `[DOM-11]`, `[DOM-15]`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/agent-context/runbooks/maintaining-traceability.md`

## Spec Baseline

- `84fcc5706834fd85115ab404c1beae47ab9f4e08` —
  `docs/specs/13-message-identity.md`,
  `docs/specs/12-broadcast.md`, and
  `docs/specs/product-section-registry.md` at plan authoring time.
- Promotion baseline: `84fcc5706834fd85115ab404c1beae47ab9f4e08` plus the
  uncommitted spec-promotion worktree diff in
  `docs/specs/13-message-identity.md`,
  `docs/specs/product-section-registry.md`, `README.md`,
  `docs/agent-kernel.md`, and
  `tests/test_message_identity_contract_sb_id.py`.

## Current Structure and Key Files

### Exact-ID admission

- `simplebroker/_message_id.py::normalize_message_id` is shared by exact
  selectors and insertion. It accepts integer `0` and the 19-digit zero string.
  It must continue doing so because rejecting zero here would also prevent
  inspection and cleanup of legacy rows.
- `simplebroker/_message_insert.py::normalize_insert_records` is the shared
  pre-mutation admission point for `insert_messages(...)`. SQLite/PostgreSQL
  reach it through `simplebroker/db.py::BrokerCore.insert_messages`; Redis
  reaches it through
  `extensions/simplebroker_redis/simplebroker_redis/core.py::RedisBrokerCore.insert_messages`;
  `Queue.insert_messages` delegates to those handles.
- `simplebroker/_dump.py::load_lines` normalizes dump IDs and then calls
  `insert_messages` in batches. A legacy dump containing ID `0` will therefore
  stop reloading unchanged once the guard lands. That is an intentional,
  user-visible compatibility cost and needs an actionable line-numbered error.
- Storage encoders and schemas currently represent zero. Do not add a SQL
  constraint or make Redis `encode_id(0)` fail: those lower layers are needed
  to inspect, move, delete, dump, and clean up pre-existing zero-ID rows.

### Redis ordinary write

- `RedisBrokerCore._write_message` currently calls `generate_timestamp()`.
  `TimestampGenerator.generate()` persists `meta.last_ts` through
  `ADVANCE_LAST_TS`; `_write_message` then separately evaluates
  `scripts.WRITE_MESSAGE` to insert the body and indexes. Redis serializes
  each Lua call, but another client may interleave between the two calls.
- `TimestampGenerator._reserve_candidates` already reserves process-local
  candidates without persisting them. Redis atomic broadcast combines this
  helper with a server-side stale-high-water fence. Reuse that pattern; do not
  introduce a second allocator.
- `scripts.WRITE_MESSAGE` currently owns namespace/duplicate preflight and the
  body, all-ID, pending, and queue-registry mutations. Extend this existing
  script rather than adding a parallel write script.
- `RedisBrokerCore._resync_timestamp_generator` currently repairs state with
  an unconditional `write_last_ts` after reading high-water/max ID. Another
  writer can advance between those steps, allowing the repair to overwrite
  high-water backward. The atomic write fence is incomplete unless this repair
  becomes monotone.
- `_publish(queue)` remains a post-commit wakeup hint, and maintenance
  accounting remains after the write path. Neither belongs in the new Lua
  atomic boundary.

### Evidence and downstream use

- `tests/test_write_visibility.py` proves the intended allocation-plus-insert
  visibility shape for SQLite only.
- `extensions/simplebroker_redis/tests/test_redis_atomicity.py` and
  `test_redis_state_machine_transitions.py` contain the real-Valkey atomic Lua
  and transition-table patterns to reuse.
- `tests/backend_benchmark.py` already has a `write_single` workload, but its
  per-operation CLI process launch masks a one-round-trip Redis change. Use it
  only as a broad regression signal; record a separate non-gating direct-core
  sample.
- Weft uses generated positive IDs for spawn exact insertion and does not
  insert zero. Its dump/load path delegates to SimpleBroker and therefore
  inherits the legacy-zero reload incompatibility. Weft also uses
  `after_timestamp` broadly, so the ordinary-write versus late-older-ID
  distinction remains relevant even though its declared primary optional
  backend is PostgreSQL.

### Required-reading comprehension gate

Before editing, the implementer must answer:

1. Why must the zero guard live in `normalize_insert_records` rather than
   `normalize_message_id`, a SQL schema, or `encode_id`?
2. Which Redis state is changed by the first Lua call and the second Lua call
   today, and where can another client interleave?
3. Why must every anticipated Lua result be detected before the script's first
   mutation?
4. Why does atomic ordinary `write()` not make a monotone checkpoint complete
   for queues that receive moves, exact insertion, or patterned broadcast?

An incorrect answer blocks implementation until the relevant code and contract
are reread.

## Decisions

1. **Reserve zero at exact-insert admission, not at decoding.** Newly created
   exact-ID rows require `message_id > 0`; ordinary generated writes already
   produce positive IDs. Exact selectors continue accepting zero so legacy
   rows remain inspectable and removable.
2. **Reject both normalized zero forms.** Integer `0` and the exact 19-digit
   zero string normalize to the same reserved value and raise `ValueError`
   before any batch mutation or high-water change.
3. **No legacy-load escape hatch.** A dump containing a zero-ID message fails
   before that batch is applied. A private bypass would let any crafted dump
   recreate the state being prohibited and would split insertion semantics.
   Documentation must tell operators to inspect the source target and
   intentionally re-ID that row before restore.
4. **Use one fenced Lua operation for ordinary Redis `write()`.** Reserve one
   local candidate, reject a stale fence without mutation, then advance
   persisted high-water and insert the row in the same script invocation.
5. **Make resynchronization monotone.** Replace unconditional Redis high-water
   repair with compare-and-advance followed by refresh.
6. **Keep wakeups and maintenance outside the data commit.** Preserve the
   existing commit → Pub/Sub hint → best-effort maintenance order.
7. **Limit the visibility promise to ordinary `write()`.** Patterned Redis
   broadcast remains a Python snapshot plus separately persisted generated
   candidates and exact insertion. Moves and exact insertion remain explicit
   older-ID admission paths.
8. **Do not change the watcher sentinel in this precursor.** The watcher still
   collapses an explicit lower bound of `0` to its no-bound filter state. Once
   new zero-ID insertion is prohibited, that is observationally equivalent for
   conforming new data. A native legacy zero row remains the recovery edge and
   is not made a conforming new message by this equivalence.

## Invariants and Constraints

### Identity and compatibility

- A successful ordinary generated `write()` returns a strictly positive ID.
- `after_timestamp=0` remains a valid lower-bound origin.
- Generic exact selectors continue accepting `0` and the exact 19-digit zero
  string for legacy recovery.
- New `insert_messages` calls cannot create ID `0` on SQLite, PostgreSQL, or
  Redis; one zero in a mixed batch rejects the entire batch before mutation.
- Existing zero-ID storage remains decodable. No schema migration, key rewrite,
  or automatic re-ID occurs.
- A legacy dump containing ID `0` cannot reload unchanged after this contract
  change. The error must identify the offending dump line and explain that
  zero is reserved.
- Positive exact-ID insertion behavior, normalization-before-deduplication,
  high-water advancement, move preservation, and far-future-ID warnings do not
  change.

### Redis atomicity and concurrency

- For one successful ordinary Redis `write()`, persisted high-water advancement
  and body/index insertion have one server-side visibility point.
- A stale candidate, duplicate candidate, or missing namespace causes no script
  mutation.
- Redis message IDs must not be converted to Lua numbers for ordering. Compare
  current high-water and candidate only as their 19-digit padded encodings,
  while persisting the raw decimal candidate in `meta.last_ts`.
- A stale-fence retry never inserts the rejected candidate. It refreshes
  persisted state, reserves a new candidate, and remains bounded by the
  existing three-attempt conflict budget. Candidate-exists (`-1`) and stale
  fence (`-6`) consume the same shared budget; any third conflict terminates.
- Timestamp repair never moves persisted `last_ts` backward.
- Standalone `generate_timestamp()` remains public and continues to persist a
  generated ID without a row.
- A Redis transport error after `EVAL` remains outcome-ambiguous. Do not
  automatically retry it and risk a duplicate; preserve the current translated
  error posture.
- Pub/Sub failure or process death after commit may lose a wakeup hint, not the
  row. Polling remains the liveness backstop. Do not move Pub/Sub into Lua.
- Maintenance remains best-effort and cannot roll back or downgrade the
  committed row.

### Scope and implementation discipline

- Reuse `_reserve_candidates`, `encode_id`, `ADVANCE_LAST_TS`, the broadcast
  stale-fence protocol, and existing Redis result translation.
- Keep write-script result decoding beside `_write_message`; do not add a
  generic Lua RPC framework.
- Add no dependency and no storage-format or Redis-key migration.
- Do not change move identity, patterned-broadcast selection/atomicity,
  exact-plus-range precedence, CLI timestamp grammar, or watcher lifecycle in
  this plan.
- Any proposed generalized “generated messages are checkpoint ordered” wording
  is a stop condition because patterned broadcast remains excluded.

## Rollout, Rollback, and One-Way Doors

There is no data migration and no one-way storage operation. Existing positive
rows and high-water values remain readable by old and new packages.

Rollout order:

1. Promote the reviewed `[SB-ID-*]` delta and derived-view pointers.
2. Land the zero preflight with shared released-backend tests.
3. Land the Redis atomic write and monotone-resync slice with real-Valkey
   adversarial tests.
4. Reconcile implementation docs, changelog, downstream evidence, and
   traceability; only then call the precursor complete.

The zero guard is a public compatibility narrowing. Roll it out with a release
note that calls out legacy zero-ID dumps. Operators needing to move such data
must rewrite the row to a positive compatible ID before loading it into the new
target.

Rollback is a coordinated code/spec/package revert. The Redis revert requires
no data rollback, but it explicitly reopens the known two-EVAL visibility
window and the backward-resync race. Reverting only the zero guard while
leaving the positive-only contract, or reverting only the spec while leaving
the guard, is forbidden.

Post-deploy/canary signals:

- no increase in Redis timestamp-conflict terminal `RuntimeError`s;
- no persisted-high-water regressions in the adversarial probe;
- no generated ordinary row observed behind a checkpoint advanced through a
  later ordinary write;
- Redis write error rate stays flat;
- direct-core p50/p95 write latency is neutral or lower; the expected change is
  approximately one Redis round trip saved per ordinary write;
- zero-ID insertion failures are explicit `ValueError`s, not backend-specific
  integrity failures.

Stop rollout and revert if stale-fence retries loop, high-water regresses,
ordinary writes disappear or duplicate, or real-Valkey latency/errors regress
materially. A noisy microbenchmark alone is not a rollback trigger.

## Proposed Spec Delta

Promotion strategy: A — apply these edits to
`docs/specs/13-message-identity.md` before implementation. Do not add
new implementation-link claims until their code and firing tests land.

### `[SB-ID-1]` — add after the signed-range sentence

> Broker-generated message IDs are strictly positive. ID `0` is reserved as
> the lower-bound and empty-high-water origin and is not valid for a newly
> inserted message. Exact selectors and storage decoders continue to accept
> zero so a broker target created by an older release can be inspected and
> cleaned up; this recovery compatibility does not permit new zero-ID
> insertion.

### `[SB-ID-2]` — replace the final paragraph

> For an ordinary generated `write()`, allocation/high-water advancement and
> insertion of the message row commit as one backend-atomic outcome. A stale
> candidate must not advance persisted high-water or insert a row. This
> visibility rule is limited to ordinary `write()`; standalone
> `generate_timestamp()` intentionally advances high-water without a row, and
> ID-preserving moves, exact-ID insertion, and backend operations explicitly
> excluded by their owning contracts may admit an older ID later. The rule
> therefore does not make `after_timestamp` a universal durable cursor.

### `[SB-ID-4]` — add after the accepted-form list

> The accepted forms above apply to exact selectors so legacy ID `0` remains
> addressable. New exact-ID insertion is narrower:
> `insert_messages(...)` requires each normalized ID to be greater than zero.
> Integer `0` and an exact 19-digit string that normalizes to zero raise
> `ValueError`. The complete input is still snapshotted and validated before
> mutation, so one reserved zero in a batch inserts no rows and does not change
> persisted high-water.

### `[SB-ID-2]` and `[SB-ID-4]` verification rows

Add only after the firing tests land:

> `[SB-ID-2]`: bind the real-Valkey stale-fence/no-mutation, two-writer
> visibility, monotone-resync, and `SM-REDIS-WRITE` transition-table tests;
> also bind the shared SQLite/PostgreSQL transaction-ordering case in
> `tests/test_write_visibility.py` as the backend-general ordinary-write proof.
> PostgreSQL and SQLite share the `BrokerCore` transaction boundary exercised
> there; Redis supplies the separate real-service proof.

> `[SB-ID-4]`: bind shared integer-zero, string-zero, mixed-batch
> preflight/no-mutation, Queue delegation, legacy selector compatibility, and
> dump line-error tests.

### Derived views and ownership wiring

- Keep the registry concern and clause range unchanged; strengthen its Gate
  cell to name reserved-zero and Redis generated-write atomicity evidence.
- Add concise README and agent-kernel restatements: generated/newly inserted
  IDs are positive, `0` is the checkpoint origin, selectors retain legacy-zero
  recovery, and ordinary Redis `write()` has one visibility point.
- Do not move strict selection/checkpoint ownership out of its current
  `readme-only` registry row in this plan.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Implementation Slices

### 1. Spec-promotion slice

Outcome: the active identity contract and derived views state the reviewed
behavior before code cites it.

Files:

- `docs/specs/13-message-identity.md`
- `docs/specs/product-section-registry.md`
- `README.md`
- `docs/agent-kernel.md`
- `tests/test_message_identity_contract_sb_id.py`
- this plan, promotion-baseline field and execution log

Actions:

- Apply the exact `[SB-ID-1]`, `[SB-ID-2]`, and `[SB-ID-4]` text above.
- Update structural assertions for the new obligations, but do not claim
  behavioral firing tests exist until their implementation slices land.
- Record the promotion baseline identifier.
- Independently review the promoted text against the exact plan delta.

Verification:

```bash
python3 bin/check-dom15-fixtures
python3 bin/check-doc-paths --root .
uv run pytest -q -n 0 tests/test_message_identity_contract_sb_id.py
git diff --check
```

Stop if the text rejects zero selectors, implies a schema migration, or
promises checkpoint completeness for moves/exact insertion/patterned
broadcast. Under promotion strategy A, the normative behavior may be promoted
first, but no firing-test bind, implementation backlink, implemented-language
claim, release note, or closeout claim may be added until code matches it.

Done signal: promoted text matches the reviewed delta, structural routing
passes, and the promotion identifier is recorded.

### 2. Reserved-zero admission slice

Outcome: all first-party exact-insertion surfaces reject normalized zero before
mutation while legacy selectors remain compatible.

Files:

- `simplebroker/_message_insert.py`
- `simplebroker/_dump.py`
- `tests/test_insert_messages.py`
- `tests/test_dump_load.py`
- `tests/test_message_id_validation.py`
- `tests/test_message_by_timestamp.py` or the closest existing legacy-selector
  integration file
- `tests/test_message_identity_contract_sb_id.py`

Actions:

- Add one shared reserved-zero check immediately after exact-ID normalization in
  `normalize_insert_records`.
- Use one actionable `ValueError`, for example:
  `message_id 0 is reserved; use a positive compatible SimpleBroker message ID`.
- Preserve full-input preflight and normalized duplicate handling.
- Give `load_lines` line-numbered context for zero before a batch flush. Do not
  add a hidden `allow_zero` bypass.
- Add a legacy fixture through backend-native storage only where necessary to
  prove existing zero rows remain addressable; production code must not create
  the fixture.

Required red-green evidence:

- integer zero and exact-string zero each fail before state changes;
- a mixed valid/zero batch inserts nothing and leaves high-water unchanged;
- `Queue.insert_messages` reaches the same guard;
- generated IDs are positive on a fresh target;
- exact read/move/delete of a native legacy zero row remains possible;
- `after_timestamp=0` still selects generated positive rows;
- dump load reports the zero line and performs no writes from that unflushed
  batch.

Run the shared tests on SQLite, PostgreSQL, and Redis. Do not mock broker
storage or `insert_messages`; only native legacy-fixture setup may bypass the
public path.

Stop if the clean implementation requires changing
`normalize_message_id`, schemas, Redis encoding, or unrelated dump format.

Done signal: the shared tests fail on the promotion baseline, pass after the
guard, and every released backend exhibits the same admission result.

### 3. Redis atomic ordinary-write slice

Outcome: ordinary Redis allocation/high-water advancement and row insertion
share one Lua visibility point, and resynchronization cannot regress high-water.

Files:

- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/tests/test_redis_atomicity.py`
- `extensions/simplebroker_redis/tests/test_redis_integration.py`
- `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py`
- `tests/state_machine_manifest.py`
- `tests/test_state_machine_policy.py`
- `docs/implementation/07-complexity-and-state-machine-map.md`
- `tests/test_write_visibility.py`
- `tests/test_message_identity_contract_sb_id.py`

Implementation shape:

1. `_write_message` reserves one candidate through
   `_timestamp_gen._reserve_candidates(1)` without persisting it.
2. The revised `WRITE_MESSAGE` receives queue, raw candidate, encoded
   candidate, and body.
3. Before its first mutation, Lua checks namespace, stale high-water, and
   candidate nonexistence. It pads stored raw `last_ts` to 19 decimal digits
   and compares that string with the already encoded 19-digit candidate. It
   never calls `tonumber` on either ID and never compares raw decimal strings.
4. Success sets raw `last_ts`, body, all-ID index, pending index, and queue
   registry in one script.
5. Use the existing result vocabulary where possible:
   `1` success, `-1` candidate already exists, `-2` namespace missing, and
   `-6` stale fence, matching broadcast.
6. Stale fence refreshes persisted high-water and retries with a new candidate.
   Candidate-exists performs the existing conflict/resync posture. Both result
   codes consume one shared conflict-attempt counter capped at three total
   conflicts, matching the broadcast protocol.
7. A Redis transport error is translated and returned without blind retry.
8. `_resync_timestamp_generator` uses monotone `advance_last_ts` and refreshes;
   it never calls unconditional `write_last_ts` for conflict repair.
9. On success, preserve `_publish(queue)` and maintenance ordering outside Lua.

Required red-green real-Valkey evidence:

- The baseline red probe pauses writer A after the current
  `generate_timestamp()` has persisted/advanced its candidate but before
  `WRITE_MESSAGE` inserts the row. Writer B is deterministically forced above
  A's candidate using the existing broadcast force-advance pattern, commits,
  and lets a reader advance its checkpoint. The baseline then exposes A behind
  that checkpoint.
- The fixed green probe pauses writer A at the new production seam after local
  `_reserve_candidates(1)` but before the fenced `WRITE_MESSAGE` evaluation.
  Writer B is again deterministically forced above A's unpersisted candidate,
  commits, and advances the reader checkpoint. A's stale candidate causes zero
  mutation; A refreshes, retries above B, and remains visible after that
  checkpoint.
- Both probes use explicit events/barriers and controlled candidates. Sleeps,
  wall-clock luck, process scheduling, and probabilistic retries are forbidden
  as ordering evidence.
- Direct script invocation with a stale candidate leaves metadata, bodies,
  all-ID index, pending index, and queue registry unchanged.
- A concurrent resync cannot overwrite a later high-water value backward.
- One successful steady-state ordinary write performs one data `EVAL`, not the
  former advance-plus-write pair.
- Namespace missing, repeated stale fences, candidate-exists, unexpected
  result, reservation error, and transport error each take their named path.
- Refactor `tests/test_write_visibility.py` so its current SQLite
  multiprocessing stress case remains individually `sqlite_only`, while its
  pass-through transaction recorder becomes a `shared` SQLite/PostgreSQL test.
  The shared test must wrap each harness's real runner, exercise the real
  `BrokerCore.write()`, and observe begin → high-water advance → row insert →
  commit without substituting storage behavior. It must run and pass, not
  skip, in `bin/pytest-pg`. Redis remains excluded from that SQL-specific case
  and is proven by the real-Valkey tests above. Because the repository's
  `shared` marker also selects Redis, the SQL-ordering case must use one
  explicit, named Redis-only unsupported-backend skip; record that exact test
  name and do not count the skip as Redis evidence.

Add `SM-REDIS-WRITE` to the state-machine map, transition suite, and
`tests/state_machine_manifest.py`; run `tests/test_state_machine_policy.py`
with all optional components installed to prove the complete inventory and
entry wiring. No policy-text change is required merely to add a conforming
entry. Mocked Lua responses may test Python dispatch branches; the atomicity,
visibility, and monotonicity claims must use a real Valkey/Redis service.

Stop if the implementation adds a new allocator, retries ambiguous transport
failures, moves Pub/Sub into Lua, or widens the contract to patterned
broadcast.

Done signal: the old path deterministically fails the barrier probe, the new
path passes all real-Valkey transitions, and focused independent review finds
no unowned result code or mutation-before-validation path.

### 4. Documentation, performance observation, and traceability reconciliation

Outcome: the public contract, implementation rationale, release note, plan
state, and evidence graph agree.

Files:

- `docs/implementation/08-message-identity-and-write-visibility.md` (new)
- `docs/implementation/00-implementation-index.md`
- `docs/implementation/02-repository-map.md`
- `docs/implementation/05-product-invariant-inventory.md`
- `docs/implementation/07-complexity-and-state-machine-map.md`
- `CHANGELOG.md`
- `docs/plans/2026-07-30-product-documentation-cutover-plan.md`
- `docs/specs/13-message-identity.md`
- `docs/specs/product-section-registry.md`
- this plan

Actions:

- Explain why zero is reserved at insertion but decoded for recovery, why the
  Redis write fence is server-side, why resync is monotone, and why move/exact
  insertion/patterned broadcast remain outside the ordinary-write guarantee.
- Add a current changelog correction; do not silently rewrite the historical
  4.10.0 record that said Redis was already atomic.
- Record a non-gating before/after direct-core Redis sample with warmup, at
  least five runs, and median/p95 over at least 1,000 sequential writes. Do not
  add a flaky performance threshold. The deterministic performance gate is the
  one-EVAL command count.
- Recheck Weft against its local source. Record that normal spawn exact IDs are
  generated positive and that legacy-zero dump load is the compatibility edge.
- Update the Phase 2B readiness record: this precursor restores ordinary
  `write()` visibility but does not resolve moves, exact insertion, patterned
  broadcast, granular exact-plus-range behavior, or CLI grammar ownership.
- Complete row-local spec firing binds, implementation backlinks, review log,
  execution log, and deviation dispositions.

Stop if documentation says all Redis generated-message paths are ordered, if
the benchmark is treated as a correctness proof, or if a pending deviation
remains.

Done signal: traceability and document-path gates pass, the implementation doc
explains the realized boundary, and the active cutover plan names the remaining
Phase 2B decisions without the resolved ordinary-write discrepancy.

## Testing Plan

### Contract matrix

| Obligation | Real proof | Must not be mocked |
|------------|------------|--------------------|
| New exact insert rejects normalized zero atomically | shared `insert_messages` tests on SQLite/PostgreSQL/Redis | public broker/Queue path and backend state |
| Legacy zero remains recoverable | backend-native fixture followed by public exact read/move/delete | selector and move paths |
| Legacy dump zero fails clearly | real `load_lines` and real broker target | loader-to-insert handoff |
| Generated ID is positive | real generator on fresh target, with controlled clock edge where useful | generator/high-water interaction |
| Redis write has one visibility point | deterministic two-core barrier with real Valkey and checkpoint reader | Lua, Redis storage, or reader |
| Ordinary write is backend-atomic | non-skipping shared transaction-ordering case in `tests/test_write_visibility.py` on SQLite/PostgreSQL plus the real-Valkey proof | backend transaction/script boundary |
| Stale script is mutation-free | before/after real Redis key-state assertions | Lua execution |
| Resync is monotone | two-core race against real persisted high-water | backend plugin compare-and-advance |
| Retry/result branches are complete | `SM-REDIS-WRITE` transition table; limited response injection for dispatch only | success atomicity and persistence |
| Performance does not add a round trip | real-client command-count evidence plus non-gating timing sample | Redis client call boundary |

### Focused commands

Root/SQLite:

```bash
python3 bin/check-dom15-fixtures
python3 bin/check-doc-paths --root .
SIMPLEBROKER_REQUIRE_FULL_MANIFEST=1 \
  uv run pytest -q -n 0 tests/test_state_machine_policy.py
uv run pytest -q -n 0 \
  tests/test_message_identity_contract_sb_id.py \
  tests/test_message_id_validation.py \
  tests/test_insert_messages.py \
  tests/test_dump_load.py \
  tests/test_message_by_timestamp.py \
  tests/test_after_flag.py \
  tests/test_write_returns_id.py \
  tests/test_write_visibility.py
```

PostgreSQL:

```bash
uv run ./bin/pytest-pg -q -n 0 \
  tests/test_message_identity_contract_sb_id.py \
  tests/test_message_id_validation.py \
  tests/test_insert_messages.py \
  tests/test_dump_load.py \
  tests/test_message_by_timestamp.py \
  tests/test_after_flag.py \
  tests/test_write_returns_id.py \
  tests/test_write_visibility.py
```

Redis:

```bash
uv run ./bin/pytest-redis -q -n 0 \
  tests/test_message_identity_contract_sb_id.py \
  tests/test_message_id_validation.py \
  tests/test_insert_messages.py \
  tests/test_dump_load.py \
  tests/test_message_by_timestamp.py \
  tests/test_after_flag.py \
  tests/test_write_returns_id.py \
  tests/test_write_visibility.py \
  extensions/simplebroker_redis/tests/test_redis_atomicity.py \
  extensions/simplebroker_redis/tests/test_redis_integration.py \
  extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py
```

Static and final:

```bash
uv run ruff check simplebroker tests extensions/simplebroker_redis
uv run ruff format --check simplebroker tests extensions/simplebroker_redis
uv run pytest -q -n 0
uv run ./bin/pytest-pg -q -n 0
uv run ./bin/pytest-redis -q -n 0
git diff --check
```

If the extension harnesses provide one expected unsupported capability skip,
record it by exact test name. Any new skip, xfail, warning, or unbound spec
clause blocks completion.

### Adversarial acceptance floors

- Empty exact-insert input remains a no-op.
- Integer zero and all supported Unicode/exact-string representations that
  normalize to zero reach one rejection rule.
- Invalid zero after valid records proves preflight, not partial mutation.
- Fresh, existing-positive, and native-legacy-zero targets are distinct
  fixtures.
- Redis stale fence, candidate collision, namespace loss, transport error,
  repeated conflict, and concurrent resync all fire.
- The baseline concurrency proof pauses after persisted generation and before
  row insertion; the fixed proof pauses at the production seam between local
  reservation and Lua evaluation. Each forces B's committed ID above A's
  rejected candidate. Do not replace either proof with sleeps or probability.

## Independent Review Loop

Plan review must occur before spec promotion. Prefer a different model family
from the author. The reviewer reads:

- this entire plan, especially `## Proposed Spec Delta`;
- `[SB-ID-1]` through `[SB-ID-4]`;
- the active product-documentation cutover Phase 2B stop;
- `_message_id.py`, `_message_insert.py`, `_dump.py`;
- Redis `_write_message`, `_resync_timestamp_generator`, `WRITE_MESSAGE`,
  `ADVANCE_LAST_TS`, and the atomic broadcast fence;
- the closest shared and real-Valkey tests;
- the cited Weft insert/load paths.

Review stance:

> Could a zero-context engineer implement this confidently without breaking
> legacy recovery, widening checkpoint guarantees, or mocking away the Redis
> interleaving? Challenge the exact spec text, every Lua preflight/mutation
> boundary, the monotone-resync repair, the legacy dump incompatibility, and
> the claim that the scope is limited to ordinary `write()`. Identify
> performative gates or missing failure branches as aggressively as missing
> safeguards.

After each meaningful slice, run a fresh independent completed-slice review.
Every finding is reproduced, dispositioned in the append-only Review Log, and
followed by a focused recheck. A reviewer who cannot implement confidently is
a blocker.

## Out of Scope

- Changing move to allocate a new ID.
- Making `after_timestamp` a durable broker offset.
- Changing watcher state to distinguish explicit zero from no bound.
- Supporting new zero-ID inserts through dump/load, private flags, or backend
  bypasses.
- Rewriting or migrating existing zero-ID rows automatically.
- Making Redis patterned broadcast allocation/insertion checkpoint-ordered.
- Changing patternless/exact broadcast semantics.
- Deciding granular exact-plus-range precedence.
- Adding `[SB-CLI-5]` or promoting the Phase 2B selection contract.
- Moving Redis Pub/Sub notification into the write Lua script.
- Adding a dependency, new storage key, schema version, or generic Lua RPC
  framework.

## Stop-and-Re-evaluate Gates

Stop and amend/review the plan if:

- evidence shows generated ordinary IDs can naturally be zero;
- a first-party exact-insert path bypasses `normalize_insert_records`;
- preserving required dump/load compatibility needs a second insertion path;
- the revised Lua script must mutate before all expected failures are known;
- a transport-error retry is proposed despite ambiguous server outcome;
- Redis resync has another non-monotone writer outside the named path;
- patterned broadcast must be included to make a proposed public sentence true;
- Weft or another primary downstream intentionally inserts zero;
- a new key/schema/version/dependency is required;
- a test can pass while replacing Redis, the Lua script, or the checkpoint
  reader with a mock;
- implementation changes authority, blast radius, or invariants beyond this
  reviewed text.

## Fresh-Eyes Checklist

- Does the plan preserve zero decoding while prohibiting zero creation?
- Does it state the legacy dump incompatibility rather than hiding it?
- Are all Lua error checks before the first mutation?
- Is the stale-candidate retry deterministic and bounded?
- Can resync ever move high-water backward?
- Does every atomicity claim say “ordinary `write()`,” not all generated paths?
- Are Pub/Sub and maintenance correctly outside the commit boundary?
- Are real Valkey, all three released backends, and the downstream check named?
- Can rollback be performed without data conversion?
- Does every enumerable branch have a firing test and spec bind?

## Revision Log

Append-only after initial review. Approval attaches to the reviewed diff.

| Date | Revision | Reason | Review status |
|------|----------|--------|---------------|
| 2026-07-30 | Initial independent-review corrections: specified encoded-string Lua ordering and raw persistence, split the deterministic baseline/fixed concurrency seams, added backend-general write visibility and state-machine manifest evidence, unified the conflict budget, reconciled strategy A, and corrected result-code wording | Outside review found two implementation-ambiguity risks plus bounded traceability and wording gaps | focused recheck pending |
| 2026-07-30 | Post-recheck evidence correction: required the current SQLite-only visibility module to gain a non-skipping shared SQLite/PostgreSQL transaction-ordering case, and replaced an invented optional policy-spec path with the actual complete-manifest policy test | Maintainer verification found that the listed PostgreSQL command would skip the current module and that the conditional spec path did not exist | narrow evidence recheck pending |
| 2026-07-30 | Marker-selection clarification: named the expected Redis-only skip for the SQL transaction-ordering case and kept real Valkey as the Redis evidence owner | The `shared` marker selects Redis as well as SQLite/PostgreSQL; the exclusion needed an executable disposition rather than implication | passed |

## Review Log

| Date | Reviewer | Scope | Verdict | Disposition |
|------|----------|-------|---------|-------------|
| 2026-07-30 | Outside independent reviewer (Grok 4.5) | Full class-5 plan, exact spec delta, zero compatibility boundary, Redis Lua protocol, concurrency proof, state-machine traceability, and strategy-A sequencing | PASS with eight bounded corrections | Accepted `F1`–`F8`; also adopted the optional watcher-zero clarification. Encoded comparison and the two-seam deterministic test are required before authorization; focused recheck pending. |
| 2026-07-30 | Outside independent reviewer (Grok 4.5), focused recheck | Accepted `F1`–`F9` corrections and contradiction check | PASS | The reviewer marked all findings verified. Maintainer reproduction then found that the current visibility module is `sqlite_only`, so the proposed PostgreSQL invocation would skip, and that the optional manifest-policy spec path did not exist. Treated as an evidence-mapping correction requiring one narrow recheck. |
| 2026-07-30 | Outside independent reviewer (Grok 4.5), evidence-map recheck | Non-skipping SQLite/PostgreSQL transaction evidence and actual manifest-policy artifacts | PASS | Verified both corrections and found no contradiction; a final marker-selection check also passed after the expected Redis-only skip was made explicit. |
| 2026-07-30 | Independent implementation reviewer | Completed spec-promotion slice and derived-view gates | CONCERNS | Accepted the premature registry evidence claim and reverted it until behavioral tests land; strengthened README/kernel assertions. Promotion identifier was already recorded as the permitted exact diff base plus named uncommitted worktree state, and the execution row now records that identifier and the passing gates. Focused recheck pending. |
| 2026-07-30 | Independent implementation reviewer, focused rechecks | Corrected spec-promotion sequencing, promotion identifier, and README/kernel assertions | PASS | Verified the registry claim was deferred until tests landed, the uncommitted promotion identifier is valid, and both derived views bind positive IDs, legacy recovery, the Redis visibility point, and late-older-ID exclusions. |
| 2026-07-30 | Independent reserved-zero reviewer | Shared admission guard, dump diagnostic, legacy recovery fixture, and released-backend evidence | PASS on runtime; two traceability gaps | Added the new `[SB-ID-1]`/`[SB-ID-4]` row-local firing binds and strengthened the registry Gate after tests landed. |
| 2026-07-30 | Independent Redis implementation reviewer | Lua preflights and mutations, bounded conflict protocol, monotone resync, real-Valkey concurrency proof, and transition coverage | PASS | No findings. Transport ambiguity and post-commit Pub/Sub remain explicit exclusions. |
| 2026-07-30 | Final independent implementation reviewer | Full plan, runtime, contract/docs, and adversarial acceptance floor | CONCERN | Runtime and Redis protocol passed. Added Arabic-Indic, fullwidth, and mixed-script 19-digit zero cases after the reviewer found that supported Unicode zero representations lacked explicit firing evidence; focused recheck pending. |
| 2026-07-30 | Full Redis released-backend run | Shared Redis suite under ten-queue concurrent writer load | CONCERN | Reproduced local reserve/EVAL reordering that could exhaust the three-conflict budget. Added a fork-safe per-core reserve-through-EVAL lock and a focused 16-thread proof; the failing watcher case and focused proof now pass. Full-suite recheck pending. |
| 2026-07-30 | Final independent implementation reviewer, focused rechecks | Unicode-zero cases, same-core lock boundary, deterministic contention proof, fork reset, and `[SB-ID-2]` firing binds | PASS | Moved Pub/Sub outside the lock, replaced the probabilistic contention probe with an event-controlled reserve/EVAL proof, added a held-lock PID-reset proof, and bound the regression in both traceability owners. Reviewer accepted the bounded event observation and simulated PID change as owner-local evidence that does not mock Redis atomicity. |
| 2026-07-30 | Full Redis released-backend recheck | Complete shared suite plus complete Redis extension directory on real Valkey 7.2 | PASS | Shared suite reached 100% with expected skips; the harness's unscoped extension phase collected no tests under concurrent discovery changes, so `extensions/simplebroker_redis/tests` was then selected explicitly and reached 100% with its one opt-in diagnostic skip. |

## Execution Log

| Slice | Baseline | Result | Verification | Review |
|-------|----------|--------|--------------|--------|
| Plan and exact delta | `84fcc5706834fd85115ab404c1beae47ab9f4e08` | reviewed and authorized for spec promotion | `check-dom15-fixtures`, `check-doc-paths`, and `git diff --check` passed | outside review and all focused rechecks passed |
| Spec promotion | `84fcc5706834fd85115ab404c1beae47ab9f4e08` plus the named uncommitted promotion diff recorded under `Spec Baseline` | promoted exact `[SB-ID-1]`, `[SB-ID-2]`, and `[SB-ID-4]` text plus README/kernel derived views; behavioral claims were deferred until tests landed | `check-dom15-fixtures`, `check-doc-paths`, focused contract test (2 passed), and `git diff --check` passed | PASS after correcting one premature registry evidence claim and strengthening derived-view assertions |
| Reserved-zero admission | promotion baseline above | shared post-normalization guard; line-numbered dump rejection; positive generation and native legacy-zero recovery proofs | focused SQLite (7 passed), PostgreSQL (7 passed), and Redis (7 passed) released-backend runs | runtime PASS; row-local and registry traceability findings corrected |
| Redis atomic ordinary write | promotion baseline above; deterministic old-path probe run in detached `84fcc570` worktree | one fenced Lua write; shared three-conflict budget; monotone resync; fork-safe same-core reserve/EVAL serialization; real-Valkey green barrier, contention, and one-EVAL proofs; `SM-REDIS-WRITE` registered | old-path barrier failed as expected with A absent after B's checkpoint; fixed real-Valkey stale/visibility/resync/command-count tests passed; 12 transition rows passed; full-manifest policy 13 passed; SQL ordering passed on SQLite/PostgreSQL with named Redis skip; focused same-core and former watcher failure passed; full shared and extension Redis suites passed | initial and final focused independent reviews PASS |
| Traceability and closeout | promotion baseline above | spec, derived views, implementation map, invariant inventory, release note, and downstream recheck aligned in the targeted commit | DOM-15 fixtures, doc paths, contract/manifest policy, focused Ruff/format, and diff checks passed; full SQLite root reached 100% except the unrelated concurrent suppression-registry mismatch; full PostgreSQL and split full Redis suites passed | final independent review PASS; targeted commit complete |

### Non-gating Redis performance observation

Direct-core ordinary writes were sampled against local Valkey 7.2 with 100
warmup writes, five runs of 1,000 sequential writes, and per-operation
latencies. This is diagnostic evidence, not a threshold:

| Path | Run medians (ms) | Run p95s (ms) | Overall median / p95 (ms) |
|------|------------------|----------------|---------------------------|
| Baseline `84fcc570` two-EVAL write | 0.329, 0.295, 0.346, 0.413, 0.338 | 0.412, 0.430, 0.507, 0.540, 0.493 | 0.337 / 0.501 |
| Fenced one-EVAL candidate | 0.256, 0.248, 0.255, 0.251, 0.249 | 0.435, 0.377, 0.381, 0.390, 0.354 | 0.252 / 0.390 |

The deterministic performance gate remains
`test_steady_state_ordinary_write_uses_one_data_eval`.

### Downstream recheck

Local Weft source still generates implicit spawn TIDs through
`weft/core/spawn_requests.py::generate_spawn_request_timestamp` and passes
those positive IDs to the exact-insert path in
`_write_spawn_request_with_timestamp`. Its dump/load commands delegate to
SimpleBroker in `weft/commands/_dump_support.py` and
`weft/commands/_load_support.py`. Normal spawn submission is compatible; a
legacy dump containing ID `0` is the explicit restore incompatibility.
