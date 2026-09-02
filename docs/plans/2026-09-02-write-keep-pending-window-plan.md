# Write-Time Pending Window Plan

Status: completed
Class: 5. This adds a destructive public CLI and Python write option, changes
the backend protocol, and adds an atomic claim transition on every backend. The
public contract, CLI grammar, compatibility surface, contention behavior, and
one-way delivery-state change make the hardening checklist mandatory under
`[DOM-5]`, `[DOM-6]`, `[DOM-11]`, and `[DOM-15]`.
Plan type: implementation with spec revision

## Goal and Boundary

Add an optional write-time pending window:

```text
broker write --keep-newest N QUEUE [MESSAGE|-]
Queue.write(message, *, keep_newest=N)
```

`N` is an integer from 1 to 9999. A successful keep-write allocates and
inserts the new message, then claims every older pending message except the
`N` highest public message IDs, in one atomic backend operation: the write's
own transaction on SQL backends, one script on Redis/Valkey. The cost is
linear in the number of displaced rows (measured in Backend Design), and
concurrent operations wait through each backend's existing contention
policy. The inserted message counts toward `N`
and, because it has a newly generated ID above the durable high-water mark,
is always retained pending. When the queue has fewer than `N` pending
messages before the write, no existing message changes state. That case is an
ordinary successful write, not a no-match outcome.

The option is intended for a dedicated queue with a single producer. On a
shared or multi-purpose queue it claims other producers' pending messages.

This is an operation-scoped pending-delivery window, not a persistent queue
policy. Later ordinary writes, exact inserts, broadcasts, or moves may grow
the pending set above `N`. Claimed rows remain stored and count in queue
metadata until vacuum removes them, so `--keep-newest` is not a physical storage cap.

The feature is write-only. It does not apply to broadcast, move, exact-ID
insertion, load, watch, or any read/peek form. It does not add queue
configuration, backpressure, producer blocking, or an automatic retention
daemon.

## Product Fit

`docs/program-theory.md` [THEORY-1] and [THEORY-4] allow internal backend
complexity when it protects one small, explicit, matching CLI/Python queue
operation. The feature meets that bar only if the interface stays local to
write, the destructive effect is named in help and safety text, and all
backends expose one atomic meaning. It would stop fitting SimpleBroker if it
became durable queue policy, capacity management, time-based retention, or an
application-level definition of which work is obsolete.

Concrete pressure ([THEORY-4]): Weft's existing
write-then-delete-previous pattern in `weft/core/manager.py`
(`_prune_older_self_registry_entries` and "Failed to prune previous manager
registry heartbeat") shows the application-space version: two calls, two
failure outcomes, and per-producer bookkeeping. A proposed Weft redesign that
would have separated `weft.state.tid_mappings` into one queue per tid was
later rejected by Weft's completed
`2026-08-29-liveness-reaper-and-custody-split-plan.md`; no current Weft
production call site is therefore claimed for this option. On 2026-09-02 the
owner nevertheless authorized implementation as a general queue primitive,
judging the demonstrated application-space pattern and the explicit atomic
operation sufficient product pressure without making downstream adoption a
release gate. The option is explicitly invoked per call, keeps no stored
state, and is hard to do well in application space; the accepted cost is the
claimed-state expansion below. The feature is only safe on a dedicated
single-producer queue. On a shared registry queue such as Weft's
multi-manager services registry, or on an inbox that also carries commands,
it would claim other producers' live messages. Every teaching surface must
state this rule.

`[THEORY-2]` and `[THEORY-3]` place the use-level meaning in SimpleBroker and
the atomic realization in each backend. The application still decides whether
discarding older pending work is semantically safe. SimpleBroker only performs
the explicit delivery-state transition.

Theory-fit judgment (2026-09-02, owner with Claude-family reviewer): the
feature fits. It places as a delivery-state transition on a named queue
owned by SimpleBroker with the backend supplying the atomic realization
([THEORY-3]); it adds no concept, since the pending set, public ID order,
claim, and vacuum already exist; and it satisfies every [THEORY-4] principle
as an explicit per-call option with no stored policy. [THEORY-3] already
defines claim as a delivery-state transition distinct from anything the
application did, so a superseded-by-write claim is consistent with the
concept even though it changes what the docs teach. Weft's
write-then-delete-previous registry code is the [THEORY-6] signal of a
missing primitive. The Out of Scope list is the theory boundary: a stored
cap, time or size retention, body-based selection, a new claim state, or a
background lifecycle would each be the platform growth [THEORY-5] refuses.
Fit and the decision to implement are settled. Downstream adoption remains a
separate product choice and is not a release gate.

The primary benefit is a compact snapshot/state-feed primitive: a producer can
publish new state and bound obsolete pending states without a race between
write and cleanup. The main cost is semantic: `claimed` no longer implies a
consume attempt. That expansion must be stated everywhere claimed state is
taught.

## Decisions Locked by This Plan

1. The CLI spelling is exactly `--keep-newest N`. There is no short flag,
   alias, environment variable, config key, or stored queue default. The name
   carries the selection rule so an agent reading a transcript does not have
   to guess which end of the queue survives. Bare `--keep` was rejected
   because `read --keep` would read naturally as "do not delete".
2. The matching Python surface is
   `Queue.write(message: str, *, keep_newest: int | None = None) -> int`.
   `simplebroker.commands.cmd_write(..., *, keep_newest: int | None = None, ...)`
   carries the same option. Omission or `None` preserves ordinary write.
3. `N` is bounded: its value must be from 1 to 9999 inclusive. Python accepts
   only an exact `int` in that range. `bool` raises `TypeError`; any other
   non-`int` type, including digit strings, raises `TypeError`; an `int`
   outside 1..9999 raises `ValueError`. JSON-sourced callers convert with
   `int()` at the call site: a count capped at 9999 has no transport reason
   to travel as a string, unlike 64-bit message IDs. Validation happens
   before target acquisition or mutation.
4. CLI `N` is one or more ASCII decimal digits. Leading zeroes are stripped;
   what remains must be one to four digits with value at least 1, so the
   check never converts an unbounded string and `007` means `7`. Signs,
   underscores, non-ASCII digits, missing values, duplicate `--keep-newest`
   occurrences, zero, and values above 9999 are invalid arguments. Validation
   happens before stdin is read, alias or target resolution, or mutation.
5. The cap of 9999 is deliberate and arbitrary. It bounds the per-write index
   walk that locates the retained-set cutoff, which is O(N) on SQLite and
   PostgreSQL, and it sits far above any snapshot-window need. Measured
   cutoff selection is under a millisecond on every backend, so the cap is a
   usability guard, not a performance one. A caller who wants to retain more
   rows wants a ring-buffer log, which is a different feature with a
   different cost. The cap does not bound the displaced set; see decision 9.
6. “Most recent” means highest integer public message ID under [SB-SELECT-5],
   not insertion order or wall-clock observation. The post-operation pending
   set is the highest `N` IDs from the prior pending set plus the new row.
7. Displaced rows are claimed, not deleted. The owner reconsidered deletion on
   2026-09-02 and retained claim: it reuses the existing deletion-pending
   lifecycle and keeps the include-claimed forensic window until vacuum. The
   accepted cost is that `claimed` no longer implies a consume attempt.
   Existing claimed rows neither count toward `N` nor return to pending. Rows
   newly displaced become ordinary claimed, deletion-pending rows, visible
   only through existing claimed-inspection surfaces and vacuumable under the
   existing policy, which a large first trim may make immediately eligible.
8. On SQL backends the allocation/high-water update, insertion, and
   displaced-row claims are one transaction. A known rejection or rollback
   leaves no new row, no new durable high-water value from this attempt, and
   no newly claimed row. A commit failure with unknown outcome may leave the
   whole unit committed, but never a durable subset. At commit the queue
   contains exactly `min(N, prior_pending_count + 1)` pending rows from this
   operation's serial history. A later operation may immediately exceed the
   window.
9. On Redis/Valkey the write and every displaced-row claim are one Lua
   script. Before its first mutation the script computes the displaced set,
   verifies bodies, and checks reservations; after that point it mutates
   high-water, inserts the row, and transfers every displaced member from
   pending to claimed. There is no window, no ceiling, and no convergence
   loop. The cost is linear in displaced rows: measured at 166 ms for 100k
   and 377 ms for 220k displaced rows on Valkey 7.2, during which the server
   is blocked; the default 5 s busy threshold is not reached below roughly
   2.9 million displaced rows. The same linearity holds on SQL: 46/106 ms on
   SQLite and 425/1000 ms under the PostgreSQL table lock at 100k/220k.
   `DELETE_QUEUE`'s unbounded `ZRANGE` is the precedent.
10. An active at-least-once batch reservation is never stolen. SQL waits
    through normal lock/retry policy; PostgreSQL reservations remain protected
    until their transaction finishes because PostgreSQL has no batch-expiry
    mechanism. Redis recovers stale batches first, then
    fails the keep-write atomically, before any mutation, with a retryable
    `OperationalError` if an active reservation intersects the rows that would
    be claimed; the caller lets the batch finish or expire and retries the
    whole write. A Redis reservation among the retained newest `N` rows does
    not cause conflict rejection; PostgreSQL's coarser lock still waits for an
    open batch even when its rows would be retained. Skipping reserved rows
    was rejected: it makes the
    pending count unable to reach `N` and, without a cursor, livelocks on a
    reserved lowest window.
11. The public return and output shapes do not grow a claimed-count field.
    `Queue.write` still returns the new message ID; plain CLI write remains
    quiet; `-t` and `--json` retain their current ID-only output. This keeps the
    operation substitutable with ordinary write. Callers that need counts can
    inspect `stats` before and after, accepting concurrent change.
12. A committed keep-write attempts the same post-commit activity publication
    sequence as an ordinary write. No hint or maintenance accounting occurs
    before the atomic core succeeds. Redis publication retains the existing
    post-commit transport-failure ambiguity: the call may raise after durable
    state committed, and a raising call returns no ID. Automatic maintenance
    remains caught/best-effort and records one completed write, not one event
    per displaced row.
13. The backend API advances from v8 to v9. All first-party backends implement
    the optional `keep_newest` argument together; mixed versions fail at the
    existing exact handshake. No fallback decomposes the operation into write
    followed by public claims.
14. The coordinated release is a core minor release with matching first-party
    extension minor releases: core 8.1.0 and PostgreSQL/Redis 4.1.0 from the
    current 8.0.0/4.0.0/4.0.0 baseline, with release map entry `9: "8.1.0"`.
    Every public surface is additive and ordinary write is unchanged.
    Precedent in `bin/release.py::BACKEND_API_MIN_CORE_VERSION`: backend API
    v2 through v7 shipped in minor or patch core releases; only v8 was a
    major, driven by the storage migration and order re-contract, not by the
    API number. The release task records the exact versions from the release
    driver. Coordination is still required because the exact handshake makes
    a mixed pair fail at import.
15. The stated guarantee is atomicity plus linear cost, not bounded time. A
    first keep-write against a deep backlog is a bulk operation whose cost is
    the measured table in Backend Design, paid once; in steady state one row
    is displaced per write. Other operations wait through the existing
    contention policy on every backend: SQLite `busy_timeout` plus retry,
    PostgreSQL lock wait with retryable SQLSTATEs, Redis socket wait with
    BUSY treated as retryable. There is no displaced-row ceiling and no new
    error class. Documentation states the linear cost and the 220k
    observed-maximum figures.

## Alternatives Considered

- **Write, then call a separate trim/claim operation.** Rejected, but not
  because of a race. For a snapshot feed an interleaved ordinary write
  over-retains by one row and an interleaved consumer read is harmless. The
  honest drivers are one explicit call, one failure outcome instead of two,
  and the difficulty of doing this in application space without private
  reaches; Weft's registry prune code shows the cost of the two-call form.
- **A persistent queue `max_pending` setting.** Rejected. It adds durable
  policy, cross-operation enforcement, configuration lifecycle, and capacity
  semantics. Those are materially larger concepts than an explicit write
  option.
- **Physically delete older rows.** Reconsidered on 2026-09-02 and rejected
  by the owner. Deletion would have kept `claimed` meaning consumed-only and
  removed the vacuum interaction, and `delete` is already a first-class
  destructive lifecycle, so the earlier "second lifecycle" objection was
  weak. It was rejected because it loses the include-claimed forensic window
  for superseded rows and puts a second removal path on the write hot path.
- **Return the displaced count or a new result object.** Rejected for this
  version. It changes the established scalar write result and JSON shape for a
  value not needed to perform the operation. The explicit flag and post-write
  stats are enough for the intended primitive.
- **Apply the option to move or broadcast.** Rejected. Each would need a
  different multi-source or multi-target atomicity and result contract.
- **Bare `--keep N` and `keep=`.** Rejected. Ambiguous across verbs, and the
  "N includes the new message" rule is an off-by-one trap that a
  self-describing name defuses.
- **No upper bound on `N`, with a `2**63` no-trim ceiling.** Rejected. It
  needed a digit-string normalizer, a skip-trim special case, and spec prose
  to describe a value nobody can use, while leaving the per-write SQL cutoff
  walk unbounded. See decision 5.
- **A core major release.** Rejected. See decision 14.
- **Bounded Redis windows converging within the call.** Proposed in the
  2026-09-02 owner revision and withdrawn after independent review. It broke
  atomicity into a convergence protocol with transient over-retention,
  durable partial cleanup after failure, and consumers able to claim
  superseded rows between windows; with reserved rows skipped it could not
  reach `N` and, without a cursor, livelocked on a reserved lowest window.
- **A hard displaced-row ceiling that fails the write before mutation.**
  Considered next and rejected on measurement. The stall it prevents is 1 s
  on PostgreSQL and under 0.4 s on Valkey at the 220k observed maximum, paid
  once on first use, and every backend's existing contention policy carries
  waiters through it without failures. A refused write, a non-retryable
  error class, and a delete-first recovery are a worse trade than that.
- **Skipping reserved rows instead of failing.** Rejected; see decision 10.
- **Use a PostgreSQL queue advisory-lock protocol across every producer.** Not
  selected initially. It offers more concurrency but would alter move,
  exact-insert, broadcast, and rename lock ordering to protect one optional
  write mode. The initial design instead takes the existing meta-row lock and
  a transaction-scoped `SHARE ROW EXCLUSIVE` lock on `messages` for a
  keep-write. Reopen the finer lock design only if real contention evidence
  shows the table lock is unacceptable.

## Source Documents

Theory and process:

- `docs/program-theory.md` [THEORY-1], [THEORY-2], [THEORY-3], [THEORY-4]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-6], [DOM-10], [DOM-11], [DOM-15]
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`
- `skills/interface-review/SKILL.md`

Winning product contracts:

- `docs/specs/10-cli.md` [SB-CLI-1] through [SB-CLI-6]
- `docs/specs/11-delivery.md` [SB-DELIVERY-1], [SB-DELIVERY-5],
  [SB-DELIVERY-8]
- `docs/specs/13-message-identity.md` [SB-ID-1], [SB-ID-2], [SB-ID-3]
- `docs/specs/14-timestamp-selection.md` [SB-SELECT-5]
- `docs/specs/16-python-library-api.md` [SB-API-4], [SB-API-9],
  [SB-API-10], [SB-API-11], [SB-API-12]
- `docs/specs/17-ops.md` [SB-OPS-1], [SB-OPS-2], [SB-OPS-6]
- `docs/specs/product-section-registry.md`

Human and agent entry points:

- `README.md`
- `docs/agent-kernel.md`
- `docs/guides/python.md`
- `docs/guides/backends.md`
- `CHANGELOG.md`

Implementation rationale and downstream evidence:

- `docs/implementation/05-product-invariant-inventory.md`
- `docs/implementation/07-complexity-and-state-machine-map.md`
- `docs/implementation/08-message-identity-and-write-visibility.md`
- `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
- sibling checkout `../weft/pyproject.toml`
- sibling checkout `../weft/weft/commands/interactive.py`
- sibling checkout `../weft/weft/commands/tasks.py`
- sibling checkout `../weft/tests/helpers/weft_harness.py`

## Context and Key Files

| Owner | Current load-bearing behavior | Planned change |
|-------|-------------------------------|----------------|
| `simplebroker/sbqueue.py::Queue.write` | Public write takes one message, returns the committed ID, and updates its cache only after connection success. | Add keyword-only `keep_newest`, validate it before `get_connection()`, and preserve the scalar return/cache timing. |
| `simplebroker/commands.py::cmd_write` | Snapshots config, obtains message content, resolves an alias, calls `Queue.write`, then emits only the optional ID. | Validate `keep_newest` before config, stdin, alias, or target work; pass it through without changing output. |
| `simplebroker/cli.py` write parser and `_protect_write_operands` / `_partition_write_arguments` | One grammar inventory owns registered tokens. Write output flags may move around free-form operands, and `--` protects literal option-shaped bodies. | Register one value-taking write option, preserve its value as a unit in normalization, reject duplicates, and keep `--keep-newest` literal only after `--`. |
| `simplebroker/_delivery.py` | Owns the closed delivery vocabulary and validation. | Add the single shared `keep_newest` validator (exact `int`, value 1..9999) because the option changes pending-to-claimed delivery state. Do not create a general retention subsystem. |
| `simplebroker/_backend_plugins.py::BrokerConnection` | Backend API v8 requires `write(queue, message) -> int`. | Advance to v9 and require `write(queue, message, *, keep_newest: int | None = None) -> int`. |
| `simplebroker/ext.py` | The public advanced/backend-facing module docstring explains the exact handshake through backend API v8. | Document v9's required optional keep-write contract and coordinated-package boundary without exposing private implementation. |
| `simplebroker/db.py::BrokerCore.write`, `_do_write_with_ts_retry`, `_do_write_transaction` | SQL write validates, retries timestamp conflicts, then atomically advances high-water and inserts. Maintenance happens after commit. | Carry `keep_newest` through the same retry owner; after insert, claim all but the highest N pending IDs before the same commit. |
| `simplebroker/_sql/_contract.py`, `simplebroker/_sql/sqlite.py`, `extensions/simplebroker_pg/simplebroker_pg/_sql.py` | The SQL contract owns shared write/retrieve statements; a partial `(queue, ts)` index covers pending rows. | Add one dialect-owned claim-below-window statement that uses public ID order and the existing pending index. |
| `extensions/simplebroker_pg/simplebroker_pg/plugin.py::prepare_queue_operation` | PostgreSQL claim/move rely on row locks; other queue operations use advisory locks. Broadcast/rename use meta-row-then-table ordering. | Give `operation="write_keep"` an explicit table-lock branch. The write transaction already holds the meta row before it requests the table lock. |
| `extensions/simplebroker_redis/simplebroker_redis/core.py::_write_message` | One Lua `EVAL` validates high-water and publishes the row atomically; Python publishes activity afterward. | Pass optional keep to the same write state machine, map reservation conflict to a retryable known-failure outcome, and make the BUSY reply's retryable classification explicit. |
| `extensions/simplebroker_redis/simplebroker_redis/scripts.py::WRITE_MESSAGE` | The script front-loads namespace, duplicate, and stale-candidate checks before high-water and row mutation. | Precompute displaced IDs, verify bodies and reservations, then mutate high-water, insert, and transfer every displaced ID from pending to claimed in the same script. |
| `bin/release.py::BACKEND_API_MIN_CORE_VERSION`, package metadata, and lock files | The release map ends at API v8/core 8.0.0; current package versions are core 8.0.0 and extensions 4.0.0. `release.py all` publishes versions already present in metadata and rejects a supplied `--version`. | Add `9: "8.1.0"` to the release map, explicitly bump core to 8.1.0 and extensions to 4.1.0 before `all`, update floors/locks, and preserve the existing extension-first artifact workflow. |

Primary test owners to extend:

- `tests/test_delivery_contract_sb_delivery.py`
- `tests/test_message_identity_contract_sb_id.py`
- `tests/test_cli_contract_sb_cli.py`
- `tests/test_cli_rearrange_args.py`
- `tests/test_cli_write_output.py`
- `tests/test_json_message_id_contract.py`
- `tests/test_agent_kernel_contract.py`
- `tests/test_write_returns_id.py`
- `tests/test_write_visibility.py`
- `tests/test_core_persistence_transition_tables.py`
- `tests/test_custom_runner_integration.py`
- `tests/test_property_queue_model.py`
- `tests/test_property_cli_args.py`
- `tests/test_python_library_api_contract_sb_api.py`
- `tests/test_operations_contract_sb_ops.py`
- `tests/test_backend_plugin_resolution.py`
- `tests/test_ext_imports.py`
- `tests/test_public_surface.py`
- `tests/test_constants.py`
- `extensions/simplebroker_pg/tests/test_pg_state_machine_transitions.py`
- `extensions/simplebroker_pg/tests/test_pg_message_id_order.py`
- `extensions/simplebroker_pg/tests/test_pg_init_backend.py`
- `extensions/simplebroker_redis/tests/test_redis_atomicity.py`
- `extensions/simplebroker_redis/tests/test_redis_batches.py`
- `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py`
- `extensions/simplebroker_redis/tests/test_redis_validation.py`
- `tests/test_release_script.py`

Before editing product code, the implementer and first reviewer must record
answers to these questions in Execution Evidence. A materially wrong answer
blocks implementation until the cited owner is reread and the plan is fixed.

1. **What does `N` count?** Expected: pending rows only, after conceptually
   adding the new generated row. Existing claimed rows do not count. The new
   row is in the retained highest-N set.
2. **What proves “recent”?** Expected: [SB-SELECT-5] defines order by integer
   public message ID. It is not row insertion order or arbitrary backend
   order. The generated row is above durable high-water and therefore above
   every stored ID.
3. **Why is the PostgreSQL table lock needed even though write is already a
   transaction?** Expected: a concurrent move or low exact-ID insert can add a
   pending row to the target without contending on the generated-write meta
   row. Without a shared mutation lock, it can commit between trim selection
   and keep-write commit, so no commit-time linearization point satisfies the
   window. The table lock blocks concurrent row mutations while permitting
   ordinary reads.
4. **Why may Redis not claim every low pending member it finds?** Expected: an
   at-least-once batch keeps selected IDs in pending while also marking them
   reserved. Claiming a reserved ID would steal an open batch and break
   commit/rollback semantics. Stale recovery runs first; a remaining
   intersection is retryable contention and the script must mutate nothing.
   Skipping instead would leave pending above `N` and risk livelock.
5. **Why does a successful keep-write not imply physical retention or a
   durable cap?** Expected: displaced rows are claimed, not deleted, and later
   operations that do not carry `keep_newest` can add pending rows. Vacuum
   and later writes are separate operations.
6. **Why does the cap on `N` not bound the cost of a keep-write?** Expected:
   the displaced count is pending minus `N`, driven by queue depth. `N`
   bounds only the sub-millisecond cutoff walk. Cost is linear in displaced
   rows on every backend, and other operations wait through the existing
   contention policy rather than failing.

## Spec Baseline

- `c1866de9a2f138cd1899149d9829134f4ba9f2f9`: `docs/specs/10-cli.md`,
  `docs/specs/11-delivery.md`, `docs/specs/13-message-identity.md`,
  `docs/specs/14-timestamp-selection.md`,
  `docs/specs/16-python-library-api.md`, `docs/specs/17-ops.md`, and
  `docs/specs/product-section-registry.md` at plan authoring time.
- Plan type: implementation with spec revision. Task 1 records the promotion
  commit SHA, or the exact spec-tree diff base if the owner requests an
  uncommitted review.

If another change alters write signatures, backend API version, claim meaning,
CLI write grammar, Redis reservation state, or PostgreSQL lock order before
promotion, stop and update this baseline and the Deviation Log.

## Proposed Spec Delta

Promotion strategy: **A, in-file edits with requirement text before
implementation-link claims.** All affected families are already
`canonical-spec`. Task 1 promotes the following exact meaning before product
implementation. Surrounding prose may be edited for grammar, but a change in
meaning requires owner approval and a Deviation Log entry.

### `docs/specs/11-delivery.md`: new [SB-DELIVERY-9]

Amend [SB-DELIVERY-1] after its first paragraph:

> Claimed is a delivery state, not proof that a caller received or attempted a
> message. In addition to consume claims, an explicit write-time pending-window
> operation may claim older pending rows without handoff under
> [SB-DELIVERY-9]. Both forms are deletion-pending and are omitted from ordinary
> pending delivery.

Add:

> ## Write-time pending window [SB-DELIVERY-9]
>
> `Queue.write(message, *, keep_newest=N)` and CLI `write --keep-newest N`
> perform one operation-scoped pending-window transition. `N` is an integer
> from 1 to 9999. The operation allocates and inserts the new generated-ID
> message, then leaves pending only the `N` highest integer public message
> IDs from the prior pending set plus that new row. Every other previously
> pending row becomes claimed. Existing claimed rows do not count toward `N`
> and remain claimed.
>
> Allocation/high-water advancement, insertion, and displaced-row claims are
> one atomic backend operation. A known rejection or rollback commits none of
> those effects. A transport or commit failure whose outcome is unknown may
> leave the complete unit committed, but never a durable subset; a raising
> call returns no ID. When the prior pending count is less than `N`, the write
> succeeds without changing any existing row's state. The new row counts
> toward `N` and remains pending. The operation's cost is linear in the
> number of displaced rows, and concurrent operations wait through the
> backend's ordinary contention policy; there is no displaced-row ceiling.
>
> The guarantee is scoped to this operation's linearization point. It is not a stored queue policy,
> physical storage limit, or backpressure mechanism. Later writes, exact
> inserts, broadcasts, and moves that do not carry `keep_newest` may
> increase the pending count above `N`. Displaced rows remain physically
> present until ordinary vacuum policy removes them. The option is intended
> for a dedicated single-producer queue; on a shared queue it claims other
> producers' pending messages.
>
> A keep-write never steals an active at-least-once reservation. The backend
> may wait under its normal contention policy. If it cannot establish the
> atomic transition, it raises an operational failure without partial write or
> claim. A retryable conflict instructs the caller to let the active batch
> finish or expire, then retry the whole write.

Add implementation mapping for `Queue.write`, `BrokerCore.write`, both SQL
dialects, PostgreSQL lock preparation, and Redis `WRITE_MESSAGE`. Add one
firing verification row that binds the full input, state, failure, concurrency,
reservation, and cross-backend matrices named below.

### `docs/specs/13-message-identity.md`: amend [SB-ID-2]

Append to [SB-ID-2]:

> When write carries the [SB-DELIVERY-9] pending window, its optional claims
> join generated-ID allocation/high-water advancement and row insertion in the
> same atomic operation. A known rejected or rolled-back trim does not commit
> the generated high-water value, message row, or claims. An outcome-ambiguous
> commit/transport failure may leave all three durable, but never a subset, and
> a raising call returns no ID. A successful call returns the new row's ID
> exactly as ordinary write does.

Extend the [SB-ID-2] verification row with SQL rollback and real-Valkey script
failure probes proving no ID/high-water/row/claim partial state.

### `docs/specs/10-cli.md`: new [SB-CLI-7]

Add:

> ## Write-time pending window option [SB-CLI-7]
>
> `write` accepts `--keep-newest N`, where `N` is one or more ASCII decimal
> digits; after leading zeroes are stripped, one to four digits must remain
> with value at least 1, so the value is from 1 to 9999 and no unbounded
> conversion occurs. A sign, underscore, non-ASCII digit, zero, value above 9999, missing value,
> or repeated `--keep-newest` is an invalid argument. It fails before stdin
> consumption, alias or target resolution, or broker mutation. Text mode
> emits the ordinary invalid-argument diagnostic and exits `1`; established
> JSON mode emits one [SB-CLI-4] `INVALID_ARGUMENT` object and exits `1`.
>
> The option is valid only for `write`. It follows the supported write-option
> positions under [SB-CLI-3] and accepts both `--keep-newest N` and
> `--keep-newest=N` before the option terminator. Because it is a registered
> CLI token, `--` is required to write or broadcast the literal body
> `--keep-newest` or `--keep-newest=<value>`. The help text states that the
> operation claims older pending messages, that the newly written message is
> included in `N`, and that the option is meant for a dedicated
> single-producer queue.
>
> Success keeps the existing write output contract: plain mode is quiet,
> `-t` / `--timestamps` prints the new ID, and `--json` emits only the existing
> timestamp object. An empty pre-write queue or a queue with fewer than `N`
> pending rows is success `0`.

Amend [SB-CLI-3] so its write-option grammar names value-taking write options,
not only output options. Add a complete token/position/terminator/duplicate
matrix and help snapshot to [SB-CLI-7] verification.

### `docs/specs/16-python-library-api.md`

Append to [SB-API-4]:

> `Queue.write(message, *, keep_newest: int | None = None) -> int` optionally
> applies [SB-DELIVERY-9]. `None` preserves ordinary write. An exact `int`
> from 1 to 9999 is required otherwise. Booleans and all other types,
> including digit strings, raise `TypeError`; an out-of-range integer raises
> `ValueError`. Validation precedes target acquisition. Success retains the
> scalar committed-message-ID result.

Append to [SB-API-10]:

> `cmd_write(..., *, keep_newest: int | None = None, ...)` exposes the
> same normalized option and validates it before config consumption, stdin,
> alias resolution, or target access. It retains existing return codes and
> stdout shapes.

Append to [SB-API-11]:

> Backend API v9 changes `BrokerConnection.write` to
> `write(queue, message, *, keep_newest: int | None = None) -> int`, receiving
> the already-normalized integer. Every backend must implement the
> [SB-DELIVERY-9] transition when `keep_newest` is present and preserve v8
> ordinary-write behavior when it is absent. First-party plugins declare v9
> exactly; older or newer versions fail the existing handshake rather than
> emulating the option as multiple public operations.

Amend [SB-API-12]'s write row to cite `[SB-ID-*]` and [SB-DELIVERY-9]. Extend
the firing evidence for [SB-API-4], [SB-API-10], and [SB-API-11]. Do not create
a new clause solely for these additions; they fit the existing surface owners.

### `docs/specs/17-ops.md`: claimed-source clarification

In [SB-OPS-2], define `claimed` count as all deletion-pending rows regardless
of whether they came from consume or [SB-DELIVERY-9]. In [SB-OPS-3], replace
“claimed rows from consume” with “claimed rows from consume or write-time
pending-window trim.” [SB-OPS-6]'s vacuum effect remains unchanged. Add
write-time claims to its verification evidence; do not add a new operations
clause because delivery owns the transition.

### Registry and index changes

- Advance the CLI range to `[SB-CLI-1]` through `[SB-CLI-7]` and bind the
  README write-options locus plus CLI grammar tests.
- Advance delivery to `[SB-DELIVERY-1]` through `[SB-DELIVERY-9]` and bind the
  README Critical Safety, agent kernel, shared contract suite, SQL concurrency,
  and real-Valkey reservation/atomicity tests.
- Update message-identity, Python-API, and operations gate cells with their new
  firing evidence without changing their owner or state.
- Update `docs/specs/00-specs-index.md` clause ranges and summary where it
  enumerates them.

## Proposed Root README Delta

These are deliberate root README restatements and examples, not a competing
source of truth.

### Quick Start

Keep the existing first-run commands non-destructive. After the ordinary write
example, add only this pointer:

> For superseding snapshot/state feeds, write can atomically claim older
> pending values with `--keep-newest`; read the destructive-state warning under Write
> options before using it.

### Commands and write options

Change the write catalog row to:

```text
write <queue> [message|-] [--keep-newest N]  Add a message; optional --keep-newest atomically
                                      claims older pending messages after the
                                      write. -t/--json print the new ID.
```

Add to **Write options**:

> - `--keep-newest N` - In the same atomic operation as the write, leave pending only
>   the `N` highest public message IDs, including the new message, and mark
>   older pending messages claimed. `N` is from 1 to 9999.

Follow it with:

> `--keep-newest` is per write. It is not a stored queue size, physical retention,
> or backpressure policy. Existing claimed rows do not count toward `N`, and
> later ordinary writes, broadcasts, exact inserts, or moves may grow the
> pending set again. Claimed rows remain visible to stats and claimed
> inspection until vacuum removes them. Normative behavior:
> [`SB-DELIVERY-9`](docs/specs/11-delivery.md#write-time-pending-window-sb-delivery-9)
> and [`SB-CLI-7`](docs/specs/10-cli.md#write-time-pending-window-option-sb-cli-7).

After that warning, add the executable example:

```bash
# Write new state and leave only the 10 highest-ID messages pending.
# Older pending messages become claimed; the new message counts toward 10.
$ broker write --keep-newest 10 snapshots "current state"
```

Update the option-position paragraph to say that `--keep-newest N` and `--keep-newest=N`
follow the same supported pre-`--` write positions as output flags, and that a
literal `--keep-newest` or `--keep-newest=<value>` write or broadcast body requires `--`.

### Critical Safety Notes

Add near Safe Message Handling:

> `write --keep-newest N` is destructive to delivery state. It can make older pending
> messages unavailable to ordinary consumers even though no consumer received
> them. Use it only when newer messages supersede older ones, and only on a
> dedicated queue with a single producer: on a shared queue it claims other
> producers' pending messages. The operation claims rather than physically
> deletes displaced rows, but automatic or explicit vacuum may later remove
> them.

### Python API tour and claimed metadata

Add to the API example:

```python
message_id = queue.write("current state", keep_newest=10)
```

State that the return remains the new message ID. Change the metadata sentence
to:

> `QueueStats.claimed` is the count of deletion-pending messages claimed by a
> consume operation or by write-time `keep_newest`, but not yet vacuumed.

### README contract gates

Extend README structural tests to require the `--keep-newest` example, the destructive
warning, the “not a stored or physical cap” boundary, and links to
[SB-DELIVERY-9] and [SB-CLI-7]. Promise-equivalence review compares each
README claim in both directions against the promoted spec text.

## Other Documentation Delta

- `docs/agent-kernel.md`: add the CLI/Python operation-map form, the atomic
  pending-window meaning, the destructive warning, the unchanged ID result,
  and the explicit non-policy/non-cap boundary.
- `docs/guides/python.md`: document the signature, validation types, pending
  versus claimed counting, concurrency meaning, and a snapshot-feed example.
- `docs/guides/backends.md`: document backend API v9 and require one atomic
  realization. Note PostgreSQL's keep-write table lock with its measured
  cross-queue hold time, and Redis's pre-mutation reservation check with its
  retryable conflict, as backend operational details, not semantic
  differences. State the linear cost with the measured figures.
- `simplebroker/ext.py`: update the public backend-authoring docstring from v8
  through v9 and point operation meaning back to [SB-DELIVERY-9].
- `docs/implementation/08-message-identity-and-write-visibility.md`: extend
  ordinary-write visibility to the optional claim step, including rollback,
  post-commit hint, and Redis validation-before-mutation order.
- `docs/implementation/09-storage-schema-and-claim-lifecycle.md`: explain
  write-time claims, SQL query/index use, PostgreSQL lock order, Redis reserved
  intersections, and vacuum consequences.
- `docs/implementation/07-complexity-and-state-machine-map.md`: extend
  `SM-REDIS-WRITE` rather than inventing a parallel state machine. Enumerate
  ordinary success, keep success/no displaced rows, keep success/displaced
  rows, timestamp conflict, namespace failure, corrupt candidate, active
  reservation conflict, transport ambiguity, and retry exhaustion.
- `docs/implementation/05-product-invariant-inventory.md`: add the pending
  window under delivery and identity mappings.
- `extensions/simplebroker_pg/README.md` and
  `extensions/simplebroker_redis/README.md`: update protocol compatibility and
  backend-specific contention/atomicity notes.
- `CHANGELOG.md`: add an Unreleased entry naming `write --keep-newest N`,
  `Queue.write(..., keep_newest=N)`, its destructive pending-to-claimed effect,
  backend API v9, unchanged output, and coordinated package requirement.

## Invariants and Hidden Couplings

1. Ordinary `write` without `keep_newest` must retain its signature behavior,
   one-row effect, one Redis data `EVAL`, output, cache update, publish timing,
   maintenance count, retry budget, and error types.
2. Keep validation has one semantic owner and must occur before any potentially
   blocking stdin read or target/config side effect on the CLI/direct-command
   path. Backends still defend their advanced direct protocol boundary.
3. Alias resolution remains command-layer only. Keep applies to the resolved
   canonical queue; `Queue` continues to take a literal queue name.
4. The inserted row and every displaced row must share one atomic durability
   outcome, including when the caller cannot determine whether commit
   occurred. Do not call public claim APIs, loop through `Queue` objects, or
   issue post-commit cleanup.
5. Existing claimed rows are untouched. The SQL predicate and Redis key set
   must make this structural, not an after-the-fact correction.
6. All selection and cutoff logic uses integer public IDs. Engine-return order,
   Redis iteration order, and local timestamp cache order are not semantic.
7. PostgreSQL lock order is meta row first, then `messages` table lock. Do not
   add an advisory lock or reverse this order locally. SQLite stays under its
   existing `BEGIN IMMEDIATE` writer serialization.
8. Redis Lua validates namespace, candidate uniqueness/staleness, displaced
   body existence, and displaced reservation absence before its first write.
   Redis script errors do not roll back prior commands, so validation after a
   mutation is a correctness defect.
9. Redis stale-batch recovery is best-effort preparation outside the Lua
   linearization point. The Lua script must recheck current reservations and
   remains the final atomic authority.
10. PostgreSQL/Redis tests must use real services for locking, script atomicity,
    reservation, and command-count claims. SQLite transaction tests use a real
    database and real runner. Mocks may observe post-commit publish or inject a
    controlled runner failure, but may not replace storage selection or
    transaction boundaries.
11. Post-commit activity publication and automatic maintenance remain
    auxiliary. They must not roll back durable state. Redis publication may
    preserve the existing raised post-commit transport ambiguity; automatic
    maintenance remains caught and best-effort. A core trim/insert failure is
    fatal and publishes nothing before commit.
12. The state transition is a one-way door once vacuum removes the newly
    claimed rows. Code rollback cannot restore them. Release notes and README
    must tell operators to back up data if older pending messages matter.
13. No schema migration or persistent format change is needed. A proposal to
    add stored keep state, a new claimed-state value, or a background cleanup
    lifecycle stops the plan and requires a new spec decision.
14. No new runtime dependency is permitted. A helper abstraction that grows
    beyond input normalization or a second keep execution path is a stop gate.
15. The keep-newest path adds no ceiling, window, or second script. Ordinary
    write remains one `EVAL` and a keep-write remains one `EVAL`. Cost is
    linear in displaced rows and is documented with measurements, not bounded
    by code.

## Backend Design

### Measured cost baseline

Measured 2026-09-02 with the real schemas and indexes, `keep_newest=5`,
small JSON bodies, and the cutoff-then-update shape below. PostgreSQL 18 and
Valkey 7.2 ran in Docker on macOS, which inflates their figures somewhat;
SQLite ran natively with WAL and `synchronous=FULL`.

| Backend | 100k pending | 220k pending (observed maximum) | Where the time goes |
|---|---|---|---|
| SQLite | 46 ms | 106 ms | update 40 / 93 ms; fsync 6 / 13 ms; cutoff select under 0.1 ms |
| PostgreSQL, table lock held | 425 ms | 1000 ms | update 401 / 975 ms; cutoff select 0.5 ms |
| Valkey, one script | 166 ms | 377 ms | server blocked for the script; cutoff range under 1 ms |

Cutoff selection is sub-millisecond everywhere, so `N` does not affect cost.
Cost is linear in displaced rows. PostgreSQL's MVCC tuple versions plus
partial-index maintenance make it the slowest and, because its lock is
table-wide, the one whose stall reaches other queues. Waiters do not fail at
these sizes on any backend: SQLite waits inside `busy_timeout` (5 s default)
with a ten-attempt retry behind it, PostgreSQL lock waiters block until
commit with no `lock_timeout` set, and Valkey clients wait on the socket,
with BUSY only past the 5 s threshold (roughly 2.9 million displaced rows)
and then matched as retryable. The implementation ladder re-measures on the
real code at 0, 1, 1,000, 10,000, 100,000, and 220,000 displaced rows and
records the figures in Execution Evidence and release notes.

### Shared SQL and SQLite

Add one SQL-contract statement that changes `claimed` from false to true for
the target queue's pending rows outside its highest-ID `N` set. Execute it
after the new row insert and before commit inside `_do_write_transaction`.
The retained-set cutoff is `ORDER BY ts DESC LIMIT 1 OFFSET N-1` on the
existing partial pending `(queue, ts)` index (a covering plan on SQLite, an
index scan on PostgreSQL), followed by one update of pending rows in the
queue with `ts` below the cutoff. The update may touch an unbounded number of
displaced rows, as the public request requires, inside the single
transaction.

`keep_newest=None` must skip both lock preparation and trim SQL.
`keep_newest=N` runs the trim even when it affects zero rows, so there is one
easy-to-audit transaction shape. SQLite's `BEGIN IMMEDIATE` serializes every
writer and therefore owns the cross-operation linearization without a new
lock.

Stop and re-evaluate if query plans on both empty/small and 100,000-row queues
do not use the pending index for retained-set selection, or if the SQL dialects
need semantically different selection rules.

### PostgreSQL

After generated timestamp allocation has locked the singleton meta row, call
`prepare_queue_operation(..., operation="write_keep", queue=queue)` before the
insert. PostgreSQL's branch takes the same transaction-scoped table lock mode
used to serialize row-changing broadcast scope. This blocks inserts, moves,
claims, deletes, and other row mutations until commit but permits ordinary
read-only observation.

The lock is table-wide, so a keep-write stalls row mutations on every queue
on that server for its duration: about 0.4 s at 100k and 1 s at 220k
displaced rows in the baseline. That is the explicit blast radius, paid once
on first use against a backlog and never in steady state, where one row is
displaced. Broadcast already takes the same lock across queues.

The coarse lock is intentional for the first version: it makes the atomic
window correct against every producer without distributing new lock rules
through all producer operations. Record real contention measurements,
including unrelated-queue writer stall at 220k. If the
lock causes unacceptable cross-queue stalls, stop and propose a stable ordered
queue-lock protocol covering every pending-row producer. Do not weaken the
linearization contract to save the implementation.

### Redis/Valkey

Extend `WRITE_MESSAGE` with an internal optional keep argument. Before any
mutation, compute `remove_count = max(0, pending_count + 1 - N)` using `ZCARD`
and fetch that many lowest-ID pending members by lexicographic range. The
generated candidate is higher than all stored IDs, so it is never in the
displaced set. Before mutation, type-check every key the script will touch:
the queue registry, metadata hash, body hash, global-ID set, and the queue's
pending, claimed, and reserved sorted sets. Verify every displaced member has
a body and is not in the queue's reserved set. Only then update durable
high-water, insert the new body/ID/pending member, and transfer each displaced
ID from pending to claimed.

The script returns only a small result code; it does not return every
displaced ID. Python maps a current reservation intersection to a known,
retryable `OperationalError`, with guidance to let the batch finish or expire
and retry the whole write. A missing displaced body is a non-retryable
`IntegrityError`. Reservation conflict is not a timestamp conflict and must
not consume the local timestamp-retry budget. Other result codes preserve
existing timestamp-conflict retry behavior. Ordinary `keep_newest=None`
writes keep their single unchanged `EVAL`.

The script blocks the server for its duration, linear in displaced rows:
166 ms at 100k and 377 ms at 220k in the baseline. `DELETE_QUEUE` and
`DELETE_FROM_QUEUES` already run unbounded `ZRANGE` scripts, so this is
established practice, now on the write path with a measured cost. Run the
ladder on the real script and record wall time, the tested service's
Lua-time-limit setting, and `SLOWLOG`/BUSY outcomes. Make the BUSY reply's
retryable classification explicit in `_translate_redis_error` rather than
relying on the SQLite-phrase marker match. Stop before release only if the
220k case emits BUSY or prevents the existing activity-wait probe from making
bounded progress. Do not batch: batching would violate atomicity. Passing
the ladder does not prove arbitrary-size safety; record the residual risk
beyond the observed maximum in release evidence and notes.

## Executable Behavior Matrix

| Case | Required result |
|------|-----------------|
| `keep_newest=None` | Exact existing ordinary-write behavior. |
| Prior pending `0`, `keep_newest=1` | New row pending; no claims; success and new ID. |
| Prior pending `N-1`, `keep_newest=N` | New row pending; no existing claims. |
| Prior pending `N`, `keep_newest=N` | Lowest prior pending ID becomes claimed; highest N including new remain pending. |
| Prior pending much greater than `N` | Every pending ID outside highest N becomes claimed in one commit. |
| Existing claimed rows | Unchanged and excluded from N; metadata total grows by one. |
| Exact lower IDs inserted earlier | Retention determined by numeric ID, not insertion time. |
| Invalid Python `None`/bool/int/type matrix | `None` means ordinary; exact int in 1..9999 succeeds; digit strings and all other invalid values fail pre-target with specified type. |
| Invalid CLI lexical/value/duplicate matrix | Exit 1 before stdin/target/mutation; JSON dialect after recognized `--json`. |
| `N` of `0`, `10000`, or a longer nonzero digit string | Invalid on CLI and in Python; rejected before stdin/target/mutation with no unbounded decimal conversion. CLI `007` is accepted as `7`. |
| Literal `--keep-newest` or `--keep-newest=3` write/broadcast body after `--` | Written literally; no pending-window effect. |
| Timestamp conflict/retry | Only surviving attempt writes and trims; failed attempts commit neither. |
| SQL trim or known pre-commit failure | Row, durable high-water, and claims roll back together. |
| Commit/transport outcome ambiguity | The whole atomic unit may be absent or committed; no durable partial unit is permitted, and a raising call returns no ID. |
| PostgreSQL concurrent move/exact insert/broadcast/write/delete/rename | A serial history exists; state at keep commit is its highest-N pending set or the ordered delete/rename outcome. |
| PostgreSQL cross-queue stall during a 220k trim | Measured hold time recorded against the baseline; unrelated-queue writers wait and succeed without error. |
| Redis displaced rows at 100k and 220k | One script; pending is exactly the highest-N set at script return; wall time recorded against the baseline. |
| Redis reserved displaced ID, including an all-reserved lowest set | Known retryable atomic failure before mutation; no high-water, row, or claim change; succeeds after the batch commits or rolls back. |
| Redis reservation only in retained N | Success; reserved row remains reserved/pending. PostgreSQL still waits for its open batch transaction because the SQL lock is coarser. |
| Consumer claim or ordinary write concurrent with a keep-write, any backend | A serial history exists; the consumer sees the pre-trim or post-trim pending set, never a partial one. |
| Sustained concurrent keep-writes with different N | Each commit satisfies its own window; no deadlock; no partial outcome. |
| Redis publish failure after commit | Existing ambiguous transport outcome: the call may raise, but committed write/claims remain committed. |
| Maintenance failure after commit | Existing caught/best-effort policy; committed write/claims remain committed. |
| Later ordinary producer | May exceed N; this does not retroactively violate the keep-write. |
| Vacuum after keep | May physically remove displaced claimed rows under existing policy. |

Each row must have a named firing test. Structural contract tests alone do not
satisfy the storage and concurrency rows.

## Agent-Facing Interface Design Review

Surface: CLI, proposed delta against `c1866de9`. The final implemented surface
must repeat the full eleven-principle walk with file:line evidence before
promotion. Initial design findings:

| ID | Severity | Location | Finding | Disposition |
|----|----------|----------|---------|-------------|
| I-1 | P2 | `simplebroker/cli.py:485`, `simplebroker/cli.py:996` | Current write normalization knows only valueless output flags; treating `--keep-newest` as another boolean token would misclassify or detach its value. | Task 5 expands the single grammar owner and adds full position/value/terminator tests. |
| I-2 | P2 | `simplebroker/commands.py:651` | Current direct write obtains stdin and resolves the target before any keep validation seam exists. Invalid destructive input could block or observe the target. | Task 2 adds shared pre-config/pre-stdin validation and an adversarial no-observation probe. |
| I-3 | P2 | `extensions/simplebroker_redis/simplebroker_redis/scripts.py:40` | Current atomic write script has no reserved-set input, so a naive trim could steal a live at-least-once batch. | Task 4 makes reservation absence a pre-mutation condition and fires intersecting, non-intersecting, and all-reserved-lowest cases. |

Principle disposition: #1 departs for plain CLI success because the established
Unix write contract is quiet; callers can request the compact ID confirmation
with `-t` or `--json`, while Python always returns it. Help, README, and kernel
provide progressive disclosure (#2). The flag's help supplies the full
pending-to-claimed meaning (#3). Message IDs remain the one identity (#4). The
backend derives the displaced set (#5). Target behavior is unchanged (#6).
Rejection is limited to unsafe or ambiguous destructive values and gives a
corrective diagnostic (#7). #8 departs for the same successful quiet-write
contract; every failure still carries the existing actionable diagnostic.
Writes are atomic on every backend, and a retryable reservation conflict
gives a recovery action (#9). Application supersession judgment remains outside the broker (#10). The
public model is pending/claimed rather than backend rows or keys (#11).

Ratified judgment: keep the ordinary scalar/quiet write result instead of
adding a displaced count. This is a deliberate Unix-composability and
substitutability choice, not an omitted confirmation. Verdict: blockers I-1
through I-3 are fully addressed by the plan. Spec promotion requires its green
structural gates plus reviewed red product probes ready for the next slice;
integration readiness requires those probes to be green with real-backend
evidence. Runbook feedback: no new reusable principle candidate at plan time.

### 2026-09-02 implemented-surface review

Surface and baseline: the public CLI and matching Python write surface in the
working-tree delta against
`c1866de9a2f138cd1899149d9829134f4ba9f2f9`. The review checked the
implemented parser, validator, output, transaction, Redis script, help,
README, kernel, and firing tests rather than accepting the proposed text as
evidence.

| Principle | Result | Implemented evidence |
|-----------|--------|----------------------|
| 1. Context is the scarcest resource | **Departs, ratified.** Plain CLI write remains quiet and does not add a displaced count; `-t`/`--json` and Python return only the new public ID. This preserves the established Unix write contract while still providing an explicit compact confirmation on request. | `simplebroker/commands.py:684`; `simplebroker/sbqueue.py:392`; `tests/test_cli_write_output.py:78` |
| 2. Progressive disclosure | **Met.** The option teaches its destructive effect in command help, then README and kernel add the safety and lifecycle detail. | `simplebroker/cli.py:553`; `README.md:346`; `docs/agent-kernel.md:67` |
| 3. Self-explanatory names; no lookup tables | **Met.** `--keep-newest` names the surviving end, and help says that N includes this write and older pending messages become claimed. | `simplebroker/cli.py:553`; `README.md:350` |
| 4. One identity per thing | **Met.** Selection and the result use the same integer public message ID used by read, peek, and exact-ID operations. | `simplebroker/sbqueue.py:402`; `docs/specs/11-delivery.md:279` |
| 5. Derive what is derivable | **Met.** The caller supplies only N. SQL and Redis derive the cutoff and displaced set from queue state. | `simplebroker/db.py:2094`; `extensions/simplebroker_redis/simplebroker_redis/scripts.py:83` |
| 6. No hidden session setup | **Met.** The window is a keyword/flag on each write; no queue config, session handle, or stored default is introduced. | `simplebroker/sbqueue.py:392`; `README.md:354` |
| 7. Teach, don't reject | **Departs, ratified.** Leading zeroes are safely canonicalized, but quiet CLI success does not report that normalization in-band. Unsafe or ambiguous values are rejected with the valid ASCII range. | `simplebroker/cli.py:135`; `simplebroker/cli.py:143`; `tests/test_cli_write_output.py:111` |
| 8. Every message carries its action | **Departs, ratified.** Quiet success has no guidance envelope. JSON failures remain structured, and a reservation conflict says to finish or expire the batch and retry the whole write. No traceback is exposed. | `simplebroker/cli.py:175`; `extensions/simplebroker_redis/simplebroker_redis/core.py:464`; `tests/test_cli_write_output.py:145` |
| 9. Atomic writes with a recovery path on conflict | **Met.** SQL inserts and claims within one transaction; Redis validates the full displaced set before its first mutation in one script. The retryable reservation error supplies the recovery sequence. The merge clause is not applicable to this scalar queue operation. | `simplebroker/db.py:2078`; `extensions/simplebroker_redis/simplebroker_redis/scripts.py:76`; `extensions/simplebroker_redis/simplebroker_redis/core.py:464` |
| 10. Draw the trust boundary in the interface | **Met.** The broker performs only the explicit state transition; README tells the caller that deciding whether newer values supersede older ones is application judgment and limits use to a dedicated single-producer queue. | `README.md:451`; `docs/specs/11-delivery.md:297` |
| 11. Wire format matches the agent's mental model, not the storage model | **Met.** The interface speaks in write, newest public IDs, pending, and claimed. SQL rows, Redis keys, and cutoff mechanics remain internal. | `README.md:350`; `docs/agent-kernel.md:138` |

Related gates: the enumerable write-option inventory is checked by
`tests/test_cli_rearrange_args.py:117`; value, duplicate, missing-value, JSON,
and no-target-observation cases fire in `tests/test_cli_write_output.py:100`;
registered and escaped spellings are covered by
`tests/test_property_cli_args.py:97`; the CLI error-code classification is
checked by `tests/test_cli_contract_sb_cli.py:79`. The public option/value
matrix, literal `--` escape, corrupt target, unread stdin, rollback,
concurrency, reservation, and real-service probes satisfy the applicable
adversarial floors. No touched enumerable interface element lacks a firing
test.

| ID | Severity | Location | Finding | Suggested disposition |
|----|----------|----------|---------|-----------------------|
| IR-1 | P1 | `simplebroker/cli.py:1084` | Independent review found that a missing `--keep-newest` value could bypass command-local JSON error output and fall back to argparse text. | **Resolved.** Normalize the missing value to the bounded post-parse validator; `tests/test_cli_write_output.py:145` proves both option positions. |
| IR-2 | P2 | `simplebroker/cli.py:1073` | A separate dash-leading invalid value such as `-1` or `--bogus` could likewise be parsed as a new option instead of a keep value. | **Resolved.** Attach the token to the option so the same validator owns it; `tests/test_cli_write_output.py:157` proves the JSON result and untouched target. |
| IR-3 | P3 | `extensions/simplebroker_pg/tests/test_pg_write_keep.py:46` | The first PostgreSQL concurrency probe signaled only that its contender thread had started, not that the real runner had attempted the blocked mutation. | **Resolved.** `_MutationAttemptRunner` signals at the real runner boundary; `extensions/simplebroker_pg/tests/test_pg_write_keep.py:192` uses that boundary for every named competing mutation. |

| Ratified judgment | Decision |
|--------------------|----------|
| Quiet/scalar write result | Keep the ordinary quiet CLI and scalar-ID Python result. Do not add a displaced count or success envelope solely for this option. |
| CLI canonicalization | Accept `007` as 7 without an in-band note because adding success commentary would break the quiet-write contract. Continue to reject signs, Unicode digits, separators, duplicates, missing values, zero, and values above 9999. |
| Application judgment | Expose the atomic pending-to-claimed mechanism, but do not infer which queues are semantically safe. The explicit dedicated single-producer warning is the trust boundary. |

Verdict: **no blocker.** IR-1 through IR-3 are resolved and covered by firing
tests. Runbook feedback: no new reusable principle candidate; the two
departures are consequences of this CLI's existing quiet-success contract,
not a general amendment to the agent-interface runbook.

## Rollout and Rollback

### Rollout

1. Land promoted spec text and green structural contract gates before
   implementation-link claims. Immediately afterward, add and observe the
   failing product probes locally; never land a knowingly red default suite.
2. Implement backend API v9 across core, built-in SQLite, PostgreSQL, and Redis
   in one compatibility slice. Mixed v8/v9 packages must fail closed.
3. Complete real SQLite/PostgreSQL/Valkey atomicity, contention, reservation,
   and performance probes before exposing the CLI flag as release-ready.
4. Run the full core and extension suites, package smoke tests, interface
   review, independent implementation review, and Weft source-checkout tests.
5. Use `uv run python bin/release.py all` for the coordinated release. Follow
   its extension/core tag order and verify exact published artifacts together.
6. Update Weft's floors and lock in its own authorized change after the
   compatible release exists. Its current ordinary `Queue.write(...)` calls
   need no source change; adoption of `keep_newest` is a separate Weft product choice.

Post-release success signals: no rise in ordinary write errors or latency;
keep-write pending counts match N at operation checkpoints; no Redis BUSY or
stuck reservation reports; no PostgreSQL deadlock increase; mixed-package
installs fail at the explicit API handshake; Weft's ordinary write and manager
suites remain green.

### Rollback

Before any use of `keep_newest`, code rollback is a coordinated package rollback to
the v8-compatible set; no schema rollback is needed. After a successful
keep-write, rolling back code does not unclaim displaced messages. If they have
not been vacuumed, an operator could recover them only through a separately
designed explicit state repair; this plan does not add one. If vacuum has run,
restore from a pre-use backend backup. Release notes must state this one-way
data consequence.

Do not attempt mixed package rollback. Stop clients, restore a coherent core
and extension set, and restore data only if the keep side effect itself must be
undone.

## Work Plan

### Task 0: Freeze baseline, answer possession gates, and inspect downstream

1. Record branch, HEAD, clean/dirty file inventory, package versions, backend
   API version, and first-party plugin versions.
2. Record the six comprehension answers above.
3. Reconfirm every named code/test/doc surface exists.
4. Inspect Weft's public SimpleBroker imports and every production
   `Queue.write` call. Record that current calls omit `keep_newest` and
   identify any wrapper whose type signature would reject the additive
   keyword. Record that Weft's completed liveness-custody design rejected the
   proposed `weft.state.tid_mappings` queue-per-tid redesign and that no Weft
   production call site is part of this release.
5. Run baseline targeted write, delivery, grammar, PostgreSQL, and Redis suites
   before changing contracts.

Stop if Weft or another first-party path implements its own `BrokerConnection`
or assumes the exact old `Queue.write` signature in a way not covered here.

### Task 1: Green spec promotion, then uncommitted red product probes

1. Apply the Proposed Spec Delta to specs 10, 11, 13, 16, 17, the registry,
   and specs index.
2. Add structural contract tests for every new clause and the intended
   README/kernel bindings. Keep this promotion slice green and free of false
   implementation-link claims.
3. Record the owner's 2026-09-02 implementation authorization, land the green
   spec-promotion slice separately when commits are authorized, and record its
   commit SHA as the implementation baseline. In an uncommitted implementation
   session, record the original SHA plus exact worktree files as the promotion
   baseline instead.
4. After promotion, add public behavior probes for the input and state
   matrices. Run them and record the expected failures against missing
   implementation. Keep them uncommitted until the matching implementation
   slice makes the default suite green, or land each probe with that slice.
5. Re-run the green documentation gates after the red probes are written to
   prove the contract structure remains valid.

Stop if review cannot agree that `claimed` may include rows never offered to
a consumer, or if the requested meaning changes from operation-scoped to
stored policy. A downstream production call site is not required for this
release; adoption remains out of scope.

### Task 2: Shared validation, Python surface, and backend API v9

1. Add one shared keep validator at the delivery boundary: exact `int`,
   value 1..9999. The CLI parses its digit grammar into that `int` first.
2. Add keyword-only `keep_newest` to `Queue.write`, `cmd_write`, and
   `BrokerConnection.write`; preserve defaults, return, cache, and output.
3. Advance core and first-party plugin API declarations to v9 and update exact
   compatibility gates, `simplebroker.ext` backend-authoring prose,
   `bin/release.py::BACKEND_API_MIN_CORE_VERSION`, and extension dependency
   floors.
4. Carry the normalized value through direct and SQL/Redis connections without
   adding a second execution path.
5. Make validation-order tests green, including corrupt/inaccessible targets
   and a stdin object that would fail if read.

Stop if the change requires a new public result type, an overload family, or a
new config value not authorized by the promoted spec.

### Task 3: Shared SQL, SQLite, and PostgreSQL atomicity

1. Add the SQL-contract trim statement and fire SQLite query-plan evidence.
2. Join trim to the current generated-write transaction after insert and
   before commit. Preserve timestamp-conflict retry ownership.
3. Add the PostgreSQL write-keep table-lock branch in the established
   meta-row-then-table order.
4. Fire the full behavior matrix on real SQLite and PostgreSQL, including
   active at-least-once batches and concurrent moves, exact inserts,
   broadcasts, ordinary writes, deletes, renames, and two keep-writers with
   different N values.
5. Add rollback injection only through the real runner/transaction path.
6. Record `EXPLAIN` evidence, the unrelated-queue writer stall during a 220k
   trim under the PostgreSQL table lock, and contention measurements against
   the baseline.

Stop if a transaction can commit with an interleaved pre-commit producer that
is neither included in the retained set nor ordered after the keep-write, or
if lock acquisition introduces a reproducible deadlock.

### Task 4: Redis atomic script, reservation safety, and performance

1. Recover stale batches through the existing preparation owner, then pass the
   active reserved key and normalized keep value into `WRITE_MESSAGE`.
2. Put every fallible state check, including reservation intersection, before
   the first Lua mutation. This includes Redis type preflight for every key
   touched and body existence for every displaced ID.
3. Extend `SM-REDIS-WRITE` transition data and map every script result code.
   Reservation conflict is retryable `OperationalError` without timestamp
   retry; a missing displaced body is non-retryable `IntegrityError`.
4. Make the BUSY reply's retryable classification explicit in
   `_translate_redis_error`.
5. Fire real-Valkey tests for no-op trim, large trim, timestamp retry,
   reservation intersection including an all-reserved lowest set, retained
   reservation, stale recovery, transport failure, concurrent consumers and
   producers during a trim, and unchanged ordinary one-EVAL behavior.
6. Run and record the ladder and activity-progress probe from Backend Design,
   including the server Lua-time-limit setting and the residual risk beyond
   the observed maximum.

Stop rather than batch if real service evidence contradicts the baseline at
the 220k observed maximum. Return the measurements and alternatives to the
owner.

### Task 5: CLI grammar, help, and agent-facing review

1. Add `--keep-newest` through the parser-construction grammar owner as a value-taking
   write option. Support separated and `=` values in every promised position.
2. Reject duplicate/malformed values before stdin and target work; preserve
   JSON error dialect after successful JSON parsing.
3. Pass normalized N to `cmd_write`. Preserve plain, timestamp, JSON, closed-
   stdout, and literal-body behavior.
4. Add parser inventory, complete token matrix, help snapshot, shell, and fuzz
   cases. Fuzzing must include `--`, registered foreign flags, missing values,
   Unicode digits, huge digit strings, and duplicate options.
5. Repeat the eleven-principle interface review with code line evidence,
   findings, ratified judgments, verdict, and runbook feedback.

Stop if preprocessing and argparse disagree on whether a token is an option or
message, or if any invalid spelling can read stdin or touch a target.

### Task 6: Documentation and invariant alignment

1. Apply the Proposed Root README Delta and Other Documentation Delta.
2. Update every claimed-state phrase that implies consume is the only source.
3. Update implementation mapping, state-machine inventory, backend guides,
   extension READMEs, and CHANGELOG.
4. Run two-way promise-equivalence checks from specs to README/kernel and back.
5. Record any intentionally retained wording difference and why it is not a
   semantic difference.

Stop if docs describe `keep_newest` as deleting rows, limiting physical size, or
persisting a queue cap.

### Task 7: Full verification, compatibility, and downstream proof

1. Run focused suites after each slice, then the full core, PostgreSQL, Redis,
   fuzz, static, docs, and release-gate commands below.
2. Build core and both extension artifacts; install only those artifacts into
   clean environments and prove exact API v9 compatibility.
3. Before invoking `bin/release.py all`, bump core package metadata to 8.1.0,
   PostgreSQL and Redis package metadata to 4.1.0, update their core floors to
   `>=8.1.0` and lock files, and prove release-map entry `9: "8.1.0"`. The
   `all` command does not accept `--version`; it releases the current
   unpublished metadata versions.
4. Run Weft against the SimpleBroker source checkout, then against the built
   artifact set. Prove existing ordinary writes are unchanged.
5. Run independent implementation review after SQL, Redis, and final combined
   slices. Resolve or explicitly disposition every finding.
6. Record exact SHAs, commands, outputs, residual risks, and performance data.

Stop if a test imports source when claiming artifact proof, if a backend is
mocked for a concurrency claim, or if a version mismatch degrades instead of
failing explicitly.

### Task 8: Release and closure

1. Ensure the plan, promoted specs, docs, implementation, tests, and release
   notes agree with no pending deviation.
2. Use the coordinated release driver and verify the exact published wheels,
   sdists, tags, versions, and backend API handshake.
3. Run post-publish clean-install probes and the selected Weft compatibility
   suite.
4. Record post-release signals and any owner-authorized downstream adoption.
5. Change the Status Index row and in-file status to `completed` only in the
   committed closure change after every gate is satisfied.

## Dependency Graph

```text
Task 0 baseline/downstream
        |
        v
Task 1 spec promotion + red probes
        |
        v
Task 2 shared surface + backend API v9
       / \
      v   v
Task 3 SQL/PG     Task 4 Redis
       \           /
        v         v
       Task 5 CLI/interface
              |
              v
       Task 6 documentation
              |
              v
       Task 7 full/artifact/downstream proof
              |
              v
       Task 8 release/closure
```

Task 5 may start its parser-only red tests after Task 1, but it does not become
green or reviewable until Task 2 provides the normalized command surface.

## Verification Commands and Evidence Floors

Focused commands may be narrowed during iteration. Before integration-ready:

```bash
uv run pytest \
  tests/test_delivery_contract_sb_delivery.py \
  tests/test_message_identity_contract_sb_id.py \
  tests/test_cli_contract_sb_cli.py \
  tests/test_cli_rearrange_args.py \
  tests/test_cli_write_output.py \
  tests/test_json_message_id_contract.py \
  tests/test_agent_kernel_contract.py \
  tests/test_write_returns_id.py \
  tests/test_write_visibility.py \
  tests/test_core_persistence_transition_tables.py \
  tests/test_custom_runner_integration.py \
  tests/test_property_queue_model.py \
  tests/test_python_library_api_contract_sb_api.py \
  tests/test_operations_contract_sb_ops.py \
  tests/test_backend_plugin_resolution.py \
  tests/test_ext_imports.py \
  tests/test_public_surface.py \
  tests/test_constants.py \
  tests/test_release_script.py

uv run pytest
uv run mypy simplebroker tests
uv run ruff check .
python3 bin/check-dom15-fixtures
bin/check-plan-context
git diff --check
```

Run the repository's documented PostgreSQL and Redis/Valkey real-service suite
drivers, including the named extension tests. Run the CLI fuzz suite and every
release-gate test that checks protocol versions, dependency floors, artifact
imports, or coordinated publication. Use `uv run python bin/release.py all
--dry-run` or the driver's documented non-publishing verification mode before
any authorized release.

Adversarial acceptance floors:

- empty, one-row, boundary `N-1/N/N+1`, large, and existing-claimed queues;
- missing/zero/negative/Unicode/duplicate keep values, plus `10000` and a
  huge nonzero digit string that must both be rejected without unbounded
  conversion, and Python digit strings rejected with `TypeError`;
- keep-writes at 100k and 220k displaced rows on every backend with a
  concurrent writer on an unrelated queue, recording stall and success;
- option-like message bodies with and without `--`;
- invalid config, missing target, corrupt target, unwritable target, and stdin
  that must not be read on invalid input;
- concurrent keep/ordinary write/move/exact insert/broadcast/claim;
- forced failure before trim, during trim, at commit, after commit publish, and
  at Redis transport boundaries;
- exact state inspection of pending, claimed, total, high-water, returned ID,
  activity hints, and reservations after every outcome.

Anti-mocking floor: SQLite files, PostgreSQL transactions/locks, and Valkey Lua
scripts/reservations remain real. A fake runner is acceptable only to force a
named failure edge after a separate real-backend success/rollback proof.

## Acceptance Criteria

1. The promoted specs own CLI grammar, write-time claim meaning, identity
   atomicity, Python/backend surfaces, and claimed metadata with firing tests.
2. `--keep-newest N` and `Queue.write(..., keep_newest=N)` have matching semantics on
   SQLite, PostgreSQL, and Redis/Valkey.
3. All invalid values fail at the promised pre-observation boundary.
4. At the keep-write linearization point, only the N highest pending IDs from
   prior pending plus new remain; all displaced pending rows are claimed.
5. Existing claimed rows and active batch reservations retain their stated
   semantics.
6. No failure produces a partial high-water, insert, or claim outcome on any
   backend. Known rejection/rollback produces none; outcome ambiguity may
   leave the complete unit committed.
7. Ordinary write behavior and output remain unchanged, including one Redis
   data EVAL and scalar committed-ID return.
8. PostgreSQL has real evidence for linearization and lock order against every
   pending-row producer named in the matrix.
9. Redis has real evidence for validation-before-mutation, reservation
   conflict including the all-reserved case, state-machine completeness, and
   measured trim cost at 100k and 220k displaced rows against the baseline.
10. README, agent kernel, Python/backend guides, implementation docs, extension
    READMEs, registry, invariant inventory, and CHANGELOG agree with the spec.
11. Backend API v9 artifacts install and operate only as a coherent set; Weft's
    existing ordinary-write paths pass against source and artifacts.
12. Independent reviews contain no unresolved blocker, the Deviation Log has
    no `pending` entry, and closure is committed before status becomes
    `completed`.

## Out of Scope

- `move --keep-newest`, `broadcast --keep-newest`, read/peek/watch keep modes
- stored queue retention or capacity configuration
- time-, byte-, or body-based selection
- producer blocking, backpressure, quotas, or overflow errors
- physical deletion in the keep transaction
- automatic unclaim/requeue or a new claim-state enum
- a displaced-count result or changes to write JSON
- applying keep to `insert_messages`, dump/load, or exact-ID writes
- a general Redis script batching framework
- a PostgreSQL queue-lock redesign absent measured need
- Weft adoption of keep for any particular queue, and the `tid_mappings`
  redesign itself, which is the pressure and not part of this plan
- `N` above 9999, or a ring-buffer log window
- a displaced-row ceiling, bounded convergence windows, or any second Redis
  script
- a new operator recovery command for already claimed/vacuumed messages

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| Plan Task 1 downstream gate | Require an accepted Weft queue-per-tid design and named production call site before spec promotion. | Gate removed before implementation; Weft adoption remains out of scope. | Weft's completed 2026-08-29 liveness-custody plan explicitly rejected per-TID queues. The product owner authorized SimpleBroker implementation on 2026-09-02 without a downstream-call-site prerequisite after separately settling theory fit. | No product-spec change; this changes only the implementation precondition. |
| Plan Task 8 steps 2 through 4 | Publish the coordinated package set and run post-publish proof before closing the implementation plan. | The owner directed the verified implementation, specs, docs, tests, and release metadata to close and commit without publishing. The coordinated release remains a separate external operation; only its dry-run was executed. | Publication changes external package, tag, and release state and is not required to make the implementation commit complete. Keeping it separate also preserves the release driver's exact-main and clean-worktree gates. | No product-spec change; release mechanics and versions remain as planned. |

## Execution Evidence

Append dated entries only after work and its evidence exist. Each meaningful
slice records changed files, commands, observed results, residual risk, review
findings, and the six possession answers. Do not record transient working-tree
state as durable plan fact.

### 2026-09-02 implementation baseline and possession gate

The owner authorized implementation after separately settling theory fit and
removing the obsolete downstream-call-site gate. Because commits were not
authorized, the spec-promotion baseline is committed source
`c1866de9a2f138cd1899149d9829134f4ba9f2f9` plus the uncommitted changes to
`docs/specs/00-specs-index.md`, `docs/specs/10-cli.md`,
`docs/specs/11-delivery.md`, `docs/specs/13-message-identity.md`,
`docs/specs/16-python-library-api.md`, `docs/specs/17-ops.md`,
`docs/specs/product-section-registry.md`, `README.md`,
`docs/agent-kernel.md`, and
`tests/test_delivery_contract_sb_delivery.py`. The focused structural gate was
`uv run pytest tests/test_delivery_contract_sb_delivery.py tests/test_cli_contract_sb_cli.py tests/test_message_identity_contract_sb_id.py tests/test_python_library_api_contract_sb_api.py tests/test_operations_contract_sb_ops.py -q`:
144 passed.

Possession answers before product-code work:

1. `N` counts pending rows after adding the generated row. Existing claimed
   rows do not count; the generated highest-ID row is retained.
2. “Recent” means integer public message-ID order under [SB-SELECT-5], not
   insertion order or backend iteration order.
3. PostgreSQL needs the table lock because move and low exact-ID insert can
   produce pending rows without taking the generated-write meta-row lock. The
   shared mutation lock supplies a commit-time serial order.
4. Redis may not claim a reserved ID because the ID still belongs to an open
   at-least-once batch. A displaced-set intersection is retryable contention;
   the script changes nothing rather than stealing or skipping the row.
5. Keep-write claims displaced rows; it does not delete them or install a
   policy. Vacuum may later delete them, and a later ordinary producer may
   raise the pending count above `N`.
6. The 9999 cap bounds only the cutoff walk. Work is linear in the number of
   displaced rows, which is driven by existing queue depth rather than `N`.

### 2026-09-02 public contract and implementation slice

Changed surfaces: the promoted `[SB-CLI-7]` / `[SB-DELIVERY-9]` contract and
linked identity, API, and ops clauses; root README, agent kernel, Python and
backend guides, invariant/state/identity/storage implementation notes,
CHANGELOG, and extension READMEs; `simplebroker/_delivery.py`,
`simplebroker/cli.py`, `simplebroker/commands.py`, `simplebroker/sbqueue.py`,
`simplebroker/db.py`, the backend protocol/export owners, and the shared SQL
contract. The structural, CLI, Queue, transaction, lifecycle, and property
tests were updated, with the main behavioral matrix in
`tests/test_keep_newest.py` and the opt-in ladder in
`tests/test_keep_newest_benchmark.py`.

Observed verification:

- `uv run pytest -q` exited 0 from the final code state. This includes the
  CLI grammar/property probes, shared SQLite behavior, rollback, maintenance,
  contract, version, and documentation gates; opt-in services and benchmarks
  retained their normal skips.
- The shared `tests/test_keep_newest.py` suite was then run against real
  PostgreSQL and real Valkey via `BROKER_TEST_BACKEND`; all 25 tests passed on
  each backend.
- `uv run ruff check .`, the repository-wide `ruff format --check`,
  `uv run mypy simplebroker`, both extension mypy checks, root and extension
  `uv lock --check`,
  `python3 bin/check-dom15-fixtures`, `bin/check-plan-context`, and
  `git diff --check` all exited 0.
- A combined tracked-and-untracked added-line scan for `noqa`,
  `type: ignore`, coverage exclusions, pylint suppressions, and fmt/type-check
  exclusions returned no match. No suppression was added for this feature.

The public result remains the exact new message ID. Invalid Python values fail
before opening the Queue connection; invalid CLI values, including missing,
duplicate, Unicode, huge, and dash-leading separate values, fail before stdin,
alias, or target observation. The independent review found two JSON-dialect
parser gaps after the first implementation. Both were reproduced, fixed, and
made firing cases at `tests/test_cli_write_output.py:145` and `:157`.

Residual risk: the flexible free-form write grammar has more normalization
logic than an option-only command would need. Its conservation, explicit `--`,
registered-token, missing-value, and property tests are therefore part of the
contract rather than incidental parser coverage.

### 2026-09-02 backend, contention, and performance slice

Changed backend surfaces: SQLite shared SQL; PostgreSQL SQL, lock preparation,
and `test_pg_write_keep.py`; Redis core, script protocol, transition table,
atomicity and real transport/publication tests. The PostgreSQL keep path locks
the high-water owner before `SHARE ROW EXCLUSIVE` on `messages`. Redis remains
one EVAL and checks key types, displaced bodies, and active reservations before
its first mutation.

Real-service verification:

- `SIMPLEBROKER_PG_TEST_DSN=... uv run --project
  extensions/simplebroker_pg pytest extensions/simplebroker_pg/tests -q`
  completed at 100 percent with six standard opt-in skips.
- `PYTHONPATH=extensions/simplebroker_redis
  SIMPLEBROKER_VALKEY_TEST_URL=... uv run --project
  extensions/simplebroker_redis pytest
  extensions/simplebroker_redis/tests -q` completed at 100 percent with one
  diagnostic opt-in skip.
- The named ladder command was `uv run pytest
  tests/test_keep_newest_benchmark.py -m benchmark -n 0 -s -q`, with the
  appropriate real-service environment for each extension. Every point also
  asserted exact pending, claimed, and total state.

Observed wall time in seconds, by pending depth before the keep-write:

| Backend | 0 | 1 | 1k | 10k | 100k | 220k |
|---------|---:|--:|---:|----:|-----:|-----:|
| SQLite, WAL, synchronous FULL | 0.000202 | 0.000143 | 0.000524 | 0.003917 | 0.045192 | 0.111042 |
| PostgreSQL 18 | 0.005259 | 0.004619 | 0.005193 | 0.032198 | 0.340216 | 0.750613 |
| Valkey 7.2 | 0.001383 | 0.001132 | 0.003904 | 0.016003 | 0.125337 | 0.324295 |

A separate PostgreSQL 220k cross-queue probe measured 0.814 seconds for the
keep-write and 0.791 seconds of wait for an unrelated raw insert. Both
committed, with keep state `(pending=5, claimed=220000, total=220005)`. Valkey
reported `lua-time-limit=5000`; `SLOWLOG` recorded the keep EVAL at 133,905
microseconds for 100k and 335,095 microseconds for 220k. No BUSY response was
observed. Large seeding scripts in the same slow log were excluded from these
figures.

The evidence supports the specified atomicity and linear-cost guarantee through
the observed 220k maximum. It does not establish a general time bound. Redis
blocks its server for the script duration; PostgreSQL blocks row mutations on
all queues through the coarse table lock. Deeper queues remain an explicit
residual risk, not a hidden displaced-row cap or convergence protocol.

### 2026-09-02 artifact, downstream, and independent-review slice

Changed release surfaces: core 8.1.0, PostgreSQL and Redis 4.1.0, backend API
v9, extension core floors, the release-map entry, and all three lock files.
No Weft file was changed.

Observed verification:

- `uv run ./bin/packaging-smoke --python 3.11` built and clean-installed the
  root wheel/sdist and both extension artifacts successfully.
- A separate clean Python 3.11 wheel-only install asserted all three versions,
  both plugins' `backend_api_version == 9`, a real Queue keep-write state, and
  an import path inside the temporary environment's `site-packages` rather
  than this checkout.
- Fresh PostgreSQL and Redis extension builds via `python -m build` completed
  successfully.
- Weft's architecture import-boundary, core manager, manager-command, and
  harness-registration modules passed first against this source checkout and
  then in a clean Python 3.14 environment containing the built SimpleBroker
  wheel set plus editable Weft. The artifact run again asserted that
  SimpleBroker imported from the temporary `site-packages`; three PostgreSQL-
  only cases retained their normal skips. The initial Python 3.11 Weft setup
  correctly failed its declared `Requires-Python >=3.12` constraint and was
  replaced by the valid 3.14 harness rather than treated as a product failure.
- `uv run python bin/release.py all --dry-run` exited 0. It recognized the
  coordinated unpublished 8.1.0/4.1.0/4.1.0 set and API floors, enumerated all
  pre-publication commands, and correctly warned that a real release would
  reject the current dirty, non-main worktree. No publish, tag, commit, or push
  occurred.

Independent implementation reviewer: GPT-5.5, read-only, against the same
working-tree delta and committed baseline.

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| E1 | P1 | Missing `--keep-newest` values could emit argparse text even after command-local `--json`. | Fixed by routing the missing value through the post-parse bounded validator; both placements now emit `INVALID_ARGUMENT` JSON and leave the target absent. |
| E2 | P2 | A dash-leading separate invalid value could still escape that JSON path. | Fixed by attaching it as the option value for validation; `-1` and `--bogus` are firing black-box cases. |
| E3 | P3 | The PostgreSQL mutation test originally proved contender thread start, not actual runner entry. | Fixed with `_MutationAttemptRunner`; the test now signals at the real mutation-call boundary for move, claim, exact insert, broadcast, ordinary write, delete, and rename. |

Reviewer verdict after fixes: no P0/P1 finding remained. The reviewer could not
run external services on its host, so the real PostgreSQL and Valkey evidence
above was run independently in the implementation session.

Possession was rechecked after each slice: (1) N describes the pending set at
that keep-write's own linearization point; (2) order is integer public ID; (3)
PostgreSQL's coarse table lock is necessary to serialize producers that do not
take the high-water lock, and its cross-queue cost is now measured; (4) Redis
rejects, rather than steals or skips, a displaced active reservation; (5)
displaced rows are claimed and later vacuumable, not immediately deleted; (6)
9999 bounds the retained cutoff walk but not displaced work. This recheck also
corrected one test assumption: after sustained writers with different N, the
final count can lie between their minimum and maximum values because a larger
N is a no-op when fewer than N rows are pending. Each individual commit still
satisfies its own upper window.

Before the closure authorization below, the worktree remained intentionally
uncommitted and the plan remained `active`. The release has not run.

### 2026-09-02 closure authorization and final gate

The owner directed this implementation and its separate configured-project-
filename diagnostic fix to close and commit together. The implementation
worktree was verified before closure with the full root suite, full real
PostgreSQL and Valkey extension suites, clean artifact installs, Weft source
and artifact compatibility, the 0 through 220k benchmark ladder, independent
review, and the release-driver dry-run recorded above.

After the project-config diagnostic fix, `uv run pytest -q` again exited 0;
`tests/test_project_config.py` passed all cases, including the non-default
`.weft/broker.toml` semantic-validation and SQLite-target diagnostics. `uv run
ruff check .`, repository-wide `ruff format --check`, `uv run mypy
simplebroker`, `python3 bin/check-dom15-fixtures`, `bin/check-plan-context`,
and `git diff --check` all exited 0. The added-line suppression scan remained
empty.

This change closes the implementation plan. No package publication, release,
tag, or push is part of the closing commit; those remain a separately
authorized release operation under the existing rollout and release driver.

### Executed behavior-matrix traceability

| Matrix case | Firing evidence |
|-------------|-----------------|
| Ordinary write unchanged | `tests/test_cli_write_output.py::test_write_default_is_silent`; `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_steady_state_ordinary_write_uses_one_data_eval` |
| Empty queue, N=1 | `tests/test_keep_newest.py::test_write_keep_newest_one_on_empty_queue_keeps_the_new_row` |
| Prior pending N-1 | `tests/test_keep_newest.py::test_write_keep_newest_is_noop_with_exactly_n_minus_one_prior_rows` |
| Prior pending N | `tests/test_keep_newest.py::test_write_keep_newest_claims_older_pending_rows` |
| Prior pending much greater than N | `tests/test_keep_newest.py::test_write_keep_newest_claims_every_row_outside_a_much_smaller_window` |
| Existing claimed rows excluded | `tests/test_keep_newest.py::test_write_keep_newest_excludes_existing_claimed_rows` |
| Numeric public-ID order, not insertion order | `tests/test_keep_newest.py::test_write_keep_newest_orders_by_public_id_not_insertion_time` |
| Complete Python type/range matrix | `tests/test_keep_newest.py::test_write_keep_newest_accepts_complete_python_boundary_matrix`; `::test_write_keep_newest_rejects_remaining_python_types_and_ranges` |
| CLI lexical/value/duplicate/missing matrix and pre-target boundary | `tests/test_cli_write_output.py::test_write_keep_newest_rejects_invalid_cli_value_before_target`; `::test_write_keep_newest_rejects_duplicate_values_before_target`; `::test_write_keep_newest_missing_value_uses_requested_json_error`; `::test_write_keep_newest_dash_leading_separate_value_uses_json_error` |
| Zero, 10000, huge digits, and `007` | `tests/test_cli_write_output.py::test_write_keep_newest_rejects_invalid_cli_value_before_target`; `::test_write_keep_newest_accepts_leading_zeroes`; `tests/test_keep_newest.py::test_write_keep_newest_rejects_remaining_python_types_and_ranges` |
| Literal write/broadcast body after `--` | `tests/test_cli_contract_sb_cli.py::test_sb_cli_7_contract_help_and_literal_escape`; `tests/test_cli_rearrange_args.py::TestArgumentProcessor::test_write_keep_newest_after_explicit_escape_is_literal` |
| Timestamp conflict retries the whole unit | `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py::test_redis_write_fires_transition_table`, case `KEEP-STALE-FENCE-REFRESH` |
| SQL trim/pre-commit rollback | `tests/test_custom_runner_integration.py::test_write_keep_rolls_back_insert_high_water_and_claims_together`; `tests/test_write_visibility.py::test_write_keep_claims_between_insert_and_commit` |
| Commit/transport ambiguity permits only the complete unit | `extensions/simplebroker_redis/tests/test_redis_core_behaviors.py::test_keep_write_transport_failure_after_real_eval_leaves_whole_unit_committed` |
| PostgreSQL competing producer/mutator serial history | `extensions/simplebroker_pg/tests/test_pg_write_keep.py::test_keep_write_serializes_every_pending_row_producer_and_queue_mutator` |
| PostgreSQL unrelated-queue stall | `extensions/simplebroker_pg/tests/test_pg_write_keep.py::test_keep_write_blocks_unrelated_queue_row_mutation_until_commit` plus the measured 220k probe above |
| Redis 100k/220k one-script exact state | `tests/test_keep_newest_benchmark.py::test_write_keep_newest_displacement_ladder`; `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_write_keep_newest_claims_all_older_pending_in_one_eval` |
| Redis displaced reservation, including all-displaced reserved | `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_write_keep_newest_rejects_displaced_active_reservation_atomically`; `::test_write_keep_newest_rejects_an_all_reserved_displaced_set_atomically` |
| Redis retained reservation | `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_write_keep_newest_allows_reservation_in_retained_window` |
| Concurrent claim/ordinary write and keep | `tests/test_keep_newest.py::test_keep_write_is_serial_with_consumer_and_ordinary_writer`; `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_write_keep_newest_is_serial_with_consumer_and_ordinary_writer` |
| Sustained keep writers with different N | `tests/test_keep_newest.py::test_sustained_keep_writers_with_different_windows_finish_serially`; `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_sustained_keep_writers_with_different_windows_are_serial` |
| Redis publish failure after commit | `extensions/simplebroker_redis/tests/test_redis_core_behaviors.py::test_keep_write_publish_failure_leaves_whole_unit_committed` |
| Maintenance failure after commit | `tests/test_message_claim.py::test_automatic_vacuum_failure_preserves_committed_keep_write` |
| Later ordinary producer may exceed N | `tests/test_keep_newest.py::test_later_ordinary_write_may_grow_past_the_prior_window` |
| Vacuum may remove displaced claimed rows | `tests/test_keep_newest.py::test_vacuum_may_physically_remove_rows_claimed_by_keep` |

## Independent Plan Review

Required before this draft is offered as implementation-ready. The reviewer
must existence-check every named surface first, then challenge:

- whether operation-scoped keep still fits [THEORY-1] and [THEORY-4];
- the expansion of claimed state beyond consume attempts;
- exact validation and CLI preparse behavior;
- PostgreSQL commit-time linearization and lock order;
- Redis reservation safety and validation-before-mutation;
- unbounded trim performance and the no-batching consequence;
- backend API/release/downstream sequencing;
- README/spec promise equivalence and the unchanged result-shape judgment.

Record reviewer identity/family, baseline, findings, dispositions, verdict, and
fresh-eyes check here. A review against an uncommitted draft cites the exact
diff base. A blocker leaves this plan `draft`.

### 2026-09-02 independent draft review

Reviewer: independent GPT-5.5 agent. Baseline: committed source
`c1866de9a2f138cd1899149d9829134f4ba9f2f9` plus the plan diff against that
base. The reviewer did not edit files and existence-checked the named repo and
Weft surfaces first.

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| R1 | P2 | Backend API v9 also changes the public version narrative in `simplebroker/ext.py:61` and the release invariant in `bin/release.py:109`, but the first draft named only generic release owners. | Accepted. Both exact owners now appear in Context, documentation work, Task 2, and verification. The plan requires the release map entry and its tests; the version was later revised to `9: "8.1.0"` under O3. |
| R2 | P2 | `bin/release.py all` rejects `--version` (`bin/release.py:2236`) and discovers current metadata versions (`bin/release.py:1846`), so the draft did not say how the bumped metadata versions (then 9.0.0/5.0.0, now 8.1.0/4.1.0 under O3) exist before release. | Accepted. Task 7 now requires explicit core/extension version, floor, and lock updates before dry-run or release. |
| R3 | P2 | The focused list omitted direct structural owners for identity, kernel, JSON IDs, ext/public exports, constants, and extension API-version declarations. | Accepted. The primary owner list and focused command now include the core structural suites; named extension init/validation tests remain mandatory through the documented real-service drivers. |
| R4 | P3 | A destructive delivery-state command appeared too early in Quick Start. | Accepted. Quick Start now gets a warning pointer only; the executable example follows the full Write Options warning. |
| R5 | P3 | Passing the 100,000-row Redis ladder does not prove arbitrary-size Lua safety. | Accepted. Evidence now records the actual Lua-time-limit setting and carries unmeasured larger-size risk into release evidence and notes. |

Verdict: **PASS; no P0/P1 blocker.** The plan remains `draft` pending product-
owner authorization of the proposed contract, not because of an unresolved
review finding.

Ratified judgments: operation-scoped keep fits [THEORY-1]/[THEORY-4]; claimed
may include explicit write-time trims if every teaching surface says so; the
existing quiet/scalar write result remains coherent; registered `--keep-newest`
requires `--` for literal write and broadcast bodies; PostgreSQL's coarse
first-version table lock is defensible with real contention/deadlock proof;
Redis must remain one atomic Lua operation rather than silently batching
(O4 below records a bounded-window proposal that I1 and I2 later superseded;
the final contract remains one atomic script).

Fresh-eyes result: all omissions found by the reviewer were the five items
above and are incorporated. Runbook feedback: no new reusable interface or
planning rule identified.

### 2026-09-02 owner review with Claude-family reviewer

Reviewer: Claude (Fable 5.1) with the product owner, after the GPT-5.5
review above. Baseline: the plan text that review saw, in the working tree
against `c1866de9`. The reviewer existence-checked the PostgreSQL lock order
(`plugin.py:871`, `_sql.py:278`), the Redis pending-set encoding and claim
windowing (`scripts.py:224`, `core.py:523`), `DELETE_QUEUE`'s unbounded
range, the release map in `bin/release.py:110`, and every Weft production
`Queue.write` call site.

| ID | Severity | Finding | Owner disposition |
|----|----------|---------|-------------------|
| O1 | P1 | No concrete pressure recorded. The nearest Weft pattern is a shared multi-manager registry queue, where keep would claim other managers' live rows; the per-manager heartbeat writes into an inbox that also carries commands. | Recorded the Weft `tid_mappings` queue-per-tid redesign as the pressure; added the dedicated single-producer rule to Product Fit, spec text, README safety, and help. Owner cautiously in favor, not sold. |
| O2 | P1 | `--keep N` is ambiguous across verbs and "N includes the new message" is an off-by-one trap. | Renamed to `--keep-newest N` and `keep_newest=`. |
| O3 | P2 | Major release unjustified: backend API v2 through v7 shipped in minor or patch releases; only v8 was a major, for the storage migration. | Core 8.1.0, extensions 4.1.0, release map `9: "8.1.0"`. |
| O4 | P2 | One unbounded Lua transfer on the write path. `DELETE_QUEUE` is precedent, but a script that has written cannot be killed, and bounding `N` does not bound displaced rows. | At this review stage, bounded-window convergence with reserved members skipped was proposed. I1 and I2 below superseded it; the final decision is one atomic script with pre-mutation reservation conflict. |
| O5 | P2 | `2**63` no-trim ceiling over-engineered. | `N` capped at 9999; digit strings accepted on both surfaces. Python string acceptance later withdrawn under I4 below. |
| O6 | P3 | Write-then-trim rejection overstated the race; over-retention by one is benign. | Rationale rewritten to one explicit call, one failure outcome, and application-space difficulty. |
| O7 | P3 | Claimed-state overload loses diagnostic signal; deletion offered as the alternative. | Owner retained claim for the include-claimed forensic window; deletion recorded as reconsidered and rejected. |

Verdict: **revised draft; still `draft`** pending owner authorization to
promote the spec delta. Fresh-eyes check: the GPT review's five findings
remain incorporated; R2 and Task 7 step 3 now carry minor version numbers.
Runbook feedback: candidate lesson for `docs/lessons.md`, to be recorded at
closure if it holds up, that a backend API version bump is a coordination
requirement and not by itself a semver signal.

### 2026-09-02 independent review of the revised draft

Reviewer: independent GPT-family agent, read-only, against the revised
working-tree draft above. Repository checks passed. Verdict as submitted: not
implementation-ready.

| ID | Severity | Finding | Owner disposition |
|----|----------|---------|-------------------|
| I1 | P1 | Reserved rows that stay pending make "pending at or below N" unreachable, and a lowest window of reserved rows livelocks without a cursor. | Accepted. Reservation intersection fails the write before mutation with a retryable error (decision 10); skipping withdrawn. |
| I2 | P1 | Bounded convergence is not the proposed atomic operation: multiple scripts, transient over-retention, durable partial cleanup, and consumers claiming superseded rows between windows. | Accepted. One script, strict atomicity restored (decision 9). The reviewer's displaced-row ceiling was then evaluated against measurements and existing contention recovery and not adopted (decision 15, Alternatives). |
| I3 | P1 | The PostgreSQL table lock is global action at a distance whose worst case was left to an implementation benchmark. | Accepted in substance. The blast radius is stated with measured figures (0.4 s at 100k, 1 s at 220k) in Backend Design, "no action at a distance" was reworded, and unrelated-queue stall is a firing test. |
| I4 | P2 | Digit-string rules contradicted each other, and `MessageIdInput` is not a precedent for a capped count. | Accepted. Python takes `int | None`; the CLI grammar strips leading zeroes then requires one to four digits. |
| I5 | P2 | The Weft pressure is prospective and the owner is not sold. | Accepted as a gate. Task 1 stops unless the queue-per-tid design is accepted with a named call site. Theory fit was judged separately and recorded in Product Fit. |

Additional owner decision recorded with this review: the level of guarantee
is atomicity plus linear cost, not bounded time (decision 15), on the basis
of the measured baseline and each backend's existing contention recovery.

Verdict at review time: **revised draft; still `draft`** pending owner
authorization to promote the spec delta and the then-current Task 1 downstream
gate. The owner subsequently authorized implementation and removed that gate;
see the Deviation Log. Fresh-eyes check: the
reviewer's four added test cases (all-reserved lowest set, consumer activity
during a trim, sustained concurrent writes, PostgreSQL cross-queue stall
duration) are in the matrix, the floors, and Tasks 3 and 4.
