# Delivery Contract Spec Promotion Plan

**Date:** 2026-07-28
**Status:** completed — implemented, verified, independently reviewed, and
authorized for targeted landing 2026-07-28
**Class:** **5** — adds normative product-spec text and promotes one registry
concern from `readme-only` to `canonical-spec`.
**Plan type:** implementation with spec revision.
**Hardening:** **required** — the plan narrows false or ambiguous published
cross-backend delivery wording and changes the governing public-contract
location. That fires [DOM-5]'s public-contract risk trigger even though
executable runtime behavior is unchanged. The checklist is integrated in
§§6, 8, 9, 11, and 17. If implementation discovers that runtime behavior must
change, revise the exact delta and repeat review before editing product code.
**Owner:** repository maintainers under explicit implementation authorization.
**Decision prerequisite:**
`docs/plans/2026-07-27-product-docs-source-ownership-decision.md`.
**Predecessor:**
`docs/plans/2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md`.

## 1. Goal

Promote the existing delivery, claim, peek, move-reservation, watch, and
transactional-generator safety contract into one canonical product spec,
`docs/specs/11-delivery.md` (`[SB-DELIVERY-1]` through
`[SB-DELIVERY-7]`).

This is an authority migration and contract characterization, not a behavior
change. The promotion must resolve the current README ambiguity around
peek-then-delete, preserve backend-specific finalization boundaries, and bind
every numbered clause to real public-path tests before the registry changes to
`canonical-spec`.

## 2. Requested Outcomes

- [x] Add the exact delivery spec in §7.1.
- [x] Promote the registry row atomically from `readme-only` to
      `canonical-spec`.
- [x] Make README and agent-kernel restatements link the canonical codes.
- [x] Correct the README's unqualified peek-then-delete recommendation:
      it is not a reservation and is safe only for a single consumer or
      idempotent handlers.
- [x] Bind every `[SB-DELIVERY-*]` clause to an implementation map and at
      least one firing test.
- [x] Prove shared semantics on SQLite, PostgreSQL, and Redis/Valkey where the
      contract is backend-neutral; keep SQL poison behavior and Redis's
      non-poison implementation boundary explicit.
- [x] Update the implementation inventory, cross-thread rationale document,
      specs index, `llms.txt`, and CHANGELOG in the same unit.
- [x] Keep message identity, claimed-row inspection/vacuum, Backstitch,
      hosted docs, and broad README restructuring out of this plan.

## 3. Premises and Scope Decisions

1. **`SB-DELIVERY` is the next product vertical.** The predecessor inventory
   names it as the next candidate, and the current registry still assigns the
   concern to the README.
2. **Existing behavior is the target.** This plan does not improve delivery
   semantics. It makes the current loss and duplicate windows and supported
   safe patterns precise enough that users and agents do not infer crash-safe
   processing from “exactly once.”
3. **The concern boundary is use-level delivery safety.** Message-ID
   preservation, checkpoint filtering, claimed-row inspection, dump/load, and
   vacuum behavior belong to future `SB-ID-*` or `SB-IO-*` verticals.
4. **Move-reservation is scoped to one broker target.** Public `Queue`
   destinations backed by a different target are rejected; this plan must not
   imply cross-target atomic movement.
5. **Same-thread use is the portable transactional-generator contract.**
   SQL backends enforce foreign-thread finalization with a poison lifecycle.
   Redis/Valkey does not share that SQL lock/transaction mechanism; its current
   tolerance is not promoted into a cross-thread-use guarantee.
6. **Backstitch remains separate.** The prior pilot satisfied the trigger for a
   future Backstitch plan, but adding that dependency or CI lane would mix
   trace-tool adoption with this bounded product-contract promotion.

## 4. Source Documents

### Governing process and ownership

- `docs/specs/01-development-documentation-operating-model.md`
  [DOM-4], [DOM-5], [DOM-6], [DOM-8], [DOM-10], [DOM-11], [DOM-15]
- `docs/plans/2026-07-27-product-docs-source-ownership-decision.md`
- `docs/specs/product-section-registry.md`
- `docs/agent-context/runbooks/writing-plans.md` §4b–4d
- `docs/agent-context/runbooks/writing-specs.md`
- `docs/agent-context/runbooks/testing-patterns.md`

### Current product authority and views

- Root `README.md`:
  - `## Critical Safety Notes`
  - `### Robust message handling with watch`
  - `## Real-time Queue Watching`
  - `### Pipe behavior`
  - `### Move Mode (--move)`
  - `### Delivery guarantees`
- `docs/agent-kernel.md`:
  - `## Delivery (use-level)`
  - `### Do not delete while draining a peek stream`
  - `## Choose a read model per queue role`
- `docs/implementation/04-cross-thread-finalization-poisoning.md`
- `docs/implementation/05-product-invariant-inventory.md`

### Implementation owners

- `simplebroker/_delivery.py` — closed selector vocabulary and validation
- `simplebroker/db.py` — SQL claim/peek/move operations, commit-before-yield,
  transactional batch rollback, and poison lifecycle
- `simplebroker/sbqueue.py` — public `Queue` delivery APIs and connection
  ownership
- `simplebroker/watcher.py` — consume, peek, and move watcher dispatch
- `simplebroker/commands.py` — CLI watch and streaming pipe behavior
- `extensions/simplebroker_pg/` — PostgreSQL runner behavior under the shared
  `BrokerCore`
- `extensions/simplebroker_redis/simplebroker_redis/core.py` — Redis
  reservation-token batch implementation and backend-specific finalization
  boundary

### Existing evidence to reuse

- `tests/test_exactly_once_delivery.py`
- `tests/test_generator_methods.py`
- `tests/test_watcher.py`
- `tests/test_watcher_concurrency.py`
- `tests/test_queue_move_watcher.py`
- `tests/test_move.py`
- `tests/test_move_claim_patterns.py`
- `tests/test_cli_broken_pipe.py`
- `tests/test_cross_thread_finalization_poisoning.py`
- `tests/test_cross_thread_generator_probe.py`
- `extensions/simplebroker_pg/tests/test_pg_cross_thread_generator_probe.py`
- `extensions/simplebroker_redis/tests/test_redis_batches.py`
- `extensions/simplebroker_redis/tests/test_redis_cross_thread_generator_probe.py`
- `tests/test_agent_kernel_contract.py`

### Downstream assumption evidence

The primary downstream, Weft, materially relies on same-target move
reservation:

- `../weft/weft/core/tasks/multiqueue_watcher.py`
  `MultiQueueWatcher._fetch_next_message()` maps reserve mode to `move_one()`;
- its consumer finalization deletes the exact reserved id after outcome
  publication;
- reserved-message policy supports keep, requeue-by-move, and clear-by-delete.

At survey time Weft pinned SimpleBroker 5.4.0 and its worktree was dirty.
Treat it as assumption evidence only. Do not edit it, run destructive commands
there, or use its locked test result as proof for this 5.6.0 plan. Because the
proposed contract is behavior-preserving, no Weft pin or code change is part
of this unit.

## 5. Spec Baseline

- **Baseline SHA:** `22422eccb732e1f0a371300fe882a21f0ce97b02`
  (`v5.6.0` at plan authoring).
- **Winning product contract at baseline:** root `README.md`, because the
  delivery registry row is `readme-only`.
- **Plan type:** implementation with spec revision.
- **Promotion strategy:** **B — atomic multi-file**. The new spec, registry
  transition, README/kernel links and corrections, implementation-doc
  alignment, and all firing gates land as one promotion slice.
- **Promotion baseline:** record the commit SHA after the atomic slice lands,
  or, for uncommitted review, record baseline `22422ecc` plus the exact
  worktree diff and rerun the named gates.
- **Promotion review baseline:** `ddb18f315db7772baa247d9d56efe656ffa0aa4f`
  (`Add exact broadcast queue provisioning`). It advanced `main` after plan
  authoring through shared README, kernel, CHANGELOG, and `db.py` paths.
  The delivery survey was reconciled against that commit, the delivery diff
  remained documentation-only in runtime files, and every named gate was
  rerun against the combined state.
- **Uncommitted delivery slice at completed-work review:** `CHANGELOG.md`,
  `README.md`, `docs/agent-kernel.md`,
  `docs/implementation/04-cross-thread-finalization-poisoning.md`,
  `docs/implementation/05-product-invariant-inventory.md`,
  `docs/plans/README.md`,
  `docs/plans/2026-07-28-delivery-contract-spec-promotion-plan.md`,
  `docs/specs/00-specs-index.md`,
  `docs/specs/11-delivery.md`,
  `docs/specs/product-section-registry.md`, `llms.txt`,
  `simplebroker/_delivery.py`, `simplebroker/db.py`,
  `simplebroker/sbqueue.py`, `simplebroker/watcher.py`,
  `tests/test_agent_kernel_contract.py`, and
  `tests/test_delivery_contract_sb_delivery.py`.

The implementer must rebase the baseline if `main` advances in any touched
delivery path. A changed baseline requires re-running the contract survey
before applying §7 verbatim.

## 6. Current Structure and Comprehension Gate

### Current behavior

- Default claim/read operations commit the claim before returning data.
  Consume-mode watchers therefore invoke the user handler after the message is
  already unavailable to another normal consumer.
- Same-target move changes queue ownership atomically and is the supported
  reservation primitive for concurrent workers.
- Peek observes pending data without reserving it. `peek_generator()` and
  `peek --all` use offset pagination over live mutable state; deleting or
  moving source rows while the stream is active shifts the window.
- Materialized `read_many()` / `move_many()` commit before returning even when
  passed `delivery_guarantee="at_least_once"`.
- Transactional generator mode defers a batch commit until the full batch has
  been yielded. Early close re-exposes the unfinished batch.
- SQL transactional generators hold thread-affine transaction and lock state.
  Foreign-thread finalization poisons that broker instance. Redis uses a
  reservation-token design and does not share the SQL poison mechanism.

### Required comprehension questions

Before editing, the implementer must be able to answer:

1. At what exact boundary does a default claim become irreversible to the
   caller, and why can a later handler failure not restore it?
2. Why is same-target `move` a reservation while `peek` followed by `delete`
   is not under concurrent consumers?
3. Why does deleting from the source during an offset-paged peek stream skip
   work even though each delete is individually correct?
4. Which semantics are portable across SQLite/PostgreSQL/Redis, and which
   poison behavior is deliberately SQL-specific?
5. Why does accepting `"at_least_once"` on a materialized batch not make that
   API transactional across caller processing?

If any answer is unclear or contradicts the code, stop and revise this plan
before writing normative text.

## 7. Proposed Spec Delta

### 7.1 New `docs/specs/11-delivery.md`

Create the file with this exact initial content:

```markdown
# Delivery Contract

Normative use-level delivery, reservation, peek, watch, and transactional
generator safety for the `broker` / `simplebroker` CLI and public `Queue` /
`QueueWatcher` APIs.

This spec governs message availability around broker delivery. It does not
promise crash-safe application processing, message-handler idempotency,
cross-target atomic movement, permanent claimed-row retention, or checkpoint
semantics. Those concerns remain with the application or their separately
registered product sections.

## Delivery risk model

| Pattern | Broker guarantee | Application failure window |
|---------|------------------|----------------------------|
| Consume / claim | One normal delivery of the committed claim | A crash or handler failure after claim can lose the work |
| Peek then delete | Message remains pending until delete | Concurrent consumers can duplicate work; a crash after the side effect but before delete can repeat it |
| Move to inflight, then delete | Same-target move reserves the message in another queue | Broker data remains in inflight after worker failure, but retry can repeat an already completed external side effect |
| At-least-once generator batch | Incomplete batch becomes available again | Items already yielded from an interrupted batch may be delivered again |

## Consume claim boundary [SB-DELIVERY-1]

Default `read` / claim operations and consume-mode `watch` make the message
unavailable to another normal consumer **before** returning it or invoking the
message handler.

Once that claim commits, a caller crash, handler error, or output failure does
not return the message to the pending queue. This is exactly-once claim
delivery (no second normal delivery of that claim), not crash-safe or
exactly-once application processing. Applications that require retry after
processing failure must use a reservation or transactional-generator pattern
defined below.

Claim is a logical delivery-state transition, not necessarily immediate
physical deletion. A claimed row may remain available to explicit inspection
until vacuum removes it, but ordinary pending delivery will not select it and
inspection creates no automatic recovery or redelivery promise.

Materialized claim APIs such as `Queue.read_many()` commit their selected
messages before returning the result list.

_Implementation mapping_:
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `simplebroker/watcher.py`
- `simplebroker/commands.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`

## Watch mode outcomes [SB-DELIVERY-2]

`watch` has three delivery modes:

- default consume mode follows [SB-DELIVERY-1];
- `--peek` observes without claiming, advances its in-memory checkpoint only
  after successful handler dispatch, and follows [SB-DELIVERY-4];
- `--move DEST` atomically moves each selected pending message to `DEST`
  within the same broker target before emitting it.

Handler completion and error-handler policy control watcher progress, not
broker acknowledgement. In consume and move modes, the broker state change has
already committed before handler dispatch. In peek mode, a successful dispatch
advances the in-memory checkpoint; a failed dispatch leaves the message
pending and does not advance that checkpoint. No handler or error-handler
outcome restores a committed consume claim.

| Mode when handler fails | Broker state |
|-------------------------|--------------|
| Consume | Message remains claimed and is not automatically returned to pending delivery |
| Peek | Message remains pending and the failed id does not advance the watcher's in-memory checkpoint |
| Move | Message remains in the destination queue; it is not moved back to the source |

_Implementation mapping_:
- `simplebroker/commands.py`
- `simplebroker/watcher.py`
- `simplebroker/sbqueue.py`

## Same-target move reservation [SB-DELIVERY-3]

For concurrent job workers, the supported reservation pattern is:

1. atomically move one pending message from the work queue to an inflight or
   worker-private queue on the **same broker target**;
2. process it from the destination queue;
3. delete it by exact message id after success;
4. on failure, leave it in inflight for retry or move it to an explicit
   dead-letter queue.

Only one concurrent same-target move can win a particular pending message.
The destination contains the moved message when the move returns. A `Queue`
object backed by a different broker target is rejected as a move destination;
this contract does not promise cross-target atomic movement.

The inflight destination must be worker-private or otherwise excluded from
ordinary rival consumption until the owning worker acknowledges or recovery
policy requeues it. Moving to a destination that another consume worker drains
immediately does not provide a durable acknowledgement window.

_Implementation mapping_:
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `extensions/simplebroker_pg/`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`

## Peek is observation, not reservation [SB-DELIVERY-4]

Peek APIs and peek-mode watch do not claim messages. Two concurrent peek
consumers may therefore observe and process the same pending message before
either deletes it. Peek-then-delete is safe only for a single consumer or
when duplicate processing is harmless through application idempotency.

`Queue.peek_generator()` and CLI `peek --all` are live, offset-paged streams,
not stable snapshots. While one is active, do not delete, move, or otherwise
remove rows from its source selection: removing earlier rows shifts the
offset window and may skip messages. A single consumer may instead repeat
one-message `peek` / `peek_one`, process, and delete by exact id.

This section governs pending-message delivery safety. Visibility and vacuum
of claimed rows through `include_claimed` belong to the separately registered
`SB-IO-*` concern.

_Implementation mapping_:
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `simplebroker/watcher.py`

## Delivery-guarantee selectors [SB-DELIVERY-5]

The closed `delivery_guarantee` vocabulary is `"exactly_once"` and
`"at_least_once"`. Unsupported values raise `ValueError` before any message
claim, move, or destination mutation and without creating a new backend
connection. Generator validation surfaces on first iteration rather than
construction.

Materialized `read_many()` and `move_many()` commit before returning.
Passing `"at_least_once"` to those APIs is accepted and is satisfied by their
stricter commit-before-return behavior; it does not extend a transaction
across caller processing.

For `read_generator()` and `move_generator()`:

- `"exactly_once"` commits each item before yielding it;
- `"at_least_once"` makes a batch available to the iterator and commits only
  after the entire batch has been yielded;
- graceful early close within an `"at_least_once"` batch makes the
  uncommitted batch available for retry, so already observed items may be
  delivered again;
- process failure may delay retry until backend recovery. SQL transaction
  cleanup occurs at connection/process teardown; Redis/Valkey reservations
  may remain unavailable until stale-batch recovery.

SQLite/PostgreSQL implement the batch with a database transaction. Redis/Valkey
implements the same public retry-on-stop outcome with reservation tokens.

_Implementation mapping_:
- `simplebroker/_delivery.py`
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`

## Transactional generator ownership [SB-DELIVERY-6]

The portable supported pattern for an `"at_least_once"` transactional
generator is to create, iterate, exhaust or explicitly close it on the same
thread. Do not abandon a live generator to garbage collection; use explicit
closure when a loop may exit early.

For SQL-backed broker instances, foreign-thread resumption or finalization
cannot safely release the owner thread's lock and transaction. The
implementation emits a best-effort `RuntimeWarning`, permanently poisons that
broker instance, and makes later poison checks raise non-retryable
`OperationalError` with the prefix `cross-thread finalization`. Recovery is a
process restart; the interrupted uncommitted batch is then available again.

Redis/Valkey does not use the SQL transaction-and-lock poison mechanism.
Its current non-poisoning finalization path does not create a portable
cross-thread-use guarantee; same-thread ownership remains the supported
public pattern.

_Implementation mapping_:
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `docs/implementation/04-cross-thread-finalization-poisoning.md`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`

## Closed-pipe delivery effects [SB-DELIVERY-7]

If the process consuming CLI stdout closes the pipe, `watch` stops at its next
delivery attempt and exits `0`. In consume mode, the message whose output
detects the closed pipe has already been claimed and is not restored; no
further messages are claimed. Exit `0` means the producer shut down cleanly,
not that the downstream consumer processed a particular message.

An `"at_least_once"` `read --all` stream instead closes its active
uncommitted batch and makes that batch eligible for retry under
[SB-DELIVERY-5].

The numeric exit-code set remains governed by
`docs/specs/10-cli.md` [SB-CLI-1].

_Implementation mapping_:
- `simplebroker/commands.py`
- `simplebroker/sbqueue.py`
- `simplebroker/watcher.py`

## Verification

| Clause | Firing gates |
|--------|--------------|
| [SB-DELIVERY-1] | `tests/test_delivery_contract_sb_delivery.py`; `tests/test_exactly_once_delivery.py`; `tests/test_watcher.py::TestErrorScenarios::test_consuming_watcher_queue_preservation_on_failure` |
| [SB-DELIVERY-2] | `tests/test_delivery_contract_sb_delivery.py`; `tests/test_watcher.py::TestQueueWatcher::test_peek_handler_failure_does_not_advance_checkpoint`; `tests/test_queue_move_watcher.py::TestQueueMoveWatcher::test_handler_failure_isolation`; `tests/test_queue_move_watcher.py::TestQueueMoveWatcher::test_transaction_safety` |
| [SB-DELIVERY-3] | `tests/test_delivery_contract_sb_delivery.py`; `tests/test_move.py`; `tests/test_move_claim_patterns.py` |
| [SB-DELIVERY-4] | `tests/test_delivery_contract_sb_delivery.py`; `tests/test_agent_kernel_contract.py` |
| [SB-DELIVERY-5] | `tests/test_delivery_contract_sb_delivery.py`; `tests/test_exactly_once_delivery.py`; `tests/test_generator_methods.py`; `extensions/simplebroker_redis/tests/test_redis_batches.py` |
| [SB-DELIVERY-6] | `tests/test_delivery_contract_sb_delivery.py` (structural binding); `tests/test_cross_thread_finalization_poisoning.py`; `tests/test_cross_thread_generator_probe.py`; `extensions/simplebroker_pg/tests/test_pg_cross_thread_generator_probe.py`; `extensions/simplebroker_redis/tests/test_redis_cross_thread_generator_probe.py` |
| [SB-DELIVERY-7] | `tests/test_cli_broken_pipe.py`; `tests/test_delivery_contract_sb_delivery.py` |

## Related Plans

- `docs/plans/2026-07-28-delivery-contract-spec-promotion-plan.md`
```

### 7.2 `docs/specs/product-section-registry.md`

Replace the delivery row with:

```markdown
| Delivery guarantees, claim/peek/watch safety | `canonical-spec` | `11-delivery.md` `[SB-DELIVERY-1]`…`[SB-DELIVERY-7]` | README Critical Safety / Delivery; agent-kernel Delivery | `tests/test_delivery_contract_sb_delivery.py` (SB-DELIVERY-1…7 + registry/README/kernel binds); `tests/test_cross_thread_finalization_poisoning.py` + backend probe suites (SB-DELIVERY-6); `tests/test_cli_broken_pipe.py` (SB-DELIVERY-7) |
```

Do not alter the `readme-only` state of message identity, dump/load and
claimed-row I/O, or embedding.

### 7.3 Specs indexes

In `docs/specs/00-specs-index.md`, add after the CLI entry:

```markdown
2. `11-delivery.md` — `[SB-DELIVERY-*]` (claim, watch, move
   reservation, peek safety, and transactional generators when the registry
   marks canonical)
```

`docs/specs/README.md` already defines the registry authority machine and
needs no policy change. Add a delivery example only if the independent review
finds the current generic text ambiguous; otherwise leave it untouched.

### 7.4 Root `README.md`

In the `Robust message handling with watch` heading (where `watch` is
formatted as code), replace the complete paragraph from the opening “When
using `watch`...” through its trailing `Example:` with:

```markdown
When using `watch` in its default consuming mode, messages are
**permanently removed** from the queue *before* your script or handler
processes them. If your script fails or crashes, **the message is lost**.
For critical work, prefer atomically moving each message to an inflight queue,
then deleting it there after successful processing. Peek-then-delete is not a
reservation: it is safe only for a single consumer or when duplicate handling
is idempotent. Do not delete or move source rows while iterating `peek --all`
or `Queue.peek_generator()`, because their live offset pagination can skip
messages.

Normative delivery contract:
`docs/specs/11-delivery.md` ([SB-DELIVERY-1]–[SB-DELIVERY-7]).

Single-consumer example:
```

Keep `permanently removed` as deliberate user-level shorthand for unavailable
to normal delivery. [SB-DELIVERY-1] carries the precise distinction between
logical claim and eventual physical vacuum. `Single-consumer example:` must
remain on its own line immediately before the retained code block.

Keep the existing shell example as a **single-consumer** example by changing
its leading comment to:

```bash
# safe-worker.sh - single-consumer peek-and-acknowledge example
```

Immediately below the comment, add:

```bash
# For concurrent workers, use move-to-inflight instead.
```

At the start of `### Delivery guarantees`, add:

```markdown
Normative detail:
`docs/specs/11-delivery.md` ([SB-DELIVERY-1]–[SB-DELIVERY-7]).
```

Do not restructure any other README section in this unit.

### 7.5 `docs/agent-kernel.md`

Immediately after `## Delivery (use-level)`, add:

```markdown
Normative: `docs/specs/11-delivery.md`
[SB-DELIVERY-1]–[SB-DELIVERY-7].
```

Keep the existing operational table and peek-stream warning as concise
restatements. Update wording only where a test demonstrates conflict with the
new spec.

### 7.6 `llms.txt`

Under `## Docs`, after the CLI contract spec entry, add:

```markdown
- [Delivery contract spec](docs/specs/11-delivery.md): Canonical claim, watch, move-reservation, peek-safety, and transactional-generator rules `[SB-DELIVERY-*]`
```

### 7.7 Implementation docs

Update `docs/implementation/04-cross-thread-finalization-poisoning.md`:

- replace the statement that README Delivery guarantees owns the public
  contract with `docs/specs/11-delivery.md`
  `[SB-DELIVERY-5]` and `[SB-DELIVERY-6]`;
- retain README as a user-facing restatement;
- keep Redis outside the SQL poison mechanism.

Update `docs/implementation/05-product-invariant-inventory.md`:

- change the delivery family to `canonical-spec`;
- point it to `docs/specs/11-delivery.md`;
- leave `SB-ID-*`, `SB-IO-*`, and `SB-EMBED-*` as `readme-only`;
- change “next vertical candidate” to a historical note that the vertical was
  promoted by this plan.

No new implementation-doc file is needed. The existing cross-thread rationale
and inventory already own the durable why and migration map.

### 7.8 `CHANGELOG.md`

Under the current release's Documentation subsection, record:

```markdown
- Promoted delivery, reservation, peek/watch safety, and transactional
  generator ownership into canonical `[SB-DELIVERY-*]` product specs. This is
  a documentation-authority migration and does not change runtime behavior.
```

### 7.9 Runtime-source documentation alignment

Update documentation inside runtime source without changing executable logic:

- `simplebroker/watcher.py`: replace the obsolete
  `DELETE...RETURNING` / “gone forever” consuming-mode explanation with the
  real claim lifecycle. State that the committed claim makes the message
  unavailable to normal delivery before handler dispatch, while the claimed
  row may remain physically present until vacuum. Make move-to-inflight the
  primary concurrent-worker guidance; qualify peek-then-delete as
  single-consumer/idempotent only.
- `simplebroker/_delivery.py`: add a module-level backlink to
  `docs/specs/11-delivery.md` [SB-DELIVERY-5] without changing the
  validator or exported vocabulary.
- `simplebroker/sbqueue.py`: in the public `read_generator()`,
  `move_generator()`, and transactional `stream_messages()` docstrings, keep
  same-thread use mandatory but qualify poison/restart as the SQL-backed
  behavior. State that Redis/Valkey does not share the SQL poison mechanism
  and that callers must not rely on backend-specific foreign-thread tolerance.
- `simplebroker/db.py`: apply the same backend qualifier to direct-core
  `claim_generator()` and `move_generator()` docstrings.

Do not change sidecar docstrings in this delivery vertical; Redis does not
expose SQL sidecar sessions, and sidecar lifecycle remains governed by
`docs/implementation/04-cross-thread-finalization-poisoning.md`.

The final AST documentation gate rejects unqualified SQL poison wording in
the touched generator docstrings. The source edit is documentation-only and
must not alter bytecode outside docstring constants.

### 7.10 Tests

Create `tests/test_delivery_contract_sb_delivery.py`. It must use public
`Queue` / `QueueWatcher` or real CLI paths and carry `pytest.mark.shared`
where the behavior is backend-neutral.

Required firing cases:

| Code | Required observable proof |
|------|---------------------------|
| SB-DELIVERY-1 | A claim/read makes the item unavailable before caller processing; a consume-watch handler failure leaves the failed item claimed while later untouched work remains pending. |
| SB-DELIVERY-2 | Spec structure names all three watch modes; focused shared watcher tests prove consume claim-before-handler, peek checkpoint-on-success, and move-before-handler. Do not mock watcher dispatch. |
| SB-DELIVERY-3 | Two same-target move attempts cannot both reserve one message; source loses it and destination owns it. A different-target `Queue` destination is rejected. |
| SB-DELIVERY-4 | Two independent peeks can observe the same pending id; peeking mutates nothing. A real small-batch `peek_generator()` with delete-as-you-go leaves at least one source message, proving why mutation during a live offset stream is unsupported. Do not assert an exact skipped count. |
| SB-DELIVERY-5 | The selector vocabulary is exactly the two documented values; invalid materialized input fails before target creation or state mutation, and invalid generator input does so on first iteration; materialized APIs commit before return; an early-closed at-least-once generator replays its unfinished batch. |
| SB-DELIVERY-6 | Structural test binds the clause to the existing real-thread/process-isolated SQLite and PostgreSQL poison probes and Redis non-poison probe. Do not replace those probes with an `RLock`, thread, or runner mock. |
| SB-DELIVERY-7 | Existing black-box pipe tests prove consume-watch's one-message loss window, clean exit, no later claims, and at-least-once `read --all` batch retry. Do not mock stdout or watcher dispatch. |

Also update:

- `tests/test_agent_kernel_contract.py` to require the delivery spec link and
  `[SB-DELIVERY-1]`…`[SB-DELIVERY-7]`;
- the new test to parse the registry row and assert that the canonical clause
  set and gate references are complete;
- the new test to assert the README and kernel both link the spec.

The test must parse structure, not pin cosmetic prose. Exact code sets, paths,
registry state, and observable queue transitions are stable assertions;
paragraph wording is not.

## 8. Invariants and Constraints

1. No executable runtime behavior, public method signature, CLI flag, exit
   code, or package export changes. Runtime-source edits are limited to
   correcting docstrings that conflict with current behavior.
2. “Exactly once” remains scoped to claim delivery. The spec must not promise
   exactly-once application processing.
3. `move` reservation claims apply only within one broker target. Do not imply
   cross-target atomicity.
4. Peek remains non-reserving. Do not bless concurrent peek-then-delete as a
   safe worker protocol.
5. Do not “fix” the offset-stream hazard in this plan. The firing test is a
   characterization of an unsupported mutation pattern, not a request for a
   second pagination design.
6. `include_claimed`, vacuum, claimed-row retention, and dump/load stay owned
   by the future `SB-IO-*` vertical.
7. Move identity preservation and move-plus-checkpoint skip semantics stay
   owned by the future `SB-ID-*` vertical.
8. SQL poison behavior must not be generalized to Redis. Redis tolerance must
   not be advertised as portable cross-thread support.
9. The registry transition, spec, README/kernel links, implementation-doc
   ownership update, and per-clause gates are one atomic promotion slice.
10. No Backstitch dependency, configuration, CI job, hosted-docs stack, or
    broad README collapse.

## 9. Authority Transition, Rollback, and Stop Gates

### Rollout

This is a documentation-only release unit:

1. land the atomic promotion slice;
2. publish the updated package/docs through the normal release process;
3. verify the PyPI/GitHub rendered README links resolve and the source tree's
   registry points to the shipped spec.

No staged runtime rollout or downstream pin bump is required because behavior
does not change.

### Rollback

- **Before release:** revert the atomic promotion commit, returning the
  registry row to `readme-only`.
- **After release:** correct the canonical spec in place, update its gates,
  README/kernel restatements, and CHANGELOG. Do not de-promote authority to the
  README as the primary repair.

### Stop and re-plan if

- any proposed sentence is false on a released backend;
- satisfying a clause requires product-code changes;
- same-target move is not atomic for a supported backend;
- the peek-stream characterization does not reproduce on one backend and the
  difference changes the portable contract;
- the Redis/SQL finalization boundary cannot be stated without promising
  unsupported cross-thread use;
- a clause pulls message identity, claimed-row lifecycle, or dump/load into
  this vertical;
- implementation introduces Backstitch or a new dependency;
- `main` changes any delivery implementation or test named in §4 before the
  promotion slice.

Any executable product-code correction remains Class 5 with the public-contract
risk trigger already active. Amend the plan, expand the hardening evidence,
update the exact spec delta, and repeat independent review before continuing.

## 10. Dependency-Ordered Tasks

### Task 1 — Independent review of the plan and exact delta

- Reviewer reads this entire plan, especially §7.1 and the backend boundaries.
- Reviewer reads the README/kernel loci, `simplebroker/db.py`,
  `simplebroker/sbqueue.py`, `simplebroker/watcher.py`,
  `simplebroker/commands.py`, Redis core, cross-thread implementation note,
  and named tests.
- Reviewer returns `PASS` or `BLOCKED` using the two-question plan-review
  standard in `review-loops-and-agent-bootstrap.md`.
- Author records every disposition in §14.
- **Done signal:** no unresolved blocker and no unverified rule-form sentence.

### Task 2 — Add red contract gates inside the atomic slice

- Create `tests/test_delivery_contract_sb_delivery.py` and update
  `tests/test_agent_kernel_contract.py`.
- Run the targeted test file before the spec/docs changes. Structural
  assertions must fail because the spec, canonical registry state, and links
  do not yet exist. Record the failure, satisfying red-green without inventing
  a runtime regression.
- Behavioral characterizations that already pass on baseline remain supporting
  evidence; do not force them to fail.
- **Done signal:** observed red is attributable only to the missing authority
  artifacts, and public-path behavior tests describe current behavior.

### Task 3 — Atomic spec-promotion slice (strategy B)

- Apply §7.1–§7.9.
- Keep the spec, registry, README/kernel links, implementation docs, CHANGELOG,
  and tests in one landing unit.
- Record the promotion baseline identifier in §5.
- Run the targeted SQLite gate from §12.
- **Done signal:** every structural test turns green, every numbered clause has
  a named firing gate, and no executable runtime behavior changed.

### Task 4 — Cross-backend contract proof

- Run the shared delivery test on PostgreSQL and Redis/Valkey.
- Run Redis reservation-batch tests.
- Run process-isolated SQL poison probes and the Redis non-poison boundary
  probe with their opt-in environment variable.
- Do not claim cross-backend completion if Docker/backend services are
  unavailable; record the missing environment as a blocker rather than
  weakening the canonical clause.
- **Done signal:** portable clauses pass on all released backends and
  backend-specific finalization results match [SB-DELIVERY-6], including
  backend-specific retry timing.

### Task 5 — Traceability reconciliation

- Confirm spec ↔ plan ↔ implementation docs ↔ implementation mappings ↔ tests.
- Confirm `llms.txt`, specs index, registry, README, kernel, and CHANGELOG point
  to the same code range.
- Run DOM fixtures, formatting checks, and link/structure greps.
- Evaluate whether plan/spec runbooks missed a reusable step; update them only
  in a separately classified change if the improvement is material.
- **Done signal:** no dangling authority claim, no unbound clause, and no
  `readme-only` delivery row remains.

### Task 6 — Completion review and plan closure

- Run an independent scoped review of the completed diff and current evidence.
- Resolve each finding.
- Flip this plan's Status Index row from `draft`/`active` to `completed` in the
  same change as the completion claim.
- Do not commit on behalf of a user who requested uncommitted review.
- **Done signal:** completed-work review passes, status index is closed, and a
  commit containing the final plan state is verified with `git log` before the
  work is called ready to land.

## 11. Testing Plan

### Anti-mocking posture

- Keep the broker, database/backend, real threads, subprocess pipes, and public
  `Queue` / `QueueWatcher` / CLI entry points real.
- Test doubles are acceptable only for deterministic handler outcomes or
  timing coordination around a real broker operation.
- Do not mock claim, move, peek pagination, transaction rollback, Redis
  reservation state, stdout writes, or poison-lock behavior.

### Coverage matrix

| Contract | SQLite | PostgreSQL | Redis/Valkey |
|----------|--------|------------|--------------|
| Claim-before-processing | shared public-path test | shared public-path test | shared public-path test |
| Watch modes / closed pipe | real CLI path | not rerun: backend-neutral CLI layer | not rerun: backend-neutral CLI layer |
| Same-target move reservation | shared move tests | shared move tests | shared move tests |
| Peek non-reservation / live-offset hazard | shared public-path test | shared public-path test | shared public-path test |
| Selector and retry-on-stop | shared generator tests | shared generator tests | shared + Redis batch tests |
| Foreign-thread finalization | SQLite process probe | PostgreSQL opt-in process probe | Redis explicit non-poison probe |

### Red-green rule

The first red is structural: the new test asserts an absent canonical spec,
registry state, and links. Runtime behavior is already shipped and should not
be made artificially wrong. If a behavioral characterization fails on the
baseline, the plan's premise is false: stop, log the mismatch, and re-plan
instead of changing code under this docs-only authorization.

## 12. Verification and Gates

### Plan-authoring gate

```bash
python3 bin/check-dom15-fixtures
git diff --check
```

### Targeted SQLite promotion gate

```bash
uv run pytest -n0 -q \
  tests/test_delivery_contract_sb_delivery.py \
  tests/test_agent_kernel_contract.py \
  tests/test_exactly_once_delivery.py \
  tests/test_generator_methods.py \
  tests/test_move.py \
  tests/test_cli_broken_pipe.py \
  tests/test_cross_thread_finalization_poisoning.py \
  tests/test_queue_move_watcher.py
```

Run the opt-in SQLite process probe with its required flag so the tests fire
rather than skip:

```bash
SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 uv run pytest -n0 -q \
  tests/test_cross_thread_generator_probe.py
```

Run the focused watcher failure proof separately to keep its node id explicit:

```bash
uv run pytest -n0 -q \
  tests/test_watcher.py::TestErrorScenarios::test_consuming_watcher_queue_preservation_on_failure \
  tests/test_watcher.py::TestQueueWatcher::test_peek_handler_failure_does_not_advance_checkpoint \
  tests/test_queue_move_watcher.py::TestQueueMoveWatcher::test_handler_failure_isolation \
  tests/test_queue_move_watcher.py::TestQueueMoveWatcher::test_transaction_safety
```

### PostgreSQL gate

```bash
uv run ./bin/pytest-pg -q \
  tests/test_delivery_contract_sb_delivery.py \
  tests/test_exactly_once_delivery.py \
  tests/test_generator_methods.py \
  tests/test_move.py

SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 uv run ./bin/pytest-pg -q \
  extensions/simplebroker_pg/tests/test_pg_cross_thread_generator_probe.py
```

### Redis/Valkey gate

```bash
uv run ./bin/pytest-redis -q \
  tests/test_delivery_contract_sb_delivery.py \
  tests/test_exactly_once_delivery.py \
  tests/test_generator_methods.py \
  tests/test_move.py \
  extensions/simplebroker_redis/tests/test_redis_batches.py

SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 uv run ./bin/pytest-redis -q \
  extensions/simplebroker_redis/tests/test_redis_cross_thread_generator_probe.py
```

### Final documentation and traceability gate

```bash
python3 bin/check-dom15-fixtures
git diff --check
rg -n "11-delivery|SB-DELIVERY" \
  README.md llms.txt docs tests
python3 - <<'PY'
import ast
from pathlib import Path

targets = {
    "simplebroker/db.py": {"claim_generator", "move_generator"},
    "simplebroker/sbqueue.py": {
        "read_generator",
        "move_generator",
        "stream_messages",
    },
}
for filename, names in targets.items():
    tree = ast.parse(Path(filename).read_text(encoding="utf-8"))
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names:
            found.add(node.name)
            doc = ast.get_docstring(node) or ""
            assert "Foreign-thread finalization permanently poisons" not in doc
            assert "SQL-backed" in doc and "Redis/Valkey" in doc
    assert found == names, (filename, found, names)
PY
git status --short
git log -1 --oneline
```

Success means:

- every command exits `0`;
- the registry has exactly one delivery row and it is `canonical-spec`;
- `[SB-DELIVERY-1]` through `[SB-DELIVERY-7]` are the exact clause set;
- each clause's gate fires on the surface it claims;
- README and kernel link rather than silently owning conflicting rules;
- no executable product logic or dependency manifest changed;
- backend-specific finalization results remain explicit.

## 13. Independent Review Loop

### Plan review

Prefer a different agent family from the author. Give the reviewer:

- this plan and its `## Proposed Spec Delta`;
- baseline `22422ecc`;
- the source documents and implementation owners in §4;
- the registry and invariant inventory;
- the named tests, including SQL and Redis finalization probes.

Prompt:

> Read the plan at
> `docs/plans/2026-07-28-delivery-contract-spec-promotion-plan.md`, including
> its exact proposed spec text and strategy-B atomic promotion. Review the
> current code and tests at baseline `22422ecc`. Do not implement. Look for
> false or overbroad delivery claims, missing backend distinctions, unsafe
> worker guidance, unbound clauses, redundant ceremony, and tasks a
> zero-context engineer could misapply. Answer PASS or BLOCKED based on:
> (1) could you implement it confidently and correctly, and
> (2) would implementation avoid degrading behavior, security, or robustness?
> A BLOCKED verdict must trace to one of those questions.

### Completed-work review

Use the scoped-change template from
`docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md` §4a against
the full promotion diff and current verification evidence. Scope expansions
become observations, not blockers, unless this change makes the concern worse.

## 14. Review Log

| Date | Reviewer | Verdict | Finding | Disposition |
|------|----------|---------|---------|-------------|
| 2026-07-28 | Claude Code (independent different-family review) | PASS | DCR-1 (P2): SQLite finalization commands could skip the probe and omitted the primary poison test. | Accepted: added the primary test to the SQLite gate and a flagged opt-in process-probe command. |
| 2026-07-28 | Claude Code (independent different-family review) | PASS | DCR-2 (P2): the matrix overstated PostgreSQL/Redis closed-pipe coverage. | Accepted: scoped the firing gate to the backend-neutral CLI layer on SQLite. |
| 2026-07-28 | Claude Code (independent different-family review) | PASS | DCR-3 (P3): invalid generator selectors fail on first iteration, and persistent queues already own a connection. | Accepted: corrected the clause and required test wording. |
| 2026-07-28 | Claude Code (independent different-family review) | PASS | DCR-4 (P3): the README splice could strand `Example:` and blur logical claim versus physical deletion. | Accepted: specified the full paragraph layout and documented the retained shorthand. |
| 2026-07-28 | Claude Code (independent different-family review) | PASS | DCR-5 (P3): the new structural test's SB-DELIVERY-6/7 attribution was inconsistent. | Accepted: aligned the spec table and registry row across all seven clauses. |
| 2026-07-28 | Claude Code (independent different-family review) | PASS | DCR-6 (nit): grep and AST gates duplicated the same docstring check. | Accepted: retained one AST gate and removed the duplicate grep instruction. |
| 2026-07-28 | Claude Code (independent different-family review) | PASS | DCR-7 (nit): interrupted-batch redelivery wording was ambiguous. | Accepted: clarified that already-yielded items may be delivered again. |
| 2026-07-28 | Claude Code (independent different-family review) | PASS | DCR-8 (nit): move-watcher evidence was missing from the source inventory. | Accepted: added `tests/test_queue_move_watcher.py`. |
| 2026-07-28 | Claude Code (independent different-family follow-up) | PASS | All DCR-1…8 dispositions were verified against the amended plan; no contradiction was introduced. Two non-blocking wording observations remained. | Accepted: removed closed-pipe evidence from SB-DELIVERY-2 and matched the README heading's inline-code spelling. |
| 2026-07-28 | Claude Code (independent different-family completed-work review) | PASS | CR-1 (P3): the concurrent broadcast plan's completion digest was refreshed from a worktree containing delivery docstrings before broadcast commit `ddb18f31`. | Out-of-scope observation: the broadcast commit landed during review; no delivery file depends on its attestation. Preserve the concurrent commit and surface the attribution concern to the repository owner. |
| 2026-07-28 | Claude Code (independent different-family completed-work review) | PASS | CR-2 (P3): plan closure requires a promotion baseline, completed index row, and authorized commit. | Resolved: recorded baseline `ddb18f31` and the exact slice; the owner then authorized a targeted landing commit, so this commit closes the plan and index row. |
| 2026-07-28 | Claude Code (independent different-family closure follow-up) | PASS | The delivery slice applies cleanly to `ddb18f31`; the 17-file manifest, process deviation, review dispositions, and runtime-docstring-only boundary are truthful. The authoring baseline's expanded SHA was transcribed incorrectly. | Accepted: corrected the v5.6.0 baseline to `22422eccb732e1f0a371300fe882a21f0ce97b02` before landing. |

## 15. Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| Process only (Task 2) | Run the new structural test before adding authority artifacts and observe the expected red. | The exact spec was added before the new structural test file; the first recorded run was green. Baseline inspection still proves `docs/specs/11-delivery.md` was absent at `22422ecc`, and no runtime failure was manufactured. | Implementation followed the exact-delta slice before the test-authoring slice. Record the sequencing miss honestly; all behavioral characterizations and final structural gates ran on SQLite, PostgreSQL, and Redis/Valkey. | None. Product behavior and the final contract are unchanged. |

No row may remain with `pending` in the final column at completion.

## 16. Out of Scope

- Runtime changes to claim, move, peek, watch, or generator implementations
- New retry, acknowledgement, lease, timeout, or dead-letter APIs
- Cross-target move support or distributed transactions
- Message identity, move-preserved IDs, and checkpoint skip semantics
- Claimed-row inspection, vacuum, dump/load, or physical retention
- Backstitch adoption or another new dependency
- Hosted docs
- Broad README progressive-disclosure work
- Sidecar behavior except where the existing cross-thread implementation note
  distinguishes it from delivery generators
- Weft/Taut code changes or dependency-pin changes

## 17. Fresh-Eyes Checklist

- [x] Every normative sentence is true on all backends in its stated scope.
- [x] Every backend-specific sentence names its boundary.
- [x] Every `[SB-DELIVERY-*]` code has a public-path firing gate.
- [x] The peek-stream hazard test avoids pinning an exact skipped count.
- [x] “Exactly once” never expands into exactly-once application processing.
- [x] Move atomicity never expands beyond one broker target.
- [x] Redis non-poisoning is not presented as supported cross-thread use.
- [x] No task asks the implementer to change runtime behavior.
- [x] The authority transition is atomic and reversible before release.
- [x] The plan does not pull future `SB-ID-*` or `SB-IO-*` concerns forward.
- [x] Verification commands identify the real backend harnesses and opt-in
      probes.
- [x] Independent review dispositions are recorded before implementation.

## 18. Completion Criteria

- Plan review is `PASS`.
- The strategy-B promotion slice is implemented without runtime changes.
- The promotion baseline identifier is recorded.
- All seven clauses have firing gates and pass on their declared backends.
- README, kernel, registry, indexes, implementation docs, and CHANGELOG align.
- Completed-work review has no unresolved blocker.
- The Status Index row is `completed`.
- The final state is committed only when the user authorizes landing; otherwise
  the handoff names the uncommitted files and does not claim completion.

Completion evidence recorded 2026-07-28:

- targeted SQLite promotion suite and four explicit watcher nodes passed;
- opt-in SQLite finalization probes passed;
- PostgreSQL shared delivery suite and opt-in poison probes passed;
- Redis/Valkey shared delivery suite, batch tests, and non-poison probe passed;
- full root suite passed: 1,982 tests with 17 expected skips;
- `python3 bin/check-dom15-fixtures` and the runtime-docstring AST gate passed;
- `uv run ruff check .` passed;
- `uv run ruff format --check .` passed after formatting the new test file;
- independent completed-work review and closure follow-up both returned
  `PASS`.
