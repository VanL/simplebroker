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
`docs/specs/10-cli-contract.md` [SB-CLI-1].

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

- retired: 2026-07-28-delivery-contract-spec-promotion-plan — source
  `36e2f356`; see the ledger in `docs/plans/README.md`
