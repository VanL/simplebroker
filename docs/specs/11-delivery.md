# Delivery

Normative use-level delivery for the `broker` / `simplebroker` CLI and public
`Queue` / `QueueWatcher` APIs: claim, peek, watch, move reservation, and
generator delivery modes.

## Consume claim boundary [SB-DELIVERY-1]

Default `read` / claim operations and consume-mode `watch` claim the message
**before** returning it or invoking the handler.

A message is **claimed exactly once** under normal consume delivery (atomic).
If the process does not fail after the claim commits and before the message is
**delivered** to the caller or handler, that claim is **delivered exactly
once**. A crash or abort in that window can leave the message claimed and not
handed off; it is not automatically returned to pending. This is not a promise
of exactly-once application processing or external side effects.

Claimed rows are not selected again for ordinary pending delivery. They may
remain visible to explicit inspection until vacuum removes them.

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
- `--peek` observes without claiming and delivers as messages come; the
  watcher’s internal progress advances after successful handler dispatch and
  does not advance on handler failure;
- `--move DEST` atomically moves each selected pending message to `DEST`
  before emitting it ([SB-DELIVERY-3]).

In consume and move modes, the broker state change has already committed before
handler dispatch. No handler outcome restores a committed consume claim.

| Mode when handler fails | Broker state |
|-------------------------|--------------|
| Consume | Message remains claimed |
| Peek | Message remains pending; progress does not advance for that id |
| Move | Message remains in the destination queue |

_Implementation mapping_:
- `simplebroker/commands.py`
- `simplebroker/watcher.py`
- `simplebroker/sbqueue.py`

## Move reservation [SB-DELIVERY-3]

Move is **atomic** and **non-consuming** in the sense that after a successful
move the message still exists (in the destination queue) with the same public
id. A crash after a successful move does not lose the message the way a
consume-claim can.

Concurrent same-database moves: only one winner for a given pending message.
The destination holds the message when the move returns. A `Queue` destination
on a different database/target is rejected.

A common worker pattern is move to a private or inflight queue, process, then
delete by id (or dead-letter on failure). That is application structure on top
of atomic move, not a second delivery mode.

_Implementation mapping_:
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `extensions/simplebroker_pg/`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`

## Peek is observation [SB-DELIVERY-4]

Peek APIs and peek-mode watch do not claim messages. Multiple callers may
observe the same pending message. Mutating actions on a message (delete, move,
claim) are atomic: one winner.

`Queue.peek_generator()` and CLI `peek --all` are live, offset-paged streams.
Removing rows from the source while such a stream is active can shift offsets
and skip messages. One-message peek, process, delete-by-id avoids that.

_Implementation mapping_:
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `simplebroker/watcher.py`

## Delivery-guarantee selectors [SB-DELIVERY-5]

The `delivery_guarantee` values are `"exactly_once"` and `"at_least_once"`.
Unsupported values raise `ValueError` before message claim, move, or
destination mutation (generators validate on first iteration).

**exactly_once** (default for generators): one message at a time; commit before
yield — the consume claim boundary of [SB-DELIVERY-1] per item.

**at_least_once** (batch): the strongest public promise for batch processing.
A batch is made available to the iterator and commits only after the entire
batch has been yielded. Graceful early close within a batch makes the
uncommitted batch available again, so items already observed in that batch may
be delivered again. Materialized `read_many()` / `move_many()` commit before
return; passing `"at_least_once"` there is accepted and satisfied by that
stricter commit-before-return behavior.

`Queue.stream_messages(...)` with batch processing uses the same delivery
vocabulary where applicable.

_Implementation mapping_:
- `simplebroker/_delivery.py`
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`

## Transactional generator ownership [SB-DELIVERY-6]

Create, iterate, exhaust, and close an `"at_least_once"` transactional
generator on the **same thread**. Do not abandon a live generator to garbage
collection; close explicitly when a loop may exit early.

Crossing threads is **undefined behavior**. Implementations may fail loudly
(warning, poison, error) rather than silently corrupt state. That is a safety
net, not a supported multi-thread API; backends may differ in how they react.

_Implementation mapping_:
- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `docs/implementation/04-cross-thread-finalization-poisoning.md`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`

## Closed-pipe delivery effects [SB-DELIVERY-7]

If the process consuming CLI stdout closes the pipe, `watch` stops at its next
delivery attempt and exits `0`. In consume mode, the message whose output
detects the closed pipe has already been claimed and is not restored; no
further messages are claimed. Exit `0` means the producer shut down cleanly.

An `"at_least_once"` `read --all` stream closes its active uncommitted batch
and makes that batch eligible for retry under [SB-DELIVERY-5].

Exit codes remain governed by `docs/specs/10-cli.md` [SB-CLI-1].

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
