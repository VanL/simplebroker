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
Moving a claimed row changes only its queue binding. It remains claimed and is
not selected for ordinary pending delivery at the destination.

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

The table describes broker state after message-handler failure. The
error-handler continuation and terminal-failure rules are [SB-API-6]. If the
error handler itself raises an ordinary exception, the watcher stops before
another dispatch in every mode; it does not undo the already-committed consume
claim or move, any state already materialized by an existing batch boundary,
and it does not advance peek progress past the failed id.

_Implementation mapping_:
- `simplebroker/commands.py`
- `simplebroker/watcher.py`
- `simplebroker/sbqueue.py`

## Move reservation [SB-DELIVERY-3]

Move is **atomic** and **non-consuming** in the sense that after a successful
move the message still exists (in the destination queue) with the same public
id. A crash after a successful move does not lose the message the way a
consume-claim can.

Move preserves the selected row's delivery state: a pending row remains
pending and a claimed row remains claimed. `require_unclaimed` changes which
rows may be selected; it does not release a claim. Move is not a requeue or
claim-release operation.

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

`Queue.peek_generator()` returns a single-use closeable iterator. Creating the
iterator is lazy and starts no Queue operation. Its first advancement attempt
enters one iterator-owned Queue operation and establishes the owner thread.
While active, the iterator must be advanced, exhausted, and closed on that same
thread; cross-thread use is unsupported, and callers must not rely on garbage
collection for cleanup.

Advancing the iterator through `StopIteration`, an exception raised by an
advancement attempt (including validation or backend failure), or an explicit
same-thread `close()` synchronously exits that Queue operation and invokes its
owned cleanup before the action returns or raises. An exception raised by the
caller's loop body does not advance or terminate the iterator; the caller must
close it in `finally`.

Closing before first advancement acquires no Queue operation and makes the
single-use iterator terminal, so a later `next()` raises `StopIteration`.
Closing after a terminal outcome or more than once is safe. A caller that may
stop early must close the iterator before closing its Queue or higher-level
client.

Operation exit does not close resources owned by another lifecycle: a
persistent Queue may retain its cached process session, core, or backend
checkout; an ephemeral Queue releases its operation-owned connection/core
handle; and a Queue with a caller-supplied runner retains its cached
connection/core handle until `Queue.close()` without closing or shutting down
the runner. These lifecycle rules do not change the live, offset-paged
traversal or strengthen peek into a snapshot, claim, or exhaustive concurrent
traversal.

Replacing the offset with the public message ID or the current storage
sequence would not by itself make this traversal complete under concurrent
mutation. Exact insertion may put an older public ID behind an advanced
`(timestamp, id)` cursor. Move re-homes a row in place while preserving both
its public ID and current internal sequence, so a moved-in row may also land
behind a cursor on either ordering. Callers needing one bounded observation
should use a materialized peek; a future exhaustive concurrent traversal
would first need to choose and specify fixed-start, live-rescan, or snapshot
semantics.

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

## Message and queue-name constraints [SB-DELIVERY-8]

Constraints a caller must satisfy for a write to be accepted, and the one
place they differ by backend.

**Queue names.** Non-empty, at most 512 characters
(`MAX_QUEUE_NAME_LENGTH`), containing only ASCII letters, digits,
underscore, period, and hyphen, and beginning with an ASCII letter, digit,
or underscore. Non-ASCII letters are not accepted. Violations raise
`QueueNameError`.

**Message bodies** are Python strings containing UTF-8 text. A non-string
body, or a string that is not UTF-8 encodable (including a lone surrogate),
raises `MessageError` before message, high-water, alias, or broadcast-target
mutation.

**Message size** is limited to 10 MB by default; override with
`BROKER_MAX_MESSAGE_SIZE`. Oversized bodies raise `MessageError`.

**NUL bytes diverge by backend.** SQLite and Redis round-trip a raw NUL
(`\x00`) in a body. The PostgreSQL backend rejects it at write time with
`OperationalError` and stores nothing; the queue remains usable. Code that
must be portable across backends should not put raw NUL in bodies.

_Implementation mapping_:
- `simplebroker/db.py` (queue-name validation, message-size validation)
- `simplebroker/_exceptions.py`
- `extensions/simplebroker_pg/`
- `extensions/simplebroker_redis/simplebroker_redis/core.py`

## Verification

| Clause | Firing gates |
|--------|--------------|
| [SB-DELIVERY-1] | `tests/test_delivery_contract_sb_delivery.py`; `tests/test_exactly_once_delivery.py`; `tests/test_watcher.py::TestErrorScenarios::test_consuming_watcher_queue_preservation_on_failure` |
| [SB-DELIVERY-2] | `tests/test_delivery_contract_sb_delivery.py`; `tests/test_watcher_error_handler_contract.py` (consume, peek, and move terminal-callback matrix); `tests/test_watcher.py::TestQueueWatcher::test_peek_handler_failure_does_not_advance_checkpoint`; `tests/test_queue_move_watcher.py::TestQueueMoveWatcher::test_handler_failure_isolation`; `tests/test_queue_move_watcher.py::TestQueueMoveWatcher::test_transaction_safety` |
| [SB-DELIVERY-3] | `tests/test_delivery_contract_sb_delivery.py`; `tests/test_move.py`; `tests/test_move_by_id.py`; `tests/test_move_claim_patterns.py`; first-party PostgreSQL and Redis exact-ID move tests |
| [SB-DELIVERY-4] | `tests/test_peek_generator_lifecycle.py`; `tests/test_delivery_contract_sb_delivery.py::test_live_peek_stream_rejects_naive_cursor_completeness`, `::test_closeable_peek_lifecycle_contract_is_bound_to_real_backends`; `tests/test_agent_kernel_contract.py` |
| [SB-DELIVERY-5] | `tests/test_delivery_contract_sb_delivery.py`; `tests/test_exactly_once_delivery.py`; `tests/test_generator_methods.py`; `extensions/simplebroker_redis/tests/test_redis_batches.py` |
| [SB-DELIVERY-6] | `tests/test_delivery_contract_sb_delivery.py` (structural binding); `tests/test_cross_thread_finalization_poisoning.py`; `tests/test_cross_thread_generator_probe.py`; `extensions/simplebroker_pg/tests/test_pg_cross_thread_generator_probe.py`; `extensions/simplebroker_redis/tests/test_redis_cross_thread_generator_probe.py` |
| [SB-DELIVERY-7] | `tests/test_cli_broken_pipe.py`; `tests/test_delivery_contract_sb_delivery.py` |
| [SB-DELIVERY-8] | `tests/test_delivery_contract_sb_delivery.py`; `tests/test_property_queue_names.py`; `tests/test_message_size_contract.py::test_non_string_bodies_raise_message_error_before_any_mutation`; `tests/test_property_message_roundtrip.py::test_lone_surrogate_bodies_raise_message_error`, `::test_nul_byte_bodies_pinned_per_backend` (shared, per-backend NUL stance) |

## Related Plans

- completed: [2026-08-24-peek-generator-close-contract-plan](../plans/2026-08-24-peek-generator-close-contract-plan.md)
  — closeable peek iterator and same-thread synchronous Queue-operation cleanup
- completed: [2026-08-24-comprehensive-review-findings-remediation-plan](../plans/2026-08-24-comprehensive-review-findings-remediation-plan.md)
  — preserves pending/claimed delivery state while changing only queue binding
- completed: [2026-08-24-failure-path-and-contract-findings-resolution-plan](../plans/2026-08-24-failure-path-and-contract-findings-resolution-plan.md)
  — terminal watcher callback-failure delivery state at baseline `1b8ecfa0`
- retired: 2026-08-23-correctness-and-concurrency-review-remediation-plan —
  source `23d6c9d1` (local-only pin); see the ledger in
  `docs/plans/README.md`
- retired: 2026-08-10-test-suite-signal-remediation-plan — source `0d15871`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-06-audit-remediation-plan — source `94e15bc`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-08-04-cmd-watch-locality-plan — source `5023710`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-08-04-worker-example-error-handling-plan — source `695dc16a`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-05-worker-portability-and-example-corrections-plan — source
  `6481ca08`; see the ledger in `docs/plans/README.md`
- retired: 2026-07-28-delivery-contract-spec-promotion-plan — source
  `197629e2`; see the ledger in `docs/plans/README.md`
