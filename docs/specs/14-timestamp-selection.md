# Timestamp Selection

Status: Active

Owner: SimpleBroker ordered-selection surface: read, peek, and move bounds,
cross-backend result order, and watcher lower bounds. Exact message-ID identity
remains with `[SB-ID-*]`. Delivery claim/peek/move outcomes remain with
`[SB-DELIVERY-*]`. CLI string-to-integer bound parsing for non-exact forms is
`[SB-CLI-5]`.

Boundary: integer lower/upper bounds on message ID, the order of eligible
bounded results, the filter nature of bounds, and consequences when IDs arrive
later with values below a bound already used as a filter. Exact single-ID
targeting (`-m` / `message_id`) is `[SB-ID-4]`. Generator and live traversal
lifecycle remain with `[SB-API-*]` and `[SB-DELIVERY-*]`; this section fixes
their default order but does not add reverse live traversal.

## Integer predicates [SB-SELECT-1]

After any CLI string form is parsed to an integer (or when Python supplies an
integer bound), selection uses **strict open bounds**:

- `after_timestamp` means `message_id > after_timestamp`
- `before_timestamp` means `message_id < before_timestamp`

Together they select messages where
`after_timestamp < message_id < before_timestamp`.

These predicates apply to `read`, `peek`, and `move` surfaces that accept the
bounds, and to Python `after_timestamp` / `before_timestamp` arguments on the
corresponding APIs. `watch` does not take CLI `--before`; consume/peek
watchers may take an initial lower bound (`after_timestamp`) as a filter on
what they consider next.

## Filters, not stream offsets [SB-SELECT-2]

A lower or upper bound is a **pure filter** on which message ids are eligible
for that operation. It is **not**:

- a guarantee that nothing was written, moved, or exact-inserted with an id
  at or below the lower bound after the bound was chosen; or
- a durable, complete stream offset for a queue under concurrent writers,
  moves, or exact-id insertion.

Applications may **use** a last-seen id as a resume bound (for example
`--after <id>`). That is a filter choice. It does not promise that every
message that ever entered the queue will appear under that monotone bound.

## Bound advancement and late older ids [SB-SELECT-3]

If a consumer keeps an unchanged lower bound `L` (or advances it only to ids
it has already selected), any message with `message_id <= L` is **not
selected** by that filter. That is invisibility under the filter, not
physical deletion.

`move` preserves public id (`[SB-ID-5]`). Exact-id insertion may supply an
earlier valid id (`[SB-ID-4]`). Patterned broadcast may deliver a copy into a
queue selected after a consumer chose its bound. Any of these can place a
still-pending message into a queue with an id that is **behind** a bound the
consumer already uses. An
unfiltered scan, or a deliberately lower bound (including from the origin),
can still select such a pending row if it remains pending.

Queues that receive move traffic or exact-id imports are poor fits for a
single monotone “last id” resume strategy unless the application accepts that
filter incompleteness or periodically rescans with a lower bound.

## Watch progress [SB-SELECT-4]

`watch --peek` (and peek-mode `QueueWatcher`) delivers messages **as they
come**. After a successful handler dispatch, the watcher’s internal progress
advances so that message is not re-dispatched by that watcher under the same
progress rule; a failed dispatch does not advance that progress
(`[SB-DELIVERY-2]`).

Consume-mode watch that starts with an `after_timestamp` uses that value as a
lower bound filter on eligibility; it does not change claim semantics
(`[SB-DELIVERY-1]`).

## Cross-backend retrieval order [SB-SELECT-5]

After queue, claim-state, exact-ID, and open-bound predicates determine the
eligible set, `read`, `peek`, and `move` order eligible messages by the integer
public message ID. `oldest` means ascending ID and is the default; `newest`
means descending ID. Selection and the sequence returned by a bounded-many
operation use the same order on SQLite, PostgreSQL, and Redis. No operation may
depend on database-engine row order or SQL `RETURNING` order.

Ordinary generated writes retain insertion-order behavior because generated
IDs are monotone. Exact insertion, load/import, or ID-preserving move may add a
lower ID later; the lower ID is then selected earlier under `oldest`. `oldest`
and `newest` therefore describe public-ID order, not general insertion time or
wall-clock chronology.

Python one-message and bounded-many read/peek/move forms accept the closed
string choice `order="oldest" | "newest"`; any other value raises `ValueError`
before target acquisition or mutation. High-level forms accept the choice only
when `all_messages=False`; a non-default order with `all_messages=True` raises
the same pre-target error. Exact single-ID selection accepts either value but
has at most one observable result. Open `after_timestamp` and
`before_timestamp` bounds filter first and order the remaining set.

Generator, all-messages, stream, and watch forms expose only `oldest` and do
not accept a reverse-order control. Their traversal is ascending public message
ID. Reverse live traversal requires a separate cursor and concurrency contract
and is not implied by bounded `newest` selection.

## Implementation mapping

- Bound application on retrieve paths: `simplebroker/db.py`,
  `simplebroker/sbqueue.py`
- CLI flag wiring: `simplebroker/commands.py`, `simplebroker/cli.py`
- Watcher progress / initial bound: `simplebroker/watcher.py`
- CLI string forms for non-exact bounds: `[SB-CLI-5]`

## Verification

| Clause | Firing evidence |
|--------|-----------------|
| [SB-SELECT-1] | `tests/test_timestamp_selection_contract_sb_select.py::test_strict_open_bounds_on_queue_api`; `tests/test_after_flag.py::test_after_boundary_is_strictly_greater`; `tests/test_generator_methods.py::TestGeneratorMethods::test_generator_with_after_timestamp`; `tests/test_watcher.py::TestQueueWatcher::test_explicit_zero_after_timestamp_excludes_legacy_zero` |
| [SB-SELECT-2] | `tests/test_timestamp_selection_contract_sb_select.py` (structural + filter framing); README/kernel residual links |
| [SB-SELECT-3] | `tests/test_timestamp_selection_contract_sb_select.py`; `tests/test_move_by_id.py` (id preserved under move); move-behind-bound documentation bind |
| [SB-SELECT-4] | `tests/test_timestamp_selection_contract_sb_select.py::test_select_watch_progress`; `tests/test_watcher.py::TestQueueWatcher::test_peek_handler_failure_does_not_advance_checkpoint`, `tests/test_watcher.py::TestQueueWatcher::test_explicit_zero_after_timestamp_excludes_legacy_zero`; `[SB-DELIVERY-2]` |
| [SB-SELECT-5] | `tests/test_timestamp_selection_contract_sb_select.py::test_bounded_peek_orders_by_public_message_id`, `tests/test_timestamp_selection_contract_sb_select.py::test_bounded_one_and_many_order_matrix`, `tests/test_timestamp_selection_contract_sb_select.py::test_invalid_or_unbounded_order_fails_before_target_acquisition`, `tests/test_timestamp_selection_contract_sb_select.py::test_generator_signatures_do_not_expose_order`, `tests/test_timestamp_selection_contract_sb_select.py::test_direct_command_accepts_normalized_newest_order`, `tests/test_timestamp_selection_contract_sb_select.py::test_direct_command_rejects_newest_all_before_target_resolution`; `tests/test_sqlite_message_id_returning_order.py::test_claim_many_normalizes_sqlite_returning_rows_by_public_id`, `tests/test_sqlite_message_id_returning_order.py::test_claim_generator_uses_ascending_ids_when_returning_rows_are_reversed`, `tests/test_sqlite_message_id_returning_order.py::test_move_many_normalizes_sqlite_returning_rows_by_public_id`, `tests/test_sqlite_message_id_returning_order.py::test_move_generator_uses_ascending_ids_when_returning_rows_are_reversed`; `extensions/simplebroker_pg/tests/test_pg_message_id_order.py::test_postgres_retrieve_queries_order_and_address_by_public_id`, `extensions/simplebroker_pg/tests/test_pg_message_id_order.py::test_real_postgres_bounded_pending_selection_uses_timestamp_index`; `extensions/simplebroker_redis/tests/test_redis_message_id_order.py::test_redis_one_many_bounds_and_live_order_use_public_ids`, `extensions/simplebroker_redis/tests/test_redis_message_id_order.py::test_redis_newest_include_claimed_merges_both_states`, `extensions/simplebroker_redis/tests/test_redis_message_id_order.py::test_redis_newest_lua_resumes_below_reserved_windows`, `extensions/simplebroker_redis/tests/test_redis_message_id_order.py::test_redis_concurrent_newest_claims_select_distinct_highest_ids` |

## Related Plans

- completed: [2026-08-27-all-examples-correctness-and-contract-alignment-plan](../plans/2026-08-27-all-examples-correctness-and-contract-alignment-plan.md)
  — demonstrates bounded newest selection and complete oldest-only live scans
- active: [2026-08-27-message-id-order-and-newest-selection-plan](../plans/2026-08-27-message-id-order-and-newest-selection-plan.md)
  — owns [SB-SELECT-5], bounded newest selection, and its cross-backend proof

- retired: 2026-08-10-test-suite-signal-remediation-plan — source `0d15871`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-06-audit-remediation-plan — source `94e15bc`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-08-04-cmd-watch-locality-plan — source `5023710`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-07-30-product-documentation-cutover-plan — source `5023710`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-05-worker-portability-and-example-corrections-plan — source
  `6481ca08`; see the ledger in `docs/plans/README.md`
