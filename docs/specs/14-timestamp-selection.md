# Timestamp Selection

Status: Active

Owner: SimpleBroker ordered-selection surface (read/peek/move filters and
watcher lower bounds). Exact message-id identity remains with `[SB-ID-*]`.
Delivery claim/peek/move outcomes remain with `[SB-DELIVERY-*]`. CLI
string-to-integer bound parsing for non-exact forms is `[SB-CLI-5]`.

Boundary: integer lower/upper bounds on message id for selection, the filter
nature of those bounds, and consequences when ids arrive later with values
below a bound already used as a filter. Cross-backend result **order** is not
owned here. Exact single-id targeting (`-m` / `message_id`) is `[SB-ID-4]`.

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

## Implementation mapping

- Bound application on retrieve paths: `simplebroker/db.py`,
  `simplebroker/sbqueue.py`
- CLI flag wiring: `simplebroker/commands.py`, `simplebroker/cli.py`
- Watcher progress / initial bound: `simplebroker/watcher.py`
- CLI string forms for non-exact bounds: `[SB-CLI-5]`

## Verification

| Clause | Firing evidence |
|--------|-----------------|
| [SB-SELECT-1] | `tests/test_timestamp_selection_contract_sb_select.py`; `tests/test_after_flag.py`; `tests/test_generator_methods.py::TestGeneratorMethods::test_generator_with_after_timestamp` |
| [SB-SELECT-2] | `tests/test_timestamp_selection_contract_sb_select.py` (structural + filter framing); README/kernel residual links |
| [SB-SELECT-3] | `tests/test_timestamp_selection_contract_sb_select.py`; `tests/test_move_by_id.py` (id preserved under move); move-behind-bound documentation bind |
| [SB-SELECT-4] | `tests/test_timestamp_selection_contract_sb_select.py`; `tests/test_watcher.py::TestQueueWatcher::test_peek_handler_failure_does_not_advance_checkpoint`; `[SB-DELIVERY-2]` |

## Related Plans

- `docs/plans/2026-07-30-product-documentation-cutover-plan.md` (Phase 2B)
