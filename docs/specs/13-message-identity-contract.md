# Message Identity Contract

Status: Active

Owner: SimpleBroker message-identity and timestamp-allocation layer. Each
backend owns the storage realization of ID allocation, high-water advancement,
exact-ID insertion, and ID-preserving move.

Boundary: public message-ID representation and range; broker-generated ID
allocation; write-return identity; broker-global high-water and public cache
semantics; exact-ID normalization and insertion consequences; and preservation
of identity across move.

Strict `after_timestamp` / `before_timestamp` selection, CLI timestamp-bound
parsing, checkpoint progression, and the permanent-skip consequence of moving
an older ID behind a checkpoint are excluded. They remain with the registered
ordered-selection/checkpoint concern until Phase 2B. Claim state and delivery
guarantees remain with `[SB-DELIVERY-*]`. Dump/load formats and restore policy
remain with the persistence-I/O concern. Queue iteration and FIFO ordering are
not defined by this contract.

Required action: producers retain the ID returned by a successful write when
they need that row's identity. Callers must not infer a write's ID from
`last_ts`. Exact-ID callers use the accepted integer or exact-string forms.
Consumers and backend implementers preserve an existing message ID when moving
the row between queues.

## Representation and identity [SB-ID-1]

A stored message exposes one public message ID. JSON surfaces call this field
`timestamp`. The ID is an integer in the signed storage range
`0 <= message_id < 2**63`.

Broker-generated IDs use a hybrid timestamp encoding: the physical component
retains the magnitude of `time.time_ns()` with the low 12 bits cleared, and the
low 12 bits hold the logical counter. The physical component is
nanosecond-scaled with 4,096-nanosecond granularity; it is not a count of
microseconds.

Broker-generated IDs increase monotonically within one resolved broker target.
The stored message relation enforces uniqueness for rows that coexist. Message
bodies are payload, not identity, and may duplicate. SimpleBroker keeps no
permanent tombstone or application deduplication ledger after physical removal.

This clause does not promise that queue iteration is ordered by numeric message
ID or that every stored ID was generated from the current wall clock. Exact-ID
insertion may supply an earlier valid ID.

## Allocation and write result [SB-ID-2]

`generate_timestamp()` and its `get_ts()` alias allocate and persist a new
broker-compatible ID without inserting a message row.

Both `write()` on the broker handle returned by `open_broker()` and
`Queue.write()` return the ID of the row that committed. If an attempted ID
conflicts and the write retries, only the surviving committed row's ID is
returned. If no row commits, no ID is returned. Concurrent writers may advance
broker-global high-water after a write; that later advancement does not change
the ID returned for the earlier row. CLI display of the returned ID remains
governed by `[SB-CLI-*]`.

This clause does not promise one universal cross-backend visibility point for
high-water advancement and row insertion. Ordered visibility to checkpoint
readers remains outside this contract.

## Global high-water and caches [SB-ID-3]

Persisted `last_ts` is a broker-target-global allocation high-water mark. It is
not scoped to one queue, is not the ID of the caller's most recent write, and
need not identify a current message row. It may reflect another queue, another
writer, a generated ID with no row, or exact-ID insertion.

`get_cached_last_timestamp()` exposes the broker handle's current generator
view. `Queue.last_ts` is a per-`Queue` cache of the broker-global value and may
be stale relative to other writers. `Queue.refresh_last_ts()` and
`refresh_last_timestamp()` on the broker handle explicitly refresh from
backend high-water state. `Queue.latest_pending_timestamp()` is a different
queue-local query and is not an alias for `last_ts`.

Callers needing one write's identity use the value returned by `write()`, not
any high-water or cache surface.

## Exact-ID normalization and insertion [SB-ID-4]

Public exact-ID operations accept either:

- an integer satisfying `0 <= value < 2**63`; or
- a string which, after surrounding whitespace is stripped, contains exactly
  19 Unicode decimal digits and parses to an integer in that range.

`bool` and other unsupported types raise `TypeError`. Negative or out-of-range
integers and malformed string IDs raise `ValueError`. Range-bound parsing is a
different contract and is not widened by these exact-ID forms.

`insert_messages(...)` snapshots and validates the complete input before
mutation. IDs are normalized before duplicate detection. Duplicate normalized
IDs within the batch raise `IntegrityError`. Invalid input or an ID already
present in storage aborts the operation with no inserted rows and no
high-water change. An empty input is a no-op.

A successful exact-ID insertion operation atomically stores the
caller-supplied numeric IDs and advances persisted high-water when necessary
to at least one greater than the largest inserted ID; high-water never moves
backward. An inserted ID must therefore leave room below `2**63` for that
advancement. SQL backends realize this outcome with a transaction; Redis uses
one atomic server-side operation. Dump/load record format, fresh-target policy,
and migration behavior remain outside this contract.

Callers should supply IDs allocated by a compatible SimpleBroker timestamp
generator. A caller-supplied ID whose physical component is far ahead of the
wall clock advances high-water into that future interval. Later allocations
then consume the remaining logical-counter values at that physical component
and, once those values are exhausted, fail until the wall clock catches up.

## Move preserves identity [SB-ID-5]

A successful move changes the message's queue without allocating a replacement
message ID. Single-message, materialized-batch, and generator move surfaces
preserve each moved row's original public ID. When a move result includes a
timestamp, it reports that preserved ID.

This clause does not define claim eligibility, commit-before-yield behavior,
rollback, queue ordering, or checkpoint visibility. Those concerns remain with
their registered delivery, base-operation, and ordered-selection owners.

## Implementation Mapping

| Contract area | Owner |
|---------------|-------|
| Hybrid encoding, monotonic allocation, generator cache, persisted high-water interaction | `simplebroker/_timestamp.py::TimestampGenerator` |
| Exact-ID normalization | `simplebroker/_message_id.py::normalize_message_id` |
| Exact-ID batch validation and required high-water computation | `simplebroker/_message_insert.py` |
| SQL/core write, insertion, cache access, and move orchestration | `simplebroker/db.py::BrokerCore` |
| Public queue write, `last_ts`, refresh, insert, and move surfaces | `simplebroker/sbqueue.py::Queue` |
| CLI exact-ID boundary and write-result rendering | `simplebroker/commands.py`; `simplebroker/cli.py` |
| SQLite realization | `simplebroker/_backends/sqlite/plugin.py`; shared SQL namespace |
| PostgreSQL realization | `extensions/simplebroker_pg/simplebroker_pg/plugin.py`; `extensions/simplebroker_pg/simplebroker_pg/_sql.py` |
| Redis realization | `extensions/simplebroker_redis/simplebroker_redis/core.py`; `extensions/simplebroker_redis/simplebroker_redis/scripts.py` |
| Backend connection protocol | `simplebroker/_backend_plugins.py` |

## Verification

| Clause | Firing evidence |
|--------|-----------------|
| [SB-ID-1] | `tests/test_message_identity_contract_sb_id.py`; `tests/test_core_persistence_transition_tables.py::test_timestamp_generator_fires_transition_table`; `tests/test_timestamp_edge_cases.py::TestTimestampEdgeCases::test_timestamp_magnitude_preservation`, `test_clock_regression_keeps_generator_monotonic`, `test_shared_timestamp_generator_serializes_threads`; `tests/test_timestamp_helpers.py::TestTimestampHelpers::test_db_generate_timestamp_monotonic`; `tests/test_write_returns_id.py::test_broker_write_ids_strictly_increase`; `tests/test_message_id_validation.py::test_normalize_message_id_accepts_ints_and_exact_19_digit_strings`, `test_normalize_message_id_rejects_out_of_range_ints` |
| [SB-ID-2] | `tests/test_message_identity_contract_sb_id.py`; `tests/test_core_persistence_transition_tables.py::test_timestamp_generator_fires_transition_table`; `tests/test_timestamp_helpers.py::TestTimestampHelpers::test_db_generate_timestamp_monotonic`, `test_queue_generate_timestamp_monotonic`; `tests/test_write_returns_id.py::test_broker_write_returns_committed_id`, `test_queue_write_returns_committed_id`, `test_retry_path_returns_surviving_row_id`, `test_retry_exhaustion_raises_without_returning`, `test_concurrent_writers_get_their_own_ids`, `test_write_return_id_remains_row_identity_after_global_last_ts_advances` |
| [SB-ID-3] | `tests/test_message_identity_contract_sb_id.py`; `tests/test_core_persistence_transition_tables.py::test_timestamp_generator_fires_transition_table`; `tests/test_queue_api_comprehensive.py::TestQueueLastTimestampCaching::test_last_ts_updates_after_generate_and_write`, `test_refresh_last_ts_detects_external_writes`; `tests/test_insert_messages.py::test_broker_insert_messages_loads_single_fresh_record_and_advances_last_ts`, `test_broker_insert_messages_accepts_current_generated_id`; `tests/test_latest_pending_timestamp.py::test_latest_pending_timestamp_ignores_generated_timestamp_without_row`; `tests/test_write_returns_id.py::test_write_return_id_remains_row_identity_after_global_last_ts_advances` |
| [SB-ID-4] | `tests/test_message_identity_contract_sb_id.py`; `tests/test_message_id_validation.py::test_normalize_message_id_accepts_ints_and_exact_19_digit_strings`, `test_normalize_message_id_rejects_malformed_strings`, `test_normalize_message_id_rejects_out_of_range_ints`, `test_normalize_message_id_rejects_non_id_types`; `tests/test_insert_messages.py::test_broker_insert_messages_loads_many_records_and_preserves_ids`, `test_broker_insert_messages_rejects_mixed_form_duplicate_ids_before_writes`, `test_broker_insert_messages_rolls_back_on_existing_duplicate`, `test_broker_insert_messages_accepts_exact_string_message_id`, `test_exact_insert_preflights_mixed_valid_invalid_batch_without_mutation`, `test_broker_insert_messages_empty_input_is_noop`, `test_broker_insert_messages_does_not_move_high_water_backward`, `test_broker_insert_messages_rejects_unadvanceable_high_water`, `test_far_future_exact_insert_can_stall_later_writes_until_clock_catches_up` |
| [SB-ID-5] | `tests/test_message_identity_contract_sb_id.py`; `tests/test_move_by_id.py::test_move_by_id_preserves_timestamp`, `test_move_many_preserves_original_message_ids`, `test_move_generator_preserves_original_message_ids_in_each_delivery_mode`; `tests/test_cli_move.py::TestEdgeCases::test_move_preserves_timestamps` |

## Related Plans

- `docs/plans/2026-07-30-product-documentation-cutover-plan.md`
