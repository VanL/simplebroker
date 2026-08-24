# Message Identity

Status: Active

Owner: SimpleBroker message-identity and timestamp-allocation layer. Each
backend owns the storage realization of ID allocation, high-water advancement,
exact-ID insertion, and ID-preserving move.

Boundary: public message-ID representation and range; broker-generated ID
allocation; write-return identity; high-water and cache meaning; exact-ID
forms and insertion outcomes; move preserves identity.

Ordered timestamp filters (`--after` / `--before`) and filter consequences are
`docs/specs/14-timestamp-selection.md` `[SB-SELECT-*]`. CLI non-exact
bound string forms are `[SB-CLI-5]`. Claim and delivery remain with
`[SB-DELIVERY-*]`. Dump/load format and claimed inspection are
`docs/specs/15-persistence-io.md` `[SB-IO-*]`.

## Representation and identity [SB-ID-1]

A stored message exposes one public message ID. JSON surfaces call this field
`timestamp`. In storage and ordinary Python APIs, the id is an integer in
`0 <= message_id < 2**63`.

The **message-id JSON string** is exactly **19 ASCII decimal digits**,
left-padded with zeroes. Every SimpleBroker-owned JSON output that serializes a
message id uses this string. JSON numbers are not emitted for message ids
because generic JSON consumers cannot represent the full range exactly.
Message bodies remain opaque application payload: SimpleBroker does not
inspect or rewrite identity-like fields inside them.

Broker-**generated** message ids are strictly positive. ID `0` is reserved as
the lower-bound / empty high-water origin and is not valid for a newly
inserted message. Exact selectors still accept zero so a database created by
an older release can be inspected and cleaned up.

Broker-generated ids pack a physical component taken from `time.time_ns()`
with the low 12 bits cleared, and a logical counter in those 12 bits. The id
is format-compatible with nanosecond Unix time in that sense, but it does not
offer 1 ns resolution: host clocks and the 12-bit counter reservation make
effective time steps on the order of **~4 µs (4096 ns)**. The physical
component is **not** a microsecond counter.

Within one **database** (each broker target, including Redis, is a database
for this purpose), broker-generated ids are allocated monotonically and stored
messages have unique ids. Caller-supplied exact ids are bounded only by range
and uniqueness (`[SB-ID-4]`), so a message written later may carry a smaller
id, and stored ids are not ordered by write time in general. Message bodies are
payload and may duplicate.

A generated id’s physical component is generation-time (`now()`) within the
encoding grain. Callers may insert exact ids; those need not equal wall-clock
now. Move preserves id (see [SB-ID-5]). Storage iteration order follows an
internal sequence counter, not the public message id.

## Allocation and write result [SB-ID-2]

`generate_timestamp()` and its `get_ts()` alias allocate and persist a new
broker-compatible id without inserting a message row.

`Queue.write()` returns the id of the row that committed. If an attempted id
conflicts and the write retries, only the surviving committed row’s id is
returned. If no row commits, no id is returned. Concurrent writers may advance
global high-water after a write; that does not change the id returned for the
earlier row. CLI display of the returned id remains with `[SB-CLI-*]`.

For an ordinary generated `write()`, allocation/high-water advancement and
insertion commit together. A stale candidate must not advance persisted
high-water or insert a row.

## Global high-water and caches [SB-ID-3]

Persisted `last_ts` is a database-global allocation high-water mark. It is not
scoped to one queue, is not the id of the caller’s most recent write, and need
not identify a current message row.

When SimpleBroker serializes this high-water to JSON, its high-water JSON
string uses the same representation as the message-id JSON string; the empty
origin is `"0000000000000000000"`. Sharing the representation does not make
high-water the identity of a current row.

`Queue.last_ts` is a per-`Queue` cache of that global value and may be stale
relative to other writers. The cache updates after `queue.write()` and
`queue.generate_timestamp()`. On a fresh handle the first read lazily fetches
backend high-water — `0` on an empty target — so a caller need not generate or
refresh first. The property yields `None` only when that lazy fetch fails; a
`None` result therefore means "unknown", not "zero".
`Queue.refresh_last_ts()` refreshes from backend high-water.
`Queue.latest_pending_timestamp()` is a different queue-local query.

Callers that need one write’s identity use the value returned by `write()`,
not high-water or cache surfaces.

Repair and resynchronization are monotone compare-and-advance operations:
they never replace persisted `last_ts` with a lower value, including when a
concurrent allocator advances high-water after the repair reads its input.

Persistence load treats a valid dump-header high-water as an allocation floor.
After replay it monotonically advances persisted `last_ts` to at least that
floor, including when the dump contains no messages and when the connection's
process-local cache is already higher. The durable compare-and-advance is never
skipped because of that cache. The connection then reads persisted high-water
once, refreshes its cache, and returns that observation; another process may
advance it immediately afterward under the ordinary cache-staleness contract.
A higher message-derived or concurrently advanced watermark is never lowered.
A final-read exception raises `TimestampError` with
`outcome_ambiguous=True`; because the monotone advance may already have
committed, durable outcome is unknown but can never be lower than it was before
the attempt. A successful final read below the requested floor is instead a
known contract failure (`outcome_ambiguous=False`). Non-retryable operational
failure during the attempted advance is ambiguous; exhausted retryable lock
contention is known failure.

The dump producer also uses that sampled header value as the inclusive upper
bound for exported message IDs (`[SB-IO-1]`). This is a persistence-I/O bound,
not a change to ordinary exact insertion. Load's future-skew refusal is likewise
load-only; direct `insert_messages()` retains the behavior below.

## Exact-ID normalization and insertion [SB-ID-4]

Public exact-id operations accept either:

- an integer in range; or
- a string of exactly **19 decimal digits** denoting that integer.

Surrounding whitespace is stripped before that length check, so a padded
string of 19 digits is accepted. "Decimal digit" means Python's
`str.isdecimal()`: non-ASCII decimal digits (for example Arabic-Indic
`٠١٢`) are accepted and normalize to the same integer. Digit-like
characters that are not decimal digits (for example superscript `²`) are
rejected.

Malformed strings raise `ValueError`. Unsupported types, including `bool`,
raise `TypeError`.

New exact-id insertion rejects reserved zero (`0` and the 19-digit zero
string) with `ValueError`. Exact selectors still accept zero for legacy
access. A batch is snapshotted and validated before mutation: one reserved
zero in a batch inserts no rows and does not change high-water.

`insert_messages(...)` validates the full batch, rejects duplicate ids after
normalization, advances `last_ts` above the largest supplied id when the
operation succeeds, and inserts pending messages with their exact ids. Invalid
input or an id already present aborts with no partial insert and no high-water
change. An empty input is a no-op.

The dump-header floor applied by `load_lines()` does not change this ordinary
exact-insert contract. It is a loader-owned monotone advance after replay, not
an alternate `insert_messages()` mode.

An id at the very top of the range cannot be inserted: high-water must be able
to advance above every supplied id, so an id with no room above it is
rejected.

A caller-supplied id far ahead of the wall clock advances high-water into that
future interval. Later allocations consume remaining logical-counter values at
that physical component and, once those values are exhausted, fail until the
wall clock catches up (natural clock advance recovers absent large adjustments
such as admin time jumps).

## Move preserves identity [SB-ID-5]

A successful move changes the message’s queue without allocating a replacement
message id. It is the same message identity with the queue binding updated.

## Implementation Mapping

| Contract area | Owner |
|---------------|-------|
| Hybrid encoding, monotonic allocation, generator cache, persisted high-water interaction | `simplebroker/_timestamp.py::TimestampGenerator` |
| Exact-ID normalization and JSON formatting | `simplebroker/_message_id.py::normalize_message_id`; `simplebroker/_message_id.py::format_message_id` |
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
| [SB-ID-1] | `tests/test_message_identity_contract_sb_id.py`; `tests/test_core_persistence_transition_tables.py::test_timestamp_generator_fires_transition_table`; `tests/test_timestamp_edge_cases.py::TestTimestampEdgeCases::test_timestamp_magnitude_preservation`, `test_clock_regression_keeps_generator_monotonic`, `test_shared_timestamp_generator_serializes_threads`; `tests/test_timestamp_helpers.py::TestTimestampHelpers::test_db_generate_timestamp_monotonic`; `tests/test_write_returns_id.py::test_broker_write_ids_strictly_increase`; `tests/test_insert_messages.py::test_fresh_generated_message_id_is_positive_and_after_zero_visible`; `tests/test_message_id_validation.py::test_format_message_id_preserves_unsafe_json_integer_exactly`, `test_format_message_id_returns_canonical_ascii_string`, `test_format_message_id_reuses_exact_id_validation`, `test_normalize_message_id_accepts_ints_and_exact_19_digit_strings`, `test_normalize_message_id_rejects_out_of_range_ints`; `tests/test_json_message_id_contract.py::test_public_json_identity_producers_preserve_message_ids`, `test_shared_message_line_formats_id_without_rewriting_body`, `test_write_and_status_format_only_their_json_boundary`, `test_dump_formats_header_and_message_identity_fields`, `test_watcher_helper_formats_message_identity`, `test_adjacent_unsafe_ids_remain_distinct_after_json_parse` |
| [SB-ID-2] | `tests/test_message_identity_contract_sb_id.py`; `tests/test_core_persistence_transition_tables.py::test_timestamp_generator_fires_transition_table`; `tests/test_timestamp_helpers.py::TestTimestampHelpers::test_db_generate_timestamp_monotonic`, `test_queue_generate_timestamp_monotonic`; `tests/test_write_returns_id.py::test_broker_write_returns_committed_id`, `test_queue_write_returns_committed_id`, `test_retry_path_returns_surviving_row_id`, `test_retry_exhaustion_raises_without_returning`, `test_concurrent_writers_get_their_own_ids`, `test_write_return_id_remains_row_identity_after_global_last_ts_advances`; `tests/test_write_visibility.py::test_write_allocates_timestamp_inside_the_insert_transaction`; `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_write_script_rejects_stale_candidate_without_any_mutation`, `test_ordinary_write_retries_stale_local_candidate_above_reader_checkpoint`, `test_resync_cannot_overwrite_concurrent_high_water_backward`, `test_steady_state_ordinary_write_uses_one_data_eval`, `test_single_core_concurrent_writes_preserve_cross_writer_retry_budget`, `test_same_target_cores_serialize_candidate_reservation`; `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py::test_redis_write_fires_transition_table` |
| [SB-ID-3] | `tests/test_message_identity_contract_sb_id.py`; `tests/test_core_persistence_transition_tables.py::test_timestamp_generator_fires_transition_table`; `tests/test_queue_api_comprehensive.py::TestQueueLastTimestampCaching::test_last_ts_updates_after_generate_and_write`, `test_refresh_last_ts_detects_external_writes`; `tests/test_insert_messages.py::test_broker_insert_messages_loads_single_fresh_record_and_advances_last_ts`, `test_broker_insert_messages_accepts_current_generated_id`; `tests/test_latest_pending_timestamp.py::test_latest_pending_timestamp_ignores_generated_timestamp_without_row`; `tests/test_write_returns_id.py::test_write_return_id_remains_row_identity_after_global_last_ts_advances`; `tests/test_json_message_id_contract.py::test_write_and_status_format_only_their_json_boundary`, `test_dump_formats_header_and_message_identity_fields`; `extensions/simplebroker_pg/tests/test_pg_timestamp_resilience.py::test_resync_cannot_overwrite_concurrent_high_water_backward` |
| [SB-ID-4] | `tests/test_message_identity_contract_sb_id.py`; `tests/test_message_id_validation.py::test_normalize_message_id_accepts_ints_and_exact_19_digit_strings`, `test_normalize_message_id_rejects_malformed_strings`, `test_normalize_message_id_rejects_out_of_range_ints`, `test_normalize_message_id_rejects_non_id_types`; `tests/test_insert_messages.py::test_broker_insert_messages_loads_many_records_and_preserves_ids`, `test_broker_insert_messages_rejects_mixed_form_duplicate_ids_before_writes`, `test_broker_insert_messages_rolls_back_on_existing_duplicate`, `test_broker_insert_messages_accepts_exact_string_message_id`, `test_exact_insert_preflights_mixed_valid_invalid_batch_without_mutation`, `test_broker_insert_messages_empty_input_is_noop`, `test_broker_insert_messages_does_not_move_high_water_backward`, `test_broker_insert_messages_rejects_unadvanceable_high_water`, `test_far_future_exact_insert_can_stall_later_writes_until_clock_catches_up`, `test_broker_insert_messages_rejects_reserved_zero_before_mutation`, `test_broker_insert_messages_rejects_reserved_zero_in_mixed_batch`, `test_queue_insert_messages_rejects_reserved_zero`, `test_native_legacy_zero_remains_exactly_addressable_movable_and_deletable`; `tests/test_dump_load.py::test_load_rejects_reserved_zero_with_line_context_before_batch_flush` |
| [SB-ID-5] | `tests/test_message_identity_contract_sb_id.py`; `tests/test_move_by_id.py::test_move_by_id_preserves_timestamp`, `test_move_many_preserves_original_message_ids`, `test_move_generator_preserves_original_message_ids_in_each_delivery_mode`; `tests/test_cli_move.py::TestEdgeCases::test_move_preserves_timestamps` |

## Related Plans

- retired: 2026-08-12-bounded-live-dump-plan — source `d0d2de9`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-08-10-test-suite-signal-remediation-plan — source `0d15871`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-08-json-timestamp-string-contract-plan — source `4cb47bc9`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-07-30-product-documentation-cutover-plan — source `5023710`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-07-30-reserved-zero-and-redis-write-atomicity-plan — source
  `5023710`; see the ledger in `docs/plans/README.md`
- retired: 2026-08-05-worker-portability-and-example-corrections-plan — source
  `6481ca08`; see the ledger in `docs/plans/README.md`
- retired: 2026-08-06-audit-remediation-plan — source `94e15bc`; see the
  ledger in `docs/plans/README.md`
