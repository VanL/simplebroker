# Persistence I/O

Status: Active

Owner: SimpleBroker dump/load and claimed-row inspection surfaces.

Boundary: the portable `simplebroker-dump` v1 format; dump selection filters;
load into a destination broker; and inspection of claimed (deletion-pending)
rows. Delivery claim/vacuum lifecycle is `[SB-DELIVERY-*]`. Message-id rules
for restored ids are `[SB-ID-*]`. Live SQLite file-copy is not a supported
backup protocol.

## Dump format [SB-IO-1]

`dump` / `dump_lines` emit **versioned NDJSON** (`simplebroker-dump` v1):

1. Exactly one **header** line first (`type: header`, `format`, `version`,
   informational `backend`, and `last_ts`). `last_ts` is the high-water JSON
   string from `[SB-ID-3]`; call it H.
2. Then **alias** lines (`type: alias`), sorted by alias name.
3. Then **message** lines (`type: message` with `queue`, `body`, `id`), queues
   sorted, messages in ascending message-id order within each queue. `id` is
   the message-id JSON string from `[SB-ID-1]`.

Serialization is deterministic for a given logical dump content (stable key
order in each JSON object).

The producer samples H once before traversal. Every emitted message ID is at
most H. Backend message scans receive H as an inclusive upper bound, so a
concurrent write above H is excluded from this dump. Load validates `id <= H`
inline while parsing and rejects a violating record before inserting it; this
does not add a complete-input preflight or roll back earlier applied batches.

## Dump contents [SB-IO-2]

Dump includes **pending** messages only. Claimed (already consumed,
deletion-pending) rows are **omitted**. A queue whose messages are all claimed
contributes no message lines and does not appear as a queue after load.

Dump is a **bounded logical export**, not a guaranteed point-in-time snapshot
under concurrent writers. Its message-ID ceiling does not freeze aliases,
claims, deletes, moves, or queue membership. Quiesce writers when an exact
frozen view is required. A row above H is excluded whether it is a normal
concurrent write or evidence of anomalous source metadata; live traversal
cannot distinguish those cases without adding a freeze or a racy second scan,
so exclusion does not emit a corruption warning.

Cross-backend: a dump from one supported backend is intended to load into
another via the same public connection surface (`list_queues`,
bounded `peek_generator`, aliases, `insert_messages`,
`advance_last_timestamp`).

## Dump selection filters [SB-IO-3]

CLI `dump --include` / `--exclude` and Python `include` / `exclude` take
fnmatch-style globs against **queue names** (case-sensitive).

- When `include` is set, a queue dumps only if it matches at least one include
  glob.
- `exclude` always wins over include.
- Aliases match if **either** the alias name or its target matches include
  (when include is set), and are excluded if **either** name matches exclude.

## Load [SB-IO-4]

`load` / `load_lines` apply dump lines to a broker connection.

- First record must be a valid v1 header; malformed input raises with a
  **1-based line number**.
- Load is intended for a **fresh destination**, but freshness is caller-owned
  and not enforced. Duplicate message ids raise loudly (`IntegrityError`)
  rather than double-inserting. Header-only and disjoint data can merge into a
  non-fresh destination. Load mutates as it proceeds and may be partially
  applied when a later record or final floor operation fails.
- Message records restore exact ids (subject to `[SB-ID-4]`, including its
  integer and exact-19-digit-string input forms and rejection of reserved
  zero). This preserves loading of legacy numeric v1 records without assigning
  input-form ownership to this format clause. Aliases are re-created from alias
  lines.
- The v1 header must contain a valid `last_ts`; omission is invalid input.
  Current writers emit the canonical exact 19-digit string. Load accepts either
  an integer or an exact 19-digit string under `[SB-ID-4]`. It treats the value
  as a restored allocation floor. After replay, target high-water is at least
  that value, including for a header-only dump. Header H is also the inclusive
  message-ID upper bound from `[SB-IO-1]`. Every successfully generated later
  id is greater than both the header floor and every restored message id.
- `load_lines(broker, lines, *, force=False, config=None)` validates a callable
  `advance_last_timestamp` before consuming input, raising `TypeError` when the
  structural broker is incompatible. After parsing the header and before any
  destination mutation, it compares H's physical component with one local
  wall-clock sample. Any positive future skew emits the public
  `DumpClockSkewWarning` (`from simplebroker import DumpClockSkewWarning`).
  Skew at most `BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS` (default 300) proceeds;
  greater skew raises `ValueError` unless `force=True`, which still warns.
  The config follows `load_config()` environment defaults plus
  `resolve_config(config)` typed overrides; `.broker.toml` remains limited to
  backend-target configuration. Force bypasses only this skew refusal.
  SimpleBroker's five-minute default is an operational availability/safety
  choice informed by
  [MIT Kerberos's conventional 300-second default allowable clock skew](https://web.mit.edu/kerberos/www/krb5-1.21/doc/admin/conf_files/krb5_conf.html),
  not a universal maximum for clock correctness.
- The warning reports the apparent skew and allocation consequence. The
  12-bit logical counter permits at most 4,095 further broker-global generated
  IDs from counter zero, and potentially fewer from H's actual counter, before
  allocation waits for wall time and may raise `TimestampError`.
- CLI `load` exit codes: `0` success, `1` error (`[SB-CLI-1]`).
  `load --force` mirrors the Python override. Clock-skew warnings use stderr
  with a `broker load: warning:` prefix and are suppressed by global `--quiet`;
  errors are never suppressed.
  A direct `load_lines()` call retains ordinary Python warning behavior.
  During `cmd_load`, loud mode renders only that invocation's clock-skew notice
  as `broker load: warning:` commentary, while quiet mode suppresses only that
  notice. The command does not replace process-global warning hooks or filters
  and cannot hide a warning emitted by another thread or invocation.

Prefer dump/load over copying a live SQLite database directory (WAL/SHM/lock
companions and concurrent writers make bare copy unsafe).

## Claimed-row inspection [SB-IO-5]

Claimed rows remain visible to explicit inspection until vacuum removes them.
They are **not** ordinary pending delivery stock.

Surfaces such as peek with `include_claimed` / `--include-claimed` may return
pending and claimed rows together for diagnostics. That is **inspection**, not
a delivery or ack protocol: vacuum may remove claimed rows at any time, and
seeing a claimed row does not restore it to pending.

`exists` / stats may count claimed rows where documented; that does not change
claim delivery semantics (`[SB-DELIVERY-1]`).

## Implementation mapping

- Format and filters: `simplebroker/_dump.py` (`dump_lines`, `load_lines`,
  `LoadResult`)
- CLI: `simplebroker/commands.py` (`cmd_dump`, `cmd_load`), `simplebroker/cli.py`
- Claimed inspection: `simplebroker/sbqueue.py` peek paths with
  `include_claimed`; CLI peek `--include-claimed`
- Vacuum / claim lifecycle: `[SB-DELIVERY-*]` and maintenance config

## Verification

| Clause | Firing evidence |
|--------|-----------------|
| [SB-IO-1] | `tests/test_persistence_io_contract_sb_io.py`; `tests/test_dump_load.py::test_dump_format_header_aliases_messages_in_order`, `tests/test_dump_load.py::test_dump_header_is_inclusive_message_id_bound`; `tests/test_json_message_id_contract.py::test_dump_formats_header_and_message_identity_fields` |
| [SB-IO-2] | Core firing gates: `tests/test_dump_load.py::test_dump_format_header_aliases_messages_in_order`, `tests/test_dump_load.py::test_dump_header_is_inclusive_message_id_bound`. Routine SQLite↔PostgreSQL: `extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py::test_sqlite_to_postgres_pipe`, `extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py::test_postgres_to_sqlite_pipe`. Routine SQLite↔Redis: `extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py::test_sqlite_to_redis_pipe`, `extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py::test_redis_to_sqlite_pipe`. Opt-in direct PostgreSQL↔Redis: `tests/test_cross_backend_dump_load.py::test_postgres_to_redis_pipe`, `tests/test_cross_backend_dump_load.py::test_redis_to_postgres_pipe`. |
| [SB-IO-3] | `tests/test_dump_load.py::test_include_exclude_filters`, `test_alias_matches_on_its_own_name`, `test_filters_are_case_sensitive` |
| [SB-IO-4] | `tests/test_dump_load.py::test_load_accepts_exact_string_message_id`, `tests/test_dump_load.py::test_load_accepts_legacy_integer_message_id`, `tests/test_dump_load.py::test_load_accepts_legacy_integer_header_last_ts`, `tests/test_dump_load.py::test_header_only_load_restores_last_timestamp_floor`, `tests/test_dump_load.py::test_claimed_future_exact_ids_survive_as_header_floor`, `tests/test_dump_load.py::test_load_rejects_records_newer_than_header_bound`, `tests/test_dump_load.py::test_load_rejects_incompatible_broker_before_consuming_input`, `tests/test_dump_load.py::test_load_warns_and_proceeds_at_future_skew_limit`, `tests/test_dump_load.py::test_load_clock_skew_uses_physical_grain_boundary`, `tests/test_dump_load.py::test_load_rejects_excessive_future_skew_before_mutation`, `tests/test_dump_load.py::test_load_force_warns_and_accepts_excessive_future_skew`, `tests/test_dump_load.py::test_load_typed_config_override_changes_skew_limit`, `tests/test_dump_load.py::test_quiet_cmd_load_does_not_hide_another_threads_clock_skew_warning`, `tests/test_dump_load.py::test_cmd_load_warning_policy_resets_after_success`, `tests/test_dump_load.py::test_cmd_load_warning_policy_resets_after_every_failure`, `tests/test_dump_load.py::test_load_warning_sink_restores_outer_nested_policy`, `tests/test_dump_load.py::test_load_header_floor_persists_when_local_cache_is_ahead`, `tests/test_dump_load.py::test_load_header_floor_observes_concurrent_durable_winner`, `tests/test_dump_load.py::test_load_header_floor_final_read_failure_is_outcome_ambiguous`, `tests/test_dump_load.py::test_load_rejects_header_without_last_ts`, `tests/test_dump_load.py::test_load_rejects_invalid_header_last_ts_with_line_context`, `tests/test_dump_load.py::test_load_rejects_noncanonical_message_id_tokens_with_line_context`, `tests/test_dump_load.py::test_reloading_same_dump_fails_loudly`, `tests/test_dump_load.py::test_load_rejects_bad_input`, `tests/test_dump_load.py::test_load_rejects_reserved_zero_with_line_context_before_batch_flush`, `tests/test_dump_load.py::test_load_rejects_huge_json_integer_with_line_context`; `tests/test_cli_dump_load.py::test_load_rejects_garbage_with_line_number`, `tests/test_cli_dump_load.py::test_load_future_skew_warns_once_and_quiet_suppresses_display`, `tests/test_cli_dump_load.py::test_load_excessive_future_skew_requires_force`, `tests/test_cli_dump_load.py::test_load_timestamp_floor_failure_uses_command_diagnostic`, `tests/test_cli_dump_load.py::test_load_force_does_not_bypass_format_validation`, `tests/test_cli_dump_load.py::test_cmd_load_ambiguous_timestamp_failure_gives_recovery_guidance`, `tests/test_cli_dump_load.py::test_cmd_load_reemits_unrelated_warnings`, `tests/test_cli_dump_load.py::test_cmd_load_preserves_unrelated_warning_error_timing`; `extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py::test_postgres_header_only_load_restores_last_timestamp_floor`; `extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py::test_redis_header_only_load_restores_last_timestamp_floor` |
| [SB-IO-5] | `tests/test_peek_include_claimed.py::test_include_claimed_returns_superset_in_id_order`, `tests/test_peek_include_claimed.py::test_exact_id_peek_finds_claimed_row_only_with_flag`, `tests/test_peek_include_claimed.py::test_peeking_claimed_rows_mutates_nothing` |

## Related Plans

- active: [2026-08-27-all-examples-correctness-and-contract-alignment-plan](../plans/2026-08-27-all-examples-correctness-and-contract-alignment-plan.md)
  — replaces an ad hoc copy recipe with pending-only dump/load guidance
- active: [2026-08-25-verified-review-findings-remediation-plan](../plans/2026-08-25-verified-review-findings-remediation-plan.md)
  — invocation-owned `cmd_load` clock-skew presentation
- retired: 2026-08-12-bounded-live-dump-plan — source `d0d2de9`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-08-10-test-suite-signal-remediation-plan — source `0d15871`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-08-json-timestamp-string-contract-plan — source `4cb47bc9`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-06-audit-remediation-plan — source `94e15bc`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-07-30-product-documentation-cutover-plan — source `5023710`;
  see the ledger in `docs/plans/README.md`
