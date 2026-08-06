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
   informational `backend`, and `last_ts`).
2. Then **alias** lines (`type: alias`), sorted by alias name.
3. Then **message** lines (`type: message` with `queue`, `body`, `id`), queues
   sorted, messages in ascending message-id order within each queue.

Serialization is deterministic for a given logical dump content (stable key
order in each JSON object).

## Dump contents [SB-IO-2]

Dump includes **pending** messages only. Claimed (already consumed,
deletion-pending) rows are **omitted**. A queue whose messages are all claimed
contributes no message lines and does not appear as a queue after load.

Dump is a **logical export**, not a guaranteed point-in-time snapshot under
concurrent writers. Quiesce writers when an exact frozen view is required.

Cross-backend: a dump from one supported backend is intended to load into
another via the same public connection surface (`list_queues`,
`peek_generator`, aliases, `insert_messages`).

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
- Load is intended for a **fresh destination**. Duplicate message ids raise
  loudly (`IntegrityError`) rather than double-inserting.
- Message records restore exact ids (subject to `[SB-ID-4]`, including
  rejection of reserved zero). Aliases are re-created from alias lines.
- CLI `load` exit codes: `0` success, `1` error (`[SB-CLI-1]`).

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
| [SB-IO-1] | `tests/test_persistence_io_contract_sb_io.py`; `tests/test_dump_load.py::test_dump_format_header_aliases_messages_in_order` |
| [SB-IO-2] | Core firing gate: `tests/test_persistence_io_contract_sb_io.py::test_dump_omits_claimed_messages`. Routine SQLite↔PostgreSQL: `extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py::test_sqlite_to_postgres_pipe`, `extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py::test_postgres_to_sqlite_pipe`. Routine SQLite↔Redis: `extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py::test_sqlite_to_redis_pipe`, `extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py::test_redis_to_sqlite_pipe`. Opt-in direct PostgreSQL↔Redis: `tests/test_cross_backend_dump_load.py::test_postgres_to_redis_pipe`, `tests/test_cross_backend_dump_load.py::test_redis_to_postgres_pipe`. |
| [SB-IO-3] | `tests/test_dump_load.py::test_include_exclude_filters`, `test_alias_matches_on_its_own_name`, `test_filters_are_case_sensitive` |
| [SB-IO-4] | `tests/test_dump_load.py::test_reloading_same_dump_fails_loudly`, `tests/test_dump_load.py::test_load_rejects_bad_input`, `tests/test_dump_load.py::test_load_rejects_reserved_zero_with_line_context_before_batch_flush`, `tests/test_dump_load.py::test_load_rejects_huge_json_integer_with_line_context`; `tests/test_cli_dump_load.py::test_load_rejects_garbage_with_line_number` |
| [SB-IO-5] | `tests/test_persistence_io_contract_sb_io.py::test_io_pending_only_and_fresh_load_language`; `tests/test_peek_include_claimed.py::test_include_claimed_returns_superset_in_id_order`, `tests/test_peek_include_claimed.py::test_exact_id_peek_finds_claimed_row_only_with_flag`, `tests/test_peek_include_claimed.py::test_peeking_claimed_rows_mutates_nothing` |

## Related Plans

- retired: 2026-08-06-audit-remediation-plan — source `94e15bc`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-07-30-product-documentation-cutover-plan — source `5023710`;
  see the ledger in `docs/plans/README.md`
