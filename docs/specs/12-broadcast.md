# Broadcast

Status: Active

Owner: SimpleBroker queue-operation layer; each backend owns the atomic
substrate realization.

Boundary: broadcast target selection, validation, queue-creation policy,
atomic fan-out, CLI selector behavior, result count, and backend
compatibility.

Broadcast inserts a **copy of the message** into each selected queue.

## Target selection [SB-BCAST-1]

Selection describes **what exists at the selection point** (or the full
requested name set when creation is enabled).

- **No selector:** every queue that exists at the selection point.
- **Pattern** (`pattern=...` / CLI `--pattern GLOB`): existing literal queue
  names matching Python `fnmatchcase`. The legacy empty pattern remains
  equivalent to no pattern when used alone. Patterns name a class of existing
  queues; they never create queues.
- **Exact list** (`queue_names` / CLI `--queue QUEUE` repeated): unique
  requested literal names that exist at the selection point by default.
  Missing names are ignored and not created.
- **Exact list + `create_missing=True` (Python only):** every unique requested
  literal name, including names with no current row (this is the only create
  path).

`pattern` and `queue_names` are mutually exclusive (including `pattern=""`).
Selector-free, pattern, and CLI broadcasts never create queues. CLI exact
broadcast has no creation switch.

An empty Python exact-name sequence returns `0` and writes nothing. A missing
message body is an error. An empty string body (`""`) is a valid message and
is broadcast like any other body (including under `create_missing`).

## Python exact selector [SB-BCAST-2]

`queue_names` accepts a non-string sequence of names. SimpleBroker snapshots,
validates, and deduplicates it before mutation. `create_missing` is a boolean
and is valid only with `queue_names`.

Callers key on **exception types** (for example `TypeError` for a string-like
`queue_names` or a non-boolean `create_missing`; `ValueError` when creation is
requested without exact names or when pattern and names are combined). Message
text is diagnostic.

With creation disabled, the return value is the number of unique existing
queues reached. With creation enabled, one copy of the message is inserted for
every unique requested name and the return value is that requested-name count.
Exact selectors do not resolve aliases.

## Alias interaction [SB-BCAST-3]

Broadcast operates on literal queue names. Patterns match queue names, not
aliases. Exact names use the public queue-name validation contract; `@alias`
is not resolved as an exact broadcast target.

## Atomicity and result [SB-BCAST-4]

For the **selected set**, broadcast is atomic: every selected queue receives
one copy or none do.

- **SQL:** failures roll back the transaction for that operation.
- **Redis (patternless and exact paths):** anticipated failures are rejected
  before mutation; registry and message writes complete in one non-interleaved
  phase.

Atomicity covers failures anticipated and rejected before mutation. It does
not extend to unexpected runtime errors inside a backend's atomic script;
Redis does not promise rollback in that case.

Pattern selection resolves its target set before the atomic insertion point.
A queue created after that resolution may miss the broadcast, and a queue
deleted after it may be recreated by it. Selection is still “what matches at
selection,” not a separate create API.

No matching targets (including empty exact lists and existing-only lists that
hit nothing) is a **no-op**: return `0`, create no queues, and leave the id
high-water unchanged.

With `create_missing=True` the selected set is the complete unique requested
set, so a queue deleted before the atomic point is recreated by its new
pending message.

## CLI exact selector [SB-BCAST-5]

CLI `--queue QUEUE` is repeatable and mutually exclusive with `--pattern`.
Queue names are literal; commas are not split into multiple names. Long-option
abbreviations are rejected. `--` introduces a literal option-looking message.
CLI exact broadcast remains existing-only.

CLI output and exit status follow `[SB-CLI-*]`. A broadcast that reaches no
queues is the empty / no-matching outcome under those codes.

## Backend compatibility [SB-BCAST-6]

Exact-target broadcast is part of backend API v5. A direct backend must accept
`queue_names` and `create_missing`, preserve default existing-only selection,
implement full-requested-set creation when enabled, and preserve
`[SB-BCAST-1]` through `[SB-BCAST-4]`. Incompatible backend versions fail
during backend resolution with upgrade-or-pin guidance.

## Implementation mapping

- SQL/core selection and transaction: `simplebroker/db.py`,
  `BrokerCore.broadcast`
- CLI command boundary: `simplebroker/cli.py` and
  `simplebroker/commands.py`, `cmd_broadcast`
- Backend protocol and API version: `simplebroker/_backend_plugins.py`
- SQLite selection lock: `simplebroker/db.py::BrokerCore.broadcast` calls
  `runner.begin_immediate()` before selecting queues; the SQLite
  `simplebroker/_backends/sqlite/plugin.py::SQLiteBackendPlugin.prepare_broadcast`
  hook is intentionally a no-op
- PostgreSQL selection lock:
  `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- Redis selection and insertion:
  `extensions/simplebroker_redis/simplebroker_redis/core.py` and
  `extensions/simplebroker_redis/simplebroker_redis/scripts.py`

## Verification

| Clause | Firing evidence |
|--------|-----------------|
| [SB-BCAST-1] | `tests/test_broadcast.py::test_broadcast`, `tests/test_broadcast.py::test_broadcast_with_pattern`, `tests/test_broadcast.py::test_broadcast_to_repeated_exact_queues`, `tests/test_broadcast.py::test_broadcast_empty_pattern_still_targets_all_queues`; `tests/test_broadcast_api.py::test_broadcast_exact_empty_sequence_is_noop_not_broadcast_all`, `tests/test_broadcast_api.py::test_broadcast_empty_string_body_is_a_valid_message` |
| [SB-BCAST-2] | `tests/test_broadcast_api.py::test_broadcast_exact_deduplicates_and_ignores_missing_names`, `tests/test_broadcast_api.py::test_broadcast_exact_create_missing_reaches_full_requested_set`, `tests/test_broadcast_api.py::test_broadcast_exact_rejects_string_like_sequence`, `tests/test_broadcast_api.py::test_broadcast_create_missing_requires_boolean`, `tests/test_broadcast_api.py::test_broadcast_create_missing_requires_exact_names`, `tests/test_broadcast_api.py::test_broadcast_exact_validates_every_name_before_mutation`, `tests/test_broadcast_api.py::test_broadcast_snapshots_mutable_exact_names_once`, `tests/test_broadcast_api.py::test_broadcast_retry_uses_entry_snapshot_after_caller_mutation` |
| [SB-BCAST-3] | `tests/test_broadcast_api.py::test_broadcast_exact_does_not_resolve_aliases`; `tests/test_broadcast.py::test_broadcast_exact_queue_does_not_split_commas` |
| [SB-BCAST-4] | `tests/test_broadcast_api.py::test_broadcast_exact_rolls_back_all_targets_on_id_collision`, `tests/test_broadcast_api.py::test_broadcast_exact_create_missing_rolls_back_new_queues_on_id_collision`; `extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py::test_exact_broadcast_does_not_resurrect_queue_deleted_before_selection`, `extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py::test_exact_broadcast_create_missing_resurrects_queue_deleted_before_atomic_point`; `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_patternless_broadcast_does_not_resurrect_deleted_queue`, `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_exact_broadcast_does_not_resurrect_deleted_queue`, `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_exact_create_broadcast_resurrects_queue_deleted_before_atomic_point`, `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_patternless_broadcast_includes_queue_created_during_setup`, `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_broadcast_script_selects_queues_at_atomic_insertion_point`, `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_exact_create_script_rejects_candidate_conflicts_before_mutation`; `extensions/simplebroker_redis/tests/test_redis_integration.py::test_broadcast_empty_exact_create_missing_is_a_storage_and_maintenance_noop`, `extensions/simplebroker_redis/tests/test_redis_integration.py::test_broadcast_all_missing_exact_queue_names_preserves_persisted_last_ts`; `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py::test_redis_broadcast_fires_transition_table` |
| [SB-BCAST-5] | `tests/test_broadcast.py::test_broadcast_to_repeated_exact_queues`, `tests/test_broadcast.py::test_broadcast_pattern_and_queue_are_mutually_exclusive`, `tests/test_broadcast.py::test_broadcast_queue_prefix_is_rejected_before_mutation`, `tests/test_broadcast.py::test_broadcast_queue_prefix_can_be_literal_after_double_dash` |
| [SB-BCAST-6] | `tests/test_backend_plugin_resolution.py::test_external_backend_plugin_with_stale_backend_api_version_is_rejected`, `tests/test_backend_plugin_resolution.py::test_external_backend_plugin_with_future_backend_api_version_is_rejected`, `tests/test_backend_plugin_resolution.py::test_first_party_extension_plugins_declare_literal_backend_api_version` |

`tests/test_broadcast_contract_sb_bcast.py` binds every clause to this mapping,
AST-checks the named firing-test functions, and checks the registry, README,
agent-kernel, specs-index, and `llms.txt` pointers.

## Related Plans

- `docs/plans/2026-08-10-test-suite-signal-remediation-plan.md`
- retired: 2026-08-06-audit-remediation-plan — source `94e15bc`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-07-30-product-documentation-cutover-plan — source `5023710`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-07-28-explicit-broadcast-targets-plan — source `197629e2`;
  see `docs/plans/README.md`
- retired: 2026-07-28-broadcast-create-missing-plan — source `197629e2`;
  see `docs/plans/README.md`
