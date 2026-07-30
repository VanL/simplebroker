# Broadcast Contract

Status: Active

Owner: SimpleBroker queue-operation layer; each backend owns the atomic
substrate realization.

Boundary: broadcast target selection, validation, queue-creation policy,
atomic fan-out, CLI selector behavior, result count, and backend
compatibility. Message identity format remains with the registry's
`Message identity` concern until that concern is canonically promoted;
general CLI I/O remains with `[SB-CLI-*]`; application notification meaning
remains outside SimpleBroker.

Required action: callers choose no more than one selector model and use
Python exact-name creation only when queue creation is intended. Backend
implementers preserve the backend-specific atomicity and compatibility
boundaries below.

## Target selection [SB-BCAST-1]

With no selector, broadcast targets every queue that exists at the backend's
selection point. A non-empty `pattern` or CLI `--pattern GLOB` targets
existing literal queue names with Python `fnmatchcase` semantics. The legacy
empty pattern remains equivalent to no pattern when used alone.

Python `queue_names` and repeatable CLI `--queue QUEUE` target the unique
requested literal names that exist at the selection point by default. Python
may pass `create_missing=True` with `queue_names`; that mode targets every
unique requested name, including names with no current row.

Non-`None` `pattern` and `queue_names` are mutually exclusive, including
`pattern=""`. An empty Python exact-name sequence returns `0` and performs no
write. Missing exact names are ignored unless Python explicitly enables
creation. Selector-free, pattern, and CLI broadcasts never create queues.

## Python exact selector [SB-BCAST-2]

`queue_names` accepts a non-string sequence. SimpleBroker snapshots, validates,
and deduplicates that sequence before mutation. `create_missing` is a strict
boolean and is valid only when `queue_names` is supplied. A string-like
`queue_names` raises
`TypeError("queue_names must be a sequence of queue names, not a string")`; a
non-boolean creation value raises
`TypeError("create_missing must be a boolean")`; creation without exact names
raises `ValueError("create_missing requires queue_names")`; and combining the
two selector forms raises
`ValueError("pattern and queue_names cannot be used together")`. Every
validation failure occurs before mutation.

With creation disabled, the return value is the number of unique existing
queues reached. With creation enabled, one ordinary pending message is
inserted for every unique requested literal name and the return value is that
requested-name count. Exact selectors do not resolve aliases.

## Alias interaction [SB-BCAST-3]

Broadcast operates on literal queue names. Patterns match queue names, not
aliases. Exact names use the public queue-name validation contract; `@alias`
is not resolved as an exact broadcast target.

## Atomicity and result [SB-BCAST-4]

SQL broadcast is atomic for the selected queue set: every selected queue
receives one copy or none do, and a timestamp or insertion failure rolls back
the transaction. Redis rejects every anticipated validation, layout,
namespace, capacity, candidate, and timestamp-conflict failure before its
first mutation, then performs registry and message writes in one
non-interleaved Lua phase. Redis does not promise rollback after an unexpected
Lua runtime error.

With `create_missing=True`, the selected set is the complete unique requested
set. A queue deleted before the atomic point may therefore be recreated by its
new pending message. Queue creation and deletion may race with default
selector evaluation. Redis pattern broadcast uses a client-side queue
snapshot: a queue created after that snapshot may miss the broadcast, and a
queue deleted after the snapshot may be recreated by the broadcast.
Patternless and exact Redis selectors choose their target set at the atomic
insertion point.

An empty exact sequence in either exact mode, and an all-missing existing-only
exact request, return `0` and must not persist timestamp-allocation,
queue-registry, message, wakeup, or maintenance state.

## CLI exact selector [SB-BCAST-5]

CLI `--queue QUEUE` is repeatable and mutually exclusive with `--pattern`.
Queue names are literal and comma-containing values are not split into
multiple names. Long-option abbreviations are rejected. `--` introduces a
literal option-looking message. CLI exact broadcast remains existing-only and
exposes no queue-creation switch.

CLI output and exit status continue to follow `[SB-CLI-*]`; a broadcast
reaching no queues is the existing empty/nothing-to-do outcome.

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
- SQLite selection lock: `simplebroker/_backends/sqlite/plugin.py`
- PostgreSQL selection lock:
  `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- Redis atomic selection and insertion:
  `extensions/simplebroker_redis/simplebroker_redis/core.py` and
  `extensions/simplebroker_redis/simplebroker_redis/scripts.py`

## Verification

| Clause | Firing evidence |
|--------|-----------------|
| [SB-BCAST-1] | `tests/test_broadcast_contract_sb_bcast.py`; `tests/test_broadcast.py::test_broadcast`, `test_broadcast_with_pattern`, `test_broadcast_to_repeated_exact_queues`, `test_broadcast_empty_pattern_still_targets_all_queues`; `tests/test_broadcast_api.py::test_broadcast_exact_empty_sequence_is_noop_not_broadcast_all` |
| [SB-BCAST-2] | `tests/test_broadcast_contract_sb_bcast.py`; `tests/test_broadcast_api.py::test_broadcast_exact_deduplicates_and_ignores_missing_names`, `test_broadcast_exact_create_missing_reaches_full_requested_set`, `test_broadcast_exact_rejects_string_like_sequence`, `test_broadcast_create_missing_requires_boolean`, `test_broadcast_create_missing_requires_exact_names`, `test_broadcast_exact_validates_every_name_before_mutation`, `test_broadcast_snapshots_mutable_exact_names_once`, `test_broadcast_retry_uses_entry_snapshot_after_caller_mutation` |
| [SB-BCAST-3] | `tests/test_broadcast_contract_sb_bcast.py`; `tests/test_broadcast_api.py::test_broadcast_exact_does_not_resolve_aliases`; `tests/test_broadcast.py::test_broadcast_exact_queue_does_not_split_commas` |
| [SB-BCAST-4] | `tests/test_broadcast_contract_sb_bcast.py`; `tests/test_broadcast_api.py::test_broadcast_exact_rolls_back_all_targets_on_id_collision`, `test_broadcast_exact_create_missing_rolls_back_new_queues_on_id_collision`; `extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py::test_exact_broadcast_does_not_resurrect_queue_deleted_before_selection`, `test_exact_broadcast_create_missing_resurrects_queue_deleted_before_atomic_point`; `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_patternless_broadcast_does_not_resurrect_deleted_queue`, `test_exact_broadcast_does_not_resurrect_deleted_queue`, `test_exact_create_broadcast_resurrects_queue_deleted_before_atomic_point`, `test_patternless_broadcast_includes_queue_created_during_setup`, `test_broadcast_script_selects_queues_at_atomic_insertion_point`, `test_exact_create_script_rejects_candidate_conflicts_before_mutation`; `extensions/simplebroker_redis/tests/test_redis_integration.py::test_broadcast_empty_exact_create_missing_is_a_storage_and_maintenance_noop`, `test_broadcast_all_missing_exact_queue_names_preserves_persisted_last_ts`; `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py::test_redis_broadcast_fires_transition_table` |
| [SB-BCAST-5] | `tests/test_broadcast_contract_sb_bcast.py`; `tests/test_broadcast.py::test_broadcast_to_repeated_exact_queues`, `test_broadcast_pattern_and_queue_are_mutually_exclusive`, `test_broadcast_queue_prefix_is_rejected_before_mutation`, `test_broadcast_queue_prefix_can_be_literal_after_double_dash`; `tests/test_cli_rearrange_args.py`; `[SB-CLI-*]` contract suite |
| [SB-BCAST-6] | `tests/test_broadcast_contract_sb_bcast.py`; `tests/test_backend_plugin_resolution.py::test_external_backend_plugin_with_stale_backend_api_version_is_rejected`, `test_external_backend_plugin_with_future_backend_api_version_is_rejected`, `test_first_party_extension_plugins_declare_literal_backend_api_version`; shared `tests/test_broadcast_api.py` under PostgreSQL and Redis; backend-specific integration tests named for `[SB-BCAST-4]` |

`tests/test_broadcast_contract_sb_bcast.py` binds every clause to this mapping,
AST-checks the named firing-test functions, and checks the registry, README,
agent-kernel, specs-index, and `llms.txt` pointers.

## Related Plans

- `docs/plans/2026-07-30-product-documentation-cutover-plan.md`
- retired: 2026-07-28-explicit-broadcast-targets-plan — source `36e2f356`;
  see `docs/plans/README.md`
- retired: 2026-07-28-broadcast-create-missing-plan — source `36e2f356`;
  see `docs/plans/README.md`
