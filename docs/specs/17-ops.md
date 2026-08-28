# Queue and Broker Operations

Normative residual **queue and broker operations** not owned by the
CLI-packaging, delivery, broadcast, identity, selection, I/O, or library-surface
verticals: implicit queue existence, metadata, physical delete, rename, aliases,
vacuum of claimed rows, and destructive backend target cleanup.

Write / read / peek / move / watch **meanings** remain
`[SB-DELIVERY-*]`, `[SB-ID-*]`, `[SB-SELECT-*]`, `[SB-BCAST-*]`. CLI exit codes
and streams remain `[SB-CLI-*]`. Library import surfaces remain `[SB-API-*]`.

## Implicit queues and existence [SB-OPS-1]

Queues are **implicit**. A queue **exists** when at least one message row
exists for that name, **including claimed** (deletion-pending) rows.

- There is no separate create-queue command for ordinary use; writing (or
  other row-creating operations) creates the name by having rows.
- After **vacuum** removes all claimed rows from a claimed-only queue, that
  queue **no longer exists**.
- `exists` / stats / list surfaces that count claimed rows follow this rule
  ([SB-OPS-2]); inspection of claimed rows is not ordinary pending delivery
  (`[SB-IO-5]`, `[SB-DELIVERY-1]`).

_Implementation mapping_:
- `simplebroker/db.py` / metadata helpers
- `simplebroker/sbqueue.py` (`exists`, `stats`)
- `simplebroker/commands.py` (`cmd_exists`, `cmd_list`, `cmd_stats`)

## Metadata: exists, stats, list [SB-OPS-2]

Public metadata reports queue presence and counts without delivering messages.

| Surface | Promise |
|---------|---------|
| **exists** | True when the queue has any row (pending or claimed); CLI exits `0` when true and `2` when false (`[SB-CLI-1]`) |
| **stats** | Reports **pending**, **claimed**, and **total** for one queue; `exists` is true when `total > 0` |
| **list** | Reports queue **names** that exist under [SB-OPS-1], including claimed-only queues |
| **list --stats** / library list-with-stats | Adds pending/claimed/total (and exists) per listed queue |
| **list filters** | `--prefix` is a literal name prefix; `--pattern` is fnmatch-style; prefix and pattern are not combined |

JSON shapes for CLI metadata commands follow `[SB-CLI-4]` (command-specific
objects, not message-line objects).

Library: `Queue.exists()`, `Queue.stats()` / `QueueStats`, and connection-level
list helpers (`list_queues`, `list_queue_stats` where public).

_Implementation mapping_:
- `simplebroker/metadata.py`, `simplebroker/sbqueue.py`, `simplebroker/db.py`
- `simplebroker/commands.py`
- `tests/test_queue_metadata.py`, `tests/test_cli_metadata.py`

## Physical delete [SB-OPS-3]

**Delete** removes matching rows **immediately**. It is not a consume claim
and does not leave deletion-pending claimed rows for the deleted content.

| Form | Effect |
|------|--------|
| Delete by message id | Physically removes that message row when present |
| Delete queue (by name) | Removes all rows for that queue name |
| Delete all queues | Removes all message rows in the broker |

On the library surface, argument omission is the explicit queue-wide form:
`Queue.delete()` removes all rows for that Queue and returns `True` only when
at least one row was removed. `Queue.delete(message_id=<id>)` targets one
exact message and returns whether that row was removed. Passing
`message_id=None` explicitly is invalid and raises `TypeError` before a
backend mutation. `Queue.delete_many(message_ids)` remains the collection
operation and returns the number of rows removed. These return values do not
change the atomicity or immediate physical-delete rules below.

Successful delete is atomic per queue. Delete-all is not promised to be
failure-atomic across every selected queue on every backend: Redis performs a
start-of-operation selection followed by per-queue atomic deletion, so a later
reservation or operational failure may be reported after an earlier subset was
removed. Callers must re-list live state after an error and may retry deletion
idempotently. SQL backends may provide stronger transaction atomicity.

CLI `delete` exit codes follow `[SB-CLI-1]`. Library `Queue.delete` /
`delete_many` and connection-level delete helpers use return values /
exceptions (`[SB-API-4]`).

Ordinary **read** still uses claim-before-handoff (`[SB-DELIVERY-1]`); claimed
rows from consume are reclaimed by **vacuum** ([SB-OPS-6]), not by delete’s
claim semantics.

_Implementation mapping_:
- `simplebroker/db.py`, `simplebroker/sbqueue.py`
- `simplebroker/commands.py` (`cmd_delete`)
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
- `tests/test_batch_delete.py`, `tests/test_delete_from_queues.py`,
  `tests/test_queue_api_additions.py`, `tests/test_safety_fixes.py`

## Rename [SB-OPS-4]

**Rename** retags existing messages from one queue name to another.

- Pending and claimed rows under the source name move to the destination name
  with **message ids preserved** (identity rules remain `[SB-ID-*]`).
- Destination that **already exists** (including claimed-only) is rejected
  without mutation.
- Missing source is a no-op success at the library layer; CLI missing source
  uses the empty / no-match exit family: exit `2` (`[SB-CLI-1]`).
- By default, aliases targeting the source are retargeted to the new name;
  an explicit no-retarget mode may leave aliases behind when documented on
  the API/CLI.
- CLI may accept `@alias` operands and reports **canonical** names in JSON;
  the Python API uses **literal** queue names only (`[SB-API-12]` alias note).

_Implementation mapping_:
- `simplebroker/db.py` (`rename_queue`)
- `simplebroker/commands.py` (`cmd_rename`)
- `simplebroker/metadata.py` (`QueueRenameResult`)
- `tests/test_queue_rename.py`, `tests/test_cli_rename.py`

## Aliases [SB-OPS-5]

Aliases map an alternate **name** to a **canonical queue name**.

- Stored in the broker, durable across processes, updated atomically.
- An alias requires the `@` prefix on the operand, in the CLI and in the
  library alike. A plain name always means the literal queue, so a queue and
  an alias may share a name without colliding. `canonicalize_queue(name)`
  applies this rule: it returns a plain name unchanged, resolves `@name` to
  its target, and raises `ValueError` for an empty or undefined alias.
- Alias names and targets obey the ordinary queue-name grammar. Because queues
  are implicit, a syntactically valid target need not currently have message
  rows.
- Alias mutation validates authoritative live state and publishes the alias
  plus alias-version update atomically. A new alias is rejected when its target
  is already an alias or when its own name is already the target of a stored
  alias. Concurrent conflicting adds have at most one successful winner and
  never silently overwrite another definition. New mutations therefore cannot
  create alias-to-alias chains or cycles in either order.
- Legacy invalid alias rows created by earlier releases are not automatically
  rewritten or deleted. They remain listable, one-hop resolvable, and removable
  so operators can unwind them; new mutation must not deepen or overwrite the
  invalid graph.
- Removing an alias does not delete messages; rows stay under the canonical
  name.
- Exact broadcast selectors do not resolve aliases (`[SB-BCAST-*]`).
- Library alias management is the `BrokerConnection` alias methods
  (`add_alias`, `remove_alias`, `list_aliases`, `resolve_alias`, `has_alias`,
  `aliases_for_target`, `get_alias_version`), public via `simplebroker.ext`
  and reachable from `open_broker(...)`. `simplebroker.commands` resolves
  `@name` like the CLI; `Queue` construction uses literal names only
  (`[SB-API-12]`).

_Implementation mapping_:
- `simplebroker/db.py` alias store
- `simplebroker/commands.py` (`cmd_alias_*`)
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
- `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
- `tests/test_aliases_db.py`, `tests/test_alias_cli.py`
- `extensions/simplebroker_redis/tests/test_redis_atomicity.py`

## Vacuum claimed rows [SB-OPS-6]

**Vacuum** removes **claimed** (deletion-pending) message rows.

- After vacuum, claimed-only queues cease to exist ([SB-OPS-1]).
- CLI global `--vacuum` runs vacuum and exits; `--compact` is only valid with
  `--vacuum` and, on SQLite, also runs database compaction to reclaim disk
  space. Compact is not required for correctness of claimed-row removal.
- Automatic maintenance may vacuum under config (`MaintenanceSchedule` /
  `vacuum_is_eligible` on `simplebroker.ext` are advanced surfaces —
  `[SB-API-11]`). Automatic vacuum is eligible when there are more than 10,000
  claimed messages regardless of the configured ratio; 10,000 alone does not
  fire that absolute backstop. Other scheduling details are implementation
  policy; product promise here is the backstop and that vacuum’s effect is
  removal of claimed rows.

Claimed rows may disappear at any time once vacuum is eligible; do not use
claimed visibility as durable application state (`[SB-IO-5]`).

_Implementation mapping_:
- `simplebroker/commands.py` (`cmd_vacuum`), `simplebroker/cli.py`
- `simplebroker/_maintenance.py`, backend vacuum paths
- `tests/test_vacuum_compact.py`, `tests/test_queue_metadata.py`,
  `tests/test_maintenance_policy.py`, `tests/test_constants.py`

## Destructive backend target cleanup [SB-OPS-7]

Global `--cleanup` is an explicitly destructive request to delete the
configured backend target state and exit. It is not a backup, rollback, or
quiescent-maintenance operation. Backend-specific effects are authoritative;
CLI exit codes and streams follow `[SB-CLI-1]` / `[SB-CLI-2]`.

SQLite `:memory:` and empty targets have no owned filesystem namespace; cleanup
is a successful no-op and derives no filenames for them. For a SQLite filesystem
target, one expanded, resolved path is frozen for validation and all owned-name
derivation.

For a SQLite filesystem target, cleanup owns the complete known SimpleBroker
filesystem namespace: the resolved configured main path; names formed by
appending `-journal`, `-wal`, `-shm`, `.lock`, `.status`, and `.vacuum.lock`;
and crash-residue entries matching
`.status.tmp.<decimal-pid>.<decimal-time_ns>` in the same directory. Both
variable components are nonempty ASCII decimal digits. No other prefix, glob,
or recursive scan is authorized.

Path derivation and main-path inspection are a zero-delete ownership preflight:
an inspection error aborts before mutation. Status-temp enumeration is
best-effort; a missing parent is empty, while any other enumeration error is
recorded, every fixed owned name is still attempted, and the command later
reports the error. Matching temp entries yielded before a mid-iteration error
remain candidates and are attempted in lexical order. If the main path exists,
it must validate as an initialized SimpleBroker database before any owned entry
is unlinked; failed validation leaves the whole namespace untouched. If the
main path is absent, the explicit destructive request may still unlink the
complete orphan namespace. An owned entry that is a symlink, including a
dangling symlink, is counted and unlinked without following it.

After validation, cleanup attempts `.status.tmp.*` residues, `.status`,
`.vacuum.lock`, `.lock`, `-journal`, `-wal`, `-shm`, then the main path. Fixed
names are unlinked directly: success means found and `FileNotFoundError` means
absent. A main or temp entry observed earlier still counts as found if it
disappears before unlink. Cleanup attempts every candidate even after a prior
failure; a partial result is possible and irreversible. Exit `0` means
enumeration succeeded and every candidate was absent or unlinked. After all
possible attempts, an enumeration or unlink failure produces one nonzero
operational-error result and one stderr diagnostic naming the failed attempts
and stating that other entries may already be gone. Cleanup does not retry or
roll back. `--quiet` suppresses success/no-op status but not this error. No
result promises that a concurrent process will not recreate a deleted name.

Apart from short-lived read-only ownership validation when the main file exists,
cleanup does not open SQLite, checkpoint, or wait for other connections. If any
active SimpleBroker operation/process using the target or any raw SQLite
connection overlaps cleanup, **the exact storage, coordination, and client
outcomes are undefined**. This includes durability and visibility of old or new
writes, which database generation a client observes, whether an operation errors
or appears to succeed, whether owned names reappear, whether generations
interfere, and when unlinked disk space is reclaimed. This is SQLite's upstream
boundary for unlinking an open database on Unix, not a SimpleBroker concurrency
guarantee. Active SimpleBroker setup, phaselock, status-publication, and vacuum
operations are also undefined overlap because deleting a held lock path can
split coordination across old and replacement inodes. Concurrent directory-entry
replacement is likewise outside the validation guarantee. On Windows and other
systems that refuse deletion of an open entry, those failures follow the same
aggregate error contract; earlier successful deletions are not rolled back.
Operators who need a predictable result must stop all SimpleBroker activity and
raw SQLite clients before cleanup and must make any required backup first.

_Implementation mapping_:
- `simplebroker/_backends/sqlite/plugin.py` (`cleanup_target`)
- `simplebroker/_backends/sqlite/validation.py` (ownership validation)
- `simplebroker/cli.py` (exit, quiet, JSON, and diagnostics)

## What this family does not own

| Concern | Owner |
|---------|--------|
| Exit codes, streams, JSON packaging, bound string forms | `[SB-CLI-*]` |
| Claim, peek, watch, move reservation, generators | `[SB-DELIVERY-*]` |
| Broadcast selection and atomicity | `[SB-BCAST-*]` |
| Message id allocation and exact insert | `[SB-ID-*]` |
| After/before filters | `[SB-SELECT-*]` |
| Dump/load format | `[SB-IO-*]` |
| Public import surfaces | `[SB-API-*]` |
| Project scoping env/TOML field catalogs | README residual + `[SB-API-2]` callables |
| Full method-by-method Python catalog | README examples (non-competing) |

## Implementation mapping (summary)

- Metadata and rename: `simplebroker/metadata.py`, `db.py`, `sbqueue.py`
- Aliases: `db.py`, `commands.py`
- Delete: `db.py`, `sbqueue.py`, `commands.py`
- Vacuum: `commands.py`, `cli.py`, maintenance helpers
- Cleanup: SQLite backend plugin/validation and `cli.py`

## Verification

| Clause | Firing evidence |
|--------|-----------------|
| [SB-OPS-1] | `tests/test_operations_contract_sb_ops.py`; `tests/test_queue_metadata.py::test_vacuum_removes_claimed_only_queue_existence` |
| [SB-OPS-2] | `tests/test_operations_contract_sb_ops.py`; `tests/test_cli_metadata.py`; `tests/test_queue_metadata.py` |
| [SB-OPS-3] | `tests/test_operations_contract_sb_ops.py::test_ops_delete_removes_row_immediately`; `tests/test_queue_api_comprehensive.py::test_delete_all`; `tests/test_queue_api_additions.py::test_queue_delete_explicit_none_is_rejected_without_mutation`; `tests/test_batch_delete.py::test_queue_delete_many_uses_physical_batch_delete`; `tests/test_custom_runner_integration.py::test_queue_delete_owns_an_explicit_transaction_and_commits_once`, `tests/test_custom_runner_integration.py::test_delete_all_owns_the_same_explicit_transaction`, `tests/test_custom_runner_integration.py::test_queue_delete_rolls_back_a_mutation_failure_and_preserves_the_error`; `tests/test_commands_error_ownership.py::test_cmd_delete_missing_queue_reports_no_match_without_output`, `test_cmd_delete_all_empty_reports_no_match_without_output`, `test_cmd_delete_nonempty_reports_success`; `tests/test_safety_fixes.py::test_delete_with_all_flag`, `test_delete_no_match_uses_queue_empty_exit_without_output`; `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_delete_queue_script_rechecks_reservation_without_partial_mutation`, `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_delete_all_reports_real_partial_completion_when_later_queue_reserved` |
| [SB-OPS-4] | `tests/test_queue_rename.py`; `tests/test_cli_rename.py`; `tests/test_operations_contract_sb_ops.py` |
| [SB-OPS-5] | `tests/test_aliases_db.py::test_alias_and_target_use_queue_name_grammar`, `tests/test_aliases_db.py::test_alias_rejects_chain_in_creation_order_without_mutation`, `tests/test_aliases_db.py::test_alias_add_revalidates_against_live_state`, `tests/test_aliases_db.py::test_legacy_alias_chain_remains_one_hop_visible_and_removable`; `tests/test_alias_cli.py::test_alias_add_help_calls_target_a_canonical_queue_name`; `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_concurrent_alias_adds_have_one_winner_and_flat_live_state` |
| [SB-OPS-6] | `tests/test_operations_contract_sb_ops.py::test_ops_language_core_promises`; `tests/test_maintenance_policy.py::test_vacuum_eligibility_preserves_ratio_and_absolute_rules`; `tests/test_queue_metadata.py::test_vacuum_removes_claimed_only_queue_existence`; `tests/test_vacuum_compact.py::test_vacuum_compact_database_size_reduction`; `extensions/simplebroker_pg/tests/test_pg_maintenance.py::test_vacuum_leases_connection_for_advisory_lock_lifetime`, `extensions/simplebroker_pg/tests/test_pg_maintenance.py::test_vacuum_unlock_false_releases_without_warning`, `extensions/simplebroker_pg/tests/test_pg_maintenance.py::test_vacuum_discards_checkout_when_unlock_completion_is_unknown`, `extensions/simplebroker_pg/tests/test_pg_maintenance.py::test_vacuum_body_base_exception_survives_ordinary_rollback_failure`; `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py::test_discard_thread_connection_preserves_nested_lease_for_replacement`, `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py::test_leased_commit_failure_closes_advisory_lock_session_before_replacement`; `extensions/simplebroker_pg/tests/test_pg_state_machine_transitions.py::test_pg_vacuum_fires_transition_table` |
| [SB-OPS-7] | `tests/test_operations_contract_sb_ops.py::test_ops_language_core_promises`; `tests/test_cli_argument_parsing.py::test_cleanup_help_uses_backend_generic_target_wording`; `tests/test_cleanup.py::test_cleanup_removes_complete_owned_namespace_only`; `tests/test_cleanup.py::test_cleanup_nonexistent_database`; `tests/test_cleanup.py::test_cleanup_rejects_plain_file`; `tests/test_cleanup.py::test_cleanup_rejects_directory_main_before_deleting_sidecars`; `tests/test_cleanup.py::test_cleanup_rejects_unreadable_main_before_deleting_sidecars`; `tests/test_cleanup.py::test_cleanup_rejects_sqlite_db_with_wrong_magic`; `tests/test_cleanup.py::test_cleanup_removes_owned_orphans_when_main_is_absent`; `tests/test_cleanup.py::test_cleanup_attempts_every_later_path_after_each_unlink_failure`; `tests/test_cleanup.py::test_cleanup_unlinks_owned_symlinks_without_touching_targets`; `tests/test_cleanup.py::test_cleanup_observed_main_disappearance_still_counts_as_found`; `tests/test_cleanup.py::test_cleanup_enumerated_temp_disappearance_still_counts_as_found`; `tests/test_cleanup.py::test_cleanup_aggregates_multiple_cli_failures_and_json_error`; `tests/test_cleanup.py::test_cleanup_windows_open_handle_refusal_is_clean_and_nonrollback`; `tests/test_cleanup.py::test_cleanup_validates_literal_uri_metacharacters`; `tests/test_cleanup.py::test_cleanup_cli_accepts_literal_percent_filename`; `tests/test_cleanup.py::test_cleanup_cli_retains_unsafe_metacharacter_rejection`; `tests/test_cleanup.py::test_cleanup_no_namespace_targets_are_noops_without_creation_or_open`; `tests/test_cleanup.py::test_cleanup_path_derivation_error_is_a_clean_database_error`; `tests/test_cleanup.py::test_cleanup_freezes_resolved_symlink_target_namespace`; `tests/test_cleanup.py::test_cleanup_main_lstat_failure_is_a_zero_delete_gate`; `tests/test_cleanup.py::test_cleanup_enumeration_failure_still_attempts_frozen_names_and_all_fixed`; `tests/test_cleanup.py::test_cleanup_reports_enumeration_before_ordered_unlink_failures`; `tests/test_cleanup.py::test_cleanup_multiple_temp_failures_are_reported_in_lexical_order`; `tests/test_cleanup.py::test_cleanup_with_quiet` |

## Related Plans

- completed: [2026-08-27-all-examples-correctness-and-contract-alignment-plan](../plans/2026-08-27-all-examples-correctness-and-contract-alignment-plan.md)
  — repairs example stats, rename, exact delete, and queue mutation handling
- active: [2026-08-25-verified-review-findings-remediation-plan](../plans/2026-08-25-verified-review-findings-remediation-plan.md)
  — delete no-match result parity and PostgreSQL uncertain-unlock session discard
- completed: [2026-08-24-failure-path-and-contract-findings-resolution-plan](../plans/2026-08-24-failure-path-and-contract-findings-resolution-plan.md)
  — explicit SQL transaction ownership for queue/all delete at baseline
  `1b8ecfa0`; Redis behavior unchanged
- retired: 2026-08-23-public-api-and-cli-review-remediation-plan — source
  `2605b79a` (local-only pin); see the ledger in `docs/plans/README.md`
- retired: 2026-08-10-test-suite-signal-remediation-plan — source `0d15871`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-06-pre-release-review-remediation-plan — source `84159198`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-06-audit-remediation-plan — source `94e15bc`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-07-30-product-documentation-cutover-plan — source `5023710`;
  see the ledger in `docs/plans/README.md`
