# Product Section Registry

Mechanical authority table for product documentation. **One row per
concern family.** States: `readme-only` | `draft-spec` | `canonical-spec`.
The ownership rules live in `docs/README.md` and this registry.

| Concern | State | Spec section | README anchor / locus | Gate (obligation → impl → test) |
|---------|-------|--------------|----------------------|----------------------------------|
| CLI exit codes and CLI I/O contract | `canonical-spec` | `10-cli.md` `[SB-CLI-1]`…`[SB-CLI-6]` | `### Exit Codes` (+ kernel Exit codes); bound string forms; newest-first bounded read/peek/move | `tests/test_documented_exit_codes.py` (SB-CLI-1 + README link); `tests/test_agent_kernel_contract.py` (SB-CLI-1 + kernel link); `tests/test_cli_contract_sb_cli.py` (SB-CLI-2…4); `tests/test_json_message_id_contract.py` (SB-CLI-4 identity strings); `tests/test_timestamp_selection_contract_sb_select.py` (SB-CLI-5 with SB-SELECT); [SB-CLI-6] firing evidence lands with the active message-ID order plan's CLI slice |
| Delivery guarantees, claim/peek/watch safety | `canonical-spec` | `11-delivery.md` `[SB-DELIVERY-1]`…`[SB-DELIVERY-8]` | README Critical Safety / Safe Message Handling / Delivery; `docs/guides/python.md` delivery + watchers depth; agent-kernel Delivery | `tests/test_delivery_contract_sb_delivery.py` (SB-DELIVERY-1…8 + registry/README/kernel binds); `tests/test_cross_thread_finalization_poisoning.py` + backend probe suites (SB-DELIVERY-6); `tests/test_cli_broken_pipe.py` (SB-DELIVERY-7); `tests/test_property_queue_names.py`, `tests/test_message_size_contract.py`, `tests/test_property_message_roundtrip.py` (SB-DELIVERY-8) |
| Broadcast selection, creation, and atomicity | `canonical-spec` | `12-broadcast.md` `[SB-BCAST-1]`…`[SB-BCAST-6]` | README “Fan-out with Broadcast”; agent-kernel broadcast table | `tests/test_broadcast_contract_sb_bcast.py` (SB-BCAST-1…6 structural, registry, README, kernel, and mapping binds); `tests/test_broadcast.py` + `tests/test_broadcast_api.py` (selectors, validation, CLI, results); SQL/Redis atomicity and backend-resolution suites (SB-BCAST-4/6) |
| Message identity, allocation, exact-ID handling, and preservation | `canonical-spec` | `13-message-identity.md` `[SB-ID-1]`…`[SB-ID-5]` | README “Timestamps as Message IDs” (bound identity sentences + move-preservation summary); `docs/guides/python.md` generation/insertion/high-water workflows; agent-kernel Message IDs | `tests/test_message_identity_contract_sb_id.py` (SB-ID-1…5 structural, authority, and row-local firing binds); `tests/test_json_message_id_contract.py` and `tests/test_message_id_validation.py` (owned JSON fields and formatter vectors); shared reserved-zero admission and SQL write-transaction suites across SQLite/PostgreSQL/Redis; real-PostgreSQL and real-Valkey monotone-resync suites; real-Valkey generated-write atomicity, stale-fence, and `SM-REDIS-WRITE` suites; shared timestamp, write-return, insertion, cache, and move-preservation suites |
| Ordered timestamp selection and filter consequences | `canonical-spec` | `14-timestamp-selection.md` `[SB-SELECT-1]`…`[SB-SELECT-5]` | README Command Options / Filtering by message id; agent-kernel filter and public-ID order note | `tests/test_timestamp_selection_contract_sb_select.py`; `tests/test_after_flag.py`; watcher peek progress tests under delivery/selection binds; [SB-SELECT-5] firing evidence lands with the active message-ID order plan's backend slices |
| Dump/load and claimed-row I/O | `canonical-spec` | `15-persistence-io.md` `[SB-IO-1]`…`[SB-IO-5]` | README dump/load + include-claimed (reduced); agent-kernel Dump/load | `tests/test_persistence_io_contract_sb_io.py`; `tests/test_dump_load.py` and `tests/test_json_message_id_contract.py` (string output and legacy numeric input); routine `extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py` and `extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py`; opt-in direct PostgreSQL↔Redis `tests/test_cross_backend_dump_load.py`; claimed inspection `tests/test_peek_include_claimed.py` |
| Python library / embedding API surfaces | `canonical-spec` | `16-python-library-api.md` `[SB-API-1]`…`[SB-API-13]` | README Python API tour; `docs/guides/python.md` (Embedding / Command layer / Sidecar / waiters / PostgreSQL inspection); agent-kernel surfaces | `tests/test_python_library_api_contract_sb_api.py`; `tests/test_ext_imports.py`; `tests/test_public_surface.py`; `extensions/simplebroker_pg/tests/test_connection_stats.py`; delivery/IO/project-config suites reused for linked meaning |
| Queue and broker residual operations | `canonical-spec` | `17-ops.md` `[SB-OPS-1]`…`[SB-OPS-7]` | README Command Reference (metadata, delete, rename, aliases, vacuum, destructive target cleanup); Python API metadata | `tests/test_operations_contract_sb_ops.py`; `tests/test_queue_metadata.py`; `tests/test_cli_metadata.py`; `tests/test_queue_rename.py`; `tests/test_cli_rename.py`; `tests/test_aliases_db.py`; `tests/test_alias_cli.py`; `tests/test_batch_delete.py`; `tests/test_delete_from_queues.py`; `tests/test_custom_runner_integration.py`; `tests/test_commands_error_ownership.py`; `tests/test_queue_api_additions.py`; `tests/test_queue_api_comprehensive.py`; `tests/test_safety_fixes.py`; `tests/test_vacuum_compact.py`; `tests/test_maintenance_policy.py`; `tests/test_constants.py`; `tests/test_cleanup.py`; `tests/test_cli_argument_parsing.py`; `extensions/simplebroker_pg/tests/test_pg_maintenance.py`; `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py`; `extensions/simplebroker_pg/tests/test_pg_state_machine_transitions.py`; `extensions/simplebroker_redis/tests/test_redis_atomicity.py` (flat concurrent alias mutation and partial delete-all truth) |

Every current product concern family is `canonical-spec`. README may still
carry catalogs, examples, project-scoping field lists, and concise restatements
with links; those are not competing SoT rows. CLI I/O and exits; delivery;
broadcast; message identity; timestamp selection; dump/load; library surfaces;
and residual ops each have a registered owner above.

## README TOC ownership (final cutover audit)

Major root-`README.md` product sections and their registered owners. Orientation,
install, patterns, performance catalogs, architecture narrative, and
contributing are **human entry** (not separate SoT rows). Project-scoping
**field lists** are catalog residual; discovery **callables** are `[SB-API-2]`.

| README locus | Owner |
|--------------|-------|
| Command Reference / Global Options / Commands table | Catalog + `[SB-CLI-3]` (global option position); per-command meaning via rows below |
| Queue Aliases | `[SB-OPS-5]` |
| Command Options (filters, write flags, watch flags, metadata options) | `[SB-SELECT-*]` / `[SB-CLI-5]` for bounds; `[SB-OPS-1]`–`[SB-OPS-2]` for metadata; delivery for watch modes |
| Exit Codes | `[SB-CLI-1]`–`[SB-CLI-4]` |
| Critical Safety / Safe Message Handling / watch | `[SB-DELIVERY-*]` |
| Timestamps as Message IDs | `[SB-ID-*]` |
| Filtering by message id | `[SB-SELECT-*]` (+ `[SB-CLI-5]`) |
| Dump/load mentions | `[SB-IO-*]` |
| Real-time Queue Watching (compact; depth in `docs/guides/python.md`) / Pipe behavior | `[SB-DELIVERY-2]`, `[SB-DELIVERY-3]`, `[SB-DELIVERY-7]` |
| Python API tour / Delivery guarantees summary (depth in `docs/guides/python.md`) | `[SB-API-*]` packaging + `[SB-DELIVERY-*]` |
| Queue metadata | `[SB-OPS-1]`–`[SB-OPS-2]` |
| Global destructive target cleanup | `[SB-OPS-7]` |
| Exact IDs / high-water / generate timestamp (bound identity sentences in README Core Concepts; workflows in `docs/guides/python.md`) | `[SB-ID-*]` |
| Sidecar / Command layer / Embedding / PostgreSQL inspection (in `docs/guides/python.md`; README keeps embedding summary) | `[SB-API-7]`, `[SB-API-10]`, `[SB-API-13]`, `[SB-API-1]`–`[SB-API-13]` |
| Fan-out / broadcast examples | `[SB-BCAST-*]` |
| Environment Variables (common-settings table; full catalog in `docs/guides/configuration.md`) / Project Scoping summary (full treatment in the same guide) | Human catalog; targets/discovery callables `[SB-API-2]` |

## Transition rule

A **migration** state change requires one PR that updates this table, the
spec file (if any), the README pointer when entering `canonical-spec`, and
every Gate cell named for that row. **Entering `canonical-spec` requires a
firing test per numbered clause** (no unbound obligations) and two-way
promise-equivalence evidence against the winning source baseline. Migration
may relocate promises but may not add, remove, narrow, broaden, correct, or
deprecate them. Implementation evidence can block the transition or fire an
existing clause; it cannot supply a new clause. Bring inconsistencies to the
product owner. A contract change requires an exact proposed delta and explicit
owner authorization, lands separately, and is followed by a migration
rebaseline. After canonical, **edit the spec in place** for
behavior/wording changes; update this registry only when ownership or gates
change (e.g. new clause + new gate), not to “retire” the section. Incomplete
migration transitions are forbidden. Abandoning an **unshipped** `draft-spec`
may return to `readme-only` per the ownership decision.

## Related Plans

- retired: 2026-07-27-product-docs-source-ownership-decision — source
  `197629e2`; see the ledger in `docs/plans/README.md`
- retired: 2026-07-27-product-spec-doctrine-and-cli-vertical-plan — source
  `197629e2`; see the ledger in `docs/plans/README.md`
- retired: 2026-07-28-delivery-contract-spec-promotion-plan — source
  `197629e2`; see the ledger in `docs/plans/README.md`
- retired: 2026-07-29-program-theory-and-negative-knowledge-plan — source
  `5023710`; see the ledger in `docs/plans/README.md`
- retired: 2026-07-30-product-documentation-cutover-plan — source `5023710`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-04-docs-information-architecture-plan — source `c403c5eb`;
  see the ledger in `docs/plans/README.md`
