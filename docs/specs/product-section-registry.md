# Product Section Registry

Mechanical authority table for product documentation. **One row per
concern family.** States: `readme-only` | `draft-spec` | `canonical-spec`.
The ownership rules live in `docs/README.md` and this registry.

| Concern | State | Spec section | README anchor / locus | Gate (obligation → impl → test) |
|---------|-------|--------------|----------------------|----------------------------------|
| CLI exit codes and CLI I/O contract | `canonical-spec` | `10-cli-contract.md` `[SB-CLI-1]`…`[SB-CLI-5]` | `### Exit Codes` (+ kernel Exit codes); bound string forms | `tests/test_documented_exit_codes.py` (SB-CLI-1 + README link); `tests/test_agent_kernel_contract.py` (SB-CLI-1 + kernel link); `tests/test_cli_contract_sb_cli.py` (SB-CLI-2…4); `tests/test_timestamp_selection_contract_sb_select.py` (SB-CLI-5 with SB-SELECT) |
| Delivery guarantees, claim/peek/watch safety | `canonical-spec` | `11-delivery-contract.md` `[SB-DELIVERY-1]`…`[SB-DELIVERY-7]` | README Critical Safety / Delivery; agent-kernel Delivery | `tests/test_delivery_contract_sb_delivery.py` (SB-DELIVERY-1…7 + registry/README/kernel binds); `tests/test_cross_thread_finalization_poisoning.py` + backend probe suites (SB-DELIVERY-6); `tests/test_cli_broken_pipe.py` (SB-DELIVERY-7) |
| Broadcast selection, creation, and atomicity | `canonical-spec` | `12-broadcast-contract.md` `[SB-BCAST-1]`…`[SB-BCAST-6]` | README “Fan-out with Broadcast”; agent-kernel broadcast table | `tests/test_broadcast_contract_sb_bcast.py` (SB-BCAST-1…6 structural, registry, README, kernel, and mapping binds); `tests/test_broadcast.py` + `tests/test_broadcast_api.py` (selectors, validation, CLI, results); SQL/Redis atomicity and backend-resolution suites (SB-BCAST-4/6) |
| Message identity, allocation, exact-ID handling, and preservation | `canonical-spec` | `13-message-identity-contract.md` `[SB-ID-1]`…`[SB-ID-5]` | README “Timestamps as Message IDs,” timestamp generation/insertion/cache sections, and move-preservation summaries; agent-kernel Message IDs | `tests/test_message_identity_contract_sb_id.py` (SB-ID-1…5 structural, authority, and row-local firing binds); shared reserved-zero admission and SQL write-transaction suites across SQLite/PostgreSQL/Redis; real-Valkey generated-write atomicity, stale-fence, monotone-resync, and `SM-REDIS-WRITE` suites; shared timestamp, write-return, insertion, cache, and move-preservation suites |
| Ordered timestamp selection and filter consequences | `canonical-spec` | `14-timestamp-selection-contract.md` `[SB-SELECT-1]`…`[SB-SELECT-4]` | README Command Options / Checkpoint-based Processing (reduced); agent-kernel filter note | `tests/test_timestamp_selection_contract_sb_select.py`; `tests/test_after_flag.py`; watcher peek progress tests under delivery/selection binds |
| Dump/load and claimed-row I/O | `canonical-spec` | `15-persistence-io-contract.md` `[SB-IO-1]`…`[SB-IO-5]` | README dump/load + include-claimed (reduced); agent-kernel Dump/load | `tests/test_persistence_io_contract_sb_io.py`; `tests/test_dump_load.py`; `tests/test_cli_dump_load.py`; `tests/test_cross_backend_dump_load.py` |
| Embedding targets, backends, sidecar | `readme-only` | — | README Embedding / Advanced | (future) |
| Base queue/broker operation catalog residual | `readme-only` | — | README Command Reference / Python API | (future) |

The base operation row owns only the remaining command/API catalog and base
operation meanings. It excludes CLI I/O and exits; delivery,
claim/peek/watch safety; broadcast selection, creation, and atomicity; message
identity, allocation, exact-ID handling, and preservation; ordered timestamp
selection and filter consequences; dump/load; and embedding, backends, and
sidecar. Those concerns remain with their existing rows.

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
  `36e2f356`; see the ledger in `docs/plans/README.md`
- retired: 2026-07-27-product-spec-doctrine-and-cli-vertical-plan — source
  `36e2f356`; see the ledger in `docs/plans/README.md`
- retired: 2026-07-28-delivery-contract-spec-promotion-plan — source
  `36e2f356`; see the ledger in `docs/plans/README.md`
- `docs/plans/2026-07-29-program-theory-and-negative-knowledge-plan.md`
- `docs/plans/2026-07-30-product-documentation-cutover-plan.md`
