# Queue and Broker Operations

Normative residual **queue and broker operations** not owned by the
CLI-packaging, delivery, broadcast, identity, selection, I/O, or library-surface
verticals: implicit queue existence, metadata, physical delete, rename, aliases,
and vacuum of claimed rows.

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
- `tests/test_queue_metadata.py`, `tests/test_cli_queue_metadata.py`

## Physical delete [SB-OPS-3]

**Delete** removes matching rows **immediately**. It is not a consume claim
and does not leave deletion-pending claimed rows for the deleted content.

| Form | Effect |
|------|--------|
| Delete by message id | Physically removes that message row when present |
| Delete queue (by name) | Removes all rows for that queue name |
| Delete all queues | Removes all message rows in the broker |

CLI `delete` exit codes follow `[SB-CLI-1]`. Library `Queue.delete` /
`delete_many` and connection-level delete helpers use return values /
exceptions (`[SB-API-4]`).

Ordinary **read** still uses claim-before-handoff (`[SB-DELIVERY-1]`); claimed
rows from consume are reclaimed by **vacuum** ([SB-OPS-6]), not by delete’s
claim semantics.

_Implementation mapping_:
- `simplebroker/db.py`, `simplebroker/sbqueue.py`
- `simplebroker/commands.py` (`cmd_delete`)
- `tests/test_batch_delete.py`, `tests/test_delete_from_queues.py`

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
- Alias **names** are stored without `@`. Targets must be real queue names,
  not aliases (no alias-to-alias / cycles).
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
- `tests/test_aliases_db.py`, `tests/test_alias_cli.py`

## Vacuum claimed rows [SB-OPS-6]

**Vacuum** removes **claimed** (deletion-pending) message rows.

- After vacuum, claimed-only queues cease to exist ([SB-OPS-1]).
- CLI global `--vacuum` runs vacuum and exits; `--compact` is only valid with
  `--vacuum` and, on SQLite, also runs database compaction to reclaim disk
  space. Compact is not required for correctness of claimed-row removal.
- Automatic maintenance may vacuum under config (`MaintenanceSchedule` /
  `vacuum_is_eligible` on `simplebroker.ext` are advanced surfaces —
  `[SB-API-11]`); product promise here is that vacuum’s effect is removal of
  claimed rows, not a schedule contract.

Claimed rows may disappear at any time once vacuum is eligible; do not use
claimed visibility as durable application state (`[SB-IO-5]`).

_Implementation mapping_:
- `simplebroker/commands.py` (`cmd_vacuum`), `simplebroker/cli.py`
- `simplebroker/_maintenance.py`, backend vacuum paths
- `tests/test_vacuum_compact.py`, `tests/test_queue_metadata.py`

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

## Verification

| Clause | Firing evidence |
|--------|-----------------|
| [SB-OPS-1] | `tests/test_operations_contract_sb_ops.py`; `tests/test_queue_metadata.py::test_vacuum_removes_claimed_only_queue_existence` |
| [SB-OPS-2] | `tests/test_operations_contract_sb_ops.py`; `tests/test_cli_queue_metadata.py`; `tests/test_queue_metadata.py` |
| [SB-OPS-3] | `tests/test_operations_contract_sb_ops.py`; `tests/test_batch_delete.py`; `tests/test_delete_from_queues.py` |
| [SB-OPS-4] | `tests/test_queue_rename.py`; `tests/test_cli_rename.py`; `tests/test_operations_contract_sb_ops.py` |
| [SB-OPS-5] | `tests/test_aliases_db.py`; `tests/test_alias_cli.py`; `tests/test_operations_contract_sb_ops.py` |
| [SB-OPS-6] | `tests/test_vacuum_compact.py`; `tests/test_queue_metadata.py`; `tests/test_operations_contract_sb_ops.py` |

## Related Plans

- `docs/plans/2026-07-30-product-documentation-cutover-plan.md` (Phase 5)
