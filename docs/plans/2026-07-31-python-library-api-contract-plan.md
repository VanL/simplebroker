# Python Library API Contract

Status: active — reopened 2026-07-31 for Revision R1 (command-layer signature
correction before the surface freezes). Original scope completed 2026-07-31.
Date: 2026-07-31

## Goal

Add a **canonical product spec** for the public **Python library API**
(package root + `simplebroker.ext`, and any other intentionally public
modules), with cross-references to CLI equivalents where they exist (and
vice versa). Document **existing** public promises—do not invent a better
API in the port.

## Why now

CLI/delivery/broadcast/identity verticals are rewritten to README-true
product promises. Embedding consumers (especially Weft and Taut) use
`Queue`, `open_broker`, targets, dump/load, and a large slice of
`simplebroker.ext`. That surface is still largely `readme-only` / scattered
and is easy to over-document with mechanism.

## Consumers (evidence drivers)

Primary:

- **Weft** (`../weft`): `Queue`, `BrokerTarget`, `open_broker` patterns via
  helpers, `commands` for some paths, `dump_lines` / `load_lines`,
  `simplebroker.ext` (`BrokerError`, `IntegrityError`, `BaseWatcher`,
  `StopWatching`, `SidecarSession`, `PollingStrategy`, `get_backend_plugin`,
  activity waiters, etc.).
- **Taut** (`../taut`): `Queue`, `open_broker`, `BrokerTarget` /
  `resolve_broker_target`, `simplebroker.ext` (`IntegrityError`,
  `TimestampError`, `TimestampGenerator`, `SidecarSession`, `PollingStrategy`,
  `StopWatching`, backend plugin hooks).

Secondary: in-repo examples and tests; first-party backend packages as
**consumers of ext + private modules** (ext scope note already distinguishes
embedders vs backend authors).

## Proposed surface inventory (starting point)

### Package root (`simplebroker`)

From `__all__` today: `Queue`, `QueueWatcher`, `QueueMoveWatcher`,
`open_broker`, `BrokerTarget`, resolve/serialize/target helpers, `dump_lines` /
`load_lines`, `LoadResult`, `resolve_config`, `create_activity_waiter_for_queues`,
`ActivityWaiter`, stats/rename result types, `__version__`.

### `simplebroker.ext`

Stable embedder facade per module docstring: exceptions, `TimestampGenerator`,
`SidecarSession`, runners/setup hooks, delivery-guarantee types, maintenance
helpers, watcher bases/strategies, backend plugin **types**,
`get_backend_plugin`, and **project-config discovery helpers**
(`find_project_config`, `project_config_path_for_directory`,
`resolve_project_target`) — with the existing note that full alternate
backends also use private modules under pin + `backend_api_version`.

The three project-config helpers are the same objects as on
`simplebroker.project` (also listed in `project.__all__`). Prefer
`simplebroker.ext` for new embedder code; existing `simplebroker.project`
imports remain valid until consumers migrate on their own schedule.

### Second public surface: `simplebroker.commands`

CLI-equivalent `cmd_*` functions (stdout/stderr + exit codes), not the
primary Queue embedder path. Document as a named public submodule with
behavior owned by `[SB-CLI-*]` and the verticals for the underlying
operation. Weft proxies through this layer for CLI parity / implementation
reuse; that proxy pattern is valid for CLI wrappers, not the default
embedder guidance.

### Explicit non-goals for this spec

- Private `_` modules as public contract (except what ext already re-exports).
- Backend SDK completeness for third parties (handshake + pin story stays).
- Redesigning APIs “for better embedding.”
- Replacing CLI contract; only **cross-links** where the same operation exists
  in both forms.
- Promoting every Weft import path into package root `__all__` (commands stay
  a submodule; project-config helpers live on `ext` + `project`).

## Spec shape (proposed)

- New file: `docs/specs/16-python-library-api.md`, H1 **Python Library API**,
  stable codes **`[SB-API-*]`**.
- Registry: replace the current
  `Embedding targets, backends, sidecar` `readme-only` row (and only the
  **surface / embedding** part of residual README) with a `canonical-spec`
  row after inventory + firing gates. **Base queue/broker operation catalog**
  may stay `readme-only` for command/API catalog residual not claimed by
  verticals or this surface spec.
- Style bar from the product-contract rewrite: positive promises, no non-effect
  laundry lists, no frozen incidental exception **messages**, no mechanism as
  law unless callers must depend on it.

### Proposed file outline and provisional codes (review before full prose)

Preamble (no code): purpose; three public surfaces; document existing
behavior; embedder vs backend-author; non-goals; “verticals own semantics.”

| Code | Section title | Normative promise (one line) | Defers to |
|------|---------------|------------------------------|-----------|
| **SB-API-1** | Public surfaces | Package root `__all__`, `simplebroker.ext.__all__`, and `simplebroker.commands.__all__` are the supported import surfaces; private `_` modules are not public | — |
| **SB-API-2** | Targets and discovery | `BrokerTarget`, `open_broker`, `resolve_broker_target`, `target_for_directory`, serialize/deserialize, and project-config helpers (`find_project_config`, `project_config_path_for_directory`, `resolve_project_target` on **ext** and `project`) are the public ways to bind a broker | README project-scoping residual for env/TOML field catalog where needed |
| **SB-API-3** | Queue lifecycle | `Queue` is the primary programmatic queue handle; construction from path/target; close / context-manager cleanup | — |
| **SB-API-4** | Queue operations (library shape) | Write/read/peek/move/delete (and related public methods) return values or raise; they do not use CLI exit codes or stdout as the primary contract | `[SB-DELIVERY-*]`, `[SB-ID-*]`, `[SB-SELECT-*]`, `[SB-BCAST-*]` for operation meaning |
| **SB-API-5** | Generators and materialization | Generator and `*_many` forms follow delivery claim/handoff rules of the corresponding consume/peek/move mode; materializing APIs commit selected claims before returning lists where delivery requires it | `[SB-DELIVERY-*]` |
| **SB-API-6** | Watchers and activity waiters | Root `QueueWatcher` / `QueueMoveWatcher` / activity-waiter helpers and ext `BaseWatcher` / `PollingStrategy` / `StopWatching` / `default_error_handler` are the public watch embedding surface | `[SB-DELIVERY-2]` watch modes |
| **SB-API-7** | Sidecar | `sidecar` session entry (`SidecarSession`, `SidecarUnavailableError`, `RESERVED_TABLE_NAMES`) is the public embedder table surface co-located with the broker | — |
| **SB-API-8** | Dump and load (library) | `dump_lines` / `load_lines` / `LoadResult` are the public library I/O entry points; format and selection rules are not redefined here | `[SB-IO-*]` |
| **SB-API-9** | Errors | Public exception types are importable from `simplebroker.ext`; library failure is signaled by exceptions, not process exit codes; message text is not a frozen contract | `[SB-CLI-1]` for CLI exit codes |
| **SB-API-10** | Command layer (second surface) | `simplebroker.commands` exposes CLI-equivalent `cmd_*` (and listed helpers) with print-to-stdout / exit-code semantics matching the CLI; for process/CLI reuse, not the default embedder path | `[SB-CLI-*]` + verticals for the underlying op |
| **SB-API-11** | Ext advanced / backend-facing exports | Named advanced exports on `ext` (e.g. delivery-guarantee helpers, runners, plugin types, `BACKEND_API_VERSION`, maintenance helpers) remain importable; they do not constitute a complete third-party backend SDK; alternate backends pin + handshake | ext module scope note |
| **SB-API-12** | Cross-surface matrix | Library entry ↔ CLI command ↔ owning vertical is normative for orientation (see matrix below); conflicts on **operation meaning** are resolved by the vertical, not this row’s table alone | 10–15 |

**Not separate codes (fold into preamble / SB-API-1 / 11):** full symbol laundry lists (pin via structural tests on `__all__`); every Queue method name (catalog residual may stay README until a later ops cutover); Weft/Taut integration recipes.

### Cross-ref matrix (body of SB-API-12)

| Library | CLI / `commands` | Owning vertical |
|---------|------------------|-----------------|
| `Queue.write` / exact insert helpers | `write` / `cmd_write` | `[SB-ID-*]` (+ residual write catalog) |
| `Queue.read*` | `read` / `cmd_read` | `[SB-DELIVERY-*]`, `[SB-SELECT-*]` |
| `Queue.peek*` | `peek` / `cmd_peek` | `[SB-DELIVERY-4]`, `[SB-SELECT-*]` |
| `Queue.move*` | `move` / `cmd_move` | `[SB-DELIVERY-3]`, `[SB-ID-*]` |
| `Queue.delete*` | `delete` / `cmd_delete` | residual / delivery claimed lifecycle |
| Broadcast on connection/Queue | `broadcast` / `cmd_broadcast` | `[SB-BCAST-*]` |
| Watchers | `watch` / `cmd_watch` | `[SB-DELIVERY-2]` |
| `dump_lines` / `load_lines` | `dump` / `load` | `[SB-IO-*]` |
| Targets / project-config helpers | `-f`/`-d` / project scope CLI | residual project-scoping + SB-API-2 |
| `cmd_*` only | same CLI | `[SB-CLI-*]` presentation |

### Registry delta (at promotion, not at outline review)

| Concern | From | To |
|---------|------|----|
| Embedding targets, backends, sidecar | `readme-only` | **`canonical-spec`** `16-python-library-api.md` `[SB-API-1]`…`[SB-API-12]` (or split: sidecar/targets under API; backend-author remains “advanced” within SB-API-11) |
| Base queue/broker operation catalog residual | `readme-only` | **unchanged** for now (Command Reference / method catalog) |

Gate column (provisional): new
`tests/test_python_library_api_contract_sb_api.py` structural binds for
SB-API-1…12 + registry/README/kernel pointers; behavioral reuse of existing
Queue/watch/dump/ext suites where a clause fires on real behavior.

### README residual after promotion (pointers only at outline stage)

| README locus | After promotion |
|--------------|-----------------|
| `## Python API` | Restate + link `16-…`; keep examples |
| `## Embedding…` / project scoping | Link SB-API-2; env/TOML catalog may remain residual |
| `### Command layer` | Link SB-API-10 + `[SB-CLI-*]` |
| `### Sidecar tables` | Link SB-API-7 |
| Advanced extensions / backend note | Link SB-API-11 |
| Command Reference method laundry | Stays residual until ops catalog cutover |

### Kernel / llms (at promotion)

- `docs/agent-kernel.md`: short “Library surfaces” bullet → `16-…`
- `llms.txt` / specs index: add `16-python-library-api.md`

## Dispositions (from inventory)

| Item | Disposition |
|------|-------------|
| `simplebroker.commands` | Second public surface (CLI function layer); not package root |
| `find_project_config`, `project_config_path_for_directory`, `resolve_project_target` | Promote into `simplebroker.ext` (done) and `project.__all__`; same objects |
| Package root target helpers (`open_broker`, `BrokerTarget`, …) | Remain primary root embedder path |
| Unused root/ext exports (e.g. `QueueMoveWatcher`, many backend-author types) | Catalog in spec as exported; do not invent embedder doctrine |

## Tasks

1. [x] Inventory real public call sites in Weft and Taut (and examples); mark
   frequency and critical paths.
2. [x] Diff inventory against `simplebroker.__all__` and `simplebroker.ext.__all__`;
   note undocumented-but-used vs exported-but-unused.
3. [x] Promote project-config discovery helpers into `simplebroker.ext` (+
   `project.__all__`); pin identity in `tests/test_ext_imports.py`.
4. [x] Draft exact proposed spec sections + provisional `[SB-API-1]`…`12` +
   registry/README residual pointers (this plan section; full prose next after
   owner review of codes).
5. [x] Owner approved outline; full prose landed as
   `docs/specs/16-python-library-api.md` with registry `canonical-spec`
   (independent +P review remains available as follow-up hardening).
6. [x] Firing tests: `tests/test_python_library_api_contract_sb_api.py` +
   reuse of ext/public-surface/project-config/IO/delivery suites.
7. [x] Update `docs/agent-kernel.md`, `llms.txt`, specs index, README
   pointers; program-theory and invariant inventory aligned.

## Revision R1 — Command-layer signature correction (2026-07-31)

Class rises to **5**: this changes a public callable's binding, and
`[SB-API-10]` normatively declares `simplebroker.commands.__all__` stable under
the package compatibility policy. Ships as **6.0.0**.

### Why this reverses the original scope

The original plan says "Document **existing** public promises—do not invent a
better [API]," and deliberately excluded shape changes. That was right for a
documentation cutover. It becomes wrong at the moment the documentation turns
normative: promoting `[SB-API-10]` freezes whatever shape exists, so "document,
don't change" silently converts an accident into a contract.

The accident is real and follows the same age gradient as the module naming —
the original core verbs are misshapen, the later additions are correct:

| Function | positional | keyword-only |
|----------|-----------:|-------------:|
| `cmd_peek` | 9 | 0 |
| `cmd_move` | 9 | 0 |
| `cmd_read` | 8 | 1 |
| `cmd_watch` | 8 | 0 |
| `cmd_list` | 5 | 0 |
| `cmd_write` | 3 | 3 |
| `cmd_broadcast` | 3 | 2 |
| `cmd_rename` | 3 | 2 |

`cmd_peek(db, "q", True, False, True, None, None, None, None)` is legal today
and unreadable. Three consecutive booleans is a boolean trap in a surface the
same release declares stable.

`[SB-API-10]` also states that each `cmd_*` is "the programmatic equivalent of
a CLI subcommand." CLI flags are inherently *named* (`--json`,
`--timestamps`); keyword-only parameters mirror that shape, positional booleans
do not. So the correction moves the surface toward its own stated contract.

### Delta

Make every parameter after the target and queue operands keyword-only on
`cmd_read`, `cmd_peek`, `cmd_move`, `cmd_watch`, and `cmd_list`, matching
`cmd_write` / `cmd_broadcast` / `cmd_rename`. No parameter is renamed, removed,
reordered, or given a new default; only the binding changes.

### Why major, and why now

Breaking: a positional caller stops working. `[SB-API-10]` promises stability
for this surface, so the honest number is 6.0.0. Shipping a stability promise
and breaking it in the same version would be worse than either alone.

Now rather than later, because 6.0.0 is the release that first makes this
surface normative. Freeze the intended shape rather than the accidental one.
Practical breakage is near-zero: `cli.py` already passes keywords, and no
plausible caller writes nine positional arguments.

### Accompanying organization fixes

Two module-organization corrections found in the same review ship with R1.
They carry no spec impact and no version implication of their own.

**`db.py` module docstring.** It read "handles all SQLite operations," which is
false: `BrokerCore` is database-agnostic and PostgreSQL runs through it. The
replacement states what the module contains and gives a layout map. The module
*name* is correct and stays — `[SB-ID-1]` defines a database as any broker
target, Redis included.

**Split `helpers.py`.** It holds two domains with no shared state: retry and
setup policy (12 functions) and path/filesystem security (15 functions).
Nothing outside the package imports it — it appears in no `__all__` — so the
split is free of compatibility obligations.

- `_retry_policy.py` — pairs with `_retry.py` so the mechanism/policy division
  is legible from the module list, which is the actual defect. Today a reader
  asking "how does retry work here" must find two halves with nothing
  connecting them.
- `_paths.py` — path and filesystem security.

A `helpers/` package with a re-exporting `__init__` was rejected: it organizes
the interior while leaving `from .helpers import X` working, so no call site
migrates and the discoverability problem survives behind a tidier facade.

### Sequencing constraint

The `helpers.py` split rewrites imports in roughly eight test modules. A
concurrent change (`2026-07-31-core-test-mypy-gate-plan.md`) is rewriting most
of `tests/` for the strict mypy gate. The split lands **after** that work
settles; the signature correction and the docstring touch neither.

### Open decision for the owner

First-party extension versioning. Core goes 5.7.0 → 6.0.0 and extension floors
must rise to `simplebroker>=6.0.0`. The extensions' own APIs are unchanged by
R1, but `simplebroker_redis.core.canonicalize_queue` did change behavior
earlier in this release. Either extensions stay 3.4.0 with a raised floor, or
they take a bump of their own. Recorded here rather than decided.

## Out of scope

- **Weft/Taut consumer updates** (switching imports to `ext`, dropping
  non-public tests, bumping the pin). Those are separate post-publish work
  after a SimpleBroker release that includes this plan’s surface changes.
  Inventory evidence from Weft/Taut drives the **SimpleBroker** API design
  only; it does not obligate same-plan consumer PRs.
- CHANGELOG for pure documentation authority moves; **do** note additive public
  re-exports when they ship (project-config on `ext` is such a ship).
- Pattern-broadcast race code changes (owner: acceptable under exists-at-
  selection).

## Verification

- Spec + registry + gates green (`check-doc-paths`, contract structural tests).
- Weft/Taut inventory appendix in plan or implementation note (paths + symbols).
- No new public symbols invented; every normative sentence traces to existing
  behavior or an explicit separate product change.

## Related

- `docs/specs/10-cli.md` … `13-message-identity.md`
- `simplebroker/ext.py` scope note
- `docs/plans/2026-07-30-product-documentation-cutover-plan.md`
