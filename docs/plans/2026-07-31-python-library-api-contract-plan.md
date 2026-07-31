# Python Library API Contract

Status: active — class 3+P
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
helpers, watcher bases/strategies, backend plugin **types** and
`get_backend_plugin` — with the existing note that full alternate backends also
use private modules under pin + `backend_api_version`.

### Explicit non-goals for this spec

- Private `_` modules as public contract (except what ext already re-exports).
- Backend SDK completeness for third parties (handshake + pin story stays).
- Redesigning APIs “for better embedding.”
- Replacing CLI contract; only **cross-links** where the same operation exists
  in both forms.

## Spec shape (proposed)

- New file e.g. `docs/specs/14-python-library-api-contract.md` with stable
  `[SB-API-*]` (or similar) codes.
- Registry row: concern “Python library / embedding API” → `canonical-spec`
  after inventory + firing gates.
- Sections by concern: targets & config; `Queue` / `open_broker`; write/read/
  peek/move/delete; generators & delivery_guarantee (link `[SB-DELIVERY-*]`);
  watchers; dump/load; errors; ext embedder surface; what is **not** public.
- Cross-ref matrix: library operation ↔ CLI command ↔ owning vertical spec
  (CLI / delivery / broadcast / identity).
- Style bar from the product-contract rewrite: positive promises, no non-effect
  laundry lists, no frozen incidental exception **messages**, no mechanism as
  law unless callers must depend on it.

## Tasks

1. Inventory real public call sites in Weft and Taut (and examples); mark
   frequency and critical paths.
2. Diff inventory against `simplebroker.__all__` and `simplebroker.ext.__all__`;
   note undocumented-but-used vs exported-but-unused.
3. Draft exact proposed spec sections + registry delta + README residual
   pointers (class 5 if behavior authority moves).
4. Independent review (+P) before promotion.
5. Firing tests: structural clause inventory + behavioral binds for high-risk
   APIs already covered by existing suites where possible.
6. Update `docs/agent-kernel.md` and `llms.txt` pointers; keep CLI contract
   cross-links bidirectional where useful.

## Out of scope

- CHANGELOG user-facing release notes until a published behavior/API change
  ships (this plan is documentation authority).
- Changing Weft/Taut call sites.
- Pattern-broadcast race code changes (owner: acceptable under exists-at-
  selection).

## Verification

- Spec + registry + gates green (`check-doc-paths`, contract structural tests).
- Weft/Taut inventory appendix in plan or implementation note (paths + symbols).
- No new public symbols invented; every normative sentence traces to existing
  behavior or an explicit separate product change.

## Related

- `docs/specs/10-cli-contract.md` … `13-message-identity-contract.md`
- `simplebroker/ext.py` scope note
- `docs/plans/2026-07-30-product-documentation-cutover-plan.md`
