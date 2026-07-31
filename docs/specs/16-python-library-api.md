# Python Library API

Normative public **Python** surfaces for embedding SimpleBroker: package root
exports, `simplebroker.ext`, and the CLI-equivalent command layer. Queue
**operation meaning** (claim, peek, move, ids, filters, broadcast, dump/load
format) is owned by the vertical specs; this document owns **which names are
public**, how library failure and packaging differ from the CLI, and how
surfaces relate.

Document existing public promises only. This is not a redesign and not a
complete third-party backend SDK.

## Public surfaces [SB-API-1]

Supported import surfaces:

| Surface | Role |
|---------|------|
| `simplebroker` (`__all__`) | Primary embedder API: `Queue`, root watchers, targets, dump/load, config and activity waiters |
| `simplebroker.ext` (`__all__`) | Embedder and shared extension facade: errors, sidecar, watch bases, project-config discovery, plugin types, advanced helpers |
| `simplebroker.commands` (`__all__`) | CLI-equivalent functions (print + exit codes); second public surface, not package root |

`simplebroker.project` re-exports target helpers and the same project-config
discovery objects as `ext`. Prefer `simplebroker.ext` for new project-config
imports; existing `project` imports remain valid.

Private modules (`simplebroker._…`) and other unlisted submodules are **not**
public product surface. They may change in any release. First-party backends
may import private modules under an exact pin and `backend_api_version`
handshake; that does not make those modules public for ordinary embedders.

_Implementation mapping_:
- `simplebroker/__init__.py`
- `simplebroker/ext.py`
- `simplebroker/commands.py`
- `simplebroker/project.py`

## Targets and discovery [SB-API-2]

Public ways to bind a broker for library use:

- **`BrokerTarget`** — opaque resolved target (backend name, target string,
  options, optional project root / config path metadata).
- **`open_broker`** — context manager yielding a connection for connection-scoped
  work (including dump/load).
- **`resolve_broker_target`**, **`target_for_directory`**, **`broker_root`** —
  discovery and explicit-directory binding.
- **`serialize_broker_target`** / **`deserialize_broker_target`** — lossless
  process-boundary transport; the payload may contain credentials and must not
  be logged or exposed.
- **Project-config helpers** (same objects on `simplebroker.ext` and
  `simplebroker.project`):
  - `find_project_config` — upward search for project TOML
  - `project_config_path_for_directory` — configured path under an explicit root
  - `resolve_project_target` — TOML path → `BrokerTarget`
- **`resolve_config`** — normalize a config mapping for handles and discovery.

Environment variable and TOML field catalogs for project scoping remain in the
README residual where listed; this clause owns the **public callables**, not
every config key.

_Implementation mapping_:
- `simplebroker/project.py`
- `simplebroker/_project_config.py`
- `simplebroker/db.py` (`open_broker`)
- `simplebroker/_targets.py` / target types via public re-exports

## Queue lifecycle [SB-API-3]

**`Queue`** is the primary programmatic handle for named-queue operations.

- Construct with a queue name and a path string or `BrokerTarget` (and optional
  config) as documented on the type.
- Prefer context-manager use or an explicit **`close`** when the handle owns
  resources; cleanup is part of the public lifecycle.
- Configuration passed into a Queue (or watcher) is normalized and retained as
  that instance’s snapshot unless a documented per-call override applies.

_Implementation mapping_:
- `simplebroker/sbqueue.py`

## Queue operations (library shape) [SB-API-4]

Public write, read, peek, move, delete, and related queue methods on `Queue`
are **library-shaped**:

- They **return values** or **raise** package exceptions.
- They do **not** use CLI process exit codes or stdout printing as their
  primary contract (contrast [SB-API-10] and `[SB-CLI-*]`).

**Operation meaning** (claim-before-handoff, peek observation, move
reservation, id allocation and filters, broadcast selection) is defined by the
owning vertical:

| Family | Owning vertical |
|--------|-----------------|
| Consume / claim / peek / move | `[SB-DELIVERY-*]` |
| Message ids and exact insert | `[SB-ID-*]` |
| After/before and related filters | `[SB-SELECT-*]` |
| Broadcast | `[SB-BCAST-*]` |

The full method catalog may remain README residual until a separate ops
catalog cutover; absence from this clause’s prose does not remove a name from
`simplebroker.__all__` or from `Queue`’s public methods.

_Implementation mapping_:
- `simplebroker/sbqueue.py`
- vertical specs 11–14

## Generators and materialization [SB-API-5]

Generator APIs (for example `read_generator`, `peek_generator`,
`move_generator`, streaming helpers) and materializing batch APIs (for example
`read_many`, `move_many`) follow the **delivery claim and handoff rules** of
the corresponding consume, peek, or move mode in `[SB-DELIVERY-*]`.

Where delivery requires it, materializing batch APIs **commit selected claims
before returning** their result lists. Generator modes that document
`at_least_once` or batch commit intervals follow the delivery vertical, not a
second library-only delivery model.

_Implementation mapping_:
- `simplebroker/sbqueue.py`
- `docs/specs/11-delivery.md`

## Watchers and activity waiters [SB-API-6]

Public watch embedding surface:

| Location | Names |
|----------|--------|
| Package root | `QueueWatcher`, `QueueMoveWatcher`, `create_activity_waiter_for_queues`, `ActivityWaiter` |
| `simplebroker.ext` | `BaseWatcher`, `PollingStrategy`, `StopWatching`, `default_error_handler` |

Watch **modes** (consume, peek, move) and claim/progress rules are
`[SB-DELIVERY-2]` (and related delivery clauses). This clause owns the public
types used to run and subclass watchers and multi-queue activity waiting.

_Implementation mapping_:
- `simplebroker/watcher.py`
- `simplebroker/_backend_plugins.py` (activity waiters)
- `simplebroker/ext.py` re-exports

## Sidecar [SB-API-7]

Embedders may open a **sidecar** SQL session co-located with the broker for
application tables:

- Entry via the public connection/queue sidecar API (for example
  `queue.sidecar(...)` / broker `sidecar`).
- Session type **`SidecarSession`**; failure when sidecar is unavailable raises
  **`SidecarUnavailableError`**.
- **`RESERVED_TABLE_NAMES`** lists names embedders must not use for their own
  tables.

Sidecar schema and application tables are the embedder’s product, not
SimpleBroker queue semantics.

_Implementation mapping_:
- `simplebroker/_sidecar.py`
- `simplebroker/ext.py`
- `simplebroker/sbqueue.py` / connection surface

## Dump and load (library) [SB-API-8]

Public library I/O entry points:

- **`dump_lines`** — iterator of dump lines from a broker connection
- **`load_lines`** — apply dump lines to a connection
- **`LoadResult`** — load summary type

Portable format, pending-only dump, selection filters, fresh-load rules, and
claimed-row inspection policy are **`[SB-IO-*]`**. This clause only identifies
the library callables and that they are not CLI process packaging.

_Implementation mapping_:
- `simplebroker/_dump.py` (exported via package root)
- `docs/specs/15-persistence-io.md`

## Errors [SB-API-9]

Public exception types for library and shared code are importable from
**`simplebroker.ext`** (including `BrokerError`, `DatabaseError`,
`OperationalError`, `IntegrityError`, `DataError`, `TimestampError`,
`QueueNameError`, `MessageError`, `SidecarUnavailableError`, and related types
listed in `ext.__all__`).

- Library failure is signaled by **exceptions**, not by CLI process exit codes
  (`[SB-CLI-1]` applies to the CLI and [SB-API-10]).
- Exception **message text** is not a frozen product contract; catch types, not
  substrings.
- Some runtime failures may still surface as plain `RuntimeError` (for example
  exhausted retries); `BrokerError` is the root of package-defined SimpleBroker
  exceptions, not an exhaustive catch for every failure.

_Implementation mapping_:
- `simplebroker/_exceptions.py`
- `simplebroker/ext.py`

## Command layer (second surface) [SB-API-10]

**`simplebroker.commands`** is a supported public module whose `__all__` names
are stable under the same compatibility policy as other public exports.

- Each **`cmd_*`** function is the programmatic equivalent of a CLI subcommand:
  it prints to **stdout** (and uses stderr for diagnostics) and returns an
  **integer exit code** with CLI meanings (`[SB-CLI-1]`), rather than using
  return values as the primary success channel.
- Helpers listed in that module’s `__all__` (for example
  `parse_exact_message_id`) are part of this surface.

This layer is for **process and CLI reuse** (wrappers that need shell parity
without reimplementing the CLI). Default embedding for application logic
should use **`Queue`** and related root/`ext` APIs ([SB-API-3]–[SB-API-9]).

Underlying operation meaning remains with the verticals and `[SB-CLI-*]` for
presentation.

_Implementation mapping_:
- `simplebroker/commands.py`
- `docs/specs/10-cli.md`

## Ext advanced and backend-facing exports [SB-API-11]

`simplebroker.ext` also exports names used by advanced embedders and by
backend authors (for example `DeliveryGuarantee`,
`validate_delivery_guarantee`, `MaintenanceSchedule`, `vacuum_is_eligible`,
`BackendPlugin`, `BrokerConnection`, `SQLRunner`, `SQLiteRunner`, `SetupPhase`,
`BackendAwareRunner`, `MultiQueueActivityWaiterHook`, `get_backend_plugin`,
`BACKEND_API_VERSION`, `TimestampGenerator`).

These names remain **importable and stable** as listed in `ext.__all__`. They
do **not** constitute a complete standalone third-party backend SDK. Authoring
a full alternate backend may still require private modules under pin and the
`backend_api_version` handshake described in the `simplebroker.ext` module
docstring.

_Implementation mapping_:
- `simplebroker/ext.py` and its re-export sources
- first-party `extensions/simplebroker_pg`, `extensions/simplebroker_redis`

## Cross-surface matrix [SB-API-12]

Orientation matrix. On conflict of **operation meaning**, the vertical wins;
this table does not redefine claim, id, or filter rules.

| Library | CLI / `commands` | Owning vertical / notes |
|---------|------------------|-------------------------|
| `Queue.write` / exact-insert helpers | `write` / `cmd_write` | `[SB-ID-*]` |
| `Queue.read*` | `read` / `cmd_read` | `[SB-DELIVERY-*]`, `[SB-SELECT-*]` |
| `Queue.peek*` | `peek` / `cmd_peek` | `[SB-DELIVERY-4]`, `[SB-SELECT-*]` |
| `Queue.move*` | `move` / `cmd_move` | `[SB-DELIVERY-3]`, `[SB-ID-*]` |
| `Queue.delete*` | `delete` / `cmd_delete` | `[SB-OPS-3]`; claim lifecycle `[SB-DELIVERY-*]` |
| Broadcast APIs | `broadcast` / `cmd_broadcast` | `[SB-BCAST-*]` |
| `QueueWatcher` / move watcher / ext bases | `watch` / `cmd_watch` | `[SB-DELIVERY-2]` |
| `dump_lines` / `load_lines` | `dump` / `load` | `[SB-IO-*]` |
| Targets / project-config helpers | `-f` / `-d` / project scope | [SB-API-2]; README project-scoping residual |
| `cmd_*` only | same CLI verb | `[SB-CLI-*]` presentation + vertical for the op |

Queue aliases (`@name`) remain a **CLI** convenience unless a library API
explicitly documents alias support.

## Implementation mapping (summary)

- Package root: `simplebroker/__init__.py`, `sbqueue.py`, `watcher.py`,
  `project.py`, `_dump.py`, `db.py`
- Ext facade: `simplebroker/ext.py`
- Command layer: `simplebroker/commands.py`, `cli.py`
- Verticals: `docs/specs/10-cli.md` … `15-persistence-io.md`

## Verification

| Clause | Firing evidence |
|--------|-----------------|
| [SB-API-1] | `tests/test_python_library_api_contract_sb_api.py`; `tests/test_ext_imports.py`; `tests/test_public_surface.py` |
| [SB-API-2] | `tests/test_python_library_api_contract_sb_api.py`; `tests/test_project_config.py`; `tests/test_ext_imports.py` (project-config identity) |
| [SB-API-3] | `tests/test_python_library_api_contract_sb_api.py`; Queue lifecycle coverage in `tests/test_queue_api_*.py` |
| [SB-API-4] | `tests/test_python_library_api_contract_sb_api.py` (library-shape language + matrix); delivery/id/select/bcast suites for meaning |
| [SB-API-5] | `tests/test_delivery_contract_sb_delivery.py`; Queue generator / `*_many` suites |
| [SB-API-6] | `tests/test_python_library_api_contract_sb_api.py`; `tests/test_ext_imports.py` watcher contract; watcher suites |
| [SB-API-7] | `tests/test_python_library_api_contract_sb_api.py`; sidecar suites under tests / examples |
| [SB-API-8] | `tests/test_persistence_io_contract_sb_io.py`; `tests/test_dump_load.py` |
| [SB-API-9] | `tests/test_python_library_api_contract_sb_api.py`; `tests/test_ext_imports.py` |
| [SB-API-10] | `tests/test_public_surface.py`; `tests/test_python_library_api_contract_sb_api.py` |
| [SB-API-11] | `tests/test_ext_imports.py`; `tests/test_python_library_api_contract_sb_api.py` |
| [SB-API-12] | `tests/test_python_library_api_contract_sb_api.py` (matrix present); kernel CLI↔Python map |

## Related Plans

- `docs/plans/2026-07-31-python-library-api-contract-plan.md`
- `docs/plans/2026-07-30-product-documentation-cutover-plan.md` (Phase 4)
