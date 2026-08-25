# Python Library API

Normative public **Python** surfaces for embedding SimpleBroker: package root
exports, `simplebroker.ext`, and the CLI-equivalent command layer. Queue
**operation meaning** (claim, peek, move, ids, filters, broadcast, dump/load
format) is owned by the vertical specs; this document owns **which names are
public**, how library failure and packaging differ from the CLI, and how
surfaces relate.

This contract began as recovery of existing public promises. Owner-approved
revisions may add surfaces explicitly. It is not a complete third-party
backend SDK.

## Public surfaces [SB-API-1]

Supported import surfaces:

| Surface | Role |
|---------|------|
| `simplebroker` (`__all__`) | Primary embedder API: `Queue`, `MovedMessage`, `CloseableIterator`, root watchers, targets, dump/load, message-id formatting, configuration resolution and snapshots, and activity waiters |
| `simplebroker.ext` (`__all__`) | Embedder and shared extension facade: errors, sidecar, watch bases, project-config discovery, plugin types, advanced helpers |
| `simplebroker.commands` (`__all__`) | CLI-equivalent functions (print + exit codes); second public surface, not package root |

`MovedMessage` is a `TypedDict` with required `message: str` and
`timestamp: int` fields. It describes the existing ordinary dictionaries
returned or yielded by high-level `Queue.move()`; it does not introduce a
runtime wrapper or change message-id representation.

`CloseableIterator[T]` is a package-root public structural protocol for a
single-use iterator with `__iter__`, `__next__`, and `close() -> None`. It is
compatible with ordinary `Iterator[T]` use and deliberately does not promise
generator-only `send()` or `throw()` operations. It describes the returned
object; it does not require a runtime wrapper.

`simplebroker.project` re-exports target helpers and the same project-config
discovery objects as `ext`. Prefer `simplebroker.ext` for new project-config
imports; existing `project` imports remain valid.

Private modules (`simplebroker._…`) and other unlisted submodules are **not**
public product surface. They may change in any release. First-party backends
may import private modules while declaring a minimum supported core version and
requiring an exact `backend_api_version` match; that does not make those
modules public for ordinary embedders.

The canonical public import for message-ID formatting is
`simplebroker.format_message_id(value: int | str) -> str`. This clause owns
that stable import path and callable shape. `[SB-ID-1]` owns the returned
message-ID JSON representation; `[SB-ID-4]` owns accepted exact-ID forms and
validation behavior.

The helper has no storage effect and returns a scalar for use with an ordinary
JSON encoder, not JSON text. Ordinary Queue and connection methods continue to
return integer ids. The helper is not duplicated on `simplebroker.ext`, a
stateful Queue or timestamp-generator method, or another public module.

_Implementation mapping_:
- `simplebroker/__init__.py`
- `simplebroker/sbqueue.py` (`CloseableIterator`)
- `simplebroker/_message_id.py` (`format_message_id`)
- `simplebroker/ext.py`
- `simplebroker/commands.py`
- `simplebroker/project.py`

## Targets and discovery [SB-API-2]

Public ways to bind a broker for library use:

- **`BrokerTarget`** — opaque resolved target (backend name, target string,
  options, optional project root / config path metadata). `backend_options` is
  shallow-copied at target construction and remains an ordinary picklable
  `dict`. Later mutation of the caller's source mapping cannot change the
  target; direct mutation of the target's exposed dict remains possible for
  compatibility.
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
- **`resolve_config`** / **`snapshot_config`** — resolve ordinary configuration
  or retain one complete snapshot for handles and discovery.

Project configuration is a trusted developer input. When the configured target
string contains a recognized inline password, project-config loading emits a
redacted advisory warning that does not include the password. SimpleBroker does
not inspect, warn on, or enforce project-config file mode, ownership,
parent-directory permissions, or ACLs. Confidentiality and integrity of the
config path are governed by the effective operating-system permissions across
the file and its containing directories.

`load_config()` remains the strict complete environment parser.
`resolve_config(None|ordinary_mapping)` performs a fresh strict read of the
current environment/default base, applies ordinary overrides, preserves
additional keys, and returns an ordinary `dict`. A supplied ordinary mapping
does not bypass an invalid ambient base. A recognized environment or override
value that cannot be parsed or validated raises `InvalidConfigError` with
key, source, expected-form, and safe rejected-value metadata. Existing
documented normalization and fallback cases remain unchanged.

`ResolvedConfig` is a read-only complete snapshot. It contains every
canonical SimpleBroker configuration key at minimum; canonical values are
normalized and validated. Additional keys are preserved unchanged as opaque
extension data. The core configuration layer does not interpret, normalize,
or validate them as canonical settings; extensions may interpret their own
keys. Extras nevertheless participate opaquely in the complete snapshot and
process-session identity. Its top-level bindings are copied and cannot be
reassigned; opaque extra values are not recursively copied or frozen.
Construction fills omitted canonical keys from canonical defaults without
reading ambient `BROKER_*`. Once constructed, a `ResolvedConfig` never
consults ambient configuration again. `resolve_config()` given an exact
`ResolvedConfig` returns that same object without reading ambient state;
non-exact subclasses are revalidated rather than trusted as snapshots.

`resolve_isolated_config(overrides, *, preserve_unknown=False)` constructs a
`ResolvedConfig` from canonical defaults plus explicit values without reading
ambient `BROKER_*`. By default it rejects additional keys so downstream
embedders can use it as a fail-closed canonical-schema check. With
`preserve_unknown=True`, it instead copies additional keys unchanged as opaque
extras. The flag never changes normalization or validation of recognized keys.
`snapshot_config(config=None)` is the ambient-derived snapshot factory. For
`None` or an ordinary mapping it calls the fresh environment-base resolution
once and freezes the complete result; for an exact `ResolvedConfig` it returns
that object unchanged. `snapshot_config()` preserves additional keys.

Every public configuration-consuming handle or invocation converts `None` or
an ordinary mapping to one `ResolvedConfig` at its ownership event in the
table below, then passes and retains that snapshot through Queue,
target/project discovery, broker, process-session, runner, watcher, command,
load, and CLI dump's broker-opening path. Lower layers and later lazy resource
acquisition do not reread ambient `BROKER_*`. Converting a marker to an
ordinary mapping discards that guarantee if the mapping is later passed
through an ambient-resolving public seam.

| Public surface | Snapshot event |
|----------------|----------------|
| `snapshot_config()` | During that call. |
| `Queue`, watcher, `DBConnection`, and other eager config-consuming constructors | During the constructor call, before owned resource side effects. |
| Eager discovery and load functions | At the first config-consuming branch during the function call; config-independent validation that already precedes that branch keeps its existing order. |
| `open_broker()` | On `__enter__` of the returned generator-based context manager, not when the context-manager object is created. The marker is retained through `__exit__`. |
| Transactional generator per-call config | On first iteration of the configuration-consuming `at_least_once` path, when the Python generator body begins. The resulting overlay is retained until exhaustion or close. Creating the generator object alone does not inspect an ordinary override mapping. |
| Direct `cmd_*` functions | At first actual config consumption after any contract-preserved config-independent early path; then once for the rest of that invocation. |
| `cli.main()` | Once before parser construction and argument parsing, preserving the existing invalid-config-before-parsing rule. |
| `dump_lines()` | Never. It consumes an already-open broker and receives no config argument; CLI dump configuration belongs to its broker-opening path. |

Environment variable and TOML field catalogs for project scoping remain in the
README residual where listed; this clause owns the **public callables**, not
every config key.

_Implementation mapping_:
- `simplebroker/_constants.py`
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
- Queue construction converts omitted or ordinary configuration to one
  `ResolvedConfig` and retains it as that instance's snapshot. Later ambient
  changes do not affect that Queue, including ephemeral operations and later
  lazy backend/core creation. A later Queue construction observes the
  then-current ambient configuration. Any documented per-call override applies
  to the retained snapshot without rereading the environment.

_Implementation mapping_:
- `simplebroker/sbqueue.py`

## Queue operations (library shape) [SB-API-4]

Public write, read, peek, move, delete, and related queue methods on `Queue`
are **library-shaped**:

- They **return values** or **raise** package exceptions.
- They do **not** use CLI process exit codes or stdout printing as their
  primary contract (contrast [SB-API-10] and `[SB-CLI-*]`).

High-level `Queue.read()`, `Queue.peek()`, and `Queue.move()` are supported
flag-directed convenience views over their granular `*_one`, `*_many`, and
`*_generator` methods; they are not legacy aliases or a third operation
model. `read` remains consuming and `peek` remains observational under the
delivery vertical. The selected flags determine cardinality and record shape.
Public typing uses overloads to narrow calls made with literal flag values and
retains a full union for an unknown runtime `bool`; overloads do not create a
second implementation path.

The `all_messages=True` view of `Queue.peek()` returns
`CloseableIterator[...]`; an unknown runtime `bool` includes that closeable
iterator in its existing scalar/tuple/iterator union. The read and move
families retain their existing return types.

Read and peek preserve their existing string or `(message, timestamp)` tuple
records. High-level move preserves its existing `MovedMessage` dictionary or
iterator of dictionaries; granular move methods preserve the same
scalar/tuple/list/iterator conventions as the granular read and peek methods.

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

`Queue.peek_generator()` and the high-level `Queue.peek(all_messages=True)`
view return `CloseableIterator[...]`. Their thread ownership, terminal
outcomes, synchronous Queue-operation exit, and early-close duty are
[SB-DELIVERY-4]. The backend-facing `BrokerConnection.peek_generator()`
remains an ordinary iterator seam; the public close contract is owned by the
outer Queue operation.

Where delivery requires it, materializing batch APIs **commit selected claims
before returning** their result lists. Generator modes that document
`at_least_once` or batch commit intervals follow the delivery vertical, not a
second library-only delivery model.

For transactional `claim_generator` and `move_generator` calls, an ordinary
per-call config mapping is overlaid on the broker's retained snapshot when the
configuration-consuming `at_least_once` generator is first iterated. This is
the normal Python generator-body boundary, not generator-object creation. The
overlay remains fixed for that generator's lifetime and does not read ambient
configuration. A direct `batch_size` argument still takes precedence over its
configured default.

`Queue.stream_messages()` remains one supported fixed-record streaming helper
used by command and watcher adapters. It always yields
`(message, timestamp)` tuples. Its batching controls may derive the delivery
guarantee and batch size; this derivation does not define a separate delivery
contract or require a parallel implementation.

_Implementation mapping_:
- `simplebroker/sbqueue.py`
- `docs/specs/11-delivery.md`

## Watchers and activity waiters [SB-API-6]

Public watch embedding surface:

| Location | Names |
|----------|--------|
| Package root | `QueueWatcher`, `QueueMoveWatcher`, `create_activity_waiter_for_queues`, `ActivityWaiter` |
| `simplebroker.ext` | `BaseWatcher`, `PollingStrategy`, `StopWatching`, `default_error_handler` |

An `ActivityWaiter` is a close-only leaf resource. It owns one backend
activity registration or one composite set of registrations; it does not own
the runner or shared process substrate and does not expose `shutdown()`.

The waiter owner must serialize `wait()`, replacement or ownership transfer,
and `close()`. This contract does not make `wait()` and `close()` safe to run
concurrently, and it does not define `wait()` behavior after close.

Watcher construction follows `[SB-API-3]` configuration timing: it retains one
`ResolvedConfig`, and later polling, waiting, callback dispatch, runner
creation, and documented per-call overrides do not reread ambient
configuration. A watcher constructed from an existing `Queue` adopts that
Queue's retained snapshot when watcher config is omitted. An explicit ordinary
watcher config mapping overlays the Queue snapshot without consulting the
environment; an explicit complete `ResolvedConfig` replaces it for
watcher-local policy. The supplied Queue remains governed by its own retained
snapshot in either explicit-config case.

`PollingStrategy`'s `initial_checks`, `max_interval`, `burst_sleep`, and
`jitter_factor` constructor defaults are the canonical normalized defaults of
`BROKER_INITIAL_CHECKS`, `BROKER_MAX_INTERVAL`, `BROKER_BURST_SLEEP`, and
`BROKER_JITTER_FACTOR`, respectively. Direct construction derives those
signature defaults from one isolated canonical configuration and does not read
ambient configuration. `BaseWatcher` continues to pass its retained resolved
instance configuration explicitly, and an explicit constructor argument
continues to override the corresponding default.

`ActivityWaiter.close()` is terminal and idempotent. The first invocation
marks the waiter closed before backend cleanup begins. During that invocation
it attempts every owned cleanup action that remains safe to attempt
independently after an ordinary `Exception`. It then raises the first such
exception and retains later cleanup exceptions, in cleanup order, as PEP 678
exception notes added with `BaseException.add_note()`. Every later invocation
returns without effect, including when the first invocation raised; it does
not retry partial cleanup. A `BaseException` outside `Exception` propagates
immediately, while the waiter remains terminal.

`BaseWatcher.stop()` is thread-safe against watcher startup and active-run
cleanup. Startup and stop choose one cleanup owner at one serialized lifecycle
transition. A stop that wins before startup prevents that later run from
acquiring runtime resources and owns cleanup. A run that has won startup owns
cleanup through its `finally`, including when a joining stop call times out.
Cleanup is performed at most once for a successful lifecycle release;
repeated stop calls remain safe. This guarantee does not make two concurrent
`run_forever()` calls on one watcher supported.

Error-handler outcomes have four meanings. Returning `True` or `None`
continues watching. Returning `False`, raising `StopWatching`, or raising the
internal `StopException` ends the watcher run cleanly. If the error handler
raises any other ordinary `Exception`, that callback failure is terminal: the
watcher dispatches no later message in that run, retains the original
message-handler exception as its explicit cause, and re-raises the
error-handler exception after runtime cleanup. Synchronous `run()` and
`run_forever()` expose it to their caller; `run_in_thread()` leaves it
uncaught for Python's standard `threading.excepthook`. This terminal signal
does not depend on `BROKER_LOGGING_ENABLED`. An ordinary runtime-cleanup
exception during that terminal unwind does not replace the callback failure;
it is retained as an ordered PEP 678 exception note and cleanup remains
retryable. A `BaseException` outside `Exception` may interrupt cleanup and
propagates with its existing priority.

`BaseWatcher.__exit__()` requests stop and join. An ordinary stop or cleanup
exception during exit is best effort: it is suppressed, never replaces an
exception from the `with` body, and leaves failed cleanup retryable under the
lifecycle rules above. A `BaseException` outside `Exception` propagates.
Context exit does not replay a background-thread failure into the exiting
thread; background failures use `threading.excepthook` as described above.
`BaseWatcher.__enter__()` does not expose the context body until its background
thread has claimed run ownership or has already exited. This is a scheduler
handshake only: it does not wait for backend setup or the initial queue drain.

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

Sidecar SQL without parameters is passed through unchanged. PostgreSQL
parameterized sidecar SQL adapts qmark placeholders only outside quoted,
commented, and dollar-quoted text; `??` denotes one literal question mark.
Original percent signs are escaped for psycopg's parameter template without
changing the SQL PostgreSQL executes. The PostgreSQL driver retains bind-count
validation.

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

`simplebroker.ext.InvalidConfigError` subclasses both `BrokerError` and
`ValueError`. Its `key`, `source`, `expected`, and `value_display` attributes
are public; it never retains a sensitive raw value. Importing `simplebroker`,
`simplebroker.ext`, or `simplebroker.commands` does not parse ambient
configuration or raise an import-time configuration exception. A public
handle or invocation that needs ambient/default configuration samples it once
at the ownership seam and raises a fresh `InvalidConfigError` before broker
side effects when that sample is invalid. A successful `ResolvedConfig`
remains fixed for the lifetime of its owning handle or invocation; later
ambient changes, including invalid values, do not affect it. Direct
`load_config()` and ordinary `resolve_config()` calls remain fresh strict
environment reads.

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

- Each **`cmd_*`** function is the programmatic equivalent of a CLI subcommand.
  Ordinary outcomes return integer codes with `[SB-CLI-1]` meanings. Invalid
  input and operational failures raise their typed exceptions to direct Python
  callers; `simplebroker.cli` is the sole owner that translates those
  exceptions to diagnostics and process exit codes.
  Direct selector combinations obey the CLI grammar: exact-ID selection cannot
  be combined with all/range selection, and exact-ID delete requires a queue.
  Rejected combinations raise before target access or mutation. `cmd_load`
  likewise raises its original input, integrity, and timestamp failures; only
  the CLI adds the `broker load:` recovery presentation.
- Direct `cmd_*` stdout behavior matches the corresponding CLI action when the
  consumer closes: `cmd_read`, `cmd_peek`, `cmd_move`, `cmd_dump`, and
  `cmd_watch` return clean-stop `0`; every other stdout-producing command
  function returns `1` after its ordinary plain or JSON output-delivery
  diagnostic. The
  internal closed-stdout control signal never escapes the public command
  function. Durable effects completed before output failure remain completed.
  Where a command function accepts `quiet`, it suppresses the same owned
  commentary as the CLI without suppressing errors or unrelated warnings.
- A direct command-layer caller receives `InvalidConfigError` when that command
  consumes an invalid ambient/default configuration; the integer exit-code
  guarantee applies once command execution begins. A command invoked with an
  explicit target that does not otherwise consume ambient configuration is not
  rejected merely because unrelated ambient config is invalid. The CLI process
  wrapper is the sole translator that turns a typed configuration-initialization
  failure into the `[SB-CLI-2]` stderr diagnostic and exit `1`.
- Helpers listed in that module’s `__all__` (for example
  `parse_exact_message_id`) are part of this surface.

Each direct `cmd_*` invocation that consumes configuration creates one
invocation-scoped `ResolvedConfig` and reuses it through target selection,
Queue/broker construction, and operation execution. Repeated programmatic
calls may therefore observe intentional environment changes between calls,
while no call observes a change after its snapshot is created. Existing
config-independent early-validation paths remain config-independent.

Process-signal translation remains at the CLI wrapper. Ordinary direct
`cmd_*` functions are not required to catch an arbitrary `KeyboardInterrupt`
and convert it to `130`; `cmd_watch` retains its explicit normal-stop handling
and success result.

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

Lifecycle verbs follow ownership scope. `close()` releases resources owned by
the receiving handle or runner. `shutdown()` is the optional stronger
operation when that receiver owns shared or process-wide substrate beyond an
ordinary handle release. An implementation may make one delegate to the other
when those scopes coincide.

SimpleBroker-owned runner teardown calls callable `shutdown()` when present
and otherwise calls `close()`. This preference does not transfer ownership of
an explicitly injected runner from its caller to SimpleBroker.

`SQLiteRunner.close()` closes the connections owned by the runner at that
operation's linearization point. The runner remains reusable, and a later or
concurrently linearized operation may acquire a new connection. Callers
requiring terminal operation admission must close the owning process session
or factory rather than treating runner `close()` as a permanent latch.

Backend API v6 requires every waiter returned by a backend activity-waiter hook
to satisfy `[SB-API-6]` terminal close semantics.

Backend API v7 requires
`BrokerConnection.advance_last_timestamp(timestamp)`. The operation validates
an integer timestamp (`None`, booleans, and other non-integers raise
`TypeError`), monotonically advances durable broker-global high-water
to at least that value regardless of the current process-local cache, then
reads durable high-water once without a preceding initialization read. The
final observation must be at least the requested floor. It refreshes the
connection cache and returns
the value observed by that final read. That observation may immediately become
stale under `[SB-ID-3]`; a concurrent higher value is never lowered. If the
final read fails after the monotone advance was attempted, the operation raises
`TimestampError(..., outcome_ambiguous=True)`; a non-retryable operational
failure after an attempted write is ambiguous for the same reason. Exhausted
retryable lock contention and a final observed value below the requested floor
raise `TimestampError` with `outcome_ambiguous is False`. All other existing
`TimestampError` construction defaults that public boolean attribute to false.
The true case is the typed outcome-ambiguous failure classification.
Core rejects older or newer backend API versions through the existing
exact-version handshake.

First-party extension package dependency declarations are minimum supported
core versions. Runtime compatibility additionally requires an exact
`backend_api_version` match. A breaking change to a private seam used by a
first-party extension requires a backend API version bump. Fork recovery
replaces inherited process-owned locks and resources before any affected lock
acquisition in the child.

Persistence helpers are public from the package root. The exact load interface
is `load_lines(broker, lines, *, force=False, config=None)`; its policy and
failure order are `[SB-IO-4]`. `DumpClockSkewWarning` is a public `UserWarning`
subclass importable from `simplebroker` so embedders can filter it.

`TimestampGenerator.validate()` is the public string-parser surface for
timestamp bounds. Its accepted and rejected spellings are the three grammars in
`[SB-CLI-5]`; library methods whose `after_timestamp` / `before_timestamp`
parameters already accept integer message IDs do not reparse those integers as
strings.

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
| `BrokerConnection` alias methods | `alias add` / `list` / `remove` | `[SB-OPS-5]`; `Queue` is literal-only |

Queue aliases (`@name`) are **CLI operand syntax**, resolved at the command
boundary rather than in the storage layer:

- **CLI and `simplebroker.commands`** resolve `@name`; [SB-API-10] makes the
  command layer the programmatic equivalent of the CLI. Resolution happens per
  call, so there is no stale-binding question.
- **`BrokerConnection`** (public via `simplebroker.ext`) owns alias
  management: `add_alias`, `remove_alias`, `list_aliases`, `resolve_alias`,
  `has_alias`, `aliases_for_target`, `get_alias_version`. Reachable from
  `open_broker(...)`. `canonicalize_queue(name)` applies the sigil rule —
  plain names pass through, `@name` resolves — so library callers get the
  same operand semantics as the CLI (`[SB-OPS-5]`).
- **`Queue` takes literal queue names only.** `Queue("@ali")` raises
  `QueueNameError` because `@` is not a legal queue-name character
  (`[SB-DELIVERY-8]`), and `Queue("ali")` means the literal queue `ali`, not
  an alias target. Resolve explicitly when binding:
  `Queue(conn.resolve_alias("ali"), ...)`.

## Implementation mapping (summary)

- Package root: `simplebroker/__init__.py`, `_constants.py`, `sbqueue.py`,
  `watcher.py`, `project.py`, `_dump.py`, `db.py`
- Ext facade: `simplebroker/ext.py`
- Command layer: `simplebroker/commands.py`, `cli.py`
- Verticals: `docs/specs/10-cli.md` … `15-persistence-io.md`

## Verification

| Clause | Firing evidence |
|--------|-----------------|
| [SB-API-1] | `tests/test_python_library_api_contract_sb_api.py::test_api_public_message_id_formatter_contract`, `::test_api_moved_message_is_package_root_public`, `::test_api_closeable_peek_iterator_contract`; `tests/test_queue_typing_contract.py`; `tests/test_dev_scripts.py` (isolated root wheel/sdist import and published-artifact verification); `tests/test_ext_imports.py`; `tests/test_public_surface.py` |
| [SB-API-2] | `tests/test_python_library_api_contract_sb_api.py`; `tests/test_isolated_config.py`; `tests/test_connection_config.py::test_target_discovery_samples_environment_for_each_call`; `tests/test_project_config.py::test_project_config_warns_for_inline_url_password`, `tests/test_project_config.py::test_project_config_warns_for_inline_conninfo_password`, `tests/test_project_config.py::test_project_config_does_not_judge_group_or_other_mode_bits`; `tests/test_ext_imports.py` (project-config identity); `tests/test_invalid_config_lifecycle.py::test_load_config_reports_invalid_environment_field`, `tests/test_invalid_config_lifecycle.py::test_public_snapshots_are_explicit_and_fresh_across_calls`, `tests/test_invalid_config_lifecycle.py::test_each_invalid_snapshot_raises_a_fresh_exception_and_repair_recovers` |
| [SB-API-3] | `tests/test_python_library_api_contract_sb_api.py`; `tests/test_connection_config.py::test_ephemeral_queue_keeps_constructor_snapshot_after_invalid_env_change`, `tests/test_connection_config.py::test_new_queue_observes_later_environment_while_existing_queue_stays_fixed`, `tests/test_connection_config.py::test_persistent_queue_keeps_snapshot_before_first_lazy_core_creation`; Queue lifecycle coverage in `tests/test_queue_api_*.py` |
| [SB-API-4] | `tests/test_queue_typing_contract.py`; `tests/test_peek_generator_lifecycle.py` (high-level `all_messages=True` path); `tests/test_queue_api_additions.py::test_queue_delete_explicit_none_is_rejected_without_mutation`; `tests/test_queue_api_additions.py::test_queue_move_returns_plain_dictionary_with_typed_fields`; `tests/test_python_library_api_contract_sb_api.py` (library-shape language + matrix); delivery/id/select/bcast suites for meaning |
| [SB-API-5] | `tests/test_queue_typing_contract.py`; `tests/test_peek_generator_lifecycle.py`; `tests/test_python_library_api_contract_sb_api.py::test_api_closeable_peek_iterator_contract`; `tests/test_delivery_contract_sb_delivery.py`; `tests/test_connection_config.py::test_generator_override_inherits_core_snapshot_without_ambient_reread`, `tests/test_connection_config.py::test_generator_reads_ordinary_override_on_first_iteration`; Queue generator / `*_many` suites |
| [SB-API-6] | `tests/test_python_library_api_contract_sb_api.py::test_api_activity_waiter_terminal_close_contract`, `tests/test_python_library_api_contract_sb_api.py::test_api_watcher_start_stop_cleanup_ownership_contract`, `tests/test_python_library_api_contract_sb_api.py::test_api_polling_strategy_defaults_match_canonical_config`; `tests/test_watcher_error_handler_contract.py`; `tests/test_watcher_stop_contract.py::test_stop_racing_start_has_one_cleanup_owner`, `tests/test_watcher_stop_contract.py::test_join_timeout_does_not_transfer_cleanup_from_live_run`, `tests/test_watcher_stop_contract.py::test_cleanup_failure_keeps_lifecycle_retryable`, `tests/test_watcher_stop_contract.py::test_context_exit_suppresses_stop_failure_without_replacing_body_exception`, `tests/test_watcher_stop_contract.py::test_context_exit_cleanup_failure_remains_retryable`, `tests/test_watcher_stop_contract.py::test_context_exit_propagates_base_exception_from_stop`; `tests/test_watcher_transition_tables.py::test_watcher_lifecycle_fires_transition_table`; `tests/test_watcher.py::TestPollingStrategy::test_defaults_use_ambient_free_canonical_config_snapshot`, `tests/test_watcher.py::TestPollingStrategy::test_all_defaults_derive_from_one_isolated_canonical_snapshot`; `tests/test_connection_config.py::test_watcher_instance_config_controls_live_polling`; `tests/test_connection_config.py::test_watcher_given_queue_adopts_queue_snapshot_and_overlays_without_ambient`; `extensions/simplebroker_pg/tests/test_pg_activity_waiter_lifecycle.py`; `extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py`; PostgreSQL notify and Redis integration replacement tests; watcher suites |
| [SB-API-7] | `tests/test_python_library_api_contract_sb_api.py`; sidecar suites under tests / examples |
| [SB-API-8] | `tests/test_persistence_io_contract_sb_io.py`; `tests/test_dump_load.py`, including `test_load_samples_environment_for_each_invocation` |
| [SB-API-9] | `tests/test_python_library_api_contract_sb_api.py`; `tests/test_ext_imports.py`; `tests/test_invalid_config_lifecycle.py::test_invalid_environment_does_not_break_package_import`, `tests/test_invalid_config_lifecycle.py::test_sensitive_config_failure_redacts_before_formatting`, `tests/test_invalid_config_lifecycle.py::test_each_invalid_snapshot_raises_a_fresh_exception_and_repair_recovers` |
| [SB-API-10] | `tests/test_commands_error_ownership.py` (direct invalid-input/operational exceptions, selector parity, delete no-mutation, and CLI-owned diagnostic boundary); `tests/test_commands_status.py`; `tests/test_commands_init.py`; `tests/test_cli_dump_load.py`; `tests/test_commands_stdout_delivery.py` (exact direct stdout inventory, write-versus-flush failures, mutation durability, and bare-stdout static gate); `tests/test_cli_edge_cases.py::TestCLIEdgeCases::test_keyboard_interrupt_handling`; `tests/test_cli_watch.py::TestWatchCommand::test_watch_sigint_remains_success`; `tests/test_cli_main.py::test_repeated_main_calls_rebuild_defaults_from_invocation_snapshot`; `tests/test_public_surface.py`; `tests/test_python_library_api_contract_sb_api.py`; `tests/test_invalid_config_lifecycle.py::test_direct_commands_raise_when_their_path_consumes_invalid_config`, `tests/test_invalid_config_lifecycle.py::test_direct_command_early_validation_can_remain_config_independent`, `tests/test_invalid_config_lifecycle.py::test_repeated_direct_command_calls_sample_current_environment` |
| [SB-API-11] | `tests/test_python_library_api_contract_sb_api.py::test_api_owned_runner_lifecycle_and_backend_v7_contract`, `tests/test_python_library_api_contract_sb_api.py::test_api_load_future_skew_surface_is_root_importable_and_keyword_only`; `tests/test_core_persistence_transition_tables.py::test_sqlite_runner_fires_transition_table` (`CLOSE_REOPEN`); `tests/test_runner_lifecycle.py`; `tests/test_backend_plugin_resolution.py`, including `test_sqlite_initialize_target_passes_config_snapshot_to_broker`; `extensions/simplebroker_pg/tests/test_pg_plugin_contract_edges.py::test_initialize_target_passes_one_config_snapshot_to_runner_and_core`; `extensions/simplebroker_redis/tests/test_redis_plugin_contract_edges.py::test_plugin_runner_receipt_keeps_marker_out_of_redundant_config_path`, `test_direct_runner_snapshots_environment_when_pool_options_are_missing`, `test_cleanup_reuses_one_snapshot_for_runner_and_core`; `tests/test_release_script.py::test_repository_backend_api_v7_handshake_and_floors_match`; `tests/test_dump_load.py::test_load_header_floor_persists_when_local_cache_is_ahead`, `tests/test_dump_load.py::test_load_header_floor_observes_concurrent_durable_winner`, `tests/test_dump_load.py::test_load_header_floor_final_read_failure_is_outcome_ambiguous`; `tests/test_timestamp_advance.py`; `extensions/simplebroker_pg/tests/test_pg_timestamp_resilience.py::test_postgres_missing_last_ts_row_fails_loudly`; `extensions/simplebroker_redis/tests/test_redis_core_behaviors.py::test_redis_timestamp_advance_transport_failure_is_ambiguous_after_real_eval`; `tests/test_timestamp_bound_grammar.py` (public validator grammar) |
| [SB-API-12] | `tests/test_python_library_api_contract_sb_api.py` (matrix present); kernel CLI↔Python map |

## Related Plans

- completed: [2026-08-24-peek-generator-close-contract-plan](../plans/2026-08-24-peek-generator-close-contract-plan.md)
  — closeable peek iterator and same-thread synchronous Queue-operation cleanup
- completed: [2026-08-24-comprehensive-review-findings-remediation-plan](../plans/2026-08-24-comprehensive-review-findings-remediation-plan.md)
  — target snapshots, sidecar qmark adaptation, fork recovery, compatibility,
  command-error ownership, and interface corrections
- completed: [2026-08-24-failure-path-and-contract-findings-resolution-plan](../plans/2026-08-24-failure-path-and-contract-findings-resolution-plan.md)
  — watcher callback-failure and context-exit contract promotion at baseline
  `1b8ecfa0`
- completed: 2026-08-24-cli-output-and-error-contract-remediation-plan — all
  19 exported command functions follow the ordinary-result/direct-exception
  boundary; CLI translation, selector parity, and no-mutation guards are pinned
  under owner-directed targeted closure
- completed: 2026-08-24-cli-grammar-validation-and-example-reliability-plan —
  derived every `PollingStrategy` constructor default from one isolated
  canonical configuration and implemented the linked CLI reliability slices;
  owner directed targeted closure with hosted Windows/POSIX/Atheris retained
  as post-commit evidence
- retired: 2026-08-23-correctness-and-concurrency-review-remediation-plan —
  source `23d6c9d1` (local-only pin); see the ledger in
  `docs/plans/README.md`
- retired: 2026-08-23-relative-sqlite-containment-and-config-mode-warning-removal-plan
  — source `00fb9f77` (local-only pin); see the ledger in
  `docs/plans/README.md`
- retired: 2026-08-23-polling-strategy-burst-sleep-default-plan — source
  `d63e6552` (local-only pin); see the ledger in `docs/plans/README.md`
- retired: 2026-08-23-maintainability-and-isolation-remediation-plan — source
  `a490dcc4` (local-only pin); see the ledger in `docs/plans/README.md`
- retired: 2026-08-23-configuration-snapshot-consistency-plan — source
  `32210e58` (local-only pin); see the ledger in `docs/plans/README.md`
- retired: 2026-08-23-public-api-and-cli-review-remediation-plan — source
  `2605b79a` (local-only pin); see the ledger in `docs/plans/README.md`
- retired: 2026-08-13-isolated-embedding-config-plan — source `32210e58`
  (local-only pin); see the ledger in `docs/plans/README.md`
- retired: 2026-08-13-invalid-environment-import-lifecycle-plan — source
  `6b5b3044`; see the ledger in `docs/plans/README.md`
- retired: 2026-08-12-bounded-live-dump-plan — source `d0d2de9`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-08-11-activity-waiter-terminal-close-contract-plan — source
  `27f9ae4`; see the ledger in `docs/plans/README.md`
- retired: 2026-08-10-test-suite-signal-remediation-plan — source `0d15871`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-08-json-timestamp-string-contract-plan — source `4cb47bc9`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-06-pre-release-review-remediation-plan — source `84159198`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-06-audit-remediation-plan — source `94e15bc`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-08-04-cmd-watch-locality-plan — source `5023710`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-07-31-python-library-api-contract-plan — source `6481ca08`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-07-30-product-documentation-cutover-plan — source `5023710`;
  see the ledger in `docs/plans/README.md`
