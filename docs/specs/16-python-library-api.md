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
| `simplebroker_pg.get_connection_stats` | First-party PostgreSQL-only operational inspection. It belongs to the separately installed `simplebroker-pg` distribution and is not a portable core or cross-backend operation. |

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
- **`resolve_config`**, **`Config`**, **`DEFAULT_CONFIG`**, **`ConfigField`** — select an external namespace and resolve
  broker and application settings in one read-only object.

A `Queue` binds its effective target at construction. Supported mutable
containers in a supplied target's backend options are recursively detached
using the same value and opaque-identity rules as process-session identity.
Later mutation of the supplied descriptor does not retarget that Queue,
including its ephemeral operations, move compatibility checks, or activity
waiters. A newly constructed Queue may use the descriptor's edited values.
`Queue.db_target` reports a value-equivalent detached descriptor when the
effective target is a `BrokerTarget`; changing supported containers in that
returned value does not alter the Queue. String targets remain strings.
Caller-supplied runners retain their existing resource and identity ownership.

The SQLite filename grammar in [SB-CLI-2] also applies to filesystem targets
supplied through Queue, open_broker, SQLiteRunner, and SQLite project-target
discovery. On explicit filesystem paths it constrains the terminal database
filename, not arbitrary parent directories, whether the explicit path is
relative or absolute. Arbitrary parent directories retain their spelling,
including spaces and punctuation. Discovery and absolute directory
configuration use the same host-path boundary; they do not revalidate resolved
ancestors as names. Thus `db_path="my dir/broker.db"` remains admissible;
`DEFAULT_DB_NAME="my dir/broker.db"` is a compound default name and is
rejected under [SB-CLI-2]. Validation precedes filesystem
creation or backend setup and raises ValueError (or its existing typed
boundary-specific subclass). Empty and `:memory:` non-filesystem targets
retain their existing supported/rejected behavior at boundaries that already
recognize them as non-filesystem targets; an absolute filename ending in
`:memory:` is not a sentinel. Queue's omitted/empty target still selects its
configured default. Both a supplied filename and the resolved filesystem
filename must satisfy the grammar; symlink resolution cannot admit an
otherwise invalid name.
The grammar does not constrain PostgreSQL database names, Redis namespaces,
or project-configuration filenames.

A Redis Queue constructed with a BrokerTarget and no injected runner binds
its effective namespace from explicit backend options or, when omitted, its
retained Config. The bound descriptor supplies storage, move compatibility,
and activity-waiter identity. `db_target` reports that effective namespace
in a detached options dictionary. Construction validates and normalizes
these Redis options without creating a runner, contacting storage, or
allocating a listener. The original explicit target string and project
metadata are preserved. Injected runners retain their existing ownership
and identity rules; unrelated Config tuning does not make queue targets
incompatible. A Queue destination with a different effective target is
rejected under [SB-DELIVERY-3]. Unknown-option and namespace/schema errors
raise the existing DatabaseError at construction, rather than first use.

Project configuration is a trusted developer input. When the configured target
string contains a recognized inline password, project-config loading emits a
redacted advisory warning that does not include the password. SimpleBroker does
not inspect, warn on, or enforce project-config file mode, ownership,
parent-directory permissions, or ACLs. Confidentiality and integrity of the
config path are governed by the effective operating-system permissions across
the file and its containing directories.

A project file's `backend_options` value must be a TOML table. Its recursive
TOML-native values, including nested tables and arrays, are passed unchanged to
the selected backend plugin; the core project loader does not impose a scalar-
only option schema before plugin dispatch. The plugin owns option validation
and normalization and returns an ordinary options dictionary that remains
lossless through `BrokerTarget` serialization. SQLite follows the same
ownership rule and rejects or normalizes its options through its plugin rather
than bypassing plugin validation. TOML date/time values reach the plugin as
native values; a plugin must normalize them to lossless target transport values
or reject them explicitly rather than relying on the core to coerce them.

Process-session identity preserves type distinctions within recursively
supported option and configuration values. It does not merge distinct opaque
values solely because their `repr()` strings match. When no stable value
representation exists, process-local object identity is the safe fallback:
creating an extra session is acceptable; sharing a session across distinct
backend configuration is not. Namespace and field-declaration metadata also
participate in session identity, since they determine later derived overrides.
Target options are detached at session acquisition
using the existing recursive key-material copy. Those same values supply registry
identity and lazy factory construction. A supplied Config is retained
as given: its top-level bindings are read-only, and nested custom values remain
caller-owned under the light-freeze contract. Mutable containers inside a resolved
Config can still change its values; mutating them, including through references
retained by the caller, is unsupported and may result in unspecified behavior,
including disagreement between session identity and the values used by a session.

### Shared configuration resolution

The package root exports `resolve_config`, `Config`, `DEFAULT_CONFIG`,
`ConfigField`, `serialize_config`, and `deserialize_config`. Field-specific
validators are private implementation details;
embedders reuse a field record or supply their own validator. Definitions live
together in
`simplebroker/_constants.py`; there is no separate public configuration module.
`DEFAULT_CONFIG` is a read-only mapping of uppercase unprefixed names to field
records containing a default, description/unit, validator and sensitivity flag.
Embedders copy the mapping, replace records or add their own fields. Defaults
containing only application fields are supported.

`resolve_config(prefix=None, *, defaults=None, toml=None, env=None, override=None,
config=None)` returns a read-only `Config`. Sources apply lowest to highest:
defaults, TOML, environment, override. Declared defaults are initialized before
source overlays. `env=None` means no ambient environment input. The function
mutates none of its inputs. A source that is not a mapping raises `TypeError`.

`config=` takes an already-resolved `Config` and is the only way to start from
one. When it is supplied, the resolver returns that object unchanged and reads
neither TOML nor the environment, because the config absorbed them when it was
built. With a non-empty `override` it derives a new Config: the supplied config's
values, namespace and field declarations, with the override applied and
validated. Values already resolved are not revalidated. A `config` value that is
not a `Config` raises `TypeError`, and a `prefix` or `defaults` that differs from
the supplied config's own raises `ValueError`; a config's namespace cannot be
rebound.

`Config.prefix` retains namespace metadata separately from configurable values.
The default namespace is declared once as `DEFAULT_PREFIX = "BROKER"` beside
`DEFAULT_CONFIG`. A fresh config uses the explicit prefix or `DEFAULT_PREFIX`; a
config derived through `config=` inherits the supplied config's prefix and field
declarations, so custom validators continue to apply. The namespace is not an
env/TOML/override setting.

`args` is not a resolver input. `cli.py` owns argv parsing and validation;
existing command and target options keep their documented precedence. Global
`--dir` and `--file` reuse the config path-component validator, with their own
flag-specific path rules rather than the distinct default-field constraints. No generic
configuration flags are added. Programmatic callers use namespaced `override`
values; they do not pass arbitrary mappings as command-line arguments.

Env and an explicitly supplied TOML root select only the exact prefix followed
by `_` and a suffix matching `[A-Z][A-Z0-9_]*`. Bare keys and other prefixes are
ignored. No key is case-folded. Well-formed selected custom keys survive even
without a declaration: strip the prefix and preserve the value unchanged.
Registered built-in and user-provided validators are called through the same
path. A near-miss name whose uppercase suffix matches a declared field warns
with its source and suggested field, then is ignored; external name selection does
not reject input. This rule applies when the supplied source mapping preserves
spelling. A case-insensitive operating system may canonicalize environment keys
before the process can inspect them; callers can use an explicit case-sensitive
`env` mapping when original spelling is significant. An invalid registered value warns as its source is applied;
if it is still in effect after all sources, `InvalidConfigError` is raised with
key, source, expected form and safely redacted value metadata.

`override` uses the same namespaced selection and field validators as external
env/TOML values, but rejects a bare key, another prefix, or a malformed selected
name with `ValueError` instead of ignoring or warning. Well-formed selected custom
keys are preserved. For example, with prefix `WEFT`, `WEFT_CACHE_MB` sets internal
`CACHE_MB`; `CACHE_MB`, `BROKER_CACHE_MB`, and `WEFT_cache_mb` raise. Selected
custom `WEFT_SOMETHING_CUSTOM` becomes `SOMETHING_CUSTOM`, whether declared or not.

One validator per registered field accepts that field's documented input and
returns its configuration unit. Every supplied value is validated; each invalid
value emits a `UserWarning` naming its source as that source is applied. A later
valid value replaces it. If any invalid value remains after all sources,
`InvalidConfigError` is raised for it, naming the source that supplied it. A relative `DEFAULT_DB_LOCATION` is an invalid value. After
all sources, one whole-config check validates combined project path/name depth
on the final values; its error reports the source of the final
`PROJECT_CONFIG_NAME`.
Checks skip absent broker fields. `VACUUM_THRESHOLD` stores a
percentage in 0–100: `10` and `"10"` mean ten percent; `0.1` means 0.1 percent.
Integers, floats and numeric strings are accepted for this percentage field;
booleans and boolean strings such as `"true"` are rejected.
Maintenance callers divide by 100 when passing a fractional threshold.
`JITTER_FACTOR` remains fractional.

`Config` exposes uppercase unprefixed keys only, with no namespaced or
case-folded aliases. Top-level mutation is prevented.

`serialize_config(config: Config) -> str` returns JSON text containing
`{"prefix": config.prefix, "values": {...}}`. Values use the existing uppercase
unprefixed Config names. The payload contains all resolved values, including
custom fields and values equal to their defaults. The field-declarations
table, validators, Python class identity, module/import paths and process
resources do not travel. Export does not run field validators.

Supported values are JSON null, booleans, integers, finite floats, strings,
lists, and objects with string keys, recursively. Subclasses representing
these JSON types may be accepted as their JSON value; custom attributes do not
travel. Tuples, sets, bytes, datetime objects, arbitrary objects, and non-string
object keys are unsupported. No stringification, tagged codec, or lossy
key/tuple conversion is performed. Unsupported types raise `TypeError`;
non-finite floats and cyclic containers raise `ValueError`. These restrictions
apply only to transport, not to in-process Config values.

`deserialize_config(payload: str | Mapping[str, Any], *,
defaults: Mapping[str, ConfigField] = DEFAULT_CONFIG) -> Config` accepts JSON
text or its decoded object. The envelope requires a string `prefix` and an
object `values`; additional envelope keys are ignored and cannot select code
or field declarations. Missing or incorrectly typed envelope members and a
non-object root raise `ValueError`; unsupported values follow the same JSON
type rules as export. Malformed JSON raises `ValueError` (including its
standard JSONDecodeError subclass). The prefix follows existing resolver
semantics; this transport adds no new namespace grammar.

Reception prefixes each top-level value name with the transmitted namespace
and passes the resulting mapping to `resolve_config(prefix, defaults=defaults,
override=values)`, without environment, TOML, or an existing Config. Receiver
declarations supply defaults, sensitivity metadata and validators. Built-in
declarations are the default; applications supply their own extended or
application-only table explicitly. Declared custom fields use the same
validator path as built-ins; undeclared well-formed custom fields remain
pass-through values. Invalid field names and values retain resolver error and
warning behavior, including `InvalidConfigError` (a `ValueError` subclass)
when receiver field validation remains invalid. This is distinct from
transport-shape `TypeError` and `ValueError`. Missing declared fields receive
receiver defaults. A
reconstructed Config is a new ordinary Config, not a restored subclass or
the sender's object. Later overrides use its receiver-owned declarations.

For the same field semantics, built-in values and documented units round-trip
unchanged. Custom validators run locally on received values and may normalize
or reject them; applications own consistent declarations across processes.
No cross-version compatibility or declaration-equivalence guarantee is added.
In-process configuration and session ownership behavior are unchanged.

The payload is lossless data transport and may contain credentials; do not log
it or treat it as a redacted diagnostic. Transport-shape errors must not echo
rejected values. Resolver validation retains its existing safe diagnostics.

`Config` also supports ordinary Python pickle, including direct arguments to
`multiprocessing.spawn`. Pickle preserves resolved values, prefix, field
records and normally picklable subclass state. Mapping proxies are restored
on unpickling; validators are not rerun and no environment or TOML is read.
Subsequent overrides retain the restored field declarations and validators.
Unlike JSON transport, pickle follows Python's ordinary rules for values,
classes and callable references: their definitions must be importable in the
receiver. Lambdas, local functions and other unpicklable members fail normally;
validators are never silently dropped. Picklable non-JSON values are supported.
Use JSON transport when the receiver must supply its own declarations. Pickle
is for trusted bytes, including controlled parent-to-child spawn arguments;
repository membership alone does not establish that trust. No cross-version
pickle compatibility or live-resource transport guarantee is added.


A Config handed to Queue, watcher, target discovery, broker, session, runner,
command or load code is retained without re-resolution or ambient reads,
including at later lazy acquisition. Complete values, namespace and field
declarations participate in session identity; no broker-only projection is used.

TOML activation is explicit via `toml=`. A file may also contain existing
unprefixed project-target fields, which this loader ignores. Target discovery,
its parser and its precedence remain unchanged; merely adding tuning keys to a
discovered project file does not activate them.

The old `load_config`, `snapshot_config`, `resolve_isolated_config`,
`build_config`, `ResolvedConfig`, `ConfigSnapshot`, `ConfigSchema` and
`CONFIG_DEFAULTS` APIs are removed. There are no compatibility aliases or
`preserve_unknown` flag. Backend API v9 handshake and factory signatures remain;
in-repo plugins use the same uppercase unprefixed Config as core.

Configuration-consuming functions take `Config`; public boundaries that permit
omission accept `Config | None`. Callers resolve ordinary mappings explicitly
with `resolve_config(override=namespaced_values)` before passing configuration onward. An absent
config resolves once at its ownership event below with `resolve_config()`:
defaults only, no environment. The environment is read once, by the process
entry point: `cli.main()` for the CLI, or a program that calls
`resolve_config(env=os.environ)` and passes the result on. A supplied `Config`
is retained as given: consumers pass it through `resolve_config(config=...)`,
which rejects anything that is not a `Config` with `TypeError` but neither copies
nor revalidates it. Per-call
config replaces the retained snapshot for that call; it is not a sparse overlay.
To derive an overlay, callers use `resolve_config(config=config, override=namespaced_values)`
first. Internal helpers take an already resolved `Config`.

| Public surface | Snapshot event |
|----------------|----------------|
| `resolve_config()` | During that call, using only explicit sources. |
| `Queue`, watcher, `DBConnection`, and other eager config-consuming constructors | During the constructor call, before owned resource side effects. |
| Eager discovery and load functions | At the first config-consuming branch during the function call; config-independent validation that already precedes that branch keeps its existing order. |
| `open_broker()` | On `__enter__` of the returned generator-based context manager, not when the context-manager object is created. The marker is retained through `__exit__`. |
| Transactional generator per-call config | On first iteration of the configuration-consuming `at_least_once` path, when the Python generator body begins. The supplied Config, or the broker snapshot when omitted, is retained until exhaustion or close. Creating the generator object alone does not read configuration. |
| Direct `cmd_*` functions | At first actual config consumption after any contract-preserved config-independent early path; then once for the rest of that invocation. |
| `cli.main()` | Once, from the process environment, before parser construction and argument parsing, preserving the existing invalid-config-before-parsing rule. |
| `dump_lines()` | Never. It consumes an already-open broker and receives no config argument; CLI dump configuration belongs to its broker-opening path. |

Environment variable and TOML field catalogs for project scoping remain in the
README residual where listed; this clause owns the **public callables**, not
every config key.

_Implementation mapping_:
- `simplebroker/_constants.py`
- `simplebroker/project.py`
- `simplebroker/_project_config.py`
- `simplebroker/_key_material.py` and `simplebroker/_broker_session.py`
- `simplebroker/db.py` (`open_broker`)
- `simplebroker/_targets.py` / target types via public re-exports

## Queue lifecycle [SB-API-3]

**`Queue`** is the primary programmatic handle for named-queue operations.

- Construct with a queue name and a path string or `BrokerTarget` (and optional
  config) as documented on the type.
- Prefer context-manager use or an explicit **`close`** when the handle owns
  resources; cleanup is part of the public lifecycle.
- Queue construction resolves omitted configuration to one `Config`, or retains
  the supplied `Config` directly as that instance's snapshot, including for
  ephemeral operations and later lazy backend/core creation. Any documented
  per-call config replaces the retained snapshot for that call.

`Queue.backend_name` is a read-only string containing the resolved backend
plugin name. Built-in names are `"sqlite"`, `"redis"`, and `"postgres"`;
third-party plugin names are not restricted to that set. The property follows
the same resolution owner as Queue operations, including a backend-aware
injected runner, and performs no database I/O. `"pg"` is not an alias.

_Implementation mapping_:
- `simplebroker/sbqueue.py`

## Queue operations (library shape) [SB-API-4]

Public write, read, peek, move, delete, and related queue methods on `Queue`
are **library-shaped**:

- They **return values** or **raise** package exceptions.
- They do **not** use CLI process exit codes or stdout printing as their
  primary contract (contrast [SB-API-10] and `[SB-CLI-*]`).

`Queue.write(message, *, keep_newest: int | None = None) -> int` optionally
applies [SB-DELIVERY-9]. `None` preserves ordinary write. An exact `int` from
1 to 9999 is required otherwise. Booleans and all other types, including digit
strings, raise `TypeError`; an out-of-range integer raises `ValueError`.
Validation precedes target acquisition. Success retains the scalar committed-
message-ID result.

High-level `Queue.read()`, `Queue.peek()`, and `Queue.move()` are supported
flag-directed convenience views over their granular `*_one`, `*_many`, and
`*_generator` methods; they are not legacy aliases or a third operation
model. `read` remains consuming and `peek` remains observational under the
delivery vertical. The selected flags determine cardinality and record shape.
Public typing uses overloads to narrow calls made with literal flag values and
retains a full union for an unknown runtime `bool`; overloads do not create a
second implementation path.

`Queue.read`, `Queue.peek`, and `Queue.move` accept keyword-only
`order: str = "oldest"` when they return one message or a bounded materialized
result. Their `*_one` and `*_many` forms accept the same keyword. The only
non-default value is `"newest"`; invalid values raise `ValueError` before a
broker target is acquired or mutated. A high-level call with
`all_messages=True` accepts only the default and rejects `"newest"` before
target acquisition. `*_generator`, `stream_messages`, and watcher forms do not
accept `order`. `find_message_ids()` remains an ascending-only administrative
search: it returns matching IDs in ascending integer public message-ID order
and does not accept `order`.

The `all_messages=True` views of `Queue.read()`, `Queue.peek()`, and
`Queue.move()` return `CloseableIterator[...]`; an unknown runtime `bool`
includes that closeable iterator in the existing scalar/tuple/iterator union.

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

`Queue.read_generator()`, `Queue.peek_generator()`,
`Queue.move_generator()`, and `Queue.stream_messages()` return
`CloseableIterator[...]`. The high-level `all_messages=True` read, peek, and
move views return the corresponding closeable iterator shapes. Peek lifecycle
and live traversal remain governed by [SB-DELIVERY-4]; delivery settlement
remains governed by [SB-DELIVERY-5]; read, move, and stream iterator ownership
is [SB-DELIVERY-6].

Backend-facing `BrokerConnection` generator methods remain ordinary
`Iterator[...]` seams. The public close contract belongs to the outer Queue
generator that owns `Queue.get_connection()`; it does not require backend
API changes, a runtime wrapper, or generator-only `send()` and `throw()`
operations.

Where delivery requires it, materializing batch APIs **commit selected claims
before returning** their result lists. Generator modes that document
`at_least_once` or batch commit intervals follow the delivery vertical, not a
second library-only delivery model.

For transactional `claim_generator` and `move_generator` calls, the supplied
per-call `Config` is selected, or the broker's retained snapshot when omitted,
when the configuration-consuming `at_least_once` generator is first iterated.
This is the normal Python generator-body boundary, not generator-object creation.
That snapshot remains fixed for the generator's lifetime and does not read ambient
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
`Config`, and later polling, waiting, callback dispatch, runner
creation, and documented per-call overrides do not reread ambient
configuration. A watcher constructed from an existing `Queue` adopts that
Queue's retained snapshot when watcher config is omitted. An explicit `Config`
replaces it for watcher-local policy without consulting the environment.
The supplied Queue remains governed by its own retained snapshot.

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

The run owns an active consume-batch iterator on the thread that advances it
and closes it exactly once during unwind. With no active failure, an ordinary
iterator-close exception surfaces. During an ordinary retryable failure, an
ordinary close exception is retained as an ordered PEP 678 note and does not
replace it. During terminal error-handler failure, that note is attached to
the public error-handler exception. A close exception during an otherwise
clean stop is instead a terminal cleanup failure: it bypasses watcher retry
and clean-stop swallowing, surfaces to the caller (or standard thread
exception hook), and retains the stop signal as context. A `BaseException`
outside `Exception` keeps the existing lifecycle priority.

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

SQL schema migration may rewrite only SimpleBroker-owned tables, indexes,
constraints, and sequence state. Successful and failed migration leaves each
caller-owned sidecar table definition, row, index, constraint, and sequence
state unchanged. `RESERVED_TABLE_NAMES` and broker-owned `idx_*` indexes are
not sidecars: caller changes to those owned objects are unsupported and have no
migration-preservation promise. A sidecar dependency on a removed private
broker column is unsupported, with backend-specific migration behavior:

- **SQLite:** migration does not block on such unsupported dependencies.
  Attached indexes and triggers are dropped with the old broker table; a
  detached view or foreign-key definition may survive but be broken. Take a
  whole-file backup before migration if the target may contain them.
- **PostgreSQL:** the removed column is dropped with `RESTRICT`, so such a
  dependency fails without mutation. Migration never uses `CASCADE` to erase
  caller-owned state.

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
`simplebroker.ext`, or `simplebroker.commands` does not read the environment
or raise an import-time configuration exception. Library handles and
invocations without a `Config` use defaults and never read the environment.
Each `resolve_config(env=os.environ)` call reads the environment strictly and
raises a fresh `InvalidConfigError` for an invalid value; the resulting
`Config` stays fixed for the lifetime of the handles it is passed to.

- Library failure is signaled by **exceptions**, not by CLI process exit codes
  (`[SB-CLI-1]` applies to the CLI and [SB-API-10]).
- Exception **message text** is not a frozen product contract; catch types, not
  substrings.
- Some runtime failures may still surface as plain `RuntimeError` (for example
  exhausted retries); `BrokerError` is the root of package-defined SimpleBroker
  exceptions, not an exhaustive catch for every failure.

_Implementation mapping_:
- `simplebroker/_constants.py` (`BrokerError`, `InvalidConfigError`)
- `simplebroker/_exceptions.py`
- `simplebroker/ext.py`

## Command layer (second surface) [SB-API-10]

**`simplebroker.commands`** is a supported public module whose `__all__` names
are stable under the same compatibility policy as other public exports.

`cmd_write(..., *, keep_newest: int | None = None, ...)` exposes the same
normalized option and validates it before config consumption, stdin, alias
resolution, or target access. It retains existing return codes and stdout
shapes.

- Each **`cmd_*`** function is the programmatic equivalent of a CLI subcommand.
  Ordinary outcomes return integer codes with `[SB-CLI-1]` meanings. Invalid
  input and operational failures raise their typed exceptions to direct Python
  callers; `simplebroker.cli` is the sole owner that translates those
  exceptions to diagnostics and process exit codes.
  Direct selector combinations obey the CLI grammar: exact-ID selection cannot
  be combined with all/range selection, and exact-ID delete requires a queue.
  Rejected combinations raise before target access or mutation. `cmd_load`
  likewise raises its original input, integrity, and timestamp failures; only
  the CLI adds the `broker load:` recovery presentation. Queue-wide and
  all-queue `cmd_delete` return `0` only when at least one row was deleted and
  return `[SB-CLI-1]` no-match `2` when the affected-row count is zero.
- Direct `cmd_*` stdout behavior matches the corresponding CLI action when the
  consumer closes: `cmd_read`, `cmd_peek`, `cmd_move`, `cmd_dump`, and
  `cmd_watch` return clean-stop `0`; every other stdout-producing command
  function returns `1` after its ordinary plain or JSON output-delivery
  diagnostic. The
  internal closed-stdout control signal never escapes the public command
  function. Durable effects completed before output failure remain completed.
  Where a command function accepts `quiet`, it suppresses the same owned
  commentary as the CLI without suppressing errors or unrelated warnings.
  In particular, `cmd_load` does not replace process-global warning hooks or
  filters while presenting its invocation's clock-skew notice.
- A direct command-layer caller receives `InvalidConfigError` when that command
  consumes an invalid configuration; the integer exit-code guarantee applies
  once command execution begins. Direct `cmd_*` calls without `config` use
  defaults and do not read the environment. The CLI process
  wrapper is the sole translator that turns a typed configuration-initialization
  failure into the `[SB-CLI-2]` stderr diagnostic and exit `1`.
- Helpers listed in that module’s `__all__` (for example
  `parse_exact_message_id`) are part of this surface.

Each direct `cmd_*` invocation that consumes configuration creates one
invocation-scoped `Config` and reuses it through target selection,
Queue/broker construction, and operation execution. Repeated programmatic
calls may therefore observe intentional environment changes between calls,
while no call observes a change after its snapshot is created. Existing
config-independent early-validation paths remain config-independent.

Direct command functions for bounded read, peek, and move accept the same
normalized `order` string. The CLI adapter maps `--newest` to `"newest"` and
otherwise passes `"oldest"`; it does not implement ordering independently.

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
an explicitly injected runner from its caller to SimpleBroker. SQL-backed
cores enforce that boundary through the private borrowed-runner wrapper, which
masks both destructive verbs during core, connection-manager, Queue,
context-exit, and finalizer teardown.

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

Backend target admission separates four questions: whether the target is owned
by SimpleBroker, whether its stored version is older, current, or newer than
this implementation, whether current-version correctness postconditions hold,
and whether a setup coordination phase previously completed. An older owned
target reaches its migration path, or an explicit owned-but-unsupported
diagnostic when that backend has no migration. A newer owned target is rejected
before any connection-wide setup or SimpleBroker-authored durable schema,
index, metadata, or marker mutation. Opening the one normal SQLite connection,
applying connection-local settings, and SQLite's own recovery or WAL
coordination are outside that durable-state invariant. Foreign, malformed, and
irrecoverably partial targets remain rejected. Absent targets and backend
namespaces that are present but empty may be initialized; initialization never
overwrites a foreign or partial target.

A SQLite `schema-vN` phase marker is a cache hint, not schema proof. The marker
may skip idempotent migration and repair only when database-internal proof
metadata names the current proof algorithm and records the current SQLite
`PRAGMA schema_version` cookie. Missing or stale proof takes the existing schema
lock, rechecks state, runs the fact-level slow path, and republishes proof before
phase completion. Matching proof requires only scalar metadata reads and does
not perform table, index, or message scans on ordinary open. Proof metadata is
additive and optional to older clients; it does not require a stored schema-
version bump.

Proof means only that the existing idempotent setup, migration, and repair
routine completed successfully for this SQLite schema generation using the
current proof algorithm. It is not a general schema attestation and does not
attest mutable metadata or message data. The slow path remains the single owner
of schema correctness; proof publication must not add a second schema validator
that can drift from it.

A backend entry-point name must resolve to exactly one installed entry point.
Ambiguous duplicate registrations fail before either candidate is loaded.

Process-session shutdown attempts every ordinary core, factory, and remaining
registry cleanup that is still safe after an `Exception`. One ordinary cleanup
exception remains primary using the existing convention; later failures remain
available as diagnostics, but their order is not public behavior. A
`BaseException` outside `Exception` retains its existing propagation priority
and may interrupt later cleanup.

Persistence helpers are public from the package root. The exact load interface
is `load_lines(broker, lines, *, force=False, config=None)`; its policy and
failure order are `[SB-IO-4]`. `DumpClockSkewWarning` is a public `UserWarning`
subclass importable from `simplebroker` so embedders can filter it.

`TimestampGenerator.validate()` is the public string-parser surface for
timestamp bounds. Its accepted and rejected spellings are the three grammars in
`[SB-CLI-5]`; library methods whose `after_timestamp` / `before_timestamp`
parameters already accept integer message IDs do not reparse those integers as
strings.

Backend API v8 adds the validated selection order to claim, peek, and move
one/many operations. First-party plugins declare v8 exactly. SQL backend
storage schema v6 has one supported canonical SimpleBroker-owned layout, where
`ts` is the primary key and no private surrogate exists; the v6 migration
rebuilds older layouts into it without mutating caller-owned sidecars.
PostgreSQL migration is serialized by a database advisory lock and makes its
version decision from a live under-lock metadata read. Current clients reject
a database with a newer schema version during cold admission, before any
message-table operation. For the immediately previous release, that clean
refusal guarantee covers the normal target-backed cold-admission path. The
injected `Queue(..., runner=PostgresRunner(...))` path in `simplebroker-pg`
3.10.0 is a known legacy exception: its borrowed-runner wrapper can bypass the
old metadata fast path and reach a missing `order_id` diagnostic instead.
Already-admitted old clients and that injected-runner path are outside the
mixed-version guarantee. Operators must stop every old process and every
transaction that accesses caller-owned sidecars, then upgrade core and its
backend extension as one coherent set before performing the one-way migration.
No old client may open or retain the target after v6 migration begins. The
legacy injected-runner path is not a rollback or mixed-version mechanism;
rollback requires restoring the whole pre-v6 target under the old package set.

Backend API v9 changes `BrokerConnection.write` to
`write(queue, message, *, keep_newest: int | None = None) -> int`, receiving
the already-normalized integer. Every backend implements the [SB-DELIVERY-9]
transition when `keep_newest` is present and preserves v8 ordinary-write
behavior when it is absent. First-party plugins declare v9 exactly; older or
newer versions fail the existing handshake rather than emulating the option as
multiple public operations.

A no-runner persistent Queue must check inherited process-session ownership
before acquisition, project setup, or cleanup can enter a parent-owned
lock. Inherited SQL-backed handles reject operation acquisition with
RuntimeError rather than reopening an inherited SQL core. Redis handles
preserve recovery by acquiring child-owned session state through the
process registry. Releasing an inherited lease must not finalize parent
resources. Newly constructed child Queues remain usable; these rules do
not transfer a suspended parent generator to the child or change the
low-level injected runner's own fork policy.

_Implementation mapping_:
- `simplebroker/ext.py` and its re-export sources
- `simplebroker/db.py`, `simplebroker/_runner.py`, `simplebroker/_phaselock.py`
- `simplebroker/_backend_plugins.py`, `simplebroker/_broker_session.py`
- first-party `extensions/simplebroker_pg`, `extensions/simplebroker_redis`

## Cross-surface matrix [SB-API-12]

Orientation matrix. On conflict of **operation meaning**, the vertical wins;
this table does not redefine claim, id, or filter rules.

| Library | CLI / `commands` | Owning vertical / notes |
|---------|------------------|-------------------------|
| `Queue.write` / exact-insert helpers | `write` / `cmd_write` | `[SB-ID-*]`; optional write-time window [SB-DELIVERY-9] |
| `Queue.read*` | `read` / `cmd_read` | `[SB-DELIVERY-*]`, `[SB-SELECT-*]` |
| `Queue.peek*` | `peek` / `cmd_peek` | `[SB-DELIVERY-4]`, `[SB-SELECT-*]` |
| `Queue.move*` | `move` / `cmd_move` | `[SB-DELIVERY-3]`, `[SB-ID-*]`, `[SB-SELECT-*]` |
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

## First-party PostgreSQL inspection [SB-API-13]

`simplebroker_pg.get_connection_stats(queue) -> dict[str, int]` is a public
PostgreSQL-only helper. The caller narrows on
`queue.backend_name == "postgres"`; the helper rejects any other backend with
`ValueError` before opening a connection. It is absent from the generic
`BrokerConnection` protocol and has no SQLite or Redis implementation.

The fresh returned dictionary has exactly these keys:

- `numbackends`: `sum(pg_catalog.pg_stat_database.numbackends)` across the
  server, including the helper's existing connection. Under stock catalog
  permissions it counts established database-attached backends across roles
  and databases. It can include autovacuum and other workers that do not
  consume `max_connections`; it is a conservative pressure signal, not an
  exact client-connection count.
- `max_connections`: the server's configured limit.
- `superuser_reserved_connections`: reserved superuser slots.
- `reserved_connections`: PostgreSQL 16+ general reserved slots, or zero when
  that setting does not exist.

The helper requires exactly one one-column row containing a keyed JSON object
with those fields. Every value has exact Python type `int`, not `bool`.
`max_connections` is positive, other values are non-negative, and reserve
values sum to less than `max_connections`. There is no
`numbackends <= max_connections` validation because included workers can make
it false. Malformed results raise `ValueError`; execution and permission
failures retain SimpleBroker's public `DatabaseError` hierarchy.

The metric needs no monitoring-role grant or installed object under stock
permissions. A deployment that revokes catalog access may receive a database
permission error; the helper neither grants access nor falls back to a
narrower same-role count.

The helper executes one parameter-free, read-only catalog statement through
the Queue's operation lease and SQL-backed core lock/retry path. It opens no
explicit transaction and does not use sidecar. A target-resolved persistent
Queue reuses its process-session connection on that thread. An ephemeral Queue
may open one connection. An injected runner is functionally supported but
gains no stronger checkout-retention promise from `persistent=True`.

The result is a non-atomic observation, not a permit. New connections can
arrive after the statement; consumers must retain a safety margin and must not
promise that admission cannot overshoot a PostgreSQL limit.

_Implementation mapping_:
- `simplebroker/sbqueue.py` (`Queue.backend_name`)
- `simplebroker/db.py` (private first-party SQL probe)
- `extensions/simplebroker_pg/simplebroker_pg/connections.py`
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`

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
| [SB-API-2] | `tests/test_path_security.py::test_host_ancestors_public_queue_and_project_discovery`; `tests/test_config_transport.py`; `tests/test_config_builder.py`; `tests/test_config_coexistence.py`; `tests/test_python_library_api_contract_sb_api.py`; `tests/test_isolated_config.py`; `tests/test_connection_config.py::test_library_handles_without_config_ignore_environment`; `tests/test_project_config.py` (recursive plugin-owned options, TOML-native normalization/rejection, target serialization, and SQLite rejection); `tests/test_process_broker_session.py` (type/opaque identity, one recursive key/factory snapshot, and all SQLite public option paths); `tests/test_activity_waiter_api.py::test_create_activity_waiter_for_queues_rejects_distinct_same_repr_options`; `tests/test_ext_imports.py` (project-config identity); `tests/test_invalid_config_lifecycle.py::test_load_config_reports_invalid_environment_field`, `tests/test_invalid_config_lifecycle.py::test_public_snapshots_are_explicit_and_fresh_across_calls`, `tests/test_invalid_config_lifecycle.py::test_each_invalid_snapshot_raises_a_fresh_exception_and_repair_recovers`; `tests/test_config_builder.py::test_numeric_coercion_failure_uses_warning_and_final_value_policy`; `tests/test_connection_config.py`; `tests/test_constants.py`; `extensions/simplebroker_redis/tests/test_redis_core_behaviors.py::test_queue_move_rejects_config_derived_namespaces` |
| [SB-API-3] | `tests/test_connection_config.py::test_explicit_config_is_retained_at_constructor`; `tests/test_python_library_api_contract_sb_api.py`; `tests/test_backend_plugin_resolution.py` (built-in, third-party, and injected-runner backend identity without target I/O); `tests/test_connection_config.py::test_library_handles_without_config_ignore_environment`, `tests/test_connection_config.py::test_persistent_queue_keeps_snapshot_before_first_lazy_core_creation`; Queue lifecycle coverage in `tests/test_queue_api_*.py` |
| [SB-API-4] | `tests/test_timestamp_selection_contract_sb_select.py::test_bounded_one_and_many_order_matrix`, `::test_invalid_or_unbounded_order_fails_before_target_acquisition`, `::test_generator_signatures_do_not_expose_order`; `tests/test_queue_typing_contract.py`; `tests/test_delivery_contract_sb_delivery.py::test_closeable_queue_iterator_releases_operation_on_same_thread`; `tests/test_peek_generator_lifecycle.py` (high-level `all_messages=True` path); `tests/test_queue_api_additions.py::test_queue_move_all_closes_transformation_delegate`, `::test_queue_delete_explicit_none_is_rejected_without_mutation`, `::test_queue_move_returns_plain_dictionary_with_typed_fields`; `tests/test_python_library_api_contract_sb_api.py::test_api_write_keep_newest_signatures_and_public_validator`; `tests/test_keep_newest.py`; delivery/id/select/bcast suites for meaning |
| [SB-API-5] | `tests/test_queue_typing_contract.py`; `tests/test_delivery_contract_sb_delivery.py::test_closeable_queue_iterator_releases_operation_on_same_thread`; `tests/test_peek_generator_lifecycle.py`; `tests/test_python_library_api_contract_sb_api.py::test_api_closeable_peek_iterator_contract`; `tests/test_connection_config.py::test_generator_override_inherits_core_snapshot_without_ambient_reread`, `tests/test_connection_config.py::test_generator_retains_explicit_config_on_first_iteration`; Queue generator / `*_many` suites |
| [SB-API-6] | `tests/test_python_library_api_contract_sb_api.py::test_api_activity_waiter_terminal_close_contract`, `tests/test_python_library_api_contract_sb_api.py::test_api_watcher_start_stop_cleanup_ownership_contract`, `tests/test_python_library_api_contract_sb_api.py::test_api_polling_strategy_defaults_match_canonical_config`; `tests/test_watcher_error_handler_contract.py`, including `test_batch_iterator_close_failure_is_secondary_to_error_handler_failure`; `tests/test_watcher_stop_contract.py::test_stop_racing_start_has_one_cleanup_owner`, `test_join_timeout_does_not_transfer_cleanup_from_live_run`, `test_cleanup_failure_keeps_lifecycle_retryable`, `test_context_exit_suppresses_stop_failure_without_replacing_body_exception`, `test_context_exit_cleanup_failure_remains_retryable`, `test_context_exit_propagates_base_exception_from_stop`, `test_batch_iterators_close_once_on_exhaustion_after_handler_continuation`, `test_batch_iterator_close_failure_without_active_failure_surfaces`, `test_batch_iterator_close_failure_is_note_on_retryable_failure`, `test_batch_iterator_close_failure_during_clean_stop_is_terminal`, `test_batch_iterator_close_base_exception_keeps_cleanup_priority`; `tests/test_watcher_transition_tables.py::test_watcher_lifecycle_fires_transition_table`; `tests/test_watcher.py::TestPollingStrategy::test_defaults_use_ambient_free_canonical_config_snapshot`, `tests/test_watcher.py::TestPollingStrategy::test_all_defaults_derive_from_one_isolated_canonical_snapshot`; `tests/test_connection_config.py::test_watcher_instance_config_maps_into_strategy_fields`, `tests/test_connection_config.py::test_polling_strategy_fields_determine_delay_schedule`; `tests/test_connection_config.py::test_watcher_given_queue_adopts_queue_snapshot_and_overlays`; `extensions/simplebroker_pg/tests/test_pg_activity_waiter_lifecycle.py`; `extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py`; PostgreSQL notify and Redis integration replacement tests; watcher suites; `extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py::test_config_derived_namespace_wakes_public_waiter` |
| [SB-API-7] | `tests/test_python_library_api_contract_sb_api.py::test_api_generators_watchers_sidecar_io_errors_language`; `tests/test_sqlite_schema.py::test_schema_v6_migrates_despite_unsupported_caller_objects`; `extensions/simplebroker_pg/tests/test_pg_message_id_order.py::test_real_postgres_removed_key_dependency_rolls_back_v5_migration`; sidecar suites under tests / examples |
| [SB-API-8] | `tests/test_persistence_io_contract_sb_io.py`; `tests/test_dump_load.py`, including `test_load_without_config_ignores_environment` |
| [SB-API-9] | `tests/test_python_library_api_contract_sb_api.py`; `tests/test_ext_imports.py`; `tests/test_invalid_config_lifecycle.py::test_invalid_environment_does_not_break_package_import`, `tests/test_invalid_config_lifecycle.py::test_sensitive_config_failure_redacts_before_formatting`, `tests/test_invalid_config_lifecycle.py::test_each_invalid_snapshot_raises_a_fresh_exception_and_repair_recovers`; `tests/test_malformed_target_diagnostics.py`; `tests/test_config_builder.py::test_sensitive_validator_overflow_keeps_safe_metadata` |
| [SB-API-10] | `tests/test_timestamp_selection_contract_sb_select.py::test_direct_command_accepts_normalized_newest_order`, `::test_direct_command_rejects_newest_all_before_target_resolution`; `tests/test_commands_error_ownership.py` (direct invalid-input/operational exceptions, selector parity, delete no-mutation, queue/all delete result, and CLI-owned diagnostic boundary); `tests/test_commands_status.py`; `tests/test_commands_init.py`; `tests/test_cli_dump_load.py`; `tests/test_dump_load.py::test_quiet_cmd_load_does_not_hide_another_threads_clock_skew_warning`, `test_cmd_load_warning_policy_resets_after_success`, `test_cmd_load_warning_policy_resets_after_every_failure`, `test_load_warning_sink_restores_outer_nested_policy`; `tests/test_commands_stdout_delivery.py` (exact direct stdout inventory, write-versus-flush failures, mutation durability, and bare-stdout static gate); `tests/test_cli_main.py::test_keyboard_interrupt_handling`; `tests/test_cli_watch.py::TestWatchCommand::test_watch_sigint_remains_success`; `tests/test_cli_main.py::test_repeated_main_calls_rebuild_defaults_from_invocation_snapshot`; `tests/test_public_surface.py`; `tests/test_python_library_api_contract_sb_api.py::test_api_write_keep_newest_signatures_and_public_validator`; `tests/test_cli_write_output.py` keep-window validation and output cases; `tests/test_invalid_config_lifecycle.py::test_direct_command_early_validation_can_remain_config_independent`, `tests/test_invalid_config_lifecycle.py::test_direct_command_calls_ignore_environment` |
| [SB-API-11] | `tests/test_python_library_api_contract_sb_api.py::test_api_owned_runner_lifecycle_and_backend_v9_contract`, `::test_api_write_keep_newest_signatures_and_public_validator`, `::test_api_load_future_skew_surface_is_root_importable_and_keyword_only`, `::test_api_v6_cutover_contract_names_the_legacy_pg_exception`; `tests/test_sqlite_admission.py` (early version admission, factual migration receipts, scalar proof fast path, stale/missing/fault/concurrent proof cases); `tests/test_sqlite_schema.py` (semantic uniqueness and keep cutoff query plan); `tests/test_phaselock.py`; `tests/test_process_broker_session.py` (continued cleanup and diagnostics); `tests/test_custom_runner_integration.py::test_sql_borrowed_runner_masks_destructive_verbs_across_teardown`; `tests/test_core_persistence_transition_tables.py::test_sqlite_runner_fires_transition_table` (`CLOSE_REOPEN`); `tests/test_runner_lifecycle.py`; `tests/test_backend_plugin_resolution.py` (including v9 exact-version handshake and duplicate ambiguity before load); `extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py`, `test_pg_plugin_contract_edges.py`, `test_pg_ownership.py`; `extensions/simplebroker_redis/tests/test_redis_validation.py`, `test_redis_plugin_validation_paths.py`, `test_redis_plugin_contract_edges.py`; `tests/test_release_script.py::test_repository_backend_api_v9_handshake_and_floors_match`; `tests/test_dump_load.py::test_load_header_floor_persists_when_local_cache_is_ahead`, `tests/test_dump_load.py::test_load_header_floor_observes_concurrent_durable_winner`, `tests/test_dump_load.py::test_load_header_floor_final_read_failure_is_outcome_ambiguous`; `tests/test_timestamp_advance.py`; `extensions/simplebroker_pg/tests/test_pg_timestamp_resilience.py::test_postgres_missing_last_ts_row_fails_loudly`; `extensions/simplebroker_redis/tests/test_redis_core_behaviors.py::test_redis_timestamp_advance_transport_failure_is_ambiguous_after_real_eval`; `tests/test_timestamp_bound_grammar.py` (public validator grammar and exact ISO conversion); `tests/test_fork_safety.py::test_inherited_queue_rejects_before_parent_session_lock`; `extensions/simplebroker_redis/tests/test_redis_pool.py::test_public_persistent_queue_recovers_child_owned_session` |
| [SB-API-12] | `tests/test_python_library_api_contract_sb_api.py` (matrix present); kernel CLI↔Python map |
| [SB-API-13] | `tests/test_python_library_api_contract_sb_api.py::test_api_postgres_connection_inspection_contract`; `tests/test_backend_probe.py`; `extensions/simplebroker_pg/tests/test_connection_stats.py` (shape, ordinary role, cross-role/database, lifecycle, autovacuum, PG15, and PG18) |

## Related Plans

- [Host paths and database names](../plans/2026-09-14-host-path-name-boundary-plan.md): preserve host ancestors while validating selected names.

- retired: 2026-09-13-verified-review-remediation-plan — source `1c6898b`;
  see the ledger in `docs/plans/README.md`. It owns target binding, fork
  ownership, SQLite name clauses, and config-overflow handling.

- retired: 2026-09-11-shared-configuration-loader-plan — source `4efe7b3`;
  see the ledger in `docs/plans/README.md`. It owns the additive shared API
  with preserved legacy views and lifecycle.

- retired: 2026-09-07-critical-review-remediation-plan — source `dbace84`;
  see the ledger in `docs/plans/README.md`.

- retired: 2026-09-02-write-keep-pending-window-plan — source `3418079`;
  see the ledger in `docs/plans/README.md`. It extends [SB-API-4],
  [SB-API-10], [SB-API-11], and [SB-API-12] for the write-time pending window
  and backend API v9.

- retired: 2026-08-27-all-examples-correctness-and-contract-alignment-plan —
  source `813dd7ce`; see the ledger in `docs/plans/README.md`. It aligns public,
  async-wrapper, watcher, sidecar, and runner examples.
- completed: [2026-08-27-message-id-order-and-newest-selection-plan](../plans/2026-08-27-message-id-order-and-newest-selection-plan.md)
  — owns bounded order, sidecar-safe SQL schema v6, backend API v8, and
  cross-surface parity
- retired: 2026-08-25-postgres-connection-pressure-inspection-plan — source
  `813dd7ce`; see the ledger in `docs/plans/README.md`. It owns PostgreSQL-only
  zero-setup connection pressure through the Queue's normal
  connection lifecycle
- retired: 2026-08-25-schema-and-representation-assumption-remediation-plan —
  source `813dd7ce`; see the ledger in `docs/plans/README.md`. It owns storage
  admission/proof, backend target states, session identity/cleanup,
  and plugin uniqueness
- retired: 2026-08-25-verified-review-findings-remediation-plan — source
  `813dd7ce`; see the ledger in `docs/plans/README.md`. It owns borrowed-runner
  teardown, watcher iterator cleanup, command results and
  warning ownership, and exact bounded timestamp parsing
- retired: 2026-08-25-closeable-queue-iterator-contract-plan — source
  `813dd7ce`; see the ledger in `docs/plans/README.md`. It owns the public
  closeable read, move, and stream iterator contract.
- retired: 2026-08-24-peek-generator-close-contract-plan — source `813dd7ce`;
  see the ledger in `docs/plans/README.md`. It owns closeable peek iteration
  and same-thread synchronous Queue-operation cleanup.
- retired: 2026-08-24-comprehensive-review-findings-remediation-plan — source
  `813dd7ce`; see the ledger in `docs/plans/README.md`. It owns target
  snapshots, sidecar qmark adaptation, fork recovery, compatibility,
  command-error ownership, and interface corrections
- retired: 2026-08-24-failure-path-and-contract-findings-resolution-plan —
  source `813dd7ce`; see the ledger in `docs/plans/README.md`. It owns watcher
  callback-failure and context-exit contract promotion at baseline
  `1b8ecfa0`
- retired: 2026-08-24-cli-output-and-error-contract-remediation-plan — source
  `813dd7ce`; see the ledger in `docs/plans/README.md`. All 19 exported command
  functions follow the ordinary-result/direct-exception
  boundary; CLI translation, selector parity, and no-mutation guards are pinned
  under owner-directed targeted closure
- retired: 2026-08-24-cli-grammar-validation-and-example-reliability-plan —
  source `813dd7ce`; see the ledger in `docs/plans/README.md`. It derived every
  `PollingStrategy` constructor default from one isolated
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
