# Shared configuration loader and unprefixed snapshots

Status: completed
Class: 5 — additive public loader and compatibility-sensitive internal refactor; existing public and backend contracts remain unchanged.
Hardening checklist: required for plan quality under writing-plans.md (shared-library boundary and multiple execution contexts); no product-hardening behavior is in scope.
Plan type: implementation with additive spec revision; implementation authorized by user.
Owner: SimpleBroker owns all implementation, tests, documentation, and in-repo extension changes in this plan. Weft and Taut are reference consumers only.

## Binding compatibility requirement

The purpose is to remove duplicated configuration machinery. This is an internal
refactor plus an additive shared-builder API, not a hardening or policy change. Existing public
Python APIs, exported names, returned mapping spellings/iteration, CLI behavior,
env grammar, TOML target behavior, plugin API version and extension contracts
remain compatible. No major-version bump, deprecation, required downstream
migration or backend API bump is proposed. Earlier review dispositions proposing
those breaks are superseded by this explicit user correction.

Canonical unprefixed storage and internal access do not require changing legacy
public views. Compatibility views delegate to the same retained values; they are
not separately resolved broker/app configs. The new builder's schema and snapshot
surface is additive. Public legacy wrappers retain their documented behavior.

## Goal

Implement one shared schema-driven loader in SimpleBroker, migrate this repo's
`_constants` and consumers to unprefixed keys, and make the public API usable by
embedders without a second broker-only configuration. External names retain a
caller-selected prefix such as `BROKER`, `WEFT`, or `TAUT`. Most settings need no
user input but remain present, documented, and individually overridable.

**Repository boundary:** every planned edit and required verification runs in
this SimpleBroker repository, including its `extensions/` packages. Weft and
Taut code, specs, precedence, and lifetimes are read-only reference material.
Their adoption, plans, code, tests, manifests, locks, releases, and runtime
operations are outside this plan. Completion does not depend on either product
migrating. Local synthetic embedders prove the capabilities their usage motivates.

User decisions incorporated: preserve external prefixes; remove internal
prefix-only duplication, including `_constants` and consuming lookups; do not
extract a second broker-only configuration object from an application snapshot.
Embedder extension with non-SimpleBroker fields, defaults, and validators is
a primary design requirement, not a broker-only convenience. Existing documented
precedence is an invariant. Migration details below are proposed for adoption;
this plan does not authorize a new precedence policy.

## Source Documents

Consulted: `docs/agent-context/context.index.yaml` read order, including
`docs/program-theory.md` [THEORY-1], [THEORY-3], [THEORY-4],
[REV-THEORY-004]; shared hub, decision hierarchy, principles, engineering
principles, lessons pointer, and `docs/lessons.md`. Read the writing-plans,
hardening-plans, testing-patterns, review-loops-and-agent-bootstrap,
designing-agent-facing-interfaces, and adversarial-acceptance-probes runbooks.
Applied `skills/interface-review/SKILL.md` and use
`skills/call-agent/SKILL.md` for independent review.

Winning local contracts and realization:

- `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-2],
  [SB-API-3], [SB-API-6], [SB-API-9], [SB-API-10], [SB-API-11]:
  exports, resolution, snapshot timing, watchers, errors, and plugin interfaces.
- `docs/specs/10-cli.md` [SB-CLI-1], [SB-CLI-2], [SB-CLI-3],
  [SB-CLI-4]: CLI parsing, preparse config failures, and output dialects.
- `docs/specs/product-section-registry.md`, `docs/README.md`:
  contract ownership; configuration remains in the API/CLI families.
- `docs/guides/configuration.md`, `docs/guides/python.md`:
  external settings, project-target precedence, and embedding recipes.
- `docs/implementation/06-process-session-core-ownership.md` and
  `docs/implementation/07-complexity-and-state-machine-map.md`:
  session keys, opaque extras, immutable receipts, and lazy resource ownership.
- `docs/specs/01-development-documentation-operating-model.md`
  [DOM-5], [DOM-10], [DOM-11], [DOM-15], [DOM-16].

Explicitly located downstream checkouts for this planning session are
`/Users/van/Developer/weft` and `/Users/van/Developer/taut`. They are evidence
locations, not package dependencies or assumptions for a future implementer.
No sibling checkout is required to execute this plan; reinspection, if useful, is read-only. Downstream paths in this
plan are labeled with `weft:` or `taut:` and relative to the identified checkout.

- `weft:docs/specifications/04-SimpleBroker_Integration.md` [SB-0.4],
  “Current Context API”; `weft:docs/specifications/14-Python_API_Surfaces.md`
  [PY-2]; `weft:docs/specifications/10-CLI_Interface.md` [CLI-1.1.2];
  `weft:docs/specifications/00-Quick_Reference.md`.
- `taut:docs/specs/02-taut-core.md` [TAUT-3.2], [TAUT-3.4];
  `taut:docs/specs/05-taut-mcp.md` [MCP-4], [MCP-8], [MCP-12];
  `taut:docs/specs/08-persistence-io.md`;
  `taut:docs/implementation/04-taut-architecture.md`.
- Downstream AGENTS/shared context and relevant lessons; Weft's
  runtime-and-context-patterns runbook; Taut's configuration/isolation sources.

Theory judgment: the local benefit is one configuration definition/resolution
engine and canonical internal access. It is not a net reduction in public concepts:
the additive builder and schema API, a second public snapshot type, and naming
views add concepts while compatibility is retained. Downstream duplication can
fall only after optional adoption, which is outside this plan. This is a deliberate
cost for reusable configuration and stable existing callers, assessed against
[THEORY-4], not proof of fewer concepts. Application policy remains outside core.

## Spec Baseline

This plan adds a builder and simplifies internals while preserving existing
configuration contracts, including the published prefixed API.

| Repository | Source revision | Additional content identifier |
|------------|-----------------|-------------------------------|
| SimpleBroker | `5e4bcb7f78b7e6f29721095b25da437fdad062d0` | API spec SHA-256 `2a0c8126294d3509d5e92cb849cab058bbe47e818b5d32d5a3b9cb617073fb71`; CLI spec `0ad4bad2488c15d61d803340e23d228519d74f869dbba245bcae2bf990124aa2` |
| Weft | `34ca4bad82b56e917f20f69b0bbb66a755806128` | Integration spec `98a95a6bfadfdacbc922e934cb33d913054618ffbfb96705c9093fb926b2a2a3`; inspected Python API spec `a88aea1278e1370c8c64db998423bd4d1bbfa7e5e5b6abfdafc72150ba2b9a7b`; inspected context source `35053542ca955517770f96af41b26407517f11916e4b05186fe8410d11d80e7e` |
| Taut | `6f5ae8896b30b1965286a9b0cd5cfd936214c015` | Core spec `c81b2683be9acc2b4a68a927c79281b932c76f52bf3f691147d1e35b26dd32a8`; MCP spec `3bf65915f3e076a3b2b09418b96689f70e547acd43d4838e1a27ef1bebd076fe` |

Content identifiers distinguish the inspected source from a revision alone.
Before local implementation, reconcile the SimpleBroker baseline with current
checkout changes. Downstream baselines record reference evidence only. Record
local promotion baselines as a commit SHA or base SHA plus reviewable spec diff.

## Current Structure and Key Files

Downstream rows below describe reference constraints and possible future adoption,
not files to edit or adoption tasks in this plan.

| Owner / existing files | Present behavior and local migration or downstream reference |
|------------------------|-----------------------------------------|
| `simplebroker/_constants.py` | `_CONFIG_FIELDS` owns 32 defaults/parsers; `load_config`, `resolve_config`, `resolve_isolated_config`, `snapshot_config`, `_overlay_config`, and `ResolvedConfig` own multiple resolution paths. Move configuration machinery to the new public `simplebroker/config.py`; retain ordinary constants and version data in `_constants.py`. The new module imports constants, never the reverse. |
| `simplebroker/__init__.py`, `simplebroker/ext.py`, `simplebroker/_exceptions.py` | Public exports and safe `InvalidConfigError` metadata. Keep error identity and redaction; introduce canonical field/source metadata and documented wrappers below. |
| `simplebroker/project.py`, `simplebroker/_project_config.py`, `simplebroker/_targets.py`, `simplebroker/cli.py`, `simplebroker/commands.py` | Target discovery and TOML are a different contract from generic setting precedence. Parser defaults currently come from a preparse snapshot. Migrate key reads and pass snapshots without changing the command grammar or target selection. |
| `simplebroker/sbqueue.py`, `simplebroker/db.py`, `simplebroker/_broker_session.py`, `simplebroker/_runner.py`, `simplebroker/watcher.py`, `simplebroker/_retry_policy.py` | Lower-layer key reads, watcher defaults, lazy acquisition, per-call overlays, and session identity. Preserve complete config identity including opaque extras while using canonical internal access. |
| `simplebroker/_backend_plugins.py`, `simplebroker/_backends/sqlite/plugin.py`; `extensions/simplebroker_pg/simplebroker_pg/plugin.py`, `runner.py`; `extensions/simplebroker_redis/simplebroker_redis/plugin.py`, `runner.py`, `pool.py` | Config-consuming plugin/runner boundaries and Redis timeout coupling; public backend API is v9 at baseline. Internal consumers may change, but existing plugin handshakes and config views remain compatible. |
| `weft:weft/_constants.py` | Full alias table, duplicated broker defaults, Weft parsers/override rules, and exact-output-key guard. Reference: motivates schema composition and product-specific input policy in one loader. |
| `weft:weft/context.py`, `weft:weft/helpers/__init__.py` | Context has separate `config`/`broker_config`; helper module caches configuration. Context currently overlays project autostart and adds an autostart directory. Reference: motivates one retained snapshot with source ownership preserved; paths remain context attributes. |
| `weft:weft/core/tasks/base.py`, `tasks/multiqueue_watcher.py`, `core/manager.py`, `core/pipelines.py`, `core/spawn_requests.py`, `core/queue_wait.py`, `core/manager_runtime.py`; `weft:weft/commands/interactive.py`, `init.py`, `load.py`, `run.py`, `submission.py` | Application/broker handoffs and ordinary mappings for spawn. Reference: motivates canonical values and transport without a second translated configuration. |
| `weft:weft/bootstrap.py`, `weft:weft/commands/serve.py` | `WEFT_ENV_FILE` is explicit process-env bootstrap; serve passes only supplied options. Do not fold bootstrap into the shared loader or lose omitted-option information. |
| `taut:taut/_constants.py`, `taut:taut/client/_base.py`, `client/__init__.py`, `client/_watching.py`, `watcher.py`, `_maintenance.py`, `persistence/_operations.py`, `debug.py` | Taut owns raw prefix inputs, `TAUT_DB` selection, error relabeling, and refreezing. Reference: motivates direct snapshot handoffs with unchanged source selection and lifetimes. |
| `taut:extensions/taut_mcp/taut_mcp/_workspace_reactor.py` | Each workspace attachment carries its selected target and snapshot. Explicit workspace selection suppresses ambient `TAUT_DB`; identity inheritance remains independently controllable. |

Paths shown as comma-separated filenames in a row share that row's directory.
Before edits, expand the consumer inventory with `rg -n 'BROKER_|WEFT_|TAUT_|ResolvedConfig|freeze_broker_config'`
over this repository's source, extensions, examples, tests, and docs. Classify each
hit as external spelling, internal lookup, exported Python name, domain data,
or historical reference; a global string replacement is forbidden. Add any
newly discovered config consumers to the slice file list before editing.

### Comprehension gate before implementation

The implementer must answer these in the execution log before source edits.
An incorrect answer requires rereading the cited owners before proceeding.

1. Why is replacing `BROKER_` with `WEFT_` insufficient? Expected: the
   isolation guarantee belongs to the validated snapshot; an ordinary mapping
   can cause a fresh ambient read, and defaults/units/ownership must also agree.
2. Can any snapshot containing a `cache_mb` key be trusted by the broker?
   Expected: no; the broker checks completeness and revalidates its owned fields
   with core-owned `CONFIG_DEFAULTS` validators, without trusting carried labels.
   App-only snapshots lacking broker fields fail at the broker boundary before target I/O.
3. May app fields be omitted from identity while existing plugins can read them?
   Expected: no. Preserve the current conservative complete-config identity,
   including extras. Canonical keys and public compatibility names identify the
   same values, not duplicate key material. Pool optimization is outside scope.
4. Does settings-file precedence replace project-target or identity precedence?
   Expected: no; existing broker-target TOML and Taut identity/presentation
   sources retain their own selection and lifetime rules.

## Invariants, Constraints, and Hidden Couplings

1. External environment spellings remain unchanged. Every internal schema,
   stored field and internal consumer lookup is unprefixed lowercase. Namespaced
   read aliases and existing public mapping/transport formats remain supported. Python symbol names remain conventional uppercase for ordinary
   constants. Rename private constants only where they exist solely to carry a
   prefixed config key; do not mass-rename queue names, protocol keys, or exports.
2. The loader never mutates `env`, `os.environ`, its schema, input mappings, or
   files. Shared-loader and SimpleBroker imports do not sample ambient configuration;
   existing Weft helper sampling/refresh lifetimes remain as documented. `env=None` means no
   environment, not an implicit `os.environ` read. App entry wrappers explicitly
   supply their environment at the existing ownership event.
3. Local example embedder snapshots include the inherited broker schema. SimpleBroker accepts
   them directly, reads only its owned fields, and does not read ambient state,
   strip prefixes, drop validated app fields, or construct a second broker config.
4. Core canonical validators cannot be weakened by an app. Apps can change
   defaults, descriptions and external-input parsers while preserving canonical
   types, units, and validation. Extending a schema with an existing field name
   is an error; intentional inherited-default/input-policy overrides use an
   explicit derive operation. No silent last-writer-wins schema merge.
5. Keep existing external value grammars in this change, including the unusual
   broker debug/strict-one booleans and vacuum percent input. Expose canonical
   units in descriptions; do not repeat source-dependent percentage coercion
   when copying an already normalized snapshot. Grammar unification is separate
   work, not an accidental consequence of key renaming.
6. Preserve default storage separation: `.broker.db`/`.broker.toml`,
   `.weft/broker.db`/`.weft/broker.toml`, `.taut.db`/`.taut.toml`. Explicit same
   targets may still share storage. This is not a database/schema migration.
7. Preserve snapshot lifetimes at queue, watcher, lazy runner, command, generator,
   and MCP workspace boundaries. Operation overlays derive a new snapshot from
   retained values without rereading env. Spawn transfers canonical values and
   reconstructs a snapshot under the child's application schema without env.
8. Preserve complete configuration identity, including opaque extension values
   and composed app fields exposed to plugins. No subset-keyed pooling change is
   introduced. Plugin-visible values and keyed values agree. Public compatibility
   views and internal canonical access share one backing store; neither re-resolves
   configuration. Existing target/backend-option identity semantics stay intact.
9. `WEFT_ENV_FILE` remains an explicit bootstrap exception outside the loader:
   it fills missing process values before CLI import and may contain other
   prefixes. Taut's identity inputs, per-failure debug controls, selected-project
   reactions, and CWD presentation policy retain their own owners and lifetimes.
10. Preserve documented value precedence **and validation order**. SimpleBroker
    and Weft validate their ambient base before ordinary overrides; Taut selects
    its explicit override over the raw ambient value before normalization.
    Schema-declared input policy must express these differences through the same
    loader. Bad selected settings fail before broker I/O. File syntax/read failures and
    invalid schemas are fatal; a malformed selected TOML document is not repaired
    by an option. Lower-priority *field values* can be shadowed before parsing.
    Base-validation policies below may still reject a shadowed ambient value.
    Warnings that remain contractual cannot expose secrets or replace a primary
    error. CLI exception translation remains owned by each CLI.

## Simplification boundary

Reuse existing normalization, validation, lifetime and transport machinery where
possible. Add only what is needed for schema extension, prefix selection and one
canonical backing configuration. Do not use the refactor to tighten input policy,
restrict subclasses/plugins, redesign pooling or replace proven behavior. Prefer
a small adapter over a parallel implementation. Tests prove old behavior survives
and the new builder supports app-defined fields; they do not justify new policy.

## Proposed Design and API

All new filenames/API names in this section are proposed additions, not claims
that they already exist. No third-party dependency or separate distribution is
needed: the three products already depend on SimpleBroker.

Public owner: **new** `simplebroker/config.py`, with root re-exports. It contains
`ConfigField`, `ConfigSchema`, `ConfigSnapshot`, `CONFIG_DEFAULTS`, and
`build_config`. `CONFIG_DEFAULTS` is the immutable broker schema, not a mutable
global dictionary. Field metadata includes canonical default, description with
units, external parser, canonical validator and existing sensitivity metadata. A schema also declares its ordered sources and whether its ambient
base is validated before options, preserving the existing consumer contract.
Dependency-aware defaults name their dependencies; schema construction
rejects cycles and missing dependencies. This is a bounded DAG for demonstrated
defaults, not an expression language or executable TOML. An independent schema
may contain only application fields: the generic builder has no mandatory broker
fields. Broker completeness is required only when handing a snapshot to a broker.

```python
build_config(
    prefix: str,
    env: Mapping[str, str] | None = None,
    config_file: PathLike[str] | Mapping[str, object] | None = None,
    options: Mapping[str, object] | None = None,
    *,
    defaults: ConfigSchema = CONFIG_DEFAULTS,
) -> ConfigSnapshot
```

- Prefix is a nonempty uppercase identifier without a trailing underscore.
  External key spelling is exactly `prefix + '_' + field.upper()`.
- `env` is supplied explicitly. Select exact matching prefixed fields declared
  by the schema; ignore other prefixes and unrecognized environment names as
  existing broker behavior does. Malformed recognized fields retain existing
  validation behavior. Embedders add fields by extending the schema, not by
  modifying a broker allowlist. Selector names owned elsewhere, such as TAUT_DB
  or WEFT_CONTEXT, remain for the owning adapter; no env mutation occurs.
- `config_file` is optional, explicit, read-only, and never auto-discovered by
  this function. A path is read once with `tomllib`; a mapping represents the
  same parsed root table. Select root keys with exactly `prefix + '_'`, just as
  for env: `BROKER_CACHE_MB`, `WEFT_CACHE_MB`, or an embedder-declared
  `WEFT_RETENTION_DAYS`. Strip the selected prefix once into canonical fields
  and validate against the supplied schema. There is no broker-only TOML key
  allowlist; newly declared app fields work through TOML and env alike.
  Unprefixed keys and other prefixes are ignored by the builder. Existing project
  keys/tables and namespaced settings may coexist in the same document; this API
  does not require a separate settings file. Existing target parsers retain their
  ownership and precedence. Only selected names are checked against the schema;
  undeclared TOML settings are ignored, just like undeclared env names. TOML tables may be values of declared structured
  fields. No recursive search for matching names in unrelated tables, shell
  expansion, includes, or environment interpolation.
- `options` is a sparse mapping of canonical unprefixed names and typed values
  from a CLI/API adapter, not raw argv. Defaults emitted by argparse/Typer must
  not be mistaken for supplied options. `False`, zero, empty string, and an
  explicitly allowed `None` are values; absence means no override.
- Source order and validation timing are declared by the composed schema and
  its existing source adapter, according to the precedence register below.
  Do not hardcode a universal env-over-file merge. For the new explicit generic
  settings-file API only, use options > file > env > defaults, consistent with
  project-authoritative values; this does not add file input to existing tuning
  fields automatically. SimpleBroker/Weft retain ambient-base validation before
  options; Taut retains raw selection before normalization. All policies use the
  same source selection/parser/validator engine, not separate app loaders. Canonical defaults and options are validated, never re-parsed as
  environment strings. Settings-file parsers accept TOML-native input with the
  same external units as env. Missing-only derived defaults run in dependency
  order after their inputs are resolved; explicitly supplied values win.
- `ConfigSnapshot` is a read-only Mapping of all composed canonical fields.
  Namespaced lookup for the builder's selected prefix aliases the canonical field:
  a WEFT snapshot accepts both `cache_mb` and `WEFT_CACHE_MB`, including get and
  membership, without another stored value. New snapshot iteration/to_values use
  canonical names once. Legacy public views retain existing prefixed iteration
  and serialization. Broker compatibility views expose existing BROKER names for
  broker fields even when the original snapshot was built under another prefix.
  Aliases never widen which environment/TOML prefix is read. Resolve exact stored
  fields before aliases so a legitimate extra key is never shadowed or rewritten.
  Store schema compatibility and source *labels*, not raw source dictionaries.
  Safe schema/source inspection and exceptions redact sensitive values. Supported
  container values are defensively copied and recursively frozen (mapping,
  sequence, set); primitive/TOML immutable scalar types retain their meaning.
  Application validators normalize domain-specific input into these supported
  value types. Runtime handles/callables are not config values.
- `snapshot.with_options(mapping)` validates an ambient-free canonical overlay
  under its same schema and returns a snapshot. Recompute only derived defaults
  whose inputs changed and whose values still have default/derived provenance;
  never overwrite explicit/file/env values. This is also how Weft applies
  project autostart at its existing precedence point.
- `snapshot.to_values()` produces a detached, transportable canonical mapping;
  `ConfigSnapshot.from_values(values, schema=...)` validates a *complete*
  canonical mapping without defaults or env. It rejects missing/unknown keys.
  No validators, arbitrary code, or schema objects are deserialized from a
  transport payload; the receiving application supplies its own schema.
  Reconstructed values count as explicit for subsequent overlays.
- New composed snapshots are accepted directly and their broker fields checked
  against core-owned validators before target I/O, without trusting schema labels
  or resolving env again. Existing ResolvedConfig constructor, exact-instance
  fast path and subclass revalidation remain unchanged. New ConfigSnapshot
  subclasses are supported too: validate their exposed values rather than rejecting
  them or trusting inherited type/metadata alone. The exact-instance no-copy
  guarantee is not a requirement to trust overridden subclass access. Legacy opaque extras keep
  their documented shallow preservation; recursive freezing applies to new
  schema-declared fields, not retroactively to arbitrary legacy objects.
- Legacy public wrappers and plugin handoffs expose their existing prefixed keys,
  iteration, get/membership, copying and opaque extras. Internal code uses canonical
  access backed by those same values. Implement a boundary view/access adapter,
  not a second loader or separately built config. Preserve exported types, return
  types, ordinary dict mutability where currently promised, and supported mapping
  operations. Do not add deprecation warnings. New build_config results expose
  canonical keys and are passed unchanged into Queue/watcher handles.

### Snapshot types and recognition

Pin two public receipt classes, ConfigSnapshot and the existing ResolvedConfig,
as sibling facades over one private `_ConfigReceipt` base and canonical backing
machinery. They are not aliases and neither facade stores a second resolved
configuration. ConfigSnapshot owns the new schema/canonical-view semantics;
ResolvedConfig preserves its existing constructor, prefixed view, exact-instance
identity and shallow opaque-extra behavior. Name views: canonical plus the
selected-prefix read aliases on new snapshots, and the existing BROKER public
view at legacy/plugin boundaries. No per-product snapshot class or loader exists.

Preserve old fast-path *behavior*, not the old type tests verbatim. Update all
five current type branches in `_constants.py`: resolve_config's exact/subclass
branches, snapshot_config's exact branch, and _overlay_config's exact/subclass
branches. Dispatch exact ConfigSnapshot and exact ResolvedConfig before ordinary
Mapping handling. Return exact complete receipts unchanged; revalidate subclasses
from their exposed values and carried schema without ambient reads. A private
base alone does not confer the exact-instance fast path. Add overloads for new
inputs while preserving existing overloads/return behavior for old inputs.

When overlaying a canonical snapshot, retain its schema, app fields and source
policy. Ordinary legacy mappings retain their existing ambient/opaque behavior.
No new snapshot or subclass may reach load_config merely because its concrete
type is unfamiliar. Queue, watcher, DBConnection and
`_project_config._config_snapshot` must recognize the new receipt path; internal
canonical access and old public views read its same backing values. Test each
seam and overlay independently with conflicting and malformed ambient BROKER
values after snapshot creation. Observe retained cache/app values and actual
broker consumption, plus exact receipt identity where applicable.

Example shape (proposed API, not executable before implementation):

```python
WEFT_DEFAULTS = CONFIG_DEFAULTS.derive(
    defaults={"project_scope": True},
    fields={"directory_name": ConfigField(default=".weft", ...)},
    # Missing-only database/config-path defaults depend on directory_name.
)
config = build_config("WEFT", env=environment, options=options,
                      defaults=WEFT_DEFAULTS)
queue = Queue("tasks", db_path=target, config=config)
cache_mb = config["cache_mb"]
```

### File activation and source ownership

`config_file=` is explicit activation of namespaced file settings. Merely placing
BROKER_CACHE_MB in an automatically discovered .broker.toml does not activate the
new builder for the existing CLI. Document that distinction beside every mixed-file
example. The same document can serve both consumers, but callers must explicitly
pass it to build_config to read namespaced settings. Test both paths: existing
project loading resolves only its target/options with unchanged behavior; an
explicit builder call consumes the namespaced values. Neither automatic tuning
activation nor warnings on ignored project keys are added here. Those would
change external behavior/precedence and require a separately authorized design.

### Source adapters and scope limits

Existing project TOML (`version`, `backend`, `target`, `backend_options`),
Weft `config.json` autostart metadata, and Taut `[reactions]`/`[terminal_text]`
remain separately owned inputs. The builder ignores those unprefixed keys and
selects only namespaced settings from a supplied TOML document, even when both
share that document. Retain existing target parsers and precedence. This plan adds the
optional explicit settings-file capability to the shared API, not a new CLI
flag, automatic config-file discovery, or a rewrite of existing user files.

Weft derives `default_db_name` and `project_config_path` from `directory_name`,
`log_tasks_external_enabled` from `task_monitor_mode`, and the default reserved
cleanup age from retention. These motivate local synthetic schema dependency tests;
this plan does not implement a Weft schema.
`autostart_dir` remains on context; project autostart overrides env/default only
where the existing context owner does so, and the explicit argument wins.

Taut's `TAUT_DB` selects the location/name pair; explicit workspace/CLI selection
overrides that pair. This motivates a local example adapter before
calling the shared resolver; actual Taut adapter changes are out of scope; do not generalize it into every schema. Identity
and dynamic debug variables are separately owned external inputs, not frozen client
broker fields. Adding other Taut fields is allowed only at their documented
owner/lifetime, not simply because they start with `TAUT_`.

The shared loader accepts parsed options. Existing SimpleBroker CLI retains its
preparse config validation and invalid-config-before-help/version rule; that
is an explicitly preserved CLI boundary, not a second configuration algorithm.
Its argument parser reads the initial snapshot for defaults and overlays only
explicit settings without another ambient read. The direct builder uses its
schema's declared validation order as well; it is not a bypass for that policy. Do not introduce generic
flags for all schema fields; all values remain available through env/API/TOML.

## Documented Precedence Register (preserve)

These are separate source domains. Preserve local broker behavior and support
the generic source-order mechanisms needed by embedders. Downstream rows and
their tests are reference evidence only, not product behavior to implement here;
sharing mechanics is not permission to replace their policy.

| Domain / owner | Highest-to-lowest order or failure rule | Firing test owner |
|----------------|----------------------------------------|-------------------|
| Broker configuration values: [SB-API-2], configuration guide | Valid explicit ordinary overrides > current BROKER env > defaults; **validate ambient base before applying ordinary overrides**. Isolated snapshots do not consult ambient env. | `tests/test_invalid_config_lifecycle.py`, `tests/test_isolated_config.py` |
| Broker target: configuration guide “Precedence rules”; project/CLI owners | Explicit non-init CLI SQLite selection; selected-directory project file; upward project file when scoped; legacy project SQLite discovery when scoped; env-selected non-SQLite backend; SQLite defaults. Project target fields remain authoritative; env may supplement secrets. Explicit `-d` wins over default location wherever applicable. | `tests/test_project_config.py`, `tests/test_cli_main.py`, `tests/test_connection_config.py` |
| Weft compile: [SB-0.4], `_constants.py::compile_config` | Ambient WEFT base validated first; explicit values then win. At the legacy Python boundary, explicit BROKER aliases beat explicit WEFT aliases for the same broker field; external BROKER env never supplies Weft values. Debug/logging retain their documented Weft policy. | `weft:tests/system/test_constants.py` |
| Weft target: [SB-0.4], [CLI-5] | Explicit context selects root; Weft-scoped project TOML wins over env backend synthesis, which wins over SQLite fallback. Automatic discovery searches the configured Weft file only, then falls back to explicit-root resolution at CWD. | `weft:tests/context/test_context.py`, `weft:tests/context/test_context_sqlite_only.py` |
| Weft bootstrap: [CLI-5] | Existing process env > WEFT_ENV_FILE; task `--env`/TaskSpec env are not bootstrap or target inputs. | `weft:tests/cli/test_env_file_bootstrap.py` |
| Weft autostart: context owner and CLI init contract | Explicit argument > project config.json > env/default. Do not apply the generic settings-file order to this field at context resolution. | `weft:tests/context/test_context.py` |
| Taut compile: [TAUT-3.2] | Explicit typed options > TAUT_DB-derived location/name > matching TAUT env > defaults; explicit location/name suppress the alias. Select raw values before validation. | `taut:tests/test_constants.py` |
| Taut target: [TAUT-3.2] | Explicit --db/db_path/TAUT_DB path selection > authoritative selected project file > no-project env backend selection/default SQLite. A selected file is not merged with unrelated project files. | `taut:tests/test_project_config.py`, `taut:tests/test_client.py` |
| Taut reactions/presentation/debug: [TAUT-3.2], [TAUT-6.4], [TAUT-13] | Reactions from selected storage project with packaged fallback; presentation from CWD; debug action sampled per failure. These are not interchangeable global file/env sources. | `taut:tests/test_project_config.py`, `taut:tests/test_debug_capture.py`, MCP reactor suites |

External selectors and legacy Python alias precedence remain source-adapter
responsibilities. At the compatibility boundary only, collapse accepted old
prefixed Python keys to canonical names in their documented order; after that
there is one canonical snapshot. Preserve existing override precedence and opaque
keys at old entry points; do not add new ambiguity errors there. New canonical
options are specified separately from read aliases. Do not silently reinterpret
which spelling wins in an existing call. Existing public aliases are not deprecated or removed by this change.
This boundary adapter is not a second config engine or a broker-only dictionary.

New generic TOML settings are an additive API surface with the explicit order
stated above. They do not supersede any existing source domain in this table.
If implementation discovers an unlisted documented precedence, add it and its
firing test before editing that owner; request a scope revision before changing
its behavior.

## Proposed Spec Delta

Promotion strategy **A** for the following in-file edits: apply exact normative
text before code; add changed implementation-link claims with the corresponding
code slice. Do not reclassify whole existing specs. The text below replaces the
named configuration paragraphs, not unrelated requirements in those sections.
Only the SimpleBroker normative sections below are promoted by this plan.
Downstream source contracts remain unchanged and serve as design references.

### SimpleBroker [SB-API-1/2] — additive configuration API

Retain the existing normative paragraphs for all existing exports and behavior.
Add the following text; update implementation links without deleting compatibility
promises or rewriting existing helper signatures:

> `simplebroker.config` adds ConfigField, ConfigSchema, ConfigSnapshot,
> CONFIG_DEFAULTS and build_config, also exported at package root. The immutable
> default schema declares canonical unprefixed fields, descriptions, defaults,
> parsers and validators. Embedders may derive defaults/input policies and add
> their own fields without changing SimpleBroker. Broker canonical validation
> cannot be weakened. Independent app-only schemas also work with the builder.
>
> `build_config(prefix, env=None, config_file=None, options=None, *,
> defaults=CONFIG_DEFAULTS)` returns a complete canonical ConfigSnapshot.
> Canonical lookup and selected-prefix lookup name the same stored field:
> snapshot['cache_mb'] and snapshot['WEFT_CACHE_MB'] agree for a WEFT build.
> get and membership follow the same alias rule; canonical iteration/transport
> emits each new-snapshot field once. Existing public views retain their current
> spellings and serialization. Read aliases do not expand source-prefix selection.
> Explicit env and TOML root fields use PREFIX_FIELD names. Other prefixes and
> unprefixed external names are ignored. Unknown environment names retain the
> broker's ignore behavior; undeclared TOML names are likewise ignored. New file/options inputs validate selected fields
> under the supplied schema. TOML can contain both existing project-target fields
> and namespaced settings; the loader ignores unprefixed target fields and leaves
> their existing parser and precedence unchanged. No separate settings file,
> discovery, includes or interpolation is required or introduced.
>
> Existing precedence and validation timing remain. The shared engine supports
> declared source policies without moving app target/alias/lifetime logic into
> core. For new explicit file input, options > file > env > defaults. Existing
> public entry points acquire no new file lookup or precedence change. Putting a
> namespaced tuning key in discovered project TOML alone does not activate it;
> callers explicitly pass config_file to use the additive settings path.
>
> New snapshots recursively freeze declared values, retain safe source labels,
> and support ambient-free with_options overlays. Derived defaults recompute only
> while their provenance remains default/derived; supplied values remain supplied.
> to_values exports detached canonical transport data; from_values requires a
> complete schema-validated payload without reading env or filling defaults.
>
> Queue/watcher consumers accept a composed snapshot directly, retain its app
> fields and use core-owned validation for broker fields. One backing config
> supplies canonical internal access and legacy public compatibility views.
> Existing resolver exports, signatures, return types, prefixed lookup/iteration,
> opaque extras, preserve_unknown behavior, ResolvedConfig construction and
> subclass revalidation retain their contracts. New ConfigSnapshot subclasses
> are also accepted through value revalidation, not rejected for subclassing.
> No warning or deprecation is added.

### SimpleBroker [SB-API-3/6/9/10/11] — preserve lifecycle and plugins

> Existing snapshot events, lazy acquisition, error types/metadata and plugin API
> version remain. Internal configuration field names are unprefixed. Existing
> public/plugin config views retain their current key spelling and extension
> access. Complete configuration identity, including opaque extras, remains the
> conservative pooling rule; namespacing adapters do not duplicate identity fields.
> New composed fields visible through plugin config also participate in identity.
> No plugin config_schema requirement, field-access restriction or new handshake
> is introduced. The same retained values back all config views.

### SimpleBroker [SB-CLI-2/4] — retain timing, clarify shared implementation

> The CLI resolves its broker environment snapshot before parser-dependent
> behavior as specified above; invalid selected configuration still produces
> the plain preparse exit-1 error, including help, version, and raw --json.
> Parsed explicit settings overlay that retained snapshot without another
> environment read. This preserved CLI failure ordering is distinct from a
> schema's value-precedence rule and is preserved for direct broker resolution
> as well. Command syntax,
> target-selection precedence, and the post-parse JSON error contract are
> unchanged. Internal field names are canonical; env diagnostics retain the
> external `BROKER_*` spelling.

### Downstream reference implications (not spec changes)

Weft's dual carriers, derived defaults, helper refresh and process transport,
and Taut's alias, client and MCP snapshot lifetimes motivate the shared API.
Local example embedders must demonstrate schema composition, one-snapshot
handoffs and transport without implementing either product's context, manager,
client, MCP integration or source-selection policy. Their existing specs remain
reference evidence; no downstream spec delta is part of this plan.

## Compatibility and Release Scope

No major-version or backend API change is part of this plan. Version selection
follows the repository's normal process for an additive API and internal refactor;
this planning task does not set a version or authorize publication.

Preserve existing public config lookup and iteration (including prefixed keys),
return types and mutability where promised, helper signatures and imports,
unknown-env handling, error metadata, constructor/default filling, subclass
revalidation, opaque extra preservation, preserve_unknown and complete session
identity. Keep plugin API v9 and its current config access, factories and handshake.
No downstream edits, upgrades, bounds changes or restart requirements are imposed.

The alternative of dropping legacy views to enforce canonical-only public reads
is rejected by the user's unchanged-external-contract requirement. A compatibility
view is a naming boundary over one set of values, not a duplicated configuration
engine. Preserve whole Mapping behavior, not just __getitem__; iteration and
serialization are part of compatibility too. Do not emit deprecation warnings.

ResolvedConfig and ConfigSnapshot are sibling facades over the private shared
receipt base, as specified above. Preserve the existing identity and constructor
contract through canonical backing machinery. Opaque extras retain their old shallow semantics;
they are not forcibly converted to new schema fields. New embedders use declared
fields and the canonical snapshot directly. Existing dict-returning helpers may
materialize their documented output at the public boundary; internal broker/app
handoffs do not maintain a second mirrored configuration.

Namespaced access and legacy views are supported architecture, not scheduled
for removal. No exit date or deprecation is implied. Only a separately authorized
public-contract change by the repository owner could retire them; this plan does
not create such follow-up work.

Rollback is ordinary package rollback with unchanged storage and config formats.
Check isolated candidate core/extension installs and a local legacy-plugin fixture;
publication and operational changes remain outside this task.

## Tasks

Implementation and closure are authorized by the user. Each task is a
coherent review unit. Verification and independent review evidence are below;
this plan closes in the implementation commit.

1. [x] **Rebase local evidence and inventory fields.**
   Read the sources and answer the comprehension questions. Inventory each local
   field's external spelling, canonical name, type/unit, parser, validator,
   default/derivation, sensitivity, lifetime, identity participation and consumption
   test. Classify local config consumers before edits. Use downstream alias/selector
   evidence (including TAUT_DB and WEFT_CONTEXT) to design local example schemas
   and ignored-selector tests, not to change downstream code. Done: no unowned local
   field, collision, or unaccounted local consumer; no sibling edits required.

2. [x] **Spec-promotion slice.**
   Apply the exact delta above using strategy A in the named SimpleBroker API/CLI sections; retain existing mapping/opaque-key promises and add the new API alongside them. Align additive API signatures and
   public inventories and verification rows. Add local plan backlinks and record
   promotion baselines. Update `docs/specs/product-section-registry.md` only if
   concern references change; no new competing config spec is needed. Run this
   repository's documentation gates. Stop on unresolved normative contradictions.

3. [x] **Build and exercise the shared loader before broad renaming.**
   Add `simplebroker/config.py`, migrate configuration definitions from
   `_constants.py`, and update `_exceptions.py`, root/ext exports and public
   surface tests. Implement the five receipt-recognition branches and minimum
   Queue/watcher/DBConnection/project adapter seams before broad key renaming.
   Run the new composed-snapshot handoff/poisoned-env proof from task 5 here as
   an early integration gate. Add **new** `tests/test_config_builder.py` for source matrix,
   immutable schema derivation, canonical copy semantics, derived defaults,
   source metadata, ignored unrelated names, and safe errors. Use actual TOML files,
   not mocked parser returns. Update `tests/test_isolated_config.py`,
   `tests/test_invalid_config_lifecycle.py`, and
   `tests/test_python_library_api_contract_sb_api.py` to the promoted contract.
   Stop if import requires reading env, multiple normalizer engines appear,
   or a derived broker field bypasses canonical validation. Done: targeted
   resolver/public tests pass and independent review accepts the new core.

4. [x] **Migrate SimpleBroker consumers and both backend extensions.**
   This rename is an explicit user requirement, not a prerequisite for legacy
   reads to work. Its benefit is keeping namespace interpretation at the boundary,
   so internal maintenance uses one vocabulary instead of depending permanently
   on compatibility lookups. Its cost is broad source churn; start only after
   task 3's additive builder and real-consumer seam tests pass, then review the
   coherent rename separately. Preserve public exports and observable behavior.
   Update all classified internal config reads in the files listed above, plus
   the inventory's additional consumers. Update `_session_spec` and recursive
   key ownership without making a broker-only config. Use compatible schema
   checks at public boundaries, retain operation overlay timing, and derive
   `PollingStrategy` defaults from the shared schema without env reads.
   Keep backend API v9. Inventory all public and plugin config handoffs and
   preserve their existing views while internal consumers use canonical access.
   Retain opaque-extra/non-repr cases in tests/test_process_broker_session.py
   (existing lines 534–780) as regression tests; add new composed-field identity
   cases. Test a local unchanged v9 plugin fixture with prefixed keys and opaque
   extras. Test actual SQLite/PG/Valkey behavior, credentials/options, waiter
   identity, cleanup and lazy acquisition. Stop on any old contract test failure;
   do not rewrite an assertion merely to bless a public change.
   Done: old API/plugin contract suites and new embedding tests pass together.

5. [x] **Prove embedding capabilities with local fixtures.**
   Add new `tests/test_config_coexistence.py` using independent BROKER/WEFT/TAUT
   prefixes and small synthetic application schemas. Prove app-defined fields,
   default overrides and dependencies, both documented validation-order policies,
   ignored selectors, exact snapshot handoff to real Queue/watcher consumers,
   and actual spawn transport under hostile child env. These fixtures exercise
   the public API; they do not copy downstream loaders or implement app behavior.
   Done: local embedding and transport tests pass without sibling imports or edits;
   independent review accepts the public extension contract.

6. [x] **Local documents and release preparation.**
   Remove obsolete internal-prefix lookups and handwritten broker-default copies
   in this repo. Update local guides, README, CHANGELOG, examples, implementation
   docs 06/07, repository map, and core/extension manifests as needed. Document
   new opt-in embedder usage as reference guidance; no downstream migration is required. Run local full
   gates and independent review; evaluate heavily used skills/runbooks. Prepare
   local package compatibility and rollback notes. Publishing is separate work.
   Close this plan/index only when its local implementation scope is complete.

## Testing Plan and Acceptance Matrix

All required tests and fixtures live in this repo and import only its packages.
Downstream test paths in the precedence register identify reference evidence,
not commands or gates to execute. Product-specific target/bootstrap/lifetime rows
remain outside implementation scope; local examples test the generic extension
mechanisms they motivate.

Every schema field requires a consumer-level firing assertion, not only
`snapshot[key] == default`. Reuse existing tests; parameterize the schema
inventory to catch unbound new fields. Do not duplicate the full defaults in
test literals. Use representative nondefault values and actual consumption
(PRAGMA, polling schedule, backend target, message size, policy selection).

| Contract element | Required proof / owner |
|------------------|------------------------|
| Exact prefix selection | Distinct 11/22/33 cache values across three local prefix/schema instances in one process; malformed other-prefix values ignored; similarly named near-prefix not accepted; env unchanged byte-for-byte. |
| Precedence | Fire the local broker rows and generic extension policies motivated by the reference register: valid conflicts and invalid lower-priority values, omitted vs False/0/empty/None, alias pairs, local target discovery, synthetic source-policy priority, malformed/unreadable TOML, and ignored undeclared names. Broker and a synthetic base-first schema reject invalid ambient input before options; a synthetic selection-first schema permits override repair. |
| Units and parser separation | Env/file percentages normalize once; canonical snapshot/transport/overlay never rescales; per-product boolean grammars remain; canonical invalid types/ranges fail. |
| Embedder-defined values (mandatory) | A test embedder adds `retention_days` with default 7 and a nonnegative-integer validator. Exercise default/env/file/options, reject invalid input, preserve the value and schema in the exact snapshot passed to a real Queue/watcher, and preserve it across spawn. SimpleBroker must neither reject nor interpret this application-only field; no core source edit or plugin registration may be needed. |
| TOML namespace and extension | Use a real TOML file containing existing version/backend/target/backend_options entries, BROKER_CACHE_MB, WEFT_CACHE_MB and a declared WEFT_RETENTION_DAYS. Each prefix reads only its settings; unprefixed retention_days does not supply the field; malformed other-prefix values are ignored; undeclared same-prefix TOML keys are ignored; an invalid selected app field fails its validator. Existing project parser still resolves the original target/options unchanged. No separate app TOML registration or core source change is needed. |
| Lookup aliases | Canonical and selected-prefix lookup/get/membership return the same field with no duplicate storage. Canonical iteration/to_values emit one key; old public views retain prefixed iteration/dict behavior without warnings. BROKER compatibility view works on a WEFT-built snapshot; exact opaque keys retain precedence over aliases; no alias admits other-prefix env values. |
| Schema composition | Shared defaults unchanged after app derivation; add valid app field; reject collisions/cycles/missing dependencies/weakened broker validator; accept new inherited field without handwritten alias edits. |
| Receipt recognition | Exercise all five resolver/snapshot/overlay type branches with exact and subclass forms of both receipt types, then Queue/watcher/DBConnection/project seams. Poison ambient env after building a WEFT snapshot; retain its cache/app values without load_config fallthrough. Verify actual SQLite settings and exact identity where promised, not only Mapping equality. |
| Snapshot trust | Pure app schema builds; broker consumption rejects missing/invalid core fields without target I/O. Exact existing ResolvedConfig fast path and subclass revalidation retain behavior. Valid ConfigSnapshot subclasses work; an overriding subclass exposing an invalid core value fails validation before target I/O, not merely for its type. New composed snapshots retain identity; complete transport is validated without env/default recovery. |
| One snapshot | New composed object is retained at app-to-Queue/watcher boundaries. Legacy naming views and canonical access share backing values; no separately resolved broker/app mirror. |
| Session identity | Preserve existing complete-config identity and opaque extra/non-repr regressions. Changing any plugin-visible extra, including composed app fields, isolates as current semantics require; public aliases do not double-count fields. |
| Public/plugin compatibility | An unchanged v9 plugin fixture reads prefixed fields and opaque extras with existing lookup/get/membership/iteration/copy behavior. All visible config fields participate in identity. Old factories, signatures and handshake still work. |
| Provenance transitions | default→explicit and derived→explicit remain explicit after dependencies change, including a supplied value equal to the old default; derived→derived recomputes in dependency order when inputs change; unchanged inputs leave derivations stable; explicit/env/file values are never overwritten. Fire chained dependencies, repeated overlays, invalid derived results and complete transport reconstruction (restored values are explicit). Original snapshots remain unchanged. |
| Unknown env compatibility | BROKER_VACUUM_LOCK_TIMEOUT and BROKER_TYPO_XYZ remain ignored for normal CLI/help/version; malformed recognized keys still fail as before. Unrelated prefixes do not affect resolution. |
| Spawn | Real spawned child retains both broker and app canonical values after changed/malformed child env; validates a complete payload; no schema/validator deserialization from payload. |
| Target scope | Synthetic schemas use distinct default storage paths; SimpleBroker project discovery remains unchanged and explicit same targets still share. Derived-default and ignored-selector tests exercise the public extension API, not downstream context/workspace implementations. |
| Errors | Every `source` enum and field error path fires; selected external key + canonical field accurate; hostile repr/control chars/long strings safe; secrets redacted for env/file/options/schema defaults and snapshot displays. |
| CLI | Installed entry points retain help/version invalid-env failure order, exit classes, option placement and sticky JSON; no traceback, no target writes on config errors; explicit valid/invalid options tested. |
| Backend compatibility | Existing v9 plugin handshake, PG pool and Redis timeout behavior pass without a version bump or downstream migration. |

Anti-mocking: never mock the shared resolver, TOML parser, prefix selection,
snapshot handoff, queue storage, or process spawn in the principal proof.
Fault-inject only file access failures/hostile values and narrow module-owned
resource seams. Use existing CI timeout helpers for process liveness. Prove
configuration consumption before opening production resources; use temp paths
and managed backend fixtures. No tests may contact user-selected production DSNs.

## Verification and Gates

### Planning-only verification (this request)

Run in SimpleBroker, inspect every result, and record outcomes in the execution log:

```bash
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

Existence-check all existing named paths/sections/commands; proposed additions
are explicitly labeled new. Independent plan/delta review is required. Runtime
implementation tests are not substitutes for these planning gates and are not
claimed as evidence that the proposed loader exists.

### Implementation commands and success criteria

SimpleBroker targeted configuration and lifecycle gates:

```bash
uv run pytest tests/test_config_builder.py tests/test_isolated_config.py tests/test_invalid_config_lifecycle.py tests/test_connection_config.py tests/test_project_config.py tests/test_process_broker_session.py tests/test_python_library_api_contract_sb_api.py tests/test_public_surface.py
uv run pytest tests/test_cli_main.py tests/test_cli_contract_sb_cli.py tests/test_cli_rearrange_args.py tests/test_watcher.py tests/test_watcher_transition_tables.py tests/test_backend_plugin_resolution.py
uv run ./bin/pytest-pg
uv run ./bin/pytest-redis
uv run pytest
uv run ruff check .
uv run mypy simplebroker bin/release.py
```

The first command includes the proposed new test file and applies after task 3.
Also run the core-test mypy, extension typing, and packaging checks documented
in `CONTRIBUTING.md` and `bin/release.py` for the selected release set; use the
current enumerators rather than freeze a stale list here. Root suite + managed
PG/Redis commands must all pass before runtime completion.

Final candidate-package proof uses isolated installs of this repo's core and
extension wheels with local synthetic embedder fixtures; record artifact versions,
interpreter and imported `__file__`. No downstream suite, lockfile, environment,
release or runtime operation is required by this plan. Post-release checks, if
separately authorized, use disposable local targets to confirm CLI operation,
prefix isolation, snapshot transport and existing complete-config pool identity.

### Required plan-quality checklist (no new product hardening)

The repository runbook applies because the additive API crosses a compatibility
boundary and snapshot transport spans processes. Checklist coverage: (1–2)
invariants and named receipt gates above; (3) existing wrappers separate ambient
entry from shared resolution; (4–5) task stop gates and explicit exclusions;
(6–7) anti-mocking and old/new contract tests; (8) existing failure behavior
preserved; (9) local installed-package observations; (10) baseline/source map;
(11–12) no coordinated rollout, ordinary package rollback; (13) publishing the
new API is a later separately authorized step; (14) no new async/temp-file
lifecycle, only existing transport; (15) comprehension gate before source edits.
No downstream rc or branch work is required. Local composed-embedder and legacy
plugin fixtures provide the in-scope integration evidence; real downstream trial
is optional read-only evidence, not a release/adoption dependency.

## Independent Review Loop

Prefer Claude under the repository's read-only call-agent invocation; use a
bounded 540-second attempt and retain stdout/stderr. If unavailable, record
bounded attempts before using a separately tasked Codex reviewer. Reviewers
must not be the agents that supplied the downstream inventory.

Brief: review this plan and exact proposed delta against the listed source
baselines. Existence-check first. Accepted scope is external prefixes plus one
composed unprefixed snapshot, not a translated broker mirror. Pre-existing
concerns are observations unless this change worsens them. Prefer removing
unnecessary work. Report findings with IDs/severity/location/suggested disposition
and separate non-actionable observations. Answer PASS/BLOCKED based on whether
you can implement confidently/correctly and whether the change would materially
degrade behavior/security/robustness. Check source precedence, schema trust,
session identity, process transport, and release compatibility especially.

Append findings and author dispositions below. Accepted fixes receive a scoped
round-2 check limited to those IDs and newly introduced defects. Repeat independent
review after tasks 3, 4, 5, 6 and before implementation closure.

## Interface Review (design, not runtime acceptance)

| Principle | Proposed disposition / evidence owner |
|-----------|---------------------------------------|
| 1 Context | Met in design: one schema and one snapshot; API delta [SB-API-1/2]. |
| 2 Progressive disclosure | Met in design: defaults supply all fields; no new flags required. |
| 3 Names | Met in design: external prefix + canonical field; descriptions include units; legacy parser quirks explicitly retained. |
| 4 One identity | One canonical backing field per setting; two public receipt facades and naming views are an explicit compatibility cost. Test alias equality and no duplicated resolved state; do not claim one public representation. |
| 5 Derive | Met in design: local missing-only dependency tests motivated by Weft; no repeated broker default tables. |
| 6 Setup | Met in design: explicit env/file/options; no hidden env mutations; existing CLI wrapper timing declared. |
| 7 Teach | Preserve old diagnostics and unknown-env behavior; new schema-declared fields expose descriptions and validation without tightening old APIs. |
| 8 Action | Met in design: existing diagnostics and additive source metadata; firing error tests remain implementation work. |
| 9 Atomicity | N/A for concurrent writes: loader is read-only and publishes only a completed validated snapshot. |
| 10 Trust | Met in design: composed-schema validation, no runtime code from TOML/transport, redaction before formatting. |
| 11 Format | Met in design: familiar prefixed env inputs, canonical internal fields, actual project-target formats preserved. |

Ratified direction from user: external prefixes retained; no separate Weft and
broker config differing only in names. Other detailed policies are proposals in
this draft. Initial independent review: PASS with corrections recorded below;
no runtime acceptance claim. Runbook feedback: no new general rule proposed; existing ownership,
enumerability and trust-boundary checks cover this design.

## Out of Scope

New hardening policies, stricter unknown-env handling, subclass restrictions,
plugin API changes, pooling redesign, major-version/deprecation requirements;
any edits, adoption plans, test runs, release changes or runtime operations in
Weft or Taut; requiring downstream migration for local completion;
changes to documented precedence or validation order; automatic settings-file discovery or new CLI flags; rewrites of existing target
TOML/project JSON; grammar unification or unrelated numeric hardening; renaming
queue/protocol/persisted keys; broker storage migrations; new secret stores or
config watchers; putting runtime objects in config; changing Taut identity,
presentation, reactions, summon or dynamic debug lifetimes; a separately packaged
general-purpose configuration framework; coalescing; implementation or publication
as part of this planning request.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Review Log

**Superseding user decision:** this is internal; externally nothing moves.
Earlier review findings are retained verbatim as history, not active requirements.
All earlier dispositions supporting major/backend bumps, fatal unknown env,
canonical-only legacy output, opaque-extra removal, subset pooling, restricted
plugin views, or subclass rejection are superseded. The active design above
preserves existing behavior and adds the builder without requiring migration.
E1/E3/E5/E7 now resolve through compatibility preservation; E2 resolves through
retaining complete identity, not restricting plugin access. E4/E6 remain applicable.


Scope correction after rounds 1–2: user clarified that Weft/Taut are reference
only. Those reviews cover the earlier draft; their downstream execution scope
is superseded. The current tasks, spec promotion and gates are local-only.


| Review | Finding / verdict | Disposition / evidence |
|--------|-------------------|------------------------|
| Authoring inventory (superseded) | Weft and Taut have independent source/lifetime and transport contracts, not just differently spelled keys. | Historical draft proposed session-key projection; that disposition is withdrawn. Current plan preserves complete identity and uses downstream sources as reference only. |

### Independent review, round 1

Claude CLI 2.1.207, read-only `--permission-mode plan --allowedTools Read,Grep,Glob`,
540-second bound, exit 0, empty stderr. Reviewed plan SHA256:
`9b2dd58750c07b7eeb37d826971dc58cd941102fabd4f8513c2a6554fa5221dd`.
Verdict: PASS. Findings reproduced verbatim:

| ID | Sev | Location | Finding | Suggested disposition |
|----|-----|----------|---------|----------------------|
| F1 | P2 | "Proposed Design and API" env bullet + "Source adapters and scope limits" + Precedence register | The strict-unknown rule rejects any `PREFIX_*` name that is neither a field nor a declared reserved name. The illustrative reserved set (`TAUT_AS`, `TAUT_TOKEN`, `WEFT_ENV_FILE`) **omits known adapter-consumed selectors** `TAUT_DB` (line 215, `taut/_constants.py`) and `WEFT_CONTEXT` (line 915, `weft/_constants.py`). If `build_config` receives raw `env`, these hard-fail; if the adapter pre-strips them, that path isn't stated. The plan defers the full list to the task-1 inventory but the examples risk misleading the implementer. | Make task-1 inventory of *every* alias/selector external name a gate before edits; add `TAUT_DB` and `WEFT_CONTEXT` to the reserved examples and state whether selectors are reserved or stripped pre-resolver. |
| F2 | P2 | Comprehension gate Q2; [SB-API-2] delta "A broker consumer accepts a compatible composed snapshot… verifies that coverage"; `ConfigSnapshot` "Store schema compatibility and source labels" | The mechanism by which the broker *certifies* a foreign composed snapshot's broker fields is left between two readings — trust a snapshot-carried compatibility **label/lineage** vs **revalidate** broker-owned fields. This is the security/correctness boundary for invariant 3 (SimpleBroker accepts app snapshots directly). Label-trust lets an incorrect/hostile composed schema present malformed broker values. | Pin the mechanism: broker re-validates its owned fields under `CONFIG_DEFAULTS` validators at the boundary and projects identity over its *own* broker-field name list, rather than trusting metadata the snapshot carries. |
| F3 | P2 | "Proposed Design and API" `config_file` bullet + [SB-API-2] delta generic-TOML paragraph | The generic settings-file surface (new `config_file` param, root-table-of-prefixed-keys format, `options>file>env>defaults` precedence branch, plus its test matrix rows) has **no first-party consumer** in this migration — existing project/Taut/Weft TOMLs are explicitly *not* generic settings TOML, and the accepted extensibility goal is met by schema composition (`derive`/`fields`). This is machinery the brief asks to prefer removing. | Defer/cut the file-loader capability and its precedence branch and tests; reintroduce when a real consumer requires it. Extensibility (F-goal) does not depend on it. |
| F4 | P3 | [SB-API-1]/[SB-API-2] delta; "Compatibility, Rollout" | `load_config`/`resolve_config`/`snapshot_config` now return lowercase-unprefixed keys, but the legacy adapter covers **input** keys only. Every downstream/external read of `config["BROKER_*"]`/`["WEFT_*"]` off a returned mapping breaks with no shim. This is the single largest migration surface and is understated. | State explicitly in the delta + migration that there is no output-key compatibility shim; enumerate output-key reads as the primary consumer-migration workload. |
| F5 | P3 | [SB-API-2] delta re `resolve_config` | Today `resolve_config`'s ordinary-override path passes unknown keys through as opaque values (`_constants.py:754-760`), distinct from `resolve_isolated_config`. The plan's strict direction removes this but only enumerates the `resolve_isolated_config`/`preserve_unknown` removal. | Call out the ordinary-`resolve_config` unknown-key pass-through removal alongside the `preserve_unknown` removal. |
| F6 | P3 | Tasks 3–4 | `tests/test_process_broker_session.py` exercises the opaque-extra / `preserve_unknown=True` session-key cases (lines 645–780) that this change eliminates; task 3 enumerates other config tests for rework but not this one (only the verification gate runs it). | Add `test_process_broker_session.py`'s opaque-extra cases to the explicit rework list in task 3/4. |

| Finding | Author disposition |
|---------|--------------------|
| F1 | Accepted: reserve inventoried selectors explicitly; adapters read the same env; exhaustive inventory is a pre-edit gate. |
| F2 | Accepted: core validates owned values using its own validators; core and selected plugin own identity field lists. Caller schema labels cannot confer trust. |
| F3 | Declined: optional TOML input is an explicit user requirement. The synthetic embedder exercises real file input for its own field. No discovery or new CLI flags are added. |
| F4 | Accepted: canonical output migration is explicit, with no prefixed output shim. Input compatibility does not preserve output indexing. |
| F5 | Accepted: ordinary resolve_config unknown-key pass-through removal is explicit. |
| F6 | Accepted: session tests migrate opaque extras to declared app/plugin fields and preserve non-repr identity coverage. |
| A1 | Author correction: limit import purity to shared loader/core; preserve Weft helper lifetimes. Clarify first-acquirer session snapshot versus each handle's retained snapshot. |
| A2 | Author correction: the generic builder supports entirely independent app schemas; only broker consumers require broker completeness. |

Round 2 scope: accepted F1/F2/F4/F5/F6 and A1/A2 only. F3 is closed by owner requirement.

### Independent review, round 2

Same read-only Claude invocation and 540-second bound; exit 0, empty stderr.
Reviewed SHA256 `f715bfbe9f028c18b72dada350612de6c16a807ac4c108f4759c9f50c515e171`.
Verdict: **No blocker**. Reviewer confirmed F1/F2/F4/F5/F6 and A1/A2 are
consistently incorporated and grounded in baseline code. Repository plan hash
was unchanged by the reviewer; its detailed report was written to its own
external plan file. Findings reproduced verbatim:

Two minor, non-blocking tightenings. Both are optional and owner-decided.

| ID | Sev | Location | Finding | Suggested disposition |
|----|-----|----------|---------|-----------------------|
| R2-1 | P3 | Comprehension gate Q2 | Q2's answer ("the snapshot **must certify** compatible inherited broker field validators and completeness") reads looser than the F2 mechanism the design later pins (the *broker* revalidates its owned fields with its own `CONFIG_DEFAULTS` validators and does not trust carried labels). A careless implementer could read Q2 as endorsing label-trust. | Optional: reword Q2 to say the broker revalidates owned fields with core validators and checks completeness, so the comprehension gate matches the pinned mechanism. Not blocking — the authoritative Design/[SB-API-2] text is unambiguous. |
| R2-2 | P3 | Compatibility bullet / Task 1 inventory | F4's second half ("enumerate output-key reads as the primary consumer-migration workload") is satisfied only implicitly via the task-1 `rg` inventory that classifies internal-lookup hits; the plan states the no-shim rule clearly but never ranks output-key migration as the largest surface. | Optional: one sentence in Compatibility naming output-key reads as the primary consumer-migration workload. The core ask (explicit no-shim) is fully met, so this is a clarity nicety only. |

Author disposition: R2-1 and R2-2 accepted as wording-only fixes. Q2 now states
core-owned revalidation; Compatibility identifies output-key reads as primary
migration work. Author also preserved existing `overrides` parameter names in
proposed wrapper signatures after the review's non-actionable naming observation.
No further contract expansion or unresolved review finding remains.

### Scope-correction review

Independent same-family read-only reviewer found no remaining downstream
execution blocker in tasks, spec promotion, gates, rollout or index. Accepted
two cleanup findings: update review checkpoints to tasks 3/4/5/6 and identify
metadata-priority proof as synthetic source-policy testing. Reviewed correction
is local-only; prior reviews remain historical evidence for the API design.
Final documentation gates passed after this correction.

### User-supplied external review E1–E8

Findings reproduced verbatim from the supplied review (reviewer identity and
invocation were not supplied):

```text
Findings

Formatted to paste into the plan's Review Log.

ID	Sev	Location	Finding	Suggested disposition
E1	P1	Design/API env bullet; Interface Review row 7; Compatibility section	Unknown BROKER_* env names become fatal, and this is not enumerated as a break. Today they are silently ignored — verified: BROKER_VACUUM_LOCK_TIMEOUT=99 BROKER_TYPO_XYZ=1 simplebroker --version → simplebroker 8.1.1, exit 0. Under the plan, unknown selected names fail, and the preserved [SB-CLI-2] rule makes preparse config failure exit 1 including help and version. BROKER_VACUUM_LOCK_TIMEOUT was actually removed (CHANGELOG:1277), so stale shell profiles are a real population. Sharper: weft writes BROKER_* into the process env (_constants.py:2382-2409) and maintains REMOVED_SIMPLEBROKER_CONFIG_KEYS precisely because of this coupling — under strict-unknown that list becomes load-bearing for not crashing.	Either scope strict-unknown to options and app prefixes while the ambient BROKER_ base warns; or keep it fatal and add it to Compatibility + CHANGELOG as an enumerated break with a firing test. Don't leave it as a side effect of row 7.
E2	P1	Invariant 8 vs comprehension Q3 vs [SB-API-2] amendment	"Backends cannot consume undeclared application fields" is prose with no mechanism, and it contradicts invariant 8. Q3 and the amendment both say the runner receives the whole composed snapshot, while identity is keyed on a subset — so "values supplied to the runner must match those used for the key" is literally false. Plugins get Mapping[str, Any] and index it freely today (simplebroker_pg/plugin.py:369). Failure mode: two handles differing only in an app field a plugin happens to read share a pool and behave differently. Per engineering principle 12 this is a declared contract element with no gate.	Pass plugins a projection — core fields + that plugin's declared config_schema — so the sentence becomes true by construction and invariant 8 holds literally. Nothing in the plan requires the runner to see app fields. Failing that, downgrade to a plugin-author obligation and add the negative test.
E3	P2	Compatibility ("no prefixed output accessor shim"); F4 disposition	The no-shim decision is asserted, never argued, against ~650 call sites. A key-normalizing __getitem__ on ConfigSnapshot (canonical storage, BROKER_FOO → foo on read, DeprecationWarning) is ~5 lines, is inherited by .get/in/Mapping mixins, and is one engine with an alias at the boundary — which is engineering principle 2, not a violation of it. Repo precedent cuts the other way (zero DeprecationWarning in the codebase; 8.0.0 was already a hard cutover two weeks ago), so this is a legitimate owner call — but it should be a recorded rejected alternative, not an unexamined premise.	Record the alternative with a reason. If declined, keep as-is; if accepted, one deprecated-read major buys downstream incremental migration.
E4	P2	config_file bullet; [SB-API-2] generic-TOML paragraph	F3 was declined on authority, and the counter-argument is stronger than round 1 made it. An embedder can already parse its own file and pass options=; the only thing config_file adds is the file-above-env precedence slot, which the embedder can choose itself. What it costs: a second precedence order (options > file > env) inside a loader whose selling point is one order, plus a second TOML format alongside .broker.toml that is explicitly not it — both against [THEORY-1] and [THEORY-4] small-concept-count.	Worth re-asking you, not relitigating: was the requirement "settings can come from a file" (already satisfied by options=) or "SimpleBroker defines a settings-file format"? Only the second needs this machinery.
E5	P2	Compatibility bullets	A documented extension mechanism is withdrawn without being enumerated. Current [SB-API-2] (spec lines 143-147) promises opaque extras are preserved and "extensions may interpret their own keys", and that extras participate in process-session identity. The plan removes both. First-party plugins migrate to v10; any third-party plugin using extras has no path and no mention. Also inverts a safety default: today unknown → isolate (safe), tomorrow undeclared → share.	Name the removed promise explicitly in Compatibility and CHANGELOG. Interacts with E2.
E6	P3	with_options bullet; Acceptance matrix	The provenance rule — "recompute only derived defaults whose inputs changed and whose values still have default/derived provenance" — is the subtlest thing in the design and has no enumerated firing tests. The matrix gates the 7-value source enum but not provenance transitions (default→explicit, derived→explicit, derived→derived, explicit-never-overwritten).	Add an acceptance-matrix row enumerating provenance transitions.
E7	P3	Design bullet on snapshot type	Subclass handling tightens from today's "non-exact subclasses are revalidated rather than trusted" to outright rejection. Defensible, but it's an enumerable break, and it means embedders wanting typed/attribute access must wrap rather than subclass — worth one sentence given extensibility is the headline goal.	Enumerate in Compatibility; note the wrap-don't-subclass guidance.
E8	P3	Task 3/4 rework list (F6 disposition)	Opaque-identity cases also live at tests/test_process_broker_session.py:534-574, outside the cited 645-780 range.	Widen the cited range.

Non-actionable observations: the two uncommitted spec deltas are one ## Related Plans line each — baseline reconciliation is a no-op, not drift. The plan index row exists at docs/plans/README.md:32 as draft, class 5, correctly. llms.txt and the configuration guide need no key churn since env spellings don't change.
```

| Finding | Disposition |
|---------|-------------|
| E1 | Accepted: retain fatal strictness as a proposed major break, explicitly enumerate stale/unknown BROKER env, help/version exit 1, CHANGELOG migration and firing tests. |
| E2 | Accepted: restricted read-only plugin/runner view backed by the retained snapshot; exactly the identity fields are exposed, with negative access tests at every handoff. No second config state. |
| E3 | Accepted as an alternatives-analysis gap: record the legitimate read shim and its benefits; propose declining it for the combined major contract cutover, with reasons and owner option explicit. |
| E4 | Resolved by user: retain config_file; external TOML settings are namespaced like env, and the supplied schema includes embedder-defined fields. Select matching keys from the document, allowing existing project fields to coexist without requiring a separate file or broker-only key inventory. Preserve existing target ownership and precedence. |
| E5 | Accepted: enumerate opaque-extension preservation/interpretation/identity withdrawal, third-party plugin declaration path and CHANGELOG requirement; E2 supplies the access gate. |
| E6 | Accepted: enumerate provenance transitions, explicit equal-to-default, chains, stable inputs, transport and invalid derivation tests. |
| E7 | Accepted: enumerate subclass rejection and wrap/pass-underlying-snapshot guidance. |
| E8 | Accepted: widen explicit test rework to 534–780, including non-repr cases at 534–574. |

### Review of E1–E8 dispositions

Independent same-family read-only review found no blocking contradictions in
E1/E2/E3/E5/E6/E7/E8. It confirmed restricted views align plugin access with
session identity across design, spec, tasks and negative tests; scope stays local.
At that review E4 was open; the subsequent user clarification above resolves it.
User correctly noted existing PostgreSQL TOML support: verified
`extensions/simplebroker_pg/README.md` CLI Usage and
`simplebroker/_project_config.py::load_project_config` (core tomllib parser).
Existing version/backend/target/backend_options format and project-over-env target
precedence are baseline contracts, not new capabilities. The question is how the
shared builder selects namespaced settings from a document, not whether to add
TOML support from scratch. The user then explicitly confirmed prefix-based TOML
selection and schema-defined embedder fields; the design/spec reflect that choice.

### Latest compatibility correction

User confirms this is internal, namespaced lookups are supported, and unknown
env vars must not be blocked. Active design preserves old views/contracts while
adding canonical storage and selected-prefix read aliases. Unknown env values
are not parsed unless their fields are declared. No major or backend bump.

### Simplification compatibility review

Independent same-family read-only review checked the active plan against the
latest user corrections. Accepted all cleanup findings: ignore undeclared TOML
keys like env keys; replace reservation machinery with ignored-selector tests;
preserve complete pool identity; remove migration framing and downstream action
wording. No active version bump, plugin contract change, subclass ban, unknown-env
rejection or downstream completion gate remains. Documentation gates passed.

### Independent evaluation of user-supplied G1–G7 review

Findings as supplied (reviewer identity/invocation not supplied):

```text
ID	Sev	Location	Finding	Suggested disposition
G1	P1	Design bullet "Existing ResolvedConfig constructor, exact-instance fast path… remain unchanged"; Compatibility "ResolvedConfig need not be a type alias"	The ConfigSnapshot↔ResolvedConfig type relationship is unspecified, and "keep the fast path unchanged" makes the default outcome a silent ambient read. Five type gates in the three funnel functions every config seam passes through: _constants.py:745,747 (resolve_config), :773 (snapshot_config), :785,787 (_overlay_config) — all type(x) is ResolvedConfig / isinstance(x, ResolvedConfig). Trace a composed snapshot: snapshot_config(cs) → resolve_config(cs) → falls through both gates → load_config() reads ambient env → then iterates cs, whose canonical keys aren't in _CONFIG_NORMALIZERS, so they land as opaque extras while the broker uses ambient BROKER_*. The embedder's cache_mb=11 is silently discarded, ambient wins, and pool identity gains junk keys. Violates invariants 2 and 7, and contradicts "New build_config results… are passed unchanged into Queue/watcher handles."	Pin the type relationship (facade over a shared base, or ResolvedConfig as a subclass of ConfigSnapshot), then enumerate all five gates plus Queue/watcher/DBConnection/_project_config._config_snapshot:90 as must-recognize seams. Firing test: a composed snapshot survives each seam with env poisoned to conflicting values.
G2	P1	config_file bullet; [SB-API-1/2] delta; acceptance row "TOML namespace and extension"	Namespaced settings in the project document are a silent no-op for SimpleBroker's own users. load_project_config (_project_config.py:57-87) reads exactly version/backend/target/backend_options and ignores every other root key; core never passes .broker.toml as config_file. So BROKER_CACHE_MB = 20 in .broker.toml parses, raises nothing, warns nothing, and does nothing. The acceptance row at line 620 pins that as correct behavior. This is the archetypal agent-facing-interface failure — a plausible input that silently no-ops — in a repo whose CLI is explicitly an agent surface.	Pick one: (i) the project loader consumes namespaced settings with declared precedence; (ii) it warns on unconsumed PREFIX_* root keys; (iii) drop the coexistence claim and keep the formats separate. (ii) is cheapest and closes the trap without new precedence.
G3	P2	Header line 5	Hardening: required was dropped, but the mandatory triggers still fire. writing-plans.md:84-95: "a public contract… or compatibility surface is changing" — the header itself says "compatibility-sensitive"; "contains a one-way door" — publishing build_config/ConfigSchema/ConfigSnapshot to PyPI cannot be unpublished; and spawn transport puts the same logic in a second execution context. "A simplification, not a product-hardening project" is a category label, not a reason (engineering principle 10).	Restore the declaration and run the checklist — it's cheap now that most answers are "unchanged." Related, one line: the builder is still frozen before either intended consumer has touched it; an rc validated against weft on a branch is cheap insurance now that no coordination is required.
G4	P2	Source Documents theory paragraph; Interface Review row 4	The [THEORY-4] justification "reduces the concept count" is now false locally. End state adds a second snapshot type (ConfigSnapshot, recursive freeze) beside ResolvedConfig (shallow extras), a selected-prefix read alias, a BROKER-compat view over foreign-prefix snapshots, and retained legacy prefixed iteration/serialization. The reduction happens only downstream, only if weft/taut adopt, which is out of scope. Row 4 ("One identity — met in design") wasn't revisited after the alias design landed.	State the honest end-state inventory — how many types, how many naming views — and either revise the theory paragraph or accept the increase as a deliberate temporary cost. Don't leave a justification the implementation will falsify.
G5	P2	Goal; Task 4	The internal canonical rename is now optional and is the bulk of the work. "Migrate this repo's _constants and consumers to unprefixed keys" is inherited from the breaking draft. But a BROKER-compat view now exists by design, so the ~329 internal prefixed reads across 41 files would keep working against canonical storage untouched. Task 4 is thus the highest-churn, highest-regression-risk slice in stable code with no external effect. It may still be right — leaving internals on the compat view entrenches the naming duplication — but the plan presents it as given rather than as a trade.	Justify it on its own merits, or split it so tasks 3+5 land and get exercised first.
G6	P3	Compatibility; Out of Scope	The compatibility layer has no recorded exit. "Do not add deprecation warnings", "aliases are not deprecated or removed", and Out of Scope excludes major/deprecation requirements. The rejected alternative is recorded (lines 512-516 — good), but nothing marks the layer as transitional or names who decides its removal, so the next agent reads it as permanent architecture.	One line recording the intended future disposition, per [DOM-16] / principle 15.
G7	P3	Review Log authoring-inventory row (768); Interface Review rows 3-4	Stale text from the superseded draft. The authoring-inventory row still cites "session-key projection" as an adopted disposition — that's exactly what the rescope reversed — and reads as active guidance rather than history.	Mark superseded the way the other rows are.
```

| Finding | Author evaluation and disposition |
|---------|-----------------------------------|
| G1 | Accept the gap and failure trace, not a claim that the unimplemented builder already fails. Current five branches do fall through to ambient for an unrelated Mapping type. Pin sibling receipt facades/shared base, explicit dispatch/overloads, all named consumer seams and poisoned-env consumption tests. |
| G2 | Accept the activation/documentation ambiguity; decline warning or automatic-loading remedies. The explicit API is additive; existing CLI discovery must not change. A mixed file can serve both consumers without a second format or automatic activation. State explicit config_file activation beside examples and test both paths. Severity P2 for plan ambiguity rather than a current P1 implementation regression. |
| G3 | Accept mandatory plan-quality checklist coverage; distinguish it from product hardening. Decline a required downstream rc/branch trial because scope is local and synthetic real-consumer tests supply current gates. Publication is separately authorized, not an irreversible action performed by this plan. |
| G4 | Accept: replace unsupported net-concept reduction claim with the actual inventory and local consolidation benefit. Two public receipt facades and naming views are a compatibility cost; downstream simplification is contingent, not claimed delivered. |
| G5 | Partly accept: explain churn/tradeoff and require additive seam proof before broad renaming. Decline making canonical internal access optional: the user explicitly requested _constants and consumers use unprefixed values. |
| G6 | Decline a mandatory removal plan. Supported namespaced access is not necessarily transitional. Record owner authority for any future contract change, with no planned deprecation or required follow-up. |
| G7 | Accept: mark authoring-inventory projection disposition superseded; update active interface inventory. |

### Scoped implementation re-review

Independent read-only reviewer checked the accepted implementation fixes and
reported no blocker: canonical consumer access, legacy opaque overlays, hashable
set transport, core canonical-value checks and ordinary plugin mapping identity.
Its verification reran all 39 new tests successfully. The author then reran managed
PostgreSQL/Valkey suites and Python 3.11 packaging checks; all passed. The current
wheel also passes all 39 tests in the isolated Python 3.11 environment above.
Existing package versions and backend API remain unchanged.

### Final external review attempt

A separate Claude read-only review was attempted with a 540-second bound and
timed out without a verdict. It is not recorded as a pass. The completed
independent scoped review above supplies the review evidence; a different-family
final verdict remains unavailable. No new findings were returned by the timed-out
attempt. Implementation and verification were recorded in the working tree before the
user authorized closure and commit. This plan and its index close in that
commit. Publication remains outside this task.

## Execution Log

- Implementation adopted. Comprehension gate: prefix replacement alone cannot
  preserve receipt/lifetime semantics; core checks canonical field completeness;
  full plugin-visible configuration remains keyed; project-target precedence is
  separate from explicit settings files. Local source baseline matches the plan.
- Shared definitions, parser and validators moved to config.py; _constants keeps
  constants/path validation with lazy compatibility imports. Public old outputs
  remain prefixed, with a canonical backing for snapshots. New exact/subclass
  recognition runs before ambient branches. Internal consumer migration is
  separately reviewed; downstream repos remain untouched.


- User scope correction: removed downstream migration tasks, spec deltas,
  mandatory test runs and release dependencies. Replaced them with local synthetic
  embedding/transport proof. No downstream files were edited.


- Planning evidence: local and downstream source baselines and ownership seams
  were inspected; downstream inventory supplied by separate read-only agents.
  No implementation or runtime acceptance is claimed by this record.
- Prior reconnaissance in this conversation: 150 Weft and 132 Taut targeted
  config/context cases passed against their installed core versions (8.1.1 and
  8.0.0 respectively); a combined source-import probe produced independent
  11/22/33 cache values and unchanged env. These establish baseline behavior,
  not validation of the proposed implementation.
- Initial planning gates passed: `python3 bin/check-dom15-fixtures`,
  `bin/check-plan-context`, `bin/check-doc-paths`, and `git diff --check`.
  Final post-review rerun: all four gates passed (exit 0).
- Fresh source probe confirms bad ambient plus valid override fails in broker
  and Weft, succeeds in Taut; the documented Weft legacy override order remains.
  This is baseline evidence, not proposed implementation validation.

### Implementation evidence (working tree)

- Source field inventory below covers all 32 inherited broker definitions. Keys,
  parsers and units come from the single config.py registry; old field/error
  contracts remain covered by constants, invalid-config, connection, project,
  watcher and backend suites. New fields need only schema declarations.
- Shared engine/type seam slice and canonical consumer slice received independent
  read-only review. Accepted findings fixed: opaque canonical-looking keys must
  never drive broker settings; legacy overlays preserve opaque keys; hashable set
  transport retains shape; broker canonical validation cannot discard a changed
  result. Each has a firing regression in the new tests. Integer opaque legacy
  keys remain supported as before. Benchmark helper now uses the shared resolver.
- `uv run pytest -q`: final full source suite passed; expected platform, opt-in
  diagnostic and cross-backend-service skips remain. New builder/coexistence suite:
  39 passed, including real Queue/watcher/SQLite and spawn tests.
- Managed PostgreSQL: 1628 shared tests and 319 extension tests passed. Managed
  Valkey: 1620 shared tests and 313 extension tests passed (documented skips only).
- `uv run pytest examples -q`: passed (140 tests). Core mypy passed (46 files);
  release root-test, example and extension-test mypy enumerators passed (217,
  16, 26 and 19 files). Ruff lint and formatting gates passed.
- Python 3.11 packaging smoke passed for core wheel/sdist and both extension
  wheels. New 39 tests also passed against an isolated installed core wheel at
  `/var/folders/m_/2tncpj593tj8s_jdbdhj8g5m0000gn/T/sb-config-wheel-proof-rm3b6hdt/venv`;
  imported `simplebroker.__file__` was inside that environment, not the checkout.
  Built versions remain core 8.1.1 / extensions 4.1.1; no version or publication change.
- Configuration guide example passed with actual root imports. Stale
  BROKER_VACUUM_LOCK_TIMEOUT and BROKER_TYPO_XYZ still allow CLI --version,
  returning simplebroker 8.1.1 and exit 0. All documentation gates passed.
- Theory possession probe: a request to move embedder task execution into the
  builder belongs to the app, not this loader. This change consolidates settings
  resolution and keeps orchestration out of core. Skill/runbook evaluation:
  existing compatibility/lifetime review guidance sufficed; no workflow expansion.

| Canonical field | Existing expected form/units | Example local consumer |
|-----------------|-----------------------------|------------------------|
| `busy_timeout` | an integer number of milliseconds | `simplebroker/_retry_policy.py`, `simplebroker/_runner.py` |
| `cache_mb` | an integer number of megabytes | `simplebroker/_backends/sqlite/runtime.py` |
| `sync_mode` | FULL, NORMAL, or OFF | `simplebroker/_backends/sqlite/runtime.py` |
| `wal_autocheckpoint` | an integer page count | `simplebroker/_backends/sqlite/runtime.py` |
| `max_message_size` | an integer byte count | `simplebroker/db.py`, `simplebroker/watcher.py` |
| `read_commit_interval` | an integer message count | `simplebroker/commands.py` |
| `generator_batch_size` | an integer message count | `simplebroker/db.py`, `extensions/simplebroker_redis/simplebroker_redis/core.py` |
| `load_max_future_skew_seconds` | a non-negative integer number of seconds | `simplebroker/_dump.py` |
| `auto_vacuum` | an integer flag | `simplebroker/db.py`, `extensions/simplebroker_redis/simplebroker_redis/core.py` |
| `auto_vacuum_interval` | an integer mutation count | `simplebroker/db.py`, `extensions/simplebroker_redis/simplebroker_redis/core.py` |
| `vacuum_threshold` | a numeric percentage | `simplebroker/db.py`, `extensions/simplebroker_redis/simplebroker_redis/core.py` |
| `vacuum_batch_size` | an integer message count | `simplebroker/_backends/sqlite/maintenance.py`, `extensions/simplebroker_redis/simplebroker_redis/core.py` |
| `skip_idle_check` | a boolean flag | `simplebroker/watcher.py` |
| `jitter_factor` | a numeric ratio | `simplebroker/watcher.py` |
| `initial_checks` | an integer check count | `simplebroker/watcher.py` |
| `max_interval` | a numeric number of seconds | `simplebroker/watcher.py` |
| `burst_sleep` | a numeric number of seconds | `simplebroker/watcher.py` |
| `debug` | a boolean flag | `simplebroker/db.py` |
| `logging_enabled` | a boolean flag | `simplebroker/db.py`, `simplebroker/sbqueue.py` |
| `default_db_location` | an absolute directory path or empty string | `simplebroker/sbqueue.py`, `simplebroker/cli.py` |
| `default_db_name` | a relative database path with at most one directory | `simplebroker/cli.py`, `simplebroker/project.py` |
| `project_config_path` | an absolute directory or one relative directory | `simplebroker/_project_config.py` |
| `project_config_name` | a relative config path with at most one directory | `simplebroker/_project_config.py` |
| `project_scope` | a boolean flag | `simplebroker/cli.py` |
| `backend` | a backend name | `simplebroker/_project_config.py`, `simplebroker/_dump.py` |
| `backend_host` | a host name | `extensions/simplebroker_redis/simplebroker_redis/plugin.py`, `extensions/simplebroker_pg/simplebroker_pg/plugin.py` |
| `backend_port` | an integer port | `extensions/simplebroker_redis/simplebroker_redis/plugin.py`, `extensions/simplebroker_pg/simplebroker_pg/plugin.py` |
| `backend_user` | a user name | `extensions/simplebroker_pg/simplebroker_pg/plugin.py` |
| `backend_password` | a password string | `extensions/simplebroker_redis/simplebroker_redis/plugin.py`, `extensions/simplebroker_pg/simplebroker_pg/plugin.py` |
| `backend_database` | a database name | `extensions/simplebroker_redis/simplebroker_redis/plugin.py`, `extensions/simplebroker_pg/simplebroker_pg/plugin.py` |
| `backend_schema` | a schema name | `extensions/simplebroker_redis/simplebroker_redis/plugin.py`, `extensions/simplebroker_pg/simplebroker_pg/plugin.py` |
| `backend_target` | a backend target string | `simplebroker/_project_config.py`, `extensions/simplebroker_redis/simplebroker_redis/plugin.py` |

## Fresh-Eyes Review

Before handing off the draft, verify that every named existing surface exists,
every new surface is labeled proposed, the exact spec delta agrees with the
tasks, and no implementation step silently changes target/policy lifetimes or the
documented precedence register. Prove non-broker embedder fields through the
actual builder/consumer path, not an opaque-extra bypass.
Check especially that a compatibility accessor is object identity, not another
config carrier; transport does not re-parse canonical values; and existing external callers/plugins need no migration or version change.
The implementation plan is completed. On 2026-09-12 the user authorized closure
and commit of the verified implementation. The plan and index close in the same
commit as the implementation. The independent review passed; the additional
external review timed out without a verdict, as recorded above. No publication
or version change is included.
