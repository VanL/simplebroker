# Config simplification: one defaults table, one function, one object

Status: active
Class: 5 — normative spec text added by `f213b12` in
`docs/specs/16-python-library-api.md` is replaced (exports, builder, key
spelling, precedence). Promotion strategy **A** (in-file text-first).
Hardening: required — a [DOM-5] risky trigger fires ("the same core behavior
must run in more than one execution context": configuration is reconstructed in
a spawned child).
Plan type: implementation with spec revision.
Owner: SimpleBroker owns all implementation, tests, docs, and in-repo extension
changes. Weft and Taut are migrated by the owner, outside this plan.

## Goal

**One module, one defaults table, one function, one object.**

- `_constants.py` — the only configuration module. `simplebroker/config.py` is
  deleted.
- `DEFAULT_CONFIG` — a frozendict there. Every named constant that is a
  configuration default lives in it, now carrying its metadata and its validator
  instead of leaving validation ad-hoc elsewhere.
- `resolve_config()` — copies `DEFAULT_CONFIG` into a new `Config`, applies
  `toml`, `env`, `override` in that order, returns it lightly frozen.
- `Config` — the resolved object. Uppercase unprefixed keys, one stored value
  per field, no aliases; retained prefix and field declarations for derivation.

The minimal set needed by an embedder is exported to the package root
`__all__`; nothing else is public, and there is no public configuration module.

That is the whole surface. Everything else in today's configuration layer is
deleted.

**Success is structural, not numeric.** No line-count target: the file ends up
the size it ends up, per engineering principle §14 (floors, not line counts).
What must hold is that there is exactly one way to do each thing:

| Thing | The one way | Fails if |
|-------|-------------|----------|
| Own configuration | `_constants.py` | a second configuration module exists |
| Declare fields | `DEFAULT_CONFIG` | a second field registry exists |
| Resolve | `resolve_config()` | a second entry point resolves configuration |
| Hold resolved values | `Config` | a second config type exists |
| Validate a field | its one validator | a field has a parser *and* a validator |
| Read a value | `config["KEYNAME"]` | a wrapper, alias, or second spelling resolves |
| Get an isolated config | `env=None` | a second function exists for it |
| Extend | copy `DEFAULT_CONFIG` and edit | a schema type or derive operation exists |

Each row is enumerable, so each gets a firing check (engineering principle §12).

### What the old design was, and what changes

Old: all constants lived in `_constants.py` and *were* the defaults; a function
built a config from them; validation was somewhat ad-hoc — per-field parsers, a
separate validator pass, and a `>1` heuristic inside one field.

New: the constants still live in `_constants.py` and are still the defaults —
but each now carries its metadata and its single validator as part of
`DEFAULT_CONFIG`, and making that table a frozendict means the package defaults
cannot be updated by accident rather than merely by convention.

### Deleted

`load_config`, `snapshot_config`, `resolve_isolated_config`, `build_config`,
`ResolvedConfig`, `ConfigSnapshot`, `ConfigSchema`, `CONFIG_DEFAULTS`,
`ConfigField`'s parser/dependency/default-factory machinery, `_NamingView`,
`canonical_config`, `legacy_config`, `_ConfigReceipt`, `to_values`,
`from_values`, `with_options`, `_CONFIG_FIELDS`, `_CONFIG_NORMALIZERS`,
`_canonical_validator`, `preserve_unknown`, the `_constants.py` `__getattr__`
shim, and **`simplebroker/config.py` itself**.

Isolation stops being a function and becomes "don't pass `env`".

Deleting the module is not cosmetic. `f213b12` split configuration out of
`_constants.py` in order to have a *public* module, then needed a 17-name
`__getattr__` shim to bridge the boundary it had just created — the invisible
wall engineering principle §14 warns about. Publication is what the root
`__all__` is for, so the module earned nothing and cost a shim. Collapsing back
removes the shim's reason to exist rather than just the shim. A ~900-line
`_constants.py` is explicitly sanctioned by §14 ("do not propose a file split on
size grounds alone") and by the 2026-08-25 lesson reserving that module for
shared config values.

### Version and consumers

Owner decision: this is a **minor**. There are **no third-party embedders** of
the configuration API; Weft and Taut are the only consumers, and the owner is
rebuilding their `_constants` to match this design. No compatibility shim,
prefixed view or deprecation is built here. Backend API stays v9, so no
extension release is forced by the plugin contract.

Downstream migration is coordination, not a completion gate: this plan is done
when SimpleBroker and its in-repo extensions are done.

## Source Documents

Read order per `docs/agent-context/context.index.yaml`: `docs/program-theory.md`
([THEORY-1], [THEORY-4] small concept count), decision hierarchy, principles,
`docs/agent-context/engineering-principles.md` (§2 canonicalize at boundaries,
§6 reuse local paths, §7 YAGNI, §10 failing test first, §11 update all
consumers, §12 enumerable contracts get gates), `docs/lessons.md` — in
particular 2026-08-25 on `_constants.py` ownership of shared config values and
the no-magic-constants tier policy. Runbooks: writing-plans, hardening-plans,
testing-patterns, designing-agent-facing-interfaces,
adversarial-acceptance-probes.

Winning contracts: `docs/specs/16-python-library-api.md` [SB-API-1],
[SB-API-2], [SB-API-3], [SB-API-9], [SB-API-10], [SB-API-11];
`docs/specs/10-cli.md` [SB-CLI-2], [SB-CLI-4]; `docs/guides/configuration.md`.
Predecessor `docs/plans/2026-09-11-shared-configuration-loader-plan.md`
(`completed`) is history; the spec tree is the contract.

## Spec Baseline

| Repository | Source revision | Content identifier |
|------------|-----------------|--------------------|
| SimpleBroker | `f213b12d48fdad74872046c1cb81d03a863092e3` | API spec SHA-256 `66e21f9e15af459512486cca16c13279001592f93fa03444c060a8d38a196e58`; CLI spec `ceeb98cc8f448bb8f594e9e652cf757e3d797ff5bd7e5ff62fd09d2857d7b315` |

An uncommitted `docs/coalescing.md` edit exists in the working tree, unrelated.

## Measured Baseline Behavior

Obtained by execution at authoring, not inference.

| Probe | Result |
|-------|--------|
| `vacuum_threshold` numeric `10` | `0.1` via legacy `resolve_config`; `10.0` via the `f213b12` builder. **Real divergence**, caused by two validation paths. |
| `vacuum_threshold` numeric `0.1` | `0.1` via legacy — the `>1` heuristic treats `≤1` as already fractional, so `"10"`, `10` and `0.1` all currently mean 10%. |
| `jitter_factor` `15` / `"15"` | `15.0` through all four paths. **No divergence**; its parser is plain `float`. Its meaning is a fraction and tests must protect that. |
| Inline literals in the fields table | 26 of 32 defaults are unnamed inline literals (`"5000"`, `"0.15"`, `"5432"`, `"localhost"`, `"simplebroker_pg_v1"`…), all strings even for numeric fields. Only 4 reference a named constant. |
| `BROKER_cache_mb=99 BROKER_Cache_Mb=98` in env | Both the legacy loader and the builder return the default `10`; neither name reaches the result. Case-variant external names are **already** never read. |
| `_maintenance` API | The function is `vacuum_is_eligible` (`_maintenance.py:27`), exported from `simplebroker.ext`, called at `db.py:3699` and `simplebroker_redis/core.py:1327`. No `should_vacuum` module function exists. |
| Wrapper call sites | 112 across 19 files: `db.py` 24, `watcher.py` 17, `cli.py` 16, `_runner.py` 7, `_project_config.py` 5, `sqlite/runtime.py` 5, `project.py` 4, `sbqueue.py` 4, extensions 23, others 7. |
| Downstream **source** callers | `resolve_isolated_config`: weft `_constants.py:2412,2432`; taut `_constants.py:187,188,226,228`. `ResolvedConfig`: 4 weft and 4 taut modules, nearly all type annotations, plus `isinstance` at `weft/context.py:145`. `resolve_config`, `snapshot_config`, `load_config`, `build_config`: **zero**. |
| Downstream **test** callers | `resolve_config`: `weft/tests/system/test_constants.py:14`, `taut/tests/test_constants.py:7`. `ResolvedConfig`: 10+ weft test modules. |
| Public config names | Root `__all__` holds `ResolvedConfig`, `resolve_config`, `resolve_isolated_config`, `snapshot_config` (pre-existing) plus `CONFIG_DEFAULTS`, `ConfigField`, `ConfigSchema`, `ConfigSnapshot`, `build_config` (added by `f213b12`, unpushed). `load_config` is not in `__all__`. |
| `f213b12` publication state | Committed, **not pushed** (`main` ahead by 2), no release cut; versions remain core 8.1.1 / extensions 4.1.1. |

## Design

### `DEFAULT_CONFIG` — a frozendict in `_constants.py`

Each entry pairs a named, typed constant with its metadata and its one
validator:

```python
DEFAULT_CONFIG: Final[Mapping[str, ConfigField]] = MappingProxyType({
    "BUSY_TIMEOUT":     ConfigField(DEFAULT_BUSY_TIMEOUT_MS, "milliseconds", int),
    "CACHE_MB":         ConfigField(DEFAULT_CACHE_MB, "megabytes", int),
    "VACUUM_THRESHOLD": ConfigField(DEFAULT_VACUUM_THRESHOLD_PCT, "percent 0-100", _percent),
    "DEFAULT_DB_NAME":  ConfigField(DEFAULT_DB_NAME, "relative path", _db_name_path),
    ...
})
```

The 26 inline literals become named, typed constants in `_constants.py` — the
module the repository reserves for shared config values — and typed rather than
stringly: `DEFAULT_BUSY_TIMEOUT_MS: Final[int] = 5000`, not `"5000"`. One
validator per field makes the string-first defaults unnecessary.

`ConfigField` is a frozen record: default, unit/description, validator,
sensitive flag. Nothing else.

`ConfigField`, private field validators, `Config` and `resolve_config` all live in
`_constants.py` beside `DEFAULT_CONFIG`. With one module there is no import
direction to get wrong and no boundary for a shim to bridge.

### Validators

Exactly one validator per field, shared by every source, accepting the
equivalent string and numeric spellings and returning the field's declared
unit. There is no separate parser stage, so no source can disagree with another.

The set is derived from what the 32 fields actually need, measured: `int` for
11 fields, `str` for 10, `float` for 3, plus four existing one-off parsers
(strict-one boolean ×2, debug flag, project scope, vacuum percent) and one enum
(`_sync_mode`). Use `int`, `str` and `float` directly. Field-specific
validators remain private; embedders can copy an existing `ConfigField` or
supply their own callable without importing its implementation.

An earlier draft proposed `non_negative_int`, `positive_int`, `port`,
`fraction`, `enum_of(*values)` and `relative_path`. Two errors there, both
corrected here. `positive_int`, `port` and `fraction` would have *invented*
bounds no field has today — exactly what invariant 2 forbids. And the claim that
no field validates paths was simply false.

**Existing validation that must survive consolidation**, measured:

| Field | Constraint that exists today |
|-------|------------------------------|
| `DEFAULT_DB_LOCATION` | safe path components; a relative value **warns and is blanked**, not rejected — a fallback, not a failure |
| `DEFAULT_DB_NAME` | safe components; absolute rejected; more than one directory level rejected |
| `PROJECT_CONFIG_PATH` | safe components; a relative value with more than one part rejected |
| `PROJECT_CONFIG_NAME` | safe components; absolute rejected; nested rejected |
| `LOAD_MAX_FUTURE_SKEW_SECONDS` | rejects booleans, non-integer numbers, non-integer strings and negatives |
| `SYNC_MODE` | enumerated; an unrecognized value falls back to `FULL` |

Those four path validators are per-field and stay per-field, under names that
say which field they serve. They are not a generic `relative_path` helper —
their rules differ.

**The one cross-field constraint.** `PROJECT_CONFIG_PATH` and
`PROJECT_CONFIG_NAME` are also checked *together*: their combined directory
parts must not exceed one level. One validator per field cannot express that,
and inventing a dependency mechanism to hold it would rebuild the schema
framework this plan deletes.

It belongs in **one explicit whole-config function called after each complete
source overlay**, before applying the next source and before freezing. Run it
on the initialized defaults too. Check after the entire source has been applied,
not between individual keys, so mapping iteration order cannot affect the
combined constraint. This is a short hand-written function with a couple of
conditions, not a second resolution path. That is also where `DEFAULT_DB_LOCATION`'s
warn-and-blank fallback runs, since it must mutate while the values are still a
plain dict. Not a framework — no declared dependencies, no ordering graph, no
DAG. If that step ever grows a second cross-field rule it stays a function with
two conditions; if it grows a mechanism, stop.

Each condition runs only when its required broker fields are present. The
combined path check requires both `PROJECT_CONFIG_PATH` and
`PROJECT_CONFIG_NAME`; the location fallback requires `DEFAULT_DB_LOCATION`.
Skip absent fields without injecting broker defaults or rejecting an
application-only defaults mapping.

This timing preserves base-first failure: environment values
`PROJECT_CONFIG_PATH=dir` and `PROJECT_CONFIG_NAME=sub/file.toml` fail before
an argument or override can replace the name with `.broker.toml`. Checking only
the final result would incorrectly accept that input.

Three boolean grammars survive (`strict_one_bool`, the truthy debug flag, and
standard bool parsing) because they are pre-existing external contracts and
grammar unification is out of scope. That is a known wart, deliberately kept,
and the only place "one way to do it" does not hold — noted here rather than
quietly passed over.

Many fields share one validator; none has two. No validator infers a unit from
a Python type.

`VACUUM_THRESHOLD`'s unit is a **percentage, 0-100**: `10` and `"10"` mean 10%,
`0.1` means 0.1%. The `>1` heuristic is deleted. `vacuum_is_eligible` keeps its
fraction parameter, so each of its callers — in `db.py` and the Redis extension
core — passes `config["VACUUM_THRESHOLD"] / 100`. (Symbols, not line numbers:
line pins rot, per `docs/lessons.md` 2026-08-23. The dated pins in Measured
Baseline are observations, not references to maintain.)

`JITTER_FACTOR` stays a fraction and is consistent today; its tests are
protective, not a repair.

### `resolve_config` and `Config`

```python
def resolve_config(
    prefix: str | None = None,
    *,
    base: Config | None = None,
    defaults: Mapping[str, ConfigField] | None = None,
    toml: os.PathLike[str] | str | Mapping[str, Any] | None = None,
    env: Mapping[str, str] | None = None,
    override: Mapping[str, Any] | None = None,
) -> Config:
```

Copies `defaults` into a fresh mutable dict, overlays **toml, env,
override** in that order — lowest to highest — validating each value as it is
applied, and returns a lightly frozen `Config`.

`override` is new and highest. It exists mainly for tests: build a
fully-correct configuration and change one value. Empty by default. Override keys use the external namespace; bare, wrong-prefix
or malformed names raise `ValueError`. Valid selected custom names remain accepted.

Validating per overlay rather than once at the end is what preserves the
existing failure order — a malformed ambient value fails even when a
higher-priority source would replace it. After each complete source overlay,
the whole-config function checks applicable combined constraints and applies
the location fallback before the next source is processed. That behavior is pinned today by
`tests/test_invalid_config_lifecycle.py`.

`env=None` means no environment. That is how an isolated configuration is
built; there is no second function for it.

**Keys.** Stored keys are uppercase and unprefixed: `CACHE_MB`. No aliases — a
prefixed or case-variant lookup raises `KeyError`. The prefix is retained
as `config.prefix` metadata, separate from configurable values.

**External selection.** `toml` and `env` select `prefix + "_"` followed by a
suffix matching `[A-Z][A-Z0-9_]*`. Anything else is not part of the namespace:
`BROKER_cache_mb` is never read, never stored, never passed through — which
matches measured current behavior. Uppercase is enforced at declaration and
never imposed on set; case-folding on set would be a lossy transformation.

**Near-miss warning.** A `UserWarning` fires *during the parse* — inside the
same pass that selects prefixed names — when a supplied external name is
`prefix + "_"` plus a non-conforming suffix whose uppercase form is a declared
field. Unrelated `PREFIX_*` names stay silent.

The message says where the bad value is: the source it came from (the
environment, or the TOML document's path), the name exactly as supplied, and the
declared field it appears to be a near-miss for — enough to fix it without
guessing. Sensitive fields redact the value, as elsewhere.

**External selection never fails on a name.** Malformed names, bare external names and
names from another prefix are ignored, with only the specified near-miss
warning. A well-formed selected name is retained even when undeclared; if no
validator is registered, its value passes through unchanged. Name selection is
distinct from value validation: a declared field carrying an invalid value
still fails before target I/O.

**One warning per source, deliberately.** Because it fires during parse and
parse runs per overlay, the same malformed name present in both the TOML and the
environment warns twice. That is correct rather than noisy: those are two
separate mistakes in two separate places, and each message names its own
location. Deduplicating by field would hide one of the two places the user has
to go fix.

Note this multiplies only on the *name* path. A declared field carrying a bad
*value* raises instead of warning, so it stops at the first offending source and
cannot multiply.

**Repetition across calls is the real noise risk, and Python already handles
it.** `resolve_config` runs at every ownership boundary, so a stale
`BROKER_cache_mb` in the environment would otherwise warn on every Queue
construction. The default warning filter shows a given warning once per unique
message and code location, which suppresses that flood — provided the message
stays stable for a given (source, name, field) triple. So the message must not
carry volatile data (a timestamp, an object id, a temp path that changes per
call), or it defeats the registry and the flood returns. That is a real
constraint on the message text, not an implementation detail.

This costs nothing extra. Passing through undeclared but well-formed keys
already requires iterating the supplied mapping — you cannot `get()` a name you
do not know — so the loop exists regardless and the near-miss is one condition
inside it. An earlier draft of this plan called the warning "added machinery
requiring a scan the loader does not do today"; that was wrong on both counts.

**Undeclared keys.** A well-formed selected key that is not declared is
preserved unchanged; declaration supplies a default and a validator, not an
allowlist. There is no extras carrier and no separate category.

**Freezing** is light: enough to prevent accidental mutation, not a fortress.
`Config` is built from a plain mutable dict and returned read-only.

**Transport** carries `config.prefix` beside an ordinary mapping copy. The
receiver namespaces those values and supplies its field declarations to
`resolve_config(prefix, defaults=fields, override=payload)`. Stored values
round-trip in their declared units.

### Extension

Embedders copy the frozendict and edit:

```python
fields = dict(simplebroker.DEFAULT_CONFIG)
fields["CACHE_MB"] = replace(fields["CACHE_MB"], default=20)


def non_negative_days(value):  # the embedder's own, not published by core
    parsed = int(value)
    if parsed < 0:
        raise ValueError("must be zero or more days")
    return parsed


fields["RETENTION_DAYS"] = ConfigField(7, "days", non_negative_days)
config = resolve_config(
    "WEFT", defaults=fields, env=os.environ, override=namespaced_overrides
)
```

The bound lives in the embedder's validator because no core field needs it. Core
publishes the shapes it actually uses; an embedder wanting a narrower rule writes
it, which is the extension point working as intended rather than core carrying
speculative helpers.

`resolve_config` never mutates what it is handed — not `defaults`, not `env`,
not `os.environ`, not the file or `override`. No `deepcopy`: a
shallow copy plus frozen field records is sufficient.

The minimal embedder set is published through the package root:
`resolve_config`, `Config`, `DEFAULT_CONFIG` and `ConfigField`. That set is
determined by what an embedder needs in order to declare a field and resolve a configuration — nothing more. Consumers import from
`simplebroker`; `_constants` is where things are defined, not where they are
imported from.

## Invariants

1. **One path.** One defaults table, one resolution function, one object. A
   second validation path, a second field registry, or a per-field special case
   is a stop condition, not a detail.
2. **One validator and one unit per field.** The same input through toml, env,
   override yields the same stored value. Units are declared; nothing
   infers a unit from a Python type; no bound is invented.
3. **One stored value per field, uppercase and unprefixed, no aliases.**
4. **External input is namespaced and well-formed.** `PREFIX_` plus
   `[A-Z][A-Z0-9_]*`. Nothing else is read.
4a. **External name selection never fails, and undeclared well-formed names are kept.**
   Malformed, near-miss and other-prefix names are ignored (the near-miss one
   warns first). A **well-formed namespaced name that is simply undeclared is
   preserved as-is** — declaration supplies a default and a validator, not an
   allowlist, so declared and undeclared well-formed names travel the same path
   and differ only in whether a validator runs. Neither raises. Value validation
   is the separate path that does fail: a declared field with an invalid value
   still raises before target I/O.
5. **No access wrappers.** A read site is `config["CACHE_MB"]`. If a step needs
   a helper call at a read site, stop.
6. **Unit conversion happens at the point of use**, not in storage.
7. **`resolve_config` mutates nothing** it is handed. `env=None` means no
   environment, never an implicit `os.environ` read.
8. **Existing failure order survives**: each overlay validates as applied, and
   the CLI keeps its preparse exit-1 behavior including `--help`/`--version`.
9. **A supplied `Config` is retained, not rebuilt.** Every config-consuming
   constructor and every lazy acquisition uses the object it was given, without
   re-resolving or sampling the environment. The old snapshot functions may go;
   this guarantee may not.
10. **Session identity semantics are unchanged** — whatever the object holds is
   what identity uses. No subset keying is introduced.
11. **Backend API stays v9.** Plugins receive the resolved configuration; the
    handshake, factories and signatures do not change.
12. **Hidden coupling: extensions.** Both first-party extensions read
    configuration and import from `simplebroker._constants`; they are updated in
    the same change (engineering principle §11).
13. **Hidden coupling: spawn.** Configuration is reconstructed in a child
    process through a namespaced `override`; that path is real, not mocked, in its proof.

## Proposed Spec Delta

Strategy **A**, in-file text-first, replacing the exports bullet and the "Shared
configuration builder" section added by `f213b12` in
`docs/specs/16-python-library-api.md`, together with the `[SB-API-2]` paragraphs
describing `ResolvedConfig`, `resolve_config`, `snapshot_config` and
`resolve_isolated_config`. `docs/specs/10-cli.md` is re-checked; its preparse
ordering text is preserved.

> - **`ConfigField`**, **`Config`**, **`DEFAULT_CONFIG`**, **`resolve_config`**
>   declare fields, select one external
>   namespace, and resolve one configuration object.
>
> `DEFAULT_CONFIG` is an immutable mapping of uppercase unprefixed field name to
> `ConfigField`, each declaring a default, its unit, one validator and a
> sensitivity flag. Embedders copy that mapping, replace defaults or validators,
> and add their own fields. A mapping of only application fields is valid.
>
> `resolve_config(prefix=None, *, base=None, defaults=None, toml=None,
> env=None, override=None)` copies defaults or base values and applies toml,
> environment and override in that order, lowest to highest, coercing
> and validating each value as it is applied, and returns one read-only
> `Config`. An invalid lower-priority value still fails even when a
> higher-priority source would replace it. After initializing defaults and
> after each complete source overlay, one whole-config function checks
> applicable combined constraints and applies the location fallback before
> processing the next source. Checks skip missing broker fields, so
> application-only defaults remain valid. `env=None` means no environment
> input, which is how an ambient-free configuration is built. The function
> mutates no input.
>
> External input is namespaced and well-formed: the environment and a supplied
> TOML root table are selected by the prefix followed by an uppercase field
> suffix. Other prefixes, unprefixed names and non-conforming suffixes are never
> read. A warning is emitted during parsing when a non-conforming suffix matches
> a declared field once uppercased, identifying the source, the supplied name and
> the field it resembles; the value remains ignored. Parsing an external name
> never fails for env/TOML. Override uses the same external names and value
> validators but raises for bare, wrong-prefix and malformed names.
>
> `Config` stores one value per field under its uppercase unprefixed name and
> exposes no aliases; a prefixed or case-variant lookup raises `KeyError`. A
> well-formed selected key that is not declared is preserved unchanged.
> Transport carries prefix and ordinary values separately, reconstructed as a
> namespaced `override` with receiver-owned field declarations.
>
> Each field declares one unit and one validator, shared by every source, so one
> input value has one stored meaning regardless of origin. Stored values are in
> the declared unit; code requiring a different unit converts at the point of
> use. `VACUUM_THRESHOLD` is a percentage between 0 and 100.
>
> Existing snapshot events, lazy acquisition, error types and metadata, plugin
> API version remain unchanged. Namespace and field declarations join resolved
> values in session identity, so derived overrides retain their meaning.

## Tasks

1. [x] **Failing test first, then inventory.** Prove the `vacuum_threshold`
   divergence fails on `f213b12`; add a protective `jitter_factor` test that
   passes. Re-run the consumer inventory and classify every hit.

2. [x] **Spec-promotion slice.** Apply the delta; re-check `10-cli.md`; update
   the `[SB-API-2]` verification row, implementation links and backlinks. Run
   `python3 bin/check-dom15-fixtures`, `bin/check-plan-context`,
   `bin/check-doc-paths`, `git diff --check`.

3. [x] **Name the constants.** Move all 32 defaults to named, typed constants in
   `_constants.py`; add `ConfigField`, private field validators and the
   `DEFAULT_CONFIG` frozendict there. **Stop if** `_constants.py` needs to
   import from `config.py`.

4. [x] **Write `resolve_config` and `Config`.** One function, one object, light
   freeze, per-overlay validation, the whole-config check after each complete
   source (skipping absent broker fields), namespaced and shape-gated selection,
   the near-miss warning. Task 1's vacuum test goes green. **Stop if** a second
   validation path appears, a field gains two validators, or a unit is inferred
   from a type.

5. [x] **Delete and migrate.** Remove every name in the Deleted list; convert
   every wrapper call site to a direct `config["KEYNAME"]` read; apply the
   `/100` at each `vacuum_is_eligible` caller; repoint in-repo and test imports
   off the `_constants` shim and delete it; update both extensions. Backend API
   stays v9. The criterion is mechanical — `rg 'canonical_config|legacy_config'`
   returns nothing — not a count; hand-maintained counts drift from code
   (`docs/lessons.md`, 2026-08-04). **Stop if** any read site still needs a
   helper call.

6. [x] **Tests, docs, structure check.** Rewrite `tests/test_config_builder.py` and
   `tests/test_config_coexistence.py`; add every acceptance row. Update
   `docs/guides/configuration.md`, `docs/guides/python.md`, `README.md`,
   `CHANGELOG.md`, examples and implementation docs 06/07 — all reference
   removed names. Fire a check per row of the one-way table. Full gates and
   final independent review.

## Testing Plan and Acceptance Matrix

| Contract element | Required proof |
|------------------|----------------|
| One meaning per source | `VACUUM_THRESHOLD` is a percentage everywhere: `10`, `"10"` → 10% and `0.1` → 0.1% identically through toml, env and override. A value outside 0-100 is rejected; no other bound is invented. `vacuum_is_eligible` still fires at the documented point after the callers' `/100`. |
| Fractional meaning preserved | `JITTER_FACTOR` stays a fraction and stays consistent across all sources — a protective guard, not a repair. |
| Units and round-trip | Every stored value is in its declared unit; Transporting the prefix with `dict(config)` and rebuilding a namespaced `override` reproduces the same configuration with no repeated conversion; no validator branches on Python type to pick a unit. |
| Precedence and failure order | `defaults/base < toml < env < override`, including omitted vs `False`/`0`/`""`/`None`. An invalid lower-priority value fails even when a higher-priority source would replace it, with the correct external key and source. Every source label fires. |
| Combined-constraint failure order | Env `BROKER_PROJECT_CONFIG_PATH=dir` plus `BROKER_PROJECT_CONFIG_NAME=sub/file.toml` raises before override can replace the name with `.broker.toml`. A valid pair supplied together in one source is checked only after both keys are applied; reversing mapping order does not change the result. |
| Application-only defaults | Resolve a defaults mapping containing only `RETENTION_DAYS` through all sources. Missing broker fields neither raise nor get injected. Also exercise partial broker mappings: the combined check runs only when both path fields exist, and the location fallback only when its field exists. |
| Keys and aliases | `CACHE_MB` is the only spelling that resolves; `BROKER_CACHE_MB`, `WEFT_CACHE_MB` and `cache_mb` raise `KeyError`. Registering a non-uppercase field name fails at declaration. |
| External name shape | `BROKER_cache_mb` and `BROKER_Cache_Mb` are not read from env or TOML: the field keeps its default and no stray entry is created, matching measured pre-change behavior. A well-formed unknown suffix passes through unchanged. |
| Near-miss warning | Exactly one `UserWarning` when a non-conforming suffix uppercases to a declared field, raised during the parse; none for unrelated `PREFIX_*` names, well-formed unknowns, other prefixes, or conforming names. The message names the source (environment, or the TOML path), the name as supplied, and the near-miss field; a sensitive field's value is redacted. Nothing reaches stdout. |
| Warning multiplicity | The same malformed name in both TOML and environment warns **twice**, each message naming its own source. Two different malformed names from one source warn twice. A declared field with a bad value raises at the first source instead of warning, so the failure path cannot multiply. |
| Warning repetition | Under the default warning filter, repeated `resolve_config` calls with identical inputs do not produce a growing warning count — the message is stable for a given source/name/field and carries no volatile data. Assert without forcing `always` filtering, which would defeat the registry the test is checking. |
| Parse never fails on a name | No external name — malformed, near-miss, unknown prefix or undeclared — raises. Asserted for env and TOML alike. A declared field with an invalid *value* still raises before target I/O, proving the two paths stay separate. |
| No spooky action | After `resolve_config`, the caller's `defaults`, `base`, `env`, `override` and `os.environ` are unchanged byte-for-byte; `DEFAULT_CONFIG` rejects mutation; two builds from one copied mapping do not affect each other. |
| Light freeze | Assignment, deletion and in-place update of a returned `Config` fail — the operations ordinary code reaches for by accident. The list is illustrative, not a completeness claim: scope is accident prevention, not adversarial containment, so a determined caller getting through is not a defect. |
| Embedder extension | Copy `DEFAULT_CONFIG`, change one default, add `RETENTION_DAYS` with a shared validator; exercise default/toml/env/override, reject invalid input, pass to a real `Queue`/watcher, preserve across a real spawn — no core edit, no plugin registration. |
| Snapshot retention (ownership) | A `Config` passed into `Queue`, a watcher, or any config-consuming constructor is **retained as given**: not rebuilt from `DEFAULT_CONFIG`, not re-resolved, and not re-sampled from the environment. Asserted with `os.environ` poisoned with conflicting values after the object is built and before the handle is constructed, and again at lazy resource acquisition — the values the handle uses must be the ones it was handed. This is the [SB-API-2] ownership guarantee that the deleted `snapshot_config`/exact-instance fast paths implement today; deleting those functions is fine, losing the behavior is not. |
| Isolation without a second function | `resolve_config(env=None)` produces an ambient-free configuration with a poisoned `os.environ` present, proving no implicit read. |
| CLI | Installed entry points retain help/version invalid-env failure order, exit classes, option placement and sticky JSON; no traceback and no target writes on config errors. |
| Backend compatibility | v9 handshake, PG pool and Redis timeout behavior unchanged; both extensions read configuration through the new object with no new core import. |
| Session identity | Existing identity and non-repr regressions still pass. |
| One way to do it | A firing check per row of the one-way table: no second configuration module, field registry, resolution entry point, or config type; no field with both a parser and a validator; no wrapper/alias/second spelling that resolves; no second isolation function; no schema type or derive operation. |

Anti-mocking: never mock the resolver, `tomllib`, prefix selection, queue
storage or process spawn in the principal proof. Fault-inject only file-access
failures and hostile values. Temp paths and managed backend fixtures; no
production DSNs.

## Verification and Gates

```bash
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

```bash
uv run pytest tests/test_config_builder.py tests/test_config_coexistence.py tests/test_isolated_config.py tests/test_invalid_config_lifecycle.py tests/test_connection_config.py tests/test_project_config.py tests/test_process_broker_session.py tests/test_python_library_api_contract_sb_api.py tests/test_public_surface.py
uv run pytest tests/test_cli_main.py tests/test_cli_contract_sb_cli.py tests/test_cli_rearrange_args.py tests/test_watcher.py tests/test_backend_plugin_resolution.py
uv run ./bin/pytest-pg
uv run ./bin/pytest-redis
uv run pytest
uv run pytest examples -q
uv run ruff check .
uv run mypy simplebroker bin/release.py
```

Plus the extension and example mypy enumerators and packaging checks in
`CONTRIBUTING.md` and `bin/release.py`. Root suite, managed PG and managed
Valkey must all pass. Re-run the wheel-install proof and record artifact
versions, interpreter and imported `__file__`.

**Rollback:** `git revert`. Storage formats are untouched and `f213b12` is
unpushed, so nothing is externally live at authoring time.

## Independent Review Loop

Read-only Claude via `skills/call-agent/SKILL.md`, 540-second bound, stdout and
stderr retained; a separately tasked Codex reviewer if unavailable. Review after
task 4 (the new core) and task 5 (the deletion and migration), and once before
closure. Task 6 is documentation and does not earn its own round.

Brief: verify there is exactly one defaults table, one resolution function and
one object; that each field has one validator and one declared unit; that no
unit is inferred from a type and no bound was invented; that no access wrapper
survives; that external selection is namespaced and shape-gated; that the CLI
failure order and backend v9 handshake are intact; and that every row of the
one-way table holds. Judge organization and structure, not size. Report
ID/severity/location/disposition and answer PASS/BLOCKED.

## Out of Scope

Weft and Taut migration (owner's, separately); publication; backend API changes;
pooling or session-identity redesign; new CLI flags or config-file discovery;
changes to project-target TOML parsing or its precedence; renaming queue,
protocol or persisted keys; storage migrations; reinstating derived defaults or
a schema type; compatibility shims, prefixed views or deprecations.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| [SB-API-2] | `resolve_config` takes `toml`, `env`, `args` and `override`; `args` carries parsed CLI/API values. | No `args` parameter. CLI argv parsing and validation stay in `cli.py`. | Most arguments are per-call options, not global configuration (owner; Namespace ownership revision). | Promoted in [SB-API-2] |
| [SB-API-2] | `override` uses uppercase unprefixed keys. | `override` uses namespaced `PREFIX_KEY` names; bare, wrong-prefix and malformed names raise `ValueError`. Valid custom names remain accepted. | One external naming rule for every source, strict for programmatic input. | Promoted in [SB-API-2] |
| [SB-API-2] | No derivation parameter; transport rebuilds through `args`. | `config=` with `override` derives from an existing Config (an interim `base=` parameter was removed). `Config` carries `prefix` and its field declarations, which a derived config inherits. | Derived configs keep their namespace and validators without restating them. | Promoted in [SB-API-2] |
| [SB-API-1] | Shared validators are published at the package root. | Field validators are private. Config exports are `resolve_config`, `Config`, `DEFAULT_CONFIG` and `ConfigField`. | Embedders reuse a field record or supply their own validator; core does not carry speculative helpers. | Promoted in [SB-API-1] |
| [SB-API-2] | Session identity semantics are unchanged. | Session keys include the namespace and field declarations as well as values. | A shared session must not substitute another namespace or validator table. | Promoted in [SB-API-2] |
| [SB-API-2] | Supplied mutable containers are detached at session acquisition. | Config is read-only at its top level only; mutating nested containers is unsupported. | Owner decision (Remaining findings): keep the shallow freeze, no recursive copying or freezing. | Promoted in [SB-API-2] |
| [SB-API-2] | A supplied Config is retained; plain mapping inputs are unspecified. | Consumer `config=` parameters take `Config`; callers resolve mappings explicitly. | Owner follow-up simplification removed consumer-side mapping normalization. | Promoted in [SB-API-2] |
| [SB-API-2] | Transport sends `dict(config)` and rebuilds it as `args`. | Transport sends `config.prefix` and the values; the receiver rebuilds with a namespaced `override` and its own field declarations. | Follows the removal of `args`; callables are never serialized. | Promoted in [SB-API-2] |

## Review Log

| Review | Finding | Disposition |
|--------|---------|-------------|
| Prior external review H1-H7 | Two registries, 112 read wrappers, seven prefix-strip copies, `derive()` cannot express its own worked example, private-name shim keeps the migration open, +54% machinery. | Superseded by this design: one table, one function, one object — deleted rather than reorganized. |
| Owner, ratio rule | "String is percent, number is ratio, reject >1" breaks existing behavior: numeric `10` is `0.1` today. | Withdrawn. Replaced by declared units per field; `VACUUM_THRESHOLD` is a percentage 0-100. |
| Owner, jitter | `jitter_factor` was wrongly paired with `vacuum_threshold`. | Corrected — measured consistent at `15.0` through all four paths. Its test is protective. |
| Owner, extras | "`config.extras`? Who says you need it? Simpler." | Carrier deleted. With uppercase unprefixed keys and exact stripping, undeclared keys are ordinary entries; there is no extras category. |
| Owner, aliases | Namespaced lookups contradicted the alias deletion. | All alias branches removed; `Config` raises `KeyError` on prefixed or case-variant lookup. |
| Owner, freeze | Freeze omitted `__delitem__`/`__ior__`; a mutable builder producing a read-only mapping is simpler. | Adopted, and the eight-mutator enumeration dropped as a fortress that was not asked for. Freeze is accident prevention only. |
| Owner, dict fast paths | The justification overstated the problem; behavior is per-operation. | Conceded. Required behavior is specified and tested per operation. |
| Owner, external capitalization | Values whose suffix does not match the capitalization rule are never read. | Adopted; measured zero compatibility cost, since exact-name lookup already ignores them. Supersedes the reviewer's "did you mean" proposal. |
| Owner, six contradictions | Aliases, warnings, compatibility, the V1 conditional, `should_vacuum`, and the collision rule stated two ways. | All corrected; the collision rule is moot under the final design. |
| Owner, design | One defaults table in `_constants` carrying metadata and validation as a frozendict; one `resolve_config` applying toml/env/args/override; one object. | This plan. |
| Owner review, unknown keys | Invariant 4a said undeclared external names are "always ignored", contradicting the design's preservation of undeclared well-formed namespaced keys. | Fixed: malformed/near-miss/other-prefix are ignored; well-formed undeclared names are preserved. Declared and undeclared travel the same path and differ only in whether a validator runs. |
| Owner review, validator inventory | The claim that no field validates relative paths was false — four path validators exist, `LOAD_MAX_FUTURE_SKEW_SECONDS` rejects negatives/booleans/non-integers, and `PROJECT_CONFIG_PATH`+`PROJECT_CONFIG_NAME` share a combined constraint that one-validator-per-field cannot express. | Corrected with a measured table of surviving constraints. The cross-field check and `DEFAULT_DB_LOCATION` warn-and-blank fallback use one explicit whole-config function. Follow-up review corrected its timing: run after initializing defaults and each complete source overlay, before the next source; skip conditions whose broker fields are absent. Named as a function with a couple of conditions, with a stop condition if it ever grows a mechanism. |
| Owner review, examples | Both examples used `non_negative_int`, a helper the prose had just withdrawn, and the extension example never said whose it was. | Defaults example uses `integer` and gains a path-validated field; the extension example defines its own `non_negative_days`, with a note that narrower bounds belong to the embedder rather than to core. |
| Owner, snapshot ownership | Passing an existing `Config` into `Queue` or a watcher must retain it — no rebuild from defaults, no environment sampling — and needs a direct acceptance test. | Added as invariant 9 and an acceptance row asserting retention with `os.environ` poisoned after construction and again at lazy acquisition. Deleting the old snapshot functions is fine; losing the [SB-API-2] ownership guarantee is not. |
| Owner, near-miss warning | It stays; it fires on parse, explains where the bad value is, and the parse never fails. | Adopted. Also corrects the audit finding below: the warning is not added machinery, because passthrough of undeclared well-formed keys already requires iterating supplied names, so the loop exists regardless. |
| Self-audit for fake targets, brittleness and complication (owner-requested) | Six findings against this plan's own text: an invented validator taxonomy contradicting its own invariant; a hand-maintained "112 call sites" criterion; line-number pins in normative text; an untestable freeze row; four review rounds for six tasks; and the near-miss warning as the only added machinery — the last of which was itself wrong. | Five applied: validators renamed to what the 32 fields measurably need, with the invented-bounds error recorded; the migration criterion is now mechanical (`rg` returns nothing) rather than a count; line pins confined to dated Measured Baseline observations; the freeze row names the operations checked and disclaims completeness; reviews reduced to tasks 4, 5 and closure. The warning is raised separately as an owner decision. |
| Owner, consumers | "I will deal with weft and taut" — their `_constants` is being rebuilt to match — and there are **no third-party embedders**. | No compatibility shims, prefixed views or deprecations. The reviewer's third-party concern is withdrawn as factually inapplicable, and the minor designation stands. |

| Follow-up review, source boundaries and custom fields | End-only combined validation would allow a valid override to hide invalid ambient paths; parse wording still conflated undeclared with malformed names; application-only defaults require missing-field guards. | Applied directly: whole-config checks run after each complete source, malformed/other-prefix names are distinguished from retained custom names, and missing broker fields are skipped. Acceptance cases added. |

## Execution Log

- Implementation authorized by the owner. Comprehension check: an existing
  Config is retained at every ownership seam; only a fresh boundary explicitly
  supplies ambient env. Cross-field checks run after a whole source, before
  later overrides, and skip absent broker fields. Target TOML remains a separate
  parser. Config percentage values convert only at maintenance callers.
- Baseline regression reproduced before edits: legacy numeric vacuum 10 gave
  0.1, new builder numeric 10 gave 10.0; equality assertion failed as expected.
- Baseline facts in Measured Baseline were obtained by execution against
  `f213b12` before implementation. Implementation and subsequent verification
  are recorded below; changes remain uncommitted and closure is pending.

### Explicit config argument (2026-09-12)

Owner decision: an existing Config is passed explicitly as `config=`, not inferred
from the type of `override`. `resolve_config(config=c)` returns `c` without reading
TOML or the environment; with a non-empty `override` it derives exactly as `base`
does. A non-Config `config`, or a non-mapping source, raises `TypeError` (the old
`assert` gave a bare `AssertionError` and vanished under `python -O`). `config`
with `base`, or with a contradicting `prefix` or `defaults`, raises `ValueError`.
Not yet done: rewriting the ownership and per-call seams to use `config=`, removing
the now-overlapping `base` parameter, and the proposed `env=os.environ` default with
its follow-ups. The session-sharing guide sentence now leads with declaring custom
fields once, since override-only configs already share sessions (measured).

### Seams use config= and base is removed (2026-09-12)

Owner decision: every ownership seam passes a supplied config through
`resolve_config(env=os.environ, config=config)`, and every per-call seam uses
`self._config if config is None else resolve_config(config=config)`, so a non-Config
fails with `TypeError` at the boundary. The public `base=` parameter is removed;
`config=` with `override` is the only derivation path, and a contradicting `prefix`
or `defaults` raises `ValueError`, so prefix rebinding on derive is gone. A
structural test forbids reintroducing the inline `... if config is None else config`
seam. The prefix-rebind test was deleted with that behavior; its replacement
coverage is `test_config_argument_rejects_contradicting_inputs`.

### Only the process entry reads the environment (2026-09-12)

Owner decision, on the Unix model (`setlocale(LC_ALL, "")` is the program's
opt-in, not the library's): `BROKER_*` variables are the `broker` command's
interface. `cli.main()` reads them once with `resolve_config(env=os.environ)`;
every library seam uses `resolve_config(config=config)`, so a handle or `cmd_*`
call without a Config uses defaults. Python programs opt in once at startup and
pass the Config down. Per-handle environment sampling tests were replaced by one
test that config-less handles ignore the environment. The PostgreSQL test
harness now supplies its password through libpq's `PGPASSWORD`. Rejected: a
process-global first-use cache (hidden state in the library).

### Invalid values raise only when final (2026-09-12)

Owner decision: every supplied value is still validated, and each invalid value
warns as its source is applied; a later valid value replaces it. Resolution
raises `InvalidConfigError` only if an invalid value remains after all sources,
naming its source.
The combined project-path check runs once on the final values. A relative
`DEFAULT_DB_LOCATION` became an ordinary invalid value (its validator rejects
it), removing the one warn-and-blank exception. This reverses the follow-up
review's "a valid override cannot hide an invalid lower-priority value": the
warning keeps the value visible, and with one environment read an overridden
value has no other effect.

### `_constants` imports only the standard library (2026-09-12)

Owner decision: `BrokerError` and `InvalidConfigError` are defined in
`_constants`, which raises them during resolution; `_exceptions` imports
`BrokerError` as the base for the other package errors. Internal imports of
`InvalidConfigError` use `_constants`; `simplebroker.ext` still exports both, so
public import paths are unchanged. Config test modules ignore only the
"ignoring invalid" warning and assert it or the raise explicitly.

## Fresh-Eyes Review

Before handing off: every named existing surface exists and every new surface is
labelled proposed; the delta agrees with the tasks; no step silently changes
target precedence, session identity, plugin access or CLI failure ordering.
Confirm specifically that there is one defaults table, one resolution function
and one object; that no field ended with two validators or an inferred unit;
and that no read site regained a wrapper. Check the one-way table row by row:
each has exactly one way, and each way has a firing check. This plan stays
`draft` until adopted.

### Implementation review evidence

- Core slice independent review found shared mutable defaults. Fixed with a
  shallow copy of each default before validation; independent re-probe confirms
  mutating one list default does not alter another build or the field record.
  This follows the light-freeze scope, without a deep-copy framework.
- Independent core review passed real TOML/custom selection, app-only defaults,
  precedence, failure metadata, default-filter warning repetition, and the
  combined path failure before a later override.
- Independent consumer review passed Config identity through Queue/watcher,
  DBConnection, core, runner, session, and PG/Redis handoffs. A real SQLite
  write/read succeeds with hostile ambient settings introduced after Config
  construction; custom fields and object identity are retained. Vacuum callers
  divide percentage by 100; plugin handshake stays v9.
- Source import and `uv run mypy simplebroker bin/release.py` passed. Full
  integration and artifact verification are recorded below when completed.

### Interface review (CLI and embedding guide)

Principles 1–6: met by compact existing CLI diagnostics, progressive guide
examples, uppercase field names, one Config identity, field defaults, and
explicit resolver env input. Principle 7: owner-ratified departure from automatic
case correction; near-miss warning teaches the exact spelling and custom fields
pass through. Principle 8: diagnostics identify source/key/expected form.
Principle 9: not applicable to read-only config loading; target writes remain
behind successful resolution. Principles 10–11: external namespace selection is
explicit and units match the documented input, with function conversions at use.
No new runbook candidate. Final CLI acceptance gates remain part of task 6.

### Verification and review before the follow-up simplification

- Full root `uv run pytest -q` passed (exit 0), with the platform/opt-in and
  optional downstream skips reported by the harness. Two added raw-input
  ownership cases and the strengthened sensitive-env case also passed in a
  focused follow-up. Root test collection includes the complete local suite.
- Managed PostgreSQL passed: 1,628 shared tests and 319 extension tests;
  expected skips 12 and 7. Managed Valkey passed: 1,620 shared tests and 313
  extension tests; expected skips 20 and 1.
- Examples passed (140 tests). Product/release mypy passed (45 files), as did
  the release driver's root-test (217), example (16), PG-test (26) and
  Redis-test (19) partitions. Ruff, formatter, suppression-registry check,
  DOM-15 fixtures, plan-context, doc-path and diff checks passed.
- Python 3.11 packaging smoke passed for core 8.1.1 and extension 4.1.1
  artifacts. All 63 builder/coexistence tests passed in a clean Python 3.11.15
  environment importing the built core wheel from site-packages. The wheel's
  constants, Queue, DB and watcher source bytes match the verified tree.
- Final independent scoped integration review: PASS, no remaining blocker.
  It reran builder/coexistence, connection-config and recursive session tests,
  and verified subclass/Config identity, ordinary mapping detachment, percent
  conversion, custom values, spawn transport and deletion of the old machinery.
- The additional Claude review was bounded at 540 seconds and timed out with
  no verdict (exit 124); it is not counted as a pass. Separate independent
  in-session reviews supply the completed review evidence.
- The optional local Weft regression skips with its incompatible-import reason
  because the sibling still imports removed configuration APIs. No downstream
  code changed; the plan explicitly assigns that migration to the owner.
- Runtime follow-up from the full suite restored the existing recursive
  key-material copy at ordinary Queue/DB/watcher capture boundaries. Explicit
  Config objects remain retained by identity under the light-freeze contract.
- Removed the new complexity suppression by extracting only external-name
  selection and TOML reading helpers. The resolver retains one source loop and
  one validator path; the existing suppression registry passes unchanged.
- Documentation now includes the agent kernel and repository map as well as
  the canonical contracts and guides. Used skills/runbooks need no new rule:
  the ownership and source-order checks already express the lessons here.

### Follow-up simplification (2026-09-12)

The owner's eight findings supersede the consumer-side ordinary-mapping
normalization described in the preceding verification record. Consumers accept
`Config`; public optional boundaries call `resolve_config(env=os.environ)` only
when no config is supplied. Internal helpers require `Config`. A per-call
config replaces the inherited snapshot directly. Callers wanting an overlay
build it explicitly with `resolve_config(base=base, override=changes)`.

Adopted: remove duplicate conversion branches and dead read-site defaults;
trust `SYNC_MODE` after its single validator; use built-in `int`/`str`/`float`;
keep field-specific validators private; make `SetupPhase` own its enum values;
delete unused time constants; validate built-in table names in a test and only
check caller-supplied tables at runtime; repair the queue-name constant's
docstring. No new warning is added for the existing silent invalid-sync
fallback. The public config export set is the four names above.

One evidence claim was inaccurate: a PG `verify_env` test supplies a partial
mapping. It must explicitly resolve its input under the new contract. Redis
fallbacks that express backend-specific defaults remain valid and are not dead
read-site fallbacks. Tests and docs must prove explicit snapshot replacement,
identity retention and caller-owned overlays rather than implicit mapping
normalization.

Follow-up verification:

- Full root pytest passed; full extension tests passed with live-service cases
  skipped in that run. Managed Valkey then passed 1,619 shared tests and 313
  extension tests (20 and 1 expected skips).
- All example tests passed. Product/release mypy (45 files), root-test (217),
  examples (16), PG-test (26) and Redis-test (19) partitions passed. Ruff,
  formatting, suppression registry, DOM-15 fixtures, plan-context, doc-path and
  diff checks passed.
- Python 3.11 packaging smoke passed. All 64 builder/coexistence tests passed
  against the installed wheel in a clean Python 3.11.15 environment.
- Independent review initially found missed benchmark producers, async-example
  wrappers and three project-discovery fallbacks. All were fixed. Final review
  passed with no blockers; its 49 targeted tests and five-file mypy check passed.
- Managed PostgreSQL passed 1,627 shared tests and 319 extension tests
  (12 and 7 expected skips).

The plan remains active and changes remain uncommitted; publication and
downstream migration remain outside this task.


### Remaining findings (2026-09-12)

Owner decision: mutation of containers inside a resolved Config is unsupported
and may result in unspecified behavior. Retain shallow freezing and Config
identity; do not introduce recursive copying or freezing.

Malformed `args` and `override` names raise `ValueError` with their source;
uppercase identifier syntax is required, while valid custom fields remain open.
Only external env/TOML selection strips a namespace. The two reported stale
callers were already migrated. Fix the persistence spec's obsolete loader API
references and the stale pre-implementation sentence in this log. Active status
means implementation is uncommitted and plan closure remains pending.

Cross-field errors continue reporting the overlay that first makes the combined
constraint invalid; specify that meaning and test both field orderings rather
than add per-field provenance. Reject booleans for `VACUUM_THRESHOLD` through its
single validator. Integers, floats and numeric strings remain accepted; booleans
and boolean strings do not become numbers. Full root pytest and focused
builder/coexistence, PRAGMA and load tests passed. Additional numeric-string
and boolean-string cases passed after the full run. Product/builder mypy, Ruff,
formatting, DOM-15 fixtures, plan-context, doc-path and diff checks passed.
Independent review passed with no blockers. Changes remain uncommitted.


### Namespace ownership revision (2026-09-12)

This revision supersedes earlier use of `args` as a programmatic mapping and
bare override keys. The resolver has no `args` parameter. Existing argv parsing
and validation remain in `cli.py`; no generic config flags are added and CLI
target precedence is unchanged. Programmatic calls use strict namespaced
`override`; wrong-prefix, bare and malformed names raise `ValueError`, while
selected custom names remain open. Env/TOML keep tolerant external selection.

`DEFAULT_PREFIX` is declared beside `DEFAULT_CONFIG`. Config retains read-only
`prefix` metadata and a private immutable field-declaration mapping, separate
from configurable values. `resolve_config(base=config, override=...)` inherits
both namespace and validators. Explicit prefix or defaults arguments replace
the inherited metadata; base values remain already resolved. Session keys include
the metadata so shared sessions cannot substitute another namespace or custom
validator table. Mutable custom values retain the previously accepted shallow
freeze/unsupported-mutation limitation.

Transport sends namespace and values separately; the receiving application
supplies field declarations and rebuilds through namespaced override. No callable
serialization or downstream repository migration is introduced. Tests must use
real Queue/session/spawn paths, assert custom validator inheritance, verify all
strict-name errors and tolerant external cases, and preserve CLI precedence and
preparse failure timing. Rollback remains reverting this uncommitted change;
publication is out of scope.

Namespace revision verification: full root pytest passed; live PostgreSQL passed
1,627 shared and 319 extension tests (12 and 7 expected skips); live Valkey passed
1,619 shared and 313 extension tests (20 and 1 expected skips). Example tests,
55 process-session tests and the focused CLI/path suite passed. All 105 config
builder/coexistence tests passed against the installed Python 3.11 wheel.
Packaging smoke, product/release and all test/example mypy partitions, Ruff,
formatting, suppression registry, documentation gates and diff checks passed.
Independent core/session review and the final CLI validation review passed with
no blockers. The plan remains active and all implementation changes uncommitted.


CLI validator reuse clarification: the existing early global value options are
`--dir` and `--file`. Both reuse `_validate_safe_path_components` from the config
module, now imported directly by `cli.py`. Full field validators cannot be
substituted: explicit `--file` permits absolute and nested paths while
`DEFAULT_DB_NAME` is restricted to a relative name of limited depth. Keep those
per-value rules and existing diagnostic timing; command/action flags continue
selecting functions and their arguments. No arbitrary mapping or raw argv input
returns to the resolver.
