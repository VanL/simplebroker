# Data-only Config transport

Status: completed
Class: 5 — additive public serialization API and a defined process-transport
format extend [SB-API-2]. Hardening applies because configuration crosses a
process boundary. No +P guidance change is proposed.
Plan type: implementation with spec revision; promotion strategy A.
Owner: SimpleBroker owns the helpers, local integration proofs, and documentation.
Boundary: this repository only. Weft detached-manager startup is an intended
use and read-only reference; no Weft work is authorized. Taut is a reference
consumer.

## Goal

Transport resolved Config values and their namespace as JSON, then rebuild a
Config using declarations and validators supplied by the receiving application.
Support ordinary trusted parent-to-child spawn arguments and JSON-based startup
without transporting callables in JSON, changing in-process Config resolution,
or adding an import registry or serialization framework. Owner extension below also adds ordinary
Python pickle support with declarations retained.

## Source Documents

- `docs/program-theory.md` [THEORY-2], [THEORY-3], [THEORY-4]: applications own
  process orchestration and field meaning; SimpleBroker supplies the primitive.
- `docs/specs/16-python-library-api.md` [SB-API-2]: Config values, namespace,
  receiver-owned transport declarations, resolver behavior and target transport.
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-10], [DOM-11], [DOM-15]: plan promotion, proof, review and closure.
- `docs/guides/configuration.md`: existing manual namespace/value reconstruction.
- `docs/implementation/06-process-session-core-ownership.md`: session identity
  includes values, namespace and field declarations; sessions stay process-local.
- `docs/plans/2026-09-12-config-object-simplification-plan.md`: accepted shallow
  freeze, custom fields, and later owner decisions on warnings/final validation.
- Process guidance: `docs/agent-context/runbooks/writing-plans.md`,
  `docs/agent-context/runbooks/hardening-plans.md`,
  `docs/agent-context/runbooks/testing-patterns.md`, and
  `docs/agent-context/runbooks/adversarial-acceptance-probes.md`.

## Spec Baseline

Baseline: `eccdb4b2b69917e9252a2bb2d4b494a2952619a2`, including
`docs/specs/16-python-library-api.md` and `simplebroker/_constants.py`.
This plan explicitly extends the spec. Record the promoted spec commit or
baseline-plus-spec-diff identifier after Task 1; implement against that text.

## Context and Key Files

| File | Current ownership and planned change |
|------|--------------------------------------|
| `simplebroker/_constants.py` | Config holds mapping proxies for values/declarations and a prefix. Add the two transport functions beside Config/resolution; reuse `resolve_config`, not a second field-validation path. |
| `simplebroker/__init__.py` | Export the two proposed helpers alongside the existing four config names. |
| `simplebroker/project.py` | Read only: existing `serialize_broker_target` returns JSON text and `deserialize_broker_target` accepts text or a mapping. Follow this API convention without merging target and config formats. |
| `tests/test_config_coexistence.py` | Real spawn receiver currently reconstructs a WEFT config from a plain values dict. Migrate that proof to the public transport helpers. |
| `tests/test_config_builder.py` | Existing resolver behavior and extension tests; preserve these contracts and update any exhaustive public-config-surface assertion. |
| `tests/test_public_surface.py` | Public import/export coverage. |
| `tests/test_config_transport.py` (new) | JSON type/shape, receiver validation, isolation, and subprocess transport proofs. |
| `docs/specs/16-python-library-api.md` | Promote the exact delta below, with verification links after tests exist. |
| `docs/guides/configuration.md`, `docs/guides/python.md`, `docs/agent-kernel.md` | Replace manual transport advice with sender/receiver examples; retain explicit receiver declarations. |
| `docs/implementation/06-process-session-core-ownership.md` | Explain why values travel but validators and process resources do not. |
| `docs/implementation/10-ruff-suppression-registry.md` | Register two reviewed TRY004 exceptions preserving the approved ValueError envelope contract. |
| `CHANGELOG.md`, `docs/plans/README.md` | Add the public capability and maintain plan status at closure. |

Measured baseline: direct pickle of Config fails on `mappingproxy`; JSON encoding
Config directly also fails. `dict(config)` is JSON-encodable for built-in values.
Neither failure requires changing Config's Mapping behavior. Trusted spawn uses
pickle to carry its ordinary arguments; passing a JSON string through spawn is
still normal trusted process transport. Repository membership is not the trust
boundary; control of bytes and their receiver is.

Read-only downstream reference: Weft's
`../weft/weft/core/manager_runtime.py` `_build_manager_process_command` JSON-encodes
`dict(context.config)` and base64-encodes it into argv; `../weft/weft/manager_process.py`
`main` decodes it; task construction reaches
`../weft/weft/_constants.py` `resolve_runtime_config`, which applies
`WEFT_CONFIG_DEFAULTS` without an ambient env read. No production detached-manager
config startup path exists in SimpleBroker. Do not invent one or edit these
sibling files. The local JSON subprocess proof demonstrates the transport shape;
actual launcher adoption and old-payload compatibility belong to Weft.

Before editing, the implementer records answers in the Execution Log:

1. Why not serialize `_defaults` or an import path? Expected: callables and field
   ownership stay local; the receiver selects declarations, never the payload.
2. Does reception return Config directly from the transmitted values? Expected:
   no, it validates through `resolve_config` with receiver defaults and strict
   namespaced overrides, with no env/TOML input. Wrong answers require rereading
   [SB-API-2] before implementation.

## Design and Proposed Spec Delta

Strategy A: replace the manual process-transport paragraph in [SB-API-2] with
this exact text, and add both helpers to its public-surface list:

> `serialize_config(config: Config) -> str` returns JSON text containing
> `{"prefix": config.prefix, "values": {...}}`. Values use the existing uppercase
> unprefixed Config names. The payload contains all resolved values, including
> custom fields and values equal to their defaults. The field-declarations
> table, validators, Python class identity, module/import paths and process
> resources do not travel. Export does not run field validators.
>
> Supported values are JSON null, booleans, integers, finite floats, strings,
> lists, and objects with string keys, recursively. Subclasses representing
> these JSON types may be accepted as their JSON value; custom attributes do not
> travel. Tuples, sets, bytes, datetime objects, arbitrary objects, and non-string
> object keys are unsupported. No stringification, tagged codec, or lossy
> key/tuple conversion is performed. Unsupported types raise `TypeError`;
> non-finite floats and cyclic containers raise `ValueError`. These restrictions
> apply only to transport, not to in-process Config values.
>
> `deserialize_config(payload: str | Mapping[str, Any], *,
> defaults: Mapping[str, ConfigField] = DEFAULT_CONFIG) -> Config` accepts JSON
> text or its decoded object. The envelope requires a string `prefix` and an
> object `values`; additional envelope keys are ignored and cannot select code
> or field declarations. Missing or incorrectly typed envelope members and a
> non-object root raise `ValueError`; unsupported values follow the same JSON
> type rules as export. Malformed JSON raises `ValueError` (including its
> standard JSONDecodeError subclass). The prefix follows existing resolver
> semantics; this transport adds no new namespace grammar.
>
> Reception prefixes each top-level value name with the transmitted namespace
> and passes the resulting mapping to `resolve_config(prefix, defaults=defaults,
> override=values)`, without environment, TOML, or an existing Config. Receiver
> declarations supply defaults, sensitivity metadata and validators. Built-in
> declarations are the default; applications supply their own extended or
> application-only table explicitly. Declared custom fields use the same
> validator path as built-ins; undeclared well-formed custom fields remain
> pass-through values. Invalid field names and values retain resolver error and
> warning behavior, including `InvalidConfigError` (a `ValueError` subclass)
> when receiver field validation remains invalid. This is distinct from
> transport-shape `TypeError` and `ValueError`. Missing declared fields receive
> receiver defaults. A
> reconstructed Config is a new ordinary Config, not a restored subclass or
> the sender's object. Later overrides use its receiver-owned declarations.
>
> For the same field semantics, built-in values and documented units round-trip
> unchanged. Custom validators run locally on received values and may normalize
> or reject them; applications own consistent declarations across processes.
> No cross-version compatibility or declaration-equivalence guarantee is added.
> In-process configuration and session ownership behavior are unchanged.
>
> The payload is lossless data transport and may contain credentials; do not log
> it or treat it as a redacted diagnostic. Transport-shape errors must not echo
> rejected values. Resolver validation retains its existing safe diagnostics.

API choice: the pair centralizes the envelope and JSON fidelity checks as well
as reconstruction; export is not included solely for naming symmetry. JSON
text matches the existing target-serialization pair, works as
one spawn argument, and can be decoded into an outer manager JSON object when
needed. Accepting a mapping on import avoids forcing an extra encode/decode in
an application that has already parsed an outer JSON envelope. Do not add
parallel `to_dict` or `to_json` methods. Ordinary pickle is added by the owner
amendment below.

Implementation boundary: use the standard `json` module, with
`allow_nan=False`, `sort_keys=True`, and `separators=(",", ":")` on export, matching
target transport's compact deterministic encoding. A single small private JSON
value check enforces string keys and supported types: stdlib encoding silently
converts tuple values to lists and integer keys to strings. Apply the same check
after decoding JSON text and to mapping input, since `json.loads` accepts NaN and
Infinity by default. If this check traverses containers before encoding, it
needs an active recursion stack so cycles fail and shared acyclic containers
remain valid. Do not duplicate stdlib encoding or build an encoder registry.
Do not impose speculative payload-size/depth limits or add a separate deep-copy
pass. Exported text already separates sender values from decoded payloads;
receiving a mapping retains the normal resolver's shallow-value ownership
contract. Neither helper mutates caller inputs. Structural validation remains
distinct from the single existing field-validation path.

## Invariants, Couplings, and Scope

- Existing `resolve_config(config=...)` identity, strict override names, warning
  and final-invalid-value behavior, and CLI-only ambient reads do not move.
- Do not silently redact secrets in transport; that would change configuration.
  Unsupported-value diagnostics identify the problem without dumping payloads.
- Receiving does not open a broker, load plugins, import a payload-selected
  module, access env, or discover TOML. Actual broker construction follows only
  after reception succeeds.
- Receiver declarations determine future override behavior and session identity.
  JSON transfers no parent session, lock, or callable identity. In-process
  mutable-container use remains subject to the existing unsupported-mutation
  contract; serialization does not become a concurrency snapshot guarantee.
- No auto-import field registry, schema identifier, format-version mechanism,
  new dependency, or new CLI flag. Ordinary pickle support is covered by the
  owner amendment below. Reconsider an
  identifier only if one real receiver must support multiple declaration sets.
- This is additive to released 8.2.0. Old manual transport remains possible.
  No Weft/Taut changes or manager rollout are part of this implementation.

## Tasks

1. [x] Promote [SB-API-2] text and record its baseline identifier. Add failing
   tests for public helper names, JSON envelope, and receiver validation.
2. [x] Implement the two helpers and export them. Exercise the type/error
   matrix below. Stop and revise if a second config validator path or custom
   codec/import system appears. Run an independent review of this coherent slice.
3. [x] Migrate the real spawn proof. Add a subprocess proof that reads JSON from
   stdin in a fresh interpreter, uses receiver-local declarations, constructs a
   real Queue, and returns observable results. This simulates JSON startup
   transport without claiming Weft launcher integration. Keep process cleanup
   and bounded waits explicit; follow existing process-test helpers.
4. [x] Update guides, kernel, rationale, public-surface tests and changelog.
   Run final gates and independent review; disposition all findings. Close the
   plan and its index row together in the authorized implementation commit.

## Acceptance Matrix

| Contract | Required firing proof |
|----------|-----------------------|
| Envelope and isolation | Prefix and all built-in values survive `json.loads(serialize_config(config))`; export has exactly prefix/values and no validator metadata. No caller mapping changes. A decoded payload mutation cannot change the sender Config. |
| JSON values | Nested null/bool/int/finite-float/string/list/object and Unicode round-trip; empty collections and false/zero values survive. Include an ordinary subtype without requiring arbitrary subclass preservation. |
| Unsupported values | Tuple, set, bytes, datetime, arbitrary object, non-string key, NaN, positive/negative infinity and cycles fail on export and decoded-mapping import; repeated non-cyclic references succeed. Failures do not disclose sentinel secrets. |
| Envelope errors | Invalid JSON, null/array/scalar root, missing prefix/values, non-string prefix and non-object values fail. Extra root metadata is ignored and never chooses declarations. |
| Field naming | WEFT and TAUT prefixes round-trip. Malformed internal value names fail after reconstruction; a valid custom name containing the namespace string is preserved without extra stripping. |
| Receiver ownership | A local validator rejects a value that was accepted by the sender. Local lambdas need not be serializable. Changing the receiver's declared default affects omitted fields; supplied fields retain resolver precedence. An application-only table does not inject broker fields. |
| Units and continuation | Percentages retain 0–100 units, booleans are still rejected as percentages, and numeric strings receive normal validation. A subsequent namespaced override invokes the receiver's custom validator and inherits its prefix. |
| Ambient independence | Poison receiver env with invalid/conflicting BROKER/WEFT values and provide no env/TOML input. Reception still uses only payload and receiver declarations. |
| Real spawn | Existing `test_spawn_round_trip_uses_declared_units` passes serialized JSON to a spawn child. The child imports local fields, checks prefix/custom values, writes/reads using a real SQLite Queue and observes the configured cache PRAGMA. |
| JSON startup | A fresh Python subprocess receives JSON on stdin and performs the same receiver validation before opening a real Queue. Invalid payload yields failure and no broker file. Always check child status and close/join resources. |
| Public/artifact | Helpers import from `simplebroker`; tests against the installed wheel prove no repository import dependency. Existing config/coexistence and public-surface tests continue passing. |

## Verification

Planning-only gates: `python3 bin/check-dom15-fixtures`,
`bin/check-plan-context`, `bin/check-doc-paths`, and `git diff --check`.

Implementation gates, targeted during iteration and full once stable:

```bash
uv run pytest tests/test_config_transport.py tests/test_config_builder.py tests/test_config_coexistence.py tests/test_public_surface.py -q
uv run pytest tests -q
uv run ruff check .
uv run ruff format --check .
uv run mypy simplebroker tests/test_config_transport.py tests/test_config_coexistence.py
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
uv run ./bin/packaging-smoke --python 3.11
```

The new test file is a proposed Task 1 artifact, not an existing command target
at planning time. Repeat its transport tests in a clean environment importing
the built wheel. PostgreSQL/Redis live suites are not required for a codec-only
change with unchanged backend consumers; if implementation touches those paths,
expand the gate accordingly. Do not mock JSON, resolver validation, spawn,
subprocess startup or SQLite in the integration proofs.

## Rollout and Rollback

No deployment or version bump is authorized by this plan. Implementation is an
additive library change; before release it can be reverted as a unit. After a
release, preserve published helper semantics and fix forward rather than remove
the API. Weft adoption must coordinate its own sender/receiver payload switch;
this plan does not promise compatibility between its current flat payload and
the new envelope. Success is a receiver using its own validators and the
transmitted namespace/values in a real child process with poisoned ambient env.
No monitoring daemon or new persistent format lifecycle is introduced.

## Independent Review and Review Log

An independent reviewer checks the plan's concrete paths, helper return types,
receiver ownership, JSON type/error policy, and downstream scope. During
implementation review after Task 2 and again after integration; record findings
and dispositions here. In-process review is sufficient; a separate agent family
is preferred when available without adding a new external workflow.

| Review | Finding | Disposition |
|--------|---------|-------------|
| Independent plan review, 2026-09-14 | No actionable findings. Verified current helper/spawn paths, receiver-owned validation, transport-only JSON restrictions, concrete spec delta and Weft-only-as-reference boundary. | PASS; no design changes required. |
| Cross-model review: Claude Opus 4.8 (`claude-opus-4-8`), 2026-09-14 | PASS; explicit overengineering verdict: “Not overengineered.” One medium wording issue: “no defaults” could imply omitting resolved values equal to defaults. | Corrected to transmit every resolved value while excluding the declaration table and validators. |
| Same cross-model review, JSON fidelity | Low: explain actual stdlib coercion gaps and check decoded JSON text too; export helper earns its place through fidelity rather than symmetry. | Added rationale and a single shared value check, with no codec registry or duplicate validation. Stdlib probes confirmed tuple/key coercion and NaN decoding. |
| Same cross-model review, copying | Low: avoid an extra deep-copy pass for transport isolation. | Adopted. Clarified that JSON text separates the sender; mapping-input reception retains normal shallow resolver ownership. Fresh outer dicts alone do not detach nested values. |
| Same cross-model review, errors/encoding | Low: name inherited InvalidConfigError explicitly; optional compact sorted JSON to match target transport. | Both adopted, without changing public error categories or adding another API. |

| Core implementation review, 2026-09-14 | PASS after correcting a reviewer assumption about mapping-input ownership. | No recursive copy added: the approved shallow ownership decision remains in force. |
| Final integration review, 2026-09-14 | PASS; independently ran transport, coexistence and public-surface tests. | No actionable findings or overengineering identified. |
| Suppression review, 2026-09-14 | Written TypeError alternative failed nine envelope contract cases. | Approved RUFF-SUP-038: two direct ValueError raises preserve the agreed contract; a lint-only helper would add indirection. |

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Execution Log

- Owner confirmed that Weft remains an intended use only; no Weft work belongs
  in this plan.
- Planning inspection confirmed Config's mapping proxies prevent direct pickle,
  built-in values can be JSON encoded, existing spawn transport is manual, and
  the production detached-manager JSON path is in Weft. No runtime changes are
  part of this planning task.

- Planning verification: DOM-15 fixtures, plan-context, doc-paths, plan code-block
  formatting and diff-whitespace checks passed. Independent plan review passed.

- Cross-model review completed using Claude Opus 4.8 in tool-less mode against
  the complete plan and supplied current source. Returned PASS with one required
  wording correction and low-severity clarifications; independently evaluated
  and dispositioned above. No implementation or Weft changes were made.

- Implementation authorized by the owner. Comprehension: declarations and import
  paths stay local because the receiver owns validators; reception invokes
  resolve_config with receiver defaults and strict namespaced overrides, with
  no environment or TOML input.
- Promoted [SB-API-2] before implementation: baseline eccdb4b plus spec file
  SHA-256 `c03ac3d9b6c241a435447878f1c5603b0f2d070082d379606fd7669825aea25c`.

- Implemented the two public helpers with one private JSON value check and the
  existing resolver. Receiver fields remain local; no framework, CLI, backend,
  Weft or Taut changes. Updated guides, kernel, ownership rationale and changelog.
- Added the transport matrix, real spawn migration and JSON-stdin Queue proof.
  Invalid receiver validation fails before broker-file creation. Targeted config,
  coexistence, public-surface and Ruff-policy tests pass. The 49 transport cases
  also pass against the installed Python 3.11 wheel in a clean environment.
- Ruff check/format, mypy (46 source files), packaging smoke (wheel, sdist and
  extension imports on Python 3.11), DOM-15, plan-context, doc-paths and suppression
  index checks passed. No version bump or release was performed.
- Initial full-suite run overlapped the spawn-test edit and caught unregistered
  TRY004 suppressions; both are corrected and their targeted tests pass. It also
  found the sibling Weft launcher attempting to pickle Config directly. The same
  mappingproxy TypeError reproduces from an untouched HEAD archive. That downstream
  integration is outside this plan; no test was weakened or downstream code changed.
- Stable full-suite verification passed (exit 0): `uv run pytest tests -q
  --ignore=tests/test_weft_sqlite_stop_corruption_regression.py`. Only the confirmed
  baseline downstream failure was excluded; normal platform/backend skips remain.
  Task 4 documentation, gates and final review were verified at the JSON checkpoint;
  the pickle amendment below subsequently passed the full suite without exclusions.

## Owner amendment: ordinary pickle support (2026-09-14)

The owner requested direct Config pickle support and explicitly selected
preserving field declarations with normal Python pickle. This supersedes the
original no-pickle scope. [SB-API-2] was promoted before this implementation.
The JSON contract and receiver-owned JSON validation remain unchanged.

Implementation: convert the two mapping proxies to dicts in ordinary pickle
state, then restore proxies on reception. Preserve normal subclass dictionary
and slot state; do not rerun validators, read ambient sources, change units,
introduce a registry, drop validators, or wrap Python's pickle errors.
Importable validators remain usable for later overrides; local functions and
lambdas fail under normal pickle rules. Bytes must be trusted. No Weft edits.

- [x] Implement pickle state and tests for protocols, declarations, later
  overrides, normal unsupported callable failures, subclass state, non-JSON
  values and a real spawn argument.
- [x] Align transport docs, run config/full/typing/lint/docs/artifact gates,
  independently review, and record evidence.

Comprehension: pickle carries normal callable references and preserves the
already-resolved object; JSON still carries data only and runs receiver-owned
validation. This is the owner's explicit change to the former transport scope.

- Pickle implementation and independent review passed: ordinary state hooks
  convert only the two proxies, retain dictionary/slot state, and restore proxies
  without validation or ambient reads. All protocol and real-spawn tests pass.
  No additional serialization registry, dependency or public helper was added.
- The previous baseline Weft pickle regression now passes unmodified. The full
  suite passed (exit 0) with no exclusions for this amendment: `uv run pytest
  tests -q`. Only ordinary platform/backend skips remain.
- Ruff, format, mypy, documentation gates, executable guide examples and Python
  3.11 packaging smoke pass.
- Amendment spec baseline: eccdb4b plus promoted [SB-API-2] content; current
  spec SHA-256 `d8f699a7acc801a614c65a2d94ff8e011950d3b138ad02f4643f0d326ae53c6e`.
- Installed-wheel verification: all 68 transport/coexistence cases passed on
  Python 3.11, including direct Config spawn. Final implementation review passed;
  no unresolved findings.
- Owner authorized targeted commit and closure. Plan and index are completed
  together with the config transport implementation, tests and documentation.
  No downstream edits, version bump or release are included.
