# Verified review remediation

Status: completed
Class: 5 — owner-directed restriction of database names changes accepted public
input; explicit target-binding and fork-lifecycle clauses clarify the repaired
boundaries. Hardening is required for concurrent reservation cleanup, forked
resource ownership, and public compatibility changes. No +P change is proposed.
Plan type: implementation with spec revision; promotion strategy A.
Owner: SimpleBroker core and its first-party PostgreSQL/Redis extensions.
Boundary: six verified findings from the 2026-09-13 review, with one repair slice
per finding and linked consequences kept in that slice.

## Goal

Restore exclusive Redis message ownership, reject moves between different
effective targets, keep passwords out of diagnostics, fail or recover before
inherited-lock acquisition, enforce the owner's database-name character set,
and preserve configuration warning/override semantics on numeric overflow.
Keep the existing queue model and backend ownership boundaries.

The owner explicitly selected database-name restriction, using only dot, dash,
underscore, and ASCII letters/digits. That supersedes the review's proposed
percent-escaping-only repair. Cross-target moves remain unsupported.

## Source Documents

Consulted in the declared read order: `docs/program-theory.md` ([THEORY-1],
[THEORY-2], [THEORY-3], [THEORY-4], [REV-THEORY-003], [REV-THEORY-004]);
`docs/agent-context/README.md`, `docs/agent-context/decision-hierarchy.md`,
`docs/agent-context/principles.md`,
`docs/agent-context/engineering-principles.md`; relevant runbooks
`writing-plans.md`, `hardening-plans.md`, `review-loops-and-agent-bootstrap.md`,
`testing-patterns.md`, `adversarial-acceptance-probes.md`,
`maintaining-traceability.md`, and `designing-agent-facing-interfaces.md` under
`docs/agent-context/runbooks/`; `docs/agent-context/lessons.md` and the Golden
Rules/current dated entries in `docs/lessons.md`.

Winning contracts and rationale:

- `docs/README.md`, `docs/specs/product-section-registry.md`:
  product-section ownership.
- `docs/specs/10-cli.md` [SB-CLI-1/2/3/4]: error ownership, path admission,
  secret redaction, configuration failure order, and established JSON mode.
- `docs/specs/11-delivery.md` [SB-DELIVERY-3/5/6]: atomic move, distinct-target
  rejection, transactional generators, and cleanup ownership.
- `docs/specs/13-message-identity.md` [SB-ID-1/5]: unique stored identity and
  ID-preserving move.
- `docs/specs/16-python-library-api.md` [SB-API-2/3/5/6/9/11]: configuration,
  effective targets, handles, waiters, typed errors, and process/backend scope.
- `docs/specs/17-ops.md` [SB-OPS-7]: filesystem cleanup and non-file targets.
- `docs/implementation/06-process-session-core-ownership.md`: registry,
  manager, factory, borrowed-runner, and fork boundaries.
- `docs/implementation/07-complexity-and-state-machine-map.md` and
  `docs/implementation/09-storage-schema-and-claim-lifecycle.md`: transition
  and cleanup owners; `docs/guides/configuration.md`: user-facing names and
  redaction; `skills/interface-review/SKILL.md`: CLI/API review walk.
- `dbace84:docs/plans/2026-09-07-critical-review-remediation-plan.md`, Tasks 2/4:
  descriptor snapshots and fresh-child registry recovery.
- `docs/plans/2026-09-12-config-object-simplification-plan.md`, especially
  “Invalid values raise only when final”: current owner-selected precedence.

Retrieved historical evidence (Git sources, not live-file claims):

- `197629e2:docs/plans/2026-05-14-simplebroker-redis-second-backend-plan.md`,
  Tasks 7/8: token-based reservations and stale recovery; the old plan omitted
  atomic revalidation while expressly preserving non-stale batches.
- `813dd7ce:docs/plans/2026-08-24-comprehensive-review-findings-remediation-plan.md`:
  pre-lock fork handling requirement.
- `813dd7ce:docs/plans/2026-08-25-schema-and-representation-assumption-remediation-plan.md`,
  F8: prior acceptance of filesystem-valid punctuation, now narrowed by the owner.
- `197629e2:docs/plans/2026-07-05-independent-review-fixes-plan.md` and
  `197629e2:docs/plans/2026-07-12-code-scanning-alert-triage-plan.md`:
  safe diagnostic boundaries and credential handling.

## Spec Baseline

Code baseline: `c4819a7894d1f25ace27310a23f01b57edcc4c59`.
The review also used owner-supplied documentation revisions. These content
identifiers capture the exact starting text before this plan's backlinks:

| Source | SHA-256 |
|---|---|
| `docs/specs/10-cli.md` | `210c71f6ef767f6bef49f015c6adc01979eb24271bb6862f0f256565378aeb75` |
| `docs/specs/11-delivery.md` | `c7c24f9e605327e9e6fe64b9ea29aed9648d0a1ed00ec18aef0d4503fe23d2af` |
| `docs/specs/13-message-identity.md` | `46f3f133276222175ec69c766a62c4aed32150760eb689a37fba5252f8523c35` |
| `docs/specs/16-python-library-api.md` | `7968903851e6ebe2399efe14f99f2d341c23f50423995d4dfc11a0075bd6c510` |
| `docs/program-theory.md` | `6a830707ff45ef6826d5e2353b1522ed7044a68d050940ecb353f5c1ab824b14` |

Promotion baseline: record the resulting spec commit, or diff base plus exact
spec hashes, in the execution log after promotion. Preserve unrelated owner
edits; do not reset any source to HEAD to apply this plan.

## Findings and Dispositions

| ID | Verified path and impact | Contract | Disposition |
|---|---|---|---|
| F1, P1 | Old Redis recovery snapshots IDs; old generator closes; a fresh batch reserves the ID; recovery removes the new reservation; two moves commit. Deleting one copy removes the shared body and strands the other queue index. Real Redis failure and ordered control reproduced. | [SB-DELIVERY-3], [SB-ID-1/5] | S1: atomic token-checked recovery. |
| F2, P2 | Two public Queues have the same raw Redis target but distinct config-derived namespaces. The unsupported move succeeds into the source namespace. Single/multi-queue waiters can subscribe to the default namespace instead. Both persistent modes reproduced; explicit-namespace and same-target controls distinguish the defect. | [SB-DELIVERY-3], [SB-API-2/6] | S2: bind one effective Redis target before storage, comparison, or subscription. |
| F3, P2 | A malformed PostgreSQL URI with an inline password reaches ordinary CLI stderr and the public project resolver's exception string through raw psycopg parse text. No connection is needed. The existing redactor also raises on malformed IPv6 brackets. | [SB-CLI-3], redaction design; API advisory contract remains narrower | S3: safe parse diagnostics plus total malformed-URI redaction. |
| F4, P2 | Parent public write holds session admission while another thread forks; inherited persistent Queue write/read in the child hangs before SQL fork rejection. Unheld controls raise; parent remains usable. | [SB-API-11] | S4: process preflight before shared-manager/session locks, preserving backend policy. |
| F5, P2 | `BROKER_DEFAULT_DB_NAME=100%.db` passes old name validation but breaks argparse help; Python 3.14 validates help during construction and rejects all CLI commands. Python 3.11 help also fails. | Owner's new grammar; [SB-CLI-2/3], [SB-API-2] | S5: restrict database-name components to `[A-Za-z0-9._-]+`, not an escaping-only fix. |
| F6, P2 | Real TOML `BROKER_CACHE_MB = inf` or `-inf` raises bare OverflowError despite valid override 32, with no warning. `nan` and invalid-string controls warn and return 32. | [SB-API-2/9] | S6: translate ordinary numeric overflow through the existing invalid-value path. |

No-action register: do not reopen claim-before-processing loss windows, live
peek incompleteness, caller-owned same-thread generator close, backend-owned
durability, trusted project-file authority, alias self-retargeting without
proved behavioral harm, or the owner-assigned Weft/Taut config migration.
None is a dependency of these repairs.

Principle-level diagnosis: F1 and F4 are ownership checks placed after the
boundary they must protect (engineering principles §9); S1 moves the check
inside atomic mutation and S4 moves it before inherited locks. F2, F3 and F5
require canonicalization/admission at their existing public boundaries (§2),
not repeated fixes in downstream consumers. F6 missed one ordinary validator
failure (§1 and §12); extend the existing translation and give that case a
firing test. Real public regressions (§4 and §10) govern every slice.

## Context and Key Files

| Owner to read first | Current responsibility and edit boundary |
|---|---|
| `extensions/simplebroker_redis/simplebroker_redis/core.py`, `scripts.py`, `keys.py` | `recover_stale_batches` scans/qualifies tokens; current unsafe SMEMBERS/pipeline spans ownership changes. BEGIN_BATCH, COMMIT_CLAIM_BATCH, COMMIT_MOVE_BATCH, and ROLLBACK_BATCH already mutate token state atomically. |
| `simplebroker/sbqueue.py` | `_canonicalize_queue_target` binds/detaches descriptors; `_move_destination_name` uses `_activity_waiter_identity`; waiter hooks consume that identity. Fix their common input, not three independent comparisons. |
| `extensions/simplebroker_redis/simplebroker_redis/plugin.py` | `init_backend` reuses `_normalize_backend_options` / `_namespace_from_options`; `create_runner` also normalizes. PostgreSQL's similarly named hook has different enrichment behavior. |
| `extensions/simplebroker_pg/simplebroker_pg/plugin.py`, `simplebroker/_targets.py` | `_validated_target` currently echoes ProgrammingError; `redact_backend_target` calls parsed-URL redaction before its raw-userinfo fallback. |
| `simplebroker/db.py`, `simplebroker/_broker_session.py` | DBConnection holds `_shared_key` including PID and `_shared_session`; acquisition, project setup, cleanup and release must not enter inherited locks. Registry recovery already retains abandoned graphs without finalizing them. |
| `simplebroker/_constants.py`, `simplebroker/_paths.py`, `simplebroker/cli.py`, `simplebroker/_runner.py`, `simplebroker/_project_config.py` | Config coercion, compound database names, CLI path/error ownership, direct SQLiteRunner admission, and explicit SQLite project targets. Do not tighten the generic directory/config-file validator. |

Required comprehension checks: before editing, write answers in the execution
log; compare with these expected answers. A wrong answer blocks that slice
until its owners are reread.

1. Why is “read stale IDs, then MULTI/EXEC their deletion” insufficient?
   Expected: atomic deletion does not make its prior ownership snapshot atomic;
   the same ID may belong to a new token. Verify current token metadata and
   obtain its live ID set inside the deletion operation.
2. Why not compare whole Config objects for moves or call every plugin's
   initializer during Queue construction? Expected: unrelated tuning is not
   target identity; PostgreSQL initialization may enrich credentials/defaults
   and discard options, while generic eager initialization changes timing.
3. Why not reject every inherited runner? Expected: SQL cores reject inherited
   use, but Redis core/runner recovery is intentional. Persistent manager state
   must respect this difference without touching parent-owned locks/resources.

## Invariants and Constraints

- One pending message has one owning queue; moves preserve its ID and body.
  Recovery must never remove a fresh token's reservation. Preserve stale-age
  policy, expiry defaults, exact timestamp arithmetic, and namespace isolation.
- Reject different effective Queue targets before acquiring/mutating the source
  for a move. Do not implement cross-target transfer. Same target with different
  unrelated Config tuning remains compatible; session sharing still uses full Config.
- Bind configuration once. Preserve supplied Config identity, detached target
  options/getters, explicit-option precedence, runner identity and borrowed
  resource ownership. No ambient rereads and no backend normalization formula
  copied into core.
- Keep password-bearing driver input and parse text out of normal diagnostics
  and formatted exception chains. Error class/exit code and established JSON
  mode remain truthful; errors may have safer text.
- Never wait for or finalize parent-owned state in a child. Fresh child Queues
  remain usable. Suspended parent generators are not transferable to a child.
- Database-name restrictions apply only to the SQLite filename/name fields
  defined below, not PostgreSQL database names, Redis namespaces, arbitrary
  parent directories, or project-config filenames. Preserve containment,
  Windows reserved-name checks, path depth, and traversal rejection.
- Keep one field validator and one invalid-value aggregation path. Do not catch
  arbitrary application exceptions or BaseException as invalid configuration.
- No new dependency, daemon, schema migration, delivery mode, lease-renewal
  protocol, public plugin hook, or backend API version change is planned.
  Discovery of a required shared/private plugin seam break triggers replanning
  against [SB-API-11], not a silent waiver of its version rule.

## Rollout, Rollback, and One-Way Doors

These slices do not publish packages or mutate an installed production target.
During a later authorized release, apply the repository's exact-SHA release
driver and artifact gates; do not infer version numbers in this plan.

Redis key layout and token wire values remain readable by both versions, but
an old recoverer can still corrupt new reservations. Quiesce clients for each
affected namespace and upgrade every client before claiming F1 protection.
The fix prevents new corruption; it does not reconcile existing duplicated IDs
or missing bodies. Preserve evidence and handle any data repair separately.

Name restriction intentionally rejects previously accepted SQLite names.
Document this compatibility change in CHANGELOG and the public guide before
shipping; check the owner's downstream defaults and examples. Do not rename
database/WAL/SHM/phase-marker files automatically, or suggest moving only a live
main database. A migration requires stopped clients and separately reviewed
operator instructions. Version policy for publication must acknowledge this
input contraction (a major release unless the owner explicitly chooses an
exception); authorization of the grammar is not a choice of release version.
On 2026-09-14, after this compatibility effect was surfaced explicitly, the
owner directed continuation of the already prepared coordinated release and
chose the exception: publish the contraction in SimpleBroker 8.2.0, with both
extensions at 4.2.0. This is a deliberate project version-policy decision, not
a claim that the input change is SemVer-compatible by default.

Source rollback is possible without a storage migration but restores the
respective bugs and old input policy. Rolling back F1 is not a safe service
fallback. A published artifact cannot be unpublished as a rollback promise.
No automatic cleanup of prior corruption or credentials in historical logs is
part of this implementation.

## Proposed Spec Delta

Strategy A for the following insertions: promote exact text before code slices,
without new implementation-link claims. Existing section mappings continue to
describe baseline code until their repair slice updates them. The source-tree
spec becomes the sole contract after promotion; this appendix then records the
reviewed proposal. No program-theory revision is needed.

### [SB-CLI-2] — insert after the environment-validation paragraph

> SQLite database names use only ASCII letters (`a-z`, `A-Z`), digits (`0-9`),
> dot (`.`), dash (`-`), and underscore (`_`). Each name component is nonempty
> and matches `[A-Za-z0-9._-]+`; the special traversal components `.` and `..`
> remain invalid. The grammar is necessary but not sufficient: it is ANDed
> with existing traversal, containment, platform-specific reserved-name and
> length checks. `DEFAULT_DB_NAME` and relative `--file` names retain their
> existing single optional directory component, with this grammar applied to
> each component. Path separators delimit components and are not admitted by
> the component grammar. For absolute `--file` paths, the grammar applies to
> the terminal filename; parent-directory paths retain their existing rules.
> Invalid names are rejected before target creation or mutation, with the
> allowed character set in the diagnostic. A bad environment default retains
> the preparse exit-1 rule; a bad explicit filename uses the established plain
> or JSON error dialect. No existing database is renamed automatically.

### [SB-API-2] — append to the target-resolution provisions

> The SQLite filename grammar in [SB-CLI-2] also applies to filesystem targets
> supplied through Queue, open_broker, SQLiteRunner, and SQLite project-target
> discovery. On explicit filesystem paths it constrains the terminal database
> filename, not arbitrary parent directories, whether the explicit path is
> relative or absolute. Thus `db_path="my dir/broker.db"` remains admissible;
> `DEFAULT_DB_NAME="my dir/broker.db"` is a compound default name and is
> rejected under [SB-CLI-2]. Validation precedes filesystem
> creation or backend setup and raises ValueError (or its existing typed
> boundary-specific subclass). Empty and `:memory:` non-filesystem targets
> retain their existing supported/rejected behavior at boundaries that already
> recognize them as non-filesystem targets; an absolute filename ending in
> `:memory:` is not a sentinel. Queue's omitted/empty target still selects its
> configured default. Both a supplied filename and the resolved filesystem
> filename must satisfy the grammar; symlink resolution cannot admit an
> otherwise invalid name.
> The grammar does not constrain PostgreSQL database names, Redis namespaces,
> or project-configuration filenames.

> A Redis Queue constructed with a BrokerTarget and no injected runner binds
> its effective namespace from explicit backend options or, when omitted, its
> retained Config. The bound descriptor supplies storage, move compatibility,
> and activity-waiter identity. `db_target` reports that effective namespace
> in a detached options dictionary. Construction validates and normalizes
> these Redis options without creating a runner, contacting storage, or
> allocating a listener. The original explicit target string and project
> metadata are preserved. Injected runners retain their existing ownership
> and identity rules; unrelated Config tuning does not make queue targets
> incompatible. A Queue destination with a different effective target is
> rejected under [SB-DELIVERY-3]. Unknown-option and namespace/schema errors
> raise the existing DatabaseError at construction, rather than first use.

### [SB-API-11] — append to the fork-before-lock provision

> A no-runner persistent Queue must check inherited process-session ownership
> before acquisition, project setup, or cleanup can enter a parent-owned
> lock. Inherited SQL-backed handles reject operation acquisition with
> RuntimeError rather than reopening an inherited SQL core. Redis handles
> preserve recovery by acquiring child-owned session state through the
> process registry. Releasing an inherited lease must not finalize parent
> resources. Newly constructed child Queues remain usable; these rules do
> not transfer a suspended parent generator to the child or change the
> low-level injected runner's own fork policy.

## Tasks and Slice Order

### Preparation and spec promotion

- [x] Obtain independent review of this plan and exact delta; answer every
  finding in the review log before implementation.
- [x] Apply the delta and record its promotion baseline. Retain the backlinks
  installed with this plan. Run docs gates. Move the Status Index row to active
  when implementation is authorized and begins.

S1–S6 are separate reviewable repair units. Their order below is the default.
S1 can be implemented in parallel with S3 or S5 in isolated write scopes; S2
and S4 share ownership rationale and should be sequenced. Shared CHANGELOG,
spec mappings, and implementation-doc edits have one integrator. Each slice
owns its regression, rationale update, and changelog entry; integration is not
a substitute for slice-local evidence.

### S1 — Atomic Redis stale-batch recovery (F1)

- [x] Read the token scripts and recovered Task 8. Modify Redis `core.py` and
  `scripts.py`; extend existing `test_redis_batches.py` and
  `test_redis_atomicity.py` under the extension test directory. Update
  `docs/implementation/09-storage-schema-and-claim-lifecycle.md`, Redis README,
  and the delivery/identity verification mappings.
- [x] Keep Python SCAN, namespace filtering, metadata parsing, exact integer
  age qualification (`created_ns <= cutoff`), and negative-disable behavior.
  Replace the Python SMEMBERS/pipeline mutation with one new
  `RECOVER_STALE_BATCH` Lua constant. KEYS are source reserved set, token ID set,
  and the discovered token metadata key; ARGV are snapshot source and raw
  creation timestamp. Inside Lua, require current source and created_ns to
  match both arguments exactly; disappearance/change returns zero. Then read
  the live token IDs, remove their reservations, delete the two token keys,
  and return the recovered count. Aggregate actual counts in Python.
  Preserve the count's existing meaning: number of live token IDs processed
  (`#ids`), with zero for a token skipped by revalidation.
- [x] Do not use Lua tonumber on nanosecond timestamps. Snapshot equality
  makes Python's exact age decision valid at mutation time. Existing UUID token
  identity and atomic BEGIN/COMMIT/ROLLBACK suffice; do not add a per-ID owner map.
- [x] Capture a red failing public Queue schedule before editing using a
  pass-through pause after the old real SMEMBERS response. After the unsafe
  read is removed, bind the equivalent pause before real recovery EVAL
  dispatch. Record this test-seam change explicitly; do not keep a dormant
  baseline-only hook. All responses, commands, generators and Redis stay real.
  Close the old generator and create the fresh one in their owner threads.
- [x] Prove exact single-winner ID/body conservation, empty loser/source,
  pending-count/read agreement, and that deleting the losing destination
  cannot remove the surviving body. Cover claim and move, non-stale/absent/
  refreshed/malformed metadata, inclusive age cutoff, repeated/concurrent
  recovery, both commit-versus-recovery orderings, rollback before recovery,
  and namespace isolation. Use existing redis_url/redis_namespace fixtures.
  One case uses default 300-second settings with only the old token age
  backdated as an explicit age fixture; another uses real expiry with the same
  one-second setting for every actor. Never delete reservations to fake the race.
- [x] Verify using `uv run --locked ./bin/pytest-redis extensions/simplebroker_redis/tests/test_redis_batches.py extensions/simplebroker_redis/tests/test_redis_atomicity.py -n 0`.
  Then run the wrapper on `tests/test_generator_methods.py`,
  `tests/test_batch_operations.py`, and `tests/test_move.py`.
  Done signal: decisive cases execute, one winner survives, and independent
  slice review finds no missing token-release ordering.
  Stop/replan for token reuse, lease renewal, new keys, schema changes, or
  automatic repair of existing corruption.

### S2 — Bind effective Redis target once (F2, including waiters)

- [x] Extend `simplebroker/sbqueue.py::_canonicalize_queue_target` to accept
  the retained Config and runner-presence information. Only for an explicit
  Redis BrokerTarget with no injected runner, call its existing
  `plugin.init_backend(config, toml_target=target.target,
  toml_options=detached_options)`. Retain the returned normalized options in
  the bound descriptor; preserve the original target string and every project
  metadata field. Continue using snapshot_key_material/replace for detachment.
- [x] Do not normalize SQLite, PostgreSQL, or other named plugins through this
  branch. PostgreSQL's initializer can rewrite credentials/defaults and is not
  interchangeable with runner construction. Do not import Redis-private
  helpers, duplicate namespace fallback logic, add a required plugin hook, or
  compare whole Config objects. Existing storage, move and waiter paths should
  consume the same bound descriptor without new parallel identity logic.
- [x] Extend `extensions/simplebroker_redis/tests/test_redis_core_behaviors.py`,
  `test_redis_plugin_contract_edges.py`, and `test_redis_activity_waiter_lifecycle.py`;
  retain core delegation coverage in `tests/test_queue_move_cross_target.py`
  and `tests/test_activity_waiter_api.py`. Prove persistent/ephemeral rejection
  for different Config-derived namespaces before source mutation; exact
  source ID/body stays present and both possible destination namespaces stay
  empty. Prove same-target success across equivalent explicit namespace,
  legacy schema and Config-fallback forms; explicit namespace overrides Config.
- [x] Different unrelated Config tuning must still permit compatible moves
  and waiter grouping. Prove real writes wake single/multi-queue waiters in the
  effective namespace; different namespaces reject grouping. Preserve Config
  identity, detached reporting, injected-runner identity, redacted failures,
  opaque option snapshots, and SQLite/PG/custom-plugin behavior. Recording
  boundary tests may prove zero runner/listener/storage creation at construction;
  the routing and wakeup proof itself uses real Valkey.
- [x] Update implementation doc 06 and the [SB-API-2/6] mappings. Verify with
  `uv run --locked pytest -n 0 tests/test_queue_move_cross_target.py tests/test_activity_waiter_api.py tests/test_process_broker_session.py tests/test_queue_api_additions.py tests/test_backend_plugin_resolution.py`
  and the Redis wrapper over the three extension files above plus
  `extensions/simplebroker_redis/tests/test_redis_plugin_validation_paths.py`.
  Pin DatabaseError at construction for unknown options and namespace/schema
  mismatch, and record the earlier error timing in CHANGELOG.
  Done signal: rejected cross-target requests have no durable effect and
  waiters follow the reported effective target. Stop for shared hook changes,
  eager network activity, discarded option fields, or cross-target transfer.

### S3 — Safe PostgreSQL parse diagnostics and redaction (F3)

- [x] Modify `extensions/simplebroker_pg/simplebroker_pg/plugin.py::_validated_target`
  to translate ProgrammingError without echoing raw driver text. Prefer a
  stable actionable parse diagnostic; do not invent a parser for psycopg prose.
  Suppress raw parser context in normal formatted exception chains, while
  retaining DatabaseError and the existing CLI translation/JSON owner.
- [x] In `simplebroker/_targets.py`, make malformed URL parsing fall through
  to the existing conservative raw-userinfo/conninfo redaction paths rather
  than raising before them. Keep ASCII-control neutralization and conservative
  masking. No raw-target fallback on a redaction failure.
- [x] Extend `tests/test_target_redaction.py`, `tests/test_project_config.py`,
  `tests/test_cli_contract_sb_cli.py`, and
  `extensions/simplebroker_pg/tests/test_pg_plugin_contract_edges.py`.
  Use fake markers through ordinary CLI env target and public
  resolve_broker_target(project config): marker absent from warnings,
  exception text/formatted chains, and plain/established-JSON stderr; exit 1,
  empty stdout, no traceback and no storage contact. Cover malformed brackets,
  URL/conninfo passwords, separate BACKEND_PASSWORD, valid target controls,
  and BrokerTarget display/repr. Real psycopg parsing must remain real.
- [x] Verify the four test files above with `uv run --locked pytest -n 0`;
  parser cases must execute without a server. Run remaining PG integration via
  the PG wrapper during final verification. Update configuration guide and
  implementation doc 07 with the safe error boundary.
  Done signal: every marker is absent and correct error classes/dialects remain.
  Stop for blanket exception swallowing or a second credential formatter.

### S4 — Check shared-manager process ownership before locks (F4)

- [x] Modify `simplebroker/db.py` at DBConnection's shared acquisition and
  cleanup boundaries. Use its existing `_shared_key.pid` as the ownership
  fact; add one owner-local preflight called before project setup or session
  admission. A mismatched SQL-backed shared manager raises RuntimeError with
  the existing “forked process” recovery guidance before any inherited lock.
  Do not move the check into a retry loop or wait for an inherited lock.
- [x] For direct Redis shared managers, preserve recovery by acquiring a fresh
  child registry/session through acquire_process_broker_session, with the
  same bound target and Config. Reuse the registry's PID recovery and abandoned
  graph retention. Reset manager-local project-setup synchronization/state and
  thread-local operation bookkeeping before use. Do not close or drain the
  old session/factory/core graph. Existing injected-runner and non-shared paths
  retain their backend-specific policy; no blanket runner rejection.
- [x] Guard cleanup and release-after-use too: a failed child acquisition must
  not pop/release an inherited operation lease in Queue's finally block, and
  cleanup must not recycle a parent thread's core. Closing an inherited shared
  lease uses the existing registry release path, which discards stale-PID keys
  without finalizing parent resources. Read current PID through the existing
  `simplebroker._broker_session._getpid` module seam at call time, the same
  seam that stamps `_SessionKey.pid`; do not copy its binding into a second
  independent seam or patch shared os.getpid in concurrent tests.
- [x] Extend `tests/test_fork_safety.py` and
  `tests/test_process_broker_session.py`, plus
  `extensions/simplebroker_redis/tests/test_redis_pool.py` for a public shared
  Redis Queue recovery control. Use real POSIX fork with parent-owned ready
  state and bounded pipe result/child kill cleanup. Pause an ordinary parent
  Queue write inside admission using a pass-through test hook or trace, not a
  fake storage implementation. Test child write/read/get_core acquisition,
  cleanup/close after rejection, held/unheld controls, fresh child Queues,
  parent continued exact message use, and Redis child recovery. Include held
  project-setup state for the acquisition ordering covered by the new preflight.
  Tests skip only where fork is unavailable; liveness clocks are watchdogs.
- [x] Update implementation doc 06; verify with
  `uv run --locked pytest -n 0 tests/test_fork_safety.py tests/test_process_broker_session.py tests/test_runner_lifecycle.py`
  and the Redis wrapper on `extensions/simplebroker_redis/tests/test_redis_pool.py`.
  Done signal: inherited SQL use reports promptly, direct recovery works, and
  no child path waits on or finalizes parent-owned state. Stop for generator
  transfer, new public runner policy, resetting a live parent's locks, or
  abandoning resources without the existing retention mechanism.

### S5 — Restrict database names to the owner's grammar (F5)

- [x] Add one private component/name validator beside existing config path
  validators in `simplebroker/_constants.py`; reuse it from `_db_name_path`,
  `simplebroker/_paths.py::_is_compound_db_name`, CLI file admission, SQLite
  project-target resolution, `simplebroker/db.py::BrokerDB.__init__` before
  its parent mkdir, and `simplebroker/_runner.py::SQLiteRunner`
  filesystem-target admission. BrokerDB currently creates parents before
  constructing SQLiteRunner, so runner-only validation is too late for
  Queue/open_broker's no-side-effect contract. Do not modify the generic directory validator
  to impose database naming rules on unrelated paths. At direct path boundaries
  validate the terminal filename; in DEFAULT_DB_NAME and relative --file's
  compound-name form validate every selected name component.
- [x] Where canonicalization can discard the supplied spelling, validate the
  incoming terminal filename before Path.resolve and the resolved terminal
  filename before opening/setup. Cover `sbqueue.py::_canonicalize_queue_target`
  for no-runner SQLite handles, BrokerDB's path resolution, and explicit SQLite
  project-target resolution. Keep the generic target-identity normalizer pure;
  do not impose directory rules there. Injected runners own their actual target
  admission; an ignored Queue path is not a second filesystem target to open.
  Test invalid symlink name → valid file and valid symlink name → invalid file,
  with no mutation. Recognize empty/`:memory:` only at existing non-file
  boundaries; do not exempt an absolute colon-containing filename.
- [x] Keep component selection at the existing boundary: `_is_compound_db_name`
  handles compound default names, not arbitrary explicit Python paths. A single
  pure component validator supports both policies without a second grammar.
  Explicit Python relative paths preserve their parent-directory spellings
  and depth just like absolute paths. Test `my dir/broker.db` as an allowed
  explicit Python target and a rejected DEFAULT_DB_NAME/relative --file name.
- [x] Keep path separators structural, existing absolute-path allowance,
  containment, optional one-directory name depth, reserved Windows names,
  dot/traversal rejection, and supported non-file sentinels. Validate before
  creating directories, database, WAL/SHM, or phase markers. Existing files
  with disallowed names must fail cleanly, not be renamed/deleted/opened first.
- [x] The former `%` example must now fail with a character-set diagnostic
  before parser construction when supplied in the environment. No additional
  argparse escaping-only solution is required to admit rejected names. Test
  every allowed character class and rejected `%`, whitespace, Unicode,
  punctuation, controls and malformed path components; match diagnostic
  meaning, not full wording. Preserve legitimate parent directories containing
  spaces/punctuation when the explicit absolute terminal filename is valid.
  Include explicit `.`/`..` rejection and backslash-delimited compound names
  using the existing separator-normalization rules.
- [x] Extend `tests/test_constants.py`, `tests/test_cli_validation.py`,
  `tests/test_cli_contract_sb_cli.py`, `tests/test_path_security.py`,
  `tests/test_project_config.py`, and `tests/test_connection_config.py`.
  Exercise config TOML/env/override, `--help`, `--version`, read/write and an
  explicit file override, public Queue/open_broker, and direct SQLiteRunner.
  Reject bad env defaults even with explicit valid --file (existing preparse
  precedence); reject explicit bad filenames in established JSON mode. Verify
  no filesystem mutation and valid-name CLI/API round trips on Python 3.11
  and 3.14. Existing punctuation tests for parent directories stay meaningful;
  update database-name acceptance expectations to the owner-selected contract.
- [x] Update README's database-name examples/catalog and the configuration
  guide, plus implementation doc 07. Verify the six modules above with
  `uv run --locked pytest -n 0`; run relevant black-box cases on both declared
  interpreter versions using isolated uv environments. Inspect Weft/Taut
  configured names read-only and record any migration impact; their config
  API migration is still outside this plan.
  Done signal: one grammar fires at every listed entry path and all failures
  are clean before side effects. Stop for directory-name restrictions, silent
  character substitution, automatic database migration, or weakened containment.

### S6 — Numeric overflow through the existing config failure path (F6)

- [x] Extend `simplebroker/_constants.py::_validated_value` to translate
  OverflowError alongside TypeError/ValueError into InvalidConfigError. Reuse
  safe display, field sensitivity, source/key metadata, warning emission and
  end-of-sources failure aggregation. Do not reject all nonfinite numbers in
  unrelated fields or catch arbitrary custom-validator RuntimeError.
- [x] Extend `tests/test_config_builder.py`: real TOML inf/-inf for an integer
  field, with/without a valid later override; `nan`/invalid-string controls;
  direct override overflow; source labels; one warning per invalid supplied
  value; final typed error; sensitive custom-validator overflow uses redacted
  value metadata. Confirm a custom RuntimeError still propagates and supplied
  Config remains unvalidated on its fast path.
- [x] Verify `uv run --locked pytest -n 0 tests/test_config_builder.py tests/test_constants.py`.
  Update implementation doc 07 and [SB-API-2/9] verification mapping.
  Done signal: valid later input wins with a warning, final invalid input is
  typed, no duplicate validator or alternate precedence path appears.
  Stop for changed units, field bounds, source order, or blanket exception catch.

## Testing Plan and Final Gates

Use failing-first real public proofs at each slice. The review's temporary
probe files are optional diagnostics, not durable prerequisites. Port the
schedule and assertions above into the repository's existing test owners.
All server tests use owned unique namespaces/schemas and close resources before
cleanup. Fake servers, no-op Redis responses, and tests that remove the
contention under review are not acceptance evidence.

- [x] Independently review each meaningful slice, answering every finding;
  then review the integrated diff against all six dispositions and invariants.
- [x] Run `uv run --locked pytest` with its default concurrency policy.
- [x] Run `uv run --locked ./bin/pytest-redis` and
  `uv run --locked ./bin/pytest-pg`. The wrappers provision local test services
  and run shared/extension cohorts. Required backend regressions must execute;
  an all-skipped or unconfigured backend run is not a green gate.
- [x] Run `uv run --locked ruff check .` and
  `uv run --locked mypy simplebroker bin/release.py`; use the current release
  driver's established extension/test mypy enumerators for changed cohorts,
  retaining negative fixtures in their dedicated gate.
- [x] Run `python3 bin/check-dom15-fixtures`, `bin/check-plan-context`,
  `bin/check-doc-paths`, and `git diff --check`. Reconcile any affected named
  state-machine tables with `tests/test_state_machine_policy.py`; extend an
  existing table when its transition changes, without inventing a new framework.
- [x] Reconcile specs, README/guide restatements, CHANGELOG, implementation
  rationale, and this plan. Add a concise dated lesson only for a new reusable
  correction not already captured by existing ownership/validation rules;
  no durable process promotion is part of this plan.
- [x] Record exact commands, non-skipped results, changed files, review
  dispositions and residual platform limits. Final readiness requires every
  enumerated acceptance case and each touched contract mapping to fire.
- [x] When implementation is completed, close this Status Index row in the
  same change. Completion/land-readiness claims require the requested commits
  to be verified in git log; do not commit on the owner's behalf merely to
  satisfy that gate. If reviewed uncommitted, report that state and changed
  files without claiming landed completion.

Post-deploy acceptance for a later authorized release: run the repaired
single-winner recovery schedule in a disposable namespace against the actual
installed artifacts; observe rejected cross-target moves with source intact,
correct waiter wakeups, secret-free malformed-target diagnostics, bounded
fork results, clean rejected database names, and successful valid config
overrides. Use positive state evidence, not absence of log warnings alone.

## Independent Review Loop

Use `skills/call-agent/SKILL.md` for a different-family plan reviewer (Claude
is locally installed); bound attempts, retain command/timeout/outcome evidence,
and fall back to an independent same-family role only with the limitation
recorded. The planning task writes the plan, backlinks and reviewer-availability
evidence; it does not authorize implementation or publication.

Reviewer brief: review this six-finding remediation at the recorded baseline
and exact proposed delta, not the whole subsystem afresh. Existence-check every
symbol/path/command first. Accepted boundaries are the no-action register and
the owner's name grammar. Existing unrelated concerns belong in a separate
observations outlet unless this plan worsens them. Prefer removing unnecessary
work. Return PASS/BLOCKED based on whether implementation is unambiguous and
whether it would degrade correctness/security, with severity, location, and
suggested disposition per finding. Root must answer each point explicitly.

Apply the interface-review walk to repaired CLI/API behavior before integration
readiness. The walk must preserve compact outputs, identity, configuration
ownership, error dialects, unsupported cross-target rejection and trust scope;
it does not introduce new guidance payloads into this Unix CLI.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|---|---|---|---|---|
| [SB-CLI-2], [SB-API-11] | Filename paragraph was mislabeled SB-CLI-3; API-11 insertion referred to an absent fork-before-lock paragraph | Promote filename text under actual path-admission owner SB-CLI-2 and fork paragraph within API-11; qualify old permissive filename restatement | Verified live section ownership during promotion; reviewed behavior is unchanged | Citation/insertion correction only; no new behavior. |

## Review Log

Implementation slice review: Claude used the documented read-only invocation
with a 540-second bound and returned after 272.8 seconds with no blocker for
S1, S2, S3 and S6. Its full result was retrieved from its harness plan artifact
(the CLI stdout contained only a summary). The two P3 observations were
validated: custom-validator exception causes retain the pre-existing chaining
policy (S6 improves the primary diagnostic without changing that policy), and
the Redis-specific normalization branch intentionally keys on the registered
backend name. Neither adds a repair dependency. Later S3 malformed-whitespace
redaction is included in final review rather than inferred covered here.

SHA-256 identifiers of the exact plan text embedded in each review brief:
first pass `85f3d8feddf8d4afb7162fd0bdfb00167d811785ee0d1da1d15e30cc07432635`;
scoped second pass `bd577ea5fcb0776be393c437bb99be0be4572c145216c8ea29202d9baf320eb9`.
Later log/index/availability entries record the returned evidence and do not
alter the reviewed repair design.

| Review | Baseline / command / bound | Result | Disposition |
|---|---|---|---|
| Planning input: Redis storage specialist | Current code, token scripts, live fixtures and recovered May 14 plan; read-only | Existing UUID layout is sufficient; exact metadata snapshot revalidation avoids Lua timestamp precision loss | Incorporated in S1; no new fencing format. |
| Planning input: target-identity specialist | Queue binding, Redis/PG/SQLite init hooks and current target tests; read-only | Blanket plugin initialization changes unrelated behavior | Incorporated Redis-only existing-hook use in S2; original target string retained. |
| Author boundary check | `simplebroker/db.py::BrokerDB.__init__`, baseline code | BrokerDB creates parent directories before SQLiteRunner admission | Added pre-mkdir reuse of the same S5 validator; no separate validation formula. |
| Author filename check | Queue canonicalization, BrokerDB and project resolution call Path.resolve; direct SQLite runner/plugin admit non-file sentinels | Resolving first can discard an invalid supplied symlink name; a blanket basename sentinel exemption could admit a colon-containing file | S5 validates supplied and effective terminal filenames at their owners, with real symlink controls; sentinel exceptions remain specific to existing non-file entry paths. |
| Claude plan review, R1 [P2] | Read-only `claude -p <embedded-plan> --permission-mode plan --allowedTools Read,Grep,Glob`; 540-second bound; exit 0 after 296.4 seconds; full draft and exact delta | PASS with a relative-path policy ambiguity | Clarification accepted. Verified explicit Queue paths use `_canonicalize_queue_target` → DBConnection/BrokerDB, not `_is_compound_db_name`; the claimed forced shared route is incorrect. Explicit Python parent paths remain exempt, relative or absolute. Compound defaults/relative CLI names constrain each selected component. Added concrete examples/tests; one component validator, no duplicate grammar. |
| Claude R2 [P3] | Same review | Regex must not replace traversal and reserved-name checks | Accepted: exact delta now explicitly ANDs the grammar with existing checks; explicit dot/traversal and separator controls added to S5. |
| Claude R3 [P3] | Same review | Eager Redis option-error timing lacks a named type | Accepted: exact delta, S2 tests and CHANGELOG requirements now pin existing DatabaseError at construction for unknown options and namespace/schema errors. |
| Claude R4 [P3] | Same review | Separate PID seams could disagree under tests | Accepted: S4 reads the existing session module's `_getpid` seam at call time; no second independent PID seam. |
| Claude scoped second pass | Same read-only invocation and 540-second bound; revised plan plus first review and accepted R1–R4; exit 0 after 127.6 seconds | PASS; all four revisions and pre-mkdir/symlink/sentinel boundary corrections verified; no new defects found | Review loop closed for the plan. In particular, the reviewer confirmed the original R1 shared-call-path premise was incorrect. Accepted rollout/input-contraction risks remain as documented. |
| Second-pass observation | Generic `_validate_cli_path_components` delegates to `_validate_safe_path_components` | Suggested this shared funnel helps reuse | No further action: that funnel also validates `--dir`, so adding the filename grammar indiscriminately there would violate S5. Retain filename-specific admission and one pure component validator, as already planned. |

## Execution Log

Spec promotion: exact reviewed behavior promoted on authorization to implement;
diff baseline `c4819a7894d1f25ace27310a23f01b57edcc4c59`. docs/specs/10-cli.md: SHA-256 0fadaaba2a293000f4dfe314f4f96bb63d891eae60dad1bb952cda5b9ccce13b; docs/specs/16-python-library-api.md: SHA-256 dd788467f1c73349bcdbdec89c2d80456c01ef236d9354d3a4f2cc97d4083a71.
Comprehension: S1 must validate token metadata and read live IDs inside atomic
mutation; S2 compares resolved namespace, not whole Config, and uses only the
Redis existing hook; S4 checks process ownership before any inherited lock,
rejecting SQL acquisition while preserving Redis child-session recovery.


Planning verification, 2026-09-13: created this plan and its Status Index row;
added Related Plans backlinks to specs 10/11/13/16 and refreshed implementation
doc 03 with the observed reviewer availability. Verified all five pinned
historical plan paths with `git cat-file -e`, every literal full path, and all
five starting-source hashes after removing only this plan's new backlinks.
`python3 bin/check-dom15-fixtures`, `bin/check-plan-context` (four in-flight
source declarations), `bin/check-doc-paths`, and `git diff --check` passed.
`bin/coalesce-check` reported all cues resolving locally; existing foreign
claims and local-only publication pins remain explicit limitations.

Implementation checkpoints (slice verification evidence):

- S1: real Redis corruption and baseline repository regression failed before
  correction. Atomic token recovery then passed 87 batch/atomicity and 36 shared
  generator/batch/move tests. The pause moved from post-SMEMBERS to pre-EVAL as
  planned; no baseline-only hook remains. Root inspected the Lua and public proof.
- S2: cross-config namespace move failed to reject on baseline; after binding,
  all 11 namespace move/control cases and 81 extension core/waiter/plugin cases
  passed. A named-Redis test double was updated to expose the already-declared
  init hook. Unrelated plugin initializers remain lazy. Config/plugin cohort:
  145 passed, including the 118 config tests shared with S6.
- S3: 12 failures reproduced across real psycopg, CLI, public resolver and
  redactor. The repair passed 53 targeted cases and 147 neighboring cases;
  whitespace/control credentials in malformed authorities received two extra
  failing-first regressions before the final redactor correction.
- S4: a real parent write paused inside admission reproduced a child hang;
  after preflight, all six write/read/get_core held/control cases passed.
  Session/fork/runner cohort: 82 passed. Redis real-fork admission/project-lock
  and no-lock controls passed; full Redis pool module: 25 passed.
- S5: 402 planned/portability cases passed with eight platform/filesystem skips;
  isolated Python 3.11.15 and 3.14.4 each passed 107 acceptance tests, plus 62
  portability/connection tests after the fallback correction. Real public raw
  terminal `/.` and separator cases failed before the basename correction.
  Downstream defaults `.weft/broker.db` and `.taut.db` conform. Custom names must
  conform on upgrade; downstream implementation remains outside this plan.
- S6: real TOML inf failed with bare OverflowError before the one-line repair.
  Config-builder tests then passed for inf/-inf/nan/string controls, later
  override, source warnings, typed final failure and sensitive metadata; a
  custom RuntimeError still propagates.

Implementation adjustments validated against concrete call paths: open_broker
now admits SQLite filenames before DBConnection retry wrapping (preserves
ValueError); relative CLI joins use the already-admitted separator convention;
raw terminal names use basename before pathlib normalization; new resolution
checks reuse existing normalize_sqlite_target fallback. S3 public diagnostics
use `tests/test_malformed_target_diagnostics.py` to avoid parallel test-file
ownership conflicts. S4's new public fork schedules live in test_fork_safety and
Redis test_redis_pool; the existing process-session module was run unchanged,
since its registry transitions did not change. The state-machine map names the
new manager preflight and tests. These are within the promoted behavior, not
spec waivers.
The first full core run passed 3687 cases with 19 skips and exposed six obsolete
filename acceptance fixtures plus one path-resolution-fallback regression; the
fixtures retain their original parent-path/quoting/legacy-cleanup purpose and
the real fallback regression was repaired before final verification.

The owner authorized implementation and, on 2026-09-14, a targeted closing
commit. All six slices and review gates are verified; this plan and its Status
Index row close in that commit. Unrelated pre-existing documentation edits stay
outside the commit. No release publication is asserted.

### Final independent review disposition

Claude read-only S4/S5 and integrated review completed with exit 0 after 495.3
seconds within the 540-second bound, using `claude -p <embedded-plan-and-diff>
--permission-mode plan --allowedTools Read,Grep,Glob`. It returned NO BLOCKER
for S4, S5 and all six slices together, and separately confirmed S3's later
whitespace/control redaction correction. Root verified the cited code and test
assertions and ran the suites independently; the reviewer performed static review.

| Observation | Root disposition |
| --- | --- |
| O1 [P3]: explicitly empty DEFAULT_DB_NAME is invalid | Accepted documentation clarification: CHANGELOG now explicitly says so. This is the promoted nonempty component rule, with ordinary configuration aggregation/override semantics preserved. |
| O2 [P3]: conservative raw redaction can over-mask query text containing @ | No action: pre-existing diagnostic tradeoff, no credential leak or storage mutation; the new branch widens masking for malformed whitespace credentials. |
| O3: sentinel boundaries | No code action: existing direct SQLiteRunner/plugin non-file entry points retain sentinels; Queue's omitted/empty target selects defaults, while filename paths must conform. The review's shorthand “only via an injected runner” is narrower than the actual supported direct-runner/plugin boundary and is not adopted as a new restriction. |

Review closure covers all six slices and the integrated production diff. Subsequent
changes were test annotations, the exact SB-OPS-7 test-name mapping, formatting,
and this documentation record; no further production behavior changed.

### Final verification record

- `uv run --locked pytest`: 3702 passed, 19 skipped. The preceding run's
  lone evidence-manifest failure was corrected in both SB-OPS-7 and its exact
  executable manifest; its targeted six-test cohort also passed.
- `uv run --locked ./bin/pytest-pg`: shared cohort 1691 passed / 12 skipped;
  extension cohort 323 passed / 7 skipped.
- `uv run --locked ./bin/pytest-redis`: shared cohort 1683 passed / 20 skipped;
  extension cohort 360 passed / 1 skipped. All new backend regressions executed.
- `uv run --locked ruff check .` and `uv run --locked ruff format --check
  simplebroker tests extensions/simplebroker_pg extensions/simplebroker_redis`:
  passed, 328 files formatted.
- Release-driver `_core_test_mypy_command` and
  `_extension_test_mypy_commands(include_pg=True, include_redis=True)`:
  215 core test files, 26 PG test files and 19 Redis test files passed.
  `uv run --locked mypy simplebroker bin/release.py`: 45 source files passed.
  Missing annotations in new fork/recovery tests were corrected. Post-edit
  Redis pool/batch cohort passed 51 tests; root fork cohort passed 20 tests.
- `python3 bin/check-dom15-fixtures`, `bin/check-plan-context`,
  `bin/check-doc-paths`, and `git diff --check`: passed. The complete core run
  includes the state-machine policy and touched contract mappings.

Residual verification limits: this host is macOS; Windows-only and filesystem
capability probes skipped. Opt-in diagnostic/autovacuum probes were not enabled.
The ordinary suite's two simultaneous PG/Redis dump-pipe tests skipped because
both service variables were not present together; dump formats were unchanged.
The installed Weft import still expects removed ResolvedConfig and its regression
skipped; that owner-assigned downstream migration is outside this repair plan.
Artifact-only expectations also remain outside source-suite verification.

Changed implementation owners: `_constants`, `_paths`, `_project_config`,
`_runner`, `_targets`, `cli`, `db`, `sbqueue`, PostgreSQL plugin, and Redis
core/scripts. Their regression tests, winning specs 10/11/13/16, SB-OPS-7 test
mapping, README/configuration guide, implementation 06/07/09, CHANGELOG and this
plan/index are reconciled. Existing owner documentation edits were preserved.

### Interface review and theory possession probe

Scope: the existing CLI and matching Python API, against the promoted delta.
The eleven interface principles were checked as follows:

| Principle | Disposition and evidence |
| --- | --- |
| 1. Context is scarce | Met: constant actionable PostgreSQL parse diagnostic, `extensions/simplebroker_pg/simplebroker_pg/plugin.py:255`; no payload expansion. |
| 2. Progressive disclosure | Met: `README.md:243` links to SB-CLI-2; `docs/guides/configuration.md:236` explains migration and parent-path distinctions. |
| 3. Self-explanatory names | Met: `simplebroker/_constants.py:519` reports the exact allowed character set. |
| 4. One identity | Met: `simplebroker/sbqueue.py:136` binds effective namespace once for moves, storage and waiters. |
| 5. Derive rather than request | Met: `simplebroker/sbqueue.py:136` derives missing Redis namespace from retained Config using the existing plugin initializer. |
| 6. No hidden sessions | Met: no new public handles; `simplebroker/db.py:928` enforces process ownership before inherited locks. |
| 7. Teach rather than reject | Ratified contraction: the owner explicitly requires rejecting invalid names. The diagnostic teaches the allowed set; automatic renaming would change target identity. |
| 8. Every message gives an action | Met for changed errors: `extensions/simplebroker_pg/simplebroker_pg/plugin.py:255`, `simplebroker/_constants.py:519`, and `simplebroker/db.py:928` teach syntax, allowed characters, or creation of a new child Queue. No new structured guidance field: the existing Unix stdout/stderr contract owns formatting. |
| 9. Atomic writes | Met: `extensions/simplebroker_redis/simplebroker_redis/scripts.py:751` validates current token metadata and releases current membership in one Lua operation. Record-merge semantics are not applicable. |
| 10. Trust boundaries | Met: `tests/test_malformed_target_diagnostics.py:47` checks public diagnostics; `tests/test_cli_contract_sb_cli.py:860` checks invalid names before storage side effects. |
| 11. Wire format matches use | Met: `simplebroker/sbqueue.py:136` binds targets without changing message serializers; `simplebroker/_constants.py:519` validates names internally. |

| ID | Severity | Location (file:line) | Finding | Suggested disposition |
| --- | --- | --- | --- | --- |
| IR-1 | P2 | `simplebroker/_constants.py:530` | Path normalization could discard invalid terminal spelling. | Resolved: validate raw basename before normalization; public regressions pass. |
| IR-2 | P2 | `simplebroker/_runner.py:215` | New validation bypassed the established path-resolution fallback. | Resolved: reuse normalize_sqlite_target; portability regression passes. |
| IR-3 | P2 | `simplebroker/db.py:1229` | open_broker admission failure could become a generic retry failure. | Resolved: admit before manager retry wrapping; public regression passes. |

Verdict: no blocker. Ratified judgments are principles 7 and 8 above.
Runbook feedback: no new candidates; current public-boundary and failing-first
guidance covers these corrections.

Review findings: raw terminal names could be normalized away, new resolution
could bypass an established fallback, and open_broker could wrap admission errors
in retry failure. All three were corrected and have firing public-boundary tests;
there are no outstanding interface findings. The input contraction and existing
Unix diagnostic format are explicit judgments, not hidden exceptions.

[THEORY-6] possession probe: should stale-batch recovery retry the application
handler? No. SimpleBroker owns atomic message/claim state, while business retries
and orchestration remain downstream. S1 repairs current ownership without adding
application policy; S2 rejects incompatible targets rather than inventing transfer.
The same boundary explains why S5 rejects names rather than migrating files.

The renamed percent-filename test also required updating the exact SB-OPS-7
verification manifest and its contract test. The six operations-contract tests
passed after both references were reconciled.

## Out of Scope

Cross-target transfer; application retries/orchestration; new lease protocols;
automatic corruption repair; wholesale configuration redesign; broad module
splits; release publication/version selection; hosted security-policy changes;
Weft/Taut implementation; unrelated active-plan closure or coalescing.

## Fresh-Eyes Review

The plan keeps six repair owners, states real public failure paths, and makes
the owner's input-policy change explicit. It preserves the difference between
target compatibility and resource-session identity, between Redis and SQL fork
policies, and between a stale snapshot and atomic current ownership. Before
offering it for implementation, existence-check all referenced files and flags,
run plan/document gates, and append independent review dispositions above.
Skills/runbook evaluation: current ownership, failing-first, and atomicity
guidance covers these corrections; no guidance expansion is proposed.
